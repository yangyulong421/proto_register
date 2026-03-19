# Media 媒体服务 — Kafka 消费者/生产者实现伪代码

## 概述

Media 服务是文件处理流水线的核心。上传完成后通过 `media.uploaded` 事件触发**自消费**处理流水线（缩略图生成、视频截图、音频波形提取、视频转码等），
处理完成后通知下游服务（Message、User、Group）。同时消费外部事件处理用户注销清理、违规内容处理和配置热更新。

**处理流水线设计：**
- **Image**：生成缩略图（200×200 等比缩放）、提取 EXIF 信息（拍摄时间/GPS/设备）
- **Video**：生成 1s 处截图、转码为 H.264/MP4（如果源格式不兼容）、生成缩略图
- **Audio**：提取时长、生成波形数据
- **Voice**：提取时长（语音消息通常不需要额外处理）
- **File**：仅提取元信息（不做处理）

## 生产 Topic 列表

| Topic | 事件 | 生产时机 | Key | 消费方 |
|-------|------|----------|-----|--------|
| `media.uploaded` | MediaUploadCompletedEvent | UploadCallback / CompleteMultipartUpload 成功后 | media_id | 自消费（触发处理流水线） |
| `media.process.result` | MediaProcessResultEvent | 处理流水线完成或失败（通过 process_status 区分） | media_id | Message（更新媒体状态/URL） |
| `media.avatar.processed` | MediaAvatarProcessedEvent | 头像处理完成（多尺寸裁剪） | owner_id | User（更新头像 URL）、Group（更新群头像 URL） |
| `media.deleted` | MediaDeletedEvent | DeleteMedia / 违规删除 / 用户注销清理 | media_id | Message（清理引用）、Search（删除索引） |
| `media.stats` | MediaStatsEvent | 定时统计任务（每 5min） | server_id | Audit（统计审计） |

## 消费 Topic 列表

| Topic | 来源 | 用途 |
|-------|------|------|
| `media.uploaded` | 自消费 | 上传完成 → 触发处理流水线（缩略图/转码/截图/元信息提取） |
| `user.deactivated` | User 服务 | 用户注销 → 标记用户媒体文件待延迟删除 |
| `audit.content.violation` | Audit 服务 | 内容违规 → 删除/封禁违规媒体文件 |
| `config.changed` | Config 服务 | 配置变更 → 热更新上传限制/允许类型/最大文件大小/处理参数等 |

## Redis Key 设计（消费者专用）

| Key 格式 | 类型 | 值 | TTL | 说明 |
|----------|------|-----|-----|------|
| `media:kafka:dedup:{event_id}` | STRING | "1" | 24h | Kafka 消费幂等去重 |
| `media:process_status:{media_id}` | STRING | JSON（status/progress/fail_reason） | 1h | 处理进度实时更新 |
| `media:info:{media_id}` | HASH | 媒体元信息 | 1h | 处理完成后更新缓存 |

---

## 消费者实现

### Consumer: `media.uploaded` — 处理流水线入口（自消费）

> 来源：自身（UploadCallback / CompleteMultipartUpload 投递）。  
> 这是整个媒体处理流水线的入口，根据媒体类型分发到不同的处理分支：
> - IMAGE → generateThumbnail + extractEXIF
> - VIDEO → generateSnapshot + transcodeIfNeeded + generateThumbnail
> - AUDIO → extractDuration + generateWaveform
> - VOICE → extractDuration
> - FILE → extractMetadata（仅提取，不处理）

```go
func (c *MediaConsumer) HandleMediaUploaded(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_media.MediaUploadCompletedEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("unmarshal MediaUploadCompletedEvent failed", "err", err)
        return nil // 反序列化失败不重试
    }

    // ==================== 2. 幂等检查 — Redis ====================
    dedupKey := fmt.Sprintf("media:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        log.Debug("duplicate media.uploaded event", "event_id", event.Header.EventId)
        return nil
    }

    // ==================== 3. 参数校验 ====================
    if event.MediaId == "" {
        log.Error("media.uploaded: media_id is empty", "event_id", event.Header.EventId)
        return nil
    }
    if event.StoragePath == "" {
        log.Error("media.uploaded: storage_path is empty", "media_id", event.MediaId)
        return nil
    }

    log.Info("media uploaded, start processing pipeline",
        "media_id", event.MediaId, "media_type", event.MediaType,
        "content_type", event.ContentType, "file_size", event.FileSize,
        "storage_path", event.StoragePath)

    // ==================== 4. 更新处理状态为 PROCESSING — PgSQL + Redis ====================
    // 延迟双删 — process_status 即将变更，使 media:info 缓存失效
    c.delayedDoubleDelete(ctx,
        fmt.Sprintf("media:info:%s", event.MediaId),
    )

    _, err := c.db.ExecContext(ctx,
        `UPDATE media_records
         SET process_status = 1, updated_at = NOW()
         WHERE media_id = $1 AND process_status IN (0, 1)`,
        event.MediaId,
    )
    if err != nil {
        log.Error("update process_status to PROCESSING failed",
            "media_id", event.MediaId, "err", err)
        return fmt.Errorf("update process_status failed: %w", err)
    }

    // 更新 Redis 处理状态
    statusJSON, _ := json.Marshal(map[string]interface{}{
        "status":   1, // PROCESSING
        "progress": 0,
    })
    c.redis.Set(ctx, fmt.Sprintf("media:process_status:%s", event.MediaId),
        string(statusJSON), 1*time.Hour)

    // ==================== 5. 获取 Bucket 信息（从 DB 确认） ====================
    var bucket string
    err = c.db.QueryRowContext(ctx,
        `SELECT bucket FROM media_records WHERE media_id = $1`,
        event.MediaId,
    ).Scan(&bucket)
    if err != nil {
        log.Error("query bucket failed", "media_id", event.MediaId, "err", err)
        return fmt.Errorf("query bucket failed: %w", err)
    }

    // ==================== 6. 根据媒体类型分发处理 ====================
    var processErr error
    switch event.MediaType {
    case kafka_media.MEDIA_TYPE_IMAGE, kafka_media.MEDIA_TYPE_AVATAR:
        processErr = c.processImage(ctx, &event, bucket)
    case kafka_media.MEDIA_TYPE_VIDEO:
        processErr = c.processVideo(ctx, &event, bucket)
    case kafka_media.MEDIA_TYPE_VOICE:
        processErr = c.processVoice(ctx, &event, bucket)
    case kafka_media.MEDIA_TYPE_FILE:
        processErr = c.processFile(ctx, &event, bucket)
    default:
        log.Warn("unknown media type, skip processing",
            "media_id", event.MediaId, "media_type", event.MediaType)
        processErr = c.processFile(ctx, &event, bucket) // 未知类型当文件处理
    }

    // ==================== 7. 处理结果 ====================
    if processErr != nil {
        log.Error("media processing failed",
            "media_id", event.MediaId, "media_type", event.MediaType, "err", processErr)
        c.handleProcessFailed(ctx, &event, processErr)
        return nil // 不重试，已记录失败状态
    }

    log.Info("media processing completed",
        "media_id", event.MediaId, "media_type", event.MediaType)
    return nil
}

// ========== 图片处理流水线 ==========

// processImage 处理图片：生成缩略图 + 提取 EXIF
func (c *MediaConsumer) processImage(ctx context.Context, event *kafka_media.MediaUploadCompletedEvent, bucket string) error {
    log.Info("processing image", "media_id", event.MediaId, "storage_path", event.StoragePath)

    // ---- 6a. 从对象存储下载原图到临时文件 ----
    tmpFile, err := c.ossClient.DownloadToTemp(ctx, bucket, event.StoragePath)
    if err != nil {
        return fmt.Errorf("download image from OSS failed: %w", err)
    }
    defer os.Remove(tmpFile) // 清理临时文件

    // ---- 6b. 提取图片尺寸 ----
    imgConfig, err := imaging.DecodeConfig(tmpFile)
    if err != nil {
        log.Warn("decode image config failed, skip dimension extraction",
            "media_id", event.MediaId, "err", err)
    }
    width := imgConfig.Width
    height := imgConfig.Height

    // 更新处理进度 — Redis
    c.updateProcessProgress(ctx, event.MediaId, 30)

    // ---- 6c. 生成缩略图（等比缩放到 200x200 以内） ----
    thumbnailKey := ""
    img, err := imaging.Open(tmpFile)
    if err == nil {
        // 等比缩放，最长边不超过 200
        thumbnail := imaging.Fit(img, 200, 200, imaging.Lanczos)

        // 保存缩略图到临时文件
        thumbTmpFile := tmpFile + "_thumb.webp"
        if err := imaging.Save(thumbnail, thumbTmpFile); err == nil {
            defer os.Remove(thumbTmpFile)

            // 上传缩略图到对象存储
            thumbnailKey = strings.TrimSuffix(event.StoragePath, filepath.Ext(event.StoragePath)) + "_thumb.webp"
            if err := c.ossClient.Upload(ctx, bucket, thumbnailKey, thumbTmpFile, "image/webp"); err != nil {
                log.Error("upload thumbnail failed", "media_id", event.MediaId, "err", err)
                thumbnailKey = "" // 缩略图上传失败不影响整体流程
            }
        } else {
            log.Warn("save thumbnail failed", "media_id", event.MediaId, "err", err)
        }
    } else {
        log.Warn("open image for thumbnail failed", "media_id", event.MediaId, "err", err)
    }

    c.updateProcessProgress(ctx, event.MediaId, 60)

    // ---- 6d. 提取 EXIF 信息 ----
    exifData := make(map[string]interface{})
    if exifInfo, err := exif.Decode(tmpFile); err == nil {
        if tm, err := exifInfo.DateTime(); err == nil {
            exifData["taken_at"] = tm.UnixMilli()
        }
        if lat, lng, err := exifInfo.LatLong(); err == nil {
            exifData["latitude"] = lat
            exifData["longitude"] = lng
        }
        if model, err := exifInfo.Get(exif.Model); err == nil {
            exifData["device"] = model.StringVal()
        }
    }

    c.updateProcessProgress(ctx, event.MediaId, 80)

    // ---- 6e. 更新数据库 — PgSQL ----
    extraJSON, _ := json.Marshal(map[string]interface{}{
        "exif":     exifData,
        "progress": 100,
    })
    _, err = c.db.ExecContext(ctx,
        `UPDATE media_records
         SET width = $1, height = $2, thumbnail_key = $3,
             process_status = 2, extra = $4, updated_at = NOW()
         WHERE media_id = $5`,
        width,        // $1: width
        height,       // $2: height
        thumbnailKey, // $3: thumbnail_key
        extraJSON,    // $4: extra（包含 EXIF）
        event.MediaId, // $5: media_id
    )
    if err != nil {
        return fmt.Errorf("update media_records after image processing failed: %w", err)
    }

    // ---- 6f. 更新 Redis 缓存 ----
    c.updateMediaInfoCache(ctx, event.MediaId, bucket, event.StoragePath, thumbnailKey, width, height, 0)
    c.updateProcessProgress(ctx, event.MediaId, 100)

    // ---- 6g. 投递处理完成事件 ----
    thumbnailURL := ""
    if thumbnailKey != "" {
        thumbnailURL = c.ossClient.GetObjectURL(bucket, thumbnailKey)
    }
    c.produceProcessResult(ctx, event, "thumbnail", thumbnailURL, width, height, 0)

    // ---- 6h. 如果是头像，额外投递 avatar.processed 事件 ----
    if event.MediaType == kafka_media.MEDIA_TYPE_AVATAR {
        c.produceAvatarProcessed(ctx, event, bucket, thumbnailKey)
    }

    return nil
}

// ========== 视频处理流水线 ==========

// processVideo 处理视频：截图(1s) + 转码(H.264) + 生成缩略图
func (c *MediaConsumer) processVideo(ctx context.Context, event *kafka_media.MediaUploadCompletedEvent, bucket string) error {
    log.Info("processing video", "media_id", event.MediaId, "storage_path", event.StoragePath)

    // ---- 6a. 从对象存储下载原视频到临时文件 ----
    tmpFile, err := c.ossClient.DownloadToTemp(ctx, bucket, event.StoragePath)
    if err != nil {
        return fmt.Errorf("download video from OSS failed: %w", err)
    }
    defer os.Remove(tmpFile)

    // ---- 6b. 使用 ffprobe 提取视频元信息 ----
    probeData, err := ffprobe.ProbeURL(ctx, tmpFile)
    if err != nil {
        return fmt.Errorf("ffprobe video failed: %w", err)
    }

    var width, height int
    var duration int
    var videoCodec string
    videoStream := probeData.FirstVideoStream()
    if videoStream != nil {
        width = videoStream.Width
        height = videoStream.Height
        videoCodec = videoStream.CodecName
    }
    if probeData.Format != nil {
        dur, _ := strconv.ParseFloat(probeData.Format.Duration, 64)
        duration = int(dur)
    }

    c.updateProcessProgress(ctx, event.MediaId, 15)

    // ---- 6c. 生成视频截图（1s 处） ----
    snapshotKey := ""
    snapshotTmpFile := tmpFile + "_snapshot.jpg"
    defer os.Remove(snapshotTmpFile)

    // ffmpeg -i input.mp4 -ss 1 -vframes 1 -q:v 2 snapshot.jpg
    snapshotCmd := exec.CommandContext(ctx, "ffmpeg",
        "-i", tmpFile,
        "-ss", "1",        // 第 1 秒
        "-vframes", "1",   // 只截 1 帧
        "-q:v", "2",       // JPEG 质量
        "-y",              // 覆盖输出
        snapshotTmpFile,
    )
    if err := snapshotCmd.Run(); err != nil {
        log.Warn("generate video snapshot failed, try at 0s",
            "media_id", event.MediaId, "err", err)
        // 降级：在 0s 处截图
        snapshotCmd = exec.CommandContext(ctx, "ffmpeg",
            "-i", tmpFile, "-ss", "0", "-vframes", "1", "-q:v", "2", "-y", snapshotTmpFile,
        )
        if err := snapshotCmd.Run(); err != nil {
            log.Error("generate video snapshot at 0s also failed",
                "media_id", event.MediaId, "err", err)
            // 截图失败不影响整体流程
        }
    }

    // 上传截图到对象存储
    if _, statErr := os.Stat(snapshotTmpFile); statErr == nil {
        snapshotKey = strings.TrimSuffix(event.StoragePath, filepath.Ext(event.StoragePath)) + "_snapshot.jpg"
        if err := c.ossClient.Upload(ctx, bucket, snapshotKey, snapshotTmpFile, "image/jpeg"); err != nil {
            log.Error("upload video snapshot failed", "media_id", event.MediaId, "err", err)
            snapshotKey = ""
        }
    }

    c.updateProcessProgress(ctx, event.MediaId, 35)

    // ---- 6d. 生成缩略图（从截图缩放） ----
    thumbnailKey := ""
    if snapshotKey != "" {
        // 从截图生成缩略图
        if snapImg, err := imaging.Open(snapshotTmpFile); err == nil {
            thumbnail := imaging.Fit(snapImg, 200, 200, imaging.Lanczos)
            thumbTmpFile := tmpFile + "_thumb.webp"
            defer os.Remove(thumbTmpFile)
            if err := imaging.Save(thumbnail, thumbTmpFile); err == nil {
                thumbnailKey = strings.TrimSuffix(event.StoragePath, filepath.Ext(event.StoragePath)) + "_thumb.webp"
                if err := c.ossClient.Upload(ctx, bucket, thumbnailKey, thumbTmpFile, "image/webp"); err != nil {
                    log.Error("upload video thumbnail failed", "media_id", event.MediaId, "err", err)
                    thumbnailKey = ""
                }
            }
        }
    }

    c.updateProcessProgress(ctx, event.MediaId, 50)

    // ---- 6e. 判断是否需要转码 ----
    needTranscode := false
    transcodedKey := event.StoragePath // 默认不转码，使用原始路径
    if videoCodec != "" && videoCodec != "h264" && videoCodec != "hevc" {
        needTranscode = true
        log.Info("video needs transcoding",
            "media_id", event.MediaId, "codec", videoCodec)
    }

    if needTranscode {
        // ffmpeg -i input.mp4 -c:v libx264 -crf 23 -preset medium -c:a aac -b:a 128k output.mp4
        transcodedTmpFile := tmpFile + "_transcoded.mp4"
        defer os.Remove(transcodedTmpFile)

        transcodeCmd := exec.CommandContext(ctx, "ffmpeg",
            "-i", tmpFile,
            "-c:v", "libx264",
            "-crf", "23",
            "-preset", "medium",
            "-c:a", "aac",
            "-b:a", "128k",
            "-movflags", "+faststart", // web 播放优化
            "-y",
            transcodedTmpFile,
        )
        if err := transcodeCmd.Run(); err != nil {
            log.Error("video transcoding failed",
                "media_id", event.MediaId, "codec", videoCodec, "err", err)
            // 转码失败不影响整体流程，使用原始文件
        } else {
            // 上传转码后的视频
            transcodedKey = strings.TrimSuffix(event.StoragePath, filepath.Ext(event.StoragePath)) + "_h264.mp4"
            if err := c.ossClient.Upload(ctx, bucket, transcodedKey, transcodedTmpFile, "video/mp4"); err != nil {
                log.Error("upload transcoded video failed", "media_id", event.MediaId, "err", err)
                transcodedKey = event.StoragePath // 上传失败使用原始路径
            } else {
                // 更新 ffprobe 数据（转码后可能尺寸变化）
                if newProbe, err := ffprobe.ProbeURL(ctx, transcodedTmpFile); err == nil {
                    if vs := newProbe.FirstVideoStream(); vs != nil {
                        width = vs.Width
                        height = vs.Height
                    }
                }
            }
        }
    }

    c.updateProcessProgress(ctx, event.MediaId, 85)

    // ---- 6f. 更新数据库 — PgSQL ----
    extraJSON, _ := json.Marshal(map[string]interface{}{
        "original_codec": videoCodec,
        "transcoded":     needTranscode,
        "progress":       100,
    })
    _, err = c.db.ExecContext(ctx,
        `UPDATE media_records
         SET width = $1, height = $2, duration = $3,
             thumbnail_key = $4, snapshot_key = $5,
             storage_key = CASE WHEN $6 != '' THEN $6 ELSE storage_key END,
             process_status = 2, extra = $7, updated_at = NOW()
         WHERE media_id = $8`,
        width,          // $1: width
        height,         // $2: height
        duration,       // $3: duration（秒）
        thumbnailKey,   // $4: thumbnail_key
        snapshotKey,    // $5: snapshot_key
        transcodedKey,  // $6: storage_key（转码后更新）
        extraJSON,      // $7: extra
        event.MediaId,  // $8: media_id
    )
    if err != nil {
        return fmt.Errorf("update media_records after video processing failed: %w", err)
    }

    // ---- 6g. 更新 Redis 缓存 ----
    c.updateMediaInfoCache(ctx, event.MediaId, bucket, transcodedKey, thumbnailKey, width, height, duration)
    c.updateProcessProgress(ctx, event.MediaId, 100)

    // ---- 6h. 投递处理完成事件 ----
    resultURL := c.ossClient.GetObjectURL(bucket, transcodedKey)
    c.produceProcessResult(ctx, event, "transcode", resultURL, width, height, duration)

    return nil
}

// ========== 语音/音频处理流水线 ==========

// processVoice 处理语音：提取时长
func (c *MediaConsumer) processVoice(ctx context.Context, event *kafka_media.MediaUploadCompletedEvent, bucket string) error {
    log.Info("processing voice/audio", "media_id", event.MediaId, "storage_path", event.StoragePath)

    // ---- 6a. 从对象存储下载到临时文件 ----
    tmpFile, err := c.ossClient.DownloadToTemp(ctx, bucket, event.StoragePath)
    if err != nil {
        return fmt.Errorf("download audio from OSS failed: %w", err)
    }
    defer os.Remove(tmpFile)

    // ---- 6b. 使用 ffprobe 提取音频时长 ----
    probeData, err := ffprobe.ProbeURL(ctx, tmpFile)
    if err != nil {
        return fmt.Errorf("ffprobe audio failed: %w", err)
    }

    var duration int
    if probeData.Format != nil {
        dur, _ := strconv.ParseFloat(probeData.Format.Duration, 64)
        duration = int(math.Ceil(dur)) // 向上取整
    }

    c.updateProcessProgress(ctx, event.MediaId, 50)

    // ---- 6c. 生成波形数据（仅音频消息，语音消息可选） ----
    var waveformData []float64
    if event.MediaType == kafka_media.MEDIA_TYPE_VOICE || isAudioType(event.ContentType) {
        // 使用 ffmpeg 提取音频波形采样
        // ffmpeg -i input.mp3 -ac 1 -filter:a "aresample=8000" -f f32le -
        waveformCmd := exec.CommandContext(ctx, "ffmpeg",
            "-i", tmpFile,
            "-ac", "1",
            "-filter:a", "aresample=8000,astats=metadata=1:reset=1",
            "-f", "null",
            "-",
        )
        // 简化处理：生成固定数量的采样点（50 个）
        waveformData = generateSimpleWaveform(tmpFile, 50)
    }

    c.updateProcessProgress(ctx, event.MediaId, 80)

    // ---- 6d. 更新数据库 — PgSQL ----
    extraJSON, _ := json.Marshal(map[string]interface{}{
        "waveform": waveformData,
        "progress": 100,
    })
    _, err = c.db.ExecContext(ctx,
        `UPDATE media_records
         SET duration = $1, process_status = 2, extra = $2, updated_at = NOW()
         WHERE media_id = $3`,
        duration,      // $1: duration（秒）
        extraJSON,     // $2: extra（包含波形数据）
        event.MediaId, // $3: media_id
    )
    if err != nil {
        return fmt.Errorf("update media_records after audio processing failed: %w", err)
    }

    // ---- 6e. 更新 Redis 缓存 ----
    c.updateMediaInfoCache(ctx, event.MediaId, bucket, event.StoragePath, "", 0, 0, duration)
    c.updateProcessProgress(ctx, event.MediaId, 100)

    // ---- 6f. 投递处理完成事件 ----
    resultURL := c.ossClient.GetObjectURL(bucket, event.StoragePath)
    c.produceProcessResult(ctx, event, "audio_extract", resultURL, 0, 0, duration)

    return nil
}

// ========== 文件处理流水线 ==========

// processFile 处理普通文件：仅提取元信息
func (c *MediaConsumer) processFile(ctx context.Context, event *kafka_media.MediaUploadCompletedEvent, bucket string) error {
    log.Info("processing file (metadata only)", "media_id", event.MediaId, "storage_path", event.StoragePath)

    // 普通文件不需要下载和处理，直接标记完成

    // ---- 6a. 更新数据库 — PgSQL ----
    extraJSON, _ := json.Marshal(map[string]interface{}{
        "progress": 100,
    })
    _, err := c.db.ExecContext(ctx,
        `UPDATE media_records
         SET process_status = 2, extra = $1, updated_at = NOW()
         WHERE media_id = $2`,
        extraJSON,     // $1: extra
        event.MediaId, // $2: media_id
    )
    if err != nil {
        return fmt.Errorf("update media_records after file processing failed: %w", err)
    }

    // ---- 6b. 更新 Redis 缓存 ----
    c.updateMediaInfoCache(ctx, event.MediaId, bucket, event.StoragePath, "", 0, 0, 0)
    c.updateProcessProgress(ctx, event.MediaId, 100)

    // ---- 6c. 投递处理完成事件 ----
    resultURL := c.ossClient.GetObjectURL(bucket, event.StoragePath)
    c.produceProcessResult(ctx, event, "metadata", resultURL, 0, 0, 0)

    return nil
}
```

### Consumer: `user.deactivated` — 用户注销清理

> 来源：User 服务。用户注销后标记其所有媒体文件为待延迟删除状态。  
> 不立即删除对象存储文件，而是标记 process_status = -2（待清理），由定时任务延迟 30 天后真正删除。

```go
func (c *MediaConsumer) HandleUserDeactivated(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_user.UserDeactivatedEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("unmarshal UserDeactivatedEvent failed", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 — Redis ====================
    dedupKey := fmt.Sprintf("media:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        log.Debug("duplicate user.deactivated event", "event_id", event.Header.EventId)
        return nil
    }

    if event.UserId == "" {
        log.Error("user.deactivated: user_id is empty")
        return nil
    }

    log.Info("user deactivated, marking media for delayed deletion",
        "user_id", event.UserId)

    // ==================== 3. 批量标记用户所有媒体为待清理 — PgSQL ====================
    // 使用 process_status = -2 标记待延迟删除
    // 同时记录注销时间，定时任务 30 天后真正清理
    result, err := c.db.ExecContext(ctx,
        `UPDATE media_records
         SET process_status = -2,
             extra = jsonb_set(
                 COALESCE(extra, '{}'),
                 '{deactivated_at}',
                 to_jsonb($1::bigint)
             ),
             updated_at = NOW()
         WHERE uploader_id = $2 AND process_status NOT IN (-1, -2)`,
        time.Now().UnixMilli(), // $1: deactivated_at
        event.UserId,          // $2: uploader_id
    )
    if err != nil {
        log.Error("mark media for deactivated user failed",
            "user_id", event.UserId, "err", err)
        return fmt.Errorf("mark media for deactivated user failed: %w", err)
    }
    rowsAffected, _ := result.RowsAffected()

    // ==================== 4. 批量清理 Redis 缓存 ====================
    // 查询该用户的所有 media_id，清理缓存
    rows, err := c.db.QueryContext(ctx,
        `SELECT media_id FROM media_records
         WHERE uploader_id = $1 AND process_status = -2
         LIMIT 10000`,
        event.UserId,
    )
    if err != nil {
        log.Error("query media_ids for cache cleanup failed",
            "user_id", event.UserId, "err", err)
    } else {
        defer rows.Close()
        pipe := c.redis.Pipeline()
        cleanCount := 0
        for rows.Next() {
            var mediaID string
            if err := rows.Scan(&mediaID); err != nil {
                continue
            }
            pipe.Del(ctx, fmt.Sprintf("media:info:%s", mediaID))
            pipe.Del(ctx, fmt.Sprintf("media:process_status:%s", mediaID))
            cleanCount++
            // 每 500 个执行一次，避免 pipeline 太大
            if cleanCount%500 == 0 {
                pipe.Exec(ctx)
                pipe = c.redis.Pipeline()
            }
        }
        if cleanCount%500 != 0 {
            pipe.Exec(ctx)
        }
    }

    // ==================== 5. 清理进行中的分片上传 ====================
    // 取消该用户所有进行中的分片上传
    var uploadIDs []string
    uploadRows, err := c.db.QueryContext(ctx,
        `SELECT upload_id, bucket, storage_key FROM multipart_uploads
         WHERE user_id = $1 AND status = 0`,
        event.UserId,
    )
    if err == nil {
        defer uploadRows.Close()
        for uploadRows.Next() {
            var uid, bkt, key string
            if uploadRows.Scan(&uid, &bkt, &key) == nil {
                uploadIDs = append(uploadIDs, uid)
                // 在对象存储取消分片上传
                go func(b, k, u string) {
                    if err := c.ossClient.AbortMultipartUpload(context.Background(), b, k, u); err != nil {
                        log.Warn("abort multipart upload for deactivated user failed",
                            "upload_id", u, "err", err)
                    }
                }(bkt, key, uid)
            }
        }
    }

    // 更新分片上传记录为已取消
    if len(uploadIDs) > 0 {
        _, err = c.db.ExecContext(ctx,
            `UPDATE multipart_uploads SET status = 2
             WHERE user_id = $1 AND status = 0`,
            event.UserId,
        )
        if err != nil {
            log.Error("cancel multipart uploads for deactivated user failed",
                "user_id", event.UserId, "err", err)
        }
        // 清理 Redis 分片进度
        pipe := c.redis.Pipeline()
        for _, uid := range uploadIDs {
            pipe.Del(ctx, fmt.Sprintf("media:multipart:%s", uid))
        }
        pipe.Exec(ctx)
    }

    log.Info("user media marked for delayed deletion",
        "user_id", event.UserId, "media_count", rowsAffected,
        "multipart_cancelled", len(uploadIDs))

    return nil
}
```

### Consumer: `audit.content.violation` — 违规内容处理

> 来源：Audit 审核服务。内容审核检测到违规媒体，执行删除或封禁处理。  
> 处置策略：delete=立即软删除，block=封禁（保留记录但不可访问），warn=仅记录告警。

```go
func (c *MediaConsumer) HandleContentViolation(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_audit.ContentViolationEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("unmarshal ContentViolationEvent failed", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 — Redis ====================
    dedupKey := fmt.Sprintf("media:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        log.Debug("duplicate audit.content.violation event", "event_id", event.Header.EventId)
        return nil
    }

    // ==================== 3. 参数校验 ====================
    if event.MediaId == "" {
        log.Error("audit.content.violation: media_id is empty")
        return nil
    }
    if event.Action == "" {
        log.Error("audit.content.violation: action is empty", "media_id", event.MediaId)
        return nil
    }

    log.Info("content violation detected",
        "media_id", event.MediaId, "action", event.Action,
        "reason", event.Reason, "labels", event.Labels)

    // ==================== 4. 查询媒体记录 — PgSQL ====================
    var record struct {
        UploaderID    string
        MediaType     int
        StorageKey    string
        Bucket        string
        ThumbnailKey  string
        SnapshotKey   string
        ProcessStatus int
    }
    err := c.db.QueryRowContext(ctx,
        `SELECT uploader_id, media_type, storage_key, bucket,
                thumbnail_key, snapshot_key, process_status
         FROM media_records WHERE media_id = $1`,
        event.MediaId,
    ).Scan(&record.UploaderID, &record.MediaType, &record.StorageKey, &record.Bucket,
        &record.ThumbnailKey, &record.SnapshotKey, &record.ProcessStatus)
    if err == sql.ErrNoRows {
        log.Warn("audit.content.violation: media not found", "media_id", event.MediaId)
        return nil
    }
    if err != nil {
        return fmt.Errorf("query media_records for violation failed: %w", err)
    }

    // 已删除的不重复处理
    if record.ProcessStatus == -1 {
        log.Info("media already deleted, skip violation handling", "media_id", event.MediaId)
        return nil
    }

    // ==================== 5. 延迟双删（delete/block 操作需要失效缓存） ====================
    if event.Action == "delete" || event.Action == "block" {
        c.delayedDoubleDelete(ctx,
            fmt.Sprintf("media:info:%s", event.MediaId),
            fmt.Sprintf("media:process_status:%s", event.MediaId),
        )
    }

    // ==================== 6. 根据 action 执行处置 ====================
    switch event.Action {
    case "delete":
        // 软删除媒体记录
        _, err = c.db.ExecContext(ctx,
            `UPDATE media_records
             SET process_status = -1,
                 extra = jsonb_set(
                     COALESCE(extra, '{}'),
                     '{violation}',
                     $1::jsonb
                 ),
                 updated_at = NOW()
             WHERE media_id = $2 AND process_status != -1`,
            fmt.Sprintf(`{"reason":"%s","labels":%s,"action":"delete","audit_time":%d}`,
                event.Reason, mustMarshal(event.Labels), time.Now().UnixMilli()),
            event.MediaId,
        )
        if err != nil {
            return fmt.Errorf("soft delete violating media failed: %w", err)
        }

        // 投递 media.deleted 事件
        deleteEvent := &kafka_media.MediaDeletedEvent{
            Header:     buildEventHeader("media", c.instanceID),
            MediaId:    event.MediaId,
            MediaType:  kafka_media.MediaType(record.MediaType),
            OperatorId: "audit_system",
            Reason:     "violation: " + event.Reason,
            DeleteTime: time.Now().UnixMilli(),
        }
        c.kafka.Produce(ctx, "media.deleted", event.MediaId, deleteEvent)

    case "block":
        // 封禁：保留记录但标记不可访问（process_status = -3 表示封禁）
        _, err = c.db.ExecContext(ctx,
            `UPDATE media_records
             SET process_status = -3,
                 extra = jsonb_set(
                     COALESCE(extra, '{}'),
                     '{violation}',
                     $1::jsonb
                 ),
                 updated_at = NOW()
             WHERE media_id = $2`,
            fmt.Sprintf(`{"reason":"%s","labels":%s,"action":"block","audit_time":%d}`,
                event.Reason, mustMarshal(event.Labels), time.Now().UnixMilli()),
            event.MediaId,
        )
        if err != nil {
            return fmt.Errorf("block violating media failed: %w", err)
        }

    case "warn":
        // 仅记录告警，不做处理
        _, err = c.db.ExecContext(ctx,
            `UPDATE media_records
             SET extra = jsonb_set(
                     COALESCE(extra, '{}'),
                     '{violation_warn}',
                     $1::jsonb
                 ),
                 updated_at = NOW()
             WHERE media_id = $2`,
            fmt.Sprintf(`{"reason":"%s","labels":%s,"warn_time":%d}`,
                event.Reason, mustMarshal(event.Labels), time.Now().UnixMilli()),
            event.MediaId,
        )
        if err != nil {
            log.Error("record violation warn failed", "media_id", event.MediaId, "err", err)
        }
        log.Warn("media violation warn recorded",
            "media_id", event.MediaId, "reason", event.Reason)
        return nil // warn 不清理缓存

    default:
        log.Error("unknown violation action", "media_id", event.MediaId, "action", event.Action)
        return nil
    }

    log.Info("content violation handled",
        "media_id", event.MediaId, "action", event.Action,
        "uploader_id", record.UploaderID)

    return nil
}
```

### Consumer: `config.changed` — 配置热更新

> 来源：Config 服务。配置变更时热更新媒体服务相关配置参数（上传限制、允许类型、最大文件大小、处理参数等）。

```go
func (c *MediaConsumer) HandleConfigChanged(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_config.ConfigChangedEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("unmarshal ConfigChangedEvent failed", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 — Redis ====================
    dedupKey := fmt.Sprintf("media:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    // ==================== 3. 过滤 Media 相关配置 ====================
    if event.Module != "media" && event.Module != "global" {
        return nil // 不是 Media 相关配置，跳过
    }

    log.Info("config changed for media module",
        "module", event.Module, "keys", event.ChangedKeys)

    // ==================== 4. 应用配置变更 ====================
    for _, item := range event.Items {
        switch item.Key {
        case "media.upload.max_image_size":
            // 图片最大上传大小（字节）
            if size, err := strconv.ParseInt(item.Value, 10, 64); err == nil && size > 0 {
                c.config.MaxImageSize = size
                log.Info("updated max_image_size", "value", size)
            }

        case "media.upload.max_video_size":
            // 视频最大上传大小（字节）
            if size, err := strconv.ParseInt(item.Value, 10, 64); err == nil && size > 0 {
                c.config.MaxVideoSize = size
                log.Info("updated max_video_size", "value", size)
            }

        case "media.upload.max_file_size":
            // 文件最大上传大小（字节）
            if size, err := strconv.ParseInt(item.Value, 10, 64); err == nil && size > 0 {
                c.config.MaxFileSize = size
                log.Info("updated max_file_size", "value", size)
            }

        case "media.upload.allowed_image_types":
            // 允许的图片 MIME 类型列表
            var types []string
            if json.Unmarshal([]byte(item.Value), &types) == nil && len(types) > 0 {
                c.config.AllowedImageTypes = types
                log.Info("updated allowed_image_types", "value", types)
            }

        case "media.upload.allowed_video_types":
            // 允许的视频 MIME 类型列表
            var types []string
            if json.Unmarshal([]byte(item.Value), &types) == nil && len(types) > 0 {
                c.config.AllowedVideoTypes = types
                log.Info("updated allowed_video_types", "value", types)
            }

        case "media.process.thumbnail_size":
            // 缩略图尺寸
            if size, err := strconv.Atoi(item.Value); err == nil && size > 0 {
                c.config.ThumbnailSize = size
                log.Info("updated thumbnail_size", "value", size)
            }

        case "media.process.video_snapshot_time":
            // 视频截图时间点（秒）
            if t, err := strconv.Atoi(item.Value); err == nil && t >= 0 {
                c.config.VideoSnapshotTime = t
                log.Info("updated video_snapshot_time", "value", t)
            }

        case "media.process.video_transcode_enabled":
            // 是否启用视频转码
            c.config.VideoTranscodeEnabled = item.Value == "true"
            log.Info("updated video_transcode_enabled", "value", c.config.VideoTranscodeEnabled)

        case "media.process.video_crf":
            // 视频转码 CRF 值
            if crf, err := strconv.Atoi(item.Value); err == nil && crf >= 0 && crf <= 51 {
                c.config.VideoCRF = crf
                log.Info("updated video_crf", "value", crf)
            }

        case "media.storage.bucket_prefix":
            // Bucket 前缀
            if item.Value != "" {
                c.config.BucketPrefix = item.Value
                log.Info("updated bucket_prefix", "value", item.Value)
            }

        default:
            log.Debug("unhandled media config key", "key", item.Key)
        }
    }

    return nil
}
```

---

## 公共辅助函数

```go
// delayedDoubleDelete 延迟双删工具函数
// Phase 1: 立即删除缓存（在 DB 写入前调用，防止旧缓存被读取）
// Phase 2: 延迟 500ms 后再次删除缓存（防止并发读在 DB 写入窗口期回填旧数据）
func (c *MediaConsumer) delayedDoubleDelete(ctx context.Context, keys ...string) {
    if len(keys) == 0 {
        return
    }
    // Phase 1: 立即删除
    c.redis.Del(ctx, keys...)
    // Phase 2: 延迟 500ms 后再次删除
    go func(delKeys []string) {
        time.Sleep(500 * time.Millisecond)
        if _, err := c.redis.Del(context.Background(), delKeys...).Result(); err != nil {
            log.Warn("delayedDoubleDelete phase 2 failed", "keys", delKeys, "err", err)
        }
    }(keys)
}

// handleProcessFailed 处理流水线失败的公共逻辑
func (c *MediaConsumer) handleProcessFailed(ctx context.Context, event *kafka_media.MediaUploadCompletedEvent, processErr error) {
    // 1. 更新数据库状态为 FAILED
    extraJSON, _ := json.Marshal(map[string]interface{}{
        "fail_reason": processErr.Error(),
        "failed_at":   time.Now().UnixMilli(),
    })
    _, err := c.db.ExecContext(ctx,
        `UPDATE media_records
         SET process_status = 3,
             extra = jsonb_set(COALESCE(extra, '{}'), '{fail_info}', $1::jsonb),
             updated_at = NOW()
         WHERE media_id = $2`,
        extraJSON,     // $1: fail_info
        event.MediaId, // $2: media_id
    )
    if err != nil {
        log.Error("update media_records to FAILED status failed",
            "media_id", event.MediaId, "err", err)
    }

    // 2. 更新 Redis 处理状态
    statusJSON, _ := json.Marshal(map[string]interface{}{
        "status":      3, // FAILED
        "progress":    0,
        "fail_reason": processErr.Error(),
    })
    c.redis.Set(ctx, fmt.Sprintf("media:process_status:%s", event.MediaId),
        string(statusJSON), 1*time.Hour)

    // 3. 清理 media:info 缓存
    c.redis.Del(ctx, fmt.Sprintf("media:info:%s", event.MediaId))

    // 4. 投递 media.process.result 事件（失败状态）
    failedEvent := &kafka_media.MediaProcessResultEvent{
        Header:       buildEventHeader("media", c.instanceID),
        MediaId:      event.MediaId,
        MediaType:    event.MediaType,
        ProcessType:  getProcessTypeByMediaType(event.MediaType),
        ProcessStatus: kafka_media.PROCESS_STATUS_FAILED,
        ErrorCode:    "PROCESS_ERROR",
        ErrorMsg:     processErr.Error(),
        RetryCount:   0,
        NeedRetry:    false, // 暂不自动重试，可人工触发
        FailTime:     time.Now().UnixMilli(),
        MsgId:        event.MsgId,
    }
    if err := c.kafka.Produce(ctx, "media.process.result", event.MediaId, failedEvent); err != nil {
        log.Error("produce media.process.result (failed) event failed",
            "media_id", event.MediaId, "err", err)
    }
}

// updateProcessProgress 更新处理进度到 Redis
func (c *MediaConsumer) updateProcessProgress(ctx context.Context, mediaID string, progress int) {
    status := 1 // PROCESSING
    if progress >= 100 {
        status = 2 // COMPLETED
    }
    statusJSON, _ := json.Marshal(map[string]interface{}{
        "status":   status,
        "progress": progress,
    })
    c.redis.Set(ctx, fmt.Sprintf("media:process_status:%s", mediaID),
        string(statusJSON), 1*time.Hour)
}

// updateMediaInfoCache 更新媒体信息 Redis 缓存
func (c *MediaConsumer) updateMediaInfoCache(ctx context.Context, mediaID, bucket, storageKey, thumbnailKey string, width, height, duration int) {
    infoKey := fmt.Sprintf("media:info:%s", mediaID)

    url := c.ossClient.GetObjectURL(bucket, storageKey)
    thumbnailURL := ""
    if thumbnailKey != "" {
        thumbnailURL = c.ossClient.GetObjectURL(bucket, thumbnailKey)
    }

    // 先读取现有缓存（可能 RPC GetMediaInfo 已写入部分字段）
    existing, _ := c.redis.HGetAll(ctx, infoKey).Result()

    fields := map[string]interface{}{
        "url":            url,
        "thumbnail_url":  thumbnailURL,
        "width":          width,
        "height":         height,
        "duration":       duration,
        "process_status": 2, // COMPLETED
    }
    // 保留原有的 media_id / uploader_id / original_name / file_size / mime_type
    if existing["media_id"] != "" {
        for k, v := range existing {
            if _, overwritten := fields[k]; !overwritten {
                fields[k] = v
            }
        }
    } else {
        fields["media_id"] = mediaID
    }

    c.redis.HSet(ctx, infoKey, fields)
    c.redis.Expire(ctx, infoKey, 1*time.Hour)
}

// produceProcessResult 投递处理结果事件（完成状态）
func (c *MediaConsumer) produceProcessResult(ctx context.Context, event *kafka_media.MediaUploadCompletedEvent, processType, resultURL string, width, height, duration int) {
    resultEvent := &kafka_media.MediaProcessResultEvent{
        Header:        buildEventHeader("media", c.instanceID),
        MediaId:       event.MediaId,
        ProcessId:     uuid.New().String(),
        MediaType:     event.MediaType,
        ProcessType:   processType,
        ProcessStatus: kafka_media.PROCESS_STATUS_COMPLETED,
        ResultUrl:     resultURL,
        ResultWidth:   int32(width),
        ResultHeight:  int32(height),
        ResultDuration: int32(duration),
        CompleteTime:  time.Now().UnixMilli(),
        MsgId:         event.MsgId,
    }
    if err := c.kafka.Produce(ctx, "media.process.result", event.MediaId, resultEvent); err != nil {
        log.Error("produce media.process.result event failed",
            "media_id", event.MediaId, "process_type", processType, "err", err)
    }
}

// produceAvatarProcessed 投递头像处理完成事件
func (c *MediaConsumer) produceAvatarProcessed(ctx context.Context, event *kafka_media.MediaUploadCompletedEvent, bucket, thumbnailKey string) {
    // 头像需要多尺寸裁剪：小图 100x100、中图 200x200、大图 400x400
    // 此处简化处理，假设缩略图即为中图，小图和大图由 OSS 实时处理参数生成
    originalURL := c.ossClient.GetObjectURL(bucket, event.StoragePath)
    smallURL := originalURL + "?x-oss-process=image/resize,m_fill,w_100,h_100"
    mediumURL := originalURL + "?x-oss-process=image/resize,m_fill,w_200,h_200"
    largeURL := originalURL + "?x-oss-process=image/resize,m_fill,w_400,h_400"

    avatarEvent := &kafka_media.MediaAvatarProcessedEvent{
        Header:      buildEventHeader("media", c.instanceID),
        MediaId:     event.MediaId,
        OwnerId:     event.UploaderId, // 上传者即头像所有者
        OwnerType:   "user",           // 默认用户头像，群头像由业务标记
        OriginalUrl: originalURL,
        SmallUrl:    smallURL,
        MediumUrl:   mediumURL,
        LargeUrl:    largeURL,
        ProcessTime: time.Now().UnixMilli(),
    }
    if err := c.kafka.Produce(ctx, "media.avatar.processed", event.UploaderId, avatarEvent); err != nil {
        log.Error("produce media.avatar.processed event failed",
            "media_id", event.MediaId, "owner_id", event.UploaderId, "err", err)
    }
}

// buildEventHeader 构建 Kafka 事件头
func buildEventHeader(source, instanceID string) *common.EventHeader {
    return &common.EventHeader{
        EventId:   uuid.New().String(),
        TraceId:   trace.SpanFromContext(ctx).SpanContext().TraceID().String(),
        Source:    source,
        SourceId:  instanceID,
        Timestamp: time.Now().UnixMilli(),
        Version:   1,
    }
}

// getProcessTypeByMediaType 根据媒体类型返回处理类型标识
func getProcessTypeByMediaType(mediaType kafka_media.MediaType) string {
    switch mediaType {
    case kafka_media.MEDIA_TYPE_IMAGE, kafka_media.MEDIA_TYPE_AVATAR:
        return "thumbnail"
    case kafka_media.MEDIA_TYPE_VIDEO:
        return "transcode"
    case kafka_media.MEDIA_TYPE_VOICE:
        return "audio_extract"
    case kafka_media.MEDIA_TYPE_FILE:
        return "metadata"
    default:
        return "unknown"
    }
}

// generateSimpleWaveform 简化波形生成（50 个采样点）
func generateSimpleWaveform(filePath string, samples int) []float64 {
    // 使用 ffmpeg 提取原始音频数据，采样后生成归一化波形
    cmd := exec.Command("ffmpeg",
        "-i", filePath,
        "-ac", "1",           // 单声道
        "-ar", "8000",        // 8kHz 采样率
        "-f", "s16le",        // 16位小端
        "-acodec", "pcm_s16le",
        "-",
    )
    out, err := cmd.Output()
    if err != nil || len(out) == 0 {
        return make([]float64, samples) // 返回全零
    }

    // 将原始数据分为 samples 份，每份取最大绝对值
    totalSamples := len(out) / 2 // 16bit = 2 bytes
    chunkSize := totalSamples / samples
    if chunkSize < 1 {
        chunkSize = 1
    }

    waveform := make([]float64, samples)
    var maxVal float64 = 1
    for i := 0; i < samples && i*chunkSize*2 < len(out); i++ {
        var maxAbs int16
        for j := 0; j < chunkSize && (i*chunkSize+j)*2+1 < len(out); j++ {
            idx := (i*chunkSize + j) * 2
            sample := int16(out[idx]) | int16(out[idx+1])<<8
            if sample < 0 {
                sample = -sample
            }
            if sample > maxAbs {
                maxAbs = sample
            }
        }
        waveform[i] = float64(maxAbs)
        if waveform[i] > maxVal {
            maxVal = waveform[i]
        }
    }

    // 归一化到 0~1
    for i := range waveform {
        waveform[i] = waveform[i] / maxVal
    }
    return waveform
}

// isAudioType 判断是否为音频 MIME 类型
func isAudioType(contentType string) bool {
    return strings.HasPrefix(contentType, "audio/")
}

// mustMarshal JSON 序列化，失败返回空数组
func mustMarshal(v interface{}) string {
    b, err := json.Marshal(v)
    if err != nil {
        return "[]"
    }
    return string(b)
}
```
