# Media 媒体/文件服务 — RPC 接口实现伪代码

## 概述

Media 服务负责多媒体文件的**上传凭证生成、上传回调、媒体处理（缩略图/视频截图/转码）、下载 URL 生成**。
底层使用对象存储（MinIO/S3/OSS）作为文件存储后端，本服务仅管理元信息和签名凭证。

**核心设计原则：**
- **客户端直传**：上传不经过服务端，客户端直传对象存储（MinIO/S3），本服务只生成预签名 URL 或 STS Token
- **回调触发处理**：对象存储上传完成后通过 UploadCallback 通知服务端，触发异步处理流水线
- **异步处理**：缩略图生成、视频转码、截图等重操作走 Kafka 消费者异步执行，不阻塞上传流程
- **分片上传**：大文件（≥5MB）支持分片上传，断点续传
- **预签名下载**：下载 URL 带签名和过期时间，防止未授权访问
- **幂等性**：重复回调通过 media_id + process_status 判断，不重复处理

## 外部依赖

| 依赖类型 | 依赖项 | 用途 |
|---------|--------|------|
| PgSQL | media_records 表 | 媒体元信息持久化 |
| PgSQL | multipart_uploads 表 | 分片上传记录 |
| PgSQL | multipart_parts 表 | 分片详情记录 |
| Redis | 媒体信息缓存 / 上传凭证缓存 / 分片进度 / 下载 URL 缓存 / 处理状态 | 高频读写 |
| 对象存储 | MinIO/S3/OSS | 文件实际存储后端 |
| Kafka | media.uploaded / media.process.completed / media.process.failed / media.avatar.processed / media.deleted | 事件通知 |

> **不依赖** 任何其他 RPC 服务。Media 是基础存储服务，被 Message、User、Group 等服务依赖。

## PgSQL 表结构

```sql
-- 媒体记录表（核心表）
CREATE TABLE media_records (
    id             BIGSERIAL    PRIMARY KEY,
    media_id       VARCHAR(20)  NOT NULL,                     -- 全局唯一媒体 ID（Snowflake）
    uploader_id    VARCHAR(20)  NOT NULL,                     -- 上传者用户 ID
    media_type     INT          NOT NULL DEFAULT 0,           -- 媒体类型：1=图片 2=视频 3=音频 4=文件 5=语音
    original_name  VARCHAR(500) NOT NULL DEFAULT '',          -- 原始文件名
    file_size      BIGINT       NOT NULL DEFAULT 0,           -- 文件大小（字节）
    mime_type      VARCHAR(100) NOT NULL DEFAULT '',          -- MIME 类型
    storage_key    VARCHAR(500) NOT NULL DEFAULT '',          -- 对象存储 Key（路径）
    bucket         VARCHAR(100) NOT NULL DEFAULT '',          -- 对象存储 Bucket
    width          INT          NOT NULL DEFAULT 0,           -- 宽度（像素，图片/视频）
    height         INT          NOT NULL DEFAULT 0,           -- 高度（像素，图片/视频）
    duration       INT          NOT NULL DEFAULT 0,           -- 时长（秒，音频/视频）
    thumbnail_key  VARCHAR(500) NOT NULL DEFAULT '',          -- 缩略图对象存储 Key
    snapshot_key   VARCHAR(500) NOT NULL DEFAULT '',          -- 视频截图对象存储 Key
    process_status INT          NOT NULL DEFAULT 0,           -- 处理状态：0=PENDING 1=PROCESSING 2=COMPLETED 3=FAILED
    extra          JSONB        DEFAULT '{}',                 -- 扩展信息（EXIF/波形/转码参数等）
    created_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
CREATE UNIQUE INDEX idx_media_records_media_id ON media_records(media_id);
CREATE INDEX idx_media_records_uploader ON media_records(uploader_id, created_at DESC);
CREATE INDEX idx_media_records_status ON media_records(process_status) WHERE process_status IN (0, 1);

-- 分片上传记录表
CREATE TABLE multipart_uploads (
    id             BIGSERIAL    PRIMARY KEY,
    upload_id      VARCHAR(100) NOT NULL,                     -- 分片上传 ID（对象存储返回）
    media_id       VARCHAR(20)  NOT NULL,                     -- 关联媒体 ID
    user_id        VARCHAR(20)  NOT NULL,                     -- 上传者 ID
    bucket         VARCHAR(100) NOT NULL DEFAULT '',          -- Bucket
    storage_key    VARCHAR(500) NOT NULL DEFAULT '',          -- 目标存储 Key
    total_parts    INT          NOT NULL DEFAULT 0,           -- 总分片数
    uploaded_parts INT          NOT NULL DEFAULT 0,           -- 已上传分片数
    status         INT          NOT NULL DEFAULT 0,           -- 0=进行中 1=已完成 2=已取消 3=已过期
    created_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    expires_at     TIMESTAMPTZ  NOT NULL                      -- 过期时间（默认 24h）
);
CREATE UNIQUE INDEX idx_multipart_uploads_upload_id ON multipart_uploads(upload_id);
CREATE INDEX idx_multipart_uploads_user ON multipart_uploads(user_id, status);
CREATE INDEX idx_multipart_uploads_expires ON multipart_uploads(expires_at) WHERE status = 0;

-- 分片详情表
CREATE TABLE multipart_parts (
    id          BIGSERIAL    PRIMARY KEY,
    upload_id   VARCHAR(100) NOT NULL,                        -- 分片上传 ID
    part_number INT          NOT NULL,                        -- 分片序号（从 1 开始）
    etag        VARCHAR(100) NOT NULL DEFAULT '',             -- 分片 ETag（对象存储返回）
    size        BIGINT       NOT NULL DEFAULT 0,              -- 分片大小（字节）
    uploaded_at TIMESTAMPTZ  NOT NULL DEFAULT NOW()           -- 上传时间
);
CREATE UNIQUE INDEX idx_multipart_parts_upload_part ON multipart_parts(upload_id, part_number);
```

## 媒体类型枚举

| 值 | 名称 | 说明 |
|----|------|------|
| 1 | IMAGE | 图片（jpg/png/gif/webp） |
| 2 | VIDEO | 视频（mp4/mov/avi） |
| 3 | AUDIO | 音频（mp3/aac/ogg） |
| 4 | FILE | 普通文件（pdf/doc/zip 等） |
| 5 | VOICE | 语音消息（amr/opus/silk） |

## 处理状态枚举

| 值 | 名称 | 说明 |
|----|------|------|
| 0 | PENDING | 待处理（刚上传完成） |
| 1 | PROCESSING | 处理中（缩略图/转码/截图） |
| 2 | COMPLETED | 处理完成 |
| 3 | FAILED | 处理失败 |

## 分片上传状态枚举

| 值 | 名称 | 说明 |
|----|------|------|
| 0 | IN_PROGRESS | 进行中 |
| 1 | COMPLETED | 已完成（合并成功） |
| 2 | ABORTED | 已取消 |
| 3 | EXPIRED | 已过期（超 24h 未完成） |

## Redis Key 设计

| Key 格式 | 类型 | 值 | TTL | 说明 |
|----------|------|-----|-----|------|
| `media:info:{media_id}` | HASH | 媒体元信息各字段（media_id/url/thumbnail_url/width/height/duration/file_size/mime_type/process_status） | 1h | 媒体信息缓存，GetMediaInfo/BatchGetMediaInfo 回填 |
| `media:upload_cred:{media_id}` | STRING | JSON 序列化上传凭证（upload_url/token/headers/expire_at） | 15min | 上传凭证缓存，防止重复生成 |
| `media:multipart:{upload_id}` | HASH | 分片上传进度（media_id/user_id/total_parts/uploaded_parts/status/parts_done） | 24h | 分片上传进度跟踪 |
| `media:download_url:{media_id}:{user_id}` | STRING | 预签名下载 URL | 1h | 下载 URL 缓存，避免频繁签名 |
| `media:process_status:{media_id}` | STRING | JSON 序列化处理状态（status/progress/fail_reason） | 1h | 处理状态缓存 |
| `media:kafka:dedup:{event_id}` | STRING | "1" | 24h | Kafka 消费幂等去重 |

---

## 接口实现

### 1. GetUploadCredential — 获取上传凭证

> 为客户端生成预签名上传 URL 或 STS Token，客户端直传对象存储。
> 流程：参数校验 → 检查缓存 → 生成 media_id → 计算 storage_key → 生成预签名 URL → 写入 DB 记录 → 缓存凭证 → 返回。

```go
func (s *MediaService) GetUploadCredential(ctx context.Context, req *pb.GetUploadCredentialReq) (*pb.GetUploadCredentialResp, error) {
    // ==================== 1. 参数校验 ====================
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id is required")
    }
    if req.UploadType == common.UPLOAD_TYPE_UNSPECIFIED {
        return nil, status.Error(codes.InvalidArgument, "upload_type is required")
    }
    if req.FileName == "" {
        return nil, status.Error(codes.InvalidArgument, "file_name is required")
    }
    if len(req.FileName) > 500 {
        return nil, status.Error(codes.InvalidArgument, "file_name too long, max 500 chars")
    }
    if req.MimeType == "" {
        return nil, status.Error(codes.InvalidArgument, "mime_type is required")
    }
    if req.FileSize <= 0 {
        return nil, status.Error(codes.InvalidArgument, "file_size must be positive")
    }
    // 文件大小限制校验（根据上传类型）
    maxSize := s.getMaxFileSize(req.UploadType) // 图片 10MB, 视频 100MB, 文件 200MB 等
    if req.FileSize > maxSize {
        return nil, status.Errorf(codes.InvalidArgument,
            "file_size exceeds limit: max %d bytes for type %s", maxSize, req.UploadType.String())
    }
    // MIME 类型白名单校验
    if !s.isAllowedMimeType(req.UploadType, req.MimeType) {
        return nil, status.Error(codes.InvalidArgument, "unsupported mime_type for this upload_type")
    }

    // ==================== 2. 生成 media_id — Snowflake ====================
    mediaID := s.snowflake.Generate().String()

    // ==================== 3. 计算存储路径 ====================
    // 格式：{upload_type}/{yyyy}/{MM}/{dd}/{media_id}/{original_name}
    now := time.Now()
    ext := filepath.Ext(req.FileName)
    bucket := s.getBucket(req.UploadType) // 按类型选 bucket：avatar / image / video / file
    storageKey := fmt.Sprintf("%s/%s/%s/%s/%s%s",
        strings.ToLower(req.UploadType.String()),
        now.Format("2006"), now.Format("01"), now.Format("02"),
        mediaID, ext,
    )

    // ==================== 4. 生成预签名上传 URL — 对象存储 ====================
    expireAt := now.Add(15 * time.Minute)
    uploadURL, headers, err := s.ossClient.PresignPutObject(ctx, bucket, storageKey, 15*time.Minute, map[string]string{
        "Content-Type":   req.MimeType,
        "Content-Length": strconv.FormatInt(req.FileSize, 10),
    })
    if err != nil {
        log.Error("presign put object failed",
            "media_id", mediaID, "bucket", bucket, "key", storageKey, "err", err)
        return nil, status.Error(codes.Internal, "generate upload url failed")
    }

    // ==================== 5. 写入媒体记录（PENDING 状态）— PgSQL ====================
    mediaType := s.uploadTypeToMediaType(req.UploadType)
    _, err = s.db.ExecContext(ctx,
        `INSERT INTO media_records (
            media_id, uploader_id, media_type, original_name, file_size,
            mime_type, storage_key, bucket, process_status
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
        mediaID,         // $1: media_id
        req.UserId,      // $2: uploader_id
        mediaType,       // $3: media_type
        req.FileName,    // $4: original_name
        req.FileSize,    // $5: file_size
        req.MimeType,    // $6: mime_type
        storageKey,      // $7: storage_key
        bucket,          // $8: bucket
        0,               // $9: process_status = PENDING
    )
    if err != nil {
        log.Error("insert media_records failed",
            "media_id", mediaID, "user_id", req.UserId, "err", err)
        return nil, status.Error(codes.Internal, "create media record failed")
    }

    // ==================== 6. 缓存上传凭证 — Redis ====================
    credJSON, _ := json.Marshal(map[string]interface{}{
        "upload_url": uploadURL,
        "media_id":   mediaID,
        "bucket":     bucket,
        "key":        storageKey,
        "expire_at":  expireAt.UnixMilli(),
    })
    credKey := fmt.Sprintf("media:upload_cred:%s", mediaID)
    s.redis.Set(ctx, credKey, string(credJSON), 15*time.Minute)

    // ==================== 7. 返回 ====================
    return &pb.GetUploadCredentialResp{
        Meta:        successMeta(ctx),
        UploadUrl:   uploadURL,
        UploadToken: "", // 预签名模式不需要 Token，URL 自带签名
        MediaId:     mediaID,
        ExpireAt:    expireAt.UnixMilli(),
        Headers:     headers,
    }, nil
}

// getMaxFileSize 根据上传类型返回文件大小上限（字节）
func (s *MediaService) getMaxFileSize(uploadType common.UploadType) int64 {
    switch uploadType {
    case common.UPLOAD_TYPE_AVATAR, common.UPLOAD_TYPE_GROUP_AVATAR:
        return 5 * 1024 * 1024      // 头像 5MB
    case common.UPLOAD_TYPE_IMAGE:
        return 10 * 1024 * 1024     // 图片 10MB
    case common.UPLOAD_TYPE_VOICE:
        return 20 * 1024 * 1024     // 语音 20MB
    case common.UPLOAD_TYPE_VIDEO:
        return 100 * 1024 * 1024    // 视频 100MB
    case common.UPLOAD_TYPE_FILE:
        return 200 * 1024 * 1024    // 文件 200MB
    default:
        return 10 * 1024 * 1024
    }
}

// uploadTypeToMediaType 上传类型转换为媒体类型
func (s *MediaService) uploadTypeToMediaType(uploadType common.UploadType) int {
    switch uploadType {
    case common.UPLOAD_TYPE_AVATAR, common.UPLOAD_TYPE_GROUP_AVATAR, common.UPLOAD_TYPE_IMAGE:
        return 1 // IMAGE
    case common.UPLOAD_TYPE_VIDEO:
        return 2 // VIDEO
    case common.UPLOAD_TYPE_VOICE:
        return 5 // VOICE
    case common.UPLOAD_TYPE_FILE:
        return 4 // FILE
    default:
        return 4
    }
}
```

### 2. UploadCallback — 上传完成回调

> 对象存储上传完成后的回调接口，由对象存储网关或客户端调用。
> 流程：参数校验 → 校验媒体记录 → 更新文件 URL → 投递 media.uploaded 事件触发异步处理 → 返回。

```go
func (s *MediaService) UploadCallback(ctx context.Context, req *pb.UploadCallbackReq) (*pb.UploadCallbackResp, error) {
    // ==================== 1. 参数校验 ====================
    if req.MediaId == "" {
        return nil, status.Error(codes.InvalidArgument, "media_id is required")
    }
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id is required")
    }

    // ==================== 2. 查询媒体记录 — PgSQL ====================
    var record struct {
        UploaderID    string
        MediaType     int
        OriginalName  string
        StorageKey    string
        Bucket        string
        MimeType      string
        ProcessStatus int
    }
    err := s.db.QueryRowContext(ctx,
        `SELECT uploader_id, media_type, original_name, storage_key, bucket, mime_type, process_status
         FROM media_records WHERE media_id = $1`,
        req.MediaId,
    ).Scan(&record.UploaderID, &record.MediaType, &record.OriginalName,
        &record.StorageKey, &record.Bucket, &record.MimeType, &record.ProcessStatus)
    if err == sql.ErrNoRows {
        return nil, status.Error(codes.NotFound, "media record not found")
    }
    if err != nil {
        log.Error("query media_records failed", "media_id", req.MediaId, "err", err)
        return nil, status.Error(codes.Internal, "query media record failed")
    }

    // 校验上传者身份
    if record.UploaderID != req.UserId {
        return nil, status.Error(codes.PermissionDenied, "upload user mismatch")
    }

    // 幂等：已经处理过的不重复处理
    if record.ProcessStatus != 0 { // 不是 PENDING
        log.Info("upload callback already processed, skip",
            "media_id", req.MediaId, "status", record.ProcessStatus)
        // 返回当前媒体信息
        mediaInfo, _ := s.buildMediaInfo(ctx, req.MediaId)
        return &pb.UploadCallbackResp{
            Meta:      successMeta(ctx),
            MediaInfo: mediaInfo,
        }, nil
    }

    // ==================== 3. 处理上传结果 ====================
    if !req.Success {
        // 上传失败 → 标记记录为 FAILED
        _, err = s.db.ExecContext(ctx,
            `UPDATE media_records SET process_status = 3, updated_at = NOW()
             WHERE media_id = $1`,
            req.MediaId,
        )
        if err != nil {
            log.Error("update media_records to FAILED failed", "media_id", req.MediaId, "err", err)
        }
        return &pb.UploadCallbackResp{
            Meta: failMeta(ctx, "upload failed"),
        }, nil
    }

    // ==================== 4. 更新媒体记录 — PgSQL ====================
    // 文件 URL 和实际大小
    fileURL := req.Url
    if fileURL == "" {
        // 如果回调未带 URL，自行拼接
        fileURL = s.ossClient.GetObjectURL(record.Bucket, record.StorageKey)
    }
    actualSize := req.FileSize
    if actualSize <= 0 {
        // 未带实际大小，使用预登记的大小
        actualSize = 0
    }

    // 延迟双删 — process_status 即将变更，使 media:info 缓存失效
    s.delayedDoubleDelete(ctx,
        fmt.Sprintf("media:info:%s", req.MediaId),
    )

    _, err = s.db.ExecContext(ctx,
        `UPDATE media_records
         SET file_size = CASE WHEN $1 > 0 THEN $1 ELSE file_size END,
             process_status = 1,
             updated_at = NOW()
         WHERE media_id = $2 AND process_status = 0`,
        actualSize,   // $1: actual file size
        req.MediaId,  // $2: media_id
    )
    if err != nil {
        log.Error("update media_records after upload failed",
            "media_id", req.MediaId, "err", err)
        return nil, status.Error(codes.Internal, "update media record failed")
    }

    // ==================== 5. 清理 Redis 上传凭证缓存 ====================
    s.redis.Del(ctx, fmt.Sprintf("media:upload_cred:%s", req.MediaId))

    // ==================== 6. 投递 Kafka media.uploaded 事件 → 触发异步处理流水线 ====================
    uploadEvent := &kafka_media.MediaUploadCompletedEvent{
        Header:       buildEventHeader("media", s.instanceID),
        MediaId:      req.MediaId,
        UploaderId:   req.UserId,
        MediaType:    kafka_media.MediaType(record.MediaType),
        OriginalName: record.OriginalName,
        ContentType:  record.MimeType,
        FileSize:     actualSize,
        StoragePath:  record.StorageKey,
        Url:          fileURL,
        Md5:          req.Md5,
        UploadTime:   time.Now().UnixMilli(),
    }
    if err := s.kafka.Produce(ctx, "media.uploaded", req.MediaId, uploadEvent); err != nil {
        log.Error("produce media.uploaded event failed",
            "media_id", req.MediaId, "err", err)
        // Kafka 失败不阻塞响应，通过定时补偿扫描 process_status=1 但超时未完成的记录
    }

    // ==================== 7. 缓存处理状态 — Redis ====================
    statusJSON, _ := json.Marshal(map[string]interface{}{
        "status":   1, // PROCESSING
        "progress": 0,
    })
    s.redis.Set(ctx, fmt.Sprintf("media:process_status:%s", req.MediaId),
        string(statusJSON), 1*time.Hour)

    // ==================== 8. 返回 ====================
    return &pb.UploadCallbackResp{
        Meta: successMeta(ctx),
        MediaInfo: &common.MediaInfo{
            MediaId:  req.MediaId,
            Url:      fileURL,
            FileName: record.OriginalName,
            MimeType: record.MimeType,
            FileSize: actualSize,
        },
    }, nil
}
```

### 3. InitMultipartUpload — 初始化分片上传

> 大文件（≥5MB）分片上传初始化。生成 media_id、在对象存储创建分片上传任务、记录 DB 和 Redis。

```go
func (s *MediaService) InitMultipartUpload(ctx context.Context, req *pb.InitMultipartUploadReq) (*pb.InitMultipartUploadResp, error) {
    // ==================== 1. 参数校验 ====================
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id is required")
    }
    if req.UploadType == common.UPLOAD_TYPE_UNSPECIFIED {
        return nil, status.Error(codes.InvalidArgument, "upload_type is required")
    }
    if req.FileName == "" {
        return nil, status.Error(codes.InvalidArgument, "file_name is required")
    }
    if req.MimeType == "" {
        return nil, status.Error(codes.InvalidArgument, "mime_type is required")
    }
    if req.FileSize <= 0 {
        return nil, status.Error(codes.InvalidArgument, "file_size must be positive")
    }
    // 分片上传仅用于大文件
    if req.FileSize < 5*1024*1024 {
        return nil, status.Error(codes.InvalidArgument, "file_size < 5MB, use normal upload instead")
    }
    maxSize := s.getMaxFileSize(req.UploadType)
    if req.FileSize > maxSize {
        return nil, status.Errorf(codes.InvalidArgument,
            "file_size exceeds limit: max %d bytes", maxSize)
    }
    if !s.isAllowedMimeType(req.UploadType, req.MimeType) {
        return nil, status.Error(codes.InvalidArgument, "unsupported mime_type")
    }

    // ==================== 2. 计算分片参数 ====================
    partSize := req.PartSize
    if partSize <= 0 {
        partSize = 5 * 1024 * 1024 // 默认 5MB 每片
    }
    if partSize < 5*1024*1024 {
        partSize = 5 * 1024 * 1024 // 最小 5MB
    }
    totalParts := int(math.Ceil(float64(req.FileSize) / float64(partSize)))
    if totalParts > 10000 {
        return nil, status.Error(codes.InvalidArgument, "too many parts, increase part_size")
    }

    // ==================== 3. 生成 media_id 和存储路径 ====================
    mediaID := s.snowflake.Generate().String()
    now := time.Now()
    ext := filepath.Ext(req.FileName)
    bucket := s.getBucket(req.UploadType)
    storageKey := fmt.Sprintf("%s/%s/%s/%s/%s%s",
        strings.ToLower(req.UploadType.String()),
        now.Format("2006"), now.Format("01"), now.Format("02"),
        mediaID, ext,
    )

    // ==================== 4. 在对象存储创建分片上传任务 ====================
    uploadID, err := s.ossClient.CreateMultipartUpload(ctx, bucket, storageKey, req.MimeType)
    if err != nil {
        log.Error("create multipart upload in OSS failed",
            "media_id", mediaID, "bucket", bucket, "key", storageKey, "err", err)
        return nil, status.Error(codes.Internal, "create multipart upload failed")
    }

    // ==================== 5. 写入媒体记录（PENDING）— PgSQL ====================
    mediaType := s.uploadTypeToMediaType(req.UploadType)
    expiresAt := now.Add(24 * time.Hour)

    tx, err := s.db.BeginTx(ctx, nil)
    if err != nil {
        return nil, status.Error(codes.Internal, "begin transaction failed")
    }
    defer tx.Rollback()

    // 5a. 媒体记录
    _, err = tx.ExecContext(ctx,
        `INSERT INTO media_records (
            media_id, uploader_id, media_type, original_name, file_size,
            mime_type, storage_key, bucket, process_status
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
        mediaID, req.UserId, mediaType, req.FileName, req.FileSize,
        req.MimeType, storageKey, bucket, 0,
    )
    if err != nil {
        log.Error("insert media_records for multipart failed", "media_id", mediaID, "err", err)
        return nil, status.Error(codes.Internal, "create media record failed")
    }

    // 5b. 分片上传记录
    _, err = tx.ExecContext(ctx,
        `INSERT INTO multipart_uploads (
            upload_id, media_id, user_id, bucket, storage_key,
            total_parts, uploaded_parts, status, expires_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
        uploadID, mediaID, req.UserId, bucket, storageKey,
        totalParts, 0, 0, expiresAt,
    )
    if err != nil {
        log.Error("insert multipart_uploads failed", "upload_id", uploadID, "err", err)
        return nil, status.Error(codes.Internal, "create multipart upload record failed")
    }

    if err := tx.Commit(); err != nil {
        return nil, status.Error(codes.Internal, "commit transaction failed")
    }

    // ==================== 6. 缓存分片进度 — Redis ====================
    multipartKey := fmt.Sprintf("media:multipart:%s", uploadID)
    s.redis.HSet(ctx, multipartKey, map[string]interface{}{
        "media_id":       mediaID,
        "user_id":        req.UserId,
        "bucket":         bucket,
        "storage_key":    storageKey,
        "total_parts":    totalParts,
        "uploaded_parts": 0,
        "status":         0,
    })
    s.redis.Expire(ctx, multipartKey, 24*time.Hour)

    // ==================== 7. 返回 ====================
    return &pb.InitMultipartUploadResp{
        Meta: successMeta(ctx),
        UploadInfo: &pb.MultipartUploadInfo{
            UploadId:   uploadID,
            MediaId:    mediaID,
            TotalParts: int32(totalParts),
            PartSize:   partSize,
        },
    }, nil
}
```

### 4. GetPartUploadUrl — 获取分片上传 URL

> 为指定分片生成预签名上传 URL，客户端拿 URL 直传对象存储。

```go
func (s *MediaService) GetPartUploadUrl(ctx context.Context, req *pb.GetPartUploadUrlReq) (*pb.GetPartUploadUrlResp, error) {
    // ==================== 1. 参数校验 ====================
    if req.UploadId == "" {
        return nil, status.Error(codes.InvalidArgument, "upload_id is required")
    }
    if req.MediaId == "" {
        return nil, status.Error(codes.InvalidArgument, "media_id is required")
    }
    if req.PartNumber <= 0 {
        return nil, status.Error(codes.InvalidArgument, "part_number must be positive")
    }

    // ==================== 2. 查询分片上传记录 ====================
    // 优先从 Redis 查
    multipartKey := fmt.Sprintf("media:multipart:%s", req.UploadId)
    cached, err := s.redis.HGetAll(ctx, multipartKey).Result()
    var bucket, storageKey, mediaID string
    var totalParts int
    var uploadStatus int

    if err == nil && len(cached) > 0 {
        bucket = cached["bucket"]
        storageKey = cached["storage_key"]
        mediaID = cached["media_id"]
        totalParts, _ = strconv.Atoi(cached["total_parts"])
        uploadStatus, _ = strconv.Atoi(cached["status"])
    } else {
        // Redis 未命中，回查 PgSQL
        err = s.db.QueryRowContext(ctx,
            `SELECT media_id, bucket, storage_key, total_parts, status
             FROM multipart_uploads WHERE upload_id = $1`,
            req.UploadId,
        ).Scan(&mediaID, &bucket, &storageKey, &totalParts, &uploadStatus)
        if err == sql.ErrNoRows {
            return nil, status.Error(codes.NotFound, "multipart upload not found")
        }
        if err != nil {
            return nil, status.Error(codes.Internal, "query multipart upload failed")
        }
        // 回填 Redis
        s.redis.HSet(ctx, multipartKey, map[string]interface{}{
            "media_id":    mediaID,
            "bucket":      bucket,
            "storage_key": storageKey,
            "total_parts": totalParts,
            "status":      uploadStatus,
        })
        s.redis.Expire(ctx, multipartKey, 24*time.Hour)
    }

    // 校验 media_id 一致性
    if mediaID != req.MediaId {
        return nil, status.Error(codes.InvalidArgument, "media_id mismatch")
    }

    // 校验上传状态
    if uploadStatus != 0 { // 非进行中
        return nil, status.Error(codes.FailedPrecondition, "multipart upload is not in progress")
    }

    // 校验分片序号范围
    if int(req.PartNumber) > totalParts {
        return nil, status.Errorf(codes.InvalidArgument,
            "part_number %d exceeds total_parts %d", req.PartNumber, totalParts)
    }

    // ==================== 3. 生成分片预签名 URL — 对象存储 ====================
    expireAt := time.Now().Add(15 * time.Minute)
    partURL, headers, err := s.ossClient.PresignUploadPart(ctx, bucket, storageKey,
        req.UploadId, int(req.PartNumber), 15*time.Minute)
    if err != nil {
        log.Error("presign upload part failed",
            "upload_id", req.UploadId, "part", req.PartNumber, "err", err)
        return nil, status.Error(codes.Internal, "generate part upload url failed")
    }

    // ==================== 4. 返回 ====================
    return &pb.GetPartUploadUrlResp{
        Meta:      successMeta(ctx),
        UploadUrl: partURL,
        ExpireAt:  expireAt.UnixMilli(),
        Headers:   headers,
    }, nil
}
```

### 5. CompleteMultipartUpload — 完成分片上传

> 所有分片上传完毕后调用，在对象存储合并分片，更新 DB 记录，触发处理流水线。

```go
func (s *MediaService) CompleteMultipartUpload(ctx context.Context, req *pb.CompleteMultipartUploadReq) (*pb.CompleteMultipartUploadResp, error) {
    // ==================== 1. 参数校验 ====================
    if req.UploadId == "" {
        return nil, status.Error(codes.InvalidArgument, "upload_id is required")
    }
    if req.MediaId == "" {
        return nil, status.Error(codes.InvalidArgument, "media_id is required")
    }
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id is required")
    }
    if len(req.Parts) == 0 {
        return nil, status.Error(codes.InvalidArgument, "parts is required")
    }

    // ==================== 2. 查询分片上传记录 — PgSQL ====================
    var record struct {
        MediaID    string
        UserID     string
        Bucket     string
        StorageKey string
        TotalParts int
        Status     int
    }
    err := s.db.QueryRowContext(ctx,
        `SELECT media_id, user_id, bucket, storage_key, total_parts, status
         FROM multipart_uploads WHERE upload_id = $1`,
        req.UploadId,
    ).Scan(&record.MediaID, &record.UserID, &record.Bucket, &record.StorageKey,
        &record.TotalParts, &record.Status)
    if err == sql.ErrNoRows {
        return nil, status.Error(codes.NotFound, "multipart upload not found")
    }
    if err != nil {
        return nil, status.Error(codes.Internal, "query multipart upload failed")
    }

    // 校验
    if record.MediaID != req.MediaId {
        return nil, status.Error(codes.InvalidArgument, "media_id mismatch")
    }
    if record.UserID != req.UserId {
        return nil, status.Error(codes.PermissionDenied, "user_id mismatch")
    }
    if record.Status != 0 {
        return nil, status.Error(codes.FailedPrecondition, "multipart upload is not in progress")
    }
    if len(req.Parts) != record.TotalParts {
        return nil, status.Errorf(codes.InvalidArgument,
            "parts count %d != total_parts %d", len(req.Parts), record.TotalParts)
    }

    // ==================== 3. 在对象存储合并分片 ====================
    ossParts := make([]oss.CompletedPart, 0, len(req.Parts))
    for _, p := range req.Parts {
        ossParts = append(ossParts, oss.CompletedPart{
            PartNumber: int(p.PartNumber),
            ETag:       p.Etag,
        })
    }
    fileURL, err := s.ossClient.CompleteMultipartUpload(ctx, record.Bucket, record.StorageKey,
        req.UploadId, ossParts)
    if err != nil {
        log.Error("complete multipart upload in OSS failed",
            "upload_id", req.UploadId, "media_id", req.MediaId, "err", err)
        return nil, status.Error(codes.Internal, "complete multipart upload failed")
    }

    // 延迟双删 — process_status 即将变更，使 media:info 缓存失效
    s.delayedDoubleDelete(ctx,
        fmt.Sprintf("media:info:%s", req.MediaId),
    )

    // ==================== 4. 更新数据库 — PgSQL（事务） ====================
    tx, err := s.db.BeginTx(ctx, nil)
    if err != nil {
        return nil, status.Error(codes.Internal, "begin transaction failed")
    }
    defer tx.Rollback()

    // 4a. 更新分片上传记录为已完成
    _, err = tx.ExecContext(ctx,
        `UPDATE multipart_uploads
         SET status = 1, uploaded_parts = total_parts
         WHERE upload_id = $1 AND status = 0`,
        req.UploadId,
    )
    if err != nil {
        log.Error("update multipart_uploads to completed failed",
            "upload_id", req.UploadId, "err", err)
        return nil, status.Error(codes.Internal, "update multipart upload status failed")
    }

    // 4b. 批量写入分片详情
    for _, p := range req.Parts {
        _, err = tx.ExecContext(ctx,
            `INSERT INTO multipart_parts (upload_id, part_number, etag, size)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (upload_id, part_number) DO UPDATE SET etag = $3, size = $4`,
            req.UploadId, p.PartNumber, p.Etag, 0, // size 由客户端上传时记录
        )
        if err != nil {
            log.Error("insert multipart_parts failed",
                "upload_id", req.UploadId, "part", p.PartNumber, "err", err)
            return nil, status.Error(codes.Internal, "save part info failed")
        }
    }

    // 4c. 更新媒体记录为 PROCESSING
    _, err = tx.ExecContext(ctx,
        `UPDATE media_records
         SET process_status = 1, updated_at = NOW()
         WHERE media_id = $1 AND process_status = 0`,
        req.MediaId,
    )
    if err != nil {
        log.Error("update media_records to PROCESSING failed",
            "media_id", req.MediaId, "err", err)
        return nil, status.Error(codes.Internal, "update media record failed")
    }

    if err := tx.Commit(); err != nil {
        return nil, status.Error(codes.Internal, "commit transaction failed")
    }

    // ==================== 5. 清理 Redis 分片进度缓存 ====================
    s.redis.Del(ctx, fmt.Sprintf("media:multipart:%s", req.UploadId))

    // ==================== 6. 投递 Kafka media.uploaded 事件 → 触发异步处理 ====================
    // 查询完整媒体记录构建事件
    var mediaRecord struct {
        MediaType    int
        OriginalName string
        MimeType     string
        FileSize     int64
    }
    s.db.QueryRowContext(ctx,
        `SELECT media_type, original_name, mime_type, file_size
         FROM media_records WHERE media_id = $1`, req.MediaId,
    ).Scan(&mediaRecord.MediaType, &mediaRecord.OriginalName,
        &mediaRecord.MimeType, &mediaRecord.FileSize)

    uploadEvent := &kafka_media.MediaUploadCompletedEvent{
        Header:       buildEventHeader("media", s.instanceID),
        MediaId:      req.MediaId,
        UploaderId:   req.UserId,
        MediaType:    kafka_media.MediaType(mediaRecord.MediaType),
        OriginalName: mediaRecord.OriginalName,
        ContentType:  mediaRecord.MimeType,
        FileSize:     mediaRecord.FileSize,
        StoragePath:  record.StorageKey,
        Url:          fileURL,
        UploadTime:   time.Now().UnixMilli(),
    }
    if err := s.kafka.Produce(ctx, "media.uploaded", req.MediaId, uploadEvent); err != nil {
        log.Error("produce media.uploaded event failed",
            "media_id", req.MediaId, "err", err)
    }

    // ==================== 7. 缓存处理状态 — Redis ====================
    statusJSON, _ := json.Marshal(map[string]interface{}{
        "status":   1, // PROCESSING
        "progress": 0,
    })
    s.redis.Set(ctx, fmt.Sprintf("media:process_status:%s", req.MediaId),
        string(statusJSON), 1*time.Hour)

    // ==================== 8. 返回 ====================
    return &pb.CompleteMultipartUploadResp{
        Meta: successMeta(ctx),
        MediaInfo: &common.MediaInfo{
            MediaId:  req.MediaId,
            Url:      fileURL,
            FileName: mediaRecord.OriginalName,
            MimeType: mediaRecord.MimeType,
            FileSize: mediaRecord.FileSize,
        },
    }, nil
}
```

### 6. AbortMultipartUpload — 取消分片上传

> 取消分片上传，清理对象存储中的已上传分片和本地记录。

```go
func (s *MediaService) AbortMultipartUpload(ctx context.Context, req *pb.AbortMultipartUploadReq) (*pb.AbortMultipartUploadResp, error) {
    // ==================== 1. 参数校验 ====================
    if req.UploadId == "" {
        return nil, status.Error(codes.InvalidArgument, "upload_id is required")
    }
    if req.MediaId == "" {
        return nil, status.Error(codes.InvalidArgument, "media_id is required")
    }
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id is required")
    }

    // ==================== 2. 查询分片上传记录 — PgSQL ====================
    var record struct {
        MediaID    string
        UserID     string
        Bucket     string
        StorageKey string
        Status     int
    }
    err := s.db.QueryRowContext(ctx,
        `SELECT media_id, user_id, bucket, storage_key, status
         FROM multipart_uploads WHERE upload_id = $1`,
        req.UploadId,
    ).Scan(&record.MediaID, &record.UserID, &record.Bucket, &record.StorageKey, &record.Status)
    if err == sql.ErrNoRows {
        return nil, status.Error(codes.NotFound, "multipart upload not found")
    }
    if err != nil {
        return nil, status.Error(codes.Internal, "query multipart upload failed")
    }

    // 校验
    if record.MediaID != req.MediaId {
        return nil, status.Error(codes.InvalidArgument, "media_id mismatch")
    }
    if record.UserID != req.UserId {
        return nil, status.Error(codes.PermissionDenied, "user_id mismatch")
    }
    // 幂等：已取消的直接返回成功
    if record.Status == 2 {
        return &pb.AbortMultipartUploadResp{Meta: successMeta(ctx)}, nil
    }
    if record.Status != 0 {
        return nil, status.Error(codes.FailedPrecondition, "multipart upload is not in progress")
    }

    // ==================== 3. 在对象存储取消分片上传 ====================
    if err := s.ossClient.AbortMultipartUpload(ctx, record.Bucket, record.StorageKey, req.UploadId); err != nil {
        log.Error("abort multipart upload in OSS failed",
            "upload_id", req.UploadId, "err", err)
        // 不阻塞，对象存储侧通常也有自动清理机制
    }

    // ==================== 延迟双删 — DB 写入前失效缓存，500ms 后再次删除防止并发读回填旧数据 ====================
    s.delayedDoubleDelete(ctx,
        fmt.Sprintf("media:multipart:%s", req.UploadId),
        fmt.Sprintf("media:process_status:%s", req.MediaId),
        fmt.Sprintf("media:info:%s", req.MediaId),
    )

    // ==================== 4. 更新数据库 — PgSQL ====================
    tx, err := s.db.BeginTx(ctx, nil)
    if err != nil {
        return nil, status.Error(codes.Internal, "begin transaction failed")
    }
    defer tx.Rollback()

    // 4a. 更新分片上传记录为已取消
    _, err = tx.ExecContext(ctx,
        `UPDATE multipart_uploads SET status = 2 WHERE upload_id = $1 AND status = 0`,
        req.UploadId,
    )
    if err != nil {
        return nil, status.Error(codes.Internal, "update multipart upload status failed")
    }

    // 4b. 更新媒体记录为 FAILED
    _, err = tx.ExecContext(ctx,
        `UPDATE media_records SET process_status = 3, updated_at = NOW()
         WHERE media_id = $1 AND process_status = 0`,
        req.MediaId,
    )
    if err != nil {
        log.Error("update media_records to FAILED on abort", "media_id", req.MediaId, "err", err)
    }

    // 4c. 删除分片详情记录
    _, err = tx.ExecContext(ctx,
        `DELETE FROM multipart_parts WHERE upload_id = $1`,
        req.UploadId,
    )
    if err != nil {
        log.Warn("delete multipart_parts failed", "upload_id", req.UploadId, "err", err)
    }

    if err := tx.Commit(); err != nil {
        return nil, status.Error(codes.Internal, "commit transaction failed")
    }

    log.Info("multipart upload aborted",
        "upload_id", req.UploadId, "media_id", req.MediaId, "user_id", req.UserId)

    // ==================== 6. 返回 ====================
    return &pb.AbortMultipartUploadResp{Meta: successMeta(ctx)}, nil
}
```

### 7. GetMediaInfo — 获取媒体信息

> 查询单个媒体的元信息（URL、大小、类型、宽高、时长等）。
> 优先读 Redis 缓存，未命中回查 PgSQL 并回填。

```go
func (s *MediaService) GetMediaInfo(ctx context.Context, req *pb.GetMediaInfoReq) (*pb.GetMediaInfoResp, error) {
    // ==================== 1. 参数校验 ====================
    if req.MediaId == "" {
        return nil, status.Error(codes.InvalidArgument, "media_id is required")
    }

    // ==================== 2. 查询 Redis 缓存 ====================
    infoKey := fmt.Sprintf("media:info:%s", req.MediaId)
    cached, err := s.redis.HGetAll(ctx, infoKey).Result()
    if err == nil && len(cached) > 0 {
        // 缓存命中 → 直接构造返回
        mediaInfo := s.buildMediaInfoFromCache(cached)
        return &pb.GetMediaInfoResp{
            Meta:      successMeta(ctx),
            MediaInfo: mediaInfo,
        }, nil
    }

    // ==================== 3. 缓存未命中，回查 PgSQL ====================
    var record struct {
        MediaID       string
        UploaderID    string
        MediaType     int
        OriginalName  string
        FileSize      int64
        MimeType      string
        StorageKey    string
        Bucket        string
        Width         int
        Height        int
        Duration      int
        ThumbnailKey  string
        SnapshotKey   string
        ProcessStatus int
        Extra         []byte
        CreatedAt     time.Time
    }
    err = s.db.QueryRowContext(ctx,
        `SELECT media_id, uploader_id, media_type, original_name, file_size,
                mime_type, storage_key, bucket, width, height, duration,
                thumbnail_key, snapshot_key, process_status, extra, created_at
         FROM media_records WHERE media_id = $1`,
        req.MediaId,
    ).Scan(&record.MediaID, &record.UploaderID, &record.MediaType, &record.OriginalName,
        &record.FileSize, &record.MimeType, &record.StorageKey, &record.Bucket,
        &record.Width, &record.Height, &record.Duration, &record.ThumbnailKey,
        &record.SnapshotKey, &record.ProcessStatus, &record.Extra, &record.CreatedAt)
    if err == sql.ErrNoRows {
        return nil, status.Error(codes.NotFound, "media not found")
    }
    if err != nil {
        log.Error("query media_records failed", "media_id", req.MediaId, "err", err)
        return nil, status.Error(codes.Internal, "query media info failed")
    }

    // ==================== 4. 构造 URL ====================
    url := s.ossClient.GetObjectURL(record.Bucket, record.StorageKey)
    thumbnailURL := ""
    if record.ThumbnailKey != "" {
        thumbnailURL = s.ossClient.GetObjectURL(record.Bucket, record.ThumbnailKey)
    }

    // ==================== 5. 回填 Redis 缓存 ====================
    s.redis.HSet(ctx, infoKey, map[string]interface{}{
        "media_id":       record.MediaID,
        "uploader_id":    record.UploaderID,
        "media_type":     record.MediaType,
        "original_name":  record.OriginalName,
        "file_size":      record.FileSize,
        "mime_type":      record.MimeType,
        "url":            url,
        "thumbnail_url":  thumbnailURL,
        "width":          record.Width,
        "height":         record.Height,
        "duration":       record.Duration,
        "process_status": record.ProcessStatus,
        "created_at":     record.CreatedAt.UnixMilli(),
    })
    s.redis.Expire(ctx, infoKey, 1*time.Hour)

    // ==================== 6. 返回 ====================
    return &pb.GetMediaInfoResp{
        Meta: successMeta(ctx),
        MediaInfo: &common.MediaInfo{
            MediaId:      record.MediaID,
            Url:          url,
            ThumbnailUrl: thumbnailURL,
            FileName:     record.OriginalName,
            MimeType:     record.MimeType,
            FileSize:     record.FileSize,
            Width:        int32(record.Width),
            Height:       int32(record.Height),
            Duration:     int32(record.Duration),
        },
    }, nil
}

// buildMediaInfoFromCache 从 Redis HASH 缓存构造 MediaInfo
func (s *MediaService) buildMediaInfoFromCache(cached map[string]string) *common.MediaInfo {
    fileSize, _ := strconv.ParseInt(cached["file_size"], 10, 64)
    width, _ := strconv.Atoi(cached["width"])
    height, _ := strconv.Atoi(cached["height"])
    duration, _ := strconv.Atoi(cached["duration"])
    return &common.MediaInfo{
        MediaId:      cached["media_id"],
        Url:          cached["url"],
        ThumbnailUrl: cached["thumbnail_url"],
        FileName:     cached["original_name"],
        MimeType:     cached["mime_type"],
        FileSize:     fileSize,
        Width:        int32(width),
        Height:       int32(height),
        Duration:     int32(duration),
    }
}
```

### 8. BatchGetMediaInfo — 批量获取媒体信息

> 批量查询多个媒体的元信息，支持最多 100 个。
> 先批量读 Redis，未命中的回查 PgSQL 并回填。

```go
func (s *MediaService) BatchGetMediaInfo(ctx context.Context, req *pb.BatchGetMediaInfoReq) (*pb.BatchGetMediaInfoResp, error) {
    // ==================== 1. 参数校验 ====================
    if len(req.MediaIds) == 0 {
        return nil, status.Error(codes.InvalidArgument, "media_ids is required")
    }
    if len(req.MediaIds) > 100 {
        return nil, status.Error(codes.InvalidArgument, "media_ids too many, max 100")
    }
    // 去重
    uniqueIDs := make(map[string]bool)
    mediaIDs := make([]string, 0, len(req.MediaIds))
    for _, id := range req.MediaIds {
        if id != "" && !uniqueIDs[id] {
            uniqueIDs[id] = true
            mediaIDs = append(mediaIDs, id)
        }
    }

    // ==================== 2. 批量查询 Redis 缓存 ====================
    result := make([]*common.MediaInfo, 0, len(mediaIDs))
    missedIDs := make([]string, 0)

    pipe := s.redis.Pipeline()
    cmds := make(map[string]*redis.StringStringMapCmd, len(mediaIDs))
    for _, mid := range mediaIDs {
        infoKey := fmt.Sprintf("media:info:%s", mid)
        cmds[mid] = pipe.HGetAll(ctx, infoKey)
    }
    pipe.Exec(ctx)

    for _, mid := range mediaIDs {
        cached, err := cmds[mid].Result()
        if err == nil && len(cached) > 0 && cached["media_id"] != "" {
            // 缓存命中
            result = append(result, s.buildMediaInfoFromCache(cached))
        } else {
            // 缓存未命中
            missedIDs = append(missedIDs, mid)
        }
    }

    // ==================== 3. 回查 PgSQL（未命中部分） ====================
    if len(missedIDs) > 0 {
        // 构建 IN 查询
        placeholders := make([]string, len(missedIDs))
        args := make([]interface{}, len(missedIDs))
        for i, id := range missedIDs {
            placeholders[i] = fmt.Sprintf("$%d", i+1)
            args[i] = id
        }

        query := fmt.Sprintf(
            `SELECT media_id, uploader_id, media_type, original_name, file_size,
                    mime_type, storage_key, bucket, width, height, duration,
                    thumbnail_key, process_status, created_at
             FROM media_records
             WHERE media_id IN (%s)`,
            strings.Join(placeholders, ","),
        )

        rows, err := s.db.QueryContext(ctx, query, args...)
        if err != nil {
            log.Error("batch query media_records failed", "err", err)
            // 不返回错误，返回已有结果
        } else {
            defer rows.Close()
            // 用于批量回填 Redis 的 pipeline
            cachePipe := s.redis.Pipeline()

            for rows.Next() {
                var r struct {
                    MediaID       string
                    UploaderID    string
                    MediaType     int
                    OriginalName  string
                    FileSize      int64
                    MimeType      string
                    StorageKey    string
                    Bucket        string
                    Width         int
                    Height        int
                    Duration      int
                    ThumbnailKey  string
                    ProcessStatus int
                    CreatedAt     time.Time
                }
                if err := rows.Scan(&r.MediaID, &r.UploaderID, &r.MediaType, &r.OriginalName,
                    &r.FileSize, &r.MimeType, &r.StorageKey, &r.Bucket, &r.Width, &r.Height,
                    &r.Duration, &r.ThumbnailKey, &r.ProcessStatus, &r.CreatedAt); err != nil {
                    log.Error("scan media_records row failed", "err", err)
                    continue
                }

                url := s.ossClient.GetObjectURL(r.Bucket, r.StorageKey)
                thumbnailURL := ""
                if r.ThumbnailKey != "" {
                    thumbnailURL = s.ossClient.GetObjectURL(r.Bucket, r.ThumbnailKey)
                }

                mediaInfo := &common.MediaInfo{
                    MediaId:      r.MediaID,
                    Url:          url,
                    ThumbnailUrl: thumbnailURL,
                    FileName:     r.OriginalName,
                    MimeType:     r.MimeType,
                    FileSize:     r.FileSize,
                    Width:        int32(r.Width),
                    Height:       int32(r.Height),
                    Duration:     int32(r.Duration),
                }
                result = append(result, mediaInfo)

                // 回填 Redis 缓存
                infoKey := fmt.Sprintf("media:info:%s", r.MediaID)
                cachePipe.HSet(ctx, infoKey, map[string]interface{}{
                    "media_id":       r.MediaID,
                    "uploader_id":    r.UploaderID,
                    "media_type":     r.MediaType,
                    "original_name":  r.OriginalName,
                    "file_size":      r.FileSize,
                    "mime_type":      r.MimeType,
                    "url":            url,
                    "thumbnail_url":  thumbnailURL,
                    "width":          r.Width,
                    "height":         r.Height,
                    "duration":       r.Duration,
                    "process_status": r.ProcessStatus,
                    "created_at":     r.CreatedAt.UnixMilli(),
                })
                cachePipe.Expire(ctx, infoKey, 1*time.Hour)
            }
            cachePipe.Exec(ctx) // 批量回填
        }
    }

    // ==================== 4. 返回 ====================
    return &pb.BatchGetMediaInfoResp{
        Meta:       successMeta(ctx),
        MediaInfos: result,
    }, nil
}
```

### 9. GetDownloadUrl — 获取预签名下载 URL

> 生成带签名和过期时间的下载 URL，防止未授权访问。
> 优先读 Redis 缓存，未命中则签名并缓存。

```go
func (s *MediaService) GetDownloadUrl(ctx context.Context, req *pb.GetDownloadUrlReq) (*pb.GetDownloadUrlResp, error) {
    // ==================== 1. 参数校验 ====================
    if req.MediaId == "" {
        return nil, status.Error(codes.InvalidArgument, "media_id is required")
    }
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id is required")
    }

    // ==================== 2. 查询 Redis 缓存 ====================
    downloadKey := fmt.Sprintf("media:download_url:%s:%s", req.MediaId, req.UserId)
    cachedURL, err := s.redis.Get(ctx, downloadKey).Result()
    if err == nil && cachedURL != "" {
        // 缓存命中 → 直接返回
        return &pb.GetDownloadUrlResp{
            Meta:        successMeta(ctx),
            DownloadUrl: cachedURL,
            ExpireAt:    time.Now().Add(50 * time.Minute).UnixMilli(), // 预估剩余时间
        }, nil
    }

    // ==================== 3. 查询媒体记录 — PgSQL ====================
    var bucket, storageKey, originalName string
    var processStatus int
    err = s.db.QueryRowContext(ctx,
        `SELECT bucket, storage_key, original_name, process_status
         FROM media_records WHERE media_id = $1`,
        req.MediaId,
    ).Scan(&bucket, &storageKey, &originalName, &processStatus)
    if err == sql.ErrNoRows {
        return nil, status.Error(codes.NotFound, "media not found")
    }
    if err != nil {
        log.Error("query media_records for download failed", "media_id", req.MediaId, "err", err)
        return nil, status.Error(codes.Internal, "query media record failed")
    }

    // 检查处理状态（未完成处理的不允许下载原文件，但允许下载原图）
    // PENDING 和 PROCESSING 状态也可以下载（客户端已直传完成）
    // FAILED 状态也允许（原文件仍在对象存储中）

    // ==================== 4. 生成预签名下载 URL — 对象存储 ====================
    expireDuration := 1 * time.Hour
    expireAt := time.Now().Add(expireDuration)
    downloadURL, err := s.ossClient.PresignGetObject(ctx, bucket, storageKey, expireDuration, map[string]string{
        "response-content-disposition": fmt.Sprintf("attachment; filename=\"%s\"",
            url.PathEscape(originalName)),
    })
    if err != nil {
        log.Error("presign get object failed",
            "media_id", req.MediaId, "bucket", bucket, "key", storageKey, "err", err)
        return nil, status.Error(codes.Internal, "generate download url failed")
    }

    // ==================== 5. 缓存下载 URL — Redis ====================
    s.redis.Set(ctx, downloadKey, downloadURL, 1*time.Hour)

    // ==================== 6. 返回 ====================
    return &pb.GetDownloadUrlResp{
        Meta:        successMeta(ctx),
        DownloadUrl: downloadURL,
        ExpireAt:    expireAt.UnixMilli(),
    }, nil
}
```

### 10. GetThumbnailUrl — 获取缩略图 URL

> 获取图片/视频的缩略图 URL。如果缩略图已生成（thumbnail_key 非空），直接签名返回；
> 如果缩略图还未生成（处理中），返回原图 URL + 对象存储实时缩略图处理参数。

```go
func (s *MediaService) GetThumbnailUrl(ctx context.Context, req *pb.GetThumbnailUrlReq) (*pb.GetThumbnailUrlResp, error) {
    // ==================== 1. 参数校验 ====================
    if req.MediaId == "" {
        return nil, status.Error(codes.InvalidArgument, "media_id is required")
    }
    // 缩略图尺寸默认值
    width := int(req.Width)
    height := int(req.Height)
    if width <= 0 {
        width = 200
    }
    if height <= 0 {
        height = 200
    }
    if width > 1920 || height > 1920 {
        return nil, status.Error(codes.InvalidArgument, "thumbnail size too large, max 1920x1920")
    }

    // ==================== 2. 查询媒体记录 ====================
    // 优先从 Redis 缓存读
    infoKey := fmt.Sprintf("media:info:%s", req.MediaId)
    cached, err := s.redis.HGetAll(ctx, infoKey).Result()

    var bucket, storageKey, thumbnailKey string
    var mediaType, processStatus int

    if err == nil && len(cached) > 0 && cached["media_id"] != "" {
        // 缓存命中 → 解析必要字段（缓存中可能没有 bucket/storage_key，需回查）
        mediaType, _ = strconv.Atoi(cached["media_type"])
    }

    // 回查 PgSQL 获取完整信息
    err = s.db.QueryRowContext(ctx,
        `SELECT bucket, storage_key, thumbnail_key, media_type, process_status
         FROM media_records WHERE media_id = $1`,
        req.MediaId,
    ).Scan(&bucket, &storageKey, &thumbnailKey, &mediaType, &processStatus)
    if err == sql.ErrNoRows {
        return nil, status.Error(codes.NotFound, "media not found")
    }
    if err != nil {
        log.Error("query media_records for thumbnail failed", "media_id", req.MediaId, "err", err)
        return nil, status.Error(codes.Internal, "query media record failed")
    }

    // 校验媒体类型（只有图片和视频支持缩略图）
    if mediaType != 1 && mediaType != 2 { // IMAGE=1, VIDEO=2
        return nil, status.Error(codes.FailedPrecondition, "thumbnail only available for image and video")
    }

    // ==================== 3. 生成缩略图 URL ====================
    var thumbnailURL string
    if thumbnailKey != "" {
        // 缩略图已生成 → 直接签名
        thumbnailURL, err = s.ossClient.PresignGetObject(ctx, bucket, thumbnailKey, 1*time.Hour, nil)
        if err != nil {
            log.Error("presign thumbnail failed", "media_id", req.MediaId, "err", err)
            return nil, status.Error(codes.Internal, "generate thumbnail url failed")
        }
    } else {
        // 缩略图未生成 → 使用对象存储的实时图片处理能力
        // MinIO 不支持实时处理，使用 OSS/S3 Image Processing 参数
        processParams := fmt.Sprintf("image/resize,m_fill,w_%d,h_%d,limit_0/format,webp", width, height)
        thumbnailURL, err = s.ossClient.PresignGetObjectWithProcess(ctx, bucket, storageKey,
            1*time.Hour, processParams)
        if err != nil {
            // 降级：返回原图 URL
            log.Warn("presign thumbnail with process failed, fallback to original",
                "media_id", req.MediaId, "err", err)
            thumbnailURL, _ = s.ossClient.PresignGetObject(ctx, bucket, storageKey, 1*time.Hour, nil)
        }
    }

    // ==================== 4. 返回 ====================
    return &pb.GetThumbnailUrlResp{
        Meta:         successMeta(ctx),
        ThumbnailUrl: thumbnailURL,
    }, nil
}
```

### 11. GetVideoSnapshot — 获取视频截图

> 获取视频指定时间点的截图 URL。
> 如果默认截图已生成（snapshot_key），直接返回；否则使用对象存储视频截帧能力。

```go
func (s *MediaService) GetVideoSnapshot(ctx context.Context, req *pb.GetVideoSnapshotReq) (*pb.GetVideoSnapshotResp, error) {
    // ==================== 1. 参数校验 ====================
    if req.MediaId == "" {
        return nil, status.Error(codes.InvalidArgument, "media_id is required")
    }
    timeOffset := int(req.TimeOffset)
    if timeOffset < 0 {
        return nil, status.Error(codes.InvalidArgument, "time_offset must be non-negative")
    }

    // ==================== 2. 查询媒体记录 — PgSQL ====================
    var bucket, storageKey, snapshotKey string
    var mediaType, processStatus, duration int
    err := s.db.QueryRowContext(ctx,
        `SELECT bucket, storage_key, snapshot_key, media_type, process_status, duration
         FROM media_records WHERE media_id = $1`,
        req.MediaId,
    ).Scan(&bucket, &storageKey, &snapshotKey, &mediaType, &processStatus, &duration)
    if err == sql.ErrNoRows {
        return nil, status.Error(codes.NotFound, "media not found")
    }
    if err != nil {
        log.Error("query media_records for snapshot failed", "media_id", req.MediaId, "err", err)
        return nil, status.Error(codes.Internal, "query media record failed")
    }

    // 校验媒体类型（只有视频支持截图）
    if mediaType != 2 { // VIDEO=2
        return nil, status.Error(codes.FailedPrecondition, "snapshot only available for video")
    }

    // 校验时间偏移（不能超过视频时长）
    if duration > 0 && timeOffset > duration {
        return nil, status.Errorf(codes.InvalidArgument,
            "time_offset %d exceeds video duration %d", timeOffset, duration)
    }

    // ==================== 3. 生成截图 URL ====================
    var snapshotURL string

    if timeOffset == 0 && snapshotKey != "" {
        // 请求默认截图（0s）且已生成 → 直接签名返回
        snapshotURL, err = s.ossClient.PresignGetObject(ctx, bucket, snapshotKey, 1*time.Hour, nil)
        if err != nil {
            log.Error("presign snapshot failed", "media_id", req.MediaId, "err", err)
            return nil, status.Error(codes.Internal, "generate snapshot url failed")
        }
    } else {
        // 指定时间截图 → 使用对象存储视频截帧能力
        // OSS: video/snapshot,t_{ms},f_jpg,w_0,h_0
        processParams := fmt.Sprintf("video/snapshot,t_%d,f_jpg,w_0,h_0,m_fast", timeOffset*1000)
        snapshotURL, err = s.ossClient.PresignGetObjectWithProcess(ctx, bucket, storageKey,
            1*time.Hour, processParams)
        if err != nil {
            log.Error("presign video snapshot with process failed",
                "media_id", req.MediaId, "offset", timeOffset, "err", err)
            // 降级：如果有默认截图返回默认截图
            if snapshotKey != "" {
                snapshotURL, _ = s.ossClient.PresignGetObject(ctx, bucket, snapshotKey, 1*time.Hour, nil)
            } else {
                return nil, status.Error(codes.Internal, "generate snapshot url failed")
            }
        }
    }

    // ==================== 4. 返回 ====================
    return &pb.GetVideoSnapshotResp{
        Meta:        successMeta(ctx),
        SnapshotUrl: snapshotURL,
    }, nil
}
```

### 12. GetMediaProcessStatus — 获取媒体处理状态

> 查询媒体的异步处理状态（转码、缩略图生成等进度）。
> 优先读 Redis 缓存，未命中回查 PgSQL。

```go
func (s *MediaService) GetMediaProcessStatus(ctx context.Context, req *pb.GetMediaProcessStatusReq) (*pb.GetMediaProcessStatusResp, error) {
    // ==================== 1. 参数校验 ====================
    if req.MediaId == "" {
        return nil, status.Error(codes.InvalidArgument, "media_id is required")
    }

    // ==================== 2. 查询 Redis 缓存 ====================
    statusKey := fmt.Sprintf("media:process_status:%s", req.MediaId)
    cachedJSON, err := s.redis.Get(ctx, statusKey).Result()
    if err == nil && cachedJSON != "" {
        var cached struct {
            Status     int    `json:"status"`
            Progress   int    `json:"progress"`
            FailReason string `json:"fail_reason"`
        }
        if json.Unmarshal([]byte(cachedJSON), &cached) == nil {
            return &pb.GetMediaProcessStatusResp{
                Meta:          successMeta(ctx),
                ProcessStatus: common.MediaProcessStatus(cached.Status),
                Progress:      int32(cached.Progress),
                FailReason:    cached.FailReason,
            }, nil
        }
    }

    // ==================== 3. 缓存未命中，回查 PgSQL ====================
    var processStatus int
    var extra []byte
    err = s.db.QueryRowContext(ctx,
        `SELECT process_status, extra FROM media_records WHERE media_id = $1`,
        req.MediaId,
    ).Scan(&processStatus, &extra)
    if err == sql.ErrNoRows {
        return nil, status.Error(codes.NotFound, "media not found")
    }
    if err != nil {
        log.Error("query media process status failed", "media_id", req.MediaId, "err", err)
        return nil, status.Error(codes.Internal, "query process status failed")
    }

    // 解析 extra 中的处理详情
    var extraInfo struct {
        Progress   int    `json:"progress"`
        FailReason string `json:"fail_reason"`
    }
    if len(extra) > 0 {
        json.Unmarshal(extra, &extraInfo)
    }

    // 根据 process_status 推断进度
    progress := extraInfo.Progress
    if processStatus == 2 { // COMPLETED
        progress = 100
    }

    // ==================== 4. 回填 Redis 缓存 ====================
    statusJSON, _ := json.Marshal(map[string]interface{}{
        "status":      processStatus,
        "progress":    progress,
        "fail_reason": extraInfo.FailReason,
    })
    s.redis.Set(ctx, statusKey, string(statusJSON), 1*time.Hour)

    // ==================== 5. 返回 ====================
    return &pb.GetMediaProcessStatusResp{
        Meta:          successMeta(ctx),
        ProcessStatus: common.MediaProcessStatus(processStatus),
        Progress:      int32(progress),
        FailReason:    extraInfo.FailReason,
    }, nil
}
```

### 13. DeleteMedia — 删除媒体（软删除）

> 软删除媒体记录，标记 process_status 为特殊删除状态，不立即删除对象存储文件。
> 投递 media.deleted 事件，由定时任务异步清理对象存储。

```go
func (s *MediaService) DeleteMedia(ctx context.Context, req *pb.DeleteMediaReq) (*pb.DeleteMediaResp, error) {
    // ==================== 1. 参数校验 ====================
    if req.MediaId == "" {
        return nil, status.Error(codes.InvalidArgument, "media_id is required")
    }
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id is required")
    }

    // ==================== 2. 查询媒体记录 — PgSQL ====================
    var record struct {
        UploaderID    string
        MediaType     int
        StorageKey    string
        Bucket        string
        ThumbnailKey  string
        SnapshotKey   string
        ProcessStatus int
    }
    err := s.db.QueryRowContext(ctx,
        `SELECT uploader_id, media_type, storage_key, bucket,
                thumbnail_key, snapshot_key, process_status
         FROM media_records WHERE media_id = $1`,
        req.MediaId,
    ).Scan(&record.UploaderID, &record.MediaType, &record.StorageKey, &record.Bucket,
        &record.ThumbnailKey, &record.SnapshotKey, &record.ProcessStatus)
    if err == sql.ErrNoRows {
        return nil, status.Error(codes.NotFound, "media not found")
    }
    if err != nil {
        log.Error("query media_records for delete failed", "media_id", req.MediaId, "err", err)
        return nil, status.Error(codes.Internal, "query media record failed")
    }

    // 权限校验：只有上传者可以删除（管理员通过 audit 事件删除）
    if record.UploaderID != req.UserId {
        return nil, status.Error(codes.PermissionDenied, "only uploader can delete media")
    }

    // 幂等：已删除的直接返回成功
    if record.ProcessStatus == -1 { // -1 表示已软删除
        return &pb.DeleteMediaResp{Meta: successMeta(ctx)}, nil
    }

    // ==================== 延迟双删 — DB 写入前失效缓存，500ms 后再次删除防止并发读回填旧数据 ====================
    s.delayedDoubleDelete(ctx,
        fmt.Sprintf("media:info:%s", req.MediaId),
        fmt.Sprintf("media:process_status:%s", req.MediaId),
        fmt.Sprintf("media:download_url:%s:%s", req.MediaId, req.UserId),
    )

    // ==================== 3. 软删除 — PgSQL ====================
    // 使用 process_status = -1 标记软删除，保留记录用于审计
    result, err := s.db.ExecContext(ctx,
        `UPDATE media_records
         SET process_status = -1,
             extra = jsonb_set(COALESCE(extra, '{}'), '{deleted_by}', to_jsonb($1::text)),
             updated_at = NOW()
         WHERE media_id = $2 AND process_status != -1`,
        req.UserId,  // $1: deleted_by
        req.MediaId, // $2: media_id
    )
    if err != nil {
        log.Error("soft delete media_records failed", "media_id", req.MediaId, "err", err)
        return nil, status.Error(codes.Internal, "delete media failed")
    }
    rowsAffected, _ := result.RowsAffected()
    if rowsAffected == 0 {
        // 并发删除场景，幂等返回成功
        return &pb.DeleteMediaResp{Meta: successMeta(ctx)}, nil
    }

    // ==================== 4. 投递 Kafka media.deleted 事件 ====================
    deleteEvent := &kafka_media.MediaDeletedEvent{
        Header:     buildEventHeader("media", s.instanceID),
        MediaId:    req.MediaId,
        MediaType:  kafka_media.MediaType(record.MediaType),
        OperatorId: req.UserId,
        Reason:     "user_delete",
        DeleteTime: time.Now().UnixMilli(),
    }
    if err := s.kafka.Produce(ctx, "media.deleted", req.MediaId, deleteEvent); err != nil {
        log.Error("produce media.deleted event failed",
            "media_id", req.MediaId, "err", err)
        // 不影响删除结果，定时任务会兜底清理
    }

    log.Info("media soft deleted",
        "media_id", req.MediaId, "user_id", req.UserId,
        "media_type", record.MediaType)

    // ==================== 6. 返回 ====================
    return &pb.DeleteMediaResp{Meta: successMeta(ctx)}, nil
}
```

---

## 辅助工具函数

```go
// delayedDoubleDelete 延迟双删工具函数
// Phase 1: 立即删除缓存（在 DB 写入前调用，防止旧缓存被读取）
// Phase 2: 延迟 500ms 后再次删除缓存（防止并发读在 DB 写入窗口期回填旧数据）
func (s *MediaService) delayedDoubleDelete(ctx context.Context, keys ...string) {
    if len(keys) == 0 {
        return
    }
    // Phase 1: 立即删除
    s.redis.Del(ctx, keys...)
    // Phase 2: 延迟 500ms 后再次删除
    go func(delKeys []string) {
        time.Sleep(500 * time.Millisecond)
        if _, err := s.redis.Del(context.Background(), delKeys...).Result(); err != nil {
            log.Warn("delayedDoubleDelete phase 2 failed", "keys", delKeys, "err", err)
        }
    }(keys)
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

// buildMediaInfo 从 PgSQL 构造完整 MediaInfo
func (s *MediaService) buildMediaInfo(ctx context.Context, mediaID string) (*common.MediaInfo, error) {
    var r struct {
        MediaID      string
        OriginalName string
        FileSize     int64
        MimeType     string
        StorageKey   string
        Bucket       string
        Width        int
        Height       int
        Duration     int
        ThumbnailKey string
    }
    err := s.db.QueryRowContext(ctx,
        `SELECT media_id, original_name, file_size, mime_type, storage_key,
                bucket, width, height, duration, thumbnail_key
         FROM media_records WHERE media_id = $1`,
        mediaID,
    ).Scan(&r.MediaID, &r.OriginalName, &r.FileSize, &r.MimeType, &r.StorageKey,
        &r.Bucket, &r.Width, &r.Height, &r.Duration, &r.ThumbnailKey)
    if err != nil {
        return nil, err
    }

    thumbnailURL := ""
    if r.ThumbnailKey != "" {
        thumbnailURL = s.ossClient.GetObjectURL(r.Bucket, r.ThumbnailKey)
    }

    return &common.MediaInfo{
        MediaId:      r.MediaID,
        Url:          s.ossClient.GetObjectURL(r.Bucket, r.StorageKey),
        ThumbnailUrl: thumbnailURL,
        FileName:     r.OriginalName,
        MimeType:     r.MimeType,
        FileSize:     r.FileSize,
        Width:        int32(r.Width),
        Height:       int32(r.Height),
        Duration:     int32(r.Duration),
    }, nil
}

// getBucket 根据上传类型选择 Bucket
func (s *MediaService) getBucket(uploadType common.UploadType) string {
    switch uploadType {
    case common.UPLOAD_TYPE_AVATAR, common.UPLOAD_TYPE_GROUP_AVATAR:
        return s.config.AvatarBucket // "im-avatar"
    case common.UPLOAD_TYPE_IMAGE:
        return s.config.ImageBucket  // "im-image"
    case common.UPLOAD_TYPE_VIDEO:
        return s.config.VideoBucket  // "im-video"
    case common.UPLOAD_TYPE_VOICE:
        return s.config.VoiceBucket  // "im-voice"
    case common.UPLOAD_TYPE_FILE:
        return s.config.FileBucket   // "im-file"
    default:
        return s.config.DefaultBucket // "im-default"
    }
}

// isAllowedMimeType 校验 MIME 类型白名单
func (s *MediaService) isAllowedMimeType(uploadType common.UploadType, mimeType string) bool {
    allowed := map[common.UploadType][]string{
        common.UPLOAD_TYPE_AVATAR:       {"image/jpeg", "image/png", "image/webp"},
        common.UPLOAD_TYPE_GROUP_AVATAR: {"image/jpeg", "image/png", "image/webp"},
        common.UPLOAD_TYPE_IMAGE:        {"image/jpeg", "image/png", "image/gif", "image/webp", "image/bmp"},
        common.UPLOAD_TYPE_VIDEO:        {"video/mp4", "video/quicktime", "video/x-msvideo", "video/webm"},
        common.UPLOAD_TYPE_VOICE:        {"audio/amr", "audio/ogg", "audio/opus", "audio/silk", "audio/mpeg", "audio/aac"},
        common.UPLOAD_TYPE_FILE:         {}, // 文件类型不限制 MIME
    }
    types, ok := allowed[uploadType]
    if !ok {
        return false
    }
    if len(types) == 0 {
        return true // 文件类型不限制
    }
    for _, t := range types {
        if t == mimeType {
            return true
        }
    }
    return false
}

// successMeta 构造成功响应元信息
func successMeta(ctx context.Context) *common.ResponseMeta {
    return &common.ResponseMeta{
        Code:       0,
        Message:    "success",
        ServerTime: time.Now().UnixMilli(),
        TraceId:    trace.SpanFromContext(ctx).SpanContext().TraceID().String(),
    }
}

// failMeta 构造失败响应元信息
func failMeta(ctx context.Context, msg string) *common.ResponseMeta {
    return &common.ResponseMeta{
        Code:       -1,
        Message:    msg,
        ServerTime: time.Now().UnixMilli(),
        TraceId:    trace.SpanFromContext(ctx).SpanContext().TraceID().String(),
    }
}
```

---

## 可观测性接入（OpenTelemetry）

> Media 服务处理文件上传/下载/转码，涉及外部存储（S3/MinIO），需重点观测：上传/下载延迟与吞吐、S3 API 调用延迟、转码任务耗时、文件大小分布、缩略图生成延迟。

### 第一步：服务启动时初始化 OTel SDK

```go
func main() {
    ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
    defer cancel()

    otelShutdown := observability.InitOpenTelemetry(ctx, "media", "v1.0.0")
    defer otelShutdown(ctx)
    observability.SetupLogger("media")
}
```

### 第二步：gRPC Server Interceptor（Media 不调用其他 RPC）

```go
grpcServer := grpc.NewServer(
    grpc.ChainUnaryInterceptor(otelgrpc.UnaryServerInterceptor()),
    grpc.ChainStreamInterceptor(otelgrpc.StreamServerInterceptor()), // UploadFile 是 stream
)
```

### 第三步：注册 Media 专属业务指标

```go
var meter = otel.Meter("im-chat/media")

var (
    // 文件上传指标
    uploadTotal, _ = meter.Int64Counter("media.upload_total",
        metric.WithDescription("文件上传总数"))
    uploadDuration, _ = meter.Float64Histogram("media.upload_duration_ms",
        metric.WithDescription("文件上传总耗时（含 S3 上传）"), metric.WithUnit("ms"))
    uploadBytes, _ = meter.Int64Counter("media.upload_bytes_total",
        metric.WithDescription("上传字节总数"), metric.WithUnit("By"))

    // S3 操作延迟
    s3PutLatency, _ = meter.Float64Histogram("media.s3_put_latency_ms",
        metric.WithDescription("S3 PutObject 延迟"), metric.WithUnit("ms"))
    s3GetLatency, _ = meter.Float64Histogram("media.s3_get_latency_ms",
        metric.WithDescription("S3 GetObject 延迟"), metric.WithUnit("ms"))

    // 转码任务
    transcodeTotal, _ = meter.Int64Counter("media.transcode_total",
        metric.WithDescription("转码任务总数"))
    transcodeDuration, _ = meter.Float64Histogram("media.transcode_duration_ms",
        metric.WithDescription("转码任务耗时"), metric.WithUnit("ms"))

    // 缩略图生成
    thumbnailDuration, _ = meter.Float64Histogram("media.thumbnail_duration_ms",
        metric.WithDescription("缩略图生成耗时"), metric.WithUnit("ms"))

    // 预签名 URL 生成
    presignGenerated, _ = meter.Int64Counter("media.presign_generated_total",
        metric.WithDescription("预签名 URL 生成次数"))
)
```

在业务代码中埋点：

```go
// UploadFile 中
uploadStart := time.Now()
// ... 读取 stream、写入临时文件、上传 S3 ...

s3Start := time.Now()
err := s.s3Client.PutObject(ctx, bucket, objectKey, reader, size, opts)
s3PutLatency.Record(ctx, float64(time.Since(s3Start).Milliseconds()))

uploadTotal.Add(ctx, 1, metric.WithAttributes(
    attribute.String("content_type", contentType),
))
uploadBytes.Add(ctx, fileSize)
uploadDuration.Record(ctx, float64(time.Since(uploadStart).Milliseconds()))

// 生成缩略图
thumbStart := time.Now()
// ... 缩略图逻辑 ...
thumbnailDuration.Record(ctx, float64(time.Since(thumbStart).Milliseconds()))

// 转码任务
transcodeStart := time.Now()
// ... ffmpeg 转码 ...
transcodeDuration.Record(ctx, float64(time.Since(transcodeStart).Milliseconds()))
transcodeTotal.Add(ctx, 1)
```

### 第四步：Kafka Producer 埋点 + 结构化 JSON 日志

发布 `media.uploaded` / `media.transcode.completed` 事件时注入 trace context。注意 Media 的日志应包含 `file_id`、`content_type`、`file_size` 字段。

### 接入要点总结

| 步骤 | 改动位置 | 效果 |
|------|---------|------|
| 1. `observability.InitOpenTelemetry` | main() | TracerProvider + MeterProvider + Runtime Metrics |
| 2. gRPC Server Interceptor | Server（含 Stream）| RPC 自动 trace |
| 3. 自定义业务指标 | Upload/Transcode/Presign | S3 延迟、上传吞吐、转码耗时 |
| 4. Kafka + 日志 | 事件发布 + setupLogger | 链路追踪 + 文件维度日志 |
| 5. buildEventHeader 改造 | 辅助函数 | EventHeader 携带真实 trace context |
| 6. 基础设施指标 | main() | DB/Redis 连接池可观测 |
| 7. Span 错误记录 | `observability.RecordError` | 错误 trace 在 Tempo 可见 |

### 补充：buildEventHeader 改造

将 `buildEventHeader(source, instanceID)` 替换为 `observability.BuildEventHeader(ctx, source, instanceID)`。

### 补充：基础设施指标注册

```go
observability.RegisterDBPoolMetrics(db, "media")
observability.RegisterRedisPoolMetrics(redisClient, "media")
```

### 补充：Span 错误记录

S3 调用、转码等高错误率操作尤其需要 span 错误记录：

```go
func (s *MediaServer) UploadFile(stream pb.MediaService_UploadFileServer) error {
    ctx, span := otel.Tracer("media").Start(stream.Context(), "UploadFile")
    defer span.End()

    result, err := s.doUploadFile(ctx, stream)
    if err != nil {
        observability.RecordError(span, err)
        return err
    }
    return stream.SendAndClose(result)
}
```
