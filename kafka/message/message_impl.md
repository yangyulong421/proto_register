# Message 消息服务 — Kafka 消费者/生产者实现伪代码

## 概述

Message 服务是 Kafka 事件系统的核心生产者，同时也消费来自 Media、Audit、User、Config 服务的事件。  
本文档详细描述 Message 服务的 **消费者实现**（处理外部事件）和 **生产者事件清单**（SendMessage/RecallMessage 等 RPC 方法产生的事件）。

## 生产 Topic 列表

| Topic | 事件 | 生产时机 | Key | 消费方 |
|-------|------|----------|-----|--------|
| `msg.stored.single` | MsgStoredEvent | SendMessage 存储单聊消息成功 | channel_id | Push（推送 sync notify）, Conversation（更新未读数）, Search（建索引）, OfflineQueue |
| `msg.stored.group` | MsgStoredEvent | SendMessage 存储群聊消息成功 | channel_id | Push（扇出推送）, Conversation（更新未读数）, Search（建索引）, OfflineQueue |
| `msg.recalled` | MsgRecalledEvent | RecallMessage 撤回成功 | channel_id | Push（通知客户端撤回）, Conversation（更新预览）, Search（删除索引）, OfflineQueue |
| `msg.deleted` | MsgDeletedEvent | DeleteMessage 删除成功 | user_id | Search（更新索引） |
| `msg.forwarded` | MsgForwardedEvent | ForwardMessage 转发成功 | original_msg_id | Audit（审计记录） |
| `msg.moderation.request` | MsgModerationRequestEvent | SendMessage 异步审核请求 | msg_id | Audit（内容审核） |
| `msg.search.index` | MsgSearchIndexEvent | 消息创建/撤回/删除时 | msg_id | Search（全文索引 CRUD） |
| `msg.conversation.update` | MsgConversationUpdateEvent | 新消息/撤回/删除时 | channel_id | Conversation（更新会话列表/最后消息/未读数） |

## 消费 Topic 列表

| Topic | 来源 | 用途 |
|-------|------|------|
| `media.process.result` | Media 服务 | 媒体处理结果 → 根据状态更新消息媒体信息或标记失败并通知发送者 |
| `audit.moderation.result` | Audit 服务 | 内容审核结果 → 根据结果处理消息（通过/删除/标记） |
| `user.deactivated` | User 服务 | 用户注销 → 标记该用户所有消息为 tombstone |
| `config.changed` | Config 服务 | 配置变更 → 热更新消息相关配置（文本长度限制/撤回时间等） |
| `relation.blocked` | Relation 服务 | 用户拉黑 → 更新本地拉黑关系缓存 msg:local:block:{a}:{b} |
| `relation.unblocked` | Relation 服务 | 解除拉黑 → 删除本地拉黑关系缓存 |
| `relation.friend.accepted` | Relation 服务 | 好友关系建立 → 设置本地好友缓存 msg:local:friend:{a}:{b} |
| `relation.friend.deleted` | Relation 服务 | 好友删除 → 删除本地好友缓存 |
| `group.info.updated` | Group 服务 | 群信息变更 → 更新本地群信息缓存 msg:local:group:{gid} |
| `group.dissolved` | Group 服务 | 群解散 → 删除本地群信息缓存 |
| `group.member.joined` | Group 服务 | 成员加入 → 设置本地群成员缓存 msg:local:group_member:{gid}:{uid} |
| `group.member.left` | Group 服务 | 成员退出 → 删除本地群成员缓存 |
| `group.member.kicked` | Group 服务 | 成员被踢 → 删除本地群成员缓存 |
| `group.member.mute.changed` | Group 服务 | 成员禁言状态变更 → 更新本地群成员缓存 mute 字段 |
| `group.member.role.changed` | Group 服务 | 成员角色变更 → 更新本地群成员缓存 role 字段 |
| `group.mute.changed` | Group 服务 | 群全员禁言状态变更 → 更新本地群信息缓存 mute 状态 |
| `user.profile.updated` | User 服务 | 用户资料变更 → 更新本地用户信息缓存 msg:local:user:{uid} |
| `user.settings.updated` | User 服务 | 用户设置变更 → 更新本地用户设置缓存 msg:local:user_settings:{uid} |

## Redis Key 设计（消费者专用）

| Key 格式 | 类型 | 值 | TTL | 说明 |
|----------|------|-----|-----|------|
| `msg:kafka:dedup:{event_id}` | STRING | "1" | 24h | Kafka 消费幂等去重 |
| `msg:media_status:{msg_id}` | STRING | processing / completed / failed | 1h | 消息媒体处理状态跟踪 |
| `msg:local:block:{user_id}:{target_id}` | STRING | "1" or missing | 24h | 本地拉黑关系缓存，relation.blocked/unblocked 同步 |
| `msg:local:friend:{user_id}:{target_id}` | STRING | "1" or missing | 24h | 本地好友关系缓存，relation.friend.accepted/deleted 同步 |
| `msg:local:group:{group_id}` | HASH | group info | 30min | 本地群信息缓存，group.info.updated/group.dissolved 同步 |
| `msg:local:group_member:{group_id}:{user_id}` | HASH | member info | 10min | 本地群成员缓存，group.member.* 同步 |
| `msg:local:user:{user_id}` | HASH | user info (nickname, avatar_url) | 30min | 本地用户信息缓存，user.profile.updated 同步 |
| `msg:local:user_settings:{user_id}` | HASH | {allow_stranger_msg} | 1h | 本地用户设置缓存（仅缓存 Message 服务实际使用的字段），user.settings.updated 同步 |

---

## 消费者实现

### Consumer: `media.process.result`

> 来源：Media 服务。媒体文件处理结果（完成或失败），根据 `status` 字段分支处理：  
> - **COMPLETED**：更新消息 media_url 和 media_info 为最终 URL  
> - **FAILED**：标记消息媒体状态为失败，通知发送者  
>  
> 合并说明：原 `media.process.completed` 和 `media.process.failed` 两个 Topic 的消费方完全一致（仅 Message 服务），  
> Key 均为 media_id，合并后减少 Topic 数量，降低 Kafka 消费组开销，简化消费端路由注册。

```go
func (c *MessageConsumer) HandleMediaProcessResult(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_msg.MediaProcessResultForMsg
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("unmarshal MediaProcessResultForMsg failed", "err", err)
        return nil // 反序列化失败不重试，记录日志人工排查
    }

    // ==================== 2. 幂等检查 ====================
    dedupKey := fmt.Sprintf("msg:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        log.Debug("duplicate media.process.result event", "event_id", event.Header.EventId)
        return nil
    }

    // ==================== 3. 参数校验 ====================
    if event.MsgId == "" {
        log.Error("media.process.result: msg_id is empty", "event_id", event.Header.EventId)
        return nil
    }

    // ==================== 4. 根据处理状态分支处理 ====================
    switch event.Status {
    case common.MEDIA_PROCESS_STATUS_COMPLETED:
        return c.handleMediaCompleted(ctx, &event)
    case common.MEDIA_PROCESS_STATUS_FAILED:
        return c.handleMediaFailed(ctx, &event)
    default:
        log.Error("media.process.result: unknown status",
            "msg_id", event.MsgId, "status", event.Status)
        return nil
    }
}

// handleMediaCompleted 处理媒体完成：更新消息 media_url 和 media_info
func (c *MessageConsumer) handleMediaCompleted(ctx context.Context, event *kafka_msg.MediaProcessResultForMsg) error {
    if event.Url == "" {
        log.Error("media.process.result (completed): url is empty", "msg_id", event.MsgId)
        return nil
    }

    // ---- 4a. 更新消息 media_url 和 media_info — PgSQL ----
    mediaInfo := map[string]interface{}{
        "media_id":      event.MediaId,
        "url":           event.Url,
        "thumbnail_url": event.ThumbnailUrl,
        "width":         event.Width,
        "height":        event.Height,
        "duration":      event.Duration,
        "status":        int(event.Status),
    }
    mediaInfoJSON, err := json.Marshal(mediaInfo)
    if err != nil {
        log.Error("marshal media_info failed", "msg_id", event.MsgId, "err", err)
        return fmt.Errorf("marshal media_info failed: %w", err)
    }

    result, err := c.db.ExecContext(ctx,
        `UPDATE messages
         SET media_url = $1, media_info = $2, updated_at = NOW()
         WHERE msg_id = $3 AND status = 1`,
        event.Url, mediaInfoJSON, event.MsgId,
    )
    if err != nil {
        log.Error("update message media_url failed",
            "msg_id", event.MsgId, "url", event.Url, "err", err)
        return fmt.Errorf("update message media_url failed: %w", err)
    }
    rowsAffected, _ := result.RowsAffected()
    if rowsAffected == 0 {
        log.Warn("media.process.result (completed): message not found or not normal status",
            "msg_id", event.MsgId)
        return nil
    }

    // ---- 4b. 更新 Redis 缓存 ----
    detailKey := fmt.Sprintf("msg:detail:%s", event.MsgId)
    cached, err := c.redis.Get(ctx, detailKey).Result()
    if err == nil && cached != "" {
        var chatMsg map[string]interface{}
        if json.Unmarshal([]byte(cached), &chatMsg) == nil {
            if body, ok := chatMsg["body"].(map[string]interface{}); ok {
                body["media"] = mediaInfo
            }
            chatMsg["body"].(map[string]interface{})["media"] = mediaInfo
            updatedJSON, _ := json.Marshal(chatMsg)
            c.redis.Set(ctx, detailKey, string(updatedJSON), 1*time.Hour)
        }
    } else {
        c.redis.Del(ctx, detailKey)
    }

    c.redis.Set(ctx, fmt.Sprintf("msg:media_status:%s", event.MsgId), "completed", 1*time.Hour)

    // ---- 4c. 投递搜索索引更新事件 ----
    searchEvent := &kafka_msg.MsgSearchIndexEvent{
        Header: buildEventHeader("message", c.instanceID),
        Action: "update",
        MsgId:  event.MsgId,
    }
    c.kafka.Produce(ctx, "msg.search.index", event.MsgId, searchEvent)

    log.Info("media process completed, message updated",
        "msg_id", event.MsgId, "media_id", event.MediaId, "url", event.Url)
    return nil
}

// handleMediaFailed 处理媒体失败：标记状态为失败，通知发送者
func (c *MessageConsumer) handleMediaFailed(ctx context.Context, event *kafka_msg.MediaProcessResultForMsg) error {
    // ---- 4a. 查询原始消息获取发送者信息 — PgSQL ----
    var senderID, channelID string
    var channelType int
    err := c.db.QueryRowContext(ctx,
        `SELECT sender_id, channel_id, channel_type FROM messages WHERE msg_id = $1`,
        event.MsgId,
    ).Scan(&senderID, &channelID, &channelType)
    if err == sql.ErrNoRows {
        log.Warn("media.process.result (failed): message not found", "msg_id", event.MsgId)
        return nil
    }
    if err != nil {
        log.Error("query message for media failed event failed",
            "msg_id", event.MsgId, "err", err)
        return fmt.Errorf("query message failed: %w", err)
    }

    // ---- 4b. 更新消息 media_info 标记失败 — PgSQL ----
    failedMediaInfo := map[string]interface{}{
        "media_id":    event.MediaId,
        "status":      "failed",
        "error":       event.ErrorMsg,
        "retry_count": event.RetryCount,
    }
    failedJSON, _ := json.Marshal(failedMediaInfo)

    _, err = c.db.ExecContext(ctx,
        `UPDATE messages
         SET media_info = $1, updated_at = NOW()
         WHERE msg_id = $2`,
        failedJSON, event.MsgId,
    )
    if err != nil {
        log.Error("update message media_info (failed) failed",
            "msg_id", event.MsgId, "err", err)
        return fmt.Errorf("update message media_info failed: %w", err)
    }

    // ---- 4c. 清理 Redis 缓存 ----
    pipe := c.redis.Pipeline()
    pipe.Del(ctx, fmt.Sprintf("msg:detail:%s", event.MsgId))
    pipe.Set(ctx, fmt.Sprintf("msg:media_status:%s", event.MsgId), "failed", 1*time.Hour)
    pipe.Exec(ctx)

    // ---- 4d. 通知发送者媒体处理失败 ----
    notifyEvent := &kafka_msg.MsgConversationUpdateEvent{
        Header:           buildEventHeader("message", c.instanceID),
        ConversationId:   channelID,
        ConversationType: common.ConversationType(channelType),
        MsgId:            event.MsgId,
        ContentPreview:   "[媒体处理失败]",
        MessageType:      common.MESSAGE_TYPE_SYSTEM,
        SenderId:         "system",
        Seq:              0,
        SendTime:         time.Now().UnixMilli(),
        AffectedUserIds:  []string{senderID},
        UpdateType:       "media_failed",
    }
    if err := c.kafka.Produce(ctx, "msg.conversation.update", channelID, notifyEvent); err != nil {
        log.Error("produce media failed notification failed",
            "msg_id", event.MsgId, "sender_id", senderID, "err", err)
    }

    // ---- 4e. 判断是否需要重试 ----
    if event.RetryCount < 3 {
        log.Warn("media process failed, may retry",
            "msg_id", event.MsgId, "media_id", event.MediaId,
            "retry_count", event.RetryCount, "error", event.ErrorMsg)
    } else {
        log.Error("media process permanently failed after max retries",
            "msg_id", event.MsgId, "media_id", event.MediaId,
            "retry_count", event.RetryCount, "error", event.ErrorMsg)
    }

    return nil
}
```

### Consumer: `audit.moderation.result`

> 来源：Audit 审核服务。对消息内容进行审核后返回结果（pass/review/reject）。  
> 审核流程：SendMessage → 投递 msg.moderation.request → Audit 服务异步审核 → 返回审核结果。  
> 处置策略：pass=放行不做处理，review=人工复审标记，reject=删除消息+通知发送者。

```go
func (c *MessageConsumer) HandleAuditModerationResult(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_msg.AuditModerationResultForMsg
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("unmarshal AuditModerationResultForMsg failed", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 ====================
    dedupKey := fmt.Sprintf("msg:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    if event.MsgId == "" {
        log.Error("audit.moderation.result: msg_id is empty")
        return nil
    }

    log.Info("received moderation result",
        "msg_id", event.MsgId, "result", event.Result,
        "action", event.Action, "confidence", event.Confidence,
        "labels", event.Labels)

    // ==================== 3. 查询原始消息 — PgSQL ====================
    var originalMsg struct {
        MsgID       string
        ChannelID   string
        ChannelType int
        SenderID    string
        Status      int
        Seq         int64
    }
    err := c.db.QueryRowContext(ctx,
        `SELECT msg_id, channel_id, channel_type, sender_id, status, seq
         FROM messages WHERE msg_id = $1`,
        event.MsgId,
    ).Scan(&originalMsg.MsgID, &originalMsg.ChannelID, &originalMsg.ChannelType,
        &originalMsg.SenderID, &originalMsg.Status, &originalMsg.Seq)
    if err == sql.ErrNoRows {
        log.Warn("audit.moderation.result: message not found", "msg_id", event.MsgId)
        return nil
    }
    if err != nil {
        return fmt.Errorf("query message for moderation result failed: %w", err)
    }

    // 消息已被撤回/删除则忽略审核结果
    if originalMsg.Status != 1 { // 非 NORMAL 状态
        log.Info("message already not normal, skip moderation",
            "msg_id", event.MsgId, "current_status", originalMsg.Status)
        return nil
    }

    now := time.Now().UnixMilli()

    // ==================== 4. 根据审核结果处理 ====================
    switch event.Result {
    case "pass":
        // 审核通过，不做任何处理
        log.Info("message moderation passed", "msg_id", event.MsgId)
        return nil

    case "review":
        // 需要人工复审 → 标记消息状态为 MODERATED(3)
        _, err := c.db.ExecContext(ctx,
            `UPDATE messages SET status = 3, updated_at = NOW() WHERE msg_id = $1 AND status = 1`,
            event.MsgId,
        )
        if err != nil {
            return fmt.Errorf("update message to moderated status failed: %w", err)
        }

        // 清理 Redis 缓存
        c.redis.Del(ctx, fmt.Sprintf("msg:detail:%s", event.MsgId))

        log.Info("message flagged for manual review",
            "msg_id", event.MsgId, "labels", event.Labels, "confidence", event.Confidence)
        return nil

    case "reject":
        // 审核拒绝 → 执行处置动作
        switch event.Action {
        case "delete":
            // ---- 4a. 删除消息（标记状态为 DELETED） ----
            _, err := c.db.ExecContext(ctx,
                `UPDATE messages SET status = 4,
                    content = '{"type":10,"text":"该消息因违规已被删除"}',
                    media_url = '', media_info = '{}', updated_at = NOW()
                 WHERE msg_id = $1 AND status = 1`,
                event.MsgId,
            )
            if err != nil {
                return fmt.Errorf("delete moderated message failed: %w", err)
            }

            // 清理 Redis 缓存
            pipe := c.redis.Pipeline()
            pipe.Del(ctx, fmt.Sprintf("msg:detail:%s", event.MsgId))
            recentKey := fmt.Sprintf("msg:recent:%s", originalMsg.ChannelID)
            pipe.ZRem(ctx, recentKey, event.MsgId)
            pipe.Exec(ctx)

            // 投递撤回事件（通知客户端移除该消息）
            recallEvent := &kafka_msg.MsgRecalledEvent{
                Header:           buildEventHeader("message", c.instanceID),
                MsgId:            event.MsgId,
                ConversationId:   originalMsg.ChannelID,
                ConversationType: common.ConversationType(originalMsg.ChannelType),
                SenderId:         "system_moderation",
                OriginalSeq:      originalMsg.Seq,
                RecallTime:       now,
            }
            c.kafka.Produce(ctx, "msg.recalled", originalMsg.ChannelID, recallEvent)

            // 更新搜索索引
            searchEvent := &kafka_msg.MsgSearchIndexEvent{
                Header:           buildEventHeader("message", c.instanceID),
                Action:           "delete",
                MsgId:            event.MsgId,
                ConversationId:   originalMsg.ChannelID,
                ConversationType: common.ConversationType(originalMsg.ChannelType),
            }
            c.kafka.Produce(ctx, "msg.search.index", event.MsgId, searchEvent)

            log.Warn("message deleted by moderation",
                "msg_id", event.MsgId, "sender_id", originalMsg.SenderID,
                "labels", event.Labels, "reason", event.Reason)

        case "mute":
            // ---- 4b. 删除消息 + 禁言用户（如果是群聊） ----
            // 先执行删除
            c.db.ExecContext(ctx,
                `UPDATE messages SET status = 4,
                    content = '{"type":10,"text":"该消息因违规已被删除"}',
                    media_url = '', media_info = '{}', updated_at = NOW()
                 WHERE msg_id = $1 AND status = 1`,
                event.MsgId,
            )

            // 清理缓存
            c.redis.Del(ctx, fmt.Sprintf("msg:detail:%s", event.MsgId))

            // 投递撤回事件
            recallEvent := &kafka_msg.MsgRecalledEvent{
                Header:           buildEventHeader("message", c.instanceID),
                MsgId:            event.MsgId,
                ConversationId:   originalMsg.ChannelID,
                ConversationType: common.ConversationType(originalMsg.ChannelType),
                SenderId:         "system_moderation",
                OriginalSeq:      originalMsg.Seq,
                RecallTime:       now,
            }
            c.kafka.Produce(ctx, "msg.recalled", originalMsg.ChannelID, recallEvent)

            log.Warn("message deleted and user may be muted by moderation",
                "msg_id", event.MsgId, "sender_id", originalMsg.SenderID,
                "action", event.Action, "labels", event.Labels)

        default:
            // pass 作为默认兜底
            log.Info("moderation reject with unknown action, treating as pass",
                "msg_id", event.MsgId, "action", event.Action)
        }

        // ---- 通知发送者审核结果（无论具体 action） ----
        // 通过系统消息通知发送者
        notifyEvent := &kafka_msg.MsgConversationUpdateEvent{
            Header:           buildEventHeader("message", c.instanceID),
            ConversationId:   originalMsg.ChannelID,
            ConversationType: common.ConversationType(originalMsg.ChannelType),
            MsgId:            event.MsgId,
            ContentPreview:   "[消息因违规已被处理]",
            MessageType:      common.MESSAGE_TYPE_SYSTEM,
            SenderId:         "system",
            Seq:              originalMsg.Seq,
            SendTime:         now,
            AffectedUserIds:  []string{originalMsg.SenderID},
            UpdateType:       "moderation",
        }
        c.kafka.Produce(ctx, "msg.conversation.update", originalMsg.ChannelID, notifyEvent)

        return nil

    default:
        log.Error("unknown moderation result", "msg_id", event.MsgId, "result", event.Result)
        return nil
    }
}
```

### Consumer: `user.deactivated`

> 来源：User 服务。用户注销账户后，需要对该用户的所有消息进行 tombstone 处理。  
> 策略：不物理删除消息（避免影响其他用户的聊天记录完整性），而是将发送者信息脱敏。

```go
func (c *MessageConsumer) HandleUserDeactivated(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_user.UserDeactivatedEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("unmarshal UserDeactivatedEvent failed", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 ====================
    dedupKey := fmt.Sprintf("msg:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    if event.UserId == "" {
        log.Error("user.deactivated: user_id is empty")
        return nil
    }

    log.Info("processing user deactivation for messages", "user_id", event.UserId)

    // ==================== 3. 批量更新该用户发送的消息 — PgSQL ====================
    // 不删除消息，而是将消息内容替换为 tombstone 提示
    // 使用分批处理避免长事务，每批处理 1000 条
    batchSize := 1000
    totalUpdated := int64(0)

    for {
        result, err := c.db.ExecContext(ctx,
            `UPDATE messages
             SET content = '{"type":10,"text":"该用户已注销"}',
                 media_url = '', media_info = '{}',
                 status = 4, updated_at = NOW()
             WHERE msg_id IN (
                 SELECT msg_id FROM messages
                 WHERE sender_id = $1 AND status = 1
                 LIMIT $2
             )`,
            event.UserId, batchSize,
        )
        if err != nil {
            log.Error("batch update deactivated user messages failed",
                "user_id", event.UserId, "err", err)
            return fmt.Errorf("batch update messages failed: %w", err)
        }

        affected, _ := result.RowsAffected()
        totalUpdated += affected

        if affected < int64(batchSize) {
            break // 最后一批，处理完毕
        }

        // 避免长时间占用资源，每批之间休眠
        time.Sleep(100 * time.Millisecond)
    }

    // ==================== 4. 清理该用户的 Redis 缓存 ====================
    // 扫描并删除该用户的消息去重缓存和详情缓存
    // 由于消息缓存 key 包含 msg_id 而非 user_id，无法精准清理
    // 采用惰性策略：缓存自然过期（TTL 最长 24h），查询时 DB 已更新
    log.Info("user deactivation message cleanup note: Redis cache will expire naturally",
        "user_id", event.UserId)

    // ==================== 5. 批量更新搜索索引 ====================
    // 查询该用户的所有消息 ID（分批）
    offset := 0
    for {
        rows, err := c.db.QueryContext(ctx,
            `SELECT msg_id, channel_id, channel_type FROM messages
             WHERE sender_id = $1
             ORDER BY id ASC
             LIMIT $2 OFFSET $3`,
            event.UserId, batchSize, offset,
        )
        if err != nil {
            log.Error("query deactivated user msg_ids failed",
                "user_id", event.UserId, "offset", offset, "err", err)
            break
        }

        count := 0
        for rows.Next() {
            var msgID, channelID string
            var channelType int
            if rows.Scan(&msgID, &channelID, &channelType) != nil {
                continue
            }

            searchEvent := &kafka_msg.MsgSearchIndexEvent{
                Header:           buildEventHeader("message", c.instanceID),
                Action:           "delete",
                MsgId:            msgID,
                ConversationId:   channelID,
                ConversationType: common.ConversationType(channelType),
                SenderId:         event.UserId,
            }
            c.kafka.Produce(ctx, "msg.search.index", msgID, searchEvent)
            count++
        }
        rows.Close()

        if count < batchSize {
            break
        }
        offset += batchSize
        time.Sleep(50 * time.Millisecond) // 避免 Kafka 生产压力过大
    }

    log.Info("user deactivation message processing completed",
        "user_id", event.UserId, "total_messages_updated", totalUpdated)
    return nil
}
```

### Consumer: `config.changed`

> 来源：Config 服务。消息相关配置变更时热更新，无需重启服务。  
> 支持的配置项：文本最大长度、撤回时间限制、内容审核开关、敏感词过滤开关等。

```go
func (c *MessageConsumer) HandleConfigChanged(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_config.ConfigChangedEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("unmarshal ConfigChangedEvent failed", "err", err)
        return nil
    }

    // ==================== 2. 筛选与 Message 服务相关的配置 ====================
    // 只处理 message.* 前缀的配置项
    if !strings.HasPrefix(event.ConfigKey, "message.") {
        return nil // 不是 Message 服务的配置，忽略
    }

    log.Info("config changed for message service",
        "config_key", event.ConfigKey,
        "old_value", event.OldValue,
        "new_value", event.NewValue)

    // ==================== 3. 根据配置项更新内存配置 ====================
    switch event.ConfigKey {
    case "message.max_text_length":
        // 文本消息最大字符数
        val, err := strconv.Atoi(event.NewValue)
        if err != nil || val <= 0 {
            log.Error("invalid message.max_text_length value", "value", event.NewValue)
            return nil
        }
        c.config.Store("MaxTextLength", val)
        log.Info("updated MaxTextLength", "old", event.OldValue, "new", val)

    case "message.recall_time_limit_seconds":
        // 撤回时间限制（秒）
        val, err := strconv.Atoi(event.NewValue)
        if err != nil || val <= 0 {
            log.Error("invalid message.recall_time_limit_seconds value", "value", event.NewValue)
            return nil
        }
        c.config.Store("RecallTimeLimitSeconds", val)
        log.Info("updated RecallTimeLimitSeconds", "old", event.OldValue, "new", val)

    case "message.moderation_enabled":
        // 内容审核开关
        val := strings.ToLower(event.NewValue) == "true"
        c.config.Store("ModerationEnabled", val)
        log.Info("updated ModerationEnabled", "old", event.OldValue, "new", val)

    case "message.sensitive_filter_enabled":
        // 敏感词过滤开关
        val := strings.ToLower(event.NewValue) == "true"
        c.config.Store("SensitiveFilterEnabled", val)
        log.Info("updated SensitiveFilterEnabled", "old", event.OldValue, "new", val)

    case "message.max_forward_targets":
        // 单次转发最大目标数
        val, err := strconv.Atoi(event.NewValue)
        if err != nil || val <= 0 {
            log.Error("invalid message.max_forward_targets value", "value", event.NewValue)
            return nil
        }
        c.config.Store("MaxForwardTargets", val)
        log.Info("updated MaxForwardTargets", "old", event.OldValue, "new", val)

    case "message.max_merge_messages":
        // 合并转发最大消息数
        val, err := strconv.Atoi(event.NewValue)
        if err != nil || val <= 0 {
            log.Error("invalid message.max_merge_messages value", "value", event.NewValue)
            return nil
        }
        c.config.Store("MaxMergeMessages", val)
        log.Info("updated MaxMergeMessages", "old", event.OldValue, "new", val)

    case "message.batch_send_max_receivers":
        // 批量发送最大接收者数
        val, err := strconv.Atoi(event.NewValue)
        if err != nil || val <= 0 {
            log.Error("invalid message.batch_send_max_receivers value", "value", event.NewValue)
            return nil
        }
        c.config.Store("BatchSendMaxReceivers", val)
        log.Info("updated BatchSendMaxReceivers", "old", event.OldValue, "new", val)

    case "message.dedup_ttl_hours":
        // 幂等去重 TTL（小时）
        val, err := strconv.Atoi(event.NewValue)
        if err != nil || val <= 0 {
            log.Error("invalid message.dedup_ttl_hours value", "value", event.NewValue)
            return nil
        }
        c.config.Store("DedupTTLHours", val)
        log.Info("updated DedupTTLHours", "old", event.OldValue, "new", val)

    case "message.recent_cache_max_size":
        // 近期消息 ZSET 最大数量
        val, err := strconv.Atoi(event.NewValue)
        if err != nil || val <= 0 {
            log.Error("invalid message.recent_cache_max_size value", "value", event.NewValue)
            return nil
        }
        c.config.Store("RecentCacheMaxSize", val)
        log.Info("updated RecentCacheMaxSize", "old", event.OldValue, "new", val)

    default:
        log.Debug("unknown message config key, ignored", "config_key", event.ConfigKey)
    }

    return nil
}
```

---

## 生产者事件详细说明

### 生产者: SendMessage 产生的事件

> SendMessage 是事件生产最多的方法，成功发送一条消息后会产生以下事件：

```go
// ==================== SendMessage 产生的事件流 ====================

// 1. msg.stored.single / msg.stored.group（必须，核心事件）
// - 触发时机：消息成功写入 PgSQL 后
// - Key: channel_id（保证同一会话的消息有序消费）
// - 消费方：Push 服务（发送 sync notify）, Conversation（更新未读数）, Search（建索引）, OfflineQueue（离线推送）
// - 重要性：⭐⭐⭐⭐⭐ 如果此事件丢失，接收方将无法收到消息推送
//
// 事件示例：
storedEvent := &kafka_msg.MsgStoredEvent{
    Header:            buildEventHeader("message", s.instanceID),
    MsgId:             "1234567890123456789",       // Snowflake 生成
    ClientMsgId:       "uuid-v4-from-client",        // 客户端幂等 ID
    ConversationId:    "conv_9876543210",             // 会话通道 ID
    ConversationType:  common.CONVERSATION_TYPE_SINGLE,
    SenderId:          "user_001",
    ReceiverId:        "user_002",
    MsgType:           common.MESSAGE_TYPE_TEXT,
    ContentPreview:    "你好，今天天气...",
    Status:            common.MESSAGE_STATUS_SENT,
    Seq:               42,                            // 由 Conversation 分配的序号
    SendTime:          1709856000000,                  // 客户端发送时间
    ServerTime:        1709856000123,                  // 服务端处理时间
    SenderNickname:    "张三",
    SenderAvatar:      "https://cdn.example.com/avatar/001.jpg",
    HasAt:             false,
    AtUserIds:         nil,
    HasAtAll:          false,
    Extra:             "{}",
    GroupId:           "",                             // 单聊为空
    GroupMemberCount:  0,                              // 单聊为 0
}

// 2. msg.conversation.update（必须，通知 Conversation 更新会话列表）
// - 触发时机：消息成功写入 PgSQL 后
// - Key: channel_id
// - 消费方：Conversation 服务（更新 user_conversations 的未读数、最后消息预览、排序）
//
// 事件示例：
convUpdateEvent := &kafka_msg.MsgConversationUpdateEvent{
    Header:           buildEventHeader("message", s.instanceID),
    ConversationId:   "conv_9876543210",
    ConversationType: common.CONVERSATION_TYPE_SINGLE,
    MsgId:            "1234567890123456789",
    ContentPreview:   "你好，今天天气...",
    MessageType:      common.MESSAGE_TYPE_TEXT,
    SenderId:         "user_001",
    SenderNickname:   "张三",
    Seq:              42,
    SendTime:         1709856000123,
    AffectedUserIds:  []string{"user_001", "user_002"}, // 单聊双方都需更新
    UpdateType:       "new_msg",
}

// 3. msg.moderation.request（条件性，需要审核的消息才产生）
// - 触发时机：消息存储后，文本/图片/视频/文件类型触发
// - Key: msg_id
// - 消费方：Audit 服务（异步内容审核）
// - 不阻塞消息发送流程，审核结果通过 audit.moderation.result 回调

// 4. msg.search.index（必须，搜索索引）
// - 触发时机：消息存储后
// - Key: msg_id
// - 消费方：Search 服务（建立全文搜索索引）
```

### 生产者: RecallMessage 产生的事件

```go
// ==================== RecallMessage 产生的事件流 ====================

// 1. msg.recalled（必须）
// - 消费方：Push（通知客户端撤回展示）, Conversation（更新预览为"[消息已撤回]"）, OfflineQueue（清除未推送的消息）
recallEvent := &kafka_msg.MsgRecalledEvent{
    Header:           buildEventHeader("message", s.instanceID),
    MsgId:            "1234567890123456789",
    ConversationId:   "conv_9876543210",
    ConversationType: common.CONVERSATION_TYPE_SINGLE,
    SenderId:         "user_001",               // 撤回操作者
    ReceiverId:       "user_002",
    OriginalSeq:      42,                        // 被撤回消息的 seq
    RecallTime:       1709856120000,
    GroupId:          "",
    NotifyUserIds:    []string{"user_002"},       // 需要通知的用户
}

// 2. msg.conversation.update（必须，更新会话预览）
// - update_type = "recall"

// 3. msg.search.index（必须，action = "delete"）
// - 删除搜索索引中的该消息
```

### 生产者: DeleteMessage 产生的事件

```go
// ==================== DeleteMessage 产生的事件流 ====================

// 1. msg.deleted
// - 仅用于 Search 服务更新索引
// - 不产生 Push 通知（删除仅对自己不可见）
deleteEvent := &kafka_msg.MsgDeletedEvent{
    Header:         buildEventHeader("message", s.instanceID),
    UserId:         "user_001",                      // 删除操作者
    MsgIds:         []string{"msg_001", "msg_002"},  // 被删除的消息 ID 列表
    ConversationId: "conv_9876543210",
    DeleteTime:     1709856300000,
}

// 2. msg.search.index（每条被删消息一个事件，action = "update"）
```

### 生产者: ForwardMessage 产生的事件

```go
// ==================== ForwardMessage 产生的事件流 ====================

// 对每个目标会话：
// 1. msg.stored.single / msg.stored.group（与 SendMessage 相同）
// 2. msg.conversation.update（与 SendMessage 相同）

// 额外产生：
// 3. msg.forwarded（审计记录）
forwardEvent := &kafka_msg.MsgForwardedEvent{
    Header:        buildEventHeader("message", s.instanceID),
    SenderId:      "user_001",
    OriginalMsgId: "original_msg_123",
    Targets: []*kafka_msg.ForwardTarget{
        {
            TargetId:         "user_003",
            ConversationType: common.CONVERSATION_TYPE_SINGLE,
            NewMsgId:         "new_msg_456",
            ConversationId:   "conv_new_001",
        },
    },
    ForwardTime: 1709856400000,
}
```

---

## 消费者基础设施

```go
// MessageConsumer 消息服务 Kafka 消费者
type MessageConsumer struct {
    db         *sql.DB
    redis      *redis.Client
    kafka      *kafka.Producer
    config     *sync.Map        // 热更新配置
    instanceID string
}

// NewMessageConsumer 创建消息消费者实例
func NewMessageConsumer(db *sql.DB, redis *redis.Client, kafkaProducer *kafka.Producer) *MessageConsumer {
    c := &MessageConsumer{
        db:         db,
        redis:      redis,
        kafka:      kafkaProducer,
        config:     &sync.Map{},
        instanceID: os.Getenv("INSTANCE_ID"),
    }

    // 加载默认配置
    c.config.Store("MaxTextLength", 5000)
    c.config.Store("RecallTimeLimitSeconds", 120)
    c.config.Store("ModerationEnabled", true)
    c.config.Store("SensitiveFilterEnabled", true)
    c.config.Store("MaxForwardTargets", 20)
    c.config.Store("MaxMergeMessages", 100)
    c.config.Store("BatchSendMaxReceivers", 500)
    c.config.Store("DedupTTLHours", 24)
    c.config.Store("RecentCacheMaxSize", 200)

    return c
}

// RegisterConsumers 注册所有消费者到 Kafka Consumer Group
func (c *MessageConsumer) RegisterConsumers(router *kafka.ConsumerRouter) {
    // 媒体处理结果（合并 completed + failed）
    router.Handle("media.process.result", c.HandleMediaProcessResult,
        kafka.WithRetry(3),              // 最多重试 3 次
        kafka.WithRetryBackoff(time.Second), // 重试间隔 1s
        kafka.WithConcurrency(4),        // 4 个并发消费者
    )

    // 审核结果
    router.Handle("audit.moderation.result", c.HandleAuditModerationResult,
        kafka.WithRetry(3),
        kafka.WithRetryBackoff(2*time.Second),
        kafka.WithConcurrency(4),
    )

    // 用户注销
    router.Handle("user.deactivated", c.HandleUserDeactivated,
        kafka.WithRetry(5),              // 重操作允许更多重试
        kafka.WithRetryBackoff(5*time.Second),
        kafka.WithConcurrency(1),        // 单线程处理，避免并发冲突
    )

    // 配置变更
    router.Handle("config.changed", c.HandleConfigChanged,
        kafka.WithRetry(1),
        kafka.WithConcurrency(1),
    )

    // ==================== 本地缓存维护消费者 ====================

    // 拉黑关系 → 更新本地 block 缓存
    router.Handle("relation.blocked", c.HandleRelationBlocked,
        kafka.WithRetry(3),
        kafka.WithRetryBackoff(time.Second),
        kafka.WithConcurrency(2),
    )

    // 解除拉黑 → 删除本地 block 缓存
    router.Handle("relation.unblocked", c.HandleRelationUnblocked,
        kafka.WithRetry(3),
        kafka.WithRetryBackoff(time.Second),
        kafka.WithConcurrency(2),
    )

    // 好友关系建立 → 设置本地 friend 缓存
    router.Handle("relation.friend.accepted", c.HandleRelationFriendAccepted,
        kafka.WithRetry(3),
        kafka.WithRetryBackoff(time.Second),
        kafka.WithConcurrency(2),
    )

    // 好友删除 → 删除本地 friend 缓存
    router.Handle("relation.friend.deleted", c.HandleRelationFriendDeleted,
        kafka.WithRetry(3),
        kafka.WithRetryBackoff(time.Second),
        kafka.WithConcurrency(2),
    )

    // 群信息更新 → 更新本地 group 缓存
    router.Handle("group.info.updated", c.HandleGroupInfoUpdated,
        kafka.WithRetry(3),
        kafka.WithRetryBackoff(time.Second),
        kafka.WithConcurrency(2),
    )

    // 群解散 → 删除本地 group 缓存
    router.Handle("group.dissolved", c.HandleGroupDissolved,
        kafka.WithRetry(3),
        kafka.WithRetryBackoff(time.Second),
        kafka.WithConcurrency(2),
    )

    // 群成员加入 → 设置本地 group_member 缓存
    router.Handle("group.member.joined", c.HandleGroupMemberJoined,
        kafka.WithRetry(3),
        kafka.WithRetryBackoff(time.Second),
        kafka.WithConcurrency(2),
    )

    // 群成员退出/被踢 → 删除本地 group_member 缓存
    router.Handle("group.member.left", c.HandleGroupMemberLeft,
        kafka.WithRetry(3),
        kafka.WithRetryBackoff(time.Second),
        kafka.WithConcurrency(2),
    )
    router.Handle("group.member.kicked", c.HandleGroupMemberKicked,
        kafka.WithRetry(3),
        kafka.WithRetryBackoff(time.Second),
        kafka.WithConcurrency(2),
    )

    // 群成员禁言状态变更 → 更新本地 group_member 缓存 mute 字段
    router.Handle("group.member.mute.changed", c.HandleGroupMemberMuteChanged,
        kafka.WithRetry(3),
        kafka.WithRetryBackoff(time.Second),
        kafka.WithConcurrency(2),
    )

    // 群成员角色变更 → 更新本地 group_member 缓存 role 字段
    router.Handle("group.member.role.changed", c.HandleGroupMemberRoleChanged,
        kafka.WithRetry(3),
        kafka.WithRetryBackoff(time.Second),
        kafka.WithConcurrency(2),
    )

    // 群全员禁言状态变更 → 更新本地 group 缓存 mute 状态
    router.Handle("group.mute.changed", c.HandleGroupMuteChanged,
        kafka.WithRetry(3),
        kafka.WithRetryBackoff(time.Second),
        kafka.WithConcurrency(2),
    )

    // 用户资料更新 → 更新本地 user 缓存
    router.Handle("user.profile.updated", c.HandleUserProfileUpdated,
        kafka.WithRetry(3),
        kafka.WithRetryBackoff(time.Second),
        kafka.WithConcurrency(2),
    )

    // 用户设置更新 → 更新本地 user_settings 缓存
    router.Handle("user.settings.updated", c.HandleUserSettingsUpdated,
        kafka.WithRetry(3),
        kafka.WithRetryBackoff(time.Second),
        kafka.WithConcurrency(2),
    )
}

// buildEventHeader 构建 Kafka 事件头
func buildEventHeader(source string, instanceID string) *common.EventHeader {
    return &common.EventHeader{
        EventId:   uuid.New().String(),
        TraceId:   "",
        Source:    source,
        SourceId:  instanceID,
        Timestamp: time.Now().UnixMilli(),
        Version:   1,
    }
}

// delayedDoubleDelete 延迟双删工具
// 先删一次缓存 → 处理业务写入 → 等待 delay → 再删一次缓存
// 防止并发读在 DB 更新前把旧值回填缓存
func (c *MessageConsumer) delayedDoubleDelete(ctx context.Context, key string, delay time.Duration) {
    time.Sleep(delay)
    result, err := c.redis.Del(ctx, key).Result()
    if err != nil {
        log.Warn("delayed double delete failed", "key", key, "err", err)
        return
    }
    if result > 0 {
        log.Debug("delayed double delete removed stale cache", "key", key)
    }
}
```

---

## 本地缓存维护消费者

> 以下消费者负责监听 Relation / Group / User 服务发出的 Kafka 事件，维护 Message 服务的**本地 Redis 缓存**。  
> 这些缓存在 SendMessage 等热路径中替代 RPC 调用，大幅降低跨服务延迟。  
> 每个消费者遵循：**解析事件 → 幂等去重 → 延迟双删旧缓存 → 写入新值 → 记录日志** 的统一模式。

### Consumer: `relation.blocked`

> 来源：Relation 服务。用户 A 拉黑用户 B 后，Message 服务缓存此关系。  
> 效果：B 给 A 发消息时，SendMessage 直接查本地 Redis 拒绝，无需 RPC 调用 Relation.CheckBlockship。

```go
// HandleRelationBlocked 好友拉黑事件 → 更新本地拉黑关系缓存
// 当用户 A 拉黑用户 B，Message 服务缓存此关系，B 给 A 发消息时直接查本地 Redis 拒绝
func (c *MessageConsumer) HandleRelationBlocked(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_relation.RelationBlockedEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("unmarshal RelationBlockedEvent failed", "err", err)
        return nil // 反序列化失败不重试
    }

    // ==================== 2. 幂等检查 ====================
    dedupKey := fmt.Sprintf("msg:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        log.Debug("duplicate relation.blocked event", "event_id", event.Header.EventId)
        return nil
    }

    // ==================== 3. 参数校验 ====================
    if event.BlockerId == "" || event.BlockedId == "" {
        log.Error("relation.blocked: blocker_id or blocked_id is empty",
            "event_id", event.Header.EventId)
        return nil
    }

    // ==================== 4. 延迟双删 + 写入本地缓存 ====================
    // A 拉黑 B → 缓存 block:{A}:{B} = "1"
    // SendMessage 校验时查 block:{receiver}:{sender}，即 block:{A}:{B} 表示 A 拉黑了 B
    blockKey := fmt.Sprintf("msg:local:block:%s:%s", event.BlockerId, event.BlockedId)

    // 第一次删除（清除可能的旧缓存）
    c.redis.Del(ctx, blockKey)

    // 写入新值
    c.redis.Set(ctx, blockKey, "1", 24*time.Hour)

    // 延迟双删（防止并发读回填旧值）
    go c.delayedDoubleDelete(context.Background(), blockKey, 500*time.Millisecond)

    log.Info("relation.blocked: local block cache updated",
        "blocker_id", event.BlockerId, "blocked_id", event.BlockedId)
    return nil
}
```

### Consumer: `relation.unblocked`

> 来源：Relation 服务。用户 A 解除对用户 B 的拉黑。

```go
// HandleRelationUnblocked 解除拉黑 → 删除本地缓存
func (c *MessageConsumer) HandleRelationUnblocked(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_relation.RelationUnblockedEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("unmarshal RelationUnblockedEvent failed", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 ====================
    dedupKey := fmt.Sprintf("msg:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    if event.BlockerId == "" || event.BlockedId == "" {
        log.Error("relation.unblocked: blocker_id or blocked_id is empty")
        return nil
    }

    // ==================== 3. 删除本地缓存 ====================
    blockKey := fmt.Sprintf("msg:local:block:%s:%s", event.BlockerId, event.BlockedId)
    c.redis.Del(ctx, blockKey)

    // 延迟双删
    go c.delayedDoubleDelete(context.Background(), blockKey, 500*time.Millisecond)

    log.Info("relation.unblocked: local block cache deleted",
        "blocker_id", event.BlockerId, "blocked_id", event.BlockedId)
    return nil
}
```

### Consumer: `relation.friend.accepted`

> 来源：Relation 服务。好友请求被接受，双方建立好友关系。  
> 双向设置缓存：friend:{A}:{B} = "1" 且 friend:{B}:{A} = "1"

```go
// HandleRelationFriendAccepted 好友关系建立 → 双向设置本地好友缓存
func (c *MessageConsumer) HandleRelationFriendAccepted(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_relation.FriendAcceptedEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("unmarshal FriendAcceptedEvent failed", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 ====================
    dedupKey := fmt.Sprintf("msg:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    if event.UserId == "" || event.FriendId == "" {
        log.Error("relation.friend.accepted: user_id or friend_id is empty")
        return nil
    }

    // ==================== 3. 双向设置本地好友缓存 ====================
    // A 和 B 成为好友 → 缓存双向关系
    friendKeyAB := fmt.Sprintf("msg:local:friend:%s:%s", event.UserId, event.FriendId)
    friendKeyBA := fmt.Sprintf("msg:local:friend:%s:%s", event.FriendId, event.UserId)

    pipe := c.redis.Pipeline()

    // 第一次删除旧缓存
    pipe.Del(ctx, friendKeyAB)
    pipe.Del(ctx, friendKeyBA)
    pipe.Exec(ctx)

    // 写入新值
    pipe = c.redis.Pipeline()
    pipe.Set(ctx, friendKeyAB, "1", 24*time.Hour)
    pipe.Set(ctx, friendKeyBA, "1", 24*time.Hour)
    pipe.Exec(ctx)

    // 延迟双删
    go func() {
        c.delayedDoubleDelete(context.Background(), friendKeyAB, 500*time.Millisecond)
    }()
    go func() {
        c.delayedDoubleDelete(context.Background(), friendKeyBA, 500*time.Millisecond)
    }()

    log.Info("relation.friend.accepted: local friend cache set (bidirectional)",
        "user_id", event.UserId, "friend_id", event.FriendId)
    return nil
}
```

### Consumer: `relation.friend.deleted`

> 来源：Relation 服务。好友关系删除（单方或双方）。

```go
// HandleRelationFriendDeleted 好友删除 → 双向删除本地好友缓存
func (c *MessageConsumer) HandleRelationFriendDeleted(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_relation.FriendDeletedEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("unmarshal FriendDeletedEvent failed", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 ====================
    dedupKey := fmt.Sprintf("msg:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    if event.UserId == "" || event.FriendId == "" {
        log.Error("relation.friend.deleted: user_id or friend_id is empty")
        return nil
    }

    // ==================== 3. 双向删除本地好友缓存 ====================
    friendKeyAB := fmt.Sprintf("msg:local:friend:%s:%s", event.UserId, event.FriendId)
    friendKeyBA := fmt.Sprintf("msg:local:friend:%s:%s", event.FriendId, event.UserId)

    pipe := c.redis.Pipeline()
    pipe.Del(ctx, friendKeyAB)
    pipe.Del(ctx, friendKeyBA)
    pipe.Exec(ctx)

    // 延迟双删
    go func() {
        c.delayedDoubleDelete(context.Background(), friendKeyAB, 500*time.Millisecond)
    }()
    go func() {
        c.delayedDoubleDelete(context.Background(), friendKeyBA, 500*time.Millisecond)
    }()

    log.Info("relation.friend.deleted: local friend cache deleted (bidirectional)",
        "user_id", event.UserId, "friend_id", event.FriendId)
    return nil
}
```

### Consumer: `group.info.updated`

> 来源：Group 服务。群名称、头像、公告等信息变更。

```go
// HandleGroupInfoUpdated 群信息变更 → 更新本地群信息缓存
func (c *MessageConsumer) HandleGroupInfoUpdated(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_group.GroupInfoUpdatedEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("unmarshal GroupInfoUpdatedEvent failed", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 ====================
    dedupKey := fmt.Sprintf("msg:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    if event.GroupId == "" {
        log.Error("group.info.updated: group_id is empty")
        return nil
    }

    // ==================== 3. 延迟双删 + 更新本地群信息缓存 ====================
    cacheKey := fmt.Sprintf("msg:local:group:%s", event.GroupId)

    // 第一次删除
    c.redis.Del(ctx, cacheKey)

    // 写入新值（HASH 结构）
    c.redis.HSet(ctx, cacheKey, map[string]interface{}{
        "name":         event.Name,
        "owner_id":     event.OwnerId,
        "status":       fmt.Sprintf("%d", event.Status),
        "member_count": fmt.Sprintf("%d", event.MemberCount),
        "is_muted":     fmt.Sprintf("%t", event.IsMuted),
    })
    c.redis.Expire(ctx, cacheKey, 30*time.Minute)

    // 延迟双删
    go c.delayedDoubleDelete(context.Background(), cacheKey, 500*time.Millisecond)

    log.Info("group.info.updated: local group cache updated",
        "group_id", event.GroupId, "name", event.Name)
    return nil
}
```

### Consumer: `group.dissolved`

> 来源：Group 服务。群被解散，需要清除本地群缓存及所有相关群成员缓存。

```go
// HandleGroupDissolved 群解散 → 删除本地群信息缓存
func (c *MessageConsumer) HandleGroupDissolved(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_group.GroupDissolvedEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("unmarshal GroupDissolvedEvent failed", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 ====================
    dedupKey := fmt.Sprintf("msg:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    if event.GroupId == "" {
        log.Error("group.dissolved: group_id is empty")
        return nil
    }

    // ==================== 3. 删除本地群信息缓存 ====================
    groupCacheKey := fmt.Sprintf("msg:local:group:%s", event.GroupId)
    c.redis.Del(ctx, groupCacheKey)

    // 3b. 批量删除该群所有成员的本地缓存
    // 使用 SCAN 匹配 msg:local:group_member:{group_id}:* 模式
    var cursor uint64
    memberPattern := fmt.Sprintf("msg:local:group_member:%s:*", event.GroupId)
    for {
        keys, nextCursor, err := c.redis.Scan(ctx, cursor, memberPattern, 100).Result()
        if err != nil {
            log.Warn("scan group member cache keys failed",
                "group_id", event.GroupId, "err", err)
            break
        }
        if len(keys) > 0 {
            c.redis.Del(ctx, keys...)
        }
        cursor = nextCursor
        if cursor == 0 {
            break
        }
    }

    // 延迟双删群信息缓存
    go c.delayedDoubleDelete(context.Background(), groupCacheKey, 500*time.Millisecond)

    log.Info("group.dissolved: local group and member caches deleted",
        "group_id", event.GroupId)
    return nil
}
```

### Consumer: `group.member.joined`

> 来源：Group 服务。新成员加入群组。

```go
// HandleGroupMemberJoined 成员加入群 → 设置本地群成员缓存
func (c *MessageConsumer) HandleGroupMemberJoined(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_group.GroupMemberJoinedEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("unmarshal GroupMemberJoinedEvent failed", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 ====================
    dedupKey := fmt.Sprintf("msg:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    if event.GroupId == "" || event.UserId == "" {
        log.Error("group.member.joined: group_id or user_id is empty")
        return nil
    }

    // ==================== 3. 延迟双删 + 写入本地群成员缓存 ====================
    cacheKey := fmt.Sprintf("msg:local:group_member:%s:%s", event.GroupId, event.UserId)

    // 第一次删除
    c.redis.Del(ctx, cacheKey)

    // 新成员默认角色为普通成员，未禁言
    c.redis.HSet(ctx, cacheKey, map[string]interface{}{
        "role":       fmt.Sprintf("%d", event.Role), // 通常为 MEMBER
        "is_muted":  "false",
        "mute_until": "0",
    })
    c.redis.Expire(ctx, cacheKey, 10*time.Minute)

    // 延迟双删
    go c.delayedDoubleDelete(context.Background(), cacheKey, 500*time.Millisecond)

    // 3b. 更新群信息缓存中的 member_count（如果缓存存在）
    groupCacheKey := fmt.Sprintf("msg:local:group:%s", event.GroupId)
    if exists, _ := c.redis.Exists(ctx, groupCacheKey).Result(); exists > 0 {
        c.redis.HIncrBy(ctx, groupCacheKey, "member_count", 1)
    }

    log.Info("group.member.joined: local group member cache set",
        "group_id", event.GroupId, "user_id", event.UserId, "role", event.Role)
    return nil
}
```

### Consumer: `group.member.left`

> 来源：Group 服务。成员主动退出群组。

```go
// HandleGroupMemberLeft 成员退出 → 删除本地群成员缓存
func (c *MessageConsumer) HandleGroupMemberLeft(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_group.GroupMemberLeftEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("unmarshal GroupMemberLeftEvent failed", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 ====================
    dedupKey := fmt.Sprintf("msg:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    if event.GroupId == "" || event.UserId == "" {
        log.Error("group.member.left: group_id or user_id is empty")
        return nil
    }

    // ==================== 3. 删除本地群成员缓存 ====================
    cacheKey := fmt.Sprintf("msg:local:group_member:%s:%s", event.GroupId, event.UserId)
    c.redis.Del(ctx, cacheKey)

    // 延迟双删
    go c.delayedDoubleDelete(context.Background(), cacheKey, 500*time.Millisecond)

    // 3b. 更新群信息缓存中的 member_count
    groupCacheKey := fmt.Sprintf("msg:local:group:%s", event.GroupId)
    if exists, _ := c.redis.Exists(ctx, groupCacheKey).Result(); exists > 0 {
        c.redis.HIncrBy(ctx, groupCacheKey, "member_count", -1)
    }

    log.Info("group.member.left: local group member cache deleted",
        "group_id", event.GroupId, "user_id", event.UserId)
    return nil
}
```

### Consumer: `group.member.kicked`

> 来源：Group 服务。成员被管理员/群主踢出群组。逻辑与 left 相同。

```go
// HandleGroupMemberKicked 成员被踢 → 删除本地群成员缓存
func (c *MessageConsumer) HandleGroupMemberKicked(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_group.GroupMemberKickedEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("unmarshal GroupMemberKickedEvent failed", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 ====================
    dedupKey := fmt.Sprintf("msg:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    if event.GroupId == "" || event.UserId == "" {
        log.Error("group.member.kicked: group_id or user_id is empty")
        return nil
    }

    // ==================== 3. 删除本地群成员缓存 ====================
    cacheKey := fmt.Sprintf("msg:local:group_member:%s:%s", event.GroupId, event.UserId)
    c.redis.Del(ctx, cacheKey)

    // 延迟双删
    go c.delayedDoubleDelete(context.Background(), cacheKey, 500*time.Millisecond)

    // 3b. 更新群信息缓存中的 member_count
    groupCacheKey := fmt.Sprintf("msg:local:group:%s", event.GroupId)
    if exists, _ := c.redis.Exists(ctx, groupCacheKey).Result(); exists > 0 {
        c.redis.HIncrBy(ctx, groupCacheKey, "member_count", -1)
    }

    log.Info("group.member.kicked: local group member cache deleted",
        "group_id", event.GroupId, "user_id", event.UserId,
        "operator_id", event.OperatorId)
    return nil
}
```

### Consumer: `group.member.mute.changed`

> 来源：Group 服务。成员禁言/解禁状态变更。更新本地群成员缓存中的 mute 字段。

```go
// HandleGroupMemberMuteChanged 成员禁言状态变更 → 更新本地群成员缓存 mute 字段
func (c *MessageConsumer) HandleGroupMemberMuteChanged(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_group.GroupMemberMuteChangedEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("unmarshal GroupMemberMuteChangedEvent failed", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 ====================
    dedupKey := fmt.Sprintf("msg:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    if event.GroupId == "" || event.UserId == "" {
        log.Error("group.member.mute.changed: group_id or user_id is empty")
        return nil
    }

    // ==================== 3. 更新本地群成员缓存的 mute 字段 ====================
    cacheKey := fmt.Sprintf("msg:local:group_member:%s:%s", event.GroupId, event.UserId)

    // 检查缓存是否存在，存在则更新 mute 字段
    if exists, _ := c.redis.Exists(ctx, cacheKey).Result(); exists > 0 {
        // 第一次删除
        c.redis.Del(ctx, cacheKey)
    }

    // 写入/更新 mute 字段
    c.redis.HSet(ctx, cacheKey, map[string]interface{}{
        "is_muted":   fmt.Sprintf("%t", event.IsMuted),
        "mute_until": fmt.Sprintf("%d", event.MuteUntil),
    })
    // 保留原有 role 字段（如果缓存之前有的话，此处需要完整回写）
    // 为避免部分字段丢失，最好整体覆盖
    if event.Role > 0 {
        c.redis.HSet(ctx, cacheKey, "role", fmt.Sprintf("%d", event.Role))
    }
    c.redis.Expire(ctx, cacheKey, 10*time.Minute)

    // 延迟双删
    go c.delayedDoubleDelete(context.Background(), cacheKey, 500*time.Millisecond)

    log.Info("group.member.mute.changed: local group member cache mute updated",
        "group_id", event.GroupId, "user_id", event.UserId,
        "is_muted", event.IsMuted, "mute_until", event.MuteUntil)
    return nil
}
```

### Consumer: `group.member.role.changed`

> 来源：Group 服务。成员角色变更（普通成员→管理员、管理员→群主 等）。

```go
// HandleGroupMemberRoleChanged 成员角色变更 → 更新本地群成员缓存 role 字段
func (c *MessageConsumer) HandleGroupMemberRoleChanged(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_group.GroupMemberRoleChangedEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("unmarshal GroupMemberRoleChangedEvent failed", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 ====================
    dedupKey := fmt.Sprintf("msg:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    if event.GroupId == "" || event.UserId == "" {
        log.Error("group.member.role.changed: group_id or user_id is empty")
        return nil
    }

    // ==================== 3. 更新本地群成员缓存的 role 字段 ====================
    cacheKey := fmt.Sprintf("msg:local:group_member:%s:%s", event.GroupId, event.UserId)

    // 第一次删除
    c.redis.Del(ctx, cacheKey)

    // 写入新角色（完整覆盖成员缓存字段）
    c.redis.HSet(ctx, cacheKey, map[string]interface{}{
        "role":       fmt.Sprintf("%d", event.NewRole),
        "is_muted":  fmt.Sprintf("%t", event.IsMuted),   // 保持当前禁言状态
        "mute_until": fmt.Sprintf("%d", event.MuteUntil), // 保持当前禁言截止时间
    })
    c.redis.Expire(ctx, cacheKey, 10*time.Minute)

    // 延迟双删
    go c.delayedDoubleDelete(context.Background(), cacheKey, 500*time.Millisecond)

    log.Info("group.member.role.changed: local group member cache role updated",
        "group_id", event.GroupId, "user_id", event.UserId,
        "old_role", event.OldRole, "new_role", event.NewRole)
    return nil
}
```

### Consumer: `group.mute.changed`

> 来源：Group 服务。群全员禁言状态变更（群主/管理员开启或关闭全员禁言）。

```go
// HandleGroupMuteChanged 群全员禁言状态变更 → 更新本地群信息缓存 mute 状态
func (c *MessageConsumer) HandleGroupMuteChanged(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_group.GroupMuteChangedEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("unmarshal GroupMuteChangedEvent failed", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 ====================
    dedupKey := fmt.Sprintf("msg:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    if event.GroupId == "" {
        log.Error("group.mute.changed: group_id is empty")
        return nil
    }

    // ==================== 3. 更新本地群信息缓存的 mute 状态 ====================
    cacheKey := fmt.Sprintf("msg:local:group:%s", event.GroupId)

    // 检查缓存是否存在
    if exists, _ := c.redis.Exists(ctx, cacheKey).Result(); exists > 0 {
        // 第一次删除
        c.redis.Del(ctx, cacheKey)
    }

    // 更新 is_muted 和 status 字段
    // 群全员禁言时 status = GROUP_STATUS_MUTED
    newStatus := event.Status // 由 Group 服务传递的最新群状态
    c.redis.HSet(ctx, cacheKey, map[string]interface{}{
        "is_muted": fmt.Sprintf("%t", event.IsMuted),
        "status":   fmt.Sprintf("%d", newStatus),
    })
    c.redis.Expire(ctx, cacheKey, 30*time.Minute)

    // 延迟双删
    go c.delayedDoubleDelete(context.Background(), cacheKey, 500*time.Millisecond)

    log.Info("group.mute.changed: local group cache mute status updated",
        "group_id", event.GroupId, "is_muted", event.IsMuted, "status", newStatus)
    return nil
}
```

### Consumer: `user.profile.updated`

> 来源：User 服务。用户修改昵称、头像等资料信息。  
> Message 服务缓存用户信息用于 SendMessage 时获取发送者昵称/头像。

```go
// HandleUserProfileUpdated 用户资料变更 → 更新本地用户信息缓存
func (c *MessageConsumer) HandleUserProfileUpdated(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_user.UserProfileUpdatedEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("unmarshal UserProfileUpdatedEvent failed", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 ====================
    dedupKey := fmt.Sprintf("msg:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    if event.UserId == "" {
        log.Error("user.profile.updated: user_id is empty")
        return nil
    }

    // ==================== 3. 延迟双删 + 更新本地用户信息缓存 ====================
    cacheKey := fmt.Sprintf("msg:local:user:%s", event.UserId)

    // 第一次删除
    c.redis.Del(ctx, cacheKey)

    // 从 updated_fields map 提取 Message 服务关注的字段（nickname, avatar_url）
    // 注：UserProfileUpdatedEvent.updated_fields 是 map<string,string>，按需提取
    // 注：UserInfo proto 无 Status 字段，不缓存账号状态
    fields := make(map[string]interface{})
    if v, ok := event.UpdatedFields["nickname"]; ok {
        fields["nickname"] = v
    }
    if v, ok := event.UpdatedFields["avatar_url"]; ok {
        fields["avatar_url"] = v
    }

    if len(fields) == 0 {
        // 变更字段不包含 Message 服务关注的字段，无需更新本地缓存
        log.Debug("user.profile.updated: no relevant fields changed, skip cache update",
            "user_id", event.UserId)
        return nil
    }

    // 写入新值（HASH 结构，仅更新变更的字段）
    c.redis.HSet(ctx, cacheKey, fields)
    c.redis.Expire(ctx, cacheKey, 30*time.Minute)

    // 延迟双删
    go c.delayedDoubleDelete(context.Background(), cacheKey, 500*time.Millisecond)

    log.Info("user.profile.updated: local user cache updated",
        "user_id", event.UserId, "updated_fields", fields)
    return nil
}
```

### Consumer: `user.settings.updated`

> 来源：User 服务（新增 Topic）。用户修改隐私设置（如是否允许陌生人消息）。  
> Message 服务缓存此设置，SendMessage 单聊非好友时直接查本地 Redis 判断。

```go
// HandleUserSettingsUpdated 用户设置变更 → 更新本地用户设置缓存
func (c *MessageConsumer) HandleUserSettingsUpdated(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_user.UserSettingsUpdatedEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("unmarshal UserSettingsUpdatedEvent failed", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 ====================
    dedupKey := fmt.Sprintf("msg:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    if event.UserId == "" {
        log.Error("user.settings.updated: user_id is empty")
        return nil
    }

    // ==================== 3. 延迟双删 + 更新本地用户设置缓存 ====================
    cacheKey := fmt.Sprintf("msg:local:user_settings:%s", event.UserId)

    // 第一次删除
    c.redis.Del(ctx, cacheKey)

    // 写入新值（HASH 结构）
    // ⚠️ 仅缓存 Message 服务实际使用的字段，遵循最小数据原则
    // - allow_stranger_msg: SendMessage 单聊非好友时的权限校验（高频读取 + 变化不频繁 ✅）
    // - mute_notifications: 属于 Push/Notification 服务域，Message 不使用，不缓存 ❌
    // - message_receipt_enabled (send_read_receipt): 属于 Client/Ack 服务域，Message 不使用，不缓存 ❌
    settingsMap := map[string]interface{}{
        "allow_stranger_msg": fmt.Sprintf("%t", event.Settings.AllowStrangerMsg),
    }

    c.redis.HSet(ctx, cacheKey, settingsMap)
    c.redis.Expire(ctx, cacheKey, 1*time.Hour)

    // 延迟双删
    go c.delayedDoubleDelete(context.Background(), cacheKey, 500*time.Millisecond)

    log.Info("user.settings.updated: local user settings cache updated",
        "user_id", event.UserId,
        "allow_stranger_msg", event.Settings.AllowStrangerMsg)
    return nil
}
```
