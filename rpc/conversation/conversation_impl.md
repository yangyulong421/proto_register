# Conversation 会话服务 — RPC 接口实现伪代码

## 概述

Conversation 服务管理会话通道（ConversationChannel）和用户会话视图（UserConversation）。  
通道是底层全局唯一的逻辑对象，代表一个聊天关系（两个用户之间或一个群组）；用户会话是每个用户独立的视图，包含未读数、置顶、免打扰等个性化设置。

**核心概念区分：**
- **ConversationChannel** = 底层通道（全局唯一，如 `user_A ↔ user_B` 或 `group_123`）
- **UserConversation** = 用户维度的视图（每个用户独立记录，包含 unread、pin、mute 等）
- 一个 Channel 对应多个 UserConversation（单聊 2 个，群聊 N 个）

## 外部依赖

| 依赖类型 | 依赖项 | 用途 |
|---------|--------|------|
| PgSQL | conversation_channels 表 | 通道持久化 |
| PgSQL | user_conversations 表 | 用户会话视图持久化 |
| Redis | 通道/会话缓存 + 计数器 | 降低 DB 压力、原子序号分配 |
| Kafka | conversation.* 事件 | 事件通知下游服务 |

> **不依赖** 任何其他 RPC 服务。Conversation 是核心基础数据源，被 Message / Ack / Notification 等服务调用。

## PgSQL 表结构

```sql
-- 会话通道表（底层通道，全局唯一）
CREATE TABLE conversation_channels (
    id               BIGSERIAL PRIMARY KEY,
    channel_id       VARCHAR(64)  NOT NULL UNIQUE,           -- 通道唯一 ID
    channel_type     SMALLINT     NOT NULL DEFAULT 1,        -- 1=单聊 2=群聊
    participant_a    VARCHAR(64)  DEFAULT '',                 -- 单聊参与者 A（较小 ID）
    participant_b    VARCHAR(64)  DEFAULT '',                 -- 单聊参与者 B（较大 ID）
    group_id         VARCHAR(64)  DEFAULT '',                 -- 群聊关联的群 ID
    last_msg_id      VARCHAR(64)  DEFAULT '',                 -- 最后一条消息 ID
    last_msg_seq     BIGINT       DEFAULT 0,                  -- 最后一条消息 seq
    last_msg_time    BIGINT       DEFAULT 0,                  -- 最后一条消息时间戳（毫秒）
    last_sender_id   VARCHAR(64)  DEFAULT '',                 -- 最后一条消息发送者 ID
    last_msg_preview VARCHAR(512) DEFAULT '',                 -- 最后一条消息预览文本
    max_seq          BIGINT       DEFAULT 0,                  -- 当前最大消息序号
    extra            JSONB        DEFAULT '{}',               -- 扩展字段
    status           SMALLINT     NOT NULL DEFAULT 1,         -- 1=正常 2=已删除
    created_at       BIGINT       NOT NULL,
    updated_at       BIGINT       NOT NULL
);

-- 单聊唯一约束：(channel_type, participant_a, participant_b)
CREATE UNIQUE INDEX uk_channel_single
    ON conversation_channels(channel_type, participant_a, participant_b)
    WHERE channel_type = 1 AND participant_a != '' AND participant_b != '';

-- 群聊唯一约束：(channel_type, group_id)
CREATE UNIQUE INDEX uk_channel_group
    ON conversation_channels(channel_type, group_id)
    WHERE channel_type = 2 AND group_id != '';

CREATE INDEX idx_channel_group_id ON conversation_channels(group_id) WHERE group_id != '';

-- 用户会话视图表
CREATE TABLE user_conversations (
    id            BIGSERIAL PRIMARY KEY,
    user_id       VARCHAR(64)  NOT NULL,                     -- 用户 ID
    channel_id    VARCHAR(64)  NOT NULL,                     -- 关联通道 ID
    is_top        BOOLEAN      DEFAULT false,                -- 是否置顶
    is_muted      BOOLEAN      DEFAULT false,                -- 是否免打扰
    is_show       BOOLEAN      DEFAULT true,                 -- 是否在列表中显示
    unread_count  INT          DEFAULT 0,                    -- 未读消息数
    read_seq      BIGINT       DEFAULT 0,                    -- 用户已读到的 seq
    clear_seq     BIGINT       DEFAULT 0,                    -- 清空聊天记录时的 seq
    draft         TEXT         DEFAULT '',                   -- 草稿内容
    last_ack_time BIGINT       DEFAULT 0,                    -- 最后确认时间
    extra         JSONB        DEFAULT '{}',                 -- 扩展字段
    created_at    BIGINT       NOT NULL,
    updated_at    BIGINT       NOT NULL,
    UNIQUE(user_id, channel_id)
);

CREATE INDEX idx_user_conv_user_id ON user_conversations(user_id, is_show, updated_at DESC);
CREATE INDEX idx_user_conv_channel ON user_conversations(channel_id);
```

## Redis Key 设计

| Key 格式 | 类型 | 值 | TTL | 说明 |
|----------|------|-----|-----|------|
| `conv:channel:{channel_id}` | HASH | 通道信息各字段 | 1h | 通道基本信息缓存 |
| `conv:channel:pair:{min(a,b)}:{max(a,b)}` | STRING | channel_id | 1h | 单聊用户对→通道 ID 映射 |
| `conv:channel:group:{group_id}` | STRING | channel_id | 1h | 群聊群 ID→通道 ID 映射 |
| `conv:max_seq:{channel_id}` | STRING | max_seq | 无 TTL | 原子序号计数器（持久化） |
| `conv:user_list:{user_id}` | ZSET | member=channel_id, score=last_msg_time | 30min | 用户会话列表排序 |
| `conv:unread:{user_id}:{channel_id}` | STRING | unread_count | 24h | 单会话未读数 |
| `conv:total_unread:{user_id}` | STRING | total | 5min | 总未读数（短 TTL 允许重算） |
| `conv:user_conv:{user_id}:{channel_id}` | HASH | 用户会话视图各字段 | 30min | 用户会话详情缓存 |

## Lua 脚本

```lua
-- incr_channel_seq.lua：原子递增通道序号并返回分配区间
-- KEYS[1] = conv:max_seq:{channel_id}
-- ARGV[1] = count（需要分配的序号数量）
-- 返回：{start_seq, end_seq}
local key = KEYS[1]
local count = tonumber(ARGV[1])
local current = redis.call('INCRBY', key, count)
local start_seq = current - count + 1
return {start_seq, current}
```

---

## 接口实现

### 1. GetOrCreateChannel — 获取或创建会话通道

```go
func (s *ConversationService) GetOrCreateChannel(ctx context.Context, req *pb.GetOrCreateChannelRequest) (*pb.GetOrCreateChannelResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.Type == common.CONVERSATION_TYPE_UNSPECIFIED {
        return nil, status.Error(codes.InvalidArgument, "conversation type is required")
    }

    if req.Type == common.CONVERSATION_TYPE_SINGLE {
        if len(req.MemberIds) != 2 {
            return nil, status.Error(codes.InvalidArgument, "single chat requires exactly 2 member_ids")
        }
        if req.MemberIds[0] == req.MemberIds[1] {
            return nil, status.Error(codes.InvalidArgument, "cannot create conversation with self")
        }
    }

    if req.Type == common.CONVERSATION_TYPE_GROUP {
        if req.GroupId == "" {
            return nil, status.Error(codes.InvalidArgument, "group_id is required for group chat")
        }
    }

    // ==================== 2. 尝试从 Redis 缓存查找已有通道 ====================
    var channelID string
    var cacheHit bool

    if req.Type == common.CONVERSATION_TYPE_SINGLE {
        // 单聊：按 min/max 排序确保唯一性
        pA, pB := sortPair(req.MemberIds[0], req.MemberIds[1])
        pairKey := fmt.Sprintf("conv:channel:pair:%s:%s", pA, pB)
        cached, err := s.redis.Get(ctx, pairKey).Result()
        if err == nil && cached != "" {
            channelID = cached
            cacheHit = true
        }
    } else {
        // 群聊：按 group_id 查找
        groupKey := fmt.Sprintf("conv:channel:group:%s", req.GroupId)
        cached, err := s.redis.Get(ctx, groupKey).Result()
        if err == nil && cached != "" {
            channelID = cached
            cacheHit = true
        }
    }

    // ==================== 3. 缓存命中 → 读取通道详情返回 ====================
    if cacheHit && channelID != "" {
        channelInfo, err := s.getChannelFromCacheOrDB(ctx, channelID)
        if err == nil && channelInfo != nil {
            return &pb.GetOrCreateChannelResponse{
                Meta:    successMeta(ctx),
                Channel: channelInfo,
                Created: false,
            }, nil
        }
        // 缓存脏数据，继续走 DB 路径
    }

    // ==================== 4. 缓存未命中 → 查 PgSQL ====================
    var existingChannel pb.ConversationChannel
    var found bool

    if req.Type == common.CONVERSATION_TYPE_SINGLE {
        pA, pB := sortPair(req.MemberIds[0], req.MemberIds[1])
        err := s.db.QueryRowContext(ctx,
            `SELECT channel_id, channel_type, participant_a, participant_b, group_id,
                    last_msg_id, last_msg_seq, last_msg_time, last_sender_id,
                    last_msg_preview, max_seq, extra, status, created_at, updated_at
             FROM conversation_channels
             WHERE channel_type = 1 AND participant_a = $1 AND participant_b = $2 AND status = 1`,
            pA, pB,
        ).Scan(&existingChannel.ConversationId, /* ... 其余字段 ... */)
        if err == nil {
            found = true
        } else if err != sql.ErrNoRows {
            return nil, status.Error(codes.Internal, "db query failed")
        }
    } else {
        err := s.db.QueryRowContext(ctx,
            `SELECT channel_id, channel_type, participant_a, participant_b, group_id,
                    last_msg_id, last_msg_seq, last_msg_time, last_sender_id,
                    last_msg_preview, max_seq, extra, status, created_at, updated_at
             FROM conversation_channels
             WHERE channel_type = 2 AND group_id = $1 AND status = 1`,
            req.GroupId,
        ).Scan(&existingChannel.ConversationId, /* ... 其余字段 ... */)
        if err == nil {
            found = true
        } else if err != sql.ErrNoRows {
            return nil, status.Error(codes.Internal, "db query failed")
        }
    }

    // ==================== 5. 通道已存在 → 回写缓存并返回 ====================
    if found {
        go s.cacheChannel(context.Background(), &existingChannel, req.Type, req.MemberIds, req.GroupId)
        return &pb.GetOrCreateChannelResponse{
            Meta:    successMeta(ctx),
            Channel: &existingChannel,
            Created: false,
        }, nil
    }

    // ==================== 6. 通道不存在 → 创建新通道 ====================
    newChannelID := s.snowflake.Generate().String()
    now := time.Now().UnixMilli()

    var pA, pB string
    if req.Type == common.CONVERSATION_TYPE_SINGLE {
        pA, pB = sortPair(req.MemberIds[0], req.MemberIds[1])
    }

    _, err := s.db.ExecContext(ctx,
        `INSERT INTO conversation_channels
         (channel_id, channel_type, participant_a, participant_b, group_id,
          max_seq, status, created_at, updated_at)
         VALUES ($1, $2, $3, $4, $5, 0, 1, $6, $7)
         ON CONFLICT DO NOTHING`,
        newChannelID, int32(req.Type), pA, pB, req.GroupId, now, now,
    )
    if err != nil {
        return nil, status.Error(codes.Internal, "insert channel failed")
    }

    // ON CONFLICT DO NOTHING 可能未插入（并发创建），需重新查一次
    var channel pb.ConversationChannel
    if req.Type == common.CONVERSATION_TYPE_SINGLE {
        err = s.db.QueryRowContext(ctx,
            `SELECT channel_id, channel_type, participant_a, participant_b, group_id,
                    last_msg_id, last_msg_seq, last_msg_time, last_sender_id,
                    last_msg_preview, max_seq, extra, status, created_at, updated_at
             FROM conversation_channels
             WHERE channel_type = 1 AND participant_a = $1 AND participant_b = $2 AND status = 1`,
            pA, pB,
        ).Scan(&channel.ConversationId, /* ... 其余字段 ... */)
    } else {
        err = s.db.QueryRowContext(ctx,
            `SELECT channel_id, channel_type, participant_a, participant_b, group_id,
                    last_msg_id, last_msg_seq, last_msg_time, last_sender_id,
                    last_msg_preview, max_seq, extra, status, created_at, updated_at
             FROM conversation_channels
             WHERE channel_type = 2 AND group_id = $1 AND status = 1`,
            req.GroupId,
        ).Scan(&channel.ConversationId, /* ... 其余字段 ... */)
    }
    if err != nil {
        return nil, status.Error(codes.Internal, "query channel after insert failed")
    }

    isNewlyCreated := (channel.ConversationId == newChannelID)

    // ==================== 7. 初始化 Redis 序号计数器 ====================
    if isNewlyCreated {
        seqKey := fmt.Sprintf("conv:max_seq:%s", channel.ConversationId)
        s.redis.SetNX(ctx, seqKey, "0", 0) // 持久化，无 TTL
    }

    // ==================== 8. 写入 Redis 缓存 ====================
    go s.cacheChannel(context.Background(), &channel, req.Type, req.MemberIds, req.GroupId)

    // ==================== 9. 发送 Kafka 事件: conversation.created ====================
    if isNewlyCreated {
        event := &kafka_conversation.ConversationCreatedEvent{
            Header:         buildEventHeader("conversation", s.instanceID),
            ConversationId: channel.ConversationId,
            ConversationType: req.Type,
            CreateTime:     now,
        }
        if err := s.kafka.Produce(ctx, "conversation.created", channel.ConversationId, event); err != nil {
            log.Error("发送 conversation.created 事件失败", "channel_id", channel.ConversationId, "err", err)
        }
    }

    // ==================== 10. 返回 ====================
    return &pb.GetOrCreateChannelResponse{
        Meta:    successMeta(ctx),
        Channel: &channel,
        Created: isNewlyCreated,
    }, nil
}

// cacheChannel 回写通道相关 Redis 缓存
func (s *ConversationService) cacheChannel(ctx context.Context, ch *pb.ConversationChannel, chType common.ConversationType, memberIDs []string, groupID string) {
    pipe := s.redis.Pipeline()

    // 通道信息 HASH
    channelKey := fmt.Sprintf("conv:channel:%s", ch.ConversationId)
    pipe.HSet(ctx, channelKey, map[string]interface{}{
        "channel_id":       ch.ConversationId,
        "channel_type":     int32(chType),
        "last_msg_id":      ch.LastMsgId,
        "last_msg_time":    ch.LastMsgTime,
        "max_seq":          ch.MaxSeq,
        "created_at":       ch.CreatedAt,
        "updated_at":       ch.UpdatedAt,
    })
    pipe.Expire(ctx, channelKey, 1*time.Hour)

    // 映射索引
    if chType == common.CONVERSATION_TYPE_SINGLE && len(memberIDs) == 2 {
        pA, pB := sortPair(memberIDs[0], memberIDs[1])
        pairKey := fmt.Sprintf("conv:channel:pair:%s:%s", pA, pB)
        pipe.Set(ctx, pairKey, ch.ConversationId, 1*time.Hour)
    }
    if chType == common.CONVERSATION_TYPE_GROUP && groupID != "" {
        groupKey := fmt.Sprintf("conv:channel:group:%s", groupID)
        pipe.Set(ctx, groupKey, ch.ConversationId, 1*time.Hour)
    }

    if _, err := pipe.Exec(ctx); err != nil {
        log.Warn("回写通道缓存失败", "channel_id", ch.ConversationId, "err", err)
    }
}

// sortPair 确保两个 user_id 按字典序排列，保证单聊通道唯一性
func sortPair(a, b string) (string, string) {
    if a < b {
        return a, b
    }
    return b, a
}
```

### 2. GetChannel — 获取会话通道详情

```go
func (s *ConversationService) GetChannel(ctx context.Context, req *pb.GetChannelRequest) (*pb.GetChannelResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.ConversationId == "" {
        return nil, status.Error(codes.InvalidArgument, "conversation_id is required")
    }

    // ==================== 2. 优先读 Redis 缓存 ====================
    channelKey := fmt.Sprintf("conv:channel:%s", req.ConversationId)
    cached, err := s.redis.HGetAll(ctx, channelKey).Result()
    if err == nil && len(cached) > 0 {
        channel := mapToConversationChannel(cached)
        return &pb.GetChannelResponse{
            Meta:    successMeta(ctx),
            Channel: channel,
        }, nil
    }

    // ==================== 3. 缓存未命中 → 读 PgSQL ====================
    var ch pb.ConversationChannel
    var extraJSON string
    err = s.db.QueryRowContext(ctx,
        `SELECT channel_id, channel_type, participant_a, participant_b, group_id,
                last_msg_id, last_msg_seq, last_msg_time, last_sender_id,
                last_msg_preview, max_seq, extra, status, created_at, updated_at
         FROM conversation_channels
         WHERE channel_id = $1 AND status = 1`,
        req.ConversationId,
    ).Scan(&ch.ConversationId, &ch.Type, /* participant_a, participant_b 映射到 member_ids */
        /* ... 其余字段 ... */ &extraJSON, /* status */ &ch.CreatedAt, &ch.UpdatedAt)
    if err == sql.ErrNoRows {
        return nil, status.Error(codes.NotFound, "channel not found")
    }
    if err != nil {
        return nil, status.Error(codes.Internal, "db query failed")
    }

    // ==================== 4. 回写 Redis 缓存 ====================
    go func() {
        pipe := s.redis.Pipeline()
        pipe.HSet(context.Background(), channelKey, channelToMap(&ch))
        pipe.Expire(context.Background(), channelKey, 1*time.Hour)
        pipe.Exec(context.Background())
    }()

    // ==================== 5. 返回 ====================
    return &pb.GetChannelResponse{
        Meta:    successMeta(ctx),
        Channel: &ch,
    }, nil
}

// getChannelFromCacheOrDB 通用方法：先查缓存，再查 DB
func (s *ConversationService) getChannelFromCacheOrDB(ctx context.Context, channelID string) (*pb.ConversationChannel, error) {
    channelKey := fmt.Sprintf("conv:channel:%s", channelID)
    cached, err := s.redis.HGetAll(ctx, channelKey).Result()
    if err == nil && len(cached) > 0 {
        return mapToConversationChannel(cached), nil
    }

    var ch pb.ConversationChannel
    var extraJSON string
    err = s.db.QueryRowContext(ctx,
        `SELECT channel_id, channel_type, participant_a, participant_b, group_id,
                last_msg_id, last_msg_seq, last_msg_time, last_sender_id,
                last_msg_preview, max_seq, extra, status, created_at, updated_at
         FROM conversation_channels
         WHERE channel_id = $1 AND status = 1`,
        channelID,
    ).Scan(&ch.ConversationId, &ch.Type, /* ... */ &extraJSON, /* status */ &ch.CreatedAt, &ch.UpdatedAt)
    if err != nil {
        return nil, err
    }

    go func() {
        pipe := s.redis.Pipeline()
        pipe.HSet(context.Background(), channelKey, channelToMap(&ch))
        pipe.Expire(context.Background(), channelKey, 1*time.Hour)
        pipe.Exec(context.Background())
    }()

    return &ch, nil
}
```

### 3. UpdateChannelLastMsg — 更新通道最后一条消息

```go
func (s *ConversationService) UpdateChannelLastMsg(ctx context.Context, req *pb.UpdateChannelLastMsgRequest) (*pb.UpdateChannelLastMsgResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.ConversationId == "" {
        return nil, status.Error(codes.InvalidArgument, "conversation_id is required")
    }
    if req.LastMsgId == "" {
        return nil, status.Error(codes.InvalidArgument, "last_msg_id is required")
    }
    if req.LastMsgTime <= 0 {
        return nil, status.Error(codes.InvalidArgument, "last_msg_time must be positive")
    }

    // ==================== 2. PgSQL 更新（仅当新消息时间 >= 当前最后消息时间时才更新） ====================
    now := time.Now().UnixMilli()
    result, err := s.db.ExecContext(ctx,
        `UPDATE conversation_channels
         SET last_msg_id = $1, last_msg_time = $2, updated_at = $3
         WHERE channel_id = $4 AND status = 1 AND last_msg_time <= $5`,
        req.LastMsgId, req.LastMsgTime, now, req.ConversationId, req.LastMsgTime,
    )
    if err != nil {
        return nil, status.Error(codes.Internal, "db update last_msg failed")
    }
    rowsAffected, _ := result.RowsAffected()
    if rowsAffected == 0 {
        // 可能是旧消息到达或通道不存在，不视为错误
        log.Debug("UpdateChannelLastMsg 无更新", "channel_id", req.ConversationId)
    }

    // ==================== 3. 清除 Redis 通道缓存（延迟双删） ====================
    channelKey := fmt.Sprintf("conv:channel:%s", req.ConversationId)
    s.redis.Del(ctx, channelKey)
    go func() {
        time.Sleep(500 * time.Millisecond)
        s.redis.Del(context.Background(), channelKey)
    }()

    // ==================== 4. 返回 ====================
    return &pb.UpdateChannelLastMsgResponse{
        Meta: successMeta(ctx),
    }, nil
}
```

### 4. DeleteChannel — 删除会话通道

```go
func (s *ConversationService) DeleteChannel(ctx context.Context, req *pb.DeleteChannelRequest) (*pb.DeleteChannelResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.ConversationId == "" {
        return nil, status.Error(codes.InvalidArgument, "conversation_id is required")
    }

    // ==================== 2. 查询通道信息（删除前保留信息以清理缓存） ====================
    var channelType int32
    var participantA, participantB, groupID string
    err := s.db.QueryRowContext(ctx,
        `SELECT channel_type, participant_a, participant_b, group_id
         FROM conversation_channels
         WHERE channel_id = $1 AND status = 1`,
        req.ConversationId,
    ).Scan(&channelType, &participantA, &participantB, &groupID)
    if err == sql.ErrNoRows {
        return nil, status.Error(codes.NotFound, "channel not found")
    }
    if err != nil {
        return nil, status.Error(codes.Internal, "db query failed")
    }

    // ==================== 3. PgSQL 软删除通道 ====================
    now := time.Now().UnixMilli()
    _, err = s.db.ExecContext(ctx,
        `UPDATE conversation_channels SET status = 2, updated_at = $1 WHERE channel_id = $2 AND status = 1`,
        now, req.ConversationId,
    )
    if err != nil {
        return nil, status.Error(codes.Internal, "db delete channel failed")
    }

    // ==================== 4. 清除 Redis 全部相关缓存（延迟双删） ====================
    delKeys := []string{
        fmt.Sprintf("conv:channel:%s", req.ConversationId),
        fmt.Sprintf("conv:max_seq:%s", req.ConversationId),
    }
    if channelType == 1 && participantA != "" && participantB != "" {
        delKeys = append(delKeys, fmt.Sprintf("conv:channel:pair:%s:%s", participantA, participantB))
    }
    if channelType == 2 && groupID != "" {
        delKeys = append(delKeys, fmt.Sprintf("conv:channel:group:%s", groupID))
    }
    delayedDoubleDelete(ctx, s.redis, 500*time.Millisecond, delKeys...)

    // ==================== 5. 发送 Kafka 事件: conversation.deleted ====================
    event := &kafka_conversation.ConversationDeletedEvent{
        Header:         buildEventHeader("conversation", s.instanceID),
        ConversationId: req.ConversationId,
        DeleteTime:     now,
    }
    if err := s.kafka.Produce(ctx, "conversation.deleted", req.ConversationId, event); err != nil {
        log.Error("发送 conversation.deleted 事件失败", "channel_id", req.ConversationId, "err", err)
        s.saveFailedEvent(ctx, "conversation.deleted", req.ConversationId, event)
    }

    // ==================== 6. 返回 ====================
    return &pb.DeleteChannelResponse{
        Meta: successMeta(ctx),
    }, nil
}
```

### 5. GetChannelMaxSeq — 获取通道最大 seq

```go
func (s *ConversationService) GetChannelMaxSeq(ctx context.Context, req *pb.GetChannelMaxSeqRequest) (*pb.GetChannelMaxSeqResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.ConversationId == "" {
        return nil, status.Error(codes.InvalidArgument, "conversation_id is required")
    }

    // ==================== 2. 优先读 Redis（seq 计数器无 TTL，应该始终存在） ====================
    seqKey := fmt.Sprintf("conv:max_seq:%s", req.ConversationId)
    seqStr, err := s.redis.Get(ctx, seqKey).Result()
    if err == nil && seqStr != "" {
        maxSeq, parseErr := strconv.ParseInt(seqStr, 10, 64)
        if parseErr == nil {
            return &pb.GetChannelMaxSeqResponse{
                Meta:   successMeta(ctx),
                MaxSeq: maxSeq,
            }, nil
        }
    }

    // ==================== 3. Redis 未命中 → 查 PgSQL ====================
    var maxSeq int64
    err = s.db.QueryRowContext(ctx,
        `SELECT max_seq FROM conversation_channels WHERE channel_id = $1 AND status = 1`,
        req.ConversationId,
    ).Scan(&maxSeq)
    if err == sql.ErrNoRows {
        return nil, status.Error(codes.NotFound, "channel not found")
    }
    if err != nil {
        return nil, status.Error(codes.Internal, "db query failed")
    }

    // ==================== 4. 回写 Redis（SETNX 避免覆盖并发写入的更大值） ====================
    s.redis.SetNX(ctx, seqKey, strconv.FormatInt(maxSeq, 10), 0) // 无 TTL

    // ==================== 5. 返回 ====================
    return &pb.GetChannelMaxSeqResponse{
        Meta:   successMeta(ctx),
        MaxSeq: maxSeq,
    }, nil
}
```

### 6. IncrChannelSeq — 原子递增通道序号

> **⚠️ 关键路径**：这是整个系统中最热的操作之一。每条消息发送都需要调用。  
> **必须** 使用 Redis INCRBY + Lua 脚本保证原子性，不能走 PgSQL。

> **⚠️ CRIT-03 Redis Cluster 部署要求**：
> - Conversation 服务使用**独立 Redis Cluster**（8 主 8 从 = 16 节点），与其他服务隔离
> - `conv:max_seq:{channel_id}` key 自动按 `{channel_id}` hash 分片到不同 master
> - 5000 万会话均匀分布 = 每个 master 约 625 万个 seq key ≈ 600 MB 内存
> - 峰值 50 万 QPS / 8 master = 每 master 约 6.25 万 QPS（Redis 单节点可承载 10 万+ QPS）
>
> **Seq 预分配优化**（高频群聊场景）：
> - 万人群消息爆发时，1000 条/秒的 IncrChannelSeq 调用集中在同一 key
> - 优化：Message 服务可在 `BatchSendMessage` 场景下一次性申请 count=N 个 seq（已支持）
> - 客户端 typing 频率限制：最快 3 秒一条消息，服务端对同一 channel_id 的 IncrChannelSeq 做令牌桶限流

```go
// Lua 脚本预编译（启动时加载）
var incrSeqScript = redis.NewScript(`
    local key = KEYS[1]
    local count = tonumber(ARGV[1])
    local current = redis.call('INCRBY', key, count)
    local start_seq = current - count + 1
    return {start_seq, current}
`)

func (s *ConversationService) IncrChannelSeq(ctx context.Context, req *pb.IncrChannelSeqRequest) (*pb.IncrChannelSeqResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.ConversationId == "" {
        return nil, status.Error(codes.InvalidArgument, "conversation_id is required")
    }
    count := int(req.Count)
    if count <= 0 {
        count = 1 // 默认分配 1 个序号
    }
    if count > 1000 {
        return nil, status.Error(codes.InvalidArgument, "count must not exceed 1000")
    }

    // ==================== 2. 检查 Redis 中 seq key 是否存在 ====================
    seqKey := fmt.Sprintf("conv:max_seq:%s", req.ConversationId)
    exists, err := s.redis.Exists(ctx, seqKey).Result()
    if err != nil {
        return nil, status.Error(codes.Internal, "redis check key existence failed")
    }

    // ==================== 3. 如果 key 不存在，需要从 PgSQL 加载初始值 ====================
    if exists == 0 {
        var currentMax int64
        err := s.db.QueryRowContext(ctx,
            `SELECT max_seq FROM conversation_channels WHERE channel_id = $1 AND status = 1`,
            req.ConversationId,
        ).Scan(&currentMax)
        if err == sql.ErrNoRows {
            return nil, status.Error(codes.NotFound, "channel not found")
        }
        if err != nil {
            return nil, status.Error(codes.Internal, "db query max_seq failed")
        }
        // SETNX 避免并发覆盖：如果另一个请求已经设置了，不覆盖
        s.redis.SetNX(ctx, seqKey, strconv.FormatInt(currentMax, 10), 0)
    }

    // ==================== 4. 执行 Lua 脚本原子递增 ====================
    result, err := incrSeqScript.Run(ctx, s.redis, []string{seqKey}, count).Int64Slice()
    if err != nil {
        return nil, status.Error(codes.Internal, "redis incr seq lua script failed")
    }
    if len(result) != 2 {
        return nil, status.Error(codes.Internal, "lua script returned unexpected result")
    }

    startSeq := result[0]
    endSeq := result[1]

    // ==================== 5. 异步回写 PgSQL（最终一致性） ====================
    // 使用异步写避免阻塞热路径；PgSQL 中的 max_seq 可以略微滞后
    go func() {
        _, err := s.db.ExecContext(context.Background(),
            `UPDATE conversation_channels
             SET max_seq = GREATEST(max_seq, $1), updated_at = $2
             WHERE channel_id = $3 AND status = 1`,
            endSeq, time.Now().UnixMilli(), req.ConversationId,
        )
        if err != nil {
            log.Error("异步回写 max_seq 到 PgSQL 失败",
                "channel_id", req.ConversationId,
                "end_seq", endSeq,
                "err", err,
            )
            // 写入补偿队列，后台任务重试
            s.seqSyncQueue.Push(req.ConversationId, endSeq)
        }
    }()

    // ==================== 6. 返回 ====================
    return &pb.IncrChannelSeqResponse{
        Meta:     successMeta(ctx),
        StartSeq: startSeq,
        EndSeq:   endSeq,
    }, nil
}
```

### IncrChannelSeq — Redis Cluster 部署与 Seq 预分配说明（CRIT-03）

> **Redis Cluster 部署要求**：`conv:max_seq:*` 是整个系统中**写入频率最高**的 key 族群（每条消息都触发 INCRBY）。必须以 Redis Cluster 模式部署，确保水平扩展能力。

#### Redis Cluster 部署规格

| 项目 | 配置 |
|---|---|
| 模式 | Redis Cluster（至少 8 主 8 从 = 16 节点） |
| 分片策略 | `conv:max_seq:{conversation_id}` — CRC16 对 conversation_id 取模分配到 16384 slot |
| 内存估算 | 1 亿会话 × 每 key ~80B ≈ 8 GB，单节点分担 1 GB（8 主） |
| 持久化 | AOF appendfsync=everysec + RDB 每 5 分钟 |
| 高可用 | cluster-require-full-coverage=no，单主故障不阻断其他 slot |

#### Seq 预分配优化（万人群场景）

> 万人群消息高频场景（如群公告触发大量 @ 消息），单个 conversation_id 的 INCRBY 可能成为热 key。
> 使用 **seq 预分配（预取号段）** 策略缓解：

```go
// SeqAllocator 预分配号段，减少 Redis 调用次数
// 每次向 Redis 预取 step 个 seq，本地分配直到耗尽再拉取下一批
type SeqAllocator struct {
    mu       sync.Mutex
    cache    map[string]*seqRange // conversation_id → 预分配号段
    redis    *redis.ClusterClient
    step     int64                // 默认预取 100 个 seq
}

type seqRange struct {
    current int64
    end     int64
}

func (a *SeqAllocator) Alloc(ctx context.Context, convID string, count int64) (int64, int64, error) {
    a.mu.Lock()
    defer a.mu.Unlock()

    r, ok := a.cache[convID]
    if ok && r.current+count <= r.end {
        start := r.current
        r.current += count
        return start, r.current - 1, nil
    }

    // 号段不足，向 Redis 批量申请
    fetchCount := a.step
    if count > fetchCount {
        fetchCount = count // 大批量场景直接申请所需数量
    }
    result, err := incrSeqScript.Run(ctx, a.redis, []string{
        fmt.Sprintf("conv:max_seq:%s", convID),
    }, fetchCount).Int64Slice()
    if err != nil {
        return 0, 0, err
    }
    startSeq, endSeq := result[0], result[1]

    // 本次分配 count 个，剩余存入缓存
    a.cache[convID] = &seqRange{
        current: startSeq + count,
        end:     endSeq,
    }
    return startSeq, startSeq + count - 1, nil
}

// 万人群建议 step=500，普通群 step=50，单聊 step=10
// 通过 Config Service 动态调整 step 值
```

> **注意**：预分配会导致 seq 不连续（某些号段在实例重启时会被跳过），但对 IM 系统来说 seq 只需递增不需连续，因此可接受。

### 7. SetChannelExtra — 设置通道扩展字段

```go
func (s *ConversationService) SetChannelExtra(ctx context.Context, req *pb.SetChannelExtraRequest) (*pb.SetChannelExtraResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.ConversationId == "" {
        return nil, status.Error(codes.InvalidArgument, "conversation_id is required")
    }
    if len(req.Extra) == 0 {
        return nil, status.Error(codes.InvalidArgument, "extra must not be empty")
    }

    // ==================== 2. PgSQL 合并更新 extra（JSONB ||） ====================
    extraJSON, err := json.Marshal(req.Extra)
    if err != nil {
        return nil, status.Error(codes.InvalidArgument, "invalid extra format")
    }

    now := time.Now().UnixMilli()
    result, err := s.db.ExecContext(ctx,
        `UPDATE conversation_channels
         SET extra = extra || $1::jsonb, updated_at = $2
         WHERE channel_id = $3 AND status = 1`,
        string(extraJSON), now, req.ConversationId,
    )
    if err != nil {
        return nil, status.Error(codes.Internal, "db update extra failed")
    }
    rowsAffected, _ := result.RowsAffected()
    if rowsAffected == 0 {
        return nil, status.Error(codes.NotFound, "channel not found")
    }

    // ==================== 3. 清除 Redis 通道缓存（延迟双删） ====================
    channelKey := fmt.Sprintf("conv:channel:%s", req.ConversationId)
    delayedDoubleDelete(ctx, s.redis, 500*time.Millisecond, channelKey)

    // ==================== 4. 返回 ====================
    return &pb.SetChannelExtraResponse{
        Meta: successMeta(ctx),
    }, nil
}
```

### 8. CreateUserConversation — 创建用户会话视图

```go
func (s *ConversationService) CreateUserConversation(ctx context.Context, req *pb.CreateUserConversationRequest) (*pb.CreateUserConversationResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id is required")
    }
    if req.PeerId == "" {
        return nil, status.Error(codes.InvalidArgument, "peer_id is required")
    }
    if req.ConversationType == common.CONVERSATION_TYPE_UNSPECIFIED {
        return nil, status.Error(codes.InvalidArgument, "conversation_type is required")
    }

    // ==================== 2. 获取或创建底层通道 ====================
    var getOrCreateReq pb.GetOrCreateChannelRequest
    if req.ConversationType == common.CONVERSATION_TYPE_SINGLE {
        getOrCreateReq = pb.GetOrCreateChannelRequest{
            Type:      common.CONVERSATION_TYPE_SINGLE,
            MemberIds: []string{req.UserId, req.PeerId},
        }
    } else {
        getOrCreateReq = pb.GetOrCreateChannelRequest{
            Type:    common.CONVERSATION_TYPE_GROUP,
            GroupId: req.PeerId,
        }
    }
    channelResp, err := s.GetOrCreateChannel(ctx, &getOrCreateReq)
    if err != nil {
        return nil, status.Errorf(codes.Internal, "获取或创建通道失败: %v", err)
    }
    channelID := channelResp.Channel.ConversationId

    // ==================== 3. PgSQL 插入用户会话（UPSERT 幂等） ====================
    now := time.Now().UnixMilli()
    var convID string
    err = s.db.QueryRowContext(ctx,
        `INSERT INTO user_conversations
         (user_id, channel_id, is_top, is_muted, is_show, unread_count, read_seq, clear_seq,
          draft, last_ack_time, extra, created_at, updated_at)
         VALUES ($1, $2, false, false, true, 0, 0, 0, '', 0, '{}', $3, $4)
         ON CONFLICT (user_id, channel_id) DO UPDATE
         SET is_show = true, updated_at = $5
         RETURNING id`,
        req.UserId, channelID, now, now, now,
    ).Scan(&convID)
    if err != nil {
        return nil, status.Error(codes.Internal, "insert user_conversation failed")
    }

    // ==================== 3.5 清除相关缓存（延迟双删，防止并发读到旧列表） ====================
    delayedDoubleDelete(ctx, s.redis, 500*time.Millisecond,
        fmt.Sprintf("conv:user_conv:%s:%s", req.UserId, channelID),
        fmt.Sprintf("conv:user_list:%s", req.UserId),
        fmt.Sprintf("conv:total_unread:%s", req.UserId),
    )

    // ==================== 4. 查询完整的用户会话视图返回 ====================
    conv, err := s.getUserConversationFromDB(ctx, req.UserId, channelID)
    if err != nil {
        return nil, status.Error(codes.Internal, "query user_conversation after insert failed")
    }

    // ==================== 5. 写入 Redis 缓存 ====================
    go func() {
        bgCtx := context.Background()
        pipe := s.redis.Pipeline()

        // 用户会话详情缓存
        convKey := fmt.Sprintf("conv:user_conv:%s:%s", req.UserId, channelID)
        pipe.HSet(bgCtx, convKey, userConvToMap(conv))
        pipe.Expire(bgCtx, convKey, 30*time.Minute)

        // 添加到用户会话列表 ZSET
        listKey := fmt.Sprintf("conv:user_list:%s", req.UserId)
        pipe.ZAdd(bgCtx, listKey, &redis.Z{
            Score:  float64(now),
            Member: channelID,
        })
        pipe.Expire(bgCtx, listKey, 30*time.Minute)

        // 初始化未读数
        unreadKey := fmt.Sprintf("conv:unread:%s:%s", req.UserId, channelID)
        pipe.SetNX(bgCtx, unreadKey, "0", 24*time.Hour)

        pipe.Exec(bgCtx)
    }()

    // ==================== 6. 发送 Kafka 事件: conversation.created ====================
    event := &kafka_conversation.ConversationCreatedEvent{
        Header:           buildEventHeader("conversation", s.instanceID),
        UserId:           req.UserId,
        ConversationId:   channelID,
        ConversationType: req.ConversationType,
        PeerId:           req.PeerId,
        CreateTime:       now,
    }
    if err := s.kafka.Produce(ctx, "conversation.created", req.UserId, event); err != nil {
        log.Error("发送 conversation.created 事件失败", "user_id", req.UserId, "err", err)
    }

    // ==================== 7. 返回 ====================
    return &pb.CreateUserConversationResponse{
        Meta:         successMeta(ctx),
        Conversation: conv,
    }, nil
}
```

### 9. GetUserConversation — 获取单个用户会话详情

```go
func (s *ConversationService) GetUserConversation(ctx context.Context, req *pb.GetUserConversationRequest) (*pb.GetUserConversationResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id is required")
    }
    if req.ConversationId == "" {
        return nil, status.Error(codes.InvalidArgument, "conversation_id is required")
    }

    // ==================== 2. 优先读 Redis 缓存 ====================
    convKey := fmt.Sprintf("conv:user_conv:%s:%s", req.UserId, req.ConversationId)
    cached, err := s.redis.HGetAll(ctx, convKey).Result()
    if err == nil && len(cached) > 0 {
        conv := mapToUserConversation(cached)

        // 实时读取未读数（可能被其他消费者更新）
        unreadKey := fmt.Sprintf("conv:unread:%s:%s", req.UserId, req.ConversationId)
        unreadStr, uerr := s.redis.Get(ctx, unreadKey).Result()
        if uerr == nil {
            unread, _ := strconv.ParseInt(unreadStr, 10, 64)
            conv.UnreadCount = unread
        }

        return &pb.GetUserConversationResponse{
            Meta:         successMeta(ctx),
            Conversation: conv,
        }, nil
    }

    // ==================== 3. 缓存未命中 → 读 PgSQL ====================
    conv, err := s.getUserConversationFromDB(ctx, req.UserId, req.ConversationId)
    if err == sql.ErrNoRows {
        return nil, status.Error(codes.NotFound, "user conversation not found")
    }
    if err != nil {
        return nil, status.Error(codes.Internal, "db query failed")
    }

    // ==================== 4. 回写 Redis 缓存 ====================
    go func() {
        bgCtx := context.Background()
        pipe := s.redis.Pipeline()
        pipe.HSet(bgCtx, convKey, userConvToMap(conv))
        pipe.Expire(bgCtx, convKey, 30*time.Minute)
        pipe.Exec(bgCtx)
    }()

    // ==================== 5. 返回 ====================
    return &pb.GetUserConversationResponse{
        Meta:         successMeta(ctx),
        Conversation: conv,
    }, nil
}

// getUserConversationFromDB 从 PgSQL 查询用户会话并关联通道信息
func (s *ConversationService) getUserConversationFromDB(ctx context.Context, userID, channelID string) (*pb.UserConversation, error) {
    var conv pb.UserConversation
    var extraJSON string
    err := s.db.QueryRowContext(ctx,
        `SELECT uc.id, uc.user_id, uc.channel_id, uc.is_top, uc.is_muted, uc.is_show,
                uc.unread_count, uc.read_seq, uc.clear_seq, uc.draft, uc.last_ack_time,
                uc.extra, uc.created_at, uc.updated_at,
                cc.channel_type, cc.last_msg_id, cc.last_msg_time, cc.last_msg_preview, cc.max_seq,
                CASE WHEN cc.channel_type = 1 THEN
                    CASE WHEN cc.participant_a = $1 THEN cc.participant_b ELSE cc.participant_a END
                ELSE cc.group_id END AS peer_id
         FROM user_conversations uc
         INNER JOIN conversation_channels cc ON uc.channel_id = cc.channel_id
         WHERE uc.user_id = $1 AND uc.channel_id = $2 AND cc.status = 1`,
        userID, channelID,
    ).Scan(
        &conv.Id, &conv.UserId, &conv.ConversationId,
        &conv.IsPinned, &conv.IsMuted, /* is_show */
        &conv.UnreadCount, &conv.ReadSeq, &conv.ClearSeq, &conv.Draft, /* last_ack_time */
        &extraJSON, &conv.CreatedAt, &conv.UpdatedAt,
        &conv.ConversationType, &conv.LastMsgId, &conv.LastMsgTime, &conv.LastMsgPreview,
        &conv.MaxSeq, &conv.PeerId,
    )
    if err != nil {
        return nil, err
    }
    return &conv, nil
}
```

### 10. GetUserConversationList — 获取用户会话列表

```go
func (s *ConversationService) GetUserConversationList(ctx context.Context, req *pb.GetUserConversationListRequest) (*pb.GetUserConversationListResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id is required")
    }
    limit := int(req.Limit)
    if limit <= 0 {
        limit = 20
    }
    if limit > 100 {
        limit = 100
    }

    // ==================== 2. 尝试从 Redis ZSET 获取会话列表（快路径） ====================
    listKey := fmt.Sprintf("conv:user_list:%s", req.UserId)
    var channelIDs []string
    var useRedis bool

    // 检查 ZSET 是否存在
    zsetSize, err := s.redis.ZCard(ctx, listKey).Result()
    if err == nil && zsetSize > 0 {
        // 游标分页：score=last_msg_time，按降序取
        maxScore := "+inf"
        if req.CursorTime > 0 {
            maxScore = fmt.Sprintf("(%d", req.CursorTime) // 开区间排除上次最后一条
        }

        ids, err := s.redis.ZRevRangeByScore(ctx, listKey, &redis.ZRangeBy{
            Max:    maxScore,
            Min:    "-inf",
            Count:  int64(limit + 1), // 多取一条判断 has_more
            Offset: 0,
        }).Result()
        if err == nil && len(ids) > 0 {
            channelIDs = ids
            useRedis = true
        }
    }

    // ==================== 3. Redis 命中 → 批量获取会话详情 ====================
    if useRedis && len(channelIDs) > 0 {
        hasMore := len(channelIDs) > limit
        if hasMore {
            channelIDs = channelIDs[:limit]
        }

        conversations := make([]*pb.UserConversation, 0, len(channelIDs))
        pipe := s.redis.Pipeline()
        convCmds := make(map[string]*redis.MapStringStringCmd)
        unreadCmds := make(map[string]*redis.StringCmd)
        for _, cid := range channelIDs {
            convCmds[cid] = pipe.HGetAll(ctx, fmt.Sprintf("conv:user_conv:%s:%s", req.UserId, cid))
            unreadCmds[cid] = pipe.Get(ctx, fmt.Sprintf("conv:unread:%s:%s", req.UserId, cid))
        }
        pipe.Exec(ctx)

        // 收集 Redis 未命中的 channel_id
        var missCIDs []string
        for _, cid := range channelIDs {
            result, err := convCmds[cid].Result()
            if err != nil || len(result) == 0 {
                missCIDs = append(missCIDs, cid)
                continue
            }
            conv := mapToUserConversation(result)
            if unreadStr, uerr := unreadCmds[cid].Result(); uerr == nil {
                unread, _ := strconv.ParseInt(unreadStr, 10, 64)
                conv.UnreadCount = unread
            }
            conversations = append(conversations, conv)
        }

        // 未命中部分从 PgSQL 查
        if len(missCIDs) > 0 {
            dbConvs := s.batchGetUserConversationsFromDB(ctx, req.UserId, missCIDs)
            conversations = append(conversations, dbConvs...)
        }

        // 按 last_msg_time 降序排列，置顶优先（如果需要）
        if req.PinnedFirst {
            sort.SliceStable(conversations, func(i, j int) bool {
                if conversations[i].IsPinned != conversations[j].IsPinned {
                    return conversations[i].IsPinned
                }
                return conversations[i].LastMsgTime > conversations[j].LastMsgTime
            })
        } else {
            sort.SliceStable(conversations, func(i, j int) bool {
                return conversations[i].LastMsgTime > conversations[j].LastMsgTime
            })
        }

        return &pb.GetUserConversationListResponse{
            Meta:          successMeta(ctx),
            Conversations: conversations,
            HasMore:       hasMore,
        }, nil
    }

    // ==================== 4. Redis 未命中 → 全量从 PgSQL 查询（游标分页） ====================
    query := `
        SELECT uc.id, uc.user_id, uc.channel_id, uc.is_top, uc.is_muted, uc.is_show,
               uc.unread_count, uc.read_seq, uc.clear_seq, uc.draft,
               uc.created_at, uc.updated_at,
               cc.channel_type, cc.last_msg_id, cc.last_msg_time, cc.last_msg_preview, cc.max_seq,
               CASE WHEN cc.channel_type = 1 THEN
                   CASE WHEN cc.participant_a = $1 THEN cc.participant_b ELSE cc.participant_a END
               ELSE cc.group_id END AS peer_id
        FROM user_conversations uc
        INNER JOIN conversation_channels cc ON uc.channel_id = cc.channel_id
        WHERE uc.user_id = $1 AND uc.is_show = true AND cc.status = 1
    `
    args := []interface{}{req.UserId}
    argIdx := 2

    // 游标条件
    if req.CursorTime > 0 {
        query += fmt.Sprintf(" AND cc.last_msg_time < $%d", argIdx)
        args = append(args, req.CursorTime)
        argIdx++
    }

    // 排序
    if req.PinnedFirst {
        query += " ORDER BY uc.is_top DESC, cc.last_msg_time DESC"
    } else {
        query += " ORDER BY cc.last_msg_time DESC"
    }

    // 分页
    query += fmt.Sprintf(" LIMIT $%d", argIdx)
    args = append(args, limit+1)

    rows, err := s.db.QueryContext(ctx, query, args...)
    if err != nil {
        return nil, status.Error(codes.Internal, "db query conversation list failed")
    }
    defer rows.Close()

    conversations := make([]*pb.UserConversation, 0, limit)
    for rows.Next() {
        var conv pb.UserConversation
        var isShow bool
        if err := rows.Scan(
            &conv.Id, &conv.UserId, &conv.ConversationId,
            &conv.IsPinned, &conv.IsMuted, &isShow,
            &conv.UnreadCount, &conv.ReadSeq, &conv.ClearSeq, &conv.Draft,
            &conv.CreatedAt, &conv.UpdatedAt,
            &conv.ConversationType, &conv.LastMsgId, &conv.LastMsgTime,
            &conv.LastMsgPreview, &conv.MaxSeq, &conv.PeerId,
        ); err != nil {
            log.Warn("扫描会话行失败", "err", err)
            continue
        }
        conversations = append(conversations, &conv)
    }

    hasMore := len(conversations) > limit
    if hasMore {
        conversations = conversations[:limit]
    }

    // ==================== 5. 异步回写 Redis ZSET 和会话缓存 ====================
    go func() {
        bgCtx := context.Background()
        pipe := s.redis.Pipeline()

        for _, conv := range conversations {
            // ZSET
            pipe.ZAdd(bgCtx, listKey, &redis.Z{
                Score:  float64(conv.LastMsgTime),
                Member: conv.ConversationId,
            })
            // 会话详情
            convKey := fmt.Sprintf("conv:user_conv:%s:%s", req.UserId, conv.ConversationId)
            pipe.HSet(bgCtx, convKey, userConvToMap(conv))
            pipe.Expire(bgCtx, convKey, 30*time.Minute)
            // 未读数
            unreadKey := fmt.Sprintf("conv:unread:%s:%s", req.UserId, conv.ConversationId)
            pipe.Set(bgCtx, unreadKey, strconv.FormatInt(conv.UnreadCount, 10), 24*time.Hour)
        }
        pipe.Expire(bgCtx, listKey, 30*time.Minute)
        pipe.Exec(bgCtx)
    }()

    // ==================== 6. 返回 ====================
    return &pb.GetUserConversationListResponse{
        Meta:          successMeta(ctx),
        Conversations: conversations,
        HasMore:       hasMore,
    }, nil
}

// batchGetUserConversationsFromDB 批量从 PgSQL 查询用户会话
func (s *ConversationService) batchGetUserConversationsFromDB(ctx context.Context, userID string, channelIDs []string) []*pb.UserConversation {
    rows, err := s.db.QueryContext(ctx,
        `SELECT uc.id, uc.user_id, uc.channel_id, uc.is_top, uc.is_muted, uc.is_show,
                uc.unread_count, uc.read_seq, uc.clear_seq, uc.draft,
                uc.created_at, uc.updated_at,
                cc.channel_type, cc.last_msg_id, cc.last_msg_time, cc.last_msg_preview, cc.max_seq,
                CASE WHEN cc.channel_type = 1 THEN
                    CASE WHEN cc.participant_a = $1 THEN cc.participant_b ELSE cc.participant_a END
                ELSE cc.group_id END AS peer_id
         FROM user_conversations uc
         INNER JOIN conversation_channels cc ON uc.channel_id = cc.channel_id
         WHERE uc.user_id = $1 AND uc.channel_id = ANY($2) AND cc.status = 1`,
        userID, pq.Array(channelIDs),
    )
    if err != nil {
        log.Warn("批量查询用户会话失败", "user_id", userID, "err", err)
        return nil
    }
    defer rows.Close()

    var results []*pb.UserConversation
    for rows.Next() {
        var conv pb.UserConversation
        var isShow bool
        if err := rows.Scan(
            &conv.Id, &conv.UserId, &conv.ConversationId,
            &conv.IsPinned, &conv.IsMuted, &isShow,
            &conv.UnreadCount, &conv.ReadSeq, &conv.ClearSeq, &conv.Draft,
            &conv.CreatedAt, &conv.UpdatedAt,
            &conv.ConversationType, &conv.LastMsgId, &conv.LastMsgTime,
            &conv.LastMsgPreview, &conv.MaxSeq, &conv.PeerId,
        ); err != nil {
            continue
        }
        results = append(results, &conv)
    }

    // 异步回写缓存
    go func() {
        bgCtx := context.Background()
        pipe := s.redis.Pipeline()
        for _, conv := range results {
            convKey := fmt.Sprintf("conv:user_conv:%s:%s", userID, conv.ConversationId)
            pipe.HSet(bgCtx, convKey, userConvToMap(conv))
            pipe.Expire(bgCtx, convKey, 30*time.Minute)
        }
        pipe.Exec(bgCtx)
    }()

    return results
}
```

### 11. UpdateUserConversation — 更新用户会话属性

```go
func (s *ConversationService) UpdateUserConversation(ctx context.Context, req *pb.UpdateUserConversationRequest) (*pb.UpdateUserConversationResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id is required")
    }
    if req.ConversationId == "" {
        return nil, status.Error(codes.InvalidArgument, "conversation_id is required")
    }

    // ==================== 2. 构建动态更新 SQL ====================
    setClauses := []string{}
    args := []interface{}{}
    argIdx := 1
    now := time.Now().UnixMilli()

    // 记录变更字段，用于发送对应 Kafka 事件
    var pinnedChanged, mutedChanged bool
    var newPinned, newMuted bool

    if req.IsPinned != nil {
        setClauses = append(setClauses, fmt.Sprintf("is_top = $%d", argIdx))
        args = append(args, *req.IsPinned)
        pinnedChanged = true
        newPinned = *req.IsPinned
        argIdx++
    }
    if req.IsMuted != nil {
        setClauses = append(setClauses, fmt.Sprintf("is_muted = $%d", argIdx))
        args = append(args, *req.IsMuted)
        mutedChanged = true
        newMuted = *req.IsMuted
        argIdx++
    }
    if req.Draft != nil {
        setClauses = append(setClauses, fmt.Sprintf("draft = $%d", argIdx))
        args = append(args, *req.Draft)
        argIdx++
    }

    if len(setClauses) == 0 {
        return nil, status.Error(codes.InvalidArgument, "no fields to update")
    }

    // updated_at
    setClauses = append(setClauses, fmt.Sprintf("updated_at = $%d", argIdx))
    args = append(args, now)
    argIdx++

    // WHERE
    args = append(args, req.UserId)
    args = append(args, req.ConversationId)
    query := fmt.Sprintf(
        "UPDATE user_conversations SET %s WHERE user_id = $%d AND channel_id = $%d",
        strings.Join(setClauses, ", "), argIdx, argIdx+1,
    )

    // ==================== 3. 执行 PgSQL 更新 ====================
    result, err := s.db.ExecContext(ctx, query, args...)
    if err != nil {
        return nil, status.Error(codes.Internal, "db update user_conversation failed")
    }
    rowsAffected, _ := result.RowsAffected()
    if rowsAffected == 0 {
        return nil, status.Error(codes.NotFound, "user conversation not found")
    }

    // ==================== 4. 清除 Redis 缓存（延迟双删） ====================
    delayedDoubleDelete(ctx, s.redis, 500*time.Millisecond,
        fmt.Sprintf("conv:user_conv:%s:%s", req.UserId, req.ConversationId),
        fmt.Sprintf("conv:user_list:%s", req.UserId),
    )

    // ==================== 5. 发送 Kafka 事件 ====================
    // 5a. conversation.updated（通用更新事件）
    updateEvent := &kafka_conversation.ConversationUpdatedEvent{
        Header:         buildEventHeader("conversation", s.instanceID),
        UserId:         req.UserId,
        ConversationId: req.ConversationId,
        LastMsgTime:    now,
    }
    if err := s.kafka.Produce(ctx, "conversation.updated", req.UserId, updateEvent); err != nil {
        log.Error("发送 conversation.updated 事件失败", "err", err)
    }

    // 5b. 置顶变更事件
    if pinnedChanged {
        topEvent := &kafka_conversation.ConversationTopChangedEvent{
            Header:         buildEventHeader("conversation", s.instanceID),
            UserId:         req.UserId,
            ConversationId: req.ConversationId,
            IsTop:          newPinned,
            ChangeTime:     now,
        }
        if err := s.kafka.Produce(ctx, "conversation.top.changed", req.UserId, topEvent); err != nil {
            log.Error("发送 conversation.top.changed 事件失败", "err", err)
        }
    }

    // 5c. 免打扰变更事件
    if mutedChanged {
        muteEvent := &kafka_conversation.ConversationMuteChangedEvent{
            Header:         buildEventHeader("conversation", s.instanceID),
            UserId:         req.UserId,
            ConversationId: req.ConversationId,
            IsMuted:        newMuted,
            ChangeTime:     now,
        }
        if err := s.kafka.Produce(ctx, "conversation.mute.changed", req.UserId, muteEvent); err != nil {
            log.Error("发送 conversation.mute.changed 事件失败", "err", err)
        }
    }

    // ==================== 6. 查询更新后的完整会话返回 ====================
    conv, err := s.getUserConversationFromDB(ctx, req.UserId, req.ConversationId)
    if err != nil {
        log.Warn("查询更新后的会话失败", "err", err)
        return &pb.UpdateUserConversationResponse{Meta: successMeta(ctx)}, nil
    }

    return &pb.UpdateUserConversationResponse{
        Meta:         successMeta(ctx),
        Conversation: conv,
    }, nil
}
```

### 12. DeleteUserConversation — 删除用户会话

```go
func (s *ConversationService) DeleteUserConversation(ctx context.Context, req *pb.DeleteUserConversationRequest) (*pb.DeleteUserConversationResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id is required")
    }
    if req.ConversationId == "" {
        return nil, status.Error(codes.InvalidArgument, "conversation_id is required")
    }

    // ==================== 2. PgSQL 更新：隐藏会话 + 清零未读 ====================
    now := time.Now().UnixMilli()

    // 如果 clear_msg=true，记录当前 max_seq 作为 clear_seq
    if req.ClearMsg {
        // 查询当前通道 max_seq
        var maxSeq int64
        err := s.db.QueryRowContext(ctx,
            `SELECT cc.max_seq
             FROM user_conversations uc
             INNER JOIN conversation_channels cc ON uc.channel_id = cc.channel_id
             WHERE uc.user_id = $1 AND uc.channel_id = $2`,
            req.UserId, req.ConversationId,
        ).Scan(&maxSeq)
        if err != nil && err != sql.ErrNoRows {
            return nil, status.Error(codes.Internal, "query max_seq failed")
        }

        _, err = s.db.ExecContext(ctx,
            `UPDATE user_conversations
             SET is_show = false, unread_count = 0, clear_seq = $1, updated_at = $2
             WHERE user_id = $3 AND channel_id = $4`,
            maxSeq, now, req.UserId, req.ConversationId,
        )
        if err != nil {
            return nil, status.Error(codes.Internal, "db delete user_conversation with clear failed")
        }
    } else {
        _, err := s.db.ExecContext(ctx,
            `UPDATE user_conversations
             SET is_show = false, unread_count = 0, updated_at = $1
             WHERE user_id = $2 AND channel_id = $3`,
            now, req.UserId, req.ConversationId,
        )
        if err != nil {
            return nil, status.Error(codes.Internal, "db delete user_conversation failed")
        }
    }

    // ==================== 3. 清除 Redis 缓存（延迟双删） ====================
    // ZRem 直接移除 ZSET 成员
    s.redis.ZRem(ctx, fmt.Sprintf("conv:user_list:%s", req.UserId), req.ConversationId)
    // 延迟双删缓存 key
    delayedDoubleDelete(ctx, s.redis, 500*time.Millisecond,
        fmt.Sprintf("conv:user_conv:%s:%s", req.UserId, req.ConversationId),
        fmt.Sprintf("conv:unread:%s:%s", req.UserId, req.ConversationId),
        fmt.Sprintf("conv:total_unread:%s", req.UserId),
    )

    // ==================== 4. 发送 Kafka 事件: conversation.deleted ====================
    event := &kafka_conversation.ConversationDeletedEvent{
        Header:         buildEventHeader("conversation", s.instanceID),
        UserId:         req.UserId,
        ConversationId: req.ConversationId,
        Reason:         "user_deleted",
        DeleteTime:     now,
    }
    if err := s.kafka.Produce(ctx, "conversation.deleted", req.UserId, event); err != nil {
        log.Error("发送 conversation.deleted 事件失败", "err", err)
    }

    // ==================== 5. 返回 ====================
    return &pb.DeleteUserConversationResponse{
        Meta: successMeta(ctx),
    }, nil
}
```

### 13. ClearUnread — 清除会话未读数

```go
func (s *ConversationService) ClearUnread(ctx context.Context, req *pb.ClearUnreadRequest) (*pb.ClearUnreadResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id is required")
    }
    if req.ConversationId == "" {
        return nil, status.Error(codes.InvalidArgument, "conversation_id is required")
    }

    // ==================== 2. 读取当前未读数（用于事件） ====================
    var oldUnread int32
    unreadKey := fmt.Sprintf("conv:unread:%s:%s", req.UserId, req.ConversationId)
    oldUnreadStr, err := s.redis.Get(ctx, unreadKey).Result()
    if err == nil {
        val, _ := strconv.ParseInt(oldUnreadStr, 10, 32)
        oldUnread = int32(val)
    } else {
        // Redis 未命中，从 PgSQL 读
        s.db.QueryRowContext(ctx,
            `SELECT unread_count FROM user_conversations WHERE user_id = $1 AND channel_id = $2`,
            req.UserId, req.ConversationId,
        ).Scan(&oldUnread)
    }

    if oldUnread == 0 {
        // 已经是 0，无需操作
        return &pb.ClearUnreadResponse{Meta: successMeta(ctx)}, nil
    }

    // ==================== 3. PgSQL 清零未读数 + 更新 read_seq = max_seq ====================
    now := time.Now().UnixMilli()
    _, err = s.db.ExecContext(ctx,
        `UPDATE user_conversations uc
         SET unread_count = 0,
             read_seq = (SELECT max_seq FROM conversation_channels WHERE channel_id = uc.channel_id),
             last_ack_time = $1,
             updated_at = $2
         WHERE user_id = $3 AND channel_id = $4`,
        now, now, req.UserId, req.ConversationId,
    )
    if err != nil {
        return nil, status.Error(codes.Internal, "db clear unread failed")
    }

    // ==================== 4. 更新 Redis（延迟双删） ====================
    delayedDoubleDelete(ctx, s.redis, 500*time.Millisecond,
        fmt.Sprintf("conv:unread:%s:%s", req.UserId, req.ConversationId),
        fmt.Sprintf("conv:user_conv:%s:%s", req.UserId, req.ConversationId),
        fmt.Sprintf("conv:total_unread:%s", req.UserId),
    )

    // ==================== 5. 发送 Kafka 事件: conversation.unread.reset ====================
    event := &kafka_conversation.ConversationUnreadResetEvent{
        Header:         buildEventHeader("conversation", s.instanceID),
        UserId:         req.UserId,
        ConversationId: req.ConversationId,
        OldUnreadCount: oldUnread,
        NewUnreadCount: 0,
        ResetTime:      now,
    }
    if err := s.kafka.Produce(ctx, "conversation.unread.reset", req.UserId, event); err != nil {
        log.Error("发送 conversation.unread.reset 事件失败", "err", err)
    }

    // ==================== 6. 返回 ====================
    return &pb.ClearUnreadResponse{
        Meta: successMeta(ctx),
    }, nil
}
```

### 14. SetReadSeq — 设置已读位置

```go
func (s *ConversationService) SetReadSeq(ctx context.Context, req *pb.SetReadSeqRequest) (*pb.SetReadSeqResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id is required")
    }
    if req.ConversationId == "" {
        return nil, status.Error(codes.InvalidArgument, "conversation_id is required")
    }
    if req.ReadSeq < 0 {
        return nil, status.Error(codes.InvalidArgument, "read_seq must be non-negative")
    }

    // ==================== 2. PgSQL 更新 read_seq（仅允许前进，不允许后退） ====================
    now := time.Now().UnixMilli()
    var oldReadSeq int64
    var maxSeq int64

    err := s.db.QueryRowContext(ctx,
        `SELECT uc.read_seq, cc.max_seq
         FROM user_conversations uc
         INNER JOIN conversation_channels cc ON uc.channel_id = cc.channel_id
         WHERE uc.user_id = $1 AND uc.channel_id = $2`,
        req.UserId, req.ConversationId,
    ).Scan(&oldReadSeq, &maxSeq)
    if err == sql.ErrNoRows {
        return nil, status.Error(codes.NotFound, "user conversation not found")
    }
    if err != nil {
        return nil, status.Error(codes.Internal, "db query failed")
    }

    // read_seq 只能前进
    if req.ReadSeq <= oldReadSeq {
        return &pb.SetReadSeqResponse{Meta: successMeta(ctx)}, nil
    }

    // 不能超过 max_seq
    readSeq := req.ReadSeq
    if readSeq > maxSeq {
        readSeq = maxSeq
    }

    // ==================== 3. 重新计算未读数 ====================
    newUnread := maxSeq - readSeq
    if newUnread < 0 {
        newUnread = 0
    }

    // ==================== 4. PgSQL 更新 ====================
    _, err = s.db.ExecContext(ctx,
        `UPDATE user_conversations
         SET read_seq = $1, unread_count = $2, last_ack_time = $3, updated_at = $4
         WHERE user_id = $5 AND channel_id = $6 AND read_seq < $7`,
        readSeq, newUnread, now, now, req.UserId, req.ConversationId, readSeq,
    )
    if err != nil {
        return nil, status.Error(codes.Internal, "db update read_seq failed")
    }

    // ==================== 5. 更新 Redis（延迟双删） ====================
    delayedDoubleDelete(ctx, s.redis, 500*time.Millisecond,
        fmt.Sprintf("conv:unread:%s:%s", req.UserId, req.ConversationId),
        fmt.Sprintf("conv:user_conv:%s:%s", req.UserId, req.ConversationId),
        fmt.Sprintf("conv:total_unread:%s", req.UserId),
    )

    // ==================== 6. 发送 Kafka 事件: conversation.unread.changed ====================
    unreadEvent := &kafka_conversation.ConversationUnreadResetEvent{
        Header:         buildEventHeader("conversation", s.instanceID),
        UserId:         req.UserId,
        ConversationId: req.ConversationId,
        OldUnreadCount: int32(maxSeq - oldReadSeq),
        NewUnreadCount: int32(newUnread),
        ResetTime:      now,
    }
    if err := s.kafka.Produce(ctx, "conversation.unread.changed", req.UserId, unreadEvent); err != nil {
        log.Error("发送 conversation.unread.changed 事件失败", "err", err)
    }

    // ==================== 7. 返回 ====================
    return &pb.SetReadSeqResponse{
        Meta: successMeta(ctx),
    }, nil
}
```

### 15. SetClearSeq — 设置会话清空位置

```go
func (s *ConversationService) SetClearSeq(ctx context.Context, req *pb.SetClearSeqRequest) (*pb.SetClearSeqResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id is required")
    }
    if req.ConversationId == "" {
        return nil, status.Error(codes.InvalidArgument, "conversation_id is required")
    }
    if req.ClearSeq < 0 {
        return nil, status.Error(codes.InvalidArgument, "clear_seq must be non-negative")
    }

    // ==================== 2. 查询当前 clear_seq 和 max_seq ====================
    now := time.Now().UnixMilli()
    var oldClearSeq int64
    var maxSeq int64

    err := s.db.QueryRowContext(ctx,
        `SELECT uc.clear_seq, cc.max_seq
         FROM user_conversations uc
         INNER JOIN conversation_channels cc ON uc.channel_id = cc.channel_id
         WHERE uc.user_id = $1 AND uc.channel_id = $2`,
        req.UserId, req.ConversationId,
    ).Scan(&oldClearSeq, &maxSeq)
    if err == sql.ErrNoRows {
        return nil, status.Error(codes.NotFound, "user conversation not found")
    }
    if err != nil {
        return nil, status.Error(codes.Internal, "db query failed")
    }

    // clear_seq 只能前进（不允许后退）
    if req.ClearSeq <= oldClearSeq {
        return &pb.SetClearSeqResponse{Meta: successMeta(ctx)}, nil
    }

    // 不能超过 max_seq
    clearSeq := req.ClearSeq
    if clearSeq > maxSeq {
        clearSeq = maxSeq
    }

    // ==================== 3. PgSQL 更新 clear_seq ====================
    _, err = s.db.ExecContext(ctx,
        `UPDATE user_conversations
         SET clear_seq = $1, clear_time = $2, updated_at = $3
         WHERE user_id = $4 AND channel_id = $5 AND clear_seq < $6`,
        clearSeq, now, now, req.UserId, req.ConversationId, clearSeq,
    )
    if err != nil {
        return nil, status.Error(codes.Internal, "db update clear_seq failed")
    }

    // ==================== 4. 延迟双删缓存 ====================
    delayedDoubleDelete(ctx, s.redis, 500*time.Millisecond,
        fmt.Sprintf("conv:user_conv:%s:%s", req.UserId, req.ConversationId),
    )

    // ==================== 5. 返回 ====================
    // 清空操作是用户本地行为，无需发送 Kafka 事件
    return &pb.SetClearSeqResponse{
        Meta: successMeta(ctx),
    }, nil
}
```

### 16. GetTotalUnread — 获取用户所有会话总未读数

```go
func (s *ConversationService) GetTotalUnread(ctx context.Context, req *pb.GetTotalUnreadRequest) (*pb.GetTotalUnreadResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id is required")
    }

    // ==================== 2. 优先读 Redis 缓存（TTL 5min，短期缓存避免频繁聚合） ====================
    totalKey := fmt.Sprintf("conv:total_unread:%s", req.UserId)
    cachedTotal, err := s.redis.Get(ctx, totalKey).Result()
    if err == nil && cachedTotal != "" {
        total, parseErr := strconv.ParseInt(cachedTotal, 10, 64)
        if parseErr == nil {
            return &pb.GetTotalUnreadResponse{
                Meta:        successMeta(ctx),
                TotalUnread: total,
            }, nil
        }
    }

    // ==================== 3. Redis 未命中 → PgSQL 聚合计算 ====================
    var totalUnread int64
    err = s.db.QueryRowContext(ctx,
        `SELECT COALESCE(SUM(uc.unread_count), 0)
         FROM user_conversations uc
         INNER JOIN conversation_channels cc ON uc.channel_id = cc.channel_id
         WHERE uc.user_id = $1 AND uc.is_show = true AND uc.is_muted = false AND cc.status = 1`,
        req.UserId,
    ).Scan(&totalUnread)
    if err != nil {
        return nil, status.Error(codes.Internal, "db aggregate total unread failed")
    }

    // ==================== 4. 回写 Redis 缓存 ====================
    go func() {
        s.redis.Set(context.Background(), totalKey,
            strconv.FormatInt(totalUnread, 10), 5*time.Minute)
    }()

    // ==================== 5. 返回 ====================
    return &pb.GetTotalUnreadResponse{
        Meta:        successMeta(ctx),
        TotalUnread: totalUnread,
    }, nil
}
```

---

## 辅助函数

```go
// channelToMap 将通道对象转为 Redis HASH 字段
func channelToMap(ch *pb.ConversationChannel) map[string]interface{} {
    return map[string]interface{}{
        "channel_id":       ch.ConversationId,
        "channel_type":     int32(ch.Type),
        "last_msg_id":      ch.LastMsgId,
        "last_msg_time":    ch.LastMsgTime,
        "max_seq":          ch.MaxSeq,
        "created_at":       ch.CreatedAt,
        "updated_at":       ch.UpdatedAt,
    }
}

// mapToConversationChannel 将 Redis HASH 字段转为通道对象
func mapToConversationChannel(m map[string]string) *pb.ConversationChannel {
    ch := &pb.ConversationChannel{}
    ch.ConversationId = m["channel_id"]
    ch.Type = common.ConversationType(parseInt32(m["channel_type"]))
    ch.LastMsgId = m["last_msg_id"]
    ch.LastMsgTime = parseInt64(m["last_msg_time"])
    ch.MaxSeq = parseInt64(m["max_seq"])
    ch.CreatedAt = parseInt64(m["created_at"])
    ch.UpdatedAt = parseInt64(m["updated_at"])
    return ch
}

// userConvToMap 将用户会话对象转为 Redis HASH 字段
func userConvToMap(conv *pb.UserConversation) map[string]interface{} {
    return map[string]interface{}{
        "id":                conv.Id,
        "user_id":           conv.UserId,
        "peer_id":           conv.PeerId,
        "conversation_type": int32(conv.ConversationType),
        "conversation_id":   conv.ConversationId,
        "last_msg_id":       conv.LastMsgId,
        "last_msg_preview":  conv.LastMsgPreview,
        "last_msg_time":     conv.LastMsgTime,
        "unread_count":      conv.UnreadCount,
        "is_pinned":         boolToStr(conv.IsPinned),
        "is_muted":          boolToStr(conv.IsMuted),
        "draft":             conv.Draft,
        "read_seq":          conv.ReadSeq,
        "max_seq":           conv.MaxSeq,
        "clear_seq":         conv.ClearSeq,
        "created_at":        conv.CreatedAt,
        "updated_at":        conv.UpdatedAt,
    }
}

// mapToUserConversation 将 Redis HASH 字段转为用户会话对象
func mapToUserConversation(m map[string]string) *pb.UserConversation {
    conv := &pb.UserConversation{}
    conv.Id = m["id"]
    conv.UserId = m["user_id"]
    conv.PeerId = m["peer_id"]
    conv.ConversationType = common.ConversationType(parseInt32(m["conversation_type"]))
    conv.ConversationId = m["conversation_id"]
    conv.LastMsgId = m["last_msg_id"]
    conv.LastMsgPreview = m["last_msg_preview"]
    conv.LastMsgTime = parseInt64(m["last_msg_time"])
    conv.UnreadCount = parseInt64(m["unread_count"])
    conv.IsPinned = m["is_pinned"] == "true"
    conv.IsMuted = m["is_muted"] == "true"
    conv.Draft = m["draft"]
    conv.ReadSeq = parseInt64(m["read_seq"])
    conv.MaxSeq = parseInt64(m["max_seq"])
    conv.ClearSeq = parseInt64(m["clear_seq"])
    conv.CreatedAt = parseInt64(m["created_at"])
    conv.UpdatedAt = parseInt64(m["updated_at"])
    return conv
}

// delayedDoubleDelete 延迟双删辅助函数
// 先立即删除指定的缓存 key，然后在延迟时间后再次删除，以应对「DB 写 → 缓存删 → 并发读回填旧数据」的竞态窗口
func delayedDoubleDelete(ctx context.Context, rdb redis.Cmdable, delay time.Duration, keys ...string) {
    if len(keys) == 0 {
        return
    }
    // 第一次立即删除
    rdb.Del(ctx, keys...)
    // 第二次延迟删除
    go func() {
        time.Sleep(delay)
        rdb.Del(context.Background(), keys...)
    }()
}

// successMeta 构造成功响应元信息
func successMeta(ctx context.Context) *common.ResponseMeta {
    return &common.ResponseMeta{
        Code:       0,
        Message:    "success",
        ServerTime: time.Now().UnixMilli(),
        TraceId:    extractTraceID(ctx),
    }
}

// buildEventHeader 构造 Kafka 事件头
func buildEventHeader(source, instanceID string) *common.EventHeader {
    return &common.EventHeader{
        EventId:   uuid.New().String(),
        TraceId:   "", // 从 ctx 提取
        Source:    source,
        SourceId:  instanceID,
        Timestamp: time.Now().UnixMilli(),
        Version:   1,
    }
}
```

---

## 可观测性接入（OpenTelemetry）

> Conversation 服务负责会话管理和 seq 分配，是 Message 服务的最热依赖。需重点观测：IncrChannelSeq 的延迟（直接影响消息发送速度）、会话创建速率、未读数计算耗时、Redis Lua 脚本执行延迟。

### 第一步：服务启动时初始化 OTel SDK

```go
func main() {
    ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
    defer cancel()

    otelShutdown := observability.InitOpenTelemetry(ctx, "conversation", "v1.0.0")
    defer otelShutdown(ctx)
    observability.SetupLogger("conversation")
}
```

### 第二步：gRPC Server Interceptor（Conversation 不依赖其他 RPC）

```go
grpcServer := grpc.NewServer(
    grpc.ChainUnaryInterceptor(otelgrpc.UnaryServerInterceptor()),
    grpc.ChainStreamInterceptor(otelgrpc.StreamServerInterceptor()),
)
```

### 第三步：注册 Conversation 专属业务指标

```go
var meter = otel.Meter("im-chat/conversation")

var (
    // 会话通道创建计数
    channelCreated, _ = meter.Int64Counter("conversation.channel_created_total",
        metric.WithDescription("会话通道创建总数"))

    // seq 分配调用计数（最热路径）
    seqAllocCounter, _ = meter.Int64Counter("conversation.seq_alloc_total",
        metric.WithDescription("IncrChannelSeq 调用总数"))

    // seq Redis INCRBY 延迟
    seqAllocLatency, _ = meter.Float64Histogram("conversation.seq_alloc_latency_ms",
        metric.WithDescription("seq 分配 Redis INCRBY 延迟"), metric.WithUnit("ms"))

    // 未读数计算耗时
    unreadCalcDuration, _ = meter.Float64Histogram("conversation.unread_calc_duration_ms",
        metric.WithDescription("未读数计算耗时"), metric.WithUnit("ms"))

    // Redis Lua 脚本执行延迟
    luaScriptDuration, _ = meter.Float64Histogram("conversation.lua_script_duration_ms",
        metric.WithDescription("Redis Lua 脚本执行延迟"), metric.WithUnit("ms"))
)
```

在业务代码中埋点：

```go
// IncrChannelSeq 中 — 最热路径
seqAllocCounter.Add(ctx, 1)
redisStart := time.Now()
newSeq, err := s.redis.IncrBy(ctx, seqKey, 1).Result()
seqAllocLatency.Record(ctx, float64(time.Since(redisStart).Milliseconds()))

// GetTotalUnread 中
unreadStart := time.Now()
// ... 计算总未读 ...
unreadCalcDuration.Record(ctx, float64(time.Since(unreadStart).Milliseconds()))

// Redis Lua 脚本调用处
luaStart := time.Now()
result, err := s.redis.EvalSha(ctx, scriptSHA, keys, args).Result()
luaScriptDuration.Record(ctx, float64(time.Since(luaStart).Milliseconds()))
```

### 第四步：Kafka Producer 埋点 + 结构化 JSON 日志 + buildEventHeader 改造

与所有服务一致。`buildEventHeader` 改为使用共享包：

```go
header := observability.BuildEventHeader(ctx, "conversation", instanceID)
```

### 补充：基础设施指标注册

Conversation 服务 **重度依赖 Redis**（seq 分配、未读计数、Lua 脚本），Redis 连接池健康直接影响消息发送：

```go
func main() {
    // ... initOpenTelemetry ...
    observability.RegisterRedisPoolMetrics(redisClient, "conversation")
}
```

### 补充：Span 错误记录

```go
func (s *ConversationServer) IncrChannelSeq(ctx context.Context, req *pb.IncrChannelSeqRequest) (*pb.IncrChannelSeqResponse, error) {
    ctx, span := otel.Tracer("conversation").Start(ctx, "IncrChannelSeq")
    defer span.End()

    result, err := s.doIncrChannelSeq(ctx, req)
    if err != nil {
        observability.RecordError(span, err) // seq 分配失败是严重错误
        return nil, err
    }
    return result, nil
}
```

### 接入要点总结

| 步骤 | 改动位置 | 效果 |
|------|---------|------|
| 1. `observability.InitOpenTelemetry` | main() | TracerProvider + MeterProvider + Runtime Metrics |
| 2. gRPC Server Interceptor | Server | RPC 自动 trace + RED 指标 |
| 3. 自定义业务指标 | IncrChannelSeq/GetTotalUnread | seq 分配延迟、未读计算耗时 |
| 4. Kafka + 日志 + EventHeader | `observability.BuildEventHeader` | 链路追踪 + trace_id 贯穿 |
| 5. 基础设施指标 | `RegisterRedisPoolMetrics` | Redis 连接池健康（seq 分配的命脉） |
| 6. Span 错误记录 | `observability.RecordError` | 错误 trace 在 Tempo 可见 |
