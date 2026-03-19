# Ack 消息确认/已读回执服务 — RPC 接口实现伪代码

## 概述

Ack 服务负责管理消息的已读状态和已读回执，是 IM 系统中保障消息可达性和用户体验的关键服务。
核心机制：每个用户在每个会话中维护一个 `read_seq`（已读位置），标记用户已读到的消息序号；对于群聊消息，额外跟踪每条消息的已读人数和已读用户列表。

**核心设计原则：**
- **read_seq 只进不退**：已读位置只能向前推进，绝不后退，避免消息状态回退
- **群聊已读回执**：使用 Redis SET 追踪每条消息的已读用户列表，支持百万级群消息已读统计
- **投递确认**：`ConfirmDelivered` 由 Push 服务在消息成功投递到客户端后调用，用于更新消息投递状态
- **批量操作**：`BatchSendReadAck` 在 App 启动时批量同步已读状态，减少网络请求
- **多端同步**：用户在一台设备上标记已读后，其他设备通过 Push 服务收到已读同步信号

## 外部依赖

| 依赖类型 | 依赖项 | 用途 |
|---------|--------|------|
| PgSQL | read_receipts 表 | 会话已读位置持久化 |
| PgSQL | group_msg_reads 表 | 群消息已读用户记录 |
| PgSQL | delivery_records 表 | 消息投递确认记录 |
| Redis | 已读位置缓存 / 群已读计数 / 群已读用户集合 / 投递状态 / 批量去重 | 高频读写、快速查询 |
| RPC | Message.GetMessage | 获取消息详情（conversation_id、seq、sender_id），用于已读状态判断和已读用户列表查询 |
| RPC | Conversation.SetReadSeq | 更新 Conversation 服务中的用户已读位置 |
| RPC | Conversation.ClearUnread | 清除会话未读数 |
| RPC | Conversation.GetChannelMaxSeq | 获取会话最大 seq，用于校验 read_seq 合法性 |
| RPC | Group.GetGroupMemberIDs | 群消息已读回执场景，获取群成员 ID 列表（轻量接口，仅返回 ID） |
| Kafka | ack.read.updated / ack.read.group / ack.delivered | 事件通知下游服务 |

## PgSQL 表结构

```sql
-- 会话已读位置表（每个用户每个会话一条记录）
CREATE TABLE read_receipts (
    id          BIGSERIAL PRIMARY KEY,
    user_id     VARCHAR(64)  NOT NULL,               -- 用户 ID
    channel_id  VARCHAR(64)  NOT NULL,               -- 会话通道 ID
    read_seq    BIGINT       NOT NULL DEFAULT 0,     -- 用户在该会话的已读 seq
    read_time   BIGINT       NOT NULL DEFAULT 0,     -- 最后一次标记已读时间戳（毫秒）
    updated_at  BIGINT       NOT NULL DEFAULT 0,     -- 更新时间戳（毫秒）
    UNIQUE(user_id, channel_id)
);
CREATE INDEX idx_read_receipts_user ON read_receipts(user_id);
CREATE INDEX idx_read_receipts_channel ON read_receipts(channel_id);

-- 群消息已读用户记录表
CREATE TABLE group_msg_reads (
    id          BIGSERIAL PRIMARY KEY,
    msg_id      VARCHAR(64)  NOT NULL,               -- 消息 ID
    channel_id  VARCHAR(64)  NOT NULL,               -- 群会话通道 ID
    user_id     VARCHAR(64)  NOT NULL,               -- 已读用户 ID
    read_time   BIGINT       NOT NULL DEFAULT 0,     -- 已读时间戳（毫秒）
    UNIQUE(msg_id, user_id)
);
CREATE INDEX idx_group_msg_reads_msg ON group_msg_reads(msg_id);
CREATE INDEX idx_group_msg_reads_channel ON group_msg_reads(channel_id);

-- 消息投递确认记录表
CREATE TABLE delivery_records (
    id           BIGSERIAL PRIMARY KEY,
    msg_id       VARCHAR(64)  NOT NULL,              -- 消息 ID
    user_id      VARCHAR(64)  NOT NULL,              -- 接收者用户 ID
    device_id    VARCHAR(128) NOT NULL DEFAULT '',   -- 投递设备 ID
    delivered_at BIGINT       NOT NULL DEFAULT 0,    -- 投递确认时间戳（毫秒）
    UNIQUE(msg_id, user_id, device_id)
);
CREATE INDEX idx_delivery_records_msg ON delivery_records(msg_id);
CREATE INDEX idx_delivery_records_user ON delivery_records(user_id);
```

## Redis Key 设计

| Key 格式 | 类型 | 值 | TTL | 说明 |
|----------|------|-----|-----|------|
| `ack:read_seq:{user_id}:{channel_id}` | STRING | read_seq | 24h | 用户在某会话的已读 seq 缓存 |
| `ack:group_read_count:{msg_id}` | STRING | count | 7d | 群消息已读人数计数 |
| `ack:group_read_users:{msg_id}` | SET | user_id 集合 | 7d | 群消息已读用户 ID 集合 |
| `ack:delivered:{msg_id}:{user_id}` | STRING | "1" | 7d | 消息投递确认标记 |
| `ack:batch_dedup:{request_id}` | STRING | "1" | 1min | 批量操作幂等去重 |
| `ack:kafka:dedup:{event_id}` | STRING | "1" | 24h | Kafka 事件消费幂等去重 |

---

## 接口实现

### 1. SendReadAck — 上报单聊已读

> 用户标记某会话已读到指定 seq。核心逻辑：校验 read_seq 合法性 → 只进不退更新 → 写 PgSQL → 更新 Redis → 通知 Conversation 清除未读 → 投递 Kafka 事件通知消息发送者。

```go
func (s *AckService) SendReadAck(ctx context.Context, req *pb.SendReadAckRequest) (*pb.SendReadAckResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id is required")
    }
    if req.ConversationId == "" {
        return nil, status.Error(codes.InvalidArgument, "conversation_id is required")
    }
    if req.ReadSeq <= 0 {
        return nil, status.Error(codes.InvalidArgument, "read_seq must be positive")
    }

    now := time.Now().UnixMilli()

    // ==================== 2. 校验 read_seq 不超过会话最大 seq — RPC ====================
    // 防止客户端传入非法的超大 seq
    maxSeqResp, err := s.conversationClient.GetChannelMaxSeq(ctx, &conversation_pb.GetChannelMaxSeqRequest{
        ChannelId: req.ConversationId,
    })
    if err != nil {
        log.Error("get channel max_seq failed", "channel_id", req.ConversationId, "err", err)
        // 容错：如果 Conversation 服务不可用，允许继续处理（降级）
    } else if req.ReadSeq > maxSeqResp.MaxSeq {
        // read_seq 超过最大 seq，截断到 max_seq
        req.ReadSeq = maxSeqResp.MaxSeq
    }

    // ==================== 3. read_seq 只进不退检查 — Redis ====================
    cacheKey := fmt.Sprintf("ack:read_seq:%s:%s", req.UserId, req.ConversationId)
    currentSeqStr, err := s.redis.Get(ctx, cacheKey).Result()
    if err == nil && currentSeqStr != "" {
        currentSeq, _ := strconv.ParseInt(currentSeqStr, 10, 64)
        if req.ReadSeq <= currentSeq {
            // 新 read_seq 不大于当前值，无需更新，直接返回成功
            return &pb.SendReadAckResponse{
                Meta: successMeta(ctx),
            }, nil
        }
    }

    // ==================== 4. 写入/更新 PgSQL — UPSERT ====================
    _, err = s.db.ExecContext(ctx,
        `INSERT INTO read_receipts (user_id, channel_id, read_seq, read_time, updated_at)
         VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT (user_id, channel_id) DO UPDATE
         SET read_seq = GREATEST(read_receipts.read_seq, EXCLUDED.read_seq),
             read_time = EXCLUDED.read_time,
             updated_at = EXCLUDED.updated_at
         WHERE EXCLUDED.read_seq > read_receipts.read_seq`,
        req.UserId, req.ConversationId, req.ReadSeq, now, now,
    )
    if err != nil {
        return nil, status.Error(codes.Internal, "upsert read_receipt failed: "+err.Error())
    }

    // ==================== 5. 更新 Redis 缓存 ====================
    // 使用 Lua 脚本保证原子性：只在新值 > 旧值时更新
    luaScript := `
        local key = KEYS[1]
        local newSeq = tonumber(ARGV[1])
        local ttl = tonumber(ARGV[2])
        local current = tonumber(redis.call('GET', key) or '0')
        if newSeq > current then
            redis.call('SET', key, ARGV[1], 'EX', ttl)
            return 1
        end
        return 0
    `
    updated, err := s.redis.Eval(ctx, luaScript, []string{cacheKey}, req.ReadSeq, 86400).Int()
    if err != nil {
        log.Warn("redis update read_seq failed", "user_id", req.UserId, "channel_id", req.ConversationId, "err", err)
    }

    // 如果没有实际更新（并发场景下被其他请求先更新了），直接返回
    if updated == 0 {
        return &pb.SendReadAckResponse{
            Meta: successMeta(ctx),
        }, nil
    }

    // ==================== 6. 通知 Conversation 服务更新已读位置和未读数 — RPC ====================
    go func() {
        // 6a. 设置 Conversation 的 read_seq
        _, err := s.conversationClient.SetReadSeq(context.Background(), &conversation_pb.SetReadSeqRequest{
            UserId:    req.UserId,
            ChannelId: req.ConversationId,
            ReadSeq:   req.ReadSeq,
        })
        if err != nil {
            log.Error("Conversation.SetReadSeq failed",
                "user_id", req.UserId, "channel_id", req.ConversationId, "err", err)
        }

        // 6b. 清除未读数
        _, err = s.conversationClient.ClearUnread(context.Background(), &conversation_pb.ClearUnreadRequest{
            UserId:    req.UserId,
            ChannelId: req.ConversationId,
        })
        if err != nil {
            log.Error("Conversation.ClearUnread failed",
                "user_id", req.UserId, "channel_id", req.ConversationId, "err", err)
        }
    }()

    // ==================== 7. 投递 Kafka 事件: ack.read.updated ====================
    event := &kafka_ack.AckReadEvent{
        Header:         buildEventHeader("ack", s.instanceID),
        UserId:         req.UserId,
        ConversationId: req.ConversationId,
        ReadSeq:        req.ReadSeq,
        ReadTime:       now,
        DeviceId:       req.DeviceId,
    }
    if err = s.kafka.Produce(ctx, "ack.read.updated", req.ConversationId, event); err != nil {
        log.Error("produce ack.read.updated event failed",
            "user_id", req.UserId, "channel_id", req.ConversationId, "err", err)
    }

    // ==================== 8. 返回 ====================
    return &pb.SendReadAckResponse{
        Meta: successMeta(ctx),
    }, nil
}
```

### 2. BatchSendReadAck — 批量上报多个会话已读

> App 启动时调用，一次性同步多个会话的已读状态。使用批量去重防止重复请求，逐个会话调用 SendReadAck 核心逻辑。

```go
func (s *AckService) BatchSendReadAck(ctx context.Context, req *pb.BatchSendReadAckRequest) (*pb.BatchSendReadAckResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id is required")
    }
    if len(req.ConversationIds) == 0 {
        return nil, status.Error(codes.InvalidArgument, "conversation_ids is required")
    }
    if len(req.ConversationIds) > 200 {
        return nil, status.Error(codes.InvalidArgument, "conversation_ids too many, max 200")
    }
    if len(req.ReadSeqs) == 0 {
        return nil, status.Error(codes.InvalidArgument, "read_seqs is required")
    }

    // ==================== 2. 批量幂等去重 — Redis ====================
    // 使用 request_id（由 user_id + 时间窗口生成）防止短时间内重复批量请求
    requestID := fmt.Sprintf("%s:%d", req.UserId, time.Now().UnixMilli()/60000) // 1 分钟窗口
    dedupKey := fmt.Sprintf("ack:batch_dedup:%s", requestID)
    set, err := s.redis.SetNX(ctx, dedupKey, "1", 1*time.Minute).Result()
    if err != nil {
        log.Warn("redis batch dedup check failed", "user_id", req.UserId, "err", err)
        // Redis 失败不阻塞主流程
    } else if !set {
        // 1 分钟内重复批量请求，直接返回成功
        return &pb.BatchSendReadAckResponse{
            Meta: successMeta(ctx),
        }, nil
    }

    now := time.Now().UnixMilli()

    // ==================== 3. 批量读取当前 read_seq — Redis Pipeline ====================
    pipe := s.redis.Pipeline()
    cacheKeys := make([]string, 0, len(req.ConversationIds))
    for _, convID := range req.ConversationIds {
        key := fmt.Sprintf("ack:read_seq:%s:%s", req.UserId, convID)
        cacheKeys = append(cacheKeys, key)
        pipe.Get(ctx, key)
    }
    results, _ := pipe.Exec(ctx)

    // 收集需要更新的会话（过滤掉无需更新的）
    type updateItem struct {
        ConversationID string
        NewReadSeq     int64
    }
    var needUpdate []updateItem

    for i, convID := range req.ConversationIds {
        newSeq, exists := req.ReadSeqs[convID]
        if !exists || newSeq <= 0 {
            continue
        }

        // 检查当前 read_seq
        var currentSeq int64
        if i < len(results) {
            if strCmd, ok := results[i].(*redis.StringCmd); ok {
                if val, err := strCmd.Result(); err == nil {
                    currentSeq, _ = strconv.ParseInt(val, 10, 64)
                }
            }
        }

        // 只进不退
        if newSeq > currentSeq {
            needUpdate = append(needUpdate, updateItem{
                ConversationID: convID,
                NewReadSeq:     newSeq,
            })
        }
    }

    if len(needUpdate) == 0 {
        return &pb.BatchSendReadAckResponse{
            Meta: successMeta(ctx),
        }, nil
    }

    // ==================== 4. 批量 UPSERT PgSQL ====================
    // 使用事务批量写入
    tx, err := s.db.BeginTx(ctx, nil)
    if err != nil {
        return nil, status.Error(codes.Internal, "begin transaction failed")
    }
    defer tx.Rollback()

    stmt, err := tx.PrepareContext(ctx,
        `INSERT INTO read_receipts (user_id, channel_id, read_seq, read_time, updated_at)
         VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT (user_id, channel_id) DO UPDATE
         SET read_seq = GREATEST(read_receipts.read_seq, EXCLUDED.read_seq),
             read_time = EXCLUDED.read_time,
             updated_at = EXCLUDED.updated_at
         WHERE EXCLUDED.read_seq > read_receipts.read_seq`,
    )
    if err != nil {
        return nil, status.Error(codes.Internal, "prepare statement failed")
    }
    defer stmt.Close()

    for _, item := range needUpdate {
        _, err = stmt.ExecContext(ctx, req.UserId, item.ConversationID, item.NewReadSeq, now, now)
        if err != nil {
            log.Error("batch upsert read_receipt failed",
                "user_id", req.UserId, "channel_id", item.ConversationID, "err", err)
            // 单条失败不影响其他，继续处理
        }
    }

    if err = tx.Commit(); err != nil {
        return nil, status.Error(codes.Internal, "commit transaction failed")
    }

    // ==================== 5. 批量更新 Redis 缓存 — Pipeline ====================
    writePipe := s.redis.Pipeline()
    for _, item := range needUpdate {
        key := fmt.Sprintf("ack:read_seq:%s:%s", req.UserId, item.ConversationID)
        writePipe.Set(ctx, key, item.NewReadSeq, 24*time.Hour)
    }
    if _, err = writePipe.Exec(ctx); err != nil {
        log.Warn("redis batch update read_seq failed", "user_id", req.UserId, "err", err)
    }

    // ==================== 6. 批量通知 Conversation 服务 — 异步 RPC ====================
    go func() {
        for _, item := range needUpdate {
            // 设置 read_seq
            _, err := s.conversationClient.SetReadSeq(context.Background(), &conversation_pb.SetReadSeqRequest{
                UserId:    req.UserId,
                ChannelId: item.ConversationID,
                ReadSeq:   item.NewReadSeq,
            })
            if err != nil {
                log.Error("Conversation.SetReadSeq failed in batch",
                    "user_id", req.UserId, "channel_id", item.ConversationID, "err", err)
            }

            // 清除未读数
            _, err = s.conversationClient.ClearUnread(context.Background(), &conversation_pb.ClearUnreadRequest{
                UserId:    req.UserId,
                ChannelId: item.ConversationID,
            })
            if err != nil {
                log.Error("Conversation.ClearUnread failed in batch",
                    "user_id", req.UserId, "channel_id", item.ConversationID, "err", err)
            }
        }
    }()

    // ==================== 7. 批量投递 Kafka 事件: ack.read.updated ====================
    for _, item := range needUpdate {
        event := &kafka_ack.AckReadEvent{
            Header:         buildEventHeader("ack", s.instanceID),
            UserId:         req.UserId,
            ConversationId: item.ConversationID,
            ReadSeq:        item.NewReadSeq,
            ReadTime:       now,
            DeviceId:       req.DeviceId,
        }
        if err = s.kafka.Produce(ctx, "ack.read.updated", item.ConversationID, event); err != nil {
            log.Error("produce ack.read.updated event failed in batch",
                "user_id", req.UserId, "channel_id", item.ConversationID, "err", err)
        }
    }

    // ==================== 8. 返回 ====================
    return &pb.BatchSendReadAckResponse{
        Meta: successMeta(ctx),
    }, nil
}
```

### 3. SendGroupReadAck — 上报群聊已读

> 用户在群聊中标记消息已读。除了更新 read_seq（与单聊逻辑相同），还需要更新群消息的已读计数和已读用户列表。

```go
func (s *AckService) SendGroupReadAck(ctx context.Context, req *pb.SendGroupReadAckRequest) (*pb.SendGroupReadAckResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id is required")
    }
    if req.GroupId == "" {
        return nil, status.Error(codes.InvalidArgument, "group_id is required")
    }
    if req.ConversationId == "" {
        return nil, status.Error(codes.InvalidArgument, "conversation_id is required")
    }
    if req.ReadSeq <= 0 {
        return nil, status.Error(codes.InvalidArgument, "read_seq must be positive")
    }

    now := time.Now().UnixMilli()

    // ==================== 2. 校验 read_seq 不超过最大 seq — RPC ====================
    maxSeqResp, err := s.conversationClient.GetChannelMaxSeq(ctx, &conversation_pb.GetChannelMaxSeqRequest{
        ChannelId: req.ConversationId,
    })
    if err != nil {
        log.Error("get channel max_seq failed", "channel_id", req.ConversationId, "err", err)
    } else if req.ReadSeq > maxSeqResp.MaxSeq {
        req.ReadSeq = maxSeqResp.MaxSeq
    }

    // ==================== 3. read_seq 只进不退检查 — Redis ====================
    cacheKey := fmt.Sprintf("ack:read_seq:%s:%s", req.UserId, req.ConversationId)
    currentSeqStr, err := s.redis.Get(ctx, cacheKey).Result()
    var currentSeq int64
    if err == nil && currentSeqStr != "" {
        currentSeq, _ = strconv.ParseInt(currentSeqStr, 10, 64)
    }

    seqAdvanced := req.ReadSeq > currentSeq

    // ==================== 4. 更新会话 read_seq — PgSQL UPSERT ====================
    if seqAdvanced {
        _, err = s.db.ExecContext(ctx,
            `INSERT INTO read_receipts (user_id, channel_id, read_seq, read_time, updated_at)
             VALUES ($1, $2, $3, $4, $5)
             ON CONFLICT (user_id, channel_id) DO UPDATE
             SET read_seq = GREATEST(read_receipts.read_seq, EXCLUDED.read_seq),
                 read_time = EXCLUDED.read_time,
                 updated_at = EXCLUDED.updated_at
             WHERE EXCLUDED.read_seq > read_receipts.read_seq`,
            req.UserId, req.ConversationId, req.ReadSeq, now, now,
        )
        if err != nil {
            return nil, status.Error(codes.Internal, "upsert read_receipt failed: "+err.Error())
        }

        // 更新 Redis read_seq 缓存
        s.redis.Set(ctx, cacheKey, req.ReadSeq, 24*time.Hour)
    }

    // ==================== 5. 更新群消息已读记录 — PgSQL + Redis ====================
    // 如果指定了具体消息 ID，记录该消息的已读用户
    if req.MsgId != "" {
        // 5a. 写入 group_msg_reads 表
        _, err = s.db.ExecContext(ctx,
            `INSERT INTO group_msg_reads (msg_id, channel_id, user_id, read_time)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (msg_id, user_id) DO NOTHING`,
            req.MsgId, req.ConversationId, req.UserId, now,
        )
        if err != nil {
            log.Error("insert group_msg_reads failed",
                "msg_id", req.MsgId, "user_id", req.UserId, "err", err)
            // 不阻塞主流程
        }

        // 5b. 更新 Redis 群消息已读用户集合（原子操作）
        readUsersKey := fmt.Sprintf("ack:group_read_users:%s", req.MsgId)
        added, err := s.redis.SAdd(ctx, readUsersKey, req.UserId).Result()
        if err != nil {
            log.Warn("redis SADD group_read_users failed", "msg_id", req.MsgId, "err", err)
        }
        // 设置 TTL（仅在 key 不存在 TTL 时设置）
        s.redis.Expire(ctx, readUsersKey, 7*24*time.Hour)

        // 5c. 如果用户是新增的已读者，更新已读计数
        if added > 0 {
            readCountKey := fmt.Sprintf("ack:group_read_count:%s", req.MsgId)
            s.redis.Incr(ctx, readCountKey)
            s.redis.Expire(ctx, readCountKey, 7*24*time.Hour)
        }
    }

    // ==================== 6. 通知 Conversation 服务 — 异步 RPC ====================
    if seqAdvanced {
        go func() {
            _, err := s.conversationClient.SetReadSeq(context.Background(), &conversation_pb.SetReadSeqRequest{
                UserId:    req.UserId,
                ChannelId: req.ConversationId,
                ReadSeq:   req.ReadSeq,
            })
            if err != nil {
                log.Error("Conversation.SetReadSeq failed",
                    "user_id", req.UserId, "channel_id", req.ConversationId, "err", err)
            }

            _, err = s.conversationClient.ClearUnread(context.Background(), &conversation_pb.ClearUnreadRequest{
                UserId:    req.UserId,
                ChannelId: req.ConversationId,
            })
            if err != nil {
                log.Error("Conversation.ClearUnread failed",
                    "user_id", req.UserId, "channel_id", req.ConversationId, "err", err)
            }
        }()
    }

    // ==================== 7. 投递 Kafka 事件 ====================
    // 7a. ack.read.updated — 通知 Conversation 更新未读数, Push 推送已读同步
    if seqAdvanced {
        readEvent := &kafka_ack.AckReadEvent{
            Header:         buildEventHeader("ack", s.instanceID),
            UserId:         req.UserId,
            ConversationId: req.ConversationId,
            ReadSeq:        req.ReadSeq,
            ReadTime:       now,
            DeviceId:       req.DeviceId,
        }
        if err = s.kafka.Produce(ctx, "ack.read.updated", req.ConversationId, readEvent); err != nil {
            log.Error("produce ack.read.updated failed",
                "user_id", req.UserId, "channel_id", req.ConversationId, "err", err)
        }
    }

    // 7b. ack.read.group — 通知 Push 推送群已读状态变更
    if req.MsgId != "" {
        // 获取当前已读人数
        readCountKey := fmt.Sprintf("ack:group_read_count:%s", req.MsgId)
        readCount, _ := s.redis.Get(ctx, readCountKey).Int()

        groupReadEvent := &kafka_ack.AckGroupReadEvent{
            Header:      buildEventHeader("ack", s.instanceID),
            GroupId:     req.GroupId,
            MsgId:       req.MsgId,
            ReadUserIds: []string{req.UserId},
            ReadCount:   int32(readCount),
        }
        if err = s.kafka.Produce(ctx, "ack.read.group", req.MsgId, groupReadEvent); err != nil {
            log.Error("produce ack.read.group failed",
                "msg_id", req.MsgId, "group_id", req.GroupId, "err", err)
        }
    }

    // ==================== 8. 返回 ====================
    return &pb.SendGroupReadAckResponse{
        Meta: successMeta(ctx),
    }, nil
}
```

### 4. GetReadStatus — 查询消息已读状态

> 查询某条消息对于查询者是否已读。单聊场景下查会话 read_seq；群聊场景下查 group_msg_reads。

```go
func (s *AckService) GetReadStatus(ctx context.Context, req *pb.GetReadStatusRequest) (*pb.GetReadStatusResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.MsgId == "" {
        return nil, status.Error(codes.InvalidArgument, "msg_id is required")
    }
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id is required")
    }

    // ==================== 2. 优先查 Redis 群消息已读用户集合 ====================
    // 如果该消息在群消息已读集合中，说明是群消息
    readUsersKey := fmt.Sprintf("ack:group_read_users:%s", req.MsgId)
    isMember, err := s.redis.SIsMember(ctx, readUsersKey, req.UserId).Result()
    if err == nil && isMember {
        // 用户在该消息的已读集合中 → 已读
        // 从 group_msg_reads 获取精确已读时间
        var readTime int64
        _ = s.db.QueryRowContext(ctx,
            `SELECT read_time FROM group_msg_reads WHERE msg_id = $1 AND user_id = $2`,
            req.MsgId, req.UserId,
        ).Scan(&readTime)

        return &pb.GetReadStatusResponse{
            Meta:     successMeta(ctx),
            IsRead:   true,
            ReadTime: readTime,
        }, nil
    }

    // ==================== 3. 查 PgSQL group_msg_reads（Redis 未命中或非群消息） ====================
    var readTime int64
    err = s.db.QueryRowContext(ctx,
        `SELECT read_time FROM group_msg_reads WHERE msg_id = $1 AND user_id = $2`,
        req.MsgId, req.UserId,
    ).Scan(&readTime)
    if err == nil {
        // 在 group_msg_reads 中找到记录
        // 回写 Redis
        s.redis.SAdd(ctx, readUsersKey, req.UserId)
        s.redis.Expire(ctx, readUsersKey, 7*24*time.Hour)

        return &pb.GetReadStatusResponse{
            Meta:     successMeta(ctx),
            IsRead:   true,
            ReadTime: readTime,
        }, nil
    }

    // ==================== 4. 非群消息场景：基于会话 read_seq 判断 ====================
    // 需要知道消息所在的会话和 seq，通过 Message RPC 获取（不直接访问 messages 表）
    msgResp, err := s.messageClient.GetMessage(ctx, &message_pb.GetMessageRequest{
        MsgId: req.MsgId,
    })
    if err != nil {
        st, ok := status.FromError(err)
        if ok && st.Code() == codes.NotFound {
            return nil, status.Error(codes.NotFound, "message not found")
        }
        return nil, status.Error(codes.Internal, "get message via RPC failed: "+err.Error())
    }
    if msgResp.Message == nil || msgResp.Message.Status != common.MessageStatus_NORMAL {
        return nil, status.Error(codes.NotFound, "message not found or deleted")
    }
    channelID := msgResp.Message.ConversationId
    msgSeq := msgResp.Message.Seq

    // 获取用户在该会话的 read_seq
    userReadSeq, err := s.getUserReadSeq(ctx, req.UserId, channelID)
    if err != nil {
        return nil, status.Error(codes.Internal, "get user read_seq failed: "+err.Error())
    }

    isRead := userReadSeq >= msgSeq
    var resultReadTime int64
    if isRead {
        // 从 read_receipts 获取已读时间
        _ = s.db.QueryRowContext(ctx,
            `SELECT read_time FROM read_receipts WHERE user_id = $1 AND channel_id = $2`,
            req.UserId, channelID,
        ).Scan(&resultReadTime)
    }

    return &pb.GetReadStatusResponse{
        Meta:     successMeta(ctx),
        IsRead:   isRead,
        ReadTime: resultReadTime,
    }, nil
}

// getUserReadSeq — 获取用户在某会话的 read_seq（Redis 优先，回查 PgSQL）
func (s *AckService) getUserReadSeq(ctx context.Context, userID, channelID string) (int64, error) {
    // 优先读 Redis
    cacheKey := fmt.Sprintf("ack:read_seq:%s:%s", userID, channelID)
    val, err := s.redis.Get(ctx, cacheKey).Result()
    if err == nil && val != "" {
        seq, _ := strconv.ParseInt(val, 10, 64)
        return seq, nil
    }

    // Redis 未命中 → 读 PgSQL
    var readSeq int64
    err = s.db.QueryRowContext(ctx,
        `SELECT read_seq FROM read_receipts WHERE user_id = $1 AND channel_id = $2`,
        userID, channelID,
    ).Scan(&readSeq)
    if err == sql.ErrNoRows {
        return 0, nil // 从未读过
    }
    if err != nil {
        return 0, fmt.Errorf("query read_receipts failed: %w", err)
    }

    // 回写 Redis 缓存
    s.redis.Set(ctx, cacheKey, readSeq, 24*time.Hour)
    return readSeq, nil
}
```

### 5. GetConversationReadSeq — 获取用户在某会话的已读位置

> 获取用户在某个会话中的 read_seq 和该会话的 max_seq，用于客户端判断未读消息数量。

```go
func (s *AckService) GetConversationReadSeq(ctx context.Context, req *pb.GetConversationReadSeqRequest) (*pb.GetConversationReadSeqResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id is required")
    }
    if req.ConversationId == "" {
        return nil, status.Error(codes.InvalidArgument, "conversation_id is required")
    }

    // ==================== 2. 获取用户 read_seq — Redis 优先 ====================
    readSeq, err := s.getUserReadSeq(ctx, req.UserId, req.ConversationId)
    if err != nil {
        return nil, status.Error(codes.Internal, "get read_seq failed: "+err.Error())
    }

    // ==================== 3. 获取会话 max_seq — RPC Conversation ====================
    var maxSeq int64
    maxSeqResp, err := s.conversationClient.GetChannelMaxSeq(ctx, &conversation_pb.GetChannelMaxSeqRequest{
        ChannelId: req.ConversationId,
    })
    if err != nil {
        log.Error("get channel max_seq failed", "channel_id", req.ConversationId, "err", err)
        // 降级：max_seq 返回 0，客户端可自行从消息列表推算
    } else {
        maxSeq = maxSeqResp.MaxSeq
    }

    // ==================== 4. 返回 ====================
    return &pb.GetConversationReadSeqResponse{
        Meta:    successMeta(ctx),
        ReadSeq: readSeq,
        MaxSeq:  maxSeq,
    }, nil
}
```

### 6. BatchGetConversationReadSeq — 批量获取多个会话的已读位置

> 批量查询用户在多个会话中的 read_seq 和 max_seq，减少客户端请求次数，通常在拉取会话列表时调用。

```go
func (s *AckService) BatchGetConversationReadSeq(ctx context.Context, req *pb.BatchGetConversationReadSeqRequest) (*pb.BatchGetConversationReadSeqResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id is required")
    }
    if len(req.ConversationIds) == 0 {
        return nil, status.Error(codes.InvalidArgument, "conversation_ids is required")
    }
    if len(req.ConversationIds) > 500 {
        return nil, status.Error(codes.InvalidArgument, "conversation_ids too many, max 500")
    }

    readSeqs := make(map[string]int64, len(req.ConversationIds))
    maxSeqs := make(map[string]int64, len(req.ConversationIds))

    // ==================== 2. 批量读取 read_seq — Redis Pipeline ====================
    pipe := s.redis.Pipeline()
    cacheKeys := make([]string, 0, len(req.ConversationIds))
    for _, convID := range req.ConversationIds {
        key := fmt.Sprintf("ack:read_seq:%s:%s", req.UserId, convID)
        cacheKeys = append(cacheKeys, key)
        pipe.Get(ctx, key)
    }
    results, _ := pipe.Exec(ctx)

    // 收集 Redis 命中和未命中的 conversation_ids
    var missedConvIDs []string
    for i, convID := range req.ConversationIds {
        if i < len(results) {
            if strCmd, ok := results[i].(*redis.StringCmd); ok {
                if val, err := strCmd.Result(); err == nil && val != "" {
                    seq, _ := strconv.ParseInt(val, 10, 64)
                    readSeqs[convID] = seq
                    continue
                }
            }
        }
        missedConvIDs = append(missedConvIDs, convID)
    }

    // ==================== 3. Redis 未命中 → 批量查 PgSQL ====================
    if len(missedConvIDs) > 0 {
        // 构建 IN 查询
        placeholders := make([]string, len(missedConvIDs))
        args := make([]interface{}, 0, len(missedConvIDs)+1)
        args = append(args, req.UserId)
        for i, convID := range missedConvIDs {
            placeholders[i] = fmt.Sprintf("$%d", i+2)
            args = append(args, convID)
        }

        query := fmt.Sprintf(
            `SELECT channel_id, read_seq FROM read_receipts
             WHERE user_id = $1 AND channel_id IN (%s)`,
            strings.Join(placeholders, ","),
        )
        rows, err := s.db.QueryContext(ctx, query, args...)
        if err != nil {
            return nil, status.Error(codes.Internal, "batch query read_receipts failed: "+err.Error())
        }
        defer rows.Close()

        // 写回 Redis 的 pipeline
        writePipe := s.redis.Pipeline()
        for rows.Next() {
            var channelID string
            var readSeq int64
            if err := rows.Scan(&channelID, &readSeq); err != nil {
                continue
            }
            readSeqs[channelID] = readSeq
            // 回写 Redis
            key := fmt.Sprintf("ack:read_seq:%s:%s", req.UserId, channelID)
            writePipe.Set(ctx, key, readSeq, 24*time.Hour)
        }
        if _, err := writePipe.Exec(ctx); err != nil {
            log.Warn("redis batch write-back read_seq failed", "user_id", req.UserId, "err", err)
        }

        // 未找到记录的会话，read_seq 默认为 0
        for _, convID := range missedConvIDs {
            if _, exists := readSeqs[convID]; !exists {
                readSeqs[convID] = 0
            }
        }
    }

    // ==================== 4. 批量获取 max_seq — 逐个 RPC（可优化为批量接口） ====================
    // 并发调用 Conversation.GetChannelMaxSeq
    var mu sync.Mutex
    var wg sync.WaitGroup

    // 限制并发数
    sem := make(chan struct{}, 20) // 最多 20 并发

    for _, convID := range req.ConversationIds {
        wg.Add(1)
        sem <- struct{}{}
        go func(cid string) {
            defer wg.Done()
            defer func() { <-sem }()

            resp, err := s.conversationClient.GetChannelMaxSeq(ctx, &conversation_pb.GetChannelMaxSeqRequest{
                ChannelId: cid,
            })
            if err != nil {
                log.Warn("get max_seq failed for channel", "channel_id", cid, "err", err)
                return
            }
            mu.Lock()
            maxSeqs[cid] = resp.MaxSeq
            mu.Unlock()
        }(convID)
    }
    wg.Wait()

    // ==================== 5. 返回 ====================
    return &pb.BatchGetConversationReadSeqResponse{
        Meta:     successMeta(ctx),
        ReadSeqs: readSeqs,
        MaxSeqs:  maxSeqs,
    }, nil
}
```

### 7. GetMessageReadUsers — 获取群消息已读/未读用户列表

> 获取某条群消息的已读用户列表和未读用户列表，用于群聊已读回执功能。需要交叉比对群成员列表和已读记录。

```go
func (s *AckService) GetMessageReadUsers(ctx context.Context, req *pb.GetMessageReadUsersRequest) (*pb.GetMessageReadUsersResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.MsgId == "" {
        return nil, status.Error(codes.InvalidArgument, "msg_id is required")
    }
    if req.GroupId == "" {
        return nil, status.Error(codes.InvalidArgument, "group_id is required")
    }

    // ==================== 2. 获取已读用户列表 — Redis 优先 ====================
    readUsersKey := fmt.Sprintf("ack:group_read_users:%s", req.MsgId)
    readUserIDs, err := s.redis.SMembers(ctx, readUsersKey).Result()
    if err != nil || len(readUserIDs) == 0 {
        // Redis 未命中或为空 → 回查 PgSQL
        rows, err := s.db.QueryContext(ctx,
            `SELECT user_id FROM group_msg_reads WHERE msg_id = $1`,
            req.MsgId,
        )
        if err != nil {
            return nil, status.Error(codes.Internal, "query group_msg_reads failed: "+err.Error())
        }
        defer rows.Close()

        readUserIDs = nil
        pipe := s.redis.Pipeline()
        for rows.Next() {
            var uid string
            if err := rows.Scan(&uid); err != nil {
                continue
            }
            readUserIDs = append(readUserIDs, uid)
            pipe.SAdd(ctx, readUsersKey, uid)
        }
        if len(readUserIDs) > 0 {
            pipe.Expire(ctx, readUsersKey, 7*24*time.Hour)
            // 更新已读计数
            readCountKey := fmt.Sprintf("ack:group_read_count:%s", req.MsgId)
            pipe.Set(ctx, readCountKey, len(readUserIDs), 7*24*time.Hour)
            if _, err := pipe.Exec(ctx); err != nil {
                log.Warn("redis write-back group_read_users failed", "msg_id", req.MsgId, "err", err)
            }
        }
    }

    // ==================== 3. 获取群成员列表 — RPC Group 服务 ====================
    // 通过 Message RPC 获取消息的 conversation_id 和 sender_id（不直接访问 messages 表）
    msgResp, err := s.messageClient.GetMessage(ctx, &message_pb.GetMessageRequest{
        MsgId: req.MsgId,
    })
    if err != nil {
        st, ok := status.FromError(err)
        if ok && st.Code() == codes.NotFound {
            return nil, status.Error(codes.NotFound, "message not found")
        }
        return nil, status.Error(codes.Internal, "get message via RPC failed: "+err.Error())
    }
    if msgResp.Message == nil {
        return nil, status.Error(codes.NotFound, "message not found")
    }
    channelID := msgResp.Message.ConversationId
    senderID := msgResp.Message.SenderId

    // 获取群所有成员 ID 列表（排除消息发送者本人）
    groupMembersResp, err := s.groupClient.GetGroupMemberIDs(ctx, &group_pb.GetGroupMemberIDsRequest{
        GroupId: req.GroupId,
    })
    if err != nil {
        return nil, status.Error(codes.Internal, "get group member ids failed: "+err.Error())
    }

    // ==================== 4. 计算已读/未读列表 ====================
    readSet := make(map[string]bool, len(readUserIDs))
    for _, uid := range readUserIDs {
        readSet[uid] = true
    }

    var readUsers []*common.UserBrief
    var unreadUsers []*common.UserBrief

    for _, memberID := range groupMembersResp.MemberIds {
        // 排除消息发送者本人
        if memberID == senderID {
            continue
        }

        userBrief := &common.UserBrief{
            UserId: memberID,
        }

        if readSet[memberID] {
            readUsers = append(readUsers, userBrief)
        } else {
            unreadUsers = append(unreadUsers, userBrief)
        }
    }

    // ==================== 5. 返回 ====================
    return &pb.GetMessageReadUsersResponse{
        Meta:        successMeta(ctx),
        ReadUsers:   readUsers,
        UnreadUsers: unreadUsers,
        ReadCount:   int32(len(readUsers)),
        UnreadCount: int32(len(unreadUsers)),
    }, nil
}
```

### 8. ConfirmDelivered — 消息投递确认

> Push 服务在消息成功投递到客户端设备后，回调此接口确认投递状态。用于追踪消息的投递链路，支持消息状态从"已发送"变为"已送达"。

```go
func (s *AckService) ConfirmDelivered(ctx context.Context, req *pb.ConfirmDeliveredRequest) (*pb.ConfirmDeliveredResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.MsgId == "" {
        return nil, status.Error(codes.InvalidArgument, "msg_id is required")
    }
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id is required")
    }
    if req.Timestamp <= 0 {
        req.Timestamp = time.Now().UnixMilli()
    }

    // ==================== 2. 幂等检查 — Redis ====================
    deliveredKey := fmt.Sprintf("ack:delivered:%s:%s", req.MsgId, req.UserId)
    set, err := s.redis.SetNX(ctx, deliveredKey, "1", 7*24*time.Hour).Result()
    if err != nil {
        log.Warn("redis delivered dedup check failed", "msg_id", req.MsgId, "user_id", req.UserId, "err", err)
        // Redis 失败不阻塞，继续处理
    } else if !set {
        // 已确认过投递，直接返回成功（幂等）
        return &pb.ConfirmDeliveredResponse{
            Meta: successMeta(ctx),
        }, nil
    }

    // ==================== 3. 获取设备信息用于记录 ====================
    // 从 ctx 或 metadata 中提取 device_id，实际场景下 Push 服务会携带
    deviceID := extractDeviceID(ctx)
    if deviceID == "" {
        deviceID = "unknown"
    }

    // ==================== 4. 写入 PgSQL — 投递记录 ====================
    _, err = s.db.ExecContext(ctx,
        `INSERT INTO delivery_records (msg_id, user_id, device_id, delivered_at)
         VALUES ($1, $2, $3, $4)
         ON CONFLICT (msg_id, user_id, device_id) DO NOTHING`,
        req.MsgId, req.UserId, deviceID, req.Timestamp,
    )
    if err != nil {
        log.Error("insert delivery_record failed",
            "msg_id", req.MsgId, "user_id", req.UserId, "err", err)
        return nil, status.Error(codes.Internal, "insert delivery_record failed: "+err.Error())
    }

    // ==================== 5. 投递 Kafka 事件: ack.delivered ====================
    // Push 服务消费此事件后，向消息发送者推送"消息已送达"状态更新
    event := &kafka_ack.AckReceivedEvent{
        Header:         buildEventHeader("ack", s.instanceID),
        MsgId:          req.MsgId,
        UserId:         req.UserId,
        AckTime:        req.Timestamp,
        DeviceId:       deviceID,
    }
    if err = s.kafka.Produce(ctx, "ack.delivered", req.MsgId, event); err != nil {
        log.Error("produce ack.delivered event failed",
            "msg_id", req.MsgId, "user_id", req.UserId, "err", err)
    }

    // ==================== 6. 返回 ====================
    return &pb.ConfirmDeliveredResponse{
        Meta: successMeta(ctx),
    }, nil
}
```

---

## 辅助函数

```go
// delayedDoubleDelete 延迟双删：先删除 Redis 缓存，500ms 后再删一次防止并发回填
func (s *AckService) delayedDoubleDelete(ctx context.Context, keys ...string) {
    if len(keys) == 0 { return }
    pipe := s.redis.Pipeline()
    for _, key := range keys { pipe.Del(ctx, key) }
    pipe.Exec(ctx)
    go func() {
        time.Sleep(500 * time.Millisecond)
        pipe := s.redis.Pipeline()
        for _, key := range keys { pipe.Del(context.Background(), key) }
        pipe.Exec(context.Background())
    }()
}

// extractDeviceID 从 gRPC metadata 中提取设备 ID
func extractDeviceID(ctx context.Context) string {
    md, ok := metadata.FromIncomingContext(ctx)
    if !ok {
        return ""
    }
    if vals := md.Get("x-device-id"); len(vals) > 0 {
        return vals[0]
    }
    return ""
}

// successMeta 构建成功响应元信息
func successMeta(ctx context.Context) *common.ResponseMeta {
    return &common.ResponseMeta{
        Code:       0,
        Message:    "success",
        ServerTime: time.Now().UnixMilli(),
        TraceId:    extractTraceID(ctx),
    }
}

// buildEventHeader 构建 Kafka 事件头
func buildEventHeader(source, instanceID string) *common.EventHeader {
    return &common.EventHeader{
        EventId:   uuid.NewString(),
        TraceId:   "",
        Source:    source,
        SourceId:  instanceID,
        Timestamp: time.Now().UnixMilli(),
        Version:   1,
    }
}
```

---

## 可观测性接入（OpenTelemetry）

> Ack 服务负责消息确认与已读回执，需重点观测：已读回执处理速率、群已读扇出耗时、投递确认延迟、跨服务 RPC 调用链。

### 第一步：服务启动时初始化 OTel SDK

在 `main()` 中调用统一初始化函数，创建 TracerProvider + MeterProvider + LoggerProvider，通过 OTLP gRPC 推送到 Collector，**不暴露任何监控端口**。

```go
func main() {
    ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
    defer cancel()

    otelShutdown := observability.InitOpenTelemetry(ctx, "ack", "v1.0.0")
    defer otelShutdown(ctx)
    observability.SetupLogger("ack")
    // ... 创建 gRPC Server、Kafka Producer 等
}
```

> 共享包 `InitOpenTelemetry` 的完整实现见 `observability_global.md` 第 1.1 节。

### 第二步：gRPC Server/Client 注册 OTel Interceptor

```go
// Server 端 — 自动为 SendReadAck / SendGroupReadAck 等所有 RPC 创建 span + 指标
grpcServer := grpc.NewServer(
    grpc.ChainUnaryInterceptor(otelgrpc.UnaryServerInterceptor()),
    grpc.ChainStreamInterceptor(otelgrpc.StreamServerInterceptor()),
)

// Client 端 — 调用 Conversation.SetReadSeq / Group.GetGroupMember / Push.PushToUser 时自动传播 trace
convConn := grpc.Dial("conversation:50051",
    grpc.WithChainUnaryInterceptor(otelgrpc.UnaryClientInterceptor()),
)
groupConn := grpc.Dial("group:50051",
    grpc.WithChainUnaryInterceptor(otelgrpc.UnaryClientInterceptor()),
)
pushConn := grpc.Dial("push:50051",
    grpc.WithChainUnaryInterceptor(otelgrpc.UnaryClientInterceptor()),
)
```

### 第三步：注册 Ack 服务专属业务指标

```go
var meter = otel.Meter("im-chat/ack")

var (
    // 已读回执处理计数
    readAckProcessed, _ = meter.Int64Counter("ack.read_ack_processed_total",
        metric.WithDescription("已读回执处理总数"))

    // 群已读回执扇出耗时（遍历群成员并发推送的总耗时）
    groupReadFanoutDuration, _ = meter.Float64Histogram("ack.group_read_fanout_duration_ms",
        metric.WithDescription("群已读回执扇出耗时"), metric.WithUnit("ms"))

    // 投递确认处理计数
    deliveryConfirmed, _ = meter.Int64Counter("ack.delivery_confirmed_total",
        metric.WithDescription("投递确认处理总数"))

    // read_seq 更新计数
    readSeqUpdated, _ = meter.Int64Counter("ack.read_seq_updated_total",
        metric.WithDescription("read_seq 更新次数"))
)
```

在业务代码中埋点：

```go
// SendReadAck 中
readAckProcessed.Add(ctx, 1, metric.WithAttributes(
    attribute.String("conversation_type", "single"),
))
readSeqUpdated.Add(ctx, 1)

// SendGroupReadAck 中 — 群已读扇出
fanoutStart := time.Now()
// ... 遍历群成员发送 Kafka 事件 ...
groupReadFanoutDuration.Record(ctx, float64(time.Since(fanoutStart).Milliseconds()))

// ConfirmDelivered 中
deliveryConfirmed.Add(ctx, 1)
```

### 第四步：Kafka Producer 埋点

发布 `ack.read.updated` / `ack.group.read` / `ack.delivery.confirmed` 事件时，创建 PRODUCER span + 注入 trace context 到 Kafka headers。实现方式与 message_impl.md 第四步一致。

### 第五步：结构化 JSON 日志

设置全局 `slog.JSONHandler` + traceLogHandler 包装，所有业务日志使用 `slog.InfoContext(ctx, ...)`。

### 第六步：改造 buildEventHeader

将本地 `buildEventHeader` 替换为共享包调用，从 `context.Context` 提取真实 trace_id/span_id：

```go
// 替代本地 buildEventHeader，使用共享包
header := observability.BuildEventHeader(ctx, "ack", instanceID)
```

### 补充：基础设施指标注册

Ack 服务依赖 Redis（读已读状态、缓存），需在 `main()` 中注册连接池指标：

```go
func main() {
    // ... initOpenTelemetry ...
    observability.RegisterRedisPoolMetrics(redisClient, "ack")
}
```

### 补充：Span 错误记录

在所有返回 error 的业务路径中记录 span 错误，使 Tempo 能正确显示错误 trace：

```go
func (s *AckServer) SendReadAck(ctx context.Context, req *pb.SendReadAckRequest) (*pb.SendReadAckResponse, error) {
    ctx, span := otel.Tracer("ack").Start(ctx, "SendReadAck")
    defer span.End()

    result, err := s.doSendReadAck(ctx, req)
    if err != nil {
        observability.RecordError(span, err) // ← 所有 error 路径
        return nil, err
    }
    return result, nil
}
```

### 接入要点总结

| 步骤 | 改动位置 | 效果 |
|------|---------|------|
| 1. `observability.InitOpenTelemetry` | main() | TracerProvider + MeterProvider + Runtime Metrics |
| 2. gRPC Interceptor | Server + Client | RPC 自动 trace + RED 指标 |
| 3. 自定义业务指标 | SendReadAck/SendGroupReadAck 等 | 回执处理速率、群已读扇出耗时 |
| 4. Kafka Producer 埋点 | 事件发布 | 链路追踪贯穿 |
| 5. 结构化 JSON 日志 | setupLogger | 所有日志自动带 trace_id |
| 6. buildEventHeader 改造 | `observability.BuildEventHeader` | EventHeader 携带真实 trace context |
| 7. 基础设施指标 | `RegisterRedisPoolMetrics` | Redis 连接池健康可观测 |
| 8. Span 错误记录 | `observability.RecordError` | 错误 trace 在 Tempo 可见 |
