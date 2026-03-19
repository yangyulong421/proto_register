# OfflineQueue 离线同步队列服务 — RPC 接口实现伪代码

## 概述

OfflineQueue 服务是 notify+sync 架构中的**离线信号暂存层**，负责在用户离线期间暂存轻量级同步信号（SyncSignalEntry）。  
当用户上线后，客户端批量拉取同步信号 → 了解哪些会话有新消息 → 再调用 `Message.PullMessages` 拉取真实消息内容。

**核心设计原则：**
- **极致性能**：`EnqueueSyncSignal` 是群消息扇出热路径，必须极快（Redis ZADD 直写，无 PgSQL）
- **双层存储**：Redis ZSET（热层，最多 1000 条）+ PgSQL（溢出层，超出部分落库）
- **幂等去重**：同一 signal_id 24h 内不重复入队
- **懒计数器**：`GetSyncSignalCount` 使用 Redis STRING 懒更新，不每次 ZCARD
- **自动过期**：信号默认 TTL 7 天，到期自动清理
- **职责边界**：OfflineQueue 只管信号存取，不感知推送通道（APNs/FCM 等由 Push 服务管理）

## 外部依赖

| 依赖类型 | 依赖项 | 用途 |
|---------|--------|------|
| PgSQL | offline_sync_signals 表 | 溢出信号持久化（Redis 队列超过 1000 条时） |
| Redis | 离线信号队列 / 计数器 / 溢出标记 / 去重 | 热路径读写 |
| RPC | 无（本服务被 Push 等服务调用，不主动调用其他 RPC） |
| Kafka | offline.queue.overflow / offline.stats | 溢出告警 + 统计上报 |

## PgSQL 表结构

```sql
-- 离线同步信号溢出表（Redis 队列超过 max_queue_size 时落库）
CREATE TABLE offline_sync_signals (
    id          BIGSERIAL    PRIMARY KEY,
    signal_id   VARCHAR(20)  NOT NULL,                  -- 同步信号唯一 ID（Snowflake）
    user_id     VARCHAR(20)  NOT NULL,                  -- 目标用户 ID
    signal_type INT          NOT NULL DEFAULT 1,        -- 信号类型枚举
    channel_id  VARCHAR(20)  NOT NULL,                  -- 会话通道 ID
    data        JSONB        NOT NULL DEFAULT '{}',     -- 信号完整 JSON（SyncSignalEntry 序列化）
    priority    INT          NOT NULL DEFAULT 0,        -- 优先级（0=普通，1=高优先）
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),    -- 入库时间
    expires_at  TIMESTAMPTZ  NOT NULL DEFAULT (NOW() + INTERVAL '7 days')  -- 过期时间
);

-- 核心查询索引：按用户拉取未过期信号（Dequeue 回查溢出数据时使用）
CREATE INDEX idx_offline_signals_user_created
    ON offline_sync_signals(user_id, created_at)
    WHERE expires_at > NOW();

-- 信号 ID 唯一索引（防止重复入库）
CREATE UNIQUE INDEX idx_offline_signals_signal_id
    ON offline_sync_signals(signal_id);

-- 过期清理索引（定时任务批量删除过期记录）
CREATE INDEX idx_offline_signals_expires
    ON offline_sync_signals(expires_at)
    WHERE expires_at <= NOW();
```

## Redis Key 设计

| Key 格式 | 类型 | 值 | TTL | 说明 |
|----------|------|-----|-----|------|
| `offline:queue:{user_id}` | ZSET | member=signal_json, score=timestamp（毫秒） | 7d | 离线信号有序队列，最多 1000 条 |
| `offline:count:{user_id}` | STRING | 信号数量 | 7d | 懒计数器，EnqueueSyncSignal/DequeueSyncSignals 时更新 |
| `offline:overflow:{user_id}` | STRING | "1" | 7d | 溢出标记，表示该用户有部分信号落库到 PgSQL |
| `offline:dedup:{user_id}:{signal_id}` | STRING | "1" | 24h | 入队幂等去重 |
| `offline:kafka:dedup:{event_id}` | STRING | "1" | 24h | Kafka 消费幂等去重 |

## 信号类型枚举（signal_type）

| 值 | 名称 | 说明 |
|----|------|------|
| 1 | MSG_SINGLE | 单聊新消息 |
| 2 | MSG_GROUP | 群聊新消息 |
| 3 | MSG_RECALL | 消息撤回 |
| 4 | FRIEND_REQUEST | 好友申请 |
| 5 | FRIEND_ACCEPTED | 好友通过 |
| 6 | GROUP_NOTIFICATION | 群组通知（入群/踢出/解散等） |
| 7 | SYSTEM_NOTIFICATION | 系统通知 |
| 8 | PRESENCE_CHANGE | 在线状态变更 |
| 9 | CONVERSATION_UPDATE | 会话更新 |

## 配置常量

| 常量 | 默认值 | 说明 |
|------|--------|------|
| MaxQueueSize | 1000 | 每个用户 Redis 队列最大信号数 |
| SignalTTL | 7d | 信号默认存活时间 |
| DedupTTL | 24h | 去重 Key 存活时间 |
| DefaultDequeueCount | 100 | 默认单次拉取数量 |
| MaxDequeueCount | 500 | 单次最大拉取数量 |
| OverflowAlertThreshold | 800 | 溢出预警阈值 |

---

## 接口实现

### 1. EnqueueSyncSignal — 入队同步信号（热路径）

> **性能关键路径**：群消息扇出时，Push 服务为每个离线成员调用此接口。  
> 必须极快：Redis ZADD 直写，绝不在热路径上访问 PgSQL。  
> 流程：参数校验 → 幂等去重 → Redis ZADD → 检查队列大小 → 溢出则异步落库 → 更新计数器 → 返回。

```go
func (s *OfflineQueueService) EnqueueSyncSignal(ctx context.Context, req *pb.EnqueueSyncSignalRequest) (*pb.EnqueueSyncSignalResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id is required")
    }
    if req.ConversationId == "" {
        return nil, status.Error(codes.InvalidArgument, "conversation_id is required")
    }
    if req.ConversationType == common.CONVERSATION_TYPE_UNSPECIFIED {
        return nil, status.Error(codes.InvalidArgument, "conversation_type is required")
    }
    if req.MaxSeq <= 0 {
        return nil, status.Error(codes.InvalidArgument, "max_seq must be positive")
    }

    now := time.Now().UnixMilli()

    // 过期时间：默认 7 天
    expireAt := req.ExpireAt
    if expireAt <= 0 {
        expireAt = now + 7*24*3600*1000 // 7天后过期
    }

    // ==================== 2. 生成 signal_id — Snowflake ====================
    signalID := s.snowflake.Generate().String()

    // ==================== 3. 幂等去重 — Redis ====================
    // Key: offline:dedup:{user_id}:{signal_id}
    // 注意：这里对同一会话的信号做去重——同一会话短时间内多条消息只保留最新 signal
    // 实际去重粒度可按 conversation_id 而非 signal_id，避免同一会话重复入队
    dedupKey := fmt.Sprintf("offline:dedup:%s:%s", req.UserId, req.ConversationId)
    set, err := s.redis.SetNX(ctx, dedupKey, "1", 5*time.Second).Result()
    if err != nil {
        // Redis 异常不阻断入队，记录日志继续
        log.Warn("幂等去重 Redis 异常，继续入队",
            "user_id", req.UserId, "conversation_id", req.ConversationId, "err", err)
    } else if !set {
        // 短时间内同一会话已有信号入队 → 更新已有信号的 max_seq（ZSET 中替换）
        // 通过删除旧的再插入新的实现更新
        // 注：此处选择直接追加新信号，客户端拉取时自行合并同一会话
        // 如果需要合并策略，可在此处实现
    }

    // ==================== 4. 构造信号 JSON ====================
    signalEntry := map[string]interface{}{
        "signal_id":         signalID,
        "conversation_id":   req.ConversationId,
        "conversation_type": int(req.ConversationType),
        "max_seq":           req.MaxSeq,
        "sender_id":         req.SenderId,
        "sender_name":       req.SenderName,
        "msg_type":          int(req.MsgType),
        "content_preview":   req.ContentPreview,
        "priority":          req.Priority,
        "enqueue_time":      now,
        "expire_at":         expireAt,
    }
    signalJSON, err := json.Marshal(signalEntry)
    if err != nil {
        return nil, status.Error(codes.Internal, "序列化信号失败")
    }

    // ==================== 5. Redis ZADD 入队 ====================
    // Key: offline:queue:{user_id}  ZSET member=signal_json score=timestamp
    queueKey := fmt.Sprintf("offline:queue:%s", req.UserId)

    // 使用 pipeline 减少 RTT
    pipe := s.redis.Pipeline()

    // 5a. ZADD 添加信号
    pipe.ZAdd(ctx, queueKey, &redis.Z{
        Score:  float64(now),
        Member: string(signalJSON),
    })

    // 5b. 设置/续期 TTL（7 天）
    pipe.Expire(ctx, queueKey, 7*24*time.Hour)

    // 5c. INCR 更新懒计数器
    countKey := fmt.Sprintf("offline:count:%s", req.UserId)
    pipe.Incr(ctx, countKey)
    pipe.Expire(ctx, countKey, 7*24*time.Hour)

    _, err = pipe.Exec(ctx)
    if err != nil {
        log.Error("Redis 入队失败",
            "user_id", req.UserId, "signal_id", signalID, "err", err)
        return nil, status.Error(codes.Internal, "入队失败: "+err.Error())
    }

    // ==================== 6. 检查队列大小 → 溢出处理 ====================
    queueSize, err := s.redis.ZCard(ctx, queueKey).Result()
    if err != nil {
        log.Warn("获取队列大小失败", "user_id", req.UserId, "err", err)
        queueSize = 0
    }

    if queueSize > int64(s.config.MaxQueueSize) {
        // 队列超限 → 将最旧的信号溢出到 PgSQL（异步，不阻塞热路径）
        go s.handleOverflow(context.Background(), req.UserId, queueKey, queueSize)
    }

    // ==================== 7. 返回 ====================
    return &pb.EnqueueSyncSignalResponse{
        Meta:       successMeta(ctx),
        SignalId:   signalID,
        QueueDepth: queueSize,
    }, nil
}

// handleOverflow 异步溢出处理：将 Redis 中超出 MaxQueueSize 的旧信号转移到 PgSQL
func (s *OfflineQueueService) handleOverflow(ctx context.Context, userID, queueKey string, currentSize int64) {
    // 计算需要溢出的数量
    overflowCount := currentSize - int64(s.config.MaxQueueSize)
    if overflowCount <= 0 {
        return
    }

    // 1. 从 ZSET 中取出最旧的 overflowCount 条信号（score 最小）
    signals, err := s.redis.ZRangeWithScores(ctx, queueKey, 0, overflowCount-1).Result()
    if err != nil {
        log.Error("溢出：读取旧信号失败", "user_id", userID, "err", err)
        return
    }

    // 2. 批量写入 PgSQL
    tx, err := s.db.BeginTx(ctx, nil)
    if err != nil {
        log.Error("溢出：开启事务失败", "user_id", userID, "err", err)
        return
    }
    defer tx.Rollback()

    stmt, err := tx.PrepareContext(ctx,
        `INSERT INTO offline_sync_signals (signal_id, user_id, signal_type, channel_id, data, priority, created_at, expires_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
         ON CONFLICT (signal_id) DO NOTHING`)
    if err != nil {
        log.Error("溢出：准备语句失败", "user_id", userID, "err", err)
        return
    }
    defer stmt.Close()

    var movedCount int
    for _, z := range signals {
        signalStr, ok := z.Member.(string)
        if !ok {
            continue
        }

        // 解析信号 JSON
        var entry map[string]interface{}
        if err := json.Unmarshal([]byte(signalStr), &entry); err != nil {
            log.Warn("溢出：解析信号 JSON 失败", "user_id", userID, "err", err)
            continue
        }

        signalID, _ := entry["signal_id"].(string)
        channelID, _ := entry["conversation_id"].(string)
        signalType, _ := entry["conversation_type"].(float64)
        priority, _ := entry["priority"].(float64)
        enqueueTime, _ := entry["enqueue_time"].(float64)
        expireAtMs, _ := entry["expire_at"].(float64)

        createdAt := time.UnixMilli(int64(enqueueTime))
        expiresAt := time.UnixMilli(int64(expireAtMs))

        _, err := stmt.ExecContext(ctx,
            signalID,          // $1
            userID,            // $2
            int(signalType),   // $3
            channelID,         // $4
            signalStr,         // $5: 完整 JSON
            int(priority),     // $6
            createdAt,         // $7
            expiresAt,         // $8
        )
        if err != nil {
            log.Warn("溢出：写入信号失败", "signal_id", signalID, "err", err)
            continue
        }
        movedCount++
    }

    if err := tx.Commit(); err != nil {
        log.Error("溢出：提交事务失败", "user_id", userID, "err", err)
        return
    }

    // 3. 从 Redis ZSET 中移除已溢出的信号
    members := make([]interface{}, len(signals))
    for i, z := range signals {
        members[i] = z.Member
    }
    s.redis.ZRem(ctx, queueKey, members...)

    // 4. 设置溢出标记
    // Key: offline:overflow:{user_id} = "1"  TTL: 7d
    overflowKey := fmt.Sprintf("offline:overflow:%s", userID)
    s.redis.Set(ctx, overflowKey, "1", 7*24*time.Hour)

    // 5. 更新懒计数器（减去已移出的数量，但总数不变因为落库了）
    // 计数器仍反映 Redis 中的数量，Dequeue 时会合并 PgSQL 数据
    countKey := fmt.Sprintf("offline:count:%s", userID)
    s.redis.DecrBy(ctx, countKey, int64(movedCount))

    // 6. 生产溢出告警事件 — Kafka
    overflowEvent := &kafka_offline.QueueOverflowEvent{
        Header:       buildEventHeader("offline_queue", s.instanceID),
        UserId:       userID,
        QueueSize:    int32(currentSize),
        MaxSize:      int32(s.config.MaxQueueSize),
        DroppedCount: int32(movedCount),
        OverflowTime: time.Now().UnixMilli(),
    }
    if err := s.kafka.Produce(ctx, "offline.queue.overflow", userID, overflowEvent); err != nil {
        log.Error("生产溢出告警事件失败", "user_id", userID, "err", err)
    }

    log.Info("溢出处理完成",
        "user_id", userID, "moved_count", movedCount, "remaining", currentSize-int64(movedCount))
}
```

---

### 2. BatchEnqueueSyncSignal — 批量入队同步信号（群消息扇出）

> 群消息场景下，Push 服务为所有离线成员批量入队。  
> 使用 Redis Pipeline 批量 ZADD，极大减少 RTT。  
> **性能关键**：单次调用可能涉及数百甚至上千个离线用户。

```go
func (s *OfflineQueueService) BatchEnqueueSyncSignal(ctx context.Context, req *pb.BatchEnqueueSyncSignalRequest) (*pb.BatchEnqueueSyncSignalResponse, error) {
    // ==================== 1. 参数校验 ====================
    if len(req.UserIds) == 0 {
        return nil, status.Error(codes.InvalidArgument, "user_ids is required")
    }
    if req.ConversationId == "" {
        return nil, status.Error(codes.InvalidArgument, "conversation_id is required")
    }
    if req.ConversationType == common.CONVERSATION_TYPE_UNSPECIFIED {
        return nil, status.Error(codes.InvalidArgument, "conversation_type is required")
    }
    if req.MaxSeq <= 0 {
        return nil, status.Error(codes.InvalidArgument, "max_seq must be positive")
    }
    if len(req.UserIds) > 10000 {
        return nil, status.Error(codes.InvalidArgument, "user_ids 超过上限 10000")
    }

    now := time.Now().UnixMilli()
    expireAt := req.ExpireAt
    if expireAt <= 0 {
        expireAt = now + 7*24*3600*1000
    }

    var successCount, failCount int32

    // ==================== 2. 分批处理（每批 200 个用户，避免单次 Pipeline 过大） ====================
    batchSize := 200
    for i := 0; i < len(req.UserIds); i += batchSize {
        end := i + batchSize
        if end > len(req.UserIds) {
            end = len(req.UserIds)
        }
        batch := req.UserIds[i:end]

        // 使用 Redis Pipeline 批量操作
        pipe := s.redis.Pipeline()

        signalEntries := make(map[string]string) // userID → signalJSON

        for _, userID := range batch {
            if userID == "" {
                failCount++
                continue
            }

            // 为每个用户生成独立的 signal_id
            signalID := s.snowflake.Generate().String()

            signalEntry := map[string]interface{}{
                "signal_id":         signalID,
                "conversation_id":   req.ConversationId,
                "conversation_type": int(req.ConversationType),
                "max_seq":           req.MaxSeq,
                "sender_id":         req.SenderId,
                "sender_name":       req.SenderName,
                "msg_type":          int(req.MsgType),
                "content_preview":   req.ContentPreview,
                "priority":          req.Priority,
                "enqueue_time":      now,
                "expire_at":         expireAt,
            }
            signalJSON, err := json.Marshal(signalEntry)
            if err != nil {
                failCount++
                continue
            }

            signalEntries[userID] = string(signalJSON)

            queueKey := fmt.Sprintf("offline:queue:%s", userID)
            countKey := fmt.Sprintf("offline:count:%s", userID)

            // Pipeline: ZADD + EXPIRE + INCR
            pipe.ZAdd(ctx, queueKey, &redis.Z{
                Score:  float64(now),
                Member: string(signalJSON),
            })
            pipe.Expire(ctx, queueKey, 7*24*time.Hour)
            pipe.Incr(ctx, countKey)
            pipe.Expire(ctx, countKey, 7*24*time.Hour)
        }

        // 执行 Pipeline
        results, err := pipe.Exec(ctx)
        if err != nil {
            log.Error("批量入队 Pipeline 执行异常",
                "batch_start", i, "batch_size", len(batch), "err", err)
            // Pipeline 部分失败：遍历结果统计
            for _, r := range results {
                if r.Err() != nil {
                    failCount++
                }
            }
        }

        // 统计成功数（ZADD 返回 1 表示新增，0 表示已存在被更新，都算成功）
        successCount += int32(len(batch)) - failCount

        // ==================== 3. 异步检查溢出 ====================
        go func(userIDs []string) {
            for _, userID := range userIDs {
                queueKey := fmt.Sprintf("offline:queue:%s", userID)
                queueSize, err := s.redis.ZCard(context.Background(), queueKey).Result()
                if err != nil {
                    continue
                }
                if queueSize > int64(s.config.MaxQueueSize) {
                    s.handleOverflow(context.Background(), userID, queueKey, queueSize)
                }
            }
        }(batch)
    }

    // ==================== 4. 返回 ====================
    return &pb.BatchEnqueueSyncSignalResponse{
        Meta:         successMeta(ctx),
        SuccessCount: successCount,
        FailCount:    failCount,
    }, nil
}
```

---

### 3. DequeueSyncSignals — 出队同步信号（用户上线拉取）

> 用户上线时调用，拉取并移除离线期间积累的同步信号。  
> 需要合并 Redis 队列和 PgSQL 溢出数据（如果有溢出标记）。  
> 按 priority DESC, enqueue_time ASC 排序返回。

> **⚠️ 重连风暴防护**：10M 用户同时在线场景下，网络抖动可能导致大量用户在短时间内同时重连。
> 客户端应在重连时添加 **随机抖动延迟（1~5 秒 jitter）** 再调用 DequeueSyncSignals，
> 避免所有客户端在同一瞬间触发 dequeue，形成 Redis + PgSQL 读取风暴。
> 服务端配合：如果检测到 QPS 超过阈值（如 5 万/s），启用自适应限流，返回 RESOURCE_EXHAUSTED 让客户端退避重试。

```go
// Lua 脚本预编译（启动时加载）— 原子化 ZRANGE + ZREM + DECRBY
// KEYS[1] = queueKey (offline:queue:{user_id})
// KEYS[2] = countKey (offline:count:{user_id})
// ARGV[1] = 拉取数量
// 返回：拉取到的 member 列表
var dequeueScript = redis.NewScript(`
    local queue_key = KEYS[1]
    local count_key = KEYS[2]
    local limit = tonumber(ARGV[1])

    -- 1. ZRANGE 按 score 升序拉取 limit 个
    local members = redis.call('ZRANGE', queue_key, 0, limit - 1)
    if #members == 0 then
        return {}
    end

    -- 2. ZREM 移除已拉取的 member
    redis.call('ZREM', queue_key, unpack(members))

    -- 3. DECRBY 扣减计数器
    local new_count = redis.call('DECRBY', count_key, #members)
    if new_count < 0 then
        -- 计数器漂移修正：以 ZCARD 为准
        local actual = redis.call('ZCARD', queue_key)
        redis.call('SET', count_key, actual)
    end

    return members
`)

```go
func (s *OfflineQueueService) DequeueSyncSignals(ctx context.Context, req *pb.DequeueSyncSignalsRequest) (*pb.DequeueSyncSignalsResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id is required")
    }
    count := int(req.Count)
    if count <= 0 {
        count = s.config.DefaultDequeueCount // 默认 100
    }
    if count > s.config.MaxDequeueCount {
        count = s.config.MaxDequeueCount // 最大 500
    }

    queueKey := fmt.Sprintf("offline:queue:%s", req.UserId)
    overflowKey := fmt.Sprintf("offline:overflow:%s", req.UserId)

    // ==================== 2. 检查是否有溢出数据 — Redis ====================
    hasOverflow, _ := s.redis.Exists(ctx, overflowKey).Result()

    var allSignals []*pb.SyncSignalEntry

    // ==================== 3. 从 PgSQL 拉取溢出数据（如果有） ====================
    if hasOverflow > 0 {
        rows, err := s.db.QueryContext(ctx,
            `SELECT signal_id, channel_id, signal_type, data, priority, created_at, expires_at
             FROM offline_sync_signals
             WHERE user_id = $1 AND expires_at > NOW()
             ORDER BY priority DESC, created_at ASC
             LIMIT $2`,
            req.UserId, count,
        )
        if err != nil {
            log.Error("查询溢出信号失败", "user_id", req.UserId, "err", err)
            // 不中断，继续读 Redis 数据
        } else {
            defer rows.Close()
            var pgSignalIDs []string

            for rows.Next() {
                var signalID, channelID, dataStr string
                var signalType, priority int
                var createdAt, expiresAt time.Time

                if err := rows.Scan(&signalID, &channelID, &signalType, &dataStr, &priority, &createdAt, &expiresAt); err != nil {
                    log.Warn("扫描溢出信号行失败", "err", err)
                    continue
                }

                // 解析 data JSON → SyncSignalEntry
                entry := parseSyncSignalEntry(dataStr, signalID)
                if entry != nil {
                    allSignals = append(allSignals, entry)
                    pgSignalIDs = append(pgSignalIDs, signalID)
                }
            }

            // 删除已拉取的溢出记录
            if len(pgSignalIDs) > 0 {
                go func(ids []string) {
                    _, err := s.db.ExecContext(context.Background(),
                        `DELETE FROM offline_sync_signals WHERE signal_id = ANY($1)`,
                        pq.Array(ids),
                    )
                    if err != nil {
                        log.Error("删除溢出信号失败", "user_id", req.UserId, "count", len(ids), "err", err)
                    }

                    // 检查是否还有溢出数据，没有则清除溢出标记
                    var remaining int
                    s.db.QueryRowContext(context.Background(),
                        `SELECT COUNT(*) FROM offline_sync_signals WHERE user_id = $1 AND expires_at > NOW()`,
                        req.UserId,
                    ).Scan(&remaining)
                    if remaining == 0 {
                        s.redis.Del(context.Background(), overflowKey)
                    }
                }(pgSignalIDs)
            }
        }
    }

    // ==================== 4. 从 Redis ZSET 拉取信号（Lua 脚本原子操作） ====================
    // ⚠️ HIGH-05 修复：将 ZRANGE + ZREM + DECRBY 合并为 Lua 脚本，保证原子性
    // 避免并发场景下：拉取后未删除（重复消费）或删除后未扣减计数器（计数漂移）
    remainCount := count - len(allSignals)
    if remainCount <= 0 {
        remainCount = 0
    }

    if remainCount > 0 {
        // Lua 脚本：原子执行 ZRANGE + ZREM + DECRBY
        // 返回：拉取到的 member 列表（JSON 字符串数组）
        result, err := dequeueScript.Run(ctx, s.redis, []string{queueKey, countKey}, remainCount).StringSlice()
        if err != nil && err != redis.Nil {
            log.Error("Redis Lua 脚本拉取信号失败", "user_id", req.UserId, "err", err)
            if len(allSignals) == 0 {
                return nil, status.Error(codes.Internal, "拉取信号失败")
            }
        } else {
            for _, signalStr := range result {
                entry := parseSyncSignalFromJSON(signalStr)
                if entry != nil {
                    allSignals = append(allSignals, entry)
                }
            }
        }
    }

    // ==================== 5. 检查是否还有更多 ====================
    remainingRedis, _ := s.redis.ZCard(ctx, queueKey).Result()
    hasMore := remainingRedis > 0 || hasOverflow > 0

    // ==================== 6. 更新懒计数器（仅 PgSQL 溢出部分） ====================
    // ⚠️ Redis ZSET 部分的 DECRBY 已在 Lua 脚本中原子执行，此处仅需处理 PgSQL 溢出数据的计数扣减
    if hasOverflow > 0 && len(allSignals) > 0 {
        // 溢出数据的数量需要单独扣减（这些数据不在 Lua 脚本范围内）
        pgSignalCount := len(allSignals) - int(remainCount) // PgSQL 部分的数量
        if pgSignalCount < 0 {
            pgSignalCount = 0
        }
        if pgSignalCount > 0 {
            newCount := s.redis.DecrBy(ctx, countKey, int64(pgSignalCount)).Val()
            if newCount < 0 {
                actualCount, _ := s.redis.ZCard(ctx, queueKey).Result()
                s.redis.Set(ctx, countKey, actualCount, 7*24*time.Hour)
            }
        }
    }

    // 如果全部拉完，清理 key
    if !hasMore {
        pipe := s.redis.Pipeline()
        pipe.Del(ctx, queueKey)
        pipe.Del(ctx, countKey)
        pipe.Del(ctx, overflowKey)
        pipe.Exec(ctx)
    }

    // ==================== 7. 返回 ====================
    return &pb.DequeueSyncSignalsResponse{
        Meta:    successMeta(ctx),
        Signals: allSignals,
        HasMore: hasMore,
    }, nil
}

// parseSyncSignalEntry 从 PgSQL data JSON 解析 SyncSignalEntry
func parseSyncSignalEntry(dataStr string, signalID string) *pb.SyncSignalEntry {
    var data map[string]interface{}
    if err := json.Unmarshal([]byte(dataStr), &data); err != nil {
        return nil
    }
    return &pb.SyncSignalEntry{
        SignalId:         signalID,
        ConversationId:   getStr(data, "conversation_id"),
        ConversationType: common.ConversationType(getInt(data, "conversation_type")),
        MaxSeq:           getInt64(data, "max_seq"),
        SenderId:         getStr(data, "sender_id"),
        SenderName:       getStr(data, "sender_name"),
        MsgType:          common.MessageType(getInt(data, "msg_type")),
        ContentPreview:   getStr(data, "content_preview"),
        Priority:         int32(getInt(data, "priority")),
        EnqueueTime:      getInt64(data, "enqueue_time"),
        ExpireAt:         getInt64(data, "expire_at"),
    }
}

// parseSyncSignalFromJSON 从 Redis ZSET member JSON 解析 SyncSignalEntry
func parseSyncSignalFromJSON(jsonStr string) *pb.SyncSignalEntry {
    return parseSyncSignalEntry(jsonStr, "")
}
```

---

### 4. GetSyncSignalCount — 获取待同步信号数量

> 返回用户离线队列中的待同步信号数量。使用 Redis 懒计数器快速返回。  
> 如果计数器不存在，回退到 ZCARD + PgSQL COUNT 校准。

```go
func (s *OfflineQueueService) GetSyncSignalCount(ctx context.Context, req *pb.GetSyncSignalCountRequest) (*pb.GetSyncSignalCountResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id is required")
    }

    // ==================== 2. 读取懒计数器 — Redis ====================
    countKey := fmt.Sprintf("offline:count:%s", req.UserId)
    countStr, err := s.redis.Get(ctx, countKey).Result()

    if err == nil && countStr != "" {
        // 命中懒计数器
        count, _ := strconv.ParseInt(countStr, 10, 64)
        if count >= 0 {
            return &pb.GetSyncSignalCountResponse{
                Meta:  successMeta(ctx),
                Count: count,
            }, nil
        }
    }

    // ==================== 3. 懒计数器不存在 → 回退校准 ====================
    queueKey := fmt.Sprintf("offline:queue:%s", req.UserId)
    overflowKey := fmt.Sprintf("offline:overflow:%s", req.UserId)

    // 3a. Redis ZSET 数量
    redisCount, err := s.redis.ZCard(ctx, queueKey).Result()
    if err != nil {
        log.Warn("ZCARD 失败", "user_id", req.UserId, "err", err)
        redisCount = 0
    }

    // 3b. PgSQL 溢出数量（如果有溢出标记）
    var pgCount int64
    hasOverflow, _ := s.redis.Exists(ctx, overflowKey).Result()
    if hasOverflow > 0 {
        err = s.db.QueryRowContext(ctx,
            `SELECT COUNT(*) FROM offline_sync_signals
             WHERE user_id = $1 AND expires_at > NOW()`,
            req.UserId,
        ).Scan(&pgCount)
        if err != nil {
            log.Warn("查询溢出数量失败", "user_id", req.UserId, "err", err)
            pgCount = 0
        }
    }

    totalCount := redisCount + pgCount

    // 3c. 回填懒计数器
    s.redis.Set(ctx, countKey, totalCount, 7*24*time.Hour)

    // ==================== 4. 返回 ====================
    return &pb.GetSyncSignalCountResponse{
        Meta:  successMeta(ctx),
        Count: totalCount,
    }, nil
}
```

---

### 5. ClearSyncSignals — 清空所有离线同步信号

> 清空用户的所有离线信号（如完整同步后、用户注销时调用）。  
> 同时清理 Redis 队列 + PgSQL 溢出数据 + 所有相关 Key。

```go
func (s *OfflineQueueService) ClearSyncSignals(ctx context.Context, req *pb.ClearSyncSignalsRequest) (*pb.ClearSyncSignalsResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id is required")
    }

    // ==================== 2. 清理 Redis — Pipeline ====================
    queueKey := fmt.Sprintf("offline:queue:%s", req.UserId)
    countKey := fmt.Sprintf("offline:count:%s", req.UserId)
    overflowKey := fmt.Sprintf("offline:overflow:%s", req.UserId)

    pipe := s.redis.Pipeline()
    pipe.Del(ctx, queueKey)
    pipe.Del(ctx, countKey)
    pipe.Del(ctx, overflowKey)
    _, err := pipe.Exec(ctx)
    if err != nil {
        log.Error("清理 Redis 队列失败", "user_id", req.UserId, "err", err)
        // 不中断，继续清理 PgSQL
    }

    // ==================== 3. 清理 PgSQL 溢出数据 ====================
    result, err := s.db.ExecContext(ctx,
        `DELETE FROM offline_sync_signals WHERE user_id = $1`,
        req.UserId,
    )
    if err != nil {
        log.Error("清理 PgSQL 溢出数据失败", "user_id", req.UserId, "err", err)
        return nil, status.Error(codes.Internal, "清理溢出数据失败")
    }
    deletedRows, _ := result.RowsAffected()

    log.Info("清空用户离线信号完成",
        "user_id", req.UserId, "pg_deleted", deletedRows)

    // ==================== 4. 返回 ====================
    return &pb.ClearSyncSignalsResponse{
        Meta: successMeta(ctx),
    }, nil
}
```

---

### 6. RemoveSyncSignal — 移除指定同步信号

> 移除特定会话的同步信号（如消息撤回时，移除该撤回消息对应的离线信号）。  
> 从 Redis ZSET 中按 conversation_id 匹配移除，同时清理 PgSQL 溢出数据。

```go
func (s *OfflineQueueService) RemoveSyncSignal(ctx context.Context, req *pb.RemoveSyncSignalRequest) (*pb.RemoveSyncSignalResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id is required")
    }
    if req.ConversationId == "" {
        return nil, status.Error(codes.InvalidArgument, "conversation_id is required")
    }

    queueKey := fmt.Sprintf("offline:queue:%s", req.UserId)

    // ==================== 2. 从 Redis ZSET 中查找并移除匹配的信号 ====================
    // ZSET 的 member 是 JSON 字符串，需要遍历匹配 conversation_id
    // 注意：这里不适合在超大队列上使用 ZRANGE 全量遍历，但 MaxQueueSize=1000，可接受
    allMembers, err := s.redis.ZRange(ctx, queueKey, 0, -1).Result()
    if err != nil {
        log.Error("读取 Redis 队列失败", "user_id", req.UserId, "err", err)
        return nil, status.Error(codes.Internal, "读取队列失败")
    }

    var toRemove []interface{}
    for _, member := range allMembers {
        var entry map[string]interface{}
        if err := json.Unmarshal([]byte(member), &entry); err != nil {
            continue
        }

        convID, _ := entry["conversation_id"].(string)
        msgID, _ := entry["msg_id"].(string)

        // 匹配 conversation_id
        if convID == req.ConversationId {
            // 如果指定了 msg_id，精确匹配
            if req.MsgId != "" && msgID != "" && msgID != req.MsgId {
                continue
            }
            toRemove = append(toRemove, member)
        }
    }

    // 移除匹配的信号
    removedCount := 0
    if len(toRemove) > 0 {
        removed, err := s.redis.ZRem(ctx, queueKey, toRemove...).Result()
        if err != nil {
            log.Error("从 Redis 移除信号失败", "user_id", req.UserId, "err", err)
        } else {
            removedCount = int(removed)
        }
    }

    // ==================== 3. 从 PgSQL 移除溢出数据 ====================
    overflowKey := fmt.Sprintf("offline:overflow:%s", req.UserId)
    hasOverflow, _ := s.redis.Exists(ctx, overflowKey).Result()

    if hasOverflow > 0 {
        var pgResult sql.Result
        var err error
        if req.MsgId != "" {
            // 精确匹配 signal 中的 msg_id（通过 JSONB 查询）
            pgResult, err = s.db.ExecContext(ctx,
                `DELETE FROM offline_sync_signals
                 WHERE user_id = $1 AND channel_id = $2
                   AND data->>'msg_id' = $3`,
                req.UserId, req.ConversationId, req.MsgId,
            )
        } else {
            // 移除该会话的所有溢出信号
            pgResult, err = s.db.ExecContext(ctx,
                `DELETE FROM offline_sync_signals
                 WHERE user_id = $1 AND channel_id = $2`,
                req.UserId, req.ConversationId,
            )
        }
        if err != nil {
            log.Error("从 PgSQL 移除溢出信号失败", "user_id", req.UserId, "err", err)
        } else {
            pgRemoved, _ := pgResult.RowsAffected()
            removedCount += int(pgRemoved)
        }
    }

    // ==================== 4. 更新懒计数器 ====================
    if removedCount > 0 {
        countKey := fmt.Sprintf("offline:count:%s", req.UserId)
        newCount := s.redis.DecrBy(ctx, countKey, int64(removedCount)).Val()
        if newCount < 0 {
            // 校准
            actualCount, _ := s.redis.ZCard(ctx, queueKey).Result()
            s.redis.Set(ctx, countKey, actualCount, 7*24*time.Hour)
        }
    }

    log.Info("移除指定信号完成",
        "user_id", req.UserId, "conversation_id", req.ConversationId,
        "msg_id", req.MsgId, "removed_count", removedCount)

    // ==================== 5. 返回 ====================
    return &pb.RemoveSyncSignalResponse{
        Meta: successMeta(ctx),
    }, nil
}
```

---

### 7. GetQueueStats — 获取队列统计信息（运维监控）

> 返回全局离线队列统计数据，用于运维监控面板。  
> 扫描 Redis 中所有 offline:queue:* 的 key，并汇总 PgSQL 溢出数据。  
> 注意：此接口不在热路径上，允许使用 SCAN + 聚合查询。

```go
func (s *OfflineQueueService) GetQueueStats(ctx context.Context, req *pb.GetQueueStatsRequest) (*pb.GetQueueStatsResponse, error) {
    // ==================== 1. 扫描 Redis 获取队列统计 ====================
    var totalSignals int64
    var totalUsers int64
    var maxDepth int64
    var allDepths []int64

    // 使用 SCAN 遍历 offline:queue:* 键
    var cursor uint64
    for {
        keys, nextCursor, err := s.redis.Scan(ctx, cursor, "offline:queue:*", 200).Result()
        if err != nil {
            log.Error("SCAN Redis 队列键失败", "cursor", cursor, "err", err)
            break
        }

        for _, key := range keys {
            depth, err := s.redis.ZCard(ctx, key).Result()
            if err != nil {
                continue
            }
            if depth > 0 {
                totalUsers++
                totalSignals += depth
                allDepths = append(allDepths, depth)
                if depth > maxDepth {
                    maxDepth = depth
                }
            }
        }

        cursor = nextCursor
        if cursor == 0 {
            break
        }
    }

    // ==================== 2. 查询 PgSQL 溢出统计 ====================
    var pgTotalSignals, pgTotalUsers int64
    err := s.db.QueryRowContext(ctx,
        `SELECT COALESCE(COUNT(*), 0), COALESCE(COUNT(DISTINCT user_id), 0)
         FROM offline_sync_signals
         WHERE expires_at > NOW()`,
    ).Scan(&pgTotalSignals, &pgTotalUsers)
    if err != nil {
        log.Warn("查询 PgSQL 溢出统计失败", "err", err)
    }

    // 合并统计
    totalSignals += pgTotalSignals
    // totalUsers 可能有重叠（同一用户既有 Redis 又有 PgSQL 数据），这里简单相加
    // 精确去重需要额外查询，监控场景下近似值足够

    // ==================== 3. 计算平均队列深度 ====================
    var avgDepth int64
    if totalUsers > 0 {
        avgDepth = totalSignals / totalUsers
    }

    // ==================== 4. 生产统计事件 — Kafka ====================
    statsEvent := &kafka_offline.OfflineQueueStatsEvent{
        Header:         buildEventHeader("offline_queue", s.instanceID),
        TotalEnqueued:  totalSignals,
        TotalDequeued:  0, // 需要从指标系统获取，此处仅快照
        TotalOverflow:  pgTotalSignals,
        PendingUsers:   totalUsers,
        PendingSignals: totalSignals,
        StatsTime:      time.Now().UnixMilli(),
        PeriodSeconds:  0, // 非周期性，按需调用
    }
    if err := s.kafka.Produce(ctx, "offline.stats", "stats", statsEvent); err != nil {
        log.Error("生产统计事件失败", "err", err)
    }

    // ==================== 5. 返回 ====================
    return &pb.GetQueueStatsResponse{
        Meta:          successMeta(ctx),
        TotalSignals:  totalSignals,
        TotalUsers:    totalUsers,
        AvgQueueDepth: avgDepth,
        MaxQueueDepth: maxDepth,
    }, nil
}
```

---

## 可观测性接入（OpenTelemetry）

> OfflineQueue 服务负责离线消息队列管理，需重点观测：队列深度、入队/出队速率、带离线消息的用户数、排队超时清理次数、Redis ZSet 操作延迟。

### 第一步：服务启动时初始化 OTel SDK

```go
func main() {
    ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
    defer cancel()

    otelShutdown := observability.InitOpenTelemetry(ctx, "offline_queue", "v1.0.0")
    defer otelShutdown(ctx)
    observability.SetupLogger("offline_queue")
}
```

### 第二步：gRPC Server Interceptor（OfflineQueue 不依赖其他 RPC）

```go
grpcServer := grpc.NewServer(
    grpc.ChainUnaryInterceptor(otelgrpc.UnaryServerInterceptor()),
    grpc.ChainStreamInterceptor(otelgrpc.StreamServerInterceptor()),
)
```

### 第三步：注册 OfflineQueue 专属业务指标

```go
var meter = otel.Meter("im-chat/offline_queue")

var (
    // 信号入队计数
    signalEnqueued, _ = meter.Int64Counter("offline_queue.signal_enqueued_total",
        metric.WithDescription("离线信号入队总数"))

    // 信号出队计数
    signalDequeued, _ = meter.Int64Counter("offline_queue.signal_dequeued_total",
        metric.WithDescription("离线信号出队总数"))

    // 队列深度快照（定期采集）
    queueDepth, _ = meter.Int64Histogram("offline_queue.queue_depth",
        metric.WithDescription("用户离线队列深度分布"))

    // 带离线队列的用户数
    usersWithQueue, _ = meter.Int64Gauge("offline_queue.users_with_queue",
        metric.WithDescription("当前带离线队列的用户数"))

    // 超时清理次数
    expiredCleaned, _ = meter.Int64Counter("offline_queue.expired_cleaned_total",
        metric.WithDescription("超时信号清理总数"))

    // Redis ZSet 操作延迟
    zsetOpDuration, _ = meter.Float64Histogram("offline_queue.zset_op_duration_ms",
        metric.WithDescription("Redis ZSet 操作延迟"), metric.WithUnit("ms"))
)
```

在业务代码中埋点：

```go
// EnqueueSignal 中
zsetStart := time.Now()
err := s.redis.ZAdd(ctx, queueKey, redis.Z{Score: float64(seq), Member: signalID}).Err()
zsetOpDuration.Record(ctx, float64(time.Since(zsetStart).Milliseconds()),
    metric.WithAttributes(attribute.String("op", "ZADD")))
signalEnqueued.Add(ctx, 1)

// PullOfflineSignals 中
zsetStart := time.Now()
results, _ := s.redis.ZRangeByScore(ctx, queueKey, ...).Result()
zsetOpDuration.Record(ctx, float64(time.Since(zsetStart).Milliseconds()),
    metric.WithAttributes(attribute.String("op", "ZRANGEBYSCORE")))
signalDequeued.Add(ctx, int64(len(results)))
queueDepth.Record(ctx, int64(queueLen))

// CleanExpiredSignals 定时任务中
expiredCleaned.Add(ctx, int64(cleanedCount))
```

### 第四步：Kafka Producer 埋点 + 结构化 JSON 日志

发布 `offline.stats` 等事件时注入 trace context。

### 接入要点总结

| 步骤 | 改动位置 | 效果 |
|------|---------|------|
| 1. `observability.InitOpenTelemetry` | main() | TracerProvider + MeterProvider + Runtime Metrics |
| 2. gRPC Server Interceptor | Server | RPC 自动 trace + 指标 |
| 3. 自定义业务指标 | Enqueue/Pull/CleanExpired | 队列深度、入出队速率、ZSet 延迟 |
| 4. Kafka + 日志 | 事件发布 + setupLogger | 链路追踪 + trace_id 日志 |
| 5. buildEventHeader 改造 | 辅助函数 | EventHeader 携带真实 trace context |
| 6. 基础设施指标 | main() | Redis 连接池可观测 |
| 7. Span 错误记录 | `observability.RecordError` | 错误 trace 在 Tempo 可见 |

### 补充：buildEventHeader 改造

将 `buildEventHeader(source, instanceID)` 替换为 `observability.BuildEventHeader(ctx, source, instanceID)`。

### 补充：基础设施指标注册

```go
observability.RegisterRedisPoolMetrics(redisClient, "offline_queue")
```

### 补充：Span 错误记录

```go
func (s *OfflineQueueServer) EnqueueSignal(ctx context.Context, req *pb.EnqueueSignalRequest) (*pb.EnqueueSignalResponse, error) {
    ctx, span := otel.Tracer("offline_queue").Start(ctx, "EnqueueSignal")
    defer span.End()

    result, err := s.doEnqueueSignal(ctx, req)
    if err != nil {
        observability.RecordError(span, err)
        return nil, err
    }
    return result, nil
}
```
