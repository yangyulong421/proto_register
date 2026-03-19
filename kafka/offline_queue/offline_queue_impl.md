# OfflineQueue 离线同步队列服务 — Kafka 消费者/生产者实现伪代码

## 概述

OfflineQueue 服务是 Kafka 事件的**轻量消费者**，消费连接状态变更、消息撤回、用户注销、配置变更等事件，
触发离线队列的清理、信号移除、配置热更新等操作。同时包含定时任务（每日 4:00 AM 清理过期信号）。

**职责边界：**
- OfflineQueue 服务不主动调用其他 RPC 服务
- 入队操作由 Push 服务通过 RPC `EnqueueSyncSignal` / `BatchEnqueueSyncSignal` 调用完成
- Kafka 消费者侧重于**清理和维护**，而非入队

## 生产 Topic 列表

| Topic | 事件 | 生产时机 | Key | 消费方 |
|-------|------|----------|-----|--------|
| `offline.queue.overflow` | QueueOverflowEvent | Redis 队列超过 MaxQueueSize 时 | user_id | 监控告警 / Audit |
| `offline.stats` | OfflineQueueStatsEvent | GetQueueStats 调用时 / 定时统计 | "stats" | Audit（运营统计） |

## 消费 Topic 列表

| Topic | 来源 | 用途 | 并发度 | 重试 |
|-------|------|------|--------|------|
| `conn.connected` | Connecte | 用户上线 → 通知客户端拉取离线信号 | 4 | 3 |
| `conn.disconnected` | Connecte | 用户下线 → 记录日志（无实质操作） | 2 | 1 |
| `msg.recalled` | Message | 消息撤回 → 移除对应离线信号 | 4 | 3 |
| `user.deactivated` | User | 用户注销 → 清空所有离线信号 | 2 | 3 |
| `config.changed` | Config | 配置变更 → 热更新队列参数 | 1 | 1 |

## Redis Key 设计（消费者专用）

| Key 格式 | 类型 | 值 | TTL | 说明 |
|----------|------|-----|-----|------|
| `offline:kafka:dedup:{event_id}` | STRING | "1" | 24h | Kafka 消费幂等去重 |
| `offline:queue:{user_id}` | ZSET | member=signal_json, score=timestamp | 7d | 离线信号有序队列（共享） |
| `offline:count:{user_id}` | STRING | 信号数量 | 7d | 懒计数器（共享） |
| `offline:overflow:{user_id}` | STRING | "1" | 7d | 溢出标记（共享） |
| `offline:cleanup:last_run` | STRING | ISO8601 时间戳 | 无 | 上次清理任务运行时间 |

## 配置常量（消费者侧）

| 常量 | 默认值 | 说明 |
|------|--------|------|
| MaxQueueSize | 1000 | 每个用户 Redis 队列最大信号数 |
| SignalTTL | 7d | 信号默认存活时间 |
| CleanupCron | "0 4 * * *" | 过期信号清理 cron 表达式（每日 4:00 AM） |
| CleanupBatchSize | 500 | 清理任务每批处理数量 |
| OverflowThreshold | 1000 | 溢出阈值（超过则落库 PgSQL） |

---

## 消费者实现

### Consumer: `conn.connected`

> 来源：Connecte 长连接网关服务。用户建立长连接后触发。  
> 职责：检查该用户是否有待同步的离线信号，如果有，向客户端发送一个 "pull_offline" 提示，
> 通知客户端主动调用 `DequeueSyncSignals` RPC 拉取信号。  
> **注意：OfflineQueue 不直接推送信号给客户端，仅通知客户端来拉取（lazy pull 模式）。**

```go
func (c *OfflineQueueConsumer) HandleConnConnected(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_conn.ConnConnectedEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 ConnConnectedEvent 失败", "err", err)
        return nil // 反序列化失败不重试
    }

    // ==================== 2. 幂等检查 — Redis ====================
    // Key: offline:kafka:dedup:{event_id}  TTL: 24h
    dedupKey := fmt.Sprintf("offline:kafka:dedup:%s", event.Header.EventId)
    set, err := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result()
    if err != nil {
        return fmt.Errorf("Redis 幂等检查失败: %w", err)
    }
    if !set {
        log.Debug("重复事件，跳过", "event_id", event.Header.EventId)
        return nil
    }

    // ==================== 3. 参数校验 ====================
    if event.UserId == "" {
        log.Error("conn.connected: user_id 为空", "event_id", event.Header.EventId)
        return nil
    }

    // ==================== 4. 检查用户是否有待同步信号 — Redis ====================
    // 先查懒计数器
    countKey := fmt.Sprintf("offline:count:%s", event.UserId)
    countStr, err := c.redis.Get(ctx, countKey).Result()

    var pendingCount int64
    if err == nil && countStr != "" {
        pendingCount, _ = strconv.ParseInt(countStr, 10, 64)
    } else {
        // 懒计数器不存在 → 回退到 ZCARD
        queueKey := fmt.Sprintf("offline:queue:%s", event.UserId)
        pendingCount, err = c.redis.ZCard(ctx, queueKey).Result()
        if err != nil {
            log.Warn("ZCARD 失败", "user_id", event.UserId, "err", err)
            pendingCount = 0
        }

        // 检查 PgSQL 溢出
        overflowKey := fmt.Sprintf("offline:overflow:%s", event.UserId)
        hasOverflow, _ := c.redis.Exists(ctx, overflowKey).Result()
        if hasOverflow > 0 {
            var pgCount int64
            c.db.QueryRowContext(ctx,
                `SELECT COUNT(*) FROM offline_sync_signals
                 WHERE user_id = $1 AND expires_at > NOW()`,
                event.UserId,
            ).Scan(&pgCount)
            pendingCount += pgCount
        }

        // 回填懒计数器
        if pendingCount > 0 {
            c.redis.Set(ctx, countKey, pendingCount, 7*24*time.Hour)
        }
    }

    // ==================== 5. 通知客户端拉取 ====================
    if pendingCount > 0 {
        // 构造轻量提示信号，通过 Connecte/Push 通知客户端
        // 客户端收到此信号后会主动调用 DequeueSyncSignals RPC
        log.Info("用户上线，有待同步信号，通知客户端拉取",
            "user_id", event.UserId, "pending_count", pendingCount,
            "platform", event.Platform)

        // 注意：这里不直接调用 Push RPC（避免循环依赖），
        // 而是让客户端在连接建立后的初始化流程中主动调用 GetSyncSignalCount → DequeueSyncSignals
        // 此处仅记录日志用于监控
    } else {
        log.Debug("用户上线，无待同步信号", "user_id", event.UserId)
    }

    return nil
}
```

---

### Consumer: `conn.disconnected`

> 来源：Connecte 长连接网关服务。用户断开连接时触发。  
> 职责：记录日志用于监控和排查。离线队列本身无需特殊处理（信号入队由 Push 调用 RPC 完成）。

```go
func (c *OfflineQueueConsumer) HandleConnDisconnected(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_conn.ConnDisconnectedEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 ConnDisconnectedEvent 失败", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 — Redis ====================
    dedupKey := fmt.Sprintf("offline:kafka:dedup:%s", event.Header.EventId)
    set, err := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result()
    if err != nil {
        return fmt.Errorf("Redis 幂等检查失败: %w", err)
    }
    if !set {
        return nil
    }

    // ==================== 3. 参数校验 ====================
    if event.UserId == "" {
        log.Error("conn.disconnected: user_id 为空", "event_id", event.Header.EventId)
        return nil
    }

    // ==================== 4. 仅记录日志 ====================
    // 用户断线后，后续消息由 Push 服务判断用户不在线 → 调用 EnqueueSyncSignal RPC 入队
    // OfflineQueue 本身不需要在断线时做任何操作
    log.Info("用户断线，后续消息将由 Push 入队到离线队列",
        "user_id", event.UserId,
        "platform", event.Platform,
        "device_id", event.DeviceId,
        "disconnect_reason", event.Reason)

    return nil
}
```

---

### Consumer: `msg.recalled`

> 来源：Message 服务。消息被撤回后触发。  
> 职责：如果被撤回的消息对应的同步信号还在离线队列中（用户尚未拉取），需要移除该信号，  
> 避免用户上线后看到一个已被撤回的消息提示。

```go
func (c *OfflineQueueConsumer) HandleMsgRecalled(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_msg.MsgRecalledEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 MsgRecalledEvent 失败", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 — Redis ====================
    dedupKey := fmt.Sprintf("offline:kafka:dedup:%s", event.Header.EventId)
    set, err := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result()
    if err != nil {
        return fmt.Errorf("Redis 幂等检查失败: %w", err)
    }
    if !set {
        log.Debug("重复事件，跳过", "event_id", event.Header.EventId)
        return nil
    }

    // ==================== 3. 参数校验 ====================
    if event.MsgId == "" || event.ConversationId == "" {
        log.Error("msg.recalled: 关键字段缺失",
            "msg_id", event.MsgId, "conversation_id", event.ConversationId)
        return nil
    }

    // ==================== 4. 确定受影响的用户列表 ====================
    // 单聊：只有接收方可能在离线队列中有信号
    // 群聊：需要对所有离线成员处理（但我们无法高效获取所有离线成员列表）
    //       折中方案：群聊场景下不主动移除信号，而是在 DequeueSyncSignals 时由客户端拉取后自行处理
    //       因为撤回信号本身也会生成一个新的同步信号（signal_type=MSG_RECALL）

    var targetUserIDs []string

    switch event.ConversationType {
    case common.CONVERSATION_TYPE_SINGLE:
        // 单聊：移除接收方队列中该消息的信号
        if event.ReceiverId != "" {
            targetUserIDs = []string{event.ReceiverId}
        }

    case common.CONVERSATION_TYPE_GROUP:
        // 群聊：不遍历所有成员，仅记录日志
        // 撤回信号会作为新的 MSG_RECALL 类型信号入队，客户端拉取时自行合并处理
        log.Info("群消息撤回，不逐一移除离线信号（由 MSG_RECALL 信号覆盖）",
            "msg_id", event.MsgId, "conversation_id", event.ConversationId)
        return nil

    default:
        log.Warn("msg.recalled: 未知会话类型", "type", event.ConversationType)
        return nil
    }

    // ==================== 5. 移除匹配的离线信号 ====================
    for _, userID := range targetUserIDs {
        queueKey := fmt.Sprintf("offline:queue:%s", userID)

        // 5a. 遍历 Redis ZSET 查找匹配 conversation_id 的信号
        allMembers, err := c.redis.ZRange(ctx, queueKey, 0, -1).Result()
        if err != nil {
            log.Error("读取 Redis 队列失败", "user_id", userID, "err", err)
            continue
        }

        var toRemove []interface{}
        for _, member := range allMembers {
            var entry map[string]interface{}
            if err := json.Unmarshal([]byte(member), &entry); err != nil {
                continue
            }
            convID, _ := entry["conversation_id"].(string)
            if convID == event.ConversationId {
                toRemove = append(toRemove, member)
            }
        }

        removedCount := 0
        if len(toRemove) > 0 {
            removed, err := c.redis.ZRem(ctx, queueKey, toRemove...).Result()
            if err != nil {
                log.Error("从 Redis 移除撤回消息信号失败",
                    "user_id", userID, "msg_id", event.MsgId, "err", err)
            } else {
                removedCount = int(removed)
            }
        }

        // 5b. 从 PgSQL 移除溢出数据
        overflowKey := fmt.Sprintf("offline:overflow:%s", userID)
        hasOverflow, _ := c.redis.Exists(ctx, overflowKey).Result()
        if hasOverflow > 0 {
            pgResult, err := c.db.ExecContext(ctx,
                `DELETE FROM offline_sync_signals
                 WHERE user_id = $1 AND channel_id = $2`,
                userID, event.ConversationId,
            )
            if err != nil {
                log.Error("从 PgSQL 移除溢出信号失败",
                    "user_id", userID, "conversation_id", event.ConversationId, "err", err)
            } else {
                pgRemoved, _ := pgResult.RowsAffected()
                removedCount += int(pgRemoved)
            }
        }

        // 5c. 更新懒计数器
        if removedCount > 0 {
            countKey := fmt.Sprintf("offline:count:%s", userID)
            newCount := c.redis.DecrBy(ctx, countKey, int64(removedCount)).Val()
            if newCount < 0 {
                actualCount, _ := c.redis.ZCard(ctx, queueKey).Result()
                c.redis.Set(ctx, countKey, actualCount, 7*24*time.Hour)
            }
        }

        log.Info("撤回消息：已移除离线信号",
            "user_id", userID, "msg_id", event.MsgId,
            "conversation_id", event.ConversationId, "removed_count", removedCount)
    }

    return nil
}
```

---

### Consumer: `user.deactivated`

> 来源：User 服务。用户注销账号后触发。  
> 职责：清空该用户的所有离线同步信号（Redis + PgSQL），释放存储空间。

```go
func (c *OfflineQueueConsumer) HandleUserDeactivated(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_user.UserDeactivatedEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 UserDeactivatedEvent 失败", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 — Redis ====================
    dedupKey := fmt.Sprintf("offline:kafka:dedup:%s", event.Header.EventId)
    set, err := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result()
    if err != nil {
        return fmt.Errorf("Redis 幂等检查失败: %w", err)
    }
    if !set {
        log.Debug("重复事件，跳过", "event_id", event.Header.EventId)
        return nil
    }

    // ==================== 3. 参数校验 ====================
    if event.UserId == "" {
        log.Error("user.deactivated: user_id 为空", "event_id", event.Header.EventId)
        return nil
    }

    log.Info("用户注销，开始清空离线信号", "user_id", event.UserId)

    // ==================== 4. 清理 Redis — Pipeline ====================
    queueKey := fmt.Sprintf("offline:queue:%s", event.UserId)
    countKey := fmt.Sprintf("offline:count:%s", event.UserId)
    overflowKey := fmt.Sprintf("offline:overflow:%s", event.UserId)

    pipe := c.redis.Pipeline()
    pipe.Del(ctx, queueKey)
    pipe.Del(ctx, countKey)
    pipe.Del(ctx, overflowKey)
    _, err = pipe.Exec(ctx)
    if err != nil {
        log.Error("清理 Redis 队列失败", "user_id", event.UserId, "err", err)
        // 不中断，继续清理 PgSQL
    }

    // ==================== 5. 清理去重 Key（SCAN 模式） ====================
    // Key: offline:dedup:{user_id}:*
    var cursor uint64
    dedupPattern := fmt.Sprintf("offline:dedup:%s:*", event.UserId)
    for {
        keys, nextCursor, err := c.redis.Scan(ctx, cursor, dedupPattern, 100).Result()
        if err != nil {
            log.Error("SCAN 去重 Key 失败", "user_id", event.UserId, "err", err)
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

    // ==================== 6. 清理 PgSQL 溢出数据 ====================
    result, err := c.db.ExecContext(ctx,
        `DELETE FROM offline_sync_signals WHERE user_id = $1`,
        event.UserId,
    )
    if err != nil {
        log.Error("清理 PgSQL 溢出数据失败", "user_id", event.UserId, "err", err)
        return fmt.Errorf("清理 PgSQL 数据失败: %w", err) // 返回 error 触发重试
    }
    deletedRows, _ := result.RowsAffected()

    log.Info("用户注销：离线信号清空完成",
        "user_id", event.UserId, "pg_deleted", deletedRows)

    return nil
}
```

---

### Consumer: `config.changed`

> 来源：Config 配置服务。管理后台修改离线队列相关配置后触发。  
> 职责：热更新离线队列的参数（MaxQueueSize / SignalTTL / 溢出阈值等），无需重启服务。

```go
func (c *OfflineQueueConsumer) HandleConfigChanged(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_config.ConfigChangedEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 ConfigChangedEvent 失败", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 — Redis ====================
    dedupKey := fmt.Sprintf("offline:kafka:dedup:%s", event.Header.EventId)
    set, err := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result()
    if err != nil {
        return fmt.Errorf("Redis 幂等检查失败: %w", err)
    }
    if !set {
        return nil
    }

    // ==================== 3. 过滤非本服务配置 ====================
    // 只关心 offline_queue 命名空间下的配置变更
    if event.Namespace != "offline_queue" {
        return nil
    }

    log.Info("收到配置变更事件",
        "namespace", event.Namespace, "keys_changed", len(event.Changes))

    // ==================== 4. 逐项更新配置 ====================
    for _, change := range event.Changes {
        switch change.Key {
        case "max_queue_size":
            // 更新每用户最大队列大小
            newSize, err := strconv.Atoi(change.NewValue)
            if err != nil || newSize <= 0 || newSize > 100000 {
                log.Warn("无效的 max_queue_size 配置", "value", change.NewValue)
                continue
            }
            oldSize := c.config.MaxQueueSize
            c.config.MaxQueueSize = newSize
            log.Info("更新 MaxQueueSize", "old", oldSize, "new", newSize)

        case "signal_ttl_hours":
            // 更新信号存活时间
            ttlHours, err := strconv.Atoi(change.NewValue)
            if err != nil || ttlHours <= 0 || ttlHours > 720 { // 最大 30 天
                log.Warn("无效的 signal_ttl_hours 配置", "value", change.NewValue)
                continue
            }
            oldTTL := c.config.SignalTTL
            c.config.SignalTTL = time.Duration(ttlHours) * time.Hour
            log.Info("更新 SignalTTL", "old", oldTTL, "new", c.config.SignalTTL)

        case "overflow_threshold":
            // 更新溢出阈值
            threshold, err := strconv.Atoi(change.NewValue)
            if err != nil || threshold <= 0 {
                log.Warn("无效的 overflow_threshold 配置", "value", change.NewValue)
                continue
            }
            oldThreshold := c.config.OverflowThreshold
            c.config.OverflowThreshold = threshold
            log.Info("更新 OverflowThreshold", "old", oldThreshold, "new", threshold)

        case "max_dequeue_count":
            // 更新单次最大拉取数量
            maxCount, err := strconv.Atoi(change.NewValue)
            if err != nil || maxCount <= 0 || maxCount > 5000 {
                log.Warn("无效的 max_dequeue_count 配置", "value", change.NewValue)
                continue
            }
            oldCount := c.config.MaxDequeueCount
            c.config.MaxDequeueCount = maxCount
            log.Info("更新 MaxDequeueCount", "old", oldCount, "new", maxCount)

        case "cleanup_batch_size":
            // 更新清理任务每批处理数量
            batchSize, err := strconv.Atoi(change.NewValue)
            if err != nil || batchSize <= 0 || batchSize > 10000 {
                log.Warn("无效的 cleanup_batch_size 配置", "value", change.NewValue)
                continue
            }
            oldBatch := c.config.CleanupBatchSize
            c.config.CleanupBatchSize = batchSize
            log.Info("更新 CleanupBatchSize", "old", oldBatch, "new", batchSize)

        default:
            log.Debug("忽略未识别的配置项", "key", change.Key)
        }
    }

    return nil
}
```

---

## 定时任务

### Timer: 过期信号清理（每日 4:00 AM）

> 定时清理 Redis 和 PgSQL 中已过期的离线同步信号。  
> Redis ZSET 中过期信号通过 score（timestamp）判断；PgSQL 通过 expires_at 字段判断。  
> 使用分布式锁防止多实例重复执行。

```go
// StartCleanupScheduler 启动过期信号清理定时任务
func (c *OfflineQueueConsumer) StartCleanupScheduler() {
    // 使用 cron 库注册定时任务：每日 4:00 AM
    c.cron.AddFunc("0 4 * * *", func() {
        ctx := context.Background()
        if err := c.cleanupExpiredSignals(ctx); err != nil {
            log.Error("过期信号清理任务失败", "err", err)
        }
    })
    c.cron.Start()
    log.Info("过期信号清理定时任务已启动", "cron", "0 4 * * *")
}

func (c *OfflineQueueConsumer) cleanupExpiredSignals(ctx context.Context) error {
    // ==================== 1. 分布式锁 — 防止多实例重复执行 ====================
    lockKey := "offline:cleanup:lock"
    lockVal := fmt.Sprintf("%s:%d", c.instanceID, time.Now().UnixMilli())
    locked, err := c.redis.SetNX(ctx, lockKey, lockVal, 30*time.Minute).Result()
    if err != nil {
        return fmt.Errorf("获取分布式锁失败: %w", err)
    }
    if !locked {
        log.Info("其他实例正在执行清理任务，跳过")
        return nil
    }
    // 确保释放锁
    defer func() {
        // 只释放自己持有的锁（Lua 脚本保证原子性）
        luaScript := `
            if redis.call("get", KEYS[1]) == ARGV[1] then
                return redis.call("del", KEYS[1])
            else
                return 0
            end
        `
        c.redis.Eval(ctx, luaScript, []string{lockKey}, lockVal)
    }()

    startTime := time.Now()
    log.Info("开始执行过期信号清理任务")

    // ==================== 2. 清理 Redis 中的过期信号 ====================
    // 遍历所有 offline:queue:* 键，移除 score < 当前时间 - SignalTTL 的成员
    expireBefore := float64(time.Now().Add(-c.config.SignalTTL).UnixMilli())
    var redisCleanedTotal int64
    var scannedQueues int

    var cursor uint64
    for {
        keys, nextCursor, err := c.redis.Scan(ctx, cursor, "offline:queue:*", 200).Result()
        if err != nil {
            log.Error("SCAN Redis 队列键失败", "cursor", cursor, "err", err)
            break
        }

        for _, queueKey := range keys {
            scannedQueues++

            // ZREMRANGEBYSCORE 移除 score < expireBefore 的成员
            removed, err := c.redis.ZRemRangeByScore(ctx, queueKey, "-inf",
                fmt.Sprintf("%f", expireBefore)).Result()
            if err != nil {
                log.Warn("清理过期信号失败", "key", queueKey, "err", err)
                continue
            }
            redisCleanedTotal += removed

            // 如果队列为空，删除 key 和关联的 count key
            remaining, _ := c.redis.ZCard(ctx, queueKey).Result()
            if remaining == 0 {
                // 提取 user_id
                // queueKey 格式: offline:queue:{user_id}
                parts := strings.SplitN(queueKey, ":", 3)
                if len(parts) == 3 {
                    userID := parts[2]
                    countKey := fmt.Sprintf("offline:count:%s", userID)
                    overflowKey := fmt.Sprintf("offline:overflow:%s", userID)
                    c.redis.Del(ctx, queueKey, countKey, overflowKey)
                }
            } else {
                // 更新懒计数器
                parts := strings.SplitN(queueKey, ":", 3)
                if len(parts) == 3 {
                    userID := parts[2]
                    countKey := fmt.Sprintf("offline:count:%s", userID)
                    c.redis.Set(ctx, countKey, remaining, 7*24*time.Hour)
                }
            }
        }

        cursor = nextCursor
        if cursor == 0 {
            break
        }
    }

    // ==================== 3. 清理 PgSQL 中的过期信号 ====================
    // 分批删除，避免长事务锁表
    var pgCleanedTotal int64
    for {
        result, err := c.db.ExecContext(ctx,
            `DELETE FROM offline_sync_signals
             WHERE id IN (
                 SELECT id FROM offline_sync_signals
                 WHERE expires_at <= NOW()
                 LIMIT $1
             )`,
            c.config.CleanupBatchSize,
        )
        if err != nil {
            log.Error("清理 PgSQL 过期信号失败", "err", err)
            break
        }
        deleted, _ := result.RowsAffected()
        pgCleanedTotal += deleted

        if deleted < int64(c.config.CleanupBatchSize) {
            // 没有更多过期记录
            break
        }

        // 每批之间短暂休息，避免 DB 压力过大
        time.Sleep(100 * time.Millisecond)
    }

    // ==================== 4. 更新清理状态 ====================
    c.redis.Set(ctx, "offline:cleanup:last_run", time.Now().Format(time.RFC3339), 0)

    elapsed := time.Since(startTime)

    // ==================== 5. 生产统计事件 — Kafka ====================
    statsEvent := &kafka_offline.OfflineQueueStatsEvent{
        Header:         buildEventHeader("offline_queue", c.instanceID),
        TotalEnqueued:  0,
        TotalDequeued:  0,
        TotalOverflow:  pgCleanedTotal,
        PendingUsers:   int64(scannedQueues),
        PendingSignals: 0,
        StatsTime:      time.Now().UnixMilli(),
        PeriodSeconds:  int64(elapsed.Seconds()),
    }
    if err := c.kafka.Produce(ctx, "offline.stats", "cleanup", statsEvent); err != nil {
        log.Error("生产清理统计事件失败", "err", err)
    }

    log.Info("过期信号清理任务完成",
        "redis_cleaned", redisCleanedTotal,
        "pg_cleaned", pgCleanedTotal,
        "scanned_queues", scannedQueues,
        "elapsed", elapsed.String())

    return nil
}
```
