# Relation 好友关系服务 — Kafka 消费者实现伪代码

## 概述

Relation 服务通过 Kafka 接收用户注销/封禁、内容违规、配置变更等事件，执行关系链清理和状态同步。同时定时生产关系链统计事件。

## 生产 Topic 列表

| Topic | 事件 | 生产时机 | 消费方 |
|-------|------|----------|--------|
| `relation.friend.request` | FriendRequestEvent | 发送好友申请 | Notification, Push, Audit |
| `relation.friend.accepted` | FriendAcceptedEvent | 好友通过 | Notification, Push, Conversation, Audit |
| `relation.friend.rejected` | FriendRejectedEvent | 好友拒绝 | Notification, Push |
| `relation.friend.deleted` | FriendDeletedEvent | 好友删除 | Conversation, Search, Audit |
| `relation.blocked` | UserBlockedEvent | 拉黑用户 | Conversation, Message, Push, Audit |
| `relation.unblocked` | UserUnblockedEvent | 取消拉黑 | Audit |
| `relation.remark.updated` | RemarkUpdatedEvent | 备注修改 | Search, Conversation |
| `relation.stats` | RelationStatsEvent | 定时统计 | Audit |

## 消费 Topic 列表

| Topic | 来源 | 用途 |
|-------|------|------|
| `user.deactivated` | User | 账号注销 → 清理该用户所有关系链数据 |
| `user.banned` | User | 账号封禁 → 标记用户，限制好友申请 |
| `audit.content.violation` | Audit | 内容违规 → 根据处罚级别限制社交操作 |
| `config.changed` | Config | 配置变更 → 热更新好友数上限、分组上限等 |

## Redis Key 设计（消费者侧）

| Key 格式 | 类型 | 值 | TTL | 说明 |
|----------|------|-----|-----|------|
| `rel:kafka:dedup:{event_id}` | STRING | "1" | 24h | Kafka 消费幂等去重 |
| `rel:user_banned:{user_id}` | STRING | "1" | = 封禁剩余时长 | 用户封禁状态标记 |
| `rel:user_restricted:{user_id}` | STRING | restrict_level | 根据处罚时长 | 用户社交限制标记 |

---

## 消费者实现

### Consumer: `user.deactivated`

> 来源：User 服务。用户注销账户，需清理该用户所有好友关系、好友申请、黑名单、好友分组数据，并通知受影响的好友。

```go
func (c *RelationConsumer) HandleUserDeactivated(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_user.UserDeactivatedEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 UserDeactivatedEvent 失败", "err", err)
        return nil // 反序列化失败不重试
    }

    // ==================== 2. 幂等检查 — Redis ====================
    dedupKey := fmt.Sprintf("rel:kafka:dedup:%s", event.Header.EventId)
    set, err := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result()
    if err != nil {
        return fmt.Errorf("redis 幂等检查失败: %w", err)
    }
    if !set {
        log.Info("重复事件，跳过", "event_id", event.Header.EventId)
        return nil
    }

    userID := event.UserId
    now := time.Now().UnixMilli()

    // ==================== 3. 查询该用户所有好友列表（用于后续通知） — PgSQL ====================
    friendIDs := make([]string, 0)
    rows, err := c.db.QueryContext(ctx,
        `SELECT friend_id FROM friends WHERE user_id = $1`,
        userID,
    )
    if err != nil {
        return fmt.Errorf("查询好友列表失败: %w", err)
    }
    defer rows.Close()
    for rows.Next() {
        var fid string
        if err := rows.Scan(&fid); err != nil {
            continue
        }
        friendIDs = append(friendIDs, fid)
    }

    // ==================== 4. 事务：清理所有关系链数据 — PgSQL ====================
    tx, err := c.db.BeginTx(ctx, nil)
    if err != nil {
        return fmt.Errorf("开启事务失败: %w", err)
    }
    defer tx.Rollback()

    // 4a. 删除该用户发出和收到的所有好友申请
    _, err = tx.ExecContext(ctx,
        `DELETE FROM friend_requests WHERE from_user_id = $1 OR to_user_id = $1`,
        userID,
    )
    if err != nil {
        return fmt.Errorf("删除好友申请失败: %w", err)
    }

    // 4b. 删除该用户的所有好友关系（双向）
    _, err = tx.ExecContext(ctx,
        `DELETE FROM friends WHERE user_id = $1 OR friend_id = $1`,
        userID,
    )
    if err != nil {
        return fmt.Errorf("删除好友关系失败: %w", err)
    }

    // 4c. 删除该用户的所有黑名单记录（双向）
    _, err = tx.ExecContext(ctx,
        `DELETE FROM blocks WHERE user_id = $1 OR blocked_id = $1`,
        userID,
    )
    if err != nil {
        return fmt.Errorf("删除黑名单记录失败: %w", err)
    }

    // 4d. 删除该用户的所有好友分组
    _, err = tx.ExecContext(ctx,
        `DELETE FROM friend_groups WHERE user_id = $1`,
        userID,
    )
    if err != nil {
        return fmt.Errorf("删除好友分组失败: %w", err)
    }

    if err = tx.Commit(); err != nil {
        return fmt.Errorf("提交事务失败: %w", err)
    }

    // ==================== 5. 延迟双删清除 Redis 缓存 ====================
    // 收集所有需要清除的缓存 key
    delKeys := []string{
        fmt.Sprintf("rel:friends:%s", userID),
        fmt.Sprintf("rel:blocks:%s", userID),
        fmt.Sprintf("rel:friend_count:%s", userID),
    }
    for _, friendID := range friendIDs {
        delKeys = append(delKeys,
            fmt.Sprintf("rel:friends:%s", friendID),
            fmt.Sprintf("rel:is_friend:%s:%s", friendID, userID),
            fmt.Sprintf("rel:is_friend:%s:%s", userID, friendID),
            fmt.Sprintf("rel:friend_count:%s", friendID),
        )
    }
    c.delayedDoubleDelete(ctx, delKeys...)

    // 单独 SCAN 清理 is_blocked 相关 key（不适合放入 pipeline）
    cursor := uint64(0)
    for {
        keys, nextCursor, err := c.redis.Scan(ctx, cursor, fmt.Sprintf("rel:is_blocked:*%s*", userID), 100).Result()
        if err != nil {
            log.Error("SCAN 清理 is_blocked 缓存失败", "user_id", userID, "err", err)
            break
        }
        if len(keys) > 0 {
            c.delayedDoubleDelete(ctx, keys...)
        }
        cursor = nextCursor
        if cursor == 0 {
            break
        }
    }

    // ==================== 6. 为每位受影响的好友生产 relation.friend.deleted 事件 ====================
    for _, friendID := range friendIDs {
        deleteEvent := &kafka_relation.FriendDeletedEvent{
            Header:     buildEventHeader("relation", c.instanceID),
            OperatorId: userID,
            TargetId:   friendID,
            DeleteTime: now,
        }
        if err := c.kafka.Produce(ctx, "relation.friend.deleted", friendID, deleteEvent); err != nil {
            log.Error("生产 friend.deleted 事件失败", "friend_id", friendID, "err", err)
        }
    }

    log.Info("用户注销，关系链数据已清理",
        "user_id", userID,
        "affected_friends", len(friendIDs),
    )
    return nil
}
```

### Consumer: `user.banned`

> 来源：User 服务。用户被封禁时，标记其封禁状态，阻止其发起好友申请和加入关系链操作。  
> 不删除已有好友关系（封禁解除后可恢复），但需通知在线好友。

```go
func (c *RelationConsumer) HandleUserBanned(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_user.UserBannedEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 UserBannedEvent 失败", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 — Redis ====================
    dedupKey := fmt.Sprintf("rel:kafka:dedup:%s", event.Header.EventId)
    set, err := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result()
    if err != nil {
        return fmt.Errorf("redis 幂等检查失败: %w", err)
    }
    if !set {
        log.Info("重复事件，跳过", "event_id", event.Header.EventId)
        return nil
    }

    userID := event.UserId

    // ==================== 3. 在 Redis 中标记用户封禁状态 ====================
    // 根据封禁时长设置 TTL；永久封禁使用 30 天 TTL + 定时刷新
    bannedKey := fmt.Sprintf("rel:user_banned:%s", userID)
    if event.BanDuration > 0 {
        c.redis.Set(ctx, bannedKey, "1", time.Duration(event.BanDuration)*time.Second)
    } else {
        // 永久封禁，设置 30 天 TTL，由定时任务刷新
        c.redis.Set(ctx, bannedKey, "1", 30*24*time.Hour)
    }

    // ==================== 4. 取消该用户所有待处理的好友申请 — PgSQL ====================
    _, err = c.db.ExecContext(ctx,
        `UPDATE friend_requests SET status = 2, handled_at = $1
         WHERE from_user_id = $2 AND status = 0`,
        time.Now().UnixMilli(), userID,
    )
    if err != nil {
        log.Error("取消封禁用户的待处理好友申请失败", "user_id", userID, "err", err)
        // 非致命错误，继续执行
    }

    // ==================== 5. 查询该用户的好友列表（用于通知） — PgSQL ====================
    friendIDs := make([]string, 0)
    rows, err := c.db.QueryContext(ctx,
        `SELECT friend_id FROM friends WHERE user_id = $1`,
        userID,
    )
    if err == nil {
        defer rows.Close()
        for rows.Next() {
            var fid string
            if err := rows.Scan(&fid); err != nil {
                continue
            }
            friendIDs = append(friendIDs, fid)
        }
    }

    // ==================== 6. 延迟双删清除 Redis 中该用户的关系缓存 ====================
    c.delayedDoubleDelete(ctx,
        fmt.Sprintf("rel:friends:%s", userID),
        fmt.Sprintf("rel:blocks:%s", userID),
        fmt.Sprintf("rel:friend_count:%s", userID),
    )

    log.Info("用户被封禁，关系链状态已更新",
        "user_id", userID,
        "ban_duration", event.BanDuration,
        "affected_friends", len(friendIDs),
    )
    return nil
}
```

### Consumer: `audit.content.violation`

> 来源：Audit 审核服务。当用户因内容违规（如好友申请附言含违规内容、备注含违规内容等）被处罚时，  
> Relation 服务根据违规级别限制该用户的社交操作能力。

```go
func (c *RelationConsumer) HandleContentViolation(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_audit.ContentViolationEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 ContentViolationEvent 失败", "err", err)
        return nil
    }

    // ==================== 2. 过滤非 Relation 相关的违规事件 ====================
    // 只处理来源为 relation 模块的违规事件（好友申请附言、备注等）
    isRelationViolation := false
    for _, svc := range event.AffectedServices {
        if svc == "relation" {
            isRelationViolation = true
            break
        }
    }
    if !isRelationViolation {
        return nil // 非 relation 相关的违规，跳过
    }

    // ==================== 3. 幂等检查 — Redis ====================
    dedupKey := fmt.Sprintf("rel:kafka:dedup:%s", event.Header.EventId)
    set, err := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result()
    if err != nil {
        return fmt.Errorf("redis 幂等检查失败: %w", err)
    }
    if !set {
        log.Info("重复事件，跳过", "event_id", event.Header.EventId)
        return nil
    }

    userID := event.UserId
    now := time.Now().UnixMilli()

    // ==================== 4. 根据违规动作执行对应处理 ====================
    switch event.Action {
    case "restrict_social":
        // 限制社交操作（禁止发送好友申请、设置备注等）
        // 设置限制标记到 Redis
        restrictKey := fmt.Sprintf("rel:user_restricted:%s", userID)
        var duration time.Duration
        if event.RestrictDuration > 0 {
            duration = time.Duration(event.RestrictDuration) * time.Second
        } else {
            duration = 24 * time.Hour // 默认限制 24 小时
        }
        c.redis.Set(ctx, restrictKey, event.RestrictLevel, duration)

        log.Info("用户社交操作被限制",
            "user_id", userID,
            "restrict_level", event.RestrictLevel,
            "duration_seconds", event.RestrictDuration,
        )

    case "reset_remark":
        // 违规备注：重置该用户设置的所有好友备注
        _, err := c.db.ExecContext(ctx,
            `UPDATE friends SET remark = '' WHERE user_id = $1 AND remark != ''`,
            userID,
        )
        if err != nil {
            return fmt.Errorf("重置好友备注失败: %w", err)
        }

        log.Info("用户好友备注已重置",
            "user_id", userID,
            "reason", event.Reason,
        )

    case "cancel_pending_requests":
        // 违规好友申请附言：取消该用户所有待处理的好友申请
        result, err := c.db.ExecContext(ctx,
            `UPDATE friend_requests SET status = 2, handled_at = $1
             WHERE from_user_id = $2 AND status = 0`,
            now, userID,
        )
        if err != nil {
            return fmt.Errorf("取消待处理好友申请失败: %w", err)
        }
        affected, _ := result.RowsAffected()

        log.Info("用户待处理好友申请已取消",
            "user_id", userID,
            "cancelled_count", affected,
            "reason", event.Reason,
        )

    case "force_unblock_all":
        // 滥用拉黑功能：强制解除该用户的所有黑名单
        // 先查询所有被拉黑用户
        blockedIDs := make([]string, 0)
        bRows, err := c.db.QueryContext(ctx,
            `SELECT blocked_id FROM blocks WHERE user_id = $1`, userID,
        )
        if err == nil {
            defer bRows.Close()
            for bRows.Next() {
                var bid string
                bRows.Scan(&bid)
                blockedIDs = append(blockedIDs, bid)
            }
        }

        // 删除所有黑名单记录
        _, err = c.db.ExecContext(ctx,
            `DELETE FROM blocks WHERE user_id = $1`, userID,
        )
        if err != nil {
            return fmt.Errorf("强制解除黑名单失败: %w", err)
        }

        // 延迟双删清除 Redis 黑名单缓存
        unblockDelKeys := []string{fmt.Sprintf("rel:blocks:%s", userID)}
        for _, blockedID := range blockedIDs {
            unblockDelKeys = append(unblockDelKeys, fmt.Sprintf("rel:is_blocked:%s:%s", userID, blockedID))
        }
        c.delayedDoubleDelete(ctx, unblockDelKeys...)

        // 为每个被解除拉黑的用户生产 relation.unblocked 事件
        for _, blockedID := range blockedIDs {
            unblockEvent := &kafka_relation.UserUnblockedEvent{
                Header:      buildEventHeader("relation", c.instanceID),
                OperatorId:  userID,
                BlockedId:   blockedID,
                UnblockTime: now,
            }
            c.kafka.Produce(ctx, "relation.unblocked", userID, unblockEvent)
        }

        log.Info("用户黑名单已强制清除",
            "user_id", userID,
            "unblocked_count", len(blockedIDs),
        )

    default:
        log.Warn("未知的违规处理动作",
            "user_id", userID,
            "action", event.Action,
        )
    }

    return nil
}
```

### Consumer: `config.changed`

> 来源：Config 服务。全局配置变更，Relation 服务热更新好友数上限、分组数上限、好友申请频率等运行时参数。

```go
func (c *RelationConsumer) HandleConfigChanged(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_config.ConfigChangedEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 ConfigChangedEvent 失败", "err", err)
        return nil
    }

    // ==================== 2. 过滤非本服务的配置 ====================
    affectsRelation := false
    for _, svc := range event.AffectedServices {
        if svc == "relation" {
            affectsRelation = true
            break
        }
    }
    if !affectsRelation {
        return nil // 非 relation 相关配置，跳过
    }

    // ==================== 3. 根据 config_key 更新本地运行时配置 ====================
    switch event.ConfigKey {
    case "relation.max_friends":
        // 最大好友数上限（默认 5000）
        val, err := strconv.Atoi(event.NewValue)
        if err != nil {
            log.Error("解析 max_friends 配置失败", "value", event.NewValue, "err", err)
            return nil
        }
        c.config.Store("max_friends", val)
        log.Info("好友数上限已更新", "old", event.OldValue, "new", val)

    case "relation.max_friend_groups":
        // 最大好友分组数上限（默认 20）
        val, err := strconv.Atoi(event.NewValue)
        if err != nil {
            log.Error("解析 max_friend_groups 配置失败", "value", event.NewValue, "err", err)
            return nil
        }
        c.config.Store("max_friend_groups", val)
        log.Info("好友分组上限已更新", "old", event.OldValue, "new", val)

    case "relation.max_blocks":
        // 最大黑名单数上限（默认 500）
        val, err := strconv.Atoi(event.NewValue)
        if err != nil {
            log.Error("解析 max_blocks 配置失败", "value", event.NewValue, "err", err)
            return nil
        }
        c.config.Store("max_blocks", val)
        log.Info("黑名单上限已更新", "old", event.OldValue, "new", val)

    case "relation.friend_request_daily_limit":
        // 每日好友申请发送上限（默认 50）
        val, err := strconv.Atoi(event.NewValue)
        if err != nil {
            log.Error("解析 friend_request_daily_limit 配置失败", "value", event.NewValue, "err", err)
            return nil
        }
        c.config.Store("friend_request_daily_limit", val)
        log.Info("每日好友申请上限已更新", "old", event.OldValue, "new", val)

    case "relation.friend_cache_ttl_minutes":
        // 好友列表 Redis 缓存 TTL（分钟，默认 30）
        val, err := strconv.Atoi(event.NewValue)
        if err != nil {
            log.Error("解析 friend_cache_ttl_minutes 配置失败", "value", event.NewValue, "err", err)
            return nil
        }
        c.config.Store("friend_cache_ttl_minutes", val)
        log.Info("好友缓存 TTL 已更新", "old", event.OldValue, "new_minutes", val)

    case "relation.block_cache_ttl_minutes":
        // 黑名单 Redis 缓存 TTL（分钟，默认 30）
        val, err := strconv.Atoi(event.NewValue)
        if err != nil {
            log.Error("解析 block_cache_ttl_minutes 配置失败", "value", event.NewValue, "err", err)
            return nil
        }
        c.config.Store("block_cache_ttl_minutes", val)
        log.Info("黑名单缓存 TTL 已更新", "old", event.OldValue, "new_minutes", val)

    default:
        log.Debug("未知的 relation 配置项", "key", event.ConfigKey)
    }

    return nil
}
```

---

## 定时任务：关系链统计

```go
// RunStatsTask 每 5 分钟执行一次统计并生产 relation.stats 事件
func (c *RelationConsumer) RunStatsTask(ctx context.Context) {
    ticker := time.NewTicker(5 * time.Minute)
    defer ticker.Stop()

    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            now := time.Now().UnixMilli()
            periodStart := now - 5*60*1000 // 上一个 5 分钟窗口起始
            periodEnd := now

            // 统计各项指标 — PgSQL
            var requestCount, acceptCount, rejectCount, deleteCount int64
            var blockCount, unblockCount int64

            // 好友申请数（该窗口内新增）
            c.db.QueryRowContext(ctx,
                `SELECT COUNT(*) FROM friend_requests WHERE created_at >= $1 AND created_at < $2`,
                periodStart, periodEnd,
            ).Scan(&requestCount)

            // 好友通过数
            c.db.QueryRowContext(ctx,
                `SELECT COUNT(*) FROM friend_requests
                 WHERE status = 1 AND handled_at >= $1 AND handled_at < $2`,
                periodStart, periodEnd,
            ).Scan(&acceptCount)

            // 好友拒绝数
            c.db.QueryRowContext(ctx,
                `SELECT COUNT(*) FROM friend_requests
                 WHERE status = 2 AND handled_at >= $1 AND handled_at < $2`,
                periodStart, periodEnd,
            ).Scan(&rejectCount)

            // 注：删除数、拉黑数、取消拉黑数需从本地计数器或审计日志获取
            // 这里使用服务实例内的原子计数器（在 RPC 层每次操作时递增）
            deleteCount = c.statsCounter.SwapAndReset("friend_deleted")
            blockCount = c.statsCounter.SwapAndReset("user_blocked")
            unblockCount = c.statsCounter.SwapAndReset("user_unblocked")

            // 生产 relation.stats 事件
            statsEvent := &kafka_relation.RelationStatsEvent{
                Header:             buildEventHeader("relation", c.instanceID),
                ServerId:           c.instanceID,
                PeriodStart:        periodStart,
                PeriodEnd:          periodEnd,
                FriendRequestCount: requestCount,
                FriendAcceptCount:  acceptCount,
                FriendRejectCount:  rejectCount,
                FriendDeleteCount:  deleteCount,
                BlockCount:         blockCount,
                UnblockCount:       unblockCount,
            }
            if err := c.kafka.Produce(ctx, "relation.stats", "stats", statsEvent); err != nil {
                log.Error("生产 relation.stats 事件失败", "err", err)
            }
        }
    }
}
```

---

## 内部辅助方法

### delayedDoubleDelete — 延迟双删工具方法

```go
// delayedDoubleDelete 延迟双删：保证缓存与 DB 的最终一致性
func (c *RelationConsumer) delayedDoubleDelete(ctx context.Context, keys ...string) {
    if len(keys) == 0 { return }
    pipe := c.redis.Pipeline()
    for _, key := range keys {
        pipe.Del(ctx, key)
    }
    pipe.Exec(ctx)
    go func() {
        time.Sleep(500 * time.Millisecond)
        pipe := c.redis.Pipeline()
        for _, key := range keys {
            pipe.Del(context.Background(), key)
        }
        pipe.Exec(context.Background())
    }()
}
```
