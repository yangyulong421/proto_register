# Presence 在线状态服务 — Kafka 消费者实现伪代码

## 概述

Presence 服务通过 Kafka 接收连接生命周期事件（上线/断线）、好友关系变更事件（自动订阅/取消订阅）、用户注销事件（清理全部状态数据）和配置变更事件（热更新心跳参数）。

**核心消费场景：**
- `conn.connected` / `conn.disconnected`：Connecte 网关产生的连接事件，是 Presence 状态更新的主入口
- `relation.friend.accepted` / `relation.friend.deleted`：好友关系变更时自动维护双向状态订阅
- `user.deactivated`：用户注销时清理所有 Presence 数据和订阅关系
- `config.changed`：热更新心跳间隔、优雅期时长等运行时参数

**设计要点：**
- 所有消费者均实现 Kafka 幂等去重（Redis SetNX）
- `conn.connected` / `conn.disconnected` 是高频事件，需极致性能
- 好友关系变更触发的订阅操作是批量的，使用 Pipeline 优化
- 用户注销清理使用 SCAN + Pipeline 批量删除，避免阻塞 Redis

## 生产 Topic 列表

| Topic | 事件 | 生产时机 | Key | 消费方 |
|-------|------|----------|-----|--------|
| `presence.online` | PresenceOnlineEvent | 用户上线确认后 | user_id | Push（通知订阅者）, Conversation（更新 last_online） |
| `presence.offline` | PresenceOfflineEvent | 优雅期结束确认离线后 | user_id | Push（通知订阅者） |
| `presence.status.changed` | PresenceStatusChangedEvent | 用户状态变更 | user_id | Push（通知订阅者自定义状态变化） |
| `presence.status.notify` | PresenceStatusNotifyEvent | 状态变更需通知订阅者时 | user_id | Push（批量推送给订阅者列表） |
| `presence.typing` | PresenceTypingEvent | 用户正在输入 | target_id | Push（投递给对方） |
| `presence.stats` | PresenceStatsEvent | 定时统计在线数据 | server_id | Audit（运营统计） |

## 消费 Topic 列表

| Topic | 来源 | 用途 | 并发度 | 重试 |
|-------|------|------|--------|------|
| `conn.connected` | Connecte | 用户连接建立 → 更新 Presence 为在线，添加平台 | 16 | 3 |
| `conn.disconnected` | Connecte | 用户连接断开 → 启动离线优雅期或设置离线 | 16 | 3 |
| `relation.friend.accepted` | Relation | 好友关系建立 → 自动双向订阅在线状态 | 4 | 3 |
| `relation.friend.deleted` | Relation | 好友关系删除 → 移除双向在线状态订阅 | 4 | 3 |
| `user.deactivated` | User | 用户注销 → 清理所有 Presence 数据和订阅关系 | 2 | 3 |
| `config.changed` | Config | 配置变更 → 热更新心跳间隔、优雅期等参数 | 1 | 1 |

## Redis Key 设计（消费者侧）

| Key 格式 | 类型 | 值 | TTL | 说明 |
|----------|------|-----|-----|------|
| `presence:kafka:dedup:{event_id}` | STRING | "1" | 24h | Kafka 消费幂等去重 |
| `presence:status:{user_id}` | HASH | {status, custom_text, platform, device_id, server_id, last_active, updated_at} | 6min | 用户在线状态（共享，心跳续期） |
| `presence:user_platforms:{user_id}` | SET | "platform:device_id:server_id" | 6min | 用户在线平台列表（共享） |
| `presence:online` | HYPERLOGLOG | user_id | 无 TTL | 全局在线计数（共享） |
| `presence:online:platform:{platform}` | HYPERLOGLOG | user_id | 无 TTL | 分平台在线计数（共享） |
| `presence:subs:{user_id}` | SET | subscriber_user_id 集合 | 24h | 被订阅者的订阅者列表（共享） |
| `presence:my_subs:{user_id}` | SET | target_user_id 集合 | 24h | 用户的订阅目标列表（共享） |
| `presence:offline_grace:{user_id}` | STRING | "platform:device_id" | 30s | 离线优雅期（共享） |

---

## 消费者实现

### Consumer: `conn.connected`

> 来源：Connecte 网关服务。用户 WebSocket/TCP 连接建立成功后产生此事件。  
> 职责：更新用户在线状态、添加在线平台、更新 HyperLogLog 计数、检查优雅期重连。  
> **高频热路径**：10M 用户场景下，连接事件峰值可达数万/秒（如早高峰集中上线）。  
> 本质上等同于 RPC `UserOnline` 的 Kafka 事件驱动版本，保证最终一致性。

```go
func (c *PresenceConsumer) HandleConnConnected(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_connecte.ConnConnectedEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 ConnConnectedEvent 失败", "err", err)
        return nil // 反序列化失败不重试
    }

    // ==================== 2. 幂等检查 — Redis ====================
    // Key: presence:kafka:dedup:{event_id}  TTL: 24h
    dedupKey := fmt.Sprintf("presence:kafka:dedup:%s", event.Header.EventId)
    set, err := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result()
    if err != nil {
        return fmt.Errorf("Redis 幂等检查失败: %w", err)
    }
    if !set {
        log.Debug("重复事件，跳过", "event_id", event.Header.EventId)
        return nil
    }

    // ==================== 3. 参数校验 ====================
    if event.UserId == "" || event.ServerId == "" {
        log.Error("conn.connected: 关键字段缺失",
            "user_id", event.UserId, "server_id", event.ServerId)
        return nil // 数据异常不重试
    }

    now := time.Now().UnixMilli()

    // ==================== 4. 检查离线优雅期 — 是否为重连 ====================
    // Key: presence:offline_grace:{user_id}  STRING  TTL: 30s
    graceKey := fmt.Sprintf("presence:offline_grace:%s", event.UserId)
    graceVal, graceErr := c.redis.Get(ctx, graceKey).Result()
    isReconnect := false
    if graceErr == nil && graceVal != "" {
        // 优雅期内重连 → 取消优雅期定时器，不广播上线事件
        isReconnect = true
        c.redis.Del(ctx, graceKey)
        log.Info("用户在优雅期内重连（Kafka 驱动）",
            "user_id", event.UserId,
            "platform", event.Platform,
            "grace_val", graceVal)
    }

    // ==================== 5. 检查是否为首个平台上线 ====================
    // Key: presence:user_platforms:{user_id}  SET
    platformsKey := fmt.Sprintf("presence:user_platforms:%s", event.UserId)
    existingCount, _ := c.redis.SCard(ctx, platformsKey).Result()
    isFirstPlatform := existingCount == 0 && !isReconnect

    // ==================== 6. Redis Pipeline — 原子更新在线状态 ====================
    statusKey := fmt.Sprintf("presence:status:%s", event.UserId)
    platformMember := fmt.Sprintf("%d:%s:%s", int(event.Platform), event.DeviceId, event.ServerId)

    pipe := c.redis.Pipeline()

    // 6a. 更新/创建用户状态 HASH
    if isFirstPlatform {
        // 首个平台上线：设置 ONLINE 状态
        pipe.HSet(ctx, statusKey, map[string]interface{}{
            "status":      int(common.ONLINE_STATUS_ONLINE),
            "custom_text": "",
            "platform":    int(event.Platform),
            "device_id":   event.DeviceId,
            "server_id":   event.ServerId,
            "last_active": now,
            "updated_at":  now,
        })
    } else {
        // 已有其他平台在线：仅更新活跃时间
        pipe.HSet(ctx, statusKey, map[string]interface{}{
            "last_active": now,
            "updated_at":  now,
        })
    }
    pipe.Expire(ctx, statusKey, 6*time.Minute)

    // 6b. 添加平台到用户平台集合
    pipe.SAdd(ctx, platformsKey, platformMember)
    pipe.Expire(ctx, platformsKey, 6*time.Minute)

    // 6c. 更新全局在线 HyperLogLog
    pipe.PFAdd(ctx, "presence:online", event.UserId)

    // 6d. 更新分平台 HyperLogLog
    platformHLLKey := fmt.Sprintf("presence:online:platform:%d", int(event.Platform))
    pipe.PFAdd(ctx, platformHLLKey, event.UserId)

    if _, err := pipe.Exec(ctx); err != nil {
        return fmt.Errorf("上线状态 Redis Pipeline 失败: %w", err)
    }

    // ==================== 7. 生产 presence.online 事件 → Push 服务 ====================
    // 重连不广播（优雅期内重连对订阅者不可见）
    if !isReconnect || isFirstPlatform {
        onlineEvent := &kafka_presence.PresenceOnlineEvent{
            Header:      buildEventHeader("presence", c.instanceID),
            UserId:      event.UserId,
            Platform:    event.Platform,
            DeviceId:    event.DeviceId,
            ServerId:    event.ServerId,
            ServerAddr:  event.ServerAddr,
            OnlineTime:  now,
            IsReconnect: isReconnect,
        }
        if err := c.kafka.Produce(ctx, "presence.online", event.UserId, onlineEvent); err != nil {
            log.Error("生产 presence.online 事件失败",
                "user_id", event.UserId, "err", err)
        }

        // ==================== 8. 查询订阅者并生产状态通知事件 ====================
        // Key: presence:subs:{user_id}  SET → 谁在订阅此用户
        subsKey := fmt.Sprintf("presence:subs:%s", event.UserId)
        subscribers, err := c.redis.SMembers(ctx, subsKey).Result()
        if err == nil && len(subscribers) > 0 {
            // 生产 presence.status.notify → Push 批量推送给订阅者
            notifyEvent := &kafka_presence.PresenceStatusNotifyEvent{
                Header:        buildEventHeader("presence", c.instanceID),
                UserId:        event.UserId,
                NewStatus:     common.ONLINE_STATUS_ONLINE,
                SubscriberIds: subscribers,
                NotifyTime:    now,
            }
            if err := c.kafka.Produce(ctx, "presence.status.notify", event.UserId, notifyEvent); err != nil {
                log.Error("生产 presence.status.notify 事件失败",
                    "user_id", event.UserId, "subscriber_count", len(subscribers), "err", err)
            }
        }
    }

    log.Info("conn.connected 处理完成",
        "user_id", event.UserId,
        "platform", event.Platform,
        "server_id", event.ServerId,
        "is_reconnect", isReconnect,
        "is_first_platform", isFirstPlatform)

    return nil
}
```

### Consumer: `conn.disconnected`

> 来源：Connecte 网关服务。用户 WebSocket/TCP 连接断开时产生此事件。  
> 职责：从平台集合移除断开的平台 → 如果所有平台均离线则进入 30s 优雅期 → 优雅期结束后确认离线并通知订阅者。  
> **核心防闪烁机制**：30s 优雅期内重连不广播 offline，避免网络抖动导致的状态闪烁。

```go
func (c *PresenceConsumer) HandleConnDisconnected(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_connecte.ConnDisconnectedEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 ConnDisconnectedEvent 失败", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 — Redis ====================
    dedupKey := fmt.Sprintf("presence:kafka:dedup:%s", event.Header.EventId)
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
        log.Error("conn.disconnected: user_id 缺失")
        return nil
    }

    now := time.Now().UnixMilli()

    // ==================== 4. 从平台集合中移除断开的平台 ====================
    // Key: presence:user_platforms:{user_id}  SET
    platformsKey := fmt.Sprintf("presence:user_platforms:%s", event.UserId)
    platformMember := fmt.Sprintf("%d:%s:%s", int(event.Platform), event.DeviceId, event.ServerId)

    removed, err := c.redis.SRem(ctx, platformsKey, platformMember).Result()
    if err != nil {
        return fmt.Errorf("从平台集合移除失败: %w", err)
    }
    if removed == 0 {
        // 平台成员不存在（可能已被其他事件清理），幂等返回
        log.Warn("平台成员不存在，可能已被清理",
            "user_id", event.UserId,
            "platform", event.Platform,
            "member", platformMember)
        return nil
    }

    // ==================== 5. 检查是否还有其他平台在线 ====================
    remainingCount, err := c.redis.SCard(ctx, platformsKey).Result()
    if err != nil {
        return fmt.Errorf("查询剩余平台数失败: %w", err)
    }

    if remainingCount > 0 {
        // 还有其他平台在线 → 仅日志记录，不进入优雅期
        log.Info("单平台断开，其他平台仍在线",
            "user_id", event.UserId,
            "offline_platform", event.Platform,
            "remaining_platforms", remainingCount)
        return nil
    }

    // ==================== 6. 所有平台离线 → 启动 30s 离线优雅期 ====================
    // Key: presence:offline_grace:{user_id}  STRING  TTL: 30s
    graceKey := fmt.Sprintf("presence:offline_grace:%s", event.UserId)
    graceValue := fmt.Sprintf("%d:%s", int(event.Platform), event.DeviceId)
    c.redis.Set(ctx, graceKey, graceValue, time.Duration(c.config.OfflineGracePeriodSec)*time.Second)

    // ==================== 7. 延迟执行离线确认（优雅期后） ====================
    // 生产环境建议使用 Redis Keyspace Notification 或 DelayQueue 替代 goroutine + sleep
    go func() {
        time.Sleep(time.Duration(c.config.OfflineGracePeriodSec) * time.Second)

        bgCtx := context.Background()

        // 7a. 再次检查优雅期 Key 是否仍然存在
        exists, err := c.redis.Exists(bgCtx, graceKey).Result()
        if err != nil {
            log.Error("检查优雅期 Key 失败", "user_id", event.UserId, "err", err)
            return
        }
        if exists == 0 {
            // Key 已被 conn.connected 消费者删除 → 用户已重连
            log.Info("用户已在优雅期内重连，取消离线确认",
                "user_id", event.UserId)
            return
        }

        // 7b. 删除优雅期 Key
        c.redis.Del(bgCtx, graceKey)

        // 7c. 再次确认所有平台已离线（防止优雅期内有新平台上线）
        currentCount, _ := c.redis.SCard(bgCtx, platformsKey).Result()
        if currentCount > 0 {
            log.Info("优雅期结束但有新平台上线，取消离线确认",
                "user_id", event.UserId,
                "current_platforms", currentCount)
            return
        }

        // ---- 用户确认离线 ----

        // 7d. 读取最后在线时间（计算在线时长）
        statusKey := fmt.Sprintf("presence:status:%s", event.UserId)
        lastActiveStr, _ := c.redis.HGet(bgCtx, statusKey, "last_active").Result()
        lastActive, _ := strconv.ParseInt(lastActiveStr, 10, 64)
        onlineDuration := int64(0)
        if lastActive > 0 {
            onlineDuration = (time.Now().UnixMilli() - lastActive) / 1000 // 转为秒
        }

        // 7e. 删除用户状态 HASH
        c.redis.Del(bgCtx, statusKey)

        // 7f. 删除平台集合（防御性清理）
        c.redis.Del(bgCtx, platformsKey)

        // 7g. 生产 presence.offline 事件
        offlineEvent := &kafka_presence.PresenceOfflineEvent{
            Header:         buildEventHeader("presence", c.instanceID),
            UserId:         event.UserId,
            Platform:       event.Platform,
            DeviceId:       event.DeviceId,
            Reason:         event.Reason,
            OfflineTime:    time.Now().UnixMilli(),
            OnlineDuration: onlineDuration,
        }
        if err := c.kafka.Produce(bgCtx, "presence.offline", event.UserId, offlineEvent); err != nil {
            log.Error("生产 presence.offline 事件失败",
                "user_id", event.UserId, "err", err)
        }

        // 7h. 查询订阅者并生产离线通知事件
        subsKey := fmt.Sprintf("presence:subs:%s", event.UserId)
        subscribers, err := c.redis.SMembers(bgCtx, subsKey).Result()
        if err == nil && len(subscribers) > 0 {
            notifyEvent := &kafka_presence.PresenceStatusNotifyEvent{
                Header:        buildEventHeader("presence", c.instanceID),
                UserId:        event.UserId,
                NewStatus:     common.ONLINE_STATUS_OFFLINE,
                SubscriberIds: subscribers,
                NotifyTime:    time.Now().UnixMilli(),
            }
            if err := c.kafka.Produce(bgCtx, "presence.status.notify", event.UserId, notifyEvent); err != nil {
                log.Error("生产离线 presence.status.notify 事件失败",
                    "user_id", event.UserId, "err", err)
            }
        }

        log.Info("用户确认离线（优雅期结束）",
            "user_id", event.UserId,
            "platform", event.Platform,
            "online_duration_sec", onlineDuration)
    }()

    log.Info("conn.disconnected 处理完成，进入优雅期",
        "user_id", event.UserId,
        "platform", event.Platform,
        "grace_period_sec", c.config.OfflineGracePeriodSec)

    return nil
}
```

### Consumer: `relation.friend.accepted`

> 来源：Relation 服务。好友关系建立后触发。  
> 职责：自动建立双向在线状态订阅 — A 订阅 B 的状态变更，B 也订阅 A 的状态变更。  
> 这样当好友上线/下线/状态变更时，对方会收到实时推送。  
> 使用 Redis Pipeline 批量操作 4 个 SET（双向 × 两种视角）。

```go
func (c *PresenceConsumer) HandleRelationFriendAccepted(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_relation.FriendAcceptedEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 FriendAcceptedEvent 失败", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 — Redis ====================
    dedupKey := fmt.Sprintf("presence:kafka:dedup:%s", event.Header.EventId)
    set, err := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result()
    if err != nil {
        return fmt.Errorf("Redis 幂等检查失败: %w", err)
    }
    if !set {
        log.Debug("重复事件，跳过", "event_id", event.Header.EventId)
        return nil
    }

    // ==================== 3. 参数校验 ====================
    userA := event.FromUserId  // 申请方
    userB := event.ToUserId    // 接受方
    if userA == "" || userB == "" {
        log.Error("relation.friend.accepted: 用户 ID 缺失",
            "from_user_id", userA, "to_user_id", userB)
        return nil
    }

    // ==================== 4. Redis Pipeline — 建立双向订阅关系 ====================
    pipe := c.redis.Pipeline()

    // 4a. A 订阅 B 的状态变更
    // presence:subs:{B} → 添加 A（B 的订阅者列表中加入 A）
    subsKeyB := fmt.Sprintf("presence:subs:%s", userB)
    pipe.SAdd(ctx, subsKeyB, userA)
    pipe.Expire(ctx, subsKeyB, 24*time.Hour)

    // presence:my_subs:{A} → 添加 B（A 的订阅目标列表中加入 B）
    mySubsKeyA := fmt.Sprintf("presence:my_subs:%s", userA)
    pipe.SAdd(ctx, mySubsKeyA, userB)
    pipe.Expire(ctx, mySubsKeyA, 24*time.Hour)

    // 4b. B 订阅 A 的状态变更
    // presence:subs:{A} → 添加 B（A 的订阅者列表中加入 B）
    subsKeyA := fmt.Sprintf("presence:subs:%s", userA)
    pipe.SAdd(ctx, subsKeyA, userB)
    pipe.Expire(ctx, subsKeyA, 24*time.Hour)

    // presence:my_subs:{B} → 添加 A（B 的订阅目标列表中加入 A）
    mySubsKeyB := fmt.Sprintf("presence:my_subs:%s", userB)
    pipe.SAdd(ctx, mySubsKeyB, userA)
    pipe.Expire(ctx, mySubsKeyB, 24*time.Hour)

    if _, err := pipe.Exec(ctx); err != nil {
        return fmt.Errorf("建立双向订阅关系 Redis Pipeline 失败: %w", err)
    }

    // ==================== 5. 立即推送双方当前在线状态给对方 ====================
    // A 如果在线 → 通知 B（B 刚加了 A 为好友，应该立刻看到 A 的在线状态）
    c.notifyCurrentPresence(ctx, userA, userB)
    // B 如果在线 → 通知 A
    c.notifyCurrentPresence(ctx, userB, userA)

    log.Info("好友关系建立，双向订阅已创建",
        "user_a", userA,
        "user_b", userB)

    return nil
}

// notifyCurrentPresence 查询 sourceUser 当前在线状态，如果在线则通知 targetUser
func (c *PresenceConsumer) notifyCurrentPresence(ctx context.Context, sourceUser, targetUser string) {
    // 查询 sourceUser 当前状态
    statusKey := fmt.Sprintf("presence:status:%s", sourceUser)
    result, err := c.redis.HGetAll(ctx, statusKey).Result()
    if err != nil || len(result) == 0 {
        return // 离线，无需通知
    }

    statusInt, _ := strconv.Atoi(result["status"])
    userStatus := common.OnlineStatus(statusInt)

    // 隐身不通知
    if userStatus == common.ONLINE_STATUS_INVISIBLE {
        return
    }

    // 生产状态通知事件 → Push 推送给 targetUser
    notifyEvent := &kafka_presence.PresenceStatusNotifyEvent{
        Header:        buildEventHeader("presence", c.instanceID),
        UserId:        sourceUser,
        NewStatus:     userStatus,
        CustomStatus:  result["custom_text"],
        SubscriberIds: []string{targetUser},
        NotifyTime:    time.Now().UnixMilli(),
    }
    if err := c.kafka.Produce(ctx, "presence.status.notify", sourceUser, notifyEvent); err != nil {
        log.Error("生产好友在线状态通知事件失败",
            "source_user", sourceUser, "target_user", targetUser, "err", err)
    }
}
```

### Consumer: `relation.friend.deleted`

> 来源：Relation 服务。好友关系删除后触发。  
> 职责：移除双向在线状态订阅 — A 不再订阅 B，B 也不再订阅 A。  
> 删除好友后双方不应再收到对方的上下线通知。

```go
func (c *PresenceConsumer) HandleRelationFriendDeleted(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_relation.FriendDeletedEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 FriendDeletedEvent 失败", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 — Redis ====================
    dedupKey := fmt.Sprintf("presence:kafka:dedup:%s", event.Header.EventId)
    set, err := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result()
    if err != nil {
        return fmt.Errorf("Redis 幂等检查失败: %w", err)
    }
    if !set {
        log.Debug("重复事件，跳过", "event_id", event.Header.EventId)
        return nil
    }

    // ==================== 3. 参数校验 ====================
    operatorID := event.OperatorId // 执行删除操作的用户
    targetID := event.TargetId     // 被删除的好友
    if operatorID == "" || targetID == "" {
        log.Error("relation.friend.deleted: 用户 ID 缺失",
            "operator_id", operatorID, "target_id", targetID)
        return nil
    }

    // ==================== 4. Redis Pipeline — 移除双向订阅关系 ====================
    pipe := c.redis.Pipeline()

    // 4a. 移除 A 对 B 的订阅
    // presence:subs:{B} → 移除 A
    subsKeyB := fmt.Sprintf("presence:subs:%s", targetID)
    pipe.SRem(ctx, subsKeyB, operatorID)

    // presence:my_subs:{A} → 移除 B
    mySubsKeyA := fmt.Sprintf("presence:my_subs:%s", operatorID)
    pipe.SRem(ctx, mySubsKeyA, targetID)

    // 4b. 移除 B 对 A 的订阅
    // presence:subs:{A} → 移除 B
    subsKeyA := fmt.Sprintf("presence:subs:%s", operatorID)
    pipe.SRem(ctx, subsKeyA, targetID)

    // presence:my_subs:{B} → 移除 A
    mySubsKeyB := fmt.Sprintf("presence:my_subs:%s", targetID)
    pipe.SRem(ctx, mySubsKeyB, operatorID)

    if _, err := pipe.Exec(ctx); err != nil {
        return fmt.Errorf("移除双向订阅关系 Redis Pipeline 失败: %w", err)
    }

    log.Info("好友关系删除，双向订阅已移除",
        "operator_id", operatorID,
        "target_id", targetID)

    return nil
}
```

### Consumer: `user.deactivated`

> 来源：User 服务。用户注销账户时触发。  
> 职责：清理该用户的所有 Presence 数据 — 在线状态、平台列表、所有订阅关系（双向）、优雅期、typing 状态。  
> **关键**：需要先查出所有订阅关系，再从对方的 SET 中移除自己，最后删除自己的 SET。  
> 使用 SCAN + Pipeline 批量清理，避免大 key 阻塞 Redis。

```go
func (c *PresenceConsumer) HandleUserDeactivated(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_user.UserDeactivatedEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 UserDeactivatedEvent 失败", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 — Redis ====================
    dedupKey := fmt.Sprintf("presence:kafka:dedup:%s", event.Header.EventId)
    set, err := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result()
    if err != nil {
        return fmt.Errorf("Redis 幂等检查失败: %w", err)
    }
    if !set {
        log.Debug("重复事件，跳过", "event_id", event.Header.EventId)
        return nil
    }

    // ==================== 3. 参数校验 ====================
    userID := event.UserId
    if userID == "" {
        log.Error("user.deactivated: user_id 缺失")
        return nil
    }

    log.Info("开始清理用户 Presence 数据", "user_id", userID)

    // ==================== 4. 查询该用户订阅了谁 — 用于从对方订阅者列表中移除自己 ====================
    // Key: presence:my_subs:{user_id}  SET → 我订阅了谁
    mySubsKey := fmt.Sprintf("presence:my_subs:%s", userID)
    myTargets, err := c.redis.SMembers(ctx, mySubsKey).Result()
    if err != nil {
        log.Error("查询用户订阅目标列表失败", "user_id", userID, "err", err)
        myTargets = nil // 降级：跳过这一步
    }

    // ==================== 5. 查询谁在订阅该用户 — 用于从订阅者的订阅列表中移除该用户 ====================
    // Key: presence:subs:{user_id}  SET → 谁在订阅我
    subsKey := fmt.Sprintf("presence:subs:%s", userID)
    mySubscribers, err := c.redis.SMembers(ctx, subsKey).Result()
    if err != nil {
        log.Error("查询用户订阅者列表失败", "user_id", userID, "err", err)
        mySubscribers = nil // 降级：跳过这一步
    }

    // ==================== 6. Redis Pipeline — 批量清理订阅关系 ====================
    pipe := c.redis.Pipeline()

    // 6a. 从我订阅的每个目标用户的订阅者列表中移除自己
    // 即：对每个 target_id，从 presence:subs:{target_id} 中移除 user_id
    for _, targetID := range myTargets {
        targetSubsKey := fmt.Sprintf("presence:subs:%s", targetID)
        pipe.SRem(ctx, targetSubsKey, userID)
    }

    // 6b. 从订阅我的每个用户的订阅目标列表中移除自己
    // 即：对每个 subscriber_id，从 presence:my_subs:{subscriber_id} 中移除 user_id
    for _, subscriberID := range mySubscribers {
        subscriberMySubsKey := fmt.Sprintf("presence:my_subs:%s", subscriberID)
        pipe.SRem(ctx, subscriberMySubsKey, userID)
    }

    // 6c. 删除该用户自身的所有 Presence Key
    statusKey := fmt.Sprintf("presence:status:%s", userID)
    platformsKey := fmt.Sprintf("presence:user_platforms:%s", userID)
    graceKey := fmt.Sprintf("presence:offline_grace:%s", userID)

    pipe.Del(ctx, statusKey)      // 在线状态 HASH
    pipe.Del(ctx, platformsKey)   // 平台集合
    pipe.Del(ctx, subsKey)        // 我的订阅者列表
    pipe.Del(ctx, mySubsKey)      // 我的订阅目标列表
    pipe.Del(ctx, graceKey)       // 离线优雅期

    if _, err := pipe.Exec(ctx); err != nil {
        log.Error("清理用户 Presence 数据 Redis Pipeline 失败",
            "user_id", userID, "err", err)
        return fmt.Errorf("清理 Presence 数据失败: %w", err)
    }

    // ==================== 7. SCAN 清理 typing 相关 Key ====================
    // Key 模式: presence:typing:*:{user_id}
    // 因为 typing key 格式是 presence:typing:{conversation_id}:{user_id}
    // 需要用 SCAN 匹配
    cursor := uint64(0)
    typingPattern := fmt.Sprintf("presence:typing:*:%s", userID)
    for {
        keys, nextCursor, err := c.redis.Scan(ctx, cursor, typingPattern, 100).Result()
        if err != nil {
            log.Error("SCAN 清理 typing Key 失败", "user_id", userID, "err", err)
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

    // ==================== 8. 生产 presence.offline 事件（通知订阅者该用户永久离线） ====================
    if len(mySubscribers) > 0 {
        offlineEvent := &kafka_presence.PresenceOfflineEvent{
            Header:      buildEventHeader("presence", c.instanceID),
            UserId:      userID,
            Reason:      "user_deactivated",
            OfflineTime: time.Now().UnixMilli(),
        }
        if err := c.kafka.Produce(ctx, "presence.offline", userID, offlineEvent); err != nil {
            log.Error("生产注销用户离线事件失败", "user_id", userID, "err", err)
        }

        // 通知所有订阅者
        notifyEvent := &kafka_presence.PresenceStatusNotifyEvent{
            Header:        buildEventHeader("presence", c.instanceID),
            UserId:        userID,
            NewStatus:     common.ONLINE_STATUS_OFFLINE,
            SubscriberIds: mySubscribers,
            NotifyTime:    time.Now().UnixMilli(),
        }
        if err := c.kafka.Produce(ctx, "presence.status.notify", userID, notifyEvent); err != nil {
            log.Error("生产注销用户状态通知失败", "user_id", userID, "err", err)
        }
    }

    log.Info("用户 Presence 数据清理完成",
        "user_id", userID,
        "cleaned_targets", len(myTargets),
        "cleaned_subscribers", len(mySubscribers))

    return nil
}
```

### Consumer: `config.changed`

> 来源：Config 服务。配置变更时触发。  
> 职责：热更新 Presence 服务的运行时配置参数，包括心跳间隔、优雅期时长、订阅上限、HyperLogLog 重建频率等。  
> 配置变更立即生效，无需重启服务。

```go
func (c *PresenceConsumer) HandleConfigChanged(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_config.ConfigChangedEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 ConfigChangedEvent 失败", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 — Redis ====================
    dedupKey := fmt.Sprintf("presence:kafka:dedup:%s", event.Header.EventId)
    set, err := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result()
    if err != nil {
        return fmt.Errorf("Redis 幂等检查失败: %w", err)
    }
    if !set {
        log.Debug("重复事件，跳过", "event_id", event.Header.EventId)
        return nil
    }

    // ==================== 3. 过滤非 Presence 相关配置 ====================
    // 只处理 scope 为 "presence" 或 "global" 的配置变更
    if event.Scope != "presence" && event.Scope != "global" {
        return nil // 非本服务配置，跳过
    }

    // ==================== 4. 按 Key 更新运行时配置 ====================
    configUpdated := false
    for key, value := range event.Changes {
        switch key {
        case "presence.heartbeat_interval_sec":
            // 心跳间隔（秒）— 通知 Gateway 调整心跳上报频率
            interval, err := strconv.Atoi(value)
            if err != nil || interval < 10 || interval > 300 {
                log.Warn("无效的心跳间隔配置", "value", value)
                continue
            }
            c.config.HeartbeatIntervalSec = interval
            configUpdated = true
            log.Info("心跳间隔已更新", "new_interval_sec", interval)

        case "presence.offline_grace_period_sec":
            // 离线优雅期（秒）— 控制断线后多久确认离线
            gracePeriod, err := strconv.Atoi(value)
            if err != nil || gracePeriod < 5 || gracePeriod > 120 {
                log.Warn("无效的优雅期配置", "value", value)
                continue
            }
            c.config.OfflineGracePeriodSec = gracePeriod
            configUpdated = true
            log.Info("离线优雅期已更新", "new_grace_period_sec", gracePeriod)

        case "presence.status_ttl_sec":
            // 状态 Key TTL（秒）— 控制无心跳多久后状态过期
            ttl, err := strconv.Atoi(value)
            if err != nil || ttl < 60 || ttl > 600 {
                log.Warn("无效的状态 TTL 配置", "value", value)
                continue
            }
            c.config.StatusTTLSec = ttl
            configUpdated = true
            log.Info("状态 TTL 已更新", "new_ttl_sec", ttl)

        case "presence.max_subscriptions":
            // 单用户最大订阅数
            maxSubs, err := strconv.Atoi(value)
            if err != nil || maxSubs < 100 || maxSubs > 5000 {
                log.Warn("无效的最大订阅数配置", "value", value)
                continue
            }
            c.config.MaxSubscriptions = maxSubs
            configUpdated = true
            log.Info("最大订阅数已更新", "new_max_subs", maxSubs)

        case "presence.hll_rebuild_interval_min":
            // HyperLogLog 重建频率（分钟）
            interval, err := strconv.Atoi(value)
            if err != nil || interval < 1 || interval > 60 {
                log.Warn("无效的 HLL 重建频率配置", "value", value)
                continue
            }
            c.config.HLLRebuildIntervalMin = interval
            configUpdated = true
            log.Info("HLL 重建频率已更新", "new_interval_min", interval)

        case "presence.typing_throttle_sec":
            // typing 节流时间（秒）
            throttle, err := strconv.Atoi(value)
            if err != nil || throttle < 1 || throttle > 30 {
                log.Warn("无效的 typing 节流配置", "value", value)
                continue
            }
            c.config.TypingThrottleSec = throttle
            configUpdated = true
            log.Info("typing 节流时间已更新", "new_throttle_sec", throttle)

        default:
            // 未识别的配置 Key，跳过
            log.Debug("未识别的 Presence 配置项", "key", key, "value", value)
        }
    }

    if configUpdated {
        log.Info("Presence 配置热更新完成",
            "scope", event.Scope,
            "changes_count", len(event.Changes))
    }

    return nil
}
```

---

## 辅助函数

### buildEventHeader — 构建 Kafka 事件头

```go
func buildEventHeader(source string, instanceID string) *common.EventHeader {
    return &common.EventHeader{
        EventId:   uuid.New().String(),
        TraceId:   trace.SpanFromContext(context.Background()).SpanContext().TraceID().String(),
        SpanId:    trace.SpanFromContext(context.Background()).SpanContext().SpanID().String(),
        Source:    source,
        SourceId:  instanceID,
        Timestamp: time.Now().UnixMilli(),
        Version:   1,
        Metadata:  map[string]string{},
    }
}
```

### PresenceConsumer 配置结构

```go
// PresenceConsumerConfig 消费者运行时配置（支持热更新）
type PresenceConsumerConfig struct {
    HeartbeatIntervalSec  int // 心跳间隔，默认 60
    OfflineGracePeriodSec int // 离线优雅期，默认 30
    StatusTTLSec          int // 状态 Key TTL，默认 360 (6min)
    MaxSubscriptions      int // 单用户最大订阅数，默认 500
    HLLRebuildIntervalMin int // HLL 重建频率，默认 10
    TypingThrottleSec     int // typing 节流时间，默认 5
}

func defaultPresenceConsumerConfig() *PresenceConsumerConfig {
    return &PresenceConsumerConfig{
        HeartbeatIntervalSec:  60,
        OfflineGracePeriodSec: 30,
        StatusTTLSec:          360,
        MaxSubscriptions:      500,
        HLLRebuildIntervalMin: 10,
        TypingThrottleSec:     5,
    }
}
```

### 定时统计任务 — 生产 presence.stats 事件

```go
// startStatsReporter 每 60s 生产一次 Presence 统计事件
func (c *PresenceConsumer) startStatsReporter(ctx context.Context) {
    ticker := time.NewTicker(60 * time.Second)
    defer ticker.Stop()

    var peakOnlineCount int64

    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            // 查询当前在线总数
            onlineCount, err := c.redis.PFCount(ctx, "presence:online").Result()
            if err != nil {
                log.Error("查询在线总数失败", "err", err)
                continue
            }

            // 更新峰值
            if onlineCount > peakOnlineCount {
                peakOnlineCount = onlineCount
            }

            now := time.Now().UnixMilli()
            statsEvent := &kafka_presence.PresenceStatsEvent{
                Header:           buildEventHeader("presence", c.instanceID),
                ServerId:         c.instanceID,
                PeriodStart:      now - 60000, // 前 60s
                PeriodEnd:        now,
                OnlineUserCount:  onlineCount,
                PeakOnlineCount:  peakOnlineCount,
            }
            if err := c.kafka.Produce(ctx, "presence.stats", c.instanceID, statsEvent); err != nil {
                log.Error("生产 presence.stats 事件失败", "err", err)
            }
        }
    }
}
```

---

## 性能考量

### 消费者并发度设计

| Consumer | 并发度 | 理由 |
|----------|--------|------|
| `conn.connected` | 16 | 高频热路径，峰值数万/秒（早高峰集中上线） |
| `conn.disconnected` | 16 | 与 connected 对称，断线事件同样高频 |
| `relation.friend.accepted` | 4 | 低频事件（好友建立），不需要高并发 |
| `relation.friend.deleted` | 4 | 低频事件（好友删除） |
| `user.deactivated` | 2 | 极低频事件（用户注销），但单次处理较重 |
| `config.changed` | 1 | 极低频配置变更，无需并发 |

### 离线优雅期 30s 的工程权衡

| 考量 | 说明 |
|------|------|
| 为什么不用更短（如 5s）？ | 电梯/地铁场景下 5s 太短，频繁闪烁影响用户体验 |
| 为什么不用更长（如 60s）？ | 60s 延迟过大，好友等太久才看到离线状态 |
| 30s 的 goroutine 开销 | 10M 用户中同时断线的不会太多，goroutine 开销可控 |
| 生产环境替代方案 | 建议用 Redis Keyspace Notification 或 Kafka Delay Topic 替代 goroutine + sleep |

### 订阅关系扇出控制

| 指标 | 值 |
|------|-----|
| 单用户最大订阅数 | 500（可配置） |
| 状态变更通知扇出 | = 订阅者数（好友数），通常 < 200 |
| 通知批量投递 | 一次 Kafka 事件携带全部 subscriber_ids，Push 服务批量处理 |
| 隐身用户 | 不产生通知事件，零扇出 |
