# User 服务 — Kafka 消费者/生产者实现伪代码

## 概述

User 服务通过 Kafka 接收其他服务的事件通知（注册成功、头像处理、违规处理等），并生产用户生命周期事件供下游消费。

## 生产 Topic 列表

| Topic | 事件 | 生产时机 | 消费方 |
|-------|------|----------|--------|
| `user.registered` | UserRegisteredEvent | Register RPC 成功后 | Search, Audit |
| `user.profile.updated` | UserProfileUpdatedEvent | UpdateUser RPC 成功后 | Search, Audit, Conversation |
| `user.avatar.updated` | UserAvatarUpdatedEvent | 头像变更时 | Media, Search |
| `user.settings.updated` | UserSettingsUpdatedEvent | UpdateUserSettings RPC 成功后 | Message, Push |
| `user.deactivated` | UserDeactivatedEvent | DeactivateUser 成功后 | Auth, Session, Group, Relation, Search, Conversation |
| `user.banned` | UserBannedEvent | 管理员封禁操作 | Auth, Session, Group, Presence, Notification |
| `user.unbanned` | UserUnbannedEvent | 管理员解禁操作 | Auth |
| `user.stats` | UserStatsEvent | 定时统计任务 | Audit |

## 消费 Topic 列表

| Topic | 来源 | 用途 |
|-------|------|------|
| `auth.register.success` | Auth | 注册成功后同步创建用户记录（如果采用 Auth 先行模式） |
| `media.avatar.processed` | Media | 头像处理完成后更新用户头像 URL |
| `audit.user.violation` | Audit | 违规处理（封禁/警告） |
| `config.changed` | Config | 配置变更（热更新） |

---

## 辅助方法

```go
// delayedDoubleDelete 延迟双删策略：先删缓存 → 执行 DB 操作 → 延迟再删一次
func (c *UserConsumer) delayedDoubleDelete(ctx context.Context, keys ...string) {
    if len(keys) == 0 {
        return
    }
    c.redis.Del(ctx, keys...)
    go func() {
        time.Sleep(500 * time.Millisecond)
        c.redis.Del(context.Background(), keys...)
    }()
}
```

---

## 消费者实现

### Consumer: `auth.register.success`

> 来源：Auth 服务。当用户通过 Auth 注册成功后，User 服务消费此事件确认用户数据一致性。
> 在本架构中 Register RPC 在 User 服务执行，此 consumer 仅做补偿校验。

```go
func (c *UserConsumer) HandleRegisterSuccess(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_auth.RegisterSuccessEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("unmarshal RegisterSuccessEvent failed", "err", err)
        return nil // 反序列化失败，跳过（不重试）
    }

    // ==================== 2. 幂等检查 — Redis ====================
    dedupKey := fmt.Sprintf("user:kafka:dedup:%s", event.Header.EventId)
    set, err := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result()
    if err != nil {
        return fmt.Errorf("redis dedup check failed: %w", err)
    }
    if !set {
        log.Info("duplicate event, skip", "event_id", event.Header.EventId)
        return nil
    }

    // ==================== 3. 校验用户是否已存在 — PgSQL ====================
    var exists bool
    err = c.db.QueryRowContext(ctx,
        "SELECT EXISTS(SELECT 1 FROM users WHERE user_id = $1)", event.UserId,
    ).Scan(&exists)
    if err != nil {
        return fmt.Errorf("db query failed: %w", err)
    }

    if exists {
        log.Info("user already exists, skip", "user_id", event.UserId)
        return nil
    }

    // ==================== 4. 补偿创建用户记录 — PgSQL ====================
    now := time.Now().UnixMilli()
    _, err = c.db.ExecContext(ctx,
        `INSERT INTO users (user_id, username, nickname, avatar_url, created_at, updated_at)
         VALUES ($1, $2, $3, '', $4, $5)
         ON CONFLICT (user_id) DO NOTHING`,
        event.UserId, event.Username, event.Nickname, now, now,
    )
    if err != nil {
        return fmt.Errorf("insert user failed: %w", err)
    }

    log.Info("compensate user record created", "user_id", event.UserId)
    return nil
}
```

### Consumer: `media.avatar.processed`

> 来源：Media 服务。头像上传处理完成后通知 User 服务更新头像 URL。

```go
func (c *UserConsumer) HandleAvatarProcessed(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_media.MediaProcessCompletedEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("unmarshal MediaProcessCompletedEvent failed", "err", err)
        return nil
    }

    // ==================== 2. 过滤非头像事件 ====================
    if event.ProcessType != "thumbnail" || event.MediaType != kafka_media.MEDIA_TYPE_AVATAR {
        return nil // 不是头像处理，跳过
    }

    // ==================== 3. 幂等检查 ====================
    dedupKey := fmt.Sprintf("user:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    // ==================== 4. 通过 media_id 找到 user_id ====================
    // 从事件中已有 media_id，需要查询 media_records 找到 uploader_id
    // 或者事件中直接携带了 user_id 信息（通过 msg_id 关联）
    // 这里假设 media service 在事件中包含了 uploader info
    // 实际通过 media_id 查 media_records

    // ==================== 5. 延迟双删 — 先删缓存 ====================
    now := time.Now().UnixMilli()
    userID := event.Header.Metadata["user_id"]
    c.delayedDoubleDelete(ctx, fmt.Sprintf("user:info:%s", userID))

    // ==================== 6. 更新头像 URL — PgSQL ====================
    _, err := c.db.ExecContext(ctx,
        `UPDATE users SET avatar_url = $1, updated_at = $2 WHERE user_id = $3`,
        event.ResultUrl, now, userID,
    )
    if err != nil {
        return fmt.Errorf("update avatar failed: %w", err)
    }

    log.Info("avatar updated from media event", "user_id", userID, "url", event.ResultUrl)
    return nil
}
```

### Consumer: `audit.user.violation`

> 来源：Audit 服务。内容审核发现用户违规（昵称/头像等），触发封禁或警告。

```go
func (c *UserConsumer) HandleUserViolation(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_audit.UserViolationEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("unmarshal UserViolationEvent failed", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 ====================
    dedupKey := fmt.Sprintf("user:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    // ==================== 3. 根据违规级别处理 ====================
    now := time.Now().UnixMilli()

    switch event.Action {
    case "ban":
        // 延迟双删 — 先删缓存
        c.delayedDoubleDelete(ctx,
            fmt.Sprintf("user:info:%s", event.UserId),
            fmt.Sprintf("user:settings:%s", event.UserId),
        )

        // 封禁用户
        _, err := c.db.ExecContext(ctx,
            `UPDATE users SET status = 3, updated_at = $1 WHERE user_id = $2`,
            now, event.UserId,
        )
        if err != nil {
            return fmt.Errorf("ban user failed: %w", err)
        }

        // 生产 user.banned 事件
        bannedEvent := &kafka_user.UserBannedEvent{
            Header:      buildEventHeader("user", c.instanceID),
            UserId:      event.UserId,
            Reason:      event.Reason,
            OperatorId:  "system",
            BanDuration: event.BanDuration,
            ExpireAt:    event.ExpireAt,
            BanTime:     now,
        }
        if err = c.kafka.Produce(ctx, "user.banned", event.UserId, bannedEvent); err != nil {
            log.Error("produce user.banned failed", "err", err)
        }

    case "reset_avatar":
        // 延迟双删 — 先删缓存
        c.delayedDoubleDelete(ctx, fmt.Sprintf("user:info:%s", event.UserId))

        // 重置头像为默认
        _, err := c.db.ExecContext(ctx,
            `UPDATE users SET avatar_url = '', updated_at = $1 WHERE user_id = $2`,
            now, event.UserId,
        )
        if err != nil {
            return fmt.Errorf("reset avatar failed: %w", err)
        }

    case "reset_nickname":
        // 延迟双删 — 先删缓存
        c.delayedDoubleDelete(ctx, fmt.Sprintf("user:info:%s", event.UserId))

        // 重置昵称
        _, err := c.db.ExecContext(ctx,
            `UPDATE users SET nickname = CONCAT('用户', user_id), updated_at = $1 WHERE user_id = $2`,
            now, event.UserId,
        )
        if err != nil {
            return fmt.Errorf("reset nickname failed: %w", err)
        }
    }

    log.Info("user violation handled", "user_id", event.UserId, "action", event.Action)
    return nil
}
```

### Consumer: `config.changed`

> 来源：Config 服务。全局配置变更，User 服务热更新相关配置。

```go
func (c *UserConsumer) HandleConfigChanged(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_config.ConfigChangedEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("unmarshal ConfigChangedEvent failed", "err", err)
        return nil
    }

    // ==================== 2. 过滤非本服务的配置 ====================
    affectsUser := false
    for _, svc := range event.AffectedServices {
        if svc == "user" {
            affectsUser = true
            break
        }
    }
    if !affectsUser {
        return nil
    }

    // ==================== 3. 根据 config_key 更新本地配置 ====================
    switch event.ConfigKey {
    case "user.max_nickname_length":
        val, _ := strconv.Atoi(event.NewValue)
        c.config.Store("max_nickname_length", val)
    case "user.default_avatar_url":
        c.config.Store("default_avatar_url", event.NewValue)
    case "user.registration_enabled":
        val, _ := strconv.ParseBool(event.NewValue)
        c.config.Store("registration_enabled", val)
    default:
        log.Debug("unknown config key for user service", "key", event.ConfigKey)
    }

    log.Info("config updated", "key", event.ConfigKey, "new_value", event.NewValue)
    return nil
}
```

---

## 定时任务：用户统计

```go
// 每 5 分钟执行一次统计并生产 user.stats 事件
func (c *UserConsumer) RunStatsTask(ctx context.Context) {
    ticker := time.NewTicker(5 * time.Minute)
    defer ticker.Stop()

    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            var totalUsers, activeUsers, deactivatedUsers, bannedUsers int64
            c.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM users").Scan(&totalUsers)
            c.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM users WHERE status = 1").Scan(&activeUsers)
            c.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM users WHERE status = 2").Scan(&deactivatedUsers)
            c.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM users WHERE status = 3").Scan(&bannedUsers)

            event := &kafka_user.UserStatsEvent{
                Header:      buildEventHeader("user", c.instanceID),
                TotalUsers:  totalUsers,
                ActiveUsers: activeUsers,
                StatsTime:   time.Now().UnixMilli(),
            }
            c.kafka.Produce(ctx, "user.stats", "stats", event)
        }
    }
}
```
