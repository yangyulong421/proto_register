# Session 登录会话服务 — Kafka 消费者实现伪代码

## 概述

Session 服务通过 Kafka 接收 Auth、Connecte、Config 等服务的事件，维护登录会话生命周期。核心逻辑包括：登录成功后创建会话、登出后销毁会话、Token 刷新后延长会话、连接断开后更新会话状态。

## 生产 Topic 列表

| Topic | 事件 | 生产时机 | 消费方 |
|-------|------|----------|--------|
| `session.created` | SessionCreatedEvent | 会话创建成功 | Presence, Notification |
| `session.expired` | SessionExpiredEvent | 会话过期 / 登出 | Presence, Notification |
| `session.kicked` | SessionKickedEvent | 同平台互踢 / 超设备数踢出 / 用户主动踢出 | Presence, Connecte |
| `session.refreshed` | SessionRefreshedEvent | 会话刷新 | Presence |
| `session.stats` | SessionStatsEvent | 定时统计 | Audit |

## 消费 Topic 列表

| Topic | 来源 | 用途 |
|-------|------|------|
| `auth.login.success` | Auth | 登录成功 → 创建登录会话（RPC 已处理则幂等跳过） |
| `auth.logout` | Auth | 用户登出 → 销毁登录会话 |
| `auth.token.refreshed` | Auth | Token 刷新 → 延长会话有效期 |
| `conn.disconnected` | Connecte | 长连接断开 → 更新会话最后活跃时间 |
| `config.changed` | Config | 配置变更 → 动态更新会话参数 |

## Redis Key 设计（消费者用到的）

| Key 格式 | 类型 | 用途 |
|----------|------|------|
| `session:login:{session_id}` | HASH | 会话信息缓存 |
| `session:user_devices:{user_id}` | SET | 用户活跃设备集合 |
| `session:device:{user_id}:{platform}` | STRING | 同平台最新会话索引 |
| `session:kafka:dedup:{event_id}` | STRING | Kafka 消费幂等去重 |

---

## 辅助方法

```go
// delayedDoubleDelete 延迟双删策略：先删缓存 → 执行 DB 操作 → 延迟再删一次
func (c *SessionConsumer) delayedDoubleDelete(ctx context.Context, keys ...string) {
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

### Consumer: `auth.login.success`

> 来源：Auth 服务。用户登录成功后，Session 服务确认会话已创建。
> 说明：正常流程中 Auth 已通过 RPC 调用 `Session.CreateLoginSession` 创建会话，
> 此 Kafka consumer 作为兜底保障——如果 RPC 调用失败，通过事件补偿创建。

```go
func (c *SessionConsumer) HandleAuthLoginSuccess(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_auth.LoginSuccessEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("unmarshal LoginSuccessEvent failed", "err", err)
        return nil // 反序列化失败不重试，进死信队列
    }

    // ==================== 2. 基本校验 ====================
    if event.UserId == "" || event.DeviceId == "" {
        log.Error("invalid LoginSuccessEvent: missing required fields",
            "user_id", event.UserId, "device_id", event.DeviceId)
        return nil
    }
    if event.Platform == common.PLATFORM_TYPE_UNSPECIFIED {
        log.Error("invalid LoginSuccessEvent: platform unspecified", "user_id", event.UserId)
        return nil
    }

    // ==================== 3. 幂等检查 — Redis ====================
    dedupKey := fmt.Sprintf("session:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        log.Debug("duplicate event, skip", "event_id", event.Header.EventId)
        return nil // 已处理过
    }

    // ==================== 4. 检查 RPC 是否已创建会话 ====================
    // Auth 在 RPC 流程中已调用 CreateLoginSession，这里做补偿检查
    if event.SessionId != "" {
        // 检查会话是否已在 Redis 中
        sessionKey := fmt.Sprintf("session:login:%s", event.SessionId)
        exists, _ := c.redis.Exists(ctx, sessionKey).Result()
        if exists > 0 {
            log.Debug("session already created via RPC, skip",
                "session_id", event.SessionId, "user_id", event.UserId)
            return nil
        }

        // Redis 无缓存，检查 PgSQL
        var dbCount int
        err := c.db.QueryRowContext(ctx,
            `SELECT COUNT(*) FROM login_sessions WHERE session_id = $1 AND status = 1`,
            event.SessionId,
        ).Scan(&dbCount)
        if err == nil && dbCount > 0 {
            // PgSQL 已有记录但 Redis 没有，补写 Redis
            c.rebuildSessionCache(ctx, event.SessionId)
            log.Info("session exists in PgSQL, rebuilt Redis cache",
                "session_id", event.SessionId, "user_id", event.UserId)
            return nil
        }
    }

    // ==================== 5. 补偿创建会话 ====================
    // RPC 创建失败的情况下，通过 Kafka 事件补偿
    now := time.Now().UnixMilli()
    sessionID := event.SessionId
    if sessionID == "" {
        sessionID = generateSessionID() // 生成新的 session_id
    }
    expireAt := now + c.config.SessionLifetimeMs

    // 5a. 同平台互踢检查 — Redis
    platformKey := fmt.Sprintf("session:device:%s:%d", event.UserId, event.Platform)
    oldSessionID, err := c.redis.Get(ctx, platformKey).Result()
    if err == nil && oldSessionID != "" && oldSessionID != sessionID {
        // 踢掉旧会话
        c.invalidateSessionFromConsumer(ctx, oldSessionID, event.UserId, "same_platform_kick", now)
        log.Info("same platform kick from consumer",
            "old_session_id", oldSessionID, "new_session_id", sessionID, "user_id", event.UserId)
    }

    // 5b. 写入 PgSQL — 使用 ON CONFLICT 实现幂等
    _, err = c.db.ExecContext(ctx,
        `INSERT INTO login_sessions (session_id, user_id, platform, device_id, device_name,
            os_version, app_version, ip, login_time, last_active, expire_at, status)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, 1)
         ON CONFLICT (session_id) DO NOTHING`,
        sessionID, event.UserId, int(event.Platform), event.DeviceId, event.DeviceName,
        "", "", event.Ip, event.LoginTime, now, expireAt,
    )
    if err != nil {
        log.Error("insert login_session from consumer failed", "session_id", sessionID, "err", err)
        // 删除幂等 key 以便重试
        c.redis.Del(ctx, dedupKey)
        return fmt.Errorf("insert login_session failed: %w", err)
    }

    // 5c. 写入 Redis
    sessionKey := fmt.Sprintf("session:login:%s", sessionID)
    devicesKey := fmt.Sprintf("session:user_devices:%s", event.UserId)
    ttl := time.Duration(expireAt-now) * time.Millisecond

    pipe := c.redis.Pipeline()
    pipe.HSet(ctx, sessionKey, map[string]interface{}{
        "session_id":  sessionID,
        "user_id":     event.UserId,
        "platform":    int(event.Platform),
        "device_id":   event.DeviceId,
        "device_name": event.DeviceName,
        "os_version":  "",
        "app_version": "",
        "ip":          event.Ip,
        "login_time":  event.LoginTime,
        "last_active": now,
        "expire_at":   expireAt,
        "status":      1,
    })
    pipe.Expire(ctx, sessionKey, ttl)
    pipe.SAdd(ctx, devicesKey, sessionID)
    pipe.Set(ctx, platformKey, sessionID, ttl)
    if _, err := pipe.Exec(ctx); err != nil {
        log.Error("redis pipeline failed in consumer", "session_id", sessionID, "err", err)
    }

    // ==================== 6. 生产 session.created 事件 — Kafka ====================
    createdEvent := &kafka_session.SessionCreatedEvent{
        Header:     buildEventHeader("session", c.instanceID),
        SessionId:  sessionID,
        UserId:     event.UserId,
        Platform:   event.Platform,
        DeviceId:   event.DeviceId,
        DeviceName: event.DeviceName,
        Ip:         event.Ip,
        LoginTime:  event.LoginTime,
        ExpireAt:   expireAt,
    }
    if err := c.kafka.Produce(ctx, "session.created", event.UserId, createdEvent); err != nil {
        log.Error("produce session.created failed", "session_id", sessionID, "err", err)
    }

    log.Info("session created from consumer (compensation)",
        "session_id", sessionID, "user_id", event.UserId, "platform", event.Platform)
    return nil
}
```

### Consumer: `auth.logout`

> 来源：Auth 服务。用户主动登出后，Session 销毁对应设备的登录会话。
> 说明：Auth 正常流程已通过 RPC `Session.InvalidateLoginSession` 销毁，此处为事件兜底。

```go
func (c *SessionConsumer) HandleAuthLogout(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_auth.LogoutEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("unmarshal LogoutEvent failed", "err", err)
        return nil
    }

    // ==================== 2. 基本校验 ====================
    if event.UserId == "" || event.DeviceId == "" {
        log.Error("invalid LogoutEvent: missing required fields",
            "user_id", event.UserId, "device_id", event.DeviceId)
        return nil
    }

    // ==================== 3. 幂等检查 — Redis ====================
    dedupKey := fmt.Sprintf("session:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    now := time.Now().UnixMilli()

    // ==================== 4. 查找目标会话 — PgSQL ====================
    var sessionID string
    var platformInt int
    err := c.db.QueryRowContext(ctx,
        `SELECT session_id, platform FROM login_sessions
         WHERE user_id = $1 AND device_id = $2 AND status = 1
         ORDER BY login_time DESC LIMIT 1`,
        event.UserId, event.DeviceId,
    ).Scan(&sessionID, &platformInt)
    if err == sql.ErrNoRows {
        // 会话已被 RPC 调用销毁，幂等返回
        log.Debug("session already invalidated", "user_id", event.UserId, "device_id", event.DeviceId)
        return nil
    }
    if err != nil {
        c.redis.Del(ctx, dedupKey) // 删除幂等 key 以便重试
        return fmt.Errorf("query login_session failed: %w", err)
    }

    // ==================== 5. 延迟双删 — 先删缓存 ====================
    sessionKey := fmt.Sprintf("session:login:%s", sessionID)
    devicesKey := fmt.Sprintf("session:user_devices:%s", event.UserId)
    platformKey := fmt.Sprintf("session:device:%s:%d", event.UserId, platformInt)
    c.delayedDoubleDelete(ctx, sessionKey)

    // ==================== 6. 更新 PgSQL 状态 → 已登出 ====================
    _, err = c.db.ExecContext(ctx,
        `UPDATE login_sessions SET status = 4, last_active = $1
         WHERE session_id = $2 AND status = 1`,
        now, sessionID,
    )
    if err != nil {
        c.redis.Del(ctx, dedupKey)
        return fmt.Errorf("update session status failed: %w", err)
    }

    // ==================== 7. 清除 Redis 索引 ====================
    pipe := c.redis.Pipeline()
    pipe.SRem(ctx, devicesKey, sessionID)
    pipe.Exec(ctx)

    // 原子删除平台索引（仅当仍指向当前 session 时删除）
    luaScript := `
        if redis.call("GET", KEYS[1]) == ARGV[1] then
            return redis.call("DEL", KEYS[1])
        end
        return 0
    `
    c.redis.Eval(ctx, luaScript, []string{platformKey}, sessionID)

    // ==================== 8. 生产 session.expired 事件 — Kafka ====================
    expiredEvent := &kafka_session.SessionExpiredEvent{
        Header:     buildEventHeader("session", c.instanceID),
        SessionId:  sessionID,
        UserId:     event.UserId,
        Platform:   common.PlatformType(platformInt),
        DeviceId:   event.DeviceId,
        ExpireTime: now,
    }
    if err := c.kafka.Produce(ctx, "session.expired", event.UserId, expiredEvent); err != nil {
        log.Error("produce session.expired failed", "session_id", sessionID, "err", err)
    }

    log.Info("session invalidated from logout event",
        "session_id", sessionID, "user_id", event.UserId, "device_id", event.DeviceId)
    return nil
}
```

### Consumer: `auth.token.refreshed`

> 来源：Auth 服务。Token 刷新成功后，Session 延长对应会话的有效期。
> 说明：Auth 正常流程已通过 RPC `Session.RefreshLoginSession` 延长，此处为事件兜底。

```go
func (c *SessionConsumer) HandleAuthTokenRefreshed(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_auth.TokenRefreshedEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("unmarshal TokenRefreshedEvent failed", "err", err)
        return nil
    }

    // ==================== 2. 基本校验 ====================
    if event.UserId == "" || event.DeviceId == "" {
        log.Error("invalid TokenRefreshedEvent: missing required fields",
            "user_id", event.UserId, "device_id", event.DeviceId)
        return nil
    }

    // ==================== 3. 幂等检查 — Redis ====================
    dedupKey := fmt.Sprintf("session:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    now := time.Now().UnixMilli()
    newExpireAt := now + c.config.SessionLifetimeMs

    // ==================== 4. 查找活跃会话 — PgSQL ====================
    var sessionID string
    var platformInt int
    var oldExpireAt int64
    err := c.db.QueryRowContext(ctx,
        `SELECT session_id, platform, expire_at FROM login_sessions
         WHERE user_id = $1 AND device_id = $2 AND status = 1
         ORDER BY login_time DESC LIMIT 1`,
        event.UserId, event.DeviceId,
    ).Scan(&sessionID, &platformInt, &oldExpireAt)
    if err == sql.ErrNoRows {
        // 没有活跃会话，可能已过期/被踢
        log.Warn("no active session for token refresh",
            "user_id", event.UserId, "device_id", event.DeviceId)
        return nil
    }
    if err != nil {
        c.redis.Del(ctx, dedupKey)
        return fmt.Errorf("query login_session failed: %w", err)
    }

    // ==================== 5. 检查是否已被 RPC 刷新过 ====================
    // 如果 PgSQL 中的 expire_at 已经 >= 新的 expire_at，说明 RPC 已处理
    if oldExpireAt >= newExpireAt {
        log.Debug("session already refreshed via RPC",
            "session_id", sessionID, "old_expire", oldExpireAt, "new_expire", newExpireAt)
        return nil
    }

    // ==================== 6. 更新 PgSQL ====================
    _, err = c.db.ExecContext(ctx,
        `UPDATE login_sessions SET last_active = $1, expire_at = $2
         WHERE session_id = $3 AND status = 1`,
        now, newExpireAt, sessionID,
    )
    if err != nil {
        c.redis.Del(ctx, dedupKey)
        return fmt.Errorf("update session expire_at failed: %w", err)
    }

    // ==================== 7. 更新 Redis ====================
    sessionKey := fmt.Sprintf("session:login:%s", sessionID)
    newTTL := time.Duration(newExpireAt-now) * time.Millisecond

    pipe := c.redis.Pipeline()
    pipe.HSet(ctx, sessionKey, "last_active", now, "expire_at", newExpireAt)
    pipe.Expire(ctx, sessionKey, newTTL)
    // 刷新平台索引 TTL
    platformKey := fmt.Sprintf("session:device:%s:%d", event.UserId, platformInt)
    pipe.Expire(ctx, platformKey, newTTL)
    if _, err := pipe.Exec(ctx); err != nil {
        log.Warn("redis refresh from consumer failed", "session_id", sessionID, "err", err)
    }

    // ==================== 8. 生产 session.refreshed 事件 — Kafka ====================
    refreshEvent := &kafka_session.SessionRefreshedEvent{
        Header:      buildEventHeader("session", c.instanceID),
        SessionId:   sessionID,
        UserId:      event.UserId,
        Platform:    common.PlatformType(platformInt),
        DeviceId:    event.DeviceId,
        OldExpireAt: oldExpireAt,
        NewExpireAt: newExpireAt,
        RefreshTime: now,
    }
    if err := c.kafka.Produce(ctx, "session.refreshed", event.UserId, refreshEvent); err != nil {
        log.Error("produce session.refreshed failed", "session_id", sessionID, "err", err)
    }

    log.Info("session refreshed from token refresh event",
        "session_id", sessionID, "user_id", event.UserId, "new_expire", newExpireAt)
    return nil
}
```

### Consumer: `conn.disconnected`

> 来源：Connecte 长连接网关服务。当客户端连接断开时，Session 更新最后活跃时间。
> 注意：连接断开 ≠ 登出。会话仍然有效，只是设备暂时离线。
> 如果是异常断开（如网络切换），客户端可能会重连，此时会话继续使用。

```go
func (c *SessionConsumer) HandleConnDisconnected(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_connecte.ConnDisconnectedEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("unmarshal ConnDisconnectedEvent failed", "err", err)
        return nil
    }

    // ==================== 2. 基本校验 ====================
    if event.UserId == "" || event.DeviceId == "" {
        log.Error("invalid ConnDisconnectedEvent: missing required fields",
            "user_id", event.UserId, "device_id", event.DeviceId)
        return nil
    }

    // ==================== 3. 幂等检查 — Redis ====================
    dedupKey := fmt.Sprintf("session:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    now := time.Now().UnixMilli()

    // ==================== 4. 查找该设备的活跃会话 — PgSQL ====================
    var sessionID string
    var platformInt int
    var expireAt int64
    err := c.db.QueryRowContext(ctx,
        `SELECT session_id, platform, expire_at FROM login_sessions
         WHERE user_id = $1 AND device_id = $2 AND status = 1
         ORDER BY login_time DESC LIMIT 1`,
        event.UserId, event.DeviceId,
    ).Scan(&sessionID, &platformInt, &expireAt)
    if err == sql.ErrNoRows {
        log.Debug("no active session for disconnected device",
            "user_id", event.UserId, "device_id", event.DeviceId)
        return nil
    }
    if err != nil {
        c.redis.Del(ctx, dedupKey)
        return fmt.Errorf("query login_session failed: %w", err)
    }

    // ==================== 5. 判断断开原因 ====================
    reason := event.Reason // 如: "client_close", "heartbeat_timeout", "network_error", "server_shutdown"

    // 如果是服务端主动关闭（如部署升级），不更新会话状态
    if reason == "server_shutdown" {
        log.Info("conn disconnected due to server shutdown, session unchanged",
            "session_id", sessionID, "user_id", event.UserId)
        return nil
    }

    // ==================== 6. 更新最后活跃时间 — PgSQL ====================
    _, err = c.db.ExecContext(ctx,
        `UPDATE login_sessions SET last_active = $1
         WHERE session_id = $2 AND status = 1`,
        now, sessionID,
    )
    if err != nil {
        log.Error("update last_active failed", "session_id", sessionID, "err", err)
        // 非致命错误，不重试
    }

    // ==================== 7. 更新 Redis ====================
    sessionKey := fmt.Sprintf("session:login:%s", sessionID)
    c.redis.HSet(ctx, sessionKey, "last_active", now)

    // ==================== 8. 心跳超时断开 → 可选择性设置短过期 ====================
    // 如果是心跳超时断开，给一个宽限期让客户端重连
    if reason == "heartbeat_timeout" {
        // 设置一个 10 分钟的宽限期
        gracePeriod := int64(10 * 60 * 1000) // 10 分钟 ms
        if expireAt-now > gracePeriod {
            // 会话原有效期还很长，不需要提前过期
            log.Debug("heartbeat timeout, session still valid",
                "session_id", sessionID, "user_id", event.UserId,
                "remaining_ms", expireAt-now)
        }
        // 注意：此处不缩短会话有效期，等客户端重连或会话自然过期
        // 如果需要缩短，取消下面的注释：
        // newExpireAt := now + gracePeriod
        // c.db.ExecContext(ctx,
        //     `UPDATE login_sessions SET expire_at = $1 WHERE session_id = $2 AND status = 1 AND expire_at > $1`,
        //     newExpireAt, sessionID)
        // c.redis.HSet(ctx, sessionKey, "expire_at", newExpireAt)
        // c.redis.ExpireAt(ctx, sessionKey, time.UnixMilli(newExpireAt))
    }

    // ==================== 9. 如果是客户端主动断开且标记了"不保持会话" ====================
    if reason == "client_close" && event.GetMetadata()["keep_session"] == "false" {
        // 客户端明确不保持会话，直接销毁
        c.invalidateSessionFromConsumer(ctx, sessionID, event.UserId, "client_disconnect", now)
        log.Info("session invalidated on client explicit close",
            "session_id", sessionID, "user_id", event.UserId)
        return nil
    }

    log.Info("conn disconnected, session updated",
        "session_id", sessionID, "user_id", event.UserId,
        "reason", reason, "device_id", event.DeviceId)
    return nil
}
```

### Consumer: `config.changed`

> 来源：Config 配置服务。动态更新 Session 服务的运行参数，无需重启。

```go
func (c *SessionConsumer) HandleConfigChanged(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_config.ConfigChangedEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("unmarshal ConfigChangedEvent failed", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 ====================
    dedupKey := fmt.Sprintf("session:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    // ==================== 3. 按配置 Key 分发处理 ====================
    switch event.ConfigKey {
    case "session.lifetime_hours":
        // 会话有效期（小时）
        val, err := strconv.Atoi(event.NewValue)
        if err != nil || val <= 0 || val > 720 { // 最大 30 天
            log.Error("invalid session.lifetime_hours", "value", event.NewValue)
            return nil
        }
        c.config.SessionLifetimeMs = int64(val) * 3600 * 1000
        log.Info("config updated: session.lifetime_hours", "value", val)

    case "session.max_online_devices":
        // 单用户最大在线设备数
        val, err := strconv.Atoi(event.NewValue)
        if err != nil || val <= 0 || val > 10 {
            log.Error("invalid session.max_online_devices", "value", event.NewValue)
            return nil
        }
        c.config.MaxOnlineDevices = val
        log.Info("config updated: session.max_online_devices", "value", val)

    case "session.same_platform_kick_enabled":
        // 同平台互踢开关
        val := event.NewValue == "true"
        c.config.SamePlatformKickEnabled = val
        log.Info("config updated: session.same_platform_kick_enabled", "value", val)

    case "session.cleanup_interval_minutes":
        // 过期会话清理间隔（分钟）
        val, err := strconv.Atoi(event.NewValue)
        if err != nil || val < 1 || val > 1440 {
            log.Error("invalid session.cleanup_interval_minutes", "value", event.NewValue)
            return nil
        }
        c.config.CleanupIntervalMinutes = val
        // 通知清理协程重新调度
        select {
        case c.cleanupRescheduleCh <- struct{}{}:
        default:
        }
        log.Info("config updated: session.cleanup_interval_minutes", "value", val)

    case "session.heartbeat_grace_period_minutes":
        // 心跳超时断开后的宽限期（分钟）
        val, err := strconv.Atoi(event.NewValue)
        if err != nil || val < 1 || val > 60 {
            log.Error("invalid session.heartbeat_grace_period_minutes", "value", event.NewValue)
            return nil
        }
        c.config.HeartbeatGracePeriodMs = int64(val) * 60 * 1000
        log.Info("config updated: session.heartbeat_grace_period_minutes", "value", val)

    default:
        // 非本服务关心的配置，忽略
        log.Debug("config key not handled by session service", "key", event.ConfigKey)
    }

    return nil
}
```

---

## 内部辅助函数

```go
// invalidateSessionFromConsumer 消费者中复用的会话销毁逻辑
func (c *SessionConsumer) invalidateSessionFromConsumer(ctx context.Context, sessionID, userID, reason string, now int64) {
    // 1. 查询会话信息（用于 Redis 清理和 Kafka 事件）
    var platformInt int
    var deviceID string
    err := c.db.QueryRowContext(ctx,
        `SELECT platform, device_id FROM login_sessions WHERE session_id = $1`,
        sessionID,
    ).Scan(&platformInt, &deviceID)
    if err != nil {
        log.Error("query session for invalidate failed", "session_id", sessionID, "err", err)
        return
    }

    // 2. 确定 status 值
    newStatus := 3 // 默认踢出
    switch reason {
    case "logout", "client_disconnect":
        newStatus = 4
    case "expired":
        newStatus = 2
    }

    // 3. 延迟双删 — 先删缓存
    sessionKey := fmt.Sprintf("session:login:%s", sessionID)
    devicesKey := fmt.Sprintf("session:user_devices:%s", userID)
    platformKey := fmt.Sprintf("session:device:%s:%d", userID, platformInt)
    c.delayedDoubleDelete(ctx, sessionKey)

    // 4. 更新 PgSQL
    _, err = c.db.ExecContext(ctx,
        `UPDATE login_sessions SET status = $1, last_active = $2
         WHERE session_id = $3 AND status = 1`,
        newStatus, now, sessionID,
    )
    if err != nil {
        log.Error("update session status from consumer failed", "session_id", sessionID, "err", err)
    }

    // 5. 清除 Redis 索引
    pipe := c.redis.Pipeline()
    pipe.SRem(ctx, devicesKey, sessionID)
    pipe.Exec(ctx)

    // 原子删除平台索引
    luaScript := `
        if redis.call("GET", KEYS[1]) == ARGV[1] then
            return redis.call("DEL", KEYS[1])
        end
        return 0
    `
    c.redis.Eval(ctx, luaScript, []string{platformKey}, sessionID)

    // 6. 生产 Kafka 事件
    if reason == "same_platform_kick" || reason == "max_device_exceeded" || reason == "user_kick" {
        kickEvent := &kafka_session.SessionKickedEvent{
            Header:    buildEventHeader("session", c.instanceID),
            SessionId: sessionID,
            UserId:    userID,
            Platform:  common.PlatformType(platformInt),
            DeviceId:  deviceID,
            Reason:    reason,
            KickTime:  now,
        }
        c.kafka.Produce(ctx, "session.kicked", userID, kickEvent)
    } else {
        expiredEvent := &kafka_session.SessionExpiredEvent{
            Header:     buildEventHeader("session", c.instanceID),
            SessionId:  sessionID,
            UserId:     userID,
            Platform:   common.PlatformType(platformInt),
            DeviceId:   deviceID,
            ExpireTime: now,
        }
        c.kafka.Produce(ctx, "session.expired", userID, expiredEvent)
    }

    log.Info("session invalidated from consumer",
        "session_id", sessionID, "user_id", userID, "reason", reason)
}

// rebuildSessionCache 从 PgSQL 重建 Redis 缓存
func (c *SessionConsumer) rebuildSessionCache(ctx context.Context, sessionID string) {
    var session struct {
        SessionId  string
        UserID     string
        Platform   int
        DeviceID   string
        DeviceName string
        OsVersion  string
        AppVersion string
        IP         string
        LoginTime  int64
        LastActive int64
        ExpireAt   int64
        Status     int
    }
    err := c.db.QueryRowContext(ctx,
        `SELECT session_id, user_id, platform, device_id, device_name,
                os_version, app_version, ip, login_time, last_active, expire_at, status
         FROM login_sessions WHERE session_id = $1`,
        sessionID,
    ).Scan(&session.SessionId, &session.UserID, &session.Platform, &session.DeviceID,
        &session.DeviceName, &session.OsVersion, &session.AppVersion, &session.IP,
        &session.LoginTime, &session.LastActive, &session.ExpireAt, &session.Status)
    if err != nil {
        log.Error("rebuild session cache: query failed", "session_id", sessionID, "err", err)
        return
    }

    if session.Status != 1 || session.ExpireAt < time.Now().UnixMilli() {
        return // 会话已失效，不重建缓存
    }

    remaining := session.ExpireAt - time.Now().UnixMilli()
    if remaining <= 0 {
        return
    }
    ttl := time.Duration(remaining) * time.Millisecond

    sessionKey := fmt.Sprintf("session:login:%s", sessionID)
    devicesKey := fmt.Sprintf("session:user_devices:%s", session.UserID)
    platformKey := fmt.Sprintf("session:device:%s:%d", session.UserID, session.Platform)

    pipe := c.redis.Pipeline()
    pipe.HSet(ctx, sessionKey, map[string]interface{}{
        "session_id":  session.SessionId,
        "user_id":     session.UserID,
        "platform":    session.Platform,
        "device_id":   session.DeviceID,
        "device_name": session.DeviceName,
        "os_version":  session.OsVersion,
        "app_version": session.AppVersion,
        "ip":          session.IP,
        "login_time":  session.LoginTime,
        "last_active": session.LastActive,
        "expire_at":   session.ExpireAt,
        "status":      session.Status,
    })
    pipe.Expire(ctx, sessionKey, ttl)
    pipe.SAdd(ctx, devicesKey, sessionID)
    pipe.Set(ctx, platformKey, sessionID, ttl)
    pipe.Exec(ctx)

    log.Info("session cache rebuilt", "session_id", sessionID, "user_id", session.UserID)
}

// buildEventHeader 构建 Kafka 事件头
func buildEventHeader(source, instanceID string) *common.EventHeader {
    return &common.EventHeader{
        EventId:   uuid.New().String(),
        TraceId:   trace.SpanFromContext(context.Background()).SpanContext().TraceID().String(),
        Source:    source,
        SourceId:  instanceID,
        Timestamp: time.Now().UnixMilli(),
        Version:   1,
    }
}
```
