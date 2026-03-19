# Auth 认证服务 — Kafka 消费者/生产者实现伪代码

## 概述

Auth 服务通过 Kafka 接收用户封禁/解禁/注销事件，执行 Token 吊销和认证数据清理。

## 生产 Topic 列表

| Topic | 事件 | 生产时机 | 消费方 |
|-------|------|----------|--------|
| `auth.register.success` | RegisterSuccessEvent | 注册成功 | Session, User |
| `auth.login.success` | LoginSuccessEvent | 登录成功 | Session, Audit, Presence |
| `auth.login.fail` | LoginFailEvent | 登录失败 | Audit |
| `auth.logout` | LogoutEvent | 用户登出 | Session, Presence |
| `auth.token.refreshed` | TokenRefreshedEvent | Token 刷新 | Session |
| `auth.token.revoked` | TokenRevokedEvent | Token 吊销 | ApiGateway |
| `auth.password.changed` | PasswordChangedEvent | 密码修改 | Notification |
| `auth.stats` | AuthStatsEvent | 定时统计 | Audit |

## 消费 Topic 列表

| Topic | 来源 | 用途 |
|-------|------|------|
| `user.banned` | User | 封禁用户 → 吊销 Token + 强制下线 |
| `user.unbanned` | User | 解禁用户 → 恢复登录权限 |
| `user.deactivated` | User | 注销账户 → 清除认证数据 |
| `config.changed` | Config | 配置变更 |

---

## 辅助方法

```go
// delayedDoubleDelete 延迟双删策略：先删缓存 → 执行 DB 操作 → 延迟再删一次
func (c *AuthConsumer) delayedDoubleDelete(ctx context.Context, keys ...string) {
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

### Consumer: `user.banned`

> 来源：User 服务。当用户被封禁时，Auth 需要吊销其所有 Token 并强制下线。

```go
func (c *AuthConsumer) HandleUserBanned(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_user.UserBannedEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("unmarshal UserBannedEvent failed", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 ====================
    dedupKey := fmt.Sprintf("auth:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    // ==================== 3. 吊销所有 refresh_token ====================
    // 扫描并删除该用户的所有 refresh_token（使用 SCAN 避免阻塞）
    cursor := uint64(0)
    for {
        keys, nextCursor, err := c.redis.Scan(ctx, cursor, fmt.Sprintf("auth:refresh_token:*"), 100).Result()
        if err != nil {
            break
        }
        for _, key := range keys {
            val, _ := c.redis.Get(ctx, key).Result()
            if strings.HasPrefix(val, event.UserId+":") {
                c.redis.Del(ctx, key)
            }
        }
        cursor = nextCursor
        if cursor == 0 {
            break
        }
    }

    // ==================== 4. 标记用户为封禁状态（本地缓存） ====================
    c.redis.Set(ctx, fmt.Sprintf("auth:user_banned:%s", event.UserId), "1",
        time.Duration(event.BanDuration)*time.Second)

    // ==================== 5. 生产 Token 吊销事件 ====================
    revokeEvent := &kafka_auth.TokenRevokedEvent{
        Header:     buildEventHeader("auth", c.instanceID),
        UserId:     event.UserId,
        Reason:     "user_banned",
        RevokeTime: time.Now().UnixMilli(),
    }
    c.kafka.Produce(ctx, "auth.token.revoked", event.UserId, revokeEvent)

    log.Info("user banned, all tokens revoked", "user_id", event.UserId)
    return nil
}
```

### Consumer: `user.unbanned`

```go
func (c *AuthConsumer) HandleUserUnbanned(ctx context.Context, msg *kafka.Message) error {
    var event kafka_user.UserUnbannedEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        return nil
    }

    dedupKey := fmt.Sprintf("auth:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    // 清除封禁标记
    c.redis.Del(ctx, fmt.Sprintf("auth:user_banned:%s", event.UserId))

    log.Info("user unbanned", "user_id", event.UserId)
    return nil
}
```

### Consumer: `user.deactivated`

> 来源：User 服务。用户注销账户，清除所有认证相关数据。

```go
func (c *AuthConsumer) HandleUserDeactivated(ctx context.Context, msg *kafka.Message) error {
    var event kafka_user.UserDeactivatedEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        return nil
    }

    dedupKey := fmt.Sprintf("auth:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    // ==================== 1. 删除认证凭证 — PgSQL ====================
    _, err := c.db.ExecContext(ctx,
        `DELETE FROM auth_credentials WHERE user_id = $1`, event.UserId)
    if err != nil {
        return fmt.Errorf("delete auth_credentials failed: %w", err)
    }

    // ==================== 2. 删除 OAuth 绑定 — PgSQL ====================
    _, err = c.db.ExecContext(ctx,
        `DELETE FROM oauth_accounts WHERE user_id = $1`, event.UserId)
    if err != nil {
        return fmt.Errorf("delete oauth_accounts failed: %w", err)
    }

    // ==================== 3. 清除 Redis 相关 Key ====================
    c.redis.Del(ctx, fmt.Sprintf("auth:user_banned:%s", event.UserId))

    log.Info("user deactivated, auth data cleaned", "user_id", event.UserId)
    return nil
}
```

### Consumer: `config.changed`

```go
func (c *AuthConsumer) HandleConfigChanged(ctx context.Context, msg *kafka.Message) error {
    var event kafka_config.ConfigChangedEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        return nil
    }

    switch event.ConfigKey {
    case "auth.token_expire_hours":
        val, _ := strconv.Atoi(event.NewValue)
        c.config.Store("token_expire_hours", val)
    case "auth.max_login_fail_count":
        val, _ := strconv.Atoi(event.NewValue)
        c.config.Store("max_login_fail_count", val)
    case "auth.jwt_secret":
        c.config.Store("jwt_secret", event.NewValue)
        // 注意：更换 JWT Secret 需要平滑过渡
    }
    return nil
}
```
