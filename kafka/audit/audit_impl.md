# Audit 审计/内容审核服务 — Kafka 消费者/生产者实现伪代码

## 概述

Audit 服务是整个 IM 系统中**最大的 Kafka 消费者**，因为审计日志需要记录系统中几乎所有关键操作。  
同时 Audit 也是核心的内容审核引擎，消费消息/媒体审核请求并产生审核结果事件驱动下游处置。

**核心设计原则：**
- **全量审计**：所有关键操作（登录、消息、群操作、好友操作、配置变更等）均需记录审计日志
- **高吞吐写入**：审计日志写入采用批量 INSERT，避免逐条写入造成 DB 压力
- **幂等消费**：所有 consumer 通过 `audit:kafka:dedup:{event_id}` 做幂等去重
- **内容审核流水线**：msg.moderation.request → 关键词过滤 → AI 审核 → 人工复审 → 结果事件
- **采样审计**：msg.stored.single/group 支持采样率控制，避免全量消息审计压力过大
- **配置热更新**：config.changed 事件触发审核策略实时更新

## 生产 Topic 列表

| Topic | 事件 | 生产时机 | Key | 消费方 |
|-------|------|----------|-----|--------|
| `audit.moderation.result` | ModerationResultEvent | 内容审核完成 | content_id | Message（更新消息状态）、Media（封禁媒体） |
| `audit.content.violation` | ContentViolationEvent | 内容违规确认 | user_id | User（封禁/警告）、Group（处理违规群内容）、Relation |
| `audit.media.violation` | MediaViolationEvent | 媒体违规确认 | user_id | Media（封禁/删除媒体文件） |
| `audit.user.violation` | UserViolationEvent | 用户违规升级 | user_id | User（封禁/禁言/警告） |
| `audit.operation.log` | OperationLogEvent | 管理员操作审计 | operator_id | 存档 |
| `audit.risk.alert` | RiskAlertEvent | 风控告警 | user_id | 告警系统 |
| `audit.stats` | AuditStatsEvent | 定时统计 | server_id | 监控 |

## 消费 Topic 列表

| Topic | 来源 | 用途 |
|-------|------|------|
| `auth.login.success` | Auth | 记录登录成功审计日志 |
| `auth.login.fail` | Auth | 记录登录失败审计日志 + 风控分析 |
| `auth.logout` | Auth | 记录登出审计日志 |
| `auth.password.changed` | Auth | 记录密码修改安全事件 |
| `user.registered` | User | 记录用户注册审计日志 |
| `user.profile.updated` | User | 记录资料更新 + 触发昵称/头像审核 |
| `user.deactivated` | User | 记录用户注销审计日志 |
| `msg.moderation.request` | Message | **核心**：触发消息内容审核流水线 |
| `msg.stored.single` | Message | 采样审计：随机抽检单聊消息合规性 |
| `msg.stored.group` | Message | 采样审计：随机抽检群聊消息合规性 |
| `group.created` | Group | 记录群创建审计日志 + 审核群名 |
| `group.dissolved` | Group | 记录群解散审计日志 |
| `config.changed` | Config | 记录配置变更审计日志 + 热更新审核策略 |
| `media.uploaded` | Media | 触发媒体内容审核（图片/视频/语音） |
| `apigateway.auth.fail` | ApiGateway | 风控：认证失败异常检测 |
| `apigateway.rate.limited` | ApiGateway | 风控：限流触发异常检测 |
| `apigateway.error` | ApiGateway | 故障分析：网关错误归档 |
| `apigateway.request.log` | ApiGateway | 审计归档：请求日志 |
| `offline.queue.overflow` | OfflineQueue | 风控告警：离线队列溢出 |
| `push.result` | Push | 投递审计：推送结果统计（成功/失败） |
| `push.stats` | Push | 运营统计：推送定时统计数据 |
| `push.offline.result` | Push | 投递审计：离线推送结果 |
| `msg.forwarded` | Message | 审计记录：消息转发操作归档 |

## Redis Key 设计（消费者专用）

| Key 格式 | 类型 | 值 | TTL | 说明 |
|----------|------|-----|-----|------|
| `audit:kafka:dedup:{event_id}` | STRING | "1" | 24h | Kafka 消费幂等去重 |
| `audit:moderation:{request_id}` | HASH | status/result/reason/confidence | 24h | 审核结果缓存 |
| `audit:rate:{user_id}:{action}` | STRING | count | 按 action 不同 | 操作频率统计 |
| `audit:user_violation_count:{user_id}` | STRING | count | 30d | 用户累计违规次数 |
| `audit:moderation_queue_size` | STRING | count | 无 TTL | 审核队列大小监控 |
| `audit:login_fail_track:{ip}` | STRING | count | 1h | IP 维度登录失败追踪（风控） |
| `audit:rate_limit_track:{user_id}` | STRING | count | 1h | 用户触发限流次数追踪 |
| `audit:sample_rate:{topic}` | STRING | 采样率 (0.0~1.0) | 无 TTL | 采样审计比例配置 |
| `audit:keyword_filter_version` | STRING | 版本号 | 无 TTL | 关键词过滤规则版本 |

## PgSQL 表结构（消费者写入）

```sql
-- 审计日志表（同 RPC 文档，此处为消费者批量写入使用）
-- audit_logs (按月分区)
-- moderation_records

-- 批量插入审计日志 SQL 模板
-- INSERT INTO audit_logs (log_id, user_id, action, resource_type, resource_id, detail, ip, device_info, result, created_at)
-- VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW())
```

---

## 消费者实现

### Consumer: `auth.login.success`

> 来源：Auth 服务。用户登录成功，记录登录审计日志。  
> 审计日志包含：用户 ID、登录类型、平台、设备、IP、时间。

```go
func (c *AuditConsumer) HandleAuthLoginSuccess(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_auth.LoginSuccessEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("unmarshal LoginSuccessEvent failed", "err", err)
        return nil // 反序列化失败不重试
    }

    // ==================== 2. 幂等检查 — Redis ====================
    dedupKey := fmt.Sprintf("audit:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        log.Debug("duplicate auth.login.success event", "event_id", event.Header.EventId)
        return nil
    }

    // ==================== 3. 构建审计日志详情 ====================
    detail, _ := json.Marshal(map[string]interface{}{
        "login_type": event.LoginType.String(),
        "platform":   event.Platform.String(),
        "device_id":  event.DeviceId,
        "device_name": event.DeviceName,
        "session_id": event.SessionId,
    })

    // ==================== 4. 写入审计日志 — PgSQL ====================
    logID := c.snowflake.Generate().String()
    _, err := c.db.ExecContext(ctx,
        `INSERT INTO audit_logs (log_id, user_id, action, resource_type, resource_id, detail, ip, device_info, result, created_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, to_timestamp($10 / 1000.0))`,
        logID,
        event.UserId,
        "LOGIN",              // action
        "session",            // resource_type
        event.SessionId,      // resource_id
        string(detail),       // detail JSON
        event.Ip,             // ip
        event.UserAgent,      // device_info
        "success",            // result
        event.LoginTime,      // created_at（使用事件时间）
    )
    if err != nil {
        log.Error("insert login audit_log failed",
            "log_id", logID, "user_id", event.UserId, "err", err)
        return fmt.Errorf("insert login audit_log failed: %w", err)
    }

    log.Info("login success audit log recorded",
        "log_id", logID, "user_id", event.UserId,
        "platform", event.Platform.String(), "ip", event.Ip)
    return nil
}
```

---

### Consumer: `auth.login.fail`

> 来源：Auth 服务。登录失败事件，记录审计日志 + 风控分析（IP 维度暴力破解检测）。

```go
func (c *AuditConsumer) HandleAuthLoginFail(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_auth.LoginFailEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("unmarshal LoginFailEvent failed", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 ====================
    dedupKey := fmt.Sprintf("audit:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    // ==================== 3. 记录审计日志 — PgSQL ====================
    detail, _ := json.Marshal(map[string]interface{}{
        "reason":    event.FailReason,
        "platform":  event.Platform.String(),
        "device_id": event.DeviceId,
        "username":  event.Username,
    })

    logID := c.snowflake.Generate().String()
    _, err := c.db.ExecContext(ctx,
        `INSERT INTO audit_logs (log_id, user_id, action, resource_type, resource_id, detail, ip, device_info, result, created_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, to_timestamp($10 / 1000.0))`,
        logID,
        event.UserId,        // 可能为空（用户名不存在时）
        "LOGIN",
        "auth",
        "",                  // resource_id
        string(detail),
        event.Ip,
        event.UserAgent,
        "fail",              // result = fail
        event.FailTime,
    )
    if err != nil {
        log.Error("insert login fail audit_log failed", "err", err)
        return fmt.Errorf("insert login fail audit_log: %w", err)
    }

    // ==================== 4. 风控分析：IP 维度暴力破解检测 — Redis ====================
    if event.Ip != "" {
        ipTrackKey := fmt.Sprintf("audit:login_fail_track:%s", event.Ip)
        failCount, _ := c.redis.Incr(ctx, ipTrackKey).Result()
        if failCount == 1 {
            c.redis.Expire(ctx, ipTrackKey, 1*time.Hour)
        }

        // 同一 IP 1 小时内失败超过 20 次 → 触发风控告警
        if failCount >= 20 {
            alertEvent := &kafka_audit.RiskAlertEvent{
                Header:      buildEventHeader("audit", c.instanceID),
                AlertId:     c.snowflake.Generate().String(),
                UserId:      event.UserId,
                RiskType:    "brute_force",
                RiskLevel:   "high",
                Description: fmt.Sprintf("IP %s 在 1 小时内登录失败 %d 次，疑似暴力破解", event.Ip, failCount),
                Evidence: map[string]string{
                    "ip":           event.Ip,
                    "fail_count":   fmt.Sprintf("%d", failCount),
                    "last_username": event.Username,
                },
                Action:      "restrict",
                AutoHandled: false,
                AlertTime:   time.Now().UnixMilli(),
            }
            c.kafka.Produce(ctx, "audit.risk.alert", event.Ip, alertEvent)

            log.Warn("brute force risk alert triggered",
                "ip", event.Ip, "fail_count", failCount)
        }
    }

    log.Info("login fail audit log recorded",
        "log_id", logID, "user_id", event.UserId, "ip", event.Ip,
        "reason", event.FailReason)
    return nil
}
```

---

### Consumer: `auth.logout`

> 来源：Auth 服务。用户登出事件，记录审计日志。

```go
func (c *AuditConsumer) HandleAuthLogout(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_auth.LogoutEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("unmarshal LogoutEvent failed", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 ====================
    dedupKey := fmt.Sprintf("audit:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    // ==================== 3. 记录审计日志 — PgSQL ====================
    detail, _ := json.Marshal(map[string]interface{}{
        "platform":  event.Platform.String(),
        "device_id": event.DeviceId,
    })

    logID := c.snowflake.Generate().String()
    _, err := c.db.ExecContext(ctx,
        `INSERT INTO audit_logs (log_id, user_id, action, resource_type, resource_id, detail, ip, device_info, result, created_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, to_timestamp($10 / 1000.0))`,
        logID,
        event.UserId,
        "LOGOUT",
        "session",
        event.DeviceId,
        string(detail),
        "",          // ip（登出事件可能无 IP）
        "",
        "success",
        event.LogoutTime,
    )
    if err != nil {
        log.Error("insert logout audit_log failed", "err", err)
        return fmt.Errorf("insert logout audit_log: %w", err)
    }

    log.Info("logout audit log recorded", "log_id", logID, "user_id", event.UserId)
    return nil
}
```

---

### Consumer: `auth.password.changed`

> 来源：Auth 服务。密码修改事件，记录安全审计日志。属高安全级别事件。

```go
func (c *AuditConsumer) HandlePasswordChanged(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_auth.PasswordChangedEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("unmarshal PasswordChangedEvent failed", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 ====================
    dedupKey := fmt.Sprintf("audit:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    // ==================== 3. 记录安全审计日志 — PgSQL ====================
    detail, _ := json.Marshal(map[string]interface{}{
        "change_type": "password_changed",
        "ip":          event.Ip,
    })

    logID := c.snowflake.Generate().String()
    _, err := c.db.ExecContext(ctx,
        `INSERT INTO audit_logs (log_id, user_id, action, resource_type, resource_id, detail, ip, device_info, result, created_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, to_timestamp($10 / 1000.0))`,
        logID,
        event.UserId,
        "PASSWORD_CHANGED",
        "auth",
        event.UserId,
        string(detail),
        event.Ip,
        "",
        "success",
        event.ChangeTime,
    )
    if err != nil {
        log.Error("insert password_changed audit_log failed", "err", err)
        return fmt.Errorf("insert password_changed audit_log: %w", err)
    }

    log.Info("password changed audit log recorded",
        "log_id", logID, "user_id", event.UserId, "ip", event.Ip)
    return nil
}
```

---

### Consumer: `user.registered`

> 来源：User 服务。用户注册成功事件，记录审计日志。

```go
func (c *AuditConsumer) HandleUserRegistered(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_user.UserRegisteredEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("unmarshal UserRegisteredEvent failed", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 ====================
    dedupKey := fmt.Sprintf("audit:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    // ==================== 3. 记录审计日志 — PgSQL ====================
    detail, _ := json.Marshal(map[string]interface{}{
        "username":  event.Username,
        "nickname":  event.Nickname,
        "platform":  event.Platform.String(),
        "device_id": event.DeviceId,
    })

    logID := c.snowflake.Generate().String()
    _, err := c.db.ExecContext(ctx,
        `INSERT INTO audit_logs (log_id, user_id, action, resource_type, resource_id, detail, ip, device_info, result, created_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, to_timestamp($10 / 1000.0))`,
        logID,
        event.UserId,
        "USER_REGISTERED",
        "user",
        event.UserId,
        string(detail),
        event.Ip,
        "",
        "success",
        event.RegisterTime,
    )
    if err != nil {
        log.Error("insert user_registered audit_log failed", "err", err)
        return fmt.Errorf("insert user_registered audit_log: %w", err)
    }

    // ==================== 4. 触发昵称审核（如果昵称非空） ====================
    if event.Nickname != "" {
        requestID := c.snowflake.Generate().String()
        // 写入审核记录
        c.db.ExecContext(ctx,
            `INSERT INTO moderation_records (request_id, content_type, content, media_url, user_id, source, status, created_at)
             VALUES ($1, $2, $3, '', $4, $5, 1, NOW())`,
            requestID, "nickname", event.Nickname, event.UserId, "register",
        )
        // 异步调用审核流水线
        go c.moderateContent(context.Background(), requestID, "nickname", event.Nickname, "", event.UserId, "register")
    }

    log.Info("user registered audit log recorded",
        "log_id", logID, "user_id", event.UserId, "username", event.Username)
    return nil
}
```

---

### Consumer: `user.profile.updated`

> 来源：User 服务。用户更新个人资料，记录审计日志 + 触发昵称/头像内容审核。

```go
func (c *AuditConsumer) HandleUserProfileUpdated(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_user.UserProfileUpdatedEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("unmarshal UserProfileUpdatedEvent failed", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 ====================
    dedupKey := fmt.Sprintf("audit:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    // ==================== 3. 记录审计日志 — PgSQL ====================
    // 注：UserProfileUpdatedEvent proto 定义为 updated_fields map<string,string>
    //     不存在 ChangedFields / OldValues / NewValues 字段
    detail, _ := json.Marshal(map[string]interface{}{
        "updated_fields": event.UpdatedFields,
    })

    logID := c.snowflake.Generate().String()
    _, err := c.db.ExecContext(ctx,
        `INSERT INTO audit_logs (log_id, user_id, action, resource_type, resource_id, detail, ip, device_info, result, created_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW())`,
        logID,
        event.UserId,
        "UPDATE_PROFILE",
        "user",
        event.UserId,
        string(detail),
        "",
        "",
        "success",
    )
    if err != nil {
        log.Error("insert profile_updated audit_log failed", "err", err)
        return fmt.Errorf("insert profile_updated audit_log: %w", err)
    }

    // ==================== 4. 触发昵称审核（如果昵称变更） ====================
    if newNickname, ok := event.UpdatedFields["nickname"]; ok && newNickname != "" {
        requestID := c.snowflake.Generate().String()
        c.db.ExecContext(ctx,
            `INSERT INTO moderation_records (request_id, content_type, content, media_url, user_id, source, status, created_at)
             VALUES ($1, $2, $3, '', $4, $5, 1, NOW())`,
            requestID, "nickname", newNickname, event.UserId, "profile_update",
        )
        go c.moderateContent(context.Background(), requestID, "nickname", newNickname, "", event.UserId, "profile_update")
    }

    // ==================== 5. 触发头像审核（如果头像变更） ====================
    if newAvatar, ok := event.UpdatedFields["avatar_url"]; ok && newAvatar != "" {
        requestID := c.snowflake.Generate().String()
        c.db.ExecContext(ctx,
            `INSERT INTO moderation_records (request_id, content_type, content, media_url, user_id, source, status, created_at)
             VALUES ($1, $2, '', $3, $4, $5, 1, NOW())`,
            requestID, "avatar", newAvatar, event.UserId, "profile_update",
        )
        go c.moderateContent(context.Background(), requestID, "avatar", "", newAvatar, event.UserId, "profile_update")
    }

    log.Info("profile updated audit log recorded",
        "log_id", logID, "user_id", event.UserId,
        "updated_fields", event.UpdatedFields)
    return nil
}
```

---

### Consumer: `user.deactivated`

> 来源：User 服务。用户注销账户，记录审计日志。

```go
func (c *AuditConsumer) HandleUserDeactivated(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_user.UserDeactivatedEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("unmarshal UserDeactivatedEvent failed", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 ====================
    dedupKey := fmt.Sprintf("audit:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    // ==================== 3. 记录审计日志 — PgSQL ====================
    detail, _ := json.Marshal(map[string]interface{}{
        "reason": event.Reason,
    })

    logID := c.snowflake.Generate().String()
    _, err := c.db.ExecContext(ctx,
        `INSERT INTO audit_logs (log_id, user_id, action, resource_type, resource_id, detail, ip, device_info, result, created_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW())`,
        logID,
        event.UserId,
        "USER_DEACTIVATED",
        "user",
        event.UserId,
        string(detail),
        "",
        "",
        "success",
    )
    if err != nil {
        log.Error("insert user_deactivated audit_log failed", "err", err)
        return fmt.Errorf("insert user_deactivated audit_log: %w", err)
    }

    // ==================== 4. 清理该用户的违规计数 — Redis ====================
    c.redis.Del(ctx, fmt.Sprintf("audit:user_violation_count:%s", event.UserId))

    log.Info("user deactivated audit log recorded",
        "log_id", logID, "user_id", event.UserId)
    return nil
}
```

---

### Consumer: `msg.moderation.request`

> 来源：Message 服务。消息发送后触发异步内容审核。这是内容审核的**核心消费者**。  
> 流水线：关键词过滤（同步毫秒级）→ AI 审核 API（异步秒级）→ 人工复审队列。  
> 审核结果通过 Kafka `audit.moderation.result` 事件通知 Message 服务更新消息状态。

```go
func (c *AuditConsumer) HandleMsgModerationRequest(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_msg.MsgModerationRequestEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("unmarshal MsgModerationRequestEvent failed", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 ====================
    dedupKey := fmt.Sprintf("audit:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        log.Debug("duplicate msg.moderation.request", "event_id", event.Header.EventId)
        return nil
    }

    // ==================== 3. 参数校验 ====================
    if event.MsgId == "" {
        log.Error("msg.moderation.request: msg_id is empty", "event_id", event.Header.EventId)
        return nil
    }
    if event.SenderId == "" {
        log.Error("msg.moderation.request: sender_id is empty", "msg_id", event.MsgId)
        return nil
    }

    // ==================== 4. 获取用户上下文（辅助审核决策） ====================
    violationKey := fmt.Sprintf("audit:user_violation_count:%s", event.SenderId)
    userViolationCount, _ := c.redis.Get(ctx, violationKey).Int64()

    // 高违规用户需要更严格审核
    isHighRiskUser := userViolationCount >= 3

    // ==================== 5. 生成审核请求 ID ====================
    requestID := c.snowflake.Generate().String()

    // 更新审核队列大小
    c.redis.Incr(ctx, "audit:moderation_queue_size")

    // ==================== 6. 判定内容类型并审核 ====================
    startTime := time.Now()
    var contentType string
    var textContent string
    var mediaURL string

    switch event.MsgType {
    case common.MESSAGE_TYPE_TEXT:
        contentType = "text"
        textContent = event.Content
    case common.MESSAGE_TYPE_IMAGE:
        contentType = "image"
        mediaURL = event.MediaUrl
    case common.MESSAGE_TYPE_VIDEO:
        contentType = "video"
        mediaURL = event.MediaUrl
    case common.MESSAGE_TYPE_VOICE:
        contentType = "voice"
        mediaURL = event.MediaUrl
    default:
        // 不支持审核的消息类型，直接放行
        c.redis.Decr(ctx, "audit:moderation_queue_size")
        log.Debug("unsupported msg type for moderation, auto pass",
            "msg_id", event.MsgId, "msg_type", event.MsgType)

        // 投递 PASS 结果
        passEvent := &kafka_audit.ModerationResultEvent{
            Header:       buildEventHeader("audit", c.instanceID),
            ModerationId: requestID,
            ContentId:    event.MsgId,
            ContentType:  kafka_audit.CONTENT_TYPE_TEXT,
            Result:       common.MODERATION_RESULT_PASS,
            Confidence:   1.0,
            IsAuto:       true,
            AuditTime:    time.Now().UnixMilli(),
        }
        c.kafka.Produce(ctx, "audit.moderation.result", event.MsgId, passEvent)
        return nil
    }

    // ==================== 7. 写入审核记录 — PgSQL ====================
    _, err := c.db.ExecContext(ctx,
        `INSERT INTO moderation_records (request_id, content_type, content, media_url, user_id, source, status, created_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())`,
        requestID,
        contentType,
        textContent,
        mediaURL,
        event.SenderId,
        "chat",    // source = 聊天消息
        1,         // status = PROCESSING
    )
    if err != nil {
        log.Error("insert moderation_record failed", "request_id", requestID, "err", err)
        c.redis.Decr(ctx, "audit:moderation_queue_size")
        return fmt.Errorf("insert moderation_record: %w", err)
    }

    // ==================== 8. 第一级：关键词过滤（同步） ====================
    if contentType == "text" && textContent != "" {
        matched, keyword, category := c.keywordFilter.Match(textContent)
        if matched {
            latencyMs := time.Since(startTime).Milliseconds()
            c.redis.Decr(ctx, "audit:moderation_queue_size")

            reason := fmt.Sprintf("命中关键词: %s", keyword)
            log.Info("keyword filter hit in message",
                "request_id", requestID, "msg_id", event.MsgId,
                "keyword", keyword, "category", category)

            // 更新审核记录 — PgSQL
            c.db.ExecContext(ctx,
                `UPDATE moderation_records SET status = 3, result = $1, reason = $2, confidence = 1.0 WHERE request_id = $3`,
                category, reason, requestID,
            )

            // 写入 Redis 缓存
            resultKey := fmt.Sprintf("audit:moderation:%s", requestID)
            c.redis.HSet(ctx, resultKey, map[string]interface{}{
                "request_id": requestID,
                "status":     3,
                "result":     category,
                "reason":     reason,
                "confidence": 1.0,
            })
            c.redis.Expire(ctx, resultKey, 24*time.Hour)

            // 累计违规次数
            c.redis.Incr(ctx, violationKey)
            c.redis.Expire(ctx, violationKey, 30*24*time.Hour)
            newCount := userViolationCount + 1

            // 投递审核结果事件 — Kafka audit.moderation.result
            rejectEvent := &kafka_audit.ModerationResultEvent{
                Header:        buildEventHeader("audit", c.instanceID),
                ModerationId:  requestID,
                ContentId:     event.MsgId,
                ContentType:   contentTypeToProto(contentType),
                Result:        common.MODERATION_RESULT_REJECT,
                ViolationType: violationTypeFromCategory(category),
                Confidence:    1.0,
                Detail:        reason,
                IsAuto:        true,
                AuditTime:     time.Now().UnixMilli(),
                LatencyMs:     latencyMs,
            }
            c.kafka.Produce(ctx, "audit.moderation.result", event.MsgId, rejectEvent)

            // 投递内容违规事件
            violationEvent := &kafka_audit.ContentViolationEvent{
                Header:         buildEventHeader("audit", c.instanceID),
                ViolationId:    c.snowflake.Generate().String(),
                UserId:         event.SenderId,
                ContentId:      event.MsgId,
                ContentType:    contentTypeToProto(contentType),
                ViolationType:  violationTypeFromCategory(category),
                Action:         "delete",
                Detail:         reason,
                ViolationCount: int32(newCount),
                HandleTime:     time.Now().UnixMilli(),
            }
            c.kafka.Produce(ctx, "audit.content.violation", event.SenderId, violationEvent)

            // 违规升级检查
            if newCount >= 3 {
                c.escalateUserViolation(ctx, event.SenderId, category, int(newCount))
            }

            return nil
        }
    }

    // ==================== 9. 第二级：AI 审核（异步） ====================
    go func() {
        asyncCtx := context.Background()
        aiStartTime := time.Now()

        var aiResult *AIModerateResult
        var aiErr error

        moderationCtx := &ModerationContext{
            UserID:         event.SenderId,
            Scene:          "chat",
            ViolationCount: int(userViolationCount),
        }

        switch contentType {
        case "text":
            aiResult, aiErr = c.aiModerator.ModerateText(asyncCtx, textContent, moderationCtx)
        case "image":
            aiResult, aiErr = c.aiModerator.ModerateImage(asyncCtx, mediaURL, moderationCtx)
        case "video":
            aiResult, aiErr = c.aiModerator.ModerateVideo(asyncCtx, mediaURL, moderationCtx)
        case "voice":
            aiResult, aiErr = c.aiModerator.ModerateVoice(asyncCtx, mediaURL, moderationCtx)
        }

        latencyMs := time.Since(aiStartTime).Milliseconds()
        c.redis.Decr(asyncCtx, "audit:moderation_queue_size")

        if aiErr != nil {
            // AI 审核失败 → 推入人工复审队列
            log.Error("AI moderation failed for message",
                "request_id", requestID, "msg_id", event.MsgId, "err", aiErr)

            c.db.ExecContext(asyncCtx,
                `UPDATE moderation_records SET status = 4, reason = $1 WHERE request_id = $2`,
                fmt.Sprintf("AI 审核失败: %v", aiErr), requestID,
            )

            resultKey := fmt.Sprintf("audit:moderation:%s", requestID)
            c.redis.HSet(asyncCtx, resultKey, map[string]interface{}{
                "status": 4,
                "result": "",
                "reason": fmt.Sprintf("AI 审核失败: %v", aiErr),
            })
            c.redis.Expire(asyncCtx, resultKey, 24*time.Hour)

            // 高风险用户 AI 失败时直接拒绝（安全策略）
            if isHighRiskUser {
                c.db.ExecContext(asyncCtx,
                    `UPDATE moderation_records SET status = 3, result = 'OTHER', reason = $1 WHERE request_id = $2`,
                    "高风险用户+AI审核失败，安全策略拒绝", requestID,
                )

                rejectEvent := &kafka_audit.ModerationResultEvent{
                    Header:       buildEventHeader("audit", c.instanceID),
                    ModerationId: requestID,
                    ContentId:    event.MsgId,
                    ContentType:  contentTypeToProto(contentType),
                    Result:       common.MODERATION_RESULT_REJECT,
                    Detail:       "高风险用户+AI审核失败，安全策略拒绝",
                    IsAuto:       true,
                    AuditTime:    time.Now().UnixMilli(),
                    LatencyMs:    latencyMs,
                }
                c.kafka.Produce(asyncCtx, "audit.moderation.result", event.MsgId, rejectEvent)
            }
            return
        }

        // ---- AI 审核成功 ----
        var finalStatus int
        var finalResult string
        var moderationProtoResult common.ModerationResult

        // 高风险用户降低通过阈值
        passThreshold := 0.9
        rejectThreshold := 0.8
        if isHighRiskUser {
            passThreshold = 0.95
            rejectThreshold = 0.6
        }

        if aiResult.Confidence >= passThreshold && aiResult.Result == "PASS" {
            finalStatus = 2 // PASS
            finalResult = "PASS"
            moderationProtoResult = common.MODERATION_RESULT_PASS
        } else if aiResult.Confidence >= rejectThreshold && aiResult.Result != "PASS" {
            finalStatus = 3 // REJECT
            finalResult = aiResult.Result
            moderationProtoResult = common.MODERATION_RESULT_REJECT
        } else {
            finalStatus = 4 // REVIEW（人工复审）
            finalResult = aiResult.Result
            moderationProtoResult = common.MODERATION_RESULT_REVIEW
        }

        // 更新 PgSQL
        c.db.ExecContext(asyncCtx,
            `UPDATE moderation_records
             SET status = $1, result = $2, reason = $3, confidence = $4
             WHERE request_id = $5`,
            finalStatus, finalResult, aiResult.Reason, aiResult.Confidence, requestID,
        )

        // 更新 Redis
        resultKey := fmt.Sprintf("audit:moderation:%s", requestID)
        c.redis.HSet(asyncCtx, resultKey, map[string]interface{}{
            "status":     finalStatus,
            "result":     finalResult,
            "reason":     aiResult.Reason,
            "confidence": aiResult.Confidence,
        })
        c.redis.Expire(asyncCtx, resultKey, 24*time.Hour)

        // 投递审核结果事件
        resultEvent := &kafka_audit.ModerationResultEvent{
            Header:       buildEventHeader("audit", c.instanceID),
            ModerationId: requestID,
            ContentId:    event.MsgId,
            ContentType:  contentTypeToProto(contentType),
            Result:       moderationProtoResult,
            Confidence:   aiResult.Confidence,
            Detail:       aiResult.Reason,
            IsAuto:       true,
            AuditTime:    time.Now().UnixMilli(),
            LatencyMs:    latencyMs,
        }
        if finalResult != "PASS" {
            resultEvent.ViolationType = violationTypeFromCategory(finalResult)
        }
        c.kafka.Produce(asyncCtx, "audit.moderation.result", event.MsgId, resultEvent)

        // 拒绝时投递违规事件
        if finalStatus == 3 {
            c.redis.Incr(asyncCtx, violationKey)
            c.redis.Expire(asyncCtx, violationKey, 30*24*time.Hour)
            newCount, _ := c.redis.Get(asyncCtx, violationKey).Int64()

            violationEvent := &kafka_audit.ContentViolationEvent{
                Header:         buildEventHeader("audit", c.instanceID),
                ViolationId:    c.snowflake.Generate().String(),
                UserId:         event.SenderId,
                ContentId:      event.MsgId,
                ContentType:    contentTypeToProto(contentType),
                ViolationType:  violationTypeFromCategory(finalResult),
                Action:         "delete",
                Detail:         aiResult.Reason,
                ViolationCount: int32(newCount),
                HandleTime:     time.Now().UnixMilli(),
            }
            c.kafka.Produce(asyncCtx, "audit.content.violation", event.SenderId, violationEvent)

            if newCount >= 3 {
                c.escalateUserViolation(asyncCtx, event.SenderId, finalResult, int(newCount))
            }
        }

        log.Info("message moderation completed",
            "request_id", requestID, "msg_id", event.MsgId,
            "result", finalResult, "confidence", aiResult.Confidence,
            "latency_ms", latencyMs)
    }()

    return nil
}
```

---

### Consumer: `msg.stored.single` / `msg.stored.group`

> 来源：Message 服务。消息存储成功事件。采样审计：按配置的采样率随机抽检消息内容合规性。  
> 采样率通过 Redis `audit:sample_rate:{topic}` 配置，默认 0.01（1%）。  
> 适用于合规审查：即使消息已正常发送，仍可事后随机抽检发现违规内容。

```go
func (c *AuditConsumer) HandleMsgStored(ctx context.Context, msg *kafka.Message, isGroup bool) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_msg.MsgStoredEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("unmarshal MsgStoredEvent failed", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 ====================
    dedupKey := fmt.Sprintf("audit:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    // ==================== 3. 采样率控制 ====================
    topic := "msg.stored.single"
    if isGroup {
        topic = "msg.stored.group"
    }
    sampleRateKey := fmt.Sprintf("audit:sample_rate:%s", topic)
    sampleRateStr, err := c.redis.Get(ctx, sampleRateKey).Result()
    sampleRate := 0.01 // 默认 1% 采样率
    if err == nil {
        if parsed, err := strconv.ParseFloat(sampleRateStr, 64); err == nil {
            sampleRate = parsed
        }
    }

    // 随机采样判断
    if rand.Float64() > sampleRate {
        return nil // 未命中采样，跳过
    }

    // ==================== 4. 仅审核文本消息 ====================
    if event.MsgType != common.MESSAGE_TYPE_TEXT || event.Content == "" {
        return nil // 非文本或内容为空，跳过
    }

    // ==================== 5. 执行关键词过滤 ====================
    matched, keyword, category := c.keywordFilter.Match(event.Content)
    if !matched {
        return nil // 关键词未命中，采样审计完成
    }

    // ==================== 6. 命中违规 → 记录 + 投递事件 ====================
    requestID := c.snowflake.Generate().String()
    reason := fmt.Sprintf("采样审计命中关键词: %s", keyword)

    // 写入审核记录
    c.db.ExecContext(ctx,
        `INSERT INTO moderation_records (request_id, content_type, content, media_url, user_id, source, status, result, reason, confidence, created_at)
         VALUES ($1, 'text', $2, '', $3, $4, 3, $5, $6, 1.0, NOW())`,
        requestID, event.Content, event.SenderId, "sample_audit", category, reason,
    )

    // 累计违规
    violationKey := fmt.Sprintf("audit:user_violation_count:%s", event.SenderId)
    c.redis.Incr(ctx, violationKey)
    c.redis.Expire(ctx, violationKey, 30*24*time.Hour)
    newCount, _ := c.redis.Get(ctx, violationKey).Int64()

    // 投递审核结果（拒绝）
    rejectEvent := &kafka_audit.ModerationResultEvent{
        Header:        buildEventHeader("audit", c.instanceID),
        ModerationId:  requestID,
        ContentId:     event.MsgId,
        ContentType:   kafka_audit.CONTENT_TYPE_TEXT,
        Result:        common.MODERATION_RESULT_REJECT,
        ViolationType: violationTypeFromCategory(category),
        Confidence:    1.0,
        Detail:        reason,
        IsAuto:        true,
        AuditTime:     time.Now().UnixMilli(),
    }
    c.kafka.Produce(ctx, "audit.moderation.result", event.MsgId, rejectEvent)

    // 投递内容违规事件
    violationEvent := &kafka_audit.ContentViolationEvent{
        Header:         buildEventHeader("audit", c.instanceID),
        ViolationId:    c.snowflake.Generate().String(),
        UserId:         event.SenderId,
        ContentId:      event.MsgId,
        ContentType:    kafka_audit.CONTENT_TYPE_TEXT,
        ViolationType:  violationTypeFromCategory(category),
        Action:         "delete",
        Detail:         reason,
        ViolationCount: int32(newCount),
        HandleTime:     time.Now().UnixMilli(),
    }
    c.kafka.Produce(ctx, "audit.content.violation", event.SenderId, violationEvent)

    // 违规升级
    if newCount >= 3 {
        c.escalateUserViolation(ctx, event.SenderId, category, int(newCount))
    }

    log.Warn("sample audit hit violation in stored message",
        "request_id", requestID, "msg_id", event.MsgId,
        "sender_id", event.SenderId, "keyword", keyword,
        "is_group", isGroup)
    return nil
}
```

---

### Consumer: `group.created`

> 来源：Group 服务。群创建事件，记录审计日志 + 触发群名内容审核。

```go
func (c *AuditConsumer) HandleGroupCreated(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_group.GroupCreatedEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("unmarshal GroupCreatedEvent failed", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 ====================
    dedupKey := fmt.Sprintf("audit:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    // ==================== 3. 记录审计日志 — PgSQL ====================
    detail, _ := json.Marshal(map[string]interface{}{
        "group_name":   event.GroupName,
        "creator_id":   event.CreatorId,
        "member_count": event.MemberCount,
    })

    logID := c.snowflake.Generate().String()
    _, err := c.db.ExecContext(ctx,
        `INSERT INTO audit_logs (log_id, user_id, action, resource_type, resource_id, detail, ip, device_info, result, created_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW())`,
        logID,
        event.CreatorId,
        "CREATE_GROUP",
        "group",
        event.GroupId,
        string(detail),
        "",
        "",
        "success",
    )
    if err != nil {
        log.Error("insert group_created audit_log failed", "err", err)
        return fmt.Errorf("insert group_created audit_log: %w", err)
    }

    // ==================== 4. 触发群名内容审核 ====================
    if event.GroupName != "" {
        requestID := c.snowflake.Generate().String()
        c.db.ExecContext(ctx,
            `INSERT INTO moderation_records (request_id, content_type, content, media_url, user_id, source, status, created_at)
             VALUES ($1, $2, $3, '', $4, $5, 1, NOW())`,
            requestID, "group_name", event.GroupName, event.CreatorId, "group_create",
        )
        go c.moderateContent(context.Background(), requestID, "group_name", event.GroupName, "", event.CreatorId, "group_create")
    }

    log.Info("group created audit log recorded",
        "log_id", logID, "group_id", event.GroupId,
        "creator_id", event.CreatorId, "group_name", event.GroupName)
    return nil
}
```

---

### Consumer: `group.info.updated`

> 来源：Group 服务。群信息（群名、群头像、群公告等）更新时触发。  
> 处理：1）记录群信息变更审计日志 2）对新群名/群公告触发内容审核。

```go
func (c *AuditConsumer) HandleGroupInfoUpdated(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_audit.GroupInfoUpdatedForAudit
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("unmarshal GroupInfoUpdatedForAudit failed", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 ====================
    dedupKey := fmt.Sprintf("audit:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    // ==================== 3. 记录审计日志 — PgSQL ====================
    detail, _ := json.Marshal(map[string]interface{}{
        "group_id":       event.GroupId,
        "operator_id":    event.OperatorId,
        "updated_fields": event.UpdatedFields,
    })

    logID := c.snowflake.Generate().String()
    _, err := c.db.ExecContext(ctx,
        `INSERT INTO audit_logs (log_id, user_id, action, resource_type, resource_id, detail, ip, device_info, result, created_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW())`,
        logID,
        event.OperatorId,
        "UPDATE_GROUP_INFO",
        "group",
        event.GroupId,
        string(detail),
        "", "",
        "success",
    )
    if err != nil {
        log.Error("insert group_info_updated audit_log failed", "err", err)
        return fmt.Errorf("insert group_info_updated audit_log: %w", err)
    }

    // ==================== 4. 群名/群公告内容审核 ====================
    for _, field := range event.UpdatedFields {
        switch field {
        case "name":
            if event.NewGroupName != "" {
                requestID := c.snowflake.Generate().String()
                go c.moderateContent(context.Background(), requestID, "group_name", event.NewGroupName, "", event.OperatorId, "group_update")
            }
        case "announcement":
            if event.NewAnnouncement != "" {
                requestID := c.snowflake.Generate().String()
                go c.moderateContent(context.Background(), requestID, "text", event.NewAnnouncement, "", event.OperatorId, "group_update")
            }
        case "avatar":
            if event.NewAvatarUrl != "" {
                requestID := c.snowflake.Generate().String()
                go c.moderateContent(context.Background(), requestID, "avatar", "", event.NewAvatarUrl, event.OperatorId, "group_update")
            }
        }
    }

    log.Info("group info updated audit log recorded",
        "log_id", logID, "group_id", event.GroupId,
        "operator_id", event.OperatorId, "fields", event.UpdatedFields)
    return nil
}
```

---

### Consumer: `group.dissolved`

> 来源：Group 服务。群解散事件，记录审计日志。

```go
func (c *AuditConsumer) HandleGroupDissolved(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_group.GroupDissolvedEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("unmarshal GroupDissolvedEvent failed", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 ====================
    dedupKey := fmt.Sprintf("audit:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    // ==================== 3. 记录审计日志 — PgSQL ====================
    detail, _ := json.Marshal(map[string]interface{}{
        "operator_id": event.OperatorId,
        "reason":      event.Reason,
    })

    logID := c.snowflake.Generate().String()
    _, err := c.db.ExecContext(ctx,
        `INSERT INTO audit_logs (log_id, user_id, action, resource_type, resource_id, detail, ip, device_info, result, created_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW())`,
        logID,
        event.OperatorId,
        "DISBAND_GROUP",
        "group",
        event.GroupId,
        string(detail),
        "",
        "",
        "success",
    )
    if err != nil {
        log.Error("insert group_dissolved audit_log failed", "err", err)
        return fmt.Errorf("insert group_dissolved audit_log: %w", err)
    }

    log.Info("group dissolved audit log recorded",
        "log_id", logID, "group_id", event.GroupId,
        "operator_id", event.OperatorId)
    return nil
}
```

---

### Consumer: `config.changed`

> 来源：Config 服务。配置变更事件。  
> 双重职责：1）记录配置变更审计日志 2）热更新审核相关配置（关键词库、采样率、AI 审核开关等）。

```go
func (c *AuditConsumer) HandleConfigChanged(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_config.ConfigChangedEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("unmarshal ConfigChangedEvent failed", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 ====================
    dedupKey := fmt.Sprintf("audit:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    // ==================== 3. 记录配置变更审计日志 — PgSQL ====================
    detail, _ := json.Marshal(map[string]interface{}{
        "config_key": event.ConfigKey,
        "old_value":  event.OldValue,
        "new_value":  event.NewValue,
        "operator":   event.OperatorId,
    })

    logID := c.snowflake.Generate().String()
    _, err := c.db.ExecContext(ctx,
        `INSERT INTO audit_logs (log_id, user_id, action, resource_type, resource_id, detail, ip, device_info, result, created_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW())`,
        logID,
        event.OperatorId,
        "CONFIG_CHANGED",
        "config",
        event.ConfigKey,
        string(detail),
        "",
        "",
        "success",
    )
    if err != nil {
        log.Error("insert config_changed audit_log failed", "err", err)
        // 不阻塞配置更新
    }

    // ==================== 4. 热更新审核相关配置 ====================
    switch event.ConfigKey {
    case "audit.keyword_filter_url":
        // 关键词库 URL 变更 → 重新加载关键词库
        if err := c.keywordFilter.ReloadFromURL(event.NewValue); err != nil {
            log.Error("reload keyword filter failed", "url", event.NewValue, "err", err)
        } else {
            // 更新关键词过滤版本号
            c.redis.Incr(ctx, "audit:keyword_filter_version")
            log.Info("keyword filter reloaded", "url", event.NewValue)
        }

    case "audit.sample_rate.single":
        // 单聊采样率变更
        c.redis.Set(ctx, "audit:sample_rate:msg.stored.single", event.NewValue, 0)
        log.Info("sample rate updated for single messages", "rate", event.NewValue)

    case "audit.sample_rate.group":
        // 群聊采样率变更
        c.redis.Set(ctx, "audit:sample_rate:msg.stored.group", event.NewValue, 0)
        log.Info("sample rate updated for group messages", "rate", event.NewValue)

    case "audit.ai_moderation_enabled":
        // AI 审核开关
        enabled, _ := strconv.ParseBool(event.NewValue)
        c.config.Store("ai_moderation_enabled", enabled)
        log.Info("AI moderation switch updated", "enabled", enabled)

    case "audit.violation_escalation_threshold":
        // 违规升级阈值
        val, _ := strconv.Atoi(event.NewValue)
        c.config.Store("violation_escalation_threshold", val)
        log.Info("violation escalation threshold updated", "threshold", val)

    case "audit.high_risk_user_threshold":
        // 高风险用户阈值
        val, _ := strconv.Atoi(event.NewValue)
        c.config.Store("high_risk_user_threshold", val)
        log.Info("high risk user threshold updated", "threshold", val)
    }

    log.Info("config changed audit log recorded",
        "log_id", logID, "config_key", event.ConfigKey)
    return nil
}
```

---

### Consumer: `media.uploaded`

> 来源：Media 服务。媒体上传完成事件，触发媒体内容审核（图片/视频/语音）。  
> 审核流水线与消息审核一致：关键词（不适用）→ AI 审核 → 人工复审。

```go
func (c *AuditConsumer) HandleMediaUploaded(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_media.MediaUploadedEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("unmarshal MediaUploadedEvent failed", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 ====================
    dedupKey := fmt.Sprintf("audit:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    // ==================== 3. 参数校验 ====================
    if event.MediaId == "" || event.Url == "" {
        log.Error("media.uploaded: media_id or url is empty",
            "media_id", event.MediaId, "url", event.Url)
        return nil
    }

    // ==================== 4. 判定媒体类型 ====================
    var contentType string
    switch event.MediaType {
    case common.UPLOAD_TYPE_IMAGE, common.UPLOAD_TYPE_AVATAR, common.UPLOAD_TYPE_GROUP_AVATAR:
        contentType = "image"
    case common.UPLOAD_TYPE_VIDEO:
        contentType = "video"
    case common.UPLOAD_TYPE_VOICE:
        contentType = "voice"
    default:
        // 文件类型不做内容审核
        log.Debug("media type not subject to moderation",
            "media_id", event.MediaId, "type", event.MediaType)
        return nil
    }

    // ==================== 5. 生成审核请求 + 写入审核记录 — PgSQL ====================
    requestID := c.snowflake.Generate().String()
    _, err := c.db.ExecContext(ctx,
        `INSERT INTO moderation_records (request_id, content_type, content, media_url, user_id, source, status, created_at)
         VALUES ($1, $2, '', $3, $4, $5, 1, NOW())`,
        requestID,
        contentType,
        event.Url,
        event.UserId,
        "media_upload",
    )
    if err != nil {
        log.Error("insert moderation_record for media failed",
            "request_id", requestID, "media_id", event.MediaId, "err", err)
        return fmt.Errorf("insert moderation_record: %w", err)
    }

    c.redis.Incr(ctx, "audit:moderation_queue_size")

    // ==================== 6. 异步 AI 审核 ====================
    go func() {
        asyncCtx := context.Background()
        startTime := time.Now()

        violationKey := fmt.Sprintf("audit:user_violation_count:%s", event.UserId)
        userViolationCount, _ := c.redis.Get(asyncCtx, violationKey).Int64()

        moderationCtx := &ModerationContext{
            UserID:         event.UserId,
            Scene:          "media_upload",
            ViolationCount: int(userViolationCount),
        }

        var aiResult *AIModerateResult
        var aiErr error

        switch contentType {
        case "image":
            aiResult, aiErr = c.aiModerator.ModerateImage(asyncCtx, event.Url, moderationCtx)
        case "video":
            aiResult, aiErr = c.aiModerator.ModerateVideo(asyncCtx, event.Url, moderationCtx)
        case "voice":
            aiResult, aiErr = c.aiModerator.ModerateVoice(asyncCtx, event.Url, moderationCtx)
        }

        latencyMs := time.Since(startTime).Milliseconds()
        c.redis.Decr(asyncCtx, "audit:moderation_queue_size")

        if aiErr != nil {
            log.Error("AI moderation for media failed",
                "request_id", requestID, "media_id", event.MediaId, "err", aiErr)

            c.db.ExecContext(asyncCtx,
                `UPDATE moderation_records SET status = 4, reason = $1 WHERE request_id = $2`,
                fmt.Sprintf("AI 审核失败: %v", aiErr), requestID,
            )
            return
        }

        // 处理审核结果
        var finalStatus int
        var moderationProtoResult common.ModerationResult

        if aiResult.Confidence >= 0.9 && aiResult.Result == "PASS" {
            finalStatus = 2
            moderationProtoResult = common.MODERATION_RESULT_PASS
        } else if aiResult.Confidence >= 0.8 && aiResult.Result != "PASS" {
            finalStatus = 3
            moderationProtoResult = common.MODERATION_RESULT_REJECT
        } else {
            finalStatus = 4
            moderationProtoResult = common.MODERATION_RESULT_REVIEW
        }

        // 更新 PgSQL
        c.db.ExecContext(asyncCtx,
            `UPDATE moderation_records SET status = $1, result = $2, reason = $3, confidence = $4 WHERE request_id = $5`,
            finalStatus, aiResult.Result, aiResult.Reason, aiResult.Confidence, requestID,
        )

        // 更新 Redis
        resultKey := fmt.Sprintf("audit:moderation:%s", requestID)
        c.redis.HSet(asyncCtx, resultKey, map[string]interface{}{
            "status":     finalStatus,
            "result":     aiResult.Result,
            "reason":     aiResult.Reason,
            "confidence": aiResult.Confidence,
        })
        c.redis.Expire(asyncCtx, resultKey, 24*time.Hour)

        // 投递审核结果
        resultEvent := &kafka_audit.ModerationResultEvent{
            Header:       buildEventHeader("audit", c.instanceID),
            ModerationId: requestID,
            ContentId:    event.MediaId,
            ContentType:  contentTypeToProto(contentType),
            Result:       moderationProtoResult,
            Confidence:   aiResult.Confidence,
            Detail:       aiResult.Reason,
            IsAuto:       true,
            AuditTime:    time.Now().UnixMilli(),
            LatencyMs:    latencyMs,
        }
        if aiResult.Result != "PASS" {
            resultEvent.ViolationType = violationTypeFromCategory(aiResult.Result)
        }
        c.kafka.Produce(asyncCtx, "audit.moderation.result", event.MediaId, resultEvent)

        // 拒绝时投递媒体违规事件
        if finalStatus == 3 {
            mediaViolation := &kafka_audit.MediaViolationEvent{
                Header:        buildEventHeader("audit", c.instanceID),
                ViolationId:   c.snowflake.Generate().String(),
                MediaId:       event.MediaId,
                UserId:        event.UserId,
                ViolationType: violationTypeFromCategory(aiResult.Result),
                Action:        "block",
                Detail:        aiResult.Reason,
                HandleTime:    time.Now().UnixMilli(),
            }
            c.kafka.Produce(asyncCtx, "audit.media.violation", event.UserId, mediaViolation)

            // 累计违规 + 升级检查
            c.redis.Incr(asyncCtx, violationKey)
            c.redis.Expire(asyncCtx, violationKey, 30*24*time.Hour)
            newCount, _ := c.redis.Get(asyncCtx, violationKey).Int64()
            if newCount >= 3 {
                c.escalateUserViolation(asyncCtx, event.UserId, aiResult.Result, int(newCount))
            }
        }

        log.Info("media moderation completed",
            "request_id", requestID, "media_id", event.MediaId,
            "result", aiResult.Result, "confidence", aiResult.Confidence,
            "latency_ms", latencyMs)
    }()

    return nil
}
```

---

### Consumer: `apigateway.auth.fail` / `apigateway.rate.limited` / `apigateway.error` / `apigateway.request.log`

> 来源：ApiGateway 服务。网关层事件，用于风控分析和审计归档。  
> 采用统一的网关事件处理器，按 topic 分别处理。

```go
// HandleApiAuthFail 处理客户端认证失败事件（风控分析）
func (c *AuditConsumer) HandleApiAuthFail(ctx context.Context, msg *kafka.Message) error {
    var event kafka_audit.ApiAuthFailForAudit
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        return nil
    }

    dedupKey := fmt.Sprintf("audit:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    // 记录审计日志
    detail, _ := json.Marshal(map[string]interface{}{
        "path":       event.Path,
        "error_code": event.ErrorCode,
        "error_msg":  event.ErrorMsg,
        "auth_type":  event.AuthType,
        "fail_count": event.FailCount,
    })

    logID := c.snowflake.Generate().String()
    c.db.ExecContext(ctx,
        `INSERT INTO audit_logs (log_id, user_id, action, resource_type, resource_id, detail, ip, device_info, result, created_at)
         VALUES ($1, '', $2, $3, $4, $5, $6, $7, $8, to_timestamp($9 / 1000.0))`,
        logID, "API_AUTH_FAIL", "apigateway", event.Path, string(detail),
        event.ClientIp, event.UserAgent, "fail", event.FailTime,
    )

    // 风控：连续认证失败检测
    if event.FailCount >= 10 {
        alertEvent := &kafka_audit.RiskAlertEvent{
            Header:      buildEventHeader("audit", c.instanceID),
            AlertId:     c.snowflake.Generate().String(),
            RiskType:    "abnormal",
            RiskLevel:   "medium",
            Description: fmt.Sprintf("IP %s 连续认证失败 %d 次", event.ClientIp, event.FailCount),
            Evidence:    map[string]string{"ip": event.ClientIp, "path": event.Path},
            Action:      "monitor",
            AlertTime:   time.Now().UnixMilli(),
        }
        c.kafka.Produce(ctx, "audit.risk.alert", event.ClientIp, alertEvent)
    }

    return nil
}

// HandleApiRateLimited 处理限流触发事件（异常流量检测）
func (c *AuditConsumer) HandleApiRateLimited(ctx context.Context, msg *kafka.Message) error {
    var event kafka_audit.ApiRateLimitedForAudit
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        return nil
    }

    dedupKey := fmt.Sprintf("audit:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    // 记录审计日志
    detail, _ := json.Marshal(map[string]interface{}{
        "rule_name":     event.RuleName,
        "limit_type":    event.LimitType,
        "current_count": event.CurrentCount,
    })

    logID := c.snowflake.Generate().String()
    c.db.ExecContext(ctx,
        `INSERT INTO audit_logs (log_id, user_id, action, resource_type, resource_id, detail, ip, device_info, result, created_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, to_timestamp($10 / 1000.0))`,
        logID, event.UserId, "RATE_LIMITED", "apigateway", event.Path,
        string(detail), event.ClientIp, "", "fail", event.TriggerTime,
    )

    // 追踪用户触发限流次数
    if event.UserId != "" {
        trackKey := fmt.Sprintf("audit:rate_limit_track:%s", event.UserId)
        count, _ := c.redis.Incr(ctx, trackKey).Result()
        if count == 1 {
            c.redis.Expire(ctx, trackKey, 1*time.Hour)
        }
        // 1 小时内触发限流超过 50 次 → 风控告警
        if count >= 50 {
            alertEvent := &kafka_audit.RiskAlertEvent{
                Header:      buildEventHeader("audit", c.instanceID),
                AlertId:     c.snowflake.Generate().String(),
                UserId:      event.UserId,
                RiskType:    "spam",
                RiskLevel:   "high",
                Description: fmt.Sprintf("用户 %s 1 小时内触发限流 %d 次，疑似异常流量", event.UserId, count),
                Evidence: map[string]string{
                    "user_id":       event.UserId,
                    "trigger_count": fmt.Sprintf("%d", count),
                    "last_path":     event.Path,
                },
                Action:    "restrict",
                AlertTime: time.Now().UnixMilli(),
            }
            c.kafka.Produce(ctx, "audit.risk.alert", event.UserId, alertEvent)
        }
    }

    return nil
}

// HandleApiError 处理网关错误事件（故障分析归档）
func (c *AuditConsumer) HandleApiError(ctx context.Context, msg *kafka.Message) error {
    var event kafka_audit.ApiErrorForAudit
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        return nil
    }

    dedupKey := fmt.Sprintf("audit:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    // 记录审计日志
    detail, _ := json.Marshal(map[string]interface{}{
        "request_id":  event.RequestId,
        "method":      event.Method,
        "status_code": event.StatusCode,
        "error_code":  event.ErrorCode,
        "error_msg":   event.ErrorMsg,
    })

    logID := c.snowflake.Generate().String()
    c.db.ExecContext(ctx,
        `INSERT INTO audit_logs (log_id, user_id, action, resource_type, resource_id, detail, ip, device_info, result, created_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, to_timestamp($10 / 1000.0))`,
        logID, event.UserId, "API_ERROR", "apigateway", event.Path,
        string(detail), event.ClientIp, "", "fail", event.ErrorTime,
    )

    return nil
}

// HandleApiRequestLog 处理网关请求日志（审计归档，批量写入）
func (c *AuditConsumer) HandleApiRequestLog(ctx context.Context, msg *kafka.Message) error {
    var event kafka_audit.ApiRequestLogForAudit
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        return nil
    }

    // 请求日志量大，不做幂等检查（容忍极少量重复）
    // 直接写入审计日志
    detail, _ := json.Marshal(map[string]interface{}{
        "request_id":  event.RequestId,
        "method":      event.Method,
        "status_code": event.StatusCode,
        "latency_ms":  event.LatencyMs,
    })

    logID := c.snowflake.Generate().String()
    c.db.ExecContext(ctx,
        `INSERT INTO audit_logs (log_id, user_id, action, resource_type, resource_id, detail, ip, device_info, result, created_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, to_timestamp($10 / 1000.0))`,
        logID, event.UserId, "API_REQUEST", "apigateway", event.Path,
        string(detail), event.ClientIp, event.UserAgent,
        func() string { if event.StatusCode < 400 { return "success" }; return "fail" }(),
        event.RequestTime,
    )

    return nil
}
```

---

### Consumer: `offline.queue.overflow`

> 来源：OfflineQueue 服务。离线队列溢出告警，触发风控告警事件。

```go
func (c *AuditConsumer) HandleOfflineQueueOverflow(ctx context.Context, msg *kafka.Message) error {
    var event kafka_audit.OfflineQueueOverflowForAudit
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        return nil
    }

    dedupKey := fmt.Sprintf("audit:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    // 记录审计日志
    detail, _ := json.Marshal(map[string]interface{}{
        "queue_size":    event.QueueSize,
        "max_size":      event.MaxSize,
        "dropped_count": event.DroppedCount,
    })

    logID := c.snowflake.Generate().String()
    c.db.ExecContext(ctx,
        `INSERT INTO audit_logs (log_id, user_id, action, resource_type, resource_id, detail, ip, device_info, result, created_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, to_timestamp($10 / 1000.0))`,
        logID, event.UserId, "OFFLINE_QUEUE_OVERFLOW", "offline_queue", event.UserId,
        string(detail), "", "", "fail", event.OverflowTime,
    )

    // 投递风控告警
    alertEvent := &kafka_audit.RiskAlertEvent{
        Header:      buildEventHeader("audit", c.instanceID),
        AlertId:     c.snowflake.Generate().String(),
        UserId:      event.UserId,
        RiskType:    "abnormal",
        RiskLevel:   "medium",
        Description: fmt.Sprintf("用户 %s 离线队列溢出，队列大小 %d/%d，丢弃 %d 条消息", event.UserId, event.QueueSize, event.MaxSize, event.DroppedCount),
        Evidence: map[string]string{
            "user_id":       event.UserId,
            "queue_size":    fmt.Sprintf("%d", event.QueueSize),
            "max_size":      fmt.Sprintf("%d", event.MaxSize),
            "dropped_count": fmt.Sprintf("%d", event.DroppedCount),
        },
        Action:    "monitor",
        AlertTime: time.Now().UnixMilli(),
    }
    c.kafka.Produce(ctx, "audit.risk.alert", event.UserId, alertEvent)

    return nil
}
```

---

### Consumer: `push.result` / `push.offline.result`

> 来源：Push 服务。推送投递结果，用于审计统计和投递成功率监控。  
> 这两个 topic 量较大，仅做统计聚合，不逐条写入审计日志。  
> 合并说明：原 `push.result.success` 和 `push.result.fail` 合并为 `push.result`，  
> 与 `push.offline.result` 保持一致的 `bool success` 模式。

```go
// HandlePushResult 推送结果统计（合并 success + fail）
func (c *AuditConsumer) HandlePushResult(ctx context.Context, msg *kafka.Message) error {
    var event kafka_audit.PushResultForAudit
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        return nil
    }

    // 推送结果事件量大，不做幂等检查，仅做统计聚合
    if event.Success {
        // 使用 Redis HyperLogLog 统计去重推送用户数
        c.redis.PFAdd(ctx, fmt.Sprintf("audit:push_success_users:%s", todayKey()), event.UserId)
        // 累计推送成功次数
        c.redis.Incr(ctx, fmt.Sprintf("audit:push_success_count:%s", todayKey()))
        // 累计推送延迟（用于计算平均延迟）
        c.redis.IncrBy(ctx, fmt.Sprintf("audit:push_latency_sum:%s", todayKey()), event.LatencyMs)
    } else {
        // 推送失败统计
        c.redis.Incr(ctx, fmt.Sprintf("audit:push_online_fail:%s", todayKey()))
        // 在线推送失败率异常高时告警
        successCount, _ := c.redis.Get(ctx, fmt.Sprintf("audit:push_success_count:%s", todayKey())).Int64()
        failCount, _ := c.redis.Get(ctx, fmt.Sprintf("audit:push_online_fail:%s", todayKey())).Int64()
        totalCount := successCount + failCount
        if totalCount > 100 && float64(failCount)/float64(totalCount) > 0.2 {
            log.Warn("online push failure rate too high",
                "success", successCount, "fail", failCount,
                "rate", fmt.Sprintf("%.2f%%", float64(failCount)/float64(totalCount)*100))
        }
    }

    return nil
}

// HandlePushOfflineResult 离线推送结果统计
func (c *AuditConsumer) HandlePushOfflineResult(ctx context.Context, msg *kafka.Message) error {
    var event kafka_audit.PushOfflineResultForAudit
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        return nil
    }

    if event.Success {
        c.redis.Incr(ctx, fmt.Sprintf("audit:push_offline_success:%s", todayKey()))
    } else {
        c.redis.Incr(ctx, fmt.Sprintf("audit:push_offline_fail:%s", todayKey()))
        // 离线推送失败率异常高时告警
        successCount, _ := c.redis.Get(ctx, fmt.Sprintf("audit:push_offline_success:%s", todayKey())).Int64()
        failCount, _ := c.redis.Get(ctx, fmt.Sprintf("audit:push_offline_fail:%s", todayKey())).Int64()
        totalCount := successCount + failCount
        if totalCount > 100 && float64(failCount)/float64(totalCount) > 0.3 {
            log.Warn("offline push failure rate too high",
                "success", successCount, "fail", failCount,
                "rate", fmt.Sprintf("%.2f%%", float64(failCount)/float64(totalCount)*100))
        }
    }

    return nil
}
```

---

### Consumer: `push.stats`

> 来源：Push 服务。定时推送统计事件（每分钟/每次推送后），用于运营统计面板。  
> Push 服务的生产 Topic 表中 `push.stats` 消费方标注为 Audit（运营统计），此处补齐消费者。

```go
// HandlePushStats 推送运营统计数据消费
func (c *AuditConsumer) HandlePushStats(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event struct {
        Header         *common.EventHeader `protobuf:"bytes,1"`
        ServerId       string // 推送服务实例 ID
        Period         string // 统计周期（如 "1m", "1h"）
        OnlinePushCount   int64  // 在线推送次数
        OfflinePushCount  int64  // 离线推送次数
        FailCount         int64  // 推送失败次数
        AvgLatencyMs      int64  // 平均推送延迟（毫秒）
        ActiveConnections int64  // 当前活跃连接数
        PeakQPS           int64  // 峰值 QPS
        Timestamp         int64  // 统计时间戳
    }
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 PushStatsEvent 失败", "err", err)
        return nil
    }

    // push.stats 为定时聚合数据，不做幂等检查（容忍少量重复，Redis 统计天然幂等累加）

    // ==================== 2. 聚合到 Redis 运营统计 ====================
    dayKey := todayKey()
    pipe := c.redis.Pipeline()

    // 累计在线推送次数
    pipe.IncrBy(ctx, fmt.Sprintf("audit:push_stats:online_count:%s", dayKey), event.OnlinePushCount)
    // 累计离线推送次数
    pipe.IncrBy(ctx, fmt.Sprintf("audit:push_stats:offline_count:%s", dayKey), event.OfflinePushCount)
    // 累计失败次数
    pipe.IncrBy(ctx, fmt.Sprintf("audit:push_stats:fail_count:%s", dayKey), event.FailCount)
    // 累计延迟总和（用于计算日平均延迟）
    pipe.IncrBy(ctx, fmt.Sprintf("audit:push_stats:latency_sum:%s", dayKey), event.AvgLatencyMs)
    // 统计上报次数（用于计算日平均延迟的分母）
    pipe.Incr(ctx, fmt.Sprintf("audit:push_stats:report_count:%s", dayKey))
    // 记录峰值 QPS（取最大值）
    peakKey := fmt.Sprintf("audit:push_stats:peak_qps:%s", dayKey)
    pipe.Exec(ctx)

    // 峰值 QPS 需要单独处理（取 max）
    currentPeak, _ := c.redis.Get(ctx, peakKey).Int64()
    if event.PeakQPS > currentPeak {
        c.redis.Set(ctx, peakKey, event.PeakQPS, 48*time.Hour)
    }

    // 设置统计 key 的过期时间（保留 48 小时供查询）
    expireKeys := []string{
        fmt.Sprintf("audit:push_stats:online_count:%s", dayKey),
        fmt.Sprintf("audit:push_stats:offline_count:%s", dayKey),
        fmt.Sprintf("audit:push_stats:fail_count:%s", dayKey),
        fmt.Sprintf("audit:push_stats:latency_sum:%s", dayKey),
        fmt.Sprintf("audit:push_stats:report_count:%s", dayKey),
    }
    for _, k := range expireKeys {
        c.redis.Expire(ctx, k, 48*time.Hour)
    }

    // ==================== 3. 异常检测：失败率告警 ====================
    totalPush := event.OnlinePushCount + event.OfflinePushCount
    if totalPush > 0 && event.FailCount > 0 {
        failRate := float64(event.FailCount) / float64(totalPush)
        if failRate > 0.1 { // 失败率超过 10% 告警
            alertEvent := &kafka_audit.RiskAlertEvent{
                Header:      buildEventHeader("audit", c.instanceID),
                AlertId:     c.snowflake.Generate().String(),
                RiskType:    "system",
                RiskLevel:   "high",
                Description: fmt.Sprintf("推送实例 %s 失败率 %.2f%% 超过阈值，在线推送 %d 次，离线推送 %d 次，失败 %d 次",
                    event.ServerId, failRate*100, event.OnlinePushCount, event.OfflinePushCount, event.FailCount),
                Evidence: map[string]string{
                    "server_id":     event.ServerId,
                    "online_count":  fmt.Sprintf("%d", event.OnlinePushCount),
                    "offline_count": fmt.Sprintf("%d", event.OfflinePushCount),
                    "fail_count":    fmt.Sprintf("%d", event.FailCount),
                    "fail_rate":     fmt.Sprintf("%.4f", failRate),
                },
                Action:    "monitor",
                AlertTime: time.Now().UnixMilli(),
            }
            c.kafka.Produce(ctx, "audit.risk.alert", event.ServerId, alertEvent)
        }
    }

    log.Debug("push.stats 运营统计已消费",
        "server_id", event.ServerId, "period", event.Period,
        "online", event.OnlinePushCount, "offline", event.OfflinePushCount,
        "fail", event.FailCount, "avg_latency_ms", event.AvgLatencyMs)
    return nil
}
```

---

### Consumer: `msg.forwarded`

> 来源：Message 服务。消息转发成功后触发。  
> 职责：记录转发操作审计日志（谁转发了哪条消息、转发到哪个会话），用于合规审计和溯源。

```go
// HandleMsgForwarded 消息转发审计记录
func (c *AuditConsumer) HandleMsgForwarded(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event struct {
        Header            *common.EventHeader `protobuf:"bytes,1"`
        OriginalMsgId     string // 原始消息 ID
        NewMsgId          string // 转发后生成的新消息 ID
        ForwarderId       string // 转发操作者 user_id
        SourceConversationId string // 来源会话
        TargetConversationId string // 目标会话
        TargetConversationType int32 // 目标会话类型（单聊/群聊）
        ForwardTime       int64  // 转发时间
    }
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 MsgForwardedEvent 失败", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 ====================
    dedupKey := fmt.Sprintf("audit:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    // ==================== 3. 参数校验 ====================
    if event.OriginalMsgId == "" || event.ForwarderId == "" {
        log.Error("msg.forwarded: 关键字段缺失",
            "original_msg_id", event.OriginalMsgId, "forwarder_id", event.ForwarderId)
        return nil
    }

    // ==================== 4. 记录审计日志 — PgSQL ====================
    detail, _ := json.Marshal(map[string]interface{}{
        "original_msg_id":         event.OriginalMsgId,
        "new_msg_id":              event.NewMsgId,
        "source_conversation_id":  event.SourceConversationId,
        "target_conversation_id":  event.TargetConversationId,
        "target_conversation_type": event.TargetConversationType,
    })

    logID := c.snowflake.Generate().String()
    _, err := c.db.ExecContext(ctx,
        `INSERT INTO audit_logs (log_id, user_id, action, resource_type, resource_id, detail, ip, device_info, result, created_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, to_timestamp($10 / 1000.0))`,
        logID,
        event.ForwarderId,       // user_id = 转发者
        "MSG_FORWARDED",         // action
        "message",               // resource_type
        event.OriginalMsgId,     // resource_id = 原始消息 ID
        string(detail),          // detail JSON
        "",                      // ip（Kafka 事件中通常无 IP）
        "",                      // device_info
        "success",               // result
        event.ForwardTime,       // created_at
    )
    if err != nil {
        log.Error("写入消息转发审计日志失败",
            "log_id", logID, "original_msg_id", event.OriginalMsgId, "err", err)
        return fmt.Errorf("写入消息转发审计日志失败: %w", err)
    }

    // ==================== 5. 转发频率异常检测 ====================
    // 短时间内大量转发可能是刷屏/垃圾信息传播
    rateKey := fmt.Sprintf("audit:rate:%s:forward", event.ForwarderId)
    forwardCount, _ := c.redis.Incr(ctx, rateKey).Result()
    if forwardCount == 1 {
        c.redis.Expire(ctx, rateKey, 10*time.Minute)
    }

    // 10 分钟内转发超过 50 次 → 风控告警
    if forwardCount >= 50 {
        alertEvent := &kafka_audit.RiskAlertEvent{
            Header:      buildEventHeader("audit", c.instanceID),
            AlertId:     c.snowflake.Generate().String(),
            UserId:      event.ForwarderId,
            RiskType:    "spam",
            RiskLevel:   "medium",
            Description: fmt.Sprintf("用户 %s 在 10 分钟内转发消息 %d 次，疑似批量转发/刷屏",
                event.ForwarderId, forwardCount),
            Evidence: map[string]string{
                "user_id":          event.ForwarderId,
                "forward_count":    fmt.Sprintf("%d", forwardCount),
                "last_original_msg": event.OriginalMsgId,
                "last_target":      event.TargetConversationId,
            },
            Action:    "monitor",
            AlertTime: time.Now().UnixMilli(),
        }
        c.kafka.Produce(ctx, "audit.risk.alert", event.ForwarderId, alertEvent)

        log.Warn("消息转发频率异常告警",
            "user_id", event.ForwarderId, "forward_count", forwardCount)
    }

    log.Info("消息转发审计日志已记录",
        "log_id", logID, "forwarder_id", event.ForwarderId,
        "original_msg_id", event.OriginalMsgId, "target", event.TargetConversationId)
    return nil
}
```

---

## 公共方法

```go
// delayedDoubleDelete 延迟双删工具函数
// Phase 1: 立即删除缓存（在 DB 写入前调用，防止旧缓存被读取）
// Phase 2: 延迟 500ms 后再次删除缓存（防止并发读在 DB 写入窗口期回填旧数据）
func (c *AuditConsumer) delayedDoubleDelete(ctx context.Context, keys ...string) {
    if len(keys) == 0 {
        return
    }
    // Phase 1: 立即删除
    c.redis.Del(ctx, keys...)
    // Phase 2: 延迟 500ms 后再次删除
    go func(delKeys []string) {
        time.Sleep(500 * time.Millisecond)
        if _, err := c.redis.Del(context.Background(), delKeys...).Result(); err != nil {
            log.Warn("delayedDoubleDelete phase 2 failed", "keys", delKeys, "err", err)
        }
    }(keys)
}

// moderateContent 通用内容审核流水线（被多个 consumer 调用）
func (c *AuditConsumer) moderateContent(ctx context.Context, requestID, contentType, textContent, mediaURL, userID, source string) {
    startTime := time.Now()

    // 获取用户违规计数
    violationKey := fmt.Sprintf("audit:user_violation_count:%s", userID)
    userViolationCount, _ := c.redis.Get(ctx, violationKey).Int64()

    moderationCtx := &ModerationContext{
        UserID:         userID,
        Scene:          source,
        ViolationCount: int(userViolationCount),
    }

    // 关键词过滤（仅文本类）
    if textContent != "" {
        matched, keyword, category := c.keywordFilter.Match(textContent)
        if matched {
            reason := fmt.Sprintf("命中关键词: %s", keyword)
            // 延迟双删 — 审核状态即将变更，使缓存失效
            resultKey := fmt.Sprintf("audit:moderation:%s", requestID)
            c.delayedDoubleDelete(ctx, resultKey)

            c.db.ExecContext(ctx,
                `UPDATE moderation_records SET status = 3, result = $1, reason = $2, confidence = 1.0 WHERE request_id = $3`,
                category, reason, requestID,
            )

            c.redis.HSet(ctx, resultKey, map[string]interface{}{
                "request_id": requestID, "status": 3, "result": category, "reason": reason, "confidence": 1.0,
            })
            c.redis.Expire(ctx, resultKey, 24*time.Hour)

            // 累计违规
            c.redis.Incr(ctx, violationKey)
            c.redis.Expire(ctx, violationKey, 30*24*time.Hour)
            return
        }
    }

    // AI 审核
    var aiResult *AIModerateResult
    var aiErr error
    switch contentType {
    case "text", "nickname", "group_name":
        aiResult, aiErr = c.aiModerator.ModerateText(ctx, textContent, moderationCtx)
    case "image", "avatar":
        aiResult, aiErr = c.aiModerator.ModerateImage(ctx, mediaURL, moderationCtx)
    case "video":
        aiResult, aiErr = c.aiModerator.ModerateVideo(ctx, mediaURL, moderationCtx)
    case "voice":
        aiResult, aiErr = c.aiModerator.ModerateVoice(ctx, mediaURL, moderationCtx)
    }

    latencyMs := time.Since(startTime).Milliseconds()

    if aiErr != nil {
        c.db.ExecContext(ctx,
            `UPDATE moderation_records SET status = 4, reason = $1 WHERE request_id = $2`,
            fmt.Sprintf("AI 审核失败: %v", aiErr), requestID,
        )
        log.Error("AI moderation failed in moderateContent", "request_id", requestID, "err", aiErr)
        return
    }

    var finalStatus int
    if aiResult.Confidence >= 0.9 && aiResult.Result == "PASS" {
        finalStatus = 2
    } else if aiResult.Confidence >= 0.8 && aiResult.Result != "PASS" {
        finalStatus = 3
    } else {
        finalStatus = 4
    }

    // 延迟双删 — 审核状态即将变更，使缓存失效
    resultKey := fmt.Sprintf("audit:moderation:%s", requestID)
    c.delayedDoubleDelete(ctx, resultKey)

    c.db.ExecContext(ctx,
        `UPDATE moderation_records SET status = $1, result = $2, reason = $3, confidence = $4 WHERE request_id = $5`,
        finalStatus, aiResult.Result, aiResult.Reason, aiResult.Confidence, requestID,
    )

    c.redis.HSet(ctx, resultKey, map[string]interface{}{
        "status": finalStatus, "result": aiResult.Result, "reason": aiResult.Reason, "confidence": aiResult.Confidence,
    })
    c.redis.Expire(ctx, resultKey, 24*time.Hour)

    if finalStatus == 3 {
        c.redis.Incr(ctx, violationKey)
        c.redis.Expire(ctx, violationKey, 30*24*time.Hour)
    }

    log.Info("moderateContent completed",
        "request_id", requestID, "content_type", contentType,
        "result", aiResult.Result, "latency_ms", latencyMs)
}

// escalateUserViolation 违规升级处理
func (c *AuditConsumer) escalateUserViolation(ctx context.Context, userID, category string, count int) {
    var banAction string
    var banDuration int64
    switch {
    case count >= 10:
        banAction = "ban"
        banDuration = 0
    case count >= 5:
        banAction = "ban"
        banDuration = 7 * 24 * 3600
    case count >= 3:
        banAction = "mute"
        banDuration = 24 * 3600
    }

    event := &kafka_audit.UserViolationEvent{
        Header:         buildEventHeader("audit", c.instanceID),
        ViolationId:    c.snowflake.Generate().String(),
        UserId:         userID,
        ViolationType:  violationTypeFromCategory(category),
        Action:         banAction,
        Reason:         fmt.Sprintf("累计违规 %d 次", count),
        ViolationCount: int32(count),
        BanDuration:    banDuration,
        HandleTime:     time.Now().UnixMilli(),
    }
    if banDuration > 0 {
        event.ExpireAt = time.Now().Add(time.Duration(banDuration) * time.Second).UnixMilli()
    }
    c.kafka.Produce(ctx, "audit.user.violation", userID, event)

    log.Warn("user violation escalated",
        "user_id", userID, "count", count, "action", banAction)
}

// todayKey 返回今日日期 key（用于统计聚合）
func todayKey() string {
    return time.Now().Format("20060102")
}
```
