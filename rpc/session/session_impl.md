# Session 登录会话服务 — RPC 接口实现伪代码

## 概述

Session 服务负责管理用户登录会话的完整生命周期，包括会话创建（Auth 登录成功后调用）、会话刷新、会话销毁、多端设备管理、同平台互踢策略。Session 管理的是「登录会话」，而非「聊天会话」（后者由 Conversation 管理）。

## 外部依赖

| 依赖类型 | 依赖项 | 用途 |
|---------|--------|------|
| PgSQL | login_sessions 表 | 登录会话持久化 |
| Redis | 会话信息缓存 / 设备索引 / 同平台互踢索引 | 高频读写 |
| RPC | Presence.UserOffline | 踢出设备时通知下线 |
| RPC | Connecte.KickConnection | 踢出设备时断开长连接 |
| Kafka | session.created / session.expired / session.kicked / session.refreshed / session.stats | 事件生产 |

## PgSQL 表结构

```sql
CREATE TABLE login_sessions (
    id          BIGSERIAL PRIMARY KEY,
    session_id  VARCHAR(64)  NOT NULL UNIQUE,
    user_id     VARCHAR(64)  NOT NULL,
    platform    SMALLINT     NOT NULL DEFAULT 0,     -- 对应 PlatformType 枚举
    device_id   VARCHAR(128) NOT NULL,
    device_name VARCHAR(256) NOT NULL DEFAULT '',
    os_version  VARCHAR(64)  NOT NULL DEFAULT '',
    app_version VARCHAR(64)  NOT NULL DEFAULT '',
    ip          VARCHAR(64)  NOT NULL DEFAULT '',
    login_time  BIGINT       NOT NULL,               -- 登录时间戳（毫秒）
    last_active BIGINT       NOT NULL,               -- 最后活跃时间戳（毫秒）
    expire_at   BIGINT       NOT NULL,               -- 过期时间戳（毫秒）
    status      SMALLINT     NOT NULL DEFAULT 1       -- 1=有效 2=已过期 3=已踢出 4=已登出
);
CREATE INDEX idx_login_sessions_user_id ON login_sessions(user_id);
CREATE INDEX idx_login_sessions_user_device ON login_sessions(user_id, device_id);
CREATE INDEX idx_login_sessions_expire ON login_sessions(expire_at) WHERE status = 1;
CREATE INDEX idx_login_sessions_user_platform ON login_sessions(user_id, platform) WHERE status = 1;
```

## Redis Key 设计

| Key 格式 | 类型 | 值 | TTL | 说明 |
|----------|------|-----|-----|------|
| `session:login:{session_id}` | HASH | session 各字段 (user_id, platform, device_id, device_name, os_version, app_version, ip, login_time, last_active, expire_at, status) | = 会话剩余有效期 | 登录会话信息缓存 |
| `session:user_devices:{user_id}` | SET | session_id 集合 | 无 TTL（手动维护） | 用户所有活跃设备的会话索引 |
| `session:device:{user_id}:{platform}` | STRING | session_id | = 会话剩余有效期 | 同平台最新会话索引（用于互踢判断） |
| `session:kafka:dedup:{event_id}` | STRING | "1" | 24h | Kafka 消费幂等去重 |

---

## 辅助方法

```go
// delayedDoubleDelete 延迟双删策略：先删缓存 → 执行 DB 操作 → 延迟再删一次
func (s *SessionService) delayedDoubleDelete(ctx context.Context, keys ...string) {
    if len(keys) == 0 {
        return
    }
    s.redis.Del(ctx, keys...)
    go func() {
        time.Sleep(500 * time.Millisecond)
        s.redis.Del(context.Background(), keys...)
    }()
}
```

---

## 接口实现

### 1. CreateLoginSession — 创建登录会话

> Auth 登录成功后调用，负责生成 session_id、同平台互踢、多设备数限制、写库写缓存。

```go
func (s *SessionService) CreateLoginSession(ctx context.Context, req *pb.CreateLoginSessionRequest) (*pb.CreateLoginSessionResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id is required")
    }
    if req.Platform == common.PLATFORM_TYPE_UNSPECIFIED {
        return nil, status.Error(codes.InvalidArgument, "platform is required")
    }
    if req.DeviceId == "" {
        return nil, status.Error(codes.InvalidArgument, "device_id is required")
    }
    if req.Ip == "" {
        req.Ip = extractClientIP(ctx) // 兜底从 ctx 中提取
    }

    now := time.Now().UnixMilli()
    sessionID := generateSessionID() // UUID v4 或 snowflake
    expireAt := now + s.config.SessionLifetimeMs // 默认 7天 = 604800000ms

    // ==================== 2. 同平台互踢策略 — Redis ====================
    // 检查该用户在同一平台是否已有活跃会话
    platformKey := fmt.Sprintf("session:device:%s:%d", req.UserId, req.Platform)
    var kickedSessionID string

    oldSessionID, err := s.redis.Get(ctx, platformKey).Result()
    if err == nil && oldSessionID != "" {
        // 同平台已有活跃会话，需要踢出旧会话
        kickedSessionID = oldSessionID

        // 2a. 将旧会话标记为已踢出 — Redis
        oldSessionKey := fmt.Sprintf("session:login:%s", oldSessionID)
        s.redis.HSet(ctx, oldSessionKey, "status", 3) // 3=已踢出

        // 2b. 从用户设备集合中移除旧会话
        devicesKey := fmt.Sprintf("session:user_devices:%s", req.UserId)
        s.redis.SRem(ctx, devicesKey, oldSessionID)

        // 2c. 更新 PgSQL 旧会话状态
        _, err = s.db.ExecContext(ctx,
            `UPDATE login_sessions SET status = 3, last_active = $1
             WHERE session_id = $2 AND status = 1`,
            now, oldSessionID,
        )
        if err != nil {
            log.Error("update kicked session status failed", "old_session_id", oldSessionID, "err", err)
        }

        // 2d. 通知 Presence 下线 — RPC
        go func() {
            _, err := s.presenceClient.UserOffline(context.Background(), &presence_pb.UserOfflineRequest{
                UserId:   req.UserId,
                Platform: req.Platform,
                DeviceId: req.DeviceId,
            })
            if err != nil {
                log.Error("notify presence offline failed", "user_id", req.UserId, "err", err)
            }
        }()

        // 2e. 断开旧连接 — RPC Connecte.KickConnection
        go func() {
            _, err := s.connecteClient.KickConnection(context.Background(), &connecte_pb.KickConnectionRequest{
                UserId:   req.UserId,
                Platform: req.Platform,
                DeviceId: req.DeviceId,
                Reason:   "same_platform_kick",
            })
            if err != nil {
                log.Error("kick old connection failed", "user_id", req.UserId, "err", err)
            }
        }()

        // 2f. 生产 session.kicked 事件 — Kafka
        kickEvent := &kafka_session.SessionKickedEvent{
            Header:       buildEventHeader("session", s.instanceID),
            SessionId:    oldSessionID,
            UserId:       req.UserId,
            Platform:     req.Platform,
            DeviceId:     req.DeviceId,
            NewSessionId: sessionID,
            Reason:       "same_platform_kick",
            KickTime:     now,
        }
        if err := s.kafka.Produce(ctx, "session.kicked", req.UserId, kickEvent); err != nil {
            log.Error("produce session.kicked failed", "err", err)
        }
    }

    // ==================== 3. 多设备数限制检查 — Redis ====================
    devicesKey := fmt.Sprintf("session:user_devices:%s", req.UserId)
    deviceCount, _ := s.redis.SCard(ctx, devicesKey).Result()
    if deviceCount >= int64(s.config.MaxOnlineDevices) {
        // 超出限制，踢掉最早登录的设备
        // 获取所有 session_id，按 login_time 排序后踢掉最早的
        allSessionIDs, _ := s.redis.SMembers(ctx, devicesKey).Result()
        var earliest string
        var earliestTime int64 = math.MaxInt64
        for _, sid := range allSessionIDs {
            loginTimeStr, _ := s.redis.HGet(ctx, fmt.Sprintf("session:login:%s", sid), "login_time").Result()
            loginTime, _ := strconv.ParseInt(loginTimeStr, 10, 64)
            if loginTime > 0 && loginTime < earliestTime {
                earliestTime = loginTime
                earliest = sid
            }
        }
        if earliest != "" && earliest != kickedSessionID {
            // 踢掉最早的会话（复用 InvalidateLoginSession 逻辑）
            s.invalidateSessionInternal(ctx, earliest, req.UserId, "max_device_exceeded", now)
        }
    }

    // ==================== 4. 写入 PgSQL ====================
    _, err = s.db.ExecContext(ctx,
        `INSERT INTO login_sessions (session_id, user_id, platform, device_id, device_name,
            os_version, app_version, ip, login_time, last_active, expire_at, status)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, 1)`,
        sessionID, req.UserId, int(req.Platform), req.DeviceId, req.DeviceName,
        req.OsVersion, req.AppVersion, req.Ip, now, now, expireAt,
    )
    if err != nil {
        return nil, status.Error(codes.Internal, "insert login_session failed: "+err.Error())
    }

    // ==================== 5. 写入 Redis ====================
    sessionKey := fmt.Sprintf("session:login:%s", sessionID)
    ttl := time.Duration(expireAt-now) * time.Millisecond

    pipe := s.redis.Pipeline()
    // 5a. 存储会话信息 HASH
    pipe.HSet(ctx, sessionKey, map[string]interface{}{
        "session_id":  sessionID,
        "user_id":     req.UserId,
        "platform":    int(req.Platform),
        "device_id":   req.DeviceId,
        "device_name": req.DeviceName,
        "os_version":  req.OsVersion,
        "app_version": req.AppVersion,
        "ip":          req.Ip,
        "login_time":  now,
        "last_active": now,
        "expire_at":   expireAt,
        "status":      1,
    })
    pipe.Expire(ctx, sessionKey, ttl)

    // 5b. 加入用户设备集合
    pipe.SAdd(ctx, devicesKey, sessionID)

    // 5c. 设置同平台索引
    pipe.Set(ctx, platformKey, sessionID, ttl)

    if _, err = pipe.Exec(ctx); err != nil {
        log.Error("redis pipeline failed", "session_id", sessionID, "err", err)
        // Redis 写失败不影响主流程，PgSQL 已落盘
    }

    // ==================== 6. 生产 Kafka 事件: session.created ====================
    createdEvent := &kafka_session.SessionCreatedEvent{
        Header:     buildEventHeader("session", s.instanceID),
        SessionId:  sessionID,
        UserId:     req.UserId,
        Platform:   req.Platform,
        DeviceId:   req.DeviceId,
        DeviceName: req.DeviceName,
        Ip:         req.Ip,
        LoginTime:  now,
        ExpireAt:   expireAt,
    }
    if err := s.kafka.Produce(ctx, "session.created", req.UserId, createdEvent); err != nil {
        log.Error("produce session.created failed", "session_id", sessionID, "err", err)
    }

    // ==================== 7. 返回 ====================
    return &pb.CreateLoginSessionResponse{
        Meta: successMeta(ctx),
        Session: &pb.LoginSession{
            SessionId:  sessionID,
            UserId:     req.UserId,
            Platform:   req.Platform,
            DeviceId:   req.DeviceId,
            DeviceName: req.DeviceName,
            OsVersion:  req.OsVersion,
            AppVersion: req.AppVersion,
            Ip:         req.Ip,
            LoginTime:  now,
            LastActive: now,
            ExpireAt:   expireAt,
            IsCurrent:  true,
        },
        KickedSessionId: kickedSessionID,
    }, nil
}
```

### 2. GetLoginSession — 获取登录会话详情

```go
func (s *SessionService) GetLoginSession(ctx context.Context, req *pb.GetLoginSessionRequest) (*pb.GetLoginSessionResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.SessionId == "" {
        return nil, status.Error(codes.InvalidArgument, "session_id is required")
    }

    // ==================== 2. 优先读 Redis 缓存 ====================
    sessionKey := fmt.Sprintf("session:login:%s", req.SessionId)
    cached, err := s.redis.HGetAll(ctx, sessionKey).Result()
    if err == nil && len(cached) > 0 {
        session := mapToLoginSession(cached)
        // 检查是否已过期
        if session.ExpireAt > 0 && session.ExpireAt < time.Now().UnixMilli() {
            // 会话已过期但 Redis 尚未清理
            return nil, status.Error(codes.NotFound, "session expired")
        }
        if session.Status != 1 {
            return nil, status.Error(codes.NotFound, "session is not active")
        }
        return &pb.GetLoginSessionResponse{
            Meta:    successMeta(ctx),
            Session: session,
        }, nil
    }

    // ==================== 3. 缓存未命中 → 读 PgSQL ====================
    var session pb.LoginSession
    var platformInt int
    var statusInt int
    err = s.db.QueryRowContext(ctx,
        `SELECT session_id, user_id, platform, device_id, device_name,
                os_version, app_version, ip, login_time, last_active, expire_at, status
         FROM login_sessions WHERE session_id = $1`,
        req.SessionId,
    ).Scan(&session.SessionId, &session.UserId, &platformInt, &session.DeviceId,
        &session.DeviceName, &session.OsVersion, &session.AppVersion, &session.Ip,
        &session.LoginTime, &session.LastActive, &session.ExpireAt, &statusInt)
    if err == sql.ErrNoRows {
        return nil, status.Error(codes.NotFound, "session not found")
    }
    if err != nil {
        return nil, status.Error(codes.Internal, "db query failed")
    }
    session.Platform = common.PlatformType(platformInt)

    if statusInt != 1 {
        return nil, status.Error(codes.NotFound, "session is not active")
    }
    if session.ExpireAt < time.Now().UnixMilli() {
        return nil, status.Error(codes.NotFound, "session expired")
    }

    // ==================== 4. 回写 Redis 缓存 ====================
    go func() {
        remaining := session.ExpireAt - time.Now().UnixMilli()
        if remaining <= 0 {
            return
        }
        ttl := time.Duration(remaining) * time.Millisecond
        pipe := s.redis.Pipeline()
        pipe.HSet(context.Background(), sessionKey, loginSessionToMap(&session))
        pipe.Expire(context.Background(), sessionKey, ttl)
        if _, err := pipe.Exec(context.Background()); err != nil {
            log.Warn("redis write-back failed", "session_id", req.SessionId, "err", err)
        }
    }()

    // ==================== 5. 返回 ====================
    return &pb.GetLoginSessionResponse{
        Meta:    successMeta(ctx),
        Session: &session,
    }, nil
}
```

### 3. RefreshLoginSession — 刷新会话有效期

> Auth.RefreshToken 时调用，延长会话生命周期。

```go
func (s *SessionService) RefreshLoginSession(ctx context.Context, req *pb.RefreshLoginSessionRequest) (*pb.RefreshLoginSessionResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.SessionId == "" {
        return nil, status.Error(codes.InvalidArgument, "session_id is required")
    }
    if req.DeviceId == "" {
        return nil, status.Error(codes.InvalidArgument, "device_id is required")
    }

    now := time.Now().UnixMilli()
    newExpireAt := now + s.config.SessionLifetimeMs

    // ==================== 2. 验证会话存在且归属正确 — Redis ====================
    sessionKey := fmt.Sprintf("session:login:%s", req.SessionId)
    storedDeviceID, err := s.redis.HGet(ctx, sessionKey, "device_id").Result()
    if err == redis.Nil {
        // Redis 无缓存，回退到 PgSQL 验证
        var dbDeviceID string
        var dbStatus int
        err := s.db.QueryRowContext(ctx,
            `SELECT device_id, status FROM login_sessions WHERE session_id = $1`,
            req.SessionId,
        ).Scan(&dbDeviceID, &dbStatus)
        if err == sql.ErrNoRows {
            return nil, status.Error(codes.NotFound, "session not found")
        }
        if err != nil {
            return nil, status.Error(codes.Internal, "db query failed")
        }
        if dbStatus != 1 {
            return nil, status.Error(codes.FailedPrecondition, "session is not active")
        }
        if dbDeviceID != req.DeviceId {
            return nil, status.Error(codes.PermissionDenied, "device_id mismatch")
        }
        storedDeviceID = dbDeviceID
    } else if err != nil {
        return nil, status.Error(codes.Internal, "redis query failed")
    } else {
        // Redis 有缓存，验证 device_id
        if storedDeviceID != req.DeviceId {
            return nil, status.Error(codes.PermissionDenied, "device_id mismatch")
        }
        // 检查会话状态
        statusStr, _ := s.redis.HGet(ctx, sessionKey, "status").Result()
        if statusStr != "1" {
            return nil, status.Error(codes.FailedPrecondition, "session is not active")
        }
    }

    // ==================== 3. 读取旧过期时间（用于 Kafka 事件） ====================
    var oldExpireAt int64
    oldExpireStr, _ := s.redis.HGet(ctx, sessionKey, "expire_at").Result()
    oldExpireAt, _ = strconv.ParseInt(oldExpireStr, 10, 64)
    if oldExpireAt == 0 {
        // 从 PgSQL 获取
        s.db.QueryRowContext(ctx,
            `SELECT expire_at FROM login_sessions WHERE session_id = $1`, req.SessionId,
        ).Scan(&oldExpireAt)
    }

    // ==================== 4. 更新 PgSQL ====================
    result, err := s.db.ExecContext(ctx,
        `UPDATE login_sessions SET last_active = $1, expire_at = $2
         WHERE session_id = $3 AND status = 1`,
        now, newExpireAt, req.SessionId,
    )
    if err != nil {
        return nil, status.Error(codes.Internal, "db update failed")
    }
    rowsAffected, _ := result.RowsAffected()
    if rowsAffected == 0 {
        return nil, status.Error(codes.NotFound, "session not found or not active")
    }

    // ==================== 5. 更新 Redis ====================
    newTTL := time.Duration(newExpireAt-now) * time.Millisecond
    userID, _ := s.redis.HGet(ctx, sessionKey, "user_id").Result()
    platformStr, _ := s.redis.HGet(ctx, sessionKey, "platform").Result()

    pipe := s.redis.Pipeline()
    pipe.HSet(ctx, sessionKey, "last_active", now, "expire_at", newExpireAt)
    pipe.Expire(ctx, sessionKey, newTTL)
    // 刷新同平台索引 TTL
    if userID != "" && platformStr != "" {
        platformKey := fmt.Sprintf("session:device:%s:%s", userID, platformStr)
        pipe.Expire(ctx, platformKey, newTTL)
    }
    if _, err := pipe.Exec(ctx); err != nil {
        log.Warn("redis refresh failed", "session_id", req.SessionId, "err", err)
    }

    // ==================== 6. 生产 Kafka 事件: session.refreshed ====================
    platform, _ := strconv.Atoi(platformStr)
    refreshEvent := &kafka_session.SessionRefreshedEvent{
        Header:      buildEventHeader("session", s.instanceID),
        SessionId:   req.SessionId,
        UserId:      userID,
        Platform:    common.PlatformType(platform),
        DeviceId:    req.DeviceId,
        OldExpireAt: oldExpireAt,
        NewExpireAt: newExpireAt,
        RefreshTime: now,
    }
    if err := s.kafka.Produce(ctx, "session.refreshed", userID, refreshEvent); err != nil {
        log.Error("produce session.refreshed failed", "session_id", req.SessionId, "err", err)
    }

    // ==================== 7. 返回 ====================
    return &pb.RefreshLoginSessionResponse{
        Meta:        successMeta(ctx),
        NewExpireAt: newExpireAt,
    }, nil
}
```

### 4. InvalidateLoginSession — 销毁登录会话（登出）

> Auth.Logout 时调用，销毁指定设备的登录会话。

```go
func (s *SessionService) InvalidateLoginSession(ctx context.Context, req *pb.InvalidateLoginSessionRequest) (*pb.InvalidateLoginSessionResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id is required")
    }
    if req.DeviceId == "" {
        return nil, status.Error(codes.InvalidArgument, "device_id is required")
    }
    if req.Reason == "" {
        req.Reason = "logout" // 默认原因
    }

    now := time.Now().UnixMilli()

    // ==================== 2. 查找该设备的活跃会话 — PgSQL ====================
    var sessionID string
    var platformInt int
    err := s.db.QueryRowContext(ctx,
        `SELECT session_id, platform FROM login_sessions
         WHERE user_id = $1 AND device_id = $2 AND status = 1
         ORDER BY login_time DESC LIMIT 1`,
        req.UserId, req.DeviceId,
    ).Scan(&sessionID, &platformInt)
    if err == sql.ErrNoRows {
        // 会话已不存在，幂等返回成功
        return &pb.InvalidateLoginSessionResponse{Meta: successMeta(ctx)}, nil
    }
    if err != nil {
        return nil, status.Error(codes.Internal, "db query failed")
    }

    // ==================== 3. 确定 status 值 ====================
    var newStatus int
    switch req.Reason {
    case "logout":
        newStatus = 4 // 已登出
    case "expired":
        newStatus = 2 // 已过期
    case "kicked", "banned":
        newStatus = 3 // 已踢出
    default:
        newStatus = 4
    }

    // ==================== 4. 延迟双删 — 先删缓存 ====================
    sessionKey := fmt.Sprintf("session:login:%s", sessionID)
    devicesKey := fmt.Sprintf("session:user_devices:%s", req.UserId)
    platformKey := fmt.Sprintf("session:device:%s:%d", req.UserId, platformInt)
    s.delayedDoubleDelete(ctx, sessionKey)

    // ==================== 5. 更新 PgSQL ====================
    _, err = s.db.ExecContext(ctx,
        `UPDATE login_sessions SET status = $1, last_active = $2
         WHERE session_id = $3 AND status = 1`,
        newStatus, now, sessionID,
    )
    if err != nil {
        return nil, status.Error(codes.Internal, "db update failed")
    }

    // ==================== 6. 清除 Redis 索引 ====================
    pipe := s.redis.Pipeline()
    pipe.SRem(ctx, devicesKey, sessionID)       // 从设备集合中移除
    // 只在当前 session 还是最新时才删除平台索引
    currentPlatformSession, _ := s.redis.Get(ctx, platformKey).Result()
    if currentPlatformSession == sessionID {
        pipe.Del(ctx, platformKey)
    }
    if _, err := pipe.Exec(ctx); err != nil {
        log.Warn("redis cleanup failed", "session_id", sessionID, "err", err)
    }

    // ==================== 7. 生产 Kafka 事件: session.expired ====================
    expiredEvent := &kafka_session.SessionExpiredEvent{
        Header:     buildEventHeader("session", s.instanceID),
        SessionId:  sessionID,
        UserId:     req.UserId,
        Platform:   common.PlatformType(platformInt),
        DeviceId:   req.DeviceId,
        ExpireTime: now,
    }
    topic := "session.expired"
    if req.Reason == "kicked" || req.Reason == "banned" {
        topic = "session.kicked"
    }
    if err := s.kafka.Produce(ctx, topic, req.UserId, expiredEvent); err != nil {
        log.Error("produce session event failed", "topic", topic, "session_id", sessionID, "err", err)
    }

    // ==================== 8. 返回 ====================
    return &pb.InvalidateLoginSessionResponse{Meta: successMeta(ctx)}, nil
}

// invalidateSessionInternal — 内部复用的会话销毁逻辑
func (s *SessionService) invalidateSessionInternal(ctx context.Context, sessionID, userID, reason string, now int64) {
    var platformInt int
    var deviceID string
    s.db.QueryRowContext(ctx,
        `SELECT platform, device_id FROM login_sessions WHERE session_id = $1`, sessionID,
    ).Scan(&platformInt, &deviceID)

    newStatus := 3 // 默认踢出
    if reason == "expired" {
        newStatus = 2
    }

    // 延迟双删 — 先删缓存
    s.delayedDoubleDelete(ctx, fmt.Sprintf("session:login:%s", sessionID))

    // PgSQL
    s.db.ExecContext(ctx,
        `UPDATE login_sessions SET status = $1, last_active = $2 WHERE session_id = $3 AND status = 1`,
        newStatus, now, sessionID,
    )

    // Redis 索引清理
    pipe := s.redis.Pipeline()
    pipe.SRem(ctx, fmt.Sprintf("session:user_devices:%s", userID), sessionID)
    platformKey := fmt.Sprintf("session:device:%s:%d", userID, platformInt)
    currentSID, _ := s.redis.Get(ctx, platformKey).Result()
    if currentSID == sessionID {
        pipe.Del(ctx, platformKey)
    }
    pipe.Exec(ctx)

    // 通知 Presence 下线
    s.presenceClient.UserOffline(ctx, &presence_pb.UserOfflineRequest{
        UserId:   userID,
        Platform: common.PlatformType(platformInt),
        DeviceId: deviceID,
    })

    // 断开连接
    s.connecteClient.KickConnection(ctx, &connecte_pb.KickConnectionRequest{
        UserId:   userID,
        Platform: common.PlatformType(platformInt),
        DeviceId: deviceID,
        Reason:   reason,
    })

    // Kafka
    s.kafka.Produce(ctx, "session.kicked", userID, &kafka_session.SessionKickedEvent{
        Header:    buildEventHeader("session", s.instanceID),
        SessionId: sessionID,
        UserId:    userID,
        Platform:  common.PlatformType(platformInt),
        DeviceId:  deviceID,
        Reason:    reason,
        KickTime:  now,
    })
}
```

### 5. KickDevice — 用户主动踢出指定设备

```go
func (s *SessionService) KickDevice(ctx context.Context, req *pb.KickDeviceRequest) (*pb.KickDeviceResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id is required")
    }
    if req.TargetDeviceId == "" {
        return nil, status.Error(codes.InvalidArgument, "target_device_id is required")
    }

    now := time.Now().UnixMilli()

    // ==================== 2. 查找目标设备的活跃会话 — PgSQL ====================
    var sessionID string
    var platformInt int
    err := s.db.QueryRowContext(ctx,
        `SELECT session_id, platform FROM login_sessions
         WHERE user_id = $1 AND device_id = $2 AND status = 1
         ORDER BY login_time DESC LIMIT 1`,
        req.UserId, req.TargetDeviceId,
    ).Scan(&sessionID, &platformInt)
    if err == sql.ErrNoRows {
        return nil, status.Error(codes.NotFound, "target device session not found")
    }
    if err != nil {
        return nil, status.Error(codes.Internal, "db query failed")
    }

    // ==================== 3. 延迟双删 — 先删缓存 ====================
    sessionKey := fmt.Sprintf("session:login:%s", sessionID)
    devicesKey := fmt.Sprintf("session:user_devices:%s", req.UserId)
    platformKey := fmt.Sprintf("session:device:%s:%d", req.UserId, platformInt)
    s.delayedDoubleDelete(ctx, sessionKey)

    // ==================== 4. 更新 PgSQL 状态 ====================
    _, err = s.db.ExecContext(ctx,
        `UPDATE login_sessions SET status = 3, last_active = $1
         WHERE session_id = $2 AND status = 1`,
        now, sessionID,
    )
    if err != nil {
        return nil, status.Error(codes.Internal, "db update failed")
    }

    // ==================== 5. 清除 Redis 索引 ====================
    pipe := s.redis.Pipeline()
    pipe.SRem(ctx, devicesKey, sessionID)
    currentSID, _ := s.redis.Get(ctx, platformKey).Result()
    if currentSID == sessionID {
        pipe.Del(ctx, platformKey)
    }
    if _, err := pipe.Exec(ctx); err != nil {
        log.Warn("redis cleanup failed", "session_id", sessionID, "err", err)
    }

    // ==================== 6. 通知 Presence 下线 — RPC ====================
    _, err = s.presenceClient.UserOffline(ctx, &presence_pb.UserOfflineRequest{
        UserId:   req.UserId,
        Platform: common.PlatformType(platformInt),
        DeviceId: req.TargetDeviceId,
    })
    if err != nil {
        log.Error("notify presence offline failed", "user_id", req.UserId, "err", err)
    }

    // ==================== 7. 断开长连接 — RPC Connecte.KickConnection ====================
    _, err = s.connecteClient.KickConnection(ctx, &connecte_pb.KickConnectionRequest{
        UserId:   req.UserId,
        Platform: common.PlatformType(platformInt),
        DeviceId: req.TargetDeviceId,
        Reason:   "user_kick",
    })
    if err != nil {
        log.Error("kick connection failed", "user_id", req.UserId, "device_id", req.TargetDeviceId, "err", err)
    }

    // ==================== 8. 生产 Kafka 事件: session.kicked ====================
    kickEvent := &kafka_session.SessionKickedEvent{
        Header:    buildEventHeader("session", s.instanceID),
        SessionId: sessionID,
        UserId:    req.UserId,
        Platform:  common.PlatformType(platformInt),
        DeviceId:  req.TargetDeviceId,
        Reason:    "user_kick",
        KickTime:  now,
    }
    if err := s.kafka.Produce(ctx, "session.kicked", req.UserId, kickEvent); err != nil {
        log.Error("produce session.kicked failed", "session_id", sessionID, "err", err)
    }

    // ==================== 9. 返回 ====================
    return &pb.KickDeviceResponse{Meta: successMeta(ctx)}, nil
}
```

### 6. GetLoginDevices — 获取用户所有活跃登录设备

```go
func (s *SessionService) GetLoginDevices(ctx context.Context, req *pb.GetLoginDevicesRequest) (*pb.GetLoginDevicesResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id is required")
    }

    now := time.Now().UnixMilli()

    // ==================== 2. 从 Redis SET 获取所有活跃 session_id ====================
    devicesKey := fmt.Sprintf("session:user_devices:%s", req.UserId)
    sessionIDs, err := s.redis.SMembers(ctx, devicesKey).Result()

    var devices []*pb.LoginSession

    if err == nil && len(sessionIDs) > 0 {
        // ==================== 3a. 批量从 Redis 读取会话详情 — Pipeline ====================
        pipe := s.redis.Pipeline()
        cmds := make(map[string]*redis.MapStringStringCmd, len(sessionIDs))
        for _, sid := range sessionIDs {
            cmds[sid] = pipe.HGetAll(ctx, fmt.Sprintf("session:login:%s", sid))
        }
        pipe.Exec(ctx)

        var expiredSIDs []string // 记录已过期的 session，用于懒清理
        for _, sid := range sessionIDs {
            result, err := cmds[sid].Result()
            if err != nil || len(result) == 0 {
                expiredSIDs = append(expiredSIDs, sid)
                continue
            }
            session := mapToLoginSession(result)
            // 过滤掉已过期/非活跃的会话
            if session.ExpireAt < now || session.Status != 1 {
                expiredSIDs = append(expiredSIDs, sid)
                continue
            }
            devices = append(devices, session)
        }

        // 懒清理：异步移除过期的 session_id
        if len(expiredSIDs) > 0 {
            go func() {
                for _, sid := range expiredSIDs {
                    s.redis.SRem(context.Background(), devicesKey, sid)
                }
            }()
        }
    } else {
        // ==================== 3b. Redis 无数据，回退 PgSQL ====================
        rows, err := s.db.QueryContext(ctx,
            `SELECT session_id, user_id, platform, device_id, device_name,
                    os_version, app_version, ip, login_time, last_active, expire_at
             FROM login_sessions
             WHERE user_id = $1 AND status = 1 AND expire_at > $2
             ORDER BY login_time DESC`,
            req.UserId, now,
        )
        if err != nil {
            return nil, status.Error(codes.Internal, "db query failed")
        }
        defer rows.Close()

        for rows.Next() {
            var session pb.LoginSession
            var platformInt int
            if err := rows.Scan(&session.SessionId, &session.UserId, &platformInt,
                &session.DeviceId, &session.DeviceName, &session.OsVersion,
                &session.AppVersion, &session.Ip, &session.LoginTime,
                &session.LastActive, &session.ExpireAt); err != nil {
                log.Warn("scan login_session row failed", "err", err)
                continue
            }
            session.Platform = common.PlatformType(platformInt)
            devices = append(devices, &session)
        }

        // 异步回写 Redis
        if len(devices) > 0 {
            go func() {
                pipe := s.redis.Pipeline()
                for _, d := range devices {
                    remaining := d.ExpireAt - time.Now().UnixMilli()
                    if remaining <= 0 {
                        continue
                    }
                    ttl := time.Duration(remaining) * time.Millisecond
                    sKey := fmt.Sprintf("session:login:%s", d.SessionId)
                    pipe.HSet(context.Background(), sKey, loginSessionToMap(d))
                    pipe.Expire(context.Background(), sKey, ttl)
                    pipe.SAdd(context.Background(), devicesKey, d.SessionId)
                }
                pipe.Exec(context.Background())
            }()
        }
    }

    // ==================== 4. 标记当前请求的会话 ====================
    currentSessionID := extractSessionIDFromCtx(ctx)
    for _, d := range devices {
        if d.SessionId == currentSessionID {
            d.IsCurrent = true
        }
    }

    // ==================== 5. 返回 ====================
    return &pb.GetLoginDevicesResponse{
        Meta:    successMeta(ctx),
        Devices: devices,
    }, nil
}
```

### 7. CleanExpiredSessions — 清理过期会话（定时任务）

> 内部定时任务调用（如每 10 分钟执行一次），批量清理已过期的登录会话。

```go
func (s *SessionService) CleanExpiredSessions(ctx context.Context, req *pb.CleanExpiredSessionsRequest) (*pb.CleanExpiredSessionsResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.BeforeTime <= 0 {
        req.BeforeTime = time.Now().UnixMilli() // 默认清理当前时间之前过期的
    }

    // ==================== 2. 分批查询过期会话 — PgSQL ====================
    batchSize := 500
    totalCleaned := 0

    for {
        rows, err := s.db.QueryContext(ctx,
            `SELECT session_id, user_id, platform, device_id, login_time
             FROM login_sessions
             WHERE status = 1 AND expire_at < $1
             ORDER BY expire_at ASC
             LIMIT $2`,
            req.BeforeTime, batchSize,
        )
        if err != nil {
            return nil, status.Error(codes.Internal, "db query expired sessions failed")
        }

        type expiredSession struct {
            SessionID string
            UserID    string
            Platform  int
            DeviceID  string
            LoginTime int64
        }
        var batch []expiredSession
        for rows.Next() {
            var es expiredSession
            if err := rows.Scan(&es.SessionID, &es.UserID, &es.Platform, &es.DeviceID, &es.LoginTime); err != nil {
                log.Warn("scan expired session failed", "err", err)
                continue
            }
            batch = append(batch, es)
        }
        rows.Close()

        if len(batch) == 0 {
            break // 没有更多过期会话
        }

        // ==================== 3. 批量更新状态 — PgSQL ====================
        sessionIDs := make([]string, len(batch))
        for i, es := range batch {
            sessionIDs[i] = es.SessionID
        }
        _, err = s.db.ExecContext(ctx,
            `UPDATE login_sessions SET status = 2
             WHERE session_id = ANY($1) AND status = 1`,
            pq.Array(sessionIDs),
        )
        if err != nil {
            log.Error("batch update expired sessions failed", "err", err)
            break
        }

        // ==================== 4. 批量清除 Redis — Pipeline ====================
        pipe := s.redis.Pipeline()
        for _, es := range batch {
            pipe.Del(ctx, fmt.Sprintf("session:login:%s", es.SessionID))
            pipe.SRem(ctx, fmt.Sprintf("session:user_devices:%s", es.UserID), es.SessionID)
            // 清理平台索引（只在当前 session 仍是最新时）
            platformKey := fmt.Sprintf("session:device:%s:%d", es.UserID, es.Platform)
            // 需要先查再删，Pipeline 中使用 Lua 脚本保证原子性
        }
        pipe.Exec(ctx)

        // 原子清理平台索引 — Lua 脚本
        for _, es := range batch {
            platformKey := fmt.Sprintf("session:device:%s:%d", es.UserID, es.Platform)
            luaScript := `
                if redis.call("GET", KEYS[1]) == ARGV[1] then
                    return redis.call("DEL", KEYS[1])
                end
                return 0
            `
            s.redis.Eval(ctx, luaScript, []string{platformKey}, es.SessionID)
        }

        // ==================== 5. 生产 Kafka 事件: session.expired ====================
        for _, es := range batch {
            expiredEvent := &kafka_session.SessionExpiredEvent{
                Header:     buildEventHeader("session", s.instanceID),
                SessionId:  es.SessionID,
                UserId:     es.UserID,
                Platform:   common.PlatformType(es.Platform),
                DeviceId:   es.DeviceID,
                ExpireTime: req.BeforeTime,
                CreatedAt:  es.LoginTime,
            }
            if err := s.kafka.Produce(ctx, "session.expired", es.UserID, expiredEvent); err != nil {
                log.Error("produce session.expired failed", "session_id", es.SessionID, "err", err)
            }
        }

        totalCleaned += len(batch)

        // 如果本批次不满，说明没有更多数据了
        if len(batch) < batchSize {
            break
        }
    }

    // ==================== 6. 生产统计事件: session.stats ====================
    if totalCleaned > 0 {
        // 统计当前活跃会话数
        var activeCount int64
        s.db.QueryRowContext(ctx,
            `SELECT COUNT(*) FROM login_sessions WHERE status = 1 AND expire_at > $1`,
            time.Now().UnixMilli(),
        ).Scan(&activeCount)

        statsEvent := &kafka_session.SessionStatsEvent{
            Header:         buildEventHeader("session", s.instanceID),
            ActiveSessions: activeCount,
            TotalExpired:   int64(totalCleaned),
            StatsTime:      time.Now().UnixMilli(),
            PeriodSeconds:  600, // 10 分钟
        }
        s.kafka.Produce(ctx, "session.stats", "stats", statsEvent)
    }

    log.Info("clean expired sessions done", "cleaned", totalCleaned)

    // ==================== 7. 返回 ====================
    return &pb.CleanExpiredSessionsResponse{
        Meta:         successMeta(ctx),
        CleanedCount: int32(totalCleaned),
    }, nil
}
```

---

## 辅助函数

```go
// generateSessionID 生成唯一会话 ID
func generateSessionID() string {
    return uuid.New().String()
}

// mapToLoginSession 将 Redis HASH map 转换为 LoginSession
func mapToLoginSession(m map[string]string) *pb.LoginSession {
    platform, _ := strconv.Atoi(m["platform"])
    loginTime, _ := strconv.ParseInt(m["login_time"], 10, 64)
    lastActive, _ := strconv.ParseInt(m["last_active"], 10, 64)
    expireAt, _ := strconv.ParseInt(m["expire_at"], 10, 64)
    statusVal, _ := strconv.Atoi(m["status"])
    return &pb.LoginSession{
        SessionId:  m["session_id"],
        UserId:     m["user_id"],
        Platform:   common.PlatformType(platform),
        DeviceId:   m["device_id"],
        DeviceName: m["device_name"],
        OsVersion:  m["os_version"],
        AppVersion: m["app_version"],
        Ip:         m["ip"],
        LoginTime:  loginTime,
        LastActive: lastActive,
        ExpireAt:   expireAt,
        Status:     int32(statusVal),
    }
}

// loginSessionToMap 将 LoginSession 转换为 Redis HASH map
func loginSessionToMap(s *pb.LoginSession) map[string]interface{} {
    return map[string]interface{}{
        "session_id":  s.SessionId,
        "user_id":     s.UserId,
        "platform":    int(s.Platform),
        "device_id":   s.DeviceId,
        "device_name": s.DeviceName,
        "os_version":  s.OsVersion,
        "app_version": s.AppVersion,
        "ip":          s.Ip,
        "login_time":  s.LoginTime,
        "last_active": s.LastActive,
        "expire_at":   s.ExpireAt,
        "status":      1,
    }
}

// extractSessionIDFromCtx 从 gRPC metadata 中提取当前 session_id
func extractSessionIDFromCtx(ctx context.Context) string {
    md, ok := metadata.FromIncomingContext(ctx)
    if !ok {
        return ""
    }
    vals := md.Get("x-session-id")
    if len(vals) > 0 {
        return vals[0]
    }
    return ""
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

// successMeta 返回成功的通用响应
func successMeta(ctx context.Context) *common.ResponseMeta {
    return &common.ResponseMeta{
        Code:       0,
        Message:    "success",
        ServerTime: time.Now().UnixMilli(),
        TraceId:    extractTraceID(ctx),
    }
}
```

---

## 可观测性接入（OpenTelemetry）

> Session 服务负责登录会话与设备管理，需重点观测：活跃会话数、设备踢下线频率、登录会话创建速率、多端登录分布、Redis 缓存命中率。

### 第一步：服务启动时初始化 OTel SDK

```go
func main() {
    ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
    defer cancel()

    otelShutdown := observability.InitOpenTelemetry(ctx, "session", "v1.0.0")
    defer otelShutdown(ctx)
    observability.SetupLogger("session")
}
```

### 第二步：gRPC Server Interceptor（Session 不依赖其他 RPC）

```go
grpcServer := grpc.NewServer(
    grpc.ChainUnaryInterceptor(otelgrpc.UnaryServerInterceptor()),
    grpc.ChainStreamInterceptor(otelgrpc.StreamServerInterceptor()),
)
```

### 第三步：注册 Session 专属业务指标

```go
var meter = otel.Meter("im-chat/session")

var (
    // 登录会话创建计数
    sessionCreated, _ = meter.Int64Counter("session.created_total",
        metric.WithDescription("登录会话创建总数"))

    // 设备踢下线计数
    deviceKicked, _ = meter.Int64Counter("session.device_kicked_total",
        metric.WithDescription("设备踢下线总数"))

    // 活跃会话数（Gauge）
    activeSessions, _ = meter.Int64Gauge("session.active_sessions",
        metric.WithDescription("当前活跃会话数"))

    // 多端登录分布
    loginByPlatform, _ = meter.Int64Counter("session.login_by_platform_total",
        metric.WithDescription("分平台登录总数"))

    // 会话缓存命中率
    sessionCacheHit, _ = meter.Int64Counter("session.cache_hit_total",
        metric.WithDescription("会话缓存命中"))
    sessionCacheMiss, _ = meter.Int64Counter("session.cache_miss_total",
        metric.WithDescription("会话缓存未命中"))
)
```

在业务代码中埋点：

```go
// CreateLoginSession 中
sessionCreated.Add(ctx, 1)
loginByPlatform.Add(ctx, 1, metric.WithAttributes(
    attribute.String("platform", req.Platform.String()),
))

// KickDevice 中
deviceKicked.Add(ctx, 1, metric.WithAttributes(
    attribute.String("reason", "new_login"),  // new_login / admin_kick / max_devices
))

// GetSession 中 — 缓存查询
if cached != nil {
    sessionCacheHit.Add(ctx, 1)
} else {
    sessionCacheMiss.Add(ctx, 1)
}
```

### 第四步：Kafka Producer 埋点 + 结构化 JSON 日志

发布 `session.created` / `session.device.kicked` 事件时注入 trace context。

### 接入要点总结

| 步骤 | 改动位置 | 效果 |
|------|---------|------|
| 1. `observability.InitOpenTelemetry` | main() | TracerProvider + MeterProvider + Runtime Metrics |
| 2. gRPC Server Interceptor | Server | RPC 自动 trace + 指标 |
| 3. 自定义业务指标 | Create/Kick/GetSession | 会话创建率、踢下线率、缓存命中 |
| 4. Kafka + 日志 | 事件发布 + setupLogger | 链路追踪 + trace_id 日志 |
| 5. buildEventHeader 改造 | 辅助函数 | EventHeader 携带真实 trace context |
| 6. 基础设施指标 | main() | DB/Redis 连接池可观测 |
| 7. Span 错误记录 | `observability.RecordError` | 错误 trace 在 Tempo 可见 |

### 补充：buildEventHeader 改造

将 `buildEventHeader(source, instanceID)` 替换为 `observability.BuildEventHeader(ctx, source, instanceID)`。

### 补充：基础设施指标注册

```go
observability.RegisterDBPoolMetrics(db, "session")
observability.RegisterRedisPoolMetrics(redisClient, "session")
```

### 补充：Span 错误记录

```go
func (s *SessionServer) CreateSession(ctx context.Context, req *pb.CreateSessionRequest) (*pb.CreateSessionResponse, error) {
    ctx, span := otel.Tracer("session").Start(ctx, "CreateSession")
    defer span.End()

    result, err := s.doCreateSession(ctx, req)
    if err != nil {
        observability.RecordError(span, err)
        return nil, err
    }
    return result, nil
}
```
