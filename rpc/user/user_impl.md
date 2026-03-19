# User 服务 — RPC 接口实现伪代码

## 概述

User 服务负责用户生命周期管理、资料 CRUD、个人设置。是最基础的服务之一，被大量其他服务依赖。

## 外部依赖

| 依赖类型 | 依赖项 | 用途 |
|---------|--------|------|
| PgSQL | users 表 | 用户信息持久化 |
| PgSQL | user_settings 表 | 用户设置持久化 |
| Redis | 用户信息缓存 | 降低 DB 压力 |
| Redis | 用户设置缓存 | 降低 DB 压力 |
| Kafka | user.registered / user.profile.updated / user.avatar.updated / user.settings.updated / user.deactivated / user.banned / user.unbanned / user.stats | 事件通知 |

> **不依赖** 任何其他 RPC 服务，User 是基础数据源。

## PgSQL 表结构

```sql
-- 用户表
CREATE TABLE users (
    id            BIGSERIAL PRIMARY KEY,
    user_id       VARCHAR(64)  NOT NULL UNIQUE,
    username      VARCHAR(64)  NOT NULL UNIQUE,
    nickname      VARCHAR(128) NOT NULL DEFAULT '',
    avatar_url    TEXT         NOT NULL DEFAULT '',
    email         VARCHAR(256) DEFAULT '',
    phone         VARCHAR(32)  DEFAULT '',
    gender        SMALLINT     NOT NULL DEFAULT 0,
    signature     VARCHAR(512) DEFAULT '',
    region        VARCHAR(128) DEFAULT '',
    birthday      BIGINT       DEFAULT 0,
    status        SMALLINT     NOT NULL DEFAULT 1, -- 1=正常 2=停用 3=封禁
    extra         JSONB        DEFAULT '{}',
    created_at    BIGINT       NOT NULL,
    updated_at    BIGINT       NOT NULL
);
CREATE INDEX idx_users_username ON users(username);
CREATE INDEX idx_users_phone ON users(phone) WHERE phone != '';
CREATE INDEX idx_users_email ON users(email) WHERE email != '';

-- 用户设置表
CREATE TABLE user_settings (
    id                       BIGSERIAL PRIMARY KEY,
    user_id                  VARCHAR(64) NOT NULL UNIQUE,
    -- 隐私设置
    allow_stranger_msg       BOOLEAN DEFAULT true,
    add_friend_need_confirm  BOOLEAN DEFAULT true,
    show_online_status       BOOLEAN DEFAULT true,
    allow_search_by_phone    BOOLEAN DEFAULT true,
    allow_search_by_username BOOLEAN DEFAULT true,
    allow_add_from_group     BOOLEAN DEFAULT true,
    allow_search_by_qrcode   BOOLEAN DEFAULT true,
    moments_visibility       VARCHAR(32) DEFAULT 'all',
    show_moments_to_stranger BOOLEAN DEFAULT false,
    hide_profile_from_blocked BOOLEAN DEFAULT true,
    send_read_receipt        BOOLEAN DEFAULT true,
    -- 通知设置
    notification_enabled     BOOLEAN DEFAULT true,
    notification_sound       VARCHAR(64) DEFAULT 'default',
    notification_vibrate     BOOLEAN DEFAULT true,
    notification_preview     BOOLEAN DEFAULT true,
    single_chat_notify       BOOLEAN DEFAULT true,
    group_chat_notify        BOOLEAN DEFAULT true,
    system_notify            BOOLEAN DEFAULT true,
    dnd_start_time           VARCHAR(8) DEFAULT '',
    dnd_end_time             VARCHAR(8) DEFAULT '',
    dnd_enabled              BOOLEAN DEFAULT false,
    call_notification        BOOLEAN DEFAULT true,
    at_me_force_notify       BOOLEAN DEFAULT true,
    -- 通用设置
    language                 VARCHAR(16) DEFAULT 'zh-CN',
    font_size                VARCHAR(16) DEFAULT 'normal',
    chat_background_url      TEXT DEFAULT '',
    dark_mode                BOOLEAN DEFAULT false,
    auto_download_media      VARCHAR(16) DEFAULT 'wifi_only',
    auto_play_video          VARCHAR(16) DEFAULT 'wifi_only',
    image_send_quality       VARCHAR(16) DEFAULT 'standard',
    send_sound_enabled       BOOLEAN DEFAULT true,
    earpiece_mode            BOOLEAN DEFAULT false,
    auto_scroll_to_bottom    BOOLEAN DEFAULT true,
    -- 账号安全设置
    two_factor_enabled       BOOLEAN DEFAULT false,
    two_factor_email         VARCHAR(256) DEFAULT '',
    login_device_confirm     BOOLEAN DEFAULT false,
    max_online_devices       INT DEFAULT 3,
    bound_phone              VARCHAR(32) DEFAULT '',
    bound_email              VARCHAR(256) DEFAULT '',
    last_password_change     BIGINT DEFAULT 0,
    -- 元数据
    created_at               BIGINT NOT NULL,
    updated_at               BIGINT NOT NULL
);
```

## Redis Key 设计

| Key 格式 | 类型 | 值 | TTL | 说明 |
|----------|------|-----|-----|------|
| `user:info:{user_id}` | HASH | 用户信息各字段 | 30min | 用户基本信息缓存 |
| `user:settings:{user_id}` | STRING | JSON 序列化 UserSettings | 30min | 用户设置缓存 |
| `user:username:{username}` | STRING | user_id | 1h | 用户名→ID 映射 |

---

## 辅助方法

```go
// delayedDoubleDelete 延迟双删策略：先删缓存 → 执行 DB 操作 → 延迟再删一次
// 调用时机：在 DB 写操作之前调用，确保缓存一致性
func (s *UserService) delayedDoubleDelete(ctx context.Context, keys ...string) {
    if len(keys) == 0 {
        return
    }
    // 第一次删除：立即删除缓存
    s.redis.Del(ctx, keys...)
    // 第二次删除：延迟 500ms 后再次删除，防止 DB 主从同步期间缓存被回填
    go func() {
        time.Sleep(500 * time.Millisecond)
        s.redis.Del(context.Background(), keys...)
    }()
}
```

---

## 接口实现

### 1. Register — 用户注册

```go
func (s *UserService) Register(ctx context.Context, req *pb.RegisterRequest) (*pb.RegisterResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.Username == "" {
        return nil, status.Error(codes.InvalidArgument, "username is required")
    }
    if len(req.Username) < 3 || len(req.Username) > 32 {
        return nil, status.Error(codes.InvalidArgument, "username length must be 3-32")
    }
    if req.Password == "" || len(req.Password) < 6 {
        return nil, status.Error(codes.InvalidArgument, "password must be at least 6 characters")
    }
    if req.Nickname == "" {
        req.Nickname = req.Username // 默认昵称=用户名
    }
    if len(req.Nickname) > 64 {
        return nil, status.Error(codes.InvalidArgument, "nickname too long")
    }
    if req.Email != "" && !isValidEmail(req.Email) {
        return nil, status.Error(codes.InvalidArgument, "invalid email format")
    }
    if req.Phone != "" && !isValidPhone(req.Phone) {
        return nil, status.Error(codes.InvalidArgument, "invalid phone format")
    }

    // ==================== 2. 唯一性校验 — PgSQL ====================
    var existingCount int
    err := s.db.QueryRowContext(ctx,
        "SELECT COUNT(*) FROM users WHERE username = $1", req.Username,
    ).Scan(&existingCount)
    if err != nil {
        return nil, status.Error(codes.Internal, "db query failed")
    }
    if existingCount > 0 {
        return nil, status.Error(codes.AlreadyExists, "username already exists")
    }

    // 手机号唯一性
    if req.Phone != "" {
        err = s.db.QueryRowContext(ctx,
            "SELECT COUNT(*) FROM users WHERE phone = $1", req.Phone,
        ).Scan(&existingCount)
        if err != nil {
            return nil, status.Error(codes.Internal, "db query failed")
        }
        if existingCount > 0 {
            return nil, status.Error(codes.AlreadyExists, "phone already registered")
        }
    }

    // ==================== 3. 生成用户 ID ====================
    userID := s.snowflake.Generate().String()
    now := time.Now().UnixMilli()

    // ==================== 4. 密码加密 ====================
    hashedPassword, err := bcrypt.GenerateFromPassword([]byte(req.Password), bcrypt.DefaultCost)
    if err != nil {
        return nil, status.Error(codes.Internal, "password encryption failed")
    }

    // ==================== 5. 事务写入 PgSQL ====================
    tx, err := s.db.BeginTx(ctx, nil)
    if err != nil {
        return nil, status.Error(codes.Internal, "begin transaction failed")
    }
    defer tx.Rollback()

    // 5a. 插入用户表
    _, err = tx.ExecContext(ctx,
        `INSERT INTO users (user_id, username, nickname, avatar_url, email, phone, gender, created_at, updated_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
        userID, req.Username, req.Nickname, req.AvatarUrl, req.Email, req.Phone, 0, now, now,
    )
    if err != nil {
        if isPgUniqueViolation(err) {
            return nil, status.Error(codes.AlreadyExists, "username or phone conflict")
        }
        return nil, status.Error(codes.Internal, "insert user failed")
    }

    // 5b. 插入用户设置（默认值）
    _, err = tx.ExecContext(ctx,
        `INSERT INTO user_settings (user_id, created_at, updated_at) VALUES ($1, $2, $3)`,
        userID, now, now,
    )
    if err != nil {
        return nil, status.Error(codes.Internal, "insert user_settings failed")
    }

    // 5c. 密码单独存储（Auth 服务管理，这里通过事件通知）
    // 注意：密码哈希不存在 User 表中，通过 Kafka 事件让 Auth 服务处理
    // 或者在 Auth 服务的注册流程中存储密码

    if err = tx.Commit(); err != nil {
        return nil, status.Error(codes.Internal, "commit transaction failed")
    }

    // ==================== 6. 预热 Redis 缓存 ====================
    userInfo := map[string]interface{}{
        "user_id":    userID,
        "username":   req.Username,
        "nickname":   req.Nickname,
        "avatar_url": req.AvatarUrl,
        "email":      req.Email,
        "phone":      req.Phone,
        "gender":     0,
        "created_at": now,
        "updated_at": now,
    }
    pipe := s.redis.Pipeline()
    pipe.HSet(ctx, fmt.Sprintf("user:info:%s", userID), userInfo)
    pipe.Expire(ctx, fmt.Sprintf("user:info:%s", userID), 30*time.Minute)
    pipe.Set(ctx, fmt.Sprintf("user:username:%s", req.Username), userID, 1*time.Hour)
    if _, err = pipe.Exec(ctx); err != nil {
        // Redis 写失败不影响主流程，仅记录日志
        log.Warn("redis cache warm-up failed", "user_id", userID, "err", err)
    }

    // ==================== 7. 发送 Kafka 事件: user.registered ====================
    event := &kafka_user.UserRegisteredEvent{
        Header:       buildEventHeader("user", s.instanceID),
        UserId:       userID,
        Username:     req.Username,
        Nickname:     req.Nickname,
        Avatar:       req.AvatarUrl,
        RegisterIp:   extractClientIP(ctx),
        RegisterTime: now,
        Platform:     req.Platform,
    }
    if err = s.kafka.Produce(ctx, "user.registered", userID, event); err != nil {
        log.Error("produce user.registered event failed", "user_id", userID, "err", err)
        // Kafka 写失败不影响注册结果，但需要告警
    }

    // ==================== 8. 返回 ====================
    return &pb.RegisterResponse{
        Meta:   successMeta(ctx),
        UserId: userID,
    }, nil
}
```

### 2. GetUser — 获取用户信息

```go
func (s *UserService) GetUser(ctx context.Context, req *pb.GetUserRequest) (*pb.GetUserResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id is required")
    }

    // ==================== 2. 优先读 Redis 缓存 ====================
    cacheKey := fmt.Sprintf("user:info:%s", req.UserId)
    cached, err := s.redis.HGetAll(ctx, cacheKey).Result()
    if err == nil && len(cached) > 0 {
        userInfo := mapToUserInfo(cached)
        return &pb.GetUserResponse{
            Meta: successMeta(ctx),
            User: userInfo,
        }, nil
    }

    // ==================== 3. 缓存未命中 → 读 PgSQL ====================
    var user pb.UserInfo
    err = s.db.QueryRowContext(ctx,
        `SELECT user_id, username, nickname, avatar_url, email, phone, gender,
                signature, region, birthday, created_at, updated_at, extra
         FROM users WHERE user_id = $1 AND status = 1`,
        req.UserId,
    ).Scan(&user.UserId, &user.Username, &user.Nickname, &user.AvatarUrl,
        &user.Email, &user.Phone, &user.Gender, &user.Signature,
        &user.Region, &user.Birthday, &user.CreatedAt, &user.UpdatedAt, &extraJSON)
    if err == sql.ErrNoRows {
        return nil, status.Error(codes.NotFound, "user not found")
    }
    if err != nil {
        return nil, status.Error(codes.Internal, "db query failed")
    }
    parseExtra(&user, extraJSON)

    // ==================== 4. 回写 Redis 缓存 ====================
    go func() {
        fields := userInfoToMap(&user)
        pipe := s.redis.Pipeline()
        pipe.HSet(context.Background(), cacheKey, fields)
        pipe.Expire(context.Background(), cacheKey, 30*time.Minute)
        if _, err := pipe.Exec(context.Background()); err != nil {
            log.Warn("redis write-back failed", "user_id", req.UserId, "err", err)
        }
    }()

    // ==================== 5. 返回 ====================
    return &pb.GetUserResponse{
        Meta: successMeta(ctx),
        User: &user,
    }, nil
}
```

### 3. BatchGetUser — 批量获取用户信息

```go
func (s *UserService) BatchGetUser(ctx context.Context, req *pb.BatchGetUserRequest) (*pb.BatchGetUserResponse, error) {
    // ==================== 1. 参数校验 ====================
    if len(req.UserIds) == 0 {
        return nil, status.Error(codes.InvalidArgument, "user_ids is required")
    }
    if len(req.UserIds) > 200 {
        return nil, status.Error(codes.InvalidArgument, "user_ids count exceeds limit 200")
    }
    // 去重
    userIDs := uniqueStrings(req.UserIds)

    // ==================== 2. 批量读 Redis — Pipeline ====================
    pipe := s.redis.Pipeline()
    cmds := make(map[string]*redis.MapStringStringCmd, len(userIDs))
    for _, uid := range userIDs {
        cmds[uid] = pipe.HGetAll(ctx, fmt.Sprintf("user:info:%s", uid))
    }
    pipe.Exec(ctx) // Pipeline 批量执行

    var cachedUsers []*pb.UserInfo
    var missedIDs []string
    for _, uid := range userIDs {
        result, err := cmds[uid].Result()
        if err != nil || len(result) == 0 {
            missedIDs = append(missedIDs, uid)
        } else {
            cachedUsers = append(cachedUsers, mapToUserInfo(result))
        }
    }

    // ==================== 3. 缓存未命中部分 → 批量查 PgSQL ====================
    var dbUsers []*pb.UserInfo
    if len(missedIDs) > 0 {
        rows, err := s.db.QueryContext(ctx,
            `SELECT user_id, username, nickname, avatar_url, email, phone, gender,
                    signature, region, birthday, created_at, updated_at, extra
             FROM users WHERE user_id = ANY($1) AND status = 1`,
            pq.Array(missedIDs),
        )
        if err != nil {
            return nil, status.Error(codes.Internal, "db batch query failed")
        }
        defer rows.Close()

        for rows.Next() {
            var u pb.UserInfo
            var extraJSON string
            if err := rows.Scan(&u.UserId, &u.Username, &u.Nickname, &u.AvatarUrl,
                &u.Email, &u.Phone, &u.Gender, &u.Signature,
                &u.Region, &u.Birthday, &u.CreatedAt, &u.UpdatedAt, &extraJSON); err != nil {
                log.Warn("scan user row failed", "err", err)
                continue
            }
            parseExtra(&u, extraJSON)
            dbUsers = append(dbUsers, &u)
        }

        // ==================== 4. 异步回写 Redis 缓存 ====================
        go func() {
            pipe := s.redis.Pipeline()
            for _, u := range dbUsers {
                key := fmt.Sprintf("user:info:%s", u.UserId)
                pipe.HSet(context.Background(), key, userInfoToMap(u))
                pipe.Expire(context.Background(), key, 30*time.Minute)
            }
            pipe.Exec(context.Background())
        }()
    }

    // ==================== 5. 合并结果并返回 ====================
    allUsers := append(cachedUsers, dbUsers...)
    return &pb.BatchGetUserResponse{
        Meta:  successMeta(ctx),
        Users: allUsers,
    }, nil
}
```

### 4. UpdateUser — 更新用户资料

```go
func (s *UserService) UpdateUser(ctx context.Context, req *pb.UpdateUserRequest) (*pb.UpdateUserResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id is required")
    }
    if req.Nickname != "" && len(req.Nickname) > 64 {
        return nil, status.Error(codes.InvalidArgument, "nickname too long")
    }
    if req.Email != "" && !isValidEmail(req.Email) {
        return nil, status.Error(codes.InvalidArgument, "invalid email")
    }
    if req.Phone != "" && !isValidPhone(req.Phone) {
        return nil, status.Error(codes.InvalidArgument, "invalid phone")
    }
    if req.Signature != "" && len(req.Signature) > 256 {
        return nil, status.Error(codes.InvalidArgument, "signature too long")
    }

    // ==================== 2. 构建动态更新 SQL ====================
    setClauses := []string{}
    args := []interface{}{}
    argIdx := 1
    updatedFields := map[string]string{}

    if req.Nickname != "" {
        setClauses = append(setClauses, fmt.Sprintf("nickname = $%d", argIdx))
        args = append(args, req.Nickname)
        updatedFields["nickname"] = req.Nickname
        argIdx++
    }
    if req.AvatarUrl != "" {
        setClauses = append(setClauses, fmt.Sprintf("avatar_url = $%d", argIdx))
        args = append(args, req.AvatarUrl)
        updatedFields["avatar_url"] = req.AvatarUrl
        argIdx++
    }
    if req.Email != "" {
        setClauses = append(setClauses, fmt.Sprintf("email = $%d", argIdx))
        args = append(args, req.Email)
        updatedFields["email"] = req.Email
        argIdx++
    }
    if req.Phone != "" {
        setClauses = append(setClauses, fmt.Sprintf("phone = $%d", argIdx))
        args = append(args, req.Phone)
        updatedFields["phone"] = req.Phone
        argIdx++
    }
    if req.Gender != 0 {
        setClauses = append(setClauses, fmt.Sprintf("gender = $%d", argIdx))
        args = append(args, req.Gender)
        updatedFields["gender"] = fmt.Sprint(req.Gender)
        argIdx++
    }
    if req.Signature != "" {
        setClauses = append(setClauses, fmt.Sprintf("signature = $%d", argIdx))
        args = append(args, req.Signature)
        updatedFields["signature"] = req.Signature
        argIdx++
    }
    if req.Region != "" {
        setClauses = append(setClauses, fmt.Sprintf("region = $%d", argIdx))
        args = append(args, req.Region)
        updatedFields["region"] = req.Region
        argIdx++
    }
    if req.Birthday != 0 {
        setClauses = append(setClauses, fmt.Sprintf("birthday = $%d", argIdx))
        args = append(args, req.Birthday)
        updatedFields["birthday"] = fmt.Sprint(req.Birthday)
        argIdx++
    }
    if len(req.Extra) > 0 {
        extraJSON, _ := json.Marshal(req.Extra)
        setClauses = append(setClauses, fmt.Sprintf("extra = $%d", argIdx))
        args = append(args, string(extraJSON))
        argIdx++
    }

    if len(setClauses) == 0 {
        return nil, status.Error(codes.InvalidArgument, "no fields to update")
    }

    // updated_at
    now := time.Now().UnixMilli()
    setClauses = append(setClauses, fmt.Sprintf("updated_at = $%d", argIdx))
    args = append(args, now)
    argIdx++

    // WHERE user_id = $N
    args = append(args, req.UserId)
    query := fmt.Sprintf("UPDATE users SET %s WHERE user_id = $%d AND status = 1",
        strings.Join(setClauses, ", "), argIdx)

    // ==================== 3. 延迟双删 — 先删缓存 ====================
    cacheKey := fmt.Sprintf("user:info:%s", req.UserId)
    s.delayedDoubleDelete(ctx, cacheKey)

    // ==================== 4. 执行 PgSQL 更新 ====================
    result, err := s.db.ExecContext(ctx, query, args...)
    if err != nil {
        return nil, status.Error(codes.Internal, "db update failed")
    }
    rowsAffected, _ := result.RowsAffected()
    if rowsAffected == 0 {
        return nil, status.Error(codes.NotFound, "user not found or deactivated")
    }

    // ==================== 5. 发送 Kafka 事件 ====================
    // 5a. user.profile.updated
    profileEvent := &kafka_user.UserProfileUpdatedEvent{
        Header:       buildEventHeader("user", s.instanceID),
        UserId:       req.UserId,
        UpdatedFields: updatedFields,
        UpdateTime:   now,
    }
    if err = s.kafka.Produce(ctx, "user.profile.updated", req.UserId, profileEvent); err != nil {
        log.Error("produce user.profile.updated failed", "err", err)
    }

    // 5b. 如果更新了头像，额外发送 user.avatar.updated
    if req.AvatarUrl != "" {
        avatarEvent := &kafka_user.UserAvatarUpdatedEvent{
            Header:     buildEventHeader("user", s.instanceID),
            UserId:     req.UserId,
            NewAvatar:  req.AvatarUrl,
            UpdateTime: now,
        }
        if err = s.kafka.Produce(ctx, "user.avatar.updated", req.UserId, avatarEvent); err != nil {
            log.Error("produce user.avatar.updated failed", "err", err)
        }
    }

    return &pb.UpdateUserResponse{Meta: successMeta(ctx)}, nil
}
```

### 5. DeactivateUser — 停用账户

```go
func (s *UserService) DeactivateUser(ctx context.Context, req *pb.DeactivateUserRequest) (*pb.DeactivateUserResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id is required")
    }

    // ==================== 2. 延迟双删 — 先删缓存 ====================
    now := time.Now().UnixMilli()
    s.delayedDoubleDelete(ctx,
        fmt.Sprintf("user:info:%s", req.UserId),
        fmt.Sprintf("user:settings:%s", req.UserId),
    )

    // ==================== 3. PgSQL 更新状态 ====================
    result, err := s.db.ExecContext(ctx,
        `UPDATE users SET status = 2, updated_at = $1 WHERE user_id = $2 AND status = 1`,
        now, req.UserId,
    )
    if err != nil {
        return nil, status.Error(codes.Internal, "db update failed")
    }
    rowsAffected, _ := result.RowsAffected()
    if rowsAffected == 0 {
        return nil, status.Error(codes.NotFound, "user not found or already deactivated")
    }

    // ==================== 4. 发送 Kafka 事件: user.deactivated ====================
    event := &kafka_user.UserDeactivatedEvent{
        Header:         buildEventHeader("user", s.instanceID),
        UserId:         req.UserId,
        Reason:         req.Reason,
        DeactivateTime: now,
    }
    if err = s.kafka.Produce(ctx, "user.deactivated", req.UserId, event); err != nil {
        log.Error("produce user.deactivated failed", "err", err)
        // 关键事件，写入补偿表后重试
        s.saveFailedEvent(ctx, "user.deactivated", req.UserId, event)
    }

    return &pb.DeactivateUserResponse{Meta: successMeta(ctx)}, nil
}
```

### 6. UpdateUserSettings — 更新用户设置

```go
func (s *UserService) UpdateUserSettings(ctx context.Context, req *pb.UpdateUserSettingsRequest) (*pb.UpdateUserSettingsResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id is required")
    }
    if req.Settings == nil {
        return nil, status.Error(codes.InvalidArgument, "settings is required")
    }

    // ==================== 2. 校验用户存在 ====================
    var exists bool
    err := s.db.QueryRowContext(ctx,
        "SELECT EXISTS(SELECT 1 FROM users WHERE user_id = $1 AND status = 1)", req.UserId,
    ).Scan(&exists)
    if err != nil || !exists {
        return nil, status.Error(codes.NotFound, "user not found")
    }

    // ==================== 3. 延迟双删 — 先删缓存 ====================
    now := time.Now().UnixMilli()
    s.delayedDoubleDelete(ctx, fmt.Sprintf("user:settings:%s", req.UserId))

    // ==================== 4. 构建动态更新 — PgSQL ====================
    // 使用 JSONB 合并策略：将整个 settings 序列化后整行 UPSERT
    settings := req.Settings

    _, err = s.db.ExecContext(ctx,
        `INSERT INTO user_settings (user_id, allow_stranger_msg, add_friend_need_confirm,
            show_online_status, allow_search_by_phone, allow_search_by_username,
            allow_add_from_group, allow_search_by_qrcode, moments_visibility,
            show_moments_to_stranger, hide_profile_from_blocked, send_read_receipt,
            notification_enabled, notification_sound, notification_vibrate,
            notification_preview, single_chat_notify, group_chat_notify,
            system_notify, dnd_start_time, dnd_end_time, dnd_enabled,
            call_notification, at_me_force_notify,
            language, font_size, chat_background_url, dark_mode,
            auto_download_media, auto_play_video, image_send_quality,
            send_sound_enabled, earpiece_mode, auto_scroll_to_bottom,
            two_factor_enabled, login_device_confirm, max_online_devices,
            created_at, updated_at)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,
                 $19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,
                 $35,$36,$37,$38,$39)
         ON CONFLICT (user_id) DO UPDATE SET
            allow_stranger_msg = EXCLUDED.allow_stranger_msg,
            add_friend_need_confirm = EXCLUDED.add_friend_need_confirm,
            show_online_status = EXCLUDED.show_online_status,
            -- ... 其余字段同理 UPSERT ...
            updated_at = EXCLUDED.updated_at`,
        req.UserId,
        settings.Privacy.AllowStrangerMsg, settings.Privacy.AddFriendNeedConfirm,
        settings.Privacy.ShowOnlineStatus, settings.Privacy.AllowSearchByPhone,
        settings.Privacy.AllowSearchByUsername, settings.Privacy.AllowAddFromGroup,
        settings.Privacy.AllowSearchByQrcode, settings.Privacy.MomentsVisibility,
        settings.Privacy.ShowMomentsToStranger, settings.Privacy.HideProfileFromBlocked,
        settings.Privacy.SendReadReceipt,
        settings.Notification.Enabled, settings.Notification.Sound,
        settings.Notification.Vibrate, settings.Notification.Preview,
        settings.Notification.SingleChatNotify, settings.Notification.GroupChatNotify,
        settings.Notification.SystemNotify, settings.Notification.DndStartTime,
        settings.Notification.DndEndTime, settings.Notification.DndEnabled,
        settings.Notification.CallNotification, settings.Notification.AtMeForceNotify,
        settings.General.Language, settings.General.FontSize,
        settings.General.ChatBackgroundUrl, settings.General.DarkMode,
        settings.General.AutoDownloadMedia, settings.General.AutoPlayVideo,
        settings.General.ImageSendQuality, settings.General.SendSoundEnabled,
        settings.General.EarpieceMode, settings.General.AutoScrollToBottom,
        settings.Security.TwoFactorEnabled, settings.Security.LoginDeviceConfirm,
        settings.Security.MaxOnlineDevices,
        now, now,
    )
    if err != nil {
        return nil, status.Error(codes.Internal, "upsert user_settings failed")
    }

    // ==================== 5. Kafka 事件: user.settings.updated ====================
    settingsEvent := &kafka_user.UserSettingsUpdatedEvent{
        Header:  buildEventHeader("user", s.instanceID),
        UserId:  req.UserId,
        Settings: &kafka_user.UserSettingsSnapshot{
            PushEnabled:      settings.Notification.Enabled,
            DndStart:         settings.Notification.DndStartTime,
            DndEnd:           settings.Notification.DndEndTime,
            DndEnabled:       settings.Notification.DndEnabled,
            ShowContent:      settings.Notification.Preview,
            AllowStrangerMsg: settings.Privacy.AllowStrangerMsg,
        },
        UpdateTime: now,
    }
    if err = s.kafka.Produce(ctx, "user.settings.updated", req.UserId, settingsEvent); err != nil {
        log.Error("produce user.settings.updated failed", "err", err)
    }

    return &pb.UpdateUserSettingsResponse{Meta: successMeta(ctx)}, nil
}
```

### 7. GetUserSettings — 获取用户设置

```go
func (s *UserService) GetUserSettings(ctx context.Context, req *pb.GetUserSettingsRequest) (*pb.GetUserSettingsResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id is required")
    }

    // ==================== 2. 读 Redis 缓存 ====================
    cacheKey := fmt.Sprintf("user:settings:%s", req.UserId)
    cached, err := s.redis.Get(ctx, cacheKey).Result()
    if err == nil && cached != "" {
        var settings pb.UserSettings
        if json.Unmarshal([]byte(cached), &settings) == nil {
            settings.UserId = req.UserId
            return &pb.GetUserSettingsResponse{
                Meta:     successMeta(ctx),
                Settings: &settings,
            }, nil
        }
    }

    // ==================== 3. PgSQL 查询 ====================
    var s0 pb.UserSettings
    s0.Privacy = &pb.PrivacySettings{}
    s0.Notification = &pb.NotificationSettings{}
    s0.General = &pb.GeneralSettings{}
    s0.Security = &pb.AccountSecuritySettings{}

    err = s.db.QueryRowContext(ctx,
        `SELECT allow_stranger_msg, add_friend_need_confirm, show_online_status,
                allow_search_by_phone, allow_search_by_username, allow_add_from_group,
                allow_search_by_qrcode, moments_visibility, show_moments_to_stranger,
                hide_profile_from_blocked, send_read_receipt,
                notification_enabled, notification_sound, notification_vibrate,
                notification_preview, single_chat_notify, group_chat_notify,
                system_notify, dnd_start_time, dnd_end_time, dnd_enabled,
                call_notification, at_me_force_notify,
                language, font_size, chat_background_url, dark_mode,
                auto_download_media, auto_play_video, image_send_quality,
                send_sound_enabled, earpiece_mode, auto_scroll_to_bottom,
                two_factor_enabled, login_device_confirm, max_online_devices,
                bound_phone, bound_email, last_password_change
         FROM user_settings WHERE user_id = $1`, req.UserId,
    ).Scan(
        &s0.Privacy.AllowStrangerMsg, &s0.Privacy.AddFriendNeedConfirm,
        &s0.Privacy.ShowOnlineStatus, &s0.Privacy.AllowSearchByPhone,
        &s0.Privacy.AllowSearchByUsername, &s0.Privacy.AllowAddFromGroup,
        &s0.Privacy.AllowSearchByQrcode, &s0.Privacy.MomentsVisibility,
        &s0.Privacy.ShowMomentsToStranger, &s0.Privacy.HideProfileFromBlocked,
        &s0.Privacy.SendReadReceipt,
        &s0.Notification.Enabled, &s0.Notification.Sound,
        &s0.Notification.Vibrate, &s0.Notification.Preview,
        &s0.Notification.SingleChatNotify, &s0.Notification.GroupChatNotify,
        &s0.Notification.SystemNotify, &s0.Notification.DndStartTime,
        &s0.Notification.DndEndTime, &s0.Notification.DndEnabled,
        &s0.Notification.CallNotification, &s0.Notification.AtMeForceNotify,
        &s0.General.Language, &s0.General.FontSize,
        &s0.General.ChatBackgroundUrl, &s0.General.DarkMode,
        &s0.General.AutoDownloadMedia, &s0.General.AutoPlayVideo,
        &s0.General.ImageSendQuality, &s0.General.SendSoundEnabled,
        &s0.General.EarpieceMode, &s0.General.AutoScrollToBottom,
        &s0.Security.TwoFactorEnabled, &s0.Security.LoginDeviceConfirm,
        &s0.Security.MaxOnlineDevices, &s0.Security.BoundPhone,
        &s0.Security.BoundEmail, &s0.Security.LastPasswordChange,
    )
    if err == sql.ErrNoRows {
        return nil, status.Error(codes.NotFound, "user settings not found")
    }
    if err != nil {
        return nil, status.Error(codes.Internal, "db query failed")
    }
    s0.UserId = req.UserId

    // ==================== 4. 回写 Redis ====================
    go func() {
        data, _ := json.Marshal(&s0)
        s.redis.Set(context.Background(), cacheKey, string(data), 30*time.Minute)
    }()

    return &pb.GetUserSettingsResponse{
        Meta:     successMeta(ctx),
        Settings: &s0,
    }, nil
}
```

---

## 可观测性接入（OpenTelemetry）

> User 服务负责用户资料 CRUD，是被调用最频繁的服务之一。需重点观测：注册速率、资料更新频率、GetUser 查询延迟（最热路径）、缓存命中率、头像上传频率。

### 第一步：服务启动时初始化 OTel SDK

```go
func main() {
    ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
    defer cancel()

    otelShutdown := observability.InitOpenTelemetry(ctx, "user", "v1.0.0")
    defer otelShutdown(ctx)
    observability.SetupLogger("user")
}
```

### 第二步：gRPC Server Interceptor（User 不依赖其他 RPC）

```go
grpcServer := grpc.NewServer(
    grpc.ChainUnaryInterceptor(otelgrpc.UnaryServerInterceptor()),
    grpc.ChainStreamInterceptor(otelgrpc.StreamServerInterceptor()),
)
```

### 第三步：注册 User 专属业务指标

```go
var meter = otel.Meter("im-chat/user")

var (
    // 用户注册计数
    userRegistered, _ = meter.Int64Counter("user.registered_total",
        metric.WithDescription("用户注册总数"))

    // 资料更新计数
    profileUpdated, _ = meter.Int64Counter("user.profile_updated_total",
        metric.WithDescription("用户资料更新总数"))

    // GetUser 查询延迟（最热路径）
    getUserDuration, _ = meter.Float64Histogram("user.get_user_duration_ms",
        metric.WithDescription("GetUser 查询延迟"), metric.WithUnit("ms"))

    // 缓存命中率
    userCacheHit, _ = meter.Int64Counter("user.cache_hit_total",
        metric.WithDescription("用户缓存命中"))
    userCacheMiss, _ = meter.Int64Counter("user.cache_miss_total",
        metric.WithDescription("用户缓存未命中"))

    // 头像上传计数
    avatarUploaded, _ = meter.Int64Counter("user.avatar_uploaded_total",
        metric.WithDescription("头像上传总数"))

    // 批量查询用户数
    batchGetSize, _ = meter.Int64Histogram("user.batch_get_size",
        metric.WithDescription("BatchGetUser 请求的用户数"))
)
```

在业务代码中埋点：

```go
// RegisterUser 中
userRegistered.Add(ctx, 1)

// UpdateProfile 中
profileUpdated.Add(ctx, 1, metric.WithAttributes(
    attribute.String("field", "nickname"),  // nickname / avatar / bio
))

// GetUser 中— 最热路径
queryStart := time.Now()
if cached != nil {
    userCacheHit.Add(ctx, 1)
} else {
    userCacheMiss.Add(ctx, 1)
    // ... DB 查询 ...
}
getUserDuration.Record(ctx, float64(time.Since(queryStart).Milliseconds()))

// BatchGetUser 中
batchGetSize.Record(ctx, int64(len(req.UserIds)))

// UpdateAvatar 中
avatarUploaded.Add(ctx, 1)
```

### 第四步：Kafka Producer 埋点 + 结构化 JSON 日志

发布 `user.registered` / `user.profile.updated` 事件时注入 trace context。

### 接入要点总结

| 步骤 | 改动位置 | 效果 |
|------|---------|------|
| 1. `observability.InitOpenTelemetry` | main() | TracerProvider + MeterProvider + Runtime Metrics |
| 2. gRPC Server Interceptor | Server | RPC 自动 trace + 指标 |
| 3. 自定义业务指标 | GetUser/Register/UpdateProfile | 注册率、查询延迟、缓存命中 |
| 4. Kafka + 日志 | 事件发布 + setupLogger | 链路追踪 + trace_id 日志 |
| 5. buildEventHeader 改造 | 辅助函数 | EventHeader 携带真实 trace context |
| 6. 基础设施指标 | main() | DB/Redis 连接池可观测 |
| 7. Span 错误记录 | `observability.RecordError` | 错误 trace 在 Tempo 可见 |

### 补充：buildEventHeader 改造

将 `buildEventHeader(source, instanceID)` 替换为 `observability.BuildEventHeader(ctx, source, instanceID)`。

### 补充：基础设施指标注册

```go
observability.RegisterDBPoolMetrics(db, "user")
observability.RegisterRedisPoolMetrics(redisClient, "user")
```

### 补充：Span 错误记录

```go
func (s *UserServer) GetUser(ctx context.Context, req *pb.GetUserRequest) (*pb.GetUserResponse, error) {
    ctx, span := otel.Tracer("user").Start(ctx, "GetUser")
    defer span.End()

    result, err := s.doGetUser(ctx, req)
    if err != nil {
        observability.RecordError(span, err)
        return nil, err
    }
    return result, nil
}
```
