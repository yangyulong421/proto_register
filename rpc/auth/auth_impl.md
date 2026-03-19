# Auth 认证服务 — RPC 接口实现伪代码

## 概述

Auth 服务负责用户登录/登出、Token 管理（签发/刷新/验证）、验证码发送与校验、第三方 OAuth 登录、密码管理。

## 外部依赖

| 依赖类型 | 依赖项 | 用途 |
|---------|--------|------|
| PgSQL | auth_credentials 表 | 存储密码哈希 |
| PgSQL | oauth_accounts 表 | 第三方登录账号绑定 |
| PgSQL | verify_codes 表 | 验证码记录（可选，Redis 为主） |
| Redis | Token 黑名单/验证码/登录失败计数 | 高频操作 |
| RPC | Session.CreateLoginSession | 登录成功后创建会话 |
| RPC | Session.InvalidateLoginSession | 登出时销毁会话 |
| RPC | Session.RefreshLoginSession | 刷新 Token 时延长会话 |
| RPC | User.GetUser | 校验用户是否存在/获取用户信息 |
| RPC | User.Register | 注册流程中创建用户记录（手机号/邮箱注册时调用） |
| Kafka | auth.login.success / auth.logout / auth.token.refreshed / auth.password.changed 等 | 事件通知 |

## PgSQL 表结构

```sql
CREATE TABLE auth_credentials (
    id             BIGSERIAL PRIMARY KEY,
    user_id        VARCHAR(64) NOT NULL UNIQUE,
    username       VARCHAR(64) NOT NULL UNIQUE,
    password_hash  VARCHAR(256) NOT NULL,
    created_at     BIGINT NOT NULL,
    updated_at     BIGINT NOT NULL
);

CREATE TABLE oauth_accounts (
    id          BIGSERIAL PRIMARY KEY,
    user_id     VARCHAR(64) NOT NULL,
    provider    VARCHAR(32) NOT NULL, -- wechat / google / apple / github
    provider_id VARCHAR(256) NOT NULL,
    extra       JSONB DEFAULT '{}',
    created_at  BIGINT NOT NULL,
    UNIQUE(provider, provider_id)
);
CREATE INDEX idx_oauth_user ON oauth_accounts(user_id);
```

## Redis Key 设计

| Key 格式 | 类型 | 值 | TTL | 说明 |
|----------|------|-----|-----|------|
| `auth:token:blacklist:{token_hash}` | STRING | "1" | = token 剩余有效期 | Token 黑名单 |
| `auth:verify_code:{target}:{purpose}` | STRING | sha256(code) | 5min | 验证码（哈希存储，防泄露） |
| `auth:verify_rate:{target}` | STRING | count | 1h | 验证码发送频率限制 |
| `auth:login_fail:{identifier}` | STRING | count | 30min | 登录失败计数（防暴力破解） |
| `auth:refresh_token:{token_hash}` | STRING | user_id:device_id | 7d | 刷新令牌映射 |

---

## 辅助方法

```go
// delayedDoubleDelete 延迟双删策略：先删缓存 → 执行 DB 操作 → 延迟再删一次
func (s *AuthService) delayedDoubleDelete(ctx context.Context, keys ...string) {
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

### 1. Login — 用户名密码登录

```go
func (s *AuthService) Login(ctx context.Context, req *pb.LoginRequest) (*pb.LoginResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.Username == "" {
        return nil, status.Error(codes.InvalidArgument, "username is required")
    }
    if req.Password == "" {
        return nil, status.Error(codes.InvalidArgument, "password is required")
    }
    if req.Platform == common.PLATFORM_TYPE_UNSPECIFIED {
        return nil, status.Error(codes.InvalidArgument, "platform is required")
    }
    if req.DeviceId == "" {
        return nil, status.Error(codes.InvalidArgument, "device_id is required")
    }

    // ==================== 2. 登录失败次数检查 — Redis ====================
    failKey := fmt.Sprintf("auth:login_fail:%s", req.Username)
    failCount, _ := s.redis.Get(ctx, failKey).Int()
    if failCount >= 5 {
        return nil, status.Error(codes.ResourceExhausted, "too many login attempts, try again later")
    }

    // ==================== 3. 查询凭证 — PgSQL ====================
    var userID, passwordHash string
    err := s.db.QueryRowContext(ctx,
        `SELECT user_id, password_hash FROM auth_credentials WHERE username = $1`,
        req.Username,
    ).Scan(&userID, &passwordHash)
    if err == sql.ErrNoRows {
        // 增加失败计数
        s.redis.Incr(ctx, failKey)
        s.redis.Expire(ctx, failKey, 30*time.Minute)
        // 生产 auth.login.fail 事件
        s.produceLoginFailEvent(ctx, req, "user_not_found")
        return nil, status.Error(codes.Unauthenticated, "invalid username or password")
    }
    if err != nil {
        return nil, status.Error(codes.Internal, "db query failed")
    }

    // ==================== 4. 校验密码 ====================
    if err := bcrypt.CompareHashAndPassword([]byte(passwordHash), []byte(req.Password)); err != nil {
        // 增加失败计数
        s.redis.Incr(ctx, failKey)
        s.redis.Expire(ctx, failKey, 30*time.Minute)
        s.produceLoginFailEvent(ctx, req, "wrong_password")
        return nil, status.Error(codes.Unauthenticated, "invalid username or password")
    }

    // ==================== 5. 检查用户状态 — RPC User.GetUser ====================
    userResp, err := s.userClient.GetUser(ctx, &user_pb.GetUserRequest{UserId: userID})
    if err != nil {
        return nil, status.Error(codes.Internal, "get user info failed")
    }
    // 注：如果用户被封禁 GetUser 可能返回 NotFound

    // ==================== 6. 签发 Token ====================
    now := time.Now()
    accessToken, err := s.jwt.Sign(jwt.Claims{
        UserID:   userID,
        Platform: req.Platform,
        DeviceID: req.DeviceId,
        Exp:      now.Add(2 * time.Hour).Unix(),
    })
    if err != nil {
        return nil, status.Error(codes.Internal, "sign access_token failed")
    }

    refreshToken, err := s.jwt.Sign(jwt.Claims{
        UserID:   userID,
        Platform: req.Platform,
        DeviceID: req.DeviceId,
        Type:     "refresh",
        Exp:      now.Add(7 * 24 * time.Hour).Unix(),
    })
    if err != nil {
        return nil, status.Error(codes.Internal, "sign refresh_token failed")
    }

    // 存储 refresh_token 映射 — Redis
    rtHash := sha256Hex(refreshToken)
    s.redis.Set(ctx, fmt.Sprintf("auth:refresh_token:%s", rtHash),
        fmt.Sprintf("%s:%s", userID, req.DeviceId), 7*24*time.Hour)

    // ==================== 7. 创建登录会话 — RPC Session.CreateLoginSession ====================
    sessionResp, err := s.sessionClient.CreateLoginSession(ctx, &session_pb.CreateLoginSessionRequest{
        UserId:     userID,
        Platform:   req.Platform,
        DeviceId:   req.DeviceId,
        DeviceName: req.DeviceName,
        Ip:         extractClientIP(ctx),
    })
    if err != nil {
        log.Error("create login session failed", "err", err)
        // 非致命错误，Token 已签发，降级处理
    }

    // ==================== 8. 清除失败计数 — Redis ====================
    s.redis.Del(ctx, failKey)

    // ==================== 9. 生产 Kafka 事件: auth.login.success ====================
    loginEvent := &kafka_auth.LoginSuccessEvent{
        Header:     buildEventHeader("auth", s.instanceID),
        UserId:     userID,
        LoginType:  kafka_auth.LOGIN_TYPE_PASSWORD,
        Platform:   req.Platform,
        DeviceId:   req.DeviceId,
        DeviceName: req.DeviceName,
        Ip:         extractClientIP(ctx),
        UserAgent:  extractUserAgent(ctx),
        SessionId:  sessionResp.GetSession().GetSessionId(),
        LoginTime:  now.UnixMilli(),
    }
    s.kafka.Produce(ctx, "auth.login.success", userID, loginEvent)

    // ==================== 10. 返回 ====================
    return &pb.LoginResponse{
        Meta:   successMeta(ctx),
        UserId: userID,
        TokenInfo: &pb.TokenInfo{
            AccessToken:  accessToken,
            RefreshToken: refreshToken,
            ExpiresAt:    now.Add(2 * time.Hour).Unix(),
            TokenType:    "Bearer",
        },
    }, nil
}
```

### 2. Logout — 登出

```go
func (s *AuthService) Logout(ctx context.Context, req *pb.LogoutRequest) (*pb.LogoutResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id is required")
    }
    if req.DeviceId == "" {
        return nil, status.Error(codes.InvalidArgument, "device_id is required")
    }

    // ==================== 2. 将当前 Token 加入黑名单 — Redis ====================
    accessToken := extractAccessToken(ctx)
    if accessToken != "" {
        tokenHash := sha256Hex(accessToken)
        claims, err := s.jwt.Parse(accessToken)
        if err == nil {
            remaining := time.Until(time.Unix(claims.Exp, 0))
            if remaining > 0 {
                s.redis.Set(ctx, fmt.Sprintf("auth:token:blacklist:%s", tokenHash), "1", remaining)
            }
        }
    }

    // ==================== 3. 销毁登录会话 — RPC Session.InvalidateLoginSession ====================
    _, err := s.sessionClient.InvalidateLoginSession(ctx, &session_pb.InvalidateLoginSessionRequest{
        UserId:   req.UserId,
        DeviceId: req.DeviceId,
        Reason:   "logout",
    })
    if err != nil {
        log.Error("invalidate login session failed", "err", err)
    }

    // ==================== 4. 生产 Kafka 事件: auth.logout ====================
    logoutEvent := &kafka_auth.LogoutEvent{
        Header:     buildEventHeader("auth", s.instanceID),
        UserId:     req.UserId,
        Platform:   req.Platform,
        DeviceId:   req.DeviceId,
        LogoutTime: time.Now().UnixMilli(),
    }
    s.kafka.Produce(ctx, "auth.logout", req.UserId, logoutEvent)

    return &pb.LogoutResponse{Meta: successMeta(ctx)}, nil
}
```

### 3. RefreshToken — 刷新访问令牌

```go
func (s *AuthService) RefreshToken(ctx context.Context, req *pb.RefreshTokenRequest) (*pb.RefreshTokenResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.RefreshToken == "" {
        return nil, status.Error(codes.InvalidArgument, "refresh_token is required")
    }

    // ==================== 2. 验证 refresh_token — Redis ====================
    rtHash := sha256Hex(req.RefreshToken)
    rtKey := fmt.Sprintf("auth:refresh_token:%s", rtHash)
    stored, err := s.redis.Get(ctx, rtKey).Result()
    if err == redis.Nil {
        return nil, status.Error(codes.Unauthenticated, "refresh token expired or invalid")
    }
    if err != nil {
        return nil, status.Error(codes.Internal, "redis query failed")
    }

    // 解析 user_id:device_id
    parts := strings.SplitN(stored, ":", 2)
    if len(parts) != 2 {
        return nil, status.Error(codes.Internal, "invalid refresh token data")
    }
    userID, deviceID := parts[0], parts[1]

    // ==================== 3. 验证 JWT 签名 ====================
    claims, err := s.jwt.Parse(req.RefreshToken)
    if err != nil {
        s.redis.Del(ctx, rtKey)
        return nil, status.Error(codes.Unauthenticated, "invalid refresh token")
    }
    if claims.UserID != userID {
        return nil, status.Error(codes.Unauthenticated, "token user mismatch")
    }

    // ==================== 4. 签发新的 access_token ====================
    now := time.Now()
    newAccessToken, err := s.jwt.Sign(jwt.Claims{
        UserID:   userID,
        Platform: req.Platform,
        DeviceID: deviceID,
        Exp:      now.Add(2 * time.Hour).Unix(),
    })
    if err != nil {
        return nil, status.Error(codes.Internal, "sign new access_token failed")
    }

    // ==================== 5. 签发新的 refresh_token（旋转策略）====================
    newRefreshToken, err := s.jwt.Sign(jwt.Claims{
        UserID: userID, Platform: req.Platform, DeviceID: deviceID,
        Type: "refresh", Exp: now.Add(7 * 24 * time.Hour).Unix(),
    })
    if err != nil {
        return nil, status.Error(codes.Internal, "sign new refresh_token failed")
    }

    // 删除旧 refresh_token，写入新的
    pipe := s.redis.Pipeline()
    pipe.Del(ctx, rtKey)
    newRtHash := sha256Hex(newRefreshToken)
    pipe.Set(ctx, fmt.Sprintf("auth:refresh_token:%s", newRtHash),
        fmt.Sprintf("%s:%s", userID, deviceID), 7*24*time.Hour)
    pipe.Exec(ctx)

    // ==================== 6. 延长登录会话 — RPC Session.RefreshLoginSession ====================
    s.sessionClient.RefreshLoginSession(ctx, &session_pb.RefreshLoginSessionRequest{
        SessionId: claims.SessionID,
        DeviceId:  deviceID,
    })

    // ==================== 7. 生产 Kafka 事件: auth.token.refreshed ====================
    event := &kafka_auth.TokenRefreshedEvent{
        Header:      buildEventHeader("auth", s.instanceID),
        UserId:      userID,
        Platform:    req.Platform,
        DeviceId:    deviceID,
        RefreshTime: now.UnixMilli(),
    }
    s.kafka.Produce(ctx, "auth.token.refreshed", userID, event)

    return &pb.RefreshTokenResponse{
        Meta: successMeta(ctx),
        TokenInfo: &pb.TokenInfo{
            AccessToken:  newAccessToken,
            RefreshToken: newRefreshToken,
            ExpiresAt:    now.Add(2 * time.Hour).Unix(),
            TokenType:    "Bearer",
        },
    }, nil
}
```

### 4. VerifyToken — 验证令牌有效性

```go
func (s *AuthService) VerifyToken(ctx context.Context, req *pb.VerifyTokenRequest) (*pb.VerifyTokenResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.AccessToken == "" {
        return nil, status.Error(codes.InvalidArgument, "access_token is required")
    }

    // ==================== 2. 检查黑名单 — Redis ====================
    tokenHash := sha256Hex(req.AccessToken)
    exists, _ := s.redis.Exists(ctx, fmt.Sprintf("auth:token:blacklist:%s", tokenHash)).Result()
    if exists > 0 {
        return &pb.VerifyTokenResponse{
            Meta: successMeta(ctx), IsValid: false,
        }, nil
    }

    // ==================== 3. 解析并验证 JWT ====================
    claims, err := s.jwt.Parse(req.AccessToken)
    if err != nil {
        return &pb.VerifyTokenResponse{
            Meta: successMeta(ctx), IsValid: false,
        }, nil
    }

    remaining := time.Until(time.Unix(claims.Exp, 0)).Seconds()
    if remaining <= 0 {
        return &pb.VerifyTokenResponse{
            Meta: successMeta(ctx), IsValid: false,
        }, nil
    }

    return &pb.VerifyTokenResponse{
        Meta:             successMeta(ctx),
        IsValid:          true,
        UserId:           claims.UserID,
        Platform:         claims.Platform,
        RemainingSeconds: int64(remaining),
    }, nil
}
```

### 5. SendVerifyCode — 发送验证码

```go
func (s *AuthService) SendVerifyCode(ctx context.Context, req *pb.SendVerifyCodeRequest) (*pb.SendVerifyCodeResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.Target == "" {
        return nil, status.Error(codes.InvalidArgument, "target is required")
    }
    if req.Type != "sms" && req.Type != "email" {
        return nil, status.Error(codes.InvalidArgument, "type must be sms or email")
    }
    if req.Purpose != "login" && req.Purpose != "register" && req.Purpose != "reset_password" {
        return nil, status.Error(codes.InvalidArgument, "invalid purpose")
    }
    if req.Type == "sms" && !isValidPhone(req.Target) {
        return nil, status.Error(codes.InvalidArgument, "invalid phone number")
    }
    if req.Type == "email" && !isValidEmail(req.Target) {
        return nil, status.Error(codes.InvalidArgument, "invalid email")
    }

    // ==================== 2. 频率限制 — Redis ====================
    rateKey := fmt.Sprintf("auth:verify_rate:%s", req.Target)
    count, _ := s.redis.Incr(ctx, rateKey).Result()
    if count == 1 {
        s.redis.Expire(ctx, rateKey, 1*time.Hour)
    }
    if count > 5 {
        return nil, status.Error(codes.ResourceExhausted, "too many verify code requests")
    }

    // ==================== 3. 生成验证码 ====================
    code := fmt.Sprintf("%06d", rand.Intn(1000000))

    // ==================== 4. 存储验证码（哈希后）— Redis ====================
    // 注：不存储明文验证码，防止 Redis 数据泄露后验证码被直接使用
    codeHash := sha256Hex(code) // SHA-256(验证码)
    codeKey := fmt.Sprintf("auth:verify_code:%s:%s", req.Target, req.Purpose)
    s.redis.Set(ctx, codeKey, codeHash, 5*time.Minute)

    // ==================== 5. 发送验证码（异步） ====================
    go func() {
        switch req.Type {
        case "sms":
            if err := s.smsProvider.Send(req.Target, fmt.Sprintf("您的验证码：%s，5分钟内有效", code)); err != nil {
                log.Error("send sms failed", "target", req.Target, "err", err)
            }
        case "email":
            if err := s.emailProvider.Send(req.Target, "验证码", fmt.Sprintf("您的验证码：%s，5分钟内有效", code)); err != nil {
                log.Error("send email failed", "target", req.Target, "err", err)
            }
        }
    }()

    return &pb.SendVerifyCodeResponse{
        Meta:          successMeta(ctx),
        ExpireSeconds: 300,
    }, nil
}
```

### 6. CheckVerifyCode — 校验验证码

```go
func (s *AuthService) CheckVerifyCode(ctx context.Context, req *pb.CheckVerifyCodeRequest) (*pb.CheckVerifyCodeResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.Target == "" || req.Code == "" || req.Purpose == "" {
        return nil, status.Error(codes.InvalidArgument, "target, code and purpose are required")
    }

    // ==================== 2. 读取验证码哈希 — Redis ====================
    codeKey := fmt.Sprintf("auth:verify_code:%s:%s", req.Target, req.Purpose)
    storedHash, err := s.redis.Get(ctx, codeKey).Result()
    if err == redis.Nil {
        return &pb.CheckVerifyCodeResponse{Meta: successMeta(ctx), IsValid: false}, nil
    }
    if err != nil {
        return nil, status.Error(codes.Internal, "redis query failed")
    }

    // ==================== 3. 比对验证码（哈希比较） ====================
    isValid := storedHash == sha256Hex(req.Code)
    if isValid {
        // 验证成功后立即删除，防止重复使用
        s.redis.Del(ctx, codeKey)
    }

    return &pb.CheckVerifyCodeResponse{
        Meta: successMeta(ctx), IsValid: isValid,
    }, nil
}
```

### 7. OAuthLogin — 第三方 OAuth 登录

```go
func (s *AuthService) OAuthLogin(ctx context.Context, req *pb.OAuthLoginRequest) (*pb.OAuthLoginResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.Provider == "" || req.AuthCode == "" {
        return nil, status.Error(codes.InvalidArgument, "provider and auth_code are required")
    }
    supportedProviders := map[string]bool{"wechat": true, "google": true, "apple": true, "github": true}
    if !supportedProviders[req.Provider] {
        return nil, status.Error(codes.InvalidArgument, "unsupported oauth provider")
    }
    if req.DeviceId == "" {
        return nil, status.Error(codes.InvalidArgument, "device_id is required")
    }

    // ==================== 2. 换取第三方 Token + 用户信息 ====================
    oauthUser, err := s.oauthProviders[req.Provider].ExchangeAndGetUser(ctx, req.AuthCode)
    if err != nil {
        return nil, status.Error(codes.Unauthenticated, "oauth exchange failed: "+err.Error())
    }

    // ==================== 3. 查找已绑定账号 — PgSQL ====================
    var userID string
    var isNewUser bool
    err = s.db.QueryRowContext(ctx,
        `SELECT user_id FROM oauth_accounts WHERE provider = $1 AND provider_id = $2`,
        req.Provider, oauthUser.ProviderID,
    ).Scan(&userID)

    if err == sql.ErrNoRows {
        // ==================== 4. 新用户：自动注册 ====================
        isNewUser = true
        // 4a. 创建用户 — RPC User.Register (内部调用)
        regResp, err := s.userClient.Register(ctx, &user_pb.RegisterRequest{
            Username:  fmt.Sprintf("%s_%s", req.Provider, oauthUser.ProviderID[:8]),
            Password:  generateRandomPassword(16),
            Nickname:  oauthUser.Nickname,
            AvatarUrl: oauthUser.AvatarURL,
            Platform:  req.Platform,
        })
        if err != nil {
            return nil, status.Error(codes.Internal, "auto register failed")
        }
        userID = regResp.UserId

        // 4b. 绑定 OAuth 账号 — PgSQL
        _, err = s.db.ExecContext(ctx,
            `INSERT INTO oauth_accounts (user_id, provider, provider_id, extra, created_at)
             VALUES ($1, $2, $3, $4, $5)`,
            userID, req.Provider, oauthUser.ProviderID, "{}", time.Now().UnixMilli(),
        )
        if err != nil {
            log.Error("bind oauth account failed", "err", err)
        }
    } else if err != nil {
        return nil, status.Error(codes.Internal, "db query failed")
    }

    // ==================== 5. 签发 Token（同 Login） ====================
    now := time.Now()
    accessToken, _ := s.jwt.Sign(jwt.Claims{
        UserID: userID, Platform: req.Platform, DeviceID: req.DeviceId,
        Exp: now.Add(2 * time.Hour).Unix(),
    })
    refreshToken, _ := s.jwt.Sign(jwt.Claims{
        UserID: userID, Platform: req.Platform, DeviceID: req.DeviceId,
        Type: "refresh", Exp: now.Add(7 * 24 * time.Hour).Unix(),
    })
    rtHash := sha256Hex(refreshToken)
    s.redis.Set(ctx, fmt.Sprintf("auth:refresh_token:%s", rtHash),
        fmt.Sprintf("%s:%s", userID, req.DeviceId), 7*24*time.Hour)

    // ==================== 6. 创建登录会话 — RPC ====================
    s.sessionClient.CreateLoginSession(ctx, &session_pb.CreateLoginSessionRequest{
        UserId: userID, Platform: req.Platform, DeviceId: req.DeviceId,
        DeviceName: req.DeviceName, Ip: extractClientIP(ctx),
    })

    // ==================== 7. 生产 Kafka 事件 ====================
    s.kafka.Produce(ctx, "auth.login.success", userID, &kafka_auth.LoginSuccessEvent{
        Header: buildEventHeader("auth", s.instanceID), UserId: userID,
        LoginType: kafka_auth.LOGIN_TYPE_THIRD_PARTY, Platform: req.Platform,
        DeviceId: req.DeviceId, LoginTime: now.UnixMilli(),
    })

    return &pb.OAuthLoginResponse{
        Meta: successMeta(ctx), UserId: userID, IsNewUser: isNewUser,
        TokenInfo: &pb.TokenInfo{
            AccessToken: accessToken, RefreshToken: refreshToken,
            ExpiresAt: now.Add(2 * time.Hour).Unix(), TokenType: "Bearer",
        },
    }, nil
}
```

### 8. ChangePassword — 修改密码

```go
func (s *AuthService) ChangePassword(ctx context.Context, req *pb.ChangePasswordRequest) (*pb.ChangePasswordResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id is required")
    }
    if req.OldPassword == "" || req.NewPassword == "" {
        return nil, status.Error(codes.InvalidArgument, "old_password and new_password are required")
    }
    if len(req.NewPassword) < 6 {
        return nil, status.Error(codes.InvalidArgument, "new_password must be at least 6 characters")
    }
    if req.OldPassword == req.NewPassword {
        return nil, status.Error(codes.InvalidArgument, "new password must differ from old password")
    }

    // ==================== 2. 验证旧密码 — PgSQL ====================
    var passwordHash string
    err := s.db.QueryRowContext(ctx,
        `SELECT password_hash FROM auth_credentials WHERE user_id = $1`, req.UserId,
    ).Scan(&passwordHash)
    if err == sql.ErrNoRows {
        return nil, status.Error(codes.NotFound, "user not found")
    }
    if err != nil {
        return nil, status.Error(codes.Internal, "db query failed")
    }

    if err := bcrypt.CompareHashAndPassword([]byte(passwordHash), []byte(req.OldPassword)); err != nil {
        return nil, status.Error(codes.Unauthenticated, "old password incorrect")
    }

    // ==================== 3. 更新密码 — PgSQL ====================
    newHash, _ := bcrypt.GenerateFromPassword([]byte(req.NewPassword), bcrypt.DefaultCost)
    now := time.Now().UnixMilli()
    _, err = s.db.ExecContext(ctx,
        `UPDATE auth_credentials SET password_hash = $1, updated_at = $2 WHERE user_id = $3`,
        string(newHash), now, req.UserId,
    )
    if err != nil {
        return nil, status.Error(codes.Internal, "update password failed")
    }

    // ==================== 4. 生产 Kafka 事件: auth.password.changed ====================
    event := &kafka_auth.PasswordChangedEvent{
        Header:     buildEventHeader("auth", s.instanceID),
        UserId:     req.UserId,
        ChangeTime: now,
        Ip:         extractClientIP(ctx),
    }
    s.kafka.Produce(ctx, "auth.password.changed", req.UserId, event)

    return &pb.ChangePasswordResponse{Meta: successMeta(ctx)}, nil
}
```

---

## 可观测性接入（OpenTelemetry）

> Auth 服务负责认证鉴权，需重点观测：登录成功/失败率、Token 签发/刷新速率、验证码发送频率、OAuth 调用延迟、密码变更频率。

### 第一步：服务启动时初始化 OTel SDK

```go
func main() {
    ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
    defer cancel()

    otelShutdown := observability.InitOpenTelemetry(ctx, "auth", "v1.0.0")
    defer otelShutdown(ctx)
    observability.SetupLogger("auth")
}
```

### 第二步：gRPC Server/Client Interceptor

```go
grpcServer := grpc.NewServer(
    grpc.ChainUnaryInterceptor(otelgrpc.UnaryServerInterceptor()),
    grpc.ChainStreamInterceptor(otelgrpc.StreamServerInterceptor()),
)
// Client — 调用 User.GetUser / Session.CreateLoginSession / Session.KickDevice
userConn := grpc.Dial("user:50051",
    grpc.WithChainUnaryInterceptor(otelgrpc.UnaryClientInterceptor()),
)
sessionConn := grpc.Dial("session:50051",
    grpc.WithChainUnaryInterceptor(otelgrpc.UnaryClientInterceptor()),
)
```

### 第三步：注册 Auth 专属业务指标

```go
var meter = otel.Meter("im-chat/auth")

var (
    // 登录结果分布
    loginResult, _ = meter.Int64Counter("auth.login_total",
        metric.WithDescription("登录尝试总数（按结果分类）"))

    // Token 签发计数
    tokenIssued, _ = meter.Int64Counter("auth.token_issued_total",
        metric.WithDescription("Token 签发总数"))

    // Token 刷新计数
    tokenRefreshed, _ = meter.Int64Counter("auth.token_refreshed_total",
        metric.WithDescription("Token 刷新总数"))

    // 验证码发送计数
    verifyCodeSent, _ = meter.Int64Counter("auth.verify_code_sent_total",
        metric.WithDescription("验证码发送总数"))

    // OAuth 外部调用延迟
    oauthCallLatency, _ = meter.Float64Histogram("auth.oauth_call_latency_ms",
        metric.WithDescription("OAuth 提供商 API 调用延迟"), metric.WithUnit("ms"))
)
```

在业务代码中埋点：

```go
// Login 中
loginResult.Add(ctx, 1, metric.WithAttributes(
    attribute.String("result", "success"),  // success / invalid_password / locked / not_found
    attribute.String("login_type", "password"),
))
tokenIssued.Add(ctx, 1)

// OAuthLogin 中 — 外部 API 调用
oauthStart := time.Now()
userInfo, err := s.oauthProvider.GetUserInfo(ctx, accessToken)
oauthCallLatency.Record(ctx, float64(time.Since(oauthStart).Milliseconds()),
    metric.WithAttributes(attribute.String("provider", provider)))

// SendVerifyCode 中
verifyCodeSent.Add(ctx, 1, metric.WithAttributes(
    attribute.String("channel", "sms"),  // sms / email
))
```

### 第四步：Kafka Producer 埋点 + 结构化 JSON 日志

发布 `auth.login.success` / `auth.password.changed` 等事件时注入 trace context。日志设置与所有服务一致。

### 接入要点总结

| 步骤 | 改动位置 | 效果 |
|------|---------|------|
| 1. `observability.InitOpenTelemetry` | main() | TracerProvider + MeterProvider + Runtime Metrics |
| 2. gRPC Interceptor | Server + Client | RPC 自动 trace |
| 3. 自定义业务指标 | Login/OAuthLogin/SendVerifyCode | 登录成功率、OAuth 延迟、验证码频率 |
| 4. Kafka + 日志 | 事件发布 + setupLogger | 链路追踪 + trace_id 日志 |
| 5. buildEventHeader 改造 | 辅助函数 | EventHeader 携带真实 trace context |
| 6. 基础设施指标 | main() | DB/Redis 连接池可观测 |
| 7. Span 错误记录 | `observability.RecordError` | 错误 trace 在 Tempo 可见 |

### 补充：buildEventHeader 改造

将 `buildEventHeader(source, instanceID)` 替换为 `observability.BuildEventHeader(ctx, source, instanceID)`，确保 `auth.login.success` / `auth.password.changed` 等 Kafka 事件携带真实 trace context。

### 补充：基础设施指标注册

```go
observability.RegisterDBPoolMetrics(db, "auth")
observability.RegisterRedisPoolMetrics(redisClient, "auth")
```

### 补充：Span 错误记录

在所有返回 error 的业务路径中记录 span 错误：

```go
func (s *AuthServer) Login(ctx context.Context, req *pb.LoginRequest) (*pb.LoginResponse, error) {
    ctx, span := otel.Tracer("auth").Start(ctx, "Login")
    defer span.End()

    result, err := s.doLogin(ctx, req)
    if err != nil {
        observability.RecordError(span, err)
        return nil, err
    }
    return result, nil
}
```
