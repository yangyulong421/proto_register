# ApiGateway API 网关服务 — RPC 接口实现伪代码

## 概述

ApiGateway 是整个 IM 系统面向客户端（App/Web）的**唯一 HTTP/HTTPS 入口**，负责认证校验、限流、请求路由、协议转换（HTTP→gRPC）以及若干数据聚合 API。
所有客户端请求必须经过 ApiGateway 中间件链：**RateLimit → Auth → Route → Handler**，网关本身**完全无状态**，可水平扩展。

**与 Connecte 的区别：**
- **ApiGateway** = 客户端 HTTP 网关（短连接，面向 App/Web 用户的 REST API）
- **Connecte** = 长连接网关（WebSocket/TCP，面向客户端实时通信）

**核心设计原则：**
- **无状态**：网关不持有任何业务状态，所有数据通过 RPC 代理到下游微服务，可任意水平扩缩容
- **SyncData 极致优化**：App 每次启动必调，使用 goroutine fan-out 并行调用 10+ 下游服务，单服务超时 2s，总超时 5s
- **BatchQuery 减少 RTT**：移动端弱网场景下，最多合并 10 个子请求为 1 次 HTTP 调用
- **中间件链**：所有请求经过 RateLimit → Auth → Route → Handler，任一环节失败立即短路返回
- **安全监控**：认证失败、限流触发、网关错误均投递 Kafka 事件供 Audit 服务审计分析

## 外部依赖

| 依赖类型 | 依赖项 | 用途 |
|---------|--------|------|
| PgSQL | **无** | 网关完全无状态，不持有任何数据库 |
| Redis | 限流计数 / 路由缓存 / 版本信息 / 维护模式 / 时间校准 | 高频配置与计数存储 |
| RPC | Auth.VerifyToken | 中间件层校验客户端 Token |
| RPC | User.GetUser | SyncData / GetInitData 获取用户信息 |
| RPC | User.GetUserSettings | SyncData / GetInitData 获取用户设置 |
| RPC | Conversation.GetUserConversationList | SyncData 获取会话列表 |
| RPC | Conversation.GetTotalUnread | SyncData / GetInitData 获取总未读数 |
| RPC | Relation.GetFriendList | SyncData 获取好友列表 |
| RPC | Group.GetUserGroups | SyncData 获取用户群组列表 |
| RPC | Presence.BatchGetPresence | SyncData 批量获取好友在线状态 |
| RPC | Notification.GetUnreadNotificationCount | SyncData / GetInitData 获取未读通知数 |
| RPC | Config.GetFeatureFlags | SyncData / GetInitData 获取功能开关 |
| RPC | OfflineQueue.DequeueSyncSignals | SyncData 拉取离线期间积累的信令 |
| RPC | OfflineQueue.GetSyncSignalCount | SyncData 获取离线信令计数 |
| RPC | Connecte.GetGatewayList | GetGatewayEndpoint 获取可用长连接网关列表 |
| Kafka | apigateway.request.log / apigateway.event.reported / apigateway.error / apigateway.auth.fail / apigateway.rate.limited | 事件通知下游 Audit 服务 |

## PgSQL 表结构

```
无 — ApiGateway 是完全无状态的 HTTP 网关，不持有任何数据库。
所有业务数据通过 RPC 代理到下游微服务获取。
请求日志、审计记录由 Audit 服务通过消费 Kafka 事件自行存储。
```

## Redis Key 设计

| Key 格式 | 类型 | 值 | TTL | 说明 |
|----------|------|-----|-----|------|
| `gw:rate_limit:{user_id}:{api}` | STRING | count（请求次数） | 1min | 用户维度 per-API 限流计数器，滑动窗口 1 分钟 |
| `gw:rate_limit_global:{ip}` | STRING | count（请求次数） | 1min | IP 维度全局限流计数器，防刷/防 DDoS |
| `gw:route_cache` | HASH | field=path, value=service_name | 5min | 路由映射缓存，path→下游服务名 |
| `gw:server_time_offset` | STRING | offset_ms（毫秒偏移量） | 无 TTL（定时校准） | 服务器时间校准偏移量，NTP 定时同步写入 |
| `gw:app_version:{platform}` | HASH | {min_version, latest_version, update_url, force_update} | 无 TTL（config.changed 更新） | 各平台客户端版本信息，CheckUpdate 使用 |
| `gw:maintenance_mode` | STRING | JSON {enabled, message, estimated_end} | 无 TTL（config.changed 更新） | 系统维护模式开关，开启后所有请求返回维护提示 |
| `gw:kafka:dedup:{event_id}` | STRING | "1" | 24h | Kafka 消费幂等去重 |

## 中间件链设计

```
客户端请求 → [RateLimit] → [Auth] → [Route] → [Handler] → 响应
              │              │          │          │
              │ 超限→429     │ 失败→401  │ 未匹配→404 │ 业务处理
              │ 投递Kafka    │ 投递Kafka │          │
              ↓              ↓          ↓          ↓
         rate.limited   auth.fail    error     request.log
```

| 中间件 | 职责 | 失败响应 |
|--------|------|---------|
| RateLimit | 检查用户级 + IP 级限流计数器，超限直接拒绝 | HTTP 429 Too Many Requests |
| Auth | 调用 Auth.VerifyToken 校验 Token，白名单路径免验证 | HTTP 401 Unauthorized |
| Route | 根据路由缓存 `gw:route_cache` 匹配下游服务 | HTTP 404 Not Found |
| Handler | 协议转换（HTTP→gRPC）并调用下游服务 | 透传下游错误码 |

---

## 接口实现

### 0. 中间件实现

#### RateLimit 中间件 — 限流

> 所有请求进入的第一道关卡。检查维护模式 → 检查 IP 级限流 → 检查用户级限流。  
> 使用 Redis INCR + TTL 实现固定窗口计数器，简单高效。

```go
func (m *RateLimitMiddleware) Handle(ctx context.Context, req *HTTPRequest, next Handler) (*HTTPResponse, error) {
    // ==================== 1. 维护模式检查 ====================
    // Key: gw:maintenance_mode  STRING  JSON
    maintenanceRaw, err := m.redis.Get(ctx, "gw:maintenance_mode").Result()
    if err == nil && maintenanceRaw != "" {
        var maintenance struct {
            Enabled      bool   `json:"enabled"`
            Message      string `json:"message"`
            EstimatedEnd int64  `json:"estimated_end"`
        }
        if json.Unmarshal([]byte(maintenanceRaw), &maintenance) == nil && maintenance.Enabled {
            return &HTTPResponse{
                StatusCode: 503,
                Body: map[string]interface{}{
                    "code":          -1,
                    "message":       maintenance.Message,
                    "estimated_end": maintenance.EstimatedEnd,
                },
            }, nil
        }
    }

    // ==================== 2. IP 级全局限流 ====================
    // Key: gw:rate_limit_global:{ip}  STRING  TTL: 1min
    // 默认每 IP 每分钟 600 次（可通过 config.changed 热更新）
    ipKey := fmt.Sprintf("gw:rate_limit_global:%s", req.ClientIP)
    ipCount, err := m.redis.Incr(ctx, ipKey).Result()
    if err != nil {
        log.Warn("IP 限流计数器 Redis 异常，放行请求", "ip", req.ClientIP, "err", err)
        // Redis 异常时放行，避免误杀（fail-open 策略）
    } else {
        if ipCount == 1 {
            // 首次写入，设置 1 分钟 TTL
            m.redis.Expire(ctx, ipKey, 1*time.Minute)
        }
        if ipCount > m.config.IPRateLimit { // 默认 600
            // 投递限流事件 → Kafka
            m.produceRateLimitedEvent(ctx, req, "ip", m.config.IPRateLimit, ipCount)
            return &HTTPResponse{
                StatusCode: 429,
                Body:       map[string]interface{}{"code": -1, "message": "rate limit exceeded, try again later"},
            }, nil
        }
    }

    // ==================== 3. 用户级 per-API 限流 ====================
    // 仅在已认证（user_id 非空）时检查
    // Key: gw:rate_limit:{user_id}:{api}  STRING  TTL: 1min
    if req.UserID != "" {
        apiPath := normalizeAPIPath(req.Path) // /v1/message/send → message.send
        userKey := fmt.Sprintf("gw:rate_limit:%s:%s", req.UserID, apiPath)
        userCount, err := m.redis.Incr(ctx, userKey).Result()
        if err != nil {
            log.Warn("用户限流计数器 Redis 异常，放行请求",
                "user_id", req.UserID, "api", apiPath, "err", err)
        } else {
            if userCount == 1 {
                m.redis.Expire(ctx, userKey, 1*time.Minute)
            }
            // 不同 API 有不同限流阈值，默认 60 次/分钟
            limit := m.getAPIRateLimit(apiPath)
            if userCount > limit {
                m.produceRateLimitedEvent(ctx, req, "user", limit, userCount)
                return &HTTPResponse{
                    StatusCode: 429,
                    Body:       map[string]interface{}{"code": -1, "message": "rate limit exceeded for this API"},
                }, nil
            }
        }
    }

    // ==================== 4. 放行，继续下一个中间件 ====================
    return next(ctx, req)
}

// produceRateLimitedEvent 投递限流触发事件到 Kafka
func (m *RateLimitMiddleware) produceRateLimitedEvent(ctx context.Context, req *HTTPRequest, limitType string, limit int64, current int64) {
    event := &kafka_gw.RateLimitedEvent{
        Header:       buildEventHeader("apigateway", m.instanceID),
        UserId:       req.UserID,
        ClientIp:     req.ClientIP,
        Path:         req.Path,
        RuleName:     fmt.Sprintf("%s_rate_limit", limitType),
        LimitType:    limitType,
        LimitValue:   limit,
        CurrentCount: current,
        WindowSec:    60,
        TriggerTime:  time.Now().UnixMilli(),
    }
    if err := m.kafka.Produce(ctx, "apigateway.rate.limited", req.ClientIP, event); err != nil {
        log.Error("投递 apigateway.rate.limited 事件失败", "err", err)
    }
}
```

#### Auth 中间件 — 认证校验

> 校验请求中的 Bearer Token，调用 Auth.VerifyToken RPC。白名单路径（如 GetServerTime、CheckUpdate）免验证。

```go
// authWhiteList 不需要认证的路径白名单
var authWhiteList = map[string]bool{
    "/v1/gateway/server-time":  true, // GetServerTime 免登录
    "/v1/gateway/check-update": true, // CheckUpdate 免登录
    "/health":                  true, // 健康检查
    "/ready":                   true, // 就绪检查
}

func (m *AuthMiddleware) Handle(ctx context.Context, req *HTTPRequest, next Handler) (*HTTPResponse, error) {
    // ==================== 1. 白名单路径放行 ====================
    if authWhiteList[req.Path] {
        return next(ctx, req)
    }

    // ==================== 2. 提取 Token ====================
    token := extractBearerToken(req.Headers["Authorization"])
    if token == "" {
        m.produceAuthFailEvent(ctx, req, "missing_token", "Authorization header is missing or invalid")
        return &HTTPResponse{
            StatusCode: 401,
            Body:       map[string]interface{}{"code": -1, "message": "unauthorized: missing token"},
        }, nil
    }

    // ==================== 3. 调用 Auth.VerifyToken RPC ====================
    verifyResp, err := m.authClient.VerifyToken(ctx, &auth_pb.VerifyTokenRequest{
        Token: token,
    })
    if err != nil {
        // RPC 调用失败
        if st, ok := status.FromError(err); ok {
            switch st.Code() {
            case codes.Unauthenticated:
                m.produceAuthFailEvent(ctx, req, "invalid_token", st.Message())
                return &HTTPResponse{
                    StatusCode: 401,
                    Body:       map[string]interface{}{"code": -1, "message": "unauthorized: " + st.Message()},
                }, nil
            case codes.PermissionDenied:
                m.produceAuthFailEvent(ctx, req, "token_expired", st.Message())
                return &HTTPResponse{
                    StatusCode: 401,
                    Body:       map[string]interface{}{"code": -1, "message": "unauthorized: token expired"},
                }, nil
            default:
                // 下游异常，投递 error 事件
                m.produceGatewayErrorEvent(ctx, req, "auth", 500, "auth_service_error", st.Message())
                return &HTTPResponse{
                    StatusCode: 500,
                    Body:       map[string]interface{}{"code": -1, "message": "internal error"},
                }, nil
            }
        }
        m.produceGatewayErrorEvent(ctx, req, "auth", 500, "auth_rpc_failed", err.Error())
        return &HTTPResponse{
            StatusCode: 500,
            Body:       map[string]interface{}{"code": -1, "message": "internal error"},
        }, nil
    }

    // ==================== 4. 注入用户信息到上下文 ====================
    req.UserID = verifyResp.UserId
    req.Platform = verifyResp.Platform
    req.DeviceID = verifyResp.DeviceId
    ctx = context.WithValue(ctx, "user_id", verifyResp.UserId)
    ctx = context.WithValue(ctx, "platform", verifyResp.Platform)

    // ==================== 5. 继续下一个中间件 ====================
    return next(ctx, req)
}

// produceAuthFailEvent 投递认证失败事件到 Kafka
func (m *AuthMiddleware) produceAuthFailEvent(ctx context.Context, req *HTTPRequest, errorCode, errorMsg string) {
    event := &kafka_gw.AuthFailEvent{
        Header:    buildEventHeader("apigateway", m.instanceID),
        ClientIp:  req.ClientIP,
        Path:      req.Path,
        UserAgent: req.Headers["User-Agent"],
        Token:     maskToken(extractBearerToken(req.Headers["Authorization"])), // 脱敏
        ErrorCode: errorCode,
        ErrorMsg:  errorMsg,
        AuthType:  "bearer",
        FailTime:  time.Now().UnixMilli(),
    }
    if err := m.kafka.Produce(ctx, "apigateway.auth.fail", req.ClientIP, event); err != nil {
        log.Error("投递 apigateway.auth.fail 事件失败", "err", err)
    }
}

// produceGatewayErrorEvent 投递网关错误事件到 Kafka
func (m *AuthMiddleware) produceGatewayErrorEvent(ctx context.Context, req *HTTPRequest, upstream string, statusCode int, errCode, errMsg string) {
    event := &kafka_gw.GatewayErrorEvent{
        Header:     buildEventHeader("apigateway", m.instanceID),
        RequestId:  req.RequestID,
        Path:       req.Path,
        Upstream:   upstream,
        StatusCode: int32(statusCode),
        ErrorCode:  errCode,
        ErrorMsg:   errMsg,
        ErrorType:  "upstream_error",
        ClientIp:   req.ClientIP,
        ErrorTime:  time.Now().UnixMilli(),
    }
    if err := m.kafka.Produce(ctx, "apigateway.error", req.RequestID, event); err != nil {
        log.Error("投递 apigateway.error 事件失败", "err", err)
    }
}
```

---

### 1. GetGatewayEndpoint — 获取长连接网关端点

> 客户端建立长连接前调用此接口，返回可用的 WebSocket/TCP 网关地址。  
> 流程：参数校验 → 查询可用网关列表（通过 Connecte 服务）→ 就近/负载均衡选择 → 签发连接令牌 → 返回。

```go
func (s *ApiGatewayService) GetGatewayEndpoint(ctx context.Context, req *pb.GetGatewayEndpointRequest) (*pb.GetGatewayEndpointResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id 不能为空")
    }
    if req.Platform == common.PLATFORM_TYPE_UNSPECIFIED {
        return nil, status.Error(codes.InvalidArgument, "platform 不能为空")
    }
    if req.ClientVersion == "" {
        return nil, status.Error(codes.InvalidArgument, "client_version 不能为空")
    }

    // ==================== 2. 查询可用网关节点 — 通过 Connecte 服务 RPC ====================
    // 注：不直接读取 Connecte 的 Redis key（conn:gateway_list / conn:gateway:*）
    //     跨服务 Redis 直连违反服务边界原则，Connecte 修改 key 格式时 ApiGateway 会静默失败
    gatewayResp, err := s.connecteClient.GetGatewayNodes(ctx, &connecte_pb.GetGatewayNodesRequest{})
    if err != nil || len(gatewayResp.Nodes) == 0 {
        log.Error("查询可用网关节点失败或无可用节点",
            "user_id", req.UserId, "err", err)
        return nil, status.Error(codes.Unavailable, "no available gateway endpoints")
    }

    // ==================== 3. 选择负载最低的网关节点 ====================
    type gatewayInfo struct {
        ID   string
        Addr string
        Port string
    }
    var availableGateways []gatewayInfo

    for _, node := range gatewayResp.Nodes {
        if node.Status != "active" {
            continue
        }
        availableGateways = append(availableGateways, gatewayInfo{
            ID:   node.NodeId,
            Addr: node.Addr,
            Port: node.Port,
        })
    }

    if len(availableGateways) == 0 {
        return nil, status.Error(codes.Unavailable, "no active gateway endpoints")
    }

    // ==================== 4. 就近选择（如果有 client_ip） ====================
    // 简化实现：选择列表中第一个（连接数最少的节点）
    // 生产环境可基于 IP 地理位置做就近接入
    primary := availableGateways[0]

    // ==================== 5. 签发连接令牌 ====================
    // 连接令牌用于客户端连接 WebSocket/TCP 时的鉴权，有效期 5 分钟
    now := time.Now()
    connToken, err := s.jwt.Sign(jwt.Claims{
        UserID:   req.UserId,
        Platform: req.Platform,
        Type:     "conn",
        Exp:      now.Add(5 * time.Minute).Unix(),
    })
    if err != nil {
        log.Error("签发连接令牌失败", "user_id", req.UserId, "err", err)
        return nil, status.Error(codes.Internal, "sign connection token failed")
    }

    // ==================== 6. 组装 WebSocket / TCP URL ====================
    wsURL := fmt.Sprintf("wss://%s:%s/ws?token=%s", primary.Addr, primary.Port, connToken)
    tcpURL := fmt.Sprintf("tcp://%s:%s", primary.Addr, primary.Port)

    // 备用地址
    var backupURLs []string
    for i := 1; i < len(availableGateways); i++ {
        gw := availableGateways[i]
        backupURLs = append(backupURLs, fmt.Sprintf("wss://%s:%s/ws", gw.Addr, gw.Port))
    }

    // ==================== 7. 投递请求日志 — Kafka ====================
    s.produceRequestLog(ctx, req.UserId, "GetGatewayEndpoint", 200, time.Since(now).Milliseconds())

    // ==================== 8. 返回 ====================
    return &pb.GetGatewayEndpointResponse{
        Meta:       successMeta(ctx),
        WsUrl:      wsURL,
        TcpUrl:     tcpURL,
        BackupUrls: backupURLs,
        Token:      connToken,
        ExpireTime: now.Add(5 * time.Minute).UnixMilli(),
    }, nil
}
```

---

### 2. SyncData — 综合数据同步（App 启动核心路径）

> **最关键的聚合接口**，客户端每次启动/重连时调用。  
> 使用 goroutine fan-out 并行调用 10+ 下游服务，每个服务调用独立超时 2s，总超时 5s。  
> 部分服务失败不阻塞整体返回，降级为空数据并标记 partial_failure。

> **⚠️ MED-04 熔断器模式（Circuit Breaker）**：  
> 每个下游服务维护独立的熔断器实例。连续 5 次 RPC 失败后触发 **OPEN 状态**（30s 内直接跳过该服务），
> 30s 后进入 **HALF-OPEN** 状态放行 1 个探针请求，成功则恢复，失败则继续熔断。  
> **核心服务 vs 非核心服务**：
> - **核心服务**（Conversation/Relation/OfflineQueue）：熔断时返回空数据 + partial=true
> - **非核心服务**（Config.GetFeatureFlags/Notification.GetUnread）：熔断时直接跳过，不影响整体响应

```go
// 服务级熔断器（启动时初始化，所有 SyncData 调用共享）
type ServiceCircuitBreaker struct {
    breakers map[string]*circuitbreaker.CircuitBreaker // service_name → breaker
}

// 每个下游服务的熔断器配置
var syncDataBreakers = NewServiceCircuitBreaker(map[string]circuitbreaker.Config{
    "conversation": {MaxFailures: 5, OpenDuration: 30 * time.Second},  // 核心
    "relation":     {MaxFailures: 5, OpenDuration: 30 * time.Second},  // 核心
    "group":        {MaxFailures: 5, OpenDuration: 30 * time.Second},  // 核心
    "offline_queue":{MaxFailures: 5, OpenDuration: 30 * time.Second},  // 核心
    "user":         {MaxFailures: 5, OpenDuration: 30 * time.Second},  // 核心
    "notification": {MaxFailures: 10, OpenDuration: 60 * time.Second}, // 非核心，容忍更多失败
    "config":       {MaxFailures: 10, OpenDuration: 60 * time.Second}, // 非核心
    "presence":     {MaxFailures: 10, OpenDuration: 60 * time.Second}, // 非核心
})

```go
func (s *ApiGatewayService) SyncData(ctx context.Context, req *pb.SyncDataRequest) (*pb.SyncDataResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id 不能为空")
    }
    // last_sync_time 为 0 表示全量同步（新设备/清缓存）
    fullSync := req.LastSyncTime == 0

    startTime := time.Now()

    // ==================== 2. 设置总超时 5s ====================
    syncCtx, syncCancel := context.WithTimeout(ctx, 5*time.Second)
    defer syncCancel()

    // ==================== 3. goroutine fan-out — 并行调用所有下游服务 ====================
    // 使用 errgroup 管理并发，每个 goroutine 独立 2s 超时
    // 任何单个服务失败不影响其他服务返回

    type syncResult struct {
        mu                    sync.Mutex
        conversationsData     []byte         // 会话增量数据
        relationsData         []byte         // 好友列表数据
        groupsData            []byte         // 群组数据
        notificationsData     []byte         // 通知数据
        userInfo              []byte         // 用户信息
        userSettings          []byte         // 用户设置
        totalUnread           int64          // 总未读数
        unreadNotificationCnt int64          // 未读通知数
        featureFlags          []byte         // 功能开关
        presenceData          []byte         // 好友在线状态
        offlineSignals        []byte         // 离线信令
        offlineSignalCount    int64          // 离线信令数
        partialFailures       []string       // 部分失败的服务列表
    }
    result := &syncResult{}

    var wg sync.WaitGroup

    // ---- 3a. 会话列表 — Conversation.GetUserConversationList ----
    wg.Add(1)
    go func() {
        defer wg.Done()
        callCtx, cancel := context.WithTimeout(syncCtx, 2*time.Second)
        defer cancel()

        resp, err := s.conversationClient.GetUserConversationList(callCtx, &conv_pb.GetUserConversationListRequest{
            UserId:      req.UserId,
            UpdatedAfter: req.ConversationVersion, // 增量：只拉取版本号之后变更的会话
        })
        if err != nil {
            log.Warn("SyncData: 获取会话列表失败，降级为空",
                "user_id", req.UserId, "err", err)
            result.mu.Lock()
            result.partialFailures = append(result.partialFailures, "conversation_list")
            result.mu.Unlock()
            return
        }
        data, _ := proto.Marshal(resp)
        result.mu.Lock()
        result.conversationsData = data
        result.mu.Unlock()
    }()

    // ---- 3b. 总未读数 — Conversation.GetTotalUnread ----
    wg.Add(1)
    go func() {
        defer wg.Done()
        callCtx, cancel := context.WithTimeout(syncCtx, 2*time.Second)
        defer cancel()

        resp, err := s.conversationClient.GetTotalUnread(callCtx, &conv_pb.GetTotalUnreadRequest{
            UserId: req.UserId,
        })
        if err != nil {
            log.Warn("SyncData: 获取总未读数失败，降级为 0",
                "user_id", req.UserId, "err", err)
            result.mu.Lock()
            result.partialFailures = append(result.partialFailures, "total_unread")
            result.mu.Unlock()
            return
        }
        result.mu.Lock()
        result.totalUnread = resp.TotalUnread
        result.mu.Unlock()
    }()

    // ---- 3c. 好友列表 — Relation.GetFriendList ----
    wg.Add(1)
    go func() {
        defer wg.Done()
        callCtx, cancel := context.WithTimeout(syncCtx, 2*time.Second)
        defer cancel()

        resp, err := s.relationClient.GetFriendList(callCtx, &relation_pb.GetFriendListRequest{
            UserId:       req.UserId,
            UpdatedAfter: req.RelationVersion, // 增量：只拉取版本号之后变更的好友
        })
        if err != nil {
            log.Warn("SyncData: 获取好友列表失败，降级为空",
                "user_id", req.UserId, "err", err)
            result.mu.Lock()
            result.partialFailures = append(result.partialFailures, "friend_list")
            result.mu.Unlock()
            return
        }
        data, _ := proto.Marshal(resp)
        result.mu.Lock()
        result.relationsData = data
        result.mu.Unlock()
    }()

    // ---- 3d. 用户群组列表 — Group.GetUserGroups ----
    wg.Add(1)
    go func() {
        defer wg.Done()
        callCtx, cancel := context.WithTimeout(syncCtx, 2*time.Second)
        defer cancel()

        resp, err := s.groupClient.GetUserGroups(callCtx, &group_pb.GetUserGroupsRequest{
            UserId:       req.UserId,
            UpdatedAfter: req.GroupVersion, // 增量：只拉取版本号之后变更的群组
        })
        if err != nil {
            log.Warn("SyncData: 获取用户群组失败，降级为空",
                "user_id", req.UserId, "err", err)
            result.mu.Lock()
            result.partialFailures = append(result.partialFailures, "user_groups")
            result.mu.Unlock()
            return
        }
        data, _ := proto.Marshal(resp)
        result.mu.Lock()
        result.groupsData = data
        result.mu.Unlock()
    }()

    // ---- 3e. 用户信息 — User.GetUser ----
    wg.Add(1)
    go func() {
        defer wg.Done()
        callCtx, cancel := context.WithTimeout(syncCtx, 2*time.Second)
        defer cancel()

        resp, err := s.userClient.GetUser(callCtx, &user_pb.GetUserRequest{
            UserId: req.UserId,
        })
        if err != nil {
            log.Warn("SyncData: 获取用户信息失败，降级为空",
                "user_id", req.UserId, "err", err)
            result.mu.Lock()
            result.partialFailures = append(result.partialFailures, "user_info")
            result.mu.Unlock()
            return
        }
        data, _ := proto.Marshal(resp.User)
        result.mu.Lock()
        result.userInfo = data
        result.mu.Unlock()
    }()

    // ---- 3f. 用户设置 — User.GetUserSettings ----
    wg.Add(1)
    go func() {
        defer wg.Done()
        callCtx, cancel := context.WithTimeout(syncCtx, 2*time.Second)
        defer cancel()

        resp, err := s.userClient.GetUserSettings(callCtx, &user_pb.GetUserSettingsRequest{
            UserId: req.UserId,
        })
        if err != nil {
            log.Warn("SyncData: 获取用户设置失败，降级为空",
                "user_id", req.UserId, "err", err)
            result.mu.Lock()
            result.partialFailures = append(result.partialFailures, "user_settings")
            result.mu.Unlock()
            return
        }
        data, _ := proto.Marshal(resp.Settings)
        result.mu.Lock()
        result.userSettings = data
        result.mu.Unlock()
    }()

    // ---- 3g. 好友在线状态 — Presence.BatchGetPresence ----
    // 注意：需要先拿到好友列表才能批量查在线，但为了并行化，这里直接使用缓存的好友列表
    // 生产环境可拆为两阶段：第一阶段拉好友列表，第二阶段拉在线状态
    wg.Add(1)
    go func() {
        defer wg.Done()
        callCtx, cancel := context.WithTimeout(syncCtx, 2*time.Second)
        defer cancel()

        // 先快速获取好友 ID 列表（轻量调用）
        friendResp, err := s.relationClient.GetFriendList(callCtx, &relation_pb.GetFriendListRequest{
            UserId:  req.UserId,
            IdsOnly: true, // 仅返回 ID 列表，不返回详情
        })
        if err != nil || len(friendResp.FriendIds) == 0 {
            return // 好友列表获取失败或无好友，跳过
        }

        // 批量查在线状态（最多查 200 个好友）
        queryIDs := friendResp.FriendIds
        if len(queryIDs) > 200 {
            queryIDs = queryIDs[:200]
        }

        presenceResp, err := s.presenceClient.BatchGetPresence(callCtx, &presence_pb.BatchGetPresenceRequest{
            UserIds: queryIDs,
        })
        if err != nil {
            log.Warn("SyncData: 批量获取好友在线状态失败，降级为空",
                "user_id", req.UserId, "err", err)
            result.mu.Lock()
            result.partialFailures = append(result.partialFailures, "presence")
            result.mu.Unlock()
            return
        }
        data, _ := proto.Marshal(presenceResp)
        result.mu.Lock()
        result.presenceData = data
        result.mu.Unlock()
    }()

    // ---- 3h. 未读通知数 — Notification.GetUnreadNotificationCount ----
    wg.Add(1)
    go func() {
        defer wg.Done()
        callCtx, cancel := context.WithTimeout(syncCtx, 2*time.Second)
        defer cancel()

        resp, err := s.notificationClient.GetUnreadNotificationCount(callCtx, &notification_pb.GetUnreadNotificationCountRequest{
            UserId: req.UserId,
        })
        if err != nil {
            log.Warn("SyncData: 获取未读通知数失败，降级为 0",
                "user_id", req.UserId, "err", err)
            result.mu.Lock()
            result.partialFailures = append(result.partialFailures, "notification_count")
            result.mu.Unlock()
            return
        }
        result.mu.Lock()
        result.unreadNotificationCnt = resp.UnreadCount
        result.mu.Unlock()
    }()

    // ---- 3i. 功能开关 — Config.GetFeatureFlags ----
    wg.Add(1)
    go func() {
        defer wg.Done()
        callCtx, cancel := context.WithTimeout(syncCtx, 2*time.Second)
        defer cancel()

        resp, err := s.configClient.GetFeatureFlags(callCtx, &config_pb.GetFeatureFlagsRequest{
            UserId: req.UserId, // 传入 user_id 用于灰度判断
        })
        if err != nil {
            log.Warn("SyncData: 获取功能开关失败，降级为空",
                "user_id", req.UserId, "err", err)
            result.mu.Lock()
            result.partialFailures = append(result.partialFailures, "feature_flags")
            result.mu.Unlock()
            return
        }
        data, _ := proto.Marshal(resp)
        result.mu.Lock()
        result.featureFlags = data
        result.mu.Unlock()
    }()

    // ---- 3j. 离线信令 — OfflineQueue.DequeueSyncSignals + GetSyncSignalCount ----
    wg.Add(1)
    go func() {
        defer wg.Done()
        callCtx, cancel := context.WithTimeout(syncCtx, 2*time.Second)
        defer cancel()

        // 先获取离线信令计数
        countResp, err := s.offlineQueueClient.GetSyncSignalCount(callCtx, &oq_pb.GetSyncSignalCountRequest{
            UserId: req.UserId,
        })
        if err != nil {
            log.Warn("SyncData: 获取离线信令计数失败",
                "user_id", req.UserId, "err", err)
            result.mu.Lock()
            result.partialFailures = append(result.partialFailures, "offline_signals")
            result.mu.Unlock()
            return
        }
        result.mu.Lock()
        result.offlineSignalCount = countResp.Count
        result.mu.Unlock()

        if countResp.Count == 0 {
            return // 无离线信令，跳过
        }

        // 拉取离线信令（最多拉取 500 条，超出的由客户端后续分页拉取）
        dequeueResp, err := s.offlineQueueClient.DequeueSyncSignals(callCtx, &oq_pb.DequeueSyncSignalsRequest{
            UserId:   req.UserId,
            MaxCount: 500,
        })
        if err != nil {
            log.Warn("SyncData: 拉取离线信令失败",
                "user_id", req.UserId, "err", err)
            result.mu.Lock()
            result.partialFailures = append(result.partialFailures, "offline_signals")
            result.mu.Unlock()
            return
        }
        data, _ := proto.Marshal(dequeueResp)
        result.mu.Lock()
        result.offlineSignals = data
        result.mu.Unlock()
    }()

    // ==================== 4. 等待所有 goroutine 完成 ====================
    wg.Wait()

    // ==================== 5. 检查总超时 ====================
    if syncCtx.Err() == context.DeadlineExceeded {
        log.Warn("SyncData 总超时 5s，部分数据可能不完整",
            "user_id", req.UserId, "partial_failures", result.partialFailures)
    }

    // ==================== 6. 组装响应 ====================
    resp := &pb.SyncDataResponse{
        Meta:              successMeta(ctx),
        FullSync:          fullSync,
        ConversationsData: result.conversationsData,
        RelationsData:     result.relationsData,
        GroupsData:        result.groupsData,
        NotificationsData: result.notificationsData,
        ServerTime:        time.Now().UnixMilli(),
    }

    // 如果有部分失败，在 meta 中标记
    if len(result.partialFailures) > 0 {
        resp.Meta.Code = 1 // 1 = 部分成功
        resp.Meta.Message = fmt.Sprintf("partial_success: %s failed", strings.Join(result.partialFailures, ","))
        log.Warn("SyncData 部分服务调用失败",
            "user_id", req.UserId, "failures", result.partialFailures,
            "duration_ms", time.Since(startTime).Milliseconds())
    }

    // ==================== 7. 投递请求日志 — Kafka ====================
    s.produceRequestLog(ctx, req.UserId, "SyncData", 200, time.Since(startTime).Milliseconds())

    // ==================== 8. 返回 ====================
    log.Info("SyncData 完成",
        "user_id", req.UserId, "full_sync", fullSync,
        "partial_failures", len(result.partialFailures),
        "duration_ms", time.Since(startTime).Milliseconds())
    return resp, nil
}
```

---

### 3. GetInitData — 获取轻量初始化数据

> 客户端登录成功后调用，返回用户基础信息、设置、未读数、功能开关等轻量数据。  
> 比 SyncData 更轻量，不包含会话列表/好友列表等大数据量接口。

```go
func (s *ApiGatewayService) GetInitData(ctx context.Context, req *pb.GetInitDataRequest) (*pb.GetInitDataResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id 不能为空")
    }
    if req.Platform == common.PLATFORM_TYPE_UNSPECIFIED {
        return nil, status.Error(codes.InvalidArgument, "platform 不能为空")
    }

    startTime := time.Now()

    // ==================== 2. 设置总超时 3s（轻量接口，超时更短） ====================
    initCtx, initCancel := context.WithTimeout(ctx, 3*time.Second)
    defer initCancel()

    // ==================== 3. 并行调用下游服务 ====================
    var (
        wg              sync.WaitGroup
        mu              sync.Mutex
        userInfo        []byte
        userSettings    []byte
        totalUnread     int64
        friendReqCount  int32
        partialFailures []string
    )

    // ---- 3a. 用户信息 — User.GetUser ----
    wg.Add(1)
    go func() {
        defer wg.Done()
        callCtx, cancel := context.WithTimeout(initCtx, 2*time.Second)
        defer cancel()

        resp, err := s.userClient.GetUser(callCtx, &user_pb.GetUserRequest{UserId: req.UserId})
        if err != nil {
            log.Warn("GetInitData: 获取用户信息失败", "user_id", req.UserId, "err", err)
            mu.Lock()
            partialFailures = append(partialFailures, "user_info")
            mu.Unlock()
            return
        }
        data, _ := proto.Marshal(resp.User)
        mu.Lock()
        userInfo = data
        mu.Unlock()
    }()

    // ---- 3b. 用户设置 — User.GetUserSettings ----
    wg.Add(1)
    go func() {
        defer wg.Done()
        callCtx, cancel := context.WithTimeout(initCtx, 2*time.Second)
        defer cancel()

        resp, err := s.userClient.GetUserSettings(callCtx, &user_pb.GetUserSettingsRequest{UserId: req.UserId})
        if err != nil {
            log.Warn("GetInitData: 获取用户设置失败", "user_id", req.UserId, "err", err)
            mu.Lock()
            partialFailures = append(partialFailures, "user_settings")
            mu.Unlock()
            return
        }
        data, _ := proto.Marshal(resp.Settings)
        mu.Lock()
        userSettings = data
        mu.Unlock()
    }()

    // ---- 3c. 总未读数 — Conversation.GetTotalUnread ----
    wg.Add(1)
    go func() {
        defer wg.Done()
        callCtx, cancel := context.WithTimeout(initCtx, 2*time.Second)
        defer cancel()

        resp, err := s.conversationClient.GetTotalUnread(callCtx, &conv_pb.GetTotalUnreadRequest{UserId: req.UserId})
        if err != nil {
            log.Warn("GetInitData: 获取总未读数失败", "user_id", req.UserId, "err", err)
            mu.Lock()
            partialFailures = append(partialFailures, "total_unread")
            mu.Unlock()
            return
        }
        mu.Lock()
        totalUnread = resp.TotalUnread
        mu.Unlock()
    }()

    // ---- 3d. 待处理好友请求数 — Relation.GetPendingFriendRequestCount ----
    wg.Add(1)
    go func() {
        defer wg.Done()
        callCtx, cancel := context.WithTimeout(initCtx, 2*time.Second)
        defer cancel()

        resp, err := s.relationClient.GetPendingFriendRequestCount(callCtx, &relation_pb.GetPendingFriendRequestCountRequest{
            UserId: req.UserId,
        })
        if err != nil {
            log.Warn("GetInitData: 获取好友请求数失败", "user_id", req.UserId, "err", err)
            mu.Lock()
            partialFailures = append(partialFailures, "friend_request_count")
            mu.Unlock()
            return
        }
        mu.Lock()
        friendReqCount = resp.Count
        mu.Unlock()
    }()

    // ==================== 4. 等待所有 goroutine 完成 ====================
    wg.Wait()

    // ==================== 5. 组装响应 ====================
    resp := &pb.GetInitDataResponse{
        Meta:               successMeta(ctx),
        UserInfo:           userInfo,
        UserSettings:       userSettings,
        TotalUnread:        totalUnread,
        FriendRequestCount: friendReqCount,
        ServerTime:         time.Now().UnixMilli(),
    }

    if len(partialFailures) > 0 {
        resp.Meta.Code = 1
        resp.Meta.Message = fmt.Sprintf("partial_success: %s failed", strings.Join(partialFailures, ","))
    }

    // ==================== 6. 投递请求日志 — Kafka ====================
    s.produceRequestLog(ctx, req.UserId, "GetInitData", 200, time.Since(startTime).Milliseconds())

    return resp, nil
}
```

---

### 4. BatchQuery — 批量查询（减少移动端 RTT）

> 移动端在弱网环境下，合并多次查询为一次 HTTP 请求。  
> 最多支持 10 个子请求，每个子请求并行调用对应下游服务。

```go
func (s *ApiGatewayService) BatchQuery(ctx context.Context, req *pb.BatchQueryRequest) (*pb.BatchQueryResponse, error) {
    // ==================== 1. 参数校验 ====================
    totalQueries := len(req.UserIds) + len(req.GroupIds) + len(req.ConversationIds)
    if totalQueries == 0 {
        return nil, status.Error(codes.InvalidArgument, "至少提供一个查询 ID")
    }
    if totalQueries > 10 {
        return nil, status.Error(codes.InvalidArgument, "批量查询最多支持 10 个 ID（user_ids + group_ids + conversation_ids 总计）")
    }
    // 单类 ID 数量限制
    if len(req.UserIds) > 10 {
        return nil, status.Error(codes.InvalidArgument, "user_ids 最多 10 个")
    }
    if len(req.GroupIds) > 10 {
        return nil, status.Error(codes.InvalidArgument, "group_ids 最多 10 个")
    }
    if len(req.ConversationIds) > 10 {
        return nil, status.Error(codes.InvalidArgument, "conversation_ids 最多 10 个")
    }

    startTime := time.Now()

    // ==================== 2. 设置总超时 3s ====================
    batchCtx, batchCancel := context.WithTimeout(ctx, 3*time.Second)
    defer batchCancel()

    // ==================== 3. 并行查询 ====================
    var (
        wg     sync.WaitGroup
        mu     sync.Mutex
        users  = make(map[string][]byte) // key=user_id, value=序列化的用户信息
        groups = make(map[string][]byte) // key=group_id, value=序列化的群信息
        convs  = make(map[string][]byte) // key=conversation_id, value=序列化的会话信息
    )

    // ---- 3a. 批量查询用户信息 ----
    if len(req.UserIds) > 0 {
        wg.Add(1)
        go func() {
            defer wg.Done()
            callCtx, cancel := context.WithTimeout(batchCtx, 2*time.Second)
            defer cancel()

            for _, uid := range req.UserIds {
                resp, err := s.userClient.GetUser(callCtx, &user_pb.GetUserRequest{UserId: uid})
                if err != nil {
                    log.Warn("BatchQuery: 获取用户信息失败", "user_id", uid, "err", err)
                    continue
                }
                data, _ := proto.Marshal(resp.User)
                mu.Lock()
                users[uid] = data
                mu.Unlock()
            }
        }()
    }

    // ---- 3b. 批量查询群组信息 ----
    if len(req.GroupIds) > 0 {
        wg.Add(1)
        go func() {
            defer wg.Done()
            callCtx, cancel := context.WithTimeout(batchCtx, 2*time.Second)
            defer cancel()

            for _, gid := range req.GroupIds {
                resp, err := s.groupClient.GetGroup(callCtx, &group_pb.GetGroupRequest{GroupId: gid})
                if err != nil {
                    log.Warn("BatchQuery: 获取群组信息失败", "group_id", gid, "err", err)
                    continue
                }
                data, _ := proto.Marshal(resp.Group)
                mu.Lock()
                groups[gid] = data
                mu.Unlock()
            }
        }()
    }

    // ---- 3c. 批量查询会话信息 ----
    if len(req.ConversationIds) > 0 {
        wg.Add(1)
        go func() {
            defer wg.Done()
            callCtx, cancel := context.WithTimeout(batchCtx, 2*time.Second)
            defer cancel()

            for _, cid := range req.ConversationIds {
                resp, err := s.conversationClient.GetConversation(callCtx, &conv_pb.GetConversationRequest{
                    ConversationId: cid,
                })
                if err != nil {
                    log.Warn("BatchQuery: 获取会话信息失败", "conversation_id", cid, "err", err)
                    continue
                }
                data, _ := proto.Marshal(resp.Conversation)
                mu.Lock()
                convs[cid] = data
                mu.Unlock()
            }
        }()
    }

    // ==================== 4. 等待所有查询完成 ====================
    wg.Wait()

    // ==================== 5. 投递请求日志 — Kafka ====================
    s.produceRequestLog(ctx, "", "BatchQuery", 200, time.Since(startTime).Milliseconds())

    // ==================== 6. 返回 ====================
    return &pb.BatchQueryResponse{
        Meta:          successMeta(ctx),
        Users:         users,
        Groups:        groups,
        Conversations: convs,
    }, nil
}
```

---

### 5. ReportEvent — 客户端事件上报

> 客户端上报运营分析事件（app_open、app_background、page_view、crash 等）。  
> 轻量接口，仅做参数校验后直接投递 Kafka，不做任何持久化操作。

```go
func (s *ApiGatewayService) ReportEvent(ctx context.Context, req *pb.ReportEventRequest) (*pb.ReportEventResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id 不能为空")
    }
    if req.EventType == "" {
        return nil, status.Error(codes.InvalidArgument, "event_type 不能为空")
    }
    if req.Platform == common.PLATFORM_TYPE_UNSPECIFIED {
        return nil, status.Error(codes.InvalidArgument, "platform 不能为空")
    }

    // 事件类型白名单校验
    allowedEventTypes := map[string]bool{
        "app_open":       true,
        "app_close":      true,
        "app_background": true,
        "app_foreground":  true,
        "page_view":      true,
        "crash":          true,
        "performance":    true,
        "custom":         true,
    }
    if !allowedEventTypes[req.EventType] {
        return nil, status.Error(codes.InvalidArgument, "不支持的 event_type: "+req.EventType)
    }

    // event_data 大小限制（最大 10KB）
    if len(req.EventData) > 10*1024 {
        return nil, status.Error(codes.InvalidArgument, "event_data 过大，最大 10KB")
    }

    // 补充事件时间（如果客户端未传）
    if req.EventTime == 0 {
        req.EventTime = time.Now().UnixMilli()
    }

    // ==================== 2. 投递事件到 Kafka — apigateway.event.reported ====================
    event := &kafka_gw.RequestLogEvent{
        Header:      buildEventHeader("apigateway", s.instanceID),
        RequestId:   getRequestID(ctx),
        UserId:      req.UserId,
        Method:      "POST",
        Path:        "/v1/gateway/report-event",
        StatusCode:  200,
        RequestTime: time.Now().UnixMilli(),
    }

    // 构造客户端事件载荷
    reportedEvent := map[string]interface{}{
        "event_id":       uuid.New().String(),
        "user_id":        req.UserId,
        "event_type":     req.EventType,
        "event_data":     req.EventData,
        "platform":       int(req.Platform),
        "client_version": req.ClientVersion,
        "event_time":     req.EventTime,
        "server_time":    time.Now().UnixMilli(),
    }
    reportedJSON, _ := json.Marshal(reportedEvent)

    if err := s.kafka.Produce(ctx, "apigateway.event.reported", req.UserId, reportedJSON); err != nil {
        log.Error("投递 apigateway.event.reported 事件失败",
            "user_id", req.UserId, "event_type", req.EventType, "err", err)
        // 事件上报失败不阻塞客户端，降级记录本地日志
    }

    // ==================== 3. 投递请求日志 ====================
    if err := s.kafka.Produce(ctx, "apigateway.request.log", req.UserId, event); err != nil {
        log.Error("投递 apigateway.request.log 事件失败", "err", err)
    }

    // ==================== 4. 返回 ====================
    return &pb.ReportEventResponse{
        Meta: successMeta(ctx),
    }, nil
}
```

---

### 6. CheckUpdate — 检查客户端版本更新

> 客户端启动时检查是否有新版本可用。  
> 从 Redis 读取平台对应的版本信息，对比客户端当前版本决定是否需要更新。  
> 此接口**免登录**（在 Auth 中间件白名单中）。

```go
func (s *ApiGatewayService) CheckUpdate(ctx context.Context, req *pb.CheckUpdateRequest) (*pb.CheckUpdateResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.Platform == common.PLATFORM_TYPE_UNSPECIFIED {
        return nil, status.Error(codes.InvalidArgument, "platform 不能为空")
    }
    if req.CurrentVersion == "" {
        return nil, status.Error(codes.InvalidArgument, "current_version 不能为空")
    }

    // ==================== 2. 从 Redis 获取平台版本信息 ====================
    // Key: gw:app_version:{platform}  HASH  Fields: min_version, latest_version, update_url, force_update
    versionKey := fmt.Sprintf("gw:app_version:%d", int(req.Platform))
    versionInfo, err := s.redis.HGetAll(ctx, versionKey).Result()
    if err != nil || len(versionInfo) == 0 {
        log.Warn("获取平台版本信息失败或无配置",
            "platform", req.Platform, "err", err)
        // 未配置版本信息时，默认无更新
        return &pb.CheckUpdateResponse{
            Meta:      successMeta(ctx),
            HasUpdate: false,
        }, nil
    }

    minVersion := versionInfo["min_version"]       // 最低支持版本（低于此版本强制更新）
    latestVersion := versionInfo["latest_version"]  // 最新版本
    updateURL := versionInfo["update_url"]          // 下载地址
    forceUpdate := versionInfo["force_update"]      // 是否强制更新 "true"/"false"

    // ==================== 3. 版本对比 ====================
    // 语义化版本对比：major.minor.patch
    hasUpdate := compareVersion(req.CurrentVersion, latestVersion) < 0
    needForceUpdate := compareVersion(req.CurrentVersion, minVersion) < 0

    // 如果 Redis 中标记了强制更新，且当前版本低于最新版本，则强制
    if forceUpdate == "true" && hasUpdate {
        needForceUpdate = true
    }

    if !hasUpdate {
        // 当前已是最新版本
        return &pb.CheckUpdateResponse{
            Meta:          successMeta(ctx),
            HasUpdate:     false,
            LatestVersion: latestVersion,
        }, nil
    }

    // ==================== 4. 读取更新说明（可选） ====================
    releaseNotes := versionInfo["release_notes"]
    packageSizeStr := versionInfo["package_size"]
    md5Hash := versionInfo["md5"]

    packageSize, _ := strconv.ParseInt(packageSizeStr, 10, 64)

    // ==================== 5. 返回 ====================
    return &pb.CheckUpdateResponse{
        Meta:          successMeta(ctx),
        HasUpdate:     true,
        LatestVersion: latestVersion,
        ForceUpdate:   needForceUpdate,
        DownloadUrl:   updateURL,
        ReleaseNotes:  releaseNotes,
        PackageSize:   packageSize,
        Md5:           md5Hash,
    }, nil
}

// compareVersion 语义化版本对比
// 返回: -1 (v1 < v2), 0 (v1 == v2), 1 (v1 > v2)
func compareVersion(v1, v2 string) int {
    parts1 := strings.Split(v1, ".")
    parts2 := strings.Split(v2, ".")

    maxLen := len(parts1)
    if len(parts2) > maxLen {
        maxLen = len(parts2)
    }

    for i := 0; i < maxLen; i++ {
        var n1, n2 int
        if i < len(parts1) {
            n1, _ = strconv.Atoi(parts1[i])
        }
        if i < len(parts2) {
            n2, _ = strconv.Atoi(parts2[i])
        }
        if n1 < n2 {
            return -1
        }
        if n1 > n2 {
            return 1
        }
    }
    return 0
}
```

---

### 7. GetServerTime — 获取服务器时间戳（客户端校时）

> 返回服务器当前时间戳，客户端用于校准本地时钟。  
> 此接口**免登录**（在 Auth 中间件白名单中），且必须极低延迟。  
> 不调用任何下游服务，不读取 Redis，直接返回系统时间。

```go
func (s *ApiGatewayService) GetServerTime(ctx context.Context, req *pb.GetServerTimeRequest) (*pb.GetServerTimeResponse, error) {
    // ==================== 1. 获取服务器当前时间 ====================
    now := time.Now().UnixMilli()

    // ==================== 2. 应用时间校准偏移（如果有 NTP 校准） ====================
    // Key: gw:server_time_offset  STRING  offset_ms
    // NTP 定时任务会写入偏移量，网关读取并修正
    offsetStr, err := s.redis.Get(ctx, "gw:server_time_offset").Result()
    if err == nil && offsetStr != "" {
        offset, _ := strconv.ParseInt(offsetStr, 10, 64)
        now += offset // 加上 NTP 校准偏移
    }

    // ==================== 3. 计算客户端与服务器的时间差 ====================
    var timeDiff int64
    if req.ClientTime > 0 {
        timeDiff = now - req.ClientTime // 正值表示客户端时间落后，负值表示客户端时间超前
    }

    // ==================== 4. 返回（不投递 Kafka 日志，避免增加延迟） ====================
    return &pb.GetServerTimeResponse{
        Meta:       successMeta(ctx),
        ServerTime: now,
        TimeDiff:   timeDiff,
    }, nil
}
```

---

## 辅助函数

```go
// successMeta 构造成功响应元信息
func successMeta(ctx context.Context) *common.ResponseMeta {
    return &common.ResponseMeta{
        Code:       0,
        Message:    "success",
        ServerTime: time.Now().UnixMilli(),
        TraceId:    getTraceID(ctx),
    }
}

// buildEventHeader 构造 Kafka 事件头
func buildEventHeader(source, instanceID string) *common.EventHeader {
    return &common.EventHeader{
        EventId:   uuid.New().String(),
        TraceId:   "", // 从 ctx 提取
        Source:    source,
        SourceId:  instanceID,
        Timestamp: time.Now().UnixMilli(),
        Version:   1,
    }
}

// produceRequestLog 投递通用请求日志到 Kafka
func (s *ApiGatewayService) produceRequestLog(ctx context.Context, userID, apiName string, statusCode int, latencyMs int64) {
    event := &kafka_gw.RequestLogEvent{
        Header:     buildEventHeader("apigateway", s.instanceID),
        RequestId:  getRequestID(ctx),
        UserId:     userID,
        Method:     getHTTPMethod(ctx),
        Path:       getRequestPath(ctx),
        StatusCode: int32(statusCode),
        LatencyMs:  latencyMs,
        ClientIp:   getClientIP(ctx),
        UserAgent:  getUserAgent(ctx),
        Upstream:   apiName,
        RequestTime: time.Now().UnixMilli(),
    }
    if err := s.kafka.Produce(ctx, "apigateway.request.log", getRequestID(ctx), event); err != nil {
        log.Error("投递 apigateway.request.log 事件失败", "err", err)
    }
}

// extractBearerToken 从 Authorization header 提取 Bearer Token
func extractBearerToken(authHeader string) string {
    if authHeader == "" {
        return ""
    }
    parts := strings.SplitN(authHeader, " ", 2)
    if len(parts) != 2 || strings.ToLower(parts[0]) != "bearer" {
        return ""
    }
    return strings.TrimSpace(parts[1])
}

// maskToken Token 脱敏：只保留前 8 位和后 4 位
func maskToken(token string) string {
    if len(token) <= 12 {
        return "***"
    }
    return token[:8] + "***" + token[len(token)-4:]
}

// normalizeAPIPath 标准化 API 路径为限流键
// /v1/message/send → message.send
func normalizeAPIPath(path string) string {
    path = strings.TrimPrefix(path, "/v1/")
    path = strings.TrimPrefix(path, "/v2/")
    return strings.ReplaceAll(path, "/", ".")
}

// getAPIRateLimit 获取指定 API 的限流阈值
// 不同 API 有不同限流值，高频操作（如 SyncData）限流更宽松
func (s *ApiGatewayService) getAPIRateLimit(apiPath string) int64 {
    limits := map[string]int64{
        "gateway.sync-data":       10,  // SyncData: 10 次/分钟（App 启动才调）
        "gateway.get-init-data":   10,  // GetInitData: 10 次/分钟
        "gateway.report-event":    120, // ReportEvent: 120 次/分钟（事件上报较频繁）
        "gateway.server-time":     60,  // GetServerTime: 60 次/分钟
        "gateway.check-update":    10,  // CheckUpdate: 10 次/分钟
        "gateway.batch-query":     30,  // BatchQuery: 30 次/分钟
        "gateway.endpoint":        10,  // GetGatewayEndpoint: 10 次/分钟
        "message.send":            60,  // SendMessage: 60 次/分钟
        "message.pull":            120, // PullMessages: 120 次/分钟
    }
    if limit, ok := limits[apiPath]; ok {
        return limit
    }
    return 60 // 默认 60 次/分钟
}
```

---

## 可观测性接入（OpenTelemetry）

> ApiGateway 是客户端唯一入口，需重点观测：HTTP 请求速率/延迟/错误率、限流命中率、认证失败率、到下游 RPC 的调用链、按 API 路径维度的细粒度指标。

### 第一步：服务启动时初始化 OTel SDK

```go
func main() {
    ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
    defer cancel()

    otelShutdown := observability.InitOpenTelemetry(ctx, "apigateway", "v1.0.0")
    defer otelShutdown(ctx)
    observability.SetupLogger("apigateway")
    // ...
}
```

### 第二步：HTTP 中间件埋点（ApiGateway 特有）

ApiGateway 是 HTTP 服务，需在 HTTP 中间件链中接入 OTel HTTP instrumentation。

```go
import "go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

// 在 HTTP Router 层包裹 OTel 中间件
handler := otelhttp.NewHandler(router, "apigateway",
    otelhttp.WithMessageEvents(otelhttp.ReadEvents, otelhttp.WriteEvents),
)
http.ListenAndServe(":8080", handler)
```

**自动产出的指标**（无需手写）：

| 指标名 | 类型 | 含义 |
|--------|------|------|
| `http.server.duration` | Histogram | HTTP 请求延迟分布 |
| `http.server.active_requests` | UpDownCounter | 当前正在处理的请求数 |
| `http.server.request.size` | Histogram | 请求体大小 |
| `http.server.response.size` | Histogram | 响应体大小 |

### 第三步：gRPC Client Interceptor（调用下游 11 个 RPC 服务时）

```go
// 所有下游 RPC 连接都挂 client interceptor
messageConn := grpc.Dial("message:50051",
    grpc.WithChainUnaryInterceptor(otelgrpc.UnaryClientInterceptor()),
)
authConn := grpc.Dial("auth:50051",
    grpc.WithChainUnaryInterceptor(otelgrpc.UnaryClientInterceptor()),
)
// 同理: user / group / relation / conversation / session / config / search / notification / presence
```

### 第四步：注册 ApiGateway 专属业务指标

```go
var meter = otel.Meter("im-chat/apigateway")

var (
    // 限流命中计数
    rateLimitHit, _ = meter.Int64Counter("apigateway.rate_limit_hit_total",
        metric.WithDescription("限流命中次数（按 API 路径）"))

    // 认证失败计数
    authFailed, _ = meter.Int64Counter("apigateway.auth_failed_total",
        metric.WithDescription("Token 认证失败次数"))

    // 按 API 路径的请求计数（用于热力图）
    apiRequestCounter, _ = meter.Int64Counter("apigateway.api_requests_total",
        metric.WithDescription("按 API 路径统计的请求总数"))

    // 下游 RPC 调用失败计数
    downstreamRpcFailed, _ = meter.Int64Counter("apigateway.downstream_rpc_failed_total",
        metric.WithDescription("下游 RPC 调用失败次数"))
)
```

在中间件代码中埋点：

```go
// RateLimit 中间件
if isRateLimited {
    rateLimitHit.Add(ctx, 1, metric.WithAttributes(
        attribute.String("api_path", apiPath),
        attribute.String("user_id", userID),
    ))
}

// Auth 中间件
if tokenErr != nil {
    authFailed.Add(ctx, 1, metric.WithAttributes(
        attribute.String("reason", tokenErr.Error()),
    ))
}

// 路由 Handler 入口
apiRequestCounter.Add(ctx, 1, metric.WithAttributes(
    attribute.String("api_path", apiPath),
    attribute.String("method", httpMethod),
))
```

### 第五步：Kafka Producer 埋点

发布 `gateway.event.report` / `gateway.data.sync` 等事件时，创建 PRODUCER span + 注入 trace context 到 Kafka headers。

### 第六步：结构化 JSON 日志

设置 `slog.JSONHandler` + traceLogHandler，注意 ApiGateway 的访问日志应包含 `client_ip`、`user_agent`、`api_path`、`status_code` 字段。

```go
slog.InfoContext(ctx, "request completed",
    "api_path", apiPath,
    "method", httpMethod,
    "status", statusCode,
    "duration_ms", durationMs,
    "client_ip", clientIP,
)
```

### 接入要点总结

| 步骤 | 改动位置 | 效果 |
|------|---------|------|
| 1. `observability.InitOpenTelemetry` | main() | TracerProvider + MeterProvider + Runtime Metrics |
| 2. HTTP 中间件 | otelhttp.NewHandler | HTTP 请求自动 trace + 指标 |
| 3. gRPC Client Interceptor | 11 个下游 RPC 连接 | 跨服务调用链自动传播 |
| 4. 自定义业务指标 | 中间件 + Handler | 限流命中率、认证失败率 |
| 5. Kafka 埋点 | 事件发布 | 链路追踪 |
| 6. 结构化 JSON 日志 | setupLogger | 访问日志 + trace_id |
| 7. buildEventHeader 改造 | 辅助函数 | EventHeader 携带真实 trace context |
| 8. 基础设施指标 | main() | Redis 连接池可观测 |
| 9. Span 错误记录 | 所有 error 路径 | 错误事件关联到 trace |

### 补充：buildEventHeader 改造

将 `buildEventHeader(source, instanceID)` 替换为 `observability.BuildEventHeader(ctx, source, instanceID)`。

### 补充：基础设施指标注册

```go
// main() 中
observability.RegisterRedisPoolMetrics(redisClient, "apigateway")
```

### 补充：Span 错误记录

所有下游 RPC 调用失败路径加入 `observability.RecordError(span, err)`。
