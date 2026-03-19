# Connecte 长连接网关服务 — RPC 接口实现伪代码

## 概述

Connecte 服务是整个 IM 系统的**连接层核心**，管理客户端与网关服务器之间的 WebSocket / TCP 长连接。它维护一张**实时路由表**（user_id + platform → gateway_id + connection_id），使其他服务（尤其是 Push）能够精确定位用户所在的网关节点，并通过该网关下发数据。

**在 notify+sync 架构中的角色：**
- Push 服务通过 RPC 调用 `Connecte.SendToConnection` / `BatchSendToConnections` 下发 sync notify 信号
- Connecte 仅负责**数据透传**，不解析消息内容，不存储任何业务数据
- 客户端收到 sync notify 后自行调用 `Message.PullMessages` 拉取真实消息

**多端登录支持：**
- 同一用户可在多个平台同时在线（手机 + PC + Web + iPad 等）
- `GetUserConnections` 返回用户所有平台的连接信息
- 通过 `KickConnection` 可踢掉指定连接（如单平台互踢策略）

**核心设计原则：**
- **纯实时路由服务**：所有状态全部存储在 Redis，无 PgSQL 持久化，节点无状态
- **高可用**：Redis Key 带 TTL，通过心跳持续续期，心跳丢失则自动清除过期路由
- **高性能**：路由查询全走 Redis，单次查询 < 1ms；批量发送使用 Pipeline + goroutine 池
- **网关自治**：网关节点直接调用 `RegisterConnection` / `UnregisterConnection`，无需中心协调

## 外部依赖

| 依赖类型 | 依赖项 | 用途 |
|---------|--------|------|
| PgSQL | **无** | 纯实时路由服务，无持久化需求 |
| Redis | 连接路由 / 网关状态 / 在线统计 | 全部运行时状态存储 |
| RPC | Presence.UserOnline | 连接注册后通知 Presence 用户上线 |
| RPC | Presence.UserOffline | 连接注销后通知 Presence 用户下线 |
| RPC | Session.GetLoginSession | 连接注册时校验登录会话有效性 |
| Kafka | conn.connected / conn.disconnected / conn.route.updated | 连接生命周期事件通知下游 |

## PgSQL 表结构

```
无 — Connecte 是纯实时路由服务，所有状态存储在 Redis 中。
网关节点无状态，连接信息通过 Redis TTL + 心跳续期自动维护。
```

## Redis Key 设计

| Key 格式 | 类型 | 值 | TTL | 说明 |
|----------|------|-----|-----|------|
| `conn:user:{user_id}:{platform}` | HASH | {gateway_id, conn_id, device_id, connected_at, last_heartbeat, client_ip, server_addr} | 5min（心跳续期） | 用户在指定平台的连接信息，心跳每 30s 续期一次 |
| `conn:user_all:{user_id}` | SET | member="platform:gateway_id:conn_id" | 5min（心跳续期） | 用户所有平台的连接索引，用于快速查询多端在线 |
| `conn:gateway:{gateway_id}` | HASH | {addr, port, conn_count, cpu_usage, memory_usage, status, last_heartbeat, max_connections, started_at} | 30s（网关心跳续期） | 网关节点状态信息 |
| `conn:gateway_conns:{gateway_id}` | SET | member="user_id:platform" | 30s（网关心跳续期） | 网关节点上的所有连接索引，用于网关故障时批量清理 |
| `conn:gateway_list` | ZSET | member=gateway_id, score=conn_count | 无 TTL（手动维护） | 全局网关节点列表，score 为连接数，用于负载均衡选择 |
| `conn:total_online` | STRING | count | 无 TTL（定时更新） | 全局在线连接总数，由定时任务汇总更新 |

---

## 接口实现

### 1. GetUserConnection — 查询用户在指定平台的连接信息

> 查询指定用户在指定平台（如 iOS / Android / Web）的连接信息。  
> Push 服务和 Presence 服务高频调用此接口做路由查询。全走 Redis HGETALL，< 1ms 延迟。

```go
func (s *ConnecteService) GetUserConnection(ctx context.Context, req *pb.GetUserConnectionRequest) (*pb.GetUserConnectionResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id 不能为空")
    }
    if req.Platform == common.PLATFORM_TYPE_UNSPECIFIED {
        return nil, status.Error(codes.InvalidArgument, "platform 不能为空")
    }

    // ==================== 2. 查询 Redis — 用户指定平台连接信息 ====================
    // Key: conn:user:{user_id}:{platform}  HASH
    // Fields: gateway_id, conn_id, device_id, connected_at, last_heartbeat, client_ip, server_addr
    connKey := fmt.Sprintf("conn:user:%s:%d", req.UserId, int(req.Platform))
    result, err := s.redis.HGetAll(ctx, connKey).Result()
    if err != nil {
        log.Error("查询用户连接信息失败",
            "user_id", req.UserId, "platform", req.Platform, "err", err)
        return nil, status.Error(codes.Internal, "查询用户连接信息失败")
    }

    // ==================== 3. 未找到连接 — 用户在该平台不在线 ====================
    if len(result) == 0 {
        return &pb.GetUserConnectionResponse{
            Meta:       successMeta(ctx),
            Connection: nil,
            Online:     false,
        }, nil
    }

    // ==================== 4. 组装连接信息 ====================
    connectedAt, _ := strconv.ParseInt(result["connected_at"], 10, 64)
    lastActive, _ := strconv.ParseInt(result["last_heartbeat"], 10, 64)

    connInfo := &pb.ConnectionInfo{
        ConnectionId: result["conn_id"],
        UserId:       req.UserId,
        Platform:     req.Platform,
        DeviceId:     result["device_id"],
        ServerId:     result["gateway_id"],
        ServerAddr:   result["server_addr"],
        ClientIp:     result["client_ip"],
        ConnectedAt:  connectedAt,
        LastActive:   lastActive,
    }

    // ==================== 5. 返回 ====================
    return &pb.GetUserConnectionResponse{
        Meta:       successMeta(ctx),
        Connection: connInfo,
        Online:     true,
    }, nil
}
```

### 2. GetUserConnections — 查询用户所有平台的连接信息（多端在线）

> 查询用户在所有平台的连接列表。Push 服务 PushToUser 时需要获取所有在线连接做全端推送。  
> 先从 `conn:user_all:{user_id}` SET 获取连接索引，再批量 HGETALL 各平台连接详情。

```go
func (s *ConnecteService) GetUserConnections(ctx context.Context, req *pb.GetUserConnectionsRequest) (*pb.GetUserConnectionsResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id 不能为空")
    }

    // ==================== 2. 查询用户所有平台连接索引 — Redis SET ====================
    // Key: conn:user_all:{user_id}  SET
    // Members: "platform:gateway_id:conn_id"
    allKey := fmt.Sprintf("conn:user_all:%s", req.UserId)
    members, err := s.redis.SMembers(ctx, allKey).Result()
    if err != nil {
        log.Error("查询用户连接索引失败", "user_id", req.UserId, "err", err)
        return nil, status.Error(codes.Internal, "查询用户连接索引失败")
    }

    // 无任何连接
    if len(members) == 0 {
        return &pb.GetUserConnectionsResponse{
            Meta:        successMeta(ctx),
            Connections: nil,
        }, nil
    }

    // ==================== 3. 批量查询各平台连接详情 — Redis Pipeline ====================
    // 解析 member 格式："platform:gateway_id:conn_id"
    // 对每个 platform 执行 HGETALL conn:user:{user_id}:{platform}
    pipe := s.redis.Pipeline()
    type connCmd struct {
        platform string
        cmd      *redis.MapStringStringCmd
    }
    cmds := make([]connCmd, 0, len(members))

    parsedPlatforms := make(map[string]bool) // 去重：同一 platform 只查一次
    for _, member := range members {
        parts := strings.SplitN(member, ":", 3) // platform:gateway_id:conn_id
        if len(parts) < 3 {
            log.Warn("无效的连接索引格式", "user_id", req.UserId, "member", member)
            continue
        }
        platform := parts[0]
        if parsedPlatforms[platform] {
            continue
        }
        parsedPlatforms[platform] = true

        connKey := fmt.Sprintf("conn:user:%s:%s", req.UserId, platform)
        cmd := pipe.HGetAll(ctx, connKey)
        cmds = append(cmds, connCmd{platform: platform, cmd: cmd})
    }

    if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
        log.Error("Pipeline 查询连接详情失败", "user_id", req.UserId, "err", err)
        return nil, status.Error(codes.Internal, "查询连接详情失败")
    }

    // ==================== 4. 组装连接列表 ====================
    connections := make([]*pb.ConnectionInfo, 0, len(cmds))
    for _, c := range cmds {
        result, err := c.cmd.Result()
        if err != nil || len(result) == 0 {
            continue // 该平台连接可能已过期，跳过
        }

        platformInt, _ := strconv.Atoi(c.platform)
        connectedAt, _ := strconv.ParseInt(result["connected_at"], 10, 64)
        lastActive, _ := strconv.ParseInt(result["last_heartbeat"], 10, 64)

        connections = append(connections, &pb.ConnectionInfo{
            ConnectionId: result["conn_id"],
            UserId:       req.UserId,
            Platform:     common.PlatformType(platformInt),
            DeviceId:     result["device_id"],
            ServerId:     result["gateway_id"],
            ServerAddr:   result["server_addr"],
            ClientIp:     result["client_ip"],
            ConnectedAt:  connectedAt,
            LastActive:   lastActive,
        })
    }

    // ==================== 5. 返回 ====================
    return &pb.GetUserConnectionsResponse{
        Meta:        successMeta(ctx),
        Connections: connections,
    }, nil
}
```

### 3. BatchGetUserConnections — 批量查询多个用户的连接信息

> 用于群消息推送、状态变更扩散等高频批量场景，避免 N+1 RPC 调用。  
> 内部对每个用户复用 GetUserConnections 的 Redis Pipeline 逻辑，合并为一次大 Pipeline 执行。

```go
func (s *ConnecteService) BatchGetUserConnections(ctx context.Context, req *pb.BatchGetUserConnectionsRequest) (*pb.BatchGetUserConnectionsResponse, error) {
    // ==================== 1. 参数校验 ====================
    if len(req.UserIds) == 0 {
        return &pb.BatchGetUserConnectionsResponse{
            Meta:            successMeta(ctx),
            UserConnections: nil,
        }, nil
    }
    if len(req.UserIds) > 2000 {
        return nil, status.Error(codes.InvalidArgument, "user_ids 最多 2000 个")
    }

    // ==================== 2. 批量查询各用户连接索引 — Redis Pipeline ====================
    pipe := s.redis.Pipeline()
    type indexCmd struct {
        userID string
        cmd    *redis.StringSliceCmd
    }
    indexCmds := make([]indexCmd, 0, len(req.UserIds))
    for _, uid := range req.UserIds {
        allKey := fmt.Sprintf("conn:user_all:%s", uid)
        cmd := pipe.SMembers(ctx, allKey)
        indexCmds = append(indexCmds, indexCmd{userID: uid, cmd: cmd})
    }
    if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
        log.Error("批量查询用户连接索引失败", "err", err)
        return nil, status.Error(codes.Internal, "批量查询连接索引失败")
    }

    // ==================== 3. 批量查询各平台连接详情 — Redis Pipeline ====================
    type detailCmd struct {
        userID   string
        platform string
        cmd      *redis.MapStringStringCmd
    }
    detailPipe := s.redis.Pipeline()
    detailCmds := make([]detailCmd, 0)

    for _, ic := range indexCmds {
        members, err := ic.cmd.Result()
        if err != nil || len(members) == 0 {
            continue
        }
        parsedPlatforms := make(map[string]bool)
        for _, member := range members {
            parts := strings.SplitN(member, ":", 3)
            if len(parts) < 3 {
                continue
            }
            platform := parts[0]
            if parsedPlatforms[platform] {
                continue
            }
            parsedPlatforms[platform] = true
            connKey := fmt.Sprintf("conn:user:%s:%s", ic.userID, platform)
            cmd := detailPipe.HGetAll(ctx, connKey)
            detailCmds = append(detailCmds, detailCmd{userID: ic.userID, platform: platform, cmd: cmd})
        }
    }

    if len(detailCmds) > 0 {
        if _, err := detailPipe.Exec(ctx); err != nil && err != redis.Nil {
            log.Error("批量查询连接详情失败", "err", err)
            return nil, status.Error(codes.Internal, "批量查询连接详情失败")
        }
    }

    // ==================== 4. 组装结果 ====================
    userConnMap := make(map[string][]*pb.ConnectionInfo)
    for _, dc := range detailCmds {
        result, err := dc.cmd.Result()
        if err != nil || len(result) == 0 {
            continue
        }
        platformInt, _ := strconv.Atoi(dc.platform)
        connectedAt, _ := strconv.ParseInt(result["connected_at"], 10, 64)
        lastActive, _ := strconv.ParseInt(result["last_heartbeat"], 10, 64)

        conn := &pb.ConnectionInfo{
            ConnectionId: result["conn_id"],
            UserId:       dc.userID,
            Platform:     common.PlatformType(platformInt),
            DeviceId:     result["device_id"],
            ServerId:     result["gateway_id"],
            ServerAddr:   result["server_addr"],
            ClientIp:     result["client_ip"],
            ConnectedAt:  connectedAt,
            LastActive:   lastActive,
        }
        userConnMap[dc.userID] = append(userConnMap[dc.userID], conn)
    }

    entries := make([]*pb.UserConnectionsEntry, 0, len(userConnMap))
    for uid, conns := range userConnMap {
        entries = append(entries, &pb.UserConnectionsEntry{
            UserId:      uid,
            Connections: conns,
        })
    }

    // ==================== 5. 返回 ====================
    return &pb.BatchGetUserConnectionsResponse{
        Meta:            successMeta(ctx),
        UserConnections: entries,
    }, nil
}
```

### 4. KickConnection — 踢掉指定连接

> 强制断开用户的指定连接。使用场景：  
> - Session 服务检测到同平台互踢，调用此接口断开旧连接  
> - 管理后台封禁用户时批量踢连接  
> - 安全审计触发的强制断线  
> 流程：校验连接存在 → 查路由获取 gateway_id → 调网关内部断开接口 → 清理 Redis 路由 → 投递 Kafka 事件。

```go
func (s *ConnecteService) KickConnection(ctx context.Context, req *pb.KickConnectionRequest) (*pb.KickConnectionResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id 不能为空")
    }
    if req.ConnectionId == "" {
        return nil, status.Error(codes.InvalidArgument, "connection_id 不能为空")
    }
    if req.Reason == "" {
        req.Reason = "server_kick" // 默认踢出原因
    }

    // ==================== 2. 查找连接所在的网关和平台 ====================
    // 遍历用户所有连接索引，找到匹配 connection_id 的条目
    allKey := fmt.Sprintf("conn:user_all:%s", req.UserId)
    members, err := s.redis.SMembers(ctx, allKey).Result()
    if err != nil {
        log.Error("查询用户连接索引失败", "user_id", req.UserId, "err", err)
        return nil, status.Error(codes.Internal, "查询连接信息失败")
    }

    var targetGatewayID string
    var targetPlatform string
    for _, member := range members {
        // member 格式: "platform:gateway_id:conn_id"
        parts := strings.SplitN(member, ":", 3)
        if len(parts) == 3 && parts[2] == req.ConnectionId {
            targetPlatform = parts[0]
            targetGatewayID = parts[1]
            break
        }
    }

    if targetGatewayID == "" {
        // 连接不存在或已断开
        log.Warn("踢连接时未找到目标连接",
            "user_id", req.UserId, "connection_id", req.ConnectionId)
        return &pb.KickConnectionResponse{
            Meta: successMeta(ctx),
        }, nil
    }

    // ==================== 3. 获取网关节点地址 — Redis ====================
    // Key: conn:gateway:{gateway_id}  HASH
    gatewayKey := fmt.Sprintf("conn:gateway:%s", targetGatewayID)
    gatewayAddr, err := s.redis.HGet(ctx, gatewayKey, "addr").Result()
    if err != nil {
        log.Error("查询网关地址失败",
            "gateway_id", targetGatewayID, "err", err)
        // 网关不可达，直接清理路由信息
        s.cleanupConnectionRoute(ctx, req.UserId, targetPlatform, req.ConnectionId, targetGatewayID)
        return &pb.KickConnectionResponse{
            Meta: successMeta(ctx),
        }, nil
    }
    gatewayPort, _ := s.redis.HGet(ctx, gatewayKey, "port").Result()
    gatewayFullAddr := fmt.Sprintf("%s:%s", gatewayAddr, gatewayPort)

    // ==================== 4. 调用网关内部接口断开连接 ====================
    // 通过 gRPC 连接到目标网关节点，调用其内部 KickConn 方法
    gatewayConn, err := s.gatewayPool.GetConn(gatewayFullAddr)
    if err != nil {
        log.Error("获取网关 gRPC 连接失败",
            "gateway_addr", gatewayFullAddr, "err", err)
        // 网关不可达，直接清理路由
        s.cleanupConnectionRoute(ctx, req.UserId, targetPlatform, req.ConnectionId, targetGatewayID)
        return &pb.KickConnectionResponse{
            Meta: successMeta(ctx),
        }, nil
    }

    // 调用网关的内部断开方法（网关收到后会关闭 WebSocket/TCP 连接）
    _, kickErr := gateway_pb.NewGatewayInternalClient(gatewayConn).KickConn(ctx, &gateway_pb.KickConnRequest{
        ConnectionId: req.ConnectionId,
        Reason:       req.Reason,
    })
    if kickErr != nil {
        log.Warn("调用网关踢连接失败（可能连接已断开）",
            "gateway_id", targetGatewayID, "connection_id", req.ConnectionId, "err", kickErr)
        // 不管网关是否成功，都要清理路由
    }

    // ==================== 5. 清理 Redis 路由信息 ====================
    s.cleanupConnectionRoute(ctx, req.UserId, targetPlatform, req.ConnectionId, targetGatewayID)

    // ==================== 6. 通知 Presence 用户下线 — RPC ====================
    platformInt, _ := strconv.Atoi(targetPlatform)
    _, err = s.presenceClient.UserOffline(ctx, &presence_pb.UserOfflineRequest{
        UserId:   req.UserId,
        Platform: common.PlatformType(platformInt),
    })
    if err != nil {
        log.Warn("通知 Presence 用户下线失败（不阻塞主流程）",
            "user_id", req.UserId, "platform", targetPlatform, "err", err)
    }

    // ==================== 7. 投递 Kafka conn.disconnected 事件 ====================
    disconnEvent := &kafka_connecte.DisconnectedEvent{
        Header:      buildEventHeader("connecte", s.instanceID),
        UserId:      req.UserId,
        DeviceId:    "", // kick 场景不一定有 device_id
        Platform:    common.PlatformType(platformInt),
        Gateway:     targetGatewayID,
        Reason:      req.Reason,
        DisconnTime: time.Now().UnixMilli(),
    }
    if err := s.kafka.Produce(ctx, "conn.disconnected", req.UserId, disconnEvent); err != nil {
        log.Error("投递 conn.disconnected 事件失败",
            "user_id", req.UserId, "err", err)
    }

    log.Info("踢连接完成",
        "user_id", req.UserId,
        "connection_id", req.ConnectionId,
        "gateway_id", targetGatewayID,
        "reason", req.Reason)

    // ==================== 8. 返回 ====================
    return &pb.KickConnectionResponse{
        Meta: successMeta(ctx),
    }, nil
}

// cleanupConnectionRoute 清理用户连接在 Redis 中的所有路由信息
func (s *ConnecteService) cleanupConnectionRoute(ctx context.Context, userID, platform, connID, gatewayID string) {
    now := time.Now().UnixMilli()
    _ = now

    pipe := s.redis.Pipeline()

    // 1. 删除用户指定平台连接信息
    // Key: conn:user:{user_id}:{platform}
    connKey := fmt.Sprintf("conn:user:%s:%s", userID, platform)
    pipe.Del(ctx, connKey)

    // 2. 从用户连接索引中移除
    // Key: conn:user_all:{user_id}
    allKey := fmt.Sprintf("conn:user_all:%s", userID)
    member := fmt.Sprintf("%s:%s:%s", platform, gatewayID, connID)
    pipe.SRem(ctx, allKey, member)

    // 3. 从网关连接索引中移除
    // Key: conn:gateway_conns:{gateway_id}
    gatewayConnsKey := fmt.Sprintf("conn:gateway_conns:%s", gatewayID)
    gatewayMember := fmt.Sprintf("%s:%s", userID, platform)
    pipe.SRem(ctx, gatewayConnsKey, gatewayMember)

    // 4. 网关连接计数 -1（更新 ZSET score）
    // Key: conn:gateway_list  ZSET
    pipe.ZIncrBy(ctx, "conn:gateway_list", -1, gatewayID)

    if _, err := pipe.Exec(ctx); err != nil {
        log.Error("清理连接路由信息失败",
            "user_id", userID, "platform", platform,
            "conn_id", connID, "gateway_id", gatewayID, "err", err)
    }
}
```

### 5. SendToConnection — 向指定连接发送数据

> **sync notify 的最终出口**。Push 服务调用此接口将数据包发送到指定连接。  
> 流程：校验参数 → 查路由获取 gateway 地址 → 通过 gRPC 转发到目标网关 → 网关写入 WebSocket/TCP。  
> 这是整个 IM 下行链路中**最热**的 RPC 调用。

```go
func (s *ConnecteService) SendToConnection(ctx context.Context, req *pb.SendToConnectionRequest) (*pb.SendToConnectionResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.ConnectionId == "" {
        return nil, status.Error(codes.InvalidArgument, "connection_id 不能为空")
    }
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id 不能为空")
    }
    if req.DataType == "" {
        return nil, status.Error(codes.InvalidArgument, "data_type 不能为空")
    }
    if len(req.Data) == 0 {
        return nil, status.Error(codes.InvalidArgument, "data 不能为空")
    }

    // ==================== 2. 查找连接所在的网关 — Redis ====================
    // 遍历用户连接索引，找到目标 connection_id 对应的 gateway
    allKey := fmt.Sprintf("conn:user_all:%s", req.UserId)
    members, err := s.redis.SMembers(ctx, allKey).Result()
    if err != nil {
        log.Error("查询用户连接索引失败", "user_id", req.UserId, "err", err)
        return nil, status.Error(codes.Internal, "查询连接路由失败")
    }

    var targetGatewayID string
    for _, member := range members {
        // member 格式: "platform:gateway_id:conn_id"
        parts := strings.SplitN(member, ":", 3)
        if len(parts) == 3 && parts[2] == req.ConnectionId {
            targetGatewayID = parts[1]
            break
        }
    }

    if targetGatewayID == "" {
        // 连接不存在（可能已断开）
        return nil, status.Error(codes.NotFound, "连接不存在或已断开")
    }

    // ==================== 3. 获取网关节点地址 — Redis HASH ====================
    // Key: conn:gateway:{gateway_id}
    gatewayKey := fmt.Sprintf("conn:gateway:%s", targetGatewayID)
    gatewayInfo, err := s.redis.HMGet(ctx, gatewayKey, "addr", "port", "status").Result()
    if err != nil || gatewayInfo[0] == nil {
        log.Error("查询网关节点信息失败",
            "gateway_id", targetGatewayID, "err", err)
        return nil, status.Error(codes.Unavailable, "网关节点不可用")
    }

    gatewayStatus, _ := gatewayInfo[2].(string)
    if gatewayStatus == "stopped" || gatewayStatus == "draining" {
        log.Warn("目标网关正在下线",
            "gateway_id", targetGatewayID, "status", gatewayStatus)
        return nil, status.Error(codes.Unavailable, "网关节点正在下线")
    }

    gatewayAddr := fmt.Sprintf("%s:%s", gatewayInfo[0], gatewayInfo[1])

    // ==================== 4. 转发数据到目标网关 — 内部 gRPC ====================
    // 通过连接池获取到目标网关的 gRPC 连接
    gatewayConn, err := s.gatewayPool.GetConn(gatewayAddr)
    if err != nil {
        log.Error("获取网关 gRPC 连接失败",
            "gateway_addr", gatewayAddr, "err", err)
        return nil, status.Error(codes.Unavailable, "无法连接到网关节点")
    }

    // 调用网关内部的 WriteToConn 方法，网关收到后将数据写入 WebSocket/TCP 连接
    _, writeErr := gateway_pb.NewGatewayInternalClient(gatewayConn).WriteToConn(ctx, &gateway_pb.WriteToConnRequest{
        ConnectionId: req.ConnectionId,
        DataType:     req.DataType,
        Data:         req.Data,
    })
    if writeErr != nil {
        log.Error("向连接写入数据失败",
            "connection_id", req.ConnectionId,
            "gateway_id", targetGatewayID,
            "data_type", req.DataType,
            "err", writeErr)
        // 判断是否是连接已断开的错误
        if st, ok := status.FromError(writeErr); ok && st.Code() == codes.NotFound {
            // 连接已断开，清理路由（异步）
            go s.handleStaleConnection(context.Background(), req.UserId, req.ConnectionId)
            return nil, status.Error(codes.NotFound, "连接已断开")
        }
        return nil, status.Error(codes.Internal, "发送数据到连接失败")
    }

    // ==================== 5. 返回 ====================
    return &pb.SendToConnectionResponse{
        Meta: successMeta(ctx),
    }, nil
}

// handleStaleConnection 异步清理失效的连接路由
func (s *ConnecteService) handleStaleConnection(ctx context.Context, userID, connID string) {
    allKey := fmt.Sprintf("conn:user_all:%s", userID)
    members, err := s.redis.SMembers(ctx, allKey).Result()
    if err != nil {
        return
    }
    for _, member := range members {
        parts := strings.SplitN(member, ":", 3)
        if len(parts) == 3 && parts[2] == connID {
            s.cleanupConnectionRoute(ctx, userID, parts[0], connID, parts[1])
            break
        }
    }
}
```

### 6. BatchSendToConnections — 批量向多个连接发送数据

> Push 服务 fan-out 群消息时调用此接口，将 sync notify 批量发送给多个在线连接。  
> 按 gateway_id 分组 → 对每个网关使用 goroutine 并行发送 → 汇总成功/失败计数。  
> 使用 goroutine 池控制并发度，避免大群场景下 goroutine 爆炸。

```go
func (s *ConnecteService) BatchSendToConnections(ctx context.Context, req *pb.BatchSendToConnectionsRequest) (*pb.BatchSendToConnectionsResponse, error) {
    // ==================== 1. 参数校验 ====================
    if len(req.ConnectionIds) == 0 {
        return nil, status.Error(codes.InvalidArgument, "connection_ids 不能为空")
    }
    if req.DataType == "" {
        return nil, status.Error(codes.InvalidArgument, "data_type 不能为空")
    }
    if len(req.Data) == 0 {
        return nil, status.Error(codes.InvalidArgument, "data 不能为空")
    }
    if len(req.ConnectionIds) > 10000 {
        return nil, status.Error(codes.InvalidArgument, "connection_ids 数量不能超过 10000")
    }

    // ==================== 2. 查询所有连接的网关路由 — 按 gateway 分组 ====================
    // 需要一个反向索引: connection_id → gateway_id
    // 这里通过内存中的连接管理器（ConnManager）快速查找
    // ConnManager 内部维护了一份 conn_id → {gateway_id, user_id, platform} 的本地缓存
    type connRoute struct {
        connID    string
        gatewayID string
    }

    // 按 gateway_id 分组
    gatewayGroups := make(map[string][]string) // gateway_id → []connection_id
    missingConns := make([]string, 0)

    for _, connID := range req.ConnectionIds {
        route, ok := s.connManager.GetRoute(connID)
        if !ok {
            missingConns = append(missingConns, connID)
            continue
        }
        gatewayGroups[route.GatewayID] = append(gatewayGroups[route.GatewayID], connID)
    }

    if len(missingConns) > 0 {
        log.Warn("批量发送时部分连接未找到路由",
            "missing_count", len(missingConns),
            "total", len(req.ConnectionIds))
    }

    // ==================== 3. 按网关并行发送 — goroutine 池 ====================
    var (
        mu           sync.Mutex
        successCount int32
        failCount    int32
    )

    // 使用 semaphore 控制并发度，最多 32 个 goroutine 同时工作
    sem := make(chan struct{}, 32)
    var wg sync.WaitGroup

    for gatewayID, connIDs := range gatewayGroups {
        gatewayID := gatewayID
        connIDs := connIDs

        wg.Add(1)
        sem <- struct{}{} // 获取令牌

        go func() {
            defer wg.Done()
            defer func() { <-sem }() // 释放令牌

            // 获取网关地址
            gatewayKey := fmt.Sprintf("conn:gateway:%s", gatewayID)
            gatewayInfo, err := s.redis.HMGet(ctx, gatewayKey, "addr", "port").Result()
            if err != nil || gatewayInfo[0] == nil {
                log.Error("查询网关地址失败（批量发送）",
                    "gateway_id", gatewayID, "err", err)
                mu.Lock()
                failCount += int32(len(connIDs))
                mu.Unlock()
                return
            }

            gatewayAddr := fmt.Sprintf("%s:%s", gatewayInfo[0], gatewayInfo[1])

            // 获取 gRPC 连接
            gatewayConn, err := s.gatewayPool.GetConn(gatewayAddr)
            if err != nil {
                log.Error("获取网关 gRPC 连接失败（批量发送）",
                    "gateway_addr", gatewayAddr, "err", err)
                mu.Lock()
                failCount += int32(len(connIDs))
                mu.Unlock()
                return
            }

            // 调用网关批量写入接口
            batchResp, err := gateway_pb.NewGatewayInternalClient(gatewayConn).BatchWriteToConns(ctx, &gateway_pb.BatchWriteToConnsRequest{
                ConnectionIds: connIDs,
                DataType:      req.DataType,
                Data:          req.Data,
            })
            if err != nil {
                log.Error("网关批量写入失败",
                    "gateway_id", gatewayID, "conn_count", len(connIDs), "err", err)
                mu.Lock()
                failCount += int32(len(connIDs))
                mu.Unlock()
                return
            }

            mu.Lock()
            successCount += batchResp.SuccessCount
            failCount += batchResp.FailCount
            mu.Unlock()
        }()
    }

    wg.Wait()

    // 未找到路由的连接计入失败数
    failCount += int32(len(missingConns))

    // ==================== 4. 返回 ====================
    return &pb.BatchSendToConnectionsResponse{
        Meta:         successMeta(ctx),
        SuccessCount: successCount,
        FailCount:    failCount,
    }, nil
}
```

### 7. GetGatewayStatus — 查询网关节点状态

> 运维/监控接口。查询指定网关节点的实时状态（连接数、CPU、内存、运行状态）。  
> 全走 Redis HGETALL，数据由网关节点定期上报（每 10s 上报一次，TTL 30s）。

```go
func (s *ConnecteService) GetGatewayStatus(ctx context.Context, req *pb.GetGatewayStatusRequest) (*pb.GetGatewayStatusResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.ServerId == "" {
        return nil, status.Error(codes.InvalidArgument, "server_id 不能为空")
    }

    // ==================== 2. 查询网关节点信息 — Redis HASH ====================
    // Key: conn:gateway:{gateway_id}
    // Fields: addr, port, conn_count, cpu_usage, memory_usage, status, last_heartbeat, max_connections, started_at
    gatewayKey := fmt.Sprintf("conn:gateway:%s", req.ServerId)
    result, err := s.redis.HGetAll(ctx, gatewayKey).Result()
    if err != nil {
        log.Error("查询网关状态失败", "server_id", req.ServerId, "err", err)
        return nil, status.Error(codes.Internal, "查询网关状态失败")
    }

    // 网关节点不存在或已过期
    if len(result) == 0 {
        return nil, status.Error(codes.NotFound, "网关节点不存在或已离线")
    }

    // ==================== 3. 组装网关节点信息 ====================
    connCount, _ := strconv.Atoi(result["conn_count"])
    maxConns, _ := strconv.Atoi(result["max_connections"])
    cpuUsage, _ := strconv.ParseFloat(result["cpu_usage"], 64)
    memUsage, _ := strconv.ParseFloat(result["memory_usage"], 64)
    startedAt, _ := strconv.ParseInt(result["started_at"], 10, 64)

    node := &pb.GatewayNode{
        ServerId:        req.ServerId,
        Addr:            fmt.Sprintf("%s:%s", result["addr"], result["port"]),
        ConnectionCount: int32(connCount),
        MaxConnections:  int32(maxConns),
        CpuUsage:        cpuUsage,
        MemoryUsage:     memUsage,
        StartedAt:       startedAt,
        Status:          result["status"],
    }

    // ==================== 4. 返回 ====================
    return &pb.GetGatewayStatusResponse{
        Meta: successMeta(ctx),
        Node: node,
    }, nil
}
```

### 8. GetGatewayNodes — 获取所有活跃网关节点列表

> 运维/监控接口 + 负载均衡入口。返回所有活跃网关节点列表，按连接数升序排列。  
> 客户端初次连接时，负载均衡器可通过此接口选择连接数最少的网关。

```go
func (s *ConnecteService) GetGatewayNodes(ctx context.Context, req *pb.GetGatewayNodesRequest) (*pb.GetGatewayNodesResponse, error) {
    // ==================== 1. 从 ZSET 获取所有网关节点 ID — 按连接数升序 ====================
    // Key: conn:gateway_list  ZSET  member=gateway_id  score=conn_count
    gatewayMembers, err := s.redis.ZRangeWithScores(ctx, "conn:gateway_list", 0, -1).Result()
    if err != nil {
        log.Error("查询网关列表失败", "err", err)
        return nil, status.Error(codes.Internal, "查询网关列表失败")
    }

    if len(gatewayMembers) == 0 {
        return &pb.GetGatewayNodesResponse{
            Meta:  successMeta(ctx),
            Nodes: nil,
        }, nil
    }

    // ==================== 2. 批量查询每个网关的详细信息 — Redis Pipeline ====================
    pipe := s.redis.Pipeline()
    type gatewayCmd struct {
        gatewayID string
        connCount int32
        cmd       *redis.MapStringStringCmd
    }
    cmds := make([]gatewayCmd, 0, len(gatewayMembers))

    for _, z := range gatewayMembers {
        gid := z.Member.(string)
        gatewayKey := fmt.Sprintf("conn:gateway:%s", gid)
        cmd := pipe.HGetAll(ctx, gatewayKey)
        cmds = append(cmds, gatewayCmd{
            gatewayID: gid,
            connCount: int32(z.Score),
            cmd:       cmd,
        })
    }

    if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
        log.Error("Pipeline 查询网关详情失败", "err", err)
        return nil, status.Error(codes.Internal, "查询网关详情失败")
    }

    // ==================== 3. 组装结果，过滤掉已过期的网关 ====================
    nodes := make([]*pb.GatewayNode, 0, len(cmds))
    staleGateways := make([]string, 0) // 已过期但还在 ZSET 中的网关

    for _, c := range cmds {
        result, err := c.cmd.Result()
        if err != nil || len(result) == 0 {
            // HASH 已过期（TTL 30s 到了），说明网关已下线
            staleGateways = append(staleGateways, c.gatewayID)
            continue
        }

        maxConns, _ := strconv.Atoi(result["max_connections"])
        cpuUsage, _ := strconv.ParseFloat(result["cpu_usage"], 64)
        memUsage, _ := strconv.ParseFloat(result["memory_usage"], 64)
        startedAt, _ := strconv.ParseInt(result["started_at"], 10, 64)

        nodes = append(nodes, &pb.GatewayNode{
            ServerId:        c.gatewayID,
            Addr:            fmt.Sprintf("%s:%s", result["addr"], result["port"]),
            ConnectionCount: c.connCount,
            MaxConnections:  int32(maxConns),
            CpuUsage:        cpuUsage,
            MemoryUsage:     memUsage,
            StartedAt:       startedAt,
            Status:          result["status"],
        })
    }

    // ==================== 4. 异步清理已过期的网关（从 ZSET 移除） ====================
    if len(staleGateways) > 0 {
        go func() {
            cleanPipe := s.redis.Pipeline()
            for _, gid := range staleGateways {
                cleanPipe.ZRem(context.Background(), "conn:gateway_list", gid)
                cleanPipe.Del(context.Background(), fmt.Sprintf("conn:gateway_conns:%s", gid))
            }
            if _, err := cleanPipe.Exec(context.Background()); err != nil {
                log.Warn("清理过期网关失败", "gateways", staleGateways, "err", err)
            } else {
                log.Info("已清理过期网关", "count", len(staleGateways), "gateways", staleGateways)
            }
        }()
    }

    // ==================== 5. 返回 ====================
    return &pb.GetGatewayNodesResponse{
        Meta:  successMeta(ctx),
        Nodes: nodes,
    }, nil
}
```

### 9. RegisterConnection — 注册连接（网关内部调用）

> **连接生命周期起点**。网关服务器在用户 WebSocket/TCP 握手成功后调用此接口注册连接。  
> 流程：校验登录会话有效性 → 写入 Redis 路由表 → 通知 Presence 上线 → 投递 Kafka 事件。  
> 如果用户在同一平台已有旧连接（如网络切换重连），会先踢掉旧连接再注册新连接。

```go
func (s *ConnecteService) RegisterConnection(ctx context.Context, req *pb.RegisterConnectionRequest) (*pb.RegisterConnectionResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id 不能为空")
    }
    if req.ConnectionId == "" {
        return nil, status.Error(codes.InvalidArgument, "connection_id 不能为空")
    }
    if req.Platform == common.PLATFORM_TYPE_UNSPECIFIED {
        return nil, status.Error(codes.InvalidArgument, "platform 不能为空")
    }
    if req.DeviceId == "" {
        return nil, status.Error(codes.InvalidArgument, "device_id 不能为空")
    }
    if req.ServerId == "" {
        return nil, status.Error(codes.InvalidArgument, "server_id（网关节点 ID）不能为空")
    }

    now := time.Now().UnixMilli()

    // ==================== 2. 校验登录会话 — RPC Session.GetLoginSession ====================
    // 确保用户持有有效的登录 Token，防止未登录的连接注册
    sessionResp, err := s.sessionClient.GetLoginSession(ctx, &session_pb.GetLoginSessionRequest{
        UserId:   req.UserId,
        DeviceId: req.DeviceId,
    })
    if err != nil {
        log.Error("校验登录会话失败",
            "user_id", req.UserId, "device_id", req.DeviceId, "err", err)
        return nil, status.Error(codes.Unauthenticated, "登录会话校验失败")
    }
    if sessionResp.Session == nil || sessionResp.Session.Status != 1 {
        log.Warn("登录会话无效或已过期",
            "user_id", req.UserId, "device_id", req.DeviceId)
        return nil, status.Error(codes.Unauthenticated, "登录会话无效或已过期")
    }

    // ==================== 3. 检查同平台旧连接 — Redis ====================
    // Key: conn:user:{user_id}:{platform}
    // 如果存在旧连接（如网络切换），先踢掉旧连接
    connKey := fmt.Sprintf("conn:user:%s:%d", req.UserId, int(req.Platform))
    oldConnID, err := s.redis.HGet(ctx, connKey, "conn_id").Result()
    var oldGatewayID string
    if err == nil && oldConnID != "" && oldConnID != req.ConnectionId {
        // 存在旧连接，执行踢出
        oldGatewayID, _ = s.redis.HGet(ctx, connKey, "gateway_id").Result()
        log.Info("同平台旧连接存在，执行踢出",
            "user_id", req.UserId,
            "platform", req.Platform,
            "old_conn_id", oldConnID,
            "new_conn_id", req.ConnectionId)

        // 异步踢旧连接（不阻塞新连接注册）
        go func() {
            _, kickErr := s.KickConnection(context.Background(), &pb.KickConnectionRequest{
                UserId:       req.UserId,
                ConnectionId: oldConnID,
                Reason:       "new_connection_same_platform",
            })
            if kickErr != nil {
                log.Warn("踢旧连接失败（不影响新连接）",
                    "user_id", req.UserId, "old_conn_id", oldConnID, "err", kickErr)
            }
        }()
    }

    // ==================== 4. 获取网关节点地址（用于填充连接信息） ====================
    gatewayKey := fmt.Sprintf("conn:gateway:%s", req.ServerId)
    gatewayAddr, _ := s.redis.HGet(ctx, gatewayKey, "addr").Result()
    gatewayPort, _ := s.redis.HGet(ctx, gatewayKey, "port").Result()
    serverAddr := fmt.Sprintf("%s:%s", gatewayAddr, gatewayPort)

    // ==================== 5. 写入 Redis 路由表 — Pipeline 原子操作 ====================
    connTTL := 5 * time.Minute // 心跳续期周期内必须刷新

    pipe := s.redis.Pipeline()

    // 5a. 写入用户指定平台连接信息
    // Key: conn:user:{user_id}:{platform}  HASH
    pipe.HSet(ctx, connKey, map[string]interface{}{
        "gateway_id":     req.ServerId,
        "conn_id":        req.ConnectionId,
        "device_id":      req.DeviceId,
        "connected_at":   now,
        "last_heartbeat": now,
        "client_ip":      req.ClientIp,
        "server_addr":    serverAddr,
    })
    pipe.Expire(ctx, connKey, connTTL)

    // 5b. 添加到用户连接索引
    // Key: conn:user_all:{user_id}  SET
    allKey := fmt.Sprintf("conn:user_all:%s", req.UserId)
    // 先移除旧的同平台 member（如果有）
    if oldConnID != "" && oldGatewayID != "" {
        oldMember := fmt.Sprintf("%d:%s:%s", int(req.Platform), oldGatewayID, oldConnID)
        pipe.SRem(ctx, allKey, oldMember)
    }
    newMember := fmt.Sprintf("%d:%s:%s", int(req.Platform), req.ServerId, req.ConnectionId)
    pipe.SAdd(ctx, allKey, newMember)
    pipe.Expire(ctx, allKey, connTTL)

    // 5c. 添加到网关连接索引
    // Key: conn:gateway_conns:{gateway_id}  SET
    gatewayConnsKey := fmt.Sprintf("conn:gateway_conns:%s", req.ServerId)
    gatewayMember := fmt.Sprintf("%s:%d", req.UserId, int(req.Platform))
    pipe.SAdd(ctx, gatewayConnsKey, gatewayMember)
    // 不设置 TTL，由网关心跳续期

    // 5d. 更新网关连接计数
    // Key: conn:gateway_list  ZSET  member=gateway_id  score=conn_count
    pipe.ZIncrBy(ctx, "conn:gateway_list", 1, req.ServerId)

    if _, err := pipe.Exec(ctx); err != nil {
        log.Error("写入连接路由信息失败",
            "user_id", req.UserId, "connection_id", req.ConnectionId, "err", err)
        return nil, status.Error(codes.Internal, "注册连接路由失败")
    }

    // ==================== 6. 通知 Presence 用户上线 — RPC ====================
    _, err = s.presenceClient.UserOnline(ctx, &presence_pb.UserOnlineRequest{
        UserId:   req.UserId,
        Platform: req.Platform,
        DeviceId: req.DeviceId,
    })
    if err != nil {
        log.Warn("通知 Presence 用户上线失败（不阻塞连接注册）",
            "user_id", req.UserId, "err", err)
        // Presence 通知失败不影响连接注册，Kafka 事件会做兜底
    }

    // ==================== 7. 投递 Kafka conn.connected 事件 ====================
    connectedEvent := &kafka_connecte.ConnectedEvent{
        Header:   buildEventHeader("connecte", s.instanceID),
        UserId:   req.UserId,
        DeviceId: req.DeviceId,
        Platform: req.Platform,
        Gateway:  req.ServerId,
        Ip:       req.ClientIp,
        ConnTime: now,
    }
    if err := s.kafka.Produce(ctx, "conn.connected", req.UserId, connectedEvent); err != nil {
        log.Error("投递 conn.connected 事件失败",
            "user_id", req.UserId, "err", err)
    }

    // ==================== 8. 如果踢了旧连接，投递路由变更事件 ====================
    if oldConnID != "" && oldGatewayID != "" && oldGatewayID != req.ServerId {
        routeEvent := &kafka_connecte.ConnRouteUpdatedEvent{
            Header:     buildEventHeader("connecte", s.instanceID),
            UserId:     req.UserId,
            DeviceId:   req.DeviceId,
            Platform:   req.Platform,
            OldGateway: oldGatewayID,
            NewGateway: req.ServerId,
            UpdateTime: now,
        }
        if err := s.kafka.Produce(ctx, "conn.route.updated", req.UserId, routeEvent); err != nil {
            log.Error("投递 conn.route.updated 事件失败",
                "user_id", req.UserId, "err", err)
        }
    }

    log.Info("连接注册成功",
        "user_id", req.UserId,
        "connection_id", req.ConnectionId,
        "platform", req.Platform,
        "device_id", req.DeviceId,
        "server_id", req.ServerId,
        "client_ip", req.ClientIp)

    // ==================== 9. 返回 ====================
    return &pb.RegisterConnectionResponse{
        Meta: successMeta(ctx),
    }, nil
}
```

### 10. UnregisterConnection — 注销连接（网关内部调用）

> **连接生命周期终点**。网关服务器在用户 WebSocket/TCP 连接断开后调用此接口注销连接。  
> 断开原因包括：客户端主动关闭、心跳超时、网络异常、服务端踢出等。  
> 流程：校验连接归属 → 清理 Redis 路由表 → 检查用户是否还有其他在线连接 → 通知 Presence → 投递 Kafka 事件。

```go
func (s *ConnecteService) UnregisterConnection(ctx context.Context, req *pb.UnregisterConnectionRequest) (*pb.UnregisterConnectionResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id 不能为空")
    }
    if req.ConnectionId == "" {
        return nil, status.Error(codes.InvalidArgument, "connection_id 不能为空")
    }

    now := time.Now().UnixMilli()

    // ==================== 2. 查找连接信息 — Redis ====================
    // 遍历用户连接索引，找到匹配 connection_id 的条目
    allKey := fmt.Sprintf("conn:user_all:%s", req.UserId)
    members, err := s.redis.SMembers(ctx, allKey).Result()
    if err != nil {
        log.Error("查询用户连接索引失败", "user_id", req.UserId, "err", err)
        return nil, status.Error(codes.Internal, "查询连接信息失败")
    }

    var targetPlatform string
    var targetGatewayID string
    var targetMember string
    for _, member := range members {
        parts := strings.SplitN(member, ":", 3)
        if len(parts) == 3 && parts[2] == req.ConnectionId {
            targetPlatform = parts[0]
            targetGatewayID = parts[1]
            targetMember = member
            break
        }
    }

    if targetGatewayID == "" {
        // 连接已经不在路由表中（可能已被踢出或超时清理）
        log.Warn("注销连接时未找到路由信息",
            "user_id", req.UserId, "connection_id", req.ConnectionId)
        return &pb.UnregisterConnectionResponse{
            Meta: successMeta(ctx),
        }, nil
    }

    // ==================== 3. 获取连接详情（用于事件投递） ====================
    connKey := fmt.Sprintf("conn:user:%s:%s", req.UserId, targetPlatform)
    connInfo, _ := s.redis.HGetAll(ctx, connKey).Result()
    var deviceID string
    var connectedAt int64
    if len(connInfo) > 0 {
        deviceID = connInfo["device_id"]
        connectedAt, _ = strconv.ParseInt(connInfo["connected_at"], 10, 64)
    }

    // 确认 Redis 中存储的 conn_id 与请求一致（防止并发注册覆盖后误删新连接）
    if connInfo["conn_id"] != "" && connInfo["conn_id"] != req.ConnectionId {
        log.Warn("连接 ID 不匹配（可能已被新连接覆盖），跳过路由清理",
            "user_id", req.UserId,
            "request_conn_id", req.ConnectionId,
            "current_conn_id", connInfo["conn_id"])
        // 仍然从 SET 中移除旧 member
        s.redis.SRem(ctx, allKey, targetMember)
        return &pb.UnregisterConnectionResponse{
            Meta: successMeta(ctx),
        }, nil
    }

    // ==================== 4. 清理 Redis 路由表 ====================
    s.cleanupConnectionRoute(ctx, req.UserId, targetPlatform, req.ConnectionId, targetGatewayID)

    // ==================== 5. 检查用户是否还有其他在线连接 ====================
    // 如果用户所有平台都断开了，通知 Presence 用户完全下线
    remainingMembers, _ := s.redis.SMembers(ctx, allKey).Result()
    platformInt, _ := strconv.Atoi(targetPlatform)

    // 通知 Presence 该平台下线
    _, err = s.presenceClient.UserOffline(ctx, &presence_pb.UserOfflineRequest{
        UserId:   req.UserId,
        Platform: common.PlatformType(platformInt),
    })
    if err != nil {
        log.Warn("通知 Presence 用户下线失败（不阻塞注销流程）",
            "user_id", req.UserId, "platform", targetPlatform, "err", err)
    }

    // ==================== 6. 投递 Kafka conn.disconnected 事件 ====================
    duration := int64(0)
    if connectedAt > 0 {
        duration = now - connectedAt
    }

    disconnEvent := &kafka_connecte.DisconnectedEvent{
        Header:      buildEventHeader("connecte", s.instanceID),
        UserId:      req.UserId,
        DeviceId:    deviceID,
        Platform:    common.PlatformType(platformInt),
        Gateway:     targetGatewayID,
        Reason:      "client_disconnect", // 正常断开，kick 场景在 KickConnection 中投递
        DisconnTime: now,
        Duration:    duration,
    }
    if err := s.kafka.Produce(ctx, "conn.disconnected", req.UserId, disconnEvent); err != nil {
        log.Error("投递 conn.disconnected 事件失败",
            "user_id", req.UserId, "err", err)
    }

    log.Info("连接注销成功",
        "user_id", req.UserId,
        "connection_id", req.ConnectionId,
        "platform", targetPlatform,
        "device_id", deviceID,
        "gateway_id", targetGatewayID,
        "duration_ms", duration,
        "remaining_connections", len(remainingMembers))

    // ==================== 7. 返回 ====================
    return &pb.UnregisterConnectionResponse{
        Meta: successMeta(ctx),
    }, nil
}
```

---

## 辅助方法

### successMeta — 构建成功响应元信息

```go
func successMeta(ctx context.Context) *common.ResponseMeta {
    return &common.ResponseMeta{
        Code:       0,
        Message:    "success",
        ServerTime: time.Now().UnixMilli(),
        TraceId:    extractTraceID(ctx),
    }
}
```

### buildEventHeader — 构建 Kafka 事件头

```go
func buildEventHeader(source, instanceID string) *common.EventHeader {
    return &common.EventHeader{
        EventId:   uuid.New().String(),
        TraceId:   "",  // 从 ctx 中提取
        SpanId:    "",
        Source:    source,
        SourceId:  instanceID,
        Timestamp: time.Now().UnixMilli(),
        Version:   1,
    }
}
```

### ConnManager — 连接路由本地缓存管理器

```go
// ConnManager 维护 conn_id → route 的本地缓存，用于 BatchSendToConnections 高速查找
// 数据来源：RegisterConnection 写入，UnregisterConnection 删除
// 本地缓存 + Redis 双写，保证最终一致
type ConnManager struct {
    mu     sync.RWMutex
    routes map[string]*ConnRoute // conn_id → route
}

type ConnRoute struct {
    GatewayID string
    UserID    string
    Platform  int
}

func (m *ConnManager) GetRoute(connID string) (*ConnRoute, bool) {
    m.mu.RLock()
    defer m.mu.RUnlock()
    r, ok := m.routes[connID]
    return r, ok
}

func (m *ConnManager) SetRoute(connID string, route *ConnRoute) {
    m.mu.Lock()
    defer m.mu.Unlock()
    m.routes[connID] = route
}

func (m *ConnManager) RemoveRoute(connID string) {
    m.mu.Lock()
    defer m.mu.Unlock()
    delete(m.routes, connID)
}
```

### 网关心跳与连接续期（定时任务）

> 网关节点每 10s 上报一次自身状态，每个连接每 30s 通过心跳续期 Redis TTL。  
> 如果心跳超时（5min 无续期），Redis Key 自动过期，连接视为断开。

```go
// startHeartbeatLoop 网关节点定时上报状态 + 续期所有连接
// 由网关进程启动时调用，在后台 goroutine 中运行
func (s *ConnecteService) startHeartbeatLoop(gatewayID string) {
    ticker := time.NewTicker(10 * time.Second)
    defer ticker.Stop()

    for {
        select {
        case <-s.stopCh:
            return
        case <-ticker.C:
            ctx := context.Background()

            // ---- 1. 上报网关节点状态 ----
            // Key: conn:gateway:{gateway_id}  HASH  TTL: 30s
            gatewayKey := fmt.Sprintf("conn:gateway:%s", gatewayID)
            stats := s.collectGatewayStats() // 收集 CPU/内存/连接数
            pipe := s.redis.Pipeline()
            pipe.HSet(ctx, gatewayKey, map[string]interface{}{
                "addr":            s.config.AdvertiseAddr,
                "port":            s.config.AdvertisePort,
                "conn_count":      stats.ConnCount,
                "max_connections": s.config.MaxConnections,
                "cpu_usage":       stats.CPUUsage,
                "memory_usage":    stats.MemoryUsage,
                "status":          "running",
                "last_heartbeat":  time.Now().UnixMilli(),
                "started_at":      s.startedAt,
            })
            pipe.Expire(ctx, gatewayKey, 30*time.Second)

            // 更新 ZSET 中的连接数（用于负载均衡）
            pipe.ZAddArgs(ctx, "conn:gateway_list", redis.ZAddArgs{
                Members: []redis.Z{{Score: float64(stats.ConnCount), Member: gatewayID}},
            })

            // 续期网关连接索引
            gatewayConnsKey := fmt.Sprintf("conn:gateway_conns:%s", gatewayID)
            pipe.Expire(ctx, gatewayConnsKey, 30*time.Second)

            if _, err := pipe.Exec(ctx); err != nil {
                log.Error("网关心跳上报失败", "gateway_id", gatewayID, "err", err)
            }

            // ---- 2. 续期所有用户连接 ----
            // 遍历网关上的所有连接，刷新 Redis TTL
            connMembers, err := s.redis.SMembers(ctx, gatewayConnsKey).Result()
            if err != nil {
                log.Error("查询网关连接列表失败", "gateway_id", gatewayID, "err", err)
                continue
            }

            renewPipe := s.redis.Pipeline()
            for _, member := range connMembers {
                // member 格式: "user_id:platform"
                parts := strings.SplitN(member, ":", 2)
                if len(parts) != 2 {
                    continue
                }
                userID := parts[0]
                platform := parts[1]

                // 续期用户平台连接 HASH
                connKey := fmt.Sprintf("conn:user:%s:%s", userID, platform)
                renewPipe.Expire(ctx, connKey, 5*time.Minute)
                renewPipe.HSet(ctx, connKey, "last_heartbeat", time.Now().UnixMilli())

                // 续期用户连接索引 SET
                allKey := fmt.Sprintf("conn:user_all:%s", userID)
                renewPipe.Expire(ctx, allKey, 5*time.Minute)
            }

            if len(connMembers) > 0 {
                if _, err := renewPipe.Exec(ctx); err != nil {
                    log.Error("续期连接 TTL 失败",
                        "gateway_id", gatewayID,
                        "conn_count", len(connMembers),
                        "err", err)
                }
            }

            // ---- 3. 更新全局在线统计 ----
            // Key: conn:total_online  STRING
            // 汇总所有网关的连接数
            go s.updateTotalOnlineCount(context.Background())
        }
    }
}

// updateTotalOnlineCount 汇总全局在线连接数
func (s *ConnecteService) updateTotalOnlineCount(ctx context.Context) {
    // 从 ZSET 汇总所有网关的连接数之和
    gatewayMembers, err := s.redis.ZRangeWithScores(ctx, "conn:gateway_list", 0, -1).Result()
    if err != nil {
        return
    }

    totalOnline := int64(0)
    for _, z := range gatewayMembers {
        totalOnline += int64(z.Score)
    }

    // Key: conn:total_online  STRING
    s.redis.Set(ctx, "conn:total_online", totalOnline, 0)
}
```

---

## 可观测性接入（OpenTelemetry）

> Connecte 是长连接网关，需重点观测：WebSocket 连接数、握手耗时、sync notify 投递成功率、按平台连接分布、连接异常断开率。

### 第一步：服务启动时初始化 OTel SDK

```go
func main() {
    ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
    defer cancel()

    otelShutdown := observability.InitOpenTelemetry(ctx, "connecte", "v1.0.0")
    defer otelShutdown(ctx)

    observability.SetupLogger("connecte")
    observability.RegisterRedisPoolMetrics(redisClient, "connecte")
    // ...
}
```

> 共享包 `InitOpenTelemetry` 的完整实现见 `observability_global.md` 第 1.1 节。

### 第二步：gRPC Server/Client Interceptor

```go
// Server 端 — 接收 Push 服务的 SendToConnection / BatchSendToConnections 请求
grpcServer := grpc.NewServer(
    grpc.ChainUnaryInterceptor(otelgrpc.UnaryServerInterceptor()),
    grpc.ChainStreamInterceptor(otelgrpc.StreamServerInterceptor()),
)

// Client 端 — 调用 Auth.VerifyToken / Session.GetLoginSession / Presence.UserOnline
authConn := grpc.Dial("auth:50051",
    grpc.WithChainUnaryInterceptor(otelgrpc.UnaryClientInterceptor()),
)
sessionConn := grpc.Dial("session:50051",
    grpc.WithChainUnaryInterceptor(otelgrpc.UnaryClientInterceptor()),
)
presenceConn := grpc.Dial("presence:50051",
    grpc.WithChainUnaryInterceptor(otelgrpc.UnaryClientInterceptor()),
)
```

### 第三步：WebSocket 连接生命周期埋点

WebSocket 握手、连接建立、断开是 Connecte 的核心流程，需要在连接管理器中手动创建 span 和记录指标。

```go
var meter = otel.Meter("im-chat/connecte")
var tracer = otel.Tracer("im-chat/connecte")

var (
    // 活跃连接数（UpDownCounter，连接时 +1，断开时 -1）
    activeConnections, _ = meter.Int64UpDownCounter("connecte.active_connections",
        metric.WithDescription("当前活跃 WebSocket 连接数"))

    // 按平台连接数
    connectionsByPlatform, _ = meter.Int64UpDownCounter("connecte.connections_by_platform",
        metric.WithDescription("按平台分类的活跃连接数"))

    // 握手耗时（从 HTTP Upgrade 到 WebSocket 连接建立完成）
    handshakeDuration, _ = meter.Float64Histogram("connecte.handshake_duration_ms",
        metric.WithDescription("WebSocket 握手耗时"), metric.WithUnit("ms"))

    // Sync Notify 投递计数
    syncNotifySent, _ = meter.Int64Counter("connecte.sync_notify_sent_total",
        metric.WithDescription("发送给客户端的 sync notify 总数"))

    // Sync Notify 投递失败计数
    syncNotifyFailed, _ = meter.Int64Counter("connecte.sync_notify_failed_total",
        metric.WithDescription("sync notify 投递失败总数（连接已断开等）"))

    // 连接断开原因分布
    disconnectCounter, _ = meter.Int64Counter("connecte.disconnects_total",
        metric.WithDescription("连接断开总数（按原因分类）"))

    // 网关在线连接数（定时任务上报到 Redis 的值）
    gatewayOnlineGauge, _ = meter.Int64Gauge("connecte.gateway_online_count",
        metric.WithDescription("本网关节点的在线连接数"))
)
```

在业务代码中埋点：

```go
// RegisterConnection —— 新连接建立
func (s *ConnecteServer) RegisterConnection(ctx context.Context, req *pb.RegisterConnectionRequest) {
    handshakeStart := time.Now()

    // ... 验证 token、注册到 Redis 路由表 ...

    handshakeDuration.Record(ctx, float64(time.Since(handshakeStart).Milliseconds()))
    activeConnections.Add(ctx, 1)
    connectionsByPlatform.Add(ctx, 1, metric.WithAttributes(
        attribute.String("platform", req.Platform.String()),
    ))

    slog.InfoContext(ctx, "connection registered",
        "user_id", req.UserId,
        "platform", req.Platform.String(),
        "gateway_id", s.gatewayID,
    )
}

// UnregisterConnection —— 连接断开
func (s *ConnecteServer) UnregisterConnection(ctx context.Context, req *pb.UnregisterConnectionRequest) {
    activeConnections.Add(ctx, -1)
    connectionsByPlatform.Add(ctx, -1, metric.WithAttributes(
        attribute.String("platform", req.Platform.String()),
    ))
    disconnectCounter.Add(ctx, 1, metric.WithAttributes(
        attribute.String("reason", req.DisconnectReason),  // normal / timeout / kicked / error
    ))
}

// SendToConnection —— sync notify 投递
func (s *ConnecteServer) SendToConnection(ctx context.Context, req *pb.SendToConnectionRequest) {
    ctx, span := tracer.Start(ctx, "SendToConnection",
        trace.WithAttributes(attribute.String("user_id", req.UserId)),
    )
    defer span.End()

    err := s.connManager.Send(ctx, req.ConnectionId, req.Payload)
    if err != nil {
        syncNotifyFailed.Add(ctx, 1)
        span.SetStatus(codes.Error, err.Error())
    } else {
        syncNotifySent.Add(ctx, 1)
    }
}

// 网关心跳定时任务中上报在线数
gatewayOnlineGauge.Record(ctx, totalOnline)
```

### 第四步：Kafka Producer/Consumer 埋点

Connecte 服务向 Kafka 发布 `conn.user.online` / `conn.user.offline` 事件，消费 `session.kicked` 等事件。使用共享包函数：

```go
// Producer：发布事件时创建 PRODUCER span + 注入 trace context
headers := make([]kafka.Header, 0)
ctx, span := observability.StartKafkaProducerSpan(ctx, "conn.user.online", &headers)
defer span.End()
// ... kafkaWriter.WriteMessages(ctx, kafka.Message{Headers: headers, ...})

// Consumer：消费事件时从 headers 提取 trace context + 创建 CONSUMER span
ctx, span := observability.StartKafkaConsumerSpan(ctx, msg.Topic, msg.Headers)
defer span.End()
```

> 共享包函数见 `observability_global.md` 第 1.3 节。

### 第五步：结构化 JSON 日志

与所有服务一致。注意 Connecte 的高频操作（heartbeat）应使用 DEBUG 级别或跳过日志，避免日志量爆炸。

```go
// heartbeat 使用 DEBUG 级别，生产环境日志级别设为 INFO 时自动跳过
slog.DebugContext(ctx, "heartbeat received", "user_id", userID, "conn_id", connID)

// 连接建立/断开使用 INFO 级别
slog.InfoContext(ctx, "connection registered", "user_id", userID, "platform", platform)
```

### 接入要点总结

| 步骤 | 改动位置 | 效果 |
|------|---------|------|
| 1. `observability.InitOpenTelemetry` | main() | TracerProvider + MeterProvider + Runtime Metrics |
| 2. gRPC Interceptor | Server + Client (Auth/Session/Presence) | RPC 自动 trace + RED 指标 |
| 3. WebSocket 生命周期埋点 | Register/Unregister/Send | 连接数、握手耗时、投递成功率 |
| 4. Kafka 埋点 | `StartKafkaProducerSpan` / `StartKafkaConsumerSpan` | 链路追踪贯穿 |
| 5. `observability.SetupLogger` | main() | 日志自动带 trace_id + 动态级别 |
| 6. `observability.BuildEventHeader` | 辅助函数 | EventHeader 携带真实 trace context |
| 7. 基础设施指标 | `RegisterRedisPoolMetrics` | Redis 连接池可观测 |
| 8. Span 错误记录 | `observability.RecordError` | 错误事件关联到 trace |

### 补充：buildEventHeader 改造

将现有 `buildEventHeader(source, instanceID)` 调用替换为 `observability.BuildEventHeader(ctx, source, instanceID)`，确保 Kafka 事件头中 `TraceId` / `SpanId` 不再是空字符串。

### 补充：基础设施指标注册

> ✅ 已合并到第一步 `main()` 中。Connecte 服务依赖 Redis，通过 `observability.RegisterRedisPoolMetrics` 注册连接池指标。

### 补充：Span 错误记录

所有返回 error 的代码路径加入 `observability.RecordError(span, err)`：

```go
if err != nil {
    observability.RecordError(span, err)
    return nil, status.Errorf(codes.Internal, "...")
}
```
