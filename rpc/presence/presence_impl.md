# Presence 在线状态服务 — RPC 接口实现伪代码

## 概述

Presence 服务是整个 IM 系统的**在线状态唯一权威来源**，负责用户在线/离线/忙碌/离开/隐身状态管理、自定义状态文案、正在输入提示、状态订阅推送。

**核心设计原则：**
- **纯实时状态服务**：所有数据全部存储在 Redis，无 PgSQL 持久化，节点完全无状态
- **极致性能**：Heartbeat 为最热路径，10M 用户 ≈ 166K heartbeats/sec，仅执行 Redis SET + EXPIRE，禁止任何阻塞 I/O
- **离线优雅期**：用户断线后保留 30s 优雅期，期间重连不广播离线事件，避免网络抖动导致状态闪烁
- **多端感知**：同一用户可在多个平台同时在线，任一平台在线即视为在线，全部平台离线才广播 offline
- **订阅驱动**：状态变更仅推送给订阅者（通常是好友），不做全局广播，控制扇出量
- **HyperLogLog 计数**：在线总数使用 HyperLogLog 统计，O(1) 空间、O(1) 时间，误差 < 0.81%

## 外部依赖

| 依赖类型 | 依赖项 | 用途 |
|---------|--------|------|
| PgSQL | **无** | 纯实时状态服务，无持久化需求 |
| Redis | 状态存储 / 订阅关系 / 在线计数 / 优雅期 / 输入提示 | 全部运行时状态存储 |
| RPC | **无** | Presence 是被调用方，不主动调用其他服务 |
| Kafka | presence.online / presence.offline / presence.status.changed / presence.typing | 事件通知下游（Push 推送给订阅者） |

## PgSQL 表结构

```
无 — Presence 是纯实时状态服务，所有数据存储在 Redis 中。
用户离线后状态自然过期（TTL），无需持久化。
历史在线记录由 Audit 服务通过消费 Kafka 事件自行存储。
```

## Redis Key 设计

| Key 格式 | 类型 | 值 | TTL | 说明 |
|----------|------|-----|-----|------|
| `presence:status:{user_id}` | HASH | {status, custom_text, platform, device_id, server_id, last_active, updated_at} | 6min（心跳每 60s 续期） | 用户在线状态详情，心跳持续续期，6min 无心跳自动过期 |
| `presence:user_platforms:{user_id}` | SET | member="platform:device_id:server_id" | 6min（心跳续期） | 用户所有在线平台索引，用于多端判断 |
| `presence:online` | HYPERLOGLOG | user_id | 无 TTL（每 10min 重建） | 全局在线用户计数，PFADD/PFCOUNT，误差 < 0.81% |
| `presence:online:platform:{platform}` | HYPERLOGLOG | user_id | 无 TTL（每 10min 重建） | 分平台在线计数 |
| `presence:subs:{user_id}` | SET | subscriber_user_id 集合 | 24h | 谁在订阅此用户的状态变更（被观察者视角） |
| `presence:my_subs:{user_id}` | SET | target_user_id 集合 | 24h | 此用户订阅了谁的状态（观察者视角） |
| `presence:typing:{conversation_id}:{user_id}` | STRING | action（typing/recording_voice/uploading_file） | 5s | 正在输入指示器，5s 自动过期 |
| `presence:offline_grace:{user_id}` | STRING | "platform:device_id" | 30s | 离线优雅期，30s 内重连不广播 offline 事件 |
| `presence:kafka:dedup:{event_id}` | STRING | "1" | 24h | Kafka 消费幂等去重 |

## 在线状态枚举

| 值 | 名称 | 说明 |
|----|------|------|
| 0 | UNSPECIFIED | 未指定 |
| 1 | ONLINE | 在线 |
| 2 | OFFLINE | 离线 |
| 3 | BUSY | 忙碌（用户手动设置） |
| 4 | AWAY | 离开（用户手动设置或自动检测） |
| 5 | INVISIBLE | 隐身（对外显示为离线，但实际在线） |

---

## 接口实现

### 1. SetPresence — 设置用户自定义状态

> 用户主动设置在线状态（忙碌/离开/隐身等）和自定义状态文案。  
> 流程：参数校验 → 更新 Redis → 生产 Kafka 状态变更事件 → 返回。  
> 隐身状态特殊处理：实际在线但对订阅者显示为离线。

```go
func (s *PresenceService) SetPresence(ctx context.Context, req *pb.SetPresenceRequest) (*pb.SetPresenceResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id 不能为空")
    }
    if req.Status == common.ONLINE_STATUS_UNSPECIFIED {
        return nil, status.Error(codes.InvalidArgument, "status 不能为空")
    }
    if req.Platform == common.PLATFORM_TYPE_UNSPECIFIED {
        return nil, status.Error(codes.InvalidArgument, "platform 不能为空")
    }
    // 自定义状态文案长度限制
    if len(req.CustomStatus) > 100 {
        return nil, status.Error(codes.InvalidArgument, "custom_status 过长，最多 100 字符")
    }
    // 不允许通过 SetPresence 设置 OFFLINE（离线由 UserOffline 管理）
    if req.Status == common.ONLINE_STATUS_OFFLINE {
        return nil, status.Error(codes.InvalidArgument, "不允许手动设置 OFFLINE，请使用 UserOffline 接口")
    }

    now := time.Now().UnixMilli()

    // ==================== 2. 读取当前状态（用于 Kafka 事件的 old_status） ====================
    // Key: presence:status:{user_id}  HASH
    statusKey := fmt.Sprintf("presence:status:%s", req.UserId)
    oldStatusStr, _ := s.redis.HGet(ctx, statusKey, "status").Result()
    oldStatus := common.ONLINE_STATUS_OFFLINE // 默认旧状态为离线
    if oldStatusStr != "" {
        oldStatusInt, _ := strconv.Atoi(oldStatusStr)
        oldStatus = common.OnlineStatus(oldStatusInt)
    }

    // ==================== 3. 更新 Redis — 用户状态 HASH ====================
    // Key: presence:status:{user_id}  HASH  TTL: 6min
    // 使用 Pipeline 原子操作：更新字段 + 续期 TTL
    pipe := s.redis.Pipeline()
    pipe.HSet(ctx, statusKey, map[string]interface{}{
        "status":      int(req.Status),
        "custom_text": req.CustomStatus,
        "platform":    int(req.Platform),
        "device_id":   req.DeviceId,
        "last_active": now,
        "updated_at":  now,
    })
    pipe.Expire(ctx, statusKey, 6*time.Minute) // 心跳会持续续期

    // 续期平台集合 TTL
    platformsKey := fmt.Sprintf("presence:user_platforms:%s", req.UserId)
    pipe.Expire(ctx, platformsKey, 6*time.Minute)

    if _, err := pipe.Exec(ctx); err != nil {
        log.Error("更新用户状态 Redis 失败",
            "user_id", req.UserId, "status", req.Status, "err", err)
        return nil, status.Error(codes.Internal, "更新状态失败")
    }

    // ==================== 4. 生产 Kafka 状态变更事件 ====================
    // 仅当状态实际发生变化时生产事件
    if oldStatus != req.Status || oldStatusStr == "" {
        statusChangedEvent := &kafka_presence.PresenceStatusChangedEvent{
            Header:       buildEventHeader("presence", s.instanceID),
            UserId:       req.UserId,
            OldStatus:    oldStatus,
            NewStatus:    req.Status,
            CustomStatus: req.CustomStatus,
            Platform:     req.Platform,
            ChangeTime:   now,
        }
        if err := s.kafka.Produce(ctx, "presence.status.changed", req.UserId, statusChangedEvent); err != nil {
            log.Error("生产 presence.status.changed 事件失败",
                "user_id", req.UserId, "err", err)
            // 不阻塞主流程
        }
    }

    // ==================== 5. 返回 ====================
    return &pb.SetPresenceResponse{
        Meta: successMeta(ctx),
    }, nil
}
```

### 2. GetPresence — 获取单个用户在线状态

> 查询指定用户的在线状态信息。全走 Redis HGETALL，延迟 < 1ms。  
> 如果目标用户设置了隐身（INVISIBLE），对外返回 OFFLINE。  
> 返回用户在所有在线平台上的状态列表。

```go
func (s *PresenceService) GetPresence(ctx context.Context, req *pb.GetPresenceRequest) (*pb.GetPresenceResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id 不能为空")
    }

    // ==================== 2. 查询 Redis — 用户状态 HASH ====================
    // Key: presence:status:{user_id}  HASH
    statusKey := fmt.Sprintf("presence:status:%s", req.UserId)
    result, err := s.redis.HGetAll(ctx, statusKey).Result()
    if err != nil {
        log.Error("查询用户状态失败", "user_id", req.UserId, "err", err)
        return nil, status.Error(codes.Internal, "查询状态失败")
    }

    // 未找到状态 → 用户离线
    if len(result) == 0 {
        return &pb.GetPresenceResponse{
            Meta:      successMeta(ctx),
            Presences: []*pb.PresenceInfo{
                {
                    UserId: req.UserId,
                    Status: common.ONLINE_STATUS_OFFLINE,
                },
            },
        }, nil
    }

    // ==================== 3. 查询用户所有在线平台 — Redis SET ====================
    // Key: presence:user_platforms:{user_id}  SET
    // Members: "platform:device_id:server_id"
    platformsKey := fmt.Sprintf("presence:user_platforms:%s", req.UserId)
    members, err := s.redis.SMembers(ctx, platformsKey).Result()
    if err != nil {
        log.Error("查询用户平台列表失败", "user_id", req.UserId, "err", err)
        // 降级：仅返回主状态
        members = nil
    }

    // ==================== 4. 解析状态 ====================
    statusInt, _ := strconv.Atoi(result["status"])
    userStatus := common.OnlineStatus(statusInt)
    lastActive, _ := strconv.ParseInt(result["last_active"], 10, 64)
    customText := result["custom_text"]

    // 隐身处理：对外展示为 OFFLINE，隐藏 custom_text
    if userStatus == common.ONLINE_STATUS_INVISIBLE {
        return &pb.GetPresenceResponse{
            Meta:      successMeta(ctx),
            Presences: []*pb.PresenceInfo{
                {
                    UserId:     req.UserId,
                    Status:     common.ONLINE_STATUS_OFFLINE,
                    LastActive: lastActive,
                },
            },
        }, nil
    }

    // ==================== 5. 组装各平台的在线状态列表 ====================
    presences := make([]*pb.PresenceInfo, 0, len(members)+1)

    if len(members) > 0 {
        for _, member := range members {
            // member 格式: "platform:device_id:server_id"
            parts := strings.SplitN(member, ":", 3)
            if len(parts) < 3 {
                continue
            }
            platformInt, _ := strconv.Atoi(parts[0])
            presences = append(presences, &pb.PresenceInfo{
                UserId:       req.UserId,
                Status:       userStatus,
                Platform:     common.PlatformType(platformInt),
                DeviceId:     parts[1],
                ServerId:     parts[2],
                LastActive:   lastActive,
                CustomStatus: customText,
            })
        }
    } else {
        // 降级：无平台信息，仅返回主状态
        platformInt, _ := strconv.Atoi(result["platform"])
        presences = append(presences, &pb.PresenceInfo{
            UserId:       req.UserId,
            Status:       userStatus,
            Platform:     common.PlatformType(platformInt),
            DeviceId:     result["device_id"],
            LastActive:   lastActive,
            CustomStatus: customText,
        })
    }

    // ==================== 6. 返回 ====================
    return &pb.GetPresenceResponse{
        Meta:      successMeta(ctx),
        Presences: presences,
    }, nil
}
```

### 3. BatchGetPresence — 批量获取在线状态

> 批量查询多个用户的在线状态，用于好友列表、群成员列表展示。  
> 使用 Redis Pipeline 批量查询，单次请求最多 200 个用户。  
> **性能目标**：200 用户 < 5ms（Pipeline 合并网络往返）。

```go
func (s *PresenceService) BatchGetPresence(ctx context.Context, req *pb.BatchGetPresenceRequest) (*pb.BatchGetPresenceResponse, error) {
    // ==================== 1. 参数校验 ====================
    if len(req.UserIds) == 0 {
        return nil, status.Error(codes.InvalidArgument, "user_ids 不能为空")
    }
    if len(req.UserIds) > 200 {
        return nil, status.Error(codes.InvalidArgument, "user_ids 过多，单次最多 200 个")
    }

    // 去重
    seen := make(map[string]bool, len(req.UserIds))
    uniqueIDs := make([]string, 0, len(req.UserIds))
    for _, uid := range req.UserIds {
        if uid != "" && !seen[uid] {
            seen[uid] = true
            uniqueIDs = append(uniqueIDs, uid)
        }
    }

    // ==================== 2. Redis Pipeline 批量查询 ====================
    // 对每个用户执行 HGETALL presence:status:{user_id}
    pipe := s.redis.Pipeline()
    type userCmd struct {
        userID string
        cmd    *redis.MapStringStringCmd
    }
    cmds := make([]userCmd, 0, len(uniqueIDs))

    for _, uid := range uniqueIDs {
        statusKey := fmt.Sprintf("presence:status:%s", uid)
        cmd := pipe.HGetAll(ctx, statusKey)
        cmds = append(cmds, userCmd{userID: uid, cmd: cmd})
    }

    if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
        log.Error("Pipeline 批量查询在线状态失败", "count", len(uniqueIDs), "err", err)
        return nil, status.Error(codes.Internal, "批量查询状态失败")
    }

    // ==================== 3. 组装结果 ====================
    presences := make([]*pb.PresenceInfo, 0, len(cmds))
    for _, c := range cmds {
        result, err := c.cmd.Result()
        if err != nil || len(result) == 0 {
            // 无状态记录 → 离线
            presences = append(presences, &pb.PresenceInfo{
                UserId: c.userID,
                Status: common.ONLINE_STATUS_OFFLINE,
            })
            continue
        }

        statusInt, _ := strconv.Atoi(result["status"])
        userStatus := common.OnlineStatus(statusInt)
        lastActive, _ := strconv.ParseInt(result["last_active"], 10, 64)
        platformInt, _ := strconv.Atoi(result["platform"])

        // 隐身处理：对外显示为离线
        if userStatus == common.ONLINE_STATUS_INVISIBLE {
            presences = append(presences, &pb.PresenceInfo{
                UserId:     c.userID,
                Status:     common.ONLINE_STATUS_OFFLINE,
                LastActive: lastActive,
            })
            continue
        }

        presences = append(presences, &pb.PresenceInfo{
            UserId:       c.userID,
            Status:       userStatus,
            Platform:     common.PlatformType(platformInt),
            DeviceId:     result["device_id"],
            LastActive:   lastActive,
            CustomStatus: result["custom_text"],
        })
    }

    // ==================== 4. 返回 ====================
    return &pb.BatchGetPresenceResponse{
        Meta:      successMeta(ctx),
        Presences: presences,
    }, nil
}
```

### 4. SubscribePresence — 订阅用户状态变更

> 订阅指定用户的在线状态变更（通常在加好友、打开会话时触发）。  
> 订阅关系存储在 Redis 双向 SET 中：  
> - `presence:subs:{target_user_id}` → 谁在订阅这个人（被观察者视角）  
> - `presence:my_subs:{subscriber_user_id}` → 我订阅了谁（观察者视角）  
> 单个用户最多订阅 500 人状态。

> **⚠️ MED-07 高扇出优化（社交达人场景）**：  
> 拥有 500 好友的用户，每次上/下线需通知 500 个订阅者。10M 用户场景下如果 1% 是高扇出用户 = 10 万人，
> 早高峰每分钟 1 万次上线 × 500 订阅者 = 500 万/分钟的状态推送风暴。  
> **缓解策略**：
> 1. **聚合延迟通知**：5 秒窗口内多次上/下线切换只发 1 次通知（debounce）
>    - 实现：`SetStatus` 后写入 `presence:pending_notify:{user_id}` key（TTL 5s）
>    - 后台 goroutine 扫描 pending_notify → 批量发送 Kafka `presence.status.notify`
> 2. **lazy 模式降级**：订阅者超过 200 人时，后半部分订阅者不主动推送，由客户端定期拉取
>    - `presence.status.notify` 事件只包含前 200 个订阅者，其余通过 `GetPresence` 拉取
> 3. **订阅总上限 500**（当前已实现 ✅）

```go
func (s *PresenceService) SubscribePresence(ctx context.Context, req *pb.SubscribePresenceRequest) (*pb.SubscribePresenceResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id 不能为空")
    }
    if len(req.TargetUserIds) == 0 {
        return nil, status.Error(codes.InvalidArgument, "target_user_ids 不能为空")
    }
    if len(req.TargetUserIds) > 100 {
        return nil, status.Error(codes.InvalidArgument, "单次最多订阅 100 个用户")
    }

    // ==================== 2. 检查订阅数上限 ====================
    // Key: presence:my_subs:{user_id}  SET
    mySubsKey := fmt.Sprintf("presence:my_subs:%s", req.UserId)
    currentCount, err := s.redis.SCard(ctx, mySubsKey).Result()
    if err != nil {
        log.Error("查询订阅数失败", "user_id", req.UserId, "err", err)
        return nil, status.Error(codes.Internal, "查询订阅数失败")
    }
    if currentCount+int64(len(req.TargetUserIds)) > 500 {
        return nil, status.Error(codes.ResourceExhausted, "订阅数超过上限（最多 500 人）")
    }

    // ==================== 3. 去重目标用户列表 ====================
    seen := make(map[string]bool)
    targets := make([]string, 0, len(req.TargetUserIds))
    for _, tid := range req.TargetUserIds {
        if tid != "" && tid != req.UserId && !seen[tid] { // 不能订阅自己
            seen[tid] = true
            targets = append(targets, tid)
        }
    }
    if len(targets) == 0 {
        return &pb.SubscribePresenceResponse{Meta: successMeta(ctx)}, nil
    }

    // ==================== 4. Redis Pipeline — 双向添加订阅关系 ====================
    pipe := s.redis.Pipeline()
    for _, targetID := range targets {
        // 4a. 将 subscriber 加入目标用户的订阅者集合
        // Key: presence:subs:{target_user_id}  SET  TTL: 24h
        subsKey := fmt.Sprintf("presence:subs:%s", targetID)
        pipe.SAdd(ctx, subsKey, req.UserId)
        pipe.Expire(ctx, subsKey, 24*time.Hour)

        // 4b. 将目标用户加入 subscriber 的订阅列表
        // Key: presence:my_subs:{user_id}  SET  TTL: 24h
        pipe.SAdd(ctx, mySubsKey, targetID)
    }
    pipe.Expire(ctx, mySubsKey, 24*time.Hour)

    if _, err := pipe.Exec(ctx); err != nil {
        log.Error("添加订阅关系失败", "user_id", req.UserId, "targets", targets, "err", err)
        return nil, status.Error(codes.Internal, "添加订阅关系失败")
    }

    // ==================== 5. 返回 ====================
    log.Info("订阅状态变更成功",
        "user_id", req.UserId,
        "target_count", len(targets))

    return &pb.SubscribePresenceResponse{
        Meta: successMeta(ctx),
    }, nil
}
```

### 5. UnsubscribePresence — 取消订阅用户状态变更

> 取消订阅指定用户的在线状态变更（通常在删除好友时触发）。  
> 从 Redis 双向 SET 中移除订阅关系。

```go
func (s *PresenceService) UnsubscribePresence(ctx context.Context, req *pb.UnsubscribePresenceRequest) (*pb.UnsubscribePresenceResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id 不能为空")
    }
    if len(req.TargetUserIds) == 0 {
        return nil, status.Error(codes.InvalidArgument, "target_user_ids 不能为空")
    }
    if len(req.TargetUserIds) > 100 {
        return nil, status.Error(codes.InvalidArgument, "单次最多取消 100 个订阅")
    }

    // ==================== 2. 去重 ====================
    seen := make(map[string]bool)
    targets := make([]string, 0, len(req.TargetUserIds))
    for _, tid := range req.TargetUserIds {
        if tid != "" && !seen[tid] {
            seen[tid] = true
            targets = append(targets, tid)
        }
    }
    if len(targets) == 0 {
        return &pb.UnsubscribePresenceResponse{Meta: successMeta(ctx)}, nil
    }

    // ==================== 3. Redis Pipeline — 双向移除订阅关系 ====================
    pipe := s.redis.Pipeline()
    mySubsKey := fmt.Sprintf("presence:my_subs:%s", req.UserId)

    for _, targetID := range targets {
        // 3a. 从目标用户的订阅者集合中移除
        // Key: presence:subs:{target_user_id}  SET
        subsKey := fmt.Sprintf("presence:subs:%s", targetID)
        pipe.SRem(ctx, subsKey, req.UserId)

        // 3b. 从自己的订阅列表中移除
        // Key: presence:my_subs:{user_id}  SET
        pipe.SRem(ctx, mySubsKey, targetID)
    }

    if _, err := pipe.Exec(ctx); err != nil {
        log.Error("移除订阅关系失败", "user_id", req.UserId, "targets", targets, "err", err)
        return nil, status.Error(codes.Internal, "移除订阅关系失败")
    }

    // ==================== 4. 返回 ====================
    log.Info("取消订阅状态变更成功",
        "user_id", req.UserId,
        "target_count", len(targets))

    return &pb.UnsubscribePresenceResponse{
        Meta: successMeta(ctx),
    }, nil
}
```

### 6. UserOnline — 用户上线（内部接口，由 Connecte 调用）

> Connecte 连接注册成功后调用此接口通知 Presence 用户上线。  
> 流程：检查离线优雅期 → 更新状态 → 添加平台 → 更新 HyperLogLog → 生产上线事件。  
> 如果用户在优雅期内重连，标记为 is_reconnect = true，不广播上线事件。

```go
func (s *PresenceService) UserOnline(ctx context.Context, req *pb.UserOnlineRequest) (*pb.UserOnlineResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id 不能为空")
    }
    if req.Platform == common.PLATFORM_TYPE_UNSPECIFIED {
        return nil, status.Error(codes.InvalidArgument, "platform 不能为空")
    }
    if req.DeviceId == "" {
        return nil, status.Error(codes.InvalidArgument, "device_id 不能为空")
    }
    if req.ServerId == "" {
        return nil, status.Error(codes.InvalidArgument, "server_id 不能为空")
    }

    now := time.Now().UnixMilli()

    // ==================== 2. 检查离线优雅期 — 是否为重连 ====================
    // Key: presence:offline_grace:{user_id}  STRING  TTL: 30s
    graceKey := fmt.Sprintf("presence:offline_grace:%s", req.UserId)
    graceVal, err := s.redis.Get(ctx, graceKey).Result()
    isReconnect := false
    if err == nil && graceVal != "" {
        // 优雅期内重连 → 取消优雅期，不广播上线事件
        isReconnect = true
        s.redis.Del(ctx, graceKey)
        log.Info("用户在优雅期内重连",
            "user_id", req.UserId,
            "platform", req.Platform,
            "grace_val", graceVal)
    }

    // ==================== 3. 检查用户之前是否已经有其他平台在线 ====================
    // Key: presence:user_platforms:{user_id}  SET
    platformsKey := fmt.Sprintf("presence:user_platforms:%s", req.UserId)
    existingCount, _ := s.redis.SCard(ctx, platformsKey).Result()
    isFirstPlatform := existingCount == 0 && !isReconnect // 首个平台上线

    // ==================== 4. Redis Pipeline — 原子更新状态 ====================
    statusKey := fmt.Sprintf("presence:status:%s", req.UserId)
    platformMember := fmt.Sprintf("%d:%s:%s", int(req.Platform), req.DeviceId, req.ServerId)

    pipe := s.redis.Pipeline()

    // 4a. 更新/创建用户状态 HASH
    // 只有当用户之前完全离线时才重置 status 为 ONLINE
    // 如果用户已经在其他平台在线（且设置了 BUSY 等状态），保留原有 status
    if isFirstPlatform {
        pipe.HSet(ctx, statusKey, map[string]interface{}{
            "status":      int(common.ONLINE_STATUS_ONLINE),
            "custom_text": "",
            "platform":    int(req.Platform),
            "device_id":   req.DeviceId,
            "server_id":   req.ServerId,
            "last_active": now,
            "updated_at":  now,
        })
    } else {
        // 已有其他平台在线，仅更新活跃时间和新平台信息
        pipe.HSet(ctx, statusKey, map[string]interface{}{
            "last_active": now,
            "updated_at":  now,
        })
    }
    pipe.Expire(ctx, statusKey, 6*time.Minute)

    // 4b. 添加平台到用户平台集合
    // Key: presence:user_platforms:{user_id}  SET  TTL: 6min
    pipe.SAdd(ctx, platformsKey, platformMember)
    pipe.Expire(ctx, platformsKey, 6*time.Minute)

    // 4c. 更新全局在线 HyperLogLog
    // Key: presence:online  HYPERLOGLOG
    pipe.PFAdd(ctx, "presence:online", req.UserId)

    // 4d. 更新分平台 HyperLogLog
    // Key: presence:online:platform:{platform}  HYPERLOGLOG
    platformHLLKey := fmt.Sprintf("presence:online:platform:%d", int(req.Platform))
    pipe.PFAdd(ctx, platformHLLKey, req.UserId)

    if _, err := pipe.Exec(ctx); err != nil {
        log.Error("用户上线 Redis Pipeline 失败",
            "user_id", req.UserId, "platform", req.Platform, "err", err)
        return nil, status.Error(codes.Internal, "更新上线状态失败")
    }

    // ==================== 5. 生产 Kafka 上线事件 ====================
    // 优雅期内重连不广播（除非是首个平台上线且恰好也是重连——理论上不会同时成立）
    if !isReconnect || isFirstPlatform {
        onlineEvent := &kafka_presence.PresenceOnlineEvent{
            Header:      buildEventHeader("presence", s.instanceID),
            UserId:      req.UserId,
            Platform:    req.Platform,
            DeviceId:    req.DeviceId,
            ServerId:    req.ServerId,
            OnlineTime:  now,
            IsReconnect: isReconnect,
        }
        if err := s.kafka.Produce(ctx, "presence.online", req.UserId, onlineEvent); err != nil {
            log.Error("生产 presence.online 事件失败",
                "user_id", req.UserId, "err", err)
        }
    }

    // ==================== 6. 返回 ====================
    log.Info("用户上线",
        "user_id", req.UserId,
        "platform", req.Platform,
        "device_id", req.DeviceId,
        "server_id", req.ServerId,
        "is_reconnect", isReconnect,
        "is_first_platform", isFirstPlatform)

    return &pb.UserOnlineResponse{
        Meta: successMeta(ctx),
    }, nil
}
```

### 7. UserOffline — 用户下线（内部接口，由 Connecte 调用）

> Connecte 连接断开时调用此接口。  
> **核心机制**：不立即设置离线，而是进入 30s 优雅期。如果用户在 30s 内重连（网络切换、电梯场景），  
> 则 UserOnline 会取消优雅期，不广播离线事件，避免状态闪烁。  
> 只有当所有平台都离线时才真正广播 offline 事件。

```go
func (s *PresenceService) UserOffline(ctx context.Context, req *pb.UserOfflineRequest) (*pb.UserOfflineResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id 不能为空")
    }
    if req.Platform == common.PLATFORM_TYPE_UNSPECIFIED {
        return nil, status.Error(codes.InvalidArgument, "platform 不能为空")
    }

    now := time.Now().UnixMilli()

    // ==================== 2. 从平台集合中移除当前断开的平台 ====================
    // Key: presence:user_platforms:{user_id}  SET
    platformsKey := fmt.Sprintf("presence:user_platforms:%s", req.UserId)
    platformMember := fmt.Sprintf("%d:%s:%s", int(req.Platform), req.DeviceId, req.ServerId)

    removed, err := s.redis.SRem(ctx, platformsKey, platformMember).Result()
    if err != nil {
        log.Error("从平台集合移除失败",
            "user_id", req.UserId, "platform", req.Platform, "err", err)
        return nil, status.Error(codes.Internal, "更新下线状态失败")
    }
    if removed == 0 {
        // 该平台连接可能已被清理（超时/踢出），幂等返回成功
        log.Warn("平台成员不存在，可能已被清理",
            "user_id", req.UserId, "platform", req.Platform, "member", platformMember)
        return &pb.UserOfflineResponse{Meta: successMeta(ctx)}, nil
    }

    // ==================== 3. 检查是否还有其他平台在线 ====================
    remainingCount, err := s.redis.SCard(ctx, platformsKey).Result()
    if err != nil {
        log.Error("查询剩余平台数失败", "user_id", req.UserId, "err", err)
        return nil, status.Error(codes.Internal, "查询剩余平台数失败")
    }

    if remainingCount > 0 {
        // 还有其他平台在线 → 仅移除当前平台，不进入优雅期，不广播离线
        log.Info("用户单平台下线，其他平台仍在线",
            "user_id", req.UserId,
            "offline_platform", req.Platform,
            "remaining_platforms", remainingCount)
        return &pb.UserOfflineResponse{Meta: successMeta(ctx)}, nil
    }

    // ==================== 4. 所有平台都离线 → 进入离线优雅期 ====================
    // Key: presence:offline_grace:{user_id}  STRING  TTL: 30s
    // 30s 内如果 UserOnline 被调用，会取消优雅期
    graceKey := fmt.Sprintf("presence:offline_grace:%s", req.UserId)
    graceValue := fmt.Sprintf("%d:%s", int(req.Platform), req.DeviceId)
    s.redis.Set(ctx, graceKey, graceValue, 30*time.Second)

    // ==================== 5. 延迟执行真正的离线逻辑（30s 后） ====================
    // 使用 goroutine + sleep 实现延迟（生产环境建议用 DelayQueue 或 Redis Keyspace Notification）
    go func() {
        time.Sleep(30 * time.Second)

        // 再次检查优雅期 Key 是否仍然存在（如果被 UserOnline 删除了，说明已重连）
        exists, err := s.redis.Exists(context.Background(), graceKey).Result()
        if err != nil {
            log.Error("检查优雅期 Key 失败", "user_id", req.UserId, "err", err)
            return
        }
        if exists == 0 {
            // 优雅期 Key 已被 UserOnline 删除 → 用户已重连，不执行离线
            log.Info("用户已在优雅期内重连，取消离线",
                "user_id", req.UserId)
            return
        }

        // ---- 优雅期结束，用户确实离线 ----

        // 5a. 删除优雅期 Key
        s.redis.Del(context.Background(), graceKey)

        // 5b. 再次确认所有平台已离线（防止优雅期内有新平台上线）
        currentCount, _ := s.redis.SCard(context.Background(), platformsKey).Result()
        if currentCount > 0 {
            log.Info("优雅期结束但有新平台上线，取消离线",
                "user_id", req.UserId,
                "current_platforms", currentCount)
            return
        }

        // 5c. 删除用户状态 HASH
        // Key: presence:status:{user_id}
        statusKey := fmt.Sprintf("presence:status:%s", req.UserId)
        s.redis.Del(context.Background(), statusKey)

        // 5d. 删除平台集合（可能已空，防御性删除）
        s.redis.Del(context.Background(), platformsKey)

        // 5e. 计算在线时长（从 last_active 到现在）
        onlineDuration := int64(0) // 实际应从 status HASH 的 last_active 计算

        // 5f. 生产 Kafka 离线事件
        offlineEvent := &kafka_presence.PresenceOfflineEvent{
            Header:         buildEventHeader("presence", s.instanceID),
            UserId:         req.UserId,
            Platform:       req.Platform,
            DeviceId:       req.DeviceId,
            Reason:         "disconnect",
            OfflineTime:    time.Now().UnixMilli(),
            OnlineDuration: onlineDuration,
        }
        if err := s.kafka.Produce(context.Background(), "presence.offline", req.UserId, offlineEvent); err != nil {
            log.Error("生产 presence.offline 事件失败",
                "user_id", req.UserId, "err", err)
        }

        log.Info("用户确认离线（优雅期结束）",
            "user_id", req.UserId,
            "platform", req.Platform,
            "device_id", req.DeviceId)
    }()

    // ==================== 6. 返回（立即返回，离线逻辑异步执行） ====================
    return &pb.UserOfflineResponse{
        Meta: successMeta(ctx),
    }, nil
}
```

### 8. Heartbeat — 心跳保活（极致性能路径）

> **最热接口**：10M 用户每 60s 发送一次心跳 ≈ 166K QPS。  
> **必须极致轻量**：仅执行 Redis EXPIRE 续期 + HSET last_active，禁止任何 PgSQL 查询、RPC 调用、JSON 序列化。  
> **批量优化**：Gateway 可攒批上报心跳（每 5s 聚合一次），减少 RPC 调用次数。  
> **Lua 脚本原子操作**：使用 EVAL 将多个 Redis 命令合并为单次网络往返。

```go
// heartbeatScript 是预编译的 Lua 脚本，将心跳的所有 Redis 操作合并为单次原子调用
// 减少网络往返：原本需要 4 次 Redis 调用（HSET + EXPIRE + EXPIRE + PFADD），现在只需 1 次
var heartbeatScript = redis.NewScript(`
    -- KEYS[1] = presence:status:{user_id}
    -- KEYS[2] = presence:user_platforms:{user_id}
    -- KEYS[3] = presence:online (HyperLogLog)
    -- ARGV[1] = now (毫秒时间戳)
    -- ARGV[2] = ttl_seconds (360 = 6min)
    -- ARGV[3] = user_id

    -- 检查状态 Key 是否存在（用户是否在线）
    local exists = redis.call('EXISTS', KEYS[1])
    if exists == 0 then
        return 0  -- 用户状态不存在，心跳无效
    end

    -- 更新 last_active 时间戳
    redis.call('HSET', KEYS[1], 'last_active', ARGV[1])

    -- 续期状态 HASH TTL
    redis.call('EXPIRE', KEYS[1], ARGV[2])

    -- 续期平台集合 TTL
    redis.call('EXPIRE', KEYS[2], ARGV[2])

    -- 更新 HyperLogLog（维持在线计数准确性）
    redis.call('PFADD', KEYS[3], ARGV[3])

    return 1  -- 成功
`)

func (s *PresenceService) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
    // ==================== 1. 参数校验（极简，不做多余操作） ====================
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id 不能为空")
    }
    if req.Platform == common.PLATFORM_TYPE_UNSPECIFIED {
        return nil, status.Error(codes.InvalidArgument, "platform 不能为空")
    }

    now := time.Now().UnixMilli()

    // ==================== 2. Lua 脚本原子执行 — 单次网络往返 ====================
    // 将 HSET + EXPIRE + EXPIRE + PFADD 合并为 1 次 Redis 调用
    statusKey := fmt.Sprintf("presence:status:%s", req.UserId)
    platformsKey := fmt.Sprintf("presence:user_platforms:%s", req.UserId)
    hllKey := "presence:online"

    result, err := heartbeatScript.Run(ctx, s.redis, []string{
        statusKey,    // KEYS[1]
        platformsKey, // KEYS[2]
        hllKey,       // KEYS[3]
    },
        now, // ARGV[1] 当前时间戳
        360, // ARGV[2] TTL 秒数 = 6min
        req.UserId, // ARGV[3] user_id (for HyperLogLog)
    ).Int()

    if err != nil {
        log.Error("心跳 Lua 脚本执行失败",
            "user_id", req.UserId, "err", err)
        return nil, status.Error(codes.Internal, "心跳处理失败")
    }

    if result == 0 {
        // 用户状态 Key 不存在 → 可能已超时离线
        // 不返回错误，由客户端重新建立连接触发 UserOnline
        log.Warn("心跳目标用户状态不存在（可能已过期）",
            "user_id", req.UserId,
            "platform", req.Platform)
    }

    // ==================== 3. 返回服务端时间戳 ====================
    // 客户端可用此时间戳校准本地时钟
    return &pb.HeartbeatResponse{
        Meta:       successMeta(ctx),
        ServerTime: now,
    }, nil
}
```

### 9. GetOnlineCount — 获取在线用户总数

> 使用 HyperLogLog PFCOUNT 统计在线用户数，O(1) 时间复杂度，误差 < 0.81%。  
> 支持按平台分别统计。  
> 用于运营后台、监控大盘展示。

```go
func (s *PresenceService) GetOnlineCount(ctx context.Context, req *pb.GetOnlineCountRequest) (*pb.GetOnlineCountResponse, error) {
    // ==================== 1. 查询全局在线总数 — HyperLogLog ====================
    // Key: presence:online  HYPERLOGLOG
    totalCount, err := s.redis.PFCount(ctx, "presence:online").Result()
    if err != nil {
        log.Error("查询全局在线总数失败", "err", err)
        return nil, status.Error(codes.Internal, "查询在线总数失败")
    }

    // ==================== 2. 查询各平台在线数 — Pipeline 批量 PFCOUNT ====================
    platformCounts := make(map[string]int64)
    platforms := []struct {
        platform common.PlatformType
        name     string
    }{
        {common.PLATFORM_TYPE_IOS, "ios"},
        {common.PLATFORM_TYPE_ANDROID, "android"},
        {common.PLATFORM_TYPE_WINDOWS, "windows"},
        {common.PLATFORM_TYPE_MACOS, "macos"},
        {common.PLATFORM_TYPE_LINUX, "linux"},
        {common.PLATFORM_TYPE_WEB, "web"},
        {common.PLATFORM_TYPE_IPAD, "ipad"},
        {common.PLATFORM_TYPE_ANDROID_PAD, "android_pad"},
        {common.PLATFORM_TYPE_MINI_PROGRAM, "mini_program"},
    }

    pipe := s.redis.Pipeline()
    type platCmd struct {
        name string
        cmd  *redis.IntCmd
    }
    cmds := make([]platCmd, 0, len(platforms))

    for _, p := range platforms {
        // Key: presence:online:platform:{platform}  HYPERLOGLOG
        key := fmt.Sprintf("presence:online:platform:%d", int(p.platform))
        cmd := pipe.PFCount(ctx, key)
        cmds = append(cmds, platCmd{name: p.name, cmd: cmd})
    }

    if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
        log.Error("Pipeline 查询分平台在线数失败", "err", err)
        // 降级：仅返回总数
    } else {
        for _, c := range cmds {
            count, err := c.cmd.Result()
            if err == nil && count > 0 {
                platformCounts[c.name] = count
            }
        }
    }

    // ==================== 3. 如果请求了特定平台的计数 ====================
    if req.Platform != common.PLATFORM_TYPE_UNSPECIFIED {
        platformKey := fmt.Sprintf("presence:online:platform:%d", int(req.Platform))
        filteredCount, err := s.redis.PFCount(ctx, platformKey).Result()
        if err != nil {
            log.Error("查询指定平台在线数失败",
                "platform", req.Platform, "err", err)
            filteredCount = 0
        }
        return &pb.GetOnlineCountResponse{
            Meta:           successMeta(ctx),
            OnlineCount:    filteredCount,
            PlatformCounts: platformCounts,
        }, nil
    }

    // ==================== 4. 返回 ====================
    return &pb.GetOnlineCountResponse{
        Meta:           successMeta(ctx),
        OnlineCount:    totalCount,
        PlatformCounts: platformCounts,
    }, nil
}
```

### 10. SendTypingIndicator — 发送正在输入状态提示

> 通知会话对方用户正在输入/录音/上传文件。  
> 使用 Redis STRING + 5s TTL 做节流：5s 内重复发送同一会话的 typing 事件直接忽略。  
> 生产 Kafka 事件给 Push 服务，由 Push 投递给目标用户。  
> **注意**：typing 事件属于尽力投递（best-effort），不需要持久化和重试。

```go
func (s *PresenceService) SendTypingIndicator(ctx context.Context, req *pb.SendTypingIndicatorRequest) (*pb.SendTypingIndicatorResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id 不能为空")
    }
    if req.ConversationId == "" {
        return nil, status.Error(codes.InvalidArgument, "conversation_id 不能为空")
    }
    if req.TargetId == "" {
        return nil, status.Error(codes.InvalidArgument, "target_id 不能为空")
    }
    // 校验 action 合法性
    validActions := map[string]bool{
        "typing":          true,
        "recording_voice": true,
        "uploading_file":  true,
    }
    if req.Action == "" {
        req.Action = "typing" // 默认为 typing
    }
    if !validActions[req.Action] {
        return nil, status.Error(codes.InvalidArgument, "action 不合法，可选: typing/recording_voice/uploading_file")
    }

    // ==================== 2. Redis 节流 — 5s 内同一会话不重复发送 ====================
    // Key: presence:typing:{conversation_id}:{user_id}  STRING  TTL: 5s
    typingKey := fmt.Sprintf("presence:typing:%s:%s", req.ConversationId, req.UserId)
    set, err := s.redis.SetNX(ctx, typingKey, req.Action, 5*time.Second).Result()
    if err != nil {
        log.Error("typing 节流 Redis 失败",
            "user_id", req.UserId, "conversation_id", req.ConversationId, "err", err)
        return nil, status.Error(codes.Internal, "处理输入状态失败")
    }
    if !set {
        // 5s 内已发送过 → 直接返回成功（节流）
        return &pb.SendTypingIndicatorResponse{Meta: successMeta(ctx)}, nil
    }

    // ==================== 3. 生产 Kafka typing 事件 ====================
    // Topic: presence.typing → Push 服务消费后投递给目标用户
    typingEvent := &kafka_presence.PresenceTypingEvent{
        Header:         buildEventHeader("presence", s.instanceID),
        UserId:         req.UserId,
        TargetId:       req.TargetId,
        ConversationId: req.ConversationId,
        Action:         req.Action,
    }
    if err := s.kafka.Produce(ctx, "presence.typing", req.TargetId, typingEvent); err != nil {
        log.Error("生产 presence.typing 事件失败",
            "user_id", req.UserId, "target_id", req.TargetId, "err", err)
        // typing 是尽力投递，不阻塞不重试
    }

    // ==================== 4. 返回 ====================
    return &pb.SendTypingIndicatorResponse{
        Meta: successMeta(ctx),
    }, nil
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

### successMeta — 构建成功响应元信息

```go
func successMeta(ctx context.Context) *common.ResponseMeta {
    return &common.ResponseMeta{
        Code:       0,
        Message:    "success",
        ServerTime: time.Now().UnixMilli(),
        TraceId:    trace.SpanFromContext(ctx).SpanContext().TraceID().String(),
    }
}
```

### HyperLogLog 定时重建任务

```go
// rebuildHyperLogLog 每 10 分钟执行一次，重建 HyperLogLog 以保证计数准确性
// 因为 HyperLogLog 只支持 PFADD（添加），不支持删除，离线用户不会自动移除
// 通过定期重建来纠正累积误差
func (s *PresenceService) rebuildHyperLogLog(ctx context.Context) {
    ticker := time.NewTicker(10 * time.Minute)
    defer ticker.Stop()

    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            // 使用分布式锁确保只有一个实例执行重建
            lockKey := "presence:hll_rebuild_lock"
            locked, err := s.redis.SetNX(ctx, lockKey, s.instanceID, 5*time.Minute).Result()
            if err != nil || !locked {
                continue // 其他实例正在重建
            }

            log.Info("开始重建 HyperLogLog")

            // 1. 创建临时 HyperLogLog Key
            tmpKey := "presence:online:tmp"
            s.redis.Del(ctx, tmpKey)

            // 2. SCAN 所有 presence:status:* Key，提取在线用户
            cursor := uint64(0)
            for {
                keys, nextCursor, err := s.redis.Scan(ctx, cursor, "presence:status:*", 1000).Result()
                if err != nil {
                    log.Error("SCAN presence:status:* 失败", "err", err)
                    break
                }

                if len(keys) > 0 {
                    // 提取 user_id 并批量 PFADD
                    userIDs := make([]interface{}, 0, len(keys))
                    for _, key := range keys {
                        // key = "presence:status:{user_id}" → 提取 user_id
                        uid := strings.TrimPrefix(key, "presence:status:")
                        userIDs = append(userIDs, uid)
                    }
                    s.redis.PFAdd(ctx, tmpKey, userIDs...)
                }

                cursor = nextCursor
                if cursor == 0 {
                    break
                }
            }

            // 3. 原子替换：RENAME tmp → presence:online
            s.redis.Rename(ctx, tmpKey, "presence:online")

            // 4. 清理锁
            s.redis.Del(ctx, lockKey)

            log.Info("HyperLogLog 重建完成")
        }
    }
}
```

---

## 性能考量

### Heartbeat 极致优化策略

| 优化项 | 策略 | 效果 |
|--------|------|------|
| Lua 脚本 | 将 4 个 Redis 命令合并为 1 次网络往返 | 延迟从 ~4ms 降至 ~1ms |
| Gateway 攒批 | Gateway 每 5s 聚合所有连接的心跳，批量上报 | RPC 调用次数降低 60x |
| Pipeline 心跳 | 如果改为 HTTP/Kafka 上报，可 Pipeline 批量处理 | 吞吐量提升 10x |
| 无 PgSQL | 心跳路径零数据库查询 | 消除 DB 瓶颈 |
| 无 RPC | 心跳路径不调用任何其他服务 | 消除跨服务延迟 |
| 连接池 | Redis 连接池 min=100, max=500 | 避免连接竞争 |

### 在线计数准确性

| 指标 | 值 |
|------|-----|
| HyperLogLog 误差率 | < 0.81% |
| 重建频率 | 每 10 分钟 |
| 内存占用 | ~12KB（固定，与用户数无关） |
| 分平台计数 | 每个平台独立 HyperLogLog，各 ~12KB |

### 离线优雅期防闪烁

| 场景 | 处理 |
|------|------|
| 网络短暂中断 (<30s) | 不广播离线，重连后恢复 |
| 电梯/地铁 (<30s) | 不广播离线，信号恢复后重连 |
| 真正离线 (>30s) | 优雅期结束后广播 offline |
| 多端：单平台断开 | 其他平台仍在线，不进入优雅期 |
| 多端：最后一个平台断开 | 进入优雅期，30s 后确认离线 |

---

## 可观测性接入（OpenTelemetry）

> Presence 服务负责用户在线状态管理，需重点观测：在线用户数（分平台）、心跳处理速率、状态变更频率、优雅期超时触发率、HyperLogLog 重建耗时。

### 第一步：服务启动时初始化 OTel SDK

```go
func main() {
    ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
    defer cancel()

    otelShutdown := observability.InitOpenTelemetry(ctx, "presence", "v1.0.0")
    defer otelShutdown(ctx)
    observability.SetupLogger("presence")
}
```

### 第二步：gRPC Server Interceptor（Presence 不依赖其他 RPC）

```go
grpcServer := grpc.NewServer(
    grpc.ChainUnaryInterceptor(otelgrpc.UnaryServerInterceptor()),
    grpc.ChainStreamInterceptor(otelgrpc.StreamServerInterceptor()),
)
```

### 第三步：注册 Presence 专属业务指标

```go
var meter = otel.Meter("im-chat/presence")

var (
    // 在线用户数（分平台）
    onlineUsers, _ = meter.Int64Gauge("presence.online_users",
        metric.WithDescription("当前在线用户数"))

    // 心跳处理计数
    heartbeatProcessed, _ = meter.Int64Counter("presence.heartbeat_processed_total",
        metric.WithDescription("心跳处理总数"))

    // 状态变更计数
    statusChanged, _ = meter.Int64Counter("presence.status_changed_total",
        metric.WithDescription("状态变更事件总数"))

    // 优雅期超时触发计数
    gracePeriodExpired, _ = meter.Int64Counter("presence.grace_period_expired_total",
        metric.WithDescription("优雅期超时确认离线总数"))

    // HyperLogLog 重建耗时
    hllRebuildDuration, _ = meter.Float64Histogram("presence.hll_rebuild_duration_ms",
        metric.WithDescription("HyperLogLog 在线计数重建耗时"), metric.WithUnit("ms"))

    // Redis 操作延迟（心跳高频路径）
    heartbeatRedisLatency, _ = meter.Float64Histogram("presence.heartbeat_redis_latency_ms",
        metric.WithDescription("心跳路径 Redis 操作延迟"), metric.WithUnit("ms"))
)
```

在业务代码中埋点：

```go
// Heartbeat 中
redisStart := time.Now()
err := s.redis.Set(ctx, heartbeatKey, time.Now().Unix(), 60*time.Second).Err()
heartbeatRedisLatency.Record(ctx, float64(time.Since(redisStart).Milliseconds()))
heartbeatProcessed.Add(ctx, 1)

// SetStatus 中
statusChanged.Add(ctx, 1, metric.WithAttributes(
    attribute.String("from", oldStatus.String()),
    attribute.String("to", newStatus.String()),
))

// 优雅期超时回调中
gracePeriodExpired.Add(ctx, 1)

// HyperLogLog 定时重建任务中
rebuildStart := time.Now()
// ... PFCOUNT + 分平台统计 ...
hllRebuildDuration.Record(ctx, float64(time.Since(rebuildStart).Milliseconds()))
onlineUsers.Record(ctx, totalOnline, metric.WithAttributes(
    attribute.String("platform", "all"),
))
```

### 第四步：Kafka Producer 埋点 + 结构化 JSON 日志

发布 `presence.status.changed` / `presence.heartbeat.batch` 事件时注入 trace context。注意：心跳日志量大，应设置为 **DEBUG** 级别，仅在需要时开启。

### 接入要点总结

| 步骤 | 改动位置 | 效果 |
|------|---------|------|
| 1. `observability.InitOpenTelemetry` | main() | TracerProvider + MeterProvider + Runtime Metrics |
| 2. gRPC Server Interceptor | Server | RPC 自动 trace + 指标 |
| 3. 自定义业务指标 | Heartbeat/SetStatus/HLL重建 | 在线数、心跳率、状态变更率 |
| 4. Kafka + 日志 | 事件发布 + setupLogger | 链路追踪 + trace_id 日志 |
| 5. buildEventHeader 改造 | 辅助函数 | EventHeader 携带真实 trace context |
| 6. 基础设施指标 | main() | Redis 连接池可观测 |
| 7. Span 错误记录 | `observability.RecordError` | 错误 trace 在 Tempo 可见 |

### 补充：buildEventHeader 改造

将 `buildEventHeader(source, instanceID)` 替换为 `observability.BuildEventHeader(ctx, source, instanceID)`。

### 补充：基础设施指标注册

```go
observability.RegisterRedisPoolMetrics(redisClient, "presence")
```

### 补充：Span 错误记录

```go
func (s *PresenceServer) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
    ctx, span := otel.Tracer("presence").Start(ctx, "Heartbeat")
    defer span.End()

    result, err := s.doHeartbeat(ctx, req)
    if err != nil {
        observability.RecordError(span, err)
        return nil, err
    }
    return result, nil
}
```
