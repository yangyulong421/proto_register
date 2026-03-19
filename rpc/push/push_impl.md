# Push 推送服务 — RPC 接口实现伪代码

## 概述

Push 服务是 notify+sync IM 架构的核心投递引擎。负责将轻量 sync notify 信号推送给在线用户（通过长连接网关），管理离线推送设备（APNs / FCM / 厂商通道），并触发离线推送通知。

**关键架构优化：本地 Redis 缓存 + Kafka 事件同步**

传统做法是每次推送都通过 RPC 查询 Presence、Group、User 等服务，在高并发场景下（千万级在线用户）造成严重的 RPC 调用风暴。本服务采用本地 Redis 缓存 + Kafka 事件驱动同步的架构：

- **Push 服务维护本地 Redis 缓存**，由 Kafka Consumer（`kafka/push/push_impl.md`）负责写入
- **缓存未命中时**，降级为 RPC 调用并回填缓存
- **所有缓存更新遵循延迟双删策略**，保证最终一致性

```
旧架构（BAD）:
  PushToUser → RPC Presence.GetPresence → RPC Connecte.GetUserConnections → RPC User.GetUserSettings
  PushToGroup → RPC Group.GetGroupMembers → loop × N × (RPC Presence.GetPresence)

新架构（GOOD）:
  PushToUser → Connecte.GetUserConnections（推送链路本就必须调用，同时判断在线/离线）
  PushToGroup → 本地缓存 push:local:group_members:{gid} → Presence.BatchGetPresence（一次 RPC 批量判断在线）
  用户设置/信息 → 本地 Redis 缓存 push:local:user_settings / push:local:user_info
  Kafka 事件（group.member.joined / user.settings.updated / user.profile.updated 等）
  主动推送变更 → kafka/push consumer 更新本地缓存

  ⚠ 在线状态不做本地缓存（变化频率不满足「变化不频繁」条件）
```

## 外部依赖

| 依赖类型 | 依赖项 | 用途 | 是否可缓存 |
|---------|--------|------|-----------|
| PgSQL | push_devices 表 | 推送设备持久化 | — |
| PgSQL | push_statistics 表 | 推送统计 | — |
| Redis（本服务自有） | push:device / push:badge / push:rate_limit 等 | 设备缓存 / 角标 / 限流 | — |
| Redis（本地缓存） | push:local:group_members:{group_id} | 群成员列表本地缓存 | Kafka 同步，TTL 30min |
| Redis（本地缓存） | push:local:user_settings:{user_id} | 用户推送设置本地缓存 | Kafka 同步，TTL 1h |
| Redis（本地缓存） | push:local:user_info:{user_id} | 用户信息本地缓存 | Kafka 同步，TTL 1h |
| RPC | Connecte.GetUserConnections | 获取用户所有连接（**实时路由，不可缓存**） | ✗ |
| RPC | Connecte.SendToConnection | 通过长连接下发信号（**实时发送**） | ✗ |
| RPC | Connecte.BatchSendToConnections | 批量下发信号（**实时发送**） | ✗ |
| RPC | Connecte.GetGatewayNodes | 获取所有网关节点（广播用） | ✗ |
| RPC | OfflineQueue.EnqueueSyncSignal | 离线同步信号入队 | ✗ |
| RPC | OfflineQueue.BatchEnqueueSyncSignal | 批量离线入队 | ✗ |
| RPC | Presence.GetPresence | **仅作为缓存未命中的降级回源** | 本地缓存 |
| RPC | Presence.BatchGetPresence | **仅作为缓存未命中的降级回源** | 本地缓存 |
| RPC | Group.GetGroupMembers | **仅作为缓存未命中的降级回源** | 本地缓存 |
| RPC | User.GetUser | **仅作为缓存未命中的降级回源** | 本地缓存 |
| RPC | User.GetUserSettings | **仅作为缓存未命中的降级回源** | 本地缓存 |
| Kafka（生产） | push.offline.trigger | 触发离线推送（APNs/FCM），自产自消 |
| Kafka（生产） | push.stats | 推送统计事件 → Audit |

## PgSQL 表结构

```sql
-- 推送设备表
CREATE TABLE push_devices (
    id             BIGSERIAL PRIMARY KEY,
    user_id        VARCHAR(64)  NOT NULL,
    device_id      VARCHAR(64)  NOT NULL,
    platform       SMALLINT     NOT NULL DEFAULT 0,
    push_token     VARCHAR(512) NOT NULL,
    push_channel   VARCHAR(32)  NOT NULL DEFAULT '',  -- apns / fcm / huawei / xiaomi / oppo / vivo
    app_version    VARCHAR(32)  NOT NULL DEFAULT '',
    os_version     VARCHAR(32)  NOT NULL DEFAULT '',
    is_enabled     BOOLEAN      NOT NULL DEFAULT TRUE,
    last_active_at BIGINT       NOT NULL DEFAULT 0,
    created_at     BIGINT       NOT NULL,
    updated_at     BIGINT       NOT NULL,
    UNIQUE(user_id, device_id)
);
CREATE INDEX idx_push_devices_user ON push_devices(user_id);
CREATE INDEX idx_push_devices_token ON push_devices(push_token);

-- 推送统计表
CREATE TABLE push_statistics (
    id            BIGSERIAL PRIMARY KEY,
    date          DATE    NOT NULL,
    total_online  BIGINT  NOT NULL DEFAULT 0,
    total_offline BIGINT  NOT NULL DEFAULT 0,
    total_apns    BIGINT  NOT NULL DEFAULT 0,
    total_fcm     BIGINT  NOT NULL DEFAULT 0,
    total_huawei  BIGINT  NOT NULL DEFAULT 0,
    total_xiaomi  BIGINT  NOT NULL DEFAULT 0,
    success_count BIGINT  NOT NULL DEFAULT 0,
    fail_count    BIGINT  NOT NULL DEFAULT 0,
    UNIQUE(date)
);
```

## Redis Key 设计

### 本服务自有 Key

| Key 格式 | 类型 | 值 | TTL | 说明 |
|----------|------|-----|-----|------|
| `push:device:{user_id}` | HASH | field=device_id value=JSON(PushDevice) | 7d | 用户推送设备缓存 |
| `push:badge:{user_id}:{device_id}` | STRING | 角标计数 | — | 未读角标数 |
| `push:rate_limit:{user_id}` | STRING | 调用次数 | 1min | 推送频率限制 |
| `push:broadcast_progress:{broadcast_id}` | HASH | {total, done, fail, ...} | 24h | 广播推送进度 |
| `push:offline_dedup:{msg_id}:{user_id}` | STRING | "1" | 5min | 离线推送去重 |
| `push:kafka:dedup:{event_id}` | STRING | "1" | 24h | Kafka 消费幂等去重 |

### 本地缓存 Key（由 Kafka Consumer 维护，本服务只读）

| Key 格式 | 类型 | 值 | TTL | Kafka 同步源 |
|----------|------|-----|-----|-------------|
| `push:local:group_members:{group_id}` | SET | user_id 集合 | 30min | group.member.joined / group.member.left / group.member.kicked |
| `push:local:user_settings:{user_id}` | HASH | {push_enabled, dnd_start, dnd_end, show_content, show_sender} | 1h | user.settings.updated |
| `push:local:user_info:{user_id}` | HASH | {nickname, avatar_url} | 1h | user.profile.updated |

> **注意：在线状态不做本地缓存。**  
> 用户在线/离线状态变化频率高于头像、设置等数据（每天数次 vs 每月一次），  
> 不满足「高频读取 + 变化不频繁」的跨服务缓存准入条件。  
> PushToUser 通过 Connecte.GetUserConnections 直接判断在线（该 RPC 本就在推送链路中必须调用）。  
> PushToGroup 通过 Presence.BatchGetPresence RPC 批量查询（单次 RPC，数据源头，零缓存延迟）。

---

## 通用工具方法

### delayedDoubleDelete — 延迟双删

```go
// delayedDoubleDelete 延迟双删工具
// 模式：立即删除缓存 → 调用方更新 DB → goroutine 延迟 500ms 再删一次
// 目的：防止并发读请求将旧数据回填到缓存中
func (s *PushService) delayedDoubleDelete(ctx context.Context, keys ...string) {
    // ==================== 1. 第一次立即删除 ====================
    pipe := s.redis.Pipeline()
    for _, key := range keys {
        pipe.Del(ctx, key)
    }
    pipe.Exec(ctx)

    // ==================== 2. 延迟 500ms 后第二次删除 ====================
    go func() {
        time.Sleep(500 * time.Millisecond)
        pipe := s.redis.Pipeline()
        for _, key := range keys {
            pipe.Del(context.Background(), key)
        }
        pipe.Exec(context.Background())
    }()
}
```

---

## 本地缓存读取辅助方法（cache-miss fallback to RPC + backfill）

### getUserOnlineConnections — 通过 Connecte 判断用户在线并获取连接

```go
// getUserOnlineConnections 通过 Connecte.GetUserConnections 判断用户是否在线
// Connecte 是连接路由的唯一权威来源，推送链路本就必须调用此 RPC
// 因此复用此调用同时判断在线/离线，无需额外的 Presence 查询或本地缓存
//
// ⚠ 设计决策：不缓存在线状态
//   在线/离线状态变化频率（每天数次）显著高于头像（每月一次）、设置（极少变化），
//   不满足「高频读取 + 变化不频繁」的跨服务缓存准入条件。
//   而 Connecte.GetUserConnections 是推送流程的必经步骤，复用它判断在线性能零额外开销。
//
// 返回值：
//   - connections: 用户的活跃连接列表（空=离线）
//   - err: 错误（Connecte RPC 失败时返回）
func (s *PushService) getUserOnlineConnections(ctx context.Context, userID string) ([]*connecte_pb.ConnectionInfo, error) {
    connResp, err := s.connecteClient.GetUserConnections(ctx, &connecte_pb.GetUserConnectionsRequest{
        UserId: userID,
    })
    if err != nil {
        return nil, fmt.Errorf("get user connections failed: %w", err)
    }
    return connResp.Connections, nil
}
    for plat := range platformSet {
        platforms = append(platforms, plat)
    }

    // ==================== 5. 回填本地缓存（短 TTL，防止长期不一致） ====================
    statusStr := "offline"
    if isOnline {
        statusStr = "online"
    }
    backfillPipe := s.redis.Pipeline()
    backfillPipe.HSet(ctx, cacheKey, map[string]interface{}{
        "status":      statusStr,
        "platforms":   strings.Join(platforms, ","),
        "last_online": fmt.Sprintf("%d", time.Now().UnixMilli()),
    })
    backfillPipe.Expire(ctx, cacheKey, 5*time.Minute)
    if _, err := backfillPipe.Exec(ctx); err != nil {
        s.logger.Warn("回填 presence 缓存失败", zap.String("user_id", userID), zap.Error(err))
    }

    return isOnline, platforms, nil
}
```

### getGroupMembersFromLocalCache — 从本地缓存获取群成员列表

```go
// getGroupMembersFromLocalCache 从本地 Redis 缓存读取群成员 ID 列表
// 缓存命中 → 直接返回 SET
// 缓存未命中 → 降级 RPC Group.GetGroupMembers → 回填 SET 缓存
func (s *PushService) getGroupMembersFromLocalCache(ctx context.Context, groupID string) ([]string, error) {
    cacheKey := fmt.Sprintf("push:local:group_members:%s", groupID)

    // ==================== 1. 读本地缓存（SET） ====================
    members, err := s.redis.SMembers(ctx, cacheKey).Result()
    if err != nil && err != redis.Nil {
        s.logger.Warn("读取 group_members 本地缓存异常，降级 RPC",
            zap.String("group_id", groupID), zap.Error(err))
    }

    // ==================== 2. 缓存命中 ====================
    if len(members) > 0 {
        return members, nil
    }

    // ==================== 3. 缓存未命中 → 降级 RPC Group.GetGroupMembers ====================
    s.logger.Debug("group_members 本地缓存未命中，降级 RPC", zap.String("group_id", groupID))

    groupResp, err := s.groupClient.GetGroupMembers(ctx, &group_pb.GetGroupMembersRequest{
        GroupId:  groupID,
        PageSize: 10000, // 拉全量（Push 场景必须全量）
    })
    if err != nil {
        return nil, fmt.Errorf("group members RPC fallback failed: %w", err)
    }

    // ==================== 4. 提取成员 ID ====================
    memberIDs := make([]string, 0, len(groupResp.Members))
    for _, m := range groupResp.Members {
        memberIDs = append(memberIDs, m.UserId)
    }

    // ==================== 5. 回填本地缓存 SET ====================
    if len(memberIDs) > 0 {
        backfillPipe := s.redis.Pipeline()
        // 先删旧的（防止脏数据叠加）
        backfillPipe.Del(ctx, cacheKey)
        // SADD 批量添加
        interfaces := make([]interface{}, len(memberIDs))
        for i, id := range memberIDs {
            interfaces[i] = id
        }
        backfillPipe.SAdd(ctx, cacheKey, interfaces...)
        backfillPipe.Expire(ctx, cacheKey, 30*time.Minute)
        if _, err := backfillPipe.Exec(ctx); err != nil {
            s.logger.Warn("回填 group_members 缓存失败",
                zap.String("group_id", groupID), zap.Error(err))
        }
    }

    return memberIDs, nil
}
```

### getUserSettingsFromLocalCache — 从本地缓存获取用户推送设置

```go
// getUserSettingsFromLocalCache 从本地 Redis 缓存读取用户推送设置
// 缓存命中 → 直接返回结构体
// 缓存未命中 → 降级 RPC User.GetUserSettings → 回填缓存
//
// 返回值结构：
//   pushEnabled  bool   - 是否开启推送
//   dndStart     string - 免打扰开始时间 "HH:MM"
//   dndEnd       string - 免打扰结束时间 "HH:MM"
//   showContent  bool   - 推送是否展示内容
//   showSender   bool   - 推送是否展示发送者
func (s *PushService) getUserSettingsFromLocalCache(ctx context.Context, userID string) (*localUserSettings, error) {
    cacheKey := fmt.Sprintf("push:local:user_settings:%s", userID)

    // ==================== 1. 读本地缓存 ====================
    result, err := s.redis.HGetAll(ctx, cacheKey).Result()
    if err != nil && err != redis.Nil {
        s.logger.Warn("读取 user_settings 本地缓存异常，降级 RPC",
            zap.String("user_id", userID), zap.Error(err))
    }

    // ==================== 2. 缓存命中 ====================
    if len(result) > 0 {
        settings := &localUserSettings{
            PushEnabled: result["push_enabled"] != "false",
            DndStart:    result["dnd_start"],
            DndEnd:      result["dnd_end"],
            ShowContent: result["show_content"] != "false",
            ShowSender:  result["show_sender"] != "false",
        }
        return settings, nil
    }

    // ==================== 3. 缓存未命中 → 降级 RPC User.GetUserSettings ====================
    s.logger.Debug("user_settings 本地缓存未命中，降级 RPC", zap.String("user_id", userID))

    settingsResp, err := s.userClient.GetUserSettings(ctx, &user_pb.GetUserSettingsRequest{
        UserId: userID,
    })
    if err != nil {
        return nil, fmt.Errorf("user settings RPC fallback failed: %w", err)
    }

    // ==================== 4. 提取设置 ====================
    settings := &localUserSettings{
        PushEnabled: settingsResp.Settings.PushEnabled,
        DndStart:    settingsResp.Settings.DndStartTime,
        DndEnd:      settingsResp.Settings.DndEndTime,
        ShowContent: settingsResp.Settings.PushShowContent,
        ShowSender:  settingsResp.Settings.PushShowSender,
    }

    // ==================== 5. 回填本地缓存 ====================
    backfillPipe := s.redis.Pipeline()
    backfillPipe.HSet(ctx, cacheKey, map[string]interface{}{
        "push_enabled": fmt.Sprintf("%t", settings.PushEnabled),
        "dnd_start":    settings.DndStart,
        "dnd_end":      settings.DndEnd,
        "show_content": fmt.Sprintf("%t", settings.ShowContent),
        "show_sender":  fmt.Sprintf("%t", settings.ShowSender),
    })
    backfillPipe.Expire(ctx, cacheKey, 1*time.Hour)
    if _, err := backfillPipe.Exec(ctx); err != nil {
        s.logger.Warn("回填 user_settings 缓存失败",
            zap.String("user_id", userID), zap.Error(err))
    }

    return settings, nil
}
```

### getUserInfoFromLocalCache — 从本地缓存获取用户基础信息

```go
// getUserInfoFromLocalCache 从本地 Redis 缓存读取用户基础信息（昵称、头像）
// 用于构造离线推送通知的标题和内容
// 缓存命中 → 直接返回
// 缓存未命中 → 降级 RPC User.GetUser → 回填缓存
func (s *PushService) getUserInfoFromLocalCache(ctx context.Context, userID string) (nickname string, avatarURL string, err error) {
    cacheKey := fmt.Sprintf("push:local:user_info:%s", userID)

    // ==================== 1. 读本地缓存 ====================
    result, err := s.redis.HGetAll(ctx, cacheKey).Result()
    if err != nil && err != redis.Nil {
        s.logger.Warn("读取 user_info 本地缓存异常，降级 RPC",
            zap.String("user_id", userID), zap.Error(err))
    }

    // ==================== 2. 缓存命中 ====================
    if len(result) > 0 {
        return result["nickname"], result["avatar_url"], nil
    }

    // ==================== 3. 缓存未命中 → 降级 RPC User.GetUser ====================
    s.logger.Debug("user_info 本地缓存未命中，降级 RPC", zap.String("user_id", userID))

    userResp, err := s.userClient.GetUser(ctx, &user_pb.GetUserRequest{
        UserId: userID,
    })
    if err != nil {
        return "", "", fmt.Errorf("user info RPC fallback failed: %w", err)
    }

    nickname = userResp.User.Nickname
    avatarURL = userResp.User.AvatarUrl

    // ==================== 4. 回填本地缓存 ====================
    backfillPipe := s.redis.Pipeline()
    backfillPipe.HSet(ctx, cacheKey, map[string]interface{}{
        "nickname":   nickname,
        "avatar_url": avatarURL,
    })
    backfillPipe.Expire(ctx, cacheKey, 1*time.Hour)
    if _, err := backfillPipe.Exec(ctx); err != nil {
        s.logger.Warn("回填 user_info 缓存失败", zap.String("user_id", userID), zap.Error(err))
    }

    return nickname, avatarURL, nil
}
```

### batchGetPresenceDirectRPC — 批量查询在线状态（直接 RPC，不缓存）

```go
// batchGetPresenceDirectRPC 批量查询用户在线状态
// 直接调用 Presence.BatchGetPresence RPC（数据源头，零缓存延迟）
//
// ⚠ 设计决策：不缓存在线状态
//   在线/离线状态变化频率（每天数次）显著高于头像（每月一次）、设置（极少变化），
//   不满足「高频读取 + 变化不频繁」的跨服务缓存准入条件。
//   对于群消息扇出，Presence.BatchGetPresence 单次 RPC 即可批量查询全部成员，
//   性能开销可控（Presence 服务底层为 Redis Pipeline HGETALL，P99 < 10ms/千人）。
//
// 返回值：
//   onlineUserIDs:  在线用户 ID 列表
//   offlineUserIDs: 离线用户 ID 列表
func (s *PushService) batchGetPresenceDirectRPC(ctx context.Context, userIDs []string) (onlineUserIDs, offlineUserIDs []string, err error) {
    if len(userIDs) == 0 {
        return nil, nil, nil
    }

    onlineUserIDs = make([]string, 0, len(userIDs)/2)
    offlineUserIDs = make([]string, 0, len(userIDs)/2)

    // ==================== 1. 分批 RPC 调用 Presence.BatchGetPresence ====================
    const batchSize = 200
    for i := 0; i < len(userIDs); i += batchSize {
        end := i + batchSize
        if end > len(userIDs) {
            end = len(userIDs)
        }
        batch := userIDs[i:end]

        batchResp, rpcErr := s.presenceClient.BatchGetPresence(ctx, &presence_pb.BatchGetPresenceRequest{
            UserIds: batch,
        })
        if rpcErr != nil {
            // RPC 失败，将此批全部视为离线（安全降级）
            s.logger.Warn("BatchGetPresence RPC 失败，安全降级视为离线",
                zap.Int("batch_size", len(batch)), zap.Error(rpcErr))
            offlineUserIDs = append(offlineUserIDs, batch...)
            continue
        }

        // ==================== 2. 解析在线/离线状态 ====================
        onlineSet := make(map[string]bool, len(batch))
        for _, uid := range batch {
            onlineSet[uid] = false // 默认离线
        }
        for uid, presenceList := range batchResp.PresenceMap {
            for _, p := range presenceList.Presences {
                if p.Status == common_pb.ONLINE_STATUS_ONLINE ||
                    p.Status == common_pb.ONLINE_STATUS_BUSY ||
                    p.Status == common_pb.ONLINE_STATUS_AWAY {
                    onlineSet[uid] = true
                    break
                }
            }
        }

        for uid, online := range onlineSet {
            if online {
                onlineUserIDs = append(onlineUserIDs, uid)
            } else {
                offlineUserIDs = append(offlineUserIDs, uid)
            }
        }
    }

    return onlineUserIDs, offlineUserIDs, nil
}
                    statusVal = "online"
                    hitOnline = append(hitOnline, uid)
                } else {
                    hitOffline = append(hitOffline, uid)
                }
                backfillPipe.HSet(ctx, cacheKey, map[string]interface{}{
                    "status":      statusVal,
                    "last_online": fmt.Sprintf("%d", time.Now().UnixMilli()),
                })
                backfillPipe.Expire(ctx, cacheKey, 5*time.Minute)
            }
            backfillPipe.Exec(ctx)
        }
    }

    return hitOnline, hitOffline, nil
}
```

### isDndTime — 判断是否在免打扰时段

```go
// isDndTime 判断当前时刻是否在用户的免打扰时段内
// 支持跨天（如 23:00 ~ 07:00）
func (s *PushService) isDndTime(dndStart, dndEnd string) bool {
    if dndStart == "" || dndEnd == "" {
        return false
    }
    now := time.Now().Format("15:04")
    if dndStart <= dndEnd {
        // 不跨天：如 22:00 ~ 23:00
        return now >= dndStart && now <= dndEnd
    }
    // 跨天：如 23:00 ~ 07:00
    return now >= dndStart || now <= dndEnd
}
```

### serializeSignal — 序列化 SyncNotifySignal

```go
// serializeSignal 将 SyncNotifySignal 序列化为 bytes（用于下行发送）
func (s *PushService) serializeSignal(signal *pb.SyncNotifySignal) ([]byte, error) {
    data, err := proto.Marshal(signal)
    if err != nil {
        return nil, fmt.Errorf("serialize signal failed: %w", err)
    }
    return data, nil
}
```

---

## 接口实现

### 1. PushToUser — 向单个用户推送 sync notify

```go
func (s *PushService) PushToUser(ctx context.Context, req *pb.PushToUserRequest) (*pb.PushToUserResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id is required")
    }
    if req.Signal == nil {
        return nil, status.Error(codes.InvalidArgument, "signal is required")
    }

    // ==================== 2. 频率限制 — Redis INCR ====================
    // push:rate_limit:{user_id} → STRING count TTL 1min，最大 100 次/分钟
    rateLimitKey := fmt.Sprintf("push:rate_limit:%s", req.UserId)
    count, err := s.redis.Incr(ctx, rateLimitKey).Result()
    if err != nil {
        // Redis 异常不阻断推送，仅记录
        s.logger.Warn("频率限制 Redis INCR 失败，跳过限流",
            zap.String("user_id", req.UserId), zap.Error(err))
    } else {
        if count == 1 {
            // 首次计数，设置 TTL
            s.redis.Expire(ctx, rateLimitKey, 1*time.Minute)
        }
        if count > 100 {
            s.logger.Warn("推送频率超限",
                zap.String("user_id", req.UserId), zap.Int64("count", count))
            return &pb.PushToUserResponse{
                Meta:   errorMeta(ctx, codes.ResourceExhausted, "push rate limit exceeded"),
                Online: false,
                Pushed: false,
            }, nil
        }
    }

    // ==================== 3. 查询在线状态 — 直接通过 Connecte 判断（无本地缓存） ====================
    // ⚠ 设计决策：不缓存在线状态
    //   在线/离线状态变化频率（每天数次）显著高于头像/设置（每月一次），
    //   不满足「高频读取 + 变化不频繁」的跨服务缓存准入条件。
    //   Connecte.GetUserConnections 是推送链路的必经步骤，复用此调用同时判断在线/离线，
    //   性能零额外开销，且数据实时准确（Connecte 是连接路由的唯一权威来源）。
    connections, err := s.getUserOnlineConnections(ctx, req.UserId)
    if err != nil {
        s.logger.Error("获取用户连接失败，安全降级为离线",
            zap.String("user_id", req.UserId), zap.Error(err))
        connections = nil // 降级为离线
    }

    // ==================== 4. 序列化信号（只序列化一次） ====================
    signalData, err := s.serializeSignal(req.Signal)
    if err != nil {
        return nil, status.Error(codes.Internal, "serialize signal failed")
    }

    // ==================== 5a. 用户在线（有活跃连接）→ 通过 Connecte 长连接下发 ====================
    if len(connections) > 0 {

        // 向该用户的每个连接发送 sync notify
        pushedCount := 0
        for _, conn := range connResp.Connections {
            _, sendErr := s.connecteClient.SendToConnection(ctx, &connecte_pb.SendToConnectionRequest{
                ConnectionId: conn.ConnectionId,
                UserId:       req.UserId,
                DataType:     "sync_notify",
                Data:         signalData,
            })
            if sendErr != nil {
                s.logger.Warn("向连接发送 sync notify 失败",
                    zap.String("user_id", req.UserId),
                    zap.String("connection_id", conn.ConnectionId),
                    zap.String("platform", conn.Platform.String()),
                    zap.Error(sendErr))
                continue
            }
            pushedCount++
        }

        if pushedCount > 0 {
            // 在线推送成功，生产统计事件
            s.kafka.Produce(ctx, "push.stats", req.UserId, &kafka_push.PushStatsEvent{
                Header:            buildEventHeader("push", s.instanceID),
                ServerId:          s.instanceID,
                OnlinePushCount:   1,
                OnlinePushSuccess: int64(pushedCount),
            })

            return &pb.PushToUserResponse{
                Meta:   successMeta(ctx),
                Online: true,
                Pushed: true,
            }, nil
        }

        // 所有连接发送都失败了，降级走离线
    }

offlinePush:
    // ==================== 5b. 用户离线 → 入队 OfflineQueue + 触发离线推送 ====================

    // 5b-1. 从本地缓存获取用户推送设置（核心优化：非 RPC）
    // 旧方案：RPC User.GetUserSettings
    // 新方案：读本地 Redis 缓存 push:local:user_settings:{user_id}
    userSettings, err := s.getUserSettingsFromLocalCache(ctx, req.UserId)
    if err != nil {
        s.logger.Warn("获取用户推送设置失败，使用默认值",
            zap.String("user_id", req.UserId), zap.Error(err))
        // 默认允许推送
        userSettings = &localUserSettings{PushEnabled: true, ShowContent: true, ShowSender: true}
    }

    // 5b-2. 入队 OfflineQueue（无论推送设置如何，同步信号必须入队，保证数据不丢）
    _, err = s.offlineQueueClient.EnqueueSyncSignal(ctx, &offline_queue_pb.EnqueueSyncSignalRequest{
        UserId:           req.UserId,
        ConversationId:   extractConversationID(req.Signal),
        ConversationType: extractConversationType(req.Signal),
        MaxSeq:           req.Signal.Seq,
        SenderId:         extractSenderID(req.Signal),
        SenderName:       extractSenderName(req.Signal),
        MsgType:          extractMsgType(req.Signal),
        ContentPreview:   extractContentPreview(req.Signal),
        Priority:         0,
        ExpireAt:         time.Now().Add(7 * 24 * time.Hour).UnixMilli(),
    })
    if err != nil {
        s.logger.Error("入队 OfflineQueue 失败",
            zap.String("user_id", req.UserId), zap.Error(err))
        // 不阻断流程，继续触发离线推送
    }

    // 5b-3. 检查是否需要发送离线推送（APNs/FCM）
    if userSettings.PushEnabled && !s.isDndTime(userSettings.DndStart, userSettings.DndEnd) {
        // 离线推送去重：push:offline_dedup:{msg_id}:{user_id}
        // 同一条消息只对同一用户触发一次离线推送
        msgID := extractMsgID(req.Signal)
        dedupKey := fmt.Sprintf("push:offline_dedup:%s:%s", msgID, req.UserId)
        isNew, _ := s.redis.SetNX(ctx, dedupKey, "1", 5*time.Minute).Result()

        if isNew {
            // 通过 Kafka 触发实际离线推送（自产自消 push.offline.trigger）
            s.kafka.Produce(ctx, "push.offline.trigger", req.UserId, &kafka_push.PushOfflineTriggerEvent{
                Header:   buildEventHeader("push", s.instanceID),
                UserId:   req.UserId,
                Title:    buildOfflineTitle(req.Signal, userSettings),
                Content:  buildOfflineContent(req.Signal, userSettings),
                MsgId:    msgID,
                SenderId: extractSenderID(req.Signal),
            })
        }
    }

    // 推送统计
    s.kafka.Produce(ctx, "push.stats", req.UserId, &kafka_push.PushStatsEvent{
        Header:           buildEventHeader("push", s.instanceID),
        ServerId:         s.instanceID,
        OfflinePushCount: 1,
    })

    return &pb.PushToUserResponse{
        Meta:   successMeta(ctx),
        Online: false,
        Pushed: false,
    }, nil
}
```

### 2. PushToUserOtherDevices — 向用户的其他设备推送（多端同步）

```go
func (s *PushService) PushToUserOtherDevices(ctx context.Context, req *pb.PushToUserOtherDevicesRequest) (*pb.PushToUserOtherDevicesResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id is required")
    }
    if req.ExcludeDeviceId == "" {
        return nil, status.Error(codes.InvalidArgument, "exclude_device_id is required")
    }
    if req.Signal == nil {
        return nil, status.Error(codes.InvalidArgument, "signal is required")
    }

    // ==================== 2. 获取用户所有连接 — 直接通过 Connecte 判断 ====================
    // 通过 Connecte.GetUserConnections 同时判断在线并获取连接列表
    // 无需额外的 Presence 查询（在线状态变化频率不满足缓存条件）
    connResp, err := s.connecteClient.GetUserConnections(ctx, &connecte_pb.GetUserConnectionsRequest{
        UserId: req.UserId,
    })
    if err != nil {
        s.logger.Error("获取用户连接失败",
            zap.String("user_id", req.UserId), zap.Error(err))
        return &pb.PushToUserOtherDevicesResponse{
            Meta:        errorMeta(ctx, codes.Internal, "get user connections failed"),
            PushedCount: 0,
        }, nil
    }

    if len(connResp.Connections) <= 1 {
        // 无连接或仅有一个连接（当前设备），无需多端同步
        return &pb.PushToUserOtherDevicesResponse{
            Meta:        successMeta(ctx),
            PushedCount: 0,
        }, nil
    }

    // ==================== 4. 序列化信号 ====================
    signalData, err := s.serializeSignal(req.Signal)
    if err != nil {
        return nil, status.Error(codes.Internal, "serialize signal failed")
    }

    // ==================== 5. 过滤当前设备，向其他设备发送 ====================
    pushedCount := int32(0)
    for _, conn := range connResp.Connections {
        // 排除发送者的当前设备
        if conn.DeviceId == req.ExcludeDeviceId {
            continue
        }

        _, sendErr := s.connecteClient.SendToConnection(ctx, &connecte_pb.SendToConnectionRequest{
            ConnectionId: conn.ConnectionId,
            UserId:       req.UserId,
            DataType:     "sync_notify",
            Data:         signalData,
        })
        if sendErr != nil {
            s.logger.Warn("多端同步发送失败",
                zap.String("user_id", req.UserId),
                zap.String("connection_id", conn.ConnectionId),
                zap.String("platform", conn.Platform.String()),
                zap.String("exclude_device", req.ExcludeDeviceId),
                zap.Error(sendErr))
            continue
        }
        pushedCount++
    }

    return &pb.PushToUserOtherDevicesResponse{
        Meta:        successMeta(ctx),
        PushedCount: pushedCount,
    }, nil
}
```

### 3. BatchPushToUsers — 批量向多个用户推送

```go
func (s *PushService) BatchPushToUsers(ctx context.Context, req *pb.BatchPushToUsersRequest) (*pb.BatchPushToUsersResponse, error) {
    // ==================== 1. 参数校验 ====================
    if len(req.UserIds) == 0 {
        return nil, status.Error(codes.InvalidArgument, "user_ids is required")
    }
    if req.Signal == nil {
        return nil, status.Error(codes.InvalidArgument, "signal is required")
    }
    if len(req.UserIds) > 10000 {
        return nil, status.Error(codes.InvalidArgument, "user_ids too many, max 10000")
    }

    // ==================== 2. 去重 ====================
    userIDSet := make(map[string]bool, len(req.UserIds))
    uniqueUserIDs := make([]string, 0, len(req.UserIds))
    for _, uid := range req.UserIds {
        if uid != "" && !userIDSet[uid] {
            userIDSet[uid] = true
            uniqueUserIDs = append(uniqueUserIDs, uid)
        }
    }

    // ==================== 3. 批量查询在线状态 — 直接 RPC 查询 Presence 源服务 ====================
    // 单次 RPC Presence.BatchGetPresence 批量查询，避免维护本地缓存
    onlineUserIDs, offlineUserIDs, err := s.batchGetPresenceDirectRPC(ctx, uniqueUserIDs)
    if err != nil {
        s.logger.Error("批量获取在线状态失败", zap.Error(err))
        // 全部视为离线（安全降级）
        offlineUserIDs = uniqueUserIDs
        onlineUserIDs = nil
    }

    // ==================== 4. 序列化信号（只序列化一次，复用 []byte） ====================
    signalData, err := s.serializeSignal(req.Signal)
    if err != nil {
        return nil, status.Error(codes.Internal, "serialize signal failed")
    }

    // ==================== 5. 在线用户 → goroutine 池并发推送 ====================
    var (
        mu          sync.Mutex
        pushedCount int32
        // 推送失败的在线用户归入离线列表
        additionalOffline []string
    )

    if len(onlineUserIDs) > 0 {
        // 使用信号量控制并发度，避免瞬间打满 Connecte RPC
        sem := make(chan struct{}, 50) // 最大 50 个并发 goroutine
        var wg sync.WaitGroup

        for _, uid := range onlineUserIDs {
            uid := uid // capture loop var
            wg.Add(1)
            sem <- struct{}{} // 获取信号量
            go func() {
                defer wg.Done()
                defer func() { <-sem }() // 释放信号量

                // 获取该用户所有连接（必须 RPC）
                connResp, connErr := s.connecteClient.GetUserConnections(ctx,
                    &connecte_pb.GetUserConnectionsRequest{UserId: uid})
                if connErr != nil || len(connResp.Connections) == 0 {
                    // 无连接，归入离线
                    mu.Lock()
                    additionalOffline = append(additionalOffline, uid)
                    mu.Unlock()
                    return
                }

                // 批量发送到该用户所有连接（一次 RPC 搞定）
                connIDs := make([]string, 0, len(connResp.Connections))
                for _, conn := range connResp.Connections {
                    connIDs = append(connIDs, conn.ConnectionId)
                }
                batchResp, sendErr := s.connecteClient.BatchSendToConnections(ctx,
                    &connecte_pb.BatchSendToConnectionsRequest{
                        ConnectionIds: connIDs,
                        DataType:      "sync_notify",
                        Data:          signalData,
                    })
                if sendErr != nil {
                    s.logger.Warn("批量推送发送失败",
                        zap.String("user_id", uid), zap.Error(sendErr))
                    mu.Lock()
                    additionalOffline = append(additionalOffline, uid)
                    mu.Unlock()
                    return
                }
                atomic.AddInt32(&pushedCount, batchResp.SuccessCount)
            }()
        }
        wg.Wait()
    }

    // 合并离线列表
    offlineUserIDs = append(offlineUserIDs, additionalOffline...)

    // ==================== 6. 离线用户 → 批量入队 OfflineQueue ====================
    if len(offlineUserIDs) > 0 {
        // 分批入队，每批 500 个（避免单次 RPC 过大）
        const enqueueBatchSize = 500
        for i := 0; i < len(offlineUserIDs); i += enqueueBatchSize {
            end := i + enqueueBatchSize
            if end > len(offlineUserIDs) {
                end = len(offlineUserIDs)
            }
            batch := offlineUserIDs[i:end]

            _, enqueueErr := s.offlineQueueClient.BatchEnqueueSyncSignal(ctx,
                &offline_queue_pb.BatchEnqueueSyncSignalRequest{
                    UserIds:          batch,
                    ConversationId:   extractConversationID(req.Signal),
                    ConversationType: extractConversationType(req.Signal),
                    MaxSeq:           req.Signal.Seq,
                    SenderId:         extractSenderID(req.Signal),
                    SenderName:       extractSenderName(req.Signal),
                    MsgType:          extractMsgType(req.Signal),
                    ContentPreview:   extractContentPreview(req.Signal),
                    Priority:         0,
                    ExpireAt:         time.Now().Add(7 * 24 * time.Hour).UnixMilli(),
                })
            if enqueueErr != nil {
                s.logger.Error("批量入队 OfflineQueue 失败",
                    zap.Int("batch_size", len(batch)), zap.Error(enqueueErr))
            }
        }
    }

    // ==================== 7. 推送统计 ====================
    s.kafka.Produce(ctx, "push.stats", "batch", &kafka_push.PushStatsEvent{
        Header:            buildEventHeader("push", s.instanceID),
        ServerId:          s.instanceID,
        OnlinePushCount:   int64(len(onlineUserIDs)),
        OnlinePushSuccess: int64(pushedCount),
        OfflinePushCount:  int64(len(offlineUserIDs)),
    })

    return &pb.BatchPushToUsersResponse{
        Meta:         successMeta(ctx),
        OnlineCount:  int32(len(onlineUserIDs)),
        PushedCount:  pushedCount,
        OfflineUsers: offlineUserIDs,
    }, nil
}
```

### 4. PushToGroup — 向群组所有在线成员推送（核心优化方法）

```go
func (s *PushService) PushToGroup(ctx context.Context, req *pb.PushToGroupRequest) (*pb.PushToGroupResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.GroupId == "" {
        return nil, status.Error(codes.InvalidArgument, "group_id is required")
    }
    if req.Signal == nil {
        return nil, status.Error(codes.InvalidArgument, "signal is required")
    }

    // ==================== 2. 获取群成员列表 — 本地缓存（最重要的优化点） ====================
    //
    // 旧方案（性能灾难）：
    //   RPC Group.GetGroupMembers → 每条群消息都调一次 RPC
    //   千人群 + 每秒 100 条消息 = 100 次 RPC/s（仅此一项）
    //
    // 新方案（本地缓存）：
    //   读本地 Redis SET push:local:group_members:{group_id}
    //   该 SET 由 kafka/push consumer 通过以下事件维护：
    //     - group.member.joined → SADD
    //     - group.member.left   → SREM
    //     - group.member.kicked → SREM
    //   缓存未命中（首次或 TTL 过期）→ 降级 RPC + 回填
    //
    memberIDs, err := s.getGroupMembersFromLocalCache(ctx, req.GroupId)
    if err != nil {
        s.logger.Error("获取群成员列表失败",
            zap.String("group_id", req.GroupId), zap.Error(err))
        return nil, status.Error(codes.Internal, "get group members failed")
    }

    totalMembers := int32(len(memberIDs))
    if totalMembers == 0 {
        return &pb.PushToGroupResponse{
            Meta:         successMeta(ctx),
            TotalMembers: 0,
            PushedCount:  0,
        }, nil
    }

    // ==================== 3. 过滤排除用户 ====================
    // 排除列表包含：发送者自己 + 已屏蔽该群的用户 + 其他需排除的用户
    excludeSet := make(map[string]bool, len(req.ExcludeUserIds))
    for _, uid := range req.ExcludeUserIds {
        excludeSet[uid] = true
    }

    filteredMemberIDs := make([]string, 0, len(memberIDs))
    for _, uid := range memberIDs {
        if !excludeSet[uid] {
            filteredMemberIDs = append(filteredMemberIDs, uid)
        }
    }

    if len(filteredMemberIDs) == 0 {
        return &pb.PushToGroupResponse{
            Meta:         successMeta(ctx),
            TotalMembers: totalMembers,
            PushedCount:  0,
        }, nil
    }

    // ==================== 4. 大群优化策略判断 ====================
    // 小群（<200人）：实时全量扇出，所有在线成员推送，离线成员入队 OfflineQueue
    // 大群（>=200人）：lazy notify，仅推送给在线成员，离线成员不入队（上线后自行拉取）
    isLargeGroup := len(filteredMemberIDs) >= 200 || req.LargeGroupLazy

    // ==================== 5. 批量查询在线状态 — 直接 RPC 查询 Presence 源服务 ====================
    //
    // 旧方案（性能灾难 × 2）：
    //   for uid := range members { RPC Presence.GetPresence(uid) }
    //   千人群 = 1000 次 RPC
    //
    // 新方案：
    //   单次 RPC Presence.BatchGetPresence 批量查询（源服务直查，无缓存一致性问题）
    //   在线状态变化频繁，不满足「高频读取 + 变化不频繁」的本地缓存条件
    //
    var onlineUserIDs, offlineUserIDs []string

    if len(req.OnlineMemberIds) > 0 {
        // 上游已预查在线成员（优化路径：Message 服务已知在线列表，避免 Push 再查一次）
        onlineSet := make(map[string]bool, len(req.OnlineMemberIds))
        for _, uid := range req.OnlineMemberIds {
            onlineSet[uid] = true
        }
        for _, uid := range filteredMemberIDs {
            if onlineSet[uid] {
                onlineUserIDs = append(onlineUserIDs, uid)
            } else {
                offlineUserIDs = append(offlineUserIDs, uid)
            }
        }
    } else {
        // 直接查询 Presence 源服务
        onlineUserIDs, offlineUserIDs, err = s.batchGetPresenceDirectRPC(ctx, filteredMemberIDs)
        if err != nil {
            s.logger.Error("批量获取群成员在线状态失败",
                zap.String("group_id", req.GroupId), zap.Error(err))
            // 安全降级：全部视为离线
            offlineUserIDs = filteredMemberIDs
            onlineUserIDs = nil
        }
    }

    // 大群优化：如果是大群且在线用户过少，只推在线的
    if isLargeGroup {
        s.logger.Debug("大群推送模式",
            zap.String("group_id", req.GroupId),
            zap.Int("total_members", len(filteredMemberIDs)),
            zap.Int("online_count", len(onlineUserIDs)),
            zap.Int("offline_count", len(offlineUserIDs)))
    }

    // ==================== 6. 序列化信号（只序列化一次） ====================
    signalData, err := s.serializeSignal(req.Signal)
    if err != nil {
        return nil, status.Error(codes.Internal, "serialize signal failed")
    }

    // ==================== 7. 在线用户 → goroutine 池并发推送 ====================
    var (
        pushedCount    int32
        failedOnline   []string
        failedOnlineMu sync.Mutex
    )

    if len(onlineUserIDs) > 0 {
        sem := make(chan struct{}, 100) // 并发度 100
        var wg sync.WaitGroup

        for _, uid := range onlineUserIDs {
            uid := uid
            wg.Add(1)
            sem <- struct{}{}
            go func() {
                defer wg.Done()
                defer func() { <-sem }()

                // 获取该用户所有连接（RPC Connecte，实时路由不可缓存）
                connResp, connErr := s.connecteClient.GetUserConnections(ctx,
                    &connecte_pb.GetUserConnectionsRequest{UserId: uid})
                if connErr != nil || len(connResp.Connections) == 0 {
                    failedOnlineMu.Lock()
                    failedOnline = append(failedOnline, uid)
                    failedOnlineMu.Unlock()
                    return
                }

                // 收集所有连接 ID，用 BatchSendToConnections 一次性推送
                connIDs := make([]string, 0, len(connResp.Connections))
                for _, conn := range connResp.Connections {
                    connIDs = append(connIDs, conn.ConnectionId)
                }

                batchResp, sendErr := s.connecteClient.BatchSendToConnections(ctx,
                    &connecte_pb.BatchSendToConnectionsRequest{
                        ConnectionIds: connIDs,
                        DataType:      "sync_notify",
                        Data:          signalData,
                    })
                if sendErr != nil {
                    s.logger.Warn("群推送发送失败",
                        zap.String("group_id", req.GroupId),
                        zap.String("user_id", uid),
                        zap.Error(sendErr))
                    failedOnlineMu.Lock()
                    failedOnline = append(failedOnline, uid)
                    failedOnlineMu.Unlock()
                    return
                }
                atomic.AddInt32(&pushedCount, batchResp.SuccessCount)
            }()
        }
        wg.Wait()

        // 推送失败的在线用户归入离线
        offlineUserIDs = append(offlineUserIDs, failedOnline...)
    }

    // ==================== 8. 离线用户处理 ====================
    if len(offlineUserIDs) > 0 {
        if isLargeGroup {
            // ---- 大群 lazy 模式 ----
            // 不为离线用户入队 OfflineQueue（他们上线后走标准的 OfflineQueue 批量拉取机制）
            // 这样 1000 人群不需要为 800 个离线用户各写一条队列
            s.logger.Debug("大群 lazy 模式，跳过离线入队",
                zap.String("group_id", req.GroupId),
                zap.Int("offline_count", len(offlineUserIDs)))
        } else {
            // ---- 小群全量模式 ----

            // 8a. 批量入队 OfflineQueue
            const enqueueBatchSize = 500
            for i := 0; i < len(offlineUserIDs); i += enqueueBatchSize {
                end := i + enqueueBatchSize
                if end > len(offlineUserIDs) {
                    end = len(offlineUserIDs)
                }
                batch := offlineUserIDs[i:end]

                _, enqueueErr := s.offlineQueueClient.BatchEnqueueSyncSignal(ctx,
                    &offline_queue_pb.BatchEnqueueSyncSignalRequest{
                        UserIds:          batch,
                        ConversationId:   extractConversationID(req.Signal),
                        ConversationType: common_pb.CONVERSATION_TYPE_GROUP,
                        MaxSeq:           req.Signal.Seq,
                        SenderId:         extractSenderID(req.Signal),
                        SenderName:       extractSenderName(req.Signal),
                        MsgType:          extractMsgType(req.Signal),
                        ContentPreview:   extractContentPreview(req.Signal),
                        Priority:         0,
                        ExpireAt:         time.Now().Add(7 * 24 * time.Hour).UnixMilli(),
                    })
                if enqueueErr != nil {
                    s.logger.Error("群离线入队 OfflineQueue 失败",
                        zap.String("group_id", req.GroupId),
                        zap.Int("batch_size", len(batch)),
                        zap.Error(enqueueErr))
                }
            }

            // 8b. 为离线用户触发 APNs/FCM 推送（通过 Kafka push.offline.trigger）
            for _, uid := range offlineUserIDs {
                msgID := extractMsgID(req.Signal)
                dedupKey := fmt.Sprintf("push:offline_dedup:%s:%s", msgID, uid)
                isNew, _ := s.redis.SetNX(ctx, dedupKey, "1", 5*time.Minute).Result()
                if isNew {
                    s.kafka.Produce(ctx, "push.offline.trigger", uid,
                        &kafka_push.PushOfflineTriggerEvent{
                            Header:         buildEventHeader("push", s.instanceID),
                            UserId:         uid,
                            Title:          buildGroupOfflineTitle(req.Signal),
                            Content:        buildGroupOfflineContent(req.Signal),
                            MsgId:          msgID,
                            ConversationId: extractConversationID(req.Signal),
                            SenderId:       extractSenderID(req.Signal),
                        })
                }
            }
        }
    }

    // ==================== 9. 推送统计 ====================
    s.kafka.Produce(ctx, "push.stats", req.GroupId, &kafka_push.PushStatsEvent{
        Header:            buildEventHeader("push", s.instanceID),
        ServerId:          s.instanceID,
        OnlinePushCount:   int64(len(onlineUserIDs)),
        OnlinePushSuccess: int64(pushedCount),
        OfflinePushCount:  int64(len(offlineUserIDs)),
    })

    return &pb.PushToGroupResponse{
        Meta:         successMeta(ctx),
        TotalMembers: totalMembers,
        PushedCount:  pushedCount,
        OfflineUsers: offlineUserIDs,
    }, nil
}
```

### 5. BroadcastPush — 全局广播推送

```go
func (s *PushService) BroadcastPush(ctx context.Context, req *pb.BroadcastPushRequest) (*pb.BroadcastPushResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.Signal == nil {
        return nil, status.Error(codes.InvalidArgument, "signal is required")
    }

    // ==================== 2. 生成广播 ID ====================
    broadcastID := generateBroadcastID() // UUID v4

    // ==================== 3. 初始化广播进度 — Redis HASH ====================
    progressKey := fmt.Sprintf("push:broadcast_progress:%s", broadcastID)
    s.redis.HSet(ctx, progressKey, map[string]interface{}{
        "status":     "running",
        "start_time": fmt.Sprintf("%d", time.Now().UnixMilli()),
        "total":      "0",
        "done":       "0",
        "fail":       "0",
    })
    s.redis.Expire(ctx, progressKey, 24*time.Hour)

    // ==================== 4. 获取所有网关节点 — RPC Connecte.GetGatewayNodes ====================
    nodesResp, err := s.connecteClient.GetGatewayNodes(ctx, &connecte_pb.GetGatewayNodesRequest{})
    if err != nil {
        return nil, status.Error(codes.Internal, "get gateway nodes failed")
    }

    if len(nodesResp.Nodes) == 0 {
        s.logger.Warn("无在线网关节点，广播取消")
        s.redis.HSet(ctx, progressKey, "status", "no_nodes")
        return &pb.BroadcastPushResponse{
            Meta:        successMeta(ctx),
            PushedCount: 0,
        }, nil
    }

    // ==================== 5. 序列化信号 ====================
    signalData, err := s.serializeSignal(req.Signal)
    if err != nil {
        return nil, status.Error(codes.Internal, "serialize signal failed")
    }

    // ==================== 6. 过滤目标平台 ====================
    targetPlatforms := make(map[common_pb.PlatformType]bool)
    if len(req.Platforms) > 0 {
        for _, p := range req.Platforms {
            targetPlatforms[p] = true
        }
    }
    // 空 = 全平台

    // ==================== 7. 更新进度：总节点数 ====================
    activeNodes := make([]*connecte_pb.GatewayNode, 0)
    for _, node := range nodesResp.Nodes {
        if node.Status == "running" {
            activeNodes = append(activeNodes, node)
        }
    }
    s.redis.HSet(ctx, progressKey, "total", fmt.Sprintf("%d", len(activeNodes)))

    // ==================== 8. 向每个网关节点广播 — 并行 RPC ====================
    var (
        totalPushed int32
        mu          sync.Mutex
    )

    var wg sync.WaitGroup
    for _, node := range activeNodes {
        node := node
        wg.Add(1)
        go func() {
            defer wg.Done()

            // 向该网关节点的所有连接广播
            // 使用特殊的 connection_id 格式 "*:{server_id}" 表示该节点所有连接
            batchResp, sendErr := s.connecteClient.BatchSendToConnections(ctx,
                &connecte_pb.BatchSendToConnectionsRequest{
                    ConnectionIds: []string{"broadcast:" + node.ServerId},
                    DataType:      "broadcast_notify",
                    Data:          signalData,
                })
            if sendErr != nil {
                s.logger.Error("向网关节点广播失败",
                    zap.String("server_id", node.ServerId),
                    zap.String("addr", node.Addr),
                    zap.Error(sendErr))
                mu.Lock()
                s.redis.HIncrBy(ctx, progressKey, "fail", 1)
                mu.Unlock()
                return
            }

            atomic.AddInt32(&totalPushed, batchResp.SuccessCount)
            mu.Lock()
            s.redis.HIncrBy(ctx, progressKey, "done", 1)
            mu.Unlock()
        }()
    }
    wg.Wait()

    // ==================== 9. 更新广播进度 — 完成 ====================
    s.redis.HSet(ctx, progressKey, map[string]interface{}{
        "status":   "completed",
        "end_time": fmt.Sprintf("%d", time.Now().UnixMilli()),
        "pushed":   fmt.Sprintf("%d", totalPushed),
    })

    // ==================== 10. 推送统计 ====================
    s.kafka.Produce(ctx, "push.stats", broadcastID, &kafka_push.PushStatsEvent{
        Header:            buildEventHeader("push", s.instanceID),
        ServerId:          s.instanceID,
        OnlinePushCount:   int64(totalPushed),
        OnlinePushSuccess: int64(totalPushed),
    })

    return &pb.BroadcastPushResponse{
        Meta:        successMeta(ctx),
        PushedCount: totalPushed,
    }, nil
}
```

### 6. RegisterDevice — 注册推送设备

```go
func (s *PushService) RegisterDevice(ctx context.Context, req *pb.RegisterDeviceRequest) (*pb.RegisterDeviceResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id is required")
    }
    if req.DeviceId == "" {
        return nil, status.Error(codes.InvalidArgument, "device_id is required")
    }
    if req.PushToken == "" {
        return nil, status.Error(codes.InvalidArgument, "push_token is required")
    }
    if req.PushChannel == "" {
        return nil, status.Error(codes.InvalidArgument, "push_channel is required")
    }
    // 校验推送通道合法性
    validChannels := map[string]bool{
        "apns": true, "fcm": true, "huawei": true,
        "xiaomi": true, "oppo": true, "vivo": true,
    }
    if !validChannels[req.PushChannel] {
        return nil, status.Error(codes.InvalidArgument, "invalid push_channel, must be: apns/fcm/huawei/xiaomi/oppo/vivo")
    }

    now := time.Now().UnixMilli()
    cacheKey := fmt.Sprintf("push:device:%s", req.UserId)

    // ==================== 2. 延迟双删 — 第一次立即删除缓存 ====================
    // 延迟双删模式：delete cache → update DB → goroutine { sleep 500ms → delete cache again }
    s.delayedDoubleDelete(ctx, cacheKey)

    // ==================== 3. UPSERT 到 PgSQL ====================
    _, err := s.db.ExecContext(ctx,
        `INSERT INTO push_devices (user_id, device_id, platform, push_token, push_channel, is_enabled, last_active_at, created_at, updated_at)
         VALUES ($1, $2, $3, $4, $5, TRUE, $6, $6, $6)
         ON CONFLICT (user_id, device_id) DO UPDATE SET
            push_token     = EXCLUDED.push_token,
            push_channel   = EXCLUDED.push_channel,
            platform       = EXCLUDED.platform,
            is_enabled     = TRUE,
            last_active_at = EXCLUDED.last_active_at,
            updated_at     = EXCLUDED.updated_at`,
        req.UserId, req.DeviceId, int32(req.Platform), req.PushToken, req.PushChannel, now,
    )
    if err != nil {
        return nil, status.Error(codes.Internal, "register device to PgSQL failed")
    }

    // ==================== 4. 回填 Redis 缓存（让后续读能命中） ====================
    // 注意：延迟双删的第二次删除会在 500ms 后执行，
    // 但在此之前回填是安全的（因为 DB 已经是最新的）
    deviceJSON, _ := json.Marshal(map[string]interface{}{
        "device_id":    req.DeviceId,
        "user_id":      req.UserId,
        "platform":     int32(req.Platform),
        "push_token":   req.PushToken,
        "push_channel": req.PushChannel,
        "enabled":      true,
    })
    s.redis.HSet(ctx, cacheKey, req.DeviceId, string(deviceJSON))
    s.redis.Expire(ctx, cacheKey, 7*24*time.Hour)

    // ==================== 5. Token 冲突检测：同一 push_token 是否绑定在其他用户/设备上 ====================
    // APNs/FCM token 是设备物理唯一的，如果同一 token 出现在另一个 (user_id, device_id) 上，
    // 说明设备换了账号，需要解绑旧的
    var oldUserID, oldDeviceID string
    err = s.db.QueryRowContext(ctx,
        `SELECT user_id, device_id FROM push_devices
         WHERE push_token = $1 AND NOT (user_id = $2 AND device_id = $3) AND is_enabled = TRUE
         LIMIT 1`,
        req.PushToken, req.UserId, req.DeviceId,
    ).Scan(&oldUserID, &oldDeviceID)

    if err == nil && oldUserID != "" {
        // 存在旧绑定，禁用旧设备的推送
        s.logger.Info("Push token 冲突，禁用旧设备",
            zap.String("old_user", oldUserID),
            zap.String("old_device", oldDeviceID),
            zap.String("new_user", req.UserId),
            zap.String("new_device", req.DeviceId))

        _, _ = s.db.ExecContext(ctx,
            `UPDATE push_devices SET is_enabled = FALSE, updated_at = $1
             WHERE user_id = $2 AND device_id = $3`,
            now, oldUserID, oldDeviceID,
        )
        // 延迟双删旧用户的设备缓存
        oldCacheKey := fmt.Sprintf("push:device:%s", oldUserID)
        s.delayedDoubleDelete(ctx, oldCacheKey)
    }

    return &pb.RegisterDeviceResponse{
        Meta: successMeta(ctx),
    }, nil
}
```

### 7. UnregisterDevice — 注销推送设备

```go
func (s *PushService) UnregisterDevice(ctx context.Context, req *pb.UnregisterDeviceRequest) (*pb.UnregisterDeviceResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id is required")
    }
    if req.DeviceId == "" {
        return nil, status.Error(codes.InvalidArgument, "device_id is required")
    }

    now := time.Now().UnixMilli()
    cacheKey := fmt.Sprintf("push:device:%s", req.UserId)

    // ==================== 2. 延迟双删 — 第一次立即删除缓存 ====================
    s.delayedDoubleDelete(ctx, cacheKey)

    // ==================== 3. PgSQL 软删除（禁用推送，保留记录用于审计） ====================
    result, err := s.db.ExecContext(ctx,
        `UPDATE push_devices SET is_enabled = FALSE, updated_at = $1
         WHERE user_id = $2 AND device_id = $3 AND is_enabled = TRUE`,
        now, req.UserId, req.DeviceId,
    )
    if err != nil {
        return nil, status.Error(codes.Internal, "unregister device from PgSQL failed")
    }

    rowsAffected, _ := result.RowsAffected()
    if rowsAffected == 0 {
        return nil, status.Error(codes.NotFound, "device not found or already unregistered")
    }

    // ==================== 4. 清除角标缓存 ====================
    badgeKey := fmt.Sprintf("push:badge:%s:%s", req.UserId, req.DeviceId)
    s.redis.Del(ctx, badgeKey)

    s.logger.Info("推送设备注销成功",
        zap.String("user_id", req.UserId),
        zap.String("device_id", req.DeviceId))

    return &pb.UnregisterDeviceResponse{
        Meta: successMeta(ctx),
    }, nil
}
```

### 8. SendOfflinePush — 发送离线推送（APNs / FCM / 厂商通道）

```go
func (s *PushService) SendOfflinePush(ctx context.Context, req *pb.SendOfflinePushRequest) (*pb.SendOfflinePushResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id is required")
    }
    if req.Payload == nil {
        return nil, status.Error(codes.InvalidArgument, "payload is required")
    }

    // ==================== 2. 推送前再次确认用户是否仍然离线 ====================
    // 防止在 Kafka 消费延迟期间用户已上线的情况
    // 直接查询 Connecte（连接层源服务），不使用本地缓存（在线状态变化频繁）
    connections, _ := s.getUserOnlineConnections(ctx, req.UserId)
    if len(connections) > 0 {
        s.logger.Debug("用户已上线，跳过离线推送",
            zap.String("user_id", req.UserId))
        return &pb.SendOfflinePushResponse{
            Meta: successMeta(ctx),
            Sent: false,
        }, nil
    }

    // ==================== 3. 查询用户推送设备 — Redis 缓存 / PgSQL ====================
    devices, err := s.getUserDevicesInternal(ctx, req.UserId)
    if err != nil {
        return nil, status.Error(codes.Internal, "get user devices failed")
    }

    if len(devices) == 0 {
        s.logger.Debug("用户无注册推送设备",
            zap.String("user_id", req.UserId))
        return &pb.SendOfflinePushResponse{
            Meta: successMeta(ctx),
            Sent: false,
        }, nil
    }

    // ==================== 4. 获取用户推送设置 — 本地缓存 ====================
    userSettings, err := s.getUserSettingsFromLocalCache(ctx, req.UserId)
    if err != nil {
        s.logger.Warn("获取用户推送设置失败，使用默认值",
            zap.String("user_id", req.UserId), zap.Error(err))
        userSettings = &localUserSettings{PushEnabled: true, ShowContent: true, ShowSender: true}
    }

    // 推送总开关关闭 → 不发送
    if !userSettings.PushEnabled {
        s.logger.Debug("用户已关闭推送",
            zap.String("user_id", req.UserId))
        return &pb.SendOfflinePushResponse{
            Meta: successMeta(ctx),
            Sent: false,
        }, nil
    }

    // 免打扰时段 → 不发送
    if s.isDndTime(userSettings.DndStart, userSettings.DndEnd) {
        s.logger.Debug("用户处于免打扰时段，跳过离线推送",
            zap.String("user_id", req.UserId),
            zap.String("dnd_start", userSettings.DndStart),
            zap.String("dnd_end", userSettings.DndEnd))
        return &pb.SendOfflinePushResponse{
            Meta: successMeta(ctx),
            Sent: false,
        }, nil
    }

    // ==================== 5. 获取用户信息 — 本地缓存（构造推送标题） ====================
    nickname, _, infoErr := s.getUserInfoFromLocalCache(ctx, req.UserId)
    if infoErr != nil {
        s.logger.Warn("获取用户信息失败，使用默认标题",
            zap.String("user_id", req.UserId), zap.Error(infoErr))
        nickname = ""
    }

    // ==================== 6. 根据用户设置构造推送内容 ====================
    pushTitle := req.Payload.Title
    pushContent := req.Payload.Content

    // 不展示内容 → 用通用提示替换
    if !userSettings.ShowContent {
        pushContent = "你收到了一条新消息"
    }
    // 不展示发送者 → 用通用标题
    if !userSettings.ShowSender {
        pushTitle = "新消息"
    } else if nickname != "" && pushTitle == "" {
        pushTitle = nickname
    }

    // ==================== 7. 向每个已注册设备发送离线推送 ====================
    sentAny := false
    for _, device := range devices {
        if !device.Enabled {
            continue
        }

        // 更新角标计数
        badgeKey := fmt.Sprintf("push:badge:%s:%s", req.UserId, device.DeviceId)
        newBadge, badgeErr := s.redis.Incr(ctx, badgeKey).Result()
        if badgeErr != nil {
            newBadge = 1 // 默认角标 1
        }

        // 根据推送通道调用对应 SDK
        var pushErr error
        switch device.PushChannel {
        case "apns":
            pushErr = s.apnsPusher.Send(ctx, &APNsPayload{
                Token:   device.PushToken,
                Title:   pushTitle,
                Body:    pushContent,
                Badge:   int(newBadge),
                Sound:   req.Payload.Sound,
                Data:    req.Payload.Data,
            })

        case "fcm":
            pushErr = s.fcmPusher.Send(ctx, &FCMPayload{
                Token:    device.PushToken,
                Title:    pushTitle,
                Body:     pushContent,
                Badge:    int(newBadge),
                Sound:    req.Payload.Sound,
                Data:     req.Payload.Data,
                ImageURL: req.Payload.ImageUrl,
            })

        case "huawei":
            pushErr = s.huaweiPusher.Send(ctx, &HuaweiPayload{
                Token: device.PushToken,
                Title: pushTitle,
                Body:  pushContent,
                Badge: int(newBadge),
                Data:  req.Payload.Data,
            })

        case "xiaomi":
            pushErr = s.xiaomiPusher.Send(ctx, &XiaomiPayload{
                Token: device.PushToken,
                Title: pushTitle,
                Body:  pushContent,
                Badge: int(newBadge),
                Data:  req.Payload.Data,
            })

        case "oppo":
            pushErr = s.oppoPusher.Send(ctx, &OPPOPayload{
                Token: device.PushToken,
                Title: pushTitle,
                Body:  pushContent,
                Data:  req.Payload.Data,
            })

        case "vivo":
            pushErr = s.vivoPusher.Send(ctx, &VivoPayload{
                Token: device.PushToken,
                Title: pushTitle,
                Body:  pushContent,
                Data:  req.Payload.Data,
            })

        default:
            s.logger.Warn("未知推送通道，跳过",
                zap.String("channel", device.PushChannel),
                zap.String("device_id", device.DeviceId))
            continue
        }

        if pushErr != nil {
            s.logger.Error("离线推送发送失败",
                zap.String("user_id", req.UserId),
                zap.String("device_id", device.DeviceId),
                zap.String("channel", device.PushChannel),
                zap.Error(pushErr))

            // 生产离线推送失败事件
            s.kafka.Produce(ctx, "push.offline.result", req.UserId,
                &kafka_push.PushOfflineResultEvent{
                    Header:      buildEventHeader("push", s.instanceID),
                    PushId:      req.Payload.PushId,
                    UserId:      req.UserId,
                    PushChannel: device.PushChannel,
                    Success:     false,
                    ErrorMsg:    pushErr.Error(),
                    PushTime:    time.Now().UnixMilli(),
                })

            // Token 失效检测：如果是 token 无效/过期错误，自动禁用该设备
            if isTokenInvalidError(pushErr) {
                s.logger.Warn("Push token 已失效，自动禁用设备",
                    zap.String("user_id", req.UserId),
                    zap.String("device_id", device.DeviceId),
                    zap.String("channel", device.PushChannel))
                _, _ = s.db.ExecContext(ctx,
                    `UPDATE push_devices SET is_enabled = FALSE, updated_at = $1
                     WHERE user_id = $2 AND device_id = $3`,
                    time.Now().UnixMilli(), req.UserId, device.DeviceId,
                )
                deviceCacheKey := fmt.Sprintf("push:device:%s", req.UserId)
                s.delayedDoubleDelete(ctx, deviceCacheKey)
            }
            continue
        }

        sentAny = true

        // 生产离线推送成功事件
        s.kafka.Produce(ctx, "push.offline.result", req.UserId,
            &kafka_push.PushOfflineResultEvent{
                Header:      buildEventHeader("push", s.instanceID),
                PushId:      req.Payload.PushId,
                UserId:      req.UserId,
                PushChannel: device.PushChannel,
                Success:     true,
                PushTime:    time.Now().UnixMilli(),
            })
    }

    // ==================== 8. 推送统计 ====================
    s.kafka.Produce(ctx, "push.stats", req.UserId, &kafka_push.PushStatsEvent{
        Header:             buildEventHeader("push", s.instanceID),
        ServerId:           s.instanceID,
        OfflinePushCount:   1,
        OfflinePushSuccess: boolToInt64(sentAny),
        OfflinePushFail:    boolToInt64(!sentAny),
    })

    return &pb.SendOfflinePushResponse{
        Meta: successMeta(ctx),
        Sent: sentAny,
    }, nil
}
```

### 9. GetUserDevices — 查询用户已注册的推送设备列表

```go
func (s *PushService) GetUserDevices(ctx context.Context, req *pb.GetUserDevicesRequest) (*pb.GetUserDevicesResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id is required")
    }

    // ==================== 2. 查 Redis HASH 缓存 ====================
    cacheKey := fmt.Sprintf("push:device:%s", req.UserId)
    cachedDevices, err := s.redis.HGetAll(ctx, cacheKey).Result()
    if err != nil && err != redis.Nil {
        s.logger.Warn("读取设备缓存失败",
            zap.String("user_id", req.UserId), zap.Error(err))
    }

    // ==================== 3. 缓存命中 → 直接返回 ====================
    if len(cachedDevices) > 0 {
        devices := make([]*pb.PushDevice, 0, len(cachedDevices))
        for _, deviceJSON := range cachedDevices {
            var deviceMap map[string]interface{}
            if err := json.Unmarshal([]byte(deviceJSON), &deviceMap); err != nil {
                s.logger.Warn("反序列化设备缓存 JSON 失败", zap.Error(err))
                continue
            }
            device := &pb.PushDevice{
                DeviceId:    getStringFromMap(deviceMap, "device_id"),
                UserId:      getStringFromMap(deviceMap, "user_id"),
                Platform:    common_pb.PlatformType(getInt32FromMap(deviceMap, "platform")),
                PushToken:   getStringFromMap(deviceMap, "push_token"),
                PushChannel: getStringFromMap(deviceMap, "push_channel"),
                Enabled:     getBoolFromMap(deviceMap, "enabled"),
            }
            // 只返回已启用的设备
            if device.Enabled {
                devices = append(devices, device)
            }
        }

        if len(devices) > 0 {
            return &pb.GetUserDevicesResponse{
                Meta:    successMeta(ctx),
                Devices: devices,
            }, nil
        }
        // 缓存中全是已禁用的设备，当作未命中继续查 DB
    }

    // ==================== 4. 缓存未命中 → 查 PgSQL ====================
    rows, err := s.db.QueryContext(ctx,
        `SELECT device_id, user_id, platform, push_token, push_channel, is_enabled, created_at
         FROM push_devices
         WHERE user_id = $1 AND is_enabled = TRUE
         ORDER BY last_active_at DESC`,
        req.UserId,
    )
    if err != nil {
        return nil, status.Error(codes.Internal, "query user devices from PgSQL failed")
    }
    defer rows.Close()

    devices := make([]*pb.PushDevice, 0)
    for rows.Next() {
        var (
            deviceID    string
            userID      string
            platform    int32
            pushToken   string
            pushChannel string
            isEnabled   bool
            createdAt   int64
        )
        if err := rows.Scan(&deviceID, &userID, &platform, &pushToken, &pushChannel, &isEnabled, &createdAt); err != nil {
            s.logger.Error("扫描设备行失败", zap.Error(err))
            continue
        }
        devices = append(devices, &pb.PushDevice{
            DeviceId:     deviceID,
            UserId:       userID,
            Platform:     common_pb.PlatformType(platform),
            PushToken:    pushToken,
            PushChannel:  pushChannel,
            Enabled:      isEnabled,
            RegisteredAt: createdAt,
        })
    }
    if err := rows.Err(); err != nil {
        return nil, status.Error(codes.Internal, "iterate device rows failed")
    }

    // ==================== 5. 回填 Redis 缓存 ====================
    if len(devices) > 0 {
        pipe := s.redis.Pipeline()
        for _, device := range devices {
            deviceJSON, _ := json.Marshal(map[string]interface{}{
                "device_id":    device.DeviceId,
                "user_id":      device.UserId,
                "platform":     int32(device.Platform),
                "push_token":   device.PushToken,
                "push_channel": device.PushChannel,
                "enabled":      device.Enabled,
            })
            pipe.HSet(ctx, cacheKey, device.DeviceId, string(deviceJSON))
        }
        pipe.Expire(ctx, cacheKey, 7*24*time.Hour)
        if _, err := pipe.Exec(ctx); err != nil {
            s.logger.Warn("回填设备缓存失败",
                zap.String("user_id", req.UserId), zap.Error(err))
        }
    }

    return &pb.GetUserDevicesResponse{
        Meta:    successMeta(ctx),
        Devices: devices,
    }, nil
}
```

---

## 内部辅助方法

### getUserDevicesInternal — 内部获取用户推送设备

```go
// getUserDevicesInternal 内部方法，获取用户已启用的推送设备
// 先查 Redis HASH 缓存，未命中查 PgSQL 并回填
// 被 SendOfflinePush 调用
func (s *PushService) getUserDevicesInternal(ctx context.Context, userID string) ([]*localPushDevice, error) {
    cacheKey := fmt.Sprintf("push:device:%s", userID)

    // ==================== 1. 查 Redis HASH ====================
    cachedDevices, err := s.redis.HGetAll(ctx, cacheKey).Result()
    if err == nil && len(cachedDevices) > 0 {
        devices := make([]*localPushDevice, 0, len(cachedDevices))
        for _, deviceJSON := range cachedDevices {
            var d localPushDevice
            if err := json.Unmarshal([]byte(deviceJSON), &d); err != nil {
                continue
            }
            if d.Enabled {
                devices = append(devices, &d)
            }
        }
        if len(devices) > 0 {
            return devices, nil
        }
    }

    // ==================== 2. 缓存未命中 → 查 PgSQL ====================
    rows, err := s.db.QueryContext(ctx,
        `SELECT device_id, user_id, platform, push_token, push_channel, is_enabled
         FROM push_devices
         WHERE user_id = $1 AND is_enabled = TRUE`,
        userID,
    )
    if err != nil {
        return nil, fmt.Errorf("query push devices failed: %w", err)
    }
    defer rows.Close()

    devices := make([]*localPushDevice, 0)
    pipe := s.redis.Pipeline()
    for rows.Next() {
        var d localPushDevice
        if err := rows.Scan(&d.DeviceId, &d.UserId, &d.Platform, &d.PushToken, &d.PushChannel, &d.Enabled); err != nil {
            s.logger.Warn("扫描设备行失败", zap.Error(err))
            continue
        }
        devices = append(devices, &d)

        // 回填缓存
        deviceJSON, _ := json.Marshal(d)
        pipe.HSet(ctx, cacheKey, d.DeviceId, string(deviceJSON))
    }

    // ==================== 3. 回填缓存 TTL ====================
    if len(devices) > 0 {
        pipe.Expire(ctx, cacheKey, 7*24*time.Hour)
        if _, err := pipe.Exec(ctx); err != nil {
            s.logger.Warn("回填设备缓存失败", zap.String("user_id", userID), zap.Error(err))
        }
    }

    return devices, nil
}
```

---

## 本地数据结构

```go
// localUserSettings 用户推送设置本地缓存结构
type localUserSettings struct {
    PushEnabled bool   `json:"push_enabled"`
    DndStart    string `json:"dnd_start"`   // "HH:MM" 格式
    DndEnd      string `json:"dnd_end"`     // "HH:MM" 格式
    ShowContent bool   `json:"show_content"`
    ShowSender  bool   `json:"show_sender"`
}

// localPushDevice 推送设备本地缓存结构
type localPushDevice struct {
    DeviceId    string `json:"device_id"`
    UserId      string `json:"user_id"`
    Platform    int32  `json:"platform"`
    PushToken   string `json:"push_token"`
    PushChannel string `json:"push_channel"`
    Enabled     bool   `json:"enabled"`
}
```

---

## 架构总结

### 本地缓存 vs RPC 调用决策矩阵

| 数据 | 来源 | 变更频率 | 一致性要求 | 决策 | 缓存 TTL |
|------|------|---------|-----------|------|---------|
| 用户在线状态 | Presence | 高（秒级） | 最终一致（5s 内） | **本地缓存** | 5min |
| 群成员列表 | Group | 低（分钟级） | 最终一致（30s 内） | **本地缓存** | 30min |
| 用户推送设置 | User | 极低（天级） | 最终一致 | **本地缓存** | 1h |
| 用户基础信息 | User | 低 | 最终一致 | **本地缓存** | 1h |
| 用户连接路由 | Connecte | 极高（毫秒级） | 强一致 | **RPC 实时查询** | 不缓存 |
| 连接发送 | Connecte | — | 实时 | **RPC 实时调用** | 不缓存 |
| 离线入队 | OfflineQueue | — | 实时 | **RPC 实时调用** | 不缓存 |

### 性能对比估算（千人群消息推送）

| 指标 | 旧架构（全 RPC） | 新架构（本地缓存） | 提升 |
|------|----------------|------------------|------|
| GetGroupMembers RPC | 1 次 | 0 次（命中）/ 1 次（未命中） | ~100% |
| GetPresence RPC | 1000 次 | 0 次（命中）/ 1 次 batch（未命中） | ~99.9% |
| GetUserSettings RPC | ~200 次（离线用户） | 0 次（命中） | ~100% |
| 总 RPC 调用 | ~1200 次 | ~1 次（仅 Connecte 路由） | **~99.9%** |
| 延迟（P99） | ~500ms | ~50ms | **10x** |

---

## 可观测性接入（OpenTelemetry）

> Push 服务是 notify+sync 的核心投递引擎，需重点观测：推送延迟、APNs/FCM 成功率、群消息扇出耗时、本地缓存命中率、Kafka 消费积压。

### 第一步：服务启动时初始化 OTel SDK

与所有服务一致，调用共享包 `observability.InitOpenTelemetry` 初始化 TracerProvider + MeterProvider。

```go
func main() {
    ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
    defer cancel()

    otelShutdown := observability.InitOpenTelemetry(ctx, "push", "v1.0.0")
    defer otelShutdown(ctx)

    observability.SetupLogger("push")
    observability.RegisterRedisPoolMetrics(redisClient, "push")
    // ... 创建 gRPC Server、Kafka Consumer 等
}
```

> 共享包 `InitOpenTelemetry` 的完整实现见 `observability_global.md` 第 1.1 节。
> ⚠️ **不要内联实现**，不要引用 "message_impl.md 第一步"，直接使用 `observability.InitOpenTelemetry`。

### 第二步：gRPC Server/Client 注册 OTel Interceptor

```go
// Server 端 — 处理来自 ApiGateway / Message 等服务的推送请求
grpcServer := grpc.NewServer(
    grpc.ChainUnaryInterceptor(otelgrpc.UnaryServerInterceptor()),
    grpc.ChainStreamInterceptor(otelgrpc.StreamServerInterceptor()),
)

// Client 端 — 调用 Connecte（发送 sync notify）、OfflineQueue（入队离线信号）
connecteConn := grpc.Dial("connecte:50051",
    grpc.WithChainUnaryInterceptor(otelgrpc.UnaryClientInterceptor()),
)
offlineQueueConn := grpc.Dial("offline_queue:50051",
    grpc.WithChainUnaryInterceptor(otelgrpc.UnaryClientInterceptor()),
)
```

### 第三步：Kafka Consumer 埋点（消费 msg.stored / group.member.joined 等事件）

Push 服务是最大的 Kafka 消费者（消费 ~40 个 topic）。使用共享包 `observability.StartKafkaConsumerSpan` 从 headers 中提取上游 trace context 并创建 CONSUMER span。

```go
func (s *PushServer) handleKafkaMessage(ctx context.Context, msg kafka.Message) error {
    // 使用共享包：从 headers 提取 trace context + 创建 CONSUMER span
    ctx, span := observability.StartKafkaConsumerSpan(ctx, msg.Topic, msg.Headers)
    defer span.End()

    // 添加业务属性
    span.SetAttributes(
        attribute.Int64("messaging.kafka.offset", msg.Offset),
        attribute.Int("messaging.kafka.partition", int(msg.Partition)),
    )

    startTime := time.Now()
    err := s.dispatchByTopic(ctx, msg)  // 根据 topic 路由到具体 handler

    // 记录处理耗时指标
    duration := float64(time.Since(startTime).Milliseconds())
    kafkaProcessDuration.Record(ctx, duration, metric.WithAttributes(
        attribute.String("topic", msg.Topic),
    ))

    if err != nil {
        observability.RecordError(span, err) // ← 统一使用共享包 RecordError
    }
    return err
}
```

> ⚠️ **不要内联 `kafkaHeaderCarrier`**，直接使用 `observability.StartKafkaConsumerSpan`（见 `observability_global.md` 第 1.3 节）。
> 共享包已封装好 `KafkaHeaderCarrier` + W3C 传播器 + span 创建逻辑。

### 第四步：注册 Push 服务专属业务指标

```go
var meter = otel.Meter("im-chat/push")

var (
    // Kafka 消费处理耗时
    kafkaProcessDuration, _ = meter.Float64Histogram("push.kafka_process_duration_ms",
        metric.WithDescription("Kafka 消息处理耗时"), metric.WithUnit("ms"))

    // 推送通知发送计数（按渠道: online_push / apns / fcm / xiaomi / huawei）
    pushSentCounter, _ = meter.Int64Counter("push.notifications_sent_total",
        metric.WithDescription("推送通知发送总数"))

    // 推送失败计数
    pushFailedCounter, _ = meter.Int64Counter("push.notifications_failed_total",
        metric.WithDescription("推送通知失败总数"))

    // APNs/FCM 推送延迟
    offlinePushLatency, _ = meter.Float64Histogram("push.offline_push_latency_ms",
        metric.WithDescription("离线推送（APNs/FCM）调用延迟"), metric.WithUnit("ms"))

    // 群消息推送扇出耗时（PushToGroup 中遍历所有成员的总耗时）
    groupFanoutDuration, _ = meter.Float64Histogram("push.group_fanout_duration_ms",
        metric.WithDescription("群消息推送扇出总耗时"), metric.WithUnit("ms"))

    // 群推送目标成员数
    groupFanoutSize, _ = meter.Int64Histogram("push.group_fanout_size",
        metric.WithDescription("群消息推送的目标成员数"))

    // Redis 缓存命中率（读取 GroupMembers / UserSettings / Presence 等）
    localCacheHit, _ = meter.Int64Counter("push.cache_hit_total",
        metric.WithDescription("Redis 缓存命中次数"))
    localCacheMiss, _ = meter.Int64Counter("push.cache_miss_total",
        metric.WithDescription("Redis 缓存未命中次数（降级 RPC）"))

    // Kafka 消费者积压
    consumerLag, _ = meter.Int64Gauge("push.kafka_consumer_lag",
        metric.WithDescription("Kafka 消费者积压消息数"))
)
```

在业务代码中埋点的位置：

```go
// PushToUser —— 在线推送成功
pushSentCounter.Add(ctx, 1, metric.WithAttributes(
    attribute.String("channel", "online_push"),
    attribute.String("platform", platformStr),
))

// SendOfflinePush —— APNs/FCM 推送
start := time.Now()
err := s.apnsClient.Push(ctx, notification)
offlinePushLatency.Record(ctx, float64(time.Since(start).Milliseconds()),
    metric.WithAttributes(attribute.String("channel", "apns")))
if err != nil {
    pushFailedCounter.Add(ctx, 1, metric.WithAttributes(attribute.String("channel", "apns")))
}

// PushToGroup —— 群扇出
fanoutStart := time.Now()
memberIDs := getGroupMemberIDs(ctx, groupID)  // 本地缓存优先
groupFanoutSize.Record(ctx, int64(len(memberIDs)))
for _, uid := range memberIDs {
    pushToSingleUser(ctx, uid, msg)
}
groupFanoutDuration.Record(ctx, float64(time.Since(fanoutStart).Milliseconds()))

// 本地缓存读取
if cached != nil {
    localCacheHit.Add(ctx, 1, metric.WithAttributes(attribute.String("cache", "group_members")))
} else {
    localCacheMiss.Add(ctx, 1, metric.WithAttributes(attribute.String("cache", "group_members")))
}
```

### 第五步：结构化 JSON 日志

与 Message 服务相同，设置全局 `slog.JSONHandler` + traceLogHandler 包装。所有业务日志使用 `slog.InfoContext(ctx, ...)`。

### 第六步：改造 buildEventHeader

改为使用共享包，从 `context.Context` 提取 trace_id/span_id：

```go
header := observability.BuildEventHeader(ctx, "push", instanceID)
```

### 补充：基础设施指标注册

> ✅ 已合并到第一步 `main()` 中。Push 服务依赖 Redis，通过 `observability.RegisterRedisPoolMetrics` 注册连接池指标。

### 补充：Span 错误记录

> ✅ Kafka Consumer 和 RPC handler 层已统一使用 `observability.RecordError(span, err)`。

### 接入要点总结

| 步骤 | 改动位置 | 效果 |
|------|---------|------|
| 1. `observability.InitOpenTelemetry` | main() | TracerProvider + MeterProvider + Runtime Metrics |
| 2. gRPC Interceptor | Server + Client 连接 | RPC 自动 trace + RED 指标 |
| 3. Kafka Consumer 埋点 | `observability.StartKafkaConsumerSpan` | 消费链路追踪 + 处理耗时指标 |
| 4. 自定义业务指标 | 推送/扇出逻辑 | 推送成功率、APNs 延迟、扇出耗时、缓存命中率 |
| 5. `observability.SetupLogger` | main() | 所有日志自动带 trace_id + 动态级别 |
| 6. `observability.BuildEventHeader` | 辅助函数 | Kafka 事件头携带真实 trace context |
| 7. 基础设施指标 | `RegisterRedisPoolMetrics` | Redis 连接池健康 |
| 8. Span 错误记录 | `observability.RecordError` | 错误 trace 在 Tempo 可见 |
