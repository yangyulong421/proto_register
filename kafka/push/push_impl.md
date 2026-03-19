# Push 推送服务 — Kafka 消费者/生产者实现伪代码

## 概述

Push 服务是整个 IM 系统中 **消费 Kafka Topic 最多** 的服务。它是 notify+sync 架构的末端投递层，几乎所有需要推送给客户端的实时事件最终都汇聚到 Push 服务。

**核心职责：**
- 消费消息存储事件 → 投递 sync notify 给接收方（在线直推 / 离线推送 + OfflineQueue）
- 消费群消息事件 → 扇出到所有群成员
- 消费已读回执/投递确认事件 → 通知消息发送者
- 消费通知/好友/群组事件 → 推送给相关用户
- 消费在线状态变更 → 推送给订阅者
- 自产自消 push.offline.trigger → 实际调用 APNs/FCM/厂商通道

**push.offline.trigger 自产自消模式说明：**
RPC 层的 handleOfflinePush 方法将离线推送任务投递到 Kafka push.offline.trigger topic，
由本服务的 Kafka Consumer 异步消费并调用 APNs/FCM SDK 发送。这种设计实现了：
1. 解耦：RPC 方法不阻塞在网络 I/O 上
2. 削峰：高并发场景下 Kafka 天然缓冲
3. 重试：消费失败可自动重试，避免推送丢失
4. 多实例负载均衡：Kafka Consumer Group 自动分配 partition

## 生产 Topic 列表

| Topic | 事件 | 生产时机 | Key | 消费方 |
|-------|------|----------|-----|--------|
| `push.offline.trigger` | PushOfflineTriggerEvent | 用户离线时触发离线推送 | user_id | Push 自身（Worker） |
| `push.result` | PushResultEvent | 在线推送结果（成功/失败） | msg_id | Audit（投递统计） |
| `push.offline.result` | PushOfflineResultEvent | 离线推送结果 | user_id | 监控告警 |
| `push.delivered` | PushDeliveredEvent | 消息已投递到客户端 | msg_id | Ack（投递确认） |
| `push.offline.enqueue` | PushOfflineEnqueueEvent | 离线信号入队通知 | user_id | OfflineQueue |
| `push.stats` | PushStatsEvent | 定时统计 / 每次推送后 | server_id | Audit（运营统计） |

## 消费 Topic 列表

| Topic | 来源 | 用途 | 并发度 | 重试 |
|-------|------|------|--------|------|
| `msg.stored.single` | Message | 单聊消息存储完成 → 投递给接收方 | 16 | 3 |
| `msg.stored.group` | Message | 群聊消息存储完成 → 扇出到所有群成员 | 8 | 3 |
| `msg.recalled` | Message | 消息撤回 → 通知相关用户 | 4 | 3 |
| `ack.read.updated` | Ack | 已读回执更新 → 通知消息发送者 | 8 | 2 |
| `ack.read.group` | Ack | 群消息已读汇总 → 通知消息发送者 | 4 | 2 |
| `ack.delivered` | Ack | 投递确认 → 通知消息发送者 | 4 | 2 |
| `notification.created` | Notification | 通知创建 → 推送给目标用户 | 4 | 3 |
| `presence.status.notify` | Presence | 在线状态变更 → 推送给订阅者 | 4 | 1 |
| `presence.typing` | Presence | 正在输入状态 → 推送给对方 | 8 | 0 |
| `relation.friend.request` | Relation | 好友申请 → 推送给目标用户 | 2 | 3 |
| `relation.friend.accepted` | Relation | 好友通过 → 推送给申请者 | 2 | 3 |
| `group.member.joined` | Group | 成员入群 → 通知群成员 | 2 | 2 |
| `group.member.kicked` | Group | 成员被踢 → 通知被踢者 + 群成员 | 2 | 2 |

### Kafka 分区与实例部署建议（HIGH-06）

> **10M 并发用户场景**下，Push 是消费 Kafka Topic 最多、吞吐量最高的服务。
> 以下为各 Topic 的分区数和 Push Consumer 实例数建议：

| Topic | 推荐分区数 | Key 分布策略 | Push 实例数 | 说明 |
|-------|-----------|------------|------------|------|
| `msg.stored.single` | **64** | receiver_user_id | 8~16 | 最高频 Topic，单聊消息量占总量 60%+ |
| `msg.stored.group` | **32** | group_id | 8~16 | 群消息扇出 CPU 密集，每条消息扇出 N 个成员 |
| `msg.recalled` | **16** | conversation_id | 4~8 | 撤回频率低 |
| `ack.read.updated` | **32** | target_user_id | 8~16 | 已读回执量与消息量正相关 |
| `ack.read.group` | **16** | sender_user_id | 4~8 | 群已读汇总频率中等 |
| `ack.delivered` | **16** | sender_user_id | 4~8 | 投递确认频率中等 |
| `notification.created` | **16** | target_user_id | 4~8 | 系统通知频率较低 |
| `presence.status.notify` | **16** | user_id | 4~8 | 在线状态变更，高峰期（早晨大量上线）需扛住 |
| `presence.typing` | **32** | conversation_id | 8~16 | 正在输入实时性要求最高，不重试 |
| `relation.friend.*` | **8** | target_user_id | 2~4 | 好友事件频率低 |
| `group.member.*` | **8** | group_id | 2~4 | 群组事件频率低 |

**部署建议：**
- **最小部署**：8 个 Push 实例（每实例 16~32 goroutine 并发消费）
- **推荐部署**：16 个 Push 实例（10M DAU 场景）
- **弹性扩缩**：基于 Consumer Lag 指标自动扩缩，Lag > 10000 时扩容，Lag < 100 时缩容
- **分区数 ≥ 实例数**：确保每个实例至少分配到 1 个分区
- **Key 选择原则**：以目标用户 ID 为 Key 确保同一用户的消息有序；群消息以 group_id 为 Key 避免同一群消息乱序
| `group.dissolved` | Group | 群解散 → 通知所有成员 | 2 | 3 |
| `group.info.updated` | Group | 群信息更新 → 通知所有成员 | 2 | 2 |
| `push.offline.trigger` | Push 自身 | 离线推送任务 → 实际调用 APNs/FCM SDK | 8 | 5 |
| `config.changed` | Config | 配置变更 → 热更新推送相关配置 | 1 | 1 |
| `group.member.left` | Group | 成员主动退群 → 清理本地群成员缓存 + 推送 | 2 | 2 |
| `user.settings.updated` | User | 用户设置变更 → 更新本地用户设置缓存 | 2 | 2 |
| `user.profile.updated` | User | 用户资料变更 → 更新本地用户信息缓存 | 2 | 2 |
| `conversation.top.changed` | Conversation | 置顶变更 → 多端同步推送 | 2 | 2 |
| `conversation.mute.changed` | Conversation | 免打扰变更 → 多端同步推送 | 2 | 2 |
| `conversation.unread.reset` | Conversation | 未读清零 → 多端同步推送 + iOS 角标更新 | 4 | 2 |
| `notification.read` | Notification | 通知已读 → iOS 角标更新 | 2 | 2 |
| `notification.read.all` | Notification | 通知全部已读 → iOS 角标更新 | 2 | 2 |
| `notification.deleted` | Notification | 通知删除 → iOS 角标更新 | 2 | 2 |
| `relation.remark.updated` | Relation | 好友备注变更 → 多端同步推送 | 2 | 2 |
| `group.application` | Group | 入群申请 → 推送给群管理员 | 2 | 3 |
| `group.application.handled` | Group | 入群申请处理结果 → 推送给申请者 | 2 | 3 |
| `group.member.role.changed` | Group | 成员角色变更 → 推送给被变更成员 | 2 | 2 |
| `group.mute.changed` | Group | 群禁言变更 → 推送给所有群成员 | 2 | 2 |
| `conversation.unread.changed` | Conversation | 未读数变更 → 推送 iOS 角标更新 | 4 | 2 |
| `relation.blocked` | Relation | 拉黑事件 → 更新本地缓存 | 2 | 1 |

## Redis Key 设计（消费者专用）

| Key 格式 | 类型 | 值 | TTL | 说明 |
|----------|------|-----|-----|------|
| `push:kafka:dedup:{event_id}` | STRING | "1" | 24h | Kafka 消费幂等去重 |
| `push:device:{user_id}` | HASH | field=device_id value=JSON | 7d | 用户推送设备缓存（共享） |
| `push:badge:{user_id}:{device_id}` | STRING | badge_count | 无 TTL | iOS 角标计数（共享） |
| `push:rate_limit:{user_id}` | STRING | count | 1min | 推送速率限制（共享） |
| `push:offline_dedup:{msg_id}:{user_id}` | STRING | "1" | 5min | 离线推送去重（共享） |
| `push:mute:{user_id}:{conversation_id}` | STRING | "1" / "0" | 10min | 会话免打扰状态缓存 |
| `push:offline_retry:{push_id}` | STRING | retry_count | 1h | 离线推送重试计数 |
| `push:local:group_members:{group_id}` | SET | member user_ids | 10min | 本地群成员列表缓存（PushToGroup 读取） |
| `push:local:user_settings:{user_id}` | HASH | mute, dnd, push_enabled 等 | 10min | 本地用户设置缓存（免打扰判断） |
| `push:local:user_info:{user_id}` | HASH | nickname, avatar_url 等 | 30min | 本地用户信息缓存（推送内容构建） |

> **注意：在线状态不做本地缓存。**  
> 在线/离线状态变化频率（每天数次）显著高于头像/设置（每月一次），不满足「高频读取 + 变化不频繁」的缓存准入条件。  
> PushToUser 直接通过 Connecte.GetUserConnections 判断在线（推送链路本就必须调用）。  
> PushToGroup 通过 Presence.BatchGetPresence RPC 批量查询（单次 RPC，数据源头）。

---

## 消费者实现

### Consumer: `msg.stored.single`

> 来源：Message 服务。单聊消息存储完成后触发。  
> 职责：将 sync notify 投递给接收方 — 在线走 Connecte 直推，离线走 OfflineQueue + APNs/FCM。  
> **这是整个 IM 系统消息投递的最关键路径。**

```go
func (c *PushConsumer) HandleMsgStoredSingle(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_push.MsgStoredForPush
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 MsgStoredForPush（单聊）失败", "err", err)
        return nil // 反序列化失败不重试
    }

    // ==================== 2. 幂等检查 — Redis ====================
    // Key: push:kafka:dedup:{event_id}  TTL: 24h
    dedupKey := fmt.Sprintf("push:kafka:dedup:%s", event.Header.EventId)
    set, err := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result()
    if err != nil {
        return fmt.Errorf("Redis 幂等检查失败: %w", err)
    }
    if !set {
        log.Debug("重复事件，跳过", "event_id", event.Header.EventId)
        return nil
    }

    // ==================== 3. 参数校验 ====================
    if event.MsgId == "" || event.ConversationId == "" || event.SenderId == "" || event.ReceiverId == "" {
        log.Error("msg.stored.single: 关键字段缺失",
            "msg_id", event.MsgId, "sender_id", event.SenderId, "receiver_id", event.ReceiverId)
        return nil
    }

    // ==================== 4. 检查接收方是否免打扰 ====================
    // 先检查本地 Redis 缓存
    // Key: push:mute:{user_id}:{conversation_id}
    muteKey := fmt.Sprintf("push:mute:%s:%s", event.ReceiverId, event.ConversationId)
    muteVal, err := c.redis.Get(ctx, muteKey).Result()
    isMuted := false
    if err == nil {
        isMuted = muteVal == "1"
    }
    // 注意：免打扰仅影响离线推送（APNs/FCM），在线 sync notify 仍然发送（客户端自行决定是否弹窗）

    // ==================== 5. 构建 sync notify 信号 ====================
    signalPayload, _ := json.Marshal(map[string]interface{}{
        "conversation_id":   event.ConversationId,
        "conversation_type": int(event.ConversationType),
        "max_seq":           event.Seq,
        "sender_id":         event.SenderId,
        "sender_name":       event.SenderNickname,
        "msg_type":          int(event.MsgType),
        "content_preview":   event.ContentPreview,
        "msg_id":            event.MsgId,
    })

    signal := &push_pb.SyncNotifySignal{
        Type:      "new_msg",
        Payload:   signalPayload,
        Seq:       event.Seq,
        Timestamp: event.SendTime,
    }

    // ==================== 6. 推送给接收方 — RPC PushToUser ====================
    pushResp, err := c.pushService.PushToUser(ctx, &push_pb.PushToUserRequest{
        UserId: event.ReceiverId,
        Signal: signal,
    })
    if err != nil {
        log.Error("推送单聊消息给接收方失败",
            "msg_id", event.MsgId, "receiver_id", event.ReceiverId, "err", err)
        return fmt.Errorf("推送给接收方失败: %w", err) // 返回 error 触发重试
    }

    // ==================== 7. 生产推送结果事件 ====================
    if pushResp.Pushed {
        // 在线推送成功 → 生产 push.result
        resultEvent := &kafka_push.PushResultEvent{
            Header:    buildEventHeader("push", c.instanceID),
            PushId:    uuid.New().String(),
            UserId:    event.ReceiverId,
            MsgId:     event.MsgId,
            Success:   true,
            Platform:  common.PLATFORM_TYPE_UNSPECIFIED, // 多端推送不区分单一平台
            PushTime:  time.Now().UnixMilli(),
            LatencyMs: time.Now().UnixMilli() - event.SendTime,
        }
        c.kafka.Produce(ctx, "push.result", event.MsgId, resultEvent)
    }

    // ==================== 8. 多端同步：推送给发送者的其他设备 ====================
    // 发送者的其他设备也需要同步新消息（如手机发送，PC 端也要更新）
    multiSyncSignal := &push_pb.SyncNotifySignal{
        Type:      "new_msg_sync",
        Payload:   signalPayload,
        Seq:       event.Seq,
        Timestamp: event.SendTime,
    }
    _, err = c.pushService.PushToUserOtherDevices(ctx, &push_pb.PushToUserOtherDevicesRequest{
        UserId:          event.SenderId,
        ExcludeDeviceId: "", // 从 event header 的 metadata 中获取，此处简化
        Signal:          multiSyncSignal,
    })
    if err != nil {
        log.Warn("多端同步推送失败（不阻塞主流程）",
            "msg_id", event.MsgId, "sender_id", event.SenderId, "err", err)
        // 多端同步失败不重试，不影响接收方推送
    }

    log.Info("单聊消息推送完成",
        "msg_id", event.MsgId,
        "sender_id", event.SenderId,
        "receiver_id", event.ReceiverId,
        "receiver_online", pushResp.Online,
        "pushed", pushResp.Pushed,
        "latency_ms", time.Now().UnixMilli()-event.SendTime)

    return nil
}
```

### Consumer: `msg.stored.group`

> 来源：Message 服务。群聊消息存储完成后触发。  
> 职责：扇出到所有群成员 — 通过 PushToGroup 执行大群/小群策略。  
> **群消息扇出是 Push 服务最大的性能热点。**

```go
func (c *PushConsumer) HandleMsgStoredGroup(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_push.MsgStoredForPush
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 MsgStoredForPush（群聊）失败", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 — Redis ====================
    dedupKey := fmt.Sprintf("push:kafka:dedup:%s", event.Header.EventId)
    set, err := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result()
    if err != nil {
        return fmt.Errorf("Redis 幂等检查失败: %w", err)
    }
    if !set {
        return nil
    }

    // ==================== 3. 参数校验 ====================
    if event.MsgId == "" || event.GroupId == "" || event.SenderId == "" {
        log.Error("msg.stored.group: 关键字段缺失",
            "msg_id", event.MsgId, "group_id", event.GroupId, "sender_id", event.SenderId)
        return nil
    }

    // ==================== 4. 构建排除列表 ====================
    // 排除：发送者自己 + 已设免打扰的用户（免打扰用户仍入 OfflineQueue 但不发 APNs/FCM）
    excludeUserIDs := []string{event.SenderId}
    // 免打扰用户列表由 Message 服务在事件中携带
    // 注意：免打扰用户仍然需要在线推送（sync notify），仅不发离线推送
    // 因此这里只排除发送者，免打扰在 handleOfflinePush 层处理

    // ==================== 5. 构建 sync notify 信号 ====================
    signalPayload, _ := json.Marshal(map[string]interface{}{
        "conversation_id":   event.ConversationId,
        "conversation_type": int(event.ConversationType),
        "max_seq":           event.Seq,
        "sender_id":         event.SenderId,
        "sender_name":       event.SenderNickname,
        "msg_type":          int(event.MsgType),
        "content_preview":   event.ContentPreview,
        "msg_id":            event.MsgId,
        "group_id":          event.GroupId,
        "group_name":        event.GroupName,
        "has_at":            event.HasAt,
        "at_user_ids":       event.AtUserIds,
        "has_at_all":        event.HasAtAll,
    })

    signal := &push_pb.SyncNotifySignal{
        Type:      "new_msg",
        Payload:   signalPayload,
        Seq:       event.Seq,
        Timestamp: event.SendTime,
    }

    // ==================== 6. 判断大群/小群策略 ====================
    isLargeGroup := event.GroupMemberCount >= 200

    // ==================== 7. 调用 PushToGroup 扇出推送 ====================
    pushResp, err := c.pushService.PushToGroup(ctx, &push_pb.PushToGroupRequest{
        GroupId:         event.GroupId,
        Signal:          signal,
        ExcludeUserIds:  excludeUserIDs,
        LargeGroupLazy:  isLargeGroup,
        OnlineMemberIds: nil, // 由 PushToGroup 内部查询
    })
    if err != nil {
        log.Error("群消息扇出推送失败",
            "msg_id", event.MsgId, "group_id", event.GroupId, "err", err)
        return fmt.Errorf("群消息扇出推送失败: %w", err) // 重试
    }

    // ==================== 8. @用户特殊处理 ====================
    // 被 @ 的用户即使免打扰也需要收到离线推送
    if event.HasAt && len(event.AtUserIds) > 0 {
        // 检查被 @ 的用户是否在离线列表中
        offlineSet := make(map[string]bool, len(pushResp.OfflineUsers))
        for _, uid := range pushResp.OfflineUsers {
            offlineSet[uid] = true
        }

        atSignalPayload, _ := json.Marshal(map[string]interface{}{
            "conversation_id":   event.ConversationId,
            "conversation_type": int(event.ConversationType),
            "max_seq":           event.Seq,
            "sender_id":         event.SenderId,
            "sender_name":       event.SenderNickname,
            "msg_type":          int(event.MsgType),
            "content_preview":   fmt.Sprintf("%s 在群聊中@了你", event.SenderNickname),
            "msg_id":            event.MsgId,
            "group_id":          event.GroupId,
            "group_name":        event.GroupName,
            "is_at":             true,
        })

        for _, atUserID := range event.AtUserIds {
            if atUserID == event.SenderId {
                continue // 不 @ 自己
            }
            if offlineSet[atUserID] {
                // 被 @ 的离线用户：强制触发离线推送（忽略免打扰）
                atSignal := &push_pb.SyncNotifySignal{
                    Type:      "at_msg",
                    Payload:   atSignalPayload,
                    Seq:       event.Seq,
                    Timestamp: event.SendTime,
                }
                c.forceOfflinePush(ctx, atUserID, atSignal, event.MsgId,
                    fmt.Sprintf("%s在群聊中@了你", event.SenderNickname))
            }
        }
    }

    // @全体成员 特殊处理
    if event.HasAtAll && len(pushResp.OfflineUsers) > 0 {
        for _, offUID := range pushResp.OfflineUsers {
            if offUID == event.SenderId {
                continue
            }
            atAllSignalPayload, _ := json.Marshal(map[string]interface{}{
                "conversation_id":   event.ConversationId,
                "conversation_type": int(event.ConversationType),
                "max_seq":           event.Seq,
                "sender_id":         event.SenderId,
                "sender_name":       event.SenderNickname,
                "msg_id":            event.MsgId,
                "group_id":          event.GroupId,
                "is_at_all":         true,
            })
            atAllSignal := &push_pb.SyncNotifySignal{
                Type:      "at_all_msg",
                Payload:   atAllSignalPayload,
                Seq:       event.Seq,
                Timestamp: event.SendTime,
            }
            c.forceOfflinePush(ctx, offUID, atAllSignal, event.MsgId,
                fmt.Sprintf("%s在群聊中@了所有人", event.SenderNickname))
        }
    }

    // ==================== 9. 多端同步：推送给发送者的其他设备 ====================
    multiSyncSignal := &push_pb.SyncNotifySignal{
        Type:      "new_msg_sync",
        Payload:   signalPayload,
        Seq:       event.Seq,
        Timestamp: event.SendTime,
    }
    c.pushService.PushToUserOtherDevices(ctx, &push_pb.PushToUserOtherDevicesRequest{
        UserId:          event.SenderId,
        ExcludeDeviceId: "",
        Signal:          multiSyncSignal,
    })

    log.Info("群聊消息扇出推送完成",
        "msg_id", event.MsgId,
        "group_id", event.GroupId,
        "total_members", pushResp.TotalMembers,
        "pushed_count", pushResp.PushedCount,
        "offline_count", len(pushResp.OfflineUsers),
        "is_large_group", isLargeGroup,
        "latency_ms", time.Now().UnixMilli()-event.SendTime)

    return nil
}

// forceOfflinePush 强制触发离线推送（忽略免打扰设置，用于 @消息）
func (c *PushConsumer) forceOfflinePush(ctx context.Context, userID string, signal *push_pb.SyncNotifySignal, msgID string, pushContent string) {
    // 离线推送去重检查
    dedupKey := fmt.Sprintf("push:offline_dedup:%s:%s", msgID, userID)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 5*time.Minute).Result(); !set {
        return // 已发送过
    }

    // 获取用户推送设备列表
    devices, err := c.getUserPushDevices(ctx, userID)
    if err != nil || len(devices) == 0 {
        return
    }

    for _, device := range devices {
        if !device.IsEnabled || device.PushToken == "" {
            continue
        }

        // 更新 iOS 角标
        badgeCount := int64(0)
        if device.Platform == int(common.PLATFORM_TYPE_IOS) ||
            device.Platform == int(common.PLATFORM_TYPE_IPAD) {
            badgeKey := fmt.Sprintf("push:badge:%s:%s", userID, device.DeviceID)
            badgeCount, _ = c.redis.Incr(ctx, badgeKey).Result()
        }

        triggerEvent := &kafka_push.PushOfflineTriggerEvent{
            Header:      buildEventHeader("push", c.instanceID),
            UserId:      userID,
            PushChannel: device.PushChannel,
            PushToken:   device.PushToken,
            DeviceId:    device.DeviceID,
            Platform:    common.PlatformType(device.Platform),
            Title:       "有人@了你",
            Content:     pushContent,
            Badge:       strconv.FormatInt(badgeCount, 10),
            Sound:       "default",
            ExpireTime:  time.Now().Add(24 * time.Hour).UnixMilli(),
            MsgId:       msgID,
        }
        c.kafka.Produce(ctx, "push.offline.trigger", userID, triggerEvent)
    }
}
```

### Consumer: `msg.recalled`

> 来源：Message 服务。消息撤回成功后触发。  
> 职责：通知所有相关用户消息已撤回，客户端收到后从聊天界面移除/替换该消息。

```go
func (c *PushConsumer) HandleMsgRecalled(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_push.MsgRecalledForPush
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 MsgRecalledForPush 失败", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 ====================
    dedupKey := fmt.Sprintf("push:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    // ==================== 3. 参数校验 ====================
    if event.MsgId == "" || event.ConversationId == "" {
        log.Error("msg.recalled: 关键字段缺失", "msg_id", event.MsgId)
        return nil
    }

    // ==================== 4. 构建撤回通知信号 ====================
    signalPayload, _ := json.Marshal(map[string]interface{}{
        "conversation_id":   event.ConversationId,
        "conversation_type": int(event.ConversationType),
        "msg_id":            event.MsgId,
        "sender_id":         event.SenderId,
        "sender_name":       event.SenderNickname,
        "original_seq":      event.OriginalSeq,
        "recall_time":       event.RecallTime,
    })

    signal := &push_pb.SyncNotifySignal{
        Type:      "recall",
        Payload:   signalPayload,
        Seq:       event.OriginalSeq,
        Timestamp: event.RecallTime,
    }

    // ==================== 5. 根据会话类型推送 ====================
    if event.ConversationType == common.CONVERSATION_TYPE_SINGLE {
        // 单聊：推送给对方
        for _, targetUID := range event.TargetUserIds {
            _, err := c.pushService.PushToUser(ctx, &push_pb.PushToUserRequest{
                UserId: targetUID,
                Signal: signal,
            })
            if err != nil {
                log.Error("推送撤回通知给用户失败",
                    "msg_id", event.MsgId, "target_user_id", targetUID, "err", err)
            }
        }
    } else if event.ConversationType == common.CONVERSATION_TYPE_GROUP {
        // 群聊：推送给所有成员（排除撤回者）
        _, err := c.pushService.PushToGroup(ctx, &push_pb.PushToGroupRequest{
            GroupId:        event.ConversationId, // 群聊时 conversation_id 关联 group_id
            Signal:         signal,
            ExcludeUserIds: []string{event.SenderId},
        })
        if err != nil {
            log.Error("推送群聊撤回通知失败",
                "msg_id", event.MsgId, "conversation_id", event.ConversationId, "err", err)
            return fmt.Errorf("推送群聊撤回通知失败: %w", err)
        }
    }

    // ==================== 6. 多端同步：通知撤回者的其他设备 ====================
    c.pushService.PushToUserOtherDevices(ctx, &push_pb.PushToUserOtherDevicesRequest{
        UserId:          event.SenderId,
        ExcludeDeviceId: "",
        Signal:          signal,
    })

    log.Info("消息撤回通知推送完成",
        "msg_id", event.MsgId, "sender_id", event.SenderId,
        "conversation_type", event.ConversationType)

    return nil
}
```

### Consumer: `ack.read.updated`

> 来源：Ack 服务。用户标记消息已读后触发。  
> 职责：通知消息发送者 — 你的消息已被对方阅读（单聊已读回执）。  
> 同时也用于发送者的多端已读同步。

```go
func (c *PushConsumer) HandleAckReadUpdated(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_push.ReadReceiptForPush
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 ReadReceiptForPush 失败", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 ====================
    dedupKey := fmt.Sprintf("push:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    // ==================== 3. 参数校验 ====================
    if event.TargetUserId == "" || event.ReaderId == "" || event.ConversationId == "" {
        log.Error("ack.read.updated: 关键字段缺失",
            "target_user_id", event.TargetUserId, "reader_id", event.ReaderId)
        return nil
    }

    // ==================== 4. 构建已读回执通知信号 ====================
    signalPayload, _ := json.Marshal(map[string]interface{}{
        "conversation_id":   event.ConversationId,
        "conversation_type": int(event.ConversationType),
        "reader_id":         event.ReaderId,
        "read_seq":          event.ReadSeq,
        "last_read_msg_id":  event.LastReadMsgId,
        "read_time":         event.ReadTime,
    })

    signal := &push_pb.SyncNotifySignal{
        Type:      "read_receipt",
        Payload:   signalPayload,
        Seq:       event.ReadSeq,
        Timestamp: event.ReadTime,
    }

    // ==================== 5. 推送给消息发送者（仅在线推送，不触发离线推送） ====================
    // 已读回执不需要离线推送（用户上线后拉取即可），仅推送给在线设备
    // 使用内部 PushToUser 方法 + online_only=true，避免重复实现 Presence 查询 + 连接获取逻辑
    _, err := c.pushService.PushToUser(ctx, &push_pb.PushToUserRequest{
        UserId:     event.TargetUserId,
        Signal:     signal,
        OnlineOnly: true, // 仅推送在线用户，已读回执不需要离线推送
    })
    if err != nil {
        log.Warn("推送已读回执失败（online_only）", "target_user_id", event.TargetUserId, "err", err)
        // 已读回执推送失败不重试
    }

    // ==================== 6. 多端已读同步：推送给 reader 的其他设备 ====================
    readSyncPayload, _ := json.Marshal(map[string]interface{}{
        "conversation_id":   event.ConversationId,
        "conversation_type": int(event.ConversationType),
        "read_seq":          event.ReadSeq,
        "read_time":         event.ReadTime,
    })
    readSyncSignal := &push_pb.SyncNotifySignal{
        Type:      "read_sync",
        Payload:   readSyncPayload,
        Seq:       event.ReadSeq,
        Timestamp: event.ReadTime,
    }
    c.pushService.PushToUserOtherDevices(ctx, &push_pb.PushToUserOtherDevicesRequest{
        UserId:          event.ReaderId,
        ExcludeDeviceId: "",
        Signal:          readSyncSignal,
    })

    log.Debug("已读回执推送完成",
        "target_user_id", event.TargetUserId, "reader_id", event.ReaderId,
        "conversation_id", event.ConversationId, "read_seq", event.ReadSeq)

    return nil
}
```

### Consumer: `ack.read.group`

> 来源：Ack 服务。群消息已读人数汇总变更后触发。  
> 职责：通知消息发送者群消息的最新已读人数和已读用户列表。

```go
func (c *PushConsumer) HandleAckReadGroup(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_push.AckGroupReadForPush
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 AckGroupReadForPush 失败", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 ====================
    dedupKey := fmt.Sprintf("push:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    // ==================== 3. 参数校验 ====================
    if event.MsgId == "" || event.SenderId == "" || event.GroupId == "" {
        log.Error("ack.read.group: 关键字段缺失",
            "msg_id", event.MsgId, "sender_id", event.SenderId)
        return nil
    }

    // ==================== 4. 构建群已读回执通知信号 ====================
    signalPayload, _ := json.Marshal(map[string]interface{}{
        "group_id":      event.GroupId,
        "msg_id":        event.MsgId,
        "read_user_ids": event.ReadUserIds,
        "read_count":    event.ReadCount,
        "total_count":   event.TotalCount,
    })

    signal := &push_pb.SyncNotifySignal{
        Type:      "group_read_receipt",
        Payload:   signalPayload,
        Timestamp: time.Now().UnixMilli(),
    }

    // ==================== 5. 仅推送给消息发送者（在线时推送） ====================
    c.pushService.PushToUser(ctx, &push_pb.PushToUserRequest{
        UserId: event.SenderId,
        Signal: signal,
    })

    log.Debug("群已读回执推送完成",
        "msg_id", event.MsgId, "sender_id", event.SenderId,
        "read_count", event.ReadCount, "total_count", event.TotalCount)

    return nil
}
```

### Consumer: `ack.delivered`

> 来源：Ack 服务。客户端确认消息已送达后触发。  
> 职责：通知消息发送者 — 你的消息已送达对方设备。

```go
func (c *PushConsumer) HandleAckDelivered(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_push.PushDeliveredEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 PushDeliveredEvent 失败", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 ====================
    dedupKey := fmt.Sprintf("push:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    // ==================== 3. 参数校验 ====================
    if event.MsgId == "" || event.UserId == "" {
        log.Error("ack.delivered: 关键字段缺失",
            "msg_id", event.MsgId, "user_id", event.UserId)
        return nil
    }

    // ==================== 4. 查询消息发送者 ID ====================
    // 从 Redis 消息缓存中获取 sender_id，缓存未命中则查 PgSQL
    // 这里简化处理：event 中应携带 sender_id 或从消息缓存获取
    // 实际实现中需要根据 msg_id 查找 sender_id
    // 此处假设事件中已包含必要信息

    // ==================== 5. 构建投递确认信号并推送给发送者 ====================
    signalPayload, _ := json.Marshal(map[string]interface{}{
        "msg_id":       event.MsgId,
        "user_id":      event.UserId,
        "platform":     int(event.Platform),
        "device_id":    event.DeviceId,
        "deliver_time": event.DeliverTime,
    })

    signal := &push_pb.SyncNotifySignal{
        Type:      "delivered",
        Payload:   signalPayload,
        Timestamp: event.DeliverTime,
    }

    // 仅在线推送（投递确认不需要离线推送）
    // 注意：这里需要知道 sender_id，实际中从 msg 缓存获取
    // 简化为：event.UserId 是接收者，sender 从消息元数据获取
    // 此处略去 sender 查找逻辑

    log.Debug("投递确认推送完成", "msg_id", event.MsgId, "user_id", event.UserId)
    return nil
}
```

### Consumer: `notification.created`

> 来源：Notification 服务。通知创建后触发（好友申请结果、系统公告、安全警告等）。  
> 职责：将通知推送给目标用户，需要时触发离线推送。

```go
func (c *PushConsumer) HandleNotificationCreated(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_push.NotificationCreatedForPush
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 NotificationCreatedForPush 失败", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 ====================
    dedupKey := fmt.Sprintf("push:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    // ==================== 3. 参数校验 ====================
    if event.TargetUserId == "" || event.NotificationId == "" {
        log.Error("notification.created: 关键字段缺失",
            "target_user_id", event.TargetUserId, "notification_id", event.NotificationId)
        return nil
    }

    // ==================== 4. 构建通知推送信号 ====================
    signalPayload, _ := json.Marshal(map[string]interface{}{
        "notification_id": event.NotificationId,
        "type":            int(event.Type),
        "title":           event.Title,
        "content":         event.Content,
        "sender_id":       event.SenderId,
        "sender_nickname": event.SenderNickname,
        "sender_avatar":   event.SenderAvatar,
        "extra":           event.Extra,
        "create_time":     event.CreateTime,
    })

    signal := &push_pb.SyncNotifySignal{
        Type:      "notification",
        Payload:   signalPayload,
        Timestamp: event.CreateTime,
    }

    // ==================== 5. 推送给目标用户 ====================
    pushResp, err := c.pushService.PushToUser(ctx, &push_pb.PushToUserRequest{
        UserId: event.TargetUserId,
        Signal: signal,
    })
    if err != nil {
        log.Error("推送通知失败",
            "notification_id", event.NotificationId,
            "target_user_id", event.TargetUserId, "err", err)
        return fmt.Errorf("推送通知失败: %w", err)
    }

    // ==================== 6. 如果用户离线且通知需要离线推送 ====================
    // PushToUser 已处理离线推送逻辑（通过 handleOfflinePush）
    // 此处无需额外处理

    log.Info("通知推送完成",
        "notification_id", event.NotificationId,
        "target_user_id", event.TargetUserId,
        "type", event.Type,
        "online", pushResp.Online)

    return nil
}
```

### Consumer: `presence.status.notify`

> 来源：Presence 服务。用户在线状态变更后触发。  
> 职责：推送给该用户的状态订阅者（好友列表中显示在线/离线/忙碌等）。

```go
func (c *PushConsumer) HandlePresenceStatusNotify(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_push.PresenceStatusNotifyForPush
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 PresenceStatusNotifyForPush 失败", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 ====================
    dedupKey := fmt.Sprintf("push:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    // ==================== 3. 参数校验 ====================
    if event.UserId == "" || len(event.SubscriberIds) == 0 {
        log.Debug("presence.status.notify: 无订阅者",
            "user_id", event.UserId, "subscriber_count", len(event.SubscriberIds))
        return nil
    }

    // ==================== 4. 构建状态变更推送信号 ====================
    signalPayload, _ := json.Marshal(map[string]interface{}{
        "user_id":       event.UserId,
        "new_status":    int(event.NewStatus),
        "custom_status": event.CustomStatus,
    })

    signal := &push_pb.SyncNotifySignal{
        Type:      "presence",
        Payload:   signalPayload,
        Timestamp: time.Now().UnixMilli(),
    }

    // ==================== 5. 批量推送给订阅者（仅在线推送，不触发离线推送） ====================
    // 状态变更不需要离线推送
    signalBytes, _ := proto.Marshal(signal)

    // 分批查询在线订阅者
    onlineSubscribers := make([]string, 0, len(event.SubscriberIds))
    batchSize := 500
    for i := 0; i < len(event.SubscriberIds); i += batchSize {
        end := i + batchSize
        if end > len(event.SubscriberIds) {
            end = len(event.SubscriberIds)
        }
        batch := event.SubscriberIds[i:end]

        presenceResp, err := c.presenceClient.BatchGetPresence(ctx, &presence_pb.BatchGetPresenceRequest{
            UserIds: batch,
        })
        if err != nil {
            log.Warn("批量查询订阅者在线状态失败", "err", err)
            continue
        }

        for _, p := range presenceResp.Presences {
            if p.Status != common.ONLINE_STATUS_OFFLINE {
                onlineSubscribers = append(onlineSubscribers, p.UserId)
            }
        }
    }

    // 批量获取连接并推送（避免 N+1 RPC 调用）
    // 分批获取在线订阅者的连接列表，然后合并为一次批量推送
    allConnIDs := make([]string, 0, len(onlineSubscribers)*2)
    connBatchSize := 200
    for i := 0; i < len(onlineSubscribers); i += connBatchSize {
        end := i + connBatchSize
        if end > len(onlineSubscribers) {
            end = len(onlineSubscribers)
        }
        batch := onlineSubscribers[i:end]

        batchConnResp, err := c.connecteClient.BatchGetUserConnections(ctx, &connecte_pb.BatchGetUserConnectionsRequest{
            UserIds: batch,
        })
        if err != nil {
            log.Warn("批量获取订阅者连接失败", "err", err)
            continue
        }

        for _, uc := range batchConnResp.UserConnections {
            for _, conn := range uc.Connections {
                allConnIDs = append(allConnIDs, conn.ConnectionId)
            }
        }
    }

    // 合并为一次批量推送
    if len(allConnIDs) > 0 {
        // 分批推送（每批最多 1000 个连接）
        pushBatchSize := 1000
        for i := 0; i < len(allConnIDs); i += pushBatchSize {
            end := i + pushBatchSize
            if end > len(allConnIDs) {
                end = len(allConnIDs)
            }
            c.connecteClient.BatchSendToConnections(ctx, &connecte_pb.BatchSendToConnectionsRequest{
                ConnectionIds: allConnIDs[i:end],
                DataType:      "sync_notify",
                Data:          signalBytes,
            })
        }
    }

    log.Debug("在线状态变更推送完成",
        "user_id", event.UserId,
        "new_status", event.NewStatus,
        "total_subscribers", len(event.SubscriberIds),
        "online_subscribers", len(onlineSubscribers))

    return nil
}
```

### Consumer: `presence.typing`

> 来源：Presence 服务。用户正在输入/录音/上传文件时触发。  
> 职责：推送正在输入状态给对方。  
> **实时性极高，但允许丢失（不重试）。**

```go
func (c *PushConsumer) HandlePresenceTyping(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_push.TypingIndicatorForPush
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 TypingIndicatorForPush 失败", "err", err)
        return nil
    }

    // 注意：typing 事件不做幂等检查（高频、允许丢失、不重试）

    // ==================== 2. 参数校验 ====================
    if event.UserId == "" || event.TargetId == "" {
        return nil
    }

    // ==================== 3. 构建正在输入信号 ====================
    signalPayload, _ := json.Marshal(map[string]interface{}{
        "user_id":         event.UserId,
        "conversation_id": event.ConversationId,
        "action":          event.Action,
        "user_nickname":   event.UserNickname,
    })

    signal := &push_pb.SyncNotifySignal{
        Type:      "typing",
        Payload:   signalPayload,
        Timestamp: time.Now().UnixMilli(),
    }

    // ==================== 4. 仅在线推送给目标用户（不走离线推送） ====================
    // 检查目标是否在线
    presenceResp, err := c.presenceClient.GetPresence(ctx, &presence_pb.GetPresenceRequest{
        UserId: event.TargetId,
    })
    if err != nil {
        return nil // typing 推送失败不重试
    }

    hasOnline := false
    for _, p := range presenceResp.Presences {
        if p.Status != common.ONLINE_STATUS_OFFLINE {
            hasOnline = true
            break
        }
    }
    if !hasOnline {
        return nil // 对方不在线，直接丢弃
    }

    // 获取连接推送
    connResp, err := c.connecteClient.GetUserConnections(ctx, &connecte_pb.GetUserConnectionsRequest{
        UserId: event.TargetId,
    })
    if err != nil || len(connResp.Connections) == 0 {
        return nil
    }

    signalBytes, _ := proto.Marshal(signal)
    connIDs := make([]string, 0, len(connResp.Connections))
    for _, conn := range connResp.Connections {
        connIDs = append(connIDs, conn.ConnectionId)
    }

    c.connecteClient.BatchSendToConnections(ctx, &connecte_pb.BatchSendToConnectionsRequest{
        ConnectionIds: connIDs,
        DataType:      "sync_notify",
        Data:          signalBytes,
    })

    return nil
}
```

### Consumer: `relation.friend.request`

> 来源：Relation 服务。发送好友申请后触发。  
> 职责：推送好友申请通知给目标用户，包含在线推送和离线推送。

```go
func (c *PushConsumer) HandleRelationFriendRequest(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    // 使用通用事件结构（Relation 服务定义的 FriendRequestEvent）
    var event struct {
        Header      *common.EventHeader `protobuf:"bytes,1"`
        RequestId   string
        FromUserId  string
        ToUserId    string
        Message     string
        Source      string
        RequestTime int64
    }
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 relation.friend.request 失败", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 ====================
    dedupKey := fmt.Sprintf("push:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    // ==================== 3. 参数校验 ====================
    if event.FromUserId == "" || event.ToUserId == "" {
        log.Error("relation.friend.request: 关键字段缺失")
        return nil
    }

    // ==================== 4. 获取申请者信息 — RPC User.GetUser ====================
    userResp, err := c.userClient.GetUser(ctx, &user_pb.GetUserRequest{
        UserId: event.FromUserId,
    })
    if err != nil {
        log.Warn("获取好友申请者信息失败", "from_user_id", event.FromUserId, "err", err)
    }

    senderNickname := event.FromUserId
    senderAvatar := ""
    if userResp != nil && userResp.User != nil {
        senderNickname = userResp.User.Nickname
        senderAvatar = userResp.User.AvatarUrl
    }

    // ==================== 5. 构建好友申请通知信号 ====================
    signalPayload, _ := json.Marshal(map[string]interface{}{
        "type":            "friend_request",
        "request_id":      event.RequestId,
        "from_user_id":    event.FromUserId,
        "from_nickname":   senderNickname,
        "from_avatar":     senderAvatar,
        "message":         event.Message,
        "source":          event.Source,
        "request_time":    event.RequestTime,
    })

    signal := &push_pb.SyncNotifySignal{
        Type:      "system",
        Payload:   signalPayload,
        Timestamp: event.RequestTime,
    }

    // ==================== 6. 推送给目标用户 ====================
    _, err = c.pushService.PushToUser(ctx, &push_pb.PushToUserRequest{
        UserId: event.ToUserId,
        Signal: signal,
    })
    if err != nil {
        log.Error("推送好友申请通知失败",
            "request_id", event.RequestId, "to_user_id", event.ToUserId, "err", err)
        return fmt.Errorf("推送好友申请通知失败: %w", err)
    }

    log.Info("好友申请通知推送完成",
        "request_id", event.RequestId,
        "from_user_id", event.FromUserId,
        "to_user_id", event.ToUserId)

    return nil
}
```

### Consumer: `relation.friend.accepted`

> 来源：Relation 服务。好友申请被接受后触发。  
> 职责：推送给申请者 — 你的好友申请已通过。

```go
func (c *PushConsumer) HandleRelationFriendAccepted(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event struct {
        Header     *common.EventHeader `protobuf:"bytes,1"`
        RequestId  string
        FromUserId string
        ToUserId   string
        AcceptTime int64
    }
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 relation.friend.accepted 失败", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 ====================
    dedupKey := fmt.Sprintf("push:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    // ==================== 3. 获取接受者信息 ====================
    userResp, err := c.userClient.GetUser(ctx, &user_pb.GetUserRequest{
        UserId: event.ToUserId,
    })
    acceptorNickname := event.ToUserId
    acceptorAvatar := ""
    if err == nil && userResp != nil && userResp.User != nil {
        acceptorNickname = userResp.User.Nickname
        acceptorAvatar = userResp.User.AvatarUrl
    }

    // ==================== 4. 构建好友通过通知信号 ====================
    signalPayload, _ := json.Marshal(map[string]interface{}{
        "type":               "friend_accepted",
        "request_id":         event.RequestId,
        "acceptor_user_id":   event.ToUserId,
        "acceptor_nickname":  acceptorNickname,
        "acceptor_avatar":    acceptorAvatar,
        "accept_time":        event.AcceptTime,
    })

    signal := &push_pb.SyncNotifySignal{
        Type:      "system",
        Payload:   signalPayload,
        Timestamp: event.AcceptTime,
    }

    // ==================== 5. 推送给原始申请者 ====================
    _, err = c.pushService.PushToUser(ctx, &push_pb.PushToUserRequest{
        UserId: event.FromUserId,
        Signal: signal,
    })
    if err != nil {
        log.Error("推送好友通过通知失败",
            "request_id", event.RequestId, "from_user_id", event.FromUserId, "err", err)
        return fmt.Errorf("推送好友通过通知失败: %w", err)
    }

    // ==================== 6. 也推送给接受者（多端同步） ====================
    acceptSignalPayload, _ := json.Marshal(map[string]interface{}{
        "type":               "friend_accepted_sync",
        "request_id":         event.RequestId,
        "friend_user_id":     event.FromUserId,
        "accept_time":        event.AcceptTime,
    })
    acceptSyncSignal := &push_pb.SyncNotifySignal{
        Type:      "system",
        Payload:   acceptSignalPayload,
        Timestamp: event.AcceptTime,
    }
    c.pushService.PushToUserOtherDevices(ctx, &push_pb.PushToUserOtherDevicesRequest{
        UserId:          event.ToUserId,
        ExcludeDeviceId: "",
        Signal:          acceptSyncSignal,
    })

    log.Info("好友通过通知推送完成",
        "request_id", event.RequestId,
        "from_user_id", event.FromUserId,
        "to_user_id", event.ToUserId)

    return nil
}
```

### Consumer: `group.member.joined`

> 来源：Group 服务。成员入群后触发（邀请加入/申请通过）。  
> 职责：通知群内所有成员有新成员加入。

```go
func (c *PushConsumer) HandleGroupMemberJoined(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event struct {
        Header    *common.EventHeader `protobuf:"bytes,1"`
        GroupId   string
        UserId    string
        Nickname  string
        InviteBy  string
        JoinTime  int64
        GroupName string
    }
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 group.member.joined 失败", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 ====================
    dedupKey := fmt.Sprintf("push:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    // ==================== 3. 参数校验 ====================
    if event.GroupId == "" || event.UserId == "" {
        log.Error("group.member.joined: 关键字段缺失")
        return nil
    }

    // ==================== 4. 构建入群通知信号 ====================
    signalPayload, _ := json.Marshal(map[string]interface{}{
        "type":       "member_joined",
        "group_id":   event.GroupId,
        "group_name": event.GroupName,
        "user_id":    event.UserId,
        "nickname":   event.Nickname,
        "invite_by":  event.InviteBy,
        "join_time":  event.JoinTime,
    })

    signal := &push_pb.SyncNotifySignal{
        Type:      "system",
        Payload:   signalPayload,
        Timestamp: event.JoinTime,
    }

    // ==================== 5. 推送给群内所有成员（排除新加入者自己） ====================
    _, err := c.pushService.PushToGroup(ctx, &push_pb.PushToGroupRequest{
        GroupId:        event.GroupId,
        Signal:         signal,
        ExcludeUserIds: []string{event.UserId}, // 排除新加入者（他已经知道自己入群了）
    })
    if err != nil {
        log.Error("推送入群通知失败", "group_id", event.GroupId, "err", err)
        return fmt.Errorf("推送入群通知失败: %w", err)
    }

    // ==================== 6. 单独推送给新加入者（欢迎信号） ====================
    welcomePayload, _ := json.Marshal(map[string]interface{}{
        "type":       "welcome_to_group",
        "group_id":   event.GroupId,
        "group_name": event.GroupName,
    })
    welcomeSignal := &push_pb.SyncNotifySignal{
        Type:      "system",
        Payload:   welcomePayload,
        Timestamp: event.JoinTime,
    }
    c.pushService.PushToUser(ctx, &push_pb.PushToUserRequest{
        UserId: event.UserId,
        Signal: welcomeSignal,
    })

    // ==================== 7. 更新本地群成员缓存 ====================
    c.handleGroupMemberJoinedCache(ctx, event.GroupId, event.UserId)

    log.Info("入群通知推送完成",
        "group_id", event.GroupId, "user_id", event.UserId, "nickname", event.Nickname)

    return nil
}
```

### Consumer: `group.member.kicked`

> 来源：Group 服务。成员被踢出群后触发。  
> 职责：通知被踢者（你已被移出群聊）+ 通知群内其他成员（XX 被移出群聊）。

```go
func (c *PushConsumer) HandleGroupMemberKicked(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event struct {
        Header     *common.EventHeader `protobuf:"bytes,1"`
        GroupId    string
        UserId     string
        Nickname   string
        OperatorId string
        KickTime   int64
        GroupName  string
        Reason     string
    }
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 group.member.kicked 失败", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 ====================
    dedupKey := fmt.Sprintf("push:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    // ==================== 3. 通知被踢者 ====================
    kickedPayload, _ := json.Marshal(map[string]interface{}{
        "type":        "kicked_from_group",
        "group_id":    event.GroupId,
        "group_name":  event.GroupName,
        "operator_id": event.OperatorId,
        "reason":      event.Reason,
        "kick_time":   event.KickTime,
    })

    kickedSignal := &push_pb.SyncNotifySignal{
        Type:      "kicked",
        Payload:   kickedPayload,
        Timestamp: event.KickTime,
    }

    _, err := c.pushService.PushToUser(ctx, &push_pb.PushToUserRequest{
        UserId: event.UserId,
        Signal: kickedSignal,
    })
    if err != nil {
        log.Error("推送踢人通知给被踢者失败",
            "group_id", event.GroupId, "user_id", event.UserId, "err", err)
    }

    // ==================== 4. 通知群内其他成员 ====================
    memberKickedPayload, _ := json.Marshal(map[string]interface{}{
        "type":        "member_kicked",
        "group_id":    event.GroupId,
        "group_name":  event.GroupName,
        "user_id":     event.UserId,
        "nickname":    event.Nickname,
        "operator_id": event.OperatorId,
        "kick_time":   event.KickTime,
    })

    memberKickedSignal := &push_pb.SyncNotifySignal{
        Type:      "system",
        Payload:   memberKickedPayload,
        Timestamp: event.KickTime,
    }

    _, err = c.pushService.PushToGroup(ctx, &push_pb.PushToGroupRequest{
        GroupId:        event.GroupId,
        Signal:         memberKickedSignal,
        ExcludeUserIds: []string{event.UserId, event.OperatorId}, // 排除被踢者和操作者
    })
    if err != nil {
        log.Error("推送踢人通知给群成员失败",
            "group_id", event.GroupId, "err", err)
        return fmt.Errorf("推送踢人通知给群成员失败: %w", err)
    }

    // ==================== 5. 更新本地群成员缓存 ====================
    c.handleGroupMemberKickedCache(ctx, event.GroupId, event.UserId)

    log.Info("踢人通知推送完成",
        "group_id", event.GroupId, "kicked_user_id", event.UserId,
        "operator_id", event.OperatorId)

    return nil
}
```

### Consumer: `group.dissolved`

> 来源：Group 服务。群解散后触发。  
> 职责：通知所有群成员 — 群已被群主解散。

```go
func (c *PushConsumer) HandleGroupDissolved(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event struct {
        Header      *common.EventHeader `protobuf:"bytes,1"`
        GroupId     string
        GroupName   string
        OwnerId     string
        MemberIds   []string
        DissolveTime int64
    }
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 group.dissolved 失败", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 ====================
    dedupKey := fmt.Sprintf("push:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    // ==================== 3. 参数校验 ====================
    if event.GroupId == "" || len(event.MemberIds) == 0 {
        log.Error("group.dissolved: 关键字段缺失", "group_id", event.GroupId)
        return nil
    }

    // ==================== 4. 构建群解散通知信号 ====================
    signalPayload, _ := json.Marshal(map[string]interface{}{
        "type":          "group_dissolved",
        "group_id":      event.GroupId,
        "group_name":    event.GroupName,
        "owner_id":      event.OwnerId,
        "dissolve_time": event.DissolveTime,
    })

    signal := &push_pb.SyncNotifySignal{
        Type:      "system",
        Payload:   signalPayload,
        Timestamp: event.DissolveTime,
    }

    // ==================== 5. 批量推送给所有成员（包括群主） ====================
    // 群已解散，无法通过 PushToGroup（Group.GetGroupMembers 会返回空）
    // 需要直接使用事件中携带的 MemberIds
    _, err := c.pushService.BatchPushToUsers(ctx, &push_pb.BatchPushToUsersRequest{
        UserIds: event.MemberIds,
        Signal:  signal,
    })
    if err != nil {
        log.Error("推送群解散通知失败",
            "group_id", event.GroupId, "member_count", len(event.MemberIds), "err", err)
        return fmt.Errorf("推送群解散通知失败: %w", err)
    }

    // ==================== 6. 清除本地群成员缓存 ====================
    c.handleGroupDissolvedCache(ctx, event.GroupId)

    log.Info("群解散通知推送完成",
        "group_id", event.GroupId, "group_name", event.GroupName,
        "member_count", len(event.MemberIds))

    return nil
}
```

### Consumer: `group.info.updated`

> 来源：Group 服务。群信息更新后触发（群名称/头像/公告/设置变更）。  
> 职责：通知所有群成员群信息已变更。

```go
func (c *PushConsumer) HandleGroupInfoUpdated(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event struct {
        Header        *common.EventHeader `protobuf:"bytes,1"`
        GroupId       string
        OperatorId    string
        UpdatedFields map[string]string
        UpdateTime    int64
    }
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 group.info.updated 失败", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 ====================
    dedupKey := fmt.Sprintf("push:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    // ==================== 3. 参数校验 ====================
    if event.GroupId == "" {
        log.Error("group.info.updated: group_id 缺失")
        return nil
    }

    // ==================== 4. 构建群信息变更通知信号 ====================
    signalPayload, _ := json.Marshal(map[string]interface{}{
        "type":           "group_info_updated",
        "group_id":       event.GroupId,
        "operator_id":    event.OperatorId,
        "updated_fields": event.UpdatedFields,
        "update_time":    event.UpdateTime,
    })

    signal := &push_pb.SyncNotifySignal{
        Type:      "system",
        Payload:   signalPayload,
        Timestamp: event.UpdateTime,
    }

    // ==================== 5. 推送给群内所有成员 ====================
    _, err := c.pushService.PushToGroup(ctx, &push_pb.PushToGroupRequest{
        GroupId:        event.GroupId,
        Signal:         signal,
        ExcludeUserIds: []string{event.OperatorId}, // 排除操作者
    })
    if err != nil {
        log.Error("推送群信息变更通知失败",
            "group_id", event.GroupId, "err", err)
        return fmt.Errorf("推送群信息变更通知失败: %w", err)
    }

    log.Info("群信息变更通知推送完成",
        "group_id", event.GroupId, "operator_id", event.OperatorId,
        "updated_fields", event.UpdatedFields)

    return nil
}
```

### Consumer: `push.offline.trigger`

> 来源：Push 服务自身（自产自消）。  
> 职责：**实际调用 APNs/FCM/华为/小米/OPPO/vivo SDK 发送离线推送通知。**  
> 这是整个推送系统最终出口，直接与第三方推送通道交互。  
> 支持失败重试（最多 5 次），Token 失效自动注销设备。

```go
func (c *PushConsumer) HandlePushOfflineTrigger(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_push.PushOfflineTriggerEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 PushOfflineTriggerEvent 失败", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 ====================
    dedupKey := fmt.Sprintf("push:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    // ==================== 3. 参数校验 ====================
    if event.UserId == "" || event.PushToken == "" || event.PushChannel == "" {
        log.Error("push.offline.trigger: 关键字段缺失",
            "user_id", event.UserId, "push_channel", event.PushChannel)
        return nil
    }

    // ==================== 4. 检查推送是否已过期 ====================
    if event.ExpireTime > 0 && time.Now().UnixMilli() > event.ExpireTime {
        log.Debug("离线推送已过期，跳过",
            "user_id", event.UserId, "msg_id", event.MsgId,
            "expire_time", event.ExpireTime)
        return nil
    }

    // ==================== 5. 检查用户是否已上线（避免重复推送） ====================
    // 用户可能在推送任务排队期间已经上线并拉取了消息
    presenceResp, err := c.presenceClient.GetPresence(ctx, &presence_pb.GetPresenceRequest{
        UserId: event.UserId,
    })
    if err == nil {
        for _, p := range presenceResp.Presences {
            if p.Status != common.ONLINE_STATUS_OFFLINE {
                log.Debug("用户已上线，跳过离线推送",
                    "user_id", event.UserId, "msg_id", event.MsgId)
                return nil
            }
        }
    }

    // ==================== 6. 重试计数检查 ====================
    // Key: push:offline_retry:{push_id}
    pushID := event.Header.EventId
    retryKey := fmt.Sprintf("push:offline_retry:%s", pushID)
    retryCount, _ := c.redis.Incr(ctx, retryKey).Result()
    if retryCount == 1 {
        c.redis.Expire(ctx, retryKey, 1*time.Hour)
    }
    if retryCount > 5 {
        log.Error("离线推送重试次数超限，放弃",
            "user_id", event.UserId, "push_channel", event.PushChannel,
            "retry_count", retryCount)
        // 生产失败结果事件
        failEvent := &kafka_push.PushOfflineResultEvent{
            Header:       buildEventHeader("push", c.instanceID),
            PushId:       pushID,
            UserId:       event.UserId,
            PushChannel:  event.PushChannel,
            Success:      false,
            ErrorCode:    "MAX_RETRY_EXCEEDED",
            ErrorMsg:     fmt.Sprintf("exceeded max retry count: %d", retryCount),
            PushTime:     time.Now().UnixMilli(),
        }
        c.kafka.Produce(ctx, "push.offline.result", event.UserId, failEvent)
        return nil
    }

    // ==================== 7. 实际调用推送通道 SDK ====================
    var pushErr error
    badge := event.Badge

    switch event.PushChannel {
    case "apns":
        pushErr = c.apnsClient.Send(ctx, &apns.Notification{
            DeviceToken: event.PushToken,
            Topic:       c.config.APNsBundleID,
            Payload: apns.NewPayload().
                AlertTitle(event.Title).
                AlertBody(event.Content).
                Badge(parseInt(badge)).
                Sound(event.Sound).
                MutableContent().
                Custom("data", event.Data),
            Expiration: time.UnixMilli(event.ExpireTime),
        })

    case "fcm":
        ttlSeconds := int64(0)
        if event.ExpireTime > 0 {
            ttlSeconds = (event.ExpireTime - time.Now().UnixMilli()) / 1000
            if ttlSeconds < 0 {
                ttlSeconds = 0
            }
        }
        pushErr = c.fcmClient.Send(ctx, &fcm.Message{
            Token: event.PushToken,
            Notification: &fcm.Notification{
                Title:    event.Title,
                Body:     event.Content,
                ImageURL: event.ImageUrl,
            },
            Data: event.Data,
            Android: &fcm.AndroidConfig{
                Priority: "high",
                TTL:      fmt.Sprintf("%ds", ttlSeconds),
                Notification: &fcm.AndroidNotification{
                    Sound:       event.Sound,
                    ClickAction: "OPEN_CHAT",
                },
            },
        })

    case "huawei":
        pushErr = c.huaweiClient.Send(ctx, &huawei.PushRequest{
            Token:   []string{event.PushToken},
            Title:   event.Title,
            Content: event.Content,
            Badge:   badge,
            Data:    mapToJSON(event.Data),
        })

    case "xiaomi":
        pushErr = c.xiaomiClient.Send(ctx, &xiaomi.PushRequest{
            RegID:   event.PushToken,
            Title:   event.Title,
            Content: event.Content,
            Extra:   event.Data,
        })

    case "oppo":
        pushErr = c.oppoClient.Send(ctx, &oppo.PushRequest{
            RegID:   event.PushToken,
            Title:   event.Title,
            Content: event.Content,
        })

    case "vivo":
        pushErr = c.vivoClient.Send(ctx, &vivo.PushRequest{
            RegID:   event.PushToken,
            Title:   event.Title,
            Content: event.Content,
        })

    default:
        log.Error("未知推送通道", "push_channel", event.PushChannel,
            "user_id", event.UserId)
        return nil
    }

    // ==================== 8. 处理推送结果 ====================
    if pushErr != nil {
        log.Error("离线推送发送失败",
            "user_id", event.UserId,
            "device_id", event.DeviceId,
            "push_channel", event.PushChannel,
            "retry_count", retryCount,
            "err", pushErr)

        // 检查 Token 是否失效
        if isTokenInvalidError(pushErr) {
            log.Warn("推送 Token 已失效，异步注销设备",
                "user_id", event.UserId, "device_id", event.DeviceId)
            go func() {
                c.pushService.UnregisterDevice(context.Background(), &push_pb.UnregisterDeviceRequest{
                    UserId:   event.UserId,
                    DeviceId: event.DeviceId,
                })
            }()
            // Token 失效不重试
            return nil
        }

        // 其他错误：返回 error 触发 Kafka Consumer 自动重试
        return fmt.Errorf("离线推送失败: %w", pushErr)
    }

    // ==================== 9. 推送成功：清理重试计数 + 生产结果事件 ====================
    c.redis.Del(ctx, retryKey)

    successEvent := &kafka_push.PushOfflineResultEvent{
        Header:      buildEventHeader("push", c.instanceID),
        PushId:      pushID,
        UserId:      event.UserId,
        PushChannel: event.PushChannel,
        Success:     true,
        PushTime:    time.Now().UnixMilli(),
    }
    c.kafka.Produce(ctx, "push.offline.result", event.UserId, successEvent)

    // ==================== 10. 异步更新推送统计 — PgSQL ====================
    go func() {
        today := time.Now().Format("2006-01-02")
        channelCol := ""
        switch event.PushChannel {
        case "apns":
            channelCol = "total_apns"
        case "fcm":
            channelCol = "total_fcm"
        case "huawei":
            channelCol = "total_huawei"
        case "xiaomi":
            channelCol = "total_xiaomi"
        default:
            channelCol = "total_offline"
        }

        _, err := c.db.ExecContext(context.Background(),
            fmt.Sprintf(
                `INSERT INTO push_statistics (date, %s, total_offline, success_count)
                 VALUES ($1, 1, 1, 1)
                 ON CONFLICT (date) DO UPDATE SET
                    %s = push_statistics.%s + 1,
                    total_offline = push_statistics.total_offline + 1,
                    success_count = push_statistics.success_count + 1`,
                channelCol, channelCol, channelCol,
            ),
            today,
        )
        if err != nil {
            log.Warn("更新推送统计失败", "date", today, "err", err)
        }
    }()

    log.Info("离线推送发送成功",
        "user_id", event.UserId,
        "device_id", event.DeviceId,
        "push_channel", event.PushChannel,
        "msg_id", event.MsgId)

    return nil
}
```

### Consumer: `config.changed`

> 来源：Config 服务。推送相关配置变更时热更新，无需重启服务。  
> 支持的配置项：速率限制、大群阈值、离线推送去重 TTL、设备缓存 TTL、推送通道开关等。

```go
func (c *PushConsumer) HandleConfigChanged(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event struct {
        Header    *common.EventHeader `protobuf:"bytes,1"`
        ConfigKey string
        OldValue  string
        NewValue  string
    }
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 config.changed 失败", "err", err)
        return nil
    }

    // ==================== 2. 筛选 push.* 前缀的配置 ====================
    if !strings.HasPrefix(event.ConfigKey, "push.") {
        return nil // 非 Push 服务配置，忽略
    }

    log.Info("Push 服务配置变更",
        "config_key", event.ConfigKey,
        "old_value", event.OldValue,
        "new_value", event.NewValue)

    // ==================== 3. 根据配置项更新内存配置 ====================
    switch event.ConfigKey {
    case "push.rate_limit_per_minute":
        // 每用户每分钟推送上限
        val, err := strconv.Atoi(event.NewValue)
        if err != nil || val <= 0 {
            log.Error("push.rate_limit_per_minute 配置值无效", "value", event.NewValue)
            return nil
        }
        c.config.Store("RateLimitPerMinute", val)
        log.Info("已更新 RateLimitPerMinute", "old", event.OldValue, "new", val)

    case "push.large_group_threshold":
        // 大群阈值
        val, err := strconv.Atoi(event.NewValue)
        if err != nil || val <= 0 {
            log.Error("push.large_group_threshold 配置值无效", "value", event.NewValue)
            return nil
        }
        c.config.Store("LargeGroupThreshold", val)
        log.Info("已更新 LargeGroupThreshold", "old", event.OldValue, "new", val)

    case "push.offline_dedup_ttl_minutes":
        // 离线推送去重 TTL（分钟）
        val, err := strconv.Atoi(event.NewValue)
        if err != nil || val <= 0 {
            log.Error("push.offline_dedup_ttl_minutes 配置值无效", "value", event.NewValue)
            return nil
        }
        c.config.Store("OfflineDedupTTLMinutes", val)
        log.Info("已更新 OfflineDedupTTLMinutes", "old", event.OldValue, "new", val)

    case "push.apns_enabled":
        val := strings.ToLower(event.NewValue) == "true"
        c.config.Store("APNsEnabled", val)
        log.Info("已更新 APNsEnabled", "old", event.OldValue, "new", val)

    case "push.fcm_enabled":
        val := strings.ToLower(event.NewValue) == "true"
        c.config.Store("FCMEnabled", val)
        log.Info("已更新 FCMEnabled", "old", event.OldValue, "new", val)

    case "push.huawei_enabled":
        val := strings.ToLower(event.NewValue) == "true"
        c.config.Store("HuaweiEnabled", val)
        log.Info("已更新 HuaweiEnabled", "old", event.OldValue, "new", val)

    case "push.xiaomi_enabled":
        val := strings.ToLower(event.NewValue) == "true"
        c.config.Store("XiaomiEnabled", val)
        log.Info("已更新 XiaomiEnabled", "old", event.OldValue, "new", val)

    case "push.max_batch_push_users":
        // 单次批量推送最大用户数
        val, err := strconv.Atoi(event.NewValue)
        if err != nil || val <= 0 {
            log.Error("push.max_batch_push_users 配置值无效", "value", event.NewValue)
            return nil
        }
        c.config.Store("MaxBatchPushUsers", val)
        log.Info("已更新 MaxBatchPushUsers", "old", event.OldValue, "new", val)

    case "push.goroutine_pool_size":
        // goroutine 池大小
        val, err := strconv.Atoi(event.NewValue)
        if err != nil || val <= 0 {
            log.Error("push.goroutine_pool_size 配置值无效", "value", event.NewValue)
            return nil
        }
        c.config.Store("GoroutinePoolSize", val)
        log.Info("已更新 GoroutinePoolSize", "old", event.OldValue, "new", val)

    default:
        log.Debug("未知 Push 配置项，忽略", "config_key", event.ConfigKey)
    }

    return nil
}
```

---

## 本地缓存维护消费者（Local Cache Consumers）

> **设计说明：** 以下消费者专门负责维护 Push 服务的本地 Redis 缓存。  
> Push RPC 层（PushToGroup 查群成员、PushToUser 查在线状态/用户设置等）从这些缓存读取数据，  
> 避免每次推送都发起远程 RPC 调用，显著降低 P99 延迟并减轻上游服务压力。  
> 缓存 TTL 较短（5~30min），配合延迟双删保证最终一致性。  
> 对于已有推送消费者的 topic（如 group.member.joined），缓存维护逻辑在已有 handler 末尾追加调用。

### 通用缓存工具方法

```go
// delayedDoubleDelete 延迟双删策略 — 保证缓存与数据源最终一致性
// 流程：先删缓存 → 延迟 500ms 后再删一次（防止并发写入脏数据）
func (c *PushConsumer) delayedDoubleDelete(ctx context.Context, cacheKey string) {
    // 第一次删除
    c.redis.Del(ctx, cacheKey)
    // 延迟 500ms 后第二次删除（异步，不阻塞主流程）
    go func() {
        time.Sleep(500 * time.Millisecond)
        c.redis.Del(context.Background(), cacheKey)
    }()
}

// handleGroupMemberJoinedCache 入群事件 → 增量更新本地群成员缓存
// 在 HandleGroupMemberJoined 推送 handler 末尾调用
func (c *PushConsumer) handleGroupMemberJoinedCache(ctx context.Context, groupId, userId string) {
    cacheKey := fmt.Sprintf("push:local:group_members:%s", groupId)
    // 缓存存在才增量更新（不存在说明从未加载，由 RPC 层按需加载）
    if exists, _ := c.redis.Exists(ctx, cacheKey).Result(); exists > 0 {
        c.redis.SAdd(ctx, cacheKey, userId)
        c.redis.Expire(ctx, cacheKey, 10*time.Minute) // 续期
    }
    log.Debug("本地群成员缓存已更新（入群）",
        "group_id", groupId, "user_id", userId)
}

// handleGroupMemberKickedCache 踢人事件 → 从本地群成员缓存移除
// 在 HandleGroupMemberKicked 推送 handler 末尾调用
func (c *PushConsumer) handleGroupMemberKickedCache(ctx context.Context, groupId, userId string) {
    cacheKey := fmt.Sprintf("push:local:group_members:%s", groupId)
    c.redis.SRem(ctx, cacheKey, userId)
    log.Debug("本地群成员缓存已更新（踢人）",
        "group_id", groupId, "user_id", userId)
}

// handleGroupDissolvedCache 群解散 → 删除整个本地群成员缓存
// 在 HandleGroupDissolved 推送 handler 末尾调用
func (c *PushConsumer) handleGroupDissolvedCache(ctx context.Context, groupId string) {
    cacheKey := fmt.Sprintf("push:local:group_members:%s", groupId)
    c.redis.Del(ctx, cacheKey)
    log.Debug("本地群成员缓存已删除（群解散）", "group_id", groupId)
}
```

### Consumer: `presence.online`（缓存维护）

> **已移除：presence.online / presence.offline 缓存维护消费者**  
> 在线/离线状态变化频率（每天数次）显著高于头像、设置等数据（每月一次），  
> 不满足「高频读取 + 变化不频繁」的跨服务缓存准入条件，因此不再维护 `push:local:presence` 本地缓存。  
> PushToUser 直接通过 Connecte.GetUserConnections 判断在线（推送链路本就必须调用）；  
> PushToGroup 通过 Presence.BatchGetPresence RPC 批量查询（单次 RPC，数据源头，零缓存延迟）。

### Consumer: `group.member.left`（缓存维护 + 推送）

> 来源：Group 服务。成员主动退群后触发。  
> 职责：SREM 退出成员从本地群成员缓存 + 推送退群通知给群内其他成员。

```go
// HandleGroupMemberLeft 成员主动退群 → 更新本地群成员缓存 + 推送退群通知
func (c *PushConsumer) HandleGroupMemberLeft(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event struct {
        Header    *common.EventHeader `protobuf:"bytes,1"`
        GroupId   string
        UserId    string
        Nickname  string
        LeaveTime int64
        GroupName string
    }
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 group.member.left 失败", "err", err)
        return nil
    }

    // ==================== 2. 幂等去重 ====================
    dedupKey := fmt.Sprintf("push:kafka:dedup:%s", event.Header.EventId)
    if !c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Val() {
        return nil
    }

    // ==================== 3. 参数校验 ====================
    if event.GroupId == "" || event.UserId == "" {
        log.Error("group.member.left: 关键字段缺失")
        return nil
    }

    // ==================== 4. 更新本地群成员缓存 — SREM ====================
    cacheKey := fmt.Sprintf("push:local:group_members:%s", event.GroupId)
    c.redis.SRem(ctx, cacheKey, event.UserId)

    // ==================== 5. 推送退群通知给群内其他成员 ====================
    signalPayload, _ := json.Marshal(map[string]interface{}{
        "type":       "member_left",
        "group_id":   event.GroupId,
        "group_name": event.GroupName,
        "user_id":    event.UserId,
        "nickname":   event.Nickname,
        "leave_time": event.LeaveTime,
    })

    signal := &push_pb.SyncNotifySignal{
        Type:      "system",
        Payload:   signalPayload,
        Timestamp: event.LeaveTime,
    }

    _, err := c.pushService.PushToGroup(ctx, &push_pb.PushToGroupRequest{
        GroupId:        event.GroupId,
        Signal:         signal,
        ExcludeUserIds: []string{event.UserId},
    })
    if err != nil {
        log.Error("推送退群通知失败", "group_id", event.GroupId, "err", err)
        return fmt.Errorf("推送退群通知失败: %w", err)
    }

    log.Info("退群通知推送完成 + 本地缓存已更新",
        "group_id", event.GroupId, "user_id", event.UserId)
    return nil
}
```

### Consumer: `user.settings.updated`（缓存维护）

> 来源：User 服务。用户修改设置后触发（免打扰/推送开关/勿扰模式等）。  
> 职责：更新本地用户设置缓存 `push:local:user_settings:{user_id}`。

```go
// HandleUserSettingsUpdated 用户设置变更 → 更新本地用户设置缓存
func (c *PushConsumer) HandleUserSettingsUpdated(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_user.UserSettingsUpdatedEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 UserSettingsUpdatedEvent 失败", "err", err)
        return nil
    }

    // ==================== 2. 幂等去重 ====================
    dedupKey := fmt.Sprintf("push:kafka:dedup:%s", event.Header.EventId)
    if !c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Val() {
        return nil
    }

    // ==================== 3. 参数校验 ====================
    if event.UserId == "" {
        log.Error("user.settings.updated: user_id 缺失")
        return nil
    }

    // ==================== 4. 延迟双删 + 写入缓存 ====================
    cacheKey := fmt.Sprintf("push:local:user_settings:%s", event.UserId)
    c.delayedDoubleDelete(ctx, cacheKey)

    // 构建设置字段（仅写入事件中携带的变化字段）
    settingsMap := map[string]interface{}{}
    if event.PushEnabled != nil {
        settingsMap["push_enabled"] = *event.PushEnabled
    }
    if event.DoNotDisturb != nil {
        settingsMap["dnd"] = *event.DoNotDisturb
    }
    if event.DndStartTime != "" {
        settingsMap["dnd_start_time"] = event.DndStartTime
    }
    if event.DndEndTime != "" {
        settingsMap["dnd_end_time"] = event.DndEndTime
    }
    if event.SoundEnabled != nil {
        settingsMap["sound_enabled"] = *event.SoundEnabled
    }
    if event.VibrateEnabled != nil {
        settingsMap["vibrate_enabled"] = *event.VibrateEnabled
    }
    if event.PreviewEnabled != nil {
        settingsMap["preview_enabled"] = *event.PreviewEnabled
    }

    if len(settingsMap) > 0 {
        c.redis.HSet(ctx, cacheKey, settingsMap)
        c.redis.Expire(ctx, cacheKey, 10*time.Minute)
    }

    log.Debug("本地用户设置缓存已更新",
        "user_id", event.UserId, "fields", settingsMap)
    return nil
}
```

### Consumer: `user.profile.updated`（缓存维护）

> 来源：User 服务。用户修改资料后触发（昵称/头像等）。  
> 职责：更新本地用户信息缓存 `push:local:user_info:{user_id}`，用于推送内容构建。

```go
// HandleUserProfileUpdatedCache 用户资料变更 → 更新本地用户信息缓存
func (c *PushConsumer) HandleUserProfileUpdatedCache(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_user.UserProfileUpdatedEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 UserProfileUpdatedEvent 失败", "err", err)
        return nil
    }

    // ==================== 2. 幂等去重 ====================
    dedupKey := fmt.Sprintf("push:kafka:dedup:%s", event.Header.EventId)
    if !c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Val() {
        return nil
    }

    // ==================== 3. 参数校验 ====================
    if event.UserId == "" {
        log.Error("user.profile.updated(cache): user_id 缺失")
        return nil
    }

    // ==================== 4. 延迟双删 + 写入缓存 ====================
    cacheKey := fmt.Sprintf("push:local:user_info:%s", event.UserId)
    c.delayedDoubleDelete(ctx, cacheKey)

    // 从 updated_fields map 提取 Push 服务关注的字段
    // 注：UserProfileUpdatedEvent.updated_fields 是 map<string,string>
    profileMap := map[string]interface{}{}
    if v, ok := event.UpdatedFields["nickname"]; ok && v != "" {
        profileMap["nickname"] = v
    }
    if v, ok := event.UpdatedFields["avatar_url"]; ok && v != "" {
        profileMap["avatar_url"] = v
    }
    if v, ok := event.UpdatedFields["username"]; ok && v != "" {
        profileMap["username"] = v
    }

    if len(profileMap) > 0 {
        c.redis.HSet(ctx, cacheKey, profileMap)
        c.redis.Expire(ctx, cacheKey, 30*time.Minute)
    }

    log.Debug("本地用户信息缓存已更新",
        "user_id", event.UserId, "fields", profileMap)
    return nil
}
```

---

## 闭环补充消费者（Self-Closing Loop）

> **设计说明：** 以下消费者补充 Push 服务消费闭环中缺失的事件推送逻辑。  
> 这些事件之前未被 Push 服务消费，导致相关用户无法及时收到推送通知。

### Consumer: `group.application`

> 来源：Group 服务。用户申请加入群组后触发。  
> 职责：推送入群申请通知给所有群管理员。

```go
// HandleGroupApplication 入群申请 → 推送给群管理员
func (c *PushConsumer) HandleGroupApplication(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event struct {
        Header        *common.EventHeader `protobuf:"bytes,1"`
        GroupId       string
        GroupName     string
        ApplicantId   string
        ApplicantName string
        Message       string
        AdminIds      []string // 群管理员 ID 列表（事件中携带）
        ApplyTime     int64
    }
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 group.application 失败", "err", err)
        return nil
    }

    // ==================== 2. 幂等去重 ====================
    dedupKey := fmt.Sprintf("push:kafka:dedup:%s", event.Header.EventId)
    if !c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Val() {
        return nil
    }

    // ==================== 3. 参数校验 ====================
    if event.GroupId == "" || event.ApplicantId == "" || len(event.AdminIds) == 0 {
        log.Error("group.application: 关键字段缺失")
        return nil
    }

    // ==================== 4. 构建入群申请通知信号 ====================
    signalPayload, _ := json.Marshal(map[string]interface{}{
        "type":           "group_application",
        "group_id":       event.GroupId,
        "group_name":     event.GroupName,
        "applicant_id":   event.ApplicantId,
        "applicant_name": event.ApplicantName,
        "message":        event.Message,
        "apply_time":     event.ApplyTime,
    })

    signal := &push_pb.SyncNotifySignal{
        Type:      "system",
        Payload:   signalPayload,
        Timestamp: event.ApplyTime,
    }

    // ==================== 5. 推送给所有群管理员 ====================
    _, err := c.pushService.BatchPushToUsers(ctx, &push_pb.BatchPushToUsersRequest{
        UserIds: event.AdminIds,
        Signal:  signal,
    })
    if err != nil {
        log.Error("推送入群申请通知给管理员失败",
            "group_id", event.GroupId, "err", err)
        return fmt.Errorf("推送入群申请通知失败: %w", err)
    }

    log.Info("入群申请通知推送完成",
        "group_id", event.GroupId, "applicant_id", event.ApplicantId,
        "admin_count", len(event.AdminIds))
    return nil
}
```

### Consumer: `group.application.handled`

> 来源：Group 服务。管理员审批入群申请后触发。  
> 职责：推送审批结果通知给申请者。

```go
// HandleGroupApplicationHandled 入群申请处理结果 → 推送给申请者
func (c *PushConsumer) HandleGroupApplicationHandled(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event struct {
        Header      *common.EventHeader `protobuf:"bytes,1"`
        GroupId     string
        GroupName   string
        ApplicantId string
        HandlerId   string
        HandlerName string
        Result      string // "approved" / "rejected"
        Reason      string
        HandleTime  int64
    }
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 group.application.handled 失败", "err", err)
        return nil
    }

    // ==================== 2. 幂等去重 ====================
    dedupKey := fmt.Sprintf("push:kafka:dedup:%s", event.Header.EventId)
    if !c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Val() {
        return nil
    }

    // ==================== 3. 参数校验 ====================
    if event.GroupId == "" || event.ApplicantId == "" {
        log.Error("group.application.handled: 关键字段缺失")
        return nil
    }

    // ==================== 4. 构建处理结果通知信号 ====================
    signalPayload, _ := json.Marshal(map[string]interface{}{
        "type":         "group_application_handled",
        "group_id":     event.GroupId,
        "group_name":   event.GroupName,
        "handler_id":   event.HandlerId,
        "handler_name": event.HandlerName,
        "result":       event.Result,
        "reason":       event.Reason,
        "handle_time":  event.HandleTime,
    })

    signal := &push_pb.SyncNotifySignal{
        Type:      "system",
        Payload:   signalPayload,
        Timestamp: event.HandleTime,
    }

    // ==================== 5. 推送给申请者 ====================
    _, err := c.pushService.PushToUser(ctx, &push_pb.PushToUserRequest{
        UserId: event.ApplicantId,
        Signal: signal,
    })
    if err != nil {
        log.Error("推送入群申请处理结果失败",
            "group_id", event.GroupId, "applicant_id", event.ApplicantId, "err", err)
        return fmt.Errorf("推送入群申请处理结果失败: %w", err)
    }

    log.Info("入群申请处理结果推送完成",
        "group_id", event.GroupId, "applicant_id", event.ApplicantId,
        "result", event.Result)
    return nil
}
```

### Consumer: `group.member.role.changed`

> 来源：Group 服务。成员角色变更后触发（普通成员↔管理员↔群主）。  
> 职责：推送角色变更通知给被变更成员 + 群内其他成员。

```go
// HandleGroupMemberRoleChanged 成员角色变更 → 推送给被变更成员
func (c *PushConsumer) HandleGroupMemberRoleChanged(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event struct {
        Header     *common.EventHeader `protobuf:"bytes,1"`
        GroupId    string
        GroupName  string
        UserId     string
        Nickname   string
        OldRole    int32 // 1=普通成员 2=管理员 3=群主
        NewRole    int32
        OperatorId string
        ChangeTime int64
    }
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 group.member.role.changed 失败", "err", err)
        return nil
    }

    // ==================== 2. 幂等去重 ====================
    dedupKey := fmt.Sprintf("push:kafka:dedup:%s", event.Header.EventId)
    if !c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Val() {
        return nil
    }

    // ==================== 3. 参数校验 ====================
    if event.GroupId == "" || event.UserId == "" {
        log.Error("group.member.role.changed: 关键字段缺失")
        return nil
    }

    // ==================== 4. 构建角色变更通知信号 ====================
    roleNames := map[int32]string{1: "普通成员", 2: "管理员", 3: "群主"}
    signalPayload, _ := json.Marshal(map[string]interface{}{
        "type":          "member_role_changed",
        "group_id":      event.GroupId,
        "group_name":    event.GroupName,
        "user_id":       event.UserId,
        "nickname":      event.Nickname,
        "old_role":      event.OldRole,
        "new_role":      event.NewRole,
        "old_role_name": roleNames[event.OldRole],
        "new_role_name": roleNames[event.NewRole],
        "operator_id":   event.OperatorId,
        "change_time":   event.ChangeTime,
    })

    signal := &push_pb.SyncNotifySignal{
        Type:      "system",
        Payload:   signalPayload,
        Timestamp: event.ChangeTime,
    }

    // ==================== 5. 推送给被变更的成员 ====================
    _, err := c.pushService.PushToUser(ctx, &push_pb.PushToUserRequest{
        UserId: event.UserId,
        Signal: signal,
    })
    if err != nil {
        log.Error("推送角色变更通知失败",
            "group_id", event.GroupId, "user_id", event.UserId, "err", err)
        return fmt.Errorf("推送角色变更通知失败: %w", err)
    }

    // ==================== 6. 通知群内其他成员 ====================
    _, err = c.pushService.PushToGroup(ctx, &push_pb.PushToGroupRequest{
        GroupId:        event.GroupId,
        Signal:         signal,
        ExcludeUserIds: []string{event.UserId, event.OperatorId},
    })
    if err != nil {
        log.Warn("推送角色变更通知给群成员失败（不阻塞）",
            "group_id", event.GroupId, "err", err)
    }

    log.Info("角色变更通知推送完成",
        "group_id", event.GroupId, "user_id", event.UserId,
        "old_role", event.OldRole, "new_role", event.NewRole)
    return nil
}
```

### Consumer: `group.mute.changed`

> 来源：Group 服务。群禁言设置变更后触发（全体禁言/单人禁言/解除禁言）。  
> 职责：推送禁言变更通知给所有群成员。

```go
// HandleGroupMuteChanged 群禁言设置变更 → 推送给所有群成员
func (c *PushConsumer) HandleGroupMuteChanged(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event struct {
        Header     *common.EventHeader `protobuf:"bytes,1"`
        GroupId    string
        GroupName  string
        MuteType   string // "all" 全体禁言 / "member" 单人禁言
        MuteUserId string // 被禁言用户（MuteType="member" 时有值）
        IsMuted    bool   // true=禁言 false=解除禁言
        OperatorId string
        Duration   int64  // 禁言时长（秒），0 表示永久
        MuteTime   int64
    }
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 group.mute.changed 失败", "err", err)
        return nil
    }

    // ==================== 2. 幂等去重 ====================
    dedupKey := fmt.Sprintf("push:kafka:dedup:%s", event.Header.EventId)
    if !c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Val() {
        return nil
    }

    // ==================== 3. 参数校验 ====================
    if event.GroupId == "" {
        log.Error("group.mute.changed: group_id 缺失")
        return nil
    }

    // ==================== 4. 构建禁言变更通知信号 ====================
    signalPayload, _ := json.Marshal(map[string]interface{}{
        "type":          "group_mute_changed",
        "group_id":      event.GroupId,
        "group_name":    event.GroupName,
        "mute_type":     event.MuteType,
        "mute_user_id":  event.MuteUserId,
        "is_muted":      event.IsMuted,
        "operator_id":   event.OperatorId,
        "duration":      event.Duration,
        "mute_time":     event.MuteTime,
    })

    signal := &push_pb.SyncNotifySignal{
        Type:      "system",
        Payload:   signalPayload,
        Timestamp: event.MuteTime,
    }

    // ==================== 5. 推送给群成员 ====================
    if event.MuteType == "member" && event.MuteUserId != "" {
        // 单人禁言：额外推送给被禁言者本人（高优先级通知）
        _, err := c.pushService.PushToUser(ctx, &push_pb.PushToUserRequest{
            UserId: event.MuteUserId,
            Signal: signal,
        })
        if err != nil {
            log.Error("推送单人禁言通知失败",
                "group_id", event.GroupId, "mute_user_id", event.MuteUserId, "err", err)
        }
    }

    // 通知群内所有成员
    _, err := c.pushService.PushToGroup(ctx, &push_pb.PushToGroupRequest{
        GroupId:        event.GroupId,
        Signal:         signal,
        ExcludeUserIds: []string{event.OperatorId},
    })
    if err != nil {
        log.Error("推送禁言变更通知给群成员失败",
            "group_id", event.GroupId, "err", err)
        return fmt.Errorf("推送禁言变更通知失败: %w", err)
    }

    log.Info("禁言变更通知推送完成",
        "group_id", event.GroupId, "mute_type", event.MuteType,
        "is_muted", event.IsMuted)
    return nil
}
```

### Consumer: `conversation.unread.changed`

> 来源：Conversation 服务。用户全局未读数变更后触发。  
> 职责：推送 iOS 角标更新（静默推送更新 badge 数字）。

```go
// HandleConversationUnreadChanged 未读数变更 → 推送 iOS 角标更新
func (c *PushConsumer) HandleConversationUnreadChanged(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event struct {
        Header         *common.EventHeader `protobuf:"bytes,1"`
        UserId         string
        TotalUnread    int64  // 全局未读总数
        ConversationId string // 变更的会话 ID
        UnreadCount    int64  // 该会话的未读数
    }
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 conversation.unread.changed 失败", "err", err)
        return nil
    }

    // ==================== 2. 幂等去重 ====================
    dedupKey := fmt.Sprintf("push:kafka:dedup:%s", event.Header.EventId)
    if !c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Val() {
        return nil
    }

    // ==================== 3. 参数校验 ====================
    if event.UserId == "" {
        log.Error("conversation.unread.changed: user_id 缺失")
        return nil
    }

    // ==================== 4. 更新 iOS 设备角标 ====================
    devices, err := c.getUserPushDevices(ctx, event.UserId)
    if err != nil || len(devices) == 0 {
        return nil
    }

    for _, device := range devices {
        // 仅更新 iOS/iPad 设备的角标
        if device.Platform != int(common.PLATFORM_TYPE_IOS) &&
            device.Platform != int(common.PLATFORM_TYPE_IPAD) {
            continue
        }
        if !device.IsEnabled || device.PushToken == "" {
            continue
        }

        // 更新本地角标缓存
        badgeKey := fmt.Sprintf("push:badge:%s:%s", event.UserId, device.DeviceID)
        c.redis.Set(ctx, badgeKey, event.TotalUnread, 0) // 直接设为全局未读总数

        // 发送静默推送更新角标（content-available=1, 无弹窗/声音）
        if device.PushChannel == "apns" {
            silentTrigger := &kafka_push.PushOfflineTriggerEvent{
                Header:      buildEventHeader("push", c.instanceID),
                UserId:      event.UserId,
                PushChannel: "apns",
                PushToken:   device.PushToken,
                DeviceId:    device.DeviceID,
                Platform:    common.PlatformType(device.Platform),
                Title:       "", // 静默推送无标题
                Content:     "", // 静默推送无内容
                Badge:       strconv.FormatInt(event.TotalUnread, 10),
                Sound:       "", // 静默推送无声音
                ExpireTime:  time.Now().Add(1 * time.Hour).UnixMilli(),
            }
            c.kafka.Produce(ctx, "push.offline.trigger", event.UserId, silentTrigger)
        }
    }

    log.Debug("iOS 角标更新推送完成",
        "user_id", event.UserId, "total_unread", event.TotalUnread)
    return nil
}
```

### Consumer: `relation.blocked`（缓存维护）

> 来源：Relation 服务。用户执行拉黑操作后触发。  
> 职责：清理与被拉黑用户相关的本地缓存数据。  
> **注意：** 拉黑是悄悄拉黑，不推送通知给被拉黑方。

```go
// HandleRelationBlocked 拉黑事件 → 更新本地缓存
func (c *PushConsumer) HandleRelationBlocked(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event struct {
        Header    *common.EventHeader `protobuf:"bytes,1"`
        UserId    string // 执行拉黑的用户
        TargetId  string // 被拉黑的用户
        BlockTime int64
    }
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 relation.blocked 失败", "err", err)
        return nil
    }

    // ==================== 2. 幂等去重 ====================
    dedupKey := fmt.Sprintf("push:kafka:dedup:%s", event.Header.EventId)
    if !c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Val() {
        return nil
    }

    // ==================== 3. 参数校验 ====================
    if event.UserId == "" || event.TargetId == "" {
        log.Error("relation.blocked: 关键字段缺失")
        return nil
    }

    // ==================== 4. 清理相关缓存 ====================
    // 清除与被拉黑用户相关的会话免打扰缓存（强制重新加载）
    // 拉黑后不应再推送任何消息给被拉黑方
    // 此处仅做缓存清理，实际拉黑过滤逻辑在 RPC 层 PushToUser 中判断
    // 注意：拉黑是悄悄拉黑，不推送通知给被拉黑方

    log.Debug("拉黑事件已处理（缓存维护）",
        "user_id", event.UserId, "target_id", event.TargetId)
    return nil
}
```

### Consumer: `conversation.top.changed`（多端同步 — 置顶）

> 来源：Conversation 服务。用户在某设备上修改会话置顶状态后触发。  
> 职责：向用户的其他设备推送置顶状态变更信号，实现多端同步。

```go
func (c *PushConsumer) HandleConversationTopChanged(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event struct {
        Header         *common.EventHeader `protobuf:"bytes,1"`
        UserId         string // 操作用户
        DeviceId       string // 来源设备（排除此设备）
        ConversationId string // 会话 ID
        IsTop          bool   // true=置顶, false=取消置顶
        UpdateTime     int64  // 操作时间戳
    }
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 conversation.top.changed 失败", "err", err)
        return nil
    }

    // ==================== 2. 幂等去重 ====================
    dedupKey := fmt.Sprintf("push:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    // ==================== 3. 参数校验 ====================
    if event.UserId == "" || event.ConversationId == "" {
        log.Error("conversation.top.changed: 关键字段缺失",
            "user_id", event.UserId, "conversation_id", event.ConversationId)
        return nil
    }

    // ==================== 4. 构建同步信号 ====================
    signalPayload, _ := json.Marshal(map[string]interface{}{
        "conversation_id": event.ConversationId,
        "is_top":          event.IsTop,
        "update_time":     event.UpdateTime,
    })

    signal := &push_pb.SyncNotifySignal{
        Type:      "conversation.top.changed",
        Payload:   signalPayload,
        Timestamp: event.UpdateTime,
    }

    // ==================== 5. 向用户其他设备推送置顶状态变更信号 ====================
    _, err := c.pushService.PushToUserOtherDevices(ctx, &push_pb.PushToUserOtherDevicesRequest{
        UserId:          event.UserId,
        ExcludeDeviceId: event.DeviceId,
        Signal:          signal,
    })
    if err != nil {
        log.Warn("推送置顶同步失败（不阻塞）",
            "user_id", event.UserId, "conversation_id", event.ConversationId, "err", err)
    }

    log.Debug("置顶同步推送完成",
        "user_id", event.UserId, "conversation_id", event.ConversationId, "is_top", event.IsTop)
    return nil
}
```

### Consumer: `conversation.mute.changed`（多端同步 — 免打扰）

> 来源：Conversation 服务。用户在某设备上修改会话免打扰状态后触发。  
> 职责：向用户的其他设备推送免打扰状态变更信号，实现多端同步；同时更新本地免打扰缓存。

```go
func (c *PushConsumer) HandleConversationMuteChanged(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event struct {
        Header         *common.EventHeader `protobuf:"bytes,1"`
        UserId         string // 操作用户
        DeviceId       string // 来源设备（排除此设备）
        ConversationId string // 会话 ID
        IsMuted        bool   // true=开启免打扰, false=关闭免打扰
        UpdateTime     int64  // 操作时间戳
    }
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 conversation.mute.changed 失败", "err", err)
        return nil
    }

    // ==================== 2. 幂等去重 ====================
    dedupKey := fmt.Sprintf("push:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    // ==================== 3. 参数校验 ====================
    if event.UserId == "" || event.ConversationId == "" {
        log.Error("conversation.mute.changed: 关键字段缺失",
            "user_id", event.UserId, "conversation_id", event.ConversationId)
        return nil
    }

    // ==================== 4. 更新本地免打扰缓存 ====================
    // Key: push:mute:{user_id}:{conversation_id}
    muteKey := fmt.Sprintf("push:mute:%s:%s", event.UserId, event.ConversationId)
    muteVal := "0"
    if event.IsMuted {
        muteVal = "1"
    }
    c.redis.Set(ctx, muteKey, muteVal, 10*time.Minute)

    // ==================== 5. 构建同步信号 ====================
    signalPayload, _ := json.Marshal(map[string]interface{}{
        "conversation_id": event.ConversationId,
        "is_muted":        event.IsMuted,
        "update_time":     event.UpdateTime,
    })

    signal := &push_pb.SyncNotifySignal{
        Type:      "conversation.mute.changed",
        Payload:   signalPayload,
        Timestamp: event.UpdateTime,
    }

    // ==================== 6. 向用户其他设备推送免打扰状态变更信号 ====================
    _, err := c.pushService.PushToUserOtherDevices(ctx, &push_pb.PushToUserOtherDevicesRequest{
        UserId:          event.UserId,
        ExcludeDeviceId: event.DeviceId,
        Signal:          signal,
    })
    if err != nil {
        log.Warn("推送免打扰同步失败（不阻塞）",
            "user_id", event.UserId, "conversation_id", event.ConversationId, "err", err)
    }

    log.Debug("免打扰同步推送完成",
        "user_id", event.UserId, "conversation_id", event.ConversationId, "is_muted", event.IsMuted)
    return nil
}
```

### Consumer: `conversation.unread.reset`（多端同步 — 未读清零 + iOS 角标）

> 来源：Conversation 服务。用户在某设备上清除会话未读数后触发。  
> 职责：1）向用户的其他设备推送未读清零信号；2）更新 iOS 角标计数。

```go
func (c *PushConsumer) HandleConversationUnreadReset(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event struct {
        Header         *common.EventHeader `protobuf:"bytes,1"`
        UserId         string // 操作用户
        DeviceId       string // 来源设备（排除此设备）
        ConversationId string // 会话 ID
        ResetUnread    int64  // 被清零的未读数量
        TotalUnread    int64  // 清零后全局总未读数
        UpdateTime     int64  // 操作时间戳
    }
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 conversation.unread.reset 失败", "err", err)
        return nil
    }

    // ==================== 2. 幂等去重 ====================
    dedupKey := fmt.Sprintf("push:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    // ==================== 3. 参数校验 ====================
    if event.UserId == "" || event.ConversationId == "" {
        log.Error("conversation.unread.reset: 关键字段缺失",
            "user_id", event.UserId, "conversation_id", event.ConversationId)
        return nil
    }

    // ==================== 4. 构建同步信号 ====================
    signalPayload, _ := json.Marshal(map[string]interface{}{
        "conversation_id": event.ConversationId,
        "reset_unread":    event.ResetUnread,
        "total_unread":    event.TotalUnread,
        "update_time":     event.UpdateTime,
    })

    signal := &push_pb.SyncNotifySignal{
        Type:      "conversation.unread.reset",
        Payload:   signalPayload,
        Timestamp: event.UpdateTime,
    }

    // ==================== 5. 向用户其他设备推送未读清零信号 ====================
    _, err := c.pushService.PushToUserOtherDevices(ctx, &push_pb.PushToUserOtherDevicesRequest{
        UserId:          event.UserId,
        ExcludeDeviceId: event.DeviceId,
        Signal:          signal,
    })
    if err != nil {
        log.Warn("推送未读清零同步失败（不阻塞）",
            "user_id", event.UserId, "conversation_id", event.ConversationId, "err", err)
    }

    // ==================== 6. 更新 iOS 角标 ====================
    // 未读清零意味着角标需要相应减少
    devices, err := c.getUserPushDevices(ctx, event.UserId)
    if err != nil {
        log.Warn("获取用户推送设备失败", "user_id", event.UserId, "err", err)
        return nil
    }

    for _, device := range devices {
        if device.Platform == int(common.PLATFORM_TYPE_IOS) ||
            device.Platform == int(common.PLATFORM_TYPE_IPAD) {
            // 直接将角标设置为当前全局总未读数
            badgeKey := fmt.Sprintf("push:badge:%s:%s", event.UserId, device.DeviceID)
            c.redis.Set(ctx, badgeKey, event.TotalUnread, 0)

            // 推送静默通知刷新 iOS 角标
            if device.PushToken != "" && device.IsEnabled {
                triggerEvent := &kafka_push.PushOfflineTriggerEvent{
                    Header:      buildEventHeader("push", c.instanceID),
                    UserId:      event.UserId,
                    PushChannel: device.PushChannel,
                    PushToken:   device.PushToken,
                    DeviceId:    device.DeviceID,
                    Platform:    common.PlatformType(device.Platform),
                    Title:       "",
                    Content:     "",
                    Badge:       fmt.Sprintf("%d", event.TotalUnread),
                    Sound:       "",
                    IsSilent:    true, // 静默推送，仅更新角标
                    ExpireTime:  time.Now().Add(1 * time.Hour).UnixMilli(),
                }
                c.kafka.Produce(ctx, "push.offline.trigger", event.UserId, triggerEvent)
            }
        }
    }

    log.Debug("未读清零同步推送完成",
        "user_id", event.UserId, "conversation_id", event.ConversationId,
        "total_unread", event.TotalUnread)
    return nil
}
```

### Consumer: `notification.read`（角标更新）

> 来源：Notification 服务。用户标记通知已读后触发。  
> 职责：更新 iOS 角标计数（通知已读后全局未读数减少）。

```go
func (c *PushConsumer) HandleNotificationRead(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event struct {
        Header         *common.EventHeader `protobuf:"bytes,1"`
        UserId         string   // 操作用户
        NotificationIds []string // 已读的通知 ID 列表
        ReadCount      int32    // 本次标记已读的数量
        TotalUnread    int64    // 标记后剩余未读总数（消息+通知）
        UpdateTime     int64    // 操作时间戳
    }
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 notification.read 失败", "err", err)
        return nil
    }

    // ==================== 2. 幂等去重 ====================
    dedupKey := fmt.Sprintf("push:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    // ==================== 3. 参数校验 ====================
    if event.UserId == "" {
        log.Error("notification.read: user_id 为空")
        return nil
    }

    // ==================== 4. 更新 iOS 角标 ====================
    devices, err := c.getUserPushDevices(ctx, event.UserId)
    if err != nil {
        log.Warn("获取用户推送设备失败", "user_id", event.UserId, "err", err)
        return nil
    }

    for _, device := range devices {
        if device.Platform == int(common.PLATFORM_TYPE_IOS) ||
            device.Platform == int(common.PLATFORM_TYPE_IPAD) {
            badgeKey := fmt.Sprintf("push:badge:%s:%s", event.UserId, device.DeviceID)
            c.redis.Set(ctx, badgeKey, event.TotalUnread, 0)

            // 推送静默通知刷新 iOS 角标
            if device.PushToken != "" && device.IsEnabled {
                triggerEvent := &kafka_push.PushOfflineTriggerEvent{
                    Header:      buildEventHeader("push", c.instanceID),
                    UserId:      event.UserId,
                    PushChannel: device.PushChannel,
                    PushToken:   device.PushToken,
                    DeviceId:    device.DeviceID,
                    Platform:    common.PlatformType(device.Platform),
                    Badge:       fmt.Sprintf("%d", event.TotalUnread),
                    IsSilent:    true,
                    ExpireTime:  time.Now().Add(1 * time.Hour).UnixMilli(),
                }
                c.kafka.Produce(ctx, "push.offline.trigger", event.UserId, triggerEvent)
            }
        }
    }

    log.Debug("通知已读角标更新完成",
        "user_id", event.UserId, "read_count", event.ReadCount, "total_unread", event.TotalUnread)
    return nil
}
```

### Consumer: `notification.read.all`（角标更新）

> 来源：Notification 服务。用户点击「全部已读」后触发。  
> 职责：将 iOS 角标中通知部分清零，只保留消息未读数。

```go
func (c *PushConsumer) HandleNotificationReadAll(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event struct {
        Header      *common.EventHeader `protobuf:"bytes,1"`
        UserId      string // 操作用户
        ReadCount   int32  // 本次标记已读的数量
        TotalUnread int64  // 标记后剩余未读总数（仅消息未读）
        UpdateTime  int64  // 操作时间戳
    }
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 notification.read.all 失败", "err", err)
        return nil
    }

    // ==================== 2. 幂等去重 ====================
    dedupKey := fmt.Sprintf("push:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    // ==================== 3. 参数校验 ====================
    if event.UserId == "" {
        log.Error("notification.read.all: user_id 为空")
        return nil
    }

    // ==================== 4. 更新 iOS 角标 ====================
    devices, err := c.getUserPushDevices(ctx, event.UserId)
    if err != nil {
        log.Warn("获取用户推送设备失败", "user_id", event.UserId, "err", err)
        return nil
    }

    for _, device := range devices {
        if device.Platform == int(common.PLATFORM_TYPE_IOS) ||
            device.Platform == int(common.PLATFORM_TYPE_IPAD) {
            badgeKey := fmt.Sprintf("push:badge:%s:%s", event.UserId, device.DeviceID)
            // 通知全部已读后，角标 = 消息未读数（通知未读已清零）
            c.redis.Set(ctx, badgeKey, event.TotalUnread, 0)

            // 推送静默通知刷新 iOS 角标
            if device.PushToken != "" && device.IsEnabled {
                triggerEvent := &kafka_push.PushOfflineTriggerEvent{
                    Header:      buildEventHeader("push", c.instanceID),
                    UserId:      event.UserId,
                    PushChannel: device.PushChannel,
                    PushToken:   device.PushToken,
                    DeviceId:    device.DeviceID,
                    Platform:    common.PlatformType(device.Platform),
                    Badge:       fmt.Sprintf("%d", event.TotalUnread),
                    IsSilent:    true,
                    ExpireTime:  time.Now().Add(1 * time.Hour).UnixMilli(),
                }
                c.kafka.Produce(ctx, "push.offline.trigger", event.UserId, triggerEvent)
            }
        }
    }

    log.Debug("通知全部已读角标更新完成",
        "user_id", event.UserId, "read_count", event.ReadCount, "total_unread", event.TotalUnread)
    return nil
}
```

### Consumer: `notification.deleted`（角标更新）

> 来源：Notification 服务。用户删除通知后触发。  
> 职责：若被删除的通知中含未读通知，需更新 iOS 角标。

```go
func (c *PushConsumer) HandleNotificationDeleted(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event struct {
        Header         *common.EventHeader `protobuf:"bytes,1"`
        UserId         string   // 操作用户
        NotificationIds []string // 被删除的通知 ID 列表
        UnreadDelta    int32    // 删除操作导致的未读数减少量（仅计未读通知）
        TotalUnread    int64    // 删除后剩余未读总数
        DeleteTime     int64    // 操作时间戳
    }
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 notification.deleted 失败", "err", err)
        return nil
    }

    // ==================== 2. 幂等去重 ====================
    dedupKey := fmt.Sprintf("push:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    // ==================== 3. 参数校验 ====================
    if event.UserId == "" {
        log.Error("notification.deleted: user_id 为空")
        return nil
    }

    // ==================== 4. 仅当删除的通知包含未读时才更新角标 ====================
    if event.UnreadDelta <= 0 {
        log.Debug("删除的通知均已读，无需更新角标", "user_id", event.UserId)
        return nil
    }

    // ==================== 5. 更新 iOS 角标 ====================
    devices, err := c.getUserPushDevices(ctx, event.UserId)
    if err != nil {
        log.Warn("获取用户推送设备失败", "user_id", event.UserId, "err", err)
        return nil
    }

    for _, device := range devices {
        if device.Platform == int(common.PLATFORM_TYPE_IOS) ||
            device.Platform == int(common.PLATFORM_TYPE_IPAD) {
            badgeKey := fmt.Sprintf("push:badge:%s:%s", event.UserId, device.DeviceID)
            c.redis.Set(ctx, badgeKey, event.TotalUnread, 0)

            // 推送静默通知刷新 iOS 角标
            if device.PushToken != "" && device.IsEnabled {
                triggerEvent := &kafka_push.PushOfflineTriggerEvent{
                    Header:      buildEventHeader("push", c.instanceID),
                    UserId:      event.UserId,
                    PushChannel: device.PushChannel,
                    PushToken:   device.PushToken,
                    DeviceId:    device.DeviceID,
                    Platform:    common.PlatformType(device.Platform),
                    Badge:       fmt.Sprintf("%d", event.TotalUnread),
                    IsSilent:    true,
                    ExpireTime:  time.Now().Add(1 * time.Hour).UnixMilli(),
                }
                c.kafka.Produce(ctx, "push.offline.trigger", event.UserId, triggerEvent)
            }
        }
    }

    log.Debug("通知删除角标更新完成",
        "user_id", event.UserId, "unread_delta", event.UnreadDelta, "total_unread", event.TotalUnread)
    return nil
}
```

### Consumer: `relation.remark.updated`（多端同步 — 好友备注）

> 来源：Relation 服务。用户修改好友备注后触发。  
> 职责：向用户的其他设备推送备注变更信号，实现多端同步。  
> **注意：** 好友备注是用户私有数据，不影响公共索引，仅需多端同步。

```go
func (c *PushConsumer) HandleRelationRemarkUpdated(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event struct {
        Header     *common.EventHeader `protobuf:"bytes,1"`
        UserId     string // 操作用户（修改备注的人）
        DeviceId   string // 来源设备（排除此设备）
        FriendId   string // 被修改备注的好友
        NewRemark  string // 新备注名
        UpdateTime int64  // 操作时间戳
    }
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 relation.remark.updated 失败", "err", err)
        return nil
    }

    // ==================== 2. 幂等去重 ====================
    dedupKey := fmt.Sprintf("push:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    // ==================== 3. 参数校验 ====================
    if event.UserId == "" || event.FriendId == "" {
        log.Error("relation.remark.updated: 关键字段缺失",
            "user_id", event.UserId, "friend_id", event.FriendId)
        return nil
    }

    // ==================== 4. 构建同步信号 ====================
    signalPayload, _ := json.Marshal(map[string]interface{}{
        "friend_id":   event.FriendId,
        "new_remark":  event.NewRemark,
        "update_time": event.UpdateTime,
    })

    signal := &push_pb.SyncNotifySignal{
        Type:      "relation.remark.updated",
        Payload:   signalPayload,
        Timestamp: event.UpdateTime,
    }

    // ==================== 5. 向用户其他设备推送备注变更信号 ====================
    _, err := c.pushService.PushToUserOtherDevices(ctx, &push_pb.PushToUserOtherDevicesRequest{
        UserId:          event.UserId,
        ExcludeDeviceId: event.DeviceId,
        Signal:          signal,
    })
    if err != nil {
        log.Warn("推送好友备注同步失败（不阻塞）",
            "user_id", event.UserId, "friend_id", event.FriendId, "err", err)
    }

    log.Debug("好友备注同步推送完成",
        "user_id", event.UserId, "friend_id", event.FriendId, "new_remark", event.NewRemark)
    return nil
}
```

### Consumer: `msg.forwarded`（多端同步 — 消息转发）

> 来源：Message 服务。消息转发完成后触发。  
> 职责：通知转发目标会话相关用户（新消息到达），以及转发者的其他设备（多端同步）。  
> 注意：实际的消息投递已通过 msg.stored.* 事件处理，此事件仅用于转发特有的多端同步场景。

```go
func (c *PushConsumer) HandleMsgForwarded(ctx context.Context, msg *kafka.Message) error {
    var event kafka_push.MsgForwardedForPush
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        return nil
    }

    dedupKey := fmt.Sprintf("push:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    // 向转发者的其他设备推送同步信号（多端同步）
    signal := &push_pb.SyncNotifySignal{
        Type:      "msg_forwarded",
        Payload:   mustMarshal(event),
        Timestamp: event.ForwardTime,
    }
    c.pushService.PushToUserOtherDevices(ctx, &push_pb.PushToUserOtherDevicesRequest{
        UserId: event.SenderId,
        Signal: signal,
    })

    log.Debug("消息转发同步推送完成", "sender_id", event.SenderId, "targets", len(event.Targets))
    return nil
}
```

### Consumer: `msg.deleted`（多端同步 — 消息删除）

> 来源：Message 服务。用户删除消息后触发。  
> 职责：向用户的其他设备推送消息删除同步信号，确保多端一致。

```go
func (c *PushConsumer) HandleMsgDeleted(ctx context.Context, msg *kafka.Message) error {
    var event kafka_push.MsgDeletedForPush
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        return nil
    }

    dedupKey := fmt.Sprintf("push:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    // 向删除操作者的其他设备推送同步信号
    signal := &push_pb.SyncNotifySignal{
        Type:      "msg_deleted",
        Payload:   mustMarshal(event),
        Timestamp: event.DeleteTime,
    }
    c.pushService.PushToUserOtherDevices(ctx, &push_pb.PushToUserOtherDevicesRequest{
        UserId: event.UserId,
        Signal: signal,
    })

    log.Debug("消息删除多端同步完成", "user_id", event.UserId, "msg_ids", event.MsgIds)
    return nil
}
```

### Consumer: `conversation.updated`（多端同步 — 会话更新）

> 来源：Conversation 服务。会话最后一条消息或属性变更后触发。  
> 职责：向用户其他设备推送会话更新同步信号。

```go
func (c *PushConsumer) HandleConversationUpdated(ctx context.Context, msg *kafka.Message) error {
    var event kafka_push.ConversationUpdatedForPush
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        return nil
    }

    signal := &push_pb.SyncNotifySignal{
        Type:      "conversation_updated",
        Payload:   mustMarshal(event),
        Timestamp: event.UpdateTime,
    }
    c.pushService.PushToUserOtherDevices(ctx, &push_pb.PushToUserOtherDevicesRequest{
        UserId: event.UserId,
        Signal: signal,
    })

    return nil
}
```

### Consumer: `conversation.created`（多端同步 — 新会话）

> 来源：Conversation 服务。首次收到某会话消息时触发。  
> 职责：向用户其他设备推送新会话同步信号。

```go
func (c *PushConsumer) HandleConversationCreated(ctx context.Context, msg *kafka.Message) error {
    var event kafka_push.ConversationCreatedForPush
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        return nil
    }

    signal := &push_pb.SyncNotifySignal{
        Type:      "conversation_created",
        Payload:   mustMarshal(event),
        Timestamp: event.CreateTime,
    }
    c.pushService.PushToUserOtherDevices(ctx, &push_pb.PushToUserOtherDevicesRequest{
        UserId: event.UserId,
        Signal: signal,
    })

    return nil
}
```

### Consumer: `conversation.deleted`（多端同步 — 会话删除）

> 来源：Conversation 服务。用户删除会话后触发。  
> 职责：向用户其他设备推送会话删除同步信号。

```go
func (c *PushConsumer) HandleConversationDeleted(ctx context.Context, msg *kafka.Message) error {
    var event kafka_push.ConversationDeletedForPush
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        return nil
    }

    signal := &push_pb.SyncNotifySignal{
        Type:      "conversation_deleted",
        Payload:   mustMarshal(event),
        Timestamp: event.DeleteTime,
    }
    c.pushService.PushToUserOtherDevices(ctx, &push_pb.PushToUserOtherDevicesRequest{
        UserId: event.UserId,
        Signal: signal,
    })

    return nil
}
```

### Consumer: `system.broadcast`（系统广播推送）

> 来源：Config 服务。系统广播事件。  
> 职责：向所有在线用户推送系统广播信号。

```go
func (c *PushConsumer) HandleSystemBroadcast(ctx context.Context, msg *kafka.Message) error {
    var event kafka_push.SystemBroadcastForPush
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        return nil
    }

    dedupKey := fmt.Sprintf("push:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    signal := &push_pb.SyncNotifySignal{
        Type:      "system_broadcast",
        Payload:   mustMarshal(event),
        Timestamp: time.Now().UnixMilli(),
    }

    // 使用 BroadcastPush 向所有在线用户推送
    c.pushService.BroadcastPush(ctx, &push_pb.BroadcastPushRequest{
        Signal:    signal,
        Platforms: event.TargetPlatforms,
    })

    log.Info("系统广播推送完成", "broadcast_id", event.BroadcastId, "title", event.Title)
    return nil
}
```

### Consumer: `offline.dequeue.batch`（离线信号批量推送）

> 来源：OfflineQueue 服务。用户上线后离线队列批量出队。  
> 职责：将离线期间积攒的同步信号批量推送给刚上线的用户，  
> 使客户端知道哪些会话有新消息，再调用 PullMessages 拉取。  
> 这是 notify+sync 模式下离线场景的关键闭环。

```go
func (c *PushConsumer) HandleOfflineDequeueBatch(ctx context.Context, msg *kafka.Message) error {
    var event kafka_push.OfflineDequeueBatchForPush
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        return nil
    }

    dedupKey := fmt.Sprintf("push:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    if len(event.Signals) == 0 {
        return nil
    }

    // 将每个离线同步信号作为一个 sync notify 推送给用户
    // 合并为一个批量信号，减少推送次数
    batchPayload, _ := proto.Marshal(&event)
    signal := &push_pb.SyncNotifySignal{
        Type:      "offline_sync_batch",
        Payload:   batchPayload,
        Timestamp: event.DequeueTime,
    }

    resp, err := c.pushService.PushToUser(ctx, &push_pb.PushToUserRequest{
        UserId: event.UserId,
        Signal: signal,
    })
    if err != nil {
        log.Error("离线信号批量推送失败", "user_id", event.UserId, "count", len(event.Signals), "err", err)
        return fmt.Errorf("离线信号批量推送失败: %w", err)
    }

    if !resp.Online {
        // 用户已再次离线（极端情况），信号会被重新入队
        log.Warn("离线信号推送时用户已离线", "user_id", event.UserId, "count", len(event.Signals))
    }

    log.Info("离线信号批量推送完成",
        "user_id", event.UserId, "signal_count", len(event.Signals), "pushed", resp.Pushed)
    return nil
}
```

```go
// PushConsumer Push 服务 Kafka 消费者
type PushConsumer struct {
    db              *sql.DB
    redis           *redis.Client
    kafka           *kafka.Producer
    pushService     push_pb.PushServiceServer          // Push RPC 服务（本地调用）
    connecteClient  connecte_pb.ConnecteServiceClient   // 长连接网关
    presenceClient  presence_pb.PresenceServiceClient   // 在线状态
    userClient      user_pb.UserServiceClient           // 用户
    apnsClient      *apns.Client                        // APNs 推送
    fcmClient       *fcm.Client                         // FCM 推送
    huaweiClient    *huawei.Client                      // 华为推送
    xiaomiClient    *xiaomi.Client                      // 小米推送
    oppoClient      *oppo.Client                        // OPPO 推送
    vivoClient      *vivo.Client                        // vivo 推送
    config          *sync.Map                           // 热更新配置
    instanceID      string
}

// NewPushConsumer 创建 Push 消费者实例
func NewPushConsumer(
    db *sql.DB,
    redis *redis.Client,
    kafkaProducer *kafka.Producer,
    pushService push_pb.PushServiceServer,
    connecteClient connecte_pb.ConnecteServiceClient,
    presenceClient presence_pb.PresenceServiceClient,
    userClient user_pb.UserServiceClient,
    pushConfig *PushConfig,
) *PushConsumer {
    apnsClient, _ := apns.NewClient(pushConfig.APNsKeyPath, pushConfig.APNsKeyID, pushConfig.APNsTeamID)
    fcmClient, _ := fcm.NewClient(pushConfig.FCMCredentialPath)
    huaweiClient, _ := huawei.NewClient(pushConfig.HuaweiAppID, pushConfig.HuaweiAppSecret)
    xiaomiClient, _ := xiaomi.NewClient(pushConfig.XiaomiAppSecret, pushConfig.XiaomiPackageName)
    oppoClient, _ := oppo.NewClient(pushConfig.OppoAppKey, pushConfig.OppoMasterSecret)
    vivoClient, _ := vivo.NewClient(pushConfig.VivoAppID, pushConfig.VivoAppKey, pushConfig.VivoAppSecret)

    c := &PushConsumer{
        db:             db,
        redis:          redis,
        kafka:          kafkaProducer,
        pushService:    pushService,
        connecteClient: connecteClient,
        presenceClient: presenceClient,
        userClient:     userClient,
        apnsClient:     apnsClient,
        fcmClient:      fcmClient,
        huaweiClient:   huaweiClient,
        xiaomiClient:   xiaomiClient,
        oppoClient:     oppoClient,
        vivoClient:     vivoClient,
        config:         &sync.Map{},
        instanceID:     os.Getenv("INSTANCE_ID"),
    }

    // 加载默认配置
    c.config.Store("RateLimitPerMinute", 100)
    c.config.Store("LargeGroupThreshold", 200)
    c.config.Store("OfflineDedupTTLMinutes", 5)
    c.config.Store("APNsEnabled", true)
    c.config.Store("FCMEnabled", true)
    c.config.Store("HuaweiEnabled", true)
    c.config.Store("XiaomiEnabled", true)
    c.config.Store("MaxBatchPushUsers", 10000)
    c.config.Store("GoroutinePoolSize", 50)

    return c
}

// RegisterConsumers 注册所有消费者到 Kafka Consumer Group
func (c *PushConsumer) RegisterConsumers(router *kafka.ConsumerRouter) {
    // ============== 消息投递（最高优先级） ==============
    // 单聊消息存储完成 → 投递给接收方
    router.Handle("msg.stored.single", c.HandleMsgStoredSingle,
        kafka.WithRetry(3),
        kafka.WithRetryBackoff(500*time.Millisecond),
        kafka.WithConcurrency(16), // 最高并发：单聊投递是最频繁的操作
    )

    // 群聊消息存储完成 → 扇出到所有群成员
    router.Handle("msg.stored.group", c.HandleMsgStoredGroup,
        kafka.WithRetry(3),
        kafka.WithRetryBackoff(1*time.Second),
        kafka.WithConcurrency(8), // 群消息扇出计算密集
    )

    // 消息撤回 → 通知相关用户
    router.Handle("msg.recalled", c.HandleMsgRecalled,
        kafka.WithRetry(3),
        kafka.WithRetryBackoff(1*time.Second),
        kafka.WithConcurrency(4),
    )

    // ============== 回执与确认 ==============
    // 已读回执 → 通知消息发送者
    router.Handle("ack.read.updated", c.HandleAckReadUpdated,
        kafka.WithRetry(2),
        kafka.WithRetryBackoff(500*time.Millisecond),
        kafka.WithConcurrency(8),
    )

    // 群已读回执汇总 → 通知消息发送者
    router.Handle("ack.read.group", c.HandleAckReadGroup,
        kafka.WithRetry(2),
        kafka.WithRetryBackoff(500*time.Millisecond),
        kafka.WithConcurrency(4),
    )

    // 投递确认 → 通知消息发送者
    router.Handle("ack.delivered", c.HandleAckDelivered,
        kafka.WithRetry(2),
        kafka.WithRetryBackoff(500*time.Millisecond),
        kafka.WithConcurrency(4),
    )

    // ============== 通知推送 ==============
    // 通知创建 → 推送给目标用户
    router.Handle("notification.created", c.HandleNotificationCreated,
        kafka.WithRetry(3),
        kafka.WithRetryBackoff(1*time.Second),
        kafka.WithConcurrency(4),
    )

    // ============== 在线状态 ==============
    // 在线状态变更 → 推送给订阅者
    router.Handle("presence.status.notify", c.HandlePresenceStatusNotify,
        kafka.WithRetry(1),
        kafka.WithRetryBackoff(500*time.Millisecond),
        kafka.WithConcurrency(4),
    )

    // 正在输入 → 推送给对方（允许丢失，不重试）
    router.Handle("presence.typing", c.HandlePresenceTyping,
        kafka.WithRetry(0), // 不重试
        kafka.WithConcurrency(8), // 高并发：typing 事件非常频繁
    )

    // ============== 好友关系 ==============
    // 好友申请 → 推送给目标用户
    router.Handle("relation.friend.request", c.HandleRelationFriendRequest,
        kafka.WithRetry(3),
        kafka.WithRetryBackoff(1*time.Second),
        kafka.WithConcurrency(2),
    )

    // 好友通过 → 推送给申请者
    router.Handle("relation.friend.accepted", c.HandleRelationFriendAccepted,
        kafka.WithRetry(3),
        kafka.WithRetryBackoff(1*time.Second),
        kafka.WithConcurrency(2),
    )

    // ============== 群组事件 ==============
    // 成员入群 → 通知群成员
    router.Handle("group.member.joined", c.HandleGroupMemberJoined,
        kafka.WithRetry(2),
        kafka.WithRetryBackoff(1*time.Second),
        kafka.WithConcurrency(2),
    )

    // 成员被踢 → 通知被踢者 + 群成员
    router.Handle("group.member.kicked", c.HandleGroupMemberKicked,
        kafka.WithRetry(2),
        kafka.WithRetryBackoff(1*time.Second),
        kafka.WithConcurrency(2),
    )

    // 群解散 → 通知所有成员
    router.Handle("group.dissolved", c.HandleGroupDissolved,
        kafka.WithRetry(3),
        kafka.WithRetryBackoff(2*time.Second),
        kafka.WithConcurrency(2),
    )

    // 群信息更新 → 通知群成员
    router.Handle("group.info.updated", c.HandleGroupInfoUpdated,
        kafka.WithRetry(2),
        kafka.WithRetryBackoff(1*time.Second),
        kafka.WithConcurrency(2),
    )

    // ============== 离线推送（自产自消） ==============
    // 离线推送任务 → 实际调用 APNs/FCM SDK
    router.Handle("push.offline.trigger", c.HandlePushOfflineTrigger,
        kafka.WithRetry(5),              // 最多重试 5 次
        kafka.WithRetryBackoff(2*time.Second), // 退避 2s
        kafka.WithConcurrency(8),        // 离线推送涉及外部 HTTP 调用，并发度适中
    )

    // ============== 配置变更 ==============
    router.Handle("config.changed", c.HandleConfigChanged,
        kafka.WithRetry(1),
        kafka.WithConcurrency(1), // 单线程处理配置变更，避免并发冲突
    )

    // ============== 本地缓存维护（Local Cache） ==============
    // ⚠ 注意：在线状态不做本地缓存（变化频率不满足「变化不频繁」条件）
    //   PushToUser 直接复用 Connecte.GetUserConnections（推送流程本身必须调用，零额外开销）
    //   PushToGroup 通过 Presence.BatchGetPresence RPC 直接查询源服务

    // 成员主动退群 → 清理本地群成员缓存 + 推送退群通知
    router.Handle("group.member.left", c.HandleGroupMemberLeft,
        kafka.WithRetry(2),
        kafka.WithRetryBackoff(1*time.Second),
        kafka.WithConcurrency(2),
    )

    // 用户设置变更 → 更新本地用户设置缓存
    router.Handle("user.settings.updated", c.HandleUserSettingsUpdated,
        kafka.WithRetry(2),
        kafka.WithRetryBackoff(1*time.Second),
        kafka.WithConcurrency(2),
    )

    // 用户资料变更 → 更新本地用户信息缓存
    router.Handle("user.profile.updated", c.HandleUserProfileUpdatedCache,
        kafka.WithRetry(2),
        kafka.WithRetryBackoff(1*time.Second),
        kafka.WithConcurrency(2),
    )

    // ============== 闭环补充（Self-Closing Loop） ==============
    // 入群申请 → 推送给群管理员
    router.Handle("group.application", c.HandleGroupApplication,
        kafka.WithRetry(3),
        kafka.WithRetryBackoff(1*time.Second),
        kafka.WithConcurrency(2),
    )

    // 入群申请处理结果 → 推送给申请者
    router.Handle("group.application.handled", c.HandleGroupApplicationHandled,
        kafka.WithRetry(3),
        kafka.WithRetryBackoff(1*time.Second),
        kafka.WithConcurrency(2),
    )

    // 成员角色变更 → 推送给被变更成员
    router.Handle("group.member.role.changed", c.HandleGroupMemberRoleChanged,
        kafka.WithRetry(2),
        kafka.WithRetryBackoff(1*time.Second),
        kafka.WithConcurrency(2),
    )

    // 群禁言变更 → 推送给所有群成员
    router.Handle("group.mute.changed", c.HandleGroupMuteChanged,
        kafka.WithRetry(2),
        kafka.WithRetryBackoff(1*time.Second),
        kafka.WithConcurrency(2),
    )

    // 未读数变更 → 推送 iOS 角标更新
    router.Handle("conversation.unread.changed", c.HandleConversationUnreadChanged,
        kafka.WithRetry(2),
        kafka.WithRetryBackoff(1*time.Second),
        kafka.WithConcurrency(4),
    )

    // 拉黑事件 → 缓存维护
    router.Handle("relation.blocked", c.HandleRelationBlocked,
        kafka.WithRetry(1),
        kafka.WithRetryBackoff(500*time.Millisecond),
        kafka.WithConcurrency(2),
    )

    // ============== 多端同步（会话属性变更） ==============
    // 置顶变更 → 向用户其他设备推送同步信号
    router.Handle("conversation.top.changed", c.HandleConversationTopChanged,
        kafka.WithRetry(2),
        kafka.WithRetryBackoff(1*time.Second),
        kafka.WithConcurrency(2),
    )

    // 免打扰变更 → 向用户其他设备推送同步信号 + 更新本地缓存
    router.Handle("conversation.mute.changed", c.HandleConversationMuteChanged,
        kafka.WithRetry(2),
        kafka.WithRetryBackoff(1*time.Second),
        kafka.WithConcurrency(2),
    )

    // 未读清零 → 向用户其他设备推送同步信号 + 更新 iOS 角标
    router.Handle("conversation.unread.reset", c.HandleConversationUnreadReset,
        kafka.WithRetry(2),
        kafka.WithRetryBackoff(1*time.Second),
        kafka.WithConcurrency(4),
    )

    // ============== 通知角标更新 ==============
    // 通知已读 → 更新 iOS 角标
    router.Handle("notification.read", c.HandleNotificationRead,
        kafka.WithRetry(2),
        kafka.WithRetryBackoff(1*time.Second),
        kafka.WithConcurrency(2),
    )

    // 通知全部已读 → 更新 iOS 角标
    router.Handle("notification.read.all", c.HandleNotificationReadAll,
        kafka.WithRetry(2),
        kafka.WithRetryBackoff(1*time.Second),
        kafka.WithConcurrency(2),
    )

    // 通知删除 → 更新 iOS 角标（仅当含未读通知时）
    router.Handle("notification.deleted", c.HandleNotificationDeleted,
        kafka.WithRetry(2),
        kafka.WithRetryBackoff(1*time.Second),
        kafka.WithConcurrency(2),
    )

    // ============== 好友备注多端同步 ==============
    // 好友备注变更 → 向用户其他设备推送同步信号
    router.Handle("relation.remark.updated", c.HandleRelationRemarkUpdated,
        kafka.WithRetry(2),
        kafka.WithRetryBackoff(1*time.Second),
        kafka.WithConcurrency(2),
    )

    // ============== 消息转发/删除多端同步 ==============
    // 消息转发 → 转发者多端同步
    router.Handle("msg.forwarded", c.HandleMsgForwarded,
        kafka.WithRetry(2),
        kafka.WithRetryBackoff(1*time.Second),
        kafka.WithConcurrency(4),
    )

    // 消息删除 → 用户其他设备同步删除
    router.Handle("msg.deleted", c.HandleMsgDeleted,
        kafka.WithRetry(2),
        kafka.WithRetryBackoff(1*time.Second),
        kafka.WithConcurrency(4),
    )

    // ============== 会话生命周期多端同步 ==============
    // 会话属性更新 → 用户其他设备同步
    router.Handle("conversation.updated", c.HandleConversationUpdated,
        kafka.WithRetry(2),
        kafka.WithRetryBackoff(1*time.Second),
        kafka.WithConcurrency(2),
    )

    // 新会话创建 → 用户其他设备同步
    router.Handle("conversation.created", c.HandleConversationCreated,
        kafka.WithRetry(2),
        kafka.WithRetryBackoff(1*time.Second),
        kafka.WithConcurrency(2),
    )

    // 会话删除 → 用户其他设备同步
    router.Handle("conversation.deleted", c.HandleConversationDeleted,
        kafka.WithRetry(2),
        kafka.WithRetryBackoff(1*time.Second),
        kafka.WithConcurrency(2),
    )

    // ============== 系统广播 ==============
    // 系统广播 → 向所有在线用户推送
    router.Handle("system.broadcast", c.HandleSystemBroadcast,
        kafka.WithRetry(3),
        kafka.WithRetryBackoff(2*time.Second),
        kafka.WithConcurrency(8),
    )

    // ============== 离线信号批量推送 ==============
    // 用户上线后离线队列出队 → 批量推送积攒的同步信号
    router.Handle("offline.dequeue.batch", c.HandleOfflineDequeueBatch,
        kafka.WithRetry(3),
        kafka.WithRetryBackoff(2*time.Second),
        kafka.WithConcurrency(8),
    )
}

// getUserPushDevices 获取用户推送设备列表（Redis 缓存优先）
// 与 PushService.getUserPushDevices 逻辑相同，消费者侧复用
func (c *PushConsumer) getUserPushDevices(ctx context.Context, userID string) ([]pushDevice, error) {
    cacheKey := fmt.Sprintf("push:device:%s", userID)
    cached, err := c.redis.HGetAll(ctx, cacheKey).Result()
    if err == nil && len(cached) > 0 {
        devices := make([]pushDevice, 0, len(cached))
        for deviceID, jsonStr := range cached {
            var d pushDevice
            if json.Unmarshal([]byte(jsonStr), &d) == nil {
                d.DeviceID = deviceID
                devices = append(devices, d)
            }
        }
        return devices, nil
    }

    rows, err := c.db.QueryContext(ctx,
        `SELECT device_id, platform, push_token, push_channel, is_enabled
         FROM push_devices
         WHERE user_id = $1 AND is_enabled = true`,
        userID,
    )
    if err != nil {
        return nil, fmt.Errorf("查询 push_devices 失败: %w", err)
    }
    defer rows.Close()

    devices := make([]pushDevice, 0)
    pipe := c.redis.Pipeline()
    for rows.Next() {
        var d pushDevice
        if err := rows.Scan(&d.DeviceID, &d.Platform, &d.PushToken, &d.PushChannel, &d.IsEnabled); err != nil {
            continue
        }
        devices = append(devices, d)
        jsonBytes, _ := json.Marshal(map[string]interface{}{
            "push_token":   d.PushToken,
            "push_channel": d.PushChannel,
            "platform":     d.Platform,
            "is_enabled":   d.IsEnabled,
        })
        pipe.HSet(ctx, cacheKey, d.DeviceID, string(jsonBytes))
    }
    if len(devices) > 0 {
        pipe.Expire(ctx, cacheKey, 7*24*time.Hour)
        pipe.Exec(ctx)
    }
    return devices, nil
}

// pushDevice 内部推送设备结构
type pushDevice struct {
    DeviceID    string `json:"device_id"`
    Platform    int    `json:"platform"`
    PushToken   string `json:"push_token"`
    PushChannel string `json:"push_channel"`
    IsEnabled   bool   `json:"is_enabled"`
}

// buildEventHeader 构建 Kafka 事件头
func buildEventHeader(source string, instanceID string) *common.EventHeader {
    return &common.EventHeader{
        EventId:   uuid.New().String(),
        TraceId:   "",
        Source:    source,
        SourceId:  instanceID,
        Timestamp: time.Now().UnixMilli(),
        Version:   1,
    }
}

// isTokenInvalidError 判断推送 Token 是否已失效
func isTokenInvalidError(err error) bool {
    if err == nil {
        return false
    }
    errStr := err.Error()
    return strings.Contains(errStr, "BadDeviceToken") ||
        strings.Contains(errStr, "Unregistered") ||
        strings.Contains(errStr, "not-registered") ||
        strings.Contains(errStr, "invalid-registration") ||
        strings.Contains(errStr, "80100003") ||
        strings.Contains(errStr, "InvalidToken")
}

// mapToJSON 将 map 转为 JSON 字符串
func mapToJSON(m map[string]string) string {
    if m == nil {
        return "{}"
    }
    data, _ := json.Marshal(m)
    return string(data)
}
```
