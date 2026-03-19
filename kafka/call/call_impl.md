# Call 音视频通话服务 — Kafka 消费者/生产者实现伪代码

## 概述

Call 服务作为 Kafka 事件的生产者（主要），以及少量事件的消费者。大部分 Kafka 事件在 RPC 层的各个接口中生产。Kafka 消费者主要处理配置变更和用户注销清理。

## 生产 Topic 列表

| Topic | 事件 | 生产时机 | Key | 消费方 |
|-------|------|----------|-----|--------|
| `call.initiated` | CallInitiatedEvent | CreateCall 创建通话 | call_id | Push（推送来电信令） |
| `call.answered` | CallAnsweredEvent | JoinCall 接听/加入 | call_id | Push（通知发起方） |
| `call.ended` | CallEndedEvent | EndCall 或超时结束 | call_id | Push（通知相关用户）、Audit |
| `call.missed` | CallMissedEvent | 60s 超时无人接听 | call_id | Push（推送未接提醒）、Notification |
| `call.cancelled` | CallCancelledEvent | 发起方取消 | call_id | Push（通知接收方） |
| `call.rejected` | CallRejectedEvent | 接收方拒绝 | call_id | Push（通知发起方） |
| `call.participant.joined` | CallParticipantJoinedEvent | 群通话成员加入 | call_id | Push（通知其他参与者） |
| `call.participant.left` | CallParticipantLeftEvent | 群通话成员离开 | call_id | Push（通知其他参与者） |
| `call.media.updated` | CallMediaUpdatedEvent | 静音/关摄像头 | call_id | Push（通知其他参与者） |
| `call.stats` | CallStatsEvent | 通话结束时 | call_id | Audit（通话统计） |

## 消费 Topic 列表

| Topic | 来源 | 用途 | 并发度 | 重试 |
|-------|------|------|--------|------|
| `config.changed` | Config | 热更新通话配置（超时时间、最大参与者等） | 1 | 1 |
| `user.deactivated` | User | 用户注销 → 结束该用户的所有进行中通话 | 2 | 3 |
| `user.profile.updated` | User | 用户资料变更 → 更新本地用户信息缓存 | 2 | 2 |
| `group.member.left` | Group | 群成员退出 → 清理本地群成员缓存 | 2 | 2 |
| `group.member.kicked` | Group | 群成员被踢 → 清理本地群成员缓存 + 强制离开群通话 | 2 | 2 |

## Redis Key 设计（消费者专用）

| Key 格式 | 类型 | TTL | 说明 |
|----------|------|-----|------|
| `call:kafka:dedup:{event_id}` | STRING | 24h | Kafka 消费幂等去重 |

---

## 消费者实现

### Consumer: `config.changed`

> 来源：Config 服务。配置变更通知。
> 职责：热更新通话相关配置参数。

```go
func (c *CallConsumer) HandleConfigChanged(ctx context.Context, msg *kafka.Message) error {
    var event kafka_config.ConfigChangedEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("unmarshal ConfigChangedEvent failed", "err", err)
        return nil
    }

    // 幂等去重
    dedupKey := fmt.Sprintf("call:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    // 更新通话配置
    for _, change := range event.Changes {
        switch change.Key {
        case "call.ringing_timeout_seconds":
            c.config.RingingTimeoutSeconds, _ = strconv.Atoi(change.NewValue)
            log.Info("call config updated: ringing_timeout", "value", change.NewValue)
        case "call.max_group_participants":
            c.config.MaxGroupParticipants, _ = strconv.Atoi(change.NewValue)
            log.Info("call config updated: max_group_participants", "value", change.NewValue)
        case "call.livekit_host":
            c.config.LiveKitHost = change.NewValue
            log.Info("call config updated: livekit_host", "value", change.NewValue)
        }
    }

    return nil
}
```

### Consumer: `user.deactivated`

> 来源：User 服务。用户注销事件。
> 职责：结束该用户所有进行中的通话，清理资源。

```go
func (c *CallConsumer) HandleUserDeactivated(ctx context.Context, msg *kafka.Message) error {
    var event kafka_user.UserDeactivatedEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("unmarshal UserDeactivatedEvent failed", "err", err)
        return nil
    }

    // 幂等去重
    dedupKey := fmt.Sprintf("call:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    userID := event.UserId
    if userID == "" {
        return nil
    }

    // 查询用户是否有活跃通话
    callID, err := c.redis.Get(ctx, fmt.Sprintf("call:user_active:%s", userID)).Result()
    if err != nil || callID == "" {
        return nil // 无活跃通话
    }

    // 获取通话信息
    callInfo, err := c.redis.HGetAll(ctx, fmt.Sprintf("call:active:%s", callID)).Result()
    if err != nil || len(callInfo) == 0 {
        c.redis.Del(ctx, fmt.Sprintf("call:user_active:%s", userID))
        return nil
    }

    // 结束通话
    c.callService.endCallInternal(ctx, callID, callInfo, call.CALL_END_REASON_NORMAL)

    // 清理本地用户缓存
    c.redis.Del(ctx, fmt.Sprintf("call:local:user:%s", userID))

    log.Info("user.deactivated: ended active call",
        "user_id", userID,
        "call_id", callID,
    )

    return nil
}
```

### Consumer: `user.profile.updated`

> 来源：User 服务。用户资料变更。
> 职责：更新本地用户信息缓存。

```go
func (c *CallConsumer) HandleUserProfileUpdated(ctx context.Context, msg *kafka.Message) error {
    var event kafka_user.UserProfileUpdatedEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("unmarshal UserProfileUpdatedEvent failed", "err", err)
        return nil
    }

    dedupKey := fmt.Sprintf("call:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    if event.UserId == "" {
        return nil
    }

    // 延迟双删
    cacheKey := fmt.Sprintf("call:local:user:%s", event.UserId)
    c.redis.Del(ctx, cacheKey)

    // 回填新数据
    c.redis.HSet(ctx, cacheKey, map[string]interface{}{
        "nickname":   event.Nickname,
        "avatar_url": event.AvatarUrl,
    })
    c.redis.Expire(ctx, cacheKey, 1*time.Hour)

    // 延迟双删第二次
    go func() {
        time.Sleep(500 * time.Millisecond)
        c.redis.Del(context.Background(), cacheKey)
    }()

    return nil
}
```

### Consumer: `group.member.left` / `group.member.kicked`

> 来源：Group 服务。群成员退出/被踢。
> 职责：清理本地群成员缓存。被踢场景额外强制该用户离开群通话。

```go
func (c *CallConsumer) HandleGroupMemberLeft(ctx context.Context, msg *kafka.Message) error {
    var event kafka_group.GroupMemberLeftEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("unmarshal GroupMemberLeftEvent failed", "err", err)
        return nil
    }

    dedupKey := fmt.Sprintf("call:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    // 延迟双删群成员缓存
    cacheKey := fmt.Sprintf("call:local:group_members:%s", event.GroupId)
    c.redis.Del(ctx, cacheKey)
    go func() {
        time.Sleep(500 * time.Millisecond)
        c.redis.Del(context.Background(), cacheKey)
    }()

    return nil
}

func (c *CallConsumer) HandleGroupMemberKicked(ctx context.Context, msg *kafka.Message) error {
    var event kafka_group.GroupMemberKickedEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("unmarshal GroupMemberKickedEvent failed", "err", err)
        return nil
    }

    dedupKey := fmt.Sprintf("call:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    // 延迟双删群成员缓存
    cacheKey := fmt.Sprintf("call:local:group_members:%s", event.GroupId)
    c.redis.Del(ctx, cacheKey)
    go func() {
        time.Sleep(500 * time.Millisecond)
        c.redis.Del(context.Background(), cacheKey)
    }()

    // 检查被踢用户是否在群通话中 → 强制离开
    for _, memberID := range event.MemberIds {
        callID, err := c.redis.Get(ctx, fmt.Sprintf("call:user_active:%s", memberID)).Result()
        if err != nil || callID == "" {
            continue
        }
        callInfo, _ := c.redis.HGetAll(ctx, fmt.Sprintf("call:active:%s", callID)).Result()
        if len(callInfo) == 0 {
            continue
        }
        // 确认是同一群的通话
        convType, _ := strconv.Atoi(callInfo["conv_type"])
        if convType == int(common.CONVERSATION_TYPE_GROUP) {
            c.callService.LeaveCall(ctx, &call.LeaveCallRequest{
                UserId: memberID,
                CallId: callID,
            })
            log.Info("group.member.kicked: forced leave call",
                "user_id", memberID,
                "call_id", callID,
                "group_id", event.GroupId,
            )
        }
    }

    return nil
}
```

---

## 可观测性接入

### 接入要点

| 步骤 | 说明 |
|------|------|
| 1 | Kafka Consumer 每条消息创建 span：`tracer.Start(ctx, "call.consumer." + topic)` |
| 2 | 从 EventHeader 提取 trace_id 关联上下文 |
| 3 | 消费处理耗时指标：`call_consumer_process_duration{topic}` |
| 4 | 消费失败计数：`call_consumer_error_total{topic}` |
| 5 | 结构化 JSON 日志，所有日志携带 trace_id 和 event_id |
