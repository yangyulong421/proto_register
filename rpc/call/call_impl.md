# Call 音视频通话服务 — RPC 接口实现伪代码

## 概述

Call 服务负责音视频通话的信令控制，集成 **LiveKit** 作为 SFU（Selective Forwarding Unit）媒体服务器。Call 服务仅管理通话信令和记录，**音视频数据流由客户端直连 LiveKit SFU**，不经过 Call 服务。

**核心设计原则：**
- **信令与媒体分离**：Call 服务 = 信令面（通话生命周期管理）、LiveKit = 数据面（音视频传输）
- **通话记录持久化**：所有通话记录写入 PgSQL，支持历史查询
- **信令推送复用 Push 服务**：通话信令通过 Kafka 事件 → Push 服务 → sync notify 推送
- **超时自动处理**：60 秒无人接听自动标记为未接来电
- **并发安全**：使用 Redis 分布式锁保证通话状态变更的原子性

## 外部依赖

### PgSQL（1 张表）

| 表名 | 用途 | 预估行数 | 分区策略 |
|------|------|---------|---------|
| `call_records` | 通话记录 | 日增 50-100 万 | 按 `initiated_at` RANGE 按月分区 |

### Redis（独享 Redis Cluster 或与低频服务共享）

| Key 格式 | 类型 | 值 | TTL | 说明 |
|----------|------|-----|-----|------|
| `call:active:{call_id}` | HASH | 通话状态（caller_id, receiver_id, room_name, status, initiated_at） | 24h | 活跃通话信息 |
| `call:user_active:{user_id}` | STRING | call_id | 24h | 用户当前活跃通话（用于忙线检测） |
| `call:room:{room_name}` | SET | 参与者 user_id 列表 | 24h | 通话房间参与者 |
| `call:lock:{call_id}` | STRING | lock_value | 10s | 分布式锁（状态变更） |
| `call:timeout:{call_id}` | STRING | "1" | 60s | 响铃超时标记（TTL 过期触发未接处理） |
| `call:kafka:dedup:{event_id}` | STRING | "1" | 24h | Kafka 消费幂等去重 |

### RPC 调用（其他微服务）

| 目标服务 | RPC 方法 | 调用场景 | 频率 | 缓存策略 |
|---------|---------|---------|------|---------|
| User.GetUser | 获取用户信息 | CreateCall 获取 caller 昵称/头像 | 每次通话 | 本地缓存 `call:local:user:{user_id}` 1h |
| Conversation.GetOrCreateChannel | 获取会话通道 | CreateCall 关联会话 | 每次通话 | 无需缓存（低频） |
| Connecte.GetUserConnections | 查询用户在线状态 | CreateCall 判断接收方是否在线 | 每次通话 | 不缓存（实时查询） |
| Group.GetGroupMemberIDs | 获取群成员 | 群通话推送信令 | 群通话 | 本地缓存 `call:local:group_members:{group_id}` 30min |

### Kafka

| 方向 | Topic | 说明 |
|------|-------|------|
| 生产 | `call.initiated` | 通话发起 |
| 生产 | `call.answered` | 通话接听 |
| 生产 | `call.ended` | 通话结束 |
| 生产 | `call.missed` | 未接来电 |
| 生产 | `call.cancelled` | 通话取消 |
| 生产 | `call.rejected` | 通话拒绝 |
| 生产 | `call.participant.joined` | 参与者加入 |
| 生产 | `call.participant.left` | 参与者离开 |
| 生产 | `call.media.updated` | 媒体状态变更 |
| 生产 | `call.stats` | 通话统计 |

## PgSQL 表结构

```sql
-- 通话记录表（按月分区）
CREATE TABLE call_records (
    call_id           VARCHAR(64)  PRIMARY KEY,
    call_type         SMALLINT     NOT NULL DEFAULT 1,   -- 1=语音 2=视频
    status            SMALLINT     NOT NULL DEFAULT 1,   -- 1=响铃 2=通话中 3=已结束 4=未接 5=已拒绝 6=已取消 7=忙线 8=失败
    caller_id         VARCHAR(64)  NOT NULL,
    receiver_id       VARCHAR(64)  NOT NULL,
    conversation_type SMALLINT     NOT NULL DEFAULT 1,   -- 1=单聊 2=群聊
    conversation_id   VARCHAR(128) NOT NULL,
    room_name         VARCHAR(128) NOT NULL,
    duration          INT          NOT NULL DEFAULT 0,    -- 通话时长（秒）
    end_reason        SMALLINT     NOT NULL DEFAULT 0,
    participants      JSONB        NOT NULL DEFAULT '[]', -- 参与者列表 JSON
    initiated_at      BIGINT       NOT NULL,
    connected_at      BIGINT       NOT NULL DEFAULT 0,
    ended_at          BIGINT       NOT NULL DEFAULT 0,
    extra             JSONB        DEFAULT '{}',
    created_at        BIGINT       NOT NULL DEFAULT (EXTRACT(EPOCH FROM NOW()) * 1000)::BIGINT
) PARTITION BY RANGE (initiated_at);

-- 索引
CREATE INDEX idx_call_records_caller ON call_records(caller_id, initiated_at DESC);
CREATE INDEX idx_call_records_receiver ON call_records(receiver_id, initiated_at DESC);
CREATE INDEX idx_call_records_conversation ON call_records(conversation_id, initiated_at DESC);
CREATE INDEX idx_call_records_status ON call_records(status) WHERE status IN (1, 2); -- 仅索引活跃通话
```

## Redis Key 设计（本地缓存）

| Key 格式 | 类型 | TTL | 说明 |
|----------|------|-----|------|
| `call:local:user:{user_id}` | HASH | 1h | 用户昵称/头像缓存（Kafka user.profile.updated 同步） |
| `call:local:group_members:{group_id}` | SET | 30min | 群成员 ID 列表缓存（Kafka group.member.* 同步） |

## LiveKit 集成

```go
// LiveKit Client 配置
type LiveKitConfig struct {
    Host      string // LiveKit 服务地址
    APIKey    string // LiveKit API Key
    APISecret string // LiveKit API Secret
}

// 生成 LiveKit Token
func (s *CallService) generateLiveKitToken(userID, roomName string, canPublish bool) (string, error) {
    at := auth.NewAccessToken(s.livekitConfig.APIKey, s.livekitConfig.APISecret)
    grant := &auth.VideoGrant{
        RoomJoin: true,
        Room:     roomName,
    }
    grant.SetCanPublish(canPublish)
    grant.SetCanSubscribe(true)

    at.AddGrant(grant).
        SetIdentity(userID).
        SetValidFor(24 * time.Hour) // Token 有效期 24h

    return at.ToJWT()
}

// 创建 LiveKit Room
func (s *CallService) createLiveKitRoom(ctx context.Context, roomName string, maxParticipants int) error {
    _, err := s.livekitClient.CreateRoom(ctx, &livekit.CreateRoomRequest{
        Name:            roomName,
        MaxParticipants: uint32(maxParticipants),
        EmptyTimeout:    300, // 空房间 5 分钟后自动销毁
    })
    return err
}

// 销毁 LiveKit Room
func (s *CallService) deleteLiveKitRoom(ctx context.Context, roomName string) error {
    _, err := s.livekitClient.DeleteRoom(ctx, &livekit.DeleteRoomRequest{
        Room: roomName,
    })
    return err
}
```

---

## 接口实现

### 1. CreateCall — 发起通话

```go
func (s *CallService) CreateCall(ctx context.Context, req *call.CreateCallRequest) (*call.CreateCallResponse, error) {
    span := trace.SpanFromContext(ctx)
    span.SetAttributes(attribute.String("caller_id", req.CallerId), attribute.String("receiver_id", req.ReceiverId))

    // ==================== 1. 参数校验 ====================
    if req.CallerId == "" || req.ReceiverId == "" {
        return nil, status.Error(codes.InvalidArgument, "caller_id and receiver_id required")
    }
    if req.CallType == call.CALL_TYPE_UNSPECIFIED {
        return nil, status.Error(codes.InvalidArgument, "call_type required")
    }
    if req.CallerId == req.ReceiverId {
        return nil, status.Error(codes.InvalidArgument, "cannot call yourself")
    }

    // ==================== 2. 检查发起方是否已有进行中通话（忙线检测） ====================
    existingCallID, err := s.redis.Get(ctx, fmt.Sprintf("call:user_active:%s", req.CallerId)).Result()
    if err == nil && existingCallID != "" {
        return nil, status.Error(codes.FailedPrecondition, "caller already in a call")
    }

    // ==================== 3. 检查接收方是否忙线 ====================
    if req.ConversationType == common.CONVERSATION_TYPE_SINGLE {
        receiverCallID, err := s.redis.Get(ctx, fmt.Sprintf("call:user_active:%s", req.ReceiverId)).Result()
        if err == nil && receiverCallID != "" {
            return nil, status.Errorf(codes.FailedPrecondition, "receiver is busy")
        }
    }

    // ==================== 4. 获取/创建会话通道 ====================
    channelResp, err := s.conversationClient.GetOrCreateChannel(ctx, &conversation.GetOrCreateChannelRequest{
        Type:      req.ConversationType,
        MemberIds: []string{req.CallerId, req.ReceiverId},
        GroupId:   func() string { if req.ConversationType == common.CONVERSATION_TYPE_GROUP { return req.ReceiverId }; return "" }(),
    })
    if err != nil {
        return nil, status.Errorf(codes.Internal, "get conversation channel failed: %v", err)
    }

    // ==================== 5. 获取 caller 信息（本地缓存） ====================
    callerInfo := s.getUserInfoCached(ctx, req.CallerId)

    // ==================== 6. 生成通话 ID 和 Room 名称 ====================
    callID := snowflake.Generate().String()
    roomName := fmt.Sprintf("call_%s", callID)

    // ==================== 7. 创建 LiveKit Room ====================
    maxParticipants := 2
    if req.ConversationType == common.CONVERSATION_TYPE_GROUP {
        maxParticipants = 50 // 群通话最多 50 人
    }
    if err := s.createLiveKitRoom(ctx, roomName, maxParticipants); err != nil {
        return nil, status.Errorf(codes.Internal, "create livekit room failed: %v", err)
    }

    // ==================== 8. 生成 Caller 的 LiveKit Token ====================
    token, err := s.generateLiveKitToken(req.CallerId, roomName, true)
    if err != nil {
        return nil, status.Errorf(codes.Internal, "generate livekit token failed: %v", err)
    }

    now := time.Now().UnixMilli()

    // ==================== 9. 写入 PgSQL 通话记录 ====================
    _, err = s.db.ExecContext(ctx,
        `INSERT INTO call_records (call_id, call_type, status, caller_id, receiver_id,
         conversation_type, conversation_id, room_name, participants, initiated_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
        callID, req.CallType, call.CALL_STATUS_RINGING, req.CallerId, req.ReceiverId,
        req.ConversationType, channelResp.Channel.ConversationId, roomName,
        `[{"user_id":"` + req.CallerId + `","joined_at":` + fmt.Sprint(now) + `}]`,
        now,
    )
    if err != nil {
        // 回滚：删除 LiveKit Room
        s.deleteLiveKitRoom(ctx, roomName)
        return nil, status.Errorf(codes.Internal, "insert call record failed: %v", err)
    }

    // ==================== 10. 写入 Redis 活跃通话状态 ====================
    pipe := s.redis.Pipeline()
    // 活跃通话信息
    pipe.HSet(ctx, fmt.Sprintf("call:active:%s", callID), map[string]interface{}{
        "caller_id":     req.CallerId,
        "receiver_id":   req.ReceiverId,
        "room_name":     roomName,
        "status":        int(call.CALL_STATUS_RINGING),
        "call_type":     int(req.CallType),
        "conv_type":     int(req.ConversationType),
        "conv_id":       channelResp.Channel.ConversationId,
        "initiated_at":  now,
    })
    pipe.Expire(ctx, fmt.Sprintf("call:active:%s", callID), 24*time.Hour)
    // 用户活跃通话标记
    pipe.Set(ctx, fmt.Sprintf("call:user_active:%s", req.CallerId), callID, 24*time.Hour)
    // 响铃超时标记（60s 后自动过期 → 定时任务检测处理）
    pipe.Set(ctx, fmt.Sprintf("call:timeout:%s", callID), "1", 60*time.Second)
    // 房间参与者
    pipe.SAdd(ctx, fmt.Sprintf("call:room:%s", roomName), req.CallerId)
    pipe.Expire(ctx, fmt.Sprintf("call:room:%s", roomName), 24*time.Hour)
    pipe.Exec(ctx)

    // ==================== 11. 发送 Kafka call.initiated 事件 ====================
    event := &kafka_call.CallInitiatedEvent{
        Header:           buildEventHeader("call", s.instanceID),
        CallId:           callID,
        CallType:         req.CallType,
        CallerId:         req.CallerId,
        CallerNickname:   callerInfo.Nickname,
        CallerAvatar:     callerInfo.AvatarUrl,
        ReceiverId:       req.ReceiverId,
        ConversationType: req.ConversationType,
        ConversationId:   channelResp.Channel.ConversationId,
        RoomName:         roomName,
        TimeoutSeconds:   60,
    }
    s.kafkaProducer.Send("call.initiated", callID, event)

    log.Info("CreateCall success",
        "call_id", callID,
        "caller_id", req.CallerId,
        "receiver_id", req.ReceiverId,
        "call_type", req.CallType,
        "room_name", roomName,
    )

    return &call.CreateCallResponse{
        Meta:       &common.ResponseMeta{Code: 0, Message: "success", ServerTime: now},
        CallId:     callID,
        Token:      token,
        RoomName:   roomName,
        LivekitUrl: s.livekitConfig.Host,
    }, nil
}
```

### 2. JoinCall — 加入/接听通话

```go
func (s *CallService) JoinCall(ctx context.Context, req *call.JoinCallRequest) (*call.JoinCallResponse, error) {
    span := trace.SpanFromContext(ctx)
    span.SetAttributes(attribute.String("call_id", req.CallId), attribute.String("user_id", req.UserId))

    // ==================== 1. 参数校验 ====================
    if req.UserId == "" || req.CallId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id and call_id required")
    }

    // ==================== 2. 获取分布式锁 ====================
    lockKey := fmt.Sprintf("call:lock:%s", req.CallId)
    lockVal := uuid.New().String()
    acquired, _ := s.redis.SetNX(ctx, lockKey, lockVal, 10*time.Second).Result()
    if !acquired {
        return nil, status.Error(codes.Aborted, "call state is being updated, retry")
    }
    defer s.releaseLock(ctx, lockKey, lockVal)

    // ==================== 3. 查询通话状态（Redis） ====================
    activeKey := fmt.Sprintf("call:active:%s", req.CallId)
    callInfo, err := s.redis.HGetAll(ctx, activeKey).Result()
    if err != nil || len(callInfo) == 0 {
        return nil, status.Error(codes.NotFound, "call not found or already ended")
    }

    callStatus, _ := strconv.Atoi(callInfo["status"])
    if callStatus != int(call.CALL_STATUS_RINGING) && callStatus != int(call.CALL_STATUS_CONNECTED) {
        return nil, status.Errorf(codes.FailedPrecondition, "call is not in ringing/connected state, current: %d", callStatus)
    }

    roomName := callInfo["room_name"]

    // ==================== 4. 生成 LiveKit Token ====================
    token, err := s.generateLiveKitToken(req.UserId, roomName, true)
    if err != nil {
        return nil, status.Errorf(codes.Internal, "generate livekit token failed: %v", err)
    }

    now := time.Now().UnixMilli()

    // ==================== 5. 更新通话状态为 CONNECTED（首次接听时） ====================
    pipe := s.redis.Pipeline()
    if callStatus == int(call.CALL_STATUS_RINGING) {
        pipe.HSet(ctx, activeKey, "status", int(call.CALL_STATUS_CONNECTED))
        pipe.HSet(ctx, activeKey, "connected_at", now)
        // 删除超时标记
        pipe.Del(ctx, fmt.Sprintf("call:timeout:%s", req.CallId))
    }
    // 标记用户活跃通话
    pipe.Set(ctx, fmt.Sprintf("call:user_active:%s", req.UserId), req.CallId, 24*time.Hour)
    // 添加到房间参与者
    pipe.SAdd(ctx, fmt.Sprintf("call:room:%s", roomName), req.UserId)
    pipe.Exec(ctx)

    // ==================== 6. 更新 PgSQL ====================
    _, err = s.db.ExecContext(ctx,
        `UPDATE call_records SET status = $1, connected_at = CASE WHEN connected_at = 0 THEN $2 ELSE connected_at END
         WHERE call_id = $3`,
        call.CALL_STATUS_CONNECTED, now, req.CallId,
    )
    if err != nil {
        log.Error("update call record failed", "call_id", req.CallId, "err", err)
    }

    // ==================== 7. 发送 Kafka call.answered 事件 ====================
    event := &kafka_call.CallAnsweredEvent{
        Header:     buildEventHeader("call", s.instanceID),
        CallId:     req.CallId,
        UserId:     req.UserId,
        CallerId:   callInfo["caller_id"],
        AnswerTime: now,
    }
    s.kafkaProducer.Send("call.answered", req.CallId, event)

    // ==================== 8. 群通话场景：通知其他参与者 ====================
    convType, _ := strconv.Atoi(callInfo["conv_type"])
    if convType == int(common.CONVERSATION_TYPE_GROUP) {
        members, _ := s.redis.SMembers(ctx, fmt.Sprintf("call:room:%s", roomName)).Result()
        notifyUserIDs := make([]string, 0)
        for _, m := range members {
            if m != req.UserId {
                notifyUserIDs = append(notifyUserIDs, m)
            }
        }
        if len(notifyUserIDs) > 0 {
            joinEvent := &kafka_call.CallParticipantJoinedEvent{
                Header:        buildEventHeader("call", s.instanceID),
                CallId:        req.CallId,
                UserId:        req.UserId,
                Nickname:      s.getUserInfoCached(ctx, req.UserId).Nickname,
                NotifyUserIds: notifyUserIDs,
            }
            s.kafkaProducer.Send("call.participant.joined", req.CallId, joinEvent)
        }
    }

    // 构造返回（省略 callRecord 完整构造）
    return &call.JoinCallResponse{
        Meta:       &common.ResponseMeta{Code: 0, Message: "success", ServerTime: now},
        Token:      token,
        RoomName:   roomName,
        LivekitUrl: s.livekitConfig.Host,
    }, nil
}
```

### 3. LeaveCall — 离开通话

```go
func (s *CallService) LeaveCall(ctx context.Context, req *call.LeaveCallRequest) (*call.LeaveCallResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.UserId == "" || req.CallId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id and call_id required")
    }

    // ==================== 2. 获取分布式锁 ====================
    lockKey := fmt.Sprintf("call:lock:%s", req.CallId)
    lockVal := uuid.New().String()
    acquired, _ := s.redis.SetNX(ctx, lockKey, lockVal, 10*time.Second).Result()
    if !acquired {
        return nil, status.Error(codes.Aborted, "retry")
    }
    defer s.releaseLock(ctx, lockKey, lockVal)

    // ==================== 3. 查询通话状态 ====================
    activeKey := fmt.Sprintf("call:active:%s", req.CallId)
    callInfo, err := s.redis.HGetAll(ctx, activeKey).Result()
    if err != nil || len(callInfo) == 0 {
        return nil, status.Error(codes.NotFound, "call not found")
    }

    roomName := callInfo["room_name"]
    convType, _ := strconv.Atoi(callInfo["conv_type"])

    // ==================== 4. 单聊：LeaveCall = EndCall ====================
    if convType == int(common.CONVERSATION_TYPE_SINGLE) {
        return nil, status.Error(codes.FailedPrecondition, "use EndCall for single chat")
    }

    // ==================== 5. 群通话：移除参与者 ====================
    pipe := s.redis.Pipeline()
    pipe.SRem(ctx, fmt.Sprintf("call:room:%s", roomName), req.UserId)
    pipe.Del(ctx, fmt.Sprintf("call:user_active:%s", req.UserId))
    pipe.Exec(ctx)

    // 检查房间是否已空
    remaining, _ := s.redis.SCard(ctx, fmt.Sprintf("call:room:%s", roomName)).Result()
    if remaining == 0 {
        // 房间为空，自动结束通话
        s.endCallInternal(ctx, req.CallId, callInfo, call.CALL_END_REASON_NORMAL)
    } else {
        // 通知其他参与者
        members, _ := s.redis.SMembers(ctx, fmt.Sprintf("call:room:%s", roomName)).Result()
        event := &kafka_call.CallParticipantLeftEvent{
            Header:        buildEventHeader("call", s.instanceID),
            CallId:        req.CallId,
            UserId:        req.UserId,
            NotifyUserIds: members,
        }
        s.kafkaProducer.Send("call.participant.left", req.CallId, event)
    }

    return &call.LeaveCallResponse{
        Meta: &common.ResponseMeta{Code: 0, Message: "success"},
    }, nil
}
```

### 4. RejectCall — 拒绝通话

```go
func (s *CallService) RejectCall(ctx context.Context, req *call.RejectCallRequest) (*call.RejectCallResponse, error) {
    if req.UserId == "" || req.CallId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id and call_id required")
    }

    // 获取分布式锁
    lockKey := fmt.Sprintf("call:lock:%s", req.CallId)
    lockVal := uuid.New().String()
    acquired, _ := s.redis.SetNX(ctx, lockKey, lockVal, 10*time.Second).Result()
    if !acquired {
        return nil, status.Error(codes.Aborted, "retry")
    }
    defer s.releaseLock(ctx, lockKey, lockVal)

    // 查询通话状态
    activeKey := fmt.Sprintf("call:active:%s", req.CallId)
    callInfo, err := s.redis.HGetAll(ctx, activeKey).Result()
    if err != nil || len(callInfo) == 0 {
        return nil, status.Error(codes.NotFound, "call not found")
    }

    callStatus, _ := strconv.Atoi(callInfo["status"])
    if callStatus != int(call.CALL_STATUS_RINGING) {
        return nil, status.Error(codes.FailedPrecondition, "call is not ringing")
    }

    now := time.Now().UnixMilli()

    // 更新状态
    pipe := s.redis.Pipeline()
    pipe.HSet(ctx, activeKey, "status", int(call.CALL_STATUS_REJECTED))
    pipe.Del(ctx, fmt.Sprintf("call:user_active:%s", callInfo["caller_id"]))
    pipe.Del(ctx, fmt.Sprintf("call:timeout:%s", req.CallId))
    pipe.Exec(ctx)

    // 更新 PgSQL
    s.db.ExecContext(ctx,
        `UPDATE call_records SET status = $1, ended_at = $2, end_reason = $3 WHERE call_id = $4`,
        call.CALL_STATUS_REJECTED, now, call.CALL_END_REASON_REJECTED, req.CallId)

    // 销毁 LiveKit Room
    go s.deleteLiveKitRoom(context.Background(), callInfo["room_name"])

    // Kafka 事件
    event := &kafka_call.CallRejectedEvent{
        Header:   buildEventHeader("call", s.instanceID),
        CallId:   req.CallId,
        UserId:   req.UserId,
        CallerId: callInfo["caller_id"],
        Reason:   req.Reason,
    }
    s.kafkaProducer.Send("call.rejected", req.CallId, event)

    return &call.RejectCallResponse{
        Meta: &common.ResponseMeta{Code: 0, Message: "success"},
    }, nil
}
```

### 5. EndCall — 结束通话

```go
func (s *CallService) EndCall(ctx context.Context, req *call.EndCallRequest) (*call.EndCallResponse, error) {
    if req.UserId == "" || req.CallId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id and call_id required")
    }

    // 获取分布式锁
    lockKey := fmt.Sprintf("call:lock:%s", req.CallId)
    lockVal := uuid.New().String()
    acquired, _ := s.redis.SetNX(ctx, lockKey, lockVal, 10*time.Second).Result()
    if !acquired {
        return nil, status.Error(codes.Aborted, "retry")
    }
    defer s.releaseLock(ctx, lockKey, lockVal)

    activeKey := fmt.Sprintf("call:active:%s", req.CallId)
    callInfo, err := s.redis.HGetAll(ctx, activeKey).Result()
    if err != nil || len(callInfo) == 0 {
        return nil, status.Error(codes.NotFound, "call not found")
    }

    duration := s.endCallInternal(ctx, req.CallId, callInfo, req.EndReason)

    return &call.EndCallResponse{
        Meta:     &common.ResponseMeta{Code: 0, Message: "success"},
        Duration: int32(duration),
    }, nil
}

// endCallInternal 内部结束通话方法（复用于 LeaveCall/EndCall/超时处理）
func (s *CallService) endCallInternal(ctx context.Context, callID string, callInfo map[string]string, reason call.CallEndReason) int64 {
    now := time.Now().UnixMilli()

    // 计算通话时长
    connectedAt, _ := strconv.ParseInt(callInfo["connected_at"], 10, 64)
    var duration int64
    if connectedAt > 0 {
        duration = (now - connectedAt) / 1000
    }

    // 清理 Redis
    roomName := callInfo["room_name"]
    members, _ := s.redis.SMembers(ctx, fmt.Sprintf("call:room:%s", roomName)).Result()

    pipe := s.redis.Pipeline()
    pipe.HSet(ctx, fmt.Sprintf("call:active:%s", callID), "status", int(call.CALL_STATUS_ENDED))
    pipe.Expire(ctx, fmt.Sprintf("call:active:%s", callID), 5*time.Minute) // 保留 5 分钟后过期
    for _, uid := range members {
        pipe.Del(ctx, fmt.Sprintf("call:user_active:%s", uid))
    }
    pipe.Del(ctx, fmt.Sprintf("call:user_active:%s", callInfo["caller_id"]))
    pipe.Del(ctx, fmt.Sprintf("call:user_active:%s", callInfo["receiver_id"]))
    pipe.Del(ctx, fmt.Sprintf("call:room:%s", roomName))
    pipe.Del(ctx, fmt.Sprintf("call:timeout:%s", callID))
    pipe.Exec(ctx)

    // 更新 PgSQL
    s.db.ExecContext(ctx,
        `UPDATE call_records SET status = $1, ended_at = $2, duration = $3, end_reason = $4 WHERE call_id = $5`,
        call.CALL_STATUS_ENDED, now, duration, reason, callID)

    // 销毁 LiveKit Room
    go s.deleteLiveKitRoom(context.Background(), roomName)

    // 通知所有相关用户
    notifyUserIDs := append(members, callInfo["caller_id"], callInfo["receiver_id"])
    notifyUserIDs = uniqueStrings(notifyUserIDs)

    event := &kafka_call.CallEndedEvent{
        Header:         buildEventHeader("call", s.instanceID),
        CallId:         callID,
        EndReason:      reason,
        EndedBy:        "", // 可能是超时自动结束
        Duration:       int32(duration),
        UserIds:        notifyUserIDs,
        ConversationId: callInfo["conv_id"],
    }
    s.kafkaProducer.Send("call.ended", callID, event)

    // 统计事件
    statsEvent := &kafka_call.CallStatsEvent{
        Header:           buildEventHeader("call", s.instanceID),
        CallId:           callID,
        Duration:         int32(duration),
        CallType:         call.CallType(mustAtoi(callInfo["call_type"])),
        ParticipantCount: int32(len(members)),
        EndReason:        reason.String(),
        InitiatedAt:      mustAtoi64(callInfo["initiated_at"]),
        EndedAt:          now,
    }
    s.kafkaProducer.Send("call.stats", callID, statsEvent)

    return duration
}
```

### 6. GetCallToken — 获取/刷新 LiveKit Token

```go
func (s *CallService) GetCallToken(ctx context.Context, req *call.GetCallTokenRequest) (*call.GetCallTokenResponse, error) {
    if req.UserId == "" || req.CallId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id and call_id required")
    }

    // 查询通话状态
    callInfo, err := s.redis.HGetAll(ctx, fmt.Sprintf("call:active:%s", req.CallId)).Result()
    if err != nil || len(callInfo) == 0 {
        return nil, status.Error(codes.NotFound, "call not found")
    }

    callStatus, _ := strconv.Atoi(callInfo["status"])
    if callStatus != int(call.CALL_STATUS_CONNECTED) && callStatus != int(call.CALL_STATUS_RINGING) {
        return nil, status.Error(codes.FailedPrecondition, "call is not active")
    }

    // 验证用户是否在通话中
    isMember, _ := s.redis.SIsMember(ctx, fmt.Sprintf("call:room:%s", callInfo["room_name"]), req.UserId).Result()
    if !isMember && req.UserId != callInfo["caller_id"] && req.UserId != callInfo["receiver_id"] {
        return nil, status.Error(codes.PermissionDenied, "user not in this call")
    }

    // 生成新 Token
    token, err := s.generateLiveKitToken(req.UserId, callInfo["room_name"], true)
    if err != nil {
        return nil, status.Errorf(codes.Internal, "generate token failed: %v", err)
    }

    expireTime := time.Now().Add(24 * time.Hour).UnixMilli()

    return &call.GetCallTokenResponse{
        Meta:       &common.ResponseMeta{Code: 0, Message: "success"},
        Token:      token,
        ExpireTime: expireTime,
    }, nil
}
```

### 7. GetCallHistory — 获取通话历史记录

```go
func (s *CallService) GetCallHistory(ctx context.Context, req *call.GetCallHistoryRequest) (*call.GetCallHistoryResponse, error) {
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id required")
    }

    page := int(req.Pagination.Page)
    pageSize := int(req.Pagination.PageSize)
    if page <= 0 { page = 1 }
    if pageSize <= 0 { pageSize = 20 }
    if pageSize > 100 { pageSize = 100 }
    offset := (page - 1) * pageSize

    // 构建查询条件
    baseWhere := "(caller_id = $1 OR receiver_id = $1)"
    args := []interface{}{req.UserId}
    argIdx := 2

    if req.CallType != call.CALL_TYPE_UNSPECIFIED {
        baseWhere += fmt.Sprintf(" AND call_type = $%d", argIdx)
        args = append(args, req.CallType)
        argIdx++
    }
    if req.MissedOnly {
        baseWhere += fmt.Sprintf(" AND status = $%d AND receiver_id = $%d", argIdx, argIdx+1)
        args = append(args, call.CALL_STATUS_MISSED, req.UserId)
        argIdx += 2
    }

    // 查询总数
    var total int64
    s.db.QueryRowContext(ctx,
        fmt.Sprintf("SELECT COUNT(*) FROM call_records WHERE %s", baseWhere),
        args...,
    ).Scan(&total)

    // 查询记录
    queryArgs := append(args, pageSize, offset)
    rows, err := s.db.QueryContext(ctx,
        fmt.Sprintf(`SELECT call_id, call_type, status, caller_id, receiver_id,
            conversation_type, conversation_id, room_name, duration, end_reason,
            initiated_at, connected_at, ended_at
            FROM call_records WHERE %s ORDER BY initiated_at DESC LIMIT $%d OFFSET $%d`,
            baseWhere, argIdx, argIdx+1),
        queryArgs...,
    )
    if err != nil {
        return nil, status.Errorf(codes.Internal, "query call history failed: %v", err)
    }
    defer rows.Close()

    var records []*call.CallRecord
    for rows.Next() {
        r := &call.CallRecord{}
        rows.Scan(&r.CallId, &r.CallType, &r.Status, &r.CallerId, &r.ReceiverId,
            &r.ConversationType, &r.ConversationId, &r.RoomName, &r.Duration, &r.EndReason,
            &r.InitiatedAt, &r.ConnectedAt, &r.EndedAt)
        records = append(records, r)
    }

    totalPages := int32((total + int64(pageSize) - 1) / int64(pageSize))

    return &call.GetCallHistoryResponse{
        Meta:    &common.ResponseMeta{Code: 0, Message: "success"},
        Records: records,
        Pagination: &common.PaginationResult{
            Page: int32(page), PageSize: int32(pageSize), Total: total, TotalPages: totalPages,
        },
    }, nil
}
```

### 8. GetActiveCall — 查询当前进行中通话

```go
func (s *CallService) GetActiveCall(ctx context.Context, req *call.GetActiveCallRequest) (*call.GetActiveCallResponse, error) {
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id required")
    }

    // 从 Redis 查询用户是否有活跃通话
    callID, err := s.redis.Get(ctx, fmt.Sprintf("call:user_active:%s", req.UserId)).Result()
    if err != nil || callID == "" {
        return &call.GetActiveCallResponse{
            Meta:   &common.ResponseMeta{Code: 0, Message: "success"},
            Active: false,
        }, nil
    }

    // 查询通话详情
    callInfo, err := s.redis.HGetAll(ctx, fmt.Sprintf("call:active:%s", callID)).Result()
    if err != nil || len(callInfo) == 0 {
        // Redis 数据不一致，清理
        s.redis.Del(ctx, fmt.Sprintf("call:user_active:%s", req.UserId))
        return &call.GetActiveCallResponse{
            Meta:   &common.ResponseMeta{Code: 0, Message: "success"},
            Active: false,
        }, nil
    }

    // 如果指定了 conversation_id，检查是否匹配
    if req.ConversationId != "" && callInfo["conv_id"] != req.ConversationId {
        return &call.GetActiveCallResponse{
            Meta:   &common.ResponseMeta{Code: 0, Message: "success"},
            Active: false,
        }, nil
    }

    record := &call.CallRecord{
        CallId:           callID,
        CallType:         call.CallType(mustAtoi(callInfo["call_type"])),
        Status:           call.CallStatus(mustAtoi(callInfo["status"])),
        CallerId:         callInfo["caller_id"],
        ReceiverId:       callInfo["receiver_id"],
        ConversationType: common.ConversationType(mustAtoi(callInfo["conv_type"])),
        ConversationId:   callInfo["conv_id"],
        RoomName:         callInfo["room_name"],
        InitiatedAt:      mustAtoi64(callInfo["initiated_at"]),
    }

    return &call.GetActiveCallResponse{
        Meta:   &common.ResponseMeta{Code: 0, Message: "success"},
        Call:   record,
        Active: true,
    }, nil
}
```

### 9. UpdateCallMedia — 更新通话媒体状态

```go
func (s *CallService) UpdateCallMedia(ctx context.Context, req *call.UpdateCallMediaRequest) (*call.UpdateCallMediaResponse, error) {
    if req.UserId == "" || req.CallId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id and call_id required")
    }

    // 查询通话状态
    callInfo, _ := s.redis.HGetAll(ctx, fmt.Sprintf("call:active:%s", req.CallId)).Result()
    if len(callInfo) == 0 {
        return nil, status.Error(codes.NotFound, "call not found")
    }

    roomName := callInfo["room_name"]
    members, _ := s.redis.SMembers(ctx, fmt.Sprintf("call:room:%s", roomName)).Result()

    // 通知其他参与者
    notifyUserIDs := make([]string, 0)
    for _, m := range members {
        if m != req.UserId {
            notifyUserIDs = append(notifyUserIDs, m)
        }
    }

    event := &kafka_call.CallMediaUpdatedEvent{
        Header:        buildEventHeader("call", s.instanceID),
        CallId:        req.CallId,
        UserId:        req.UserId,
        IsMuted:       req.IsMuted,
        IsCameraOff:   req.IsCameraOff,
        NotifyUserIds: notifyUserIDs,
    }
    s.kafkaProducer.Send("call.media.updated", req.CallId, event)

    return &call.UpdateCallMediaResponse{
        Meta: &common.ResponseMeta{Code: 0, Message: "success"},
    }, nil
}
```

### 10. CancelCall — 取消通话

```go
func (s *CallService) CancelCall(ctx context.Context, req *call.CancelCallRequest) (*call.CancelCallResponse, error) {
    if req.UserId == "" || req.CallId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id and call_id required")
    }

    // 获取分布式锁
    lockKey := fmt.Sprintf("call:lock:%s", req.CallId)
    lockVal := uuid.New().String()
    acquired, _ := s.redis.SetNX(ctx, lockKey, lockVal, 10*time.Second).Result()
    if !acquired {
        return nil, status.Error(codes.Aborted, "retry")
    }
    defer s.releaseLock(ctx, lockKey, lockVal)

    activeKey := fmt.Sprintf("call:active:%s", req.CallId)
    callInfo, _ := s.redis.HGetAll(ctx, activeKey).Result()
    if len(callInfo) == 0 {
        return nil, status.Error(codes.NotFound, "call not found")
    }

    // 只有发起方可以取消
    if callInfo["caller_id"] != req.UserId {
        return nil, status.Error(codes.PermissionDenied, "only caller can cancel")
    }

    callStatus, _ := strconv.Atoi(callInfo["status"])
    if callStatus != int(call.CALL_STATUS_RINGING) {
        return nil, status.Error(codes.FailedPrecondition, "call is not ringing")
    }

    now := time.Now().UnixMilli()

    // 清理 Redis
    pipe := s.redis.Pipeline()
    pipe.HSet(ctx, activeKey, "status", int(call.CALL_STATUS_CANCELLED))
    pipe.Expire(ctx, activeKey, 5*time.Minute)
    pipe.Del(ctx, fmt.Sprintf("call:user_active:%s", callInfo["caller_id"]))
    pipe.Del(ctx, fmt.Sprintf("call:timeout:%s", req.CallId))
    pipe.Del(ctx, fmt.Sprintf("call:room:%s", callInfo["room_name"]))
    pipe.Exec(ctx)

    // 更新 PgSQL
    s.db.ExecContext(ctx,
        `UPDATE call_records SET status = $1, ended_at = $2, end_reason = $3 WHERE call_id = $4`,
        call.CALL_STATUS_CANCELLED, now, call.CALL_END_REASON_CANCELLED, req.CallId)

    // 销毁 LiveKit Room
    go s.deleteLiveKitRoom(context.Background(), callInfo["room_name"])

    // Kafka 事件
    event := &kafka_call.CallCancelledEvent{
        Header:         buildEventHeader("call", s.instanceID),
        CallId:         req.CallId,
        CallerId:       req.UserId,
        ReceiverId:     callInfo["receiver_id"],
        ConversationId: callInfo["conv_id"],
    }
    s.kafkaProducer.Send("call.cancelled", req.CallId, event)

    return &call.CancelCallResponse{
        Meta: &common.ResponseMeta{Code: 0, Message: "success"},
    }, nil
}
```

---

## 定时任务：响铃超时处理

```go
// StartTimeoutChecker 启动响铃超时检查（每 5 秒执行一次）
func (s *CallService) StartTimeoutChecker() {
    ticker := time.NewTicker(5 * time.Second)
    defer ticker.Stop()

    for range ticker.C {
        s.checkTimeoutCalls(context.Background())
    }
}

func (s *CallService) checkTimeoutCalls(ctx context.Context) {
    // 扫描所有 RINGING 状态的通话
    // 使用 SCAN 命令避免阻塞 Redis
    var cursor uint64
    for {
        keys, nextCursor, err := s.redis.Scan(ctx, cursor, "call:active:*", 100).Result()
        if err != nil {
            break
        }

        for _, key := range keys {
            callID := strings.TrimPrefix(key, "call:active:")

            // 检查是否还有超时标记（超时标记 60s 过期）
            exists, _ := s.redis.Exists(ctx, fmt.Sprintf("call:timeout:%s", callID)).Result()
            if exists > 0 {
                continue // 还没超时
            }

            // 检查通话状态
            statusStr, _ := s.redis.HGet(ctx, key, "status").Result()
            if statusStr == strconv.Itoa(int(call.CALL_STATUS_RINGING)) {
                // 响铃超时 → 标记为未接来电
                callInfo, _ := s.redis.HGetAll(ctx, key).Result()
                s.handleMissedCall(ctx, callID, callInfo)
            }
        }

        cursor = nextCursor
        if cursor == 0 {
            break
        }
    }
}

func (s *CallService) handleMissedCall(ctx context.Context, callID string, callInfo map[string]string) {
    now := time.Now().UnixMilli()

    // 获取分布式锁
    lockKey := fmt.Sprintf("call:lock:%s", callID)
    lockVal := uuid.New().String()
    acquired, _ := s.redis.SetNX(ctx, lockKey, lockVal, 10*time.Second).Result()
    if !acquired {
        return
    }
    defer s.releaseLock(ctx, lockKey, lockVal)

    // 双重检查状态
    status, _ := s.redis.HGet(ctx, fmt.Sprintf("call:active:%s", callID), "status").Result()
    if status != strconv.Itoa(int(call.CALL_STATUS_RINGING)) {
        return
    }

    // 更新 Redis
    pipe := s.redis.Pipeline()
    pipe.HSet(ctx, fmt.Sprintf("call:active:%s", callID), "status", int(call.CALL_STATUS_MISSED))
    pipe.Expire(ctx, fmt.Sprintf("call:active:%s", callID), 5*time.Minute)
    pipe.Del(ctx, fmt.Sprintf("call:user_active:%s", callInfo["caller_id"]))
    pipe.Del(ctx, fmt.Sprintf("call:room:%s", callInfo["room_name"]))
    pipe.Exec(ctx)

    // 更新 PgSQL
    s.db.ExecContext(ctx,
        `UPDATE call_records SET status = $1, ended_at = $2, end_reason = $3 WHERE call_id = $4`,
        call.CALL_STATUS_MISSED, now, call.CALL_END_REASON_TIMEOUT, callID)

    // 销毁 LiveKit Room
    go s.deleteLiveKitRoom(context.Background(), callInfo["room_name"])

    // 发送未接来电事件
    event := &kafka_call.CallMissedEvent{
        Header:         buildEventHeader("call", s.instanceID),
        CallId:         callID,
        CallerId:       callInfo["caller_id"],
        ReceiverId:     callInfo["receiver_id"],
        CallType:       call.CallType(mustAtoi(callInfo["call_type"])),
        ConversationId: callInfo["conv_id"],
    }
    s.kafkaProducer.Send("call.missed", callID, event)

    log.Info("call timeout, marked as missed",
        "call_id", callID,
        "caller_id", callInfo["caller_id"],
        "receiver_id", callInfo["receiver_id"],
    )
}
```

---

## 辅助方法

```go
// getUserInfoCached 获取用户信息（本地 Redis 缓存优先）
func (s *CallService) getUserInfoCached(ctx context.Context, userID string) *common.UserBrief {
    cacheKey := fmt.Sprintf("call:local:user:%s", userID)
    info, err := s.redis.HGetAll(ctx, cacheKey).Result()
    if err == nil && len(info) > 0 {
        return &common.UserBrief{
            UserId:    userID,
            Nickname:  info["nickname"],
            AvatarUrl: info["avatar_url"],
        }
    }

    // Cache miss → RPC 回源
    resp, err := s.userClient.GetUser(ctx, &user.GetUserRequest{UserId: userID})
    if err != nil {
        log.Warn("getUserInfoCached: RPC fallback failed", "user_id", userID, "err", err)
        return &common.UserBrief{UserId: userID}
    }

    // 回填缓存
    s.redis.HSet(ctx, cacheKey, map[string]interface{}{
        "nickname":   resp.User.Nickname,
        "avatar_url": resp.User.AvatarUrl,
    })
    s.redis.Expire(ctx, cacheKey, 1*time.Hour)

    return &common.UserBrief{
        UserId:    userID,
        Nickname:  resp.User.Nickname,
        AvatarUrl: resp.User.AvatarUrl,
    }
}

// releaseLock 释放分布式锁（安全释放，防止释放他人的锁）
func (s *CallService) releaseLock(ctx context.Context, key, value string) {
    script := `
        if redis.call("GET", KEYS[1]) == ARGV[1] then
            return redis.call("DEL", KEYS[1])
        else
            return 0
        end
    `
    s.redis.Eval(ctx, script, []string{key}, value)
}

// buildEventHeader 构建 Kafka 事件头
func buildEventHeader(source string, instanceID string) *common.EventHeader {
    return &common.EventHeader{
        EventId:   uuid.New().String(),
        Source:    source,
        SourceId:  instanceID,
        Timestamp: time.Now().UnixMilli(),
        Version:   1,
    }
}

func mustAtoi(s string) int {
    v, _ := strconv.Atoi(s)
    return v
}

func mustAtoi64(s string) int64 {
    v, _ := strconv.ParseInt(s, 10, 64)
    return v
}

func uniqueStrings(input []string) []string {
    seen := make(map[string]bool)
    result := make([]string, 0)
    for _, s := range input {
        if s != "" && !seen[s] {
            seen[s] = true
            result = append(result, s)
        }
    }
    return result
}
```

---

## 可观测性接入

### 自定义业务指标

| 指标名 | 类型 | 标签 | 说明 |
|--------|------|------|------|
| `call_initiated_total` | Counter | call_type, conv_type | 通话发起次数 |
| `call_answered_total` | Counter | call_type | 通话接听次数 |
| `call_ended_total` | Counter | end_reason, call_type | 通话结束次数 |
| `call_missed_total` | Counter | call_type | 未接来电次数 |
| `call_duration_seconds` | Histogram | call_type | 通话时长分布 |
| `call_livekit_token_latency` | Histogram | | LiveKit Token 生成延迟 |
| `call_livekit_room_latency` | Histogram | op (create/delete) | LiveKit Room 操作延迟 |
| `call_concurrent_active` | Gauge | | 当前活跃通话数 |

### 接入要点

| 步骤 | 说明 |
|------|------|
| 1 | `observability.InitOpenTelemetry("call-service")` |
| 2 | gRPC Server 添加 `UnaryInterceptor(otelgrpc.UnaryServerInterceptor())` |
| 3 | 注册自定义业务指标（call_initiated_total 等） |
| 4 | Kafka Producer 事件头统一使用 `observability.BuildEventHeader(ctx, "call", instanceID)` |
| 5 | 结构化 JSON 日志，所有日志携带 trace_id |
| 6 | Redis 连接池指标 `observability.RegisterRedisPoolMetrics("call")` |
| 7 | LiveKit API 调用添加 span（`tracer.Start(ctx, "livekit.CreateRoom")`） |
