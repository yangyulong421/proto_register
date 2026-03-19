# Relation 好友关系服务 — RPC 接口实现伪代码

## 概述

Relation 服务负责好友申请/处理、好友列表管理、好友备注、好友分组管理（创建/编辑/删除分组，分组内好友维护）、黑名单管理、好友关系校验（单个/批量）。是社交关系链的核心服务。

## 外部依赖

| 依赖类型 | 依赖项 | 用途 |
|---------|--------|------|
| PgSQL | friend_requests 表 | 好友申请记录持久化 |
| PgSQL | friends 表 | 好友关系持久化 |
| PgSQL | blocks 表 | 黑名单持久化 |
| PgSQL | friend_groups 表 | 好友分组持久化 |
| Redis | 好友列表缓存 / 黑名单缓存 / 关系校验缓存 / 好友数量缓存 | 高频读写 |
| RPC | User.GetUser | 校验用户是否存在 |
| RPC | User.GetUserSettings | 查询隐私设置（加好友是否需要确认、是否允许陌生人消息） |
| RPC | Conversation.GetOrCreateChannel | 好友通过后创建聊天频道 |
| RPC | Conversation.CreateUserConversation | 好友通过后为双方创建会话视图 |
| Kafka | relation.friend.request / relation.friend.accepted / relation.friend.rejected / relation.friend.deleted / relation.blocked / relation.unblocked / relation.remark.updated / relation.stats | 事件通知 |

## PgSQL 表结构

```sql
-- 好友申请表
CREATE TABLE friend_requests (
    id           BIGSERIAL PRIMARY KEY,
    request_id   VARCHAR(64)  NOT NULL UNIQUE,
    from_user_id VARCHAR(64)  NOT NULL,
    to_user_id   VARCHAR(64)  NOT NULL,
    message      VARCHAR(256) NOT NULL DEFAULT '',
    status       SMALLINT     NOT NULL DEFAULT 0,   -- 0=待处理 1=已同意 2=已拒绝
    source       VARCHAR(32)  NOT NULL DEFAULT '',   -- search / group / qrcode / card
    created_at   BIGINT       NOT NULL,
    handled_at   BIGINT       NOT NULL DEFAULT 0
);
CREATE INDEX idx_friend_requests_to ON friend_requests(to_user_id, status);
CREATE INDEX idx_friend_requests_from ON friend_requests(from_user_id);
CREATE INDEX idx_friend_requests_pair ON friend_requests(from_user_id, to_user_id, status);

-- 好友关系表（双向写入：A→B 和 B→A 各一条）
CREATE TABLE friends (
    id              BIGSERIAL PRIMARY KEY,
    user_id         VARCHAR(64) NOT NULL,
    friend_id       VARCHAR(64) NOT NULL,
    remark          VARCHAR(128) NOT NULL DEFAULT '',
    source          VARCHAR(32)  NOT NULL DEFAULT '',
    friend_group_id VARCHAR(64)  NOT NULL DEFAULT '',  -- 空表示"未分组"
    created_at      BIGINT       NOT NULL,
    UNIQUE(user_id, friend_id)
);
CREATE INDEX idx_friends_user ON friends(user_id);
CREATE INDEX idx_friends_group ON friends(user_id, friend_group_id);

-- 黑名单表
CREATE TABLE blocks (
    id         BIGSERIAL PRIMARY KEY,
    user_id    VARCHAR(64) NOT NULL,
    blocked_id VARCHAR(64) NOT NULL,
    created_at BIGINT      NOT NULL,
    UNIQUE(user_id, blocked_id)
);
CREATE INDEX idx_blocks_user ON blocks(user_id);

-- 好友分组表
CREATE TABLE friend_groups (
    id         BIGSERIAL PRIMARY KEY,
    group_id   VARCHAR(64) NOT NULL UNIQUE,
    user_id    VARCHAR(64) NOT NULL,
    name       VARCHAR(64) NOT NULL,
    sort_order INT         NOT NULL DEFAULT 0,
    created_at BIGINT      NOT NULL,
    updated_at BIGINT      NOT NULL
);
CREATE INDEX idx_friend_groups_user ON friend_groups(user_id);
CREATE UNIQUE INDEX idx_friend_groups_user_name ON friend_groups(user_id, name);
```

## Redis Key 设计

| Key 格式 | 类型 | 值 | TTL | 说明 |
|----------|------|-----|-----|------|
| `rel:friends:{user_id}` | SET | friend_id 集合 | 30min | 好友列表缓存 |
| `rel:blocks:{user_id}` | SET | blocked_id 集合 | 30min | 黑名单缓存 |
| `rel:is_friend:{user_id}:{target_id}` | STRING | "1" / "0" | 10min | 好友关系判断缓存 |
| `rel:is_blocked:{user_id}:{target_id}` | STRING | "1" / "0" | 10min | 黑名单判断缓存 |
| `rel:friend_count:{user_id}` | STRING | 好友数量 | 30min | 好友计数缓存 |
| `rel:kafka:dedup:{event_id}` | STRING | "1" | 24h | Kafka 消费幂等去重 |

---

## 接口实现

### 1. SendFriendRequest — 发送好友申请

```go
func (s *RelationService) SendFriendRequest(ctx context.Context, req *pb.SendFriendRequestRequest) (*pb.SendFriendRequestResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.FromUserId == "" {
        return nil, status.Error(codes.InvalidArgument, "from_user_id is required")
    }
    if req.ToUserId == "" {
        return nil, status.Error(codes.InvalidArgument, "to_user_id is required")
    }
    if req.FromUserId == req.ToUserId {
        return nil, status.Error(codes.InvalidArgument, "cannot send friend request to yourself")
    }
    if len(req.Message) > 256 {
        return nil, status.Error(codes.InvalidArgument, "message too long, max 256 chars")
    }

    // ==================== 2. 校验目标用户是否存在 — RPC User.GetUser ====================
    _, err := s.userClient.GetUser(ctx, &user_pb.GetUserRequest{UserId: req.ToUserId})
    if err != nil {
        if st, ok := status.FromError(err); ok && st.Code() == codes.NotFound {
            return nil, status.Error(codes.NotFound, "target user not found")
        }
        return nil, status.Error(codes.Internal, "get target user failed")
    }

    // ==================== 3. 检查是否已是好友 — Redis / PgSQL ====================
    isFriend, err := s.checkFriendshipInternal(ctx, req.FromUserId, req.ToUserId)
    if err != nil {
        return nil, status.Error(codes.Internal, "check friendship failed")
    }
    if isFriend {
        return nil, status.Error(codes.AlreadyExists, "already friends")
    }

    // ==================== 4. 检查是否被对方拉黑 — Redis / PgSQL ====================
    isBlocked, err := s.checkBlockedInternal(ctx, req.ToUserId, req.FromUserId)
    if err != nil {
        return nil, status.Error(codes.Internal, "check block status failed")
    }
    if isBlocked {
        // 不暴露拉黑信息，返回通用错误
        return nil, status.Error(codes.FailedPrecondition, "cannot send friend request to this user")
    }

    // ==================== 5. 检查是否有未处理的申请 — PgSQL ====================
    var pendingCount int
    err = s.db.QueryRowContext(ctx,
        `SELECT COUNT(*) FROM friend_requests
         WHERE from_user_id = $1 AND to_user_id = $2 AND status = 0`,
        req.FromUserId, req.ToUserId,
    ).Scan(&pendingCount)
    if err != nil {
        return nil, status.Error(codes.Internal, "query pending requests failed")
    }
    if pendingCount > 0 {
        return nil, status.Error(codes.AlreadyExists, "friend request already pending")
    }

    // ==================== 6. 查询对方隐私设置 — RPC User.GetUserSettings ====================
    settingsResp, err := s.userClient.GetUserSettings(ctx, &user_pb.GetUserSettingsRequest{
        UserId: req.ToUserId,
    })
    if err != nil {
        return nil, status.Error(codes.Internal, "get user settings failed")
    }

    now := time.Now().UnixMilli()
    requestID := generateRequestID() // UUID v4

    // ==================== 7. 判断是否需要确认 ====================
    needConfirm := settingsResp.Settings.AddFriendNeedConfirm

    if !needConfirm {
        // 不需要确认，直接建立好友关系
        err = s.acceptFriendInternal(ctx, req.FromUserId, req.ToUserId, req.Source, requestID, now)
        if err != nil {
            return nil, status.Error(codes.Internal, "auto accept friend failed")
        }

        // 生产 relation.friend.accepted 事件
        acceptEvent := &kafka_relation.FriendAcceptedEvent{
            Header:     buildEventHeader("relation", s.instanceID),
            RequestId:  requestID,
            FromUserId: req.FromUserId,
            ToUserId:   req.ToUserId,
            AcceptTime: now,
        }
        s.kafka.Produce(ctx, "relation.friend.accepted", req.FromUserId, acceptEvent)

        return &pb.SendFriendRequestResponse{
            Meta:      successMeta(ctx),
            RequestId: requestID,
        }, nil
    }

    // ==================== 8. 需要确认：创建好友申请记录 — PgSQL ====================
    _, err = s.db.ExecContext(ctx,
        `INSERT INTO friend_requests (request_id, from_user_id, to_user_id, message, status, source, created_at, handled_at)
         VALUES ($1, $2, $3, $4, 0, $5, $6, 0)`,
        requestID, req.FromUserId, req.ToUserId, req.Message, req.Source, now,
    )
    if err != nil {
        return nil, status.Error(codes.Internal, "insert friend request failed")
    }

    // ==================== 9. 获取申请者信息用于事件 — RPC User.GetUser ====================
    fromUserResp, err := s.userClient.GetUser(ctx, &user_pb.GetUserRequest{UserId: req.FromUserId})
    fromNickname := ""
    fromAvatar := ""
    if err == nil {
        fromNickname = fromUserResp.User.Nickname
        fromAvatar = fromUserResp.User.AvatarUrl
    }

    // ==================== 10. 生产 Kafka 事件: relation.friend.request ====================
    reqEvent := &kafka_relation.FriendRequestEvent{
        Header:       buildEventHeader("relation", s.instanceID),
        RequestId:    requestID,
        FromUserId:   req.FromUserId,
        ToUserId:     req.ToUserId,
        FromNickname: fromNickname,
        FromAvatar:   fromAvatar,
        Message:      req.Message,
        Source:       req.Source,
        CreateTime:   now,
    }
    s.kafka.Produce(ctx, "relation.friend.request", req.ToUserId, reqEvent)

    return &pb.SendFriendRequestResponse{
        Meta:      successMeta(ctx),
        RequestId: requestID,
    }, nil
}
```

### 2. HandleFriendRequest — 处理好友申请（同意/拒绝）

```go
func (s *RelationService) HandleFriendRequest(ctx context.Context, req *pb.HandleFriendRequestRequest) (*pb.HandleFriendRequestResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id is required")
    }
    if req.RequestId == "" {
        return nil, status.Error(codes.InvalidArgument, "request_id is required")
    }
    if req.Action != 1 && req.Action != 2 {
        return nil, status.Error(codes.InvalidArgument, "action must be 1(accept) or 2(reject)")
    }

    // ==================== 2. 查询好友申请 — PgSQL ====================
    var fromUserID, toUserID, source string
    var currentStatus int32
    err := s.db.QueryRowContext(ctx,
        `SELECT from_user_id, to_user_id, source, status FROM friend_requests WHERE request_id = $1`,
        req.RequestId,
    ).Scan(&fromUserID, &toUserID, &source, &currentStatus)
    if err == sql.ErrNoRows {
        return nil, status.Error(codes.NotFound, "friend request not found")
    }
    if err != nil {
        return nil, status.Error(codes.Internal, "query friend request failed")
    }

    // ==================== 3. 权限校验 ====================
    if toUserID != req.UserId {
        return nil, status.Error(codes.PermissionDenied, "only the recipient can handle this request")
    }
    if currentStatus != 0 {
        return nil, status.Error(codes.FailedPrecondition, "friend request already handled")
    }

    now := time.Now().UnixMilli()

    // ==================== 4. 更新申请状态 — PgSQL ====================
    _, err = s.db.ExecContext(ctx,
        `UPDATE friend_requests SET status = $1, handled_at = $2 WHERE request_id = $3 AND status = 0`,
        req.Action, now, req.RequestId,
    )
    if err != nil {
        return nil, status.Error(codes.Internal, "update friend request failed")
    }

    // ==================== 5. 如果同意：建立好友关系 ====================
    if req.Action == 1 {
        err = s.acceptFriendInternal(ctx, fromUserID, toUserID, source, req.RequestId, now)
        if err != nil {
            return nil, status.Error(codes.Internal, "accept friend failed")
        }

        // 生产 relation.friend.accepted 事件
        acceptEvent := &kafka_relation.FriendAcceptedEvent{
            Header:     buildEventHeader("relation", s.instanceID),
            RequestId:  req.RequestId,
            FromUserId: fromUserID,
            ToUserId:   toUserID,
            AcceptTime: now,
        }
        s.kafka.Produce(ctx, "relation.friend.accepted", fromUserID, acceptEvent)
    } else {
        // 拒绝：生产 relation.friend.rejected 事件
        rejectEvent := &kafka_relation.FriendRejectedEvent{
            Header:     buildEventHeader("relation", s.instanceID),
            RequestId:  req.RequestId,
            FromUserId: fromUserID,
            ToUserId:   toUserID,
            RejectTime: now,
        }
        s.kafka.Produce(ctx, "relation.friend.rejected", fromUserID, rejectEvent)
    }

    return &pb.HandleFriendRequestResponse{Meta: successMeta(ctx)}, nil
}

// acceptFriendInternal — 内部方法：建立双向好友关系 + 创建会话
func (s *RelationService) acceptFriendInternal(ctx context.Context, fromUserID, toUserID, source, requestID string, now int64) error {
    // ==================== A. 双向写入好友关系 — PgSQL 事务 ====================
    tx, err := s.db.BeginTx(ctx, nil)
    if err != nil {
        return fmt.Errorf("begin tx failed: %w", err)
    }
    defer tx.Rollback()

    // A→B
    _, err = tx.ExecContext(ctx,
        `INSERT INTO friends (user_id, friend_id, remark, source, friend_group_id, created_at)
         VALUES ($1, $2, '', $3, '', $4)
         ON CONFLICT (user_id, friend_id) DO NOTHING`,
        fromUserID, toUserID, source, now,
    )
    if err != nil {
        return fmt.Errorf("insert friend A->B failed: %w", err)
    }

    // B→A
    _, err = tx.ExecContext(ctx,
        `INSERT INTO friends (user_id, friend_id, remark, source, friend_group_id, created_at)
         VALUES ($1, $2, '', $3, '', $4)
         ON CONFLICT (user_id, friend_id) DO NOTHING`,
        toUserID, fromUserID, source, now,
    )
    if err != nil {
        return fmt.Errorf("insert friend B->A failed: %w", err)
    }

    if err = tx.Commit(); err != nil {
        return fmt.Errorf("commit tx failed: %w", err)
    }

    // ==================== B. 延迟双删清除 Redis 缓存 ====================
    s.delayedDoubleDelete(ctx,
        fmt.Sprintf("rel:friends:%s", fromUserID),
        fmt.Sprintf("rel:friends:%s", toUserID),
        fmt.Sprintf("rel:is_friend:%s:%s", fromUserID, toUserID),
        fmt.Sprintf("rel:is_friend:%s:%s", toUserID, fromUserID),
        fmt.Sprintf("rel:friend_count:%s", fromUserID),
        fmt.Sprintf("rel:friend_count:%s", toUserID),
    )

    // ==================== C. 创建聊天频道 — RPC Conversation.GetOrCreateChannel ====================
    channelResp, err := s.conversationClient.GetOrCreateChannel(ctx, &conversation_pb.GetOrCreateChannelRequest{
        ConversationType: common.CONVERSATION_TYPE_SINGLE,
        MemberIds:        []string{fromUserID, toUserID},
    })
    if err != nil {
        log.Error("create channel failed", "from", fromUserID, "to", toUserID, "err", err)
        // 非致命错误，降级处理；后续可通过消息触发补偿
    }

    channelID := ""
    if channelResp != nil {
        channelID = channelResp.ChannelId
    }

    // ==================== D. 为双方创建会话视图 — RPC Conversation.CreateUserConversation ====================
    if channelID != "" {
        // 为发起方创建会话
        go func() {
            _, err := s.conversationClient.CreateUserConversation(context.Background(), &conversation_pb.CreateUserConversationRequest{
                UserId:           fromUserID,
                PeerId:           toUserID,
                ConversationType: common.CONVERSATION_TYPE_SINGLE,
            })
            if err != nil {
                log.Error("create conversation for from_user failed", "user_id", fromUserID, "err", err)
            }
        }()

        // 为接收方创建会话
        go func() {
            _, err := s.conversationClient.CreateUserConversation(context.Background(), &conversation_pb.CreateUserConversationRequest{
                UserId:           toUserID,
                PeerId:           fromUserID,
                ConversationType: common.CONVERSATION_TYPE_SINGLE,
            })
            if err != nil {
                log.Error("create conversation for to_user failed", "user_id", toUserID, "err", err)
            }
        }()
    }

    return nil
}
```

### 3. GetFriendRequests — 获取好友申请列表

```go
func (s *RelationService) GetFriendRequests(ctx context.Context, req *pb.GetFriendRequestsRequest) (*pb.GetFriendRequestsResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id is required")
    }
    page := int32(1)
    pageSize := int32(20)
    if req.Pagination != nil {
        if req.Pagination.Page > 0 {
            page = req.Pagination.Page
        }
        if req.Pagination.PageSize > 0 && req.Pagination.PageSize <= 100 {
            pageSize = req.Pagination.PageSize
        }
    }
    offset := (page - 1) * pageSize

    // ==================== 2. 查询总数 — PgSQL ====================
    var total int64
    err := s.db.QueryRowContext(ctx,
        `SELECT COUNT(*) FROM friend_requests WHERE to_user_id = $1`,
        req.UserId,
    ).Scan(&total)
    if err != nil {
        return nil, status.Error(codes.Internal, "count friend requests failed")
    }

    // ==================== 3. 分页查询申请列表 — PgSQL ====================
    rows, err := s.db.QueryContext(ctx,
        `SELECT request_id, from_user_id, to_user_id, message, status, source, created_at, handled_at
         FROM friend_requests
         WHERE to_user_id = $1
         ORDER BY created_at DESC
         LIMIT $2 OFFSET $3`,
        req.UserId, pageSize, offset,
    )
    if err != nil {
        return nil, status.Error(codes.Internal, "query friend requests failed")
    }
    defer rows.Close()

    var requests []*pb.FriendRequestInfo
    for rows.Next() {
        var r pb.FriendRequestInfo
        if err := rows.Scan(&r.RequestId, &r.FromUserId, &r.ToUserId, &r.Message,
            &r.Status, &r.Source, &r.CreatedAt, &r.HandledAt); err != nil {
            continue
        }
        requests = append(requests, &r)
    }

    // ==================== 4. 批量补充用户摘要信息 — RPC User.GetUser ====================
    for _, r := range requests {
        // 查询申请者信息
        fromResp, err := s.userClient.GetUser(ctx, &user_pb.GetUserRequest{UserId: r.FromUserId})
        if err == nil {
            r.FromUser = &common.UserBrief{
                UserId:    fromResp.User.UserId,
                Nickname:  fromResp.User.Nickname,
                AvatarUrl: fromResp.User.AvatarUrl,
            }
        }
        // 查询接收者信息
        toResp, err := s.userClient.GetUser(ctx, &user_pb.GetUserRequest{UserId: r.ToUserId})
        if err == nil {
            r.ToUser = &common.UserBrief{
                UserId:    toResp.User.UserId,
                Nickname:  toResp.User.Nickname,
                AvatarUrl: toResp.User.AvatarUrl,
            }
        }
    }

    totalPages := int32(0)
    if total > 0 {
        totalPages = int32((total + int64(pageSize) - 1) / int64(pageSize))
    }

    return &pb.GetFriendRequestsResponse{
        Meta:     successMeta(ctx),
        Requests: requests,
        Pagination: &common.PaginationResult{
            Page: page, PageSize: pageSize, Total: total, TotalPages: totalPages,
        },
    }, nil
}
```

### 4. GetFriendList — 获取好友列表

```go
func (s *RelationService) GetFriendList(ctx context.Context, req *pb.GetFriendListRequest) (*pb.GetFriendListResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id is required")
    }
    page := int32(1)
    pageSize := int32(50)
    if req.Pagination != nil {
        if req.Pagination.Page > 0 {
            page = req.Pagination.Page
        }
        if req.Pagination.PageSize > 0 && req.Pagination.PageSize <= 200 {
            pageSize = req.Pagination.PageSize
        }
    }
    offset := (page - 1) * pageSize

    // ==================== 2. 查询总数 — PgSQL ====================
    var total int64
    countSQL := `SELECT COUNT(*) FROM friends WHERE user_id = $1`
    args := []interface{}{req.UserId}
    if req.FriendGroupId != "" {
        countSQL = `SELECT COUNT(*) FROM friends WHERE user_id = $1 AND friend_group_id = $2`
        args = append(args, req.FriendGroupId)
    }
    err := s.db.QueryRowContext(ctx, countSQL, args...).Scan(&total)
    if err != nil {
        return nil, status.Error(codes.Internal, "count friends failed")
    }

    // ==================== 3. 分页查询好友列表 — PgSQL ====================
    querySQL := `SELECT friend_id, remark, source, friend_group_id, created_at
                 FROM friends WHERE user_id = $1`
    queryArgs := []interface{}{req.UserId}
    argIdx := 2

    if req.FriendGroupId != "" {
        querySQL += fmt.Sprintf(" AND friend_group_id = $%d", argIdx)
        queryArgs = append(queryArgs, req.FriendGroupId)
        argIdx++
    }

    querySQL += fmt.Sprintf(" ORDER BY created_at DESC LIMIT $%d OFFSET $%d", argIdx, argIdx+1)
    queryArgs = append(queryArgs, pageSize, offset)

    rows, err := s.db.QueryContext(ctx, querySQL, queryArgs...)
    if err != nil {
        return nil, status.Error(codes.Internal, "query friends failed")
    }
    defer rows.Close()

    var friends []*pb.FriendInfo
    for rows.Next() {
        var f pb.FriendInfo
        if err := rows.Scan(&f.UserId, &f.Remark, &f.Source, &f.FriendGroupId, &f.CreatedAt); err != nil {
            continue
        }
        friends = append(friends, &f)
    }

    // ==================== 4. 批量补充好友昵称/头像 — RPC User.GetUser ====================
    for _, f := range friends {
        userResp, err := s.userClient.GetUser(ctx, &user_pb.GetUserRequest{UserId: f.UserId})
        if err == nil {
            f.Nickname = userResp.User.Nickname
            f.AvatarUrl = userResp.User.AvatarUrl
        }
    }

    totalPages := int32(0)
    if total > 0 {
        totalPages = int32((total + int64(pageSize) - 1) / int64(pageSize))
    }

    return &pb.GetFriendListResponse{
        Meta:    successMeta(ctx),
        Friends: friends,
        Pagination: &common.PaginationResult{
            Page: page, PageSize: pageSize, Total: total, TotalPages: totalPages,
        },
    }, nil
}
```

### 5. DeleteFriend — 删除好友

```go
func (s *RelationService) DeleteFriend(ctx context.Context, req *pb.DeleteFriendRequest) (*pb.DeleteFriendResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id is required")
    }
    if req.FriendId == "" {
        return nil, status.Error(codes.InvalidArgument, "friend_id is required")
    }
    if req.UserId == req.FriendId {
        return nil, status.Error(codes.InvalidArgument, "cannot delete yourself")
    }

    // ==================== 2. 校验好友关系是否存在 — PgSQL ====================
    var exists bool
    err := s.db.QueryRowContext(ctx,
        `SELECT EXISTS(SELECT 1 FROM friends WHERE user_id = $1 AND friend_id = $2)`,
        req.UserId, req.FriendId,
    ).Scan(&exists)
    if err != nil {
        return nil, status.Error(codes.Internal, "check friendship failed")
    }
    if !exists {
        return nil, status.Error(codes.NotFound, "friend relationship not found")
    }

    // ==================== 3. 双向删除好友关系 — PgSQL 事务 ====================
    tx, err := s.db.BeginTx(ctx, nil)
    if err != nil {
        return nil, status.Error(codes.Internal, "begin tx failed")
    }
    defer tx.Rollback()

    // 删除 A→B
    _, err = tx.ExecContext(ctx,
        `DELETE FROM friends WHERE user_id = $1 AND friend_id = $2`,
        req.UserId, req.FriendId,
    )
    if err != nil {
        return nil, status.Error(codes.Internal, "delete friend A->B failed")
    }

    // 删除 B→A
    _, err = tx.ExecContext(ctx,
        `DELETE FROM friends WHERE user_id = $1 AND friend_id = $2`,
        req.FriendId, req.UserId,
    )
    if err != nil {
        return nil, status.Error(codes.Internal, "delete friend B->A failed")
    }

    if err = tx.Commit(); err != nil {
        return nil, status.Error(codes.Internal, "commit tx failed")
    }

    // ==================== 4. 延迟双删清除 Redis 缓存 ====================
    s.delayedDoubleDelete(ctx,
        fmt.Sprintf("rel:friends:%s", req.UserId),
        fmt.Sprintf("rel:friends:%s", req.FriendId),
        fmt.Sprintf("rel:is_friend:%s:%s", req.UserId, req.FriendId),
        fmt.Sprintf("rel:is_friend:%s:%s", req.FriendId, req.UserId),
        fmt.Sprintf("rel:friend_count:%s", req.UserId),
        fmt.Sprintf("rel:friend_count:%s", req.FriendId),
    )

    // ==================== 5. 生产 Kafka 事件: relation.friend.deleted ====================
    now := time.Now().UnixMilli()
    deleteEvent := &kafka_relation.FriendDeletedEvent{
        Header:     buildEventHeader("relation", s.instanceID),
        OperatorId: req.UserId,
        TargetId:   req.FriendId,
        DeleteTime: now,
    }
    s.kafka.Produce(ctx, "relation.friend.deleted", req.UserId, deleteEvent)

    return &pb.DeleteFriendResponse{Meta: successMeta(ctx)}, nil
}
```

### 6. SetFriendRemark — 设置好友备注

```go
func (s *RelationService) SetFriendRemark(ctx context.Context, req *pb.SetFriendRemarkRequest) (*pb.SetFriendRemarkResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id is required")
    }
    if req.FriendId == "" {
        return nil, status.Error(codes.InvalidArgument, "friend_id is required")
    }
    if len(req.Remark) > 128 {
        return nil, status.Error(codes.InvalidArgument, "remark too long, max 128 chars")
    }

    // ==================== 2. 查询旧备注 — PgSQL ====================
    var oldRemark string
    err := s.db.QueryRowContext(ctx,
        `SELECT remark FROM friends WHERE user_id = $1 AND friend_id = $2`,
        req.UserId, req.FriendId,
    ).Scan(&oldRemark)
    if err == sql.ErrNoRows {
        return nil, status.Error(codes.NotFound, "friend relationship not found")
    }
    if err != nil {
        return nil, status.Error(codes.Internal, "query friend remark failed")
    }

    // ==================== 3. 更新备注 — PgSQL ====================
    _, err = s.db.ExecContext(ctx,
        `UPDATE friends SET remark = $1 WHERE user_id = $2 AND friend_id = $3`,
        req.Remark, req.UserId, req.FriendId,
    )
    if err != nil {
        return nil, status.Error(codes.Internal, "update remark failed")
    }

    // ==================== 3a. 延迟双删清除 Redis 缓存 ====================
    s.delayedDoubleDelete(ctx,
        fmt.Sprintf("rel:friends:%s", req.UserId),
    )

    // ==================== 4. 生产 Kafka 事件: relation.remark.updated ====================
    now := time.Now().UnixMilli()
    remarkEvent := &kafka_relation.RemarkUpdatedEvent{
        Header:     buildEventHeader("relation", s.instanceID),
        UserId:     req.UserId,
        FriendId:   req.FriendId,
        OldRemark:  oldRemark,
        NewRemark:  req.Remark,
        UpdateTime: now,
    }
    s.kafka.Produce(ctx, "relation.remark.updated", req.UserId, remarkEvent)

    return &pb.SetFriendRemarkResponse{Meta: successMeta(ctx)}, nil
}
```

### 7. CheckFriendship — 检查好友关系

```go
func (s *RelationService) CheckFriendship(ctx context.Context, req *pb.CheckFriendshipRequest) (*pb.CheckFriendshipResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id is required")
    }
    if req.TargetUserId == "" {
        return nil, status.Error(codes.InvalidArgument, "target_user_id is required")
    }

    // ==================== 2. 检查是否被对方拉黑 — Redis 缓存优先 ====================
    blockedByKey := fmt.Sprintf("rel:is_blocked:%s:%s", req.TargetUserId, req.UserId)
    blockedByVal, err := s.redis.Get(ctx, blockedByKey).Result()
    if err == nil && blockedByVal == "1" {
        return &pb.CheckFriendshipResponse{
            Meta:   successMeta(ctx),
            Status: common.RELATION_STATUS_BLOCKED_BY,
        }, nil
    }

    // ==================== 3. 检查是否拉黑对方 — Redis 缓存优先 ====================
    blockedKey := fmt.Sprintf("rel:is_blocked:%s:%s", req.UserId, req.TargetUserId)
    blockedVal, err := s.redis.Get(ctx, blockedKey).Result()
    if err == nil && blockedVal == "1" {
        return &pb.CheckFriendshipResponse{
            Meta:   successMeta(ctx),
            Status: common.RELATION_STATUS_BLOCKED,
        }, nil
    }

    // ==================== 4. 检查好友关系 — Redis 缓存优先 ====================
    friendKey := fmt.Sprintf("rel:is_friend:%s:%s", req.UserId, req.TargetUserId)
    friendVal, err := s.redis.Get(ctx, friendKey).Result()
    if err == nil {
        if friendVal == "1" {
            return &pb.CheckFriendshipResponse{
                Meta: successMeta(ctx), Status: common.RELATION_STATUS_FRIEND,
            }, nil
        }
        return &pb.CheckFriendshipResponse{
            Meta: successMeta(ctx), Status: common.RELATION_STATUS_NONE,
        }, nil
    }

    // ==================== 5. 缓存未命中，查 PgSQL ====================
    relationStatus := s.queryRelationStatusFromDB(ctx, req.UserId, req.TargetUserId)

    // ==================== 6. 回填 Redis 缓存 ====================
    switch relationStatus {
    case common.RELATION_STATUS_FRIEND:
        s.redis.Set(ctx, friendKey, "1", 10*time.Minute)
    case common.RELATION_STATUS_BLOCKED:
        s.redis.Set(ctx, blockedKey, "1", 10*time.Minute)
    case common.RELATION_STATUS_BLOCKED_BY:
        s.redis.Set(ctx, blockedByKey, "1", 10*time.Minute)
    default:
        s.redis.Set(ctx, friendKey, "0", 10*time.Minute)
    }

    return &pb.CheckFriendshipResponse{
        Meta:   successMeta(ctx),
        Status: relationStatus,
    }, nil
}

// queryRelationStatusFromDB — 从数据库查询两人关系状态
func (s *RelationService) queryRelationStatusFromDB(ctx context.Context, userID, targetID string) common.RelationStatus {
    // 检查是否拉黑对方
    var blocked bool
    s.db.QueryRowContext(ctx,
        `SELECT EXISTS(SELECT 1 FROM blocks WHERE user_id = $1 AND blocked_id = $2)`,
        userID, targetID,
    ).Scan(&blocked)
    if blocked {
        return common.RELATION_STATUS_BLOCKED
    }

    // 检查是否被对方拉黑
    var blockedBy bool
    s.db.QueryRowContext(ctx,
        `SELECT EXISTS(SELECT 1 FROM blocks WHERE user_id = $1 AND blocked_id = $2)`,
        targetID, userID,
    ).Scan(&blockedBy)
    if blockedBy {
        return common.RELATION_STATUS_BLOCKED_BY
    }

    // 检查好友关系
    var isFriend bool
    s.db.QueryRowContext(ctx,
        `SELECT EXISTS(SELECT 1 FROM friends WHERE user_id = $1 AND friend_id = $2)`,
        userID, targetID,
    ).Scan(&isFriend)
    if isFriend {
        return common.RELATION_STATUS_FRIEND
    }

    return common.RELATION_STATUS_NONE
}
```

### 8. BatchCheckFriendship — 批量检查好友关系

```go
func (s *RelationService) BatchCheckFriendship(ctx context.Context, req *pb.BatchCheckFriendshipRequest) (*pb.BatchCheckFriendshipResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id is required")
    }
    if len(req.TargetUserIds) == 0 {
        return nil, status.Error(codes.InvalidArgument, "target_user_ids is required")
    }
    if len(req.TargetUserIds) > 200 {
        return nil, status.Error(codes.InvalidArgument, "target_user_ids max 200")
    }

    // ==================== 2. 先尝试 Redis 批量查询 ====================
    results := make([]*pb.FriendshipResult, 0, len(req.TargetUserIds))
    missedTargets := make([]string, 0) // 缓存未命中的目标

    pipe := s.redis.Pipeline()
    friendCmds := make(map[string]*redis.StringCmd, len(req.TargetUserIds))
    for _, targetID := range req.TargetUserIds {
        key := fmt.Sprintf("rel:is_friend:%s:%s", req.UserId, targetID)
        friendCmds[targetID] = pipe.Get(ctx, key)
    }
    pipe.Exec(ctx)

    for _, targetID := range req.TargetUserIds {
        val, err := friendCmds[targetID].Result()
        if err == nil {
            // 缓存命中
            st := common.RELATION_STATUS_NONE
            if val == "1" {
                st = common.RELATION_STATUS_FRIEND
            }
            results = append(results, &pb.FriendshipResult{
                TargetUserId: targetID, Status: st,
            })
        } else {
            // 缓存未命中
            missedTargets = append(missedTargets, targetID)
        }
    }

    // ==================== 3. 对缓存未命中的批量查 PgSQL ====================
    if len(missedTargets) > 0 {
        // 批量查询好友关系
        friendSet := make(map[string]bool)
        rows, err := s.db.QueryContext(ctx,
            `SELECT friend_id FROM friends WHERE user_id = $1 AND friend_id = ANY($2)`,
            req.UserId, pq.Array(missedTargets),
        )
        if err == nil {
            defer rows.Close()
            for rows.Next() {
                var fid string
                rows.Scan(&fid)
                friendSet[fid] = true
            }
        }

        // 批量查询黑名单
        blockSet := make(map[string]bool)
        bRows, err := s.db.QueryContext(ctx,
            `SELECT blocked_id FROM blocks WHERE user_id = $1 AND blocked_id = ANY($2)`,
            req.UserId, pq.Array(missedTargets),
        )
        if err == nil {
            defer bRows.Close()
            for bRows.Next() {
                var bid string
                bRows.Scan(&bid)
                blockSet[bid] = true
            }
        }

        // 批量查询被谁拉黑
        blockedBySet := make(map[string]bool)
        bbRows, err := s.db.QueryContext(ctx,
            `SELECT user_id FROM blocks WHERE user_id = ANY($1) AND blocked_id = $2`,
            pq.Array(missedTargets), req.UserId,
        )
        if err == nil {
            defer bbRows.Close()
            for bbRows.Next() {
                var uid string
                bbRows.Scan(&uid)
                blockedBySet[uid] = true
            }
        }

        // 组装结果 + 回填 Redis 缓存
        cachePipe := s.redis.Pipeline()
        for _, targetID := range missedTargets {
            var st common.RelationStatus
            if blockSet[targetID] {
                st = common.RELATION_STATUS_BLOCKED
                cachePipe.Set(ctx, fmt.Sprintf("rel:is_blocked:%s:%s", req.UserId, targetID), "1", 10*time.Minute)
            } else if blockedBySet[targetID] {
                st = common.RELATION_STATUS_BLOCKED_BY
                cachePipe.Set(ctx, fmt.Sprintf("rel:is_blocked:%s:%s", targetID, req.UserId), "1", 10*time.Minute)
            } else if friendSet[targetID] {
                st = common.RELATION_STATUS_FRIEND
                cachePipe.Set(ctx, fmt.Sprintf("rel:is_friend:%s:%s", req.UserId, targetID), "1", 10*time.Minute)
            } else {
                st = common.RELATION_STATUS_NONE
                cachePipe.Set(ctx, fmt.Sprintf("rel:is_friend:%s:%s", req.UserId, targetID), "0", 10*time.Minute)
            }
            results = append(results, &pb.FriendshipResult{
                TargetUserId: targetID, Status: st,
            })
        }
        cachePipe.Exec(ctx)
    }

    return &pb.BatchCheckFriendshipResponse{
        Meta:    successMeta(ctx),
        Results: results,
    }, nil
}
```

### 9. CreateFriendGroup — 创建好友分组

```go
func (s *RelationService) CreateFriendGroup(ctx context.Context, req *pb.CreateFriendGroupRequest) (*pb.CreateFriendGroupResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id is required")
    }
    if req.Name == "" {
        return nil, status.Error(codes.InvalidArgument, "name is required")
    }
    if len(req.Name) > 64 {
        return nil, status.Error(codes.InvalidArgument, "name too long, max 64 chars")
    }

    // ==================== 2. 检查分组名称是否重复 — PgSQL ====================
    var nameExists bool
    err := s.db.QueryRowContext(ctx,
        `SELECT EXISTS(SELECT 1 FROM friend_groups WHERE user_id = $1 AND name = $2)`,
        req.UserId, req.Name,
    ).Scan(&nameExists)
    if err != nil {
        return nil, status.Error(codes.Internal, "check group name failed")
    }
    if nameExists {
        return nil, status.Error(codes.AlreadyExists, "friend group name already exists")
    }

    // ==================== 3. 检查分组数量上限（最多 20 个） ====================
    var groupCount int
    err = s.db.QueryRowContext(ctx,
        `SELECT COUNT(*) FROM friend_groups WHERE user_id = $1`,
        req.UserId,
    ).Scan(&groupCount)
    if err != nil {
        return nil, status.Error(codes.Internal, "count friend groups failed")
    }
    if groupCount >= 20 {
        return nil, status.Error(codes.ResourceExhausted, "friend group limit exceeded, max 20")
    }

    // ==================== 4. 创建分组 — PgSQL ====================
    now := time.Now().UnixMilli()
    groupID := generateGroupID() // UUID v4

    _, err = s.db.ExecContext(ctx,
        `INSERT INTO friend_groups (group_id, user_id, name, sort_order, created_at, updated_at)
         VALUES ($1, $2, $3, $4, $5, $6)`,
        groupID, req.UserId, req.Name, req.SortOrder, now, now,
    )
    if err != nil {
        return nil, status.Error(codes.Internal, "insert friend group failed")
    }

    return &pb.CreateFriendGroupResponse{
        Meta: successMeta(ctx),
        Group: &pb.FriendGroup{
            GroupId:   groupID,
            UserId:    req.UserId,
            Name:      req.Name,
            SortOrder: req.SortOrder,
            Count:     0,
            CreatedAt: now,
            UpdatedAt: now,
        },
    }, nil
}
```

### 10. GetFriendGroups — 获取好友分组列表

```go
func (s *RelationService) GetFriendGroups(ctx context.Context, req *pb.GetFriendGroupsRequest) (*pb.GetFriendGroupsResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id is required")
    }

    // ==================== 2. 查询分组列表 — PgSQL ====================
    rows, err := s.db.QueryContext(ctx,
        `SELECT g.group_id, g.user_id, g.name, g.sort_order, g.created_at, g.updated_at,
                COALESCE(c.cnt, 0) AS count
         FROM friend_groups g
         LEFT JOIN (
             SELECT friend_group_id, COUNT(*) AS cnt
             FROM friends
             WHERE user_id = $1 AND friend_group_id != ''
             GROUP BY friend_group_id
         ) c ON g.group_id = c.friend_group_id
         WHERE g.user_id = $1
         ORDER BY g.sort_order ASC, g.created_at ASC`,
        req.UserId,
    )
    if err != nil {
        return nil, status.Error(codes.Internal, "query friend groups failed")
    }
    defer rows.Close()

    var groups []*pb.FriendGroup
    for rows.Next() {
        var g pb.FriendGroup
        if err := rows.Scan(&g.GroupId, &g.UserId, &g.Name, &g.SortOrder,
            &g.CreatedAt, &g.UpdatedAt, &g.Count); err != nil {
            continue
        }
        groups = append(groups, &g)
    }

    // ==================== 3. 补充"未分组"的好友数量 ====================
    var ungroupedCount int32
    err = s.db.QueryRowContext(ctx,
        `SELECT COUNT(*) FROM friends WHERE user_id = $1 AND friend_group_id = ''`,
        req.UserId,
    ).Scan(&ungroupedCount)
    if err == nil && ungroupedCount > 0 {
        // 在列表头部插入虚拟的"未分组"
        ungrouped := &pb.FriendGroup{
            GroupId:   "",
            UserId:    req.UserId,
            Name:      "未分组",
            SortOrder: -1,
            Count:     ungroupedCount,
        }
        groups = append([]*pb.FriendGroup{ungrouped}, groups...)
    }

    return &pb.GetFriendGroupsResponse{
        Meta:   successMeta(ctx),
        Groups: groups,
    }, nil
}
```

### 11. UpdateFriendGroup — 更新好友分组

```go
func (s *RelationService) UpdateFriendGroup(ctx context.Context, req *pb.UpdateFriendGroupRequest) (*pb.UpdateFriendGroupResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id is required")
    }
    if req.GroupId == "" {
        return nil, status.Error(codes.InvalidArgument, "group_id is required")
    }
    if req.Name == "" && req.SortOrder == 0 {
        return nil, status.Error(codes.InvalidArgument, "at least one of name or sort_order is required")
    }
    if req.Name != "" && len(req.Name) > 64 {
        return nil, status.Error(codes.InvalidArgument, "name too long, max 64 chars")
    }

    // ==================== 2. 校验分组归属权 — PgSQL ====================
    var ownerID string
    err := s.db.QueryRowContext(ctx,
        `SELECT user_id FROM friend_groups WHERE group_id = $1`,
        req.GroupId,
    ).Scan(&ownerID)
    if err == sql.ErrNoRows {
        return nil, status.Error(codes.NotFound, "friend group not found")
    }
    if err != nil {
        return nil, status.Error(codes.Internal, "query friend group failed")
    }
    if ownerID != req.UserId {
        return nil, status.Error(codes.PermissionDenied, "not your friend group")
    }

    // ==================== 3. 如果改名，检查名称重复 — PgSQL ====================
    if req.Name != "" {
        var nameExists bool
        err = s.db.QueryRowContext(ctx,
            `SELECT EXISTS(SELECT 1 FROM friend_groups WHERE user_id = $1 AND name = $2 AND group_id != $3)`,
            req.UserId, req.Name, req.GroupId,
        ).Scan(&nameExists)
        if err != nil {
            return nil, status.Error(codes.Internal, "check group name failed")
        }
        if nameExists {
            return nil, status.Error(codes.AlreadyExists, "friend group name already exists")
        }
    }

    // ==================== 4. 动态构建更新 SQL — PgSQL ====================
    now := time.Now().UnixMilli()
    setClauses := []string{"updated_at = $1"}
    args := []interface{}{now}
    argIdx := 2

    if req.Name != "" {
        setClauses = append(setClauses, fmt.Sprintf("name = $%d", argIdx))
        args = append(args, req.Name)
        argIdx++
    }
    if req.SortOrder != 0 {
        setClauses = append(setClauses, fmt.Sprintf("sort_order = $%d", argIdx))
        args = append(args, req.SortOrder)
        argIdx++
    }

    args = append(args, req.GroupId)
    updateSQL := fmt.Sprintf(
        `UPDATE friend_groups SET %s WHERE group_id = $%d`,
        strings.Join(setClauses, ", "), argIdx,
    )
    _, err = s.db.ExecContext(ctx, updateSQL, args...)
    if err != nil {
        return nil, status.Error(codes.Internal, "update friend group failed")
    }

    return &pb.UpdateFriendGroupResponse{Meta: successMeta(ctx)}, nil
}
```

### 12. DeleteFriendGroup — 删除好友分组

```go
func (s *RelationService) DeleteFriendGroup(ctx context.Context, req *pb.DeleteFriendGroupRequest) (*pb.DeleteFriendGroupResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id is required")
    }
    if req.GroupId == "" {
        return nil, status.Error(codes.InvalidArgument, "group_id is required")
    }

    // ==================== 2. 校验分组归属权 — PgSQL ====================
    var ownerID string
    err := s.db.QueryRowContext(ctx,
        `SELECT user_id FROM friend_groups WHERE group_id = $1`,
        req.GroupId,
    ).Scan(&ownerID)
    if err == sql.ErrNoRows {
        return nil, status.Error(codes.NotFound, "friend group not found")
    }
    if err != nil {
        return nil, status.Error(codes.Internal, "query friend group failed")
    }
    if ownerID != req.UserId {
        return nil, status.Error(codes.PermissionDenied, "not your friend group")
    }

    // ==================== 3. 事务：将分组内好友移入"未分组" + 删除分组 — PgSQL ====================
    tx, err := s.db.BeginTx(ctx, nil)
    if err != nil {
        return nil, status.Error(codes.Internal, "begin tx failed")
    }
    defer tx.Rollback()

    // 3a. 将分组内好友的 friend_group_id 置空（移入"未分组"）
    _, err = tx.ExecContext(ctx,
        `UPDATE friends SET friend_group_id = '' WHERE user_id = $1 AND friend_group_id = $2`,
        req.UserId, req.GroupId,
    )
    if err != nil {
        return nil, status.Error(codes.Internal, "move friends to ungrouped failed")
    }

    // 3b. 删除分组记录
    _, err = tx.ExecContext(ctx,
        `DELETE FROM friend_groups WHERE group_id = $1 AND user_id = $2`,
        req.GroupId, req.UserId,
    )
    if err != nil {
        return nil, status.Error(codes.Internal, "delete friend group failed")
    }

    if err = tx.Commit(); err != nil {
        return nil, status.Error(codes.Internal, "commit tx failed")
    }

    return &pb.DeleteFriendGroupResponse{Meta: successMeta(ctx)}, nil
}
```

### 13. MoveFriendToGroup — 移动好友到指定分组

```go
func (s *RelationService) MoveFriendToGroup(ctx context.Context, req *pb.MoveFriendToGroupRequest) (*pb.MoveFriendToGroupResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id is required")
    }
    if req.FriendId == "" {
        return nil, status.Error(codes.InvalidArgument, "friend_id is required")
    }
    // friend_group_id 为空表示移入"未分组"，合法

    // ==================== 2. 校验好友关系存在 — PgSQL ====================
    var exists bool
    err := s.db.QueryRowContext(ctx,
        `SELECT EXISTS(SELECT 1 FROM friends WHERE user_id = $1 AND friend_id = $2)`,
        req.UserId, req.FriendId,
    ).Scan(&exists)
    if err != nil {
        return nil, status.Error(codes.Internal, "check friend exists failed")
    }
    if !exists {
        return nil, status.Error(codes.NotFound, "friend relationship not found")
    }

    // ==================== 3. 如果目标分组非空，校验分组存在且属于该用户 — PgSQL ====================
    if req.FriendGroupId != "" {
        var groupOwner string
        err = s.db.QueryRowContext(ctx,
            `SELECT user_id FROM friend_groups WHERE group_id = $1`,
            req.FriendGroupId,
        ).Scan(&groupOwner)
        if err == sql.ErrNoRows {
            return nil, status.Error(codes.NotFound, "target friend group not found")
        }
        if err != nil {
            return nil, status.Error(codes.Internal, "query friend group failed")
        }
        if groupOwner != req.UserId {
            return nil, status.Error(codes.PermissionDenied, "not your friend group")
        }
    }

    // ==================== 4. 更新好友的分组 — PgSQL ====================
    _, err = s.db.ExecContext(ctx,
        `UPDATE friends SET friend_group_id = $1 WHERE user_id = $2 AND friend_id = $3`,
        req.FriendGroupId, req.UserId, req.FriendId,
    )
    if err != nil {
        return nil, status.Error(codes.Internal, "move friend to group failed")
    }

    return &pb.MoveFriendToGroupResponse{Meta: successMeta(ctx)}, nil
}
```

### 14. BlockUser — 拉黑用户

```go
func (s *RelationService) BlockUser(ctx context.Context, req *pb.BlockUserRequest) (*pb.BlockUserResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id is required")
    }
    if req.BlockUserId == "" {
        return nil, status.Error(codes.InvalidArgument, "block_user_id is required")
    }
    if req.UserId == req.BlockUserId {
        return nil, status.Error(codes.InvalidArgument, "cannot block yourself")
    }

    // ==================== 2. 校验目标用户是否存在 — RPC User.GetUser ====================
    _, err := s.userClient.GetUser(ctx, &user_pb.GetUserRequest{UserId: req.BlockUserId})
    if err != nil {
        if st, ok := status.FromError(err); ok && st.Code() == codes.NotFound {
            return nil, status.Error(codes.NotFound, "target user not found")
        }
        return nil, status.Error(codes.Internal, "get target user failed")
    }

    // ==================== 3. 检查是否已拉黑 — PgSQL ====================
    var alreadyBlocked bool
    err = s.db.QueryRowContext(ctx,
        `SELECT EXISTS(SELECT 1 FROM blocks WHERE user_id = $1 AND blocked_id = $2)`,
        req.UserId, req.BlockUserId,
    ).Scan(&alreadyBlocked)
    if err != nil {
        return nil, status.Error(codes.Internal, "check block status failed")
    }
    if alreadyBlocked {
        return nil, status.Error(codes.AlreadyExists, "user already blocked")
    }

    now := time.Now().UnixMilli()

    // ==================== 4. 事务：拉黑 + 删除好友关系（如果存在） — PgSQL ====================
    tx, err := s.db.BeginTx(ctx, nil)
    if err != nil {
        return nil, status.Error(codes.Internal, "begin tx failed")
    }
    defer tx.Rollback()

    // 4a. 插入黑名单记录
    _, err = tx.ExecContext(ctx,
        `INSERT INTO blocks (user_id, blocked_id, created_at)
         VALUES ($1, $2, $3)
         ON CONFLICT (user_id, blocked_id) DO NOTHING`,
        req.UserId, req.BlockUserId, now,
    )
    if err != nil {
        return nil, status.Error(codes.Internal, "insert block record failed")
    }

    // 4b. 删除双向好友关系（如果存在）
    _, err = tx.ExecContext(ctx,
        `DELETE FROM friends WHERE (user_id = $1 AND friend_id = $2) OR (user_id = $2 AND friend_id = $1)`,
        req.UserId, req.BlockUserId,
    )
    if err != nil {
        return nil, status.Error(codes.Internal, "delete friend for block failed")
    }

    if err = tx.Commit(); err != nil {
        return nil, status.Error(codes.Internal, "commit tx failed")
    }

    // ==================== 5. 延迟双删清除 Redis 缓存 ====================
    s.delayedDoubleDelete(ctx,
        fmt.Sprintf("rel:friends:%s", req.UserId),
        fmt.Sprintf("rel:friends:%s", req.BlockUserId),
        fmt.Sprintf("rel:is_friend:%s:%s", req.UserId, req.BlockUserId),
        fmt.Sprintf("rel:is_friend:%s:%s", req.BlockUserId, req.UserId),
        fmt.Sprintf("rel:friend_count:%s", req.UserId),
        fmt.Sprintf("rel:friend_count:%s", req.BlockUserId),
        fmt.Sprintf("rel:blocks:%s", req.UserId),
        fmt.Sprintf("rel:is_blocked:%s:%s", req.UserId, req.BlockUserId),
    )

    // ==================== 6. 生产 Kafka 事件: relation.blocked ====================
    blockEvent := &kafka_relation.UserBlockedEvent{
        Header:     buildEventHeader("relation", s.instanceID),
        OperatorId: req.UserId,
        BlockedId:  req.BlockUserId,
        BlockTime:  now,
    }
    s.kafka.Produce(ctx, "relation.blocked", req.UserId, blockEvent)

    // ==================== 7. 如果之前是好友，还需生产 relation.friend.deleted ====================
    // （在事务中已删除，这里额外生产删除事件通知下游）
    deleteEvent := &kafka_relation.FriendDeletedEvent{
        Header:     buildEventHeader("relation", s.instanceID),
        OperatorId: req.UserId,
        TargetId:   req.BlockUserId,
        DeleteTime: now,
    }
    s.kafka.Produce(ctx, "relation.friend.deleted", req.UserId, deleteEvent)

    return &pb.BlockUserResponse{Meta: successMeta(ctx)}, nil
}
```

### 15. UnblockUser — 取消拉黑

```go
func (s *RelationService) UnblockUser(ctx context.Context, req *pb.UnblockUserRequest) (*pb.UnblockUserResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id is required")
    }
    if req.BlockUserId == "" {
        return nil, status.Error(codes.InvalidArgument, "block_user_id is required")
    }

    // ==================== 2. 删除黑名单记录 — PgSQL ====================
    result, err := s.db.ExecContext(ctx,
        `DELETE FROM blocks WHERE user_id = $1 AND blocked_id = $2`,
        req.UserId, req.BlockUserId,
    )
    if err != nil {
        return nil, status.Error(codes.Internal, "delete block record failed")
    }
    affected, _ := result.RowsAffected()
    if affected == 0 {
        return nil, status.Error(codes.NotFound, "block record not found")
    }

    // ==================== 3. 延迟双删清除 Redis 缓存 ====================
    s.delayedDoubleDelete(ctx,
        fmt.Sprintf("rel:blocks:%s", req.UserId),
        fmt.Sprintf("rel:is_blocked:%s:%s", req.UserId, req.BlockUserId),
    )

    // ==================== 4. 生产 Kafka 事件: relation.unblocked ====================
    now := time.Now().UnixMilli()
    unblockEvent := &kafka_relation.UserUnblockedEvent{
        Header:      buildEventHeader("relation", s.instanceID),
        OperatorId:  req.UserId,
        BlockedId:   req.BlockUserId,
        UnblockTime: now,
    }
    s.kafka.Produce(ctx, "relation.unblocked", req.UserId, unblockEvent)

    return &pb.UnblockUserResponse{Meta: successMeta(ctx)}, nil
}
```

### 16. GetBlockList — 获取黑名单列表

```go
func (s *RelationService) GetBlockList(ctx context.Context, req *pb.GetBlockListRequest) (*pb.GetBlockListResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id is required")
    }
    page := int32(1)
    pageSize := int32(20)
    if req.Pagination != nil {
        if req.Pagination.Page > 0 {
            page = req.Pagination.Page
        }
        if req.Pagination.PageSize > 0 && req.Pagination.PageSize <= 100 {
            pageSize = req.Pagination.PageSize
        }
    }
    offset := (page - 1) * pageSize

    // ==================== 2. 查询总数 — PgSQL ====================
    var total int64
    err := s.db.QueryRowContext(ctx,
        `SELECT COUNT(*) FROM blocks WHERE user_id = $1`,
        req.UserId,
    ).Scan(&total)
    if err != nil {
        return nil, status.Error(codes.Internal, "count blocks failed")
    }

    // ==================== 3. 分页查询黑名单 — PgSQL ====================
    rows, err := s.db.QueryContext(ctx,
        `SELECT blocked_id, created_at FROM blocks
         WHERE user_id = $1
         ORDER BY created_at DESC
         LIMIT $2 OFFSET $3`,
        req.UserId, pageSize, offset,
    )
    if err != nil {
        return nil, status.Error(codes.Internal, "query blocks failed")
    }
    defer rows.Close()

    var blocks []*pb.BlockInfo
    for rows.Next() {
        var b pb.BlockInfo
        if err := rows.Scan(&b.UserId, &b.BlockedAt); err != nil {
            continue
        }
        blocks = append(blocks, &b)
    }

    // ==================== 4. 批量补充用户昵称/头像 — RPC User.GetUser ====================
    for _, b := range blocks {
        userResp, err := s.userClient.GetUser(ctx, &user_pb.GetUserRequest{UserId: b.UserId})
        if err == nil {
            b.Nickname = userResp.User.Nickname
            b.AvatarUrl = userResp.User.AvatarUrl
        }
    }

    totalPages := int32(0)
    if total > 0 {
        totalPages = int32((total + int64(pageSize) - 1) / int64(pageSize))
    }

    return &pb.GetBlockListResponse{
        Meta:   successMeta(ctx),
        Blocks: blocks,
        Pagination: &common.PaginationResult{
            Page: page, PageSize: pageSize, Total: total, TotalPages: totalPages,
        },
    }, nil
}
```

### 17. CheckBlockship — 检查黑名单关系

```go
func (s *RelationService) CheckBlockship(ctx context.Context, req *pb.CheckBlockshipRequest) (*pb.CheckBlockshipResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id is required")
    }
    if req.TargetId == "" {
        return nil, status.Error(codes.InvalidArgument, "target_id is required")
    }

    // ==================== 2. 委托内部方法查询（Redis 优先 → PgSQL 回源） ====================
    isBlocked, err := s.checkBlockedInternal(ctx, req.UserId, req.TargetId)
    if err != nil {
        return nil, status.Error(codes.Internal, "check blockship failed: "+err.Error())
    }

    // ==================== 3. 返回 ====================
    return &pb.CheckBlockshipResponse{
        Meta:      successMeta(ctx),
        IsBlocked: isBlocked,
    }, nil
}
```

---

## 内部辅助方法

### checkFriendshipInternal — 内部好友关系检查（缓存优先）

```go
func (s *RelationService) checkFriendshipInternal(ctx context.Context, userID, targetID string) (bool, error) {
    // 1. Redis 缓存查询
    key := fmt.Sprintf("rel:is_friend:%s:%s", userID, targetID)
    val, err := s.redis.Get(ctx, key).Result()
    if err == nil {
        return val == "1", nil
    }

    // 2. PgSQL 查询
    var isFriend bool
    err = s.db.QueryRowContext(ctx,
        `SELECT EXISTS(SELECT 1 FROM friends WHERE user_id = $1 AND friend_id = $2)`,
        userID, targetID,
    ).Scan(&isFriend)
    if err != nil {
        return false, err
    }

    // 3. 回填 Redis 缓存
    if isFriend {
        s.redis.Set(ctx, key, "1", 10*time.Minute)
    } else {
        s.redis.Set(ctx, key, "0", 10*time.Minute)
    }

    return isFriend, nil
}
```

### checkBlockedInternal — 内部黑名单检查（缓存优先）

```go
func (s *RelationService) checkBlockedInternal(ctx context.Context, userID, blockedID string) (bool, error) {
    // 1. Redis 缓存查询
    key := fmt.Sprintf("rel:is_blocked:%s:%s", userID, blockedID)
    val, err := s.redis.Get(ctx, key).Result()
    if err == nil {
        return val == "1", nil
    }

    // 2. PgSQL 查询
    var isBlocked bool
    err = s.db.QueryRowContext(ctx,
        `SELECT EXISTS(SELECT 1 FROM blocks WHERE user_id = $1 AND blocked_id = $2)`,
        userID, blockedID,
    ).Scan(&isBlocked)
    if err != nil {
        return false, err
    }

    // 3. 回填 Redis 缓存
    if isBlocked {
        s.redis.Set(ctx, key, "1", 10*time.Minute)
    } else {
        s.redis.Set(ctx, key, "0", 10*time.Minute)
    }

    return isBlocked, nil
}
```

### delayedDoubleDelete — 延迟双删工具方法

```go
// delayedDoubleDelete 延迟双删：保证缓存与 DB 的最终一致性
func (s *RelationService) delayedDoubleDelete(ctx context.Context, keys ...string) {
    if len(keys) == 0 { return }
    pipe := s.redis.Pipeline()
    for _, key := range keys {
        pipe.Del(ctx, key)
    }
    pipe.Exec(ctx)
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

## 可观测性接入（OpenTelemetry）

> Relation 服务负责好友关系与黑名单管理，需重点观测：好友请求/同意/拒绝速率、拉黑/取消拉黑速率、好友列表查询延迟、缓存命中率、双删操作次数。

### 第一步：服务启动时初始化 OTel SDK

```go
func main() {
    ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
    defer cancel()

    otelShutdown := observability.InitOpenTelemetry(ctx, "relation", "v1.0.0")
    defer otelShutdown(ctx)
    observability.SetupLogger("relation")
}
```

### 第二步：gRPC Server/Client Interceptor

```go
grpcServer := grpc.NewServer(
    grpc.ChainUnaryInterceptor(otelgrpc.UnaryServerInterceptor()),
    grpc.ChainStreamInterceptor(otelgrpc.StreamServerInterceptor()),
)
// Client — 调用 User.GetUser / Notification.SendSystemNotification / Conversation.CreateChannel
userConn := grpc.Dial("user:50051",
    grpc.WithChainUnaryInterceptor(otelgrpc.UnaryClientInterceptor()),
)
notifConn := grpc.Dial("notification:50051",
    grpc.WithChainUnaryInterceptor(otelgrpc.UnaryClientInterceptor()),
)
```

### 第三步：注册 Relation 专属业务指标

```go
var meter = otel.Meter("im-chat/relation")

var (
    // 好友请求计数
    friendRequestSent, _ = meter.Int64Counter("relation.friend_request_sent_total",
        metric.WithDescription("好友请求发送总数"))

    // 好友请求处理结果
    friendRequestHandled, _ = meter.Int64Counter("relation.friend_request_handled_total",
        metric.WithDescription("好友请求处理总数（同意/拒绝）"))

    // 拉黑操作计数
    blockOps, _ = meter.Int64Counter("relation.block_ops_total",
        metric.WithDescription("拉黑/取消拉黑操作总数"))

    // 好友列表查询延迟
    friendListQueryDuration, _ = meter.Float64Histogram("relation.friend_list_query_duration_ms",
        metric.WithDescription("好友列表查询延迟"), metric.WithUnit("ms"))

    // 缓存命中率
    relationCacheHit, _ = meter.Int64Counter("relation.cache_hit_total",
        metric.WithDescription("关系缓存命中"))
    relationCacheMiss, _ = meter.Int64Counter("relation.cache_miss_total",
        metric.WithDescription("关系缓存未命中"))

    // 延迟双删触发次数
    doubleDeleteTriggered, _ = meter.Int64Counter("relation.double_delete_triggered_total",
        metric.WithDescription("延迟双删触发总数"))
)
```

在业务代码中埋点：

```go
// SendFriendRequest 中
friendRequestSent.Add(ctx, 1)

// HandleFriendRequest 中
friendRequestHandled.Add(ctx, 1, metric.WithAttributes(
    attribute.String("action", "accepted"),  // accepted / rejected
))

// BlockUser / UnblockUser 中
blockOps.Add(ctx, 1, metric.WithAttributes(
    attribute.String("action", "block"),  // block / unblock
))

// GetFriendList 中
queryStart := time.Now()
// ... 查询 ...
friendListQueryDuration.Record(ctx, float64(time.Since(queryStart).Milliseconds()))

// delayedDoubleDelete 中
doubleDeleteTriggered.Add(ctx, 1)
```

### 第四步：Kafka Producer 埋点 + 结构化 JSON 日志

与所有服务一致。

### 接入要点总结

| 步骤 | 改动位置 | 效果 |
|------|---------|------|
| 1. `observability.InitOpenTelemetry` | main() | TracerProvider + MeterProvider + Runtime Metrics |
| 2. gRPC Interceptor | Server + Client | RPC 自动 trace |
| 3. 自定义业务指标 | FriendRequest/Block/GetFriendList | 关系操作速率、缓存命中率、双删次数 |
| 4. Kafka + 日志 | 事件发布 + setupLogger | 链路追踪 + trace_id 日志 |
| 5. buildEventHeader 改造 | 辅助函数 | EventHeader 携带真实 trace context |
| 6. 基础设施指标 | main() | DB/Redis 连接池可观测 |
| 7. Span 错误记录 | `observability.RecordError` | 错误 trace 在 Tempo 可见 |

### 补充：buildEventHeader 改造

将 `buildEventHeader(source, instanceID)` 替换为 `observability.BuildEventHeader(ctx, source, instanceID)`。

### 补充：基础设施指标注册

```go
observability.RegisterDBPoolMetrics(db, "relation")
observability.RegisterRedisPoolMetrics(redisClient, "relation")
```

### 补充：Span 错误记录

```go
func (s *RelationServer) SendFriendRequest(ctx context.Context, req *pb.SendFriendRequestRequest) (*pb.SendFriendRequestResponse, error) {
    ctx, span := otel.Tracer("relation").Start(ctx, "SendFriendRequest")
    defer span.End()

    result, err := s.doSendFriendRequest(ctx, req)
    if err != nil {
        observability.RecordError(span, err)
        return nil, err
    }
    return result, nil
}
```
