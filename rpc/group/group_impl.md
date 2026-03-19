# Group 群组服务 — RPC 接口实现伪代码

## 概述

Group 服务负责群组生命周期管理（创建/解散/转让）、成员管理（邀请/踢出/退出/设置角色/禁言）、群信息维护（名称/公告/头像/设置）、入群审批（主动申请/被邀请）。是 IM 系统中最复杂的核心服务之一。

## 外部依赖

| 依赖类型 | 依赖项 | 用途 |
|---------|--------|------|
| PgSQL | groups 表 | 群组信息持久化 |
| PgSQL | group_members 表 | 群成员关系持久化 |
| PgSQL | group_applications 表 | 入群申请记录持久化 |
| Redis | 群信息缓存 / 成员列表缓存 / 成员信息缓存 / 成员计数缓存 / 用户群列表缓存 | 高频读写 |
| RPC | User.GetUser | 校验用户是否存在 / 获取用户信息 |
| RPC | User.BatchGetUser | 批量获取用户信息 |
| RPC | Conversation.GetOrCreateChannel | 建群时创建群聊频道 |
| RPC | Conversation.CreateUserConversation | 为成员创建会话视图 |
| RPC | Conversation.DeleteChannel | 解散群时删除聊天频道 |
| RPC | Conversation.DeleteUserConversation | 成员退出/被踢时删除会话视图 |
| Kafka | group.created / group.dissolved / group.info.updated / group.member.joined / group.member.left / group.member.kicked / group.member.role.changed / group.member.mute.changed / group.mute.changed / group.transfer / group.application / group.application.handled / group.stats | 事件通知 |

## PgSQL 表结构

```sql
-- 群组表
CREATE TABLE groups (
    id                  BIGSERIAL PRIMARY KEY,
    group_id            VARCHAR(64)  NOT NULL UNIQUE,
    name                VARCHAR(128) NOT NULL DEFAULT '',
    avatar_url          TEXT         NOT NULL DEFAULT '',
    description         VARCHAR(512) DEFAULT '',
    owner_id            VARCHAR(64)  NOT NULL,
    member_count        INT          NOT NULL DEFAULT 0,
    max_members         INT          NOT NULL DEFAULT 500,
    status              SMALLINT     NOT NULL DEFAULT 1,  -- 1=正常 2=全员禁言 3=已解散
    announcement        TEXT         DEFAULT '',
    join_need_approve   BOOLEAN      DEFAULT true,
    invite_need_approve BOOLEAN      DEFAULT false,
    extra               JSONB        DEFAULT '{}',
    created_at          BIGINT       NOT NULL,
    updated_at          BIGINT       NOT NULL
);
CREATE INDEX idx_groups_owner ON groups(owner_id);
CREATE INDEX idx_groups_status ON groups(status) WHERE status != 3;

-- 群成员表
CREATE TABLE group_members (
    id        BIGSERIAL PRIMARY KEY,
    group_id  VARCHAR(64) NOT NULL,
    user_id   VARCHAR(64) NOT NULL,
    nickname  VARCHAR(128) NOT NULL DEFAULT '',
    role      SMALLINT     NOT NULL DEFAULT 3,  -- 1=群主 2=管理员 3=普通成员
    is_muted  BOOLEAN      NOT NULL DEFAULT false,
    mute_until BIGINT      NOT NULL DEFAULT 0,
    joined_at  BIGINT      NOT NULL,
    invite_by  VARCHAR(64) NOT NULL DEFAULT '',
    UNIQUE(group_id, user_id)
);
CREATE INDEX idx_group_members_group ON group_members(group_id);
CREATE INDEX idx_group_members_user ON group_members(user_id);
CREATE INDEX idx_group_members_role ON group_members(group_id, role);

-- 群申请表
CREATE TABLE group_applications (
    id             BIGSERIAL PRIMARY KEY,
    application_id VARCHAR(64)  NOT NULL UNIQUE,
    group_id       VARCHAR(64)  NOT NULL,
    user_id        VARCHAR(64)  NOT NULL,
    type           SMALLINT     NOT NULL DEFAULT 1,  -- 1=主动申请 2=被邀请
    status         SMALLINT     NOT NULL DEFAULT 1,  -- 1=待处理 2=已通过 3=已拒绝 4=已过期
    message        VARCHAR(256) NOT NULL DEFAULT '',
    operator_id    VARCHAR(64)  NOT NULL DEFAULT '',
    handle_time    BIGINT       NOT NULL DEFAULT 0,
    created_at     BIGINT       NOT NULL
);
CREATE INDEX idx_group_applications_group ON group_applications(group_id, status);
CREATE INDEX idx_group_applications_user ON group_applications(user_id, status);
```

## Redis Key 设计

| Key 格式 | 类型 | 值 | TTL | 说明 |
|----------|------|-----|-----|------|
| `group:info:{group_id}` | HASH | 群组信息各字段 | 30min | 群基本信息缓存 |
| `group:members:{group_id}` | SET | user_id 集合 | 30min | 群成员 ID 集合缓存 |
| `group:member:{group_id}:{user_id}` | HASH | 成员信息各字段 | 10min | 单个成员信息缓存 |
| `group:member_count:{group_id}` | STRING | 成员数量 | 30min | 群成员计数缓存 |
| `group:user_groups:{user_id}` | SET | group_id 集合 | 30min | 用户所在群列表缓存 |
| `group:kafka:dedup:{event_id}` | STRING | "1" | 24h | Kafka 消费幂等去重 |

---

## 接口实现

### 1. CreateGroup — 创建群组

```go
func (s *GroupService) CreateGroup(ctx context.Context, req *pb.CreateGroupRequest) (*pb.CreateGroupResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.OwnerId == "" {
        return nil, status.Error(codes.InvalidArgument, "owner_id is required")
    }
    if req.Name == "" {
        return nil, status.Error(codes.InvalidArgument, "name is required")
    }
    if len(req.Name) > 128 {
        return nil, status.Error(codes.InvalidArgument, "name too long, max 128 chars")
    }
    if len(req.Description) > 512 {
        return nil, status.Error(codes.InvalidArgument, "description too long, max 512 chars")
    }
    if len(req.MemberIds) > 499 {
        return nil, status.Error(codes.InvalidArgument, "initial member count exceeds limit (max 499 + owner)")
    }

    // ==================== 2. 校验创建者是否存在 — RPC User.GetUser ====================
    ownerResp, err := s.userClient.GetUser(ctx, &user_pb.GetUserRequest{UserId: req.OwnerId})
    if err != nil {
        if st, ok := status.FromError(err); ok && st.Code() == codes.NotFound {
            return nil, status.Error(codes.NotFound, "owner user not found")
        }
        return nil, status.Error(codes.Internal, "get owner user failed")
    }

    // ==================== 3. 批量校验初始成员是否存在 — RPC User.BatchGetUser ====================
    validMemberIDs := make([]string, 0, len(req.MemberIds))
    if len(req.MemberIds) > 0 {
        // 去重 + 排除群主
        memberSet := make(map[string]bool)
        for _, uid := range req.MemberIds {
            if uid != req.OwnerId && !memberSet[uid] {
                memberSet[uid] = true
                validMemberIDs = append(validMemberIDs, uid)
            }
        }

        batchResp, err := s.userClient.BatchGetUser(ctx, &user_pb.BatchGetUserRequest{
            UserIds: validMemberIDs,
        })
        if err != nil {
            return nil, status.Error(codes.Internal, "batch get users failed")
        }
        // 过滤掉不存在的用户
        existingUsers := make(map[string]bool)
        for _, u := range batchResp.Users {
            existingUsers[u.UserId] = true
        }
        filtered := make([]string, 0, len(validMemberIDs))
        for _, uid := range validMemberIDs {
            if existingUsers[uid] {
                filtered = append(filtered, uid)
            }
        }
        validMemberIDs = filtered
    }

    now := time.Now().UnixMilli()
    groupID := generateGroupID() // snowflake 或 UUID v4
    memberCount := int32(1 + len(validMemberIDs)) // 群主 + 初始成员

    // ==================== 4. 事务：创建群 + 写入成员 — PgSQL ====================
    tx, err := s.db.BeginTx(ctx, nil)
    if err != nil {
        return nil, status.Error(codes.Internal, "begin tx failed")
    }
    defer tx.Rollback()

    // 4a. 插入群组记录
    _, err = tx.ExecContext(ctx,
        `INSERT INTO groups (group_id, name, avatar_url, description, owner_id, member_count, max_members,
                             status, announcement, join_need_approve, invite_need_approve, extra, created_at, updated_at)
         VALUES ($1, $2, $3, $4, $5, $6, 500, 1, '', true, false, '{}', $7, $7)`,
        groupID, req.Name, req.AvatarUrl, req.Description, req.OwnerId, memberCount, now,
    )
    if err != nil {
        return nil, status.Error(codes.Internal, "insert group failed")
    }

    // 4b. 插入群主为成员（role=1）
    _, err = tx.ExecContext(ctx,
        `INSERT INTO group_members (group_id, user_id, nickname, role, is_muted, mute_until, joined_at, invite_by)
         VALUES ($1, $2, '', 1, false, 0, $3, '')`,
        groupID, req.OwnerId, now,
    )
    if err != nil {
        return nil, status.Error(codes.Internal, "insert owner member failed")
    }

    // 4c. 批量插入初始成员（role=3）
    for _, uid := range validMemberIDs {
        _, err = tx.ExecContext(ctx,
            `INSERT INTO group_members (group_id, user_id, nickname, role, is_muted, mute_until, joined_at, invite_by)
             VALUES ($1, $2, '', 3, false, 0, $3, $4)
             ON CONFLICT (group_id, user_id) DO NOTHING`,
            groupID, uid, now, req.OwnerId,
        )
        if err != nil {
            return nil, status.Error(codes.Internal, "insert initial member failed")
        }
    }

    if err = tx.Commit(); err != nil {
        return nil, status.Error(codes.Internal, "commit tx failed")
    }

    // ==================== 5. 创建群聊频道 — RPC Conversation.GetOrCreateChannel ====================
    allMemberIDs := append([]string{req.OwnerId}, validMemberIDs...)
    channelResp, err := s.conversationClient.GetOrCreateChannel(ctx, &conversation_pb.GetOrCreateChannelRequest{
        Type:             common.CONVERSATION_TYPE_GROUP,
        MemberIds:        allMemberIDs,
        GroupId:          groupID, // 群聊频道 ID = group_id
    })
    if err != nil {
        log.Error("创建群聊频道失败", "group_id", groupID, "err", err)
        // 非致命错误，降级处理；可通过补偿任务修复
    }

    channelID := groupID
    if channelResp != nil && channelResp.Channel != nil {
        channelID = channelResp.Channel.ChannelId // 从 Channel 对象中获取
    }

    // ==================== 6. 为所有成员创建会话视图 — RPC Conversation.CreateUserConversation ====================
    for _, uid := range allMemberIDs {
        go func(userID string) {
            _, err := s.conversationClient.CreateUserConversation(context.Background(), &conversation_pb.CreateUserConversationRequest{
                UserId:           userID,
                PeerId:           groupID,
                ConversationType: common.CONVERSATION_TYPE_GROUP,
            })
            if err != nil {
                log.Error("创建群会话视图失败", "user_id", userID, "group_id", groupID, "err", err)
            }
        }(uid)
    }

    // ==================== 7. 预热 Redis 缓存 ====================
    pipe := s.redis.Pipeline()

    // 群信息缓存
    groupInfoKey := fmt.Sprintf("group:info:%s", groupID)
    pipe.HSet(ctx, groupInfoKey, map[string]interface{}{
        "group_id": groupID, "name": req.Name, "avatar_url": req.AvatarUrl,
        "description": req.Description, "owner_id": req.OwnerId,
        "member_count": memberCount, "max_members": 500, "status": 1,
        "announcement": "", "join_need_approve": "true", "invite_need_approve": "false",
        "created_at": now, "updated_at": now,
    })
    pipe.Expire(ctx, groupInfoKey, 30*time.Minute)

    // 成员集合缓存
    membersKey := fmt.Sprintf("group:members:%s", groupID)
    memberArgs := make([]interface{}, 0, len(allMemberIDs))
    for _, uid := range allMemberIDs {
        memberArgs = append(memberArgs, uid)
    }
    pipe.SAdd(ctx, membersKey, memberArgs...)
    pipe.Expire(ctx, membersKey, 30*time.Minute)

    // 成员计数缓存
    pipe.Set(ctx, fmt.Sprintf("group:member_count:%s", groupID), memberCount, 30*time.Minute)

    // 每个成员的 user_groups 缓存失效
    for _, uid := range allMemberIDs {
        pipe.Del(ctx, fmt.Sprintf("group:user_groups:%s", uid))
    }

    pipe.Exec(ctx)

    // ==================== 8. 生产 Kafka 事件: group.created ====================
    createEvent := &kafka_group.GroupCreatedEvent{
        Header:        buildEventHeader("group", s.instanceID),
        GroupId:       groupID,
        GroupName:     req.Name,
        GroupAvatar:   req.AvatarUrl,
        CreatorId:     req.OwnerId,
        CreatorName:   ownerResp.User.Nickname,
        InitMemberIds: allMemberIDs,
        MemberCount:   memberCount,
        CreateTime:    now,
        Description:   req.Description,
    }
    s.kafka.Produce(ctx, "group.created", groupID, createEvent)

    // ==================== 9. 返回 ====================
    return &pb.CreateGroupResponse{
        Meta: successMeta(ctx),
        Group: &pb.GroupInfo{
            GroupId:           groupID,
            Name:              req.Name,
            AvatarUrl:         req.AvatarUrl,
            Description:       req.Description,
            OwnerId:           req.OwnerId,
            MemberCount:       memberCount,
            MaxMembers:        500,
            Status:            common.GROUP_STATUS_NORMAL,
            Announcement:      "",
            JoinNeedApprove:   true,
            InviteNeedApprove: false,
            CreatedAt:         now,
            UpdatedAt:         now,
        },
    }, nil
}
```

### 2. GetGroup — 获取群组信息

```go
func (s *GroupService) GetGroup(ctx context.Context, req *pb.GetGroupRequest) (*pb.GetGroupResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.GroupId == "" {
        return nil, status.Error(codes.InvalidArgument, "group_id is required")
    }

    // ==================== 2. 尝试从 Redis 缓存获取 ====================
    groupInfoKey := fmt.Sprintf("group:info:%s", req.GroupId)
    cached, err := s.redis.HGetAll(ctx, groupInfoKey).Result()
    if err == nil && len(cached) > 0 {
        group := parseGroupInfoFromCache(cached)
        if group.Status == int32(common.GROUP_STATUS_DISBANDED) {
            return nil, status.Error(codes.NotFound, "group has been disbanded")
        }
        return &pb.GetGroupResponse{
            Meta:  successMeta(ctx),
            Group: group,
        }, nil
    }

    // ==================== 3. 缓存未命中，查 PgSQL ====================
    var g pb.GroupInfo
    var statusVal int32
    var joinApprove, inviteApprove bool
    var extra string
    err = s.db.QueryRowContext(ctx,
        `SELECT group_id, name, avatar_url, description, owner_id, member_count, max_members,
                status, announcement, join_need_approve, invite_need_approve, extra, created_at, updated_at
         FROM groups WHERE group_id = $1`,
        req.GroupId,
    ).Scan(&g.GroupId, &g.Name, &g.AvatarUrl, &g.Description, &g.OwnerId,
        &g.MemberCount, &g.MaxMembers, &statusVal, &g.Announcement,
        &joinApprove, &inviteApprove, &extra, &g.CreatedAt, &g.UpdatedAt)
    if err == sql.ErrNoRows {
        return nil, status.Error(codes.NotFound, "group not found")
    }
    if err != nil {
        return nil, status.Error(codes.Internal, "query group failed")
    }

    g.Status = common.GroupStatus(statusVal)
    g.JoinNeedApprove = joinApprove
    g.InviteNeedApprove = inviteApprove

    if g.Status == common.GROUP_STATUS_DISBANDED {
        return nil, status.Error(codes.NotFound, "group has been disbanded")
    }

    // ==================== 4. 回填 Redis 缓存 ====================
    pipe := s.redis.Pipeline()
    pipe.HSet(ctx, groupInfoKey, map[string]interface{}{
        "group_id": g.GroupId, "name": g.Name, "avatar_url": g.AvatarUrl,
        "description": g.Description, "owner_id": g.OwnerId,
        "member_count": g.MemberCount, "max_members": g.MaxMembers,
        "status": int32(g.Status), "announcement": g.Announcement,
        "join_need_approve": fmt.Sprintf("%t", g.JoinNeedApprove),
        "invite_need_approve": fmt.Sprintf("%t", g.InviteNeedApprove),
        "created_at": g.CreatedAt, "updated_at": g.UpdatedAt,
    })
    pipe.Expire(ctx, groupInfoKey, 30*time.Minute)
    pipe.Exec(ctx)

    return &pb.GetGroupResponse{
        Meta:  successMeta(ctx),
        Group: &g,
    }, nil
}
```

### 3. BatchGetGroup — 批量获取群组信息

```go
func (s *GroupService) BatchGetGroup(ctx context.Context, req *pb.BatchGetGroupRequest) (*pb.BatchGetGroupResponse, error) {
    // ==================== 1. 参数校验 ====================
    if len(req.GroupIds) == 0 {
        return nil, status.Error(codes.InvalidArgument, "group_ids is required")
    }
    if len(req.GroupIds) > 100 {
        return nil, status.Error(codes.InvalidArgument, "group_ids too many, max 100")
    }

    groups := make([]*pb.GroupInfo, 0, len(req.GroupIds))
    missingIDs := make([]string, 0)

    // ==================== 2. 批量从 Redis 缓存获取 ====================
    pipe := s.redis.Pipeline()
    cmds := make(map[string]*redis.MapStringStringCmd)
    for _, gid := range req.GroupIds {
        key := fmt.Sprintf("group:info:%s", gid)
        cmds[gid] = pipe.HGetAll(ctx, key)
    }
    pipe.Exec(ctx)

    for _, gid := range req.GroupIds {
        cached, err := cmds[gid].Result()
        if err == nil && len(cached) > 0 {
            g := parseGroupInfoFromCache(cached)
            if g.Status != int32(common.GROUP_STATUS_DISBANDED) {
                groups = append(groups, g)
            }
        } else {
            missingIDs = append(missingIDs, gid)
        }
    }

    // ==================== 3. 缓存未命中的从 PgSQL 查询 ====================
    if len(missingIDs) > 0 {
        placeholders := buildPlaceholders(len(missingIDs)) // $1, $2, ...
        args := toInterfaceSlice(missingIDs)

        rows, err := s.db.QueryContext(ctx,
            fmt.Sprintf(
                `SELECT group_id, name, avatar_url, description, owner_id, member_count, max_members,
                        status, announcement, join_need_approve, invite_need_approve, extra, created_at, updated_at
                 FROM groups WHERE group_id IN (%s) AND status != 3`, placeholders),
            args...,
        )
        if err != nil {
            return nil, status.Error(codes.Internal, "batch query groups failed")
        }
        defer rows.Close()

        cachePipe := s.redis.Pipeline()
        for rows.Next() {
            var g pb.GroupInfo
            var statusVal int32
            var joinApprove, inviteApprove bool
            var extra string
            if err := rows.Scan(&g.GroupId, &g.Name, &g.AvatarUrl, &g.Description, &g.OwnerId,
                &g.MemberCount, &g.MaxMembers, &statusVal, &g.Announcement,
                &joinApprove, &inviteApprove, &extra, &g.CreatedAt, &g.UpdatedAt); err != nil {
                continue
            }
            g.Status = common.GroupStatus(statusVal)
            g.JoinNeedApprove = joinApprove
            g.InviteNeedApprove = inviteApprove
            groups = append(groups, &g)

            // 回填 Redis 缓存
            key := fmt.Sprintf("group:info:%s", g.GroupId)
            cachePipe.HSet(ctx, key, map[string]interface{}{
                "group_id": g.GroupId, "name": g.Name, "avatar_url": g.AvatarUrl,
                "description": g.Description, "owner_id": g.OwnerId,
                "member_count": g.MemberCount, "max_members": g.MaxMembers,
                "status": int32(g.Status), "announcement": g.Announcement,
                "join_need_approve": fmt.Sprintf("%t", g.JoinNeedApprove),
                "invite_need_approve": fmt.Sprintf("%t", g.InviteNeedApprove),
                "created_at": g.CreatedAt, "updated_at": g.UpdatedAt,
            })
            cachePipe.Expire(ctx, key, 30*time.Minute)
        }
        cachePipe.Exec(ctx)
    }

    return &pb.BatchGetGroupResponse{
        Meta:   successMeta(ctx),
        Groups: groups,
    }, nil
}
```

### 4. UpdateGroup — 更新群组信息

```go
func (s *GroupService) UpdateGroup(ctx context.Context, req *pb.UpdateGroupRequest) (*pb.UpdateGroupResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.GroupId == "" {
        return nil, status.Error(codes.InvalidArgument, "group_id is required")
    }
    if req.OperatorId == "" {
        return nil, status.Error(codes.InvalidArgument, "operator_id is required")
    }
    if req.Name != "" && len(req.Name) > 128 {
        return nil, status.Error(codes.InvalidArgument, "name too long, max 128 chars")
    }

    // ==================== 2. 校验群是否存在 — 内部方法 ====================
    groupInfo, err := s.getGroupInternal(ctx, req.GroupId)
    if err != nil {
        return nil, err
    }

    // ==================== 3. 权限校验：操作者须为群主或管理员 ====================
    operatorRole, err := s.getMemberRoleInternal(ctx, req.GroupId, req.OperatorId)
    if err != nil {
        return nil, status.Error(codes.PermissionDenied, "operator is not a member of the group")
    }
    if operatorRole != int32(common.GROUP_MEMBER_ROLE_OWNER) && operatorRole != int32(common.GROUP_MEMBER_ROLE_ADMIN) {
        return nil, status.Error(codes.PermissionDenied, "only owner or admin can update group info")
    }

    // ==================== 4. 构建更新字段 — PgSQL 动态 SQL ====================
    setClauses := make([]string, 0)
    args := make([]interface{}, 0)
    argIdx := 1
    updatedFields := make(map[string]string) // 记录变更字段（用于 Kafka 事件）

    if req.Name != "" && req.Name != groupInfo.Name {
        setClauses = append(setClauses, fmt.Sprintf("name = $%d", argIdx))
        args = append(args, req.Name)
        argIdx++
        updatedFields["name"] = req.Name
    }
    if req.AvatarUrl != "" && req.AvatarUrl != groupInfo.AvatarUrl {
        setClauses = append(setClauses, fmt.Sprintf("avatar_url = $%d", argIdx))
        args = append(args, req.AvatarUrl)
        argIdx++
        updatedFields["avatar_url"] = req.AvatarUrl
    }
    if req.Description != "" {
        setClauses = append(setClauses, fmt.Sprintf("description = $%d", argIdx))
        args = append(args, req.Description)
        argIdx++
        updatedFields["description"] = req.Description
    }
    // join_need_approve 和 invite_need_approve 仅群主可修改
    if operatorRole == int32(common.GROUP_MEMBER_ROLE_OWNER) {
        // 注意: proto3 bool 默认值为 false，需要通过字段掩码或额外标记判断是否传入
        // 这里简化处理，直接写入请求值
        setClauses = append(setClauses, fmt.Sprintf("join_need_approve = $%d", argIdx))
        args = append(args, req.JoinNeedApprove)
        argIdx++
        updatedFields["join_need_approve"] = fmt.Sprintf("%t", req.JoinNeedApprove)

        setClauses = append(setClauses, fmt.Sprintf("invite_need_approve = $%d", argIdx))
        args = append(args, req.InviteNeedApprove)
        argIdx++
        updatedFields["invite_need_approve"] = fmt.Sprintf("%t", req.InviteNeedApprove)
    }

    if len(setClauses) == 0 {
        return &pb.UpdateGroupResponse{Meta: successMeta(ctx)}, nil
    }

    now := time.Now().UnixMilli()
    setClauses = append(setClauses, fmt.Sprintf("updated_at = $%d", argIdx))
    args = append(args, now)
    argIdx++

    // WHERE group_id = $N
    args = append(args, req.GroupId)
    updateSQL := fmt.Sprintf("UPDATE groups SET %s WHERE group_id = $%d AND status != 3",
        strings.Join(setClauses, ", "), argIdx)

    // ==================== 5. 延迟双删 + 执行更新 — PgSQL ====================
    s.delayedDoubleDelete(ctx, fmt.Sprintf("group:info:%s", req.GroupId))

    result, err := s.db.ExecContext(ctx, updateSQL, args...)
    if err != nil {
        return nil, status.Error(codes.Internal, "update group failed")
    }
    affected, _ := result.RowsAffected()
    if affected == 0 {
        return nil, status.Error(codes.NotFound, "group not found or disbanded")
    }

    // ==================== 6. 生产 Kafka 事件: group.info.updated ====================
    updateEvent := &kafka_group.GroupInfoUpdatedEvent{
        Header:        buildEventHeader("group", s.instanceID),
        GroupId:       req.GroupId,
        OperatorId:    req.OperatorId,
        UpdatedFields: updatedFields,
        UpdateTime:    now,
    }
    s.kafka.Produce(ctx, "group.info.updated", req.GroupId, updateEvent)

    return &pb.UpdateGroupResponse{Meta: successMeta(ctx)}, nil
}
```

### 5. DisbandGroup — 解散群组

```go
func (s *GroupService) DisbandGroup(ctx context.Context, req *pb.DisbandGroupRequest) (*pb.DisbandGroupResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.GroupId == "" {
        return nil, status.Error(codes.InvalidArgument, "group_id is required")
    }
    if req.OperatorId == "" {
        return nil, status.Error(codes.InvalidArgument, "operator_id is required")
    }

    // ==================== 2. 校验群是否存在 ====================
    groupInfo, err := s.getGroupInternal(ctx, req.GroupId)
    if err != nil {
        return nil, err
    }

    // ==================== 3. 权限校验：仅群主可解散 ====================
    if groupInfo.OwnerId != req.OperatorId {
        return nil, status.Error(codes.PermissionDenied, "only group owner can disband group")
    }

    // ==================== 4. 查询所有成员（用于清理和通知） — PgSQL ====================
    memberIDs := make([]string, 0)
    rows, err := s.db.QueryContext(ctx,
        `SELECT user_id FROM group_members WHERE group_id = $1`,
        req.GroupId,
    )
    if err != nil {
        return nil, status.Error(codes.Internal, "query group members failed")
    }
    defer rows.Close()
    for rows.Next() {
        var uid string
        if err := rows.Scan(&uid); err != nil {
            continue
        }
        memberIDs = append(memberIDs, uid)
    }

    now := time.Now().UnixMilli()

    // ==================== 5. 延迟双删 + 事务：标记群解散 + 删除所有成员 — PgSQL ====================
    delKeys := []string{
        fmt.Sprintf("group:info:%s", req.GroupId),
        fmt.Sprintf("group:members:%s", req.GroupId),
        fmt.Sprintf("group:member_count:%s", req.GroupId),
    }
    for _, uid := range memberIDs {
        delKeys = append(delKeys, fmt.Sprintf("group:member:%s:%s", req.GroupId, uid))
        delKeys = append(delKeys, fmt.Sprintf("group:user_groups:%s", uid))
    }
    s.delayedDoubleDelete(ctx, delKeys...)

    tx, err := s.db.BeginTx(ctx, nil)
    if err != nil {
        return nil, status.Error(codes.Internal, "begin tx failed")
    }
    defer tx.Rollback()

    // 5a. 标记群为已解散
    _, err = tx.ExecContext(ctx,
        `UPDATE groups SET status = 3, updated_at = $1 WHERE group_id = $2 AND status != 3`,
        now, req.GroupId,
    )
    if err != nil {
        return nil, status.Error(codes.Internal, "update group status failed")
    }

    // 5b. 删除所有群成员记录
    _, err = tx.ExecContext(ctx,
        `DELETE FROM group_members WHERE group_id = $1`,
        req.GroupId,
    )
    if err != nil {
        return nil, status.Error(codes.Internal, "delete group members failed")
    }

    // 5c. 过期所有待处理的入群申请
    _, err = tx.ExecContext(ctx,
        `UPDATE group_applications SET status = 4, handle_time = $1
         WHERE group_id = $2 AND status = 1`,
        now, req.GroupId,
    )
    if err != nil {
        return nil, status.Error(codes.Internal, "expire group applications failed")
    }

    if err = tx.Commit(); err != nil {
        return nil, status.Error(codes.Internal, "commit tx failed")
    }

    // ==================== 6. 删除群聊频道 — RPC Conversation.DeleteChannel ====================
    go func() {
        _, err := s.conversationClient.DeleteChannel(context.Background(), &conversation_pb.DeleteChannelRequest{
            ConversationId: req.GroupId,
        })
        if err != nil {
            log.Error("删除群聊频道失败", "group_id", req.GroupId, "err", err)
        }
    }()

    // ==================== 8. 为每个成员删除会话视图 — RPC Conversation.DeleteUserConversation ====================
    for _, uid := range memberIDs {
        go func(userID string) {
            _, err := s.conversationClient.DeleteUserConversation(context.Background(), &conversation_pb.DeleteUserConversationRequest{
                UserId:         userID,
                ConversationId: req.GroupId,
            })
            if err != nil {
                log.Error("删除成员会话视图失败", "user_id", userID, "group_id", req.GroupId, "err", err)
            }
        }(uid)
    }

    // ==================== 9. 生产 Kafka 事件: group.dissolved ====================
    dissolveEvent := &kafka_group.GroupDissolvedEvent{
        Header:       buildEventHeader("group", s.instanceID),
        GroupId:      req.GroupId,
        GroupName:    groupInfo.Name,
        OperatorId:   req.OperatorId,
        MemberCount:  int32(len(memberIDs)),
        DissolveTime: now,
    }
    s.kafka.Produce(ctx, "group.dissolved", req.GroupId, dissolveEvent)

    return &pb.DisbandGroupResponse{Meta: successMeta(ctx)}, nil
}
```

### 6. TransferGroupOwner — 转让群主

```go
func (s *GroupService) TransferGroupOwner(ctx context.Context, req *pb.TransferGroupOwnerRequest) (*pb.TransferGroupOwnerResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.GroupId == "" {
        return nil, status.Error(codes.InvalidArgument, "group_id is required")
    }
    if req.OwnerId == "" {
        return nil, status.Error(codes.InvalidArgument, "owner_id is required")
    }
    if req.NewOwnerId == "" {
        return nil, status.Error(codes.InvalidArgument, "new_owner_id is required")
    }
    if req.OwnerId == req.NewOwnerId {
        return nil, status.Error(codes.InvalidArgument, "cannot transfer to yourself")
    }

    // ==================== 2. 校验群是否存在 ====================
    groupInfo, err := s.getGroupInternal(ctx, req.GroupId)
    if err != nil {
        return nil, err
    }

    // ==================== 3. 权限校验：仅群主可转让 ====================
    if groupInfo.OwnerId != req.OwnerId {
        return nil, status.Error(codes.PermissionDenied, "only current owner can transfer ownership")
    }

    // ==================== 4. 校验新群主是否为群成员 — PgSQL ====================
    var newOwnerExists bool
    err = s.db.QueryRowContext(ctx,
        `SELECT EXISTS(SELECT 1 FROM group_members WHERE group_id = $1 AND user_id = $2)`,
        req.GroupId, req.NewOwnerId,
    ).Scan(&newOwnerExists)
    if err != nil {
        return nil, status.Error(codes.Internal, "check new owner membership failed")
    }
    if !newOwnerExists {
        return nil, status.Error(codes.FailedPrecondition, "new owner is not a member of the group")
    }

    now := time.Now().UnixMilli()

    // ==================== 5. 延迟双删 + 事务：转让群主 — PgSQL ====================
    s.delayedDoubleDelete(ctx,
        fmt.Sprintf("group:info:%s", req.GroupId),
        fmt.Sprintf("group:member:%s:%s", req.GroupId, req.OwnerId),
        fmt.Sprintf("group:member:%s:%s", req.GroupId, req.NewOwnerId),
    )

    tx, err := s.db.BeginTx(ctx, nil)
    if err != nil {
        return nil, status.Error(codes.Internal, "begin tx failed")
    }
    defer tx.Rollback()

    // 5a. 更新群 owner_id
    _, err = tx.ExecContext(ctx,
        `UPDATE groups SET owner_id = $1, updated_at = $2 WHERE group_id = $3`,
        req.NewOwnerId, now, req.GroupId,
    )
    if err != nil {
        return nil, status.Error(codes.Internal, "update group owner failed")
    }

    // 5b. 原群主降为普通成员
    _, err = tx.ExecContext(ctx,
        `UPDATE group_members SET role = 3 WHERE group_id = $1 AND user_id = $2`,
        req.GroupId, req.OwnerId,
    )
    if err != nil {
        return nil, status.Error(codes.Internal, "demote old owner failed")
    }

    // 5c. 新群主设为群主
    _, err = tx.ExecContext(ctx,
        `UPDATE group_members SET role = 1 WHERE group_id = $1 AND user_id = $2`,
        req.GroupId, req.NewOwnerId,
    )
    if err != nil {
        return nil, status.Error(codes.Internal, "promote new owner failed")
    }

    if err = tx.Commit(); err != nil {
        return nil, status.Error(codes.Internal, "commit tx failed")
    }

    // ==================== 6. 生产 Kafka 事件: group.transfer ====================
    transferEvent := &kafka_group.GroupTransferEvent{
        Header:       buildEventHeader("group", s.instanceID),
        GroupId:      req.GroupId,
        OldOwnerId:  req.OwnerId,
        NewOwnerId:  req.NewOwnerId,
        TransferTime: now,
    }
    s.kafka.Produce(ctx, "group.transfer", req.GroupId, transferEvent)

    return &pb.TransferGroupOwnerResponse{Meta: successMeta(ctx)}, nil
}
```

### 7. JoinGroup — 申请加入群组

```go
func (s *GroupService) JoinGroup(ctx context.Context, req *pb.JoinGroupRequest) (*pb.JoinGroupResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.GroupId == "" {
        return nil, status.Error(codes.InvalidArgument, "group_id is required")
    }
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id is required")
    }
    if len(req.Message) > 256 {
        return nil, status.Error(codes.InvalidArgument, "message too long, max 256 chars")
    }

    // ==================== 2. 校验群是否存在 ====================
    groupInfo, err := s.getGroupInternal(ctx, req.GroupId)
    if err != nil {
        return nil, err
    }

    // ==================== 3. 校验用户是否存在 — RPC User.GetUser ====================
    userResp, err := s.userClient.GetUser(ctx, &user_pb.GetUserRequest{UserId: req.UserId})
    if err != nil {
        if st, ok := status.FromError(err); ok && st.Code() == codes.NotFound {
            return nil, status.Error(codes.NotFound, "user not found")
        }
        return nil, status.Error(codes.Internal, "get user failed")
    }

    // ==================== 4. 检查是否已是群成员 — Redis / PgSQL ====================
    isMember, err := s.checkMembershipInternal(ctx, req.GroupId, req.UserId)
    if err != nil {
        return nil, status.Error(codes.Internal, "check membership failed")
    }
    if isMember {
        return nil, status.Error(codes.AlreadyExists, "already a member of the group")
    }

    // ==================== 5. 检查群人数是否已满 ====================
    if groupInfo.MemberCount >= groupInfo.MaxMembers {
        return nil, status.Error(codes.ResourceExhausted, "group member count has reached the limit")
    }

    // ==================== 6. 检查是否有未处理的申请 — PgSQL ====================
    var pendingCount int
    err = s.db.QueryRowContext(ctx,
        `SELECT COUNT(*) FROM group_applications
         WHERE group_id = $1 AND user_id = $2 AND type = 1 AND status = 1`,
        req.GroupId, req.UserId,
    ).Scan(&pendingCount)
    if err != nil {
        return nil, status.Error(codes.Internal, "query pending applications failed")
    }
    if pendingCount > 0 {
        return nil, status.Error(codes.AlreadyExists, "join application already pending")
    }

    now := time.Now().UnixMilli()
    applicationID := generateApplicationID() // UUID v4

    // ==================== 7. 判断是否需要审批 ====================
    if !groupInfo.JoinNeedApprove {
        // 不需要审批，直接加入
        err = s.addMemberInternal(ctx, req.GroupId, req.UserId, "", int32(common.GROUP_MEMBER_ROLE_MEMBER), now)
        if err != nil {
            return nil, status.Error(codes.Internal, "join group failed")
        }

        // 生产 group.member.joined 事件
        joinEvent := &kafka_group.GroupMemberJoinedEvent{
            Header:      buildEventHeader("group", s.instanceID),
            GroupId:     req.GroupId,
            GroupName:   groupInfo.Name,
            UserId:      req.UserId,
            Nickname:    userResp.User.Nickname,
            InviterId:   "",
            JoinType:    kafka_group.JOIN_TYPE_APPLIED,
            MemberCount: groupInfo.MemberCount + 1,
            JoinTime:    now,
        }
        s.kafka.Produce(ctx, "group.member.joined", req.GroupId, joinEvent)

        return &pb.JoinGroupResponse{Meta: successMeta(ctx)}, nil
    }

    // ==================== 8. 需要审批：创建入群申请记录 — PgSQL ====================
    _, err = s.db.ExecContext(ctx,
        `INSERT INTO group_applications (application_id, group_id, user_id, type, status, message, operator_id, handle_time, created_at)
         VALUES ($1, $2, $3, 1, 1, $4, '', 0, $5)`,
        applicationID, req.GroupId, req.UserId, req.Message, now,
    )
    if err != nil {
        return nil, status.Error(codes.Internal, "insert group application failed")
    }

    // ==================== 9. 生产 Kafka 事件: group.application ====================
    appEvent := &kafka_group.GroupApplicationEvent{
        Header:        buildEventHeader("group", s.instanceID),
        ApplicationId: applicationID,
        GroupId:       req.GroupId,
        GroupName:     groupInfo.Name,
        ApplicantId:   req.UserId,
        ApplicantName: userResp.User.Nickname,
        Message:       req.Message,
        Source:        "search",
        ApplyTime:     now,
    }
    s.kafka.Produce(ctx, "group.application", req.GroupId, appEvent)

    return &pb.JoinGroupResponse{Meta: successMeta(ctx)}, nil
}
```

### 8. LeaveGroup — 退出群组

```go
func (s *GroupService) LeaveGroup(ctx context.Context, req *pb.LeaveGroupRequest) (*pb.LeaveGroupResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.GroupId == "" {
        return nil, status.Error(codes.InvalidArgument, "group_id is required")
    }
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id is required")
    }

    // ==================== 2. 校验群是否存在 ====================
    groupInfo, err := s.getGroupInternal(ctx, req.GroupId)
    if err != nil {
        return nil, err
    }

    // ==================== 3. 群主不能直接退出 ====================
    if groupInfo.OwnerId == req.UserId {
        return nil, status.Error(codes.FailedPrecondition, "group owner cannot leave, please transfer ownership first or disband the group")
    }

    // ==================== 4. 校验是否为群成员 — PgSQL ====================
    var memberNickname string
    var memberRole int32
    err = s.db.QueryRowContext(ctx,
        `SELECT nickname, role FROM group_members WHERE group_id = $1 AND user_id = $2`,
        req.GroupId, req.UserId,
    ).Scan(&memberNickname, &memberRole)
    if err == sql.ErrNoRows {
        return nil, status.Error(codes.NotFound, "not a member of the group")
    }
    if err != nil {
        return nil, status.Error(codes.Internal, "query member failed")
    }

    now := time.Now().UnixMilli()

    // ==================== 5. 延迟双删 + 事务：删除成员 + 更新计数 — PgSQL ====================
    s.delayedDoubleDelete(ctx,
        fmt.Sprintf("group:info:%s", req.GroupId),
        fmt.Sprintf("group:members:%s", req.GroupId),
        fmt.Sprintf("group:member:%s:%s", req.GroupId, req.UserId),
        fmt.Sprintf("group:member_count:%s", req.GroupId),
        fmt.Sprintf("group:user_groups:%s", req.UserId),
    )

    tx, err := s.db.BeginTx(ctx, nil)
    if err != nil {
        return nil, status.Error(codes.Internal, "begin tx failed")
    }
    defer tx.Rollback()

    _, err = tx.ExecContext(ctx,
        `DELETE FROM group_members WHERE group_id = $1 AND user_id = $2`,
        req.GroupId, req.UserId,
    )
    if err != nil {
        return nil, status.Error(codes.Internal, "delete member failed")
    }

    _, err = tx.ExecContext(ctx,
        `UPDATE groups SET member_count = member_count - 1, updated_at = $1
         WHERE group_id = $2 AND member_count > 0`,
        now, req.GroupId,
    )
    if err != nil {
        return nil, status.Error(codes.Internal, "update member count failed")
    }

    if err = tx.Commit(); err != nil {
        return nil, status.Error(codes.Internal, "commit tx failed")
    }

    // ==================== 6. 删除会话视图 — RPC Conversation.DeleteUserConversation ====================
    go func() {
        _, err := s.conversationClient.DeleteUserConversation(context.Background(), &conversation_pb.DeleteUserConversationRequest{
            UserId:         req.UserId,
            ConversationId: req.GroupId,
        })
        if err != nil {
            log.Error("删除退群成员会话视图失败", "user_id", req.UserId, "group_id", req.GroupId, "err", err)
        }
    }()

    // ==================== 8. 生产 Kafka 事件: group.member.left ====================
    // 获取用户昵称用于事件
    userResp, _ := s.userClient.GetUser(ctx, &user_pb.GetUserRequest{UserId: req.UserId})
    nickname := ""
    if userResp != nil {
        nickname = userResp.User.Nickname
    }

    leaveEvent := &kafka_group.GroupMemberLeftEvent{
        Header:      buildEventHeader("group", s.instanceID),
        GroupId:     req.GroupId,
        GroupName:   groupInfo.Name,
        UserId:      req.UserId,
        Nickname:    nickname,
        MemberCount: groupInfo.MemberCount - 1,
        LeaveTime:   now,
    }
    s.kafka.Produce(ctx, "group.member.left", req.GroupId, leaveEvent)

    return &pb.LeaveGroupResponse{Meta: successMeta(ctx)}, nil
}
```

### 9. KickGroupMember — 踢出群成员

```go
func (s *GroupService) KickGroupMember(ctx context.Context, req *pb.KickGroupMemberRequest) (*pb.KickGroupMemberResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.GroupId == "" {
        return nil, status.Error(codes.InvalidArgument, "group_id is required")
    }
    if req.OperatorId == "" {
        return nil, status.Error(codes.InvalidArgument, "operator_id is required")
    }
    if len(req.MemberIds) == 0 {
        return nil, status.Error(codes.InvalidArgument, "member_ids is required")
    }
    if len(req.MemberIds) > 50 {
        return nil, status.Error(codes.InvalidArgument, "member_ids too many, max 50 per request")
    }

    // ==================== 2. 校验群是否存在 ====================
    groupInfo, err := s.getGroupInternal(ctx, req.GroupId)
    if err != nil {
        return nil, err
    }

    // ==================== 3. 权限校验：操作者须为群主或管理员 ====================
    operatorRole, err := s.getMemberRoleInternal(ctx, req.GroupId, req.OperatorId)
    if err != nil {
        return nil, status.Error(codes.PermissionDenied, "operator is not a member of the group")
    }
    if operatorRole != int32(common.GROUP_MEMBER_ROLE_OWNER) && operatorRole != int32(common.GROUP_MEMBER_ROLE_ADMIN) {
        return nil, status.Error(codes.PermissionDenied, "only owner or admin can kick members")
    }

    // ==================== 4. 过滤不可踢的成员 ====================
    // 不能踢自己、不能踢群主、管理员不能踢管理员
    validKickIDs := make([]string, 0, len(req.MemberIds))
    for _, kickID := range req.MemberIds {
        if kickID == req.OperatorId {
            continue // 不能踢自己
        }
        if kickID == groupInfo.OwnerId {
            continue // 不能踢群主
        }
        // 查询被踢成员角色
        kickRole, err := s.getMemberRoleInternal(ctx, req.GroupId, kickID)
        if err != nil {
            continue // 不是群成员，跳过
        }
        // 管理员不能踢其他管理员（只有群主可以踢管理员）
        if operatorRole == int32(common.GROUP_MEMBER_ROLE_ADMIN) && kickRole == int32(common.GROUP_MEMBER_ROLE_ADMIN) {
            continue
        }
        validKickIDs = append(validKickIDs, kickID)
    }

    if len(validKickIDs) == 0 {
        return nil, status.Error(codes.InvalidArgument, "no valid members to kick")
    }

    now := time.Now().UnixMilli()

    // ==================== 5. 延迟双删 + 事务：删除成员 + 更新计数 — PgSQL ====================
    delKeys := []string{
        fmt.Sprintf("group:info:%s", req.GroupId),
        fmt.Sprintf("group:members:%s", req.GroupId),
        fmt.Sprintf("group:member_count:%s", req.GroupId),
    }
    for _, uid := range validKickIDs {
        delKeys = append(delKeys, fmt.Sprintf("group:member:%s:%s", req.GroupId, uid))
        delKeys = append(delKeys, fmt.Sprintf("group:user_groups:%s", uid))
    }
    s.delayedDoubleDelete(ctx, delKeys...)

    tx, err := s.db.BeginTx(ctx, nil)
    if err != nil {
        return nil, status.Error(codes.Internal, "begin tx failed")
    }
    defer tx.Rollback()

    placeholders := buildPlaceholders(len(validKickIDs))
    deleteArgs := []interface{}{req.GroupId}
    for _, uid := range validKickIDs {
        deleteArgs = append(deleteArgs, uid)
    }
    inClause := buildPlaceholdersFrom(2, len(validKickIDs)) // $2, $3, ...
    _, err = tx.ExecContext(ctx,
        fmt.Sprintf(`DELETE FROM group_members WHERE group_id = $1 AND user_id IN (%s)`, inClause),
        deleteArgs...,
    )
    if err != nil {
        return nil, status.Error(codes.Internal, "delete kicked members failed")
    }

    _, err = tx.ExecContext(ctx,
        `UPDATE groups SET member_count = member_count - $1, updated_at = $2
         WHERE group_id = $3 AND member_count >= $1`,
        len(validKickIDs), now, req.GroupId,
    )
    if err != nil {
        return nil, status.Error(codes.Internal, "update member count failed")
    }

    if err = tx.Commit(); err != nil {
        return nil, status.Error(codes.Internal, "commit tx failed")
    }

    // ==================== 6. 为被踢成员删除会话视图 — RPC Conversation.DeleteUserConversation ====================
    for _, uid := range validKickIDs {
        go func(userID string) {
            _, err := s.conversationClient.DeleteUserConversation(context.Background(), &conversation_pb.DeleteUserConversationRequest{
                UserId:         userID,
                ConversationId: req.GroupId,
            })
            if err != nil {
                log.Error("删除被踢成员会话视图失败", "user_id", userID, "group_id", req.GroupId, "err", err)
            }
        }(uid)
    }

    // ==================== 8. 生产 Kafka 事件: group.member.kicked ====================
    kickEvent := &kafka_group.GroupMemberKickedEvent{
        Header:      buildEventHeader("group", s.instanceID),
        GroupId:     req.GroupId,
        GroupName:   groupInfo.Name,
        OperatorId:  req.OperatorId,
        KickedIds:   validKickIDs,
        Reason:      "",
        MemberCount: groupInfo.MemberCount - int32(len(validKickIDs)),
        KickTime:    now,
    }
    s.kafka.Produce(ctx, "group.member.kicked", req.GroupId, kickEvent)

    return &pb.KickGroupMemberResponse{Meta: successMeta(ctx)}, nil
}
```

### 10. InviteToGroup — 邀请用户加入群组

```go
func (s *GroupService) InviteToGroup(ctx context.Context, req *pb.InviteToGroupRequest) (*pb.InviteToGroupResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.GroupId == "" {
        return nil, status.Error(codes.InvalidArgument, "group_id is required")
    }
    if req.InviterId == "" {
        return nil, status.Error(codes.InvalidArgument, "inviter_id is required")
    }
    if len(req.InviteeIds) == 0 {
        return nil, status.Error(codes.InvalidArgument, "invitee_ids is required")
    }
    if len(req.InviteeIds) > 50 {
        return nil, status.Error(codes.InvalidArgument, "invitee_ids too many, max 50 per request")
    }

    // ==================== 2. 校验群是否存在 ====================
    groupInfo, err := s.getGroupInternal(ctx, req.GroupId)
    if err != nil {
        return nil, err
    }

    // ==================== 3. 校验邀请者是否为群成员 ====================
    isMember, err := s.checkMembershipInternal(ctx, req.GroupId, req.InviterId)
    if err != nil {
        return nil, status.Error(codes.Internal, "check inviter membership failed")
    }
    if !isMember {
        return nil, status.Error(codes.PermissionDenied, "inviter is not a member of the group")
    }

    // ==================== 4. 批量校验被邀请用户 — RPC User.BatchGetUser ====================
    batchResp, err := s.userClient.BatchGetUser(ctx, &user_pb.BatchGetUserRequest{
        UserIds: req.InviteeIds,
    })
    if err != nil {
        return nil, status.Error(codes.Internal, "batch get invitee users failed")
    }
    existingUsers := make(map[string]*user_pb.UserInfo)
    for _, u := range batchResp.Users {
        existingUsers[u.UserId] = u
    }

    // 过滤：排除已是成员的和不存在的
    validInviteeIDs := make([]string, 0)
    for _, uid := range req.InviteeIds {
        if _, exists := existingUsers[uid]; !exists {
            continue
        }
        isMember, _ := s.checkMembershipInternal(ctx, req.GroupId, uid)
        if isMember {
            continue
        }
        validInviteeIDs = append(validInviteeIDs, uid)
    }

    if len(validInviteeIDs) == 0 {
        return nil, status.Error(codes.InvalidArgument, "no valid invitees")
    }

    // ==================== 5. 检查群人数上限 ====================
    if int(groupInfo.MemberCount)+len(validInviteeIDs) > int(groupInfo.MaxMembers) {
        return nil, status.Error(codes.ResourceExhausted, "inviting these members would exceed group member limit")
    }

    now := time.Now().UnixMilli()

    // ==================== 6. 判断是否需要审批 ====================
    if groupInfo.InviteNeedApprove {
        // 需要审批：为每个被邀请者创建申请记录
        for _, uid := range validInviteeIDs {
            applicationID := generateApplicationID()
            _, err = s.db.ExecContext(ctx,
                `INSERT INTO group_applications (application_id, group_id, user_id, type, status, message, operator_id, handle_time, created_at)
                 VALUES ($1, $2, $3, 2, 1, $4, '', 0, $5)
                 ON CONFLICT DO NOTHING`,
                applicationID, req.GroupId, uid, fmt.Sprintf("invited by %s", req.InviterId), now,
            )
            if err != nil {
                log.Error("创建邀请申请记录失败", "group_id", req.GroupId, "user_id", uid, "err", err)
                continue
            }

            // 生产 group.application 事件
            inviteeName := ""
            if u, ok := existingUsers[uid]; ok {
                inviteeName = u.Nickname
            }
            appEvent := &kafka_group.GroupApplicationEvent{
                Header:        buildEventHeader("group", s.instanceID),
                ApplicationId: applicationID,
                GroupId:       req.GroupId,
                GroupName:     groupInfo.Name,
                ApplicantId:   uid,
                ApplicantName: inviteeName,
                Message:       fmt.Sprintf("invited by %s", req.InviterId),
                Source:        "invite",
                ApplyTime:     now,
            }
            s.kafka.Produce(ctx, "group.application", req.GroupId, appEvent)
        }

        return &pb.InviteToGroupResponse{Meta: successMeta(ctx)}, nil
    }

    // ==================== 7. 不需要审批：直接加入 ====================
    for _, uid := range validInviteeIDs {
        err = s.addMemberInternal(ctx, req.GroupId, uid, req.InviterId, int32(common.GROUP_MEMBER_ROLE_MEMBER), now)
        if err != nil {
            log.Error("邀请加入群失败", "group_id", req.GroupId, "user_id", uid, "err", err)
            continue
        }

        // 获取成员昵称
        inviteeName := ""
        if u, ok := existingUsers[uid]; ok {
            inviteeName = u.Nickname
        }

        // 生产 group.member.joined 事件
        joinEvent := &kafka_group.GroupMemberJoinedEvent{
            Header:      buildEventHeader("group", s.instanceID),
            GroupId:     req.GroupId,
            GroupName:   groupInfo.Name,
            UserId:      uid,
            Nickname:    inviteeName,
            InviterId:   req.InviterId,
            JoinType:    kafka_group.JOIN_TYPE_INVITED,
            MemberCount: groupInfo.MemberCount + int32(len(validInviteeIDs)),
            JoinTime:    now,
        }
        s.kafka.Produce(ctx, "group.member.joined", req.GroupId, joinEvent)
    }

    return &pb.InviteToGroupResponse{Meta: successMeta(ctx)}, nil
}
```

### 11. GetGroupMembers — 获取群成员列表（分页）

```go
func (s *GroupService) GetGroupMembers(ctx context.Context, req *pb.GetGroupMembersRequest) (*pb.GetGroupMembersResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.GroupId == "" {
        return nil, status.Error(codes.InvalidArgument, "group_id is required")
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
    err := s.db.QueryRowContext(ctx,
        `SELECT COUNT(*) FROM group_members WHERE group_id = $1`,
        req.GroupId,
    ).Scan(&total)
    if err != nil {
        return nil, status.Error(codes.Internal, "count group members failed")
    }

    // ==================== 3. 分页查询成员列表 — PgSQL ====================
    // 按角色排序：群主 > 管理员 > 普通成员，同角色按加入时间升序
    rows, err := s.db.QueryContext(ctx,
        `SELECT group_id, user_id, nickname, role, is_muted, mute_until, joined_at, invite_by
         FROM group_members
         WHERE group_id = $1
         ORDER BY role ASC, joined_at ASC
         LIMIT $2 OFFSET $3`,
        req.GroupId, pageSize, offset,
    )
    if err != nil {
        return nil, status.Error(codes.Internal, "query group members failed")
    }
    defer rows.Close()

    var members []*pb.GroupMemberInfo
    userIDs := make([]string, 0)
    for rows.Next() {
        var m pb.GroupMemberInfo
        var roleVal int32
        if err := rows.Scan(&m.GroupId, &m.UserId, &m.Nickname, &roleVal,
            &m.IsMuted, &m.MuteUntil, &m.JoinedAt, &m.InviteBy); err != nil {
            continue
        }
        m.Role = common.GroupMemberRole(roleVal)
        members = append(members, &m)
        userIDs = append(userIDs, m.UserId)
    }

    // ==================== 4. 批量获取用户头像 — RPC User.BatchGetUser ====================
    if len(userIDs) > 0 {
        batchResp, err := s.userClient.BatchGetUser(ctx, &user_pb.BatchGetUserRequest{
            UserIds: userIDs,
        })
        if err == nil {
            userMap := make(map[string]*user_pb.UserInfo)
            for _, u := range batchResp.Users {
                userMap[u.UserId] = u
            }
            for _, m := range members {
                if u, ok := userMap[m.UserId]; ok {
                    m.AvatarUrl = u.AvatarUrl
                    // 如果群内昵称为空，使用用户昵称
                    if m.Nickname == "" {
                        m.Nickname = u.Nickname
                    }
                }
            }
        }
    }

    totalPages := int32(0)
    if total > 0 {
        totalPages = int32((total + int64(pageSize) - 1) / int64(pageSize))
    }

    return &pb.GetGroupMembersResponse{
        Meta:    successMeta(ctx),
        Members: members,
        Pagination: &common.PaginationResult{
            Page: page, PageSize: pageSize, Total: total, TotalPages: totalPages,
        },
    }, nil
}
```

### 12. GetGroupMemberIDs — 获取群成员 ID 列表（轻量级，高频路径）

```go
func (s *GroupService) GetGroupMemberIDs(ctx context.Context, req *pb.GetGroupMemberIDsRequest) (*pb.GetGroupMemberIDsResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.GroupId == "" {
        return nil, status.Error(codes.InvalidArgument, "group_id is required")
    }

    // ==================== 2. 优先读 Redis SET 缓存 ====================
    membersKey := fmt.Sprintf("group:members:%s", req.GroupId)
    memberIDs, err := s.redis.SMembers(ctx, membersKey).Result()
    if err == nil && len(memberIDs) > 0 {
        return &pb.GetGroupMemberIDsResponse{
            Meta:      successMeta(ctx),
            MemberIds: memberIDs,
        }, nil
    }

    // ==================== 3. 缓存未命中 → PgSQL 查询 ====================
    rows, err := s.db.QueryContext(ctx,
        `SELECT user_id FROM group_members WHERE group_id = $1`,
        req.GroupId,
    )
    if err != nil {
        return nil, status.Error(codes.Internal, "query group member ids failed")
    }
    defer rows.Close()

    memberIDs = make([]string, 0)
    for rows.Next() {
        var uid string
        if err := rows.Scan(&uid); err != nil {
            continue
        }
        memberIDs = append(memberIDs, uid)
    }

    if len(memberIDs) == 0 {
        return &pb.GetGroupMemberIDsResponse{
            Meta:      successMeta(ctx),
            MemberIds: []string{},
        }, nil
    }

    // ==================== 4. 回填 Redis SET 缓存 ====================
    pipe := s.redis.Pipeline()
    pipe.Del(ctx, membersKey) // 先清空，保证与 DB 一致
    pipe.SAdd(ctx, membersKey, strSliceToInterfaceSlice(memberIDs)...)
    pipe.Expire(ctx, membersKey, 30*time.Minute)
    if _, err := pipe.Exec(ctx); err != nil {
        log.Warn("redis write-back group:members failed", "group_id", req.GroupId, "err", err)
    }

    // ==================== 5. 返回 ====================
    return &pb.GetGroupMemberIDsResponse{
        Meta:      successMeta(ctx),
        MemberIds: memberIDs,
    }, nil
}
```

### 13. GetGroupMember — 获取单个群成员信息

```go
func (s *GroupService) GetGroupMember(ctx context.Context, req *pb.GetGroupMemberRequest) (*pb.GetGroupMemberResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.GroupId == "" {
        return nil, status.Error(codes.InvalidArgument, "group_id is required")
    }
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id is required")
    }

    // ==================== 2. 尝试从 Redis 缓存获取 ====================
    memberKey := fmt.Sprintf("group:member:%s:%s", req.GroupId, req.UserId)
    cached, err := s.redis.HGetAll(ctx, memberKey).Result()
    if err == nil && len(cached) > 0 {
        member := parseGroupMemberFromCache(cached)
        // 补充用户头像
        userResp, err := s.userClient.GetUser(ctx, &user_pb.GetUserRequest{UserId: req.UserId})
        if err == nil {
            member.AvatarUrl = userResp.User.AvatarUrl
            if member.Nickname == "" {
                member.Nickname = userResp.User.Nickname
            }
        }
        return &pb.GetGroupMemberResponse{
            Meta:   successMeta(ctx),
            Member: member,
        }, nil
    }

    // ==================== 3. 缓存未命中，查 PgSQL ====================
    var m pb.GroupMemberInfo
    var roleVal int32
    err = s.db.QueryRowContext(ctx,
        `SELECT group_id, user_id, nickname, role, is_muted, mute_until, joined_at, invite_by
         FROM group_members WHERE group_id = $1 AND user_id = $2`,
        req.GroupId, req.UserId,
    ).Scan(&m.GroupId, &m.UserId, &m.Nickname, &roleVal,
        &m.IsMuted, &m.MuteUntil, &m.JoinedAt, &m.InviteBy)
    if err == sql.ErrNoRows {
        return nil, status.Error(codes.NotFound, "member not found in group")
    }
    if err != nil {
        return nil, status.Error(codes.Internal, "query group member failed")
    }
    m.Role = common.GroupMemberRole(roleVal)

    // ==================== 4. 回填 Redis 缓存 ====================
    pipe := s.redis.Pipeline()
    pipe.HSet(ctx, memberKey, map[string]interface{}{
        "group_id": m.GroupId, "user_id": m.UserId, "nickname": m.Nickname,
        "role": int32(m.Role), "is_muted": fmt.Sprintf("%t", m.IsMuted),
        "mute_until": m.MuteUntil, "joined_at": m.JoinedAt, "invite_by": m.InviteBy,
    })
    pipe.Expire(ctx, memberKey, 10*time.Minute)
    pipe.Exec(ctx)

    // ==================== 5. 补充用户头像 — RPC User.GetUser ====================
    userResp, err := s.userClient.GetUser(ctx, &user_pb.GetUserRequest{UserId: req.UserId})
    if err == nil {
        m.AvatarUrl = userResp.User.AvatarUrl
        if m.Nickname == "" {
            m.Nickname = userResp.User.Nickname
        }
    }

    return &pb.GetGroupMemberResponse{
        Meta:   successMeta(ctx),
        Member: &m,
    }, nil
}
```

### 14. SearchGroupMembers — 搜索群成员

```go
func (s *GroupService) SearchGroupMembers(ctx context.Context, req *pb.SearchGroupMembersRequest) (*pb.SearchGroupMembersResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.GroupId == "" {
        return nil, status.Error(codes.InvalidArgument, "group_id is required")
    }
    if req.Keyword == "" {
        return nil, status.Error(codes.InvalidArgument, "keyword is required")
    }
    if len(req.Keyword) > 64 {
        return nil, status.Error(codes.InvalidArgument, "keyword too long, max 64 chars")
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

    // ==================== 2. 模糊搜索群成员（群内昵称匹配） — PgSQL ====================
    likePattern := "%" + req.Keyword + "%"

    var total int64
    err := s.db.QueryRowContext(ctx,
        `SELECT COUNT(*) FROM group_members WHERE group_id = $1 AND nickname ILIKE $2`,
        req.GroupId, likePattern,
    ).Scan(&total)
    if err != nil {
        return nil, status.Error(codes.Internal, "count search results failed")
    }

    rows, err := s.db.QueryContext(ctx,
        `SELECT group_id, user_id, nickname, role, is_muted, mute_until, joined_at, invite_by
         FROM group_members
         WHERE group_id = $1 AND nickname ILIKE $2
         ORDER BY role ASC, joined_at ASC
         LIMIT $3 OFFSET $4`,
        req.GroupId, likePattern, pageSize, offset,
    )
    if err != nil {
        return nil, status.Error(codes.Internal, "search group members failed")
    }
    defer rows.Close()

    var members []*pb.GroupMemberInfo
    userIDs := make([]string, 0)
    for rows.Next() {
        var m pb.GroupMemberInfo
        var roleVal int32
        if err := rows.Scan(&m.GroupId, &m.UserId, &m.Nickname, &roleVal,
            &m.IsMuted, &m.MuteUntil, &m.JoinedAt, &m.InviteBy); err != nil {
            continue
        }
        m.Role = common.GroupMemberRole(roleVal)
        members = append(members, &m)
        userIDs = append(userIDs, m.UserId)
    }

    // ==================== 3. 如果群内昵称匹配结果不足，补充用户昵称搜索 ====================
    // 先批量查用户信息，用用户真实昵称二次匹配
    if total == 0 && len(members) == 0 {
        // 回退到全成员遍历 + 用户昵称匹配（适用于群内昵称为空的场景）
        allRows, err := s.db.QueryContext(ctx,
            `SELECT user_id FROM group_members WHERE group_id = $1`,
            req.GroupId,
        )
        if err == nil {
            defer allRows.Close()
            var allUserIDs []string
            for allRows.Next() {
                var uid string
                allRows.Scan(&uid)
                allUserIDs = append(allUserIDs, uid)
            }

            if len(allUserIDs) > 0 {
                batchResp, err := s.userClient.BatchGetUser(ctx, &user_pb.BatchGetUserRequest{UserIds: allUserIDs})
                if err == nil {
                    matchedUIDs := make(map[string]*user_pb.UserInfo)
                    for _, u := range batchResp.Users {
                        if strings.Contains(strings.ToLower(u.Nickname), strings.ToLower(req.Keyword)) {
                            matchedUIDs[u.UserId] = u
                        }
                    }

                    // 查询匹配的成员详情
                    for uid, userInfo := range matchedUIDs {
                        var m pb.GroupMemberInfo
                        var roleVal int32
                        err := s.db.QueryRowContext(ctx,
                            `SELECT group_id, user_id, nickname, role, is_muted, mute_until, joined_at, invite_by
                             FROM group_members WHERE group_id = $1 AND user_id = $2`,
                            req.GroupId, uid,
                        ).Scan(&m.GroupId, &m.UserId, &m.Nickname, &roleVal,
                            &m.IsMuted, &m.MuteUntil, &m.JoinedAt, &m.InviteBy)
                        if err == nil {
                            m.Role = common.GroupMemberRole(roleVal)
                            m.AvatarUrl = userInfo.AvatarUrl
                            if m.Nickname == "" {
                                m.Nickname = userInfo.Nickname
                            }
                            members = append(members, &m)
                        }
                    }
                    total = int64(len(members))
                }
            }
        }
    }

    // ==================== 4. 批量补充用户头像 — RPC User.BatchGetUser ====================
    if len(userIDs) > 0 {
        batchResp, err := s.userClient.BatchGetUser(ctx, &user_pb.BatchGetUserRequest{UserIds: userIDs})
        if err == nil {
            userMap := make(map[string]*user_pb.UserInfo)
            for _, u := range batchResp.Users {
                userMap[u.UserId] = u
            }
            for _, m := range members {
                if u, ok := userMap[m.UserId]; ok {
                    m.AvatarUrl = u.AvatarUrl
                    if m.Nickname == "" {
                        m.Nickname = u.Nickname
                    }
                }
            }
        }
    }

    totalPages := int32(0)
    if total > 0 {
        totalPages = int32((total + int64(pageSize) - 1) / int64(pageSize))
    }

    return &pb.SearchGroupMembersResponse{
        Meta:    successMeta(ctx),
        Members: members,
        Pagination: &common.PaginationResult{
            Page: page, PageSize: pageSize, Total: total, TotalPages: totalPages,
        },
    }, nil
}
```

### 15. SetGroupMemberRole — 设置群成员角色

```go
func (s *GroupService) SetGroupMemberRole(ctx context.Context, req *pb.SetGroupMemberRoleRequest) (*pb.SetGroupMemberRoleResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.GroupId == "" {
        return nil, status.Error(codes.InvalidArgument, "group_id is required")
    }
    if req.OperatorId == "" {
        return nil, status.Error(codes.InvalidArgument, "operator_id is required")
    }
    if req.MemberId == "" {
        return nil, status.Error(codes.InvalidArgument, "member_id is required")
    }
    if req.Role == common.GROUP_MEMBER_ROLE_UNSPECIFIED {
        return nil, status.Error(codes.InvalidArgument, "role is required")
    }
    if req.Role == common.GROUP_MEMBER_ROLE_OWNER {
        return nil, status.Error(codes.InvalidArgument, "cannot set owner role directly, use TransferGroupOwner instead")
    }
    if req.OperatorId == req.MemberId {
        return nil, status.Error(codes.InvalidArgument, "cannot change your own role")
    }

    // ==================== 2. 校验群是否存在 ====================
    groupInfo, err := s.getGroupInternal(ctx, req.GroupId)
    if err != nil {
        return nil, err
    }

    // ==================== 3. 权限校验：仅群主可设置角色 ====================
    if groupInfo.OwnerId != req.OperatorId {
        return nil, status.Error(codes.PermissionDenied, "only group owner can set member roles")
    }

    // ==================== 4. 查询目标成员当前角色 — PgSQL ====================
    var oldRole int32
    err = s.db.QueryRowContext(ctx,
        `SELECT role FROM group_members WHERE group_id = $1 AND user_id = $2`,
        req.GroupId, req.MemberId,
    ).Scan(&oldRole)
    if err == sql.ErrNoRows {
        return nil, status.Error(codes.NotFound, "member not found in group")
    }
    if err != nil {
        return nil, status.Error(codes.Internal, "query member role failed")
    }

    if oldRole == int32(req.Role) {
        return &pb.SetGroupMemberRoleResponse{Meta: successMeta(ctx)}, nil // 无变化
    }

    // ==================== 5. 延迟双删 + 更新角色 — PgSQL ====================
    s.delayedDoubleDelete(ctx, fmt.Sprintf("group:member:%s:%s", req.GroupId, req.MemberId))

    _, err = s.db.ExecContext(ctx,
        `UPDATE group_members SET role = $1 WHERE group_id = $2 AND user_id = $3`,
        int32(req.Role), req.GroupId, req.MemberId,
    )
    if err != nil {
        return nil, status.Error(codes.Internal, "update member role failed")
    }

    // ==================== 6. 生产 Kafka 事件: group.member.role.changed ====================
    now := time.Now().UnixMilli()
    roleEvent := &kafka_group.GroupMemberRoleChangedEvent{
        Header:     buildEventHeader("group", s.instanceID),
        GroupId:    req.GroupId,
        OperatorId: req.OperatorId,
        UserId:     req.MemberId,
        OldRole:    common.GroupMemberRole(oldRole),
        NewRole:    req.Role,
        ChangeTime: now,
    }
    s.kafka.Produce(ctx, "group.member.role.changed", req.GroupId, roleEvent)

    return &pb.SetGroupMemberRoleResponse{Meta: successMeta(ctx)}, nil
}
```

### 16. SetGroupMemberNickname — 设置群内昵称

```go
func (s *GroupService) SetGroupMemberNickname(ctx context.Context, req *pb.SetGroupMemberNicknameRequest) (*pb.SetGroupMemberNicknameResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.GroupId == "" {
        return nil, status.Error(codes.InvalidArgument, "group_id is required")
    }
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id is required")
    }
    if len(req.Nickname) > 128 {
        return nil, status.Error(codes.InvalidArgument, "nickname too long, max 128 chars")
    }

    // ==================== 2. 校验是否为群成员 — PgSQL ====================
    var exists bool
    err := s.db.QueryRowContext(ctx,
        `SELECT EXISTS(SELECT 1 FROM group_members WHERE group_id = $1 AND user_id = $2)`,
        req.GroupId, req.UserId,
    ).Scan(&exists)
    if err != nil {
        return nil, status.Error(codes.Internal, "check membership failed")
    }
    if !exists {
        return nil, status.Error(codes.NotFound, "not a member of the group")
    }

    // ==================== 3. 延迟双删 + 更新群内昵称 — PgSQL ====================
    s.delayedDoubleDelete(ctx, fmt.Sprintf("group:member:%s:%s", req.GroupId, req.UserId))

    _, err = s.db.ExecContext(ctx,
        `UPDATE group_members SET nickname = $1 WHERE group_id = $2 AND user_id = $3`,
        req.Nickname, req.GroupId, req.UserId,
    )
    if err != nil {
        return nil, status.Error(codes.Internal, "update member nickname failed")
    }

    return &pb.SetGroupMemberNicknameResponse{Meta: successMeta(ctx)}, nil
}
```

### 17. MuteGroupMember — 禁言群成员

```go
func (s *GroupService) MuteGroupMember(ctx context.Context, req *pb.MuteGroupMemberRequest) (*pb.MuteGroupMemberResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.GroupId == "" {
        return nil, status.Error(codes.InvalidArgument, "group_id is required")
    }
    if req.OperatorId == "" {
        return nil, status.Error(codes.InvalidArgument, "operator_id is required")
    }
    if req.MemberId == "" {
        return nil, status.Error(codes.InvalidArgument, "member_id is required")
    }
    if req.MuteDuration < 0 {
        return nil, status.Error(codes.InvalidArgument, "mute_duration must be >= 0")
    }

    // ==================== 2. 校验群是否存在 ====================
    groupInfo, err := s.getGroupInternal(ctx, req.GroupId)
    if err != nil {
        return nil, err
    }

    // ==================== 3. 权限校验：操作者须为群主或管理员 ====================
    operatorRole, err := s.getMemberRoleInternal(ctx, req.GroupId, req.OperatorId)
    if err != nil {
        return nil, status.Error(codes.PermissionDenied, "operator is not a member of the group")
    }
    if operatorRole != int32(common.GROUP_MEMBER_ROLE_OWNER) && operatorRole != int32(common.GROUP_MEMBER_ROLE_ADMIN) {
        return nil, status.Error(codes.PermissionDenied, "only owner or admin can mute members")
    }

    // ==================== 4. 不能禁言群主 / 管理员不能禁言管理员 ====================
    if req.MemberId == groupInfo.OwnerId {
        return nil, status.Error(codes.PermissionDenied, "cannot mute group owner")
    }
    targetRole, err := s.getMemberRoleInternal(ctx, req.GroupId, req.MemberId)
    if err != nil {
        return nil, status.Error(codes.NotFound, "target member not found in group")
    }
    if operatorRole == int32(common.GROUP_MEMBER_ROLE_ADMIN) && targetRole == int32(common.GROUP_MEMBER_ROLE_ADMIN) {
        return nil, status.Error(codes.PermissionDenied, "admin cannot mute another admin")
    }

    now := time.Now().UnixMilli()
    isMuted := req.MuteDuration > 0
    muteUntil := int64(0)
    if isMuted {
        muteUntil = now + req.MuteDuration*1000 // 秒转毫秒
    }

    // ==================== 5. 延迟双删 + 更新禁言状态 — PgSQL ====================
    s.delayedDoubleDelete(ctx, fmt.Sprintf("group:member:%s:%s", req.GroupId, req.MemberId))

    _, err = s.db.ExecContext(ctx,
        `UPDATE group_members SET is_muted = $1, mute_until = $2
         WHERE group_id = $3 AND user_id = $4`,
        isMuted, muteUntil, req.GroupId, req.MemberId,
    )
    if err != nil {
        return nil, status.Error(codes.Internal, "update mute status failed")
    }

    // ==================== 6. 生产 Kafka 事件: group.member.mute.changed ====================
    muteEvent := &kafka_group.GroupMemberMuteChangedEvent{
        Header:     buildEventHeader("group", s.instanceID),
        GroupId:    req.GroupId,
        OperatorId: req.OperatorId,
        UserId:     req.MemberId,
        IsMuted:    isMuted,
        MuteUntil:  muteUntil,
        ChangeTime: now,
    }
    s.kafka.Produce(ctx, "group.member.mute.changed", req.GroupId, muteEvent)

    return &pb.MuteGroupMemberResponse{Meta: successMeta(ctx)}, nil
}
```

### 18. MuteGroup — 全员禁言/取消全员禁言

```go
func (s *GroupService) MuteGroup(ctx context.Context, req *pb.MuteGroupRequest) (*pb.MuteGroupResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.GroupId == "" {
        return nil, status.Error(codes.InvalidArgument, "group_id is required")
    }
    if req.OperatorId == "" {
        return nil, status.Error(codes.InvalidArgument, "operator_id is required")
    }

    // ==================== 2. 校验群是否存在 ====================
    groupInfo, err := s.getGroupInternal(ctx, req.GroupId)
    if err != nil {
        return nil, err
    }

    // ==================== 3. 权限校验：仅群主或管理员可全员禁言 ====================
    operatorRole, err := s.getMemberRoleInternal(ctx, req.GroupId, req.OperatorId)
    if err != nil {
        return nil, status.Error(codes.PermissionDenied, "operator is not a member of the group")
    }
    if operatorRole != int32(common.GROUP_MEMBER_ROLE_OWNER) && operatorRole != int32(common.GROUP_MEMBER_ROLE_ADMIN) {
        return nil, status.Error(codes.PermissionDenied, "only owner or admin can mute/unmute group")
    }

    // ==================== 4. 确定新状态 ====================
    var newStatus int32
    if req.IsMute {
        newStatus = int32(common.GROUP_STATUS_MUTED) // 2=全员禁言
    } else {
        newStatus = int32(common.GROUP_STATUS_NORMAL) // 1=正常
    }

    // 如果状态未变化，直接返回
    currentMuted := groupInfo.Status == common.GROUP_STATUS_MUTED
    if currentMuted == req.IsMute {
        return &pb.MuteGroupResponse{Meta: successMeta(ctx)}, nil
    }

    now := time.Now().UnixMilli()

    // ==================== 5. 延迟双删 + 更新群状态 — PgSQL ====================
    s.delayedDoubleDelete(ctx, fmt.Sprintf("group:info:%s", req.GroupId))

    _, err = s.db.ExecContext(ctx,
        `UPDATE groups SET status = $1, updated_at = $2 WHERE group_id = $3 AND status != 3`,
        newStatus, now, req.GroupId,
    )
    if err != nil {
        return nil, status.Error(codes.Internal, "update group mute status failed")
    }

    // ==================== 6. 生产 Kafka 事件: group.mute.changed ====================
    muteEvent := &kafka_group.GroupMuteChangedEvent{
        Header:     buildEventHeader("group", s.instanceID),
        GroupId:    req.GroupId,
        OperatorId: req.OperatorId,
        IsMuted:    req.IsMute,
        ChangeTime: now,
    }
    s.kafka.Produce(ctx, "group.mute.changed", req.GroupId, muteEvent)

    return &pb.MuteGroupResponse{Meta: successMeta(ctx)}, nil
}
```

### 19. SetGroupAnnouncement — 设置群公告

```go
func (s *GroupService) SetGroupAnnouncement(ctx context.Context, req *pb.SetGroupAnnouncementRequest) (*pb.SetGroupAnnouncementResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.GroupId == "" {
        return nil, status.Error(codes.InvalidArgument, "group_id is required")
    }
    if req.OperatorId == "" {
        return nil, status.Error(codes.InvalidArgument, "operator_id is required")
    }
    if len(req.Announcement) > 2048 {
        return nil, status.Error(codes.InvalidArgument, "announcement too long, max 2048 chars")
    }

    // ==================== 2. 校验群是否存在 ====================
    _, err := s.getGroupInternal(ctx, req.GroupId)
    if err != nil {
        return nil, err
    }

    // ==================== 3. 权限校验：操作者须为群主或管理员 ====================
    operatorRole, err := s.getMemberRoleInternal(ctx, req.GroupId, req.OperatorId)
    if err != nil {
        return nil, status.Error(codes.PermissionDenied, "operator is not a member of the group")
    }
    if operatorRole != int32(common.GROUP_MEMBER_ROLE_OWNER) && operatorRole != int32(common.GROUP_MEMBER_ROLE_ADMIN) {
        return nil, status.Error(codes.PermissionDenied, "only owner or admin can set announcement")
    }

    now := time.Now().UnixMilli()

    // ==================== 4. 延迟双删 + 更新群公告 — PgSQL ====================
    s.delayedDoubleDelete(ctx, fmt.Sprintf("group:info:%s", req.GroupId))

    _, err = s.db.ExecContext(ctx,
        `UPDATE groups SET announcement = $1, updated_at = $2 WHERE group_id = $3 AND status != 3`,
        req.Announcement, now, req.GroupId,
    )
    if err != nil {
        return nil, status.Error(codes.Internal, "update announcement failed")
    }

    // ==================== 5. 生产 Kafka 事件: group.info.updated ====================
    updateEvent := &kafka_group.GroupInfoUpdatedEvent{
        Header:     buildEventHeader("group", s.instanceID),
        GroupId:    req.GroupId,
        OperatorId: req.OperatorId,
        UpdatedFields: map[string]string{
            "announcement": req.Announcement,
        },
        UpdateTime: now,
    }
    s.kafka.Produce(ctx, "group.info.updated", req.GroupId, updateEvent)

    return &pb.SetGroupAnnouncementResponse{Meta: successMeta(ctx)}, nil
}
```

### 20. GetGroupApplications — 获取入群申请列表

```go
func (s *GroupService) GetGroupApplications(ctx context.Context, req *pb.GetGroupApplicationsRequest) (*pb.GetGroupApplicationsResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.GroupId == "" {
        return nil, status.Error(codes.InvalidArgument, "group_id is required")
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
        `SELECT COUNT(*) FROM group_applications WHERE group_id = $1`,
        req.GroupId,
    ).Scan(&total)
    if err != nil {
        return nil, status.Error(codes.Internal, "count group applications failed")
    }

    // ==================== 3. 分页查询申请列表 — PgSQL ====================
    rows, err := s.db.QueryContext(ctx,
        `SELECT application_id, group_id, user_id, type, status, message, operator_id, handle_time, created_at
         FROM group_applications
         WHERE group_id = $1
         ORDER BY created_at DESC
         LIMIT $2 OFFSET $3`,
        req.GroupId, pageSize, offset,
    )
    if err != nil {
        return nil, status.Error(codes.Internal, "query group applications failed")
    }
    defer rows.Close()

    var applications []*pb.GroupApplicationInfo
    userIDs := make([]string, 0)
    for rows.Next() {
        var a pb.GroupApplicationInfo
        var appType, appStatus int32
        if err := rows.Scan(&a.ApplicationId, &a.GroupId, &a.UserId, &appType,
            &appStatus, &a.Message, &a.OperatorId, &a.HandleTime, &a.CreatedAt); err != nil {
            continue
        }
        a.Type = common.GroupApplicationType(appType)
        a.Status = common.GroupApplicationStatus(appStatus)
        applications = append(applications, &a)
        userIDs = append(userIDs, a.UserId)
    }

    // ==================== 4. 批量补充用户信息和群名称 — RPC ====================
    // 获取群名称
    groupInfo, _ := s.getGroupInternal(ctx, req.GroupId)

    if len(userIDs) > 0 {
        batchResp, err := s.userClient.BatchGetUser(ctx, &user_pb.BatchGetUserRequest{UserIds: userIDs})
        if err == nil {
            userMap := make(map[string]*user_pb.UserInfo)
            for _, u := range batchResp.Users {
                userMap[u.UserId] = u
            }
            for _, a := range applications {
                if u, ok := userMap[a.UserId]; ok {
                    a.UserNickname = u.Nickname
                    a.UserAvatar = u.AvatarUrl
                }
                if groupInfo != nil {
                    a.GroupName = groupInfo.Name
                }
            }
        }
    }

    totalPages := int32(0)
    if total > 0 {
        totalPages = int32((total + int64(pageSize) - 1) / int64(pageSize))
    }

    return &pb.GetGroupApplicationsResponse{
        Meta:         successMeta(ctx),
        Applications: applications,
        Pagination: &common.PaginationResult{
            Page: page, PageSize: pageSize, Total: total, TotalPages: totalPages,
        },
    }, nil
}
```

### 21. HandleGroupApplication — 处理入群申请

```go
func (s *GroupService) HandleGroupApplication(ctx context.Context, req *pb.HandleGroupApplicationRequest) (*pb.HandleGroupApplicationResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.GroupId == "" {
        return nil, status.Error(codes.InvalidArgument, "group_id is required")
    }
    if req.ApplicationId == "" {
        return nil, status.Error(codes.InvalidArgument, "application_id is required")
    }
    if req.OperatorId == "" {
        return nil, status.Error(codes.InvalidArgument, "operator_id is required")
    }
    if req.Action != 1 && req.Action != 2 {
        return nil, status.Error(codes.InvalidArgument, "action must be 1(approve) or 2(reject)")
    }

    // ==================== 2. 校验群是否存在 ====================
    groupInfo, err := s.getGroupInternal(ctx, req.GroupId)
    if err != nil {
        return nil, err
    }

    // ==================== 3. 权限校验：操作者须为群主或管理员 ====================
    operatorRole, err := s.getMemberRoleInternal(ctx, req.GroupId, req.OperatorId)
    if err != nil {
        return nil, status.Error(codes.PermissionDenied, "operator is not a member of the group")
    }
    if operatorRole != int32(common.GROUP_MEMBER_ROLE_OWNER) && operatorRole != int32(common.GROUP_MEMBER_ROLE_ADMIN) {
        return nil, status.Error(codes.PermissionDenied, "only owner or admin can handle applications")
    }

    // ==================== 4. 查询申请记录 — PgSQL ====================
    var applicantID string
    var applicationType, currentStatus int32
    err = s.db.QueryRowContext(ctx,
        `SELECT user_id, type, status FROM group_applications
         WHERE application_id = $1 AND group_id = $2`,
        req.ApplicationId, req.GroupId,
    ).Scan(&applicantID, &applicationType, &currentStatus)
    if err == sql.ErrNoRows {
        return nil, status.Error(codes.NotFound, "application not found")
    }
    if err != nil {
        return nil, status.Error(codes.Internal, "query application failed")
    }

    if currentStatus != 1 { // 非待处理状态
        return nil, status.Error(codes.FailedPrecondition, "application already handled")
    }

    now := time.Now().UnixMilli()

    // ==================== 5. 更新申请状态 — PgSQL ====================
    var newStatus int32
    if req.Action == 1 {
        newStatus = 2 // 已通过
    } else {
        newStatus = 3 // 已拒绝
    }

    _, err = s.db.ExecContext(ctx,
        `UPDATE group_applications SET status = $1, operator_id = $2, handle_time = $3
         WHERE application_id = $4 AND status = 1`,
        newStatus, req.OperatorId, now, req.ApplicationId,
    )
    if err != nil {
        return nil, status.Error(codes.Internal, "update application status failed")
    }

    // ==================== 6. 如果通过：将申请人加入群 ====================
    if req.Action == 1 {
        // 检查群人数上限
        if groupInfo.MemberCount >= groupInfo.MaxMembers {
            // 回滚申请状态
            s.db.ExecContext(ctx,
                `UPDATE group_applications SET status = 1, operator_id = '', handle_time = 0
                 WHERE application_id = $1`,
                req.ApplicationId,
            )
            return nil, status.Error(codes.ResourceExhausted, "group member count has reached the limit")
        }

        inviterID := ""
        if applicationType == 2 { // 被邀请类型
            inviterID = req.OperatorId
        }

        err = s.addMemberInternal(ctx, req.GroupId, applicantID, inviterID, int32(common.GROUP_MEMBER_ROLE_MEMBER), now)
        if err != nil {
            return nil, status.Error(codes.Internal, "add member failed")
        }

        // 获取申请人信息
        userResp, _ := s.userClient.GetUser(ctx, &user_pb.GetUserRequest{UserId: applicantID})
        applicantNickname := ""
        if userResp != nil {
            applicantNickname = userResp.User.Nickname
        }

        // 生产 group.member.joined 事件
        joinType := kafka_group.JOIN_TYPE_APPLIED
        if applicationType == 2 {
            joinType = kafka_group.JOIN_TYPE_INVITED
        }
        joinEvent := &kafka_group.GroupMemberJoinedEvent{
            Header:      buildEventHeader("group", s.instanceID),
            GroupId:     req.GroupId,
            GroupName:   groupInfo.Name,
            UserId:      applicantID,
            Nickname:    applicantNickname,
            InviterId:   inviterID,
            JoinType:    joinType,
            MemberCount: groupInfo.MemberCount + 1,
            JoinTime:    now,
        }
        s.kafka.Produce(ctx, "group.member.joined", req.GroupId, joinEvent)
    }

    // ==================== 7. 生产 Kafka 事件: group.application.handled ====================
    handledEvent := &kafka_group.GroupApplicationHandledEvent{
        Header:        buildEventHeader("group", s.instanceID),
        ApplicationId: req.ApplicationId,
        GroupId:       req.GroupId,
        ApplicantId:   applicantID,
        HandlerId:     req.OperatorId,
        Approved:      req.Action == 1,
        Reason:        "",
        HandleTime:    now,
    }
    s.kafka.Produce(ctx, "group.application.handled", req.GroupId, handledEvent)

    return &pb.HandleGroupApplicationResponse{Meta: successMeta(ctx)}, nil
}
```

### 22. GetUserGroups — 获取用户所在群列表

```go
func (s *GroupService) GetUserGroups(ctx context.Context, req *pb.GetUserGroupsRequest) (*pb.GetUserGroupsResponse, error) {
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

    // ==================== 2. 尝试从 Redis 缓存获取用户群列表 ====================
    userGroupsKey := fmt.Sprintf("group:user_groups:%s", req.UserId)
    cachedGroupIDs, err := s.redis.SMembers(ctx, userGroupsKey).Result()
    if err == nil && len(cachedGroupIDs) > 0 {
        // 缓存命中：根据分页从缓存的 group_ids 获取群详情
        total := int64(len(cachedGroupIDs))
        start := int(offset)
        end := start + int(pageSize)
        if start >= len(cachedGroupIDs) {
            return &pb.GetUserGroupsResponse{
                Meta:   successMeta(ctx),
                Groups: []*pb.GroupInfo{},
                Pagination: &common.PaginationResult{
                    Page: page, PageSize: pageSize, Total: total,
                    TotalPages: int32((total + int64(pageSize) - 1) / int64(pageSize)),
                },
            }, nil
        }
        if end > len(cachedGroupIDs) {
            end = len(cachedGroupIDs)
        }
        pageGroupIDs := cachedGroupIDs[start:end]

        // 批量获取群详情
        batchResp, err := s.BatchGetGroup(ctx, &pb.BatchGetGroupRequest{GroupIds: pageGroupIDs})
        if err != nil {
            return nil, status.Error(codes.Internal, "batch get groups failed")
        }

        totalPages := int32((total + int64(pageSize) - 1) / int64(pageSize))
        return &pb.GetUserGroupsResponse{
            Meta:   successMeta(ctx),
            Groups: batchResp.Groups,
            Pagination: &common.PaginationResult{
                Page: page, PageSize: pageSize, Total: total, TotalPages: totalPages,
            },
        }, nil
    }

    // ==================== 3. 缓存未命中，查 PgSQL ====================
    // 先查总数
    var total int64
    err = s.db.QueryRowContext(ctx,
        `SELECT COUNT(*) FROM group_members gm
         INNER JOIN groups g ON gm.group_id = g.group_id
         WHERE gm.user_id = $1 AND g.status != 3`,
        req.UserId,
    ).Scan(&total)
    if err != nil {
        return nil, status.Error(codes.Internal, "count user groups failed")
    }

    // 分页查询
    rows, err := s.db.QueryContext(ctx,
        `SELECT g.group_id, g.name, g.avatar_url, g.description, g.owner_id, g.member_count,
                g.max_members, g.status, g.announcement, g.join_need_approve, g.invite_need_approve,
                g.created_at, g.updated_at
         FROM group_members gm
         INNER JOIN groups g ON gm.group_id = g.group_id
         WHERE gm.user_id = $1 AND g.status != 3
         ORDER BY gm.joined_at DESC
         LIMIT $2 OFFSET $3`,
        req.UserId, pageSize, offset,
    )
    if err != nil {
        return nil, status.Error(codes.Internal, "query user groups failed")
    }
    defer rows.Close()

    var groups []*pb.GroupInfo
    allGroupIDs := make([]string, 0)
    for rows.Next() {
        var g pb.GroupInfo
        var statusVal int32
        var joinApprove, inviteApprove bool
        if err := rows.Scan(&g.GroupId, &g.Name, &g.AvatarUrl, &g.Description, &g.OwnerId,
            &g.MemberCount, &g.MaxMembers, &statusVal, &g.Announcement,
            &joinApprove, &inviteApprove, &g.CreatedAt, &g.UpdatedAt); err != nil {
            continue
        }
        g.Status = common.GroupStatus(statusVal)
        g.JoinNeedApprove = joinApprove
        g.InviteNeedApprove = inviteApprove
        groups = append(groups, &g)
        allGroupIDs = append(allGroupIDs, g.GroupId)
    }

    // ==================== 4. 回填 Redis 缓存（完整群列表） ====================
    // 查询用户所有群 ID（不分页）用于缓存
    go func() {
        allRows, err := s.db.QueryContext(context.Background(),
            `SELECT gm.group_id FROM group_members gm
             INNER JOIN groups g ON gm.group_id = g.group_id
             WHERE gm.user_id = $1 AND g.status != 3`,
            req.UserId,
        )
        if err != nil {
            return
        }
        defer allRows.Close()
        var ids []interface{}
        for allRows.Next() {
            var gid string
            allRows.Scan(&gid)
            ids = append(ids, gid)
        }
        if len(ids) > 0 {
            pipe := s.redis.Pipeline()
            pipe.Del(context.Background(), userGroupsKey)
            pipe.SAdd(context.Background(), userGroupsKey, ids...)
            pipe.Expire(context.Background(), userGroupsKey, 30*time.Minute)
            pipe.Exec(context.Background())
        }
    }()

    totalPages := int32(0)
    if total > 0 {
        totalPages = int32((total + int64(pageSize) - 1) / int64(pageSize))
    }

    return &pb.GetUserGroupsResponse{
        Meta:   successMeta(ctx),
        Groups: groups,
        Pagination: &common.PaginationResult{
            Page: page, PageSize: pageSize, Total: total, TotalPages: totalPages,
        },
    }, nil
}
```

---

## 内部辅助方法

### delayedDoubleDelete — 延迟双删

```go
// delayedDoubleDelete 延迟双删：保证缓存与 DB 的最终一致性
// 原理：先删缓存 → 更新 DB → 500ms 后再次删除缓存，防止并发读回填脏数据
func (s *GroupService) delayedDoubleDelete(ctx context.Context, keys ...string) {
    if len(keys) == 0 {
        return
    }
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

### getGroupInternal — 内部获取群信息（带缓存）

```go
func (s *GroupService) getGroupInternal(ctx context.Context, groupID string) (*pb.GroupInfo, error) {
    // 优先 Redis
    groupInfoKey := fmt.Sprintf("group:info:%s", groupID)
    cached, err := s.redis.HGetAll(ctx, groupInfoKey).Result()
    if err == nil && len(cached) > 0 {
        g := parseGroupInfoFromCache(cached)
        if g.Status == int32(common.GROUP_STATUS_DISBANDED) {
            return nil, status.Error(codes.NotFound, "group has been disbanded")
        }
        return g, nil
    }

    // 回退 PgSQL
    var g pb.GroupInfo
    var statusVal int32
    var joinApprove, inviteApprove bool
    var extra string
    err = s.db.QueryRowContext(ctx,
        `SELECT group_id, name, avatar_url, description, owner_id, member_count, max_members,
                status, announcement, join_need_approve, invite_need_approve, extra, created_at, updated_at
         FROM groups WHERE group_id = $1`,
        groupID,
    ).Scan(&g.GroupId, &g.Name, &g.AvatarUrl, &g.Description, &g.OwnerId,
        &g.MemberCount, &g.MaxMembers, &statusVal, &g.Announcement,
        &joinApprove, &inviteApprove, &extra, &g.CreatedAt, &g.UpdatedAt)
    if err == sql.ErrNoRows {
        return nil, status.Error(codes.NotFound, "group not found")
    }
    if err != nil {
        return nil, status.Error(codes.Internal, "query group failed")
    }
    g.Status = common.GroupStatus(statusVal)
    g.JoinNeedApprove = joinApprove
    g.InviteNeedApprove = inviteApprove

    if g.Status == common.GROUP_STATUS_DISBANDED {
        return nil, status.Error(codes.NotFound, "group has been disbanded")
    }

    // 回填缓存
    pipe := s.redis.Pipeline()
    pipe.HSet(ctx, groupInfoKey, map[string]interface{}{
        "group_id": g.GroupId, "name": g.Name, "avatar_url": g.AvatarUrl,
        "description": g.Description, "owner_id": g.OwnerId,
        "member_count": g.MemberCount, "max_members": g.MaxMembers,
        "status": int32(g.Status), "announcement": g.Announcement,
        "join_need_approve": fmt.Sprintf("%t", g.JoinNeedApprove),
        "invite_need_approve": fmt.Sprintf("%t", g.InviteNeedApprove),
        "created_at": g.CreatedAt, "updated_at": g.UpdatedAt,
    })
    pipe.Expire(ctx, groupInfoKey, 30*time.Minute)
    pipe.Exec(ctx)

    return &g, nil
}
```

### getMemberRoleInternal — 内部获取成员角色

```go
func (s *GroupService) getMemberRoleInternal(ctx context.Context, groupID, userID string) (int32, error) {
    // 优先 Redis 缓存
    memberKey := fmt.Sprintf("group:member:%s:%s", groupID, userID)
    roleStr, err := s.redis.HGet(ctx, memberKey, "role").Result()
    if err == nil && roleStr != "" {
        role, _ := strconv.Atoi(roleStr)
        return int32(role), nil
    }

    // 回退 PgSQL
    var role int32
    err = s.db.QueryRowContext(ctx,
        `SELECT role FROM group_members WHERE group_id = $1 AND user_id = $2`,
        groupID, userID,
    ).Scan(&role)
    if err == sql.ErrNoRows {
        return 0, fmt.Errorf("not a member")
    }
    if err != nil {
        return 0, fmt.Errorf("query member role failed: %w", err)
    }
    return role, nil
}
```

### checkMembershipInternal — 内部检查是否为群成员

```go
func (s *GroupService) checkMembershipInternal(ctx context.Context, groupID, userID string) (bool, error) {
    // 优先 Redis SET
    membersKey := fmt.Sprintf("group:members:%s", groupID)
    exists, err := s.redis.SIsMember(ctx, membersKey, userID).Result()
    if err == nil {
        // 确认 SET key 存在（避免空集合误判）
        keyExists, _ := s.redis.Exists(ctx, membersKey).Result()
        if keyExists > 0 {
            return exists, nil
        }
    }

    // 回退 PgSQL
    var isMember bool
    err = s.db.QueryRowContext(ctx,
        `SELECT EXISTS(SELECT 1 FROM group_members WHERE group_id = $1 AND user_id = $2)`,
        groupID, userID,
    ).Scan(&isMember)
    if err != nil {
        return false, fmt.Errorf("check membership failed: %w", err)
    }
    return isMember, nil
}
```

### addMemberInternal — 内部添加群成员

```go
func (s *GroupService) addMemberInternal(ctx context.Context, groupID, userID, inviterID string, role int32, now int64) error {
    // ==================== A. 延迟双删 + 事务：插入成员 + 更新群成员数 — PgSQL ====================
    s.delayedDoubleDelete(ctx,
        fmt.Sprintf("group:info:%s", groupID),
        fmt.Sprintf("group:members:%s", groupID),
        fmt.Sprintf("group:member_count:%s", groupID),
        fmt.Sprintf("group:user_groups:%s", userID),
    )

    tx, err := s.db.BeginTx(ctx, nil)
    if err != nil {
        return fmt.Errorf("begin tx failed: %w", err)
    }
    defer tx.Rollback()

    _, err = tx.ExecContext(ctx,
        `INSERT INTO group_members (group_id, user_id, nickname, role, is_muted, mute_until, joined_at, invite_by)
         VALUES ($1, $2, '', $3, false, 0, $4, $5)
         ON CONFLICT (group_id, user_id) DO NOTHING`,
        groupID, userID, role, now, inviterID,
    )
    if err != nil {
        return fmt.Errorf("insert member failed: %w", err)
    }

    _, err = tx.ExecContext(ctx,
        `UPDATE groups SET member_count = member_count + 1, updated_at = $1
         WHERE group_id = $2`,
        now, groupID,
    )
    if err != nil {
        return fmt.Errorf("update member count failed: %w", err)
    }

    if err = tx.Commit(); err != nil {
        return fmt.Errorf("commit tx failed: %w", err)
    }

    // ==================== B. 创建会话视图 — RPC Conversation.CreateUserConversation ====================
    go func() {
        _, err := s.conversationClient.CreateUserConversation(context.Background(), &conversation_pb.CreateUserConversationRequest{
            UserId:           userID,
            PeerId:           groupID,
            ConversationType: common.CONVERSATION_TYPE_GROUP,
        })
        if err != nil {
            log.Error("创建新成员群会话视图失败", "user_id", userID, "group_id", groupID, "err", err)
        }
    }()

    return nil
}
```

### parseGroupInfoFromCache — 解析 Redis 缓存的群信息

```go
func parseGroupInfoFromCache(cached map[string]string) *pb.GroupInfo {
    g := &pb.GroupInfo{
        GroupId:     cached["group_id"],
        Name:        cached["name"],
        AvatarUrl:   cached["avatar_url"],
        Description: cached["description"],
        OwnerId:     cached["owner_id"],
        Announcement: cached["announcement"],
    }
    g.MemberCount, _ = strconv.Atoi(cached["member_count"])
    g.MaxMembers, _ = strconv.Atoi(cached["max_members"])
    statusVal, _ := strconv.Atoi(cached["status"])
    g.Status = common.GroupStatus(statusVal)
    g.JoinNeedApprove = cached["join_need_approve"] == "true"
    g.InviteNeedApprove = cached["invite_need_approve"] == "true"
    g.CreatedAt, _ = strconv.ParseInt(cached["created_at"], 10, 64)
    g.UpdatedAt, _ = strconv.ParseInt(cached["updated_at"], 10, 64)
    return g
}
```

### parseGroupMemberFromCache — 解析 Redis 缓存的成员信息

```go
func parseGroupMemberFromCache(cached map[string]string) *pb.GroupMemberInfo {
    m := &pb.GroupMemberInfo{
        GroupId:  cached["group_id"],
        UserId:   cached["user_id"],
        Nickname: cached["nickname"],
        InviteBy: cached["invite_by"],
    }
    roleVal, _ := strconv.Atoi(cached["role"])
    m.Role = common.GroupMemberRole(roleVal)
    m.IsMuted = cached["is_muted"] == "true"
    m.MuteUntil, _ = strconv.ParseInt(cached["mute_until"], 10, 64)
    m.JoinedAt, _ = strconv.ParseInt(cached["joined_at"], 10, 64)
    return m
}
```

---

## 可观测性接入（OpenTelemetry）

> Group 服务是数据量最大的 CRUD 服务之一，需重点观测：群创建/解散速率、成员变动频率、群操作 DB 写入延迟、Redis 缓存命中率、邀请/申请审批延迟。

### 第一步：服务启动时初始化 OTel SDK

```go
func main() {
    ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
    defer cancel()

    otelShutdown := observability.InitOpenTelemetry(ctx, "group", "v1.0.0")
    defer otelShutdown(ctx)
    observability.SetupLogger("group")
}
```

### 第二步：gRPC Server/Client Interceptor

```go
grpcServer := grpc.NewServer(
    grpc.ChainUnaryInterceptor(otelgrpc.UnaryServerInterceptor()),
    grpc.ChainStreamInterceptor(otelgrpc.StreamServerInterceptor()), // 统一添加 Stream Interceptor
)
// Client — 调用 Conversation.CreateChannel / Notification.SendSystemNotification / User.GetUser
convConn := grpc.Dial("conversation:50051",
    grpc.WithChainUnaryInterceptor(otelgrpc.UnaryClientInterceptor()),
)
notifConn := grpc.Dial("notification:50051",
    grpc.WithChainUnaryInterceptor(otelgrpc.UnaryClientInterceptor()),
)
```

### 第三步：注册 Group 专属业务指标

```go
var meter = otel.Meter("im-chat/group")

var (
    // 群创建/解散计数
    groupCreated, _ = meter.Int64Counter("group.created_total",
        metric.WithDescription("群创建总数"))
    groupDismissed, _ = meter.Int64Counter("group.dismissed_total",
        metric.WithDescription("群解散总数"))

    // 成员变动计数
    memberJoined, _ = meter.Int64Counter("group.member_joined_total",
        metric.WithDescription("新成员加入总数"))
    memberLeft, _ = meter.Int64Counter("group.member_left_total",
        metric.WithDescription("成员退出/被踢总数"))

    // 群人数分布（可用于群规模分析）
    groupMemberCount, _ = meter.Int64Histogram("group.member_count",
        metric.WithDescription("群操作时的群人数快照"))

    // 缓存命中率
    groupCacheHit, _ = meter.Int64Counter("group.cache_hit_total",
        metric.WithDescription("群信息/成员列表缓存命中"))
    groupCacheMiss, _ = meter.Int64Counter("group.cache_miss_total",
        metric.WithDescription("群信息/成员列表缓存未命中"))

    // DB 批量写入延迟（群操作经常涉及批量 INSERT/UPDATE）
    batchDBWriteLatency, _ = meter.Float64Histogram("group.batch_db_write_latency_ms",
        metric.WithDescription("群操作批量 DB 写入延迟"), metric.WithUnit("ms"))
)
```

在业务代码中埋点：

```go
// CreateGroup 中
groupCreated.Add(ctx, 1)

// AddGroupMembers 中
memberJoined.Add(ctx, int64(len(memberIDs)))
groupMemberCount.Record(ctx, int64(currentMemberCount))

// KickGroupMember / LeaveGroup 中
memberLeft.Add(ctx, 1, metric.WithAttributes(
    attribute.String("reason", "kicked"), // kicked / left
))

// getGroupInfoWithCache 中
if cached != nil {
    groupCacheHit.Add(ctx, 1)
} else {
    groupCacheMiss.Add(ctx, 1)
}
```

### 第四步：Kafka Producer 埋点 + 结构化 JSON 日志 + buildEventHeader 改造

与所有服务一致。`buildEventHeader` 改为使用共享包：

```go
header := observability.BuildEventHeader(ctx, "group", instanceID)
```

### 补充：基础设施指标注册

Group 服务依赖 PgSQL（群数据存储）+ Redis（缓存群信息/成员列表），需注册连接池指标：

```go
func main() {
    // ... initOpenTelemetry ...
    observability.RegisterDBPoolMetrics(db, "group")
    observability.RegisterRedisPoolMetrics(redisClient, "group")
}
```

### 补充：Span 错误记录

```go
func (s *GroupServer) CreateGroup(ctx context.Context, req *pb.CreateGroupRequest) (*pb.CreateGroupResponse, error) {
    ctx, span := otel.Tracer("group").Start(ctx, "CreateGroup")
    defer span.End()

    result, err := s.doCreateGroup(ctx, req)
    if err != nil {
        observability.RecordError(span, err)
        return nil, err
    }
    return result, nil
}
```

### 接入要点总结

| 步骤 | 改动位置 | 效果 |
|------|---------|------|
| 1. `observability.InitOpenTelemetry` | main() | TracerProvider + MeterProvider + Runtime Metrics |
| 2. gRPC Interceptor | Server(Unary+Stream) + Client | RPC 自动 trace |
| 3. 自定义业务指标 | CreateGroup/AddMembers/KickMember | 群操作速率、成员变动、缓存命中率 |
| 4. Kafka + 日志 + EventHeader | `observability.BuildEventHeader` | 链路追踪 + trace_id 贯穿 |
| 5. 基础设施指标 | `RegisterDBPoolMetrics` + `RegisterRedisPoolMetrics` | DB/Redis 连接池健康 |
| 6. Span 错误记录 | `observability.RecordError` | 错误 trace 在 Tempo 可见 |
