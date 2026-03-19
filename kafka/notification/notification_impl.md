# Notification 系统通知服务 — Kafka 消费者实现伪代码

## 概述

Notification 服务是 Kafka 事件的**重要消费者**，接收来自 Relation（好友关系）、Group（群组）、Auth（认证）、User（用户）、Config（配置）等服务的事件，自动生成对应的系统通知。
同时，Notification 服务也是 Kafka 事件的**生产者**，创建通知后投递事件给 Push 服务完成推送投递和角标更新。

**核心职责：**
- 消费好友关系事件 → 生成好友请求/通过通知
- 消费群组事件 → 生成群申请/申请结果/被踢/解散等通知
- 消费认证安全事件 → 生成账号安全通知
- 消费用户注销事件 → 清理该用户的所有通知数据
- 消费配置变更事件 → 热更新通知相关配置

**设计原则：**
- 每个消费者都做 Kafka 幂等去重（`notif:kafka:dedup:{event_id}`）
- 创建通知统一走内部方法 `createNotificationInternal`，避免重复代码
- 通知创建后同步更新 Redis 未读计数 + 列表缓存，并投递 `notification.created` 事件
- 对于面向群管理员的通知（如群申请），需查询所有管理员 ID 并逐一创建

**⚠️ LOW-05 本地缓存优化建议：**
> Notification Consumer 频繁调用 `User.GetUser` 和 `Group.GetGroupAdmins` RPC。
> 建议增加本地 Redis 缓存层（参考 Message/Push 服务模式）：
> - `notif:local:user:{user_id}` — 用户昵称/头像缓存（TTL 10 分钟，Kafka `user.profile.updated` 事件驱动失效）
> - `notif:local:group_admins:{group_id}` — 群管理员 ID 列表缓存（TTL 5 分钟，Kafka `group.member.role.changed` 事件驱动失效）
> - `notif:local:group_info:{group_id}` — 群名称/头像缓存（TTL 10 分钟，Kafka `group.info.updated` 事件驱动失效）
> 缓存命中率预估 > 90%（通知场景中同一用户/群组的数据在短时间内被多次引用）

## 生产 Topic 列表

| Topic | 事件 | 生产时机 | Key | 消费方 |
|-------|------|----------|-----|--------|
| `notification.created` | NotificationCreatedEvent | 每条通知创建后 | target_user_id | Push（推送给客户端 + 离线推送） |
| `notification.read` | NotificationReadEvent | 通知标记已读（RPC 触发，此处不涉及） | user_id | Push（更新角标） |
| `notification.read.all` | NotificationReadAllEvent | 全部标记已读（RPC 触发，此处不涉及） | user_id | Push（更新角标） |
| `notification.deleted` | NotificationDeletedEvent | 通知删除（RPC 触发，此处不涉及） | user_id | Push（更新角标） |

## 消费 Topic 列表

| Topic | 来源 | 用途 | 并发度 | 重试 |
|-------|------|------|--------|------|
| `relation.friend.request` | Relation | 收到好友申请 → 创建 FRIEND_REQUEST 通知 | 4 | 3 |
| `relation.friend.accepted` | Relation | 好友通过 → 创建 FRIEND_ACCEPTED 通知 | 4 | 3 |
| `relation.friend.rejected` | Relation | 好友拒绝 → 通知申请者 | 4 | 3 |
| `relation.friend.deleted` | Relation | 好友删除 → 通知被删除者 | 4 | 3 |
| `group.application` | Group | 入群申请 → 创建 GROUP_APPLICATION 通知（通知群管理员） | 4 | 3 |
| `group.application.handled` | Group | 入群申请处理结果 → 创建 GROUP_APPLICATION_RESULT 通知 | 4 | 3 |
| `group.member.joined` | Group | 新成员加入 → 通知群管理员 | 4 | 3 |
| `group.member.kicked` | Group | 成员被踢 → 创建 SERVICE_NOTIFICATION 通知 | 2 | 3 |
| `group.member.left` | Group | 成员退出 → 通知群管理员 | 2 | 3 |
| `group.member.role.changed` | Group | 角色变更 → 通知被变更成员 | 2 | 3 |
| `group.member.mute.changed` | Group | 禁言/解禁 → 通知被操作成员 | 2 | 3 |
| `group.transfer` | Group | 群主转让 → 通知新群主 | 2 | 3 |
| `group.dissolved` | Group | 群解散 → 创建 SERVICE_NOTIFICATION 通知（通知所有成员） | 2 | 3 |
| `user.banned` | User | 账号封禁 → 通知被封禁用户 | 2 | 3 |
| `auth.login.success` | Auth | 新设备登录 → 创建 ACCOUNT_SECURITY 通知（可选） | 2 | 2 |
| `auth.password.changed` | Auth | 密码变更 → 安全通知 | 2 | 3 |
| `audit.risk.alert` | Audit | 风控告警 → 通知管理员 | 2 | 3 |
| `session.expired` | Session | 会话过期 → 安全通知 | 2 | 2 |
| `offline.queue.overflow` | OfflineQueue | 离线队列溢出 → 通知用户 | 2 | 2 |
| `system.broadcast` | Config | 系统广播 → 持久化广播记录 | 2 | 3 |
| `user.deactivated` | User | 用户注销 → 清理该用户所有通知数据 | 1 | 5 |
| `config.changed` | Config | 配置变更 → 热更新通知相关配置 | 1 | 1 |

## Redis Key 设计（消费者专用）

| Key 格式 | 类型 | 值 | TTL | 说明 |
|----------|------|-----|-----|------|
| `notif:kafka:dedup:{event_id}` | STRING | "1" | 24h | Kafka 消费幂等去重 |
| `notif:unread:{user_id}` | STRING | 总未读通知数 | 24h | 创建通知时 INCR（与 RPC 共享） |
| `notif:unread_type:{user_id}:{type}` | STRING | 该类型未读通知数 | 24h | 创建通知时 INCR（与 RPC 共享） |
| `notif:list:{user_id}` | ZSET | member=notification_id, score=created_at | 1h | 最近通知列表（与 RPC 共享） |
| `notif:login_notify:{user_id}` | STRING | "1" | 24h | 登录通知限频：同一用户 24h 内最多 1 条新设备登录通知 |

---

## 内部公共方法

### createNotificationInternal — 内部创建通知（消费者统一入口）

> 所有消费者创建通知都通过此方法，统一处理：生成 ID → 写 PgSQL → 更新 Redis → 投递 Kafka。

```go
// createNotificationParams 创建通知参数
type createNotificationParams struct {
    UserID    string                  // 接收者用户 ID
    Type      common.NotificationType // 通知类型
    Title     string                  // 通知标题
    Content   string                  // 通知正文
    SenderID  string                  // 发送者/触发者 ID
    Data      map[string]string       // 扩展数据（业务 JSON）
    ExpiresIn *time.Duration          // 过期时间间隔（nil 表示不过期）
}

// createNotificationInternal 内部统一创建通知方法
func (c *NotificationConsumer) createNotificationInternal(ctx context.Context, params *createNotificationParams) (string, error) {
    // 1. 生成通知 ID — Snowflake
    notificationID := c.snowflake.Generate().String()
    now := time.Now()
    nowMs := now.UnixMilli()

    // 2. 序列化扩展数据
    dataJSON := "{}"
    if len(params.Data) > 0 {
        dataBytes, err := json.Marshal(params.Data)
        if err != nil {
            return "", fmt.Errorf("序列化扩展数据失败: %w", err)
        }
        dataJSON = string(dataBytes)
    }

    // 3. 计算过期时间
    var expiresAt *time.Time
    if params.ExpiresIn != nil {
        exp := now.Add(*params.ExpiresIn)
        expiresAt = &exp
    }

    // 4. 写入 PgSQL
    _, err := c.db.ExecContext(ctx,
        `INSERT INTO notifications (notification_id, user_id, type, title, content, data, sender_id, is_read, is_deleted, created_at, expires_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, false, false, $8, $9)`,
        notificationID, params.UserID, int(params.Type),
        params.Title, params.Content, dataJSON, params.SenderID,
        now, expiresAt,
    )
    if err != nil {
        return "", fmt.Errorf("写入通知到 PgSQL 失败: %w", err)
    }

    // 5. 更新 Redis 未读计数和列表
    pipe := c.redis.Pipeline()

    // 总未读数 +1  Key: notif:unread:{user_id}
    unreadKey := fmt.Sprintf("notif:unread:%s", params.UserID)
    pipe.Incr(ctx, unreadKey)
    pipe.Expire(ctx, unreadKey, 24*time.Hour)

    // 按类型未读数 +1  Key: notif:unread_type:{user_id}:{type}
    unreadTypeKey := fmt.Sprintf("notif:unread_type:%s:%d", params.UserID, int(params.Type))
    pipe.Incr(ctx, unreadTypeKey)
    pipe.Expire(ctx, unreadTypeKey, 24*time.Hour)

    // 添加到最近通知列表 ZSET  Key: notif:list:{user_id}
    listKey := fmt.Sprintf("notif:list:%s", params.UserID)
    pipe.ZAdd(ctx, listKey, &redis.Z{
        Score:  float64(nowMs),
        Member: notificationID,
    })
    pipe.Expire(ctx, listKey, 1*time.Hour)
    pipe.ZRemRangeByRank(ctx, listKey, 0, -101) // 保留最近 100 条

    if _, err := pipe.Exec(ctx); err != nil {
        log.Warn("更新 Redis 通知缓存失败（不阻塞）",
            "notification_id", notificationID, "user_id", params.UserID, "err", err)
    }

    // 6. 获取发送者信息（用于推送展示）
    var senderNickname, senderAvatar string
    if params.SenderID != "" {
        senderResp, err := c.userClient.GetUser(ctx, &user_pb.GetUserRequest{UserId: params.SenderID})
        if err == nil && senderResp.User != nil {
            senderNickname = senderResp.User.Nickname
            senderAvatar = senderResp.User.AvatarUrl
        }
    }

    // 7. 投递 Kafka 事件 — notification.created → Push 服务
    createdEvent := &kafka_notification.NotificationCreatedEvent{
        Header:          buildEventHeader("notification", c.instanceID),
        NotificationId:  notificationID,
        TargetUserId:    params.UserID,
        Type:            params.Type,
        Title:           params.Title,
        Content:         params.Content,
        SenderId:        params.SenderID,
        SenderNickname:  senderNickname,
        SenderAvatar:    senderAvatar,
        Extra:           params.Data,
        CreateTime:      nowMs,
    }
    if expiresAt != nil {
        createdEvent.ExpireTime = expiresAt.UnixMilli()
    }
    if err := c.kafka.Produce(ctx, "notification.created", params.UserID, createdEvent); err != nil {
        log.Error("投递 notification.created 事件失败",
            "notification_id", notificationID, "user_id", params.UserID, "err", err)
    }

    return notificationID, nil
}
```

---

## 消费者实现

### Consumer: `relation.friend.request`

> 来源：Relation 服务。用户发送好友申请后触发。  
> 处理：为目标用户创建一条 FRIEND_REQUEST（好友请求）类型的通知。  
> 通知内容包含申请者信息和附加留言，过期时间 7 天。

```go
func (c *NotificationConsumer) HandleFriendRequest(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_relation.FriendRequestForNotification
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 FriendRequestForNotification 失败", "err", err)
        return nil // 反序列化失败不重试
    }

    // ==================== 2. 幂等检查 — Redis ====================
    // Key: notif:kafka:dedup:{event_id}  TTL: 24h
    dedupKey := fmt.Sprintf("notif:kafka:dedup:%s", event.Header.EventId)
    set, err := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result()
    if err != nil {
        return fmt.Errorf("Redis 幂等检查失败: %w", err)
    }
    if !set {
        log.Debug("重复事件，跳过", "event_id", event.Header.EventId)
        return nil
    }

    // ==================== 3. 参数校验 ====================
    if event.FromUserId == "" || event.ToUserId == "" {
        log.Error("relation.friend.request: 关键字段缺失",
            "from_user_id", event.FromUserId, "to_user_id", event.ToUserId)
        return nil
    }

    // ==================== 4. 获取申请者信息 — RPC User.GetUser ====================
    var fromNickname string
    senderResp, err := c.userClient.GetUser(ctx, &user_pb.GetUserRequest{UserId: event.FromUserId})
    if err != nil {
        log.Warn("获取好友申请者信息失败，使用默认值",
            "from_user_id", event.FromUserId, "err", err)
        fromNickname = event.FromUserId
    } else {
        fromNickname = senderResp.User.Nickname
    }

    // ==================== 5. 创建 FRIEND_REQUEST 通知 ====================
    expiresDuration := 7 * 24 * time.Hour // 好友请求通知 7 天过期
    notifID, err := c.createNotificationInternal(ctx, &createNotificationParams{
        UserID:   event.ToUserId,
        Type:     common.NOTIFICATION_TYPE_FRIEND_REQUEST, // = 1
        Title:    "新的好友请求",
        Content:  fmt.Sprintf("%s 请求添加你为好友", fromNickname),
        SenderID: event.FromUserId,
        Data: map[string]string{
            "request_id": event.RequestId,
            "message":    event.Message,  // 申请留言
            "source":     event.Source,    // 来源：search / group / qrcode / card
        },
        ExpiresIn: &expiresDuration,
    })
    if err != nil {
        log.Error("创建好友请求通知失败",
            "from_user_id", event.FromUserId, "to_user_id", event.ToUserId, "err", err)
        return fmt.Errorf("创建好友请求通知失败: %w", err)
    }

    log.Info("好友请求通知创建成功",
        "notification_id", notifID,
        "from_user_id", event.FromUserId,
        "to_user_id", event.ToUserId,
        "request_id", event.RequestId)
    return nil
}
```

### Consumer: `relation.friend.accepted`

> 来源：Relation 服务。好友申请被接受后触发。  
> 处理：为原申请者创建一条 FRIEND_ACCEPTED（好友已通过）类型的通知。

```go
func (c *NotificationConsumer) HandleFriendAccepted(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_relation.FriendAcceptedForNotification
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 FriendAcceptedForNotification 失败", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 — Redis ====================
    dedupKey := fmt.Sprintf("notif:kafka:dedup:%s", event.Header.EventId)
    set, err := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result()
    if err != nil {
        return fmt.Errorf("Redis 幂等检查失败: %w", err)
    }
    if !set {
        log.Debug("重复事件，跳过", "event_id", event.Header.EventId)
        return nil
    }

    // ==================== 3. 参数校验 ====================
    if event.FromUserId == "" || event.ToUserId == "" {
        log.Error("relation.friend.accepted: 关键字段缺失",
            "from_user_id", event.FromUserId, "to_user_id", event.ToUserId)
        return nil
    }

    // ==================== 4. 获取接受者信息 — RPC User.GetUser ====================
    var toNickname string
    toUserResp, err := c.userClient.GetUser(ctx, &user_pb.GetUserRequest{UserId: event.ToUserId})
    if err != nil {
        log.Warn("获取好友接受者信息失败，使用默认值",
            "to_user_id", event.ToUserId, "err", err)
        toNickname = event.ToUserId
    } else {
        toNickname = toUserResp.User.Nickname
    }

    // ==================== 5. 创建 FRIEND_ACCEPTED 通知（通知原申请者） ====================
    notifID, err := c.createNotificationInternal(ctx, &createNotificationParams{
        UserID:   event.FromUserId, // 通知原申请者：你的好友请求已被接受
        Type:     common.NOTIFICATION_TYPE_FRIEND_ACCEPTED, // = 2
        Title:    "好友请求已通过",
        Content:  fmt.Sprintf("%s 已同意你的好友请求", toNickname),
        SenderID: event.ToUserId,
        Data: map[string]string{
            "request_id": event.RequestId,
        },
    })
    if err != nil {
        log.Error("创建好友通过通知失败",
            "from_user_id", event.FromUserId, "to_user_id", event.ToUserId, "err", err)
        return fmt.Errorf("创建好友通过通知失败: %w", err)
    }

    log.Info("好友通过通知创建成功",
        "notification_id", notifID,
        "notified_user", event.FromUserId,
        "accepted_by", event.ToUserId)
    return nil
}
```

### Consumer: `group.application`

> 来源：Group 服务。用户提交入群申请后触发。  
> 处理：查询群的所有管理员（群主 + 管理员），为每位管理员创建一条 GROUP_APPLICATION（入群申请）通知。

```go
func (c *NotificationConsumer) HandleGroupApplication(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_group.GroupApplicationForNotification
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 GroupApplicationForNotification 失败", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 — Redis ====================
    dedupKey := fmt.Sprintf("notif:kafka:dedup:%s", event.Header.EventId)
    set, err := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result()
    if err != nil {
        return fmt.Errorf("Redis 幂等检查失败: %w", err)
    }
    if !set {
        log.Debug("重复事件，跳过", "event_id", event.Header.EventId)
        return nil
    }

    // ==================== 3. 参数校验 ====================
    if event.GroupId == "" || event.ApplicantId == "" {
        log.Error("group.application: 关键字段缺失",
            "group_id", event.GroupId, "applicant_id", event.ApplicantId)
        return nil
    }

    // ==================== 4. 获取申请者信息 — RPC User.GetUser ====================
    var applicantNickname string
    applicantResp, err := c.userClient.GetUser(ctx, &user_pb.GetUserRequest{UserId: event.ApplicantId})
    if err != nil {
        log.Warn("获取入群申请者信息失败，使用默认值",
            "applicant_id", event.ApplicantId, "err", err)
        applicantNickname = event.ApplicantId
    } else {
        applicantNickname = applicantResp.User.Nickname
    }

    // ==================== 5. 查询群管理员列表 — RPC Group.GetGroupAdmins ====================
    // 群主 + 管理员都需要收到通知
    adminIDs := make([]string, 0)

    // 方式一：如果事件中包含管理员 ID 列表则直接使用
    if len(event.AdminIds) > 0 {
        adminIDs = event.AdminIds
    } else {
        // 方式二：通过 RPC 查询群管理员
        adminsResp, err := c.groupClient.GetGroupMembers(ctx, &group_pb.GetGroupMembersRequest{
            GroupId:    event.GroupId,
            RoleFilter: []common.GroupMemberRole{common.GROUP_MEMBER_ROLE_OWNER, common.GROUP_MEMBER_ROLE_ADMIN},
        })
        if err != nil {
            log.Error("查询群管理员列表失败",
                "group_id", event.GroupId, "err", err)
            return fmt.Errorf("查询群管理员列表失败: %w", err)
        }
        for _, member := range adminsResp.Members {
            adminIDs = append(adminIDs, member.UserId)
        }
    }

    if len(adminIDs) == 0 {
        log.Warn("群没有管理员，无法通知", "group_id", event.GroupId)
        return nil
    }

    // ==================== 6. 为每位管理员创建 GROUP_APPLICATION 通知 ====================
    groupName := event.GroupName
    if groupName == "" {
        groupName = event.GroupId
    }

    expiresDuration := 7 * 24 * time.Hour // 入群申请通知 7 天过期
    successCount := 0
    for _, adminID := range adminIDs {
        notifID, err := c.createNotificationInternal(ctx, &createNotificationParams{
            UserID:   adminID,
            Type:     common.NOTIFICATION_TYPE_GROUP_INVITE, // = 3，复用群邀请类型表示群申请
            Title:    "入群申请",
            Content:  fmt.Sprintf("%s 申请加入群「%s」", applicantNickname, groupName),
            SenderID: event.ApplicantId,
            Data: map[string]string{
                "application_id": event.ApplicationId,
                "group_id":       event.GroupId,
                "group_name":     groupName,
                "message":        event.Message, // 申请理由
            },
            ExpiresIn: &expiresDuration,
        })
        if err != nil {
            log.Error("为管理员创建入群申请通知失败",
                "admin_id", adminID, "group_id", event.GroupId, "err", err)
            continue
        }
        successCount++
        log.Debug("入群申请通知已创建",
            "notification_id", notifID, "admin_id", adminID)
    }

    log.Info("入群申请通知创建完成",
        "group_id", event.GroupId,
        "applicant_id", event.ApplicantId,
        "total_admins", len(adminIDs),
        "success_count", successCount)
    return nil
}
```

### Consumer: `group.application.handled`

> 来源：Group 服务。群管理员处理入群申请后触发（通过或拒绝）。  
> 处理：为原申请者创建一条 GROUP_APPLICATION_RESULT 通知，告知审批结果。

```go
func (c *NotificationConsumer) HandleGroupApplicationHandled(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_group.GroupApplicationHandledForNotification
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 GroupApplicationHandledForNotification 失败", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 — Redis ====================
    dedupKey := fmt.Sprintf("notif:kafka:dedup:%s", event.Header.EventId)
    set, err := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result()
    if err != nil {
        return fmt.Errorf("Redis 幂等检查失败: %w", err)
    }
    if !set {
        log.Debug("重复事件，跳过", "event_id", event.Header.EventId)
        return nil
    }

    // ==================== 3. 参数校验 ====================
    if event.GroupId == "" || event.ApplicantId == "" || event.HandlerId == "" {
        log.Error("group.application.handled: 关键字段缺失",
            "group_id", event.GroupId, "applicant_id", event.ApplicantId,
            "handler_id", event.HandlerId)
        return nil
    }

    // ==================== 4. 构建通知内容 ====================
    groupName := event.GroupName
    if groupName == "" {
        groupName = event.GroupId
    }

    var title, content string
    var resultStr string

    switch event.Result {
    case "approved":
        title = "入群申请已通过"
        content = fmt.Sprintf("你的加入群「%s」的申请已通过", groupName)
        resultStr = "approved"
    case "rejected":
        title = "入群申请已拒绝"
        content = fmt.Sprintf("你的加入群「%s」的申请已被拒绝", groupName)
        if event.Reason != "" {
            content += fmt.Sprintf("，原因：%s", event.Reason)
        }
        resultStr = "rejected"
    default:
        log.Error("group.application.handled: 未知处理结果",
            "result", event.Result, "group_id", event.GroupId)
        return nil
    }

    // ==================== 5. 创建 GROUP_APPLICATION_RESULT 通知（通知申请者） ====================
    notifID, err := c.createNotificationInternal(ctx, &createNotificationParams{
        UserID:   event.ApplicantId,
        Type:     common.NOTIFICATION_TYPE_GROUP_KICKED, // = 5，复用枚举值表示申请结果
        Title:    title,
        Content:  content,
        SenderID: event.HandlerId,
        Data: map[string]string{
            "application_id": event.ApplicationId,
            "group_id":       event.GroupId,
            "group_name":     groupName,
            "result":         resultStr,
            "reason":         event.Reason,
        },
    })
    if err != nil {
        log.Error("创建入群申请结果通知失败",
            "applicant_id", event.ApplicantId, "group_id", event.GroupId, "err", err)
        return fmt.Errorf("创建入群申请结果通知失败: %w", err)
    }

    log.Info("入群申请结果通知创建成功",
        "notification_id", notifID,
        "applicant_id", event.ApplicantId,
        "group_id", event.GroupId,
        "result", resultStr)
    return nil
}
```

### Consumer: `group.member.kicked`

> 来源：Group 服务。群成员被管理员踢出后触发。  
> 处理：为被踢用户创建一条 SERVICE_NOTIFICATION 类型的通知。

```go
func (c *NotificationConsumer) HandleGroupMemberKicked(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_group.GroupMemberKickedForNotification
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 GroupMemberKickedForNotification 失败", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 — Redis ====================
    dedupKey := fmt.Sprintf("notif:kafka:dedup:%s", event.Header.EventId)
    set, err := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result()
    if err != nil {
        return fmt.Errorf("Redis 幂等检查失败: %w", err)
    }
    if !set {
        log.Debug("重复事件，跳过", "event_id", event.Header.EventId)
        return nil
    }

    // ==================== 3. 参数校验 ====================
    if event.GroupId == "" || event.KickedUserId == "" {
        log.Error("group.member.kicked: 关键字段缺失",
            "group_id", event.GroupId, "kicked_user_id", event.KickedUserId)
        return nil
    }

    // ==================== 4. 构建通知内容 ====================
    groupName := event.GroupName
    if groupName == "" {
        groupName = event.GroupId
    }

    // ==================== 5. 创建 SERVICE_NOTIFICATION 通知（通知被踢用户） ====================
    notifID, err := c.createNotificationInternal(ctx, &createNotificationParams{
        UserID:   event.KickedUserId,
        Type:     common.NOTIFICATION_TYPE_GROUP_KICKED, // = 5
        Title:    "已被移出群聊",
        Content:  fmt.Sprintf("你已被管理员移出群「%s」", groupName),
        SenderID: event.OperatorId, // 操作者（管理员/群主）
        Data: map[string]string{
            "group_id":    event.GroupId,
            "group_name":  groupName,
            "operator_id": event.OperatorId,
            "reason":      event.Reason,
        },
    })
    if err != nil {
        log.Error("创建被踢通知失败",
            "kicked_user_id", event.KickedUserId, "group_id", event.GroupId, "err", err)
        return fmt.Errorf("创建被踢通知失败: %w", err)
    }

    log.Info("被踢通知创建成功",
        "notification_id", notifID,
        "kicked_user_id", event.KickedUserId,
        "group_id", event.GroupId,
        "operator_id", event.OperatorId)
    return nil
}
```

### Consumer: `group.dissolved`

> 来源：Group 服务。群组解散后触发。  
> 处理：为群的所有成员（不含群主）创建 SERVICE_NOTIFICATION 类型的通知。  
> **注意**：群解散事件中应携带完整成员列表，否则需 RPC 回查（此时群可能已不存在）。

```go
func (c *NotificationConsumer) HandleGroupDissolved(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_group.GroupDissolvedForNotification
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 GroupDissolvedForNotification 失败", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 — Redis ====================
    dedupKey := fmt.Sprintf("notif:kafka:dedup:%s", event.Header.EventId)
    set, err := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result()
    if err != nil {
        return fmt.Errorf("Redis 幂等检查失败: %w", err)
    }
    if !set {
        log.Debug("重复事件，跳过", "event_id", event.Header.EventId)
        return nil
    }

    // ==================== 3. 参数校验 ====================
    if event.GroupId == "" || event.OwnerId == "" {
        log.Error("group.dissolved: 关键字段缺失",
            "group_id", event.GroupId, "owner_id", event.OwnerId)
        return nil
    }

    // ==================== 4. 获取需要通知的成员列表 ====================
    memberIDs := make([]string, 0)

    if len(event.MemberIds) > 0 {
        // 事件中携带了成员列表（推荐方式）
        for _, mid := range event.MemberIds {
            if mid != event.OwnerId { // 排除群主自己
                memberIDs = append(memberIDs, mid)
            }
        }
    } else {
        // 事件未携带成员列表 → 此时群已解散，无法通过 RPC 查询
        log.Warn("group.dissolved 事件未携带成员列表，无法通知成员",
            "group_id", event.GroupId)
        return nil
    }

    if len(memberIDs) == 0 {
        log.Info("群无其他成员，无需通知", "group_id", event.GroupId)
        return nil
    }

    // ==================== 5. 构建通知内容 ====================
    groupName := event.GroupName
    if groupName == "" {
        groupName = event.GroupId
    }

    // ==================== 6. 为每位成员创建 SERVICE_NOTIFICATION 通知 ====================
    successCount := 0
    batchSize := 100

    for i := 0; i < len(memberIDs); i += batchSize {
        end := i + batchSize
        if end > len(memberIDs) {
            end = len(memberIDs)
        }
        batch := memberIDs[i:end]

        for _, memberID := range batch {
            notifID, err := c.createNotificationInternal(ctx, &createNotificationParams{
                UserID:   memberID,
                Type:     common.NOTIFICATION_TYPE_GROUP_DISBANDED, // = 6
                Title:    "群聊已解散",
                Content:  fmt.Sprintf("群「%s」已被群主解散", groupName),
                SenderID: event.OwnerId,
                Data: map[string]string{
                    "group_id":   event.GroupId,
                    "group_name": groupName,
                    "owner_id":   event.OwnerId,
                },
            })
            if err != nil {
                log.Error("创建群解散通知失败",
                    "member_id", memberID, "group_id", event.GroupId, "err", err)
                continue
            }
            successCount++
            log.Debug("群解散通知已创建",
                "notification_id", notifID, "member_id", memberID)
        }

        // 避免批量创建给 DB/Redis 造成过大压力
        if end < len(memberIDs) {
            time.Sleep(50 * time.Millisecond)
        }
    }

    log.Info("群解散通知创建完成",
        "group_id", event.GroupId,
        "total_members", len(memberIDs),
        "success_count", successCount)
    return nil
}
```

### Consumer: `auth.login.success`

> 来源：Auth 服务。用户登录成功后触发（可选消费）。  
> 处理：如果检测到新设备登录，创建一条 ACCOUNT_SECURITY（账号安全）类型的通知。  
> 限频策略：同一用户 24h 内最多 1 条新设备登录通知，避免频繁登录导致通知轰炸。

```go
func (c *NotificationConsumer) HandleAuthLoginSuccess(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_auth.LoginSuccessForNotification
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 LoginSuccessForNotification 失败", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 — Redis ====================
    dedupKey := fmt.Sprintf("notif:kafka:dedup:%s", event.Header.EventId)
    set, err := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result()
    if err != nil {
        return fmt.Errorf("Redis 幂等检查失败: %w", err)
    }
    if !set {
        log.Debug("重复事件，跳过", "event_id", event.Header.EventId)
        return nil
    }

    // ==================== 3. 参数校验 ====================
    if event.UserId == "" {
        log.Error("auth.login.success: user_id is empty")
        return nil
    }

    // ==================== 4. 判断是否为新设备登录 ====================
    // 只有新设备登录才创建安全通知
    if !event.IsNewDevice {
        log.Debug("非新设备登录，跳过通知", "user_id", event.UserId, "device_id", event.DeviceId)
        return nil
    }

    // ==================== 5. 限频检查 — Redis ====================
    // Key: notif:login_notify:{user_id}  TTL: 24h
    // 同一用户 24h 内最多 1 条新设备登录通知
    loginNotifyKey := fmt.Sprintf("notif:login_notify:%s", event.UserId)
    notified, err := c.redis.SetNX(ctx, loginNotifyKey, "1", 24*time.Hour).Result()
    if err != nil {
        log.Warn("登录通知限频检查失败，继续创建", "user_id", event.UserId, "err", err)
    } else if !notified {
        log.Debug("用户 24h 内已收到新设备登录通知，跳过",
            "user_id", event.UserId, "device_id", event.DeviceId)
        return nil
    }

    // ==================== 6. 构建通知内容 ====================
    platformStr := event.Platform
    if platformStr == "" {
        platformStr = "未知设备"
    }
    ipStr := event.Ip
    if ipStr == "" {
        ipStr = "未知 IP"
    }

    loginTime := time.UnixMilli(event.LoginTime).Format("2006-01-02 15:04:05")
    content := fmt.Sprintf("你的账号于 %s 在新设备（%s）上登录，IP: %s。如非本人操作，请及时修改密码。",
        loginTime, platformStr, ipStr)

    // ==================== 7. 创建 ACCOUNT_SECURITY 通知 ====================
    notifID, err := c.createNotificationInternal(ctx, &createNotificationParams{
        UserID:   event.UserId,
        Type:     common.NOTIFICATION_TYPE_SECURITY_ALERT, // = 12（安全警告）
        Title:    "新设备登录提醒",
        Content:  content,
        SenderID: "", // 系统通知，无发送者
        Data: map[string]string{
            "device_id":  event.DeviceId,
            "platform":   platformStr,
            "ip":         ipStr,
            "login_time": fmt.Sprintf("%d", event.LoginTime),
            "location":   event.Location,
        },
    })
    if err != nil {
        log.Error("创建新设备登录安全通知失败",
            "user_id", event.UserId, "device_id", event.DeviceId, "err", err)
        // 清除限频标记，允许下次重试
        c.redis.Del(ctx, loginNotifyKey)
        return fmt.Errorf("创建新设备登录安全通知失败: %w", err)
    }

    log.Info("新设备登录安全通知创建成功",
        "notification_id", notifID,
        "user_id", event.UserId,
        "device_id", event.DeviceId,
        "platform", platformStr,
        "ip", ipStr)
    return nil
}
```

### Consumer: `user.deactivated`

> 来源：User 服务。用户注销账户后触发。  
> 处理：清理该用户的所有通知数据 — 软删除 PgSQL 记录 + 清除 Redis 缓存。  
> **注意**：使用分批处理避免大事务，每批处理 1000 条。

```go
func (c *NotificationConsumer) HandleUserDeactivated(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_user.UserDeactivatedEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 UserDeactivatedEvent 失败", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 — Redis ====================
    dedupKey := fmt.Sprintf("notif:kafka:dedup:%s", event.Header.EventId)
    set, err := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result()
    if err != nil {
        return fmt.Errorf("Redis 幂等检查失败: %w", err)
    }
    if !set {
        log.Debug("重复事件，跳过", "event_id", event.Header.EventId)
        return nil
    }

    if event.UserId == "" {
        log.Error("user.deactivated: user_id is empty")
        return nil
    }

    log.Info("开始清理注销用户的通知数据", "user_id", event.UserId)

    // ==================== 延迟双删 Phase 1 — PgSQL 写入前先失效缓存 ====================
    // 防止并发读在 PgSQL 写入期间回填旧缓存
    notifCacheKeys := []string{
        fmt.Sprintf("notif:unread:%s", event.UserId),
        fmt.Sprintf("notif:list:%s", event.UserId),
        fmt.Sprintf("notif:login_notify:%s", event.UserId),
    }
    for notifType := 1; notifType <= 12; notifType++ {
        notifCacheKeys = append(notifCacheKeys, fmt.Sprintf("notif:unread_type:%s:%d", event.UserId, notifType))
    }
    phase1Pipe := c.redis.Pipeline()
    for _, key := range notifCacheKeys {
        phase1Pipe.Del(ctx, key)
    }
    phase1Pipe.Exec(ctx)

    // ==================== 3. 分批软删除该用户的所有通知 — PgSQL ====================
    batchSize := 1000
    totalDeleted := int64(0)

    for {
        result, err := c.db.ExecContext(ctx,
            `UPDATE notifications
             SET is_deleted = true
             WHERE id IN (
                 SELECT id FROM notifications
                 WHERE user_id = $1 AND is_deleted = false
                 LIMIT $2
             )`,
            event.UserId, batchSize,
        )
        if err != nil {
            log.Error("分批软删除通知失败",
                "user_id", event.UserId, "err", err)
            return fmt.Errorf("分批软删除通知失败: %w", err)
        }

        affected, _ := result.RowsAffected()
        totalDeleted += affected

        if affected < int64(batchSize) {
            break // 最后一批，处理完毕
        }

        // 避免长时间占用资源，每批之间休眠
        time.Sleep(100 * time.Millisecond)
    }

    // ==================== 4. 同时清理该用户作为发送者的通知标记（可选） ====================
    // 如果该用户是通知的发送者（如好友请求），不删除通知，但清空 sender 信息
    _, err = c.db.ExecContext(ctx,
        `UPDATE notifications
         SET sender_id = '', data = jsonb_set(COALESCE(data, '{}')::jsonb, '{sender_deactivated}', '"true"')
         WHERE sender_id = $1 AND is_deleted = false`,
        event.UserId,
    )
    if err != nil {
        log.Warn("清理注销用户的发送者信息失败（不阻塞）",
            "user_id", event.UserId, "err", err)
    }

    // ==================== 5. 延迟双删 Phase 2 — DB 写入后再次清除缓存 ====================
    c.delayedDoubleDelete(ctx, notifCacheKeys...)

    log.Info("注销用户通知数据清理完成",
        "user_id", event.UserId,
        "total_deleted", totalDeleted)
    return nil
}
```

### Consumer: `config.changed`

> 来源：Config 服务。通知相关配置变更时热更新，无需重启服务。  
> 支持的配置项：通知过期时间、批量发送上限、新设备登录通知开关等。

```go
func (c *NotificationConsumer) HandleConfigChanged(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_config.ConfigChangedEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 ConfigChangedEvent 失败", "err", err)
        return nil
    }

    // ==================== 2. 筛选与 Notification 服务相关的配置 ====================
    // 只处理 notification.* 前缀的配置项
    if !strings.HasPrefix(event.ConfigKey, "notification.") {
        return nil // 不是 Notification 服务的配置，忽略
    }

    log.Info("通知服务配置变更",
        "config_key", event.ConfigKey,
        "old_value", event.OldValue,
        "new_value", event.NewValue)

    // ==================== 3. 根据配置项更新内存配置 ====================
    switch event.ConfigKey {
    case "notification.friend_request_expire_days":
        // 好友请求通知过期天数
        val, err := strconv.Atoi(event.NewValue)
        if err != nil || val <= 0 {
            log.Error("无效的 notification.friend_request_expire_days 值", "value", event.NewValue)
            return nil
        }
        c.config.Store("FriendRequestExpireDays", val)
        log.Info("已更新 FriendRequestExpireDays", "old", event.OldValue, "new", val)

    case "notification.system_announcement_expire_days":
        // 系统公告过期天数
        val, err := strconv.Atoi(event.NewValue)
        if err != nil || val <= 0 {
            log.Error("无效的 notification.system_announcement_expire_days 值", "value", event.NewValue)
            return nil
        }
        c.config.Store("SystemAnnouncementExpireDays", val)
        log.Info("已更新 SystemAnnouncementExpireDays", "old", event.OldValue, "new", val)

    case "notification.batch_send_max_users":
        // 批量发送最大用户数
        val, err := strconv.Atoi(event.NewValue)
        if err != nil || val <= 0 {
            log.Error("无效的 notification.batch_send_max_users 值", "value", event.NewValue)
            return nil
        }
        c.config.Store("BatchSendMaxUsers", val)
        log.Info("已更新 BatchSendMaxUsers", "old", event.OldValue, "new", val)

    case "notification.new_device_login_notify_enabled":
        // 新设备登录通知开关
        val := strings.ToLower(event.NewValue) == "true"
        c.config.Store("NewDeviceLoginNotifyEnabled", val)
        log.Info("已更新 NewDeviceLoginNotifyEnabled", "old", event.OldValue, "new", val)

    case "notification.login_notify_cooldown_hours":
        // 登录通知限频时间（小时）
        val, err := strconv.Atoi(event.NewValue)
        if err != nil || val <= 0 {
            log.Error("无效的 notification.login_notify_cooldown_hours 值", "value", event.NewValue)
            return nil
        }
        c.config.Store("LoginNotifyCooldownHours", val)
        log.Info("已更新 LoginNotifyCooldownHours", "old", event.OldValue, "new", val)

    case "notification.max_content_length":
        // 通知内容最大长度
        val, err := strconv.Atoi(event.NewValue)
        if err != nil || val <= 0 {
            log.Error("无效的 notification.max_content_length 值", "value", event.NewValue)
            return nil
        }
        c.config.Store("MaxContentLength", val)
        log.Info("已更新 MaxContentLength", "old", event.OldValue, "new", val)

    case "notification.recent_list_max_size":
        // 最近通知列表 ZSET 最大数量
        val, err := strconv.Atoi(event.NewValue)
        if err != nil || val <= 0 {
            log.Error("无效的 notification.recent_list_max_size 值", "value", event.NewValue)
            return nil
        }
        c.config.Store("RecentListMaxSize", val)
        log.Info("已更新 RecentListMaxSize", "old", event.OldValue, "new", val)

    default:
        log.Debug("未知的通知配置项，已忽略", "config_key", event.ConfigKey)
    }

    return nil
}
```

### Consumer: `relation.friend.rejected`

> 来源：Relation 服务。好友申请被拒绝时触发。  
> 处理：为原申请者创建一条 FRIEND_REQUEST 类型的通知，告知申请被拒绝。

```go
func (c *NotificationConsumer) HandleFriendRejected(ctx context.Context, msg *kafka.Message) error {
    var event kafka_notification.FriendRejectedForNotification
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 FriendRejectedForNotification 失败", "err", err)
        return nil
    }

    dedupKey := fmt.Sprintf("notif:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    if event.FromUserId == "" || event.ToUserId == "" {
        return nil
    }

    expireDuration := 7 * 24 * time.Hour
    _, err := c.createNotificationInternal(ctx, &createNotificationParams{
        UserID:    event.FromUserId, // 通知原申请者
        Type:      common.NOTIFICATION_TYPE_FRIEND_REQUEST,
        Title:     "好友申请被拒绝",
        Content:   "你的好友申请已被对方拒绝",
        SenderID:  event.ToUserId,
        Data:      map[string]string{"request_id": event.RequestId, "action": "rejected", "reason": event.Reason},
        ExpiresIn: &expireDuration,
    })
    if err != nil {
        return fmt.Errorf("创建好友拒绝通知失败: %w", err)
    }
    return nil
}
```

### Consumer: `relation.friend.deleted`

> 来源：Relation 服务。用户删除好友时触发。  
> 处理：为被删除者创建一条 SERVICE_NOTIFICATION 类型的通知。

```go
func (c *NotificationConsumer) HandleFriendDeleted(ctx context.Context, msg *kafka.Message) error {
    var event kafka_notification.FriendDeletedForNotification
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 FriendDeletedForNotification 失败", "err", err)
        return nil
    }

    dedupKey := fmt.Sprintf("notif:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    if event.OperatorId == "" || event.TargetId == "" {
        return nil
    }

    _, err := c.createNotificationInternal(ctx, &createNotificationParams{
        UserID:   event.TargetId, // 通知被删除者
        Type:     common.NOTIFICATION_TYPE_SERVICE,
        Title:    "好友关系变更",
        Content:  "你已被对方从好友列表中移除",
        SenderID: event.OperatorId,
        Data:     map[string]string{"action": "friend_deleted"},
    })
    if err != nil {
        return fmt.Errorf("创建好友删除通知失败: %w", err)
    }
    return nil
}
```

### Consumer: `group.member.joined`

> 来源：Group 服务。新成员加入群时触发。  
> 处理：为群管理员创建 SERVICE_NOTIFICATION，告知有新成员加入。

```go
func (c *NotificationConsumer) HandleGroupMemberJoined(ctx context.Context, msg *kafka.Message) error {
    var event kafka_group.GroupMemberJoinedForNotification
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 GroupMemberJoinedForNotification 失败", "err", err)
        return nil
    }

    dedupKey := fmt.Sprintf("notif:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    // 查询群管理员列表（通过 Group RPC）
    adminsResp, err := c.groupClient.GetGroupAdmins(ctx, &group_pb.GetGroupAdminsRequest{GroupId: event.GroupId})
    if err != nil {
        return fmt.Errorf("查询群管理员失败: %w", err)
    }

    content := fmt.Sprintf("%s 加入了群聊", event.Nickname)
    for _, adminID := range adminsResp.AdminIds {
        if adminID == event.UserId {
            continue // 不通知自己
        }
        c.createNotificationInternal(ctx, &createNotificationParams{
            UserID:   adminID,
            Type:     common.NOTIFICATION_TYPE_SERVICE,
            Title:    "群成员变更",
            Content:  content,
            SenderID: event.UserId,
            Data: map[string]string{
                "group_id":     event.GroupId,
                "group_name":   event.GroupName,
                "action":       "member_joined",
                "member_count": fmt.Sprintf("%d", event.MemberCount),
            },
        })
    }
    return nil
}
```

### Consumer: `group.member.left`

> 来源：Group 服务。成员主动退出群时触发。  
> 处理：通知群管理员有成员退出。

```go
func (c *NotificationConsumer) HandleGroupMemberLeft(ctx context.Context, msg *kafka.Message) error {
    var event kafka_notification.GroupMemberLeftForNotification
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 GroupMemberLeftForNotification 失败", "err", err)
        return nil
    }

    dedupKey := fmt.Sprintf("notif:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    // 查询群管理员列表
    adminsResp, err := c.groupClient.GetGroupAdmins(ctx, &group_pb.GetGroupAdminsRequest{GroupId: event.GroupId})
    if err != nil {
        return fmt.Errorf("查询群管理员失败: %w", err)
    }

    content := fmt.Sprintf("%s 退出了群聊「%s」", event.Nickname, event.GroupName)
    for _, adminID := range adminsResp.AdminIds {
        c.createNotificationInternal(ctx, &createNotificationParams{
            UserID:   adminID,
            Type:     common.NOTIFICATION_TYPE_SERVICE,
            Title:    "群成员退出",
            Content:  content,
            SenderID: event.UserId,
            Data: map[string]string{
                "group_id":     event.GroupId,
                "group_name":   event.GroupName,
                "action":       "member_left",
                "member_count": fmt.Sprintf("%d", event.MemberCount),
            },
        })
    }
    return nil
}
```

### Consumer: `group.member.role.changed`

> 来源：Group 服务。群成员角色变更时触发。  
> 处理：通知被变更角色的成员。

```go
func (c *NotificationConsumer) HandleGroupMemberRoleChanged(ctx context.Context, msg *kafka.Message) error {
    var event kafka_notification.GroupMemberRoleChangedForNotification
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 GroupMemberRoleChangedForNotification 失败", "err", err)
        return nil
    }

    dedupKey := fmt.Sprintf("notif:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    content := fmt.Sprintf("你在群中的角色已变更为 %s", event.NewRole)
    _, err := c.createNotificationInternal(ctx, &createNotificationParams{
        UserID:   event.UserId, // 通知被变更成员
        Type:     common.NOTIFICATION_TYPE_SERVICE,
        Title:    "群角色变更",
        Content:  content,
        SenderID: event.OperatorId,
        Data: map[string]string{
            "group_id": event.GroupId,
            "old_role": event.OldRole,
            "new_role": event.NewRole,
            "action":   "role_changed",
        },
    })
    if err != nil {
        return fmt.Errorf("创建角色变更通知失败: %w", err)
    }
    return nil
}
```

### Consumer: `group.member.mute.changed`

> 来源：Group 服务。群成员禁言/解禁时触发。  
> 处理：通知被禁言/解禁的成员。

```go
func (c *NotificationConsumer) HandleGroupMemberMuteChanged(ctx context.Context, msg *kafka.Message) error {
    var event kafka_notification.GroupMemberMuteChangedForNotification
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 GroupMemberMuteChangedForNotification 失败", "err", err)
        return nil
    }

    dedupKey := fmt.Sprintf("notif:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    var title, content string
    if event.IsMuted {
        title = "群禁言通知"
        muteUntil := time.UnixMilli(event.MuteUntil).Format("2006-01-02 15:04")
        content = fmt.Sprintf("你已被管理员禁言，解除时间: %s", muteUntil)
    } else {
        title = "群解除禁言"
        content = "你的禁言已被管理员解除"
    }

    _, err := c.createNotificationInternal(ctx, &createNotificationParams{
        UserID:   event.UserId,
        Type:     common.NOTIFICATION_TYPE_SERVICE,
        Title:    title,
        Content:  content,
        SenderID: event.OperatorId,
        Data: map[string]string{
            "group_id":   event.GroupId,
            "is_muted":   fmt.Sprintf("%t", event.IsMuted),
            "mute_until": fmt.Sprintf("%d", event.MuteUntil),
            "action":     "mute_changed",
        },
    })
    if err != nil {
        return fmt.Errorf("创建禁言变更通知失败: %w", err)
    }
    return nil
}
```

### Consumer: `group.transfer`

> 来源：Group 服务。群主转让群时触发。  
> 处理：通知新群主。

```go
func (c *NotificationConsumer) HandleGroupTransfer(ctx context.Context, msg *kafka.Message) error {
    var event kafka_notification.GroupTransferForNotification
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 GroupTransferForNotification 失败", "err", err)
        return nil
    }

    dedupKey := fmt.Sprintf("notif:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    _, err := c.createNotificationInternal(ctx, &createNotificationParams{
        UserID:   event.NewOwnerId, // 通知新群主
        Type:     common.NOTIFICATION_TYPE_SERVICE,
        Title:    "群主权限转让",
        Content:  "你已成为群主",
        SenderID: event.OldOwnerId,
        Data: map[string]string{
            "group_id":     event.GroupId,
            "old_owner_id": event.OldOwnerId,
            "action":       "group_transfer",
        },
    })
    if err != nil {
        return fmt.Errorf("创建群主转让通知失败: %w", err)
    }
    return nil
}
```

### Consumer: `user.banned`

> 来源：User 服务。用户账号被封禁时触发。  
> 处理：为被封禁用户创建 SERVICE_NOTIFICATION 通知。  
> 注意：即使用户已被封禁无法立即查看，但通知会在解封后展示。

```go
func (c *NotificationConsumer) HandleUserBanned(ctx context.Context, msg *kafka.Message) error {
    var event kafka_user.UserBannedForNotification
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 UserBannedForNotification 失败", "err", err)
        return nil
    }

    dedupKey := fmt.Sprintf("notif:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    content := fmt.Sprintf("你的账号已被封禁，原因: %s。如有异议请联系客服。", event.Reason)
    _, err := c.createNotificationInternal(ctx, &createNotificationParams{
        UserID:   event.UserId,
        Type:     common.NOTIFICATION_TYPE_SERVICE,
        Title:    "账号封禁通知",
        Content:  content,
        SenderID: "",
        Data: map[string]string{
            "action":     "user_banned",
            "reason":     event.Reason,
            "ban_until":  fmt.Sprintf("%d", event.BanUntil),
        },
    })
    if err != nil {
        return fmt.Errorf("创建封禁通知失败: %w", err)
    }
    return nil
}
```

### Consumer: `auth.password.changed`

> 来源：Auth 服务。用户修改或重置密码后触发。  
> 处理：为用户创建 ACCOUNT_SECURITY 类型安全通知。

```go
func (c *NotificationConsumer) HandlePasswordChanged(ctx context.Context, msg *kafka.Message) error {
    var event kafka_notification.PasswordChangedForNotification
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 PasswordChangedForNotification 失败", "err", err)
        return nil
    }

    dedupKey := fmt.Sprintf("notif:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    changeTime := time.UnixMilli(event.ChangeTime).Format("2006-01-02 15:04:05")
    var action string
    if event.ChangeType == "reset" {
        action = "重置"
    } else {
        action = "修改"
    }
    content := fmt.Sprintf("你的密码已于 %s 被%s，IP: %s。如非本人操作，请立即联系客服。",
        changeTime, action, event.Ip)

    _, err := c.createNotificationInternal(ctx, &createNotificationParams{
        UserID:   event.UserId,
        Type:     common.NOTIFICATION_TYPE_SECURITY_ALERT,
        Title:    "密码变更提醒",
        Content:  content,
        SenderID: "",
        Data: map[string]string{
            "action":      "password_changed",
            "change_type": event.ChangeType,
            "platform":    event.Platform,
            "ip":          event.Ip,
        },
    })
    if err != nil {
        return fmt.Errorf("创建密码变更安全通知失败: %w", err)
    }
    return nil
}
```

### Consumer: `audit.risk.alert`

> 来源：Audit 服务。风控系统检测到异常时触发。  
> 处理：为系统管理员创建风控告警通知。

```go
func (c *NotificationConsumer) HandleRiskAlert(ctx context.Context, msg *kafka.Message) error {
    var event kafka_notification.RiskAlertForNotification
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 RiskAlertForNotification 失败", "err", err)
        return nil
    }

    dedupKey := fmt.Sprintf("notif:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    // 查询管理员用户列表（通过配置获取）
    adminIDsRaw, _ := c.config.Load("AdminUserIDs")
    adminIDs, ok := adminIDsRaw.([]string)
    if !ok || len(adminIDs) == 0 {
        log.Warn("未配置管理员用户 ID，无法发送风控告警通知", "alert_id", event.AlertId)
        return nil
    }

    content := fmt.Sprintf("[%s] %s - %s", event.RiskLevel, event.RiskType, event.Description)
    data := map[string]string{
        "alert_id":   event.AlertId,
        "risk_type":  event.RiskType,
        "risk_level": event.RiskLevel,
        "user_id":    event.UserId,
        "action":     event.Action,
    }

    for _, adminID := range adminIDs {
        c.createNotificationInternal(ctx, &createNotificationParams{
            UserID:   adminID,
            Type:     common.NOTIFICATION_TYPE_SECURITY_ALERT,
            Title:    "风控告警",
            Content:  content,
            SenderID: "",
            Data:     data,
        })
    }
    return nil
}
```

### Consumer: `session.expired`

> 来源：Session 服务。用户会话过期时触发。  
> 处理：为用户创建安全通知，告知其在某设备上的会话已过期。

```go
func (c *NotificationConsumer) HandleSessionExpired(ctx context.Context, msg *kafka.Message) error {
    var event kafka_notification.SessionExpiredForNotification
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 SessionExpiredForNotification 失败", "err", err)
        return nil
    }

    dedupKey := fmt.Sprintf("notif:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    content := fmt.Sprintf("你在 %s 上的登录已过期，请重新登录。", event.Platform)
    _, err := c.createNotificationInternal(ctx, &createNotificationParams{
        UserID:   event.UserId,
        Type:     common.NOTIFICATION_TYPE_SECURITY_ALERT,
        Title:    "登录过期提醒",
        Content:  content,
        SenderID: "",
        Data: map[string]string{
            "session_id": event.SessionId,
            "platform":   event.Platform,
            "device_id":  event.DeviceId,
            "action":     "session_expired",
        },
    })
    if err != nil {
        return fmt.Errorf("创建会话过期通知失败: %w", err)
    }
    return nil
}
```

### Consumer: `offline.queue.overflow`

> 来源：OfflineQueue 服务。用户离线队列溢出时触发。  
> 处理：为用户创建 SERVICE_NOTIFICATION，提示有消息可能丢失。

```go
func (c *NotificationConsumer) HandleOfflineQueueOverflow(ctx context.Context, msg *kafka.Message) error {
    var event kafka_notification.OfflineQueueOverflowForNotification
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 OfflineQueueOverflowForNotification 失败", "err", err)
        return nil
    }

    dedupKey := fmt.Sprintf("notif:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    content := fmt.Sprintf("你的离线消息队列已满，有 %d 条消息可能未被保存。请尽快上线查看。",
        event.DroppedCount)
    _, err := c.createNotificationInternal(ctx, &createNotificationParams{
        UserID:   event.UserId,
        Type:     common.NOTIFICATION_TYPE_SERVICE,
        Title:    "离线消息溢出提醒",
        Content:  content,
        SenderID: "",
        Data: map[string]string{
            "queue_size":    fmt.Sprintf("%d", event.QueueSize),
            "max_size":      fmt.Sprintf("%d", event.MaxSize),
            "dropped_count": fmt.Sprintf("%d", event.DroppedCount),
            "action":        "offline_queue_overflow",
        },
    })
    if err != nil {
        return fmt.Errorf("创建离线队列溢出通知失败: %w", err)
    }
    return nil
}
```

### Consumer: `system.broadcast`

> 来源：Config 服务。系统广播事件。  
> 处理：为每个在线/活跃用户创建 SYSTEM_ANNOUNCEMENT 类型的通知，  
> 这样用户即使当前不在线，之后打开通知列表也能看到系统广播记录。  
> 注意：Push 服务负责实时推送，此处负责持久化通知记录。

```go
func (c *NotificationConsumer) HandleSystemBroadcast(ctx context.Context, msg *kafka.Message) error {
    var event kafka_config.SystemBroadcastForNotification
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 SystemBroadcastForNotification 失败", "err", err)
        return nil
    }

    dedupKey := fmt.Sprintf("notif:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    // 系统广播使用全局通知表记录，而非逐用户创建
    // 用户拉取通知列表时会自动合并全局广播通知
    var expiresAt *time.Time
    if event.ExpireTime > 0 {
        exp := time.UnixMilli(event.ExpireTime)
        expiresAt = &exp
    }

    _, err := c.db.ExecContext(ctx,
        `INSERT INTO system_broadcasts (broadcast_id, title, content, data_type, payload, created_at, expires_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7)
         ON CONFLICT (broadcast_id) DO NOTHING`,
        event.BroadcastId, event.Title, event.Content, event.DataType,
        event.Payload, time.Now(), expiresAt,
    )
    if err != nil {
        return fmt.Errorf("写入系统广播记录失败: %w", err)
    }

    log.Info("系统广播通知记录已保存", "broadcast_id", event.BroadcastId, "title", event.Title)
    return nil
}
```

```go
// NotificationConsumer 通知服务 Kafka 消费者
type NotificationConsumer struct {
    db          *sql.DB
    redis       *redis.Client
    kafka       *kafka.Producer
    snowflake   *snowflake.Node
    config      *sync.Map // 热更新配置
    userClient  user_pb.UserServiceClient
    groupClient group_pb.GroupServiceClient
    instanceID  string
}

// NewNotificationConsumer 创建通知消费者实例
func NewNotificationConsumer(
    db *sql.DB,
    redis *redis.Client,
    kafkaProducer *kafka.Producer,
    userClient user_pb.UserServiceClient,
    groupClient group_pb.GroupServiceClient,
) *NotificationConsumer {
    node, _ := snowflake.NewNode(1)
    c := &NotificationConsumer{
        db:          db,
        redis:       redis,
        kafka:       kafkaProducer,
        snowflake:   node,
        config:      &sync.Map{},
        userClient:  userClient,
        groupClient: groupClient,
        instanceID:  os.Getenv("INSTANCE_ID"),
    }

    // 加载默认配置
    c.config.Store("FriendRequestExpireDays", 7)
    c.config.Store("SystemAnnouncementExpireDays", 30)
    c.config.Store("BatchSendMaxUsers", 10000)
    c.config.Store("NewDeviceLoginNotifyEnabled", true)
    c.config.Store("LoginNotifyCooldownHours", 24)
    c.config.Store("MaxContentLength", 5000)
    c.config.Store("RecentListMaxSize", 100)

    return c
}

// RegisterConsumers 注册所有消费者到 Kafka Consumer Group
func (c *NotificationConsumer) RegisterConsumers(router *kafka.ConsumerRouter) {
    // 好友申请 → 创建好友请求通知
    router.Handle("relation.friend.request", c.HandleFriendRequest,
        kafka.WithRetry(3),
        kafka.WithRetryBackoff(time.Second),
        kafka.WithConcurrency(4),
    )

    // 好友通过 → 创建好友通过通知
    router.Handle("relation.friend.accepted", c.HandleFriendAccepted,
        kafka.WithRetry(3),
        kafka.WithRetryBackoff(time.Second),
        kafka.WithConcurrency(4),
    )

    // 入群申请 → 通知群管理员
    router.Handle("group.application", c.HandleGroupApplication,
        kafka.WithRetry(3),
        kafka.WithRetryBackoff(2*time.Second),
        kafka.WithConcurrency(4),
    )

    // 入群申请处理结果 → 通知申请者
    router.Handle("group.application.handled", c.HandleGroupApplicationHandled,
        kafka.WithRetry(3),
        kafka.WithRetryBackoff(2*time.Second),
        kafka.WithConcurrency(4),
    )

    // 成员被踢 → 通知被踢用户
    router.Handle("group.member.kicked", c.HandleGroupMemberKicked,
        kafka.WithRetry(3),
        kafka.WithRetryBackoff(time.Second),
        kafka.WithConcurrency(2),
    )

    // 群解散 → 通知所有成员
    router.Handle("group.dissolved", c.HandleGroupDissolved,
        kafka.WithRetry(3),
        kafka.WithRetryBackoff(2*time.Second),
        kafka.WithConcurrency(2),
    )

    // 新设备登录 → 安全通知（可选）
    router.Handle("auth.login.success", c.HandleAuthLoginSuccess,
        kafka.WithRetry(2),
        kafka.WithRetryBackoff(time.Second),
        kafka.WithConcurrency(2),
    )

    // 用户注销 → 清理通知数据
    router.Handle("user.deactivated", c.HandleUserDeactivated,
        kafka.WithRetry(5),
        kafka.WithRetryBackoff(5*time.Second),
        kafka.WithConcurrency(1), // 单线程处理，避免并发冲突
    )

    // 配置变更 → 热更新
    router.Handle("config.changed", c.HandleConfigChanged,
        kafka.WithRetry(1),
        kafka.WithConcurrency(1),
    )

    // 好友拒绝 → 通知申请者
    router.Handle("relation.friend.rejected", c.HandleFriendRejected,
        kafka.WithRetry(3),
        kafka.WithRetryBackoff(time.Second),
        kafka.WithConcurrency(4),
    )

    // 好友删除 → 通知被删除者
    router.Handle("relation.friend.deleted", c.HandleFriendDeleted,
        kafka.WithRetry(3),
        kafka.WithRetryBackoff(time.Second),
        kafka.WithConcurrency(4),
    )

    // 群成员加入 → 通知群管理员
    router.Handle("group.member.joined", c.HandleGroupMemberJoined,
        kafka.WithRetry(3),
        kafka.WithRetryBackoff(2*time.Second),
        kafka.WithConcurrency(4),
    )

    // 群成员退出 → 通知群管理员
    router.Handle("group.member.left", c.HandleGroupMemberLeft,
        kafka.WithRetry(3),
        kafka.WithRetryBackoff(2*time.Second),
        kafka.WithConcurrency(2),
    )

    // 群成员角色变更 → 通知被变更成员
    router.Handle("group.member.role.changed", c.HandleGroupMemberRoleChanged,
        kafka.WithRetry(3),
        kafka.WithRetryBackoff(time.Second),
        kafka.WithConcurrency(2),
    )

    // 群成员禁言变更 → 通知被禁言/解禁成员
    router.Handle("group.member.mute.changed", c.HandleGroupMemberMuteChanged,
        kafka.WithRetry(3),
        kafka.WithRetryBackoff(time.Second),
        kafka.WithConcurrency(2),
    )

    // 群主转让 → 通知新群主
    router.Handle("group.transfer", c.HandleGroupTransfer,
        kafka.WithRetry(3),
        kafka.WithRetryBackoff(time.Second),
        kafka.WithConcurrency(2),
    )

    // 账号封禁 → 通知被封禁用户
    router.Handle("user.banned", c.HandleUserBanned,
        kafka.WithRetry(3),
        kafka.WithRetryBackoff(time.Second),
        kafka.WithConcurrency(2),
    )

    // 密码变更 → 安全通知
    router.Handle("auth.password.changed", c.HandlePasswordChanged,
        kafka.WithRetry(3),
        kafka.WithRetryBackoff(time.Second),
        kafka.WithConcurrency(2),
    )

    // 风控告警 → 通知管理员
    router.Handle("audit.risk.alert", c.HandleRiskAlert,
        kafka.WithRetry(3),
        kafka.WithRetryBackoff(2*time.Second),
        kafka.WithConcurrency(2),
    )

    // 会话过期 → 安全通知
    router.Handle("session.expired", c.HandleSessionExpired,
        kafka.WithRetry(2),
        kafka.WithRetryBackoff(time.Second),
        kafka.WithConcurrency(2),
    )

    // 离线队列溢出 → 通知用户
    router.Handle("offline.queue.overflow", c.HandleOfflineQueueOverflow,
        kafka.WithRetry(2),
        kafka.WithRetryBackoff(time.Second),
        kafka.WithConcurrency(2),
    )

    // 系统广播 → 持久化广播记录
    router.Handle("system.broadcast", c.HandleSystemBroadcast,
        kafka.WithRetry(3),
        kafka.WithRetryBackoff(2*time.Second),
        kafka.WithConcurrency(2),
    )
}

// delayedDoubleDelete 延迟双删：保证缓存与 DB 的最终一致性
// Phase 1: 立即删除缓存（防止后续读到旧值）
// Phase 2: 500ms 后再次删除（防止并发读在 DB 写入窗口期回填旧缓存）
func (c *NotificationConsumer) delayedDoubleDelete(ctx context.Context, keys ...string) {
    if len(keys) == 0 {
        return
    }
    pipe := c.redis.Pipeline()
    for _, key := range keys {
        pipe.Del(ctx, key)
    }
    pipe.Exec(ctx)
    go func() {
        time.Sleep(500 * time.Millisecond)
        pipe := c.redis.Pipeline()
        for _, key := range keys {
            pipe.Del(context.Background(), key)
        }
        pipe.Exec(context.Background())
    }()
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
```
