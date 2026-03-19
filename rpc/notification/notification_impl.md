# Notification 系统通知服务 — RPC 接口实现伪代码

## 概述

Notification 服务管理**系统通知**（非聊天消息），包括好友请求通知、群组邀请/申请/踢出/解散通知、系统公告、账号安全提醒、服务通知等。
通知与聊天消息的核心区别：通知是**单向、一次性、非会话式**的，不走 Message/Conversation 流程，有自己独立的存储和未读计数。

**核心设计原则：**
- **通知 ID 全局唯一**：Snowflake 算法生成，时间有序
- **按类型分类管理**：每种 NotificationType 有独立的未读计数和查询过滤
- **未读计数双缓存**：Redis 缓存总未读数 + 按类型未读数，写操作同步更新
- **最近通知列表 ZSET**：Redis 缓存最近 100 条通知 ID，按创建时间排序，避免频繁查库
- **软删除**：删除操作不物理删除，通过 `is_deleted` 标记，支持审计回溯
- **过期清理**：通知支持 `expires_at` 过期时间，过期通知不展示但不自动删除（定时任务清理）

## 外部依赖

| 依赖类型 | 依赖项 | 用途 |
|---------|--------|------|
| PgSQL | notifications 表 | 通知持久化存储 |
| Redis | 未读计数缓存 / 按类型未读计数缓存 / 最近通知列表 | 高频读写 |
| RPC | User.GetUser | 获取通知发送者信息（补充头像、昵称等） |
| Kafka | notification.created / notification.read / notification.read.all / notification.deleted | 事件通知下游（Push 服务投递 / 角标更新） |

## PgSQL 表结构

```sql
-- 系统通知表
CREATE TABLE notifications (
    id              BIGSERIAL    PRIMARY KEY,
    notification_id VARCHAR(20)  NOT NULL UNIQUE,              -- Snowflake 全局唯一通知 ID
    user_id         VARCHAR(20)  NOT NULL,                     -- 接收者用户 ID
    type            INT          NOT NULL,                     -- 通知类型枚举（见下方）
    title           VARCHAR(200) NOT NULL DEFAULT '',          -- 通知标题
    content         TEXT         NOT NULL DEFAULT '',          -- 通知正文
    data            JSONB        NOT NULL DEFAULT '{}',        -- 扩展数据（业务自定义 JSON）
    sender_id       VARCHAR(20)  NOT NULL DEFAULT '',          -- 发送者/触发者用户 ID（系统通知为空）
    is_read         BOOLEAN      NOT NULL DEFAULT false,       -- 是否已读
    is_deleted      BOOLEAN      NOT NULL DEFAULT false,       -- 是否已删除（软删除）
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),       -- 创建时间
    expires_at      TIMESTAMPTZ                                -- 过期时间（NULL 表示不过期）
);

-- 核心查询索引：按用户获取未删除通知列表（按时间倒序）
CREATE INDEX idx_notifications_user_list ON notifications(user_id, is_deleted, created_at DESC);

-- 未读通知查询索引（部分索引，仅未删除的通知参与）
CREATE INDEX idx_notifications_user_unread ON notifications(user_id, is_read, is_deleted)
    WHERE is_deleted = false;

-- 按类型+用户查询（类型筛选场景）
CREATE INDEX idx_notifications_user_type ON notifications(user_id, type, is_deleted, created_at DESC);

-- 通知 ID 快速定位（已有 UNIQUE 约束自动创建索引）

-- 系统广播表（全局广播通知，非逐用户存储，用户拉取时合并展示）
CREATE TABLE system_broadcasts (
    id            BIGSERIAL    PRIMARY KEY,
    broadcast_id  VARCHAR(64)  NOT NULL UNIQUE,              -- 广播唯一 ID
    title         VARCHAR(200) NOT NULL DEFAULT '',          -- 广播标题
    content       TEXT         NOT NULL DEFAULT '',          -- 广播正文
    data_type     VARCHAR(32)  NOT NULL DEFAULT 'text',      -- 内容类型（text/html/markdown）
    payload       JSONB        NOT NULL DEFAULT '{}',        -- 扩展数据
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),       -- 创建时间
    expires_at    TIMESTAMPTZ                                -- 过期时间（NULL 表示不过期）
);
CREATE INDEX idx_system_broadcasts_time ON system_broadcasts(created_at DESC);
CREATE INDEX idx_system_broadcasts_active ON system_broadcasts(created_at DESC)
    WHERE expires_at IS NULL OR expires_at > NOW();
```

## 通知类型枚举

| 值 | 名称 | 说明 |
|----|------|------|
| 1 | FRIEND_REQUEST | 收到好友请求 |
| 2 | FRIEND_ACCEPTED | 好友请求已通过 |
| 3 | GROUP_INVITATION | 收到群组邀请 |
| 4 | GROUP_APPLICATION | 收到入群申请（群管理员收到） |
| 5 | GROUP_APPLICATION_RESULT | 入群申请结果（申请者收到） |
| 6 | SYSTEM_ANNOUNCEMENT | 系统公告（运营发出的全局通知） |
| 7 | ACCOUNT_SECURITY | 账号安全提醒（新设备登录、密码修改等） |
| 8 | SERVICE_NOTIFICATION | 业务服务通知（群解散、被踢出群等） |

## Redis Key 设计

| Key 格式 | 类型 | 值 | TTL | 说明 |
|----------|------|-----|-----|------|
| `notif:unread:{user_id}` | STRING | 总未读通知数 | 24h | 用户未读通知总数缓存 |
| `notif:unread_type:{user_id}:{type}` | STRING | 该类型未读通知数 | 24h | 按通知类型的未读数缓存 |
| `notif:list:{user_id}` | ZSET | member=notification_id, score=created_at（毫秒时间戳） | 1h | 最近 100 条通知 ID 有序集合 |
| `notif:kafka:dedup:{event_id}` | STRING | "1" | 24h | Kafka 生产幂等去重（RPC 侧用于事件生产去重） |

---

## 接口实现

### 1. SendNotification — 创建并发送系统通知

> 创建一条系统通知并投递给目标用户。  
> 流程：参数校验 → 获取发送者信息 → 生成通知 ID → 写入 PgSQL → 更新 Redis 未读计数和列表 → 投递 Kafka 事件 → 返回。

```go
func (s *NotificationService) SendNotification(ctx context.Context, req *pb.SendNotificationRequest) (*pb.SendNotificationResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id is required")
    }
    if req.Type == common.NOTIFICATION_TYPE_UNSPECIFIED {
        return nil, status.Error(codes.InvalidArgument, "notification type is required")
    }
    if req.Title == "" && req.Content == "" {
        return nil, status.Error(codes.InvalidArgument, "title or content is required")
    }
    if len(req.Title) > 200 {
        return nil, status.Error(codes.InvalidArgument, "title too long, max 200 chars")
    }
    if len(req.Content) > 5000 {
        return nil, status.Error(codes.InvalidArgument, "content too long, max 5000 chars")
    }

    // ==================== 2. 获取发送者信息（如果有 sender_id）— RPC User.GetUser ====================
    var senderNickname, senderAvatar string
    if req.SenderId != "" {
        senderResp, err := s.userClient.GetUser(ctx, &user_pb.GetUserRequest{UserId: req.SenderId})
        if err != nil {
            // 发送者查不到不阻塞通知创建，仅记录日志
            log.Warn("获取通知发送者信息失败，继续创建通知",
                "sender_id", req.SenderId, "err", err)
        } else {
            senderNickname = senderResp.User.Nickname
            senderAvatar = senderResp.User.AvatarUrl
        }
    }

    // ==================== 3. 生成通知 ID — Snowflake ====================
    notificationID := s.snowflake.Generate().String()
    now := time.Now()
    nowMs := now.UnixMilli()

    // ==================== 4. 序列化扩展数据 ====================
    extraJSON := "{}"
    if len(req.Extra) > 0 {
        extraBytes, err := json.Marshal(req.Extra)
        if err != nil {
            return nil, status.Error(codes.InvalidArgument, "invalid extra data")
        }
        extraJSON = string(extraBytes)
    }

    // ==================== 5. 写入 PgSQL ====================
    var expiresAt *time.Time
    // 某些通知类型设置默认过期时间：好友请求 7 天过期，系统公告 30 天过期
    switch req.Type {
    case common.NOTIFICATION_TYPE_FRIEND_REQUEST:
        exp := now.Add(7 * 24 * time.Hour)
        expiresAt = &exp
    case common.NOTIFICATION_TYPE_SYSTEM_ANNOUNCEMENT:
        exp := now.Add(30 * 24 * time.Hour)
        expiresAt = &exp
    }

    _, err := s.db.ExecContext(ctx,
        `INSERT INTO notifications (notification_id, user_id, type, title, content, data, sender_id, is_read, is_deleted, created_at, expires_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, false, false, $8, $9)`,
        notificationID, req.UserId, int(req.Type),
        req.Title, req.Content, extraJSON, req.SenderId,
        now, expiresAt,
    )
    if err != nil {
        log.Error("写入通知到 PgSQL 失败",
            "notification_id", notificationID, "user_id", req.UserId, "err", err)
        return nil, status.Error(codes.Internal, "create notification failed")
    }

    // ==================== 6. 更新 Redis 未读计数和列表 ====================
    pipe := s.redis.Pipeline()

    // 6a. 总未读数 +1
    // Key: notif:unread:{user_id}  TTL: 24h
    unreadKey := fmt.Sprintf("notif:unread:%s", req.UserId)
    pipe.Incr(ctx, unreadKey)
    pipe.Expire(ctx, unreadKey, 24*time.Hour)

    // 6b. 按类型未读数 +1
    // Key: notif:unread_type:{user_id}:{type}  TTL: 24h
    unreadTypeKey := fmt.Sprintf("notif:unread_type:%s:%d", req.UserId, int(req.Type))
    pipe.Incr(ctx, unreadTypeKey)
    pipe.Expire(ctx, unreadTypeKey, 24*time.Hour)

    // 6c. 添加到最近通知列表 ZSET
    // Key: notif:list:{user_id}  member=notification_id  score=created_at(毫秒)  TTL: 1h
    listKey := fmt.Sprintf("notif:list:%s", req.UserId)
    pipe.ZAdd(ctx, listKey, &redis.Z{
        Score:  float64(nowMs),
        Member: notificationID,
    })
    pipe.Expire(ctx, listKey, 1*time.Hour)
    // 裁剪 ZSET 只保留最近 100 条
    pipe.ZRemRangeByRank(ctx, listKey, 0, -101)

    _, err = pipe.Exec(ctx)
    if err != nil {
        log.Warn("更新 Redis 通知缓存失败（不影响主流程）",
            "notification_id", notificationID, "user_id", req.UserId, "err", err)
    }

    // ==================== 7. 投递 Kafka 事件 — notification.created ====================
    createdEvent := &kafka_notification.NotificationCreatedEvent{
        Header:          buildEventHeader("notification", s.instanceID),
        NotificationId:  notificationID,
        TargetUserId:    req.UserId,
        Type:            req.Type,
        Title:           req.Title,
        Content:         req.Content,
        SenderId:        req.SenderId,
        SenderNickname:  senderNickname,
        SenderAvatar:    senderAvatar,
        Extra:           req.Extra,
        CreateTime:      nowMs,
        ExpireTime:      0, // 由 expiresAt 推断
    }
    if expiresAt != nil {
        createdEvent.ExpireTime = expiresAt.UnixMilli()
    }
    if err := s.kafka.Produce(ctx, "notification.created", req.UserId, createdEvent); err != nil {
        log.Error("投递 notification.created 事件失败",
            "notification_id", notificationID, "user_id", req.UserId, "err", err)
        // Kafka 投递失败不影响 RPC 返回，通知已持久化
    }

    log.Info("通知创建成功",
        "notification_id", notificationID,
        "user_id", req.UserId,
        "type", req.Type,
        "sender_id", req.SenderId)

    return &pb.SendNotificationResponse{
        Meta:           successMeta(ctx),
        NotificationId: notificationID,
    }, nil
}
```

### 2. BatchSendNotification — 批量发送系统通知

> 向多个用户发送同一条系统通知（如系统公告、群事件通知）。  
> 逐用户创建独立的通知记录，互不影响（每个用户可独立标记已读/删除）。

```go
func (s *NotificationService) BatchSendNotification(ctx context.Context, req *pb.BatchSendNotificationRequest) (*pb.BatchSendNotificationResponse, error) {
    // ==================== 1. 参数校验 ====================
    if len(req.UserIds) == 0 {
        return nil, status.Error(codes.InvalidArgument, "user_ids is required")
    }
    if len(req.UserIds) > 10000 {
        return nil, status.Error(codes.InvalidArgument, "user_ids too many, max 10000")
    }
    if req.Type == common.NOTIFICATION_TYPE_UNSPECIFIED {
        return nil, status.Error(codes.InvalidArgument, "notification type is required")
    }
    if req.Title == "" && req.Content == "" {
        return nil, status.Error(codes.InvalidArgument, "title or content is required")
    }
    if len(req.Title) > 200 {
        return nil, status.Error(codes.InvalidArgument, "title too long, max 200 chars")
    }

    // ==================== 2. 用户去重 ====================
    userIDSet := make(map[string]struct{})
    uniqueUserIDs := make([]string, 0, len(req.UserIds))
    for _, uid := range req.UserIds {
        if uid == "" {
            continue
        }
        if _, exists := userIDSet[uid]; !exists {
            userIDSet[uid] = struct{}{}
            uniqueUserIDs = append(uniqueUserIDs, uid)
        }
    }
    if len(uniqueUserIDs) == 0 {
        return nil, status.Error(codes.InvalidArgument, "no valid user_ids")
    }

    // ==================== 3. 获取发送者信息 — RPC User.GetUser ====================
    var senderNickname, senderAvatar string
    if req.SenderId != "" {
        senderResp, err := s.userClient.GetUser(ctx, &user_pb.GetUserRequest{UserId: req.SenderId})
        if err != nil {
            log.Warn("批量通知：获取发送者信息失败，继续执行",
                "sender_id", req.SenderId, "err", err)
        } else {
            senderNickname = senderResp.User.Nickname
            senderAvatar = senderResp.User.AvatarUrl
        }
    }

    // ==================== 4. 序列化扩展数据 ====================
    extraJSON := "{}"
    if len(req.Extra) > 0 {
        extraBytes, err := json.Marshal(req.Extra)
        if err != nil {
            return nil, status.Error(codes.InvalidArgument, "invalid extra data")
        }
        extraJSON = string(extraBytes)
    }

    // ==================== 5. 计算过期时间 ====================
    now := time.Now()
    nowMs := now.UnixMilli()
    var expiresAt *time.Time
    switch req.Type {
    case common.NOTIFICATION_TYPE_FRIEND_REQUEST:
        exp := now.Add(7 * 24 * time.Hour)
        expiresAt = &exp
    case common.NOTIFICATION_TYPE_SYSTEM_ANNOUNCEMENT:
        exp := now.Add(30 * 24 * time.Hour)
        expiresAt = &exp
    }

    // ==================== 6. 分批写入 PgSQL + 更新 Redis + 投递 Kafka ====================
    successCount := int32(0)
    failedUserIDs := make([]string, 0)
    batchSize := 200 // 每批处理 200 个用户

    for i := 0; i < len(uniqueUserIDs); i += batchSize {
        end := i + batchSize
        if end > len(uniqueUserIDs) {
            end = len(uniqueUserIDs)
        }
        batch := uniqueUserIDs[i:end]

        // 6a. 批量构建 INSERT 语句（使用 VALUES 批量插入，减少 DB 往返）
        valueStrings := make([]string, 0, len(batch))
        valueArgs := make([]interface{}, 0, len(batch)*9)
        notifIDs := make(map[string]string) // user_id → notification_id

        for j, userID := range batch {
            notifID := s.snowflake.Generate().String()
            notifIDs[userID] = notifID

            base := j * 9
            valueStrings = append(valueStrings,
                fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d)",
                    base+1, base+2, base+3, base+4, base+5, base+6, base+7, base+8, base+9))
            valueArgs = append(valueArgs,
                notifID, userID, int(req.Type),
                req.Title, req.Content, extraJSON, req.SenderId,
                now, expiresAt,
            )
        }

        query := fmt.Sprintf(
            `INSERT INTO notifications (notification_id, user_id, type, title, content, data, sender_id, created_at, expires_at)
             VALUES %s`,
            strings.Join(valueStrings, ", "),
        )

        _, err := s.db.ExecContext(ctx, query, valueArgs...)
        if err != nil {
            log.Error("批量写入通知失败",
                "batch_start", i, "batch_size", len(batch), "err", err)
            // 整批失败，逐条回退尝试
            for _, userID := range batch {
                notifID := notifIDs[userID]
                _, retryErr := s.db.ExecContext(ctx,
                    `INSERT INTO notifications (notification_id, user_id, type, title, content, data, sender_id, created_at, expires_at)
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                     ON CONFLICT (notification_id) DO NOTHING`,
                    notifID, userID, int(req.Type),
                    req.Title, req.Content, extraJSON, req.SenderId,
                    now, expiresAt,
                )
                if retryErr != nil {
                    failedUserIDs = append(failedUserIDs, userID)
                    log.Error("单条通知写入也失败", "user_id", userID, "err", retryErr)
                    continue
                }
                successCount++
            }
            continue
        }

        // 6b. 批量更新 Redis 缓存
        pipe := s.redis.Pipeline()
        for _, userID := range batch {
            notifID := notifIDs[userID]

            // 总未读数 +1
            unreadKey := fmt.Sprintf("notif:unread:%s", userID)
            pipe.Incr(ctx, unreadKey)
            pipe.Expire(ctx, unreadKey, 24*time.Hour)

            // 按类型未读数 +1
            unreadTypeKey := fmt.Sprintf("notif:unread_type:%s:%d", userID, int(req.Type))
            pipe.Incr(ctx, unreadTypeKey)
            pipe.Expire(ctx, unreadTypeKey, 24*time.Hour)

            // 添加到最近通知列表 ZSET
            listKey := fmt.Sprintf("notif:list:%s", userID)
            pipe.ZAdd(ctx, listKey, &redis.Z{
                Score:  float64(nowMs),
                Member: notifID,
            })
            pipe.Expire(ctx, listKey, 1*time.Hour)
            pipe.ZRemRangeByRank(ctx, listKey, 0, -101)
        }
        if _, err := pipe.Exec(ctx); err != nil {
            log.Warn("批量更新 Redis 通知缓存部分失败", "err", err)
        }

        // 6c. 批量投递 Kafka 事件
        for _, userID := range batch {
            notifID := notifIDs[userID]

            createdEvent := &kafka_notification.NotificationCreatedEvent{
                Header:          buildEventHeader("notification", s.instanceID),
                NotificationId:  notifID,
                TargetUserId:    userID,
                Type:            req.Type,
                Title:           req.Title,
                Content:         req.Content,
                SenderId:        req.SenderId,
                SenderNickname:  senderNickname,
                SenderAvatar:    senderAvatar,
                Extra:           req.Extra,
                CreateTime:      nowMs,
            }
            if expiresAt != nil {
                createdEvent.ExpireTime = expiresAt.UnixMilli()
            }
            if err := s.kafka.Produce(ctx, "notification.created", userID, createdEvent); err != nil {
                log.Error("投递 notification.created 事件失败",
                    "notification_id", notifID, "user_id", userID, "err", err)
            }
        }

        successCount += int32(len(batch))
    }

    log.Info("批量通知发送完成",
        "total_users", len(uniqueUserIDs),
        "success_count", successCount,
        "failed_count", len(failedUserIDs),
        "type", req.Type)

    return &pb.BatchSendNotificationResponse{
        Meta:          successMeta(ctx),
        SuccessCount:  successCount,
        FailedUserIds: failedUserIDs,
    }, nil
}
```

### 3. GetNotifications — 获取通知列表

> 获取用户的通知列表，支持分页、类型过滤、仅未读过滤。  
> 优先从 Redis ZSET 缓存读取通知 ID 列表，缓存未命中则查 PgSQL 并回填。

```go
func (s *NotificationService) GetNotifications(ctx context.Context, req *pb.GetNotificationsRequest) (*pb.GetNotificationsResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id is required")
    }

    // 分页参数默认值
    page := int(req.Pagination.GetPage())
    pageSize := int(req.Pagination.GetPageSize())
    if page <= 0 {
        page = 1
    }
    if pageSize <= 0 {
        pageSize = 20
    }
    if pageSize > 100 {
        pageSize = 100
    }
    offset := (page - 1) * pageSize

    // ==================== 2. 尝试 Redis ZSET 缓存快速路径（无类型过滤 + 无仅未读 + 第一页） ====================
    if req.Type == common.NOTIFICATION_TYPE_UNSPECIFIED && !req.UnreadOnly && page == 1 && pageSize <= 100 {
        listKey := fmt.Sprintf("notif:list:%s", req.UserId)
        notifIDs, err := s.redis.ZRevRange(ctx, listKey, 0, int64(pageSize-1)).Result()
        if err == nil && len(notifIDs) > 0 {
            // 缓存命中 → 批量查 PgSQL 获取通知详情
            notifications, err := s.batchGetNotificationsByIDs(ctx, req.UserId, notifIDs)
            if err == nil && len(notifications) > 0 {
                // 查询总数
                var total int64
                err = s.db.QueryRowContext(ctx,
                    `SELECT COUNT(*) FROM notifications
                     WHERE user_id = $1 AND is_deleted = false`,
                    req.UserId,
                ).Scan(&total)
                if err != nil {
                    total = int64(len(notifications))
                }

                return &pb.GetNotificationsResponse{
                    Meta:          successMeta(ctx),
                    Notifications: notifications,
                    PaginationResult: &common.PaginationResult{
                        Page:       int32(page),
                        PageSize:   int32(pageSize),
                        Total:      total,
                        TotalPages: int32((total + int64(pageSize) - 1) / int64(pageSize)),
                    },
                }, nil
            }
        }
    }

    // ==================== 3. Redis 未命中或有过滤条件 → 查 PgSQL ====================
    // 3a. 构建动态查询条件
    conditions := []string{"user_id = $1", "is_deleted = false"}
    args := []interface{}{req.UserId}
    argIdx := 2

    // 过期通知不展示
    conditions = append(conditions, fmt.Sprintf("(expires_at IS NULL OR expires_at > $%d)", argIdx))
    args = append(args, time.Now())
    argIdx++

    if req.Type != common.NOTIFICATION_TYPE_UNSPECIFIED {
        conditions = append(conditions, fmt.Sprintf("type = $%d", argIdx))
        args = append(args, int(req.Type))
        argIdx++
    }
    if req.UnreadOnly {
        conditions = append(conditions, "is_read = false")
    }

    whereClause := strings.Join(conditions, " AND ")

    // 3b. 查询总数
    var total int64
    countSQL := fmt.Sprintf("SELECT COUNT(*) FROM notifications WHERE %s", whereClause)
    err := s.db.QueryRowContext(ctx, countSQL, args...).Scan(&total)
    if err != nil {
        log.Error("查询通知总数失败", "user_id", req.UserId, "err", err)
        return nil, status.Error(codes.Internal, "query notification count failed")
    }

    // 3c. 分页查询通知列表
    querySQL := fmt.Sprintf(
        `SELECT notification_id, user_id, type, title, content, data, sender_id,
                is_read, created_at
         FROM notifications
         WHERE %s
         ORDER BY created_at DESC
         LIMIT $%d OFFSET $%d`,
        whereClause, argIdx, argIdx+1,
    )
    args = append(args, pageSize, offset)

    rows, err := s.db.QueryContext(ctx, querySQL, args...)
    if err != nil {
        log.Error("查询通知列表失败", "user_id", req.UserId, "err", err)
        return nil, status.Error(codes.Internal, "query notifications failed")
    }
    defer rows.Close()

    notifications := make([]*pb.NotificationInfo, 0)
    for rows.Next() {
        var n pb.NotificationInfo
        var dataJSON string
        var createdAt time.Time
        var notifType int

        err := rows.Scan(
            &n.NotificationId, &n.UserId, &notifType,
            &n.Title, &n.Content, &dataJSON, &n.SenderId,
            &n.IsRead, &createdAt,
        )
        if err != nil {
            log.Error("扫描通知记录失败", "err", err)
            continue
        }
        n.Type = common.NotificationType(notifType)
        n.CreatedAt = createdAt.UnixMilli()

        // 解析扩展数据
        if dataJSON != "" && dataJSON != "{}" {
            extra := make(map[string]string)
            if json.Unmarshal([]byte(dataJSON), &extra) == nil {
                n.Extra = extra
            }
        }

        notifications = append(notifications, &n)
    }

    // ==================== 4. 批量填充发送者信息 — RPC User.GetUser ====================
    senderIDs := make(map[string]struct{})
    for _, n := range notifications {
        if n.SenderId != "" {
            senderIDs[n.SenderId] = struct{}{}
        }
    }
    senderInfoMap := make(map[string]*common.UserBrief)
    for senderID := range senderIDs {
        senderResp, err := s.userClient.GetUser(ctx, &user_pb.GetUserRequest{UserId: senderID})
        if err == nil && senderResp.User != nil {
            senderInfoMap[senderID] = &common.UserBrief{
                UserId:    senderResp.User.UserId,
                Nickname:  senderResp.User.Nickname,
                AvatarUrl: senderResp.User.AvatarUrl,
            }
        }
    }
    for _, n := range notifications {
        if brief, ok := senderInfoMap[n.SenderId]; ok {
            n.Sender = brief
        }
    }

    // ==================== 5. 回填 Redis ZSET 缓存（无过滤条件 + 第一页时） ====================
    if req.Type == common.NOTIFICATION_TYPE_UNSPECIFIED && !req.UnreadOnly && page == 1 && len(notifications) > 0 {
        listKey := fmt.Sprintf("notif:list:%s", req.UserId)
        members := make([]*redis.Z, 0, len(notifications))
        for _, n := range notifications {
            members = append(members, &redis.Z{
                Score:  float64(n.CreatedAt),
                Member: n.NotificationId,
            })
        }
        pipe := s.redis.Pipeline()
        pipe.Del(ctx, listKey) // 先清空再回填
        pipe.ZAdd(ctx, listKey, members...)
        pipe.Expire(ctx, listKey, 1*time.Hour)
        pipe.Exec(ctx)
    }

    return &pb.GetNotificationsResponse{
        Meta:          successMeta(ctx),
        Notifications: notifications,
        PaginationResult: &common.PaginationResult{
            Page:       int32(page),
            PageSize:   int32(pageSize),
            Total:      total,
            TotalPages: int32((total + int64(pageSize) - 1) / int64(pageSize)),
        },
    }, nil
}

// batchGetNotificationsByIDs 根据通知 ID 列表批量查询通知详情（保持 ID 列表顺序）
func (s *NotificationService) batchGetNotificationsByIDs(ctx context.Context, userID string, notifIDs []string) ([]*pb.NotificationInfo, error) {
    if len(notifIDs) == 0 {
        return nil, nil
    }

    // 构建 IN 查询
    placeholders := make([]string, len(notifIDs))
    args := make([]interface{}, 0, len(notifIDs)+1)
    args = append(args, userID)
    for i, id := range notifIDs {
        placeholders[i] = fmt.Sprintf("$%d", i+2)
        args = append(args, id)
    }

    query := fmt.Sprintf(
        `SELECT notification_id, user_id, type, title, content, data, sender_id,
                is_read, created_at
         FROM notifications
         WHERE user_id = $1 AND notification_id IN (%s)
           AND is_deleted = false
           AND (expires_at IS NULL OR expires_at > NOW())`,
        strings.Join(placeholders, ", "),
    )

    rows, err := s.db.QueryContext(ctx, query, args...)
    if err != nil {
        return nil, fmt.Errorf("batch query notifications failed: %w", err)
    }
    defer rows.Close()

    notifMap := make(map[string]*pb.NotificationInfo)
    for rows.Next() {
        var n pb.NotificationInfo
        var dataJSON string
        var createdAt time.Time
        var notifType int

        if err := rows.Scan(
            &n.NotificationId, &n.UserId, &notifType,
            &n.Title, &n.Content, &dataJSON, &n.SenderId,
            &n.IsRead, &createdAt,
        ); err != nil {
            continue
        }
        n.Type = common.NotificationType(notifType)
        n.CreatedAt = createdAt.UnixMilli()

        if dataJSON != "" && dataJSON != "{}" {
            extra := make(map[string]string)
            if json.Unmarshal([]byte(dataJSON), &extra) == nil {
                n.Extra = extra
            }
        }
        notifMap[n.NotificationId] = &n
    }

    // 按原始 ID 顺序返回
    result := make([]*pb.NotificationInfo, 0, len(notifIDs))
    for _, id := range notifIDs {
        if n, ok := notifMap[id]; ok {
            result = append(result, n)
        }
    }
    return result, nil
}
```

### 4. MarkNotificationRead — 标记单条/多条通知为已读

> 将指定的一条或多条通知标记为已读，同步更新 PgSQL 状态和 Redis 未读计数。

```go
func (s *NotificationService) MarkNotificationRead(ctx context.Context, req *pb.MarkNotificationReadRequest) (*pb.MarkNotificationReadResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id is required")
    }
    if len(req.NotificationIds) == 0 {
        return nil, status.Error(codes.InvalidArgument, "notification_ids is required")
    }
    if len(req.NotificationIds) > 100 {
        return nil, status.Error(codes.InvalidArgument, "notification_ids too many, max 100")
    }

    // ==================== 2. 查询这些通知中哪些是真正未读的 — PgSQL ====================
    // 先查出未读通知的 ID 和类型，用于后续精确更新 Redis 计数
    placeholders := make([]string, len(req.NotificationIds))
    args := make([]interface{}, 0, len(req.NotificationIds)+1)
    args = append(args, req.UserId)
    for i, id := range req.NotificationIds {
        placeholders[i] = fmt.Sprintf("$%d", i+2)
        args = append(args, id)
    }

    querySQL := fmt.Sprintf(
        `SELECT notification_id, type FROM notifications
         WHERE user_id = $1 AND notification_id IN (%s)
           AND is_read = false AND is_deleted = false`,
        strings.Join(placeholders, ", "),
    )

    rows, err := s.db.QueryContext(ctx, querySQL, args...)
    if err != nil {
        log.Error("查询未读通知失败", "user_id", req.UserId, "err", err)
        return nil, status.Error(codes.Internal, "query unread notifications failed")
    }
    defer rows.Close()

    // 按类型统计未读数变化量
    unreadByType := make(map[int]int64) // type → 减少的数量
    unreadIDs := make([]string, 0)
    for rows.Next() {
        var notifID string
        var notifType int
        if err := rows.Scan(&notifID, &notifType); err != nil {
            continue
        }
        unreadIDs = append(unreadIDs, notifID)
        unreadByType[notifType]++
    }

    if len(unreadIDs) == 0 {
        // 全部已经是已读状态，直接返回
        return &pb.MarkNotificationReadResponse{
            Meta: successMeta(ctx),
        }, nil
    }

    // ==================== 3. 批量更新已读状态 — PgSQL ====================
    updatePlaceholders := make([]string, len(unreadIDs))
    updateArgs := make([]interface{}, 0, len(unreadIDs)+1)
    updateArgs = append(updateArgs, req.UserId)
    for i, id := range unreadIDs {
        updatePlaceholders[i] = fmt.Sprintf("$%d", i+2)
        updateArgs = append(updateArgs, id)
    }

    updateSQL := fmt.Sprintf(
        `UPDATE notifications SET is_read = true
         WHERE user_id = $1 AND notification_id IN (%s)
           AND is_read = false AND is_deleted = false`,
        strings.Join(updatePlaceholders, ", "),
    )

    _, err = s.db.ExecContext(ctx, updateSQL, updateArgs...)
    if err != nil {
        log.Error("更新通知已读状态失败", "user_id", req.UserId, "err", err)
        return nil, status.Error(codes.Internal, "mark notification read failed")
    }

    // ==================== 4. 延迟双删清除 Redis 未读计数缓存 ====================
    cacheKeys := []string{fmt.Sprintf("notif:unread:%s", req.UserId)}
    for notifType := range unreadByType {
        cacheKeys = append(cacheKeys, fmt.Sprintf("notif:unread_type:%s:%d", req.UserId, notifType))
    }
    s.delayedDoubleDelete(ctx, cacheKeys...)

    // ==================== 5. 投递 Kafka 事件 — notification.read ====================
    readEvent := &kafka_notification.NotificationReadEvent{
        Header:          buildEventHeader("notification", s.instanceID),
        UserId:          req.UserId,
        NotificationIds: unreadIDs,
        ReadTime:        time.Now().UnixMilli(),
    }
    if err := s.kafka.Produce(ctx, "notification.read", req.UserId, readEvent); err != nil {
        log.Error("投递 notification.read 事件失败",
            "user_id", req.UserId, "err", err)
    }

    log.Info("通知标记已读成功",
        "user_id", req.UserId,
        "marked_count", len(unreadIDs))

    return &pb.MarkNotificationReadResponse{
        Meta: successMeta(ctx),
    }, nil
}
```

### 5. MarkAllNotificationRead — 标记全部通知为已读

> 将用户所有未读通知一次性标记为已读，支持按类型过滤（仅标记指定类型的通知）。

```go
func (s *NotificationService) MarkAllNotificationRead(ctx context.Context, req *pb.MarkAllNotificationReadRequest) (*pb.MarkAllNotificationReadResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id is required")
    }

    // ==================== 2. 批量更新已读状态 — PgSQL ====================
    var result sql.Result
    var err error

    if req.Type != common.NOTIFICATION_TYPE_UNSPECIFIED {
        // 指定类型：仅标记该类型的未读通知
        result, err = s.db.ExecContext(ctx,
            `UPDATE notifications SET is_read = true
             WHERE user_id = $1 AND type = $2
               AND is_read = false AND is_deleted = false`,
            req.UserId, int(req.Type),
        )
    } else {
        // 全部类型：标记所有未读通知
        result, err = s.db.ExecContext(ctx,
            `UPDATE notifications SET is_read = true
             WHERE user_id = $1
               AND is_read = false AND is_deleted = false`,
            req.UserId,
        )
    }

    if err != nil {
        log.Error("批量标记通知已读失败", "user_id", req.UserId, "err", err)
        return nil, status.Error(codes.Internal, "mark all notifications read failed")
    }

    markedCount, _ := result.RowsAffected()

    // ==================== 3. 延迟双删清除 Redis 未读计数缓存 ====================
    cacheKeys := []string{fmt.Sprintf("notif:unread:%s", req.UserId)}
    if req.Type != common.NOTIFICATION_TYPE_UNSPECIFIED {
        // 指定类型：清除该类型计数 + 总数
        cacheKeys = append(cacheKeys, fmt.Sprintf("notif:unread_type:%s:%d", req.UserId, int(req.Type)))
    } else {
        // 全部类型：清除总数 + 所有类型计数
        for notifType := 1; notifType <= 8; notifType++ {
            cacheKeys = append(cacheKeys, fmt.Sprintf("notif:unread_type:%s:%d", req.UserId, notifType))
        }
    }
    s.delayedDoubleDelete(ctx, cacheKeys...)

    // ==================== 4. 投递 Kafka 事件 — notification.read.all ====================
    readAllEvent := &kafka_notification.NotificationReadAllEvent{
        Header:   buildEventHeader("notification", s.instanceID),
        UserId:   req.UserId,
        Type:     req.Type,
        ReadTime: time.Now().UnixMilli(),
        Count:    int32(markedCount),
    }
    if err := s.kafka.Produce(ctx, "notification.read.all", req.UserId, readAllEvent); err != nil {
        log.Error("投递 notification.read.all 事件失败",
            "user_id", req.UserId, "err", err)
    }

    log.Info("全部通知标记已读成功",
        "user_id", req.UserId,
        "type_filter", req.Type,
        "marked_count", markedCount)

    return &pb.MarkAllNotificationReadResponse{
        Meta:        successMeta(ctx),
        MarkedCount: int32(markedCount),
    }, nil
}
```

### 6. DeleteNotification — 删除通知

> 软删除指定通知（标记 `is_deleted = true`），同步更新 Redis 缓存和未读计数。

```go
func (s *NotificationService) DeleteNotification(ctx context.Context, req *pb.DeleteNotificationRequest) (*pb.DeleteNotificationResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id is required")
    }
    if len(req.NotificationIds) == 0 {
        return nil, status.Error(codes.InvalidArgument, "notification_ids is required")
    }
    if len(req.NotificationIds) > 100 {
        return nil, status.Error(codes.InvalidArgument, "notification_ids too many, max 100")
    }

    // ==================== 2. 查询待删除通知的已读状态和类型（用于更新未读计数）— PgSQL ====================
    placeholders := make([]string, len(req.NotificationIds))
    args := make([]interface{}, 0, len(req.NotificationIds)+1)
    args = append(args, req.UserId)
    for i, id := range req.NotificationIds {
        placeholders[i] = fmt.Sprintf("$%d", i+2)
        args = append(args, id)
    }

    querySQL := fmt.Sprintf(
        `SELECT notification_id, type, is_read FROM notifications
         WHERE user_id = $1 AND notification_id IN (%s)
           AND is_deleted = false`,
        strings.Join(placeholders, ", "),
    )

    rows, err := s.db.QueryContext(ctx, querySQL, args...)
    if err != nil {
        log.Error("查询待删除通知失败", "user_id", req.UserId, "err", err)
        return nil, status.Error(codes.Internal, "query notifications for delete failed")
    }
    defer rows.Close()

    deleteIDs := make([]string, 0)
    unreadByType := make(map[int]int64) // 需要扣减的未读数：type → count
    for rows.Next() {
        var notifID string
        var notifType int
        var isRead bool
        if err := rows.Scan(&notifID, &notifType, &isRead); err != nil {
            continue
        }
        deleteIDs = append(deleteIDs, notifID)
        if !isRead {
            unreadByType[notifType]++
        }
    }

    if len(deleteIDs) == 0 {
        return &pb.DeleteNotificationResponse{
            Meta: successMeta(ctx),
        }, nil
    }

    // ==================== 3. 软删除 — PgSQL ====================
    delPlaceholders := make([]string, len(deleteIDs))
    delArgs := make([]interface{}, 0, len(deleteIDs)+1)
    delArgs = append(delArgs, req.UserId)
    for i, id := range deleteIDs {
        delPlaceholders[i] = fmt.Sprintf("$%d", i+2)
        delArgs = append(delArgs, id)
    }

    updateSQL := fmt.Sprintf(
        `UPDATE notifications SET is_deleted = true
         WHERE user_id = $1 AND notification_id IN (%s)
           AND is_deleted = false`,
        strings.Join(delPlaceholders, ", "),
    )

    _, err = s.db.ExecContext(ctx, updateSQL, delArgs...)
    if err != nil {
        log.Error("软删除通知失败", "user_id", req.UserId, "err", err)
        return nil, status.Error(codes.Internal, "delete notification failed")
    }

    // ==================== 4. 延迟双删清除 Redis 缓存 ====================
    cacheKeys := []string{
        fmt.Sprintf("notif:list:%s", req.UserId),    // 通知列表 ZSET
        fmt.Sprintf("notif:unread:%s", req.UserId),  // 总未读数
    }
    for notifType := range unreadByType {
        cacheKeys = append(cacheKeys, fmt.Sprintf("notif:unread_type:%s:%d", req.UserId, notifType))
    }
    s.delayedDoubleDelete(ctx, cacheKeys...)

    // ==================== 5. 投递 Kafka 事件 — notification.deleted ====================
    deletedEvent := &kafka_notification.NotificationDeletedEvent{
        Header:          buildEventHeader("notification", s.instanceID),
        UserId:          req.UserId,
        NotificationIds: deleteIDs,
        DeleteTime:      time.Now().UnixMilli(),
    }
    if err := s.kafka.Produce(ctx, "notification.deleted", req.UserId, deletedEvent); err != nil {
        log.Error("投递 notification.deleted 事件失败",
            "user_id", req.UserId, "err", err)
    }

    log.Info("通知删除成功",
        "user_id", req.UserId,
        "deleted_count", len(deleteIDs),
        "unread_decr", totalUnreadDecr)

    return &pb.DeleteNotificationResponse{
        Meta: successMeta(ctx),
    }, nil
}
```

### 7. GetUnreadNotificationCount — 获取未读通知数量

> 获取用户未读通知总数和各类型未读数量。  
> 优先读 Redis 缓存，缓存未命中或不完整时从 PgSQL 回填。

```go
func (s *NotificationService) GetUnreadNotificationCount(ctx context.Context, req *pb.GetUnreadNotificationCountRequest) (*pb.GetUnreadNotificationCountResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id is required")
    }

    // ==================== 2. 尝试从 Redis 读取未读计数 ====================
    unreadKey := fmt.Sprintf("notif:unread:%s", req.UserId)

    if req.Type != common.NOTIFICATION_TYPE_UNSPECIFIED {
        // 指定类型：仅返回该类型未读数
        unreadTypeKey := fmt.Sprintf("notif:unread_type:%s:%d", req.UserId, int(req.Type))
        typeCount, err := s.redis.Get(ctx, unreadTypeKey).Int64()
        if err == nil && typeCount >= 0 {
            typeCountMap := map[string]int64{
                fmt.Sprintf("%d", int(req.Type)): typeCount,
            }
            return &pb.GetUnreadNotificationCountResponse{
                Meta:             successMeta(ctx),
                UnreadCount:      typeCount,
                TypeUnreadCounts: typeCountMap,
            }, nil
        }
        // Redis 未命中，走 PgSQL
    } else {
        // 全部类型：先尝试从 Redis 读总数
        totalCount, err := s.redis.Get(ctx, unreadKey).Int64()
        if err == nil && totalCount >= 0 {
            // 总数命中，继续读各类型计数
            typeCountMap := make(map[string]int64)
            allTypesHit := true

            for notifType := 1; notifType <= 8; notifType++ {
                typeKey := fmt.Sprintf("notif:unread_type:%s:%d", req.UserId, notifType)
                typeVal, err := s.redis.Get(ctx, typeKey).Int64()
                if err != nil {
                    allTypesHit = false
                    break
                }
                if typeVal > 0 {
                    typeCountMap[fmt.Sprintf("%d", notifType)] = typeVal
                }
            }

            if allTypesHit {
                return &pb.GetUnreadNotificationCountResponse{
                    Meta:             successMeta(ctx),
                    UnreadCount:      totalCount,
                    TypeUnreadCounts: typeCountMap,
                }, nil
            }
            // 部分类型缓存缺失，走 PgSQL 回填
        }
    }

    // ==================== 3. Redis 未命中 → 从 PgSQL 查询并回填 Redis ====================
    // 3a. 查询总未读数
    var totalUnread int64
    err := s.db.QueryRowContext(ctx,
        `SELECT COUNT(*) FROM notifications
         WHERE user_id = $1 AND is_read = false AND is_deleted = false
           AND (expires_at IS NULL OR expires_at > NOW())`,
        req.UserId,
    ).Scan(&totalUnread)
    if err != nil {
        log.Error("查询总未读通知数失败", "user_id", req.UserId, "err", err)
        return nil, status.Error(codes.Internal, "query unread notification count failed")
    }

    // 3b. 查询各类型未读数
    rows, err := s.db.QueryContext(ctx,
        `SELECT type, COUNT(*) as cnt FROM notifications
         WHERE user_id = $1 AND is_read = false AND is_deleted = false
           AND (expires_at IS NULL OR expires_at > NOW())
         GROUP BY type`,
        req.UserId,
    )
    if err != nil {
        log.Error("查询各类型未读通知数失败", "user_id", req.UserId, "err", err)
        return nil, status.Error(codes.Internal, "query unread notification count by type failed")
    }
    defer rows.Close()

    typeCountMap := make(map[string]int64)
    for rows.Next() {
        var notifType int
        var count int64
        if err := rows.Scan(&notifType, &count); err != nil {
            continue
        }
        if count > 0 {
            typeCountMap[fmt.Sprintf("%d", notifType)] = count
        }
    }

    // ==================== 4. 回填 Redis 缓存 ====================
    pipe := s.redis.Pipeline()

    // 总未读数
    pipe.Set(ctx, unreadKey, totalUnread, 24*time.Hour)

    // 各类型未读数
    for notifType := 1; notifType <= 8; notifType++ {
        typeKey := fmt.Sprintf("notif:unread_type:%s:%d", req.UserId, notifType)
        count := typeCountMap[fmt.Sprintf("%d", notifType)]
        pipe.Set(ctx, typeKey, count, 24*time.Hour)
    }

    if _, err := pipe.Exec(ctx); err != nil {
        log.Warn("回填 Redis 未读计数缓存失败", "user_id", req.UserId, "err", err)
    }

    // ==================== 5. 如果指定了类型过滤，仅返回该类型 ====================
    if req.Type != common.NOTIFICATION_TYPE_UNSPECIFIED {
        typeKey := fmt.Sprintf("%d", int(req.Type))
        typeCount := typeCountMap[typeKey]
        return &pb.GetUnreadNotificationCountResponse{
            Meta:        successMeta(ctx),
            UnreadCount: typeCount,
            TypeUnreadCounts: map[string]int64{
                typeKey: typeCount,
            },
        }, nil
    }

    return &pb.GetUnreadNotificationCountResponse{
        Meta:             successMeta(ctx),
        UnreadCount:      totalUnread,
        TypeUnreadCounts: typeCountMap,
    }, nil
}
```

---

## 服务基础设施

```go
// NotificationService 通知服务 RPC 实现
type NotificationService struct {
    pb.UnimplementedNotificationServiceServer

    db         *sql.DB
    redis      *redis.Client
    kafka      *kafka.Producer
    snowflake  *snowflake.Node
    userClient user_pb.UserServiceClient
    instanceID string
}

// NewNotificationService 创建通知服务实例
func NewNotificationService(
    db *sql.DB,
    redis *redis.Client,
    kafkaProducer *kafka.Producer,
    userClient user_pb.UserServiceClient,
) *NotificationService {
    node, _ := snowflake.NewNode(1) // 节点 ID 从环境变量或配置获取
    return &NotificationService{
        db:         db,
        redis:      redis,
        kafka:      kafkaProducer,
        snowflake:  node,
        userClient: userClient,
        instanceID: os.Getenv("INSTANCE_ID"),
    }
}

// successMeta 构建成功响应元信息
func successMeta(ctx context.Context) *common.ResponseMeta {
    return &common.ResponseMeta{
        Code:       0,
        Message:    "success",
        ServerTime: time.Now().UnixMilli(),
        TraceId:    extractTraceID(ctx),
    }
}

// delayedDoubleDelete 延迟双删：保证缓存与 DB 的最终一致性
// Phase 1: 立即删除缓存（防止后续读到旧值）
// Phase 2: 500ms 后再次删除（防止并发读在 DB 写入窗口期回填旧缓存）
func (s *NotificationService) delayedDoubleDelete(ctx context.Context, keys ...string) {
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

---

## 可观测性接入（OpenTelemetry）

> Notification 服务负责系统通知与批量广播，需重点观测：通知发送速率、批量广播耗时、通知投递成功/失败率、未读通知数计算延迟、Kafka 事件发布延迟。

### 第一步：服务启动时初始化 OTel SDK

```go
func main() {
    ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
    defer cancel()

    otelShutdown := observability.InitOpenTelemetry(ctx, "notification", "v1.0.0")
    defer otelShutdown(ctx)
    observability.SetupLogger("notification")
}
```

### 第二步：gRPC Server/Client Interceptor

```go
grpcServer := grpc.NewServer(
    grpc.ChainUnaryInterceptor(otelgrpc.UnaryServerInterceptor()),
    grpc.ChainStreamInterceptor(otelgrpc.StreamServerInterceptor()), // 统一添加 Stream Interceptor
)
// Client — 调用 Push.PushToUser / User.GetUser / Group.GetGroupMember
pushConn := grpc.Dial("push:50051",
    grpc.WithChainUnaryInterceptor(otelgrpc.UnaryClientInterceptor()),
)
```

### 第三步：注册 Notification 专属业务指标

```go
var meter = otel.Meter("im-chat/notification")

var (
    // 通知发送计数
    notifSent, _ = meter.Int64Counter("notification.sent_total",
        metric.WithDescription("系统通知发送总数"))

    // 批量广播耗时
    broadcastDuration, _ = meter.Float64Histogram("notification.broadcast_duration_ms",
        metric.WithDescription("批量广播通知耗时"), metric.WithUnit("ms"))

    // 批量广播目标人数
    broadcastFanout, _ = meter.Int64Histogram("notification.broadcast_fanout_size",
        metric.WithDescription("广播通知扇出人数"))

    // 通知模板命中
    templateUsed, _ = meter.Int64Counter("notification.template_used_total",
        metric.WithDescription("通知模板使用次数"))

    // 未读通知数计算延迟
    unreadCountDuration, _ = meter.Float64Histogram("notification.unread_count_duration_ms",
        metric.WithDescription("未读通知数计算延迟"), metric.WithUnit("ms"))
)
```

在业务代码中埋点：

```go
// SendSystemNotification 中
notifSent.Add(ctx, 1, metric.WithAttributes(
    attribute.String("notif_type", req.NotifType),
))

// BatchBroadcast 中
broadcastStart := time.Now()
// ... 遍历目标用户列表发送 ...
broadcastDuration.Record(ctx, float64(time.Since(broadcastStart).Milliseconds()))
broadcastFanout.Record(ctx, int64(len(targetUsers)))

// GetUnreadCount 中
unreadStart := time.Now()
// ... 统计未读 ...
unreadCountDuration.Record(ctx, float64(time.Since(unreadStart).Milliseconds()))
```

### 第四步：Kafka Producer 埋点 + 结构化 JSON 日志 + buildEventHeader 改造

与所有服务一致。`buildEventHeader` 改为使用共享包：

```go
header := observability.BuildEventHeader(ctx, "notification", instanceID)
```

### 补充：基础设施指标注册

Notification 服务依赖 PgSQL（通知存储）+ Redis（未读计数缓存），需注册连接池指标：

```go
func main() {
    // ... initOpenTelemetry ...
    observability.RegisterDBPoolMetrics(db, "notification")
    observability.RegisterRedisPoolMetrics(redisClient, "notification")
}
```

### 补充：Span 错误记录

```go
func (s *NotificationServer) BatchBroadcast(ctx context.Context, req *pb.BatchBroadcastRequest) (*pb.BatchBroadcastResponse, error) {
    ctx, span := otel.Tracer("notification").Start(ctx, "BatchBroadcast")
    defer span.End()

    result, err := s.doBatchBroadcast(ctx, req)
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
| 3. 自定义业务指标 | SendNotification/Broadcast/GetUnread | 发送速率、广播耗时与扇出、未读计算延迟 |
| 4. Kafka + 日志 + EventHeader | `observability.BuildEventHeader` | 链路追踪 + trace_id 贯穿 |
| 5. 基础设施指标 | `RegisterDBPoolMetrics` + `RegisterRedisPoolMetrics` | DB/Redis 连接池健康 |
| 6. Span 错误记录 | `observability.RecordError` | 错误 trace 在 Tempo 可见 |
