# Ack 消息确认/已读回执服务 — Kafka 消费者/生产者实现伪代码

## 概述

Ack 服务通过 Kafka 接收来自 Message、User、Config 服务的事件，处理消息存储后的自动投递确认、消息撤回后的已读清理、用户注销后的数据清理、以及运行时配置变更。
同时 Ack 服务的 RPC 方法在执行过程中会生产已读/投递事件，供 Conversation、Push 等下游服务消费。

## 生产 Topic 列表

| Topic | 事件 | 生产时机 | Key | 消费方 |
|-------|------|----------|-----|--------|
| `ack.read.updated` | AckReadEvent | SendReadAck / BatchSendReadAck / SendGroupReadAck 成功后 | conversation_id | Conversation（更新未读数）, Push（通知发送者消息已读 + 多端已读同步） |
| `ack.read.group` | AckGroupReadEvent | SendGroupReadAck 更新群消息已读后 | msg_id | Push（通知群成员已读状态变更） |
| `ack.delivered` | AckReceivedEvent | ConfirmDelivered 确认投递后 | msg_id | Push（通知消息发送者"已送达"状态） |
| `ack.stats` | AckStatsEvent | 定时统计任务 | "stats" | Audit（审计统计） |

## 消费 Topic 列表

| Topic | 来源 | 用途 |
|-------|------|------|
| `msg.stored.single` | Message 服务 | 单聊消息存储成功 → 如果接收方在线且消息已展示，自动确认投递 |
| `msg.stored.group` | Message 服务 | 群聊消息存储成功 → 同上逻辑，适用于群消息 |
| `msg.recalled` | Message 服务 | 消息撤回 → 清除该消息的已读回执追踪数据 |
| `user.deactivated` | User 服务 | 用户注销 → 清理该用户的所有已读回执、投递记录 |
| `config.changed` | Config 服务 | 配置变更 → 热更新群已读回执开关、最大群人数限制等 |

## 外部 RPC 依赖（消费者使用）

| 依赖项 | 用途 |
|--------|------|
| Presence.GetPresence | 检查接收方是否在线（投递追踪判断） |
| Group.GetGroup | 查询群成员数（群已读回执初始化时的群规模判断） |

## Redis Key 设计（消费者专用）

| Key 格式 | 类型 | 值 | TTL | 说明 |
|----------|------|-----|-----|------|
| `ack:kafka:dedup:{event_id}` | STRING | "1" | 24h | Kafka 消费幂等去重 |
| `ack:auto_deliver_lock:{msg_id}:{user_id}` | STRING | "1" | 10min | 自动投递确认分布式锁，防止重复处理 |
| `ack:cleanup_progress:{user_id}` | STRING | "processing" | 1h | 用户数据清理进度标记 |

---

## 消费者实现

### Consumer: `msg.stored.single`

> 来源：Message 服务。单聊消息存储成功后触发。  
> 职责：初始化该消息的投递追踪状态。如果接收方当前在线且客户端已展示消息（通过 Presence 判断），可自动标记为已投递。  
> 场景：用户 A 发送消息给用户 B → Message 服务存储成功后产出 msg.stored.single → Ack 消费后初始化投递追踪 → 后续由 Push 服务调用 ConfirmDelivered 确认投递。

```go
func (c *AckConsumer) HandleMsgStoredSingle(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_msg.MsgStoredEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 MsgStoredEvent 失败", "err", err)
        return nil // 反序列化失败不重试，记录日志人工排查
    }

    // ==================== 2. 幂等检查 — Redis ====================
    dedupKey := fmt.Sprintf("ack:kafka:dedup:%s", event.Header.EventId)
    set, err := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result()
    if err != nil {
        return fmt.Errorf("redis 幂等检查失败: %w", err)
    }
    if !set {
        log.Debug("重复事件，跳过", "event_id", event.Header.EventId)
        return nil
    }

    // ==================== 3. 参数校验 ====================
    if event.MsgId == "" || event.ChannelId == "" || event.SenderId == "" {
        log.Error("msg.stored.single: 关键字段缺失",
            "msg_id", event.MsgId, "channel_id", event.ChannelId, "sender_id", event.SenderId)
        return nil
    }

    // 单聊消息需要获取接收者 ID
    receiverID := event.ReceiverId
    if receiverID == "" {
        log.Error("msg.stored.single: receiver_id 为空", "msg_id", event.MsgId)
        return nil
    }

    // ==================== 4. 初始化投递追踪 ====================
    // 在 Redis 中标记该消息处于"待投递"状态
    // Push 服务成功推送后会调用 ConfirmDelivered 确认
    // 这里仅做初始化，不主动标记已投递
    lockKey := fmt.Sprintf("ack:auto_deliver_lock:%s:%s", event.MsgId, receiverID)
    locked, err := c.redis.SetNX(ctx, lockKey, "1", 10*time.Minute).Result()
    if err != nil {
        log.Warn("设置自动投递锁失败", "msg_id", event.MsgId, "err", err)
    }
    if !locked {
        // 已有其他实例在处理，跳过
        return nil
    }

    // ==================== 5. 检查接收方是否在线 — 通过 Presence 服务 RPC 查询 ====================
    // 注：不直接读取 Presence 服务的 Redis key（presence:online:*）
    //     跨服务 Redis 直连违反服务边界，Presence 服务变更 key 格式时 Ack 会静默失败
    isOnline := false
    presenceResp, err := c.presenceClient.GetPresence(ctx, &presence_pb.GetPresenceRequest{
        UserId: receiverID,
    })
    if err == nil && presenceResp.Presence != nil {
        isOnline = presenceResp.Presence.OnlineStatus != common.ONLINE_STATUS_OFFLINE
    } else if err != nil {
        log.Warn("检查接收方在线状态失败", "user_id", receiverID, "err", err)
    }

    if isOnline {
        log.Debug("接收方在线，等待 Push 服务投递确认",
            "msg_id", event.MsgId, "receiver_id", receiverID)
    } else {
        log.Debug("接收方离线，消息进入离线队列等待",
            "msg_id", event.MsgId, "receiver_id", receiverID)
    }

    // ==================== 6. 记录投递追踪日志 ====================
    // 用于后续排查消息投递链路问题
    log.Info("单聊消息投递追踪已初始化",
        "msg_id", event.MsgId,
        "sender_id", event.SenderId,
        "receiver_id", receiverID,
        "channel_id", event.ChannelId,
        "is_receiver_online", isOnline > 0,
    )

    return nil
}
```

### Consumer: `msg.stored.group`

> 来源：Message 服务。群聊消息存储成功后触发。  
> 职责：初始化群消息的已读回执追踪结构（Redis SET + 计数），为后续 SendGroupReadAck 做准备。  
> 场景：群成员发送消息 → Message 存储成功 → Ack 消费后初始化该消息的群已读追踪 → 群成员逐一上报已读时更新。

```go
func (c *AckConsumer) HandleMsgStoredGroup(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_msg.MsgStoredEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 MsgStoredEvent（群聊）失败", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 — Redis ====================
    dedupKey := fmt.Sprintf("ack:kafka:dedup:%s", event.Header.EventId)
    set, err := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result()
    if err != nil {
        return fmt.Errorf("redis 幂等检查失败: %w", err)
    }
    if !set {
        log.Debug("重复事件，跳过", "event_id", event.Header.EventId)
        return nil
    }

    // ==================== 3. 参数校验 ====================
    if event.MsgId == "" || event.ChannelId == "" {
        log.Error("msg.stored.group: 关键字段缺失",
            "msg_id", event.MsgId, "channel_id", event.ChannelId)
        return nil
    }

    groupID := event.GroupId
    if groupID == "" {
        log.Error("msg.stored.group: group_id 为空", "msg_id", event.MsgId)
        return nil
    }

    // ==================== 4. 检查群已读回执功能是否开启 ====================
    groupReadReceiptEnabled, _ := c.config.Load("group_read_receipt_enabled")
    if enabled, ok := groupReadReceiptEnabled.(bool); ok && !enabled {
        log.Debug("群已读回执功能已关闭，跳过初始化", "group_id", groupID)
        return nil
    }

    // 检查群人数是否超过已读回执最大限制
    readReceiptMaxGroupSize, _ := c.config.Load("read_receipt_max_group_size")
    maxSize := 500 // 默认值
    if v, ok := readReceiptMaxGroupSize.(int); ok && v > 0 {
        maxSize = v
    }

    // 获取群成员数（优先从事件中获取，否则通过 Group 服务 RPC 查询）
    // 注：不直接读取 Group 服务的 Redis key（group:member_count:*）
    //     跨服务 Redis 直连违反服务边界原则
    memberCount := int(event.GroupMemberCount)
    if memberCount <= 0 {
        // 事件中未携带，通过 Group RPC 查询
        groupResp, err := c.groupClient.GetGroup(ctx, &group_pb.GetGroupRequest{GroupId: groupID})
        if err == nil && groupResp.Group != nil {
            memberCount = int(groupResp.Group.MemberCount)
        }
    }

    if memberCount > maxSize {
        log.Debug("群人数超过已读回执限制，跳过初始化",
            "group_id", groupID, "member_count", memberCount, "max_size", maxSize)
        return nil
    }

    // ==================== 5. 初始化群消息已读追踪结构 — Redis ====================
    // 5a. 初始化已读用户集合（空 SET）
    readUsersKey := fmt.Sprintf("ack:group_read_users:%s", event.MsgId)
    // 发送者本人自动标记为已读
    pipe := c.redis.Pipeline()
    pipe.SAdd(ctx, readUsersKey, event.SenderId)
    pipe.Expire(ctx, readUsersKey, 7*24*time.Hour)

    // 5b. 初始化已读计数（发送者=1）
    readCountKey := fmt.Sprintf("ack:group_read_count:%s", event.MsgId)
    pipe.Set(ctx, readCountKey, 1, 7*24*time.Hour)

    if _, err := pipe.Exec(ctx); err != nil {
        log.Error("初始化群消息已读追踪失败",
            "msg_id", event.MsgId, "group_id", groupID, "err", err)
        return fmt.Errorf("redis 初始化群已读追踪失败: %w", err)
    }

    // ==================== 6. 写入 PgSQL — 发送者自动已读 ====================
    now := time.Now().UnixMilli()
    _, err = c.db.ExecContext(ctx,
        `INSERT INTO group_msg_reads (msg_id, channel_id, user_id, read_time)
         VALUES ($1, $2, $3, $4)
         ON CONFLICT (msg_id, user_id) DO NOTHING`,
        event.MsgId, event.ChannelId, event.SenderId, now,
    )
    if err != nil {
        log.Error("写入发送者自动已读记录失败",
            "msg_id", event.MsgId, "sender_id", event.SenderId, "err", err)
        // 不阻塞主流程
    }

    log.Info("群消息已读追踪已初始化",
        "msg_id", event.MsgId,
        "group_id", groupID,
        "channel_id", event.ChannelId,
        "sender_id", event.SenderId,
        "member_count", memberCount,
    )

    return nil
}
```

### Consumer: `msg.recalled`

> 来源：Message 服务。消息撤回成功后触发。  
> 职责：清除被撤回消息的已读回执追踪数据（Redis SET + 计数 + PgSQL 群已读记录）。  
> 撤回后消息不再需要追踪已读状态，及时清理释放资源。

```go
func (c *AckConsumer) HandleMsgRecalled(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_msg.MsgRecalledEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 MsgRecalledEvent 失败", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 — Redis ====================
    dedupKey := fmt.Sprintf("ack:kafka:dedup:%s", event.Header.EventId)
    set, err := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result()
    if err != nil {
        return fmt.Errorf("redis 幂等检查失败: %w", err)
    }
    if !set {
        log.Debug("重复事件，跳过", "event_id", event.Header.EventId)
        return nil
    }

    // ==================== 3. 参数校验 ====================
    if event.MsgId == "" {
        log.Error("msg.recalled: msg_id 为空", "event_id", event.Header.EventId)
        return nil
    }

    // ==================== 4. 延迟双删：先删 Redis 缓存，再删 DB，延迟再删 Redis ====================
    readUsersKey := fmt.Sprintf("ack:group_read_users:%s", event.MsgId)
    readCountKey := fmt.Sprintf("ack:group_read_count:%s", event.MsgId)
    keysToDelete := []string{readUsersKey, readCountKey}

    // 扫描投递确认标记 key
    deliveredPattern := fmt.Sprintf("ack:delivered:%s:*", event.MsgId)
    var cursor uint64
    for {
        keys, nextCursor, err := c.redis.Scan(ctx, cursor, deliveredPattern, 100).Result()
        if err != nil {
            log.Warn("扫描投递确认 key 失败", "pattern", deliveredPattern, "err", err)
            break
        }
        keysToDelete = append(keysToDelete, keys...)
        cursor = nextCursor
        if cursor == 0 {
            break
        }
    }

    // Phase 1 删除 + 延迟 Phase 2 删除（在 DB 写入前执行）
    c.delayedDoubleDelete(ctx, keysToDelete...)

    // ==================== 5. 清除 PgSQL 中的群消息已读记录 ====================
    result, err := c.db.ExecContext(ctx,
        `DELETE FROM group_msg_reads WHERE msg_id = $1`,
        event.MsgId,
    )
    if err != nil {
        log.Error("删除 group_msg_reads 失败", "msg_id", event.MsgId, "err", err)
        return fmt.Errorf("删除 group_msg_reads 失败: %w", err)
    }
    rowsDeleted, _ := result.RowsAffected()

    // ==================== 6. 清除投递记录 ====================
    _, err = c.db.ExecContext(ctx,
        `DELETE FROM delivery_records WHERE msg_id = $1`,
        event.MsgId,
    )
    if err != nil {
        log.Error("删除 delivery_records 失败", "msg_id", event.MsgId, "err", err)
        // 不阻塞主流程
    }

    log.Info("撤回消息已读追踪数据已清除",
        "msg_id", event.MsgId,
        "group_reads_deleted", rowsDeleted,
    )

    return nil
}
```

### Consumer: `user.deactivated`

> 来源：User 服务。用户注销/停用后触发。  
> 职责：清理该用户的所有已读回执数据（read_receipts、group_msg_reads、delivery_records）和相关 Redis 缓存。  
> 数据量可能很大，采用分批删除 + 进度标记防止超时。

```go
func (c *AckConsumer) HandleUserDeactivated(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_user.UserDeactivatedEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 UserDeactivatedEvent 失败", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 — Redis ====================
    dedupKey := fmt.Sprintf("ack:kafka:dedup:%s", event.Header.EventId)
    set, err := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result()
    if err != nil {
        return fmt.Errorf("redis 幂等检查失败: %w", err)
    }
    if !set {
        log.Debug("重复事件，跳过", "event_id", event.Header.EventId)
        return nil
    }

    // ==================== 3. 参数校验 ====================
    if event.UserId == "" {
        log.Error("user.deactivated: user_id 为空", "event_id", event.Header.EventId)
        return nil
    }

    // ==================== 4. 设置清理进度标记 — Redis ====================
    progressKey := fmt.Sprintf("ack:cleanup_progress:%s", event.UserId)
    c.redis.Set(ctx, progressKey, "processing", 1*time.Hour)

    // ==================== 5. 分批清理 read_receipts — PgSQL ====================
    // 先获取用户的所有会话 ID，用于后续清理 Redis 缓存
    rows, err := c.db.QueryContext(ctx,
        `SELECT channel_id FROM read_receipts WHERE user_id = $1`,
        event.UserId,
    )
    if err != nil {
        log.Error("查询用户 read_receipts 失败", "user_id", event.UserId, "err", err)
        return fmt.Errorf("查询 read_receipts 失败: %w", err)
    }
    var channelIDs []string
    for rows.Next() {
        var cid string
        if err := rows.Scan(&cid); err != nil {
            continue
        }
        channelIDs = append(channelIDs, cid)
    }
    rows.Close()

    // ==================== 5a. 延迟双删：先删 Redis 缓存，再执行 DB 删除 ====================
    // 收集所有需要删除的 Redis key
    keysToDelete := make([]string, 0, len(channelIDs))
    for _, channelID := range channelIDs {
        keysToDelete = append(keysToDelete, fmt.Sprintf("ack:read_seq:%s:%s", event.UserId, channelID))
    }

    // 扫描用户的投递确认缓存 key
    var delCursor uint64
    deliveredPattern := fmt.Sprintf("ack:delivered:*:%s", event.UserId)
    for {
        keys, nextCursor, err := c.redis.Scan(ctx, delCursor, deliveredPattern, 200).Result()
        if err != nil {
            log.Warn("扫描用户投递确认 key 失败", "user_id", event.UserId, "err", err)
            break
        }
        keysToDelete = append(keysToDelete, keys...)
        delCursor = nextCursor
        if delCursor == 0 {
            break
        }
    }

    // Phase 1 删除 + 延迟 Phase 2 删除（在 DB 写入前执行）
    c.delayedDoubleDelete(ctx, keysToDelete...)

    // 删除 read_receipts 记录
    result, err := c.db.ExecContext(ctx,
        `DELETE FROM read_receipts WHERE user_id = $1`,
        event.UserId,
    )
    if err != nil {
        log.Error("删除 read_receipts 失败", "user_id", event.UserId, "err", err)
        return fmt.Errorf("删除 read_receipts 失败: %w", err)
    }
    readReceiptsDeleted, _ := result.RowsAffected()

    // ==================== 6. 分批清理 group_msg_reads — PgSQL ====================
    // 分批删除，每批 1000 条，避免长事务锁表
    totalGroupReadsDeleted := int64(0)
    for {
        result, err := c.db.ExecContext(ctx,
            `DELETE FROM group_msg_reads WHERE id IN (
                SELECT id FROM group_msg_reads WHERE user_id = $1 LIMIT 1000
            )`,
            event.UserId,
        )
        if err != nil {
            log.Error("分批删除 group_msg_reads 失败", "user_id", event.UserId, "err", err)
            break
        }
        affected, _ := result.RowsAffected()
        totalGroupReadsDeleted += affected
        if affected < 1000 {
            break // 最后一批
        }
    }

    // ==================== 7. 分批清理 delivery_records — PgSQL ====================
    totalDeliveryDeleted := int64(0)
    for {
        result, err := c.db.ExecContext(ctx,
            `DELETE FROM delivery_records WHERE id IN (
                SELECT id FROM delivery_records WHERE user_id = $1 LIMIT 1000
            )`,
            event.UserId,
        )
        if err != nil {
            log.Error("分批删除 delivery_records 失败", "user_id", event.UserId, "err", err)
            break
        }
        affected, _ := result.RowsAffected()
        totalDeliveryDeleted += affected
        if affected < 1000 {
            break
        }
    }

    // ==================== 8. 更新群消息已读计数 ====================
    // 用户注销后，需要更新该用户参与的所有群消息的已读计数
    // 由于群消息可能很多，这里通过扫描 group_msg_reads 中已删除的 msg_id 来更新
    // 注意：步骤 6 已经删除了 group_msg_reads 记录，这里从内存中获取已删除的 msg_ids
    // 实际生产中建议：在删除前先查询需要更新的 msg_id 列表
    // 由于性能考虑，群已读计数的轻微不一致是可以接受的（TTL 7 天后自动清理）
    // 在此标记进度为完成，不做逐条更新

    // ==================== 10. 更新清理进度 ====================
    c.redis.Set(ctx, progressKey, "completed", 1*time.Hour)

    log.Info("用户注销后 Ack 数据清理完成",
        "user_id", event.UserId,
        "read_receipts_deleted", readReceiptsDeleted,
        "group_reads_deleted", totalGroupReadsDeleted,
        "delivery_records_deleted", totalDeliveryDeleted,
        "channels_cleaned", len(channelIDs),
    )

    return nil
}
```

### Consumer: `config.changed`

> 来源：Config 服务。全局配置变更，Ack 服务热更新群已读回执开关、群人数限制等运行时配置。

```go
func (c *AckConsumer) HandleConfigChanged(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_config.ConfigChangedEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 ConfigChangedEvent 失败", "err", err)
        return nil
    }

    // ==================== 2. 过滤非本服务的配置 ====================
    affectsAck := false
    for _, svc := range event.AffectedServices {
        if svc == "ack" {
            affectsAck = true
            break
        }
    }
    if !affectsAck {
        return nil // 不影响 Ack 服务，跳过
    }

    // ==================== 3. 根据 config_key 更新本地配置 ====================
    switch event.ConfigKey {
    case "ack.group_read_receipt_enabled":
        // 群已读回执功能总开关
        val, err := strconv.ParseBool(event.NewValue)
        if err != nil {
            log.Error("解析 group_read_receipt_enabled 配置失败",
                "value", event.NewValue, "err", err)
            return nil
        }
        c.config.Store("group_read_receipt_enabled", val)
        log.Info("群已读回执功能开关已更新", "enabled", val)

    case "ack.read_receipt_max_group_size":
        // 支持群已读回执的最大群人数
        val, err := strconv.Atoi(event.NewValue)
        if err != nil || val <= 0 {
            log.Error("解析 read_receipt_max_group_size 配置失败",
                "value", event.NewValue, "err", err)
            return nil
        }
        c.config.Store("read_receipt_max_group_size", val)
        log.Info("群已读回执最大群人数限制已更新", "max_size", val)

    case "ack.read_seq_cache_ttl_hours":
        // read_seq 缓存 TTL（小时）
        val, err := strconv.Atoi(event.NewValue)
        if err != nil || val <= 0 {
            log.Error("解析 read_seq_cache_ttl_hours 配置失败",
                "value", event.NewValue, "err", err)
            return nil
        }
        c.config.Store("read_seq_cache_ttl_hours", val)
        log.Info("read_seq 缓存 TTL 已更新", "hours", val)

    case "ack.group_read_data_ttl_days":
        // 群消息已读数据 TTL（天）
        val, err := strconv.Atoi(event.NewValue)
        if err != nil || val <= 0 {
            log.Error("解析 group_read_data_ttl_days 配置失败",
                "value", event.NewValue, "err", err)
            return nil
        }
        c.config.Store("group_read_data_ttl_days", val)
        log.Info("群消息已读数据 TTL 已更新", "days", val)

    case "ack.delivery_record_ttl_days":
        // 投递记录 TTL（天）
        val, err := strconv.Atoi(event.NewValue)
        if err != nil || val <= 0 {
            log.Error("解析 delivery_record_ttl_days 配置失败",
                "value", event.NewValue, "err", err)
            return nil
        }
        c.config.Store("delivery_record_ttl_days", val)
        log.Info("投递记录 TTL 已更新", "days", val)

    case "ack.batch_max_conversations":
        // 批量操作最大会话数
        val, err := strconv.Atoi(event.NewValue)
        if err != nil || val <= 0 {
            log.Error("解析 batch_max_conversations 配置失败",
                "value", event.NewValue, "err", err)
            return nil
        }
        c.config.Store("batch_max_conversations", val)
        log.Info("批量操作最大会话数已更新", "max", val)

    default:
        log.Debug("未知的 Ack 服务配置项", "key", event.ConfigKey)
    }

    return nil
}
```

### Consumer: `push.delivered`

> 来源：Push 服务。客户端确认收到 sync notify 后，Push 服务投递此事件。  
> 职责：更新消息的送达状态（delivery_records），标记该消息已成功投递到客户端。  
> 这是 ACK 链路的关键一环：msg.stored → push → client ACK → push.delivered → Ack 服务更新送达状态。

```go
func (c *AckConsumer) HandlePushDelivered(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_push.PushDeliveredEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 PushDeliveredEvent 失败", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 — Redis ====================
    dedupKey := fmt.Sprintf("ack:kafka:dedup:%s", event.Header.EventId)
    set, err := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result()
    if err != nil {
        return fmt.Errorf("redis 幂等检查失败: %w", err)
    }
    if !set {
        return nil
    }

    // ==================== 3. 参数校验 ====================
    if event.MsgId == "" || event.UserId == "" {
        log.Error("push.delivered: msg_id 或 user_id 为空")
        return nil
    }

    // ==================== 4. 更新投递记录 — PgSQL ====================
    _, err = c.db.ExecContext(ctx,
        `INSERT INTO delivery_records (msg_id, user_id, device_id, platform, delivered_at)
         VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT (msg_id, user_id, device_id) DO UPDATE SET delivered_at = $5`,
        event.MsgId, event.UserId, event.DeviceId, int(event.Platform), time.UnixMilli(event.DeliverTime),
    )
    if err != nil {
        log.Error("写入投递记录失败",
            "msg_id", event.MsgId, "user_id", event.UserId, "err", err)
        return fmt.Errorf("写入投递记录失败: %w", err)
    }

    // ==================== 5. 更新 Redis 送达缓存 ====================
    deliveredKey := fmt.Sprintf("ack:delivered:%s:%s", event.MsgId, event.UserId)
    c.redis.Set(ctx, deliveredKey, fmt.Sprintf("%d", event.DeliverTime), 7*24*time.Hour)

    // ==================== 6. 生产 ack.delivered 事件 ====================
    ackEvent := &kafka_ack.AckDeliveredEvent{
        Header:      buildEventHeader("ack", c.instanceID),
        MsgId:       event.MsgId,
        UserId:      event.UserId,
        DeviceId:    event.DeviceId,
        Platform:    event.Platform,
        DeliverTime: event.DeliverTime,
    }
    if err := c.kafka.Produce(ctx, "ack.delivered", event.MsgId, ackEvent); err != nil {
        log.Error("投递 ack.delivered 事件失败",
            "msg_id", event.MsgId, "user_id", event.UserId, "err", err)
    }

    log.Debug("消息送达确认已处理",
        "msg_id", event.MsgId, "user_id", event.UserId,
        "device_id", event.DeviceId, "deliver_time", event.DeliverTime)
    return nil
}
```

```go
// RunStatsTask 每 5 分钟执行一次统计并生产 ack.stats 事件
func (c *AckConsumer) RunStatsTask(ctx context.Context) {
    ticker := time.NewTicker(5 * time.Minute)
    defer ticker.Stop()

    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            now := time.Now().UnixMilli()
            periodStart := now - 5*60*1000 // 5 分钟前

            // 统计周期内的已读确认数
            var totalRead int64
            c.db.QueryRowContext(ctx,
                `SELECT COUNT(*) FROM read_receipts WHERE updated_at >= $1`,
                periodStart,
            ).Scan(&totalRead)

            // 统计周期内的群已读数
            var totalGroupRead int64
            c.db.QueryRowContext(ctx,
                `SELECT COUNT(*) FROM group_msg_reads WHERE read_time >= $1`,
                periodStart,
            ).Scan(&totalGroupRead)

            // 统计周期内的投递确认数
            var totalDelivered int64
            c.db.QueryRowContext(ctx,
                `SELECT COUNT(*) FROM delivery_records WHERE delivered_at >= $1`,
                periodStart,
            ).Scan(&totalDelivered)

            // 计算平均投递延迟（从消息发送到投递确认）
            // 注：delivery_records 表在 ConfirmDelivered 写入时已记录 msg_server_time（消息原始发送时间）
            //     直接在本服务表内计算延迟，不跨服务 JOIN messages 表
            var avgDelay int64
            c.db.QueryRowContext(ctx,
                `SELECT COALESCE(AVG(delivered_at - msg_server_time), 0)
                 FROM delivery_records
                 WHERE delivered_at >= $1 AND msg_server_time > 0`,
                periodStart,
            ).Scan(&avgDelay)

            // 生产统计事件
            statsEvent := &kafka_ack.AckStatsEvent{
                Header:             buildEventHeader("ack", c.instanceID),
                TotalReceived:      totalDelivered,
                TotalRead:          totalRead,
                TotalGroupRead:     totalGroupRead,
                AvgReceiveDelayMs:  avgDelay,
                StatsTime:          now,
                PeriodSeconds:      300,
            }
            if err := c.kafka.Produce(ctx, "ack.stats", "stats", statsEvent); err != nil {
                log.Error("生产 ack.stats 事件失败", "err", err)
            }

            log.Info("Ack 统计任务完成",
                "total_read", totalRead,
                "total_group_read", totalGroupRead,
                "total_delivered", totalDelivered,
                "avg_delay_ms", avgDelay,
            )
        }
    }
}
```

## 定时任务：过期数据清理

```go
// RunCleanupTask 每天凌晨 3:00 执行，清理过期的群已读数据和投递记录
func (c *AckConsumer) RunCleanupTask(ctx context.Context) {
    for {
        // 计算下次凌晨 3:00
        now := time.Now()
        next := time.Date(now.Year(), now.Month(), now.Day()+1, 3, 0, 0, 0, now.Location())
        timer := time.NewTimer(next.Sub(now))

        select {
        case <-ctx.Done():
            timer.Stop()
            return
        case <-timer.C:
            c.doCleanup(ctx)
        }
    }
}

func (c *AckConsumer) doCleanup(ctx context.Context) {
    // 从配置获取 TTL
    groupReadTTLDays := 7 // 默认 7 天
    if v, ok := c.config.Load("group_read_data_ttl_days"); ok {
        if days, ok := v.(int); ok && days > 0 {
            groupReadTTLDays = days
        }
    }

    deliveryTTLDays := 7 // 默认 7 天
    if v, ok := c.config.Load("delivery_record_ttl_days"); ok {
        if days, ok := v.(int); ok && days > 0 {
            deliveryTTLDays = days
        }
    }

    now := time.Now().UnixMilli()

    // ==================== 1. 清理过期群消息已读记录 ====================
    groupReadCutoff := now - int64(groupReadTTLDays)*24*3600*1000
    totalGroupCleaned := int64(0)
    for {
        result, err := c.db.ExecContext(ctx,
            `DELETE FROM group_msg_reads WHERE id IN (
                SELECT id FROM group_msg_reads WHERE read_time < $1 LIMIT 5000
            )`,
            groupReadCutoff,
        )
        if err != nil {
            log.Error("清理过期 group_msg_reads 失败", "err", err)
            break
        }
        affected, _ := result.RowsAffected()
        totalGroupCleaned += affected
        if affected < 5000 {
            break
        }
        // 每批之间休眠 100ms，避免对 DB 造成过大压力
        time.Sleep(100 * time.Millisecond)
    }

    // ==================== 2. 清理过期投递记录 ====================
    deliveryCutoff := now - int64(deliveryTTLDays)*24*3600*1000
    totalDeliveryCleaned := int64(0)
    for {
        result, err := c.db.ExecContext(ctx,
            `DELETE FROM delivery_records WHERE id IN (
                SELECT id FROM delivery_records WHERE delivered_at < $1 LIMIT 5000
            )`,
            deliveryCutoff,
        )
        if err != nil {
            log.Error("清理过期 delivery_records 失败", "err", err)
            break
        }
        affected, _ := result.RowsAffected()
        totalDeliveryCleaned += affected
        if affected < 5000 {
            break
        }
        time.Sleep(100 * time.Millisecond)
    }

    log.Info("Ack 过期数据清理完成",
        "group_reads_cleaned", totalGroupCleaned,
        "delivery_records_cleaned", totalDeliveryCleaned,
        "group_read_ttl_days", groupReadTTLDays,
        "delivery_ttl_days", deliveryTTLDays,
    )
}

// delayedDoubleDelete 延迟双删：先删除 Redis 缓存，500ms 后再删一次防止并发回填
func (c *AckConsumer) delayedDoubleDelete(ctx context.Context, keys ...string) {
    if len(keys) == 0 { return }
    pipe := c.redis.Pipeline()
    for _, key := range keys { pipe.Del(ctx, key) }
    pipe.Exec(ctx)
    go func() {
        time.Sleep(500 * time.Millisecond)
        pipe := c.redis.Pipeline()
        for _, key := range keys { pipe.Del(context.Background(), key) }
        pipe.Exec(context.Background())
    }()
}
```
