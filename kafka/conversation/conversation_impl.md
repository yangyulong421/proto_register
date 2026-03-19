# Conversation 会话服务 — Kafka 消费者实现伪代码

## 概述

Conversation 服务通过 Kafka 消费来自 Message、Ack、Group、Relation、User、Config 等服务的事件，维护会话通道的最后消息信息、用户会话列表的未读数、可见性等状态。这是整个 IM 系统中消息流转到用户可见会话列表的核心链路。

## 生产 Topic 列表

| Topic | 事件 | 生产时机 | 消费方 |
|-------|------|----------|--------|
| `conversation.created` | ConversationCreatedEvent | 创建用户会话 | Notification, Push |
| `conversation.updated` | ConversationUpdatedEvent | 会话更新（新消息/属性变更） | Notification, Push |
| `conversation.deleted` | ConversationDeletedEvent | 会话删除 | Notification, Push |
| `conversation.top.changed` | ConversationTopChangedEvent | 置顶状态变更 | Notification, Push |
| `conversation.mute.changed` | ConversationMuteChangedEvent | 免打扰变更 | Notification, Push |
| `conversation.unread.reset` | ConversationUnreadResetEvent | 未读数重置 | Notification, Push |
| `conversation.unread.changed` | ConversationUnreadResetEvent | 未读数变更 | Notification, Push |

## 消费 Topic 列表

| Topic | 来源 | 用途 |
|-------|------|------|
| `msg.stored.single` | Message | 单聊消息存储完成 → 更新通道 last_msg，为接收方递增未读 |
| `msg.stored.group` | Message | 群聊消息存储完成 → 更新通道 last_msg，为所有群成员（除发送者）递增未读 |
| `msg.recalled` | Message | 消息撤回 → 若是最后一条消息则更新 last_msg_preview |
| `relation.friend.deleted` | Relation | 好友删除 → 隐藏双方会话 |
| `group.dissolved` | Group | 群解散 → 删除通道 + 所有群成员的用户会话 + 清理本地群成员缓存 |
| `group.member.joined` | Group | 新成员入群 → 创建该成员的用户会话 + 维护本地群成员缓存 |
| `group.member.left` | Group | 成员退群 → 删除该成员的用户会话 + 维护本地群成员缓存 |
| `group.member.kicked` | Group | 成员被踢 → 删除被踢成员的用户会话 + 维护本地群成员缓存 |
| `ack.read.updated` | Ack | 已读回执 → 更新 read_seq，重算未读数 |
| `user.deactivated` | User | 用户注销 → 清理所有用户会话 |
| `config.changed` | Config | 配置变更 → 热更新运行时参数 |

## Redis Key 设计（消费者侧）

| Key 格式 | 类型 | 值 | TTL | 说明 |
|----------|------|-----|-----|------|
| `conv:kafka:dedup:{event_id}` | STRING | "1" | 24h | Kafka 消费幂等去重 |
| `conv:channel:{channel_id}` | HASH | 通道信息 | 1h | 通道缓存 |
| `conv:channel:pair:{a}:{b}` | STRING | channel_id | 1h | 单聊映射 |
| `conv:channel:group:{group_id}` | STRING | channel_id | 1h | 群聊映射 |
| `conv:max_seq:{channel_id}` | STRING | max_seq | 无 TTL | 序号计数器 |
| `conv:user_list:{user_id}` | ZSET | member=channel_id, score=last_msg_time | 30min | 用户会话列表 |
| `conv:unread:{user_id}:{channel_id}` | STRING | unread_count | 24h | 单会话未读数 |
| `conv:total_unread:{user_id}` | STRING | total | 5min | 总未读数 |
| `conv:user_conv:{user_id}:{channel_id}` | HASH | 会话详情 | 30min | 用户会话缓存 |
| `conv:local:group_members:{group_id}` | SET | 群成员 user_id 集合 | 30min | 本地群成员缓存，由 group.member.* 事件维护，避免跨服务查询 Group PgSQL |

---

## 消费者实现

### Consumer: `msg.stored.single`

> 来源：Message 服务。单聊消息持久化完成后，Conversation 需要：  
> 1. 更新通道的最后一条消息信息  
> 2. 为接收方递增未读数  
> 3. 确保双方的用户会话存在且可见  
> 4. 更新用户会话列表排序  

```go
func (c *ConversationConsumer) HandleMsgStoredSingle(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_message.MsgStoredSingleEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 MsgStoredSingleEvent 失败", "err", err)
        return nil // 反序列化失败不重试
    }

    // ==================== 2. 幂等检查 — Redis ====================
    dedupKey := fmt.Sprintf("conv:kafka:dedup:%s", event.Header.EventId)
    set, err := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result()
    if err != nil {
        return fmt.Errorf("redis 幂等检查失败: %w", err)
    }
    if !set {
        log.Info("重复事件，跳过", "event_id", event.Header.EventId)
        return nil
    }

    senderID := event.SenderId
    receiverID := event.ReceiverId
    msgID := event.MsgId
    msgSeq := event.Seq
    msgTime := event.MsgTime
    msgPreview := event.Preview
    conversationID := event.ConversationId

    now := time.Now().UnixMilli()

    // ==================== 3. 获取或确认通道存在 ====================
    // 单聊通道：按 min/max 排序的用户对
    pA, pB := sortPair(senderID, receiverID)

    if conversationID == "" {
        // 尝试从 Redis 查找通道 ID
        pairKey := fmt.Sprintf("conv:channel:pair:%s:%s", pA, pB)
        conversationID, _ = c.redis.Get(ctx, pairKey).Result()
    }

    if conversationID == "" {
        // Redis 未命中 → 查 PgSQL
        err := c.db.QueryRowContext(ctx,
            `SELECT channel_id FROM conversation_channels
             WHERE channel_type = 1 AND participant_a = $1 AND participant_b = $2 AND status = 1`,
            pA, pB,
        ).Scan(&conversationID)
        if err == sql.ErrNoRows {
            // 通道不存在 → 创建（理论上发消息前应该已创建，但容错处理）
            conversationID = c.snowflake.Generate().String()
            _, err = c.db.ExecContext(ctx,
                `INSERT INTO conversation_channels
                 (channel_id, channel_type, participant_a, participant_b, max_seq, status, created_at, updated_at)
                 VALUES ($1, 1, $2, $3, 0, 1, $4, $5)
                 ON CONFLICT DO NOTHING`,
                conversationID, pA, pB, now, now,
            )
            if err != nil {
                return fmt.Errorf("创建单聊通道失败: %w", err)
            }
            // ON CONFLICT 后需要重新查一次确认实际 channel_id
            c.db.QueryRowContext(ctx,
                `SELECT channel_id FROM conversation_channels
                 WHERE channel_type = 1 AND participant_a = $1 AND participant_b = $2 AND status = 1`,
                pA, pB,
            ).Scan(&conversationID)

            // 初始化 Redis seq 计数器
            seqKey := fmt.Sprintf("conv:max_seq:%s", conversationID)
            c.redis.SetNX(ctx, seqKey, "0", 0)
        } else if err != nil {
            return fmt.Errorf("查询单聊通道失败: %w", err)
        }
    }

    // ==================== 4. 更新通道 last_msg 信息 — PgSQL ====================
    _, err = c.db.ExecContext(ctx,
        `UPDATE conversation_channels
         SET last_msg_id = $1, last_msg_seq = $2, last_msg_time = $3,
             last_sender_id = $4, last_msg_preview = $5, updated_at = $6
         WHERE channel_id = $7 AND status = 1 AND last_msg_time <= $8`,
        msgID, msgSeq, msgTime, senderID, msgPreview, now,
        conversationID, msgTime,
    )
    if err != nil {
        log.Error("更新通道 last_msg 失败", "channel_id", conversationID, "err", err)
        // 不返回错误，继续处理用户会话
    }

    // ==================== 5. 清除通道缓存（延迟双删） ====================
    // 回写映射缓存
    c.redis.Set(ctx, fmt.Sprintf("conv:channel:pair:%s:%s", pA, pB), conversationID, 1*time.Hour)
    // 延迟双删通道缓存
    delayedDoubleDelete(ctx, c.redis, 500*time.Millisecond,
        fmt.Sprintf("conv:channel:%s", conversationID),
    )

    // ==================== 6. 确保双方用户会话存在且可见 ====================
    for _, userID := range []string{senderID, receiverID} {
        _, err = c.db.ExecContext(ctx,
            `INSERT INTO user_conversations
             (user_id, channel_id, is_top, is_muted, is_show, unread_count, read_seq,
              clear_seq, draft, last_ack_time, extra, created_at, updated_at)
             VALUES ($1, $2, false, false, true, 0, 0, 0, '', 0, '{}', $3, $4)
             ON CONFLICT (user_id, channel_id) DO UPDATE
             SET is_show = true, updated_at = $5`,
            userID, conversationID, now, now, now,
        )
        if err != nil {
            log.Error("确保用户会话存在失败", "user_id", userID, "channel_id", conversationID, "err", err)
        }
    }

    // ==================== 7. 为接收方递增未读数 ====================
    // 7a. PgSQL 递增
    _, err = c.db.ExecContext(ctx,
        `UPDATE user_conversations
         SET unread_count = unread_count + 1, updated_at = $1
         WHERE user_id = $2 AND channel_id = $3`,
        now, receiverID, conversationID,
    )
    if err != nil {
        log.Error("递增接收方未读数失败", "user_id", receiverID, "err", err)
    }

    // 7b. Redis INCR 原子递增未读
    unreadKey := fmt.Sprintf("conv:unread:%s:%s", receiverID, conversationID)
    c.redis.Incr(ctx, unreadKey)
    c.redis.Expire(ctx, unreadKey, 24*time.Hour)

    // 7c. 使接收方总未读数缓存失效（延迟双删）
    delayedDoubleDelete(ctx, c.redis, 500*time.Millisecond,
        fmt.Sprintf("conv:total_unread:%s", receiverID),
    )

    // ==================== 8. 更新双方用户会话列表排序（Redis ZSET） ====================
    for _, userID := range []string{senderID, receiverID} {
        listKey := fmt.Sprintf("conv:user_list:%s", userID)
        c.redis.ZAdd(ctx, listKey, &redis.Z{
            Score:  float64(msgTime),
            Member: conversationID,
        })
        // 清除用户会话详情缓存（last_msg 已更新，延迟双删）
        delayedDoubleDelete(ctx, c.redis, 500*time.Millisecond,
            fmt.Sprintf("conv:user_conv:%s:%s", userID, conversationID),
        )
    }

    // ==================== 9. 发送 Kafka 事件 ====================
    // 9a. 发送方：conversation.updated（无未读变化）
    senderEvent := &kafka_conversation.ConversationUpdatedEvent{
        Header:           buildEventHeader("conversation", c.instanceID),
        UserId:           senderID,
        ConversationId:   conversationID,
        ConversationType: common.CONVERSATION_TYPE_SINGLE,
        LastMsgId:        msgID,
        LastMsgSeq:       msgSeq,
        LastMsgPreview:   msgPreview,
        LastMsgSenderId:  senderID,
        LastMsgTime:      msgTime,
        UnreadCount:      0, // 发送方不增加未读
    }
    if err := c.kafka.Produce(ctx, "conversation.updated", senderID, senderEvent); err != nil {
        log.Error("发送 conversation.updated(sender) 失败", "err", err)
    }

    // 9b. 接收方：conversation.updated（含新未读数）
    // 查询当前最新未读数
    newUnread, _ := c.redis.Get(ctx, unreadKey).Int()
    receiverEvent := &kafka_conversation.ConversationUpdatedEvent{
        Header:           buildEventHeader("conversation", c.instanceID),
        UserId:           receiverID,
        ConversationId:   conversationID,
        ConversationType: common.CONVERSATION_TYPE_SINGLE,
        LastMsgId:        msgID,
        LastMsgSeq:       msgSeq,
        LastMsgPreview:   msgPreview,
        LastMsgSenderId:  senderID,
        LastMsgTime:      msgTime,
        UnreadCount:      int32(newUnread),
    }
    if err := c.kafka.Produce(ctx, "conversation.updated", receiverID, receiverEvent); err != nil {
        log.Error("发送 conversation.updated(receiver) 失败", "err", err)
    }

    log.Info("单聊消息会话更新完成",
        "conversation_id", conversationID,
        "sender", senderID,
        "receiver", receiverID,
        "msg_id", msgID,
        "msg_seq", msgSeq,
    )
    return nil
}
```

### Consumer: `msg.stored.group`

> 来源：Message 服务。群聊消息持久化完成后，需要更新通道 last_msg，并为所有群成员（除发送者）递增未读数。  
> **性能关键**：群成员可能有数百甚至数千人，需要批量操作。

```go
func (c *ConversationConsumer) HandleMsgStoredGroup(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_message.MsgStoredGroupEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 MsgStoredGroupEvent 失败", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 ====================
    dedupKey := fmt.Sprintf("conv:kafka:dedup:%s", event.Header.EventId)
    set, err := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result()
    if err != nil {
        return fmt.Errorf("redis 幂等检查失败: %w", err)
    }
    if !set {
        log.Info("重复事件，跳过", "event_id", event.Header.EventId)
        return nil
    }

    senderID := event.SenderId
    groupID := event.GroupId
    msgID := event.MsgId
    msgSeq := event.Seq
    msgTime := event.MsgTime
    msgPreview := event.Preview
    conversationID := event.ConversationId

    now := time.Now().UnixMilli()

    // ==================== 3. 获取通道 ID ====================
    if conversationID == "" {
        groupKey := fmt.Sprintf("conv:channel:group:%s", groupID)
        conversationID, _ = c.redis.Get(ctx, groupKey).Result()
    }

    if conversationID == "" {
        err := c.db.QueryRowContext(ctx,
            `SELECT channel_id FROM conversation_channels
             WHERE channel_type = 2 AND group_id = $1 AND status = 1`,
            groupID,
        ).Scan(&conversationID)
        if err == sql.ErrNoRows {
            // 容错：创建群通道
            conversationID = c.snowflake.Generate().String()
            _, err = c.db.ExecContext(ctx,
                `INSERT INTO conversation_channels
                 (channel_id, channel_type, group_id, max_seq, status, created_at, updated_at)
                 VALUES ($1, 2, $2, 0, 1, $3, $4)
                 ON CONFLICT DO NOTHING`,
                conversationID, groupID, now, now,
            )
            if err != nil {
                return fmt.Errorf("创建群聊通道失败: %w", err)
            }
            c.db.QueryRowContext(ctx,
                `SELECT channel_id FROM conversation_channels
                 WHERE channel_type = 2 AND group_id = $1 AND status = 1`,
                groupID,
            ).Scan(&conversationID)

            seqKey := fmt.Sprintf("conv:max_seq:%s", conversationID)
            c.redis.SetNX(ctx, seqKey, "0", 0)
        } else if err != nil {
            return fmt.Errorf("查询群聊通道失败: %w", err)
        }
    }

    // ==================== 4. 更新通道 last_msg — PgSQL ====================
    _, err = c.db.ExecContext(ctx,
        `UPDATE conversation_channels
         SET last_msg_id = $1, last_msg_seq = $2, last_msg_time = $3,
             last_sender_id = $4, last_msg_preview = $5, updated_at = $6
         WHERE channel_id = $7 AND status = 1 AND last_msg_time <= $8`,
        msgID, msgSeq, msgTime, senderID, msgPreview, now,
        conversationID, msgTime,
    )
    if err != nil {
        log.Error("更新群通道 last_msg 失败", "channel_id", conversationID, "err", err)
    }

    // 清除通道缓存（延迟双删），回写映射
    c.redis.Set(ctx, fmt.Sprintf("conv:channel:group:%s", groupID), conversationID, 1*time.Hour)
    delayedDoubleDelete(ctx, c.redis, 500*time.Millisecond,
        fmt.Sprintf("conv:channel:%s", conversationID),
    )

    // ==================== 5. 获取群成员列表（除发送者） ====================
    // 获取群成员列表 — 使用本地缓存，不直接访问 Group 服务的 PgSQL 表
    // 本地缓存由 group.member.joined / group.member.left / group.member.kicked 事件维护
    cacheKey := fmt.Sprintf("conv:local:group_members:%s", groupID)
    allMemberIDs, err := c.redis.SMembers(ctx, cacheKey).Result()
    if err != nil || len(allMemberIDs) == 0 {
        // 缓存未命中 → 降级 RPC Group.GetGroupMembers + 回填缓存
        resp, err := c.groupClient.GetGroupMembers(ctx, &group_pb.GetGroupMembersRequest{
            GroupId:  groupID,
            Page:     1,
            PageSize: 10000, // 全量拉取
        })
        if err != nil {
            return fmt.Errorf("get group members failed: %w", err)
        }
        allMemberIDs = make([]string, len(resp.Members))
        backfillPipe := c.redis.Pipeline()
        for i, m := range resp.Members {
            allMemberIDs[i] = m.UserId
            backfillPipe.SAdd(ctx, cacheKey, m.UserId)
        }
        backfillPipe.Expire(ctx, cacheKey, 30*time.Minute)
        backfillPipe.Exec(ctx)
    }

    // 排除发送者
    var memberIDs []string
    for _, uid := range allMemberIDs {
        if uid != senderID {
            memberIDs = append(memberIDs, uid)
        }
    }

    log.Info("群消息会话更新开始",
        "group_id", groupID,
        "conversation_id", conversationID,
        "member_count", len(memberIDs),
        "msg_id", msgID,
    )

    // ==================== 6. 批量确保所有成员的用户会话存在 + 递增未读 ====================
    // 分批处理，每批 100 人，避免单次 SQL 过大
    batchSize := 100
    for i := 0; i < len(memberIDs); i += batchSize {
        end := i + batchSize
        if end > len(memberIDs) {
            end = len(memberIDs)
        }
        batch := memberIDs[i:end]

        // 6a. 批量 UPSERT 用户会话
        valueStrings := make([]string, 0, len(batch))
        valueArgs := make([]interface{}, 0, len(batch)*3)
        for j, uid := range batch {
            base := j * 3
            valueStrings = append(valueStrings,
                fmt.Sprintf("($%d, $%d, false, false, true, 0, 0, 0, '', 0, '{}', $%d, $%d)",
                    base+1, base+2, base+3, base+3))
            valueArgs = append(valueArgs, uid, conversationID, now)
        }
        batchSQL := fmt.Sprintf(
            `INSERT INTO user_conversations
             (user_id, channel_id, is_top, is_muted, is_show, unread_count, read_seq,
              clear_seq, draft, last_ack_time, extra, created_at, updated_at)
             VALUES %s
             ON CONFLICT (user_id, channel_id) DO UPDATE SET is_show = true, updated_at = EXCLUDED.updated_at`,
            strings.Join(valueStrings, ", "),
        )
        _, err = c.db.ExecContext(ctx, batchSQL, valueArgs...)
        if err != nil {
            log.Error("批量 UPSERT 用户会话失败", "batch_start", i, "err", err)
        }

        // 6b. 批量递增未读数 — PgSQL
        _, err = c.db.ExecContext(ctx,
            `UPDATE user_conversations
             SET unread_count = unread_count + 1, updated_at = $1
             WHERE channel_id = $2 AND user_id = ANY($3)`,
            now, conversationID, pq.Array(batch),
        )
        if err != nil {
            log.Error("批量递增未读数失败", "batch_start", i, "err", err)
        }

        // 6c. 批量更新 Redis 未读数 + 会话列表（延迟双删）
        redisPipe := c.redis.Pipeline()
        var delayedKeys []string
        for _, uid := range batch {
            unreadKey := fmt.Sprintf("conv:unread:%s:%s", uid, conversationID)
            redisPipe.Incr(ctx, unreadKey)
            redisPipe.Expire(ctx, unreadKey, 24*time.Hour)

            listKey := fmt.Sprintf("conv:user_list:%s", uid)
            redisPipe.ZAdd(ctx, listKey, &redis.Z{
                Score:  float64(msgTime),
                Member: conversationID,
            })

            // 清除用户会话详情缓存
            userConvKey := fmt.Sprintf("conv:user_conv:%s:%s", uid, conversationID)
            totalUnreadKey := fmt.Sprintf("conv:total_unread:%s", uid)
            redisPipe.Del(ctx, userConvKey)
            redisPipe.Del(ctx, totalUnreadKey)
            delayedKeys = append(delayedKeys, userConvKey, totalUnreadKey)
        }
        if _, err := redisPipe.Exec(ctx); err != nil {
            log.Warn("批量更新 Redis 未读数失败", "batch_start", i, "err", err)
        }
        // 延迟二次删除
        go func(keys []string) {
            time.Sleep(500 * time.Millisecond)
            c.redis.Del(context.Background(), keys...)
        }(delayedKeys)
    }

    // ==================== 7. 发送方：更新会话列表排序（不增加未读） ====================
    _, err = c.db.ExecContext(ctx,
        `INSERT INTO user_conversations
         (user_id, channel_id, is_top, is_muted, is_show, unread_count, read_seq,
          clear_seq, draft, last_ack_time, extra, created_at, updated_at)
         VALUES ($1, $2, false, false, true, 0, 0, 0, '', 0, '{}', $3, $4)
         ON CONFLICT (user_id, channel_id) DO UPDATE SET is_show = true, updated_at = $5`,
        senderID, conversationID, now, now, now,
    )
    if err != nil {
        log.Error("确保发送方会话存在失败", "sender_id", senderID, "err", err)
    }

    senderListKey := fmt.Sprintf("conv:user_list:%s", senderID)
    c.redis.ZAdd(ctx, senderListKey, &redis.Z{Score: float64(msgTime), Member: conversationID})
    c.redis.Del(ctx, fmt.Sprintf("conv:user_conv:%s:%s", senderID, conversationID))

    // ==================== 8. 发送 Kafka 事件: conversation.updated ====================
    // 发送方事件
    senderEvt := &kafka_conversation.ConversationUpdatedEvent{
        Header:           buildEventHeader("conversation", c.instanceID),
        UserId:           senderID,
        ConversationId:   conversationID,
        ConversationType: common.CONVERSATION_TYPE_GROUP,
        LastMsgId:        msgID,
        LastMsgSeq:       msgSeq,
        LastMsgPreview:   msgPreview,
        LastMsgSenderId:  senderID,
        LastMsgTime:      msgTime,
        UnreadCount:      0,
    }
    c.kafka.Produce(ctx, "conversation.updated", senderID, senderEvt)

    // 接收方事件（批量发送给所有群成员）
    for _, uid := range memberIDs {
        unreadStr, _ := c.redis.Get(ctx, fmt.Sprintf("conv:unread:%s:%s", uid, conversationID)).Result()
        unread, _ := strconv.Atoi(unreadStr)
        receiverEvt := &kafka_conversation.ConversationUpdatedEvent{
            Header:           buildEventHeader("conversation", c.instanceID),
            UserId:           uid,
            ConversationId:   conversationID,
            ConversationType: common.CONVERSATION_TYPE_GROUP,
            LastMsgId:        msgID,
            LastMsgSeq:       msgSeq,
            LastMsgPreview:   msgPreview,
            LastMsgSenderId:  senderID,
            LastMsgTime:      msgTime,
            UnreadCount:      int32(unread),
        }
        c.kafka.Produce(ctx, "conversation.updated", uid, receiverEvt)
    }

    log.Info("群消息会话更新完成",
        "group_id", groupID,
        "conversation_id", conversationID,
        "affected_members", len(memberIDs),
    )
    return nil
}
```

### Consumer: `msg.recalled`

> 来源：Message 服务。消息撤回后，若被撤回的消息正好是通道的最后一条消息，需要更新 `last_msg_preview` 为 "[消息已撤回]"。

```go
func (c *ConversationConsumer) HandleMsgRecalled(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_message.MsgRecalledEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 MsgRecalledEvent 失败", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 ====================
    dedupKey := fmt.Sprintf("conv:kafka:dedup:%s", event.Header.EventId)
    set, err := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result()
    if err != nil {
        return fmt.Errorf("redis 幂等检查失败: %w", err)
    }
    if !set {
        return nil
    }

    recalledMsgID := event.MsgId
    conversationID := event.ConversationId
    now := time.Now().UnixMilli()

    // ==================== 3. 检查被撤回的消息是否是通道的最后一条 ====================
    var currentLastMsgID string
    err = c.db.QueryRowContext(ctx,
        `SELECT last_msg_id FROM conversation_channels WHERE channel_id = $1 AND status = 1`,
        conversationID,
    ).Scan(&currentLastMsgID)
    if err != nil {
        if err == sql.ErrNoRows {
            log.Warn("通道不存在，跳过撤回处理", "conversation_id", conversationID)
            return nil
        }
        return fmt.Errorf("查询通道最后消息失败: %w", err)
    }

    // ==================== 4. 如果是最后一条消息 → 更新 preview ====================
    if currentLastMsgID == recalledMsgID {
        _, err = c.db.ExecContext(ctx,
            `UPDATE conversation_channels
             SET last_msg_preview = '[消息已撤回]', updated_at = $1
             WHERE channel_id = $2 AND last_msg_id = $3 AND status = 1`,
            now, conversationID, recalledMsgID,
        )
        if err != nil {
            return fmt.Errorf("更新撤回消息 preview 失败: %w", err)
        }

        // 清除通道缓存（延迟双删）
        delayedDoubleDelete(ctx, c.redis, 500*time.Millisecond,
            fmt.Sprintf("conv:channel:%s", conversationID),
        )

        // 清除所有该通道相关用户的会话详情缓存（延迟双删）
        // 查询该通道的所有用户会话
        rows, err := c.db.QueryContext(ctx,
            `SELECT user_id FROM user_conversations WHERE channel_id = $1 AND is_show = true`,
            conversationID,
        )
        if err != nil {
            log.Warn("查询通道关联用户失败", "channel_id", conversationID, "err", err)
        } else {
            defer rows.Close()
            var userConvKeys []string
            for rows.Next() {
                var uid string
                if err := rows.Scan(&uid); err != nil {
                    continue
                }
                userConvKeys = append(userConvKeys, fmt.Sprintf("conv:user_conv:%s:%s", uid, conversationID))
            }
            if len(userConvKeys) > 0 {
                delayedDoubleDelete(ctx, c.redis, 500*time.Millisecond, userConvKeys...)
            }
        }

        // 发送 conversation.updated 通知客户端刷新 preview
        rows2, err := c.db.QueryContext(ctx,
            `SELECT user_id FROM user_conversations WHERE channel_id = $1 AND is_show = true`,
            conversationID,
        )
        if err == nil {
            defer rows2.Close()
            for rows2.Next() {
                var uid string
                if err := rows2.Scan(&uid); err != nil {
                    continue
                }
                evt := &kafka_conversation.ConversationUpdatedEvent{
                    Header:         buildEventHeader("conversation", c.instanceID),
                    UserId:         uid,
                    ConversationId: conversationID,
                    LastMsgId:      recalledMsgID,
                    LastMsgPreview: "[消息已撤回]",
                    LastMsgTime:    now,
                }
                c.kafka.Produce(ctx, "conversation.updated", uid, evt)
            }
        }
    }

    log.Info("消息撤回会话处理完成",
        "msg_id", recalledMsgID,
        "conversation_id", conversationID,
        "was_last_msg", currentLastMsgID == recalledMsgID,
    )
    return nil
}
```

### Consumer: `relation.friend.deleted`

> 来源：Relation 服务。好友删除后，隐藏双方的单聊会话（不物理删除，用户可能还想查看历史消息）。

```go
func (c *ConversationConsumer) HandleFriendDeleted(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_relation.FriendDeletedEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 FriendDeletedEvent 失败", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 ====================
    dedupKey := fmt.Sprintf("conv:kafka:dedup:%s", event.Header.EventId)
    set, err := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result()
    if err != nil {
        return fmt.Errorf("redis 幂等检查失败: %w", err)
    }
    if !set {
        return nil
    }

    operatorID := event.OperatorId
    targetID := event.TargetId
    now := time.Now().UnixMilli()

    // ==================== 3. 查找双方的单聊通道 ====================
    pA, pB := sortPair(operatorID, targetID)
    var conversationID string

    // 先查 Redis
    pairKey := fmt.Sprintf("conv:channel:pair:%s:%s", pA, pB)
    conversationID, _ = c.redis.Get(ctx, pairKey).Result()

    if conversationID == "" {
        // 查 PgSQL
        err = c.db.QueryRowContext(ctx,
            `SELECT channel_id FROM conversation_channels
             WHERE channel_type = 1 AND participant_a = $1 AND participant_b = $2 AND status = 1`,
            pA, pB,
        ).Scan(&conversationID)
        if err != nil {
            if err == sql.ErrNoRows {
                log.Info("好友删除但无会话通道，跳过", "operator", operatorID, "target", targetID)
                return nil
            }
            return fmt.Errorf("查询单聊通道失败: %w", err)
        }
    }

    // ==================== 4. 隐藏双方的用户会话 ====================
    for _, userID := range []string{operatorID, targetID} {
        _, err = c.db.ExecContext(ctx,
            `UPDATE user_conversations
             SET is_show = false, unread_count = 0, updated_at = $1
             WHERE user_id = $2 AND channel_id = $3`,
            now, userID, conversationID,
        )
        if err != nil {
            log.Error("隐藏用户会话失败", "user_id", userID, "channel_id", conversationID, "err", err)
        }

        // 清除 Redis 缓存（延迟双删）
        c.redis.ZRem(ctx, fmt.Sprintf("conv:user_list:%s", userID), conversationID)
        delayedDoubleDelete(ctx, c.redis, 500*time.Millisecond,
            fmt.Sprintf("conv:user_conv:%s:%s", userID, conversationID),
            fmt.Sprintf("conv:unread:%s:%s", userID, conversationID),
            fmt.Sprintf("conv:total_unread:%s", userID),
        )

        // 发送 conversation.deleted 事件
        evt := &kafka_conversation.ConversationDeletedEvent{
            Header:         buildEventHeader("conversation", c.instanceID),
            UserId:         userID,
            ConversationId: conversationID,
            ConversationType: common.CONVERSATION_TYPE_SINGLE,
            Reason:         "friend_deleted",
            DeleteTime:     now,
        }
        c.kafka.Produce(ctx, "conversation.deleted", userID, evt)
    }

    log.Info("好友删除会话处理完成",
        "operator", operatorID,
        "target", targetID,
        "conversation_id", conversationID,
    )
    return nil
}
```

### Consumer: `group.dissolved`

> 来源：Group 服务。群解散时，需要删除群通道 + 所有群成员的用户会话。

```go
func (c *ConversationConsumer) HandleGroupDissolved(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_group.GroupDissolvedEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 GroupDissolvedEvent 失败", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 ====================
    dedupKey := fmt.Sprintf("conv:kafka:dedup:%s", event.Header.EventId)
    set, err := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result()
    if err != nil {
        return fmt.Errorf("redis 幂等检查失败: %w", err)
    }
    if !set {
        return nil
    }

    groupID := event.GroupId
    now := time.Now().UnixMilli()

    // ==================== 3. 查找群通道 ====================
    var conversationID string
    groupKey := fmt.Sprintf("conv:channel:group:%s", groupID)
    conversationID, _ = c.redis.Get(ctx, groupKey).Result()

    if conversationID == "" {
        err = c.db.QueryRowContext(ctx,
            `SELECT channel_id FROM conversation_channels
             WHERE channel_type = 2 AND group_id = $1 AND status = 1`,
            groupID,
        ).Scan(&conversationID)
        if err != nil {
            if err == sql.ErrNoRows {
                log.Info("群解散但无通道，跳过", "group_id", groupID)
                return nil
            }
            return fmt.Errorf("查询群通道失败: %w", err)
        }
    }

    // ==================== 4. 查询所有关联的用户会话 ====================
    rows, err := c.db.QueryContext(ctx,
        `SELECT user_id FROM user_conversations WHERE channel_id = $1`,
        conversationID,
    )
    if err != nil {
        return fmt.Errorf("查询用户会话列表失败: %w", err)
    }
    defer rows.Close()

    var affectedUserIDs []string
    for rows.Next() {
        var uid string
        if err := rows.Scan(&uid); err != nil {
            continue
        }
        affectedUserIDs = append(affectedUserIDs, uid)
    }

    // ==================== 5. 事务：删除所有用户会话 + 软删除通道 ====================
    tx, err := c.db.BeginTx(ctx, nil)
    if err != nil {
        return fmt.Errorf("开启事务失败: %w", err)
    }
    defer tx.Rollback()

    // 5a. 删除所有用户会话（物理删除，群已解散无需保留）
    _, err = tx.ExecContext(ctx,
        `DELETE FROM user_conversations WHERE channel_id = $1`,
        conversationID,
    )
    if err != nil {
        return fmt.Errorf("删除用户会话失败: %w", err)
    }

    // 5b. 软删除通道
    _, err = tx.ExecContext(ctx,
        `UPDATE conversation_channels SET status = 2, updated_at = $1 WHERE channel_id = $2`,
        now, conversationID,
    )
    if err != nil {
        return fmt.Errorf("软删除通道失败: %w", err)
    }

    if err = tx.Commit(); err != nil {
        return fmt.Errorf("提交事务失败: %w", err)
    }

    // ==================== 6. 批量清除 Redis 缓存（延迟双删） ====================
    // 清除本地群成员缓存
    c.redis.Del(ctx, fmt.Sprintf("conv:local:group_members:%s", groupID))

    delKeys := []string{
        fmt.Sprintf("conv:channel:%s", conversationID),
        fmt.Sprintf("conv:channel:group:%s", groupID),
        fmt.Sprintf("conv:max_seq:%s", conversationID),
    }
    pipe := c.redis.Pipeline()
    for _, uid := range affectedUserIDs {
        pipe.ZRem(ctx, fmt.Sprintf("conv:user_list:%s", uid), conversationID)
        delKeys = append(delKeys,
            fmt.Sprintf("conv:user_conv:%s:%s", uid, conversationID),
            fmt.Sprintf("conv:unread:%s:%s", uid, conversationID),
            fmt.Sprintf("conv:total_unread:%s", uid),
        )
    }
    pipe.Exec(ctx)
    delayedDoubleDelete(ctx, c.redis, 500*time.Millisecond, delKeys...)

    // ==================== 7. 发送 Kafka 事件: conversation.deleted ====================
    for _, uid := range affectedUserIDs {
        evt := &kafka_conversation.ConversationDeletedEvent{
            Header:           buildEventHeader("conversation", c.instanceID),
            UserId:           uid,
            ConversationId:   conversationID,
            ConversationType: common.CONVERSATION_TYPE_GROUP,
            Reason:           "group_dissolved",
            DeleteTime:       now,
        }
        c.kafka.Produce(ctx, "conversation.deleted", uid, evt)
    }

    log.Info("群解散会话清理完成",
        "group_id", groupID,
        "conversation_id", conversationID,
        "affected_users", len(affectedUserIDs),
    )
    return nil
}
```

### Consumer: `group.member.joined`

> 来源：Group 服务。新成员入群后，为其创建用户会话视图。

```go
func (c *ConversationConsumer) HandleGroupMemberJoined(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_group.GroupMemberJoinedEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 GroupMemberJoinedEvent 失败", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 ====================
    dedupKey := fmt.Sprintf("conv:kafka:dedup:%s", event.Header.EventId)
    set, err := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result()
    if err != nil {
        return fmt.Errorf("redis 幂等检查失败: %w", err)
    }
    if !set {
        return nil
    }

    groupID := event.GroupId
    userID := event.UserId
    now := time.Now().UnixMilli()

    // ==================== 3. 获取群通道 ====================
    var conversationID string
    groupKey := fmt.Sprintf("conv:channel:group:%s", groupID)
    conversationID, _ = c.redis.Get(ctx, groupKey).Result()

    if conversationID == "" {
        err = c.db.QueryRowContext(ctx,
            `SELECT channel_id FROM conversation_channels
             WHERE channel_type = 2 AND group_id = $1 AND status = 1`,
            groupID,
        ).Scan(&conversationID)
        if err == sql.ErrNoRows {
            // 群通道尚未创建 → 创建
            conversationID = c.snowflake.Generate().String()
            _, err = c.db.ExecContext(ctx,
                `INSERT INTO conversation_channels
                 (channel_id, channel_type, group_id, max_seq, status, created_at, updated_at)
                 VALUES ($1, 2, $2, 0, 1, $3, $4)
                 ON CONFLICT DO NOTHING`,
                conversationID, groupID, now, now,
            )
            if err != nil {
                return fmt.Errorf("创建群通道失败: %w", err)
            }
            c.db.QueryRowContext(ctx,
                `SELECT channel_id FROM conversation_channels
                 WHERE channel_type = 2 AND group_id = $1 AND status = 1`,
                groupID,
            ).Scan(&conversationID)

            seqKey := fmt.Sprintf("conv:max_seq:%s", conversationID)
            c.redis.SetNX(ctx, seqKey, "0", 0)
        } else if err != nil {
            return fmt.Errorf("查询群通道失败: %w", err)
        }
    }

    // ==================== 4. 创建用户会话（UPSERT 幂等） ====================
    _, err = c.db.ExecContext(ctx,
        `INSERT INTO user_conversations
         (user_id, channel_id, is_top, is_muted, is_show, unread_count, read_seq,
          clear_seq, draft, last_ack_time, extra, created_at, updated_at)
         VALUES ($1, $2, false, false, true, 0, 0, 0, '', 0, '{}', $3, $4)
         ON CONFLICT (user_id, channel_id) DO UPDATE
         SET is_show = true, updated_at = $5`,
        userID, conversationID, now, now, now,
    )
    if err != nil {
        return fmt.Errorf("创建用户会话失败: %w", err)
    }

    // ==================== 5. 更新 Redis ====================
    pipe := c.redis.Pipeline()

    // 获取通道最后消息时间（用于 ZSET 排序）
    var lastMsgTime int64
    c.db.QueryRowContext(ctx,
        `SELECT COALESCE(last_msg_time, 0) FROM conversation_channels WHERE channel_id = $1`,
        conversationID,
    ).Scan(&lastMsgTime)
    if lastMsgTime == 0 {
        lastMsgTime = now
    }

    listKey := fmt.Sprintf("conv:user_list:%s", userID)
    pipe.ZAdd(ctx, listKey, &redis.Z{
        Score:  float64(lastMsgTime),
        Member: conversationID,
    })

    unreadKey := fmt.Sprintf("conv:unread:%s:%s", userID, conversationID)
    pipe.SetNX(ctx, unreadKey, "0", 24*time.Hour)

    // 回写群映射
    pipe.Set(ctx, fmt.Sprintf("conv:channel:group:%s", groupID), conversationID, 1*time.Hour)

    // 维护本地群成员缓存
    localMembersKey := fmt.Sprintf("conv:local:group_members:%s", groupID)
    pipe.SAdd(ctx, localMembersKey, userID)
    pipe.Expire(ctx, localMembersKey, 30*time.Minute)

    // 使总未读数缓存失效
    pipe.Del(ctx, fmt.Sprintf("conv:total_unread:%s", userID))

    pipe.Exec(ctx)

    // 延迟二次删除总未读数缓存
    go func() {
        time.Sleep(500 * time.Millisecond)
        c.redis.Del(context.Background(), fmt.Sprintf("conv:total_unread:%s", userID))
    }()

    // ==================== 6. 发送 Kafka 事件: conversation.created ====================
    evt := &kafka_conversation.ConversationCreatedEvent{
        Header:           buildEventHeader("conversation", c.instanceID),
        UserId:           userID,
        ConversationId:   conversationID,
        ConversationType: common.CONVERSATION_TYPE_GROUP,
        PeerId:           groupID,
        CreateTime:       now,
    }
    c.kafka.Produce(ctx, "conversation.created", userID, evt)

    log.Info("新成员入群会话创建完成",
        "group_id", groupID,
        "user_id", userID,
        "conversation_id", conversationID,
    )
    return nil
}
```

### Consumer: `group.member.left`

> 来源：Group 服务。成员主动退群后，隐藏该成员的用户会话。

```go
func (c *ConversationConsumer) HandleGroupMemberLeft(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_group.GroupMemberLeftEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 GroupMemberLeftEvent 失败", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 ====================
    dedupKey := fmt.Sprintf("conv:kafka:dedup:%s", event.Header.EventId)
    set, err := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result()
    if err != nil {
        return fmt.Errorf("redis 幂等检查失败: %w", err)
    }
    if !set {
        return nil
    }

    groupID := event.GroupId
    userID := event.UserId
    now := time.Now().UnixMilli()

    // ==================== 3. 查找群通道 ====================
    var conversationID string
    groupKey := fmt.Sprintf("conv:channel:group:%s", groupID)
    conversationID, _ = c.redis.Get(ctx, groupKey).Result()

    if conversationID == "" {
        err = c.db.QueryRowContext(ctx,
            `SELECT channel_id FROM conversation_channels
             WHERE channel_type = 2 AND group_id = $1 AND status = 1`,
            groupID,
        ).Scan(&conversationID)
        if err != nil {
            if err == sql.ErrNoRows {
                log.Info("退群但无通道，跳过", "group_id", groupID, "user_id", userID)
                return nil
            }
            return fmt.Errorf("查询群通道失败: %w", err)
        }
    }

    // ==================== 4. 隐藏用户会话 + 清零未读 ====================
    _, err = c.db.ExecContext(ctx,
        `UPDATE user_conversations
         SET is_show = false, unread_count = 0, updated_at = $1
         WHERE user_id = $2 AND channel_id = $3`,
        now, userID, conversationID,
    )
    if err != nil {
        return fmt.Errorf("隐藏用户会话失败: %w", err)
    }

    // ==================== 5. 清除 Redis 缓存（延迟双删） ====================
    // 维护本地群成员缓存
    c.redis.SRem(ctx, fmt.Sprintf("conv:local:group_members:%s", groupID), userID)

    c.redis.ZRem(ctx, fmt.Sprintf("conv:user_list:%s", userID), conversationID)
    delayedDoubleDelete(ctx, c.redis, 500*time.Millisecond,
        fmt.Sprintf("conv:user_conv:%s:%s", userID, conversationID),
        fmt.Sprintf("conv:unread:%s:%s", userID, conversationID),
        fmt.Sprintf("conv:total_unread:%s", userID),
    )

    // ==================== 6. 发送 Kafka 事件: conversation.deleted ====================
    evt := &kafka_conversation.ConversationDeletedEvent{
        Header:           buildEventHeader("conversation", c.instanceID),
        UserId:           userID,
        ConversationId:   conversationID,
        ConversationType: common.CONVERSATION_TYPE_GROUP,
        Reason:           "member_left",
        DeleteTime:       now,
    }
    c.kafka.Produce(ctx, "conversation.deleted", userID, evt)

    log.Info("成员退群会话处理完成",
        "group_id", groupID,
        "user_id", userID,
        "conversation_id", conversationID,
    )
    return nil
}
```

### Consumer: `group.member.kicked`

> 来源：Group 服务。成员被踢出群后，删除其用户会话。

```go
func (c *ConversationConsumer) HandleGroupMemberKicked(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_group.GroupMemberKickedEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 GroupMemberKickedEvent 失败", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 ====================
    dedupKey := fmt.Sprintf("conv:kafka:dedup:%s", event.Header.EventId)
    set, err := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result()
    if err != nil {
        return fmt.Errorf("redis 幂等检查失败: %w", err)
    }
    if !set {
        return nil
    }

    groupID := event.GroupId
    kickedUserID := event.KickedId
    now := time.Now().UnixMilli()

    // ==================== 3. 查找群通道 ====================
    var conversationID string
    groupKey := fmt.Sprintf("conv:channel:group:%s", groupID)
    conversationID, _ = c.redis.Get(ctx, groupKey).Result()

    if conversationID == "" {
        err = c.db.QueryRowContext(ctx,
            `SELECT channel_id FROM conversation_channels
             WHERE channel_type = 2 AND group_id = $1 AND status = 1`,
            groupID,
        ).Scan(&conversationID)
        if err != nil {
            if err == sql.ErrNoRows {
                log.Info("被踢但无通道，跳过", "group_id", groupID, "user_id", kickedUserID)
                return nil
            }
            return fmt.Errorf("查询群通道失败: %w", err)
        }
    }

    // ==================== 4. 隐藏被踢用户的会话 + 清零未读 ====================
    _, err = c.db.ExecContext(ctx,
        `UPDATE user_conversations
         SET is_show = false, unread_count = 0, updated_at = $1
         WHERE user_id = $2 AND channel_id = $3`,
        now, kickedUserID, conversationID,
    )
    if err != nil {
        return fmt.Errorf("隐藏被踢用户会话失败: %w", err)
    }

    // ==================== 5. 清除 Redis 缓存（延迟双删） ====================
    // 维护本地群成员缓存
    c.redis.SRem(ctx, fmt.Sprintf("conv:local:group_members:%s", groupID), kickedUserID)

    c.redis.ZRem(ctx, fmt.Sprintf("conv:user_list:%s", kickedUserID), conversationID)
    delayedDoubleDelete(ctx, c.redis, 500*time.Millisecond,
        fmt.Sprintf("conv:user_conv:%s:%s", kickedUserID, conversationID),
        fmt.Sprintf("conv:unread:%s:%s", kickedUserID, conversationID),
        fmt.Sprintf("conv:total_unread:%s", kickedUserID),
    )

    // ==================== 6. 发送 Kafka 事件: conversation.deleted ====================
    evt := &kafka_conversation.ConversationDeletedEvent{
        Header:           buildEventHeader("conversation", c.instanceID),
        UserId:           kickedUserID,
        ConversationId:   conversationID,
        ConversationType: common.CONVERSATION_TYPE_GROUP,
        Reason:           "member_kicked",
        DeleteTime:       now,
    }
    c.kafka.Produce(ctx, "conversation.deleted", kickedUserID, evt)

    log.Info("成员被踢会话处理完成",
        "group_id", groupID,
        "kicked_user", kickedUserID,
        "operator", event.OperatorId,
        "conversation_id", conversationID,
    )
    return nil
}
```

### Consumer: `ack.read.updated`

> 来源：Ack 服务。用户发送已读回执后，Ack 服务处理完成并通知 Conversation 更新 read_seq 和重算未读数。  
> **关键逻辑**：unread = max_seq - read_seq

```go
func (c *ConversationConsumer) HandleAckReadUpdated(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_ack.AckReadUpdatedEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 AckReadUpdatedEvent 失败", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 ====================
    dedupKey := fmt.Sprintf("conv:kafka:dedup:%s", event.Header.EventId)
    set, err := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result()
    if err != nil {
        return fmt.Errorf("redis 幂等检查失败: %w", err)
    }
    if !set {
        return nil
    }

    userID := event.UserId
    conversationID := event.ConversationId
    newReadSeq := event.ReadSeq
    now := time.Now().UnixMilli()

    // ==================== 3. 查询当前状态 ====================
    var oldReadSeq, maxSeq int64
    err = c.db.QueryRowContext(ctx,
        `SELECT uc.read_seq, cc.max_seq
         FROM user_conversations uc
         INNER JOIN conversation_channels cc ON uc.channel_id = cc.channel_id
         WHERE uc.user_id = $1 AND uc.channel_id = $2`,
        userID, conversationID,
    ).Scan(&oldReadSeq, &maxSeq)
    if err != nil {
        if err == sql.ErrNoRows {
            log.Warn("已读回执但用户会话不存在", "user_id", userID, "conversation_id", conversationID)
            return nil
        }
        return fmt.Errorf("查询用户会话状态失败: %w", err)
    }

    // read_seq 只能前进
    if newReadSeq <= oldReadSeq {
        log.Debug("read_seq 未前进，跳过",
            "user_id", userID, "old", oldReadSeq, "new", newReadSeq)
        return nil
    }

    // 不能超过 max_seq（也尝试从 Redis 获取最新值）
    seqKey := fmt.Sprintf("conv:max_seq:%s", conversationID)
    redisMaxSeqStr, err := c.redis.Get(ctx, seqKey).Result()
    if err == nil {
        redisMaxSeq, _ := strconv.ParseInt(redisMaxSeqStr, 10, 64)
        if redisMaxSeq > maxSeq {
            maxSeq = redisMaxSeq
        }
    }

    if newReadSeq > maxSeq {
        newReadSeq = maxSeq
    }

    // ==================== 4. 计算新未读数 ====================
    newUnread := maxSeq - newReadSeq
    if newUnread < 0 {
        newUnread = 0
    }
    oldUnread := maxSeq - oldReadSeq
    if oldUnread < 0 {
        oldUnread = 0
    }

    // ==================== 5. PgSQL 更新 read_seq + unread_count ====================
    _, err = c.db.ExecContext(ctx,
        `UPDATE user_conversations
         SET read_seq = $1, unread_count = $2, last_ack_time = $3, updated_at = $4
         WHERE user_id = $5 AND channel_id = $6 AND read_seq < $7`,
        newReadSeq, newUnread, now, now, userID, conversationID, newReadSeq,
    )
    if err != nil {
        return fmt.Errorf("更新 read_seq 失败: %w", err)
    }

    // ==================== 6. 更新 Redis（延迟双删） ====================
    delayedDoubleDelete(ctx, c.redis, 500*time.Millisecond,
        fmt.Sprintf("conv:unread:%s:%s", userID, conversationID),
        fmt.Sprintf("conv:user_conv:%s:%s", userID, conversationID),
        fmt.Sprintf("conv:total_unread:%s", userID),
    )

    // ==================== 7. 发送 Kafka 事件 ====================
    if newUnread != oldUnread {
        evt := &kafka_conversation.ConversationUnreadResetEvent{
            Header:         buildEventHeader("conversation", c.instanceID),
            UserId:         userID,
            ConversationId: conversationID,
            OldUnreadCount: int32(oldUnread),
            NewUnreadCount: int32(newUnread),
            ResetTime:      now,
        }
        c.kafka.Produce(ctx, "conversation.unread.changed", userID, evt)
    }

    log.Info("已读回执会话更新完成",
        "user_id", userID,
        "conversation_id", conversationID,
        "read_seq", newReadSeq,
        "old_unread", oldUnread,
        "new_unread", newUnread,
    )
    return nil
}
```

### Consumer: `user.deactivated`

> 来源：User 服务。用户注销后，清理所有该用户的会话数据。  
> **注意**：大批量操作，需分批处理。

```go
func (c *ConversationConsumer) HandleUserDeactivated(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_user.UserDeactivatedEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 UserDeactivatedEvent 失败", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 ====================
    dedupKey := fmt.Sprintf("conv:kafka:dedup:%s", event.Header.EventId)
    set, err := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result()
    if err != nil {
        return fmt.Errorf("redis 幂等检查失败: %w", err)
    }
    if !set {
        return nil
    }

    userID := event.UserId
    now := time.Now().UnixMilli()

    log.Info("开始处理用户注销-会话清理", "user_id", userID)

    // ==================== 3. 查询用户所有会话 ====================
    rows, err := c.db.QueryContext(ctx,
        `SELECT channel_id FROM user_conversations WHERE user_id = $1`,
        userID,
    )
    if err != nil {
        return fmt.Errorf("查询用户会话列表失败: %w", err)
    }
    defer rows.Close()

    var channelIDs []string
    for rows.Next() {
        var cid string
        if err := rows.Scan(&cid); err != nil {
            continue
        }
        channelIDs = append(channelIDs, cid)
    }

    if len(channelIDs) == 0 {
        log.Info("用户无会话记录，清理完成", "user_id", userID)
        return nil
    }

    // ==================== 4. 分批删除用户会话 ====================
    batchSize := 100
    for i := 0; i < len(channelIDs); i += batchSize {
        end := i + batchSize
        if end > len(channelIDs) {
            end = len(channelIDs)
        }
        batch := channelIDs[i:end]

        // PgSQL 批量删除
        _, err = c.db.ExecContext(ctx,
            `DELETE FROM user_conversations WHERE user_id = $1 AND channel_id = ANY($2)`,
            userID, pq.Array(batch),
        )
        if err != nil {
            log.Error("批量删除用户会话失败", "user_id", userID, "batch_start", i, "err", err)
        }

        // Redis 批量清除（延迟双删）
        var batchDelKeys []string
        redisPipe := c.redis.Pipeline()
        for _, cid := range batch {
            k1 := fmt.Sprintf("conv:user_conv:%s:%s", userID, cid)
            k2 := fmt.Sprintf("conv:unread:%s:%s", userID, cid)
            redisPipe.Del(ctx, k1)
            redisPipe.Del(ctx, k2)
            batchDelKeys = append(batchDelKeys, k1, k2)
        }
        redisPipe.Exec(ctx)
        // 延迟二次删除
        go func(keys []string) {
            time.Sleep(500 * time.Millisecond)
            c.redis.Del(context.Background(), keys...)
        }(batchDelKeys)
    }

    // ==================== 5. 清除用户级别的 Redis 缓存（延迟双删） ====================
    delayedDoubleDelete(ctx, c.redis, 500*time.Millisecond,
        fmt.Sprintf("conv:user_list:%s", userID),
        fmt.Sprintf("conv:total_unread:%s", userID),
    )

    // ==================== 6. 检查并清理用户参与的单聊通道 ====================
    // 对于单聊通道，如果对方也已注销，可以清理通道
    // 这里只标记，让后台任务定期清理无活跃用户的通道
    for _, cid := range channelIDs {
        var channelType int32
        c.db.QueryRowContext(ctx,
            `SELECT channel_type FROM conversation_channels WHERE channel_id = $1`,
            cid,
        ).Scan(&channelType)

        if channelType == 1 {
            // 单聊：检查对方是否还有会话记录
            var otherCount int
            c.db.QueryRowContext(ctx,
                `SELECT COUNT(*) FROM user_conversations WHERE channel_id = $1`,
                cid,
            ).Scan(&otherCount)
            if otherCount == 0 {
                // 双方都已删除 → 软删除通道
                c.db.ExecContext(ctx,
                    `UPDATE conversation_channels SET status = 2, updated_at = $1 WHERE channel_id = $2`,
                    now, cid,
                )
                c.redis.Del(ctx, fmt.Sprintf("conv:channel:%s", cid))
                c.redis.Del(ctx, fmt.Sprintf("conv:max_seq:%s", cid))
            }
        }
    }

    log.Info("用户注销会话清理完成",
        "user_id", userID,
        "conversation_count", len(channelIDs),
    )
    return nil
}
```

### Consumer: `config.changed`

> 来源：Config 服务。配置变更时热更新 Conversation 服务的运行时参数。

```go
func (c *ConversationConsumer) HandleConfigChanged(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_config.ConfigChangedEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 ConfigChangedEvent 失败", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 ====================
    dedupKey := fmt.Sprintf("conv:kafka:dedup:%s", event.Header.EventId)
    set, err := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result()
    if err != nil {
        return fmt.Errorf("redis 幂等检查失败: %w", err)
    }
    if !set {
        return nil
    }

    // ==================== 3. 只处理 conversation 相关配置 ====================
    namespace := event.Namespace
    if namespace != "conversation" && namespace != "global" {
        return nil // 非 conversation 配置，跳过
    }

    log.Info("收到配置变更",
        "namespace", namespace,
        "changed_keys", event.ChangedKeys,
    )

    // ==================== 4. 逐项更新运行时参数 ====================
    for key, value := range event.Configs {
        switch key {
        case "conversation.unread_max":
            // 单会话最大未读数限制
            if maxUnread, err := strconv.Atoi(value); err == nil && maxUnread > 0 {
                c.config.MaxUnreadPerConversation = maxUnread
                log.Info("更新配置: 单会话最大未读数", "value", maxUnread)
            }

        case "conversation.list_page_size_default":
            // 会话列表默认分页大小
            if pageSize, err := strconv.Atoi(value); err == nil && pageSize > 0 {
                c.config.DefaultPageSize = pageSize
                log.Info("更新配置: 默认分页大小", "value", pageSize)
            }

        case "conversation.cache_ttl_channel":
            // 通道缓存 TTL
            if ttl, err := time.ParseDuration(value); err == nil {
                c.config.ChannelCacheTTL = ttl
                log.Info("更新配置: 通道缓存 TTL", "value", ttl)
            }

        case "conversation.cache_ttl_user_list":
            // 用户会话列表缓存 TTL
            if ttl, err := time.ParseDuration(value); err == nil {
                c.config.UserListCacheTTL = ttl
                log.Info("更新配置: 用户列表缓存 TTL", "value", ttl)
            }

        case "conversation.cache_ttl_unread":
            // 未读数缓存 TTL
            if ttl, err := time.ParseDuration(value); err == nil {
                c.config.UnreadCacheTTL = ttl
                log.Info("更新配置: 未读数缓存 TTL", "value", ttl)
            }

        case "conversation.seq_sync_interval":
            // seq 同步间隔（Redis → PgSQL）
            if interval, err := time.ParseDuration(value); err == nil {
                c.config.SeqSyncInterval = interval
                log.Info("更新配置: seq 同步间隔", "value", interval)
            }

        case "conversation.group_batch_size":
            // 群消息处理批量大小
            if batchSize, err := strconv.Atoi(value); err == nil && batchSize > 0 {
                c.config.GroupBatchSize = batchSize
                log.Info("更新配置: 群消息批量大小", "value", batchSize)
            }

        case "conversation.kafka_concurrency":
            // Kafka 消费并发数
            if concurrency, err := strconv.Atoi(value); err == nil && concurrency > 0 {
                c.config.KafkaConcurrency = concurrency
                log.Info("更新配置: Kafka 消费并发数", "value", concurrency)
                // 动态调整消费者并发
                c.consumerGroup.SetConcurrency(concurrency)
            }

        default:
            log.Debug("未知配置项，跳过", "key", key)
        }
    }

    log.Info("配置变更处理完成", "namespace", namespace)
    return nil
}
```

### Consumer: `msg.forwarded`

> 来源：Message 服务。消息转发完成后触发。  
> 处理：为转发目标会话执行 createOrUpdate 操作（与 msg.stored 类似逻辑）。

```go
func (c *ConversationConsumer) HandleMsgForwarded(ctx context.Context, msg *kafka.Message) error {
    var event kafka_conversation.MsgForwardedForConversation
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 MsgForwardedForConversation 失败", "err", err)
        return nil
    }

    dedupKey := fmt.Sprintf("conv:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    // 对每个转发目标会话执行创建或更新
    for _, target := range event.Targets {
        if target.ConversationId == "" {
            continue
        }
        c.createOrUpdateConversation(ctx, target.ConversationId, target.UserId, &conversationUpdate{
            LastMsgPreview: target.ContentPreview,
            LastMsgSeq:     target.Seq,
            LastMsgTime:    event.ForwardTime,
            IncrUnread:     true,
        })
    }

    log.Debug("消息转发会话更新完成", "sender_id", event.SenderId, "target_count", len(event.Targets))
    return nil
}
```

### Consumer: `relation.blocked`

> 来源：Relation 服务。用户拉黑对方后触发。  
> 处理：标记该用户与被拉黑用户之间的会话为 blocked 状态。

```go
func (c *ConversationConsumer) HandleRelationBlocked(ctx context.Context, msg *kafka.Message) error {
    var event kafka_conversation.RelationBlockedForConversation
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 RelationBlockedForConversation 失败", "err", err)
        return nil
    }

    dedupKey := fmt.Sprintf("conv:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    if event.UserId == "" || event.TargetId == "" {
        return nil
    }

    // 生成单聊会话 ID（两个 user_id 排序拼接）
    u1, u2 := sortPair(event.UserId, event.TargetId)
    channelID := fmt.Sprintf("single_%s_%s", u1, u2)

    // 在会话记录上标记 blocked 状态
    cacheKey := fmt.Sprintf("conv:channel:%s:%s", event.UserId, channelID)
    delayedDoubleDelete(ctx, c.redis, 500*time.Millisecond, cacheKey)

    _, err := c.db.ExecContext(ctx,
        `UPDATE conversations SET is_blocked = true, updated_at = NOW()
         WHERE user_id = $1 AND channel_id = $2`,
        event.UserId, channelID,
    )
    if err != nil {
        return fmt.Errorf("标记会话 blocked 失败: %w", err)
    }

    // 更新会话列表缓存
    listKey := fmt.Sprintf("conv:list:%s", event.UserId)
    delayedDoubleDelete(ctx, c.redis, 500*time.Millisecond, listKey)

    log.Info("会话已标记为拉黑", "user_id", event.UserId, "target_id", event.TargetId, "channel_id", channelID)
    return nil
}
```

### Consumer: `relation.remark.updated`

> 来源：Relation 服务。用户修改好友备注后触发。  
> 处理：更新对应会话的显示名称（peer_name），使会话列表展示新备注。

```go
func (c *ConversationConsumer) HandleRelationRemarkUpdated(ctx context.Context, msg *kafka.Message) error {
    var event kafka_conversation.RemarkUpdatedForConversation
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 RemarkUpdatedForConversation 失败", "err", err)
        return nil
    }

    dedupKey := fmt.Sprintf("conv:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    if event.UserId == "" || event.FriendId == "" {
        return nil
    }

    // 生成单聊会话 ID
    u1, u2 := sortPair(event.UserId, event.FriendId)
    channelID := fmt.Sprintf("single_%s_%s", u1, u2)

    // 延迟双删
    cacheKey := fmt.Sprintf("conv:channel:%s:%s", event.UserId, channelID)
    delayedDoubleDelete(ctx, c.redis, 500*time.Millisecond, cacheKey)

    // 更新会话显示名称
    _, err := c.db.ExecContext(ctx,
        `UPDATE conversations SET peer_name = $1, updated_at = NOW()
         WHERE user_id = $2 AND channel_id = $3`,
        event.NewRemark, event.UserId, channelID,
    )
    if err != nil {
        return fmt.Errorf("更新会话备注显示名称失败: %w", err)
    }

    // 更新列表缓存
    listKey := fmt.Sprintf("conv:list:%s", event.UserId)
    delayedDoubleDelete(ctx, c.redis, 500*time.Millisecond, listKey)

    log.Info("会话显示名称已更新为备注",
        "user_id", event.UserId, "friend_id", event.FriendId, "new_remark", event.NewRemark)
    return nil
}
```

```go
func (c *ConversationConsumer) RegisterHandlers() {
    c.consumerGroup.Register("msg.stored.single", c.HandleMsgStoredSingle)
    c.consumerGroup.Register("msg.stored.group", c.HandleMsgStoredGroup)
    c.consumerGroup.Register("msg.recalled", c.HandleMsgRecalled)
    c.consumerGroup.Register("msg.forwarded", c.HandleMsgForwarded)
    c.consumerGroup.Register("relation.friend.deleted", c.HandleFriendDeleted)
    c.consumerGroup.Register("relation.blocked", c.HandleRelationBlocked)
    c.consumerGroup.Register("relation.remark.updated", c.HandleRelationRemarkUpdated)
    c.consumerGroup.Register("group.dissolved", c.HandleGroupDissolved)
    c.consumerGroup.Register("group.member.joined", c.HandleGroupMemberJoined)
    c.consumerGroup.Register("group.member.left", c.HandleGroupMemberLeft)
    c.consumerGroup.Register("group.member.kicked", c.HandleGroupMemberKicked)
    c.consumerGroup.Register("ack.read.updated", c.HandleAckReadUpdated)
    c.consumerGroup.Register("user.deactivated", c.HandleUserDeactivated)
    c.consumerGroup.Register("config.changed", c.HandleConfigChanged)
}
```

## 辅助函数

```go
// delayedDoubleDelete 延迟双删辅助函数
// 先立即删除指定的缓存 key，然后在延迟时间后再次删除，以应对「DB 写 → 缓存删 → 并发读回填旧数据」的竞态窗口
func delayedDoubleDelete(ctx context.Context, rdb redis.Cmdable, delay time.Duration, keys ...string) {
    if len(keys) == 0 {
        return
    }
    // 第一次立即删除
    rdb.Del(ctx, keys...)
    // 第二次延迟删除
    go func() {
        time.Sleep(delay)
        rdb.Del(context.Background(), keys...)
    }()
}

// sortPair 确保两个 user_id 按字典序排列
func sortPair(a, b string) (string, string) {
    if a < b {
        return a, b
    }
    return b, a
}

// buildEventHeader 构造 Kafka 事件头
func buildEventHeader(source, instanceID string) *common.EventHeader {
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
