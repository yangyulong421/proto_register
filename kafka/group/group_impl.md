# Group 群组服务 — Kafka 消费者实现伪代码

## 概述

Group 服务通过 Kafka 接收用户注销/封禁、内容违规审核、群头像处理完成、全局配置变更等事件，执行群成员清理、群信息同步和运行时参数热更新。同时定时生产群组统计事件。

## 生产 Topic 列表

| Topic | 事件 | 生产时机 | 消费方 |
|-------|------|----------|--------|
| `group.created` | GroupCreatedEvent | 创建群组 | Notification, Push, Audit, Search |
| `group.dissolved` | GroupDissolvedEvent | 解散群组 | Notification, Push, Audit, Conversation, Search |
| `group.info.updated` | GroupInfoUpdatedEvent | 更新群信息/公告 | Notification, Push, Audit, Search |
| `group.member.joined` | GroupMemberJoinedEvent | 成员加入 | Notification, Push, Audit, Conversation |
| `group.member.left` | GroupMemberLeftEvent | 成员退出 | Notification, Push, Audit, Conversation |
| `group.member.kicked` | GroupMemberKickedEvent | 成员被踢 | Notification, Push, Audit, Conversation |
| `group.member.role.changed` | GroupMemberRoleChangedEvent | 角色变更 | Notification, Push, Audit |
| `group.member.mute.changed` | GroupMemberMuteChangedEvent | 成员禁言变更 | Notification, Push |
| `group.mute.changed` | GroupMuteChangedEvent | 全员禁言变更 | Notification, Push |
| `group.transfer` | GroupTransferEvent | 群主转让 | Notification, Push, Audit |
| `group.application` | GroupApplicationEvent | 入群申请 | Notification, Push |
| `group.application.handled` | GroupApplicationHandledEvent | 申请处理 | Notification, Push |
| `group.stats` | GroupStatsEvent | 定时统计 | Audit |

## 消费 Topic 列表

| Topic | 来源 | 用途 |
|-------|------|------|
| `user.deactivated` | User | 账号注销 → 将该用户从所有群中移除，必要时转让群主 |
| `user.banned` | User | 账号封禁 → 标记封禁状态，限制群内操作 |
| `audit.content.violation` | Audit | 内容违规 → 根据处罚级别处理群名称/公告/昵称违规 |
| `media.avatar.processed` | Media | 群头像处理完成 → 更新群头像 URL |
| `config.changed` | Config | 配置变更 → 热更新群相关运行时参数 |

## Redis Key 设计（消费者侧）

| Key 格式 | 类型 | 值 | TTL | 说明 |
|----------|------|-----|-----|------|
| `group:kafka:dedup:{event_id}` | STRING | "1" | 24h | Kafka 消费幂等去重 |
| `group:user_banned:{user_id}` | STRING | "1" | = 封禁剩余时长 | 用户封禁状态标记（群场景） |
| `group:user_restricted:{user_id}` | STRING | restrict_level | 根据处罚时长 | 用户群操作限制标记 |

---

## 消费者实现

### delayedDoubleDelete — 延迟双删

```go
// delayedDoubleDelete 延迟双删：保证缓存与 DB 的最终一致性
// 原理：先删缓存 → 更新 DB → 500ms 后再次删除缓存，防止并发读回填脏数据
func (c *GroupConsumer) delayedDoubleDelete(ctx context.Context, keys ...string) {
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
```

### Consumer: `user.deactivated`

> 来源：User 服务。用户注销账户时，需将该用户从所有群组中移除。  
> 如果该用户是群主，需自动转让群主给最早加入的管理员/成员；如果群只剩该用户一人则直接解散群。

```go
func (c *GroupConsumer) HandleUserDeactivated(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_user.UserDeactivatedEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 UserDeactivatedEvent 失败", "err", err)
        return nil // 反序列化失败不重试
    }

    // ==================== 2. 幂等检查 — Redis ====================
    dedupKey := fmt.Sprintf("group:kafka:dedup:%s", event.Header.EventId)
    set, err := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result()
    if err != nil {
        return fmt.Errorf("redis 幂等检查失败: %w", err)
    }
    if !set {
        log.Info("重复事件，跳过", "event_id", event.Header.EventId)
        return nil
    }

    userID := event.UserId
    now := time.Now().UnixMilli()

    // ==================== 3. 查询该用户所在的所有群 — PgSQL ====================
    rows, err := c.db.QueryContext(ctx,
        `SELECT gm.group_id, gm.role, g.owner_id, g.name, g.member_count
         FROM group_members gm
         INNER JOIN groups g ON gm.group_id = g.group_id
         WHERE gm.user_id = $1 AND g.status != 3`,
        userID,
    )
    if err != nil {
        return fmt.Errorf("查询用户群列表失败: %w", err)
    }
    defer rows.Close()

    type groupMembership struct {
        GroupID     string
        Role        int32
        OwnerID     string
        GroupName   string
        MemberCount int32
    }
    var memberships []groupMembership
    for rows.Next() {
        var m groupMembership
        if err := rows.Scan(&m.GroupID, &m.Role, &m.OwnerID, &m.GroupName, &m.MemberCount); err != nil {
            continue
        }
        memberships = append(memberships, m)
    }

    log.Info("开始处理用户注销-群组清理",
        "user_id", userID,
        "group_count", len(memberships),
    )

    // ==================== 4. 逐群处理 ====================
    for _, m := range memberships {
        if m.Role == int32(common.GROUP_MEMBER_ROLE_OWNER) {
            // 该用户是群主
            if m.MemberCount <= 1 {
                // 4a. 群只剩群主一人 → 直接解散群
                err = c.disbandGroupInternal(ctx, m.GroupID, m.GroupName, userID, m.MemberCount, now)
                if err != nil {
                    log.Error("注销用户-解散群失败", "group_id", m.GroupID, "err", err)
                }
                continue
            }

            // 4b. 群主注销但群还有其他成员 → 自动转让群主
            // 优先选择最早加入的管理员，否则选最早加入的普通成员
            var newOwnerID string
            err = c.db.QueryRowContext(ctx,
                `SELECT user_id FROM group_members
                 WHERE group_id = $1 AND user_id != $2
                 ORDER BY role ASC, joined_at ASC
                 LIMIT 1`,
                m.GroupID, userID,
            ).Scan(&newOwnerID)
            if err != nil {
                log.Error("查询新群主失败，尝试解散群", "group_id", m.GroupID, "err", err)
                c.disbandGroupInternal(ctx, m.GroupID, m.GroupName, userID, m.MemberCount, now)
                continue
            }

            // 延迟双删 + 事务：转让群主 + 移除注销用户
            c.delayedDoubleDelete(ctx,
                fmt.Sprintf("group:info:%s", m.GroupID),
                fmt.Sprintf("group:members:%s", m.GroupID),
                fmt.Sprintf("group:member:%s:%s", m.GroupID, userID),
                fmt.Sprintf("group:member:%s:%s", m.GroupID, newOwnerID),
                fmt.Sprintf("group:member_count:%s", m.GroupID),
            )

            tx, err := c.db.BeginTx(ctx, nil)
            if err != nil {
                log.Error("开启事务失败", "group_id", m.GroupID, "err", err)
                continue
            }

            // 更新群 owner_id
            _, err = tx.ExecContext(ctx,
                `UPDATE groups SET owner_id = $1, updated_at = $2 WHERE group_id = $3`,
                newOwnerID, now, m.GroupID,
            )
            if err != nil {
                tx.Rollback()
                log.Error("更新群主失败", "group_id", m.GroupID, "err", err)
                continue
            }

            // 设置新群主角色
            _, err = tx.ExecContext(ctx,
                `UPDATE group_members SET role = 1 WHERE group_id = $1 AND user_id = $2`,
                m.GroupID, newOwnerID,
            )
            if err != nil {
                tx.Rollback()
                log.Error("设置新群主角色失败", "group_id", m.GroupID, "err", err)
                continue
            }

            // 删除注销用户
            _, err = tx.ExecContext(ctx,
                `DELETE FROM group_members WHERE group_id = $1 AND user_id = $2`,
                m.GroupID, userID,
            )
            if err != nil {
                tx.Rollback()
                log.Error("删除注销用户群成员记录失败", "group_id", m.GroupID, "err", err)
                continue
            }

            // 更新群成员数
            _, err = tx.ExecContext(ctx,
                `UPDATE groups SET member_count = member_count - 1, updated_at = $1
                 WHERE group_id = $2 AND member_count > 0`,
                now, m.GroupID,
            )
            if err != nil {
                tx.Rollback()
                log.Error("更新群成员数失败", "group_id", m.GroupID, "err", err)
                continue
            }

            if err = tx.Commit(); err != nil {
                log.Error("提交事务失败", "group_id", m.GroupID, "err", err)
                continue
            }

            // 生产 group.transfer 事件
            transferEvent := &kafka_group.GroupTransferEvent{
                Header:       buildEventHeader("group", c.instanceID),
                GroupId:      m.GroupID,
                OldOwnerId:  userID,
                NewOwnerId:  newOwnerID,
                TransferTime: now,
            }
            c.kafka.Produce(ctx, "group.transfer", m.GroupID, transferEvent)

            // 生产 group.member.left 事件
            leftEvent := &kafka_group.GroupMemberLeftEvent{
                Header:      buildEventHeader("group", c.instanceID),
                GroupId:     m.GroupID,
                GroupName:   m.GroupName,
                UserId:      userID,
                Nickname:    "",
                MemberCount: m.MemberCount - 1,
                LeaveTime:   now,
            }
            c.kafka.Produce(ctx, "group.member.left", m.GroupID, leftEvent)

            log.Info("注销用户群主已自动转让",
                "group_id", m.GroupID,
                "old_owner", userID,
                "new_owner", newOwnerID,
            )

        } else {
            // 4c. 普通成员或管理员 → 直接移除
            err = c.removeMemberInternal(ctx, m.GroupID, m.GroupName, userID, m.MemberCount, now)
            if err != nil {
                log.Error("注销用户-移除群成员失败", "group_id", m.GroupID, "user_id", userID, "err", err)
            }
        }
    }

    // ==================== 5. 清除该用户的群相关缓存 ====================
    c.delayedDoubleDelete(ctx, fmt.Sprintf("group:user_groups:%s", userID))

    // ==================== 6. 过期该用户所有待处理的入群申请 ====================
    _, err = c.db.ExecContext(ctx,
        `UPDATE group_applications SET status = 4, handle_time = $1
         WHERE user_id = $2 AND status = 1`,
        now, userID,
    )
    if err != nil {
        log.Error("过期注销用户入群申请失败", "user_id", userID, "err", err)
    }

    log.Info("用户注销，群组数据清理完成",
        "user_id", userID,
        "affected_groups", len(memberships),
    )
    return nil
}

// disbandGroupInternal — 内部方法：解散群组
func (c *GroupConsumer) disbandGroupInternal(ctx context.Context, groupID, groupName, operatorID string, memberCount int32, now int64) error {
    // 延迟双删
    c.delayedDoubleDelete(ctx,
        fmt.Sprintf("group:info:%s", groupID),
        fmt.Sprintf("group:members:%s", groupID),
        fmt.Sprintf("group:member_count:%s", groupID),
    )

    tx, err := c.db.BeginTx(ctx, nil)
    if err != nil {
        return fmt.Errorf("开启事务失败: %w", err)
    }
    defer tx.Rollback()

    // 标记群已解散
    _, err = tx.ExecContext(ctx,
        `UPDATE groups SET status = 3, updated_at = $1 WHERE group_id = $2 AND status != 3`,
        now, groupID,
    )
    if err != nil {
        return fmt.Errorf("标记群解散失败: %w", err)
    }

    // 删除所有成员
    _, err = tx.ExecContext(ctx,
        `DELETE FROM group_members WHERE group_id = $1`,
        groupID,
    )
    if err != nil {
        return fmt.Errorf("删除群成员失败: %w", err)
    }

    // 过期所有待处理申请
    _, err = tx.ExecContext(ctx,
        `UPDATE group_applications SET status = 4, handle_time = $1
         WHERE group_id = $2 AND status = 1`,
        now, groupID,
    )
    if err != nil {
        return fmt.Errorf("过期群申请失败: %w", err)
    }

    if err = tx.Commit(); err != nil {
        return fmt.Errorf("提交事务失败: %w", err)
    }

    // 生产 group.dissolved 事件
    dissolveEvent := &kafka_group.GroupDissolvedEvent{
        Header:       buildEventHeader("group", c.instanceID),
        GroupId:      groupID,
        GroupName:    groupName,
        OperatorId:   operatorID,
        MemberCount:  memberCount,
        DissolveTime: now,
    }
    c.kafka.Produce(ctx, "group.dissolved", groupID, dissolveEvent)

    log.Info("群已自动解散", "group_id", groupID, "reason", "owner_deactivated")
    return nil
}

// removeMemberInternal — 内部方法：移除群成员
func (c *GroupConsumer) removeMemberInternal(ctx context.Context, groupID, groupName, userID string, currentMemberCount int32, now int64) error {
    // 延迟双删
    c.delayedDoubleDelete(ctx,
        fmt.Sprintf("group:info:%s", groupID),
        fmt.Sprintf("group:members:%s", groupID),
        fmt.Sprintf("group:member:%s:%s", groupID, userID),
        fmt.Sprintf("group:member_count:%s", groupID),
        fmt.Sprintf("group:user_groups:%s", userID),
    )

    tx, err := c.db.BeginTx(ctx, nil)
    if err != nil {
        return fmt.Errorf("开启事务失败: %w", err)
    }
    defer tx.Rollback()

    _, err = tx.ExecContext(ctx,
        `DELETE FROM group_members WHERE group_id = $1 AND user_id = $2`,
        groupID, userID,
    )
    if err != nil {
        return fmt.Errorf("删除群成员记录失败: %w", err)
    }

    _, err = tx.ExecContext(ctx,
        `UPDATE groups SET member_count = member_count - 1, updated_at = $1
         WHERE group_id = $2 AND member_count > 0`,
        now, groupID,
    )
    if err != nil {
        return fmt.Errorf("更新群成员数失败: %w", err)
    }

    if err = tx.Commit(); err != nil {
        return fmt.Errorf("提交事务失败: %w", err)
    }

    // 生产 group.member.left 事件
    leftEvent := &kafka_group.GroupMemberLeftEvent{
        Header:      buildEventHeader("group", c.instanceID),
        GroupId:     groupID,
        GroupName:   groupName,
        UserId:      userID,
        Nickname:    "",
        MemberCount: currentMemberCount - 1,
        LeaveTime:   now,
    }
    c.kafka.Produce(ctx, "group.member.left", groupID, leftEvent)

    return nil
}
```

### Consumer: `user.banned`

> 来源：User 服务。用户被封禁时，标记其封禁状态，取消待处理的入群申请，  
> 不删除已有群成员关系（封禁解除后可恢复），但在 Redis 中标记以阻止其群内操作。

```go
func (c *GroupConsumer) HandleUserBanned(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_user.UserBannedEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 UserBannedEvent 失败", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 — Redis ====================
    dedupKey := fmt.Sprintf("group:kafka:dedup:%s", event.Header.EventId)
    set, err := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result()
    if err != nil {
        return fmt.Errorf("redis 幂等检查失败: %w", err)
    }
    if !set {
        log.Info("重复事件，跳过", "event_id", event.Header.EventId)
        return nil
    }

    userID := event.UserId
    now := time.Now().UnixMilli()

    // ==================== 3. 在 Redis 中标记用户封禁状态 ====================
    bannedKey := fmt.Sprintf("group:user_banned:%s", userID)
    if event.BanDuration > 0 {
        c.redis.Set(ctx, bannedKey, "1", time.Duration(event.BanDuration)*time.Second)
    } else {
        // 永久封禁，设置 30 天 TTL，由定时任务刷新
        c.redis.Set(ctx, bannedKey, "1", 30*24*time.Hour)
    }

    // ==================== 4. 取消该用户所有待处理的入群申请 — PgSQL ====================
    result, err := c.db.ExecContext(ctx,
        `UPDATE group_applications SET status = 3, operator_id = 'system', handle_time = $1
         WHERE user_id = $2 AND status = 1`,
        now, userID,
    )
    if err != nil {
        log.Error("取消封禁用户入群申请失败", "user_id", userID, "err", err)
    } else {
        affected, _ := result.RowsAffected()
        if affected > 0 {
            log.Info("已取消封禁用户的待处理入群申请",
                "user_id", userID,
                "cancelled_count", affected,
            )
        }
    }

    // ==================== 5. 查询该用户所在的所有群（用于通知群管理员） — PgSQL ====================
    groupIDs := make([]string, 0)
    rows, err := c.db.QueryContext(ctx,
        `SELECT group_id FROM group_members WHERE user_id = $1`,
        userID,
    )
    if err == nil {
        defer rows.Close()
        for rows.Next() {
            var gid string
            if err := rows.Scan(&gid); err != nil {
                continue
            }
            groupIDs = append(groupIDs, gid)
        }
    }

    // ==================== 6. 对该用户所有群内身份执行禁言（封禁期间不允许发言）— PgSQL ====================
    if len(groupIDs) > 0 {
        muteUntil := int64(0)
        if event.BanDuration > 0 {
            muteUntil = now + int64(event.BanDuration)*1000
        } else {
            muteUntil = now + 30*24*3600*1000 // 永久封禁按 30 天禁言
        }

        // 延迟双删
        banDelKeys := make([]string, 0, len(groupIDs))
        for _, gid := range groupIDs {
            banDelKeys = append(banDelKeys, fmt.Sprintf("group:member:%s:%s", gid, userID))
        }
        c.delayedDoubleDelete(ctx, banDelKeys...)

        _, err = c.db.ExecContext(ctx,
            `UPDATE group_members SET is_muted = true, mute_until = $1
             WHERE user_id = $2`,
            muteUntil, userID,
        )
        if err != nil {
            log.Error("封禁用户群内禁言失败", "user_id", userID, "err", err)
        }

        // 为每个群生产 group.member.mute.changed 事件
        for _, gid := range groupIDs {
            muteEvent := &kafka_group.GroupMemberMuteChangedEvent{
                Header:     buildEventHeader("group", c.instanceID),
                GroupId:    gid,
                OperatorId: "system",
                UserId:     userID,
                IsMuted:    true,
                MuteUntil:  muteUntil,
                ChangeTime: now,
            }
            c.kafka.Produce(ctx, "group.member.mute.changed", gid, muteEvent)
        }
    }

    log.Info("用户封禁，群组状态已更新",
        "user_id", userID,
        "ban_duration", event.BanDuration,
        "affected_groups", len(groupIDs),
    )
    return nil
}
```

### Consumer: `audit.content.violation`

> 来源：Audit 审核服务。当群名称、群公告、群内昵称等内容被检测到违规时，  
> Group 服务根据违规类型和处罚级别执行重置/限制操作。

```go
func (c *GroupConsumer) HandleContentViolation(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_audit.ContentViolationEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 ContentViolationEvent 失败", "err", err)
        return nil
    }

    // ==================== 2. 过滤非 Group 相关的违规事件 ====================
    isGroupViolation := false
    for _, svc := range event.AffectedServices {
        if svc == "group" {
            isGroupViolation = true
            break
        }
    }
    if !isGroupViolation {
        return nil // 非 group 相关的违规，跳过
    }

    // ==================== 3. 幂等检查 — Redis ====================
    dedupKey := fmt.Sprintf("group:kafka:dedup:%s", event.Header.EventId)
    set, err := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result()
    if err != nil {
        return fmt.Errorf("redis 幂等检查失败: %w", err)
    }
    if !set {
        log.Info("重复事件，跳过", "event_id", event.Header.EventId)
        return nil
    }

    userID := event.UserId
    now := time.Now().UnixMilli()

    // ==================== 4. 根据违规动作执行对应处理 ====================
    switch event.Action {
    case "reset_group_name":
        // 违规群名称：重置为默认群名（"群聊"）
        if event.GroupId == "" {
            log.Warn("reset_group_name 缺少 group_id", "event_id", event.Header.EventId)
            return nil
        }

        // 延迟双删
        c.delayedDoubleDelete(ctx, fmt.Sprintf("group:info:%s", event.GroupId))

        _, err := c.db.ExecContext(ctx,
            `UPDATE groups SET name = '群聊', updated_at = $1 WHERE group_id = $2 AND status != 3`,
            now, event.GroupId,
        )
        if err != nil {
            return fmt.Errorf("重置群名称失败: %w", err)
        }

        // 生产 group.info.updated 事件
        updateEvent := &kafka_group.GroupInfoUpdatedEvent{
            Header:     buildEventHeader("group", c.instanceID),
            GroupId:    event.GroupId,
            OperatorId: "system",
            UpdatedFields: map[string]string{
                "name": "群聊",
                "reason": "content_violation: " + event.Reason,
            },
            UpdateTime: now,
        }
        c.kafka.Produce(ctx, "group.info.updated", event.GroupId, updateEvent)

        log.Info("违规群名称已重置",
            "group_id", event.GroupId,
            "reason", event.Reason,
        )

    case "reset_group_announcement":
        // 违规群公告：清空公告
        if event.GroupId == "" {
            log.Warn("reset_group_announcement 缺少 group_id", "event_id", event.Header.EventId)
            return nil
        }

        // 延迟双删
        c.delayedDoubleDelete(ctx, fmt.Sprintf("group:info:%s", event.GroupId))

        _, err := c.db.ExecContext(ctx,
            `UPDATE groups SET announcement = '', updated_at = $1 WHERE group_id = $2 AND status != 3`,
            now, event.GroupId,
        )
        if err != nil {
            return fmt.Errorf("清空群公告失败: %w", err)
        }

        updateEvent := &kafka_group.GroupInfoUpdatedEvent{
            Header:     buildEventHeader("group", c.instanceID),
            GroupId:    event.GroupId,
            OperatorId: "system",
            UpdatedFields: map[string]string{
                "announcement": "",
                "reason": "content_violation: " + event.Reason,
            },
            UpdateTime: now,
        }
        c.kafka.Produce(ctx, "group.info.updated", event.GroupId, updateEvent)

        log.Info("违规群公告已清空",
            "group_id", event.GroupId,
            "reason", event.Reason,
        )

    case "reset_member_nickname":
        // 违规群内昵称：重置该用户在指定群的昵称
        if event.GroupId == "" || userID == "" {
            log.Warn("reset_member_nickname 缺少必要参数", "event_id", event.Header.EventId)
            return nil
        }

        // 延迟双删
        c.delayedDoubleDelete(ctx, fmt.Sprintf("group:member:%s:%s", event.GroupId, userID))

        _, err := c.db.ExecContext(ctx,
            `UPDATE group_members SET nickname = '' WHERE group_id = $1 AND user_id = $2`,
            event.GroupId, userID,
        )
        if err != nil {
            return fmt.Errorf("重置群内昵称失败: %w", err)
        }

        log.Info("违规群内昵称已重置",
            "group_id", event.GroupId,
            "user_id", userID,
            "reason", event.Reason,
        )

    case "reset_all_nicknames":
        // 严重违规：重置该用户在所有群的昵称
        if userID == "" {
            log.Warn("reset_all_nicknames 缺少 user_id", "event_id", event.Header.EventId)
            return nil
        }

        // 查询用户在哪些群中有自定义昵称
        affectedRows, err := c.db.QueryContext(ctx,
            `SELECT group_id FROM group_members WHERE user_id = $1 AND nickname != ''`,
            userID,
        )
        if err != nil {
            return fmt.Errorf("查询用户群内昵称失败: %w", err)
        }
        var affectedGroupIDs []string
        for affectedRows.Next() {
            var gid string
            affectedRows.Scan(&gid)
            affectedGroupIDs = append(affectedGroupIDs, gid)
        }
        affectedRows.Close()

        // 延迟双删
        nickDelKeys := make([]string, 0, len(affectedGroupIDs))
        for _, gid := range affectedGroupIDs {
            nickDelKeys = append(nickDelKeys, fmt.Sprintf("group:member:%s:%s", gid, userID))
        }
        c.delayedDoubleDelete(ctx, nickDelKeys...)

        // 批量重置
        _, err = c.db.ExecContext(ctx,
            `UPDATE group_members SET nickname = '' WHERE user_id = $1 AND nickname != ''`,
            userID,
        )
        if err != nil {
            return fmt.Errorf("批量重置群内昵称失败: %w", err)
        }

        log.Info("用户所有群内昵称已重置",
            "user_id", userID,
            "affected_groups", len(affectedGroupIDs),
            "reason", event.Reason,
        )

    case "restrict_group_operations":
        // 限制用户群操作（禁止创建群/邀请/修改群信息等）
        if userID == "" {
            log.Warn("restrict_group_operations 缺少 user_id", "event_id", event.Header.EventId)
            return nil
        }

        restrictKey := fmt.Sprintf("group:user_restricted:%s", userID)
        var duration time.Duration
        if event.RestrictDuration > 0 {
            duration = time.Duration(event.RestrictDuration) * time.Second
        } else {
            duration = 24 * time.Hour // 默认限制 24 小时
        }
        c.redis.Set(ctx, restrictKey, event.RestrictLevel, duration)

        log.Info("用户群操作已限制",
            "user_id", userID,
            "restrict_level", event.RestrictLevel,
            "duration_seconds", event.RestrictDuration,
        )

    case "disband_group":
        // 严重违规：强制解散群
        if event.GroupId == "" {
            log.Warn("disband_group 缺少 group_id", "event_id", event.Header.EventId)
            return nil
        }

        // 查询群信息
        var groupName string
        var memberCount int32
        err := c.db.QueryRowContext(ctx,
            `SELECT name, member_count FROM groups WHERE group_id = $1 AND status != 3`,
            event.GroupId,
        ).Scan(&groupName, &memberCount)
        if err != nil {
            log.Error("查询待解散群信息失败", "group_id", event.GroupId, "err", err)
            return nil
        }

        // 查询所有成员（用于清缓存）
        memberRows, _ := c.db.QueryContext(ctx,
            `SELECT user_id FROM group_members WHERE group_id = $1`, event.GroupId,
        )
        var memberIDs []string
        if memberRows != nil {
            for memberRows.Next() {
                var uid string
                memberRows.Scan(&uid)
                memberIDs = append(memberIDs, uid)
            }
            memberRows.Close()
        }

        // 延迟双删：清除成员相关缓存
        disbandDelKeys := make([]string, 0, len(memberIDs)*2)
        for _, uid := range memberIDs {
            disbandDelKeys = append(disbandDelKeys, fmt.Sprintf("group:user_groups:%s", uid))
            disbandDelKeys = append(disbandDelKeys, fmt.Sprintf("group:member:%s:%s", event.GroupId, uid))
        }
        c.delayedDoubleDelete(ctx, disbandDelKeys...)

        err = c.disbandGroupInternal(ctx, event.GroupId, groupName, "system", memberCount, now)
        if err != nil {
            return fmt.Errorf("强制解散群失败: %w", err)
        }

        log.Info("群因内容违规被强制解散",
            "group_id", event.GroupId,
            "reason", event.Reason,
        )

    default:
        log.Warn("未知的违规处理动作",
            "action", event.Action,
            "event_id", event.Header.EventId,
        )
    }

    return nil
}
```

### Consumer: `media.avatar.processed`

> 来源：Media 服务。群头像上传后经过裁剪、压缩、CDN 分发等处理，完成后通知 Group 服务更新头像 URL。

```go
func (c *GroupConsumer) HandleMediaAvatarProcessed(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_media.AvatarProcessedEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 AvatarProcessedEvent 失败", "err", err)
        return nil
    }

    // ==================== 2. 过滤非群头像的处理事件 ====================
    // 只处理 upload_type = GROUP_AVATAR 的事件
    if event.UploadType != common.UPLOAD_TYPE_GROUP_AVATAR {
        return nil // 非群头像，跳过（可能是用户头像等）
    }

    // ==================== 3. 幂等检查 — Redis ====================
    dedupKey := fmt.Sprintf("group:kafka:dedup:%s", event.Header.EventId)
    set, err := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result()
    if err != nil {
        return fmt.Errorf("redis 幂等检查失败: %w", err)
    }
    if !set {
        log.Info("重复事件，跳过", "event_id", event.Header.EventId)
        return nil
    }

    groupID := event.OwnerId // 群头像场景下 OwnerId = group_id
    now := time.Now().UnixMilli()

    // ==================== 4. 校验处理结果 ====================
    if event.Status != common.MEDIA_PROCESS_STATUS_COMPLETED {
        log.Warn("群头像处理未成功",
            "group_id", groupID,
            "status", event.Status,
            "media_id", event.MediaId,
        )
        return nil // 处理失败不需要更新
    }

    // ==================== 5. 延迟双删 + 更新群头像 URL — PgSQL ====================
    newAvatarURL := event.ProcessedUrl // 处理后的 CDN URL
    if newAvatarURL == "" {
        newAvatarURL = event.OriginalUrl // 如果没有处理后 URL，使用原始 URL
    }

    c.delayedDoubleDelete(ctx, fmt.Sprintf("group:info:%s", groupID))

    result, err := c.db.ExecContext(ctx,
        `UPDATE groups SET avatar_url = $1, updated_at = $2
         WHERE group_id = $3 AND status != 3`,
        newAvatarURL, now, groupID,
    )
    if err != nil {
        return fmt.Errorf("更新群头像 URL 失败: %w", err)
    }
    affected, _ := result.RowsAffected()
    if affected == 0 {
        log.Warn("群不存在或已解散，跳过头像更新", "group_id", groupID)
        return nil
    }

    // ==================== 6. 生产 group.info.updated 事件 ====================
    updateEvent := &kafka_group.GroupInfoUpdatedEvent{
        Header:     buildEventHeader("group", c.instanceID),
        GroupId:    groupID,
        OperatorId: "system",
        UpdatedFields: map[string]string{
            "avatar_url": newAvatarURL,
        },
        UpdateTime: now,
    }
    c.kafka.Produce(ctx, "group.info.updated", groupID, updateEvent)

    log.Info("群头像已更新",
        "group_id", groupID,
        "media_id", event.MediaId,
        "new_avatar_url", newAvatarURL,
    )
    return nil
}
```

### Consumer: `config.changed`

> 来源：Config 服务。全局配置变更时，Group 服务热更新群相关运行时参数，  
> 如最大群成员数、最大建群数、群搜索开关等。

```go
func (c *GroupConsumer) HandleConfigChanged(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_config.ConfigChangedEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 ConfigChangedEvent 失败", "err", err)
        return nil
    }

    // ==================== 2. 过滤非本服务的配置 ====================
    affectsGroup := false
    for _, svc := range event.AffectedServices {
        if svc == "group" {
            affectsGroup = true
            break
        }
    }
    if !affectsGroup {
        return nil // 非 group 相关配置，跳过
    }

    // ==================== 3. 根据 config_key 更新本地运行时配置 ====================
    switch event.ConfigKey {
    case "group.max_members_default":
        // 新建群默认最大成员数（默认 500）
        val, err := strconv.Atoi(event.NewValue)
        if err != nil {
            log.Error("解析 max_members_default 配置失败", "value", event.NewValue, "err", err)
            return nil
        }
        if val < 10 || val > 10000 {
            log.Error("max_members_default 值超出合理范围", "value", val)
            return nil
        }
        c.config.Store("max_members_default", val)
        log.Info("默认群最大成员数已更新", "old", event.OldValue, "new", val)

    case "group.max_groups_per_user":
        // 每个用户最大建群/加群数（默认 500）
        val, err := strconv.Atoi(event.NewValue)
        if err != nil {
            log.Error("解析 max_groups_per_user 配置失败", "value", event.NewValue, "err", err)
            return nil
        }
        c.config.Store("max_groups_per_user", val)
        log.Info("用户最大群数已更新", "old", event.OldValue, "new", val)

    case "group.max_admins_per_group":
        // 每个群最大管理员数（默认 10）
        val, err := strconv.Atoi(event.NewValue)
        if err != nil {
            log.Error("解析 max_admins_per_group 配置失败", "value", event.NewValue, "err", err)
            return nil
        }
        c.config.Store("max_admins_per_group", val)
        log.Info("每群最大管理员数已更新", "old", event.OldValue, "new", val)

    case "group.application_expire_hours":
        // 入群申请过期时间（小时，默认 168 = 7天）
        val, err := strconv.Atoi(event.NewValue)
        if err != nil {
            log.Error("解析 application_expire_hours 配置失败", "value", event.NewValue, "err", err)
            return nil
        }
        c.config.Store("application_expire_hours", val)
        log.Info("入群申请过期时间已更新", "old", event.OldValue, "new_hours", val)

    case "group.info_cache_ttl_minutes":
        // 群信息 Redis 缓存 TTL（分钟，默认 30）
        val, err := strconv.Atoi(event.NewValue)
        if err != nil {
            log.Error("解析 info_cache_ttl_minutes 配置失败", "value", event.NewValue, "err", err)
            return nil
        }
        c.config.Store("info_cache_ttl_minutes", val)
        log.Info("群信息缓存 TTL 已更新", "old", event.OldValue, "new_minutes", val)

    case "group.member_cache_ttl_minutes":
        // 成员信息 Redis 缓存 TTL（分钟，默认 10）
        val, err := strconv.Atoi(event.NewValue)
        if err != nil {
            log.Error("解析 member_cache_ttl_minutes 配置失败", "value", event.NewValue, "err", err)
            return nil
        }
        c.config.Store("member_cache_ttl_minutes", val)
        log.Info("成员缓存 TTL 已更新", "old", event.OldValue, "new_minutes", val)

    case "group.invite_daily_limit":
        // 每日邀请人数上限（默认 50）
        val, err := strconv.Atoi(event.NewValue)
        if err != nil {
            log.Error("解析 invite_daily_limit 配置失败", "value", event.NewValue, "err", err)
            return nil
        }
        c.config.Store("invite_daily_limit", val)
        log.Info("每日邀请上限已更新", "old", event.OldValue, "new", val)

    case "group.create_daily_limit":
        // 每日建群上限（默认 10）
        val, err := strconv.Atoi(event.NewValue)
        if err != nil {
            log.Error("解析 create_daily_limit 配置失败", "value", event.NewValue, "err", err)
            return nil
        }
        c.config.Store("create_daily_limit", val)
        log.Info("每日建群上限已更新", "old", event.OldValue, "new", val)

    default:
        log.Debug("未知的 group 配置项", "key", event.ConfigKey)
    }

    return nil
}
```

---

## 定时任务

### 群组统计任务

```go
// RunStatsTask 每 5 分钟执行一次统计并生产 group.stats 事件
func (c *GroupConsumer) RunStatsTask(ctx context.Context) {
    ticker := time.NewTicker(5 * time.Minute)
    defer ticker.Stop()

    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            now := time.Now().UnixMilli()
            periodStart := now - 5*60*1000 // 上一个 5 分钟窗口起始
            periodEnd := now

            // 统计各项指标 — PgSQL
            var groupCreatedCount, groupDissolvedCount int64
            var memberJoinedCount, memberLeftCount, memberKickedCount int64
            var applicationCount int64

            // 新建群数
            c.db.QueryRowContext(ctx,
                `SELECT COUNT(*) FROM groups WHERE created_at >= $1 AND created_at < $2`,
                periodStart, periodEnd,
            ).Scan(&groupCreatedCount)

            // 解散群数（通过 status 变更记录）
            c.db.QueryRowContext(ctx,
                `SELECT COUNT(*) FROM groups WHERE status = 3 AND updated_at >= $1 AND updated_at < $2`,
                periodStart, periodEnd,
            ).Scan(&groupDissolvedCount)

            // 入群申请数
            c.db.QueryRowContext(ctx,
                `SELECT COUNT(*) FROM group_applications WHERE created_at >= $1 AND created_at < $2`,
                periodStart, periodEnd,
            ).Scan(&applicationCount)

            // 成员加入/退出/被踢 — 使用服务实例内的原子计数器
            memberJoinedCount = c.statsCounter.SwapAndReset("member_joined")
            memberLeftCount = c.statsCounter.SwapAndReset("member_left")
            memberKickedCount = c.statsCounter.SwapAndReset("member_kicked")

            // 生产 group.stats 事件
            statsEvent := &kafka_group.GroupStatsEvent{
                Header:              buildEventHeader("group", c.instanceID),
                ServerId:            c.instanceID,
                PeriodStart:         periodStart,
                PeriodEnd:           periodEnd,
                GroupCreatedCount:   groupCreatedCount,
                GroupDissolvedCount: groupDissolvedCount,
                MemberJoinedCount:   memberJoinedCount,
                MemberLeftCount:     memberLeftCount,
                MemberKickedCount:   memberKickedCount,
                ApplicationCount:    applicationCount,
            }
            if err := c.kafka.Produce(ctx, "group.stats", "stats", statsEvent); err != nil {
                log.Error("生产 group.stats 事件失败", "err", err)
            }
        }
    }
}
```

### 过期入群申请清理任务

```go
// RunExpireApplicationsTask 每小时清理超过 7 天未处理的入群申请
func (c *GroupConsumer) RunExpireApplicationsTask(ctx context.Context) {
    ticker := time.NewTicker(1 * time.Hour)
    defer ticker.Stop()

    expireHours := 168 // 默认 7 天

    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            // 读取动态配置
            if val, ok := c.config.Load("application_expire_hours"); ok {
                expireHours = val.(int)
            }

            now := time.Now().UnixMilli()
            expireBefore := now - int64(expireHours)*3600*1000

            result, err := c.db.ExecContext(ctx,
                `UPDATE group_applications SET status = 4, handle_time = $1
                 WHERE status = 1 AND created_at < $2`,
                now, expireBefore,
            )
            if err != nil {
                log.Error("清理过期入群申请失败", "err", err)
                continue
            }
            affected, _ := result.RowsAffected()
            if affected > 0 {
                log.Info("已过期入群申请",
                    "expired_count", affected,
                    "expire_before_ms", expireBefore,
                )
            }
        }
    }
}
```
