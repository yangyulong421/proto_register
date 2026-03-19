# Message 消息服务 — RPC 接口实现伪代码

## 概述

Message 服务是整个 IM 系统的**核心中枢**，负责消息的接收、校验、幂等去重、序号分配、持久化存储、缓存写入、事件分发。
配合 **notify+sync** 模式：客户端发送消息 → 服务端存储 → 分配 seq → 产生 Kafka 事件 → Push 服务发送轻量通知 → 收件方调用 PullMessages 拉取真实消息。

**核心设计原则：**
- **幂等性**：SendMessage 通过 `client_msg_id` 做 24h 去重，重复请求返回已有消息
- **有序性**：seq 由 Conversation 服务通过 Redis INCRBY 原子分配，保证会话内全局有序
- **msg_id 全局唯一**：Snowflake 算法生成，时间有序
- **先存储后通知**：消息先写入 PgSQL → 缓存 Redis → 再投递 Kafka，保证不丢
- **读写分离**：热数据走 Redis 缓存，冷数据回查 PgSQL

## 外部依赖

| 依赖类型 | 依赖项 | 用途 |
|---------|--------|------|
| PgSQL | messages 表 | 消息持久化 |
| PgSQL | user_deleted_messages 表 | 用户维度软删除记录 |
| Redis | 消息去重 / 消息详情缓存 / 近期消息列表 | 幂等、快速拉取 |
| RPC | Conversation.IncrChannelSeq | 分配消息 seq（**最热跨服务调用**） |
| RPC | Conversation.GetOrCreateChannel | 获取/创建会话通道 |
| RPC | Conversation.UpdateChannelLastMsg | 更新通道最后一条消息 |
| RPC | Conversation.GetUserConversation | 查询用户会话（含 clear_seq） |
| RPC | Conversation.GetChannelMaxSeq | 查询通道最大 seq |
| RPC → 本地缓存 | Relation.CheckFriendship | 本地缓存 msg:local:friend:{a}:{b} 查询, cache miss 时降级 RPC |
| RPC → 本地缓存 | Relation.CheckBlockship | 本地缓存 msg:local:block:{a}:{b} 查询, cache miss 时降级 RPC |
| RPC → 本地缓存 | Group.GetGroupMember | 本地缓存 msg:local:group_member:{gid}:{uid} 查询, cache miss 时降级 RPC |
| RPC → 本地缓存 | Group.GetGroup | 本地缓存 msg:local:group:{gid} 查询, cache miss 时降级 RPC |
| RPC → 本地缓存 | User.GetUser | 本地缓存 msg:local:user:{uid} 查询, cache miss 时降级 RPC |
| Kafka | msg.stored.single / msg.stored.group / msg.recalled / msg.deleted / msg.forwarded / msg.moderation.request / msg.search.index / msg.conversation.update | 事件通知下游 |

## PgSQL 表结构

```sql
-- 消息表（核心表，按 channel_id HASH 分区以支撑大规模数据）
CREATE TABLE messages (
    id            BIGSERIAL PRIMARY KEY,
    msg_id        VARCHAR(20)  NOT NULL UNIQUE,              -- Snowflake 全局唯一消息 ID
    client_msg_id VARCHAR(64)  NOT NULL,                     -- 客户端消息 ID（幂等去重）
    channel_id    VARCHAR(20)  NOT NULL,                     -- 会话通道 ID
    channel_type  SMALLINT     NOT NULL DEFAULT 1,           -- 1=单聊 2=群聊 3=系统
    sender_id     VARCHAR(20)  NOT NULL,                     -- 发送者 ID
    msg_type      SMALLINT     NOT NULL DEFAULT 1,           -- 消息类型枚举
    content       JSONB        NOT NULL DEFAULT '{}',        -- 消息体 JSON（文本/扩展）
    media_url     TEXT         NOT NULL DEFAULT '',           -- 媒体文件 URL
    media_info    JSONB        DEFAULT '{}',                  -- 媒体元信息（宽高/时长/大小）
    at_user_ids   TEXT[]       DEFAULT '{}',                  -- @的用户 ID 列表
    at_all        BOOLEAN      NOT NULL DEFAULT false,        -- 是否 @全体
    quote_msg_id  VARCHAR(20)  DEFAULT '',                    -- 引用消息 ID
    merge_msg_ids TEXT[]       DEFAULT '{}',                  -- 合并转发的消息 ID 列表
    status        SMALLINT     NOT NULL DEFAULT 1,            -- 1=正常 2=已撤回 3=审核中 4=已删除
    seq           BIGINT       NOT NULL DEFAULT 0,            -- 会话内递增序号
    send_time     BIGINT       NOT NULL DEFAULT 0,            -- 客户端发送时间戳（毫秒）
    server_time   BIGINT       NOT NULL DEFAULT 0,            -- 服务端处理时间戳（毫秒）
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- 核心查询索引：按会话拉取消息（notify+sync 热路径）
CREATE INDEX idx_messages_channel_seq ON messages(channel_id, seq);
-- 按发送者查询（审计/管理后台）
CREATE INDEX idx_messages_sender ON messages(sender_id, created_at DESC);
-- 客户端消息 ID 去重索引
CREATE UNIQUE INDEX idx_messages_client_msg ON messages(client_msg_id);
-- 消息 ID 快速定位（撤回/转发等操作）
-- msg_id 已有 UNIQUE 约束，自动创建索引

-- ⚠️ MED-01: 10M 并发场景分区策略（必须启用）
-- 主分区：HASH(channel_id) 128 个分区，均匀分布到 PgSQL Citus 集群的 8~16 个 Worker 节点
-- 5000 万会话 × 每会话平均 1000 条 = 500 亿条记录，128 分区每分区约 4 亿条
CREATE TABLE messages (
    -- ... 字段同上 ...
) PARTITION BY HASH(channel_id);

-- 自动创建 128 个 HASH 分区（Citus 自动管理，或手动 pg_partman）
-- SELECT create_distributed_table('messages', 'channel_id', colocate_with => 'none');

-- 冷热数据分离：
-- 1. 热数据（3 个月内）保留在 NVMe SSD 上
-- 2. 温数据（3~12 个月）迁移到 SATA SSD
-- 3. 冷数据（>12 个月）归档到对象存储（S3/MinIO），仅保留索引
-- 使用 pg_cron 定时任务每日凌晨执行冷热迁移

-- 数据库部署建议：
-- | 组件 | 规格 | 节点数 |
-- |------|------|--------|
-- | Citus Coordinator | 8C32G | 2（主备） |
-- | Citus Worker | 16C64G + NVMe 2TB | 8~16 |
-- | 读副本 | 16C64G | 每 Worker 1~2 个 |
-- CREATE TABLE messages PARTITION BY HASH(channel_id);

-- 用户维度软删除记录表
CREATE TABLE user_deleted_messages (
    id         BIGSERIAL PRIMARY KEY,
    user_id    VARCHAR(20) NOT NULL,
    msg_id     VARCHAR(20) NOT NULL,
    deleted_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE UNIQUE INDEX idx_user_del_msg ON user_deleted_messages(user_id, msg_id);
```

## Redis Key 设计

| Key 格式 | 类型 | 值 | TTL | 说明 |
|----------|------|-----|-----|------|
| `msg:dedup:{client_msg_id}` | STRING | msg_id | 24h | 幂等去重：24h 内相同 client_msg_id 直接返回已存在的 msg_id |
| `msg:detail:{msg_id}` | STRING | JSON 序列化完整消息 | 1h | 消息详情懒缓存，GetMessage/GetHistoryMessages 回填 |
| `msg:recent:{channel_id}` | ZSET | member=msg_id, score=seq | 30min | 近期消息有序集合，PullMessages 快速读取，最多保留 200 条 |
| `msg:kafka:dedup:{event_id}` | STRING | "1" | 24h | Kafka 消费幂等去重 |
| `msg:local:block:{user_id}:{target_id}` | STRING | "1" or missing | 24h | 本地拉黑关系缓存，Kafka relation.blocked/unblocked 同步 |
| `msg:local:friend:{user_id}:{target_id}` | STRING | "1" or missing | 24h | 本地好友关系缓存，Kafka relation.friend.accepted/deleted 同步 |
| `msg:local:group:{group_id}` | HASH | group info | 30min | 本地群信息缓存，Kafka group.info.updated/group.dissolved 同步 |
| `msg:local:group_member:{group_id}:{user_id}` | HASH | member info (role, is_muted, mute_until) | 10min | 本地群成员缓存，Kafka group.member.* 同步 |
| `msg:local:user:{user_id}` | HASH | user info (nickname, avatar_url) | 30min | 本地用户信息缓存，Kafka user.profile.updated 同步 |
| `msg:local:user_settings:{user_id}` | HASH | {allow_stranger_msg} | 1h | 本地用户设置缓存（仅缓存 Message 服务实际使用的字段），Kafka user.settings.updated 同步 |

## 消息类型枚举

| 值 | 名称 | 说明 |
|----|------|------|
| 1 | TEXT | 文本消息 |
| 2 | IMAGE | 图片消息 |
| 3 | VIDEO | 视频消息 |
| 4 | AUDIO | 语音消息 |
| 5 | FILE | 文件消息 |
| 6 | LOCATION | 位置消息 |
| 7 | CARD | 名片消息 |
| 8 | MERGE | 合并转发消息 |
| 9 | RECALL | 撤回提示消息 |
| 10 | SYSTEM | 系统通知消息 |
| 99 | CUSTOM | 自定义消息 |

## 消息状态枚举

| 值 | 名称 | 说明 |
|----|------|------|
| 1 | NORMAL | 正常 |
| 2 | RECALLED | 已撤回 |
| 3 | MODERATED | 审核中/已审核 |
| 4 | DELETED | 已删除 |

---

## 接口实现

### 1. SendMessage — 发送消息（核心路径）

> **最关键的接口**，整个 IM 系统的消息入口。  
> 流程：参数校验 → 幂等去重 → 权限校验 → 分配 seq → 生成 msg_id → 存储 PgSQL → 缓存 Redis → 投递 Kafka → 返回。

```go
func (s *MessageService) SendMessage(ctx context.Context, req *pb.SendMessageRequest) (*pb.SendMessageResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.SenderID == "" {
        return nil, status.Error(codes.InvalidArgument, "sender_id is required")
    }
    if req.ReceiverID == "" {
        return nil, status.Error(codes.InvalidArgument, "receiver_id is required")
    }
    if req.ConversationType == common.CONVERSATION_TYPE_UNSPECIFIED {
        return nil, status.Error(codes.InvalidArgument, "conversation_type is required")
    }
    if req.Body == nil {
        return nil, status.Error(codes.InvalidArgument, "body is required")
    }
    if req.Body.Type == common.MESSAGE_TYPE_UNSPECIFIED {
        return nil, status.Error(codes.InvalidArgument, "message type is required")
    }
    if req.ClientMsgID == "" {
        return nil, status.Error(codes.InvalidArgument, "client_msg_id is required for idempotency")
    }
    if len(req.ClientMsgID) > 64 {
        return nil, status.Error(codes.InvalidArgument, "client_msg_id too long, max 64 chars")
    }

    // 文本消息长度校验
    if req.Body.Type == common.MESSAGE_TYPE_TEXT {
        if req.Body.Text == "" {
            return nil, status.Error(codes.InvalidArgument, "text content is required for text message")
        }
        if len(req.Body.Text) > s.config.MaxTextLength { // 默认 5000 字符
            return nil, status.Error(codes.InvalidArgument, "text content too long")
        }
    }

    // @用户数量限制
    if len(req.Body.AtUserIDs) > 100 {
        return nil, status.Error(codes.InvalidArgument, "at_user_ids too many, max 100")
    }

    // ==================== 2. 幂等去重 — Redis client_msg_id ====================
    // 24h 内相同 client_msg_id 的请求直接返回已存在的消息，不重复存储
    dedupKey := fmt.Sprintf("msg:dedup:%s", req.ClientMsgID)
    existingMsgID, err := s.redis.Get(ctx, dedupKey).Result()
    if err == nil && existingMsgID != "" {
        // 命中去重缓存 → 查已有消息返回
        existingMsg, err := s.getMessageByMsgID(ctx, existingMsgID)
        if err == nil && existingMsg != nil {
            return &pb.SendMessageResponse{
                Meta:       successMeta(ctx),
                MsgID:      existingMsg.MsgId,
                Seq:        existingMsg.Seq,
                ServerTime: existingMsg.ServerTime,
                Duplicate:  true, // 标记为重复消息
            }, nil
        }
        // 缓存有但消息查不到（极端场景），继续走正常流程
    }

    // 再检查 PgSQL（防止 Redis 去重 Key 过期但消息已存储）
    var existingDBMsgID string
    err = s.db.QueryRowContext(ctx,
        `SELECT msg_id FROM messages WHERE client_msg_id = $1 LIMIT 1`,
        req.ClientMsgID,
    ).Scan(&existingDBMsgID)
    if err == nil && existingDBMsgID != "" {
        // DB 中已存在该 client_msg_id → 恢复 Redis 去重缓存并返回
        s.redis.Set(ctx, dedupKey, existingDBMsgID, 24*time.Hour)
        existingMsg, _ := s.getMessageByMsgID(ctx, existingDBMsgID)
        if existingMsg != nil {
            return &pb.SendMessageResponse{
                Meta:       successMeta(ctx),
                MsgID:      existingMsg.MsgId,
                Seq:        existingMsg.Seq,
                ServerTime: existingMsg.ServerTime,
                Duplicate:  true,
            }, nil
        }
    }

    // ==================== 3. 发送者校验 — 本地缓存优先，cache miss 降级 RPC User.GetUser ====================
    senderInfo, err := s.getLocalUserInfo(ctx, req.SenderID)
    if err != nil {
        if st, ok := status.FromError(err); ok && st.Code() == codes.NotFound {
            return nil, status.Error(codes.NotFound, "sender not found")
        }
        return nil, status.Error(codes.Internal, "get sender info failed")
    }

    // ==================== 4. 权限校验 — 根据会话类型 ====================
    var channelID string
    var groupInfo *group_pb.GroupInfo     // 群聊时用
    var groupMemberCount int32            // 群聊时用

    switch req.ConversationType {
    case common.CONVERSATION_TYPE_SINGLE:
        // ---- 4a. 单聊权限校验 ----

        // 检查接收者是否存在 — 本地缓存优先
        receiverInfo, err := s.getLocalUserInfo(ctx, req.ReceiverID)
        if err != nil {
            if st, ok := status.FromError(err); ok && st.Code() == codes.NotFound {
                return nil, status.Error(codes.NotFound, "receiver not found")
            }
            return nil, status.Error(codes.Internal, "get receiver info failed")
        }
        _ = receiverInfo // 已确认存在

        // 检查是否被对方拉黑 — 读取本地缓存（非 RPC）
        blockKey := fmt.Sprintf("msg:local:block:%s:%s", req.ReceiverID, req.SenderID)
        isBlocked, err := s.redis.Get(ctx, blockKey).Result()
        if err != nil && err != redis.Nil {
            log.Warn("check local block cache failed, fallback to RPC", "err", err)
            // 本地缓存异常 → 降级 RPC
            blockResp, err := s.relationClient.CheckBlockship(ctx, &relation_pb.CheckBlockshipRequest{
                UserId:   req.ReceiverID,
                TargetId: req.SenderID,
            })
            if err != nil {
                return nil, status.Error(codes.Internal, "check blockship failed")
            }
            if blockResp.IsBlocked {
                return nil, status.Error(codes.FailedPrecondition, "message cannot be delivered")
            }
        } else if isBlocked == "1" {
            return nil, status.Error(codes.FailedPrecondition, "message cannot be delivered")
        }

        // 检查好友关系 — 读取本地缓存（非 RPC）
        friendKey := fmt.Sprintf("msg:local:friend:%s:%s", req.SenderID, req.ReceiverID)
        isFriend, err := s.redis.Get(ctx, friendKey).Result()
        if err != nil && err != redis.Nil {
            log.Warn("check local friend cache failed, fallback to RPC", "err", err)
            friendResp, err := s.relationClient.CheckFriendship(ctx, &relation_pb.CheckFriendshipRequest{
                UserId:   req.SenderID,
                TargetId: req.ReceiverID,
            })
            if err != nil {
                return nil, status.Error(codes.Internal, "check friendship failed")
            }
            if !friendResp.IsFriend {
                settingsKey := fmt.Sprintf("msg:local:user_settings:%s", req.ReceiverID)
                allowStranger, _ := s.redis.HGet(ctx, settingsKey, "allow_stranger_msg").Result()
                if allowStranger == "" || allowStranger == "false" {
                    // 缓存未命中 → 降级 RPC
                    settingsResp, err := s.userClient.GetUserSettings(ctx, &user_pb.GetUserSettingsRequest{
                        UserId: req.ReceiverID,
                    })
                    if err == nil && settingsResp.Settings != nil && !settingsResp.Settings.AllowStrangerMsg {
                        return nil, status.Error(codes.PermissionDenied, "receiver does not accept messages from strangers")
                    }
                } else if allowStranger == "0" {
                    return nil, status.Error(codes.PermissionDenied, "receiver does not accept messages from strangers")
                }
            }
        } else if isFriend != "1" {
            // 非好友 → 检查陌生人消息设置（本地缓存）
            settingsKey := fmt.Sprintf("msg:local:user_settings:%s", req.ReceiverID)
            allowStranger, _ := s.redis.HGet(ctx, settingsKey, "allow_stranger_msg").Result()
            if allowStranger == "0" || allowStranger == "false" {
                return nil, status.Error(codes.PermissionDenied, "receiver does not accept messages from strangers")
            }
        }

        // 获取或创建单聊通道
        channelResp, err := s.conversationClient.GetOrCreateChannel(ctx, &conv_pb.GetOrCreateChannelRequest{
            Type:      common.CONVERSATION_TYPE_SINGLE,
            MemberIds: []string{req.SenderID, req.ReceiverID},
        })
        if err != nil {
            return nil, status.Error(codes.Internal, "get or create channel failed")
        }
        channelID = channelResp.Channel.ConversationId

    case common.CONVERSATION_TYPE_GROUP:
        // ---- 4b. 群聊权限校验 ----

        // 获取群信息 — 本地缓存优先
        groupCacheKey := fmt.Sprintf("msg:local:group:%s", req.ReceiverID)
        groupData, err := s.redis.HGetAll(ctx, groupCacheKey).Result()
        if err != nil || len(groupData) == 0 {
            // 缓存未命中 → 降级 RPC + 回填缓存
            groupResp, err := s.groupClient.GetGroup(ctx, &group_pb.GetGroupRequest{GroupId: req.ReceiverID})
            if err != nil {
                if st, ok := status.FromError(err); ok && st.Code() == codes.NotFound {
                    return nil, status.Error(codes.NotFound, "group not found")
                }
                return nil, status.Error(codes.Internal, "get group info failed")
            }
            groupInfo = groupResp.Group
            // 异步回填本地缓存
            go s.backfillGroupCache(context.Background(), req.ReceiverID, groupInfo)
        } else {
            groupInfo = parseGroupInfoFromCache(groupData)
        }

        // 检查群状态
        if groupInfo.Status == common.GROUP_STATUS_DISBANDED {
            return nil, status.Error(codes.FailedPrecondition, "group has been disbanded")
        }

        // 检查发送者是否为群成员 — 本地缓存优先
        memberCacheKey := fmt.Sprintf("msg:local:group_member:%s:%s", req.ReceiverID, req.SenderID)
        memberData, err := s.redis.HGetAll(ctx, memberCacheKey).Result()
        var memberResp *group_pb.GetGroupMemberResponse
        if err != nil || len(memberData) == 0 {
            // 缓存未命中 → 降级 RPC + 回填缓存
            memberResp, err = s.groupClient.GetGroupMember(ctx, &group_pb.GetGroupMemberRequest{
                GroupId: req.ReceiverID,
                UserId:  req.SenderID,
            })
            if err != nil {
                if st, ok := status.FromError(err); ok && st.Code() == codes.NotFound {
                    return nil, status.Error(codes.PermissionDenied, "you are not a member of this group")
                }
                return nil, status.Error(codes.Internal, "get group member failed")
            }
            // 异步回填本地缓存
            go s.backfillGroupMemberCache(context.Background(), req.ReceiverID, req.SenderID, memberResp.Member)
        } else {
            // 从本地缓存构造 memberResp
            memberResp = &group_pb.GetGroupMemberResponse{
                Member: parseGroupMemberFromCache(memberData),
            }
        }

        // 检查全员禁言状态（群主和管理员不受限制）
        if groupInfo.Status == common.GROUP_STATUS_MUTED {
            if memberResp.Member.Role == common.GROUP_MEMBER_ROLE_MEMBER {
                return nil, status.Error(codes.PermissionDenied, "group is muted, only admins can send messages")
            }
        }

        // 检查个人禁言状态
        if memberResp.Member.IsMuted {
            if memberResp.Member.MuteUntil > time.Now().UnixMilli() {
                return nil, status.Error(codes.PermissionDenied, "you are muted in this group")
            }
        }

        groupMemberCount = groupInfo.MemberCount

        // 获取或创建群聊通道
        channelResp, err := s.conversationClient.GetOrCreateChannel(ctx, &conv_pb.GetOrCreateChannelRequest{
            Type:    common.CONVERSATION_TYPE_GROUP,
            GroupId: req.ReceiverID,
        })
        if err != nil {
            return nil, status.Error(codes.Internal, "get or create group channel failed")
        }
        channelID = channelResp.Channel.ConversationId

    default:
        return nil, status.Error(codes.InvalidArgument, "unsupported conversation type")
    }

    // ==================== 5. 分配 seq — RPC Conversation.IncrChannelSeq ====================
    // 这是最热的跨服务调用，Conversation 通过 Redis INCRBY 原子递增
    seqResp, err := s.conversationClient.IncrChannelSeq(ctx, &conv_pb.IncrChannelSeqRequest{
        ConversationId: channelID,
        Count:          1,
    })
    if err != nil {
        return nil, status.Error(codes.Internal, "allocate message seq failed")
    }
    seq := seqResp.StartSeq // 分配到的 seq

    // ==================== 6. 生成 msg_id — Snowflake ====================
    msgID := s.snowflake.Generate().String()
    now := time.Now().UnixMilli()
    sendTime := req.Body.SendTime
    if sendTime == 0 {
        sendTime = now
    }

    // ==================== 7. 构造消息内容 JSON ====================
    contentJSON, err := json.Marshal(map[string]interface{}{
        "type":       int(req.Body.Type),
        "text":       req.Body.Text,
        "extra":      req.Body.Extra,
        "at_all":     req.Body.AtAll,
        "quote":      req.Body.Quote,
        "merge":      req.Body.Merge,
    })
    if err != nil {
        return nil, status.Error(codes.Internal, "marshal message content failed")
    }

    // 媒体信息 JSON
    var mediaURL string
    var mediaInfoJSON []byte
    if req.Body.Media != nil {
        mediaURL = req.Body.Media.Url
        mediaInfoJSON, _ = json.Marshal(req.Body.Media)
    }
    if mediaInfoJSON == nil {
        mediaInfoJSON = []byte("{}")
    }

    // ==================== 8. 持久化存储 — PgSQL ====================
    // ⚠️ MED-03 性能说明：单条 INSERT 模式在 50 万/s 写入压力下可能成为瓶颈。
    // 生产环境建议搭配以下策略之一（按推荐优先级）：
    //   1. Citus 分布式表 — 自动分片到多个 Worker 节点，单条 INSERT 延迟 < 2ms
    //   2. 批量缓冲模式 — 内存攒 10ms 或 100 条消息后批量 INSERT（需配合 seq 预分配）
    //      内部使用 Ring Buffer + time.Ticker 实现 micro-batching
    //   3. PgSQL COPY 协议 — 流式写入，吞吐量是 INSERT 的 5~10 倍（适合群消息扇出场景）
    // 当前保留单条 INSERT 以保证最简实现，Citus 部署后可直接承载（无需改代码）。
    _, err = s.db.ExecContext(ctx,
        `INSERT INTO messages (
            msg_id, client_msg_id, channel_id, channel_type, sender_id,
            msg_type, content, media_url, media_info, at_user_ids,
            at_all, quote_msg_id, merge_msg_ids, status, seq,
            send_time, server_time
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)`,
        msgID,                                   // $1: msg_id
        req.ClientMsgID,                         // $2: client_msg_id
        channelID,                               // $3: channel_id
        int(req.ConversationType),               // $4: channel_type
        req.SenderID,                            // $5: sender_id
        int(req.Body.Type),                      // $6: msg_type
        contentJSON,                             // $7: content JSONB
        mediaURL,                                // $8: media_url
        mediaInfoJSON,                           // $9: media_info JSONB
        pq.Array(req.Body.AtUserIDs),            // $10: at_user_ids TEXT[]
        req.Body.AtAll,                          // $11: at_all
        getQuoteMsgID(req.Body.Quote),           // $12: quote_msg_id
        pq.Array(getMergeMsgIDs(req.Body.Merge)),// $13: merge_msg_ids TEXT[]
        1,                                       // $14: status = NORMAL
        seq,                                     // $15: seq
        sendTime,                                // $16: send_time
        now,                                     // $17: server_time
    )
    if err != nil {
        if isPgUniqueViolation(err) {
            // client_msg_id 唯一冲突 → 并发去重兜底
            var conflictMsgID string
            s.db.QueryRowContext(ctx,
                `SELECT msg_id FROM messages WHERE client_msg_id = $1`, req.ClientMsgID,
            ).Scan(&conflictMsgID)
            if conflictMsgID != "" {
                s.redis.Set(ctx, dedupKey, conflictMsgID, 24*time.Hour)
                return &pb.SendMessageResponse{
                    Meta:       successMeta(ctx),
                    MsgID:      conflictMsgID,
                    Seq:        seq,
                    ServerTime: now,
                    Duplicate:  true,
                }, nil
            }
        }
        return nil, status.Error(codes.Internal, "insert message to db failed: "+err.Error())
    }

    // ==================== 9. 写入 Redis 缓存 ====================
    pipe := s.redis.Pipeline()

    // 9a. 设置幂等去重 key（24h TTL）
    pipe.Set(ctx, dedupKey, msgID, 24*time.Hour)

    // 9b. 缓存消息详情（1h TTL）
    msgDetailJSON, _ := json.Marshal(&pb.ChatMessage{
        MsgId:            msgID,
        ClientMsgId:      req.ClientMsgID,
        ConversationId:   channelID,
        ConversationType: req.ConversationType,
        SenderId:         req.SenderID,
        ReceiverId:       req.ReceiverID,
        Body:             req.Body,
        Status:           common.MESSAGE_STATUS_SENT,
        Seq:              seq,
        SendTime:         sendTime,
        ServerTime:       now,
    })
    detailKey := fmt.Sprintf("msg:detail:%s", msgID)
    pipe.Set(ctx, detailKey, string(msgDetailJSON), 1*time.Hour)

    // 9c. 写入近期消息 ZSET（用于 PullMessages 快速读取）
    recentKey := fmt.Sprintf("msg:recent:%s", channelID)
    pipe.ZAdd(ctx, recentKey, &redis.Z{
        Score:  float64(seq),
        Member: msgID,
    })
    pipe.Expire(ctx, recentKey, 30*time.Minute)
    // 保持 ZSET 最多 200 条，裁剪旧数据
    pipe.ZRemRangeByRank(ctx, recentKey, 0, -201)

    if _, err = pipe.Exec(ctx); err != nil {
        // Redis 写失败不影响主流程，仅记录日志
        log.Warn("redis cache write failed after message stored",
            "msg_id", msgID, "channel_id", channelID, "err", err)
    }

    // ==================== 10. 更新会话最后一条消息 — RPC Conversation ====================
    contentPreview := buildContentPreview(req.Body) // 截断到 100 字符
    go func() {
        _, err := s.conversationClient.UpdateChannelLastMsg(context.Background(),
            &conv_pb.UpdateChannelLastMsgRequest{
                ConversationId: channelID,
                MsgId:          msgID,
                Seq:            seq,
                SenderId:       req.SenderID,
                Preview:        contentPreview,
                SendTime:       now,
            })
        if err != nil {
            log.Error("update channel last msg failed",
                "channel_id", channelID, "msg_id", msgID, "err", err)
        }
    }()

    // ==================== 11. 投递 Kafka 事件 ====================
    // 11a. msg.stored 事件（Push / Conversation / Search / OfflineQueue 消费）
    storedEvent := &kafka_msg.MsgStoredEvent{
        Header:            buildEventHeader("message", s.instanceID),
        MsgId:             msgID,
        ClientMsgId:       req.ClientMsgID,
        ConversationId:    channelID,
        ConversationType:  req.ConversationType,
        SenderId:          req.SenderID,
        ReceiverId:        req.ReceiverID,
        MsgType:           req.Body.Type,
        ContentPreview:    contentPreview,
        Status:            common.MESSAGE_STATUS_SENT,
        Seq:               seq,
        SendTime:          sendTime,
        ServerTime:        now,
        SenderNickname:    senderInfo.Nickname,
        SenderAvatar:      senderInfo.AvatarUrl,
        HasAt:             len(req.Body.AtUserIDs) > 0,
        AtUserIds:         req.Body.AtUserIDs,
        HasAtAll:          req.Body.AtAll,
        Extra:             req.Extra,
    }

    // 按会话类型投递不同 Topic
    var storedTopic string
    if req.ConversationType == common.CONVERSATION_TYPE_SINGLE {
        storedTopic = "msg.stored.single"
    } else if req.ConversationType == common.CONVERSATION_TYPE_GROUP {
        storedTopic = "msg.stored.group"
        storedEvent.GroupId = req.ReceiverID
        storedEvent.GroupMemberCount = groupMemberCount
    }

    if err := s.kafka.Produce(ctx, storedTopic, channelID, storedEvent); err != nil {
        log.Error("produce msg.stored event failed",
            "topic", storedTopic, "msg_id", msgID, "err", err)
        // Kafka 投递失败不回滚消息（消息已持久化），通过补偿机制处理
        // 可考虑写入本地 outbox 表，由定时任务补偿投递
    }

    // 11b. 会话更新事件（通知 Conversation 服务更新用户会话列表/未读数）
    convUpdateEvent := &kafka_msg.MsgConversationUpdateEvent{
        Header:           buildEventHeader("message", s.instanceID),
        ConversationId:   channelID,
        ConversationType: req.ConversationType,
        MsgId:            msgID,
        ContentPreview:   contentPreview,
        MessageType:      req.Body.Type,
        SenderId:         req.SenderID,
        SenderNickname:   senderInfo.Nickname,
        Seq:              seq,
        SendTime:         now,
        UpdateType:       "new_msg",
    }
    // 填充受影响的用户 ID
    if req.ConversationType == common.CONVERSATION_TYPE_SINGLE {
        convUpdateEvent.AffectedUserIds = []string{req.SenderID, req.ReceiverID}
    }
    // 群聊不在此填充（由 Conversation 服务自行查群成员）
    s.kafka.Produce(ctx, "msg.conversation.update", channelID, convUpdateEvent)

    // 11c. 内容审核请求（异步，不阻塞发送流程）
    if s.config.ModerationEnabled && needModeration(req.Body) {
        moderationEvent := &kafka_msg.MsgModerationRequestEvent{
            Header:           buildEventHeader("message", s.instanceID),
            MsgId:            msgID,
            SenderId:         req.SenderID,
            ReceiverId:       req.ReceiverID,
            ConversationType: req.ConversationType,
            ContentType:      getContentType(req.Body),
            TextContent:      req.Body.Text,
            MediaUrl:         mediaURL,
            SendTime:         now,
        }
        if req.Body.Media != nil {
            moderationEvent.MediaId = req.Body.Media.MediaId
        }
        s.kafka.Produce(ctx, "msg.moderation.request", msgID, moderationEvent)
    }

    // 11d. 搜索索引事件
    searchEvent := &kafka_msg.MsgSearchIndexEvent{
        Header:           buildEventHeader("message", s.instanceID),
        Action:           "create",
        MsgId:            msgID,
        ConversationId:   channelID,
        ConversationType: req.ConversationType,
        SenderId:         req.SenderID,
        SenderNickname:   senderInfo.Nickname,
        MessageType:      req.Body.Type,
        TextContent:      req.Body.Text,
        SendTime:         now,
        AtUserIds:        req.Body.AtUserIDs,
    }
    if req.Body.Media != nil {
        searchEvent.FileName = req.Body.Media.FileName
    }
    if req.ConversationType == common.CONVERSATION_TYPE_GROUP {
        searchEvent.GroupId = req.ReceiverID
    }
    s.kafka.Produce(ctx, "msg.search.index", msgID, searchEvent)

    // ==================== 12. 返回 ====================
    return &pb.SendMessageResponse{
        Meta:       successMeta(ctx),
        MsgID:      msgID,
        Seq:        seq,
        ServerTime: now,
        Duplicate:  false,
    }, nil
}

// buildContentPreview 构建消息内容预览（截断到 100 字符）
func buildContentPreview(body *pb.MessageBody) string {
    switch body.Type {
    case common.MESSAGE_TYPE_TEXT:
        if len(body.Text) > 100 {
            return body.Text[:100] + "..."
        }
        return body.Text
    case common.MESSAGE_TYPE_IMAGE:
        return "[图片]"
    case common.MESSAGE_TYPE_VIDEO:
        return "[视频]"
    case common.MESSAGE_TYPE_VOICE:
        return "[语音]"
    case common.MESSAGE_TYPE_FILE:
        return "[文件]"
    case common.MESSAGE_TYPE_LOCATION:
        return "[位置]"
    case common.MESSAGE_TYPE_CARD:
        return "[名片]"
    case common.MESSAGE_TYPE_MERGE:
        return "[聊天记录]"
    case common.MESSAGE_TYPE_RECALL:
        return "[消息已撤回]"
    case common.MESSAGE_TYPE_SYSTEM:
        return "[系统消息]"
    default:
        return "[消息]"
    }
}

// needModeration 判断消息是否需要内容审核
func needModeration(body *pb.MessageBody) bool {
    switch body.Type {
    case common.MESSAGE_TYPE_TEXT, common.MESSAGE_TYPE_IMAGE,
         common.MESSAGE_TYPE_VIDEO, common.MESSAGE_TYPE_FILE:
        return true
    default:
        return false
    }
}

// getContentType 获取审核内容类型
func getContentType(body *pb.MessageBody) string {
    switch body.Type {
    case common.MESSAGE_TYPE_TEXT:
        return "text"
    case common.MESSAGE_TYPE_IMAGE:
        return "image"
    case common.MESSAGE_TYPE_VIDEO:
        return "video"
    case common.MESSAGE_TYPE_FILE:
        return "file"
    default:
        return "text"
    }
}

// getQuoteMsgID 提取引用消息 ID
func getQuoteMsgID(quote *pb.QuoteInfo) string {
    if quote != nil {
        return quote.MsgId
    }
    return ""
}

// getMergeMsgIDs 提取合并转发消息 ID 列表
func getMergeMsgIDs(merge *pb.MergeForwardInfo) []string {
    if merge != nil && merge.DetailMsgId != "" {
        return []string{merge.DetailMsgId}
    }
    return nil
}
```

### 2. PullMessages — 拉取消息（notify+sync 核心）

> 客户端收到 Push 服务的轻量 sync notify（仅含 conversation_id + max_seq）后，调用此接口拉取 last_seq 之后的新消息。

```go
func (s *MessageService) PullMessages(ctx context.Context, req *pb.PullMessagesRequest) (*pb.PullMessagesResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id is required")
    }
    if req.ConversationId == "" {
        return nil, status.Error(codes.InvalidArgument, "conversation_id is required")
    }
    if req.LastSeq < 0 {
        return nil, status.Error(codes.InvalidArgument, "last_seq must be >= 0")
    }
    count := int32(20) // 默认拉取 20 条
    if req.Count > 0 {
        count = req.Count
    }
    if count > 100 {
        count = 100 // 上限 100 条
    }

    // ==================== 2. 获取用户的 clear_seq（清空聊天记录的截断点） ====================
    // 用户可能已清空过该会话消息，需要从 clear_seq 之后开始拉取
    // 注：clear_seq 属于 Conversation 服务域数据，通过 RPC 查询而非直读 DB
    clearSeq := int64(0)
    convResp, err := s.conversationClient.GetUserConversation(ctx, &conv_pb.GetUserConversationRequest{
        UserId:         req.UserId,
        ConversationId: req.ConversationId,
    })
    if err == nil && convResp.Conversation != nil {
        clearSeq = convResp.Conversation.ClearSeq
    } else if err != nil {
        // 不存在或查询失败，默认 0（允许拉取全部消息）
        log.Warn("query clear_seq via RPC failed", "user_id", req.UserId, "err", err)
    }

    // 实际起始 seq = max(last_seq, clear_seq)
    effectiveLastSeq := req.LastSeq
    if clearSeq > effectiveLastSeq {
        effectiveLastSeq = clearSeq
    }

    // ==================== 3. 优先从 Redis ZSET 拉取近期消息 ====================
    recentKey := fmt.Sprintf("msg:recent:%s", req.ConversationId)
    // 使用 ZRANGEBYSCORE 拉取 seq > effectiveLastSeq 的消息 ID
    msgIDs, err := s.redis.ZRangeByScore(ctx, recentKey, &redis.ZRangeBy{
        Min:    fmt.Sprintf("(%d", effectiveLastSeq), // 开区间，不包含 last_seq
        Max:    "+inf",
        Offset: 0,
        Count:  int64(count + 1), // 多取一条判断 has_more
    }).Result()

    var messages []*pb.ChatMessage
    var hasMore bool
    cacheHit := err == nil && len(msgIDs) > 0

    if cacheHit {
        // 3a. 从 Redis 缓存批量获取消息详情
        if len(msgIDs) > int(count) {
            hasMore = true
            msgIDs = msgIDs[:count]
        }

        pipe := s.redis.Pipeline()
        cmds := make([]*redis.StringCmd, len(msgIDs))
        for i, mid := range msgIDs {
            cmds[i] = pipe.Get(ctx, fmt.Sprintf("msg:detail:%s", mid))
        }
        pipe.Exec(ctx)

        // 收集缓存命中的消息 + 缓存未命中的 msg_id
        missedMsgIDs := make([]string, 0)
        for i, cmd := range cmds {
            val, err := cmd.Result()
            if err == nil && val != "" {
                var msg pb.ChatMessage
                if json.Unmarshal([]byte(val), &msg) == nil {
                    messages = append(messages, &msg)
                    continue
                }
            }
            missedMsgIDs = append(missedMsgIDs, msgIDs[i])
        }

        // 3b. 对缓存未命中的消息回查 PgSQL
        if len(missedMsgIDs) > 0 {
            dbMsgs, err := s.getMessagesByMsgIDs(ctx, missedMsgIDs)
            if err == nil {
                messages = append(messages, dbMsgs...)
                // 异步回填 Redis 缓存
                go s.cacheMessages(context.Background(), dbMsgs)
            }
        }
    }

    // ==================== 4. Redis 未命中 → 回查 PgSQL ====================
    if !cacheHit {
        rows, err := s.db.QueryContext(ctx,
            `SELECT msg_id, client_msg_id, channel_id, channel_type, sender_id,
                    msg_type, content, media_url, media_info, at_user_ids,
                    at_all, quote_msg_id, status, seq, send_time, server_time
             FROM messages
             WHERE channel_id = $1 AND seq > $2 AND status != 4
             ORDER BY seq ASC
             LIMIT $3`,
            req.ConversationId, effectiveLastSeq, count+1,
        )
        if err != nil {
            return nil, status.Error(codes.Internal, "pull messages from db failed")
        }
        defer rows.Close()

        for rows.Next() {
            msg, err := scanChatMessage(rows)
            if err != nil {
                continue
            }
            messages = append(messages, msg)
        }

        if len(messages) > int(count) {
            hasMore = true
            messages = messages[:count]
        }

        // 异步回填 Redis 缓存
        go func() {
            s.cacheMessages(context.Background(), messages)
            s.cacheRecentMessages(context.Background(), req.ConversationId, messages)
        }()
    }

    // ==================== 5. 过滤用户已删除的消息 ====================
    if len(messages) > 0 {
        msgIDList := make([]string, len(messages))
        for i, m := range messages {
            msgIDList[i] = m.MsgId
        }

        deletedSet, err := s.getUserDeletedMsgIDs(ctx, req.UserId, msgIDList)
        if err == nil && len(deletedSet) > 0 {
            filtered := make([]*pb.ChatMessage, 0, len(messages))
            for _, m := range messages {
                if !deletedSet[m.MsgId] {
                    filtered = append(filtered, m)
                }
            }
            messages = filtered
        }
    }

    // ==================== 6. 排序保证（按 seq 升序） ====================
    sort.Slice(messages, func(i, j int) bool {
        return messages[i].Seq < messages[j].Seq
    })

    // ==================== 7. 获取当前通道最大 seq ====================
    maxSeq := int64(0)
    if len(messages) > 0 {
        maxSeq = messages[len(messages)-1].Seq
    }
    // 通过 Conversation 服务 RPC 查询实时 max_seq（不直读 Conversation 的 Redis key）
    maxSeqResp, err := s.conversationClient.GetChannelMaxSeq(ctx, &conv_pb.GetChannelMaxSeqRequest{
        ConversationId: req.ConversationId,
    })
    if err == nil && maxSeqResp.MaxSeq > maxSeq {
        maxSeq = maxSeqResp.MaxSeq
    }

    // ==================== 8. 返回 ====================
    return &pb.PullMessagesResponse{
        Meta:     successMeta(ctx),
        Messages: messages,
        HasMore:  hasMore,
        MaxSeq:   maxSeq,
    }, nil
}

// getUserDeletedMsgIDs 查询用户已删除的消息 ID 集合
func (s *MessageService) getUserDeletedMsgIDs(ctx context.Context, userID string, msgIDs []string) (map[string]bool, error) {
    if len(msgIDs) == 0 {
        return nil, nil
    }
    rows, err := s.db.QueryContext(ctx,
        `SELECT msg_id FROM user_deleted_messages
         WHERE user_id = $1 AND msg_id = ANY($2)`,
        userID, pq.Array(msgIDs),
    )
    if err != nil {
        return nil, err
    }
    defer rows.Close()

    result := make(map[string]bool)
    for rows.Next() {
        var mid string
        rows.Scan(&mid)
        result[mid] = true
    }
    return result, nil
}

// cacheMessages 异步回填消息详情到 Redis
func (s *MessageService) cacheMessages(ctx context.Context, msgs []*pb.ChatMessage) {
    if len(msgs) == 0 {
        return
    }
    pipe := s.redis.Pipeline()
    for _, msg := range msgs {
        data, err := json.Marshal(msg)
        if err != nil {
            continue
        }
        key := fmt.Sprintf("msg:detail:%s", msg.MsgId)
        pipe.Set(ctx, key, string(data), 1*time.Hour)
    }
    if _, err := pipe.Exec(ctx); err != nil {
        log.Warn("batch cache messages failed", "count", len(msgs), "err", err)
    }
}

// cacheRecentMessages 异步回填近期消息到 Redis ZSET
func (s *MessageService) cacheRecentMessages(ctx context.Context, channelID string, msgs []*pb.ChatMessage) {
    if len(msgs) == 0 {
        return
    }
    recentKey := fmt.Sprintf("msg:recent:%s", channelID)
    pipe := s.redis.Pipeline()
    for _, msg := range msgs {
        pipe.ZAdd(ctx, recentKey, &redis.Z{
            Score:  float64(msg.Seq),
            Member: msg.MsgId,
        })
    }
    pipe.Expire(ctx, recentKey, 30*time.Minute)
    pipe.ZRemRangeByRank(ctx, recentKey, 0, -201) // 保持最多 200 条
    pipe.Exec(ctx)
}
```

### 3. GetMessage — 获取单条消息详情

```go
func (s *MessageService) GetMessage(ctx context.Context, req *pb.GetMessageRequest) (*pb.GetMessageResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.MsgId == "" {
        return nil, status.Error(codes.InvalidArgument, "msg_id is required")
    }

    // ==================== 2. 优先读 Redis 缓存 ====================
    detailKey := fmt.Sprintf("msg:detail:%s", req.MsgId)
    cached, err := s.redis.Get(ctx, detailKey).Result()
    if err == nil && cached != "" {
        var msg pb.ChatMessage
        if json.Unmarshal([]byte(cached), &msg) == nil {
            return &pb.GetMessageResponse{
                Meta:    successMeta(ctx),
                Message: &msg,
            }, nil
        }
    }

    // ==================== 3. 缓存未命中 → 读 PgSQL ====================
    msg, err := s.getMessageByMsgID(ctx, req.MsgId)
    if err != nil {
        return nil, err
    }
    if msg == nil {
        return nil, status.Error(codes.NotFound, "message not found")
    }

    // ==================== 4. 回写 Redis 缓存 ====================
    go func() {
        data, err := json.Marshal(msg)
        if err != nil {
            return
        }
        s.redis.Set(context.Background(), detailKey, string(data), 1*time.Hour)
    }()

    // ==================== 5. 返回 ====================
    return &pb.GetMessageResponse{
        Meta:    successMeta(ctx),
        Message: msg,
    }, nil
}

// getMessageByMsgID 通过 msg_id 查询消息（内部复用方法）
func (s *MessageService) getMessageByMsgID(ctx context.Context, msgID string) (*pb.ChatMessage, error) {
    // 先查缓存
    detailKey := fmt.Sprintf("msg:detail:%s", msgID)
    cached, err := s.redis.Get(ctx, detailKey).Result()
    if err == nil && cached != "" {
        var msg pb.ChatMessage
        if json.Unmarshal([]byte(cached), &msg) == nil {
            return &msg, nil
        }
    }

    // 查 PgSQL
    var msg pb.ChatMessage
    var contentJSON, mediaInfoJSON string
    var atUserIDs []string
    var quoteMsgID string
    var statusInt int
    err = s.db.QueryRowContext(ctx,
        `SELECT msg_id, client_msg_id, channel_id, channel_type, sender_id,
                msg_type, content, media_url, media_info, at_user_ids,
                at_all, quote_msg_id, status, seq, send_time, server_time
         FROM messages WHERE msg_id = $1`,
        msgID,
    ).Scan(
        &msg.MsgId, &msg.ClientMsgId, &msg.ConversationId, &msg.ConversationType,
        &msg.SenderId, &msg.Body.Type, &contentJSON, /* media_url */ &msg.Body.Media.Url,
        &mediaInfoJSON, pq.Array(&atUserIDs), /* at_all */ &msg.Body.AtAll,
        &quoteMsgID, &statusInt, &msg.Seq, &msg.SendTime, &msg.ServerTime,
    )
    if err == sql.ErrNoRows {
        return nil, nil
    }
    if err != nil {
        return nil, status.Error(codes.Internal, "query message from db failed")
    }

    // 解析 content JSON → 填充 Body 字段
    parseContentJSON(&msg, contentJSON)
    parseMediaInfoJSON(&msg, mediaInfoJSON)
    msg.Body.AtUserIDs = atUserIDs
    if quoteMsgID != "" {
        msg.Body.Quote = &pb.QuoteInfo{MsgId: quoteMsgID}
    }
    msg.Status = common.MessageStatus(statusInt)

    return &msg, nil
}
```

### 4. GetHistoryMessages — 获取历史消息（基于 seq 游标分页）

```go
func (s *MessageService) GetHistoryMessages(ctx context.Context, req *pb.GetHistoryMessagesRequest) (*pb.GetHistoryMessagesResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id is required")
    }
    if req.ConversationId == "" {
        return nil, status.Error(codes.InvalidArgument, "conversation_id is required")
    }
    count := int32(20)
    if req.Count > 0 {
        count = req.Count
    }
    if count > 100 {
        count = 100
    }

    // ==================== 2. 获取用户的 clear_seq ====================
    // 注：clear_seq 属于 Conversation 服务域数据，通过 RPC 查询
    clearSeq := int64(0)
    convResp, _ := s.conversationClient.GetUserConversation(ctx, &conv_pb.GetUserConversationRequest{
        UserId:         req.UserId,
        ConversationId: req.ConversationId,
    })
    if convResp != nil && convResp.Conversation != nil {
        clearSeq = convResp.Conversation.ClearSeq
    }

    // ==================== 3. 根据方向查询 PgSQL ====================
    var rows *sql.Rows
    var err error

    if req.Reverse {
        // 正向：从 last_seq 向新方向查（从旧到新）
        startSeq := req.LastSeq
        if startSeq < clearSeq {
            startSeq = clearSeq
        }
        rows, err = s.db.QueryContext(ctx,
            `SELECT msg_id, client_msg_id, channel_id, channel_type, sender_id,
                    msg_type, content, media_url, media_info, at_user_ids,
                    at_all, quote_msg_id, status, seq, send_time, server_time
             FROM messages
             WHERE channel_id = $1 AND seq > $2 AND seq > $3 AND status != 4
             ORDER BY seq ASC
             LIMIT $4`,
            req.ConversationId, startSeq, clearSeq, count+1,
        )
    } else {
        // 反向：从 last_seq 向旧方向查（从新到旧，默认历史浏览方向）
        if req.LastSeq == 0 {
            // last_seq=0 表示从最新消息开始
            rows, err = s.db.QueryContext(ctx,
                `SELECT msg_id, client_msg_id, channel_id, channel_type, sender_id,
                        msg_type, content, media_url, media_info, at_user_ids,
                        at_all, quote_msg_id, status, seq, send_time, server_time
                 FROM messages
                 WHERE channel_id = $1 AND seq > $2 AND status != 4
                 ORDER BY seq DESC
                 LIMIT $3`,
                req.ConversationId, clearSeq, count+1,
            )
        } else {
            rows, err = s.db.QueryContext(ctx,
                `SELECT msg_id, client_msg_id, channel_id, channel_type, sender_id,
                        msg_type, content, media_url, media_info, at_user_ids,
                        at_all, quote_msg_id, status, seq, send_time, server_time
                 FROM messages
                 WHERE channel_id = $1 AND seq < $2 AND seq > $3 AND status != 4
                 ORDER BY seq DESC
                 LIMIT $4`,
                req.ConversationId, req.LastSeq, clearSeq, count+1,
            )
        }
    }
    if err != nil {
        return nil, status.Error(codes.Internal, "query history messages failed")
    }
    defer rows.Close()

    // ==================== 4. 扫描结果 ====================
    var messages []*pb.ChatMessage
    for rows.Next() {
        msg, err := scanChatMessage(rows)
        if err != nil {
            continue
        }
        messages = append(messages, msg)
    }

    hasMore := len(messages) > int(count)
    if hasMore {
        messages = messages[:count]
    }

    // ==================== 5. 过滤用户已删除的消息 ====================
    if len(messages) > 0 {
        msgIDList := make([]string, len(messages))
        for i, m := range messages {
            msgIDList[i] = m.MsgId
        }
        deletedSet, err := s.getUserDeletedMsgIDs(ctx, req.UserId, msgIDList)
        if err == nil && len(deletedSet) > 0 {
            filtered := make([]*pb.ChatMessage, 0, len(messages))
            for _, m := range messages {
                if !deletedSet[m.MsgId] {
                    filtered = append(filtered, m)
                }
            }
            messages = filtered
        }
    }

    // ==================== 6. 按 seq 升序排列（无论查询方向，返回给客户端始终升序） ====================
    sort.Slice(messages, func(i, j int) bool {
        return messages[i].Seq < messages[j].Seq
    })

    // ==================== 7. 计算 min_seq / max_seq ====================
    var minSeq, maxSeq int64
    if len(messages) > 0 {
        minSeq = messages[0].Seq
        maxSeq = messages[len(messages)-1].Seq
    }

    // ==================== 8. 异步缓存回填 ====================
    go s.cacheMessages(context.Background(), messages)

    // ==================== 9. 返回 ====================
    return &pb.GetHistoryMessagesResponse{
        Meta:     successMeta(ctx),
        Messages: messages,
        HasMore:  hasMore,
        MinSeq:   minSeq,
        MaxSeq:   maxSeq,
    }, nil
}

// scanChatMessage 从 SQL rows 中扫描出一条 ChatMessage
func scanChatMessage(rows *sql.Rows) (*pb.ChatMessage, error) {
    var msg pb.ChatMessage
    msg.Body = &pb.MessageBody{}
    var contentJSON, mediaInfoJSON, mediaURL string
    var atUserIDs []string
    var atAll bool
    var quoteMsgID string
    var statusInt, channelType, msgType int

    err := rows.Scan(
        &msg.MsgId, &msg.ClientMsgId, &msg.ConversationId, &channelType,
        &msg.SenderId, &msgType, &contentJSON, &mediaURL, &mediaInfoJSON,
        pq.Array(&atUserIDs), &atAll, &quoteMsgID, &statusInt,
        &msg.Seq, &msg.SendTime, &msg.ServerTime,
    )
    if err != nil {
        return nil, err
    }

    msg.ConversationType = common.ConversationType(channelType)
    msg.Body.Type = common.MessageType(msgType)
    msg.Status = common.MessageStatus(statusInt)
    msg.Body.AtUserIDs = atUserIDs
    msg.Body.AtAll = atAll
    parseContentJSON(&msg, contentJSON)
    parseMediaInfoJSON(&msg, mediaInfoJSON)
    if mediaURL != "" && msg.Body.Media == nil {
        msg.Body.Media = &common.MediaInfo{Url: mediaURL}
    } else if mediaURL != "" && msg.Body.Media != nil {
        msg.Body.Media.Url = mediaURL
    }
    if quoteMsgID != "" {
        msg.Body.Quote = &pb.QuoteInfo{MsgId: quoteMsgID}
    }

    return &msg, nil
}
```

### 5. GetMessagesByConversation — 获取会话消息（基于页码分页）

```go
func (s *MessageService) GetMessagesByConversation(ctx context.Context, req *pb.GetMessagesByConversationRequest) (*pb.GetMessagesByConversationResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.ConversationId == "" {
        return nil, status.Error(codes.InvalidArgument, "conversation_id is required")
    }
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id is required")
    }
    page := int32(1)
    pageSize := int32(20)
    if req.Pagination != nil {
        if req.Pagination.Page > 0 {
            page = req.Pagination.Page
        }
        if req.Pagination.PageSize > 0 {
            pageSize = req.Pagination.PageSize
        }
    }
    if pageSize > 100 {
        pageSize = 100
    }
    offset := (page - 1) * pageSize

    // ==================== 2. 获取用户的 clear_seq ====================
    // 注：clear_seq 属于 Conversation 服务域数据，通过 RPC 查询
    clearSeq := int64(0)
    convResp, _ := s.conversationClient.GetUserConversation(ctx, &conv_pb.GetUserConversationRequest{
        UserId:         req.UserId,
        ConversationId: req.ConversationId,
    })
    if convResp != nil && convResp.Conversation != nil {
        clearSeq = convResp.Conversation.ClearSeq
    }
    ).Scan(&clearSeq)

    // ==================== 3. 查询总数 — PgSQL ====================
    var total int64
    err := s.db.QueryRowContext(ctx,
        `SELECT COUNT(*) FROM messages
         WHERE channel_id = $1 AND seq > $2 AND status != 4`,
        req.ConversationId, clearSeq,
    ).Scan(&total)
    if err != nil {
        return nil, status.Error(codes.Internal, "count messages failed")
    }

    // ==================== 4. 分页查询 — PgSQL ====================
    rows, err := s.db.QueryContext(ctx,
        `SELECT msg_id, client_msg_id, channel_id, channel_type, sender_id,
                msg_type, content, media_url, media_info, at_user_ids,
                at_all, quote_msg_id, status, seq, send_time, server_time
         FROM messages
         WHERE channel_id = $1 AND seq > $2 AND status != 4
         ORDER BY seq DESC
         LIMIT $3 OFFSET $4`,
        req.ConversationId, clearSeq, pageSize, offset,
    )
    if err != nil {
        return nil, status.Error(codes.Internal, "query messages by conversation failed")
    }
    defer rows.Close()

    var messages []*pb.ChatMessage
    for rows.Next() {
        msg, err := scanChatMessage(rows)
        if err != nil {
            continue
        }
        messages = append(messages, msg)
    }

    // ==================== 5. 过滤用户已删除的消息 ====================
    if len(messages) > 0 {
        msgIDList := make([]string, len(messages))
        for i, m := range messages {
            msgIDList[i] = m.MsgId
        }
        deletedSet, err := s.getUserDeletedMsgIDs(ctx, req.UserId, msgIDList)
        if err == nil && len(deletedSet) > 0 {
            filtered := make([]*pb.ChatMessage, 0, len(messages))
            for _, m := range messages {
                if !deletedSet[m.MsgId] {
                    filtered = append(filtered, m)
                }
            }
            messages = filtered
        }
    }

    // ==================== 6. 返回 ====================
    totalPages := int32((total + int64(pageSize) - 1) / int64(pageSize))
    return &pb.GetMessagesByConversationResponse{
        Meta:     successMeta(ctx),
        Messages: messages,
        Pagination: &common.PaginationResult{
            Page:       page,
            PageSize:   pageSize,
            Total:      total,
            TotalPages: totalPages,
        },
    }, nil
}
```

### 6. RecallMessage — 撤回消息

> 仅消息发送者可撤回，且必须在时间限制内（默认 2 分钟）。撤回后消息状态变为 RECALLED，原内容被清除。

```go
func (s *MessageService) RecallMessage(ctx context.Context, req *pb.RecallMessageRequest) (*pb.RecallMessageResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.MsgId == "" {
        return nil, status.Error(codes.InvalidArgument, "msg_id is required")
    }
    if req.SenderId == "" {
        return nil, status.Error(codes.InvalidArgument, "sender_id is required")
    }

    // ==================== 2. 查询原始消息 — PgSQL ====================
    var originalMsg struct {
        MsgID     string
        ChannelID string
        ChannelType int
        SenderID  string
        Status    int
        Seq       int64
        ServerTime int64
    }
    err := s.db.QueryRowContext(ctx,
        `SELECT msg_id, channel_id, channel_type, sender_id, status, seq, server_time
         FROM messages WHERE msg_id = $1`,
        req.MsgId,
    ).Scan(&originalMsg.MsgID, &originalMsg.ChannelID, &originalMsg.ChannelType,
        &originalMsg.SenderID, &originalMsg.Status, &originalMsg.Seq, &originalMsg.ServerTime)
    if err == sql.ErrNoRows {
        return nil, status.Error(codes.NotFound, "message not found")
    }
    if err != nil {
        return nil, status.Error(codes.Internal, "query message failed")
    }

    // ==================== 3. 权限校验 ====================
    // 3a. 消息状态校验
    if originalMsg.Status == 2 { // RECALLED
        return nil, status.Error(codes.FailedPrecondition, "message already recalled")
    }
    if originalMsg.Status == 4 { // DELETED
        return nil, status.Error(codes.FailedPrecondition, "message already deleted")
    }

    // 3b. 发送者身份校验
    isOwner := originalMsg.SenderID == req.SenderId
    isGroupAdmin := false

    if !isOwner && originalMsg.ChannelType == 2 {
        // 群聊中管理员/群主也可以撤回消息
        memberResp, err := s.groupClient.GetGroupMember(ctx, &group_pb.GetGroupMemberRequest{
            GroupId: originalMsg.ChannelID,
            UserId:  req.SenderId,
        })
        if err == nil && memberResp.Member != nil {
            if memberResp.Member.Role == common.GROUP_MEMBER_ROLE_OWNER ||
               memberResp.Member.Role == common.GROUP_MEMBER_ROLE_ADMIN {
                isGroupAdmin = true
            }
        }
    }

    if !isOwner && !isGroupAdmin {
        return nil, status.Error(codes.PermissionDenied, "only sender or group admin can recall message")
    }

    // 3c. 时间限制校验（仅对普通发送者生效，管理员不受限）
    if isOwner && !isGroupAdmin {
        recallTimeLimit := int64(s.config.RecallTimeLimitSeconds) * 1000 // 默认 120s = 2min
        elapsed := time.Now().UnixMilli() - originalMsg.ServerTime
        if elapsed > recallTimeLimit {
            return nil, status.Error(codes.FailedPrecondition,
                fmt.Sprintf("recall time exceeded, limit is %d seconds", s.config.RecallTimeLimitSeconds))
        }
    }

    now := time.Now().UnixMilli()

    // ==================== 4. 更新消息状态 — PgSQL ====================
    result, err := s.db.ExecContext(ctx,
        `UPDATE messages SET status = 2, content = '{"type":9,"text":"消息已撤回"}',
                media_url = '', media_info = '{}', updated_at = NOW()
         WHERE msg_id = $1 AND status = 1`,
        req.MsgId,
    )
    if err != nil {
        return nil, status.Error(codes.Internal, "update message status failed")
    }
    rowsAffected, _ := result.RowsAffected()
    if rowsAffected == 0 {
        return nil, status.Error(codes.FailedPrecondition, "message cannot be recalled (status changed)")
    }

    // ==================== 5. 清理 Redis 缓存（延迟双删策略） ====================
    // 第一次删除：立即清除
    detailCacheKey := fmt.Sprintf("msg:detail:%s", req.MsgId)
    s.redis.Del(ctx, detailCacheKey)
    // 延迟双删：防止在 DB 更新与缓存删除之间有并发读把旧值回填
    go s.delayedDoubleDelete(context.Background(), detailCacheKey, 500*time.Millisecond)

    // ==================== 6. 投递 Kafka 事件: msg.recalled ====================
    recallEvent := &kafka_msg.MsgRecalledEvent{
        Header:           buildEventHeader("message", s.instanceID),
        MsgId:            req.MsgId,
        ConversationId:   originalMsg.ChannelID,
        ConversationType: common.ConversationType(originalMsg.ChannelType),
        SenderId:         req.SenderId,
        OriginalSeq:      originalMsg.Seq,
        RecallTime:       now,
    }

    // 群聊时填充 group_id
    if originalMsg.ChannelType == 2 {
        recallEvent.GroupId = originalMsg.ChannelID
    }

    if err := s.kafka.Produce(ctx, "msg.recalled", originalMsg.ChannelID, recallEvent); err != nil {
        log.Error("produce msg.recalled event failed", "msg_id", req.MsgId, "err", err)
    }

    // 6b. 会话更新事件（撤回后更新会话预览）
    convUpdateEvent := &kafka_msg.MsgConversationUpdateEvent{
        Header:           buildEventHeader("message", s.instanceID),
        ConversationId:   originalMsg.ChannelID,
        ConversationType: common.ConversationType(originalMsg.ChannelType),
        MsgId:            req.MsgId,
        ContentPreview:   "[消息已撤回]",
        MessageType:      common.MESSAGE_TYPE_RECALL,
        SenderId:         req.SenderId,
        Seq:              originalMsg.Seq,
        SendTime:         now,
        UpdateType:       "recall",
    }
    s.kafka.Produce(ctx, "msg.conversation.update", originalMsg.ChannelID, convUpdateEvent)

    // 6c. 搜索索引删除
    searchEvent := &kafka_msg.MsgSearchIndexEvent{
        Header:           buildEventHeader("message", s.instanceID),
        Action:           "delete",
        MsgId:            req.MsgId,
        ConversationId:   originalMsg.ChannelID,
        ConversationType: common.ConversationType(originalMsg.ChannelType),
    }
    s.kafka.Produce(ctx, "msg.search.index", req.MsgId, searchEvent)

    // ==================== 7. 返回 ====================
    return &pb.RecallMessageResponse{
        Meta: successMeta(ctx),
    }, nil
}
```

### 7. DeleteMessage — 删除消息（仅对自己不可见）

> 软删除：在 user_deleted_messages 表写入记录，后续查询时过滤。不影响其他用户看到该消息。

```go
func (s *MessageService) DeleteMessage(ctx context.Context, req *pb.DeleteMessageRequest) (*pb.DeleteMessageResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id is required")
    }
    if len(req.MsgIds) == 0 {
        return nil, status.Error(codes.InvalidArgument, "msg_ids is required")
    }
    if len(req.MsgIds) > 100 {
        return nil, status.Error(codes.InvalidArgument, "too many msg_ids, max 100")
    }

    // ==================== 2. 校验消息存在性 — PgSQL ====================
    rows, err := s.db.QueryContext(ctx,
        `SELECT msg_id, channel_id FROM messages WHERE msg_id = ANY($1)`,
        pq.Array(req.MsgIds),
    )
    if err != nil {
        return nil, status.Error(codes.Internal, "query messages failed")
    }
    defer rows.Close()

    existingMsgIDs := make([]string, 0, len(req.MsgIds))
    var channelID string
    for rows.Next() {
        var mid, cid string
        rows.Scan(&mid, &cid)
        existingMsgIDs = append(existingMsgIDs, mid)
        channelID = cid
    }
    if len(existingMsgIDs) == 0 {
        return nil, status.Error(codes.NotFound, "none of the messages found")
    }

    // ==================== 3. 批量写入软删除记录 — PgSQL ====================
    now := time.Now()
    tx, err := s.db.BeginTx(ctx, nil)
    if err != nil {
        return nil, status.Error(codes.Internal, "begin tx failed")
    }
    defer tx.Rollback()

    for _, mid := range existingMsgIDs {
        _, err = tx.ExecContext(ctx,
            `INSERT INTO user_deleted_messages (user_id, msg_id, deleted_at)
             VALUES ($1, $2, $3)
             ON CONFLICT (user_id, msg_id) DO NOTHING`,
            req.UserId, mid, now,
        )
        if err != nil {
            return nil, status.Error(codes.Internal, "insert deleted record failed")
        }
    }

    if err = tx.Commit(); err != nil {
        return nil, status.Error(codes.Internal, "commit tx failed")
    }

    // ==================== 4. 投递 Kafka 事件: msg.deleted ====================
    deleteEvent := &kafka_msg.MsgDeletedEvent{
        Header:         buildEventHeader("message", s.instanceID),
        UserId:         req.UserId,
        MsgIds:         existingMsgIDs,
        ConversationId: channelID,
        DeleteTime:     now.UnixMilli(),
    }
    if err := s.kafka.Produce(ctx, "msg.deleted", req.UserId, deleteEvent); err != nil {
        log.Error("produce msg.deleted event failed", "user_id", req.UserId, "err", err)
    }

    // ==================== 5. 搜索索引更新（标记为该用户已删除） ====================
    for _, mid := range existingMsgIDs {
        searchEvent := &kafka_msg.MsgSearchIndexEvent{
            Header: buildEventHeader("message", s.instanceID),
            Action: "update",
            MsgId:  mid,
        }
        s.kafka.Produce(ctx, "msg.search.index", mid, searchEvent)
    }

    // ==================== 6. 返回 ====================
    return &pb.DeleteMessageResponse{
        Meta: successMeta(ctx),
    }, nil
}
```

### 8. ForwardMessage — 转发消息到指定会话

> 将一条已有消息转发到一个或多个目标会话。每个目标会话生成一条新消息（新 msg_id、新 seq）。

```go
func (s *MessageService) ForwardMessage(ctx context.Context, req *pb.ForwardMessageRequest) (*pb.ForwardMessageResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.SenderId == "" {
        return nil, status.Error(codes.InvalidArgument, "sender_id is required")
    }
    if req.OriginalMsgId == "" {
        return nil, status.Error(codes.InvalidArgument, "original_msg_id is required")
    }
    if len(req.TargetIds) == 0 {
        return nil, status.Error(codes.InvalidArgument, "target_ids is required")
    }
    if len(req.TargetIds) > 20 {
        return nil, status.Error(codes.InvalidArgument, "too many targets, max 20")
    }
    if req.TargetType == common.CONVERSATION_TYPE_UNSPECIFIED {
        return nil, status.Error(codes.InvalidArgument, "target_type is required")
    }

    // ==================== 2. 查询原始消息 ====================
    originalMsg, err := s.getMessageByMsgID(ctx, req.OriginalMsgId)
    if err != nil {
        return nil, status.Error(codes.Internal, "get original message failed")
    }
    if originalMsg == nil {
        return nil, status.Error(codes.NotFound, "original message not found")
    }
    if originalMsg.Status == common.MESSAGE_STATUS_RECALLED {
        return nil, status.Error(codes.FailedPrecondition, "cannot forward recalled message")
    }

    // ==================== 3. 获取发送者信息 ====================
    senderResp, err := s.userClient.GetUser(ctx, &user_pb.GetUserRequest{UserId: req.SenderId})
    if err != nil {
        return nil, status.Error(codes.Internal, "get sender info failed")
    }

    // ==================== 4. 逐个目标发送转发消息 ====================
    newMsgIDs := make([]string, 0, len(req.TargetIds))
    now := time.Now().UnixMilli()

    for _, targetID := range req.TargetIds {
        // 4a. 获取或创建目标会话通道
        var channelResp *conv_pb.GetOrCreateChannelResponse
        if req.TargetType == common.CONVERSATION_TYPE_SINGLE {
            channelResp, err = s.conversationClient.GetOrCreateChannel(ctx, &conv_pb.GetOrCreateChannelRequest{
                Type:      common.CONVERSATION_TYPE_SINGLE,
                MemberIds: []string{req.SenderId, targetID},
            })
        } else if req.TargetType == common.CONVERSATION_TYPE_GROUP {
            channelResp, err = s.conversationClient.GetOrCreateChannel(ctx, &conv_pb.GetOrCreateChannelRequest{
                Type:    common.CONVERSATION_TYPE_GROUP,
                GroupId: targetID,
            })
        }
        if err != nil {
            log.Error("get or create channel for forward failed",
                "target_id", targetID, "err", err)
            continue
        }
        targetChannelID := channelResp.Channel.ConversationId

        // 4b. 分配 seq
        seqResp, err := s.conversationClient.IncrChannelSeq(ctx, &conv_pb.IncrChannelSeqRequest{
            ConversationId: targetChannelID,
            Count:          1,
        })
        if err != nil {
            log.Error("alloc seq for forward failed", "target_id", targetID, "err", err)
            continue
        }

        // 4c. 生成新 msg_id
        newMsgID := s.snowflake.Generate().String()
        clientMsgID := fmt.Sprintf("fwd_%s_%s_%s", req.OriginalMsgId, targetID, newMsgID)

        // 4d. 复制原始消息内容，构造新消息写入 PgSQL
        contentJSON, _ := json.Marshal(map[string]interface{}{
            "type":         int(originalMsg.Body.Type),
            "text":         originalMsg.Body.Text,
            "extra":        originalMsg.Body.Extra,
            "forwarded_from": req.OriginalMsgId,
        })
        var mediaURL string
        var mediaInfoJSON []byte
        if originalMsg.Body.Media != nil {
            mediaURL = originalMsg.Body.Media.Url
            mediaInfoJSON, _ = json.Marshal(originalMsg.Body.Media)
        }
        if mediaInfoJSON == nil {
            mediaInfoJSON = []byte("{}")
        }

        _, err = s.db.ExecContext(ctx,
            `INSERT INTO messages (
                msg_id, client_msg_id, channel_id, channel_type, sender_id,
                msg_type, content, media_url, media_info, at_user_ids,
                at_all, quote_msg_id, merge_msg_ids, status, seq, send_time, server_time
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, '{}', false, '', '{}', 1, $10, $11, $12)`,
            newMsgID, clientMsgID, targetChannelID, int(req.TargetType),
            req.SenderId, int(originalMsg.Body.Type), contentJSON,
            mediaURL, mediaInfoJSON, seqResp.StartSeq, now, now,
        )
        if err != nil {
            log.Error("insert forwarded message failed",
                "target_id", targetID, "new_msg_id", newMsgID, "err", err)
            continue
        }

        newMsgIDs = append(newMsgIDs, newMsgID)

        // 4e. 投递 msg.stored 事件（与 SendMessage 相同的下游处理）
        storedEvent := &kafka_msg.MsgStoredEvent{
            Header:            buildEventHeader("message", s.instanceID),
            MsgId:             newMsgID,
            ClientMsgId:       clientMsgID,
            ConversationId:    targetChannelID,
            ConversationType:  req.TargetType,
            SenderId:          req.SenderId,
            ReceiverId:        targetID,
            MsgType:           originalMsg.Body.Type,
            ContentPreview:    buildContentPreview(originalMsg.Body),
            Status:            common.MESSAGE_STATUS_SENT,
            Seq:               seqResp.StartSeq,
            SendTime:          now,
            ServerTime:        now,
            SenderNickname:    senderResp.User.Nickname,
            SenderAvatar:      senderResp.User.AvatarUrl,
        }
        var topic string
        if req.TargetType == common.CONVERSATION_TYPE_SINGLE {
            topic = "msg.stored.single"
        } else {
            topic = "msg.stored.group"
            storedEvent.GroupId = targetID
        }
        s.kafka.Produce(ctx, topic, targetChannelID, storedEvent)

        // 4f. 更新会话最后消息
        s.conversationClient.UpdateChannelLastMsg(ctx, &conv_pb.UpdateChannelLastMsgRequest{
            ConversationId: targetChannelID,
            MsgId:          newMsgID,
            Seq:            seqResp.StartSeq,
            SenderId:       req.SenderId,
            Preview:        buildContentPreview(originalMsg.Body),
            SendTime:       now,
        })
    }

    // ==================== 5. 投递转发事件 ====================
    forwardEvent := &kafka_msg.MsgForwardedEvent{
        Header:        buildEventHeader("message", s.instanceID),
        SenderId:      req.SenderId,
        OriginalMsgId: req.OriginalMsgId,
        ForwardTime:   now,
    }
    s.kafka.Produce(ctx, "msg.forwarded", req.OriginalMsgId, forwardEvent)

    // ==================== 6. 返回 ====================
    return &pb.ForwardMessageResponse{
        Meta:   successMeta(ctx),
        MsgIds: newMsgIDs,
    }, nil
}
```

### 9. MergeForwardMessage — 合并转发消息

> 将多条消息合并为一条「聊天记录」消息，发送到目标会话。  
> 合并消息本身存储完整原始消息列表，客户端展示摘要，点击可展开查看完整记录。

```go
func (s *MessageService) MergeForwardMessage(ctx context.Context, req *pb.MergeForwardMessageRequest) (*pb.MergeForwardMessageResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.SenderId == "" {
        return nil, status.Error(codes.InvalidArgument, "sender_id is required")
    }
    if len(req.MsgIds) == 0 {
        return nil, status.Error(codes.InvalidArgument, "msg_ids is required")
    }
    if len(req.MsgIds) > 100 {
        return nil, status.Error(codes.InvalidArgument, "too many messages to merge, max 100")
    }
    if req.Title == "" {
        return nil, status.Error(codes.InvalidArgument, "title is required")
    }
    if len(req.TargetIds) == 0 {
        return nil, status.Error(codes.InvalidArgument, "target_ids is required")
    }
    if len(req.TargetIds) > 20 {
        return nil, status.Error(codes.InvalidArgument, "too many targets, max 20")
    }
    if req.TargetType == common.CONVERSATION_TYPE_UNSPECIFIED {
        return nil, status.Error(codes.InvalidArgument, "target_type is required")
    }

    // ==================== 2. 批量查询原始消息 ====================
    originalMsgs, err := s.getMessagesByMsgIDs(ctx, req.MsgIds)
    if err != nil {
        return nil, status.Error(codes.Internal, "get original messages failed")
    }
    if len(originalMsgs) == 0 {
        return nil, status.Error(codes.NotFound, "no valid messages found")
    }

    // 按 seq 排序
    sort.Slice(originalMsgs, func(i, j int) bool {
        return originalMsgs[i].Seq < originalMsgs[j].Seq
    })

    // ==================== 3. 构建合并转发摘要（前 4 条） ====================
    summaryItems := make([]*pb.MergeForwardSummary, 0, 4)
    for i, msg := range originalMsgs {
        if i >= 4 {
            break
        }
        // 获取发送者昵称
        senderName := msg.SenderId // 兜底用 ID
        userResp, err := s.userClient.GetUser(ctx, &user_pb.GetUserRequest{UserId: msg.SenderId})
        if err == nil && userResp.User != nil {
            senderName = userResp.User.Nickname
        }
        summaryItems = append(summaryItems, &pb.MergeForwardSummary{
            SenderName: senderName,
            Content:    buildContentPreview(msg.Body),
        })
    }

    // ==================== 4. 存储合并消息详情（作为一条隐藏的详情消息） ====================
    detailMsgID := s.snowflake.Generate().String()
    detailContent, _ := json.Marshal(map[string]interface{}{
        "type":     int(common.MESSAGE_TYPE_MERGE),
        "title":    req.Title,
        "messages": originalMsgs, // 完整原始消息列表
    })

    // 合并详情存入 PgSQL（不分配 seq，仅作为详情存储）
    _, err = s.db.ExecContext(ctx,
        `INSERT INTO messages (
            msg_id, client_msg_id, channel_id, channel_type, sender_id,
            msg_type, content, media_url, media_info, at_user_ids,
            at_all, quote_msg_id, merge_msg_ids, status, seq, send_time, server_time
        ) VALUES ($1, $2, '', 0, $3, $4, $5, '', '{}', '{}', false, '', $6, 1, 0, $7, $7)`,
        detailMsgID,
        fmt.Sprintf("merge_detail_%s", detailMsgID),
        req.SenderId,
        int(common.MESSAGE_TYPE_MERGE),
        detailContent,
        pq.Array(req.MsgIds),
        time.Now().UnixMilli(),
    )
    if err != nil {
        return nil, status.Error(codes.Internal, "store merge detail failed")
    }

    // ==================== 5. 获取发送者信息 ====================
    senderResp, err := s.userClient.GetUser(ctx, &user_pb.GetUserRequest{UserId: req.SenderId})
    if err != nil {
        return nil, status.Error(codes.Internal, "get sender info failed")
    }

    // ==================== 6. 逐个目标发送合并消息 ====================
    newMsgIDs := make([]string, 0, len(req.TargetIds))
    now := time.Now().UnixMilli()

    for _, targetID := range req.TargetIds {
        // 6a. 获取或创建目标会话通道
        var channelResp *conv_pb.GetOrCreateChannelResponse
        if req.TargetType == common.CONVERSATION_TYPE_SINGLE {
            channelResp, err = s.conversationClient.GetOrCreateChannel(ctx, &conv_pb.GetOrCreateChannelRequest{
                Type:      common.CONVERSATION_TYPE_SINGLE,
                MemberIds: []string{req.SenderId, targetID},
            })
        } else {
            channelResp, err = s.conversationClient.GetOrCreateChannel(ctx, &conv_pb.GetOrCreateChannelRequest{
                Type:    common.CONVERSATION_TYPE_GROUP,
                GroupId: targetID,
            })
        }
        if err != nil {
            log.Error("get channel for merge forward failed", "target_id", targetID, "err", err)
            continue
        }
        targetChannelID := channelResp.Channel.ConversationId

        // 6b. 分配 seq
        seqResp, err := s.conversationClient.IncrChannelSeq(ctx, &conv_pb.IncrChannelSeqRequest{
            ConversationId: targetChannelID,
            Count:          1,
        })
        if err != nil {
            log.Error("alloc seq for merge forward failed", "target_id", targetID, "err", err)
            continue
        }

        // 6c. 构造合并消息
        newMsgID := s.snowflake.Generate().String()
        clientMsgID := fmt.Sprintf("merge_%s_%s", detailMsgID, targetID)

        mergeBody := &pb.MessageBody{
            Type: common.MESSAGE_TYPE_MERGE,
            Merge: &pb.MergeForwardInfo{
                Title:       req.Title,
                Items:       summaryItems,
                DetailMsgId: detailMsgID,
            },
        }
        contentJSON, _ := json.Marshal(map[string]interface{}{
            "type":          int(common.MESSAGE_TYPE_MERGE),
            "title":         req.Title,
            "summary":       summaryItems,
            "detail_msg_id": detailMsgID,
        })

        _, err = s.db.ExecContext(ctx,
            `INSERT INTO messages (
                msg_id, client_msg_id, channel_id, channel_type, sender_id,
                msg_type, content, media_url, media_info, at_user_ids,
                at_all, quote_msg_id, merge_msg_ids, status, seq, send_time, server_time
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, '', '{}', '{}', false, '', $8, 1, $9, $10, $10)`,
            newMsgID, clientMsgID, targetChannelID, int(req.TargetType),
            req.SenderId, int(common.MESSAGE_TYPE_MERGE), contentJSON,
            pq.Array(req.MsgIds), seqResp.StartSeq, now,
        )
        if err != nil {
            log.Error("insert merge forward message failed",
                "target_id", targetID, "new_msg_id", newMsgID, "err", err)
            continue
        }

        newMsgIDs = append(newMsgIDs, newMsgID)

        // 6d. 投递 msg.stored 事件
        storedEvent := &kafka_msg.MsgStoredEvent{
            Header:           buildEventHeader("message", s.instanceID),
            MsgId:            newMsgID,
            ClientMsgId:      clientMsgID,
            ConversationId:   targetChannelID,
            ConversationType: req.TargetType,
            SenderId:         req.SenderId,
            ReceiverId:       targetID,
            MsgType:          common.MESSAGE_TYPE_MERGE,
            ContentPreview:   "[聊天记录]",
            Status:           common.MESSAGE_STATUS_SENT,
            Seq:              seqResp.StartSeq,
            SendTime:         now,
            ServerTime:       now,
            SenderNickname:   senderResp.User.Nickname,
            SenderAvatar:     senderResp.User.AvatarUrl,
        }
        var topic string
        if req.TargetType == common.CONVERSATION_TYPE_SINGLE {
            topic = "msg.stored.single"
        } else {
            topic = "msg.stored.group"
            storedEvent.GroupId = targetID
        }
        s.kafka.Produce(ctx, topic, targetChannelID, storedEvent)

        // 6e. 更新会话最后消息
        go func(chID, mID string, sq int64) {
            s.conversationClient.UpdateChannelLastMsg(context.Background(),
                &conv_pb.UpdateChannelLastMsgRequest{
                    ConversationId: chID,
                    MsgId:          mID,
                    Seq:            sq,
                    SenderId:       req.SenderId,
                    Preview:        "[聊天记录]",
                    SendTime:       now,
                })
        }(targetChannelID, newMsgID, seqResp.StartSeq)
    }

    // ==================== 7. 返回 ====================
    return &pb.MergeForwardMessageResponse{
        Meta:   successMeta(ctx),
        MsgIds: newMsgIDs,
    }, nil
}

// getMessagesByMsgIDs 批量查询消息（内部方法）
func (s *MessageService) getMessagesByMsgIDs(ctx context.Context, msgIDs []string) ([]*pb.ChatMessage, error) {
    if len(msgIDs) == 0 {
        return nil, nil
    }

    // 先批量查 Redis 缓存
    pipe := s.redis.Pipeline()
    cmds := make([]*redis.StringCmd, len(msgIDs))
    for i, mid := range msgIDs {
        cmds[i] = pipe.Get(ctx, fmt.Sprintf("msg:detail:%s", mid))
    }
    pipe.Exec(ctx)

    var result []*pb.ChatMessage
    missedIDs := make([]string, 0)

    for i, cmd := range cmds {
        val, err := cmd.Result()
        if err == nil && val != "" {
            var msg pb.ChatMessage
            if json.Unmarshal([]byte(val), &msg) == nil {
                result = append(result, &msg)
                continue
            }
        }
        missedIDs = append(missedIDs, msgIDs[i])
    }

    // 缓存未命中的从 PgSQL 查询
    if len(missedIDs) > 0 {
        rows, err := s.db.QueryContext(ctx,
            `SELECT msg_id, client_msg_id, channel_id, channel_type, sender_id,
                    msg_type, content, media_url, media_info, at_user_ids,
                    at_all, quote_msg_id, status, seq, send_time, server_time
             FROM messages WHERE msg_id = ANY($1) AND status != 4`,
            pq.Array(missedIDs),
        )
        if err != nil {
            return result, err
        }
        defer rows.Close()

        for rows.Next() {
            msg, err := scanChatMessage(rows)
            if err != nil {
                continue
            }
            result = append(result, msg)
        }

        // 异步回填缓存
        go s.cacheMessages(context.Background(), result)
    }

    return result, nil
}
```

### 10. BatchSendMessage — 批量发送消息（内部接口）

> 供 Notification 服务、Group 服务等内部调用，用于系统消息、群通知等批量发送场景。  
> 不做好友/拉黑校验，但需要校验会话有效性。

```go
func (s *MessageService) BatchSendMessage(ctx context.Context, req *pb.BatchSendMessageRequest) (*pb.BatchSendMessageResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.SenderId == "" {
        return nil, status.Error(codes.InvalidArgument, "sender_id is required")
    }
    if len(req.ReceiverIds) == 0 {
        return nil, status.Error(codes.InvalidArgument, "receiver_ids is required")
    }
    if len(req.ReceiverIds) > 500 {
        return nil, status.Error(codes.InvalidArgument, "too many receivers, max 500")
    }
    if req.Body == nil {
        return nil, status.Error(codes.InvalidArgument, "body is required")
    }
    if req.ConversationType == common.CONVERSATION_TYPE_UNSPECIFIED {
        return nil, status.Error(codes.InvalidArgument, "conversation_type is required")
    }

    // ==================== 2. 获取发送者信息 ====================
    senderResp, err := s.userClient.GetUser(ctx, &user_pb.GetUserRequest{UserId: req.SenderId})
    if err != nil {
        return nil, status.Error(codes.Internal, "get sender info failed")
    }

    // ==================== 3. 逐个接收者发送 ====================
    now := time.Now().UnixMilli()
    newMsgIDs := make([]string, 0, len(req.ReceiverIds))
    var successCount, failCount int32

    for i, receiverID := range req.ReceiverIds {
        // 3a. 构建每条消息的 client_msg_id（以 batch 前缀 + 序号区分）
        clientMsgID := fmt.Sprintf("%s_batch_%d", req.ClientMsgId, i)

        // 3b. 幂等检查
        dedupKey := fmt.Sprintf("msg:dedup:%s", clientMsgID)
        existingMsgID, err := s.redis.Get(ctx, dedupKey).Result()
        if err == nil && existingMsgID != "" {
            newMsgIDs = append(newMsgIDs, existingMsgID)
            successCount++
            continue
        }

        // 3c. 获取或创建通道
        var channelResp *conv_pb.GetOrCreateChannelResponse
        if req.ConversationType == common.CONVERSATION_TYPE_SINGLE {
            channelResp, err = s.conversationClient.GetOrCreateChannel(ctx, &conv_pb.GetOrCreateChannelRequest{
                Type:      common.CONVERSATION_TYPE_SINGLE,
                MemberIds: []string{req.SenderId, receiverID},
            })
        } else if req.ConversationType == common.CONVERSATION_TYPE_GROUP {
            channelResp, err = s.conversationClient.GetOrCreateChannel(ctx, &conv_pb.GetOrCreateChannelRequest{
                Type:    common.CONVERSATION_TYPE_GROUP,
                GroupId: receiverID,
            })
        }
        if err != nil {
            log.Error("batch send: get channel failed", "receiver_id", receiverID, "err", err)
            failCount++
            continue
        }
        channelID := channelResp.Channel.ConversationId

        // 3d. 分配 seq
        seqResp, err := s.conversationClient.IncrChannelSeq(ctx, &conv_pb.IncrChannelSeqRequest{
            ConversationId: channelID,
            Count:          1,
        })
        if err != nil {
            log.Error("batch send: alloc seq failed", "receiver_id", receiverID, "err", err)
            failCount++
            continue
        }

        // 3e. 生成 msg_id 并写入 PgSQL
        msgID := s.snowflake.Generate().String()
        contentJSON, _ := json.Marshal(map[string]interface{}{
            "type":  int(req.Body.Type),
            "text":  req.Body.Text,
            "extra": req.Body.Extra,
        })
        var mediaURL string
        var mediaInfoJSON []byte
        if req.Body.Media != nil {
            mediaURL = req.Body.Media.Url
            mediaInfoJSON, _ = json.Marshal(req.Body.Media)
        }
        if mediaInfoJSON == nil {
            mediaInfoJSON = []byte("{}")
        }

        _, err = s.db.ExecContext(ctx,
            `INSERT INTO messages (
                msg_id, client_msg_id, channel_id, channel_type, sender_id,
                msg_type, content, media_url, media_info, at_user_ids,
                at_all, quote_msg_id, merge_msg_ids, status, seq, send_time, server_time
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, '{}', false, '', '{}', 1, $10, $11, $11)`,
            msgID, clientMsgID, channelID, int(req.ConversationType),
            req.SenderId, int(req.Body.Type), contentJSON,
            mediaURL, mediaInfoJSON, seqResp.StartSeq, now,
        )
        if err != nil {
            if isPgUniqueViolation(err) {
                // client_msg_id 冲突，幂等处理
                var existID string
                s.db.QueryRowContext(ctx,
                    `SELECT msg_id FROM messages WHERE client_msg_id = $1`, clientMsgID,
                ).Scan(&existID)
                if existID != "" {
                    newMsgIDs = append(newMsgIDs, existID)
                    successCount++
                    continue
                }
            }
            log.Error("batch send: insert message failed", "receiver_id", receiverID, "err", err)
            failCount++
            continue
        }

        newMsgIDs = append(newMsgIDs, msgID)
        successCount++

        // 3f. 写入 Redis 去重缓存
        s.redis.Set(ctx, dedupKey, msgID, 24*time.Hour)

        // 3g. 投递 msg.stored 事件
        contentPreview := buildContentPreview(req.Body)
        storedEvent := &kafka_msg.MsgStoredEvent{
            Header:           buildEventHeader("message", s.instanceID),
            MsgId:            msgID,
            ClientMsgId:      clientMsgID,
            ConversationId:   channelID,
            ConversationType: req.ConversationType,
            SenderId:         req.SenderId,
            ReceiverId:       receiverID,
            MsgType:          req.Body.Type,
            ContentPreview:   contentPreview,
            Status:           common.MESSAGE_STATUS_SENT,
            Seq:              seqResp.StartSeq,
            SendTime:         now,
            ServerTime:       now,
            SenderNickname:   senderResp.User.Nickname,
            SenderAvatar:     senderResp.User.AvatarUrl,
        }
        var topic string
        if req.ConversationType == common.CONVERSATION_TYPE_SINGLE {
            topic = "msg.stored.single"
        } else {
            topic = "msg.stored.group"
            storedEvent.GroupId = receiverID
        }
        s.kafka.Produce(ctx, topic, channelID, storedEvent)

        // 3h. 更新会话最后消息
        go func(chID, mID, rID string, sq int64) {
            s.conversationClient.UpdateChannelLastMsg(context.Background(),
                &conv_pb.UpdateChannelLastMsgRequest{
                    ConversationId: chID,
                    MsgId:          mID,
                    Seq:            sq,
                    SenderId:       req.SenderId,
                    Preview:        contentPreview,
                    SendTime:       now,
                })
        }(channelID, msgID, receiverID, seqResp.StartSeq)
    }

    // ==================== 4. 返回 ====================
    return &pb.BatchSendMessageResponse{
        Meta:         successMeta(ctx),
        MsgIds:       newMsgIDs,
        SuccessCount: successCount,
        FailCount:    failCount,
    }, nil
}
```

### 11. ClearConversationMessages — 清空会话聊天记录

> 仅对当前用户不可见。通过设置 clear_seq 实现：用户看到的消息仅为 seq > clear_seq 的部分。

```go
func (s *MessageService) ClearConversationMessages(ctx context.Context, req *pb.ClearConversationMessagesRequest) (*pb.ClearConversationMessagesResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id is required")
    }
    if req.ConversationId == "" {
        return nil, status.Error(codes.InvalidArgument, "conversation_id is required")
    }

    // ==================== 2. 获取当前通道最大 seq ====================
    // 通过 Conversation 服务 RPC 查询（不直读 Conversation 的 Redis/DB）
    maxSeqResp, err := s.conversationClient.GetChannelMaxSeq(ctx, &conv_pb.GetChannelMaxSeqRequest{
        ConversationId: req.ConversationId,
    })
    if err != nil {
        return nil, status.Error(codes.Internal, "get channel max seq failed")
    }
    currentMaxSeq := maxSeqResp.MaxSeq

    if currentMaxSeq == 0 {
        return nil, status.Error(codes.FailedPrecondition, "conversation has no messages")
    }

    // ==================== 3. 通过 Conversation 服务 RPC 设置 clear_seq ====================
    // 注：clear_seq 和 user_conversations 表属于 Conversation 服务域
    //     Message 服务不应直接操作 Conversation 的 PgSQL/Redis
    //     使用 UpdateUserConversation RPC，该 RPC 未直接支持 clear_seq，
    //     因此需要 Conversation 服务新增 SetClearSeq RPC 或扩展现有接口
    //
    // 方案：复用 Conversation.SetReadSeq 的模式，新增 SetClearSeq RPC
    // 临时方案：调用 Conversation.UpdateUserConversation 传递 clear_seq
    _, err = s.conversationClient.SetClearSeq(ctx, &conv_pb.SetClearSeqRequest{
        UserId:         req.UserId,
        ConversationId: req.ConversationId,
        ClearSeq:       currentMaxSeq,
    })
    if err != nil {
        return nil, status.Error(codes.Internal, "set clear_seq failed")
    }

    // ==================== 4. 返回 ====================
    return &pb.ClearConversationMessagesResponse{
        Meta:     successMeta(ctx),
        ClearSeq: currentMaxSeq,
    }, nil
}
```

---

## 辅助方法

```go
// parseContentJSON 解析消息内容 JSON 到 MessageBody
func parseContentJSON(msg *pb.ChatMessage, contentJSON string) {
    if contentJSON == "" || contentJSON == "{}" {
        return
    }
    var content map[string]interface{}
    if err := json.Unmarshal([]byte(contentJSON), &content); err != nil {
        return
    }
    if msg.Body == nil {
        msg.Body = &pb.MessageBody{}
    }
    if text, ok := content["text"].(string); ok {
        msg.Body.Text = text
    }
    if extra, ok := content["extra"].(string); ok {
        msg.Body.Extra = extra
    }
}

// parseMediaInfoJSON 解析媒体信息 JSON
func parseMediaInfoJSON(msg *pb.ChatMessage, mediaInfoJSON string) {
    if mediaInfoJSON == "" || mediaInfoJSON == "{}" {
        return
    }
    var mediaInfo common.MediaInfo
    if err := json.Unmarshal([]byte(mediaInfoJSON), &mediaInfo); err != nil {
        return
    }
    if msg.Body == nil {
        msg.Body = &pb.MessageBody{}
    }
    msg.Body.Media = &mediaInfo
}

// buildEventHeader 构建 Kafka 事件头
func buildEventHeader(source string, instanceID string) *common.EventHeader {
    return &common.EventHeader{
        EventId:   uuid.New().String(),
        TraceId:   trace.SpanFromContext(context.Background()).SpanContext().TraceID().String(),
        Source:    source,
        SourceId:  instanceID,
        Timestamp: time.Now().UnixMilli(),
        Version:   1,
    }
}

// successMeta 构建成功响应元信息
func successMeta(ctx context.Context) *common.ResponseMeta {
    return &common.ResponseMeta{
        Code:       0,
        Message:    "success",
        ServerTime: time.Now().UnixMilli(),
        TraceId:    trace.SpanFromContext(ctx).SpanContext().TraceID().String(),
    }
}

// isPgUniqueViolation 判断是否为 PgSQL 唯一约束冲突
func isPgUniqueViolation(err error) bool {
    var pgErr *pq.Error
    if errors.As(err, &pgErr) {
        return pgErr.Code == "23505"
    }
    return false
}
```

---

## 本地缓存辅助方法

> 以下方法用于 Message 服务**本地 Redis 缓存**的读取、回填、解析。  
> 核心思路：**本地缓存优先 → cache miss 降级 RPC → 异步回填缓存**。  
> 缓存由 Kafka 消费者（见 kafka/message/message_impl.md）负责主动更新/失效。

```go
// ==================== 本地缓存辅助方法 ====================

// getLocalUserInfo 从本地缓存读取用户信息，miss 时降级 RPC 并回填
func (s *MessageService) getLocalUserInfo(ctx context.Context, userID string) (*user_pb.UserInfo, error) {
    cacheKey := fmt.Sprintf("msg:local:user:%s", userID)
    data, err := s.redis.HGetAll(ctx, cacheKey).Result()
    if err == nil && len(data) > 0 {
        return parseUserInfoFromCache(data), nil
    }
    // 缓存未命中 → 降级 RPC
    resp, err := s.userClient.GetUser(ctx, &user_pb.GetUserRequest{UserId: userID})
    if err != nil {
        return nil, err
    }
    // 异步回填本地缓存
    go s.backfillUserCache(context.Background(), userID, resp.User)
    return resp.User, nil
}

// backfillUserCache 回填用户信息到本地缓存
// 仅缓存 Message 服务实际使用的字段：nickname（消息展示）、avatar_url（消息展示）
// 注：UserInfo proto 无 Status 字段，账号状态由 User 服务管理，Message 不缓存
func (s *MessageService) backfillUserCache(ctx context.Context, userID string, info *user_pb.UserInfo) {
    cacheKey := fmt.Sprintf("msg:local:user:%s", userID)
    s.redis.HSet(ctx, cacheKey, map[string]interface{}{
        "nickname":   info.Nickname,
        "avatar_url": info.AvatarUrl,
    })
    s.redis.Expire(ctx, cacheKey, 30*time.Minute)
}

// parseUserInfoFromCache 从 Redis HASH 解析用户信息
func parseUserInfoFromCache(data map[string]string) *user_pb.UserInfo {
    info := &user_pb.UserInfo{
        Nickname:  data["nickname"],
        AvatarUrl: data["avatar_url"],
    }
    return info
}

// getLocalGroupInfo 从本地缓存读取群信息，miss 时降级 RPC 并回填
func (s *MessageService) getLocalGroupInfo(ctx context.Context, groupID string) (*group_pb.GroupInfo, error) {
    cacheKey := fmt.Sprintf("msg:local:group:%s", groupID)
    data, err := s.redis.HGetAll(ctx, cacheKey).Result()
    if err == nil && len(data) > 0 {
        return parseGroupInfoFromCache(data), nil
    }
    // 缓存未命中 → 降级 RPC
    resp, err := s.groupClient.GetGroup(ctx, &group_pb.GetGroupRequest{GroupId: groupID})
    if err != nil {
        return nil, err
    }
    go s.backfillGroupCache(context.Background(), groupID, resp.Group)
    return resp.Group, nil
}

// backfillGroupCache 回填群信息到本地缓存
func (s *MessageService) backfillGroupCache(ctx context.Context, groupID string, info *group_pb.GroupInfo) {
    cacheKey := fmt.Sprintf("msg:local:group:%s", groupID)
    s.redis.HSet(ctx, cacheKey, map[string]interface{}{
        "name":         info.Name,
        "owner_id":     info.OwnerId,
        "status":       fmt.Sprintf("%d", info.Status),
        "member_count": fmt.Sprintf("%d", info.MemberCount),
        "is_muted":     fmt.Sprintf("%t", info.Status == common.GROUP_STATUS_MUTED),
    })
    s.redis.Expire(ctx, cacheKey, 30*time.Minute)
}

// parseGroupInfoFromCache 从 Redis HASH 解析群信息
func parseGroupInfoFromCache(data map[string]string) *group_pb.GroupInfo {
    info := &group_pb.GroupInfo{
        Name:    data["name"],
        OwnerId: data["owner_id"],
    }
    if s, ok := data["status"]; ok {
        if v, err := strconv.Atoi(s); err == nil {
            info.Status = int32(v)
        }
    }
    if s, ok := data["member_count"]; ok {
        if v, err := strconv.Atoi(s); err == nil {
            info.MemberCount = int32(v)
        }
    }
    return info
}

// getLocalGroupMember 从本地缓存读取群成员信息，miss 时降级 RPC 并回填
func (s *MessageService) getLocalGroupMember(ctx context.Context, groupID, userID string) (*group_pb.GroupMember, error) {
    cacheKey := fmt.Sprintf("msg:local:group_member:%s:%s", groupID, userID)
    data, err := s.redis.HGetAll(ctx, cacheKey).Result()
    if err == nil && len(data) > 0 {
        return parseGroupMemberFromCache(data), nil
    }
    // 缓存未命中 → 降级 RPC
    resp, err := s.groupClient.GetGroupMember(ctx, &group_pb.GetGroupMemberRequest{
        GroupId: groupID,
        UserId:  userID,
    })
    if err != nil {
        return nil, err
    }
    go s.backfillGroupMemberCache(context.Background(), groupID, userID, resp.Member)
    return resp.Member, nil
}

// backfillGroupMemberCache 回填群成员信息到本地缓存
func (s *MessageService) backfillGroupMemberCache(ctx context.Context, groupID, userID string, member *group_pb.GroupMember) {
    cacheKey := fmt.Sprintf("msg:local:group_member:%s:%s", groupID, userID)
    s.redis.HSet(ctx, cacheKey, map[string]interface{}{
        "role":       fmt.Sprintf("%d", member.Role),
        "is_muted":  fmt.Sprintf("%t", member.IsMuted),
        "mute_until": fmt.Sprintf("%d", member.MuteUntil),
    })
    s.redis.Expire(ctx, cacheKey, 10*time.Minute)
}

// parseGroupMemberFromCache 从 Redis HASH 解析群成员信息
func parseGroupMemberFromCache(data map[string]string) *group_pb.GroupMember {
    member := &group_pb.GroupMember{}
    if s, ok := data["role"]; ok {
        if v, err := strconv.Atoi(s); err == nil {
            member.Role = int32(v)
        }
    }
    if s, ok := data["is_muted"]; ok {
        member.IsMuted = (s == "true")
    }
    if s, ok := data["mute_until"]; ok {
        if v, err := strconv.ParseInt(s, 10, 64); err == nil {
            member.MuteUntil = v
        }
    }
    return member
}

// getLocalUserSettings 从本地缓存读取用户设置，miss 时降级 RPC 并回填
func (s *MessageService) getLocalUserSettings(ctx context.Context, userID string) (map[string]string, error) {
    cacheKey := fmt.Sprintf("msg:local:user_settings:%s", userID)
    data, err := s.redis.HGetAll(ctx, cacheKey).Result()
    if err == nil && len(data) > 0 {
        return data, nil
    }
    // 缓存未命中 → 降级 RPC
    resp, err := s.userClient.GetUserSettings(ctx, &user_pb.GetUserSettingsRequest{
        UserId: userID,
    })
    if err != nil {
        return nil, err
    }
    // 异步回填本地缓存
    go func() {
        settings := map[string]interface{}{
            "allow_stranger_msg": fmt.Sprintf("%t", resp.Settings.AllowStrangerMsg),
        }
        s.redis.HSet(context.Background(), cacheKey, settings)
        s.redis.Expire(context.Background(), cacheKey, 1*time.Hour)
    }()
    result := map[string]string{
        "allow_stranger_msg": fmt.Sprintf("%t", resp.Settings.AllowStrangerMsg),
    }
    return result, nil
}

// ==================== 延迟双删工具 ====================

// delayedDoubleDelete 延迟双删：在 delay 后再次删除缓存 key
// 解决 DB 更新完成前并发请求将旧数据回填缓存的问题
// 流程：(1) 调用方先 DEL key → (2) 更新 DB → (3) 延迟 delay 后再次 DEL key
func (s *MessageService) delayedDoubleDelete(ctx context.Context, key string, delay time.Duration) {
    time.Sleep(delay)
    result, err := s.redis.Del(ctx, key).Result()
    if err != nil {
        log.Warn("delayed double delete failed", "key", key, "err", err)
        return
    }
    if result > 0 {
        log.Debug("delayed double delete hit stale cache", "key", key)
    }
}
```

---

## BatchPullMessages — 批量拉取多个会话增量消息

> **解决问题（CRIT-02）**：用户上线/重连时，客户端需要拉取所有活跃会话的增量消息。若逐会话调用 `PullMessages`，10M 用户每人平均 20 个活跃会话 → 2 亿次 RPC/s 的风暴。`BatchPullMessages` 合并为 **每用户 1 次调用**，将 RPC 数降低 20 倍。

### 调用时机
- 客户端上线 → 拉取 `SyncData` 获取各会话最新 seq → 对比本地 seq → 调用 `BatchPullMessages` 批量拉取 diff
- 客户端掉线恢复 → 同上

### 伪代码

```go
func (s *MessageService) BatchPullMessages(ctx context.Context, req *pb.BatchPullMessagesRequest) (*pb.BatchPullMessagesResponse, error) {
    ctx, span := s.tracer.Start(ctx, "MessageService.BatchPullMessages")
    defer span.End()

    // ── 1. 参数校验 ──
    if len(req.ConversationSeqs) == 0 {
        return nil, status.Error(codes.InvalidArgument, "conversation_seqs is empty")
    }
    maxConversations := 100 // 单次最多拉取 100 个会话，防止滥用
    if len(req.ConversationSeqs) > maxConversations {
        return nil, status.Error(codes.InvalidArgument, "too many conversations, max 100")
    }
    maxPerConv := int(req.MaxPerConversation)
    if maxPerConv <= 0 {
        maxPerConv = 20 // 默认 20 条
    }
    if maxPerConv > 50 {
        maxPerConv = 50 // 上限 50 条
    }

    // ── 2. 并发拉取：使用 goroutine pool + errgroup 控制并发 ──
    // 限制并发数 = 16，避免单用户请求打满 Redis 连接
    eg, egCtx := errgroup.WithContext(ctx)
    eg.SetLimit(16)

    result := make(map[string]*pb.ConversationMessages, len(req.ConversationSeqs))
    var mu sync.Mutex

    for convID, localSeq := range req.ConversationSeqs {
        convID, localSeq := convID, localSeq // capture loop vars
        eg.Go(func() error {
            msgs, maxSeq, hasMore, err := s.pullConversationIncremental(egCtx, convID, localSeq, maxPerConv)
            if err != nil {
                // 单个会话失败不阻断整体，记录错误并返回空
                span.RecordError(err, trace.WithAttributes(attribute.String("conversation_id", convID)))
                log.Warn("batch pull single conversation failed", "conv", convID, "err", err)
                return nil // 不传播错误，保证部分成功
            }
            if len(msgs) > 0 {
                mu.Lock()
                result[convID] = &pb.ConversationMessages{
                    Messages: msgs,
                    MaxSeq:   maxSeq,
                    HasMore:  hasMore,
                }
                mu.Unlock()
            }
            return nil
        })
    }
    _ = eg.Wait()

    span.SetAttributes(
        attribute.Int("batch.conversations_requested", len(req.ConversationSeqs)),
        attribute.Int("batch.conversations_returned", len(result)),
    )
    s.batchPullConvCount.Add(ctx, int64(len(result)))

    return &pb.BatchPullMessagesResponse{
        Meta:          &common.ResponseMeta{Code: 0},
        Conversations: result,
    }, nil
}

// pullConversationIncremental 拉取单个会话从 localSeq 之后的增量消息
// 复用 PullMessages 的核心逻辑（Redis ZRANGEBYSCORE + 回源 PgSQL）
func (s *MessageService) pullConversationIncremental(ctx context.Context, convID string, localSeq int64, limit int) ([]*pb.MessageItem, int64, bool, error) {
    // 1. 获取会话最新 seq（Redis: conv:max_seq:{convID}）
    maxSeqKey := fmt.Sprintf("conv:max_seq:%s", convID)
    maxSeq, err := s.redis.Get(ctx, maxSeqKey).Int64()
    if err == redis.Nil {
        return nil, 0, false, nil // 会话不存在或无消息
    }
    if err != nil {
        return nil, 0, false, err
    }

    // 2. 无增量消息
    if localSeq >= maxSeq {
        return nil, maxSeq, false, nil
    }

    // 3. Redis Pipeline: ZRANGEBYSCORE msg:seq_index:{convID} (localSeq, +inf LIMIT 0 limit+1
    seqIndexKey := fmt.Sprintf("msg:seq_index:%s", convID)
    zOpt := &redis.ZRangeBy{
        Min:    fmt.Sprintf("(%d", localSeq), // exclusive
        Max:    "+inf",
        Offset: 0,
        Count:  int64(limit + 1), // +1 判断 hasMore
    }
    seqMembers, err := s.redis.ZRangeByScore(ctx, seqIndexKey, zOpt).Result()
    if err != nil {
        return nil, maxSeq, false, err
    }

    hasMore := len(seqMembers) > limit
    if hasMore {
        seqMembers = seqMembers[:limit]
    }

    if len(seqMembers) == 0 {
        // seq 索引缓存 miss，回源 PgSQL（与 PullMessages 共享逻辑）
        return s.pullFromPgSQL(ctx, convID, localSeq, limit)
    }

    // 4. Pipeline 批量获取消息详情（msg:detail:{msg_id}）
    pipe := s.redis.Pipeline()
    cmds := make([]*redis.StringCmd, 0, len(seqMembers))
    for _, msgID := range seqMembers {
        cmds = append(cmds, pipe.Get(ctx, fmt.Sprintf("msg:detail:%s", msgID)))
    }
    _, _ = pipe.Exec(ctx)

    messages := make([]*pb.MessageItem, 0, len(seqMembers))
    for _, cmd := range cmds {
        data, err := cmd.Bytes()
        if err != nil {
            continue // 单条缺失不阻断
        }
        var msg pb.MessageItem
        if proto.Unmarshal(data, &msg) == nil {
            messages = append(messages, &msg)
        }
    }

    return messages, maxSeq, hasMore, nil
}
```

---

## 可观测性接入（OpenTelemetry）

> Message 服务是 IM 系统核心中枢，可观测性需覆盖：gRPC 调用链、Kafka 事件发布、消息端到端延迟、seq 分配性能、本地缓存命中率。

### 第一步：服务启动时初始化 OTel SDK

调用共享包 `pkg/observability` 中的 `InitOpenTelemetry`，一次性完成 TracerProvider + MeterProvider 的创建，所有遥测数据通过 OTLP gRPC 推送到 OTel Collector，**服务自身不暴露任何监控端口**。

> ⚠️ **必须使用共享包** `observability.InitOpenTelemetry`，不要内联实现。共享包包含以下关键逻辑，内联版本极易遗漏：
> - `resource.Merge(resource.Default(), ...)` 合并默认 resource（SDK/OS/Process 信息）
> - `AlwaysSample()` + Exporter retry 配置（配合 Gateway tail_sampling）
> - `WithView(CustomHistogramViews()...)` IM 专属 Histogram bucket
> - 每个 Provider 独立超时的 shutdown 函数

```go
func main() {
    ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
    defer cancel()

    // ── 初始化 OTel（TracerProvider + MeterProvider） ──
    // 内部自动设置 AlwaysSample + W3C Propagator + Runtime Metrics
    otelShutdown := observability.InitOpenTelemetry(ctx, "message", "v1.0.0")
    defer otelShutdown(ctx)

    // ── 设置结构化日志（自动注入 trace_id / span_id） ──
    observability.SetupLogger("message")

    // ── 注册基础设施指标 ──
    observability.RegisterDBPoolMetrics(db, "message")
    observability.RegisterRedisPoolMetrics(redisClient, "message")

    // ── 后续创建 gRPC Server / Kafka Consumer 等 ──
    // ...
}
```

> 共享包 `InitOpenTelemetry` 的完整实现见 `observability_global.md` 第 1.1 节。

### 第二步：gRPC Server 注册 OTel Interceptor

在创建 gRPC Server 时挂载 Unary + Stream Interceptor，**自动**为每个 RPC 请求创建 span、记录 `rpc.server.duration` 直方图指标、提取上游传来的 `traceparent` 实现跨服务链路串联。

```go
grpcServer := grpc.NewServer(
    grpc.ChainUnaryInterceptor(
        otelgrpc.UnaryServerInterceptor(),   // ← OTel: 自动 trace + metrics
        recoveryInterceptor(),                // 其他 interceptor 放后面
        authInterceptor(),
    ),
    grpc.ChainStreamInterceptor(
        otelgrpc.StreamServerInterceptor(),  // ← OTel: stream 也要
    ),
)
pb.RegisterMessageServiceServer(grpcServer, messageServer)
```

**自动产出的指标**（无需手写任何代码）：

| 指标名 | 类型 | 标签 | 含义 |
|--------|------|------|------|
| `rpc.server.duration` | Histogram | service_name, rpc_method, rpc_grpc_status_code | 每个 RPC 方法的延迟分布 |
| `rpc.server.active_requests` | UpDownCounter | service_name, rpc_method | 当前正在处理的请求数 |

### 第三步：gRPC Client 注册 OTel Interceptor（调用 Conversation/Relation 等服务时）

Message 服务调用 `Conversation.IncrChannelSeq`、`Relation.CheckFriendship` 等外部 RPC 时，需在客户端连接上挂 Interceptor，**自动将 trace context 注入 gRPC metadata 传播到下游服务**。

```go
// 连接 Conversation 服务
convConn := grpc.Dial("conversation:50051",
    grpc.WithChainUnaryInterceptor(
        otelgrpc.UnaryClientInterceptor(), // ← 自动注入 traceparent 到 metadata
    ),
)
s.conversationClient = conversation_pb.NewConversationServiceClient(convConn)

// 连接 Relation 服务（本地缓存 miss 时的 fallback RPC）
relationConn := grpc.Dial("relation:50051",
    grpc.WithChainUnaryInterceptor(
        otelgrpc.UnaryClientInterceptor(),
    ),
)
s.relationClient = relation_pb.NewRelationServiceClient(relationConn)

// 同理: Group、User 等所有外部 RPC 连接都要挂 client interceptor
```

### 第四步：Kafka Producer 埋点（发布 msg.stored / msg.recalled 等事件时）

SendMessage 等接口在写完 PgSQL 后会向 Kafka 发布事件。使用共享包 `observability.StartKafkaProducerSpan` 创建 PRODUCER span 并**自动将 trace context 注入 Kafka message headers**，使下游消费者（Push/Search/Audit 等）能续接同一条 trace。

```go
// 在 SendMessage 的 Kafka 事件发布处改造
func (s *MessageServer) publishKafkaEvent(ctx context.Context, topic, key string, event proto.Message) error {
    // 使用共享包：创建 PRODUCER span + 注入 trace context 到 headers
    headers := make([]kafka.Header, 0)
    ctx, span := observability.StartKafkaProducerSpan(ctx, topic, &headers)
    defer span.End()

    // 添加业务属性到 span
    span.SetAttributes(attribute.String("messaging.kafka.message.key", key))

    // 发布消息
    err := s.kafkaWriter.WriteMessages(ctx, kafka.Message{
        Topic:   topic,
        Key:     []byte(key),
        Value:   marshalProto(event),
        Headers: headers,  // headers 已包含 traceparent / baggage
    })

    if err != nil {
        observability.RecordError(span, err) // ← 统一使用 RecordError
    }
    return err
}
```

> ⚠️ **不要内联 `kafkaHeaderCarrier`**，直接使用 `observability.StartKafkaProducerSpan`（见 `observability_global.md` 第 1.3 节）。
> 共享包已封装好 `KafkaHeaderCarrier` 实现 `propagation.TextMapCarrier` 接口以及 span 创建逻辑。

### 第五步：注册 Message 服务专属业务指标

除了 gRPC Interceptor 自动采集的通用 RED 指标外，Message 服务需要注册以下与业务强相关的自定义指标：

```go
var meter = otel.Meter("im-chat/message")

// ── 注册自定义指标 ──
var (
    // 消息存储计数（按会话类型 + 消息类型拆分）
    msgStoredCounter, _ = meter.Int64Counter("message.stored_total",
        metric.WithDescription("成功存储的消息总数"))

    // 消息撤回计数
    msgRecalledCounter, _ = meter.Int64Counter("message.recalled_total",
        metric.WithDescription("消息撤回总数"))

    // 消息端到端延迟（从客户端发送时间戳到服务端存储完成的耗时）
    msgE2ELatency, _ = meter.Float64Histogram("message.e2e_latency_ms",
        metric.WithDescription("消息端到端延迟（客户端发送 → 服务端存储完成）"),
        metric.WithUnit("ms"))

    // seq 分配耗时（调用 Conversation.IncrChannelSeq 的 RPC 延迟）
    seqAllocDuration, _ = meter.Float64Histogram("message.seq_alloc_duration_ms",
        metric.WithDescription("Seq 序号分配耗时"),
        metric.WithUnit("ms"))

    // 幂等去重命中计数（重复的 client_msg_id）
    dedupHitCounter, _ = meter.Int64Counter("message.dedup_hit_total",
        metric.WithDescription("幂等去重命中次数（重复消息被拦截）"))

    // 本地缓存命中/未命中
    localCacheHit, _ = meter.Int64Counter("message.cache_hit_total",
        metric.WithDescription("本地缓存命中次数"))
    localCacheMiss, _ = meter.Int64Counter("message.cache_miss_total",
        metric.WithDescription("本地缓存未命中次数（降级 RPC）"))
)
```

在业务代码中使用这些指标的位置：

```go
// SendMessage 中 —— 消息存储成功后
msgStoredCounter.Add(ctx, 1, metric.WithAttributes(
    attribute.String("conversation_type", channelType),  // single / group
    attribute.String("msg_type", msgType),                // text / image / video ...
))

// SendMessage 中 —— 计算端到端延迟
e2eMs := float64(time.Now().UnixMilli() - req.ClientSendTime)
msgE2ELatency.Record(ctx, e2eMs, metric.WithAttributes(
    attribute.String("conversation_type", channelType),
))

// SendMessage 中 —— seq 分配
seqStart := time.Now()
seqResp, err := s.conversationClient.IncrChannelSeq(ctx, &seqReq)
seqAllocDuration.Record(ctx, float64(time.Since(seqStart).Milliseconds()))

// SendMessage 中 —— 幂等去重命中
if existingMsg != nil {
    dedupHitCounter.Add(ctx, 1)
    return existingMsg, nil
}

// 本地缓存读取时
if cached != nil {
    localCacheHit.Add(ctx, 1, metric.WithAttributes(attribute.String("cache", "friend")))
} else {
    localCacheMiss.Add(ctx, 1, metric.WithAttributes(attribute.String("cache", "friend")))
}
```

### 第六步：结构化 JSON 日志（自动携带 trace_id）

使用共享包 `observability.SetupLogger` 设置全局 slog（已在第一步的 `main()` 中调用），自动注入 `trace_id` / `span_id` 到每条日志，支持动态日志级别调整。

> ⚠️ **不要内联 `traceLogHandler` / `setupLogger`**，直接使用 `observability.SetupLogger("message")`（见 `observability_global.md` 第 1.2 节）。
> 共享包版本额外提供：`slog.LevelVar` 动态级别 + `SetLogLevel()` 运行时调整。

**所有业务代码中使用 `slog.InfoContext(ctx, ...)` 而非 `slog.Info(...)`**，确保 trace context 能被提取：

```go
// ✅ 正确：使用 InfoContext 传入 ctx
slog.InfoContext(ctx, "message stored",
    "msg_id", msg.MsgId,
    "channel_id", msg.ChannelId,
    "seq", msg.Seq,
)

// ❌ 错误：不带 ctx，日志中不会有 trace_id
slog.Info("message stored", "msg_id", msg.MsgId)
```

日志输出示例（JSON，自动携带 trace_id）：

```json
{
  "time": "2026-03-15T10:30:00.123Z",
  "level": "INFO",
  "source": {"function": "SendMessage", "file": "handler.go", "line": 142},
  "msg": "message stored",
  "service": "message",
  "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
  "span_id": "00f067aa0ba902b7",
  "msg_id": "msg_20260315_abc123",
  "channel_id": "ch_single_user1_user2",
  "seq": 1057
}
```

### 第七步：改造 buildEventHeader 注入 Trace Context

使用共享包 `observability.BuildEventHeader`，从当前 `context.Context` 提取 OTel 的 trace_id 和 span_id 填入 EventHeader，使 Kafka 事件头与 gRPC trace 贯通。

> ⚠️ **不要内联 `buildEventHeader`**，直接使用 `observability.BuildEventHeader(ctx, source, instanceID)`（见 `observability_global.md` 第 1.4 节）。

```go
// 改造前（错误）：
// header := buildEventHeader("message", instanceID)  // trace_id 为空
//
// 改造后（正确）：
header := observability.BuildEventHeader(ctx, "message", instanceID)
```

### 接入要点总结

| 步骤 | 改动位置 | 自动/手动 | 效果 |
|------|---------|----------|------|
| 1. `observability.InitOpenTelemetry` | main() | 手动一次 | TracerProvider + MeterProvider + Runtime Metrics |
| 2. gRPC Server Interceptor | grpc.NewServer() | 自动 | 所有 RPC 自动产生 span + 指标 |
| 3. gRPC Client Interceptor | grpc.Dial() | 自动 | 调用 Conversation/Relation 时自动传播 trace |
| 4. Kafka Producer 埋点 | `observability.StartKafkaProducerSpan` | 手动 | 事件发布链路追踪 + 指标 |
| 5. 自定义业务指标 | SendMessage/RecallMessage 等 | 手动 | e2e 延迟、seq 分配、去重命中率 |
| 6. `observability.SetupLogger` | main() | 手动一次 | 所有日志自动带 trace_id + 动态级别 |
| 7. `observability.BuildEventHeader` | 辅助函数 | 手动一次 | Kafka 事件头携带真实 trace context |
| 8. 基础设施指标 | `RegisterDBPoolMetrics` + `RegisterRedisPoolMetrics` | 手动一次 | DB/Redis 连接池健康可观测 |
| 9. Span 错误记录 | `observability.RecordError` | 手动 | 错误 trace 在 Tempo 可见 |

### 补充：基础设施指标注册

> ✅ 已合并到第一步 `main()` 中。Message 服务依赖 PgSQL + Redis，通过 `observability.RegisterDBPoolMetrics` 和 `observability.RegisterRedisPoolMetrics` 注册连接池指标。

### 补充：Span 错误记录

Kafka Producer 中已有 `span.RecordError`，但 RPC handler 层也需统一使用：

```go
func (s *MessageServer) SendMessage(ctx context.Context, req *pb.SendMessageRequest) (*pb.SendMessageResponse, error) {
    ctx, span := otel.Tracer("message").Start(ctx, "SendMessage")
    defer span.End()

    result, err := s.doSendMessage(ctx, req)
    if err != nil {
        observability.RecordError(span, err) // ← 所有 error 路径
        return nil, err
    }
    return result, nil
}
```

### 补充：采样率说明

> ⚠️ 共享包 `observability.InitOpenTelemetry` 中采样器为 `ParentBased(AlwaysSample())`（SDK 全量上报）。
> 智能采样决策交给 OTel Collector Gateway 的 `tail_sampling`：错误/慢请求 100% 保留，其余 10%。
> **不要使用 `TraceIDRatioBased(1.0)` 代替 `AlwaysSample()`**——虽然数值等同，但语义不同：
> - `AlwaysSample()` 明确表示"全部采样"
> - `TraceIDRatioBased(1.0)` 表示"基于 trace ID 概率采样，碰巧比例为 100%"
> 后者在未来配置修改时容易被误改为 `TraceIDRatioBased(0.1)`，导致 Gateway tail_sampling 失效。
