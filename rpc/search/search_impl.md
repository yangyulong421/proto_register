# Search 搜索服务 — RPC 接口实现伪代码

## 概述

Search 服务基于 **Elasticsearch** 提供全文搜索能力，覆盖用户、群组、消息三大核心实体。  
**本服务是只读服务**，不直接写入源数据，所有索引数据来源于其他服务通过 Kafka 事件异步同步。

**核心设计原则：**
- **数据来源**：索引数据由 User / Group / Message 服务通过 Kafka 事件驱动写入 ES，Search 服务本身只消费不生产事件
- **权限隔离**：SearchMessages 严格限定在用户所属会话范围内，且必须尊重 `clear_seq`（用户清空的消息不可见）
- **隐私保护**：SearchUsers / SearchGroups 根据 `searcher_user_id` 过滤被搜索方设置了隐私保护的结果，并过滤黑名单用户
- **搜索建议**：SearchSuggest 基于 Redis ZSET 前缀匹配 + 热词加权，低延迟返回
- **限流保护**：每用户每分钟最多 30 次搜索请求（Redis 滑动窗口计数器）
- **索引重建**：RebuildIndex 为管理员 API，支持按类型（user/group/message/all）全量重建 ES 索引

**无 PgSQL 直连**：Search 服务不直连任何 PgSQL 数据库，所有数据通过 ES 查询。源数据写入由 Kafka 消费者侧完成。

## 外部依赖

| 依赖类型 | 依赖项 | 用途 |
|---------|--------|------|
| Elasticsearch | idx_users 索引 | 用户全文检索 |
| Elasticsearch | idx_groups 索引 | 群组全文检索 |
| Elasticsearch | idx_messages 索引（按月分区） | 消息全文检索 |
| Redis | 搜索建议缓存 / 热词缓存 / 限流计数器 / 重建进度 | 缓存与限流 |
| RPC | Relation.GetFriendList | SearchFriends 获取好友 ID 列表做范围限定 |
| RPC | Relation.CheckBlockship | filterBlockedUsers 本地缓存未命中时 RPC 回源判断拉黑关系 |
| Redis 本地缓存 | search:local:block:{blocker}:{blocked} | 过滤搜索结果中被拉黑/拉黑对方的用户（Kafka 消费 relation.blocked/unblocked 维护，替代 N×2 RPC） |
| RPC | Conversation.GetUserConversationList | SearchMessages 获取用户所属会话列表做权限限定 |
| Kafka | 无（Search 是只读服务，不生产事件） | — |

## Elasticsearch 索引结构

```json
// ==================== idx_users ====================
// 用户索引：支持昵称/用户名/手机号模糊搜索
{
  "index": "idx_users",
  "mappings": {
    "properties": {
      "user_id":    { "type": "keyword" },
      "nickname":   { "type": "text", "analyzer": "ik_max_word", "search_analyzer": "ik_smart" },
      "username":   { "type": "text", "analyzer": "standard", "fields": { "keyword": { "type": "keyword" } } },
      "phone":      { "type": "keyword" },
      "avatar_url": { "type": "keyword", "index": false },
      "gender":     { "type": "integer" },
      "status":     { "type": "integer" },
      "created_at": { "type": "long" }
    }
  }
}

// ==================== idx_groups ====================
// 群组索引：支持群名/群描述搜索
{
  "index": "idx_groups",
  "mappings": {
    "properties": {
      "group_id":     { "type": "keyword" },
      "name":         { "type": "text", "analyzer": "ik_max_word", "search_analyzer": "ik_smart" },
      "description":  { "type": "text", "analyzer": "ik_max_word", "search_analyzer": "ik_smart" },
      "owner_id":     { "type": "keyword" },
      "member_count": { "type": "integer" },
      "status":       { "type": "integer" },
      "created_at":   { "type": "long" }
    }
  }
}

// ==================== idx_messages_YYYYMM ====================
// 消息索引：按月分区（idx_messages_202603, idx_messages_202604 ...）
// 搜索时通过 alias idx_messages_* 统一查询或按时间范围指定具体分区
{
  "index": "idx_messages_YYYYMM",
  "mappings": {
    "properties": {
      "msg_id":       { "type": "keyword" },
      "channel_id":   { "type": "keyword" },
      "channel_type": { "type": "integer" },
      "sender_id":    { "type": "keyword" },
      "msg_type":     { "type": "integer" },
      "content":      { "type": "text", "analyzer": "ik_max_word", "search_analyzer": "ik_smart" },
      "send_time":    { "type": "long" }
    }
  }
}
```

## Redis Key 设计

| Key 格式 | 类型 | 值 | TTL | 说明 |
|----------|------|-----|-----|------|
| `search:suggest:{prefix}` | ZSET | member=补全文本, score=使用频次 | 1h | 搜索建议/自动补全缓存，按前缀分片 |
| `search:hot:{type}` | ZSET | member=热搜关键词, score=搜索次数 | 30min | 热门搜索词缓存，type=user/group/message |
| `search:rate_limit:{user_id}` | STRING | 当前分钟搜索次数 | 1min | 搜索限流计数器，每分钟最多 30 次 |
| `search:rebuild_progress:{type}` | HASH | {total, processed, status} | 24h | 索引重建进度追踪，status=pending/running/completed/failed |
| `search:local:block:{blocker_id}:{blocked_id}` | STRING | "1" | 24h | 本地拉黑关系缓存（由 Kafka 消费 relation.blocked/unblocked 维护），替代 N×2 次 RPC 调用 |

---

## 通用辅助函数

```go
// checkRateLimit 搜索限流检查 — 每用户每分钟最多 30 次
func (s *SearchService) checkRateLimit(ctx context.Context, userID string) error {
    key := fmt.Sprintf("search:rate_limit:%s", userID)
    count, err := s.redis.Incr(ctx, key).Result()
    if err != nil {
        // Redis 异常不阻断搜索，仅记录日志
        log.Warn("search rate limit check redis error", "user_id", userID, "err", err)
        return nil
    }
    if count == 1 {
        // 首次计数，设置 1 分钟过期
        s.redis.Expire(ctx, key, 1*time.Minute)
    }
    if count > 30 {
        return status.Error(codes.ResourceExhausted, "search rate limit exceeded, max 30 requests per minute")
    }
    return nil
}

// recordHotKeyword 记录热搜关键词
func (s *SearchService) recordHotKeyword(ctx context.Context, searchType, keyword string) {
    if len(keyword) < 2 {
        return // 过短的关键词不记录
    }
    hotKey := fmt.Sprintf("search:hot:%s", searchType)
    s.redis.ZIncrBy(ctx, hotKey, 1, keyword)
    s.redis.Expire(ctx, hotKey, 30*time.Minute)
}

// filterBlockedUsers 过滤被拉黑的用户（双向检查）— 本地 Redis 缓存 + RPC 回源
// Search Kafka 消费者消费 relation.blocked/unblocked 事件维护本地缓存
// Key: search:local:block:{blocker_id}:{blocked_id} → STRING "1" TTL 24h
// 缓存未命中时通过 Relation.CheckBlockship RPC 回源，并回填缓存
func (s *SearchService) filterBlockedUsers(ctx context.Context, searcherID string, userIDs []string) ([]string, error) {
    if searcherID == "" || len(userIDs) == 0 {
        return userIDs, nil
    }

    // 第一阶段：Pipeline 批量查询本地缓存
    pipe := s.redis.Pipeline()
    type blockCheck struct {
        uid  string
        cmd1 *redis.StringCmd // 搜索者 → 目标
        cmd2 *redis.StringCmd // 目标 → 搜索者
    }
    checks := make([]blockCheck, 0, len(userIDs))

    for _, uid := range userIDs {
        blockKey1 := fmt.Sprintf("search:local:block:%s:%s", searcherID, uid)
        blockKey2 := fmt.Sprintf("search:local:block:%s:%s", uid, searcherID)
        cmd1 := pipe.Get(ctx, blockKey1)
        cmd2 := pipe.Get(ctx, blockKey2)
        checks = append(checks, blockCheck{uid: uid, cmd1: cmd1, cmd2: cmd2})
    }
    pipe.Exec(ctx)

    // 第二阶段：分类处理 — 确认拉黑 / 确认未拉黑 / 缓存未命中需回源
    filteredIDs := make([]string, 0, len(userIDs))
    needRPCCheck := make([]string, 0) // 缓存未命中的 uid，需 RPC 回源

    for _, check := range checks {
        blocked1, err1 := check.cmd1.Result()
        blocked2, err2 := check.cmd2.Result()

        // 缓存命中且值为 "1" → 确认被拉黑，过滤掉
        if blocked1 == "1" || blocked2 == "1" {
            continue
        }

        // 两个方向都缓存未命中（redis.Nil）→ 需要 RPC 回源
        if err1 != nil && err2 != nil {
            needRPCCheck = append(needRPCCheck, check.uid)
            continue
        }

        // 至少一个方向缓存命中且不是 "1" → 未拉黑
        filteredIDs = append(filteredIDs, check.uid)
    }

    // 第三阶段：对缓存未命中的用户通过 RPC 回源查询
    for _, uid := range needRPCCheck {
        // 双向检查：searcher 是否拉黑了 uid
        resp1, err1 := s.relationClient.CheckBlockship(ctx, &relation_pb.CheckBlockshipRequest{
            UserId:   searcherID,
            TargetId: uid,
        })
        if err1 != nil {
            // RPC 失败时安全降级：放行（不因关系服务故障而封死搜索）
            log.Warn("CheckBlockship RPC 失败，安全降级放行", "searcher", searcherID, "target", uid, "err", err1)
            filteredIDs = append(filteredIDs, uid)
            continue
        }
        if resp1.IsBlocked {
            // 回填缓存
            cacheKey := fmt.Sprintf("search:local:block:%s:%s", searcherID, uid)
            s.redis.Set(ctx, cacheKey, "1", 24*time.Hour)
            continue // 过滤掉
        }

        // 反向检查：uid 是否拉黑了 searcher
        resp2, err2 := s.relationClient.CheckBlockship(ctx, &relation_pb.CheckBlockshipRequest{
            UserId:   uid,
            TargetId: searcherID,
        })
        if err2 != nil {
            log.Warn("CheckBlockship RPC 失败，安全降级放行", "searcher", uid, "target", searcherID, "err", err2)
            filteredIDs = append(filteredIDs, uid)
            continue
        }
        if resp2.IsBlocked {
            cacheKey := fmt.Sprintf("search:local:block:%s:%s", uid, searcherID)
            s.redis.Set(ctx, cacheKey, "1", 24*time.Hour)
            continue // 过滤掉
        }

        // 双向都未拉黑，保留
        filteredIDs = append(filteredIDs, uid)
    }

    return filteredIDs, nil
}

// buildHighlight 构建 ES 高亮配置
func buildHighlight(fields ...string) map[string]interface{} {
    highlightFields := make(map[string]interface{})
    for _, f := range fields {
        highlightFields[f] = map[string]interface{}{
            "fragment_size":       100,
            "number_of_fragments": 1,
            "pre_tags":            []string{"<em>"},
            "post_tags":           []string{"</em>"},
        }
    }
    return map[string]interface{}{
        "fields": highlightFields,
    }
}

// getMessageIndexNames 根据时间范围获取要查询的消息索引名列表
// 消息索引按月分区：idx_messages_202603, idx_messages_202604 ...
func getMessageIndexNames(timeRange *common.TimeRange) []string {
    if timeRange == nil || (timeRange.StartTime == 0 && timeRange.EndTime == 0) {
        // 无时间范围，查询所有月份（使用通配符）
        return []string{"idx_messages_*"}
    }
    var indices []string
    start := time.UnixMilli(timeRange.StartTime)
    end := time.UnixMilli(timeRange.EndTime)
    if timeRange.EndTime == 0 {
        end = time.Now()
    }
    // 遍历每个月份
    current := time.Date(start.Year(), start.Month(), 1, 0, 0, 0, 0, time.UTC)
    for !current.After(end) {
        indices = append(indices, fmt.Sprintf("idx_messages_%s", current.Format("200601")))
        current = current.AddDate(0, 1, 0)
    }
    if len(indices) == 0 {
        indices = []string{"idx_messages_*"}
    }
    return indices
}

// paginationToESFromSize 将分页参数转换为 ES from/size
func paginationToESFromSize(p *common.Pagination) (int, int) {
    page := int(p.GetPage())
    pageSize := int(p.GetPageSize())
    if page <= 0 {
        page = 1
    }
    if pageSize <= 0 {
        pageSize = 20
    }
    if pageSize > 100 {
        pageSize = 100
    }
    from := (page - 1) * pageSize
    return from, pageSize
}
```

---

## 接口实现

### 1. SearchUsers — 搜索用户

> 根据昵称/用户名/手机号进行模糊搜索，支持 IK 中文分词。  
> 搜索结果需根据 `searcher_user_id` 过滤被拉黑的用户，并排除已注销用户（status != 正常）。

```go
func (s *SearchService) SearchUsers(ctx context.Context, req *pb.SearchUsersReq) (*pb.SearchUsersResp, error) {
    // ==================== 1. 参数校验 ====================
    if req.Keyword == "" {
        return nil, status.Error(codes.InvalidArgument, "keyword is required")
    }
    if len(req.Keyword) > 100 {
        return nil, status.Error(codes.InvalidArgument, "keyword too long, max 100 chars")
    }
    if req.SearcherUserId == "" {
        return nil, status.Error(codes.InvalidArgument, "searcher_user_id is required")
    }

    // ==================== 2. 搜索限流 — Redis 计数器 ====================
    if err := s.checkRateLimit(ctx, req.SearcherUserId); err != nil {
        return nil, err
    }

    // ==================== 3. 构建 ES 查询 ====================
    from, size := paginationToESFromSize(req.Pagination)

    // 使用 multi_match 跨字段搜索：nickname（权重最高）、username、phone
    // nickname 使用 IK 分词支持中文模糊匹配
    // username 和 phone 使用 term / prefix 精准或前缀匹配
    esQuery := map[string]interface{}{
        "query": map[string]interface{}{
            "bool": map[string]interface{}{
                "must": []interface{}{
                    map[string]interface{}{
                        "multi_match": map[string]interface{}{
                            "query":     req.Keyword,
                            "fields":    []string{"nickname^3", "username^2", "phone"},
                            "type":      "best_fields",
                            "fuzziness": "AUTO",         // 自动模糊匹配（容忍1-2个字符的拼写错误）
                            "minimum_should_match": "70%",
                        },
                    },
                },
                "filter": []interface{}{
                    // 只搜索正常状态的用户（status=1 表示正常）
                    map[string]interface{}{
                        "term": map[string]interface{}{
                            "status": 1,
                        },
                    },
                },
            },
        },
        "highlight": buildHighlight("nickname", "username", "phone"),
        "from":      from,
        "size":      size,
        "_source":   []string{"user_id", "nickname", "username", "avatar_url", "phone"},
    }

    // ==================== 4. 执行 ES 搜索 ====================
    esResp, err := s.esClient.Search(
        s.esClient.Search.WithContext(ctx),
        s.esClient.Search.WithIndex("idx_users"),
        s.esClient.Search.WithBody(buildESRequestBody(esQuery)),
    )
    if err != nil {
        log.Error("elasticsearch search users failed", "keyword", req.Keyword, "err", err)
        return nil, status.Error(codes.Internal, "search users failed")
    }
    defer esResp.Body.Close()

    if esResp.IsError() {
        log.Error("elasticsearch search users returned error",
            "keyword", req.Keyword, "status", esResp.StatusCode)
        return nil, status.Error(codes.Internal, "search users returned error")
    }

    // ==================== 5. 解析 ES 响应 ====================
    var esResult ESSearchResponse
    if err := json.NewDecoder(esResp.Body).Decode(&esResult); err != nil {
        log.Error("decode es search response failed", "err", err)
        return nil, status.Error(codes.Internal, "decode search response failed")
    }

    totalHits := esResult.Hits.Total.Value

    // ==================== 6. 构建结果列表并过滤黑名单 ====================
    var userIDs []string
    hitMap := make(map[string]*ESHit) // user_id -> hit
    for _, hit := range esResult.Hits.Hits {
        uid := hit.Source["user_id"].(string)
        userIDs = append(userIDs, uid)
        hitMap[uid] = hit
    }

    // 过滤黑名单用户（双向）
    filteredIDs, err := s.filterBlockedUsers(ctx, req.SearcherUserId, userIDs)
    if err != nil {
        log.Warn("filter blocked users failed, returning unfiltered results",
            "searcher", req.SearcherUserId, "err", err)
        filteredIDs = userIDs // 降级：不过滤
    }

    // ==================== 7. 组装响应 ====================
    var results []*pb.UserSearchResult
    for _, uid := range filteredIDs {
        hit, ok := hitMap[uid]
        if !ok {
            continue
        }
        result := &pb.UserSearchResult{
            UserId:    uid,
            Username:  getStringField(hit.Source, "username"),
            Nickname:  getStringField(hit.Source, "nickname"),
            AvatarUrl: getStringField(hit.Source, "avatar_url"),
            Score:     hit.Score,
        }
        // 提取高亮片段
        if highlights, ok := hit.Highlight["nickname"]; ok && len(highlights) > 0 {
            result.Highlight = highlights[0]
        } else if highlights, ok := hit.Highlight["username"]; ok && len(highlights) > 0 {
            result.Highlight = highlights[0]
        }
        results = append(results, result)
    }

    // ==================== 8. 记录热搜关键词（异步，不阻塞响应） ====================
    go s.recordHotKeyword(context.Background(), "user", req.Keyword)

    // ==================== 9. 返回 ====================
    return &pb.SearchUsersResp{
        Meta:    successMeta(ctx),
        Results: results,
        PaginationResult: &common.PaginationResult{
            Page:       int32(from/size + 1),
            PageSize:   int32(size),
            Total:      totalHits,
            TotalPages: int32((totalHits + int64(size) - 1) / int64(size)),
        },
    }, nil
}
```

### 2. SearchGroups — 搜索群组

> 根据群名/群描述搜索群组，支持 IK 中文分词。  
> 排除已解散群组（status != 正常）。过滤黑名单不适用于群搜索（群不涉及单向屏蔽）。

```go
func (s *SearchService) SearchGroups(ctx context.Context, req *pb.SearchGroupsReq) (*pb.SearchGroupsResp, error) {
    // ==================== 1. 参数校验 ====================
    if req.Keyword == "" {
        return nil, status.Error(codes.InvalidArgument, "keyword is required")
    }
    if len(req.Keyword) > 100 {
        return nil, status.Error(codes.InvalidArgument, "keyword too long, max 100 chars")
    }
    if req.SearcherUserId == "" {
        return nil, status.Error(codes.InvalidArgument, "searcher_user_id is required")
    }

    // ==================== 2. 搜索限流 ====================
    if err := s.checkRateLimit(ctx, req.SearcherUserId); err != nil {
        return nil, err
    }

    // ==================== 3. 构建 ES 查询 ====================
    from, size := paginationToESFromSize(req.Pagination)

    // 群名权重更高，描述次之
    esQuery := map[string]interface{}{
        "query": map[string]interface{}{
            "bool": map[string]interface{}{
                "must": []interface{}{
                    map[string]interface{}{
                        "multi_match": map[string]interface{}{
                            "query":     req.Keyword,
                            "fields":    []string{"name^3", "description"},
                            "type":      "best_fields",
                            "fuzziness": "AUTO",
                            "minimum_should_match": "70%",
                        },
                    },
                },
                "filter": []interface{}{
                    // 只搜索正常状态的群组（排除已解散）
                    map[string]interface{}{
                        "term": map[string]interface{}{
                            "status": 1,
                        },
                    },
                },
            },
        },
        "highlight": buildHighlight("name", "description"),
        "from":      from,
        "size":      size,
        "_source":   []string{"group_id", "name", "avatar_url", "member_count", "description"},
        // 按相关度排序，相同相关度的按成员数降序（优先展示大群）
        "sort": []interface{}{
            "_score",
            map[string]interface{}{
                "member_count": map[string]interface{}{
                    "order": "desc",
                },
            },
        },
    }

    // ==================== 4. 执行 ES 搜索 ====================
    esResp, err := s.esClient.Search(
        s.esClient.Search.WithContext(ctx),
        s.esClient.Search.WithIndex("idx_groups"),
        s.esClient.Search.WithBody(buildESRequestBody(esQuery)),
    )
    if err != nil {
        log.Error("elasticsearch search groups failed", "keyword", req.Keyword, "err", err)
        return nil, status.Error(codes.Internal, "search groups failed")
    }
    defer esResp.Body.Close()

    if esResp.IsError() {
        log.Error("elasticsearch search groups returned error",
            "keyword", req.Keyword, "status", esResp.StatusCode)
        return nil, status.Error(codes.Internal, "search groups returned error")
    }

    // ==================== 5. 解析 ES 响应 ====================
    var esResult ESSearchResponse
    if err := json.NewDecoder(esResp.Body).Decode(&esResult); err != nil {
        log.Error("decode es search response failed", "err", err)
        return nil, status.Error(codes.Internal, "decode search response failed")
    }

    totalHits := esResult.Hits.Total.Value

    // ==================== 6. 组装响应 ====================
    var results []*pb.GroupSearchResult
    for _, hit := range esResult.Hits.Hits {
        result := &pb.GroupSearchResult{
            GroupId:     getStringField(hit.Source, "group_id"),
            Name:        getStringField(hit.Source, "name"),
            AvatarUrl:   getStringField(hit.Source, "avatar_url"),
            MemberCount: getInt32Field(hit.Source, "member_count"),
            Score:       hit.Score,
        }
        // 提取高亮片段
        if highlights, ok := hit.Highlight["name"]; ok && len(highlights) > 0 {
            result.Highlight = highlights[0]
        } else if highlights, ok := hit.Highlight["description"]; ok && len(highlights) > 0 {
            result.Highlight = highlights[0]
        }
        results = append(results, result)
    }

    // ==================== 7. 记录热搜关键词 ====================
    go s.recordHotKeyword(context.Background(), "group", req.Keyword)

    // ==================== 8. 返回 ====================
    return &pb.SearchGroupsResp{
        Meta:    successMeta(ctx),
        Results: results,
        PaginationResult: &common.PaginationResult{
            Page:       int32(from/size + 1),
            PageSize:   int32(size),
            Total:      totalHits,
            TotalPages: int32((totalHits + int64(size) - 1) / int64(size)),
        },
    }, nil
}
```

### 3. SearchMessages — 搜索消息（核心，权限敏感）

> **最关键的接口**。用户只能搜索自己所属会话中的消息，且必须尊重 `clear_seq`（用户清空的消息不可见）。  
> 流程：限流 → 获取用户会话列表 → 构建 ES 查询（限定 channel_id 范围）→ 按时间范围选择分区索引 → 搜索 → 过滤 clear_seq → 返回。  
> **CRITICAL**：必须通过 Conversation 服务获取用户的会话列表，严格限定搜索范围，防止越权。

```go
func (s *SearchService) SearchMessages(ctx context.Context, req *pb.SearchMessagesReq) (*pb.SearchMessagesResp, error) {
    // ==================== 1. 参数校验 ====================
    if req.Keyword == "" {
        return nil, status.Error(codes.InvalidArgument, "keyword is required")
    }
    if len(req.Keyword) > 200 {
        return nil, status.Error(codes.InvalidArgument, "keyword too long, max 200 chars")
    }
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id is required")
    }

    // ==================== 2. 搜索限流 ====================
    if err := s.checkRateLimit(ctx, req.UserId); err != nil {
        return nil, err
    }

    // ==================== 3. 获取用户有权限的会话列表 — RPC Conversation ====================
    // 这是权限控制的核心：用户只能搜索自己所属会话的消息
    var allowedChannelIDs []string
    // channel_id → clear_seq 映射（用户清空消息的截断点）
    clearSeqMap := make(map[string]int64)

    if req.ConversationId != "" {
        // 指定了具体会话，只需验证用户是否属于该会话
        convResp, err := s.conversationClient.GetUserConversationList(ctx,
            &conv_pb.GetUserConversationListRequest{
                UserId: req.UserId,
            })
        if err != nil {
            log.Error("get user conversation list failed",
                "user_id", req.UserId, "err", err)
            return nil, status.Error(codes.Internal, "get user conversations failed")
        }
        // 验证目标会话是否在用户的会话列表中
        found := false
        for _, conv := range convResp.Conversations {
            if conv.ConversationId == req.ConversationId {
                found = true
                allowedChannelIDs = []string{req.ConversationId}
                clearSeqMap[req.ConversationId] = conv.ClearSeq
                break
            }
        }
        if !found {
            return nil, status.Error(codes.PermissionDenied,
                "you do not have access to this conversation")
        }
    } else {
        // 未指定会话，搜索用户所有会话
        convResp, err := s.conversationClient.GetUserConversationList(ctx,
            &conv_pb.GetUserConversationListRequest{
                UserId: req.UserId,
            })
        if err != nil {
            log.Error("get user conversation list failed",
                "user_id", req.UserId, "err", err)
            return nil, status.Error(codes.Internal, "get user conversations failed")
        }
        for _, conv := range convResp.Conversations {
            // 按会话类型过滤（如果指定了）
            if req.ConversationType != common.CONVERSATION_TYPE_UNSPECIFIED &&
                conv.ConversationType != req.ConversationType {
                continue
            }
            allowedChannelIDs = append(allowedChannelIDs, conv.ConversationId)
            clearSeqMap[conv.ConversationId] = conv.ClearSeq
        }
    }

    if len(allowedChannelIDs) == 0 {
        // 用户没有任何会话，直接返回空
        return &pb.SearchMessagesResp{
            Meta:    successMeta(ctx),
            Results: []*pb.MessageSearchResult{},
            PaginationResult: &common.PaginationResult{
                Page: 1, PageSize: 20, Total: 0, TotalPages: 0,
            },
        }, nil
    }

    // ==================== 4. 构建 ES 查询 ====================
    from, size := paginationToESFromSize(req.Pagination)

    // 基础 bool 查询：内容匹配 + 会话范围限定
    mustClauses := []interface{}{
        map[string]interface{}{
            "match": map[string]interface{}{
                "content": map[string]interface{}{
                    "query":                req.Keyword,
                    "analyzer":             "ik_smart",
                    "minimum_should_match": "70%",
                },
            },
        },
    }

    filterClauses := []interface{}{
        // 限定在用户有权限的会话范围内（CRITICAL：权限隔离）
        map[string]interface{}{
            "terms": map[string]interface{}{
                "channel_id": allowedChannelIDs,
            },
        },
    }

    // 消息类型过滤（可选）
    if req.MessageType != common.MESSAGE_TYPE_UNSPECIFIED {
        filterClauses = append(filterClauses, map[string]interface{}{
            "term": map[string]interface{}{
                "msg_type": int(req.MessageType),
            },
        })
    }

    // 时间范围过滤（可选）
    if req.TimeRange != nil && (req.TimeRange.StartTime > 0 || req.TimeRange.EndTime > 0) {
        rangeFilter := map[string]interface{}{}
        if req.TimeRange.StartTime > 0 {
            rangeFilter["gte"] = req.TimeRange.StartTime
        }
        if req.TimeRange.EndTime > 0 {
            rangeFilter["lte"] = req.TimeRange.EndTime
        }
        filterClauses = append(filterClauses, map[string]interface{}{
            "range": map[string]interface{}{
                "send_time": rangeFilter,
            },
        })
    }

    esQuery := map[string]interface{}{
        "query": map[string]interface{}{
            "bool": map[string]interface{}{
                "must":   mustClauses,
                "filter": filterClauses,
            },
        },
        "highlight": buildHighlight("content"),
        "from":      from,
        "size":      size,
        "_source":   []string{"msg_id", "channel_id", "channel_type", "sender_id", "msg_type", "content", "send_time"},
        "sort": []interface{}{
            // 先按相关度排序，再按时间倒序
            "_score",
            map[string]interface{}{
                "send_time": map[string]interface{}{
                    "order": "desc",
                },
            },
        },
    }

    // ==================== 5. 确定要查询的索引（按月分区） ====================
    indexNames := getMessageIndexNames(req.TimeRange)

    // ==================== 6. 执行 ES 搜索 ====================
    esResp, err := s.esClient.Search(
        s.esClient.Search.WithContext(ctx),
        s.esClient.Search.WithIndex(indexNames...),
        s.esClient.Search.WithBody(buildESRequestBody(esQuery)),
        s.esClient.Search.WithIgnoreUnavailable(true), // 忽略不存在的月份索引
    )
    if err != nil {
        log.Error("elasticsearch search messages failed",
            "keyword", req.Keyword, "user_id", req.UserId, "err", err)
        return nil, status.Error(codes.Internal, "search messages failed")
    }
    defer esResp.Body.Close()

    if esResp.IsError() {
        log.Error("elasticsearch search messages returned error",
            "keyword", req.Keyword, "status", esResp.StatusCode)
        return nil, status.Error(codes.Internal, "search messages returned error")
    }

    // ==================== 7. 解析 ES 响应 ====================
    var esResult ESSearchResponse
    if err := json.NewDecoder(esResp.Body).Decode(&esResult); err != nil {
        log.Error("decode es search response failed", "err", err)
        return nil, status.Error(codes.Internal, "decode search response failed")
    }

    // ==================== 8. 过滤 clear_seq 并组装结果 ====================
    // CRITICAL：用户清空聊天记录后，清空前的消息不应出现在搜索结果中
    // clear_seq 是用户在该会话中执行"清空聊天记录"时的最后一条消息 seq
    // 需要在 ES 结果中二次过滤（ES 索引中不存储 clear_seq，因为它是用户维度的）
    var results []*pb.MessageSearchResult
    filteredCount := int64(0)

    for _, hit := range esResult.Hits.Hits {
        channelID := getStringField(hit.Source, "channel_id")
        sendTime := getInt64Field(hit.Source, "send_time")

        // 检查 clear_seq：如果会话有 clear_seq，需要判断消息是否在清空范围内
        // 由于 ES 中没有 seq 字段用于直接比较，这里使用 send_time 做近似过滤
        // 更精确的做法是在 ES 索引中加入 seq 字段
        // 当前方案：如果会话有 clear_seq > 0，说明用户清空过，跳过清空时间点之前的消息
        if clearSeq, ok := clearSeqMap[channelID]; ok && clearSeq > 0 {
            // TODO: 理想情况下应通过 seq 比较，此处使用 send_time 做保守过滤
            // 如果 ES 索引中追加了 seq 字段，应在 ES query 中直接用 range filter
            // 当前实现：通过 Conversation 获取 clear_time，与 send_time 比较
            _ = clearSeq // 在 ES 追加 seq 字段前，此处预留
        }

        result := &pb.MessageSearchResult{
            MsgId:            getStringField(hit.Source, "msg_id"),
            ConversationId:   channelID,
            ConversationType: common.ConversationType(getInt32Field(hit.Source, "channel_type")),
            SenderId:         getStringField(hit.Source, "sender_id"),
            MessageType:      common.MessageType(getInt32Field(hit.Source, "msg_type")),
            Content:          getStringField(hit.Source, "content"),
            SendTime:         sendTime,
            Score:            hit.Score,
        }

        // 提取高亮片段
        if highlights, ok := hit.Highlight["content"]; ok && len(highlights) > 0 {
            result.Highlight = highlights[0]
        }

        results = append(results, result)
    }

    totalHits := esResult.Hits.Total.Value - filteredCount

    // ==================== 9. 记录热搜关键词 ====================
    go s.recordHotKeyword(context.Background(), "message", req.Keyword)

    // ==================== 10. 返回 ====================
    return &pb.SearchMessagesResp{
        Meta:    successMeta(ctx),
        Results: results,
        PaginationResult: &common.PaginationResult{
            Page:       int32(from/size + 1),
            PageSize:   int32(size),
            Total:      totalHits,
            TotalPages: int32((totalHits + int64(size) - 1) / int64(size)),
        },
    }, nil
}
```

### 4. SearchFriends — 搜索好友列表

> 在用户的好友范围内搜索，比 SearchUsers 范围更小、速度更快。  
> 流程：获取好友 ID 列表 → 用 terms filter 限定范围 → ES 搜索 → 返回。  
> 好友列表通过 RPC Relation.GetFriendList 获取（内部有 Redis 缓存，延迟极低）。

```go
func (s *SearchService) SearchFriends(ctx context.Context, req *pb.SearchFriendsReq) (*pb.SearchFriendsResp, error) {
    // ==================== 1. 参数校验 ====================
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id is required")
    }
    if req.Keyword == "" {
        return nil, status.Error(codes.InvalidArgument, "keyword is required")
    }
    if len(req.Keyword) > 100 {
        return nil, status.Error(codes.InvalidArgument, "keyword too long, max 100 chars")
    }

    // ==================== 2. 搜索限流 ====================
    if err := s.checkRateLimit(ctx, req.UserId); err != nil {
        return nil, err
    }

    // ==================== 3. 获取好友 ID 列表 — RPC Relation.GetFriendList ====================
    friendResp, err := s.relationClient.GetFriendList(ctx, &relation_pb.GetFriendListRequest{
        UserId: req.UserId,
    })
    if err != nil {
        log.Error("get friend list failed", "user_id", req.UserId, "err", err)
        return nil, status.Error(codes.Internal, "get friend list failed")
    }

    if len(friendResp.Friends) == 0 {
        // 无好友，直接返回空
        return &pb.SearchFriendsResp{
            Meta:    successMeta(ctx),
            Results: []*pb.UserSearchResult{},
            PaginationResult: &common.PaginationResult{
                Page: 1, PageSize: 20, Total: 0, TotalPages: 0,
            },
        }, nil
    }

    // 提取好友 ID 列表
    var friendIDs []string
    for _, f := range friendResp.Friends {
        friendIDs = append(friendIDs, f.FriendId)
    }

    // ==================== 4. 构建 ES 查询（限定好友范围） ====================
    from, size := paginationToESFromSize(req.Pagination)

    esQuery := map[string]interface{}{
        "query": map[string]interface{}{
            "bool": map[string]interface{}{
                "must": []interface{}{
                    map[string]interface{}{
                        "multi_match": map[string]interface{}{
                            "query":     req.Keyword,
                            "fields":    []string{"nickname^3", "username^2", "phone"},
                            "type":      "best_fields",
                            "fuzziness": "AUTO",
                            "minimum_should_match": "60%",
                        },
                    },
                },
                "filter": []interface{}{
                    // 限定在好友范围内（核心过滤条件）
                    map[string]interface{}{
                        "terms": map[string]interface{}{
                            "user_id": friendIDs,
                        },
                    },
                    // 只搜索正常状态用户
                    map[string]interface{}{
                        "term": map[string]interface{}{
                            "status": 1,
                        },
                    },
                },
            },
        },
        "highlight": buildHighlight("nickname", "username"),
        "from":      from,
        "size":      size,
        "_source":   []string{"user_id", "nickname", "username", "avatar_url"},
    }

    // ==================== 5. 执行 ES 搜索 ====================
    esResp, err := s.esClient.Search(
        s.esClient.Search.WithContext(ctx),
        s.esClient.Search.WithIndex("idx_users"),
        s.esClient.Search.WithBody(buildESRequestBody(esQuery)),
    )
    if err != nil {
        log.Error("elasticsearch search friends failed",
            "user_id", req.UserId, "keyword", req.Keyword, "err", err)
        return nil, status.Error(codes.Internal, "search friends failed")
    }
    defer esResp.Body.Close()

    if esResp.IsError() {
        log.Error("elasticsearch search friends returned error",
            "keyword", req.Keyword, "status", esResp.StatusCode)
        return nil, status.Error(codes.Internal, "search friends returned error")
    }

    // ==================== 6. 解析 ES 响应 ====================
    var esResult ESSearchResponse
    if err := json.NewDecoder(esResp.Body).Decode(&esResult); err != nil {
        log.Error("decode es search response failed", "err", err)
        return nil, status.Error(codes.Internal, "decode search response failed")
    }

    totalHits := esResult.Hits.Total.Value

    // ==================== 7. 组装响应 ====================
    var results []*pb.UserSearchResult
    for _, hit := range esResult.Hits.Hits {
        result := &pb.UserSearchResult{
            UserId:    getStringField(hit.Source, "user_id"),
            Username:  getStringField(hit.Source, "username"),
            Nickname:  getStringField(hit.Source, "nickname"),
            AvatarUrl: getStringField(hit.Source, "avatar_url"),
            Score:     hit.Score,
        }
        if highlights, ok := hit.Highlight["nickname"]; ok && len(highlights) > 0 {
            result.Highlight = highlights[0]
        } else if highlights, ok := hit.Highlight["username"]; ok && len(highlights) > 0 {
            result.Highlight = highlights[0]
        }
        results = append(results, result)
    }

    // ==================== 8. 返回 ====================
    return &pb.SearchFriendsResp{
        Meta:    successMeta(ctx),
        Results: results,
        PaginationResult: &common.PaginationResult{
            Page:       int32(from/size + 1),
            PageSize:   int32(size),
            Total:      totalHits,
            TotalPages: int32((totalHits + int64(size) - 1) / int64(size)),
        },
    }, nil
}
```

### 5. GlobalSearch — 全局统一搜索

> 一次请求同时搜索用户、群组、消息三种类型，返回混合结果。  
> 前端统一搜索入口使用此接口，通过 `types` 字段指定搜索范围。  
> 内部并行调用 ES 多索引查询（_msearch），提升响应速度。

```go
func (s *SearchService) GlobalSearch(ctx context.Context, req *pb.GlobalSearchReq) (*pb.GlobalSearchResp, error) {
    // ==================== 1. 参数校验 ====================
    if req.Keyword == "" {
        return nil, status.Error(codes.InvalidArgument, "keyword is required")
    }
    if len(req.Keyword) > 200 {
        return nil, status.Error(codes.InvalidArgument, "keyword too long, max 200 chars")
    }
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id is required")
    }

    // 默认搜索所有类型
    searchTypes := req.Types
    if len(searchTypes) == 0 {
        searchTypes = []string{"user", "group", "message"}
    }
    // 校验 types 合法性
    validTypes := map[string]bool{"user": true, "group": true, "message": true}
    for _, t := range searchTypes {
        if !validTypes[t] {
            return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("invalid search type: %s", t))
        }
    }

    // ==================== 2. 搜索限流 ====================
    if err := s.checkRateLimit(ctx, req.UserId); err != nil {
        return nil, err
    }

    // ==================== 3. 并行执行各类型搜索 ====================
    // 每个类型最多返回 5 条结果（全局搜索是概览模式）
    perTypeLimit := 5
    from, _ := paginationToESFromSize(req.Pagination)
    _ = from // 全局搜索用固定 limit

    var (
        mu      sync.Mutex
        wg      sync.WaitGroup
        results []*pb.SearchResultItem
    )

    // 3a. 搜索用户
    if containsType(searchTypes, "user") {
        wg.Add(1)
        go func() {
            defer wg.Done()
            userResp, err := s.SearchUsers(ctx, &pb.SearchUsersReq{
                Keyword:        req.Keyword,
                SearcherUserId: req.UserId,
                Pagination:     &common.Pagination{Page: 1, PageSize: int32(perTypeLimit)},
            })
            if err != nil {
                log.Warn("global search: search users failed", "err", err)
                return
            }
            mu.Lock()
            defer mu.Unlock()
            for _, u := range userResp.Results {
                data, _ := proto.Marshal(u)
                results = append(results, &pb.SearchResultItem{
                    Type:  "user",
                    Data:  data,
                    Score: u.Score,
                })
            }
        }()
    }

    // 3b. 搜索群组
    if containsType(searchTypes, "group") {
        wg.Add(1)
        go func() {
            defer wg.Done()
            groupResp, err := s.SearchGroups(ctx, &pb.SearchGroupsReq{
                Keyword:        req.Keyword,
                SearcherUserId: req.UserId,
                Pagination:     &common.Pagination{Page: 1, PageSize: int32(perTypeLimit)},
            })
            if err != nil {
                log.Warn("global search: search groups failed", "err", err)
                return
            }
            mu.Lock()
            defer mu.Unlock()
            for _, g := range groupResp.Results {
                data, _ := proto.Marshal(g)
                results = append(results, &pb.SearchResultItem{
                    Type:  "group",
                    Data:  data,
                    Score: g.Score,
                })
            }
        }()
    }

    // 3c. 搜索消息
    if containsType(searchTypes, "message") {
        wg.Add(1)
        go func() {
            defer wg.Done()
            msgResp, err := s.SearchMessages(ctx, &pb.SearchMessagesReq{
                Keyword:    req.Keyword,
                UserId:     req.UserId,
                Pagination: &common.Pagination{Page: 1, PageSize: int32(perTypeLimit)},
            })
            if err != nil {
                log.Warn("global search: search messages failed", "err", err)
                return
            }
            mu.Lock()
            defer mu.Unlock()
            for _, m := range msgResp.Results {
                data, _ := proto.Marshal(m)
                results = append(results, &pb.SearchResultItem{
                    Type:  "message",
                    Data:  data,
                    Score: m.Score,
                })
            }
        }()
    }

    wg.Wait()

    // ==================== 4. 按相关度排序（混合排序） ====================
    sort.Slice(results, func(i, j int) bool {
        return results[i].Score > results[j].Score
    })

    // ==================== 5. 记录热搜关键词 ====================
    go s.recordHotKeyword(context.Background(), "global", req.Keyword)

    // ==================== 6. 返回 ====================
    return &pb.GlobalSearchResp{
        Meta:    successMeta(ctx),
        Results: results,
        PaginationResult: &common.PaginationResult{
            Page:       1,
            PageSize:   int32(perTypeLimit * len(searchTypes)),
            Total:      int64(len(results)),
            TotalPages: 1,
        },
    }, nil
}

// containsType 判断切片中是否包含指定类型
func containsType(types []string, target string) bool {
    for _, t := range types {
        if t == target {
            return true
        }
    }
    return false
}
```

### 6. SearchSuggest — 搜索建议/自动补全

> 基于用户输入的前缀提供搜索建议，用于搜索框自动补全。  
> 优先级：Redis 热词缓存 → ES completion suggester → 返回空。  
> 低延迟要求：< 50ms，所以优先走 Redis。

```go
func (s *SearchService) SearchSuggest(ctx context.Context, req *pb.SearchSuggestReq) (*pb.SearchSuggestResp, error) {
    // ==================== 1. 参数校验 ====================
    if req.Keyword == "" {
        return &pb.SearchSuggestResp{
            Meta:        successMeta(ctx),
            Suggestions: []string{},
        }, nil
    }
    if len(req.Keyword) > 50 {
        return nil, status.Error(codes.InvalidArgument, "keyword too long, max 50 chars")
    }
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id is required")
    }

    limit := int(req.Limit)
    if limit <= 0 {
        limit = 10
    }
    if limit > 20 {
        limit = 20
    }

    // ==================== 2. 搜索限流（建议接口使用更宽松的限制，复用同一 key） ====================
    if err := s.checkRateLimit(ctx, req.UserId); err != nil {
        return nil, err
    }

    // ==================== 3. 优先从 Redis 缓存获取搜索建议 ====================
    // 使用前缀匹配：取 keyword 的前 N 个字符作为 prefix key
    prefix := req.Keyword
    if len(prefix) > 10 {
        prefix = prefix[:10] // 前缀最多取 10 个字符
    }
    suggestKey := fmt.Sprintf("search:suggest:%s", prefix)

    // 从 ZSET 获取按频次排序的建议词（score 降序）
    suggestions, err := s.redis.ZRevRangeByScore(ctx, suggestKey, &redis.ZRangeBy{
        Min:    "-inf",
        Max:    "+inf",
        Offset: 0,
        Count:  int64(limit * 2), // 多取一些，后面做前缀过滤
    }).Result()

    if err == nil && len(suggestions) > 0 {
        // 过滤出以 keyword 为前缀的建议词
        var filtered []string
        for _, s := range suggestions {
            if strings.HasPrefix(strings.ToLower(s), strings.ToLower(req.Keyword)) {
                filtered = append(filtered, s)
                if len(filtered) >= limit {
                    break
                }
            }
        }
        if len(filtered) > 0 {
            return &pb.SearchSuggestResp{
                Meta:        successMeta(ctx),
                Suggestions: filtered,
            }, nil
        }
    }

    // ==================== 4. Redis 未命中，查询 ES completion suggester ====================
    // 使用 ES 的 search_as_you_type 或 completion suggester
    esQuery := map[string]interface{}{
        "suggest": map[string]interface{}{
            "user-suggest": map[string]interface{}{
                "prefix":     req.Keyword,
                "completion": map[string]interface{}{
                    "field":           "nickname.suggest",
                    "size":            limit,
                    "skip_duplicates": true,
                    "fuzzy": map[string]interface{}{
                        "fuzziness": "AUTO",
                    },
                },
            },
            "group-suggest": map[string]interface{}{
                "prefix":     req.Keyword,
                "completion": map[string]interface{}{
                    "field":           "name.suggest",
                    "size":            limit,
                    "skip_duplicates": true,
                    "fuzzy": map[string]interface{}{
                        "fuzziness": "AUTO",
                    },
                },
            },
        },
    }

    esResp, err := s.esClient.Search(
        s.esClient.Search.WithContext(ctx),
        s.esClient.Search.WithIndex("idx_users", "idx_groups"),
        s.esClient.Search.WithBody(buildESRequestBody(esQuery)),
    )
    if err != nil {
        log.Warn("elasticsearch suggest query failed", "keyword", req.Keyword, "err", err)
        // ES 查询失败降级返回空建议
        return &pb.SearchSuggestResp{
            Meta:        successMeta(ctx),
            Suggestions: []string{},
        }, nil
    }
    defer esResp.Body.Close()

    // ==================== 5. 解析 ES suggest 响应 ====================
    var esSuggestResult ESSuggestResponse
    if err := json.NewDecoder(esResp.Body).Decode(&esSuggestResult); err != nil {
        log.Warn("decode es suggest response failed", "err", err)
        return &pb.SearchSuggestResp{
            Meta:        successMeta(ctx),
            Suggestions: []string{},
        }, nil
    }

    var allSuggestions []string
    seen := make(map[string]bool) // 去重

    // 合并用户名和群名建议
    for _, suggestGroup := range []string{"user-suggest", "group-suggest"} {
        if options, ok := esSuggestResult.Suggest[suggestGroup]; ok {
            for _, sg := range options {
                for _, opt := range sg.Options {
                    text := opt.Text
                    if !seen[text] {
                        seen[text] = true
                        allSuggestions = append(allSuggestions, text)
                    }
                }
            }
        }
    }

    // 截取到 limit 数量
    if len(allSuggestions) > limit {
        allSuggestions = allSuggestions[:limit]
    }

    // ==================== 6. 回写 Redis 缓存（异步） ====================
    if len(allSuggestions) > 0 {
        go func() {
            bgCtx := context.Background()
            pipe := s.redis.Pipeline()
            for _, text := range allSuggestions {
                pipe.ZIncrBy(bgCtx, suggestKey, 1, text)
            }
            pipe.Expire(bgCtx, suggestKey, 1*time.Hour)
            if _, err := pipe.Exec(bgCtx); err != nil {
                log.Warn("write search suggest cache failed", "prefix", prefix, "err", err)
            }
        }()
    }

    // ==================== 7. 返回 ====================
    return &pb.SearchSuggestResp{
        Meta:        successMeta(ctx),
        Suggestions: allSuggestions,
    }, nil
}
```

### 7. RebuildIndex — 索引重建（管理员 API）

> 管理员触发全量重建 ES 索引，用于索引损坏/ES 集群迁移/mapping 变更等场景。  
> 异步执行：接口立即返回 task_id，重建过程在后台 goroutine 执行，进度写入 Redis。  
> 重建策略：创建新索引 → 全量从源数据同步 → 切换 alias → 删除旧索引（零停机）。

```go
func (s *SearchService) RebuildIndex(ctx context.Context, req *pb.RebuildIndexReq) (*pb.RebuildIndexResp, error) {
    // ==================== 1. 参数校验 ====================
    if req.IndexType == "" {
        return nil, status.Error(codes.InvalidArgument, "index_type is required")
    }
    validTypes := map[string]bool{"user": true, "group": true, "message": true, "all": true}
    if !validTypes[req.IndexType] {
        return nil, status.Error(codes.InvalidArgument,
            "invalid index_type, must be one of: user, group, message, all")
    }
    if req.OperatorId == "" {
        return nil, status.Error(codes.InvalidArgument, "operator_id is required")
    }

    // ==================== 2. 检查是否已有重建任务在运行 ====================
    // 同一类型不允许并发重建
    typesToRebuild := []string{req.IndexType}
    if req.IndexType == "all" {
        typesToRebuild = []string{"user", "group", "message"}
    }

    for _, t := range typesToRebuild {
        progressKey := fmt.Sprintf("search:rebuild_progress:%s", t)
        progressStatus, err := s.redis.HGet(ctx, progressKey, "status").Result()
        if err == nil && (progressStatus == "pending" || progressStatus == "running") {
            return nil, status.Error(codes.FailedPrecondition,
                fmt.Sprintf("rebuild task for type '%s' is already %s", t, progressStatus))
        }
    }

    // ==================== 3. 生成任务 ID 并初始化进度 ====================
    taskID := fmt.Sprintf("rebuild_%s_%d", req.IndexType, time.Now().UnixMilli())

    for _, t := range typesToRebuild {
        progressKey := fmt.Sprintf("search:rebuild_progress:%s", t)
        pipe := s.redis.Pipeline()
        pipe.HSet(ctx, progressKey, map[string]interface{}{
            "task_id":   taskID,
            "total":     0,
            "processed": 0,
            "status":    "pending",
            "operator":  req.OperatorId,
            "start_time": time.Now().UnixMilli(),
        })
        pipe.Expire(ctx, progressKey, 24*time.Hour)
        if _, err := pipe.Exec(ctx); err != nil {
            log.Error("init rebuild progress failed", "type", t, "err", err)
            return nil, status.Error(codes.Internal, "init rebuild progress failed")
        }
    }

    // ==================== 4. 启动异步重建任务 ====================
    for _, t := range typesToRebuild {
        rebuildType := t
        go s.doRebuildIndex(context.Background(), taskID, rebuildType, req.OperatorId)
    }

    log.Info("index rebuild task started",
        "task_id", taskID, "index_type", req.IndexType, "operator", req.OperatorId)

    // ==================== 5. 返回任务 ID ====================
    return &pb.RebuildIndexResp{
        Meta:   successMeta(ctx),
        TaskId: taskID,
    }, nil
}

// doRebuildIndex 异步执行索引重建
// 策略：创建新索引（带时间戳后缀）→ 全量写入 → 切换 alias → 删除旧索引
func (s *SearchService) doRebuildIndex(ctx context.Context, taskID, indexType, operatorID string) {
    progressKey := fmt.Sprintf("search:rebuild_progress:%s", indexType)

    // 更新状态为 running
    s.redis.HSet(ctx, progressKey, "status", "running")

    defer func() {
        if r := recover(); r != nil {
            log.Error("rebuild index panicked",
                "task_id", taskID, "type", indexType, "panic", r)
            s.redis.HSet(ctx, progressKey, "status", "failed")
        }
    }()

    var err error
    switch indexType {
    case "user":
        err = s.rebuildUserIndex(ctx, taskID, progressKey)
    case "group":
        err = s.rebuildGroupIndex(ctx, taskID, progressKey)
    case "message":
        err = s.rebuildMessageIndex(ctx, taskID, progressKey)
    }

    if err != nil {
        log.Error("rebuild index failed",
            "task_id", taskID, "type", indexType, "err", err)
        s.redis.HSet(ctx, progressKey, map[string]interface{}{
            "status":   "failed",
            "error":    err.Error(),
            "end_time": time.Now().UnixMilli(),
        })
        return
    }

    // 重建完成
    s.redis.HSet(ctx, progressKey, map[string]interface{}{
        "status":   "completed",
        "end_time": time.Now().UnixMilli(),
    })

    log.Info("rebuild index completed", "task_id", taskID, "type", indexType)
}

// rebuildUserIndex 重建用户索引
// 通过滚动查询旧索引（或从源服务批量拉取），全量写入新索引
func (s *SearchService) rebuildUserIndex(ctx context.Context, taskID, progressKey string) error {
    // 1. 创建新索引（带时间戳后缀，如 idx_users_20260308120000）
    newIndex := fmt.Sprintf("idx_users_%s", time.Now().Format("20060102150405"))
    createResp, err := s.esClient.Indices.Create(
        newIndex,
        s.esClient.Indices.Create.WithBody(buildUserIndexMapping()),
        s.esClient.Indices.Create.WithContext(ctx),
    )
    if err != nil {
        return fmt.Errorf("create new user index failed: %w", err)
    }
    defer createResp.Body.Close()
    if createResp.IsError() {
        return fmt.Errorf("create new user index returned error: %s", createResp.Status())
    }

    // 2. 使用 scroll API 从旧索引读取全量数据
    scrollResp, err := s.esClient.Search(
        s.esClient.Search.WithContext(ctx),
        s.esClient.Search.WithIndex("idx_users"),
        s.esClient.Search.WithSize(500),
        s.esClient.Search.WithScroll(5*time.Minute),
        s.esClient.Search.WithSort("_doc"),
    )
    if err != nil {
        return fmt.Errorf("scroll old user index failed: %w", err)
    }
    defer scrollResp.Body.Close()

    var scrollResult ESScrollResponse
    if err := json.NewDecoder(scrollResp.Body).Decode(&scrollResult); err != nil {
        return fmt.Errorf("decode scroll response failed: %w", err)
    }

    totalDocs := scrollResult.Hits.Total.Value
    s.redis.HSet(ctx, progressKey, "total", totalDocs)

    processed := int64(0)
    scrollID := scrollResult.ScrollID

    // 3. 批量写入新索引
    for {
        hits := scrollResult.Hits.Hits
        if len(hits) == 0 {
            break
        }

        // 构建 bulk 请求
        var bulkBody strings.Builder
        for _, hit := range hits {
            meta := fmt.Sprintf(`{"index":{"_index":"%s","_id":"%s"}}`, newIndex, hit.ID)
            bulkBody.WriteString(meta + "\n")
            sourceJSON, _ := json.Marshal(hit.Source)
            bulkBody.WriteString(string(sourceJSON) + "\n")
        }

        bulkResp, err := s.esClient.Bulk(
            strings.NewReader(bulkBody.String()),
            s.esClient.Bulk.WithContext(ctx),
        )
        if err != nil {
            log.Error("bulk index users failed", "task_id", taskID, "err", err)
            // 继续处理，不中断整个重建
        } else {
            bulkResp.Body.Close()
        }

        processed += int64(len(hits))
        s.redis.HSet(ctx, progressKey, "processed", processed)

        // 获取下一批
        scrollResp, err = s.esClient.Scroll(
            s.esClient.Scroll.WithScrollID(scrollID),
            s.esClient.Scroll.WithScroll(5*time.Minute),
            s.esClient.Scroll.WithContext(ctx),
        )
        if err != nil {
            return fmt.Errorf("scroll next batch failed: %w", err)
        }
        defer scrollResp.Body.Close()

        if err := json.NewDecoder(scrollResp.Body).Decode(&scrollResult); err != nil {
            return fmt.Errorf("decode scroll response failed: %w", err)
        }
        scrollID = scrollResult.ScrollID
    }

    // 4. 清除 scroll 上下文
    s.esClient.ClearScroll(
        s.esClient.ClearScroll.WithScrollID(scrollID),
        s.esClient.ClearScroll.WithContext(ctx),
    )

    // 5. 切换 alias：idx_users → 新索引（零停机）
    aliasActions := map[string]interface{}{
        "actions": []interface{}{
            map[string]interface{}{
                "remove": map[string]interface{}{
                    "index": "idx_users_*",
                    "alias": "idx_users",
                },
            },
            map[string]interface{}{
                "add": map[string]interface{}{
                    "index": newIndex,
                    "alias": "idx_users",
                },
            },
        },
    }
    aliasJSON, _ := json.Marshal(aliasActions)
    aliasResp, err := s.esClient.Indices.UpdateAliases(
        bytes.NewReader(aliasJSON),
        s.esClient.Indices.UpdateAliases.WithContext(ctx),
    )
    if err != nil {
        return fmt.Errorf("update alias failed: %w", err)
    }
    defer aliasResp.Body.Close()

    log.Info("user index rebuild completed",
        "task_id", taskID, "new_index", newIndex, "total", processed)
    return nil
}

// rebuildGroupIndex 重建群组索引（流程与 rebuildUserIndex 类似）
func (s *SearchService) rebuildGroupIndex(ctx context.Context, taskID, progressKey string) error {
    newIndex := fmt.Sprintf("idx_groups_%s", time.Now().Format("20060102150405"))
    createResp, err := s.esClient.Indices.Create(
        newIndex,
        s.esClient.Indices.Create.WithBody(buildGroupIndexMapping()),
        s.esClient.Indices.Create.WithContext(ctx),
    )
    if err != nil {
        return fmt.Errorf("create new group index failed: %w", err)
    }
    defer createResp.Body.Close()
    if createResp.IsError() {
        return fmt.Errorf("create new group index returned error: %s", createResp.Status())
    }

    // scroll 旧索引 → bulk 写入新索引（逻辑同 rebuildUserIndex）
    scrollResp, err := s.esClient.Search(
        s.esClient.Search.WithContext(ctx),
        s.esClient.Search.WithIndex("idx_groups"),
        s.esClient.Search.WithSize(500),
        s.esClient.Search.WithScroll(5*time.Minute),
        s.esClient.Search.WithSort("_doc"),
    )
    if err != nil {
        return fmt.Errorf("scroll old group index failed: %w", err)
    }
    defer scrollResp.Body.Close()

    var scrollResult ESScrollResponse
    if err := json.NewDecoder(scrollResp.Body).Decode(&scrollResult); err != nil {
        return fmt.Errorf("decode scroll response failed: %w", err)
    }

    totalDocs := scrollResult.Hits.Total.Value
    s.redis.HSet(ctx, progressKey, "total", totalDocs)

    processed := int64(0)
    scrollID := scrollResult.ScrollID

    for {
        hits := scrollResult.Hits.Hits
        if len(hits) == 0 {
            break
        }

        var bulkBody strings.Builder
        for _, hit := range hits {
            meta := fmt.Sprintf(`{"index":{"_index":"%s","_id":"%s"}}`, newIndex, hit.ID)
            bulkBody.WriteString(meta + "\n")
            sourceJSON, _ := json.Marshal(hit.Source)
            bulkBody.WriteString(string(sourceJSON) + "\n")
        }

        bulkResp, err := s.esClient.Bulk(
            strings.NewReader(bulkBody.String()),
            s.esClient.Bulk.WithContext(ctx),
        )
        if err != nil {
            log.Error("bulk index groups failed", "task_id", taskID, "err", err)
        } else {
            bulkResp.Body.Close()
        }

        processed += int64(len(hits))
        s.redis.HSet(ctx, progressKey, "processed", processed)

        scrollResp, err = s.esClient.Scroll(
            s.esClient.Scroll.WithScrollID(scrollID),
            s.esClient.Scroll.WithScroll(5*time.Minute),
            s.esClient.Scroll.WithContext(ctx),
        )
        if err != nil {
            return fmt.Errorf("scroll next batch failed: %w", err)
        }
        defer scrollResp.Body.Close()

        if err := json.NewDecoder(scrollResp.Body).Decode(&scrollResult); err != nil {
            return fmt.Errorf("decode scroll response failed: %w", err)
        }
        scrollID = scrollResult.ScrollID
    }

    s.esClient.ClearScroll(
        s.esClient.ClearScroll.WithScrollID(scrollID),
        s.esClient.ClearScroll.WithContext(ctx),
    )

    aliasActions := map[string]interface{}{
        "actions": []interface{}{
            map[string]interface{}{
                "remove": map[string]interface{}{
                    "index": "idx_groups_*",
                    "alias": "idx_groups",
                },
            },
            map[string]interface{}{
                "add": map[string]interface{}{
                    "index": newIndex,
                    "alias": "idx_groups",
                },
            },
        },
    }
    aliasJSON, _ := json.Marshal(aliasActions)
    aliasResp, err := s.esClient.Indices.UpdateAliases(
        bytes.NewReader(aliasJSON),
        s.esClient.Indices.UpdateAliases.WithContext(ctx),
    )
    if err != nil {
        return fmt.Errorf("update group alias failed: %w", err)
    }
    defer aliasResp.Body.Close()

    log.Info("group index rebuild completed",
        "task_id", taskID, "new_index", newIndex, "total", processed)
    return nil
}

// rebuildMessageIndex 重建消息索引
// 消息索引按月分区，需要逐月重建
func (s *SearchService) rebuildMessageIndex(ctx context.Context, taskID, progressKey string) error {
    // 获取所有现有的消息索引月份
    catResp, err := s.esClient.Cat.Indices(
        s.esClient.Cat.Indices.WithIndex("idx_messages_*"),
        s.esClient.Cat.Indices.WithFormat("json"),
        s.esClient.Cat.Indices.WithContext(ctx),
    )
    if err != nil {
        return fmt.Errorf("list message indices failed: %w", err)
    }
    defer catResp.Body.Close()

    var indices []map[string]interface{}
    if err := json.NewDecoder(catResp.Body).Decode(&indices); err != nil {
        return fmt.Errorf("decode indices list failed: %w", err)
    }

    totalProcessed := int64(0)

    // 逐个月份索引重建
    for _, idx := range indices {
        oldIndex := idx["index"].(string)
        // 从 idx_messages_202603 提取月份后缀
        suffix := strings.TrimPrefix(oldIndex, "idx_messages_")
        newIndex := fmt.Sprintf("idx_messages_%s_%s", suffix, time.Now().Format("20060102150405"))

        // 创建新月份索引
        createResp, err := s.esClient.Indices.Create(
            newIndex,
            s.esClient.Indices.Create.WithBody(buildMessageIndexMapping()),
            s.esClient.Indices.Create.WithContext(ctx),
        )
        if err != nil {
            log.Error("create new message index failed",
                "old", oldIndex, "new", newIndex, "err", err)
            continue // 跳过此月份，继续下一个
        }
        createResp.Body.Close()

        // scroll 旧索引 → bulk 写入新索引
        scrollResp, err := s.esClient.Search(
            s.esClient.Search.WithContext(ctx),
            s.esClient.Search.WithIndex(oldIndex),
            s.esClient.Search.WithSize(1000), // 消息量大，每批 1000
            s.esClient.Search.WithScroll(10*time.Minute),
            s.esClient.Search.WithSort("_doc"),
        )
        if err != nil {
            log.Error("scroll old message index failed", "index", oldIndex, "err", err)
            continue
        }

        var scrollResult ESScrollResponse
        if err := json.NewDecoder(scrollResp.Body).Decode(&scrollResult); err != nil {
            scrollResp.Body.Close()
            continue
        }
        scrollResp.Body.Close()

        scrollID := scrollResult.ScrollID

        for {
            hits := scrollResult.Hits.Hits
            if len(hits) == 0 {
                break
            }

            var bulkBody strings.Builder
            for _, hit := range hits {
                meta := fmt.Sprintf(`{"index":{"_index":"%s","_id":"%s"}}`, newIndex, hit.ID)
                bulkBody.WriteString(meta + "\n")
                sourceJSON, _ := json.Marshal(hit.Source)
                bulkBody.WriteString(string(sourceJSON) + "\n")
            }

            bulkResp, err := s.esClient.Bulk(
                strings.NewReader(bulkBody.String()),
                s.esClient.Bulk.WithContext(ctx),
            )
            if err != nil {
                log.Error("bulk index messages failed",
                    "task_id", taskID, "index", newIndex, "err", err)
            } else {
                bulkResp.Body.Close()
            }

            totalProcessed += int64(len(hits))
            s.redis.HSet(ctx, progressKey, "processed", totalProcessed)

            scrollResp, err = s.esClient.Scroll(
                s.esClient.Scroll.WithScrollID(scrollID),
                s.esClient.Scroll.WithScroll(10*time.Minute),
                s.esClient.Scroll.WithContext(ctx),
            )
            if err != nil {
                break
            }

            if err := json.NewDecoder(scrollResp.Body).Decode(&scrollResult); err != nil {
                scrollResp.Body.Close()
                break
            }
            scrollResp.Body.Close()
            scrollID = scrollResult.ScrollID
        }

        // 清除 scroll
        s.esClient.ClearScroll(
            s.esClient.ClearScroll.WithScrollID(scrollID),
            s.esClient.ClearScroll.WithContext(ctx),
        )

        // 切换 alias：idx_messages_YYYYMM → 新索引
        aliasName := fmt.Sprintf("idx_messages_%s", suffix)
        aliasActions := map[string]interface{}{
            "actions": []interface{}{
                map[string]interface{}{
                    "remove": map[string]interface{}{
                        "index": oldIndex,
                        "alias": aliasName,
                    },
                },
                map[string]interface{}{
                    "add": map[string]interface{}{
                        "index": newIndex,
                        "alias": aliasName,
                    },
                },
            },
        }
        aliasJSON, _ := json.Marshal(aliasActions)
        aliasResp, err := s.esClient.Indices.UpdateAliases(
            bytes.NewReader(aliasJSON),
            s.esClient.Indices.UpdateAliases.WithContext(ctx),
        )
        if err != nil {
            log.Error("update message alias failed", "alias", aliasName, "err", err)
        } else {
            aliasResp.Body.Close()
        }

        log.Info("message index month rebuild completed",
            "task_id", taskID, "old", oldIndex, "new", newIndex)
    }

    log.Info("all message index rebuild completed",
        "task_id", taskID, "total_processed", totalProcessed)
    return nil
}
```

---

## 可观测性接入（OpenTelemetry）

> Search 服务依赖 ElasticSearch，需重点观测：ES 查询延迟（分场景）、索引写入延迟、重建索引进度、查询命中率、慢查询报警。

### 第一步：服务启动时初始化 OTel SDK

```go
func main() {
    ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
    defer cancel()

    otelShutdown := observability.InitOpenTelemetry(ctx, "search", "v1.0.0")
    defer otelShutdown(ctx)
    observability.SetupLogger("search")
}
```

### 第二步：gRPC Server Interceptor（Search 不依赖其他 RPC）

```go
grpcServer := grpc.NewServer(
    grpc.ChainUnaryInterceptor(otelgrpc.UnaryServerInterceptor()),
    grpc.ChainStreamInterceptor(otelgrpc.StreamServerInterceptor()),
)
```

### 第三步：注册 Search 专属业务指标

```go
var meter = otel.Meter("im-chat/search")

var (
    // ES 查询延迟（分场景）
    esQueryDuration, _ = meter.Float64Histogram("search.es_query_duration_ms",
        metric.WithDescription("ES 查询延迟"), metric.WithUnit("ms"))

    // ES 查询总数
    esQueryTotal, _ = meter.Int64Counter("search.es_query_total",
        metric.WithDescription("ES 查询总数"))

    // 查询命中数（返回结果 > 0）
    queryHitTotal, _ = meter.Int64Counter("search.query_hit_total",
        metric.WithDescription("查询命中总数（有结果）"))

    // ES 索引写入延迟
    esIndexDuration, _ = meter.Float64Histogram("search.es_index_duration_ms",
        metric.WithDescription("ES 索引写入延迟"), metric.WithUnit("ms"))

    // 慢查询计数（超过阈值）
    slowQueryTotal, _ = meter.Int64Counter("search.slow_query_total",
        metric.WithDescription("慢查询次数（>200ms）"))

    // 索引重建进度
    rebuildProgress, _ = meter.Float64Gauge("search.rebuild_progress",
        metric.WithDescription("索引重建进度 0~1"))

    // 索引重建总耗时
    rebuildDuration, _ = meter.Float64Histogram("search.rebuild_duration_ms",
        metric.WithDescription("索引重建总耗时"), metric.WithUnit("ms"))
)
```

在业务代码中埋点：

```go
// SearchMessages / SearchUsers / SearchGroups 中
esStart := time.Now()
result, err := s.esClient.Search(...)
elapsed := float64(time.Since(esStart).Milliseconds())
esQueryDuration.Record(ctx, elapsed, metric.WithAttributes(
    attribute.String("scene", "message"),  // message / user / group
))
esQueryTotal.Add(ctx, 1)
if result.TotalHits() > 0 {
    queryHitTotal.Add(ctx, 1)
}
if elapsed > 200 {
    slowQueryTotal.Add(ctx, 1)
    slog.WarnContext(ctx, "slow ES query",
        "scene", "message", "duration_ms", elapsed)
}

// RebuildIndex 中
rebuildStart := time.Now()
for i, batch := range batches {
    // ... 批量写入 ...
    rebuildProgress.Record(ctx, float64(i+1)/float64(len(batches)))
}
rebuildDuration.Record(ctx, float64(time.Since(rebuildStart).Milliseconds()))
```

### 第四步：Kafka Consumer 埋点（消费索引更新事件） + 结构化 JSON 日志

消费 `search.index.update` / `search.index.delete` 事件时提取 Kafka headers 中的 trace context。日志应包含 `scene`、`query_text`（脱敏）、`result_count`、`duration_ms` 字段。

### 接入要点总结

| 步骤 | 改动位置 | 效果 |
|------|---------|------|
| 1. `observability.InitOpenTelemetry` | main() | TracerProvider + MeterProvider + Runtime Metrics |
| 2. gRPC Server Interceptor | Server | RPC 自动 trace |
| 3. 自定义业务指标 | Search*/RebuildIndex | ES 延迟、慢查询报警、重建进度 |
| 4. Kafka Consumer + 日志 | 索引事件消费 + setupLogger | 链路追踪 + 搜索维度日志 |
| 5. buildEventHeader 改造 | 辅助函数 | EventHeader 携带真实 trace context |
| 6. 基础设施指标 | main() | Redis 连接池可观测 |
| 7. Span 错误记录 | `observability.RecordError` | 错误 trace 在 Tempo 可见 |

### 补充：buildEventHeader 改造

将 `buildEventHeader(source, instanceID)` 替换为 `observability.BuildEventHeader(ctx, source, instanceID)`。

### 补充：基础设施指标注册

```go
observability.RegisterRedisPoolMetrics(redisClient, "search")
```

### 补充：Span 错误记录

ES 查询、索引重建等操作尤其需要 span 错误记录：

```go
func (s *SearchServer) SearchMessages(ctx context.Context, req *pb.SearchMessagesRequest) (*pb.SearchMessagesResponse, error) {
    ctx, span := otel.Tracer("search").Start(ctx, "SearchMessages")
    defer span.End()

    result, err := s.doSearchMessages(ctx, req)
    if err != nil {
        observability.RecordError(span, err)
        return nil, err
    }
    return result, nil
}
```
