# Search 搜索服务 — Kafka 消费者实现伪代码

## 概述

Search 服务是 Kafka 事件系统的**重度消费者**，通过消费 User / Group / Message / Config 等服务产生的事件，维护 Elasticsearch 中的搜索索引。  
本服务是典型的 **CQRS 读侧** —— 不直接操作源数据库，而是通过事件驱动异步构建和更新搜索索引。

**核心设计原则：**
- **高吞吐索引**：消息索引是最高频的操作（每条消息都需要索引），必须使用 **ES Bulk API** 批量写入
- **幂等消费**：所有消费者通过 `search:kafka:dedup:{event_id}` Redis key 做 24h 幂等去重
- **最终一致性**：索引更新相对源数据有短暂延迟（通常 < 1s），搜索结果是最终一致的
- **容错降级**：ES 写入失败时记录失败日志并重试，不阻塞其他事件的消费
- **消息按月分区**：消息索引按月自动分区（`idx_messages_YYYYMM`），消费时自动创建不存在的月份索引

## 消费 Topic 列表

| Topic | 来源 | 消费用途 | Key |
|-------|------|----------|-----|
| `user.registered` | User 服务 | 新用户注册 → 创建 idx_users 索引文档 | user_id |
| `user.profile.updated` | User 服务 | 用户资料更新 → 更新 idx_users 索引文档 | user_id |
| `user.deactivated` | User 服务 | 用户注销 → 从 idx_users 删除索引文档 | user_id |
| `group.created` | Group 服务 | 群组创建 → 创建 idx_groups 索引文档 | group_id |
| `group.info.updated` | Group 服务 | 群信息更新 → 更新 idx_groups 索引文档 | group_id |
| `group.dissolved` | Group 服务 | 群解散 → 从 idx_groups 删除索引文档 | group_id |
| `msg.search.index` | Message 服务 | 消息搜索索引事件 → 索引到 idx_messages_YYYYMM（轻量级，仅含搜索相关字段） | msg_id |
| `msg.recalled` | Message 服务 | 消息撤回 → 从 idx_messages 删除索引文档 | channel_id |
| `msg.deleted` | Message 服务 | 消息删除 → 从 idx_messages 删除索引文档 | user_id |
| `config.changed` | Config 服务 | 配置变更 → 热更新搜索相关配置 | config_key |
| `relation.blocked` | Relation 服务 | 拉黑事件 → SET 本地拉黑缓存供 RPC 层过滤 | user_id |
| `relation.unblocked` | Relation 服务 | 取消拉黑 → DEL 本地拉黑缓存 | user_id |

## 生产 Topic 列表

| Topic | 事件 | 生产时机 | Key | 消费方 |
|-------|------|----------|-----|--------|
| `search.index.updated` | SearchIndexUpdatedEvent | 索引创建/更新完成 | document_id | Audit（审计记录） |
| `search.index.deleted` | SearchIndexDeletedEvent | 索引删除完成 | document_id | Audit（审计记录） |

## Redis Key 设计（消费者专用）

| Key 格式 | 类型 | 值 | TTL | 说明 |
|----------|------|-----|-----|------|
| `search:kafka:dedup:{event_id}` | STRING | "1" | 24h | Kafka 消费幂等去重 |
| `search:index:last_sync:{type}` | STRING | 最后同步的时间戳 | 无 | 监控用，记录每种类型最后一次索引同步时间 |
| `search:index:error_count:{type}` | STRING | 错误累计数 | 1h | 监控用，1h 内错误超过阈值告警 |
| `search:local:block:{blocker_id}:{blocked_id}` | STRING | "1" | 24h | 本地拉黑关系缓存（供 RPC 层 filterBlockedUsers 使用） |

## 通用辅助函数

```go
// SearchConsumer 搜索服务 Kafka 消费者
type SearchConsumer struct {
    esClient   *elasticsearch.Client
    redis      *redis.Client
    kafka      *kafka.Producer
    instanceID string
    config     *SearchConfig // 搜索配置（可热更新）
    configMu   sync.RWMutex // 配置读写锁
}

// SearchConfig 可热更新的搜索配置
type SearchConfig struct {
    MaxResultLimit   int     // 搜索结果最大返回数量（默认 100）
    FuzzyThreshold   string  // 模糊匹配阈值（默认 "AUTO"）
    MinShouldMatch   string  // 最小匹配度（默认 "70%"）
    BulkBatchSize    int     // Bulk 批次大小（默认 500）
    IndexRefreshInterval string // 索引刷新间隔（默认 "1s"）
}

// checkDedup 消费幂等检查
func (c *SearchConsumer) checkDedup(ctx context.Context, eventID string) bool {
    dedupKey := fmt.Sprintf("search:kafka:dedup:%s", eventID)
    set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result()
    return set // true=首次消费，false=重复消费
}

// recordSyncTime 记录最后同步时间
func (c *SearchConsumer) recordSyncTime(ctx context.Context, indexType string) {
    key := fmt.Sprintf("search:index:last_sync:%s", indexType)
    c.redis.Set(ctx, key, time.Now().UnixMilli(), 0) // 无 TTL，持久保留
}

// recordIndexError 记录索引错误并检查阈值
func (c *SearchConsumer) recordIndexError(ctx context.Context, indexType string) {
    key := fmt.Sprintf("search:index:error_count:%s", indexType)
    count, _ := c.redis.Incr(ctx, key).Result()
    if count == 1 {
        c.redis.Expire(ctx, key, 1*time.Hour)
    }
    if count > 100 {
        log.Error("search index error count exceeds threshold",
            "type", indexType, "count", count, "threshold", 100)
        // TODO: 触发告警通知
    }
}

// ensureMonthlyIndex 确保消息月份索引存在，不存在则自动创建
func (c *SearchConsumer) ensureMonthlyIndex(ctx context.Context, sendTimeMs int64) (string, error) {
    t := time.UnixMilli(sendTimeMs)
    indexName := fmt.Sprintf("idx_messages_%s", t.Format("200601"))

    // 检查索引是否存在
    existsResp, err := c.esClient.Indices.Exists(
        []string{indexName},
        c.esClient.Indices.Exists.WithContext(ctx),
    )
    if err != nil {
        return indexName, fmt.Errorf("check index exists failed: %w", err)
    }
    defer existsResp.Body.Close()

    if existsResp.StatusCode == 200 {
        return indexName, nil // 索引已存在
    }

    // 创建新月份索引
    mapping := `{
        "settings": {
            "number_of_shards": 3,
            "number_of_replicas": 1,
            "refresh_interval": "1s"
        },
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
    }`

    createResp, err := c.esClient.Indices.Create(
        indexName,
        c.esClient.Indices.Create.WithBody(strings.NewReader(mapping)),
        c.esClient.Indices.Create.WithContext(ctx),
    )
    if err != nil {
        return indexName, fmt.Errorf("create monthly index failed: %w", err)
    }
    defer createResp.Body.Close()

    if createResp.IsError() {
        // 409 = 索引已存在（并发创建），不是错误
        if createResp.StatusCode != 400 {
            return indexName, fmt.Errorf("create monthly index returned error: %s", createResp.Status())
        }
    }

    log.Info("auto-created monthly message index", "index", indexName)
    return indexName, nil
}

// buildEventHeader 构建 Kafka 事件头
func buildEventHeader(source, instanceID string) *common.EventHeader {
    return &common.EventHeader{
        EventId:   uuid.New().String(),
        TraceId:   trace.SpanFromContext(context.Background()).SpanContext().TraceID().String(),
        Source:    source,
        SourceId:  instanceID,
        Timestamp: time.Now().UnixMilli(),
        Version:   1,
    }
}
```

---

## 消费者实现

### Consumer: `user.registered`

> 来源：User 服务。新用户注册后，将用户信息索引到 ES 的 `idx_users` 索引中，使新用户可被搜索发现。

```go
func (c *SearchConsumer) HandleUserRegistered(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_user.UserRegisteredEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("unmarshal UserRegisteredEvent failed", "err", err)
        return nil // 反序列化失败不重试
    }

    // ==================== 2. 幂等检查 ====================
    if !c.checkDedup(ctx, event.Header.EventId) {
        log.Debug("duplicate user.registered event", "event_id", event.Header.EventId)
        return nil
    }

    // ==================== 3. 参数校验 ====================
    if event.UserId == "" {
        log.Error("user.registered: user_id is empty", "event_id", event.Header.EventId)
        return nil
    }

    // ==================== 4. 构建 ES 文档 ====================
    doc := map[string]interface{}{
        "user_id":    event.UserId,
        "nickname":   event.Nickname,
        "username":   event.Username,
        "phone":      event.Phone,
        "avatar_url": event.AvatarUrl,
        "gender":     int(event.Gender),
        "status":     1, // 正常状态
        "created_at": event.Header.Timestamp,
    }
    docJSON, err := json.Marshal(doc)
    if err != nil {
        log.Error("marshal user doc failed", "user_id", event.UserId, "err", err)
        return nil
    }

    // ==================== 5. 写入 ES idx_users 索引 ====================
    indexResp, err := c.esClient.Index(
        "idx_users",
        bytes.NewReader(docJSON),
        c.esClient.Index.WithDocumentID(event.UserId), // 使用 user_id 作为文档 ID
        c.esClient.Index.WithContext(ctx),
        c.esClient.Index.WithRefresh("false"), // 不立即刷新，依赖 refresh_interval
    )
    if err != nil {
        log.Error("index user to ES failed", "user_id", event.UserId, "err", err)
        c.recordIndexError(ctx, "user")
        return fmt.Errorf("index user failed: %w", err) // 返回错误触发重试
    }
    defer indexResp.Body.Close()

    if indexResp.IsError() {
        log.Error("index user to ES returned error",
            "user_id", event.UserId, "status", indexResp.StatusCode)
        c.recordIndexError(ctx, "user")
        return fmt.Errorf("index user returned error: %s", indexResp.Status())
    }

    // ==================== 6. 记录同步时间 ====================
    c.recordSyncTime(ctx, "user")

    // ==================== 7. 生产索引更新事件 ====================
    updateEvent := &kafka_search.SearchIndexUpdatedEvent{
        Header:     buildEventHeader("search", c.instanceID),
        IndexType:  kafka_search.INDEX_TYPE_USER,
        Action:     kafka_search.INDEX_ACTION_CREATE,
        DocumentId: event.UserId,
        UpdateTime: time.Now().UnixMilli(),
        Success:    true,
    }
    c.kafka.Produce(ctx, "search.index.updated", event.UserId, updateEvent)

    log.Info("user indexed successfully", "user_id", event.UserId, "nickname", event.Nickname)
    return nil
}
```

### Consumer: `user.profile.updated`

> 来源：User 服务。用户修改昵称/用户名/头像等资料后，更新 ES 索引。  
> 使用 ES partial update（doc 方式），只更新变化的字段，避免全量覆写。

```go
func (c *SearchConsumer) HandleUserProfileUpdated(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_user.UserProfileUpdatedEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("unmarshal UserProfileUpdatedEvent failed", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 ====================
    if !c.checkDedup(ctx, event.Header.EventId) {
        log.Debug("duplicate user.profile.updated event", "event_id", event.Header.EventId)
        return nil
    }

    // ==================== 3. 参数校验 ====================
    if event.UserId == "" {
        log.Error("user.profile.updated: user_id is empty")
        return nil
    }

    // ==================== 4. 构建 partial update 文档 ====================
    // 只更新事件中携带的变化字段
    // 注：UserProfileUpdatedEvent proto 定义为 updated_fields map<string,string>
    updateFields := make(map[string]interface{})

    if v, ok := event.UpdatedFields["nickname"]; ok && v != "" {
        updateFields["nickname"] = v
    }
    if v, ok := event.UpdatedFields["username"]; ok && v != "" {
        updateFields["username"] = v
    }
    if v, ok := event.UpdatedFields["phone"]; ok && v != "" {
        updateFields["phone"] = v
    }
    if v, ok := event.UpdatedFields["avatar_url"]; ok && v != "" {
        updateFields["avatar_url"] = v
    }
    if v, ok := event.UpdatedFields["gender"]; ok && v != "" {
        if gv, err := strconv.Atoi(v); err == nil {
            updateFields["gender"] = gv
        }
    }
    if v, ok := event.UpdatedFields["signature"]; ok {
        updateFields["signature"] = v
    }

    if len(updateFields) == 0 {
        log.Debug("user.profile.updated: no searchable fields changed", "user_id", event.UserId)
        return nil
    }

    // ==================== 5. ES partial update ====================
    updateDoc := map[string]interface{}{
        "doc": updateFields,
        "doc_as_upsert": true, // 如果文档不存在则创建（防止事件乱序）
    }
    updateJSON, err := json.Marshal(updateDoc)
    if err != nil {
        log.Error("marshal update doc failed", "user_id", event.UserId, "err", err)
        return nil
    }

    updateResp, err := c.esClient.Update(
        "idx_users",
        event.UserId,
        bytes.NewReader(updateJSON),
        c.esClient.Update.WithContext(ctx),
    )
    if err != nil {
        log.Error("update user index failed", "user_id", event.UserId, "err", err)
        c.recordIndexError(ctx, "user")
        return fmt.Errorf("update user index failed: %w", err)
    }
    defer updateResp.Body.Close()

    if updateResp.IsError() {
        log.Error("update user index returned error",
            "user_id", event.UserId, "status", updateResp.StatusCode)
        c.recordIndexError(ctx, "user")
        return fmt.Errorf("update user index returned error: %s", updateResp.Status())
    }

    // ==================== 6. 清理搜索建议缓存 ====================
    // 用户昵称变化可能影响搜索建议，清除相关前缀缓存
    if nickname, ok := updateFields["nickname"]; ok {
        name := nickname.(string)
        if len(name) > 0 {
            // 清除旧昵称的前缀建议缓存
            for i := 1; i <= len(name) && i <= 10; i++ {
                prefix := name[:i]
                suggestKey := fmt.Sprintf("search:suggest:%s", prefix)
                c.redis.Del(ctx, suggestKey)
            }
        }
    }

    c.recordSyncTime(ctx, "user")

    log.Info("user index updated", "user_id", event.UserId, "fields", updateFields)
    return nil
}
```

### Consumer: `user.deactivated`

> 来源：User 服务。用户注销账号后，从 ES idx_users 索引中删除文档，使该用户不再可被搜索。

```go
func (c *SearchConsumer) HandleUserDeactivated(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_user.UserDeactivatedEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("unmarshal UserDeactivatedEvent failed", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 ====================
    if !c.checkDedup(ctx, event.Header.EventId) {
        log.Debug("duplicate user.deactivated event", "event_id", event.Header.EventId)
        return nil
    }

    if event.UserId == "" {
        log.Error("user.deactivated: user_id is empty")
        return nil
    }

    // ==================== 3. 从 ES 删除用户文档 ====================
    deleteResp, err := c.esClient.Delete(
        "idx_users",
        event.UserId,
        c.esClient.Delete.WithContext(ctx),
    )
    if err != nil {
        log.Error("delete user from ES failed", "user_id", event.UserId, "err", err)
        c.recordIndexError(ctx, "user")
        return fmt.Errorf("delete user index failed: %w", err)
    }
    defer deleteResp.Body.Close()

    if deleteResp.IsError() && deleteResp.StatusCode != 404 {
        // 404 表示文档不存在，不算错误（可能重复消费或从未索引过）
        log.Error("delete user from ES returned error",
            "user_id", event.UserId, "status", deleteResp.StatusCode)
        c.recordIndexError(ctx, "user")
        return fmt.Errorf("delete user index returned error: %s", deleteResp.Status())
    }

    // ==================== 4. 生产索引删除事件 ====================
    deleteEvent := &kafka_search.SearchIndexDeletedEvent{
        Header:     buildEventHeader("search", c.instanceID),
        IndexType:  kafka_search.INDEX_TYPE_USER,
        DocumentId: event.UserId,
        Reason:     "user_deactivated",
        DeleteTime: time.Now().UnixMilli(),
    }
    c.kafka.Produce(ctx, "search.index.deleted", event.UserId, deleteEvent)

    c.recordSyncTime(ctx, "user")

    log.Info("user index deleted (deactivated)", "user_id", event.UserId)
    return nil
}
```

### Consumer: `group.created`

> 来源：Group 服务。新群组创建后，将群信息索引到 ES idx_groups 索引中，使群组可被搜索发现。

```go
func (c *SearchConsumer) HandleGroupCreated(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_group.GroupCreatedEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("unmarshal GroupCreatedEvent failed", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 ====================
    if !c.checkDedup(ctx, event.Header.EventId) {
        log.Debug("duplicate group.created event", "event_id", event.Header.EventId)
        return nil
    }

    if event.GroupId == "" {
        log.Error("group.created: group_id is empty")
        return nil
    }

    // ==================== 3. 构建 ES 文档 ====================
    doc := map[string]interface{}{
        "group_id":     event.GroupId,
        "name":         event.Name,
        "description":  event.Description,
        "owner_id":     event.OwnerId,
        "member_count": event.MemberCount,
        "status":       1, // 正常状态
        "created_at":   event.Header.Timestamp,
    }
    docJSON, err := json.Marshal(doc)
    if err != nil {
        log.Error("marshal group doc failed", "group_id", event.GroupId, "err", err)
        return nil
    }

    // ==================== 4. 写入 ES idx_groups 索引 ====================
    indexResp, err := c.esClient.Index(
        "idx_groups",
        bytes.NewReader(docJSON),
        c.esClient.Index.WithDocumentID(event.GroupId),
        c.esClient.Index.WithContext(ctx),
        c.esClient.Index.WithRefresh("false"),
    )
    if err != nil {
        log.Error("index group to ES failed", "group_id", event.GroupId, "err", err)
        c.recordIndexError(ctx, "group")
        return fmt.Errorf("index group failed: %w", err)
    }
    defer indexResp.Body.Close()

    if indexResp.IsError() {
        log.Error("index group to ES returned error",
            "group_id", event.GroupId, "status", indexResp.StatusCode)
        c.recordIndexError(ctx, "group")
        return fmt.Errorf("index group returned error: %s", indexResp.Status())
    }

    // ==================== 5. 生产索引更新事件 ====================
    updateEvent := &kafka_search.SearchIndexUpdatedEvent{
        Header:     buildEventHeader("search", c.instanceID),
        IndexType:  kafka_search.INDEX_TYPE_GROUP,
        Action:     kafka_search.INDEX_ACTION_CREATE,
        DocumentId: event.GroupId,
        UpdateTime: time.Now().UnixMilli(),
        Success:    true,
    }
    c.kafka.Produce(ctx, "search.index.updated", event.GroupId, updateEvent)

    c.recordSyncTime(ctx, "group")

    log.Info("group indexed successfully",
        "group_id", event.GroupId, "name", event.Name)
    return nil
}
```

### Consumer: `group.info.updated`

> 来源：Group 服务。群名称/描述/头像/成员数等信息变更后，更新 ES 索引。  
> 使用 partial update，仅更新变化字段。

```go
func (c *SearchConsumer) HandleGroupInfoUpdated(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_group.GroupInfoUpdatedEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("unmarshal GroupInfoUpdatedEvent failed", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 ====================
    if !c.checkDedup(ctx, event.Header.EventId) {
        log.Debug("duplicate group.info.updated event", "event_id", event.Header.EventId)
        return nil
    }

    if event.GroupId == "" {
        log.Error("group.info.updated: group_id is empty")
        return nil
    }

    // ==================== 3. 构建 partial update 文档 ====================
    updateFields := make(map[string]interface{})

    if event.Name != "" {
        updateFields["name"] = event.Name
    }
    if event.Description != "" {
        updateFields["description"] = event.Description
    }
    if event.MemberCount > 0 {
        updateFields["member_count"] = event.MemberCount
    }
    if event.OwnerId != "" {
        updateFields["owner_id"] = event.OwnerId
    }

    if len(updateFields) == 0 {
        log.Debug("group.info.updated: no searchable fields changed", "group_id", event.GroupId)
        return nil
    }

    // ==================== 4. ES partial update ====================
    updateDoc := map[string]interface{}{
        "doc":            updateFields,
        "doc_as_upsert": true,
    }
    updateJSON, err := json.Marshal(updateDoc)
    if err != nil {
        log.Error("marshal group update doc failed", "group_id", event.GroupId, "err", err)
        return nil
    }

    updateResp, err := c.esClient.Update(
        "idx_groups",
        event.GroupId,
        bytes.NewReader(updateJSON),
        c.esClient.Update.WithContext(ctx),
    )
    if err != nil {
        log.Error("update group index failed", "group_id", event.GroupId, "err", err)
        c.recordIndexError(ctx, "group")
        return fmt.Errorf("update group index failed: %w", err)
    }
    defer updateResp.Body.Close()

    if updateResp.IsError() {
        log.Error("update group index returned error",
            "group_id", event.GroupId, "status", updateResp.StatusCode)
        c.recordIndexError(ctx, "group")
        return fmt.Errorf("update group index returned error: %s", updateResp.Status())
    }

    // ==================== 5. 清理搜索建议缓存 ====================
    if name, ok := updateFields["name"]; ok {
        groupName := name.(string)
        for i := 1; i <= len(groupName) && i <= 10; i++ {
            prefix := groupName[:i]
            suggestKey := fmt.Sprintf("search:suggest:%s", prefix)
            c.redis.Del(ctx, suggestKey)
        }
    }

    c.recordSyncTime(ctx, "group")

    log.Info("group index updated", "group_id", event.GroupId, "fields", updateFields)
    return nil
}
```

### Consumer: `group.dissolved`

> 来源：Group 服务。群组解散后，从 ES idx_groups 索引中删除文档。

```go
func (c *SearchConsumer) HandleGroupDissolved(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_group.GroupDissolvedEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("unmarshal GroupDissolvedEvent failed", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 ====================
    if !c.checkDedup(ctx, event.Header.EventId) {
        log.Debug("duplicate group.dissolved event", "event_id", event.Header.EventId)
        return nil
    }

    if event.GroupId == "" {
        log.Error("group.dissolved: group_id is empty")
        return nil
    }

    // ==================== 3. 从 ES 删除群组文档 ====================
    deleteResp, err := c.esClient.Delete(
        "idx_groups",
        event.GroupId,
        c.esClient.Delete.WithContext(ctx),
    )
    if err != nil {
        log.Error("delete group from ES failed", "group_id", event.GroupId, "err", err)
        c.recordIndexError(ctx, "group")
        return fmt.Errorf("delete group index failed: %w", err)
    }
    defer deleteResp.Body.Close()

    if deleteResp.IsError() && deleteResp.StatusCode != 404 {
        log.Error("delete group from ES returned error",
            "group_id", event.GroupId, "status", deleteResp.StatusCode)
        c.recordIndexError(ctx, "group")
        return fmt.Errorf("delete group index returned error: %s", deleteResp.Status())
    }

    // ==================== 4. 生产索引删除事件 ====================
    deleteEvent := &kafka_search.SearchIndexDeletedEvent{
        Header:     buildEventHeader("search", c.instanceID),
        IndexType:  kafka_search.INDEX_TYPE_GROUP,
        DocumentId: event.GroupId,
        Reason:     "group_dissolved",
        DeleteTime: time.Now().UnixMilli(),
    }
    c.kafka.Produce(ctx, "search.index.deleted", event.GroupId, deleteEvent)

    c.recordSyncTime(ctx, "group")

    log.Info("group index deleted (dissolved)", "group_id", event.GroupId)
    return nil
}
```

### Consumer: `msg.search.index`

> 来源：Message 服务。每条消息存储后，Message 服务产出轻量级 `msg.search.index` 事件（仅含搜索相关字段：msg_id、channel_id、sender_id、text_content、file_name 等）。  
> 相比消费 `msg.stored.single/group`（包含完整消息体、seq、头像等大量无关字段），消费此轻量 topic 可显著降低带宽消耗和 consumer group 竞争。  
> **这是最高频的消费操作**，每条消息都需要索引。  
> 消息索引按月分区，消费时根据 send_time 自动路由到对应月份索引。

```go
func (c *SearchConsumer) HandleMsgSearchIndex(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_msg.MsgSearchIndexEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("unmarshal MsgSearchIndexEvent failed", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 ====================
    if !c.checkDedup(ctx, event.Header.EventId) {
        log.Debug("duplicate msg.search.index event", "event_id", event.Header.EventId)
        return nil
    }

    // ==================== 3. 参数校验 ====================
    if event.MsgId == "" {
        log.Error("msg.search.index: msg_id is empty", "event_id", event.Header.EventId)
        return nil
    }
    if event.ConversationId == "" {
        log.Error("msg.search.index: conversation_id is empty", "msg_id", event.MsgId)
        return nil
    }

    // ==================== 4. 按 action 字段路由处理 ====================
    // MsgSearchIndexEvent 的 action 字段：create / update / delete
    if event.Action == "delete" {
        // 删除索引（与 HandleMsgRecalled 类似）
        deleteQuery := map[string]interface{}{
            "query": map[string]interface{}{
                "term": map[string]interface{}{"msg_id": event.MsgId},
            },
        }
        deleteJSON, _ := json.Marshal(deleteQuery)
        c.esClient.DeleteByQuery([]string{"idx_messages_*"}, bytes.NewReader(deleteJSON),
            c.esClient.DeleteByQuery.WithContext(ctx))
        c.recordSyncTime(ctx, "message")
        return nil
    }

    // ==================== 5. 判断是否需要索引 ====================
    // 只索引有文本内容的消息类型（纯媒体消息无需全文索引）
    content := event.TextContent

    // 系统消息、撤回提示等不索引
    switch event.MessageType {
    case common.MESSAGE_TYPE_SYSTEM, common.MESSAGE_TYPE_RECALL:
        log.Debug("skip indexing non-searchable message type",
            "msg_id", event.MsgId, "type", event.MessageType)
        return nil
    }

    // 空内容消息不索引（如纯图片/视频/语音）
    if content == "" {
        // 如果是文件类型且有文件名，使用文件名作为索引内容
        if event.FileName != "" {
            content = event.FileName
        } else {
            log.Debug("skip indexing empty content message", "msg_id", event.MsgId)
            return nil
        }
    }

    // ==================== 6. 确保月份索引存在 ====================
    sendTime := event.SendTime
    indexName, err := c.ensureMonthlyIndex(ctx, sendTime)
    if err != nil {
        log.Error("ensure monthly index failed",
            "msg_id", event.MsgId, "send_time", sendTime, "err", err)
        // 降级使用通配符（理论上不应到这里）
        indexName = fmt.Sprintf("idx_messages_%s",
            time.UnixMilli(sendTime).Format("200601"))
    }

    // ==================== 7. 构建 ES 文档 ====================
    doc := map[string]interface{}{
        "msg_id":       event.MsgId,
        "channel_id":   event.ConversationId,
        "channel_type": int(event.ConversationType),
        "sender_id":    event.SenderId,
        "msg_type":     int(event.MessageType),
        "content":      content,
        "send_time":    sendTime,
    }
    docJSON, err := json.Marshal(doc)
    if err != nil {
        log.Error("marshal message doc failed", "msg_id", event.MsgId, "err", err)
        return nil
    }

    // ==================== 8. 写入 ES 消息索引 ====================
    indexResp, err := c.esClient.Index(
        indexName,
        bytes.NewReader(docJSON),
        c.esClient.Index.WithDocumentID(event.MsgId), // 使用 msg_id 作为文档 ID
        c.esClient.Index.WithContext(ctx),
        c.esClient.Index.WithRefresh("false"), // 不立即刷新，高吞吐场景依赖 refresh_interval
    )
    if err != nil {
        log.Error("index message to ES failed",
            "msg_id", event.MsgId, "index", indexName, "err", err)
        c.recordIndexError(ctx, "message")
        return fmt.Errorf("index message failed: %w", err)
    }
    defer indexResp.Body.Close()

    if indexResp.IsError() {
        log.Error("index message to ES returned error",
            "msg_id", event.MsgId, "status", indexResp.StatusCode)
        c.recordIndexError(ctx, "message")
        return fmt.Errorf("index message returned error: %s", indexResp.Status())
    }

    // ==================== 9. 更新搜索建议缓存（异步） ====================
    // 将消息中的关键词添加到搜索建议池
    go func() {
        if len(content) >= 2 && len(content) <= 20 {
            // 短文本可能是高频搜索词
            for i := 1; i <= len(content) && i <= 10; i++ {
                prefix := content[:i]
                suggestKey := fmt.Sprintf("search:suggest:%s", prefix)
                c.redis.ZIncrBy(context.Background(), suggestKey, 0.1, content)
                c.redis.Expire(context.Background(), suggestKey, 1*time.Hour)
            }
        }
    }()

    // ==================== 10. 记录同步时间 ====================
    c.recordSyncTime(ctx, "message")

    log.Debug("message indexed successfully",
        "msg_id", event.MsgId, "index", indexName, "channel_id", event.ConversationId)
    return nil
}
```

### Consumer: `msg.recalled`

> 来源：Message 服务。消息撤回后，从 ES 索引中删除该消息文档。  
> 撤回的消息不应再出现在搜索结果中。  
> 需要根据 send_time 定位到正确的月份索引进行删除。

```go
func (c *SearchConsumer) HandleMsgRecalled(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_msg.MsgRecalledEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("unmarshal MsgRecalledEvent failed", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 ====================
    if !c.checkDedup(ctx, event.Header.EventId) {
        log.Debug("duplicate msg.recalled event", "event_id", event.Header.EventId)
        return nil
    }

    if event.MsgId == "" {
        log.Error("msg.recalled: msg_id is empty")
        return nil
    }

    // ==================== 3. 从 ES 删除消息文档 ====================
    // 由于消息按月分区，但撤回事件可能不携带 send_time
    // 策略：先尝试通过 delete_by_query 跨所有月份索引删除
    deleteQuery := map[string]interface{}{
        "query": map[string]interface{}{
            "term": map[string]interface{}{
                "msg_id": event.MsgId,
            },
        },
    }
    deleteJSON, err := json.Marshal(deleteQuery)
    if err != nil {
        log.Error("marshal delete query failed", "msg_id", event.MsgId, "err", err)
        return nil
    }

    deleteResp, err := c.esClient.DeleteByQuery(
        []string{"idx_messages_*"}, // 跨所有月份索引
        bytes.NewReader(deleteJSON),
        c.esClient.DeleteByQuery.WithContext(ctx),
        c.esClient.DeleteByQuery.WithRefresh("false"),
    )
    if err != nil {
        log.Error("delete message from ES failed", "msg_id", event.MsgId, "err", err)
        c.recordIndexError(ctx, "message")
        return fmt.Errorf("delete message index failed: %w", err)
    }
    defer deleteResp.Body.Close()

    if deleteResp.IsError() {
        log.Error("delete message from ES returned error",
            "msg_id", event.MsgId, "status", deleteResp.StatusCode)
        c.recordIndexError(ctx, "message")
        return fmt.Errorf("delete message index returned error: %s", deleteResp.Status())
    }

    // 解析删除结果
    var deleteResult map[string]interface{}
    json.NewDecoder(deleteResp.Body).Decode(&deleteResult)
    deletedCount := int64(0)
    if deleted, ok := deleteResult["deleted"]; ok {
        deletedCount = int64(deleted.(float64))
    }

    // ==================== 4. 生产索引删除事件 ====================
    deleteEvent := &kafka_search.SearchIndexDeletedEvent{
        Header:     buildEventHeader("search", c.instanceID),
        IndexType:  kafka_search.INDEX_TYPE_MESSAGE,
        DocumentId: event.MsgId,
        Reason:     "message_recalled",
        DeleteTime: time.Now().UnixMilli(),
    }
    c.kafka.Produce(ctx, "search.index.deleted", event.MsgId, deleteEvent)

    c.recordSyncTime(ctx, "message")

    log.Info("message index deleted (recalled)",
        "msg_id", event.MsgId, "deleted_count", deletedCount)
    return nil
}
```

### Consumer: `msg.deleted`

> 来源：Message 服务。消息被删除后（管理员删除/审核删除），从 ES 索引中删除。  
> 注意：用户维度的 "删除消息" 是软删除（user_deleted_messages），不影响 ES 索引。  
> 只有服务端层面的真实删除（status=4）才需要从 ES 中移除。

```go
func (c *SearchConsumer) HandleMsgDeleted(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_msg.MsgDeletedEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("unmarshal MsgDeletedEvent failed", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 ====================
    if !c.checkDedup(ctx, event.Header.EventId) {
        log.Debug("duplicate msg.deleted event", "event_id", event.Header.EventId)
        return nil
    }

    // ==================== 3. 参数校验 ====================
    if len(event.MsgIds) == 0 {
        log.Error("msg.deleted: msg_ids is empty")
        return nil
    }

    // ==================== 4. 批量从 ES 删除消息文档 ====================
    // 使用 delete_by_query 批量删除
    deleteQuery := map[string]interface{}{
        "query": map[string]interface{}{
            "terms": map[string]interface{}{
                "msg_id": event.MsgIds,
            },
        },
    }
    deleteJSON, err := json.Marshal(deleteQuery)
    if err != nil {
        log.Error("marshal delete query failed", "msg_ids", event.MsgIds, "err", err)
        return nil
    }

    deleteResp, err := c.esClient.DeleteByQuery(
        []string{"idx_messages_*"},
        bytes.NewReader(deleteJSON),
        c.esClient.DeleteByQuery.WithContext(ctx),
        c.esClient.DeleteByQuery.WithRefresh("false"),
    )
    if err != nil {
        log.Error("batch delete messages from ES failed",
            "msg_ids", event.MsgIds, "err", err)
        c.recordIndexError(ctx, "message")
        return fmt.Errorf("batch delete message index failed: %w", err)
    }
    defer deleteResp.Body.Close()

    if deleteResp.IsError() {
        log.Error("batch delete messages from ES returned error",
            "msg_ids", event.MsgIds, "status", deleteResp.StatusCode)
        c.recordIndexError(ctx, "message")
        return fmt.Errorf("batch delete message index returned error: %s", deleteResp.Status())
    }

    // 解析删除结果
    var deleteResult map[string]interface{}
    json.NewDecoder(deleteResp.Body).Decode(&deleteResult)
    deletedCount := int64(0)
    if deleted, ok := deleteResult["deleted"]; ok {
        deletedCount = int64(deleted.(float64))
    }

    // ==================== 5. 为每条消息生产索引删除事件 ====================
    for _, msgID := range event.MsgIds {
        deleteEvent := &kafka_search.SearchIndexDeletedEvent{
            Header:     buildEventHeader("search", c.instanceID),
            IndexType:  kafka_search.INDEX_TYPE_MESSAGE,
            DocumentId: msgID,
            Reason:     "message_deleted",
            DeleteTime: time.Now().UnixMilli(),
        }
        c.kafka.Produce(ctx, "search.index.deleted", msgID, deleteEvent)
    }

    c.recordSyncTime(ctx, "message")

    log.Info("message indices batch deleted",
        "msg_count", len(event.MsgIds), "deleted_count", deletedCount)
    return nil
}
```

### Consumer: `config.changed`

> 来源：Config 服务。搜索相关配置变更时（结果数量限制/模糊匹配阈值/索引刷新间隔等），热更新内存配置。  
> 无需重启服务即可生效。

```go
func (c *SearchConsumer) HandleConfigChanged(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_config.ConfigChangedEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("unmarshal ConfigChangedEvent failed", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 ====================
    if !c.checkDedup(ctx, event.Header.EventId) {
        log.Debug("duplicate config.changed event", "event_id", event.Header.EventId)
        return nil
    }

    // ==================== 3. 只处理 search 相关配置 ====================
    if event.Module != "search" {
        return nil // 非搜索模块配置，忽略
    }

    log.Info("received search config change",
        "key", event.Key, "old_value", event.OldValue, "new_value", event.NewValue)

    // ==================== 4. 更新内存配置（读写锁保护） ====================
    c.configMu.Lock()
    defer c.configMu.Unlock()

    switch event.Key {
    case "search.max_result_limit":
        // 搜索结果最大返回数量
        if v, err := strconv.Atoi(event.NewValue); err == nil && v > 0 && v <= 1000 {
            c.config.MaxResultLimit = v
            log.Info("updated search.max_result_limit", "new_value", v)
        } else {
            log.Warn("invalid search.max_result_limit value", "value", event.NewValue)
        }

    case "search.fuzzy_threshold":
        // 模糊匹配阈值：AUTO / 0 / 1 / 2
        validThresholds := map[string]bool{"AUTO": true, "0": true, "1": true, "2": true}
        if validThresholds[event.NewValue] {
            c.config.FuzzyThreshold = event.NewValue
            log.Info("updated search.fuzzy_threshold", "new_value", event.NewValue)
        } else {
            log.Warn("invalid search.fuzzy_threshold value", "value", event.NewValue)
        }

    case "search.min_should_match":
        // 最小匹配度：如 "70%", "80%", "2"
        c.config.MinShouldMatch = event.NewValue
        log.Info("updated search.min_should_match", "new_value", event.NewValue)

    case "search.bulk_batch_size":
        // Bulk 批次大小
        if v, err := strconv.Atoi(event.NewValue); err == nil && v > 0 && v <= 5000 {
            c.config.BulkBatchSize = v
            log.Info("updated search.bulk_batch_size", "new_value", v)
        } else {
            log.Warn("invalid search.bulk_batch_size value", "value", event.NewValue)
        }

    case "search.index_refresh_interval":
        // ES 索引刷新间隔：如 "1s", "5s", "30s"
        oldInterval := c.config.IndexRefreshInterval
        c.config.IndexRefreshInterval = event.NewValue
        log.Info("updated search.index_refresh_interval",
            "old_value", oldInterval, "new_value", event.NewValue)

        // 动态更新 ES 索引的 refresh_interval 设置
        go c.updateESRefreshInterval(context.Background(), event.NewValue)

    default:
        log.Debug("unknown search config key", "key", event.Key)
    }

    return nil
}

// updateESRefreshInterval 动态更新所有搜索索引的 refresh_interval
func (c *SearchConsumer) updateESRefreshInterval(ctx context.Context, interval string) {
    indices := []string{"idx_users", "idx_groups", "idx_messages_*"}
    settings := fmt.Sprintf(`{"index":{"refresh_interval":"%s"}}`, interval)

    for _, idx := range indices {
        resp, err := c.esClient.Indices.PutSettings(
            strings.NewReader(settings),
            c.esClient.Indices.PutSettings.WithIndex(idx),
            c.esClient.Indices.PutSettings.WithContext(ctx),
        )
        if err != nil {
            log.Error("update ES refresh_interval failed",
                "index", idx, "interval", interval, "err", err)
            continue
        }
        resp.Body.Close()

        if resp.IsError() {
            log.Error("update ES refresh_interval returned error",
                "index", idx, "status", resp.StatusCode)
        } else {
            log.Info("ES refresh_interval updated",
                "index", idx, "interval", interval)
        }
    }
}
```

### Consumer: `relation.blocked`

> 来源：Relation 服务。用户执行拉黑操作后触发。  
> 职责：写入本地拉黑缓存 `search:local:block:{blocker}:{blocked}`，供 RPC 层 `filterBlockedUsers` 查询。  
> **替代方案说明：** 之前 Search RPC 层通过 N×2 次 `Relation.CheckBlockship` RPC 过滤拉黑用户，  
> 改为消费 Kafka 事件维护本地 Redis 缓存后，过滤延迟从 ~50ms 降至 < 1ms。

```go
// HandleRelationBlocked 拉黑事件 → SET 本地拉黑缓存
func (c *SearchConsumer) HandleRelationBlocked(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event struct {
        Header    *common.EventHeader `protobuf:"bytes,1"`
        UserId    string // 执行拉黑的用户（blocker）
        TargetId  string // 被拉黑的用户（blocked）
        BlockTime int64
    }
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("unmarshal relation.blocked event failed", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 ====================
    if !c.checkDedup(ctx, event.Header.EventId) {
        log.Debug("duplicate relation.blocked event", "event_id", event.Header.EventId)
        return nil
    }

    // ==================== 3. 参数校验 ====================
    if event.UserId == "" || event.TargetId == "" {
        log.Error("relation.blocked: user_id or target_id is empty")
        return nil
    }

    // ==================== 4. 写入本地拉黑缓存 ====================
    // Key: search:local:block:{blocker_id}:{blocked_id} → "1" TTL 24h
    // 单向写入：blocker → blocked（双向检查由 RPC 层分别查两个方向）
    blockKey := fmt.Sprintf("search:local:block:%s:%s", event.UserId, event.TargetId)
    err := c.redis.Set(ctx, blockKey, "1", 24*time.Hour).Err()
    if err != nil {
        log.Error("set block cache failed",
            "blocker", event.UserId, "blocked", event.TargetId, "err", err)
        return fmt.Errorf("set block cache failed: %w", err)
    }

    c.recordSyncTime(ctx, "block")

    log.Info("block cache set",
        "blocker", event.UserId, "blocked", event.TargetId)
    return nil
}
```

### Consumer: `relation.unblocked`

> 来源：Relation 服务。用户取消拉黑后触发。  
> 职责：删除本地拉黑缓存 `search:local:block:{blocker}:{blocked}`。

```go
// HandleRelationUnblocked 取消拉黑 → DEL 本地拉黑缓存
func (c *SearchConsumer) HandleRelationUnblocked(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event struct {
        Header      *common.EventHeader `protobuf:"bytes,1"`
        UserId      string // 执行取消拉黑的用户
        TargetId    string // 被取消拉黑的用户
        UnblockTime int64
    }
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("unmarshal relation.unblocked event failed", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 ====================
    if !c.checkDedup(ctx, event.Header.EventId) {
        log.Debug("duplicate relation.unblocked event", "event_id", event.Header.EventId)
        return nil
    }

    // ==================== 3. 参数校验 ====================
    if event.UserId == "" || event.TargetId == "" {
        log.Error("relation.unblocked: user_id or target_id is empty")
        return nil
    }

    // ==================== 4. 删除本地拉黑缓存 ====================
    blockKey := fmt.Sprintf("search:local:block:%s:%s", event.UserId, event.TargetId)
    err := c.redis.Del(ctx, blockKey).Err()
    if err != nil {
        log.Error("delete block cache failed",
            "blocker", event.UserId, "blocked", event.TargetId, "err", err)
        return fmt.Errorf("delete block cache failed: %w", err)
    }

    c.recordSyncTime(ctx, "block")

    log.Info("block cache deleted",
        "blocker", event.UserId, "target", event.TargetId)
    return nil
}
```

### Consumer: `relation.remark.updated`

> 来源：Relation 服务。用户修改好友备注后触发。  
> 职责：更新 ES 用户索引中该用户对好友的备注字段，使搜索联系人时能命中备注。

```go
func (c *SearchConsumer) HandleRelationRemarkUpdated(ctx context.Context, msg *kafka.Message) error {
    var event kafka_search.RemarkUpdatedForSearch
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("unmarshal relation.remark.updated failed", "err", err)
        return nil
    }

    if !c.checkDedup(ctx, event.Header.EventId) {
        return nil
    }

    if event.UserId == "" || event.FriendId == "" {
        return nil
    }

    // 更新用户联系人索引中的备注字段
    // 使用 user_id + friend_id 作为联合文档 ID
    docID := fmt.Sprintf("%s_%s", event.UserId, event.FriendId)
    updateBody := fmt.Sprintf(`{
        "doc": {
            "remark": "%s",
            "remark_pinyin": "%s",
            "updated_at": %d
        },
        "doc_as_upsert": true
    }`, escapeJSON(event.NewRemark), toPinyin(event.NewRemark), time.Now().UnixMilli())

    resp, err := c.esClient.Update(
        "idx_contacts",
        docID,
        strings.NewReader(updateBody),
        c.esClient.Update.WithContext(ctx),
    )
    if err != nil {
        return fmt.Errorf("更新联系人备注索引失败: %w", err)
    }
    defer resp.Body.Close()

    if resp.IsError() {
        log.Error("ES 更新联系人备注索引返回错误", "status", resp.StatusCode, "doc_id", docID)
    }

    log.Info("contact remark index updated",
        "user_id", event.UserId, "friend_id", event.FriendId, "remark", event.NewRemark)
    return nil
}
```

### Consumer: `media.deleted`

> 来源：Media 服务。媒体文件被删除后触发。  
> 职责：删除 ES 中对应的媒体文件索引条目。

```go
func (c *SearchConsumer) HandleMediaDeleted(ctx context.Context, msg *kafka.Message) error {
    var event kafka_search.MediaDeletedForSearch
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("unmarshal media.deleted failed", "err", err)
        return nil
    }

    if !c.checkDedup(ctx, event.Header.EventId) {
        return nil
    }

    if event.MediaId == "" {
        return nil
    }

    // 从 ES 删除媒体索引
    resp, err := c.esClient.Delete(
        "idx_media",
        event.MediaId,
        c.esClient.Delete.WithContext(ctx),
    )
    if err != nil {
        return fmt.Errorf("删除媒体索引失败: %w", err)
    }
    defer resp.Body.Close()

    if resp.IsError() && resp.StatusCode != 404 {
        log.Error("ES 删除媒体索引返回错误", "status", resp.StatusCode, "media_id", event.MediaId)
    }

    // 同时删除关联消息中该媒体的附件索引
    if event.MsgId != "" {
        updateBody := `{"script": {"source": "if (ctx._source.containsKey('media_id') && ctx._source.media_id == params.media_id) { ctx.op = 'delete' }", "params": {"media_id": "` + event.MediaId + `"}}}`
        c.esClient.UpdateByQuery(
            []string{"idx_messages_*"},
            c.esClient.UpdateByQuery.WithBody(strings.NewReader(updateBody)),
            c.esClient.UpdateByQuery.WithContext(ctx),
        )
    }

    log.Info("media index deleted", "media_id", event.MediaId)
    return nil
}
```

```go
// ESSearchResponse ES 搜索响应通用结构
type ESSearchResponse struct {
    Hits struct {
        Total struct {
            Value int64 `json:"value"`
        } `json:"total"`
        Hits []*ESHit `json:"hits"`
    } `json:"hits"`
}

// ESHit ES 单条搜索命中
type ESHit struct {
    ID        string                       `json:"_id"`
    Score     float64                      `json:"_score"`
    Source    map[string]interface{}        `json:"_source"`
    Highlight map[string][]string          `json:"highlight"`
}

// ESScrollResponse ES 滚动查询响应
type ESScrollResponse struct {
    ScrollID string `json:"_scroll_id"`
    ESSearchResponse
}

// ESSuggestResponse ES 建议查询响应
type ESSuggestResponse struct {
    Suggest map[string][]struct {
        Options []struct {
            Text  string  `json:"text"`
            Score float64 `json:"_score"`
        } `json:"options"`
    } `json:"suggest"`
}

// getStringField 安全获取 map 中的字符串字段
func getStringField(source map[string]interface{}, key string) string {
    if v, ok := source[key]; ok {
        if s, ok := v.(string); ok {
            return s
        }
    }
    return ""
}

// getInt32Field 安全获取 map 中的 int32 字段
func getInt32Field(source map[string]interface{}, key string) int32 {
    if v, ok := source[key]; ok {
        switch n := v.(type) {
        case float64:
            return int32(n)
        case int:
            return int32(n)
        }
    }
    return 0
}

// getInt64Field 安全获取 map 中的 int64 字段
func getInt64Field(source map[string]interface{}, key string) int64 {
    if v, ok := source[key]; ok {
        switch n := v.(type) {
        case float64:
            return int64(n)
        case int:
            return int64(n)
        }
    }
    return 0
}
```
