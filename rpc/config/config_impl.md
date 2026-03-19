# Config 配置中心 — RPC 接口实现伪代码

## 概述

Config 服务是整个 IM 系统的**集中式配置管理中心**，负责存储和分发所有微服务的运行时配置项与功能开关（Feature Flags）。
所有服务在启动时通过 `GetServiceConfigs` 拉取自身配置，运行时通过 `WatchConfig`（Server Streaming RPC）实时接收配置变更推送，实现**热更新**。

**核心设计原则：**
- **原子性**：配置变更通过 PgSQL `version` 字段实现乐观并发控制（OCC），防止并发写冲突
- **一致性**：写入 PgSQL 后立即失效 Redis 缓存，再通过 Kafka 广播变更事件
- **实时性**：`WatchConfig` 基于 Redis PubSub 实现实时推送，配置变更秒级生效
- **可审计**：所有配置变更均投递 Kafka 事件，Audit 服务全量记录变更历史
- **零外部 RPC 依赖**：Config 是基础设施服务，被所有服务调用，自身不依赖任何业务 RPC

## 外部依赖

| 依赖类型 | 依赖项 | 用途 |
|---------|--------|------|
| PgSQL | configs 表 | 配置项持久化存储 |
| PgSQL | feature_flags 表 | 功能开关持久化存储 |
| Redis | 配置缓存 / 版本号 / PubSub | 高频读缓存、乐观锁版本、实时变更通知 |
| Kafka | config.changed / config.flag.changed | 广播配置变更事件，所有服务消费 |
| **RPC** | **无** | Config 是基础设施服务，不依赖任何业务 RPC |

## PgSQL 表结构

```sql
-- 配置项表（核心表，存储所有服务的运行时配置）
CREATE TABLE configs (
    id          BIGSERIAL    PRIMARY KEY,
    service     VARCHAR(50)  NOT NULL,                     -- 所属服务名称（如 message / push / auth）
    key         VARCHAR(200) NOT NULL,                     -- 配置键（如 max_text_length / recall_time_limit）
    value       JSONB        NOT NULL DEFAULT '{}',        -- 配置值（JSON 格式，支持各种数据类型）
    description TEXT         NOT NULL DEFAULT '',           -- 配置描述说明
    value_type  VARCHAR(20)  NOT NULL DEFAULT 'string',    -- 值类型：string / int / bool / json / float
    version     INT          NOT NULL DEFAULT 1,           -- 乐观锁版本号，每次更新 +1
    updated_by  VARCHAR(20)  NOT NULL DEFAULT '',          -- 最后更新人 ID
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- 服务+键 唯一约束（同一个服务下配置键不可重复）
CREATE UNIQUE INDEX idx_configs_service_key ON configs(service, key);
-- 按服务查询所有配置
CREATE INDEX idx_configs_service ON configs(service);

-- 功能开关表（灰度发布 / 功能门控）
CREATE TABLE feature_flags (
    id                  BIGSERIAL    PRIMARY KEY,
    flag_name           VARCHAR(200) NOT NULL UNIQUE,          -- 开关名称，全局唯一
    is_enabled          BOOL         NOT NULL DEFAULT false,   -- 是否全局启用
    description         TEXT         NOT NULL DEFAULT '',       -- 功能描述
    rollout_percentage  INT          NOT NULL DEFAULT 0,       -- 灰度百分比（0-100）
    target_users        JSONB        DEFAULT '[]',             -- 白名单用户 ID 列表
    version             INT          NOT NULL DEFAULT 1,       -- 乐观锁版本号
    updated_by          VARCHAR(20)  NOT NULL DEFAULT '',      -- 最后更新人 ID
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
```

## Redis Key 设计

| Key 格式 | 类型 | 值 | TTL | 说明 |
|----------|------|-----|-----|------|
| `config:{service}:{key}` | STRING | JSON 配置值 | 无 TTL（持久缓存） | 单个配置项缓存，变更时立即失效 |
| `config:service_all:{service}` | HASH | field=key, value=JSON | 5min | 某服务全量配置缓存，`GetServiceConfigs` 使用 |
| `config:flags:{flag_name}` | HASH | {is_enabled, rollout_percentage, target_users, version} | 无 TTL | 单个功能开关缓存，变更时立即失效 |
| `config:flags_all` | HASH | field=flag_name, value=JSON | 5min | 全量功能开关缓存，`GetFeatureFlags` 使用 |
| `config:version:{service}:{key}` | STRING | version 整数 | 无 TTL | 配置版本号，用于乐观并发控制 |
| `config:watch:{service}:{key}` | PubSub | 变更通知 JSON | — | `WatchConfig` 订阅的 PubSub 频道 |
| `config:kafka:dedup:{event_id}` | STRING | "1" | 24h | Kafka 消费幂等去重 |

---

## 接口实现

### 1. GetConfig — 获取单个配置项

> 根据 service + key 获取单个配置项。优先读 Redis 缓存，miss 时回查 PgSQL 并回填缓存。

```go
func (s *ConfigService) GetConfig(ctx context.Context, req *pb.GetConfigReq) (*pb.GetConfigResp, error) {
    // ==================== 1. 参数校验 ====================
    if req.Service == "" {
        return nil, status.Error(codes.InvalidArgument, "service is required")
    }
    if req.Key == "" {
        return nil, status.Error(codes.InvalidArgument, "key is required")
    }

    // ==================== 2. 读取 Redis 缓存 ====================
    cacheKey := fmt.Sprintf("config:%s:%s", req.Service, req.Key)
    cached, err := s.redis.Get(ctx, cacheKey).Result()
    if err == nil && cached != "" {
        // 缓存命中 → 反序列化返回
        var item pb.ConfigItem
        if err := json.Unmarshal([]byte(cached), &item); err == nil {
            return &pb.GetConfigResp{
                Meta:   successMeta(ctx),
                Config: &item,
            }, nil
        }
        // 反序列化失败 → 删除脏缓存，继续回查 DB
        s.redis.Del(ctx, cacheKey)
    }

    // ==================== 3. 回查 PgSQL ====================
    var item pb.ConfigItem
    var valueRaw []byte
    err = s.db.QueryRowContext(ctx,
        `SELECT id, service, key, value, description, value_type, version, updated_at, updated_by
         FROM configs
         WHERE service = $1 AND key = $2`,
        req.Service, req.Key,
    ).Scan(
        &item.ConfigId, &item.Service, &item.Key, &valueRaw,
        &item.Description, &item.DataType, &item.Version,
        &item.UpdatedAt, &item.UpdatedBy,
    )
    if err == sql.ErrNoRows {
        return nil, status.Error(codes.NotFound, "config not found")
    }
    if err != nil {
        return nil, status.Error(codes.Internal, "query config from db failed: "+err.Error())
    }
    item.Value = string(valueRaw)

    // ==================== 4. 回填 Redis 缓存（无 TTL，持久缓存） ====================
    itemJSON, _ := json.Marshal(&item)
    s.redis.Set(ctx, cacheKey, string(itemJSON), 0) // 0 = 不过期

    // 同步写入版本号缓存
    versionKey := fmt.Sprintf("config:version:%s:%s", req.Service, req.Key)
    s.redis.Set(ctx, versionKey, item.Version, 0)

    return &pb.GetConfigResp{
        Meta:   successMeta(ctx),
        Config: &item,
    }, nil
}
```

### 2. BatchGetConfig — 批量获取配置项

> 批量获取某服务下多个配置项。先批量查 Redis，miss 的部分回查 PgSQL 并回填。

```go
func (s *ConfigService) BatchGetConfig(ctx context.Context, req *pb.BatchGetConfigReq) (*pb.BatchGetConfigResp, error) {
    // ==================== 1. 参数校验 ====================
    if req.Service == "" {
        return nil, status.Error(codes.InvalidArgument, "service is required")
    }
    if len(req.Keys) == 0 {
        return nil, status.Error(codes.InvalidArgument, "keys is required")
    }
    if len(req.Keys) > 100 {
        return nil, status.Error(codes.InvalidArgument, "too many keys, max 100")
    }

    // ==================== 2. 批量读 Redis 缓存 ====================
    pipe := s.redis.Pipeline()
    cmds := make(map[string]*redis.StringCmd, len(req.Keys))
    for _, key := range req.Keys {
        cacheKey := fmt.Sprintf("config:%s:%s", req.Service, key)
        cmds[key] = pipe.Get(ctx, cacheKey)
    }
    pipe.Exec(ctx) // 忽略部分 miss 的错误

    // 分离命中与未命中
    result := make([]*pb.ConfigItem, 0, len(req.Keys))
    missedKeys := make([]string, 0)

    for _, key := range req.Keys {
        cached, err := cmds[key].Result()
        if err == nil && cached != "" {
            var item pb.ConfigItem
            if json.Unmarshal([]byte(cached), &item) == nil {
                result = append(result, &item)
                continue
            }
        }
        missedKeys = append(missedKeys, key)
    }

    // ==================== 3. 未命中部分回查 PgSQL ====================
    if len(missedKeys) > 0 {
        query := `SELECT id, service, key, value, description, value_type, version, updated_at, updated_by
                  FROM configs
                  WHERE service = $1 AND key = ANY($2)`
        rows, err := s.db.QueryContext(ctx, query, req.Service, pq.Array(missedKeys))
        if err != nil {
            return nil, status.Error(codes.Internal, "batch query configs from db failed: "+err.Error())
        }
        defer rows.Close()

        // 用于批量回填 Redis
        cachePipe := s.redis.Pipeline()

        for rows.Next() {
            var item pb.ConfigItem
            var valueRaw []byte
            if err := rows.Scan(
                &item.ConfigId, &item.Service, &item.Key, &valueRaw,
                &item.Description, &item.DataType, &item.Version,
                &item.UpdatedAt, &item.UpdatedBy,
            ); err != nil {
                log.Error("scan config row failed", "err", err)
                continue
            }
            item.Value = string(valueRaw)
            result = append(result, &item)

            // 回填 Redis（无 TTL）
            itemJSON, _ := json.Marshal(&item)
            cacheKey := fmt.Sprintf("config:%s:%s", req.Service, item.Key)
            cachePipe.Set(ctx, cacheKey, string(itemJSON), 0)
        }

        if _, err := cachePipe.Exec(ctx); err != nil {
            log.Warn("batch backfill redis cache failed", "service", req.Service, "err", err)
        }
    }

    return &pb.BatchGetConfigResp{
        Meta:    successMeta(ctx),
        Configs: result,
    }, nil
}
```

### 3. SetConfig — 创建或更新配置项（管理 API）

> 创建新配置或更新已有配置。使用 PgSQL `version` 字段做乐观并发控制，写入后立即失效 Redis 缓存，  
> 通过 Redis PubSub 通知 WatchConfig 订阅者，最后投递 Kafka 广播配置变更事件。

```go
func (s *ConfigService) SetConfig(ctx context.Context, req *pb.SetConfigReq) (*pb.SetConfigResp, error) {
    // ==================== 1. 参数校验 ====================
    if req.Service == "" {
        return nil, status.Error(codes.InvalidArgument, "service is required")
    }
    if req.Key == "" {
        return nil, status.Error(codes.InvalidArgument, "key is required")
    }
    if req.Value == "" {
        return nil, status.Error(codes.InvalidArgument, "value is required")
    }
    if req.OperatorId == "" {
        return nil, status.Error(codes.InvalidArgument, "operator_id is required")
    }
    // 校验 value_type 合法性
    validTypes := map[string]bool{"string": true, "int": true, "bool": true, "json": true, "float": true}
    if req.DataType != "" && !validTypes[req.DataType] {
        return nil, status.Error(codes.InvalidArgument, "invalid data_type, must be string/int/bool/json/float")
    }
    if req.DataType == "" {
        req.DataType = "string" // 默认类型
    }

    // ==================== 2. 校验 value 与 data_type 是否匹配 ====================
    if err := validateConfigValue(req.Value, req.DataType); err != nil {
        return nil, status.Error(codes.InvalidArgument, "value type mismatch: "+err.Error())
    }

    now := time.Now()

    // ==================== 3. 查询是否已存在 — PgSQL ====================
    var existingID int64
    var oldValue string
    var oldVersion int
    err := s.db.QueryRowContext(ctx,
        `SELECT id, value, version FROM configs WHERE service = $1 AND key = $2`,
        req.Service, req.Key,
    ).Scan(&existingID, &oldValue, &oldVersion)

    var changeType string // "CREATE" 或 "UPDATE"
    var resultItem pb.ConfigItem

    // ==================== 延迟双删 — DB 写入前失效缓存，500ms 后再次删除防止并发读回填旧数据 ====================
    cacheKey := fmt.Sprintf("config:%s:%s", req.Service, req.Key)
    serviceAllKey := fmt.Sprintf("config:service_all:%s", req.Service)
    s.delayedDoubleDelete(ctx, cacheKey, serviceAllKey)

    if err == sql.ErrNoRows {
        // ==================== 4a. 新建配置项 — INSERT ====================
        changeType = "CREATE"
        err = s.db.QueryRowContext(ctx,
            `INSERT INTO configs (service, key, value, description, value_type, version, updated_by, created_at, updated_at)
             VALUES ($1, $2, $3, $4, $5, 1, $6, $7, $7)
             RETURNING id, service, key, value, description, value_type, version, updated_by`,
            req.Service, req.Key, req.Value, req.Description, req.DataType, req.OperatorId, now,
        ).Scan(
            &resultItem.ConfigId, &resultItem.Service, &resultItem.Key,
            &resultItem.Value, &resultItem.Description, &resultItem.DataType,
            &resultItem.Version, &resultItem.UpdatedBy,
        )
        if err != nil {
            if isPgUniqueViolation(err) {
                // 并发 INSERT 冲突 → 转为 UPDATE 逻辑重试
                return nil, status.Error(codes.AlreadyExists, "config already exists, retry as update")
            }
            return nil, status.Error(codes.Internal, "insert config failed: "+err.Error())
        }
        resultItem.UpdatedAt = now.UnixMilli()

    } else if err != nil {
        return nil, status.Error(codes.Internal, "query existing config failed: "+err.Error())
    } else {
        // ==================== 4b. 更新配置项 — UPDATE（乐观锁） ====================
        changeType = "UPDATE"

        // 值未变化则直接返回，不触发变更事件
        if oldValue == req.Value {
            return &pb.SetConfigResp{
                Meta: successMeta(ctx),
                Config: &pb.ConfigItem{
                    ConfigId:    fmt.Sprintf("%d", existingID),
                    Service:     req.Service,
                    Key:         req.Key,
                    Value:       oldValue,
                    Description: req.Description,
                    DataType:    req.DataType,
                    Version:     int64(oldVersion),
                    UpdatedBy:   req.OperatorId,
                },
            }, nil
        }

        // 乐观锁更新：WHERE version = oldVersion，防止并发写冲突
        result, err := s.db.ExecContext(ctx,
            `UPDATE configs
             SET value = $1, description = $2, value_type = $3,
                 version = version + 1, updated_by = $4, updated_at = $5
             WHERE service = $6 AND key = $7 AND version = $8`,
            req.Value, req.Description, req.DataType, req.OperatorId, now,
            req.Service, req.Key, oldVersion,
        )
        if err != nil {
            return nil, status.Error(codes.Internal, "update config failed: "+err.Error())
        }
        rowsAffected, _ := result.RowsAffected()
        if rowsAffected == 0 {
            // version 不匹配 → 并发冲突，提示客户端重试
            return nil, status.Error(codes.Aborted, "config version conflict, please retry")
        }

        // 查询更新后的完整记录
        err = s.db.QueryRowContext(ctx,
            `SELECT id, service, key, value, description, value_type, version, updated_by, updated_at
             FROM configs WHERE service = $1 AND key = $2`,
            req.Service, req.Key,
        ).Scan(
            &resultItem.ConfigId, &resultItem.Service, &resultItem.Key,
            &resultItem.Value, &resultItem.Description, &resultItem.DataType,
            &resultItem.Version, &resultItem.UpdatedBy, &resultItem.UpdatedAt,
        )
        if err != nil {
            return nil, status.Error(codes.Internal, "query updated config failed: "+err.Error())
        }
    }

    // ==================== 5. 更新版本号缓存（立即生效） ====================
    versionKey := fmt.Sprintf("config:version:%s:%s", req.Service, req.Key)
    s.redis.Set(ctx, versionKey, resultItem.Version, 0)

    // ==================== 6. Redis PubSub 通知 WatchConfig 订阅者 ====================
    pubsubChannel := fmt.Sprintf("config:watch:%s:%s", req.Service, req.Key)
    changeNotify, _ := json.Marshal(map[string]interface{}{
        "service":     req.Service,
        "key":         req.Key,
        "value":       req.Value,
        "old_value":   oldValue,
        "version":     resultItem.Version,
        "change_type": changeType,
        "changed_by":  req.OperatorId,
        "changed_at":  now.UnixMilli(),
    })
    s.redis.Publish(ctx, pubsubChannel, string(changeNotify))

    // 同时发布到服务级通配频道，让 WatchConfig 监听整个服务的客户端也能收到
    servicePubsub := fmt.Sprintf("config:watch:%s:*", req.Service)
    s.redis.Publish(ctx, servicePubsub, string(changeNotify))

    // ==================== 7. 投递 Kafka 事件: config.changed ====================
    configChangedEvent := &kafka_config.ConfigChangedEvent{
        Header:   buildEventHeader("config", s.instanceID),
        ConfigKey:       fmt.Sprintf("%s.%s", req.Service, req.Key),
        OldValue:        oldValue,
        NewValue:        req.Value,
        ConfigGroup:     req.Service,
        OperatorId:      req.OperatorId,
        Description:     req.Description,
        ChangeTime:      now.UnixMilli(),
        AffectedServices: []string{req.Service},
    }
    if err := s.kafka.Produce(ctx, "config.changed", req.Service, configChangedEvent); err != nil {
        log.Error("produce config.changed event failed",
            "service", req.Service, "key", req.Key, "err", err)
        // Kafka 投递失败不影响主流程，已写入 DB，后续可通过补偿机制重试
    }

    // ==================== 8. 返回 ====================
    return &pb.SetConfigResp{
        Meta:   successMeta(ctx),
        Config: &resultItem,
    }, nil
}

// validateConfigValue 校验配置值是否与声明的数据类型匹配
func validateConfigValue(value string, dataType string) error {
    switch dataType {
    case "int":
        if _, err := strconv.ParseInt(value, 10, 64); err != nil {
            return fmt.Errorf("value '%s' is not a valid integer", value)
        }
    case "float":
        if _, err := strconv.ParseFloat(value, 64); err != nil {
            return fmt.Errorf("value '%s' is not a valid float", value)
        }
    case "bool":
        if value != "true" && value != "false" {
            return fmt.Errorf("value '%s' is not a valid boolean, must be 'true' or 'false'", value)
        }
    case "json":
        if !json.Valid([]byte(value)) {
            return fmt.Errorf("value is not valid JSON")
        }
    case "string":
        // 字符串类型不做额外校验
    }
    return nil
}
```

### 4. DeleteConfig — 删除配置项

> 删除指定配置项。先从 PgSQL 删除，再失效所有相关 Redis 缓存，  
> 通过 PubSub 通知 WatchConfig，最后投递 Kafka 变更事件。

```go
func (s *ConfigService) DeleteConfig(ctx context.Context, req *pb.DeleteConfigReq) (*pb.DeleteConfigResp, error) {
    // ==================== 1. 参数校验 ====================
    if req.Service == "" {
        return nil, status.Error(codes.InvalidArgument, "service is required")
    }
    if req.Key == "" {
        return nil, status.Error(codes.InvalidArgument, "key is required")
    }
    if req.OperatorId == "" {
        return nil, status.Error(codes.InvalidArgument, "operator_id is required")
    }

    // ==================== 2. 查询待删除配置（用于事件中携带旧值） ====================
    var oldValue string
    var oldVersion int
    err := s.db.QueryRowContext(ctx,
        `SELECT value, version FROM configs WHERE service = $1 AND key = $2`,
        req.Service, req.Key,
    ).Scan(&oldValue, &oldVersion)
    if err == sql.ErrNoRows {
        return nil, status.Error(codes.NotFound, "config not found")
    }
    if err != nil {
        return nil, status.Error(codes.Internal, "query config for delete failed: "+err.Error())
    }

    // ==================== 延迟双删 — DB 写入前失效缓存，500ms 后再次删除防止并发读回填旧数据 ====================
    s.delayedDoubleDelete(ctx,
        fmt.Sprintf("config:%s:%s", req.Service, req.Key),
        fmt.Sprintf("config:service_all:%s", req.Service),
        fmt.Sprintf("config:version:%s:%s", req.Service, req.Key),
    )

    // ==================== 3. 删除 PgSQL 记录 ====================
    result, err := s.db.ExecContext(ctx,
        `DELETE FROM configs WHERE service = $1 AND key = $2`,
        req.Service, req.Key,
    )
    if err != nil {
        return nil, status.Error(codes.Internal, "delete config from db failed: "+err.Error())
    }
    rowsAffected, _ := result.RowsAffected()
    if rowsAffected == 0 {
        return nil, status.Error(codes.NotFound, "config not found or already deleted")
    }

    now := time.Now()

    // ==================== 4. Redis PubSub 通知 WatchConfig ====================
    pubsubChannel := fmt.Sprintf("config:watch:%s:%s", req.Service, req.Key)
    deleteNotify, _ := json.Marshal(map[string]interface{}{
        "service":     req.Service,
        "key":         req.Key,
        "value":       "",
        "old_value":   oldValue,
        "version":     oldVersion,
        "change_type": "DELETE",
        "changed_by":  req.OperatorId,
        "changed_at":  now.UnixMilli(),
    })
    s.redis.Publish(ctx, pubsubChannel, string(deleteNotify))
    s.redis.Publish(ctx, fmt.Sprintf("config:watch:%s:*", req.Service), string(deleteNotify))

    // ==================== 6. 投递 Kafka 事件: config.changed ====================
    configChangedEvent := &kafka_config.ConfigChangedEvent{
        Header:           buildEventHeader("config", s.instanceID),
        ConfigKey:        fmt.Sprintf("%s.%s", req.Service, req.Key),
        OldValue:         oldValue,
        NewValue:         "",
        ConfigGroup:      req.Service,
        OperatorId:       req.OperatorId,
        Description:      "config deleted",
        ChangeTime:       now.UnixMilli(),
        AffectedServices: []string{req.Service},
    }
    if err := s.kafka.Produce(ctx, "config.changed", req.Service, configChangedEvent); err != nil {
        log.Error("produce config.changed (delete) event failed",
            "service", req.Service, "key", req.Key, "err", err)
    }

    return &pb.DeleteConfigResp{
        Meta: successMeta(ctx),
    }, nil
}
```

### 5. GetServiceConfigs — 获取服务全量配置

> 获取指定服务下的所有配置项，支持分页。优先从 Redis HASH 缓存读取（TTL 5min），miss 时回查 PgSQL。

```go
func (s *ConfigService) GetServiceConfigs(ctx context.Context, req *pb.GetServiceConfigsReq) (*pb.GetServiceConfigsResp, error) {
    // ==================== 1. 参数校验 ====================
    if req.Service == "" {
        return nil, status.Error(codes.InvalidArgument, "service is required")
    }

    // 分页默认值
    page := int32(1)
    pageSize := int32(50)
    if req.Pagination != nil {
        if req.Pagination.Page > 0 {
            page = req.Pagination.Page
        }
        if req.Pagination.PageSize > 0 {
            pageSize = req.Pagination.PageSize
        }
    }
    if pageSize > 200 {
        pageSize = 200 // 最大 200 条/页
    }

    // ==================== 2. 尝试从 Redis HASH 缓存读取 ====================
    serviceAllKey := fmt.Sprintf("config:service_all:%s", req.Service)
    cachedMap, err := s.redis.HGetAll(ctx, serviceAllKey).Result()
    if err == nil && len(cachedMap) > 0 {
        // 缓存命中 → 反序列化 + 内存分页
        allConfigs := make([]*pb.ConfigItem, 0, len(cachedMap))
        for _, v := range cachedMap {
            var item pb.ConfigItem
            if json.Unmarshal([]byte(v), &item) == nil {
                allConfigs = append(allConfigs, &item)
            }
        }

        // 按 key 字母排序（保证稳定分页）
        sort.Slice(allConfigs, func(i, j int) bool {
            return allConfigs[i].Key < allConfigs[j].Key
        })

        total := int64(len(allConfigs))
        totalPages := int32((total + int64(pageSize) - 1) / int64(pageSize))
        start := (page - 1) * pageSize
        end := start + pageSize
        if int64(start) >= total {
            start = 0
            end = 0
        }
        if int64(end) > total {
            end = int32(total)
        }

        var pageResult []*pb.ConfigItem
        if start < end {
            pageResult = allConfigs[start:end]
        }

        return &pb.GetServiceConfigsResp{
            Meta:    successMeta(ctx),
            Configs: pageResult,
            PaginationResult: &common.PaginationResult{
                Page:       page,
                PageSize:   pageSize,
                Total:      total,
                TotalPages: totalPages,
            },
        }, nil
    }

    // ==================== 3. 回查 PgSQL ====================
    // 先查总数
    var total int64
    err = s.db.QueryRowContext(ctx,
        `SELECT COUNT(*) FROM configs WHERE service = $1`,
        req.Service,
    ).Scan(&total)
    if err != nil {
        return nil, status.Error(codes.Internal, "count configs failed: "+err.Error())
    }

    totalPages := int32((total + int64(pageSize) - 1) / int64(pageSize))
    offset := (page - 1) * pageSize

    // 分页查询
    rows, err := s.db.QueryContext(ctx,
        `SELECT id, service, key, value, description, value_type, version, updated_at, updated_by
         FROM configs
         WHERE service = $1
         ORDER BY key ASC
         LIMIT $2 OFFSET $3`,
        req.Service, pageSize, offset,
    )
    if err != nil {
        return nil, status.Error(codes.Internal, "query service configs failed: "+err.Error())
    }
    defer rows.Close()

    configs := make([]*pb.ConfigItem, 0)
    for rows.Next() {
        var item pb.ConfigItem
        var valueRaw []byte
        if err := rows.Scan(
            &item.ConfigId, &item.Service, &item.Key, &valueRaw,
            &item.Description, &item.DataType, &item.Version,
            &item.UpdatedAt, &item.UpdatedBy,
        ); err != nil {
            log.Error("scan config row failed", "err", err)
            continue
        }
        item.Value = string(valueRaw)
        configs = append(configs, &item)
    }

    // ==================== 4. 异步回填 Redis HASH 缓存（全量，TTL 5min） ====================
    go func() {
        // 查全量用于缓存（不受分页限制）
        allRows, err := s.db.QueryContext(context.Background(),
            `SELECT id, service, key, value, description, value_type, version, updated_at, updated_by
             FROM configs WHERE service = $1 ORDER BY key ASC`,
            req.Service,
        )
        if err != nil {
            log.Warn("query all configs for cache backfill failed", "service", req.Service, "err", err)
            return
        }
        defer allRows.Close()

        pipe := s.redis.Pipeline()
        for allRows.Next() {
            var item pb.ConfigItem
            var valueRaw []byte
            if err := allRows.Scan(
                &item.ConfigId, &item.Service, &item.Key, &valueRaw,
                &item.Description, &item.DataType, &item.Version,
                &item.UpdatedAt, &item.UpdatedBy,
            ); err != nil {
                continue
            }
            item.Value = string(valueRaw)
            itemJSON, _ := json.Marshal(&item)
            pipe.HSet(context.Background(), serviceAllKey, item.Key, string(itemJSON))
        }
        pipe.Expire(context.Background(), serviceAllKey, 5*time.Minute)
        pipe.Exec(context.Background())
    }()

    return &pb.GetServiceConfigsResp{
        Meta:    successMeta(ctx),
        Configs: configs,
        PaginationResult: &common.PaginationResult{
            Page:       page,
            PageSize:   pageSize,
            Total:      total,
            TotalPages: totalPages,
        },
    }, nil
}
```

### 6. WatchConfig — 监听配置变更（Server Streaming RPC）

> 客户端建立 Server Streaming RPC 连接，订阅指定 service + keys 的配置变更。  
> 底层基于 Redis PubSub，配置变更时实时推送给客户端。支持断线重连时传入 `last_version` 做增量同步。

```go
func (s *ConfigService) WatchConfig(req *pb.WatchConfigReq, stream pb.ConfigService_WatchConfigServer) error {
    ctx := stream.Context()

    // ==================== 1. 参数校验 ====================
    if req.Service == "" {
        return status.Error(codes.InvalidArgument, "service is required")
    }

    // ==================== 2. 增量同步（断线重连场景） ====================
    // 如果客户端传入了 last_version，先查 DB 获取 version > last_version 的变更
    if req.LastVersion > 0 {
        rows, err := s.db.QueryContext(ctx,
            `SELECT id, service, key, value, description, value_type, version, updated_at, updated_by
             FROM configs
             WHERE service = $1 AND version > $2
             ORDER BY version ASC`,
            req.Service, req.LastVersion,
        )
        if err != nil {
            return status.Error(codes.Internal, "query incremental configs failed: "+err.Error())
        }
        defer rows.Close()

        changes := make([]*pb.ConfigItem, 0)
        for rows.Next() {
            var item pb.ConfigItem
            var valueRaw []byte
            if err := rows.Scan(
                &item.ConfigId, &item.Service, &item.Key, &valueRaw,
                &item.Description, &item.DataType, &item.Version,
                &item.UpdatedAt, &item.UpdatedBy,
            ); err != nil {
                continue
            }
            item.Value = string(valueRaw)
            changes = append(changes, &item)
        }

        if len(changes) > 0 {
            // 发送增量变更
            if err := stream.Send(&pb.WatchConfigResp{
                Meta:       successMeta(ctx),
                Changes:    changes,
                HasChanges: true,
            }); err != nil {
                return err
            }
        }
    }

    // ==================== 3. 订阅 Redis PubSub 频道 ====================
    // 订阅模式：
    //   - 如果指定了 keys，订阅每个 key 的精确频道
    //   - 如果未指定 keys，订阅服务级通配频道
    pubsub := s.redis.Subscribe(ctx)
    defer pubsub.Close()

    if len(req.Keys) > 0 {
        channels := make([]string, 0, len(req.Keys))
        for _, key := range req.Keys {
            channels = append(channels, fmt.Sprintf("config:watch:%s:%s", req.Service, key))
        }
        if err := pubsub.Subscribe(ctx, channels...); err != nil {
            return status.Error(codes.Internal, "subscribe config watch channels failed: "+err.Error())
        }
    } else {
        // 订阅服务级通配频道
        channel := fmt.Sprintf("config:watch:%s:*", req.Service)
        if err := pubsub.PSubscribe(ctx, channel); err != nil {
            return status.Error(codes.Internal, "psubscribe config watch channel failed: "+err.Error())
        }
    }

    // ==================== 4. 消息循环 — 接收 PubSub 消息并推送给客户端 ====================
    ch := pubsub.Channel()
    for {
        select {
        case <-ctx.Done():
            // 客户端断开连接或超时
            log.Info("WatchConfig stream closed",
                "service", req.Service, "keys", req.Keys, "reason", ctx.Err())
            return nil

        case msg, ok := <-ch:
            if !ok {
                // PubSub channel 关闭
                return status.Error(codes.Unavailable, "pubsub channel closed")
            }

            // 解析变更通知
            var changeNotify map[string]interface{}
            if err := json.Unmarshal([]byte(msg.Payload), &changeNotify); err != nil {
                log.Warn("unmarshal watch config notification failed",
                    "payload", msg.Payload, "err", err)
                continue
            }

            // 构造变更 ConfigItem
            changedItem := &pb.ConfigItem{
                Service:   getStringFromMap(changeNotify, "service"),
                Key:       getStringFromMap(changeNotify, "key"),
                Value:     getStringFromMap(changeNotify, "value"),
                Version:   getInt64FromMap(changeNotify, "version"),
                UpdatedBy: getStringFromMap(changeNotify, "changed_by"),
            }

            // 推送给客户端
            if err := stream.Send(&pb.WatchConfigResp{
                Meta:       successMeta(ctx),
                Changes:    []*pb.ConfigItem{changedItem},
                HasChanges: true,
            }); err != nil {
                log.Warn("send WatchConfig response failed",
                    "service", req.Service, "err", err)
                return err
            }
        }
    }
}
```

### 7. GetFeatureFlags — 获取功能开关列表

> 获取功能开关列表，支持按服务名筛选和分页。优先从 Redis HASH 缓存读取，miss 时回查 PgSQL。

```go
func (s *ConfigService) GetFeatureFlags(ctx context.Context, req *pb.GetFeatureFlagsReq) (*pb.GetFeatureFlagsResp, error) {
    // ==================== 1. 分页默认值 ====================
    page := int32(1)
    pageSize := int32(50)
    if req.Pagination != nil {
        if req.Pagination.Page > 0 {
            page = req.Pagination.Page
        }
        if req.Pagination.PageSize > 0 {
            pageSize = req.Pagination.PageSize
        }
    }
    if pageSize > 200 {
        pageSize = 200
    }

    // ==================== 2. 尝试从 Redis HASH 缓存读取 ====================
    flagsAllKey := "config:flags_all"
    cachedMap, err := s.redis.HGetAll(ctx, flagsAllKey).Result()
    if err == nil && len(cachedMap) > 0 {
        // 缓存命中 → 反序列化 + 内存筛选 + 分页
        allFlags := make([]*pb.FeatureFlag, 0, len(cachedMap))
        for _, v := range cachedMap {
            var flag pb.FeatureFlag
            if json.Unmarshal([]byte(v), &flag) == nil {
                // 如果指定了 service，按前缀过滤（开关名以 service. 开头）
                if req.Service != "" && !strings.HasPrefix(flag.Name, req.Service+".") {
                    continue
                }
                allFlags = append(allFlags, &flag)
            }
        }

        // 按 name 排序
        sort.Slice(allFlags, func(i, j int) bool {
            return allFlags[i].Name < allFlags[j].Name
        })

        total := int64(len(allFlags))
        totalPages := int32((total + int64(pageSize) - 1) / int64(pageSize))
        start := (page - 1) * pageSize
        end := start + pageSize
        if int64(start) >= total {
            start, end = 0, 0
        }
        if int64(end) > total {
            end = int32(total)
        }

        var pageResult []*pb.FeatureFlag
        if start < end {
            pageResult = allFlags[start:end]
        }

        return &pb.GetFeatureFlagsResp{
            Meta:  successMeta(ctx),
            Flags: pageResult,
            PaginationResult: &common.PaginationResult{
                Page:       page,
                PageSize:   pageSize,
                Total:      total,
                TotalPages: totalPages,
            },
        }, nil
    }

    // ==================== 3. 回查 PgSQL ====================
    // 构建查询条件
    var whereClause string
    var args []interface{}
    if req.Service != "" {
        whereClause = "WHERE flag_name LIKE $1"
        args = append(args, req.Service+".%")
    }

    // 查总数
    countQuery := fmt.Sprintf("SELECT COUNT(*) FROM feature_flags %s", whereClause)
    var total int64
    err = s.db.QueryRowContext(ctx, countQuery, args...).Scan(&total)
    if err != nil {
        return nil, status.Error(codes.Internal, "count feature flags failed: "+err.Error())
    }

    totalPages := int32((total + int64(pageSize) - 1) / int64(pageSize))
    offset := (page - 1) * pageSize

    // 分页查询
    dataQuery := fmt.Sprintf(
        `SELECT id, flag_name, is_enabled, description, rollout_percentage, target_users, version, updated_at
         FROM feature_flags %s
         ORDER BY flag_name ASC
         LIMIT %d OFFSET %d`,
        whereClause, pageSize, offset,
    )
    rows, err := s.db.QueryContext(ctx, dataQuery, args...)
    if err != nil {
        return nil, status.Error(codes.Internal, "query feature flags failed: "+err.Error())
    }
    defer rows.Close()

    flags := make([]*pb.FeatureFlag, 0)
    for rows.Next() {
        var flag pb.FeatureFlag
        var targetUsersRaw []byte
        var rolloutPct int
        if err := rows.Scan(
            &flag.FlagId, &flag.Name, &flag.Enabled, &flag.Description,
            &rolloutPct, &targetUsersRaw, // version 用于 Rules 的 JSON 组装
            &flag.UpdatedAt, // 此处简化 —— 实际还需扫 version 等字段
        ); err != nil {
            log.Error("scan feature flag row failed", "err", err)
            continue
        }
        // 组装 rules JSON
        flag.Rules, _ = json.Marshal(map[string]interface{}{
            "rollout_percentage": rolloutPct,
            "target_users":      json.RawMessage(targetUsersRaw),
        })
        flags = append(flags, &flag)
    }

    // ==================== 4. 异步回填 Redis HASH 缓存 ====================
    go func() {
        allRows, err := s.db.QueryContext(context.Background(),
            `SELECT id, flag_name, is_enabled, description, rollout_percentage, target_users, version, updated_at
             FROM feature_flags ORDER BY flag_name ASC`,
        )
        if err != nil {
            return
        }
        defer allRows.Close()

        pipe := s.redis.Pipeline()
        for allRows.Next() {
            var flag pb.FeatureFlag
            var targetUsersRaw []byte
            var rolloutPct int
            var version int
            if err := allRows.Scan(
                &flag.FlagId, &flag.Name, &flag.Enabled, &flag.Description,
                &rolloutPct, &targetUsersRaw, &version, &flag.UpdatedAt,
            ); err != nil {
                continue
            }
            flag.Rules, _ = json.Marshal(map[string]interface{}{
                "rollout_percentage": rolloutPct,
                "target_users":      json.RawMessage(targetUsersRaw),
                "version":           version,
            })
            flagJSON, _ := json.Marshal(&flag)
            pipe.HSet(context.Background(), flagsAllKey, flag.Name, string(flagJSON))
        }
        pipe.Expire(context.Background(), flagsAllKey, 5*time.Minute)
        pipe.Exec(context.Background())
    }()

    return &pb.GetFeatureFlagsResp{
        Meta:  successMeta(ctx),
        Flags: flags,
        PaginationResult: &common.PaginationResult{
            Page:       page,
            PageSize:   pageSize,
            Total:      total,
            TotalPages: totalPages,
        },
    }, nil
}
```

### 8. SetFeatureFlag — 设置功能开关

> 创建或更新功能开关。使用 PgSQL UPSERT + 乐观锁保证原子性，  
> 写入后立即失效 Redis 缓存，投递 Kafka 事件通知所有服务。

```go
func (s *ConfigService) SetFeatureFlag(ctx context.Context, req *pb.SetFeatureFlagReq) (*pb.SetFeatureFlagResp, error) {
    // ==================== 1. 参数校验 ====================
    if req.Name == "" {
        return nil, status.Error(codes.InvalidArgument, "name is required")
    }
    if req.OperatorId == "" {
        return nil, status.Error(codes.InvalidArgument, "operator_id is required")
    }
    // 校验 rules JSON 格式
    var rules struct {
        RolloutPercentage int             `json:"rollout_percentage"`
        TargetUsers       json.RawMessage `json:"target_users"`
    }
    if req.Rules != "" {
        if err := json.Unmarshal([]byte(req.Rules), &rules); err != nil {
            return nil, status.Error(codes.InvalidArgument, "rules must be valid JSON: "+err.Error())
        }
        if rules.RolloutPercentage < 0 || rules.RolloutPercentage > 100 {
            return nil, status.Error(codes.InvalidArgument, "rollout_percentage must be 0-100")
        }
    }

    now := time.Now()

    // ==================== 2. 查询旧值（用于事件对比） ====================
    var oldEnabled bool
    var oldVersion int
    var oldRollout int
    var isNew bool
    err := s.db.QueryRowContext(ctx,
        `SELECT is_enabled, version, rollout_percentage FROM feature_flags WHERE flag_name = $1`,
        req.Name,
    ).Scan(&oldEnabled, &oldVersion, &oldRollout)
    if err == sql.ErrNoRows {
        isNew = true
    } else if err != nil {
        return nil, status.Error(codes.Internal, "query existing feature flag failed: "+err.Error())
    }

    // ==================== 延迟双删 — DB 写入前失效缓存，500ms 后再次删除防止并发读回填旧数据 ====================
    s.delayedDoubleDelete(ctx,
        fmt.Sprintf("config:flags:%s", req.Name),
        "config:flags_all",
    )

    // ==================== 3. UPSERT 功能开关 — PgSQL ====================
    var resultFlag pb.FeatureFlag
    var targetUsersJSON []byte
    if rules.TargetUsers != nil {
        targetUsersJSON = rules.TargetUsers
    } else {
        targetUsersJSON = []byte("[]")
    }

    if isNew {
        // 新建
        err = s.db.QueryRowContext(ctx,
            `INSERT INTO feature_flags (flag_name, is_enabled, description, rollout_percentage, target_users, version, updated_by, created_at, updated_at)
             VALUES ($1, $2, $3, $4, $5, 1, $6, $7, $7)
             RETURNING id, flag_name, is_enabled, description, updated_at`,
            req.Name, req.Enabled, req.Description, rules.RolloutPercentage, targetUsersJSON,
            req.OperatorId, now,
        ).Scan(
            &resultFlag.FlagId, &resultFlag.Name, &resultFlag.Enabled,
            &resultFlag.Description, &resultFlag.UpdatedAt,
        )
        if err != nil {
            if isPgUniqueViolation(err) {
                return nil, status.Error(codes.AlreadyExists, "feature flag already exists, retry")
            }
            return nil, status.Error(codes.Internal, "insert feature flag failed: "+err.Error())
        }
    } else {
        // 更新（乐观锁）
        result, err := s.db.ExecContext(ctx,
            `UPDATE feature_flags
             SET is_enabled = $1, description = $2, rollout_percentage = $3, target_users = $4,
                 version = version + 1, updated_by = $5, updated_at = $6
             WHERE flag_name = $7 AND version = $8`,
            req.Enabled, req.Description, rules.RolloutPercentage, targetUsersJSON,
            req.OperatorId, now,
            req.Name, oldVersion,
        )
        if err != nil {
            return nil, status.Error(codes.Internal, "update feature flag failed: "+err.Error())
        }
        rowsAffected, _ := result.RowsAffected()
        if rowsAffected == 0 {
            return nil, status.Error(codes.Aborted, "feature flag version conflict, please retry")
        }

        // 查询更新后的记录
        err = s.db.QueryRowContext(ctx,
            `SELECT id, flag_name, is_enabled, description, updated_at
             FROM feature_flags WHERE flag_name = $1`,
            req.Name,
        ).Scan(
            &resultFlag.FlagId, &resultFlag.Name, &resultFlag.Enabled,
            &resultFlag.Description, &resultFlag.UpdatedAt,
        )
        if err != nil {
            return nil, status.Error(codes.Internal, "query updated feature flag failed: "+err.Error())
        }
    }

    // 填充 rules
    resultFlag.Rules = req.Rules

    // ==================== 4. 回填单个开关缓存 — Redis HASH ====================
    flagCacheKey := fmt.Sprintf("config:flags:%s", req.Name)
    s.redis.HSet(ctx, flagCacheKey, map[string]interface{}{
        "is_enabled":          req.Enabled,
        "rollout_percentage":  rules.RolloutPercentage,
        "target_users":        string(targetUsersJSON),
        "version":             oldVersion + 1,
    })
    // 无 TTL（持久缓存，变更时失效重建）

    // ==================== 6. 投递 Kafka 事件: config.flag.changed ====================
    changeType := "CREATE"
    if !isNew {
        changeType = "UPDATE"
    }
    flagChangedEvent := &kafka_config.ConfigChangedEvent{
        Header:           buildEventHeader("config", s.instanceID),
        ConfigKey:        fmt.Sprintf("feature_flag.%s", req.Name),
        OldValue:         fmt.Sprintf(`{"enabled":%v,"rollout":%d}`, oldEnabled, oldRollout),
        NewValue:         fmt.Sprintf(`{"enabled":%v,"rollout":%d}`, req.Enabled, rules.RolloutPercentage),
        ConfigGroup:      "feature_flags",
        OperatorId:       req.OperatorId,
        Description:      fmt.Sprintf("%s feature flag: %s", changeType, req.Name),
        ChangeTime:       now.UnixMilli(),
        AffectedServices: []string{"*"}, // 功能开关影响所有服务
    }
    if err := s.kafka.Produce(ctx, "config.flag.changed", req.Name, flagChangedEvent); err != nil {
        log.Error("produce config.flag.changed event failed",
            "flag_name", req.Name, "err", err)
    }

    return &pb.SetFeatureFlagResp{
        Meta: successMeta(ctx),
        Flag: &resultFlag,
    }, nil
}
```

---

## 辅助函数

```go
// delayedDoubleDelete 延迟双删工具函数
// Phase 1: 立即删除缓存（在 DB 写入前调用，防止旧缓存被读取）
// Phase 2: 延迟 500ms 后再次删除缓存（防止并发读在 DB 写入窗口期回填旧数据）
func (s *ConfigService) delayedDoubleDelete(ctx context.Context, keys ...string) {
    if len(keys) == 0 {
        return
    }
    // Phase 1: 立即删除
    s.redis.Del(ctx, keys...)
    // Phase 2: 延迟 500ms 后再次删除
    go func(delKeys []string) {
        time.Sleep(500 * time.Millisecond)
        if _, err := s.redis.Del(context.Background(), delKeys...).Result(); err != nil {
            log.Warn("delayedDoubleDelete phase 2 failed", "keys", delKeys, "err", err)
        }
    }(keys)
}

// buildEventHeader 构建 Kafka 事件头
func buildEventHeader(source string, instanceID string) *common.EventHeader {
    return &common.EventHeader{
        EventId:   uuid.New().String(),
        TraceId:   trace.SpanFromContext(ctx).SpanContext().TraceID().String(),
        SpanId:    trace.SpanFromContext(ctx).SpanContext().SpanID().String(),
        Source:    source,
        SourceId:  instanceID,
        Timestamp: time.Now().UnixMilli(),
        Version:   1,
    }
}

// successMeta 构建成功响应元数据
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
    return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

// getStringFromMap 从 map 安全取字符串
func getStringFromMap(m map[string]interface{}, key string) string {
    if v, ok := m[key]; ok {
        if s, ok := v.(string); ok {
            return s
        }
    }
    return ""
}

// getInt64FromMap 从 map 安全取 int64
func getInt64FromMap(m map[string]interface{}, key string) int64 {
    if v, ok := m[key]; ok {
        switch n := v.(type) {
        case float64:
            return int64(n)
        case int64:
            return n
        }
    }
    return 0
}
```

---

## 可观测性接入（OpenTelemetry）

> Config 服务是配置中心，零外部 RPC 依赖，需观测：配置读取频率、配置变更事件、WatchConfig 长连接数、Feature Flag 查询频率。

### 第一步：服务启动时初始化 OTel SDK

```go
func main() {
    ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
    defer cancel()

    otelShutdown := observability.InitOpenTelemetry(ctx, "config", "v1.0.0")
    defer otelShutdown(ctx)
    observability.SetupLogger("config")
}
```

### 第二步：gRPC Server Interceptor（无需 Client，Config 不调用其他服务）

```go
grpcServer := grpc.NewServer(
    grpc.ChainUnaryInterceptor(otelgrpc.UnaryServerInterceptor()),
    grpc.ChainStreamInterceptor(otelgrpc.StreamServerInterceptor()), // WatchConfig 是 stream
)
```

### 第三步：注册 Config 专属业务指标

```go
var meter = otel.Meter("im-chat/config")

var (
    // 配置变更事件计数
    configChanged, _ = meter.Int64Counter("config.changed_total",
        metric.WithDescription("配置变更总数"))

    // Feature Flag 查询计数
    featureFlagQueried, _ = meter.Int64Counter("config.feature_flag_queried_total",
        metric.WithDescription("Feature Flag 查询总数"))

    // WatchConfig 活跃 stream 数
    activeWatchers, _ = meter.Int64UpDownCounter("config.active_watchers",
        metric.WithDescription("当前活跃的 WatchConfig stream 连接数"))

    // 配置热加载耗时
    hotReloadDuration, _ = meter.Float64Histogram("config.hot_reload_duration_ms",
        metric.WithDescription("配置热加载处理耗时"), metric.WithUnit("ms"))
)
```

在业务代码中埋点：

```go
// SetConfig 中 — 配置变更
configChanged.Add(ctx, 1, metric.WithAttributes(
    attribute.String("service", req.ServiceName),
    attribute.String("key", req.Key),
))

// WatchConfig 中 — stream 开启/关闭
activeWatchers.Add(ctx, 1)
defer activeWatchers.Add(ctx, -1)

// GetFeatureFlags 中
featureFlagQueried.Add(ctx, 1)
```

### 第四步：Kafka Producer 埋点 + 结构化 JSON 日志

发布 `config.changed` / `config.feature_flag.changed` 事件时注入 trace context。

### 接入要点总结

| 步骤 | 改动位置 | 效果 |
|------|---------|------|
| 1. `observability.InitOpenTelemetry` | main() | TracerProvider + MeterProvider + Runtime Metrics |
| 2. gRPC Server Interceptor | Server（无 Client） | RPC 自动 trace + 指标 |
| 3. 自定义业务指标 | SetConfig/WatchConfig/GetFeatureFlags | 配置变更率、活跃 watcher 数 |
| 4. Kafka + 日志 | 事件发布 + setupLogger | 链路追踪 + trace_id 日志 |
| 5. buildEventHeader 改造 | 辅助函数 | EventHeader 携带真实 trace context |
| 6. 基础设施指标 | main() | DB/Redis 连接池可观测 |
| 7. Span 错误记录 | `observability.RecordError` | 错误 trace 在 Tempo 可见 |

### 补充：buildEventHeader 改造

将 `buildEventHeader(source, instanceID)` 替换为 `observability.BuildEventHeader(ctx, source, instanceID)`。

### 补充：基础设施指标注册

```go
observability.RegisterDBPoolMetrics(db, "config")
observability.RegisterRedisPoolMetrics(redisClient, "config")
```

### 补充：Span 错误记录

```go
func (s *ConfigServer) SetConfig(ctx context.Context, req *pb.SetConfigRequest) (*pb.SetConfigResponse, error) {
    ctx, span := otel.Tracer("config").Start(ctx, "SetConfig")
    defer span.End()

    result, err := s.doSetConfig(ctx, req)
    if err != nil {
        observability.RecordError(span, err)
        return nil, err
    }
    return result, nil
}
```
