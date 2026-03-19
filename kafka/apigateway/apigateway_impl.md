# ApiGateway API 网关服务 — Kafka 消费者实现伪代码

## 概述

ApiGateway 作为 HTTP 网关，主要角色是 Kafka **生产者**（投递请求日志、限流事件、认证失败事件、错误事件等供 Audit 服务审计）。
同时消费少量 Topic 用于**热更新网关配置**：限流规则、路由配置、维护模式开关、版本信息、功能开关等。

**设计原则：**
- 网关完全无状态，配置全部通过 Redis 存储，消费 Kafka 事件实时刷新 Redis
- 消费者处理失败时 fail-open（不影响网关正常服务），仅记录日志
- 所有消费者均做幂等处理，防止重复消费

## 生产 Topic 列表

| Topic | 事件 | 生产时机 | Key | 消费方 |
|-------|------|----------|-----|--------|
| `apigateway.request.log` | RequestLogEvent | 每次 HTTP 请求处理完成 | request_id | Audit（请求日志记录/分析） |
| `apigateway.event.reported` | JSON 客户端事件 | ReportEvent 接口调用 | user_id | Audit（运营分析）/ Analytics（数据分析） |
| `apigateway.error` | GatewayErrorEvent | 网关错误（下游超时/熔断/5xx） | request_id | Audit（错误追踪/告警） |
| `apigateway.auth.fail` | AuthFailEvent | 认证中间件校验 Token 失败 | client_ip | Audit（安全监控/暴力破解检测） |
| `apigateway.rate.limited` | RateLimitedEvent | 限流中间件触发限流 | client_ip | Audit（限流分析/DDoS 检测） |

## 消费 Topic 列表

| Topic | 来源 | Consumer Group | 用途 |
|-------|------|----------------|------|
| `config.changed` | Config 服务 | apigateway-config-consumer | 热更新限流配置、路由配置、维护模式、版本信息 |
| `config.flag.changed` | Config 服务 | apigateway-flag-consumer | 热更新功能开关（用于 A/B 测试路由决策） |

## Redis Key 设计（消费者专用）

| Key 格式 | 类型 | 值 | TTL | 说明 |
|----------|------|-----|-----|------|
| `gw:kafka:dedup:{event_id}` | STRING | "1" | 24h | Kafka 消费幂等去重 |
| `gw:rate_limit:{user_id}:{api}` | STRING | count | 1min | per-API 限流计数器（消费 config.changed 时可能重置） |
| `gw:rate_limit_global:{ip}` | STRING | count | 1min | IP 级限流计数器 |
| `gw:route_cache` | HASH | field=path, value=service_name | 5min | 路由映射缓存 |
| `gw:server_time_offset` | STRING | offset_ms | 无 TTL | 时间校准偏移量 |
| `gw:app_version:{platform}` | HASH | {min_version, latest_version, update_url, force_update, release_notes, package_size, md5} | 无 TTL | 各平台版本信息 |
| `gw:maintenance_mode` | STRING | JSON {enabled, message, estimated_end} | 无 TTL | 系统维护模式开关 |
| `gw:feature_flags` | HASH | field=flag_name, value=JSON | 无 TTL | 功能开关缓存（A/B 测试路由决策用） |
| `gw:rate_limit_config` | HASH | field=api_path, value=limit_per_min | 无 TTL | 各 API 限流阈值配置 |
| `gw:ip_rate_limit_config` | STRING | limit_per_min | 无 TTL | IP 级限流阈值配置 |

## 请求生命周期

```
                              ┌──────────────────────────────────────────┐
                              │         ApiGateway 请求生命周期          │
                              └──────────────────────────────────────────┘

客户端 HTTP 请求
      │
      ▼
┌─────────────┐   超限   ┌──────────────────┐
│  RateLimit  │────────→ │ 429 + Kafka 事件  │
│  中间件     │          │ rate.limited      │
└──────┬──────┘          └──────────────────┘
       │ 放行
       ▼
┌─────────────┐   失败   ┌──────────────────┐
│    Auth     │────────→ │ 401 + Kafka 事件  │
│  中间件     │          │ auth.fail         │
└──────┬──────┘          └──────────────────┘
       │ 放行（注入 user_id/platform）
       ▼
┌─────────────┐   无匹配  ┌──────────────────┐
│   Route     │─────────→ │ 404 Not Found    │
│  路由匹配   │           └──────────────────┘
└──────┬──────┘
       │ 匹配到下游服务
       ▼
┌─────────────┐   下游异常  ┌──────────────────┐
│  Handler    │──────────→ │ 5xx + Kafka 事件  │
│ HTTP→gRPC   │            │ error             │
│ 协议转换    │            └──────────────────┘
└──────┬──────┘
       │ 成功
       ▼
┌─────────────┐
│ 返回响应    │──→ Kafka: apigateway.request.log
│ + 请求日志  │
└─────────────┘
```

## Kafka 配置事件对应的 Config Key 约定

ApiGateway 消费 `config.changed` 事件时，根据 `config_key` 前缀路由到不同的处理逻辑：

| config_key 模式 | 处理逻辑 | 更新的 Redis Key |
|----------------|----------|-----------------|
| `apigateway.rate_limit.*` | 更新限流配置 | `gw:rate_limit_config` / `gw:ip_rate_limit_config` |
| `apigateway.route.*` | 更新路由缓存 | `gw:route_cache` |
| `apigateway.maintenance` | 更新维护模式 | `gw:maintenance_mode` |
| `apigateway.version.*` | 更新版本信息 | `gw:app_version:{platform}` |
| `apigateway.server_time_offset` | 更新时间校准 | `gw:server_time_offset` |

---

## 消费者实现

### Consumer: `config.changed`（配置变更 — 热更新网关配置）

> 来源：Config 服务。当管理后台修改了网关相关配置（限流规则、路由、维护模式、版本信息等），  
> Config 服务投递 `config.changed` 事件，网关消费后实时刷新 Redis 缓存。  
> 网关配置的 service 字段统一为 `apigateway`，通过 key 区分具体配置项。

```go
func (c *ApiGatewayConsumer) HandleConfigChanged(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_config.ConfigChangedEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 ConfigChangedEvent 失败", "err", err)
        return nil // 反序列化失败不重试，记录日志人工排查
    }

    // ==================== 2. 幂等检查 ====================
    // Key: gw:kafka:dedup:{event_id}  STRING  TTL: 24h
    dedupKey := fmt.Sprintf("gw:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        log.Debug("重复的 config.changed 事件，跳过",
            "event_id", event.Header.EventId, "config_key", event.ConfigKey)
        return nil
    }

    // ==================== 3. 过滤非网关配置 ====================
    // config_key 格式: "{service}.{key}"
    // 仅处理 service=apigateway 的配置变更
    parts := strings.SplitN(event.ConfigKey, ".", 2)
    if len(parts) != 2 {
        log.Warn("config_key 格式不合法，跳过", "config_key", event.ConfigKey)
        return nil
    }
    service := parts[0]
    key := parts[1]

    if service != "apigateway" {
        // 非网关配置，忽略（其他服务的配置变更由对应服务自行消费）
        return nil
    }

    log.Info("收到网关配置变更事件",
        "config_key", event.ConfigKey, "action", event.Action,
        "change_time", event.ChangeTime)

    // ==================== 4. 根据 key 前缀路由到不同处理逻辑 ====================
    switch {
    case strings.HasPrefix(key, "rate_limit"):
        return c.handleRateLimitConfigChanged(ctx, key, event.NewValue, event.Action)
    case strings.HasPrefix(key, "route"):
        return c.handleRouteConfigChanged(ctx, key, event.NewValue, event.Action)
    case key == "maintenance":
        return c.handleMaintenanceModeChanged(ctx, event.NewValue, event.Action)
    case strings.HasPrefix(key, "version"):
        return c.handleVersionConfigChanged(ctx, key, event.NewValue, event.Action)
    case key == "server_time_offset":
        return c.handleServerTimeOffsetChanged(ctx, event.NewValue)
    default:
        log.Debug("未识别的网关配置 key，跳过", "key", key)
        return nil
    }
}

// ==================== 4a. 限流配置变更处理 ====================
// config_key 示例:
//   apigateway.rate_limit.ip_global     → IP 级全局限流阈值
//   apigateway.rate_limit.default       → 默认 per-API 限流阈值
//   apigateway.rate_limit.message.send  → 特定 API 限流阈值
func (c *ApiGatewayConsumer) handleRateLimitConfigChanged(ctx context.Context, key, newValue, action string) error {
    // 删除操作 → 恢复默认值
    if action == "delete" {
        log.Info("限流配置被删除，恢复默认值", "key", key)
        if key == "rate_limit.ip_global" {
            c.redis.Del(ctx, "gw:ip_rate_limit_config")
        } else {
            // 从 HASH 中删除对应字段
            apiPath := strings.TrimPrefix(key, "rate_limit.")
            c.redis.HDel(ctx, "gw:rate_limit_config", apiPath)
        }
        return nil
    }

    // 解析新值
    if newValue == "" {
        log.Warn("限流配置新值为空，跳过", "key", key)
        return nil
    }

    if key == "rate_limit.ip_global" {
        // IP 级全局限流
        // Key: gw:ip_rate_limit_config  STRING  无 TTL
        // value 示例: "600" （每分钟 600 次）
        c.redis.Set(ctx, "gw:ip_rate_limit_config", newValue, 0)
        log.Info("IP 级全局限流配置已更新", "limit", newValue)
    } else {
        // per-API 限流
        // Key: gw:rate_limit_config  HASH  field=api_path, value=limit_per_min
        // key 示例: rate_limit.message.send → api_path = message.send
        apiPath := strings.TrimPrefix(key, "rate_limit.")
        if apiPath == "default" {
            apiPath = "__default__"
        }
        c.redis.HSet(ctx, "gw:rate_limit_config", apiPath, newValue)
        log.Info("API 限流配置已更新", "api_path", apiPath, "limit", newValue)
    }

    // 清空进程内限流配置缓存（如果有本地 LRU）
    if c.localCache != nil {
        c.localCache.Delete("rate_limit_config")
    }

    return nil
}

// ==================== 4b. 路由配置变更处理 ====================
// config_key 示例:
//   apigateway.route.batch_update → 批量更新路由表
//   apigateway.route.single       → 单条路由更新
func (c *ApiGatewayConsumer) handleRouteConfigChanged(ctx context.Context, key, newValue, action string) error {
    if action == "delete" {
        // 路由删除 → 清除路由缓存，让网关重新从 Config 服务拉取
        c.redis.Del(ctx, "gw:route_cache")
        log.Info("路由缓存已清除（配置删除触发）")
        return nil
    }

    if newValue == "" {
        log.Warn("路由配置新值为空，跳过", "key", key)
        return nil
    }

    // 解析路由配置 JSON
    // value 格式: {"routes": [{"path": "/v1/message/send", "service": "message"}, ...]}
    var routeConfig struct {
        Routes []struct {
            Path    string `json:"path"`
            Service string `json:"service"`
        } `json:"routes"`
    }
    if err := json.Unmarshal([]byte(newValue), &routeConfig); err != nil {
        log.Error("解析路由配置 JSON 失败", "key", key, "err", err)
        return nil // 不重试，等待运维修复配置
    }

    if len(routeConfig.Routes) == 0 {
        log.Warn("路由配置为空，跳过", "key", key)
        return nil
    }

    // 批量写入路由缓存
    // Key: gw:route_cache  HASH  field=path, value=service_name  TTL: 5min
    pipe := c.redis.Pipeline()
    routeFields := make(map[string]interface{})
    for _, route := range routeConfig.Routes {
        if route.Path != "" && route.Service != "" {
            routeFields[route.Path] = route.Service
        }
    }

    if len(routeFields) > 0 {
        pipe.HSet(ctx, "gw:route_cache", routeFields)
        pipe.Expire(ctx, "gw:route_cache", 5*time.Minute)
        if _, err := pipe.Exec(ctx); err != nil {
            log.Error("写入路由缓存失败", "err", err)
            return fmt.Errorf("写入路由缓存失败: %w", err) // 返回错误触发重试
        }
    }

    log.Info("路由缓存已更新", "route_count", len(routeFields))
    return nil
}

// ==================== 4c. 维护模式配置变更处理 ====================
// config_key: apigateway.maintenance
// value 格式: {"enabled": true, "message": "系统维护中...", "estimated_end": 1709913600000}
func (c *ApiGatewayConsumer) handleMaintenanceModeChanged(ctx context.Context, newValue, action string) error {
    if action == "delete" {
        // 删除维护模式配置 → 关闭维护模式
        c.redis.Del(ctx, "gw:maintenance_mode")
        log.Info("维护模式已关闭（配置删除触发）")
        return nil
    }

    if newValue == "" {
        // 空值也视为关闭维护模式
        c.redis.Del(ctx, "gw:maintenance_mode")
        log.Info("维护模式已关闭（空值）")
        return nil
    }

    // 校验 JSON 格式
    var maintenance struct {
        Enabled      bool   `json:"enabled"`
        Message      string `json:"message"`
        EstimatedEnd int64  `json:"estimated_end"`
    }
    if err := json.Unmarshal([]byte(newValue), &maintenance); err != nil {
        log.Error("解析维护模式配置 JSON 失败", "err", err)
        return nil
    }

    // Key: gw:maintenance_mode  STRING  JSON  无 TTL
    c.redis.Set(ctx, "gw:maintenance_mode", newValue, 0)

    if maintenance.Enabled {
        log.Warn("⚠️ 系统维护模式已开启",
            "message", maintenance.Message,
            "estimated_end", maintenance.EstimatedEnd)
    } else {
        log.Info("系统维护模式已关闭")
    }

    return nil
}

// ==================== 4d. 版本信息配置变更处理 ====================
// config_key 示例:
//   apigateway.version.ios      → iOS 版本信息
//   apigateway.version.android  → Android 版本信息
//   apigateway.version.web      → Web 版本信息
//
// value 格式 JSON:
// {
//   "min_version": "1.0.0",
//   "latest_version": "2.1.0",
//   "update_url": "https://example.com/download/ios",
//   "force_update": "false",
//   "release_notes": "1. 修复已知问题\n2. 性能优化",
//   "package_size": "52428800",
//   "md5": "abc123..."
// }
func (c *ApiGatewayConsumer) handleVersionConfigChanged(ctx context.Context, key, newValue, action string) error {
    // 解析平台名称
    // key 示例: version.ios → platform = ios
    platformName := strings.TrimPrefix(key, "version.")
    if platformName == "" || platformName == key {
        log.Warn("无法从 key 解析平台名称", "key", key)
        return nil
    }

    // 平台名称映射到 PlatformType 枚举值
    platformMap := map[string]int{
        "ios":          int(common.PLATFORM_TYPE_IOS),
        "android":      int(common.PLATFORM_TYPE_ANDROID),
        "windows":      int(common.PLATFORM_TYPE_WINDOWS),
        "macos":        int(common.PLATFORM_TYPE_MACOS),
        "linux":        int(common.PLATFORM_TYPE_LINUX),
        "web":          int(common.PLATFORM_TYPE_WEB),
        "ipad":         int(common.PLATFORM_TYPE_IPAD),
        "android_pad":  int(common.PLATFORM_TYPE_ANDROID_PAD),
        "mini_program": int(common.PLATFORM_TYPE_MINI_PROGRAM),
    }

    platformInt, ok := platformMap[platformName]
    if !ok {
        log.Warn("不支持的平台名称", "platform", platformName)
        return nil
    }

    versionKey := fmt.Sprintf("gw:app_version:%d", platformInt)

    if action == "delete" {
        // 删除版本信息
        c.redis.Del(ctx, versionKey)
        log.Info("平台版本信息已删除", "platform", platformName)
        return nil
    }

    if newValue == "" {
        log.Warn("版本配置新值为空，跳过", "platform", platformName)
        return nil
    }

    // 解析版本信息 JSON
    var versionInfo map[string]string
    if err := json.Unmarshal([]byte(newValue), &versionInfo); err != nil {
        log.Error("解析版本信息 JSON 失败",
            "platform", platformName, "err", err)
        return nil
    }

    // 校验必要字段
    if versionInfo["min_version"] == "" || versionInfo["latest_version"] == "" {
        log.Error("版本信息缺少必要字段 min_version/latest_version",
            "platform", platformName)
        return nil
    }

    // 写入 Redis HASH
    // Key: gw:app_version:{platform}  HASH  无 TTL
    fields := make(map[string]interface{})
    for k, v := range versionInfo {
        fields[k] = v
    }

    pipe := c.redis.Pipeline()
    pipe.Del(ctx, versionKey) // 先清除旧数据
    pipe.HSet(ctx, versionKey, fields)
    if _, err := pipe.Exec(ctx); err != nil {
        log.Error("写入版本信息缓存失败",
            "platform", platformName, "err", err)
        return fmt.Errorf("写入版本信息缓存失败: %w", err)
    }

    log.Info("平台版本信息已更新",
        "platform", platformName,
        "latest_version", versionInfo["latest_version"],
        "min_version", versionInfo["min_version"],
        "force_update", versionInfo["force_update"])

    return nil
}

// ==================== 4e. 服务器时间偏移变更处理 ====================
// config_key: apigateway.server_time_offset
// value: "123" （毫秒偏移量，NTP 校准结果）
func (c *ApiGatewayConsumer) handleServerTimeOffsetChanged(ctx context.Context, newValue string) error {
    if newValue == "" {
        newValue = "0"
    }

    // 校验是否为合法整数
    offset, err := strconv.ParseInt(newValue, 10, 64)
    if err != nil {
        log.Error("server_time_offset 值不合法", "value", newValue, "err", err)
        return nil
    }

    // Key: gw:server_time_offset  STRING  无 TTL
    c.redis.Set(ctx, "gw:server_time_offset", newValue, 0)

    log.Info("服务器时间偏移量已更新", "offset_ms", offset)
    return nil
}
```

---

### Consumer: `config.flag.changed`（功能开关变更 — A/B 测试路由）

> 来源：Config 服务。当管理后台修改了功能开关（Feature Flag），Config 服务投递 `config.flag.changed` 事件。  
> 网关消费后更新本地功能开关缓存，用于：
> 1. **A/B 测试路由**：根据功能开关将部分用户路由到新版本服务
> 2. **功能门控**：某些 API 仅在功能开关开启时可用
> 3. **灰度发布**：按百分比逐步放量

```go
func (c *ApiGatewayConsumer) HandleConfigFlagChanged(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_config.ConfigChangedEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 ConfigChangedEvent（flag）失败", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 ====================
    dedupKey := fmt.Sprintf("gw:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        log.Debug("重复的 config.flag.changed 事件，跳过",
            "event_id", event.Header.EventId)
        return nil
    }

    // ==================== 3. 解析 flag_name ====================
    // config_key 格式: "feature_flag.{flag_name}"
    parts := strings.SplitN(event.ConfigKey, ".", 2)
    if len(parts) != 2 || parts[0] != "feature_flag" {
        log.Warn("config.flag.changed: config_key 格式不合法",
            "config_key", event.ConfigKey)
        return nil
    }
    flagName := parts[1]

    log.Info("收到功能开关变更事件",
        "flag_name", flagName, "action", event.Action,
        "change_time", event.ChangeTime)

    // ==================== 4. 处理删除操作 ====================
    if event.Action == "delete" {
        // 从功能开关缓存中删除
        // Key: gw:feature_flags  HASH  field=flag_name
        c.redis.HDel(ctx, "gw:feature_flags", flagName)
        log.Info("功能开关已删除", "flag_name", flagName)

        // 清空进程内缓存
        if c.localCache != nil {
            c.localCache.Delete(fmt.Sprintf("flag:%s", flagName))
        }
        return nil
    }

    // ==================== 5. 解析功能开关新值 ====================
    if event.NewValue == "" {
        log.Warn("功能开关新值为空，跳过", "flag_name", flagName)
        return nil
    }

    // value 格式 JSON:
    // {
    //   "is_enabled": true,
    //   "rollout_percentage": 50,
    //   "target_users": ["user_001", "user_002"],
    //   "description": "新消息列表 UI"
    // }
    var flagInfo struct {
        IsEnabled         bool     `json:"is_enabled"`
        RolloutPercentage int      `json:"rollout_percentage"`
        TargetUsers       []string `json:"target_users"`
        Description       string   `json:"description"`
    }
    if err := json.Unmarshal([]byte(event.NewValue), &flagInfo); err != nil {
        log.Error("解析功能开关 JSON 失败",
            "flag_name", flagName, "err", err)
        return nil
    }

    // ==================== 6. 写入 Redis 功能开关缓存 ====================
    // Key: gw:feature_flags  HASH  field=flag_name, value=JSON  无 TTL
    c.redis.HSet(ctx, "gw:feature_flags", flagName, event.NewValue)

    // ==================== 7. 更新路由决策缓存（如果是路由相关的 flag） ====================
    // 约定：以 "route_" 开头的功能开关会影响请求路由
    // 例如 "route_new_message_service" → 将消息相关请求路由到新版本服务
    if strings.HasPrefix(flagName, "route_") {
        c.updateRouteDecisionCache(ctx, flagName, flagInfo.IsEnabled, flagInfo.RolloutPercentage)
    }

    // ==================== 8. 清空进程内缓存 ====================
    if c.localCache != nil {
        c.localCache.Delete(fmt.Sprintf("flag:%s", flagName))
    }

    log.Info("功能开关已更新",
        "flag_name", flagName,
        "is_enabled", flagInfo.IsEnabled,
        "rollout_percentage", flagInfo.RolloutPercentage,
        "target_users_count", len(flagInfo.TargetUsers))

    return nil
}

// updateRouteDecisionCache 更新路由决策缓存
// 功能开关以 "route_" 开头时，影响请求路由决策
// 例如: route_new_message_service 开启后，部分用户的消息请求路由到 message-v2 服务
func (c *ApiGatewayConsumer) updateRouteDecisionCache(ctx context.Context, flagName string, enabled bool, rolloutPct int) {
    // 从 flag_name 推断受影响的服务
    // route_new_message_service → message
    // route_new_group_service   → group
    serviceName := strings.TrimPrefix(flagName, "route_new_")
    serviceName = strings.TrimSuffix(serviceName, "_service")

    if !enabled || rolloutPct == 0 {
        // 功能关闭或灰度为 0%，删除路由决策缓存（恢复默认路由）
        c.redis.HDel(ctx, "gw:route_decision", serviceName)
        log.Info("路由决策已恢复默认", "service", serviceName)
        return
    }

    // 写入路由决策
    // Key: gw:route_decision  HASH  field=service_name, value=JSON
    decision := map[string]interface{}{
        "enabled":            enabled,
        "rollout_percentage": rolloutPct,
        "new_service":        fmt.Sprintf("%s-v2", serviceName),
    }
    decisionJSON, _ := json.Marshal(decision)
    c.redis.HSet(ctx, "gw:route_decision", serviceName, string(decisionJSON))

    log.Info("路由决策已更新",
        "service", serviceName,
        "rollout_percentage", rolloutPct)
}
```

### Consumer: `auth.token.revoked`（Token 撤销 — 更新黑名单缓存）

> 来源：Auth 服务。管理员撤销用户 Token 或用户修改密码后触发。  
> 职责：将被撤销的 Token 加入 Redis 黑名单 SET，AuthMiddleware 校验 Token 时会检查此黑名单。  
> 缓存设计：Key `gw:token_blacklist` 使用 SET 存储被撤销的 Token ID，TTL 与 Token 最大有效期一致（通常 7 天）。

```go
func (c *ApiGatewayConsumer) HandleAuthTokenRevoked(ctx context.Context, msg *kafka.Message) error {
    var event kafka_auth.TokenRevokedEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 TokenRevokedEvent 失败", "err", err)
        return nil
    }

    dedupKey := fmt.Sprintf("gw:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    if event.TokenId == "" {
        log.Error("auth.token.revoked: token_id 为空")
        return nil
    }

    // 将 Token ID 加入黑名单 SET
    // TTL 使用 Token 剩余有效期或默认 7 天
    blacklistKey := "gw:token_blacklist"
    c.redis.SAdd(ctx, blacklistKey, event.TokenId)
    c.redis.Expire(ctx, blacklistKey, 7*24*time.Hour)

    // 同时在本地 LRU 缓存中标记（减少 Redis 访问）
    c.localCache.Set(fmt.Sprintf("token_revoked:%s", event.TokenId), true, 7*24*time.Hour)

    log.Info("Token 已加入黑名单",
        "token_id", event.TokenId, "user_id", event.UserId, "reason", event.Reason)
    return nil
}
```

### Consumer: `user.banned`（账号封禁 — 更新封禁名单缓存）

> 来源：User 服务。管理员封禁用户账号后触发。  
> 职责：将被封禁的用户 ID 加入 Redis 封禁名单，AuthMiddleware 鉴权时会检查此名单。  
> 如果用户已解封（is_banned = false），则从封禁名单中移除。

```go
func (c *ApiGatewayConsumer) HandleUserBanned(ctx context.Context, msg *kafka.Message) error {
    var event kafka_user.UserBannedEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 UserBannedEvent 失败", "err", err)
        return nil
    }

    dedupKey := fmt.Sprintf("gw:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    if event.UserId == "" {
        log.Error("user.banned: user_id 为空")
        return nil
    }

    bannedKey := "gw:banned_users"
    if event.IsBanned {
        // 加入封禁名单
        c.redis.SAdd(ctx, bannedKey, event.UserId)
        // 同时设置单用户封禁详情（用于返回具体封禁原因）
        detailKey := fmt.Sprintf("gw:ban_detail:%s", event.UserId)
        c.redis.HSet(ctx, detailKey, map[string]interface{}{
            "reason":    event.Reason,
            "ban_until": event.BanUntil,
            "ban_time":  event.BanTime,
        })
        if event.BanUntil > 0 {
            // 有解封时间，设置自动过期
            ttl := time.Until(time.UnixMilli(event.BanUntil))
            if ttl > 0 {
                c.redis.Expire(ctx, detailKey, ttl)
            }
        }
        c.localCache.Set(fmt.Sprintf("banned:%s", event.UserId), true, 1*time.Hour)
        log.Info("用户已加入封禁名单", "user_id", event.UserId, "reason", event.Reason)
    } else {
        // 解除封禁
        c.redis.SRem(ctx, bannedKey, event.UserId)
        c.redis.Del(ctx, fmt.Sprintf("gw:ban_detail:%s", event.UserId))
        c.localCache.Delete(fmt.Sprintf("banned:%s", event.UserId))
        log.Info("用户已从封禁名单移除", "user_id", event.UserId)
    }

    return nil
}
```

```go
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

---

## 中间件链中的配置消费联动

中间件在运行时动态读取消费者刷新的 Redis 配置，实现热更新：

| 中间件 | 读取的 Redis Key | 消费者写入时机 |
|--------|-----------------|---------------|
| RateLimit | `gw:rate_limit_config`（per-API 阈值） | `config.changed` → `apigateway.rate_limit.*` |
| RateLimit | `gw:ip_rate_limit_config`（IP 级阈值） | `config.changed` → `apigateway.rate_limit.ip_global` |
| RateLimit | `gw:maintenance_mode`（维护模式） | `config.changed` → `apigateway.maintenance` |
| Route | `gw:route_cache`（路由映射） | `config.changed` → `apigateway.route.*` |
| Route | `gw:route_decision`（A/B 路由决策） | `config.flag.changed` → `route_*` 功能开关 |
| Handler | `gw:feature_flags`（功能门控） | `config.flag.changed` → 所有功能开关 |

**热更新流程：**
```
管理后台修改配置
      │
      ▼
Config 服务写入 PgSQL + 投递 Kafka
      │
      ▼
┌─────────────────────────────────────────────┐
│ config.changed / config.flag.changed Topic  │
└──────────────────┬──────────────────────────┘
                   │
                   ▼
         ApiGateway Consumer
         解析 config_key → 路由到对应 handler
         更新 Redis 配置缓存
                   │
                   ▼
         中间件下次请求读取新配置
         ✅ 热更新完成（秒级生效）
```
