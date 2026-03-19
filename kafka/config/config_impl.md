# Config 配置中心 — Kafka 消费者实现伪代码

## 概述

Config 服务作为配置管理中心，主要角色是 Kafka **生产者**（广播配置变更事件）。
同时也消费少量 Topic 用于：审计日志记录、多实例部署时的本地缓存失效。

## 生产 Topic 列表

| Topic | 事件 | 生产时机 | Key | 消费方 |
|-------|------|----------|-----|--------|
| `config.changed` | ConfigChangedEvent | SetConfig / DeleteConfig 成功 | service | **所有服务**（热更新运行时配置） |
| `config.flag.changed` | ConfigChangedEvent | SetFeatureFlag 成功 | flag_name | **所有服务**（热更新功能开关） |

## 消费 Topic 列表

| Topic | 来源 | Consumer Group | 用途 |
|-------|------|----------------|------|
| `audit.config.change` | Audit 服务 | config-audit-consumer | 审计回调确认：Audit 服务处理完配置变更审计后回调，Config 可选记录审计状态 |
| `config.changed` | 自身（Config 服务） | config-self-invalidate | 多实例部署时，其他实例写入的变更通过 Kafka 通知本实例失效本地缓存 |

## Redis Key 设计（消费者专用）

| Key 格式 | 类型 | 值 | TTL | 说明 |
|----------|------|-----|-----|------|
| `config:kafka:dedup:{event_id}` | STRING | "1" | 24h | Kafka 消费幂等去重 |

---

## 标准配置键约定

### 命名规范

所有配置键采用 `{service}.{config_name}` 格式，服务名对应 `configs` 表的 `service` 字段，配置名使用 `snake_case`。

### 全局标准配置键清单

以下配置键在系统初始化时预置，所有服务启动时通过 `GetServiceConfigs` 拉取对应配置。

#### User 用户服务

| 配置键 | service | key | value_type | 默认值 | 说明 |
|--------|---------|-----|------------|--------|------|
| `user.max_friend_count` | user | max_friend_count | int | 5000 | 单个用户最大好友数上限 |
| `user.max_group_count` | user | max_group_count | int | 500 | 单个用户最大加群数上限 |

#### Group 群组服务

| 配置键 | service | key | value_type | 默认值 | 说明 |
|--------|---------|-----|------------|--------|------|
| `group.max_members` | group | max_members | int | 500 | 单个群最大成员数 |
| `group.max_admin_count` | group | max_admin_count | int | 10 | 单个群最大管理员数（不含群主） |

#### Message 消息服务

| 配置键 | service | key | value_type | 默认值 | 说明 |
|--------|---------|-----|------------|--------|------|
| `message.max_text_length` | message | max_text_length | int | 5000 | 单条文本消息最大字符数 |
| `message.recall_time_limit` | message | recall_time_limit | int | 120 | 消息撤回时间限制（秒），默认 2 分钟 |
| `message.max_forward_count` | message | max_forward_count | int | 50 | 单次合并转发最大消息数 |

#### Push 推送服务

| 配置键 | service | key | value_type | 默认值 | 说明 |
|--------|---------|-----|------------|--------|------|
| `push.offline_push_enabled` | push | offline_push_enabled | bool | true | 是否启用离线推送 |
| `push.apns_topic` | push | apns_topic | string | "com.chat.app" | APNs 推送 Topic（iOS Bundle ID） |
| `push.fcm_project_id` | push | fcm_project_id | string | "" | Firebase Cloud Messaging 项目 ID |

#### Presence 在线状态服务

| 配置键 | service | key | value_type | 默认值 | 说明 |
|--------|---------|-----|------------|--------|------|
| `presence.heartbeat_interval` | presence | heartbeat_interval | int | 30 | 心跳间隔（秒） |
| `presence.offline_grace_period` | presence | offline_grace_period | int | 60 | 离线宽限期（秒），超过此时间未心跳标记为离线 |

#### Media 媒体服务

| 配置键 | service | key | value_type | 默认值 | 说明 |
|--------|---------|-----|------------|--------|------|
| `media.max_file_size` | media | max_file_size | int | 104857600 | 单文件最大大小（字节），默认 100MB |
| `media.allowed_types` | media | allowed_types | json | `["image/*","video/*","audio/*","application/pdf"]` | 允许的 MIME 类型列表 |
| `media.thumbnail_size` | media | thumbnail_size | json | `{"width":200,"height":200}` | 缩略图默认尺寸 |

#### Search 搜索服务

| 配置键 | service | key | value_type | 默认值 | 说明 |
|--------|---------|-----|------------|--------|------|
| `search.max_results` | search | max_results | int | 100 | 单次搜索最大返回条数 |
| `search.fuzzy_threshold` | search | fuzzy_threshold | float | 0.7 | 模糊搜索相似度阈值（0-1） |

#### Auth 认证服务

| 配置键 | service | key | value_type | 默认值 | 说明 |
|--------|---------|-----|------------|--------|------|
| `auth.access_token_ttl` | auth | access_token_ttl | int | 7200 | Access Token 有效期（秒），默认 2 小时 |
| `auth.refresh_token_ttl` | auth | refresh_token_ttl | int | 604800 | Refresh Token 有效期（秒），默认 7 天 |
| `auth.max_login_devices` | auth | max_login_devices | int | 3 | 同一平台最大登录设备数 |

#### Session 会话服务

| 配置键 | service | key | value_type | 默认值 | 说明 |
|--------|---------|-----|------------|--------|------|
| `session.max_devices_per_platform` | session | max_devices_per_platform | int | 1 | 每个平台最大同时在线设备数 |

#### OfflineQueue 离线消息队列服务

| 配置键 | service | key | value_type | 默认值 | 说明 |
|--------|---------|-----|------------|--------|------|
| `offline_queue.max_queue_size` | offline_queue | max_queue_size | int | 10000 | 单用户离线消息队列最大长度 |
| `offline_queue.signal_ttl` | offline_queue | signal_ttl | int | 604800 | 离线信令/通知保留时长（秒），默认 7 天 |

---

## 消费者实现

### Consumer: `config.changed`（自消费 — 多实例缓存失效）

> 来源：自身（Config 服务的其他实例）。  
> 场景：Config 服务部署多个实例，实例 A 通过 SetConfig 修改配置后会失效本实例 Redis 缓存并投递 Kafka。  
> 实例 B、C 消费此事件，失效各自本地缓存（如果有进程内缓存）以及补充 Redis 缓存失效。  
> 这是一个**防御性机制**，即使 Redis 缓存已被写入实例失效，其他实例也会确认一次。

```go
func (c *ConfigConsumer) HandleConfigChanged(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_config.ConfigChangedEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("unmarshal ConfigChangedEvent failed", "err", err)
        return nil // 反序列化失败不重试
    }

    // ==================== 2. 幂等检查 ====================
    dedupKey := fmt.Sprintf("config:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        log.Debug("duplicate config.changed event", "event_id", event.Header.EventId)
        return nil
    }

    // ==================== 3. 跳过本实例产生的事件 ====================
    // 本实例写入时已经失效了缓存，无需重复处理
    if event.Header.SourceId == c.instanceID {
        log.Debug("skip self-produced config.changed event",
            "event_id", event.Header.EventId, "config_key", event.ConfigKey)
        return nil
    }

    log.Info("received config.changed from other instance",
        "config_key", event.ConfigKey, "source_id", event.Header.SourceId,
        "change_time", event.ChangeTime)

    // ==================== 4. 解析 config_key → service + key ====================
    // config_key 格式: "{service}.{key}" 或 "feature_flag.{flag_name}"
    parts := strings.SplitN(event.ConfigKey, ".", 2)
    if len(parts) != 2 {
        log.Error("invalid config_key format", "config_key", event.ConfigKey)
        return nil
    }
    service := parts[0]
    key := parts[1]

    // ==================== 5. 失效 Redis 缓存（防御性二次失效） ====================
    pipe := c.redis.Pipeline()

    if service == "feature_flag" {
        // 功能开关变更
        pipe.Del(ctx, fmt.Sprintf("config:flags:%s", key))
        pipe.Del(ctx, "config:flags_all")
    } else {
        // 普通配置变更
        pipe.Del(ctx, fmt.Sprintf("config:%s:%s", service, key))
        pipe.Del(ctx, fmt.Sprintf("config:service_all:%s", service))
        pipe.Del(ctx, fmt.Sprintf("config:version:%s:%s", service, key))
    }

    if _, err := pipe.Exec(ctx); err != nil {
        log.Warn("defensive cache invalidation failed",
            "config_key", event.ConfigKey, "err", err)
        // 不重试，Redis 缓存有 TTL 兜底
    }

    // ==================== 6. 失效本地进程内缓存（如果有） ====================
    // 如果 Config 服务有进程内 LRU 缓存（如 bigcache / freecache），在此清除
    if c.localCache != nil {
        if service == "feature_flag" {
            c.localCache.Delete(fmt.Sprintf("flag:%s", key))
        } else {
            c.localCache.Delete(fmt.Sprintf("config:%s:%s", service, key))
        }
    }

    log.Info("config cache invalidated by peer instance",
        "config_key", event.ConfigKey, "source_id", event.Header.SourceId)
    return nil
}
```

### Consumer: `audit.config.change`（审计回调确认）

> 来源：Audit 审计服务。Audit 消费 `config.changed` 事件后完成审计日志记录，回调此事件确认审计完成。  
> Config 服务可选消费此事件，用于标记配置变更的审计状态（可选实现，非核心路径）。

```go
func (c *ConfigConsumer) HandleAuditConfigChange(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event struct {
        Header    *common.EventHeader `protobuf:"bytes,1,opt,name=header"`
        ConfigKey string              `protobuf:"bytes,2,opt,name=config_key"`
        AuditID   string              `protobuf:"bytes,3,opt,name=audit_id"`
        Status    string              `protobuf:"bytes,4,opt,name=status"` // "recorded" / "flagged"
        AuditTime int64               `protobuf:"varint,5,opt,name=audit_time"`
        Details   string              `protobuf:"bytes,6,opt,name=details"`
    }
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("unmarshal audit.config.change event failed", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 ====================
    dedupKey := fmt.Sprintf("config:kafka:dedup:%s", event.Header.EventId)
    if set, _ := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    // ==================== 3. 参数校验 ====================
    if event.ConfigKey == "" || event.AuditID == "" {
        log.Error("audit.config.change: missing required fields",
            "config_key", event.ConfigKey, "audit_id", event.AuditID)
        return nil
    }

    log.Info("received audit confirmation for config change",
        "config_key", event.ConfigKey, "audit_id", event.AuditID,
        "status", event.Status, "audit_time", event.AuditTime)

    // ==================== 4. 处理审计回调 ====================
    switch event.Status {
    case "recorded":
        // 审计已记录 → 正常日志，无需额外处理
        log.Info("config change audit recorded",
            "config_key", event.ConfigKey, "audit_id", event.AuditID)

    case "flagged":
        // 审计标记异常（例如高风险变更、敏感配置修改）→ 触发告警
        log.Warn("config change flagged by audit",
            "config_key", event.ConfigKey, "audit_id", event.AuditID,
            "details", event.Details)

        // 可选：将异常配置变更推送到告警系统
        // alertManager.Send(ctx, &Alert{
        //     Level:   "warning",
        //     Title:   "配置变更审计异常",
        //     Content: fmt.Sprintf("配置 %s 变更被审计标记: %s", event.ConfigKey, event.Details),
        // })

    default:
        log.Warn("unknown audit status for config change",
            "config_key", event.ConfigKey, "status", event.Status)
    }

    return nil
}
```

---

## 配置初始化脚本（系统首次部署执行）

> 以下 SQL 用于系统首次部署时预置所有标准配置项，确保各服务启动时有默认配置可用。

```sql
-- ===================== 标准配置项初始化 =====================

-- User 用户服务
INSERT INTO configs (service, key, value, description, value_type, version, updated_by)
VALUES
    ('user', 'max_friend_count', '"5000"', '单个用户最大好友数上限', 'int', 1, 'system'),
    ('user', 'max_group_count', '"500"', '单个用户最大加群数上限', 'int', 1, 'system')
ON CONFLICT (service, key) DO NOTHING;

-- Group 群组服务
INSERT INTO configs (service, key, value, description, value_type, version, updated_by)
VALUES
    ('group', 'max_members', '"500"', '单个群最大成员数', 'int', 1, 'system'),
    ('group', 'max_admin_count', '"10"', '单个群最大管理员数（不含群主）', 'int', 1, 'system')
ON CONFLICT (service, key) DO NOTHING;

-- Message 消息服务
INSERT INTO configs (service, key, value, description, value_type, version, updated_by)
VALUES
    ('message', 'max_text_length', '"5000"', '单条文本消息最大字符数', 'int', 1, 'system'),
    ('message', 'recall_time_limit', '"120"', '消息撤回时间限制（秒）', 'int', 1, 'system'),
    ('message', 'max_forward_count', '"50"', '单次合并转发最大消息数', 'int', 1, 'system')
ON CONFLICT (service, key) DO NOTHING;

-- Push 推送服务
INSERT INTO configs (service, key, value, description, value_type, version, updated_by)
VALUES
    ('push', 'offline_push_enabled', '"true"', '是否启用离线推送', 'bool', 1, 'system'),
    ('push', 'apns_topic', '"com.chat.app"', 'APNs 推送 Topic（iOS Bundle ID）', 'string', 1, 'system'),
    ('push', 'fcm_project_id', '""', 'Firebase Cloud Messaging 项目 ID', 'string', 1, 'system')
ON CONFLICT (service, key) DO NOTHING;

-- Presence 在线状态服务
INSERT INTO configs (service, key, value, description, value_type, version, updated_by)
VALUES
    ('presence', 'heartbeat_interval', '"30"', '心跳间隔（秒）', 'int', 1, 'system'),
    ('presence', 'offline_grace_period', '"60"', '离线宽限期（秒）', 'int', 1, 'system')
ON CONFLICT (service, key) DO NOTHING;

-- Media 媒体服务
INSERT INTO configs (service, key, value, description, value_type, version, updated_by)
VALUES
    ('media', 'max_file_size', '"104857600"', '单文件最大大小（字节），100MB', 'int', 1, 'system'),
    ('media', 'allowed_types', '["image/*","video/*","audio/*","application/pdf"]', '允许的 MIME 类型列表', 'json', 1, 'system'),
    ('media', 'thumbnail_size', '{"width":200,"height":200}', '缩略图默认尺寸', 'json', 1, 'system')
ON CONFLICT (service, key) DO NOTHING;

-- Search 搜索服务
INSERT INTO configs (service, key, value, description, value_type, version, updated_by)
VALUES
    ('search', 'max_results', '"100"', '单次搜索最大返回条数', 'int', 1, 'system'),
    ('search', 'fuzzy_threshold', '"0.7"', '模糊搜索相似度阈值（0-1）', 'float', 1, 'system')
ON CONFLICT (service, key) DO NOTHING;

-- Auth 认证服务
INSERT INTO configs (service, key, value, description, value_type, version, updated_by)
VALUES
    ('auth', 'access_token_ttl', '"7200"', 'Access Token 有效期（秒），2小时', 'int', 1, 'system'),
    ('auth', 'refresh_token_ttl', '"604800"', 'Refresh Token 有效期（秒），7天', 'int', 1, 'system'),
    ('auth', 'max_login_devices', '"3"', '同一平台最大登录设备数', 'int', 1, 'system')
ON CONFLICT (service, key) DO NOTHING;

-- Session 会话服务
INSERT INTO configs (service, key, value, description, value_type, version, updated_by)
VALUES
    ('session', 'max_devices_per_platform', '"1"', '每个平台最大同时在线设备数', 'int', 1, 'system')
ON CONFLICT (service, key) DO NOTHING;

-- OfflineQueue 离线消息队列服务
INSERT INTO configs (service, key, value, description, value_type, version, updated_by)
VALUES
    ('offline_queue', 'max_queue_size', '"10000"', '单用户离线消息队列最大长度', 'int', 1, 'system'),
    ('offline_queue', 'signal_ttl', '"604800"', '离线信令/通知保留时长（秒），7天', 'int', 1, 'system')
ON CONFLICT (service, key) DO NOTHING;

-- ===================== 标准功能开关初始化 =====================

INSERT INTO feature_flags (flag_name, is_enabled, description, rollout_percentage, target_users, version, updated_by)
VALUES
    ('push.offline_push', true, '离线推送总开关', 100, '[]', 1, 'system'),
    ('message.content_moderation', false, '消息内容审核开关', 0, '[]', 1, 'system'),
    ('message.recall_enabled', true, '消息撤回功能开关', 100, '[]', 1, 'system'),
    ('group.invite_approval', false, '群邀请需审批开关', 0, '[]', 1, 'system'),
    ('user.stranger_message', true, '允许陌生人消息开关', 100, '[]', 1, 'system'),
    ('search.full_text_search', true, '全文搜索功能开关', 100, '[]', 1, 'system'),
    ('media.video_transcoding', true, '视频转码功能开关', 100, '[]', 1, 'system'),
    ('presence.show_online_status', true, '显示在线状态开关', 100, '[]', 1, 'system')
ON CONFLICT (flag_name) DO NOTHING;
```

---

## 各服务消费 `config.changed` 的标准模式

> 以下为所有业务服务消费 `config.changed` Topic 的通用实现模板。  
> 每个服务在启动时注册此 Consumer，监听自身关注的配置键变更并热更新。

```go
// ==================== 通用配置热更新消费者（所有服务复用） ====================

// ConfigHotReloader 配置热更新处理器
type ConfigHotReloader struct {
    serviceName string                           // 当前服务名称
    redis       *redis.Client                    // Redis 客户端
    configMap   sync.Map                         // 本地配置缓存 map[string]string
    handlers    map[string]func(key, value string) // 配置变更回调处理器
    instanceID  string                           // 当前实例 ID
}

func (r *ConfigHotReloader) HandleConfigChanged(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_config.ConfigChangedEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("unmarshal ConfigChangedEvent failed", "err", err)
        return nil
    }

    // ==================== 2. 幂等检查 ====================
    dedupKey := fmt.Sprintf("%s:kafka:dedup:%s", r.serviceName, event.Header.EventId)
    if set, _ := r.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result(); !set {
        return nil
    }

    // ==================== 3. 过滤：只处理本服务关注的配置 ====================
    isRelevant := false
    for _, affected := range event.AffectedServices {
        if affected == r.serviceName || affected == "*" {
            isRelevant = true
            break
        }
    }
    if !isRelevant {
        return nil // 与本服务无关，跳过
    }

    log.Info("hot-reload config change",
        "service", r.serviceName, "config_key", event.ConfigKey,
        "old_value", event.OldValue, "new_value", event.NewValue)

    // ==================== 4. 更新本地配置缓存 ====================
    r.configMap.Store(event.ConfigKey, event.NewValue)

    // ==================== 5. 调用注册的回调处理器 ====================
    if handler, ok := r.handlers[event.ConfigKey]; ok {
        handler(event.ConfigKey, event.NewValue)
    }

    return nil
}

// ==================== 使用示例（Message 服务） ====================

func initMessageConfigReloader(reloader *ConfigHotReloader, msgService *MessageService) {
    // 注册配置变更回调
    reloader.handlers["message.max_text_length"] = func(key, value string) {
        if v, err := strconv.Atoi(value); err == nil {
            msgService.config.MaxTextLength = v
            log.Info("hot-reloaded max_text_length", "new_value", v)
        }
    }
    reloader.handlers["message.recall_time_limit"] = func(key, value string) {
        if v, err := strconv.Atoi(value); err == nil {
            msgService.config.RecallTimeLimit = v
            log.Info("hot-reloaded recall_time_limit", "new_value", v)
        }
    }
    reloader.handlers["message.max_forward_count"] = func(key, value string) {
        if v, err := strconv.Atoi(value); err == nil {
            msgService.config.MaxForwardCount = v
            log.Info("hot-reloaded max_forward_count", "new_value", v)
        }
    }
}
```
