# Connecte 长连接网关服务 — Kafka 消费者实现伪代码

## 概述

Connecte 服务的 Kafka 消费者负责处理**外部触发的连接管理事件**以及**内部定时清理任务**。核心场景包括：Session 被踢时主动断开连接、用户被封禁时全量踢出、配置变更时动态调整心跳超时等参数。

**设计原则：**
- 所有消费者实现幂等（通过 Redis `conn:kafka:dedup:{event_id}` 去重）
- 连接操作最终一致性：Kafka 事件触发的连接清理作为 RPC 的兜底补偿
- 批量操作使用 Pipeline + goroutine 池，控制并发度
- 清理任务与网关心跳配合，自动清除过期连接

## 生产 Topic 列表

| Topic | 事件 | 生产时机 | Key | 消费方 |
|-------|------|----------|-----|--------|
| `conn.connected` | ConnectedEvent | 用户连接建立 | user_id | Session, Presence |
| `conn.disconnected` | DisconnectedEvent | 用户连接断开 | user_id | Session, Presence, OfflineQueue |
| `conn.route.updated` | ConnRouteUpdatedEvent | 连接路由变更（切换网关） | user_id | Push（更新路由缓存） |
| `conn.heartbeat.lost` | HeartbeatLostEvent | 心跳丢失告警 | user_id | 监控告警 |
| `conn.stats` | ConnStatsEvent | 定时统计上报 | gateway_id | Audit（运营统计） |

## 消费 Topic 列表

| Topic | 来源 | 用途 | 并发度 | 重试 |
|-------|------|------|--------|------|
| `session.kicked` | Session | 会话被踢 → 断开对应连接 | 4 | 3 |
| `user.banned` | User | 用户被封禁 → 断开所有连接 | 2 | 3 |
| `user.deactivated` | User | 用户注销/停用 → 断开所有连接 | 2 | 3 |
| `config.changed` | Config | 配置变更 → 热更新连接参数 | 1 | 1 |

## Redis Key 设计（消费者用到的）

| Key 格式 | 类型 | 值 | TTL | 说明 |
|----------|------|-----|-----|------|
| `conn:kafka:dedup:{event_id}` | STRING | "1" | 24h | Kafka 消费幂等去重 |
| `conn:user:{user_id}:{platform}` | HASH | 连接详情 | 5min | 用户指定平台连接信息（共享） |
| `conn:user_all:{user_id}` | SET | 连接索引 | 5min | 用户所有平台连接索引（共享） |
| `conn:gateway:{gateway_id}` | HASH | 网关状态 | 30s | 网关节点信息（共享） |
| `conn:gateway_conns:{gateway_id}` | SET | 连接集合 | 30s | 网关连接索引（共享） |
| `conn:gateway_list` | ZSET | gateway_id → conn_count | 无 TTL | 全局网关列表（共享） |
| `conn:config` | HASH | 动态配置项 | 无 TTL | 连接限制/心跳超时等运行时配置 |

---

## 消费者实现

### Consumer: `session.kicked`

> 来源：Session 服务。当同平台互踢、超设备数踢出、管理后台强制下线时，Session 服务产生 session.kicked 事件。  
> 职责：找到被踢用户在对应平台的连接，通过 Connecte RPC 执行 KickConnection 断开连接。  
> **关键场景**：用户在手机 A 登录后，又在手机 B 登录 → Session 踢掉手机 A 的会话 → 此 Consumer 断开手机 A 的长连接。

```go
func (c *ConnecteConsumer) HandleSessionKicked(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_session.SessionKickedEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 SessionKickedEvent 失败", "err", err)
        return nil // 反序列化失败不重试，进死信队列
    }

    // ==================== 2. 基本校验 ====================
    if event.UserId == "" || event.SessionId == "" {
        log.Error("SessionKickedEvent 关键字段缺失",
            "user_id", event.UserId, "session_id", event.SessionId)
        return nil
    }

    // ==================== 3. 幂等检查 — Redis ====================
    // Key: conn:kafka:dedup:{event_id}  TTL: 24h
    dedupKey := fmt.Sprintf("conn:kafka:dedup:%s", event.Header.EventId)
    set, err := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result()
    if err != nil {
        return fmt.Errorf("Redis 幂等检查失败: %w", err)
    }
    if !set {
        log.Debug("重复事件，跳过", "event_id", event.Header.EventId)
        return nil
    }

    // ==================== 4. 查找被踢用户在该平台的连接 ====================
    // 从事件中获取平台信息
    platform := event.Platform
    if platform == common.PLATFORM_TYPE_UNSPECIFIED {
        // 平台未指定，尝试从 device_id 反查
        log.Warn("SessionKickedEvent 平台未指定，尝试全平台查找",
            "user_id", event.UserId, "session_id", event.SessionId)
        // 全平台查找连接
        return c.kickAllConnectionsForUser(ctx, event.UserId, fmt.Sprintf("session_kicked:%s", event.Reason))
    }

    // 查询用户在指定平台的连接信息
    // Key: conn:user:{user_id}:{platform}  HASH
    connKey := fmt.Sprintf("conn:user:%s:%d", event.UserId, int(platform))
    connInfo, err := c.redis.HGetAll(ctx, connKey).Result()
    if err != nil {
        log.Error("查询用户连接信息失败",
            "user_id", event.UserId, "platform", platform, "err", err)
        // 删除幂等 key 以便重试
        c.redis.Del(ctx, dedupKey)
        return fmt.Errorf("查询连接信息失败: %w", err)
    }

    if len(connInfo) == 0 || connInfo["conn_id"] == "" {
        // 该平台无连接（可能已断开），正常返回
        log.Debug("session.kicked: 用户在该平台无连接，无需处理",
            "user_id", event.UserId, "platform", platform)
        return nil
    }

    // ==================== 5. 调用 Connecte RPC KickConnection 断开连接 ====================
    kickReason := fmt.Sprintf("session_kicked:%s", event.Reason)
    _, err = c.connecteService.KickConnection(ctx, &pb.KickConnectionRequest{
        UserId:       event.UserId,
        ConnectionId: connInfo["conn_id"],
        Reason:       kickReason,
    })
    if err != nil {
        log.Error("踢连接失败（session.kicked）",
            "user_id", event.UserId,
            "connection_id", connInfo["conn_id"],
            "platform", platform,
            "err", err)
        // 踢连接失败，删除幂等 key 触发重试
        c.redis.Del(ctx, dedupKey)
        return fmt.Errorf("KickConnection 失败: %w", err)
    }

    log.Info("session.kicked 处理完成：连接已断开",
        "user_id", event.UserId,
        "session_id", event.SessionId,
        "platform", platform,
        "connection_id", connInfo["conn_id"],
        "reason", event.Reason)

    return nil
}
```

### Consumer: `user.banned`

> 来源：User 服务。用户被管理后台封禁时触发。  
> 职责：断开被封禁用户的**所有平台**连接，确保用户立即无法使用系统。  
> 这是安全相关的关键路径，必须保证所有连接都被断开。

```go
func (c *ConnecteConsumer) HandleUserBanned(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_user.UserBannedEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 UserBannedEvent 失败", "err", err)
        return nil // 反序列化失败不重试
    }

    // ==================== 2. 基本校验 ====================
    if event.UserId == "" {
        log.Error("UserBannedEvent: user_id 缺失")
        return nil
    }

    // ==================== 3. 幂等检查 — Redis ====================
    dedupKey := fmt.Sprintf("conn:kafka:dedup:%s", event.Header.EventId)
    set, err := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result()
    if err != nil {
        return fmt.Errorf("Redis 幂等检查失败: %w", err)
    }
    if !set {
        log.Debug("重复事件，跳过", "event_id", event.Header.EventId)
        return nil
    }

    // ==================== 4. 断开用户所有连接 ====================
    err = c.kickAllConnectionsForUser(ctx, event.UserId, fmt.Sprintf("user_banned:%s", event.Reason))
    if err != nil {
        log.Error("封禁用户踢连接失败",
            "user_id", event.UserId, "err", err)
        // 安全关键路径，删除幂等 key 强制重试
        c.redis.Del(ctx, dedupKey)
        return fmt.Errorf("kickAllConnectionsForUser 失败: %w", err)
    }

    log.Info("user.banned 处理完成：所有连接已断开",
        "user_id", event.UserId,
        "reason", event.Reason,
        "ban_time", event.Header.Timestamp)

    return nil
}
```

### Consumer: `user.deactivated`

> 来源：User 服务。用户主动注销账号或被管理后台停用时触发。  
> 职责：断开用户的**所有平台**连接。与 user.banned 处理逻辑类似，但踢出原因不同。  
> 注销场景比封禁更彻底，用户数据可能随后被清理。

```go
func (c *ConnecteConsumer) HandleUserDeactivated(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_user.UserDeactivatedEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 UserDeactivatedEvent 失败", "err", err)
        return nil
    }

    // ==================== 2. 基本校验 ====================
    if event.UserId == "" {
        log.Error("UserDeactivatedEvent: user_id 缺失")
        return nil
    }

    // ==================== 3. 幂等检查 — Redis ====================
    dedupKey := fmt.Sprintf("conn:kafka:dedup:%s", event.Header.EventId)
    set, err := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result()
    if err != nil {
        return fmt.Errorf("Redis 幂等检查失败: %w", err)
    }
    if !set {
        log.Debug("重复事件，跳过", "event_id", event.Header.EventId)
        return nil
    }

    // ==================== 4. 断开用户所有连接 ====================
    err = c.kickAllConnectionsForUser(ctx, event.UserId, "user_deactivated")
    if err != nil {
        log.Error("停用用户踢连接失败",
            "user_id", event.UserId, "err", err)
        c.redis.Del(ctx, dedupKey)
        return fmt.Errorf("kickAllConnectionsForUser 失败: %w", err)
    }

    // ==================== 5. 清理用户连接索引（彻底清除） ====================
    // 用户注销后，连接索引不再需要，主动清理防止脏数据
    allKey := fmt.Sprintf("conn:user_all:%s", event.UserId)
    c.redis.Del(ctx, allKey)

    log.Info("user.deactivated 处理完成：所有连接已断开并清理",
        "user_id", event.UserId,
        "deactivate_time", event.Header.Timestamp)

    return nil
}
```

### Consumer: `config.changed`

> 来源：Config 配置服务。管理后台修改连接相关配置时触发。  
> 职责：动态更新 Connecte 服务的运行时配置，无需重启。  
> 支持热更新的配置项：最大连接数、心跳超时时间、心跳间隔、连接 TTL、并发度等。

```go
func (c *ConnecteConsumer) HandleConfigChanged(ctx context.Context, msg *kafka.Message) error {
    // ==================== 1. 反序列化 ====================
    var event kafka_config.ConfigChangedEvent
    if err := proto.Unmarshal(msg.Value, &event); err != nil {
        log.Error("反序列化 ConfigChangedEvent 失败", "err", err)
        return nil
    }

    // ==================== 2. 过滤非 connecte 相关配置 ====================
    // ConfigChangedEvent 是全局广播，只处理 connecte 命名空间的配置
    if event.Namespace != "connecte" && event.Namespace != "gateway" && event.Namespace != "global" {
        return nil // 非本服务配置，忽略
    }

    // ==================== 3. 幂等检查 — Redis ====================
    dedupKey := fmt.Sprintf("conn:kafka:dedup:%s", event.Header.EventId)
    set, err := c.redis.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result()
    if err != nil {
        return fmt.Errorf("Redis 幂等检查失败: %w", err)
    }
    if !set {
        log.Debug("重复事件，跳过", "event_id", event.Header.EventId)
        return nil
    }

    // ==================== 4. 解析并应用配置变更 ====================
    updatedCount := 0
    for _, item := range event.Items {
        switch item.Key {
        case "connecte.max_connections_per_gateway":
            // 单网关最大连接数上限
            val, err := strconv.Atoi(item.Value)
            if err != nil {
                log.Warn("配置值解析失败", "key", item.Key, "value", item.Value, "err", err)
                continue
            }
            oldVal := c.config.MaxConnectionsPerGateway
            c.config.MaxConnectionsPerGateway = val
            log.Info("配置已更新: 单网关最大连接数",
                "key", item.Key, "old", oldVal, "new", val)
            updatedCount++

        case "connecte.heartbeat_timeout_seconds":
            // 心跳超时时间（秒），超过此时间无心跳则认为连接断开
            val, err := strconv.Atoi(item.Value)
            if err != nil {
                log.Warn("配置值解析失败", "key", item.Key, "value", item.Value, "err", err)
                continue
            }
            oldVal := c.config.HeartbeatTimeoutSeconds
            c.config.HeartbeatTimeoutSeconds = val
            log.Info("配置已更新: 心跳超时时间",
                "key", item.Key, "old", oldVal, "new", val)
            updatedCount++

        case "connecte.heartbeat_interval_seconds":
            // 心跳上报间隔（秒），网关向 Redis 续期的频率
            val, err := strconv.Atoi(item.Value)
            if err != nil {
                log.Warn("配置值解析失败", "key", item.Key, "value", item.Value, "err", err)
                continue
            }
            oldVal := c.config.HeartbeatIntervalSeconds
            c.config.HeartbeatIntervalSeconds = val
            log.Info("配置已更新: 心跳上报间隔",
                "key", item.Key, "old", oldVal, "new", val)
            updatedCount++

        case "connecte.connection_ttl_seconds":
            // 连接 Redis Key TTL（秒），必须大于心跳超时
            val, err := strconv.Atoi(item.Value)
            if err != nil {
                log.Warn("配置值解析失败", "key", item.Key, "value", item.Value, "err", err)
                continue
            }
            oldVal := c.config.ConnectionTTLSeconds
            c.config.ConnectionTTLSeconds = val
            log.Info("配置已更新: 连接 TTL",
                "key", item.Key, "old", oldVal, "new", val)
            updatedCount++

        case "connecte.batch_send_concurrency":
            // BatchSendToConnections 最大并发度
            val, err := strconv.Atoi(item.Value)
            if err != nil || val <= 0 || val > 256 {
                log.Warn("配置值无效", "key", item.Key, "value", item.Value)
                continue
            }
            oldVal := c.config.BatchSendConcurrency
            c.config.BatchSendConcurrency = val
            log.Info("配置已更新: 批量发送并发度",
                "key", item.Key, "old", oldVal, "new", val)
            updatedCount++

        case "connecte.max_connections_per_user":
            // 单用户最大连接数（所有平台合计）
            val, err := strconv.Atoi(item.Value)
            if err != nil || val <= 0 {
                log.Warn("配置值无效", "key", item.Key, "value", item.Value)
                continue
            }
            oldVal := c.config.MaxConnectionsPerUser
            c.config.MaxConnectionsPerUser = val
            log.Info("配置已更新: 单用户最大连接数",
                "key", item.Key, "old", oldVal, "new", val)
            updatedCount++

        default:
            // global 命名空间的通用配置忽略
            if event.Namespace != "global" {
                log.Debug("未识别的配置项，忽略", "key", item.Key)
            }
        }
    }

    // ==================== 5. 将动态配置持久化到 Redis（供其他实例读取） ====================
    // Key: conn:config  HASH
    if updatedCount > 0 {
        configKey := "conn:config"
        configPipe := c.redis.Pipeline()
        configPipe.HSet(ctx, configKey, map[string]interface{}{
            "max_connections_per_gateway": c.config.MaxConnectionsPerGateway,
            "heartbeat_timeout_seconds":  c.config.HeartbeatTimeoutSeconds,
            "heartbeat_interval_seconds": c.config.HeartbeatIntervalSeconds,
            "connection_ttl_seconds":     c.config.ConnectionTTLSeconds,
            "batch_send_concurrency":     c.config.BatchSendConcurrency,
            "max_connections_per_user":   c.config.MaxConnectionsPerUser,
            "updated_at":                time.Now().UnixMilli(),
        })
        if _, err := configPipe.Exec(ctx); err != nil {
            log.Error("持久化动态配置到 Redis 失败", "err", err)
        }
    }

    log.Info("config.changed 处理完成",
        "namespace", event.Namespace,
        "updated_count", updatedCount,
        "total_items", len(event.Items))

    return nil
}
```

---

## 通用辅助方法

### kickAllConnectionsForUser — 断开用户所有平台的连接

> 被 `user.banned`、`user.deactivated`、`session.kicked`（无平台信息时）共用。  
> 遍历用户所有连接索引，逐个调用 KickConnection 断开。  
> 使用 goroutine 并发踢多个连接，提高效率。

```go
func (c *ConnecteConsumer) kickAllConnectionsForUser(ctx context.Context, userID, reason string) error {
    // ---- 1. 查询用户所有连接索引 ----
    // Key: conn:user_all:{user_id}  SET
    // Members: "platform:gateway_id:conn_id"
    allKey := fmt.Sprintf("conn:user_all:%s", userID)
    members, err := c.redis.SMembers(ctx, allKey).Result()
    if err != nil {
        return fmt.Errorf("查询用户连接索引失败: %w", err)
    }

    if len(members) == 0 {
        log.Debug("用户无在线连接，无需处理", "user_id", userID)
        return nil
    }

    // ---- 2. 解析所有连接信息 ----
    type connTarget struct {
        platform  string
        gatewayID string
        connID    string
    }
    targets := make([]connTarget, 0, len(members))
    for _, member := range members {
        parts := strings.SplitN(member, ":", 3) // platform:gateway_id:conn_id
        if len(parts) != 3 {
            log.Warn("无效的连接索引格式", "user_id", userID, "member", member)
            continue
        }
        targets = append(targets, connTarget{
            platform:  parts[0],
            gatewayID: parts[1],
            connID:    parts[2],
        })
    }

    if len(targets) == 0 {
        return nil
    }

    // ---- 3. 并发踢所有连接 ----
    var (
        wg          sync.WaitGroup
        mu          sync.Mutex
        kickErrors  []error
        kickedCount int
    )

    for _, target := range targets {
        target := target
        wg.Add(1)
        go func() {
            defer wg.Done()

            _, kickErr := c.connecteService.KickConnection(ctx, &pb.KickConnectionRequest{
                UserId:       userID,
                ConnectionId: target.connID,
                Reason:       reason,
            })

            mu.Lock()
            defer mu.Unlock()
            if kickErr != nil {
                kickErrors = append(kickErrors, fmt.Errorf(
                    "kick conn %s on gateway %s failed: %w",
                    target.connID, target.gatewayID, kickErr))
            } else {
                kickedCount++
            }
        }()
    }

    wg.Wait()

    // ---- 4. 汇总结果 ----
    if len(kickErrors) > 0 {
        log.Error("部分连接踢出失败",
            "user_id", userID,
            "total", len(targets),
            "kicked", kickedCount,
            "failed", len(kickErrors),
            "first_error", kickErrors[0])
        // 只要有失败就返回 error 触发重试
        return fmt.Errorf("踢出 %d/%d 个连接失败", len(kickErrors), len(targets))
    }

    log.Info("用户所有连接已断开",
        "user_id", userID,
        "kicked_count", kickedCount,
        "reason", reason)

    return nil
}
```

---

## 定时任务

### StaleConnectionCleanup — 过期连接清理任务

> 定时扫描全局网关列表和连接索引，清理已过期但未被正确注销的「僵尸连接」。  
> 触发场景：网关进程崩溃（未调用 UnregisterConnection）、Redis TTL 竞态条件等。  
> 每 60s 执行一次，作为心跳续期机制的**兜底保障**。

```go
// startStaleConnectionCleanup 启动过期连接清理定时任务
func (c *ConnecteConsumer) startStaleConnectionCleanup() {
    ticker := time.NewTicker(60 * time.Second)
    defer ticker.Stop()

    for {
        select {
        case <-c.stopCh:
            log.Info("过期连接清理任务已停止")
            return
        case <-ticker.C:
            c.doStaleConnectionCleanup()
        }
    }
}

func (c *ConnecteConsumer) doStaleConnectionCleanup() {
    ctx := context.Background()
    startTime := time.Now()

    // ==================== 1. 检查过期的网关节点 ====================
    // 从 conn:gateway_list ZSET 获取所有网关 ID
    gatewayMembers, err := c.redis.ZRangeWithScores(ctx, "conn:gateway_list", 0, -1).Result()
    if err != nil {
        log.Error("清理任务: 查询网关列表失败", "err", err)
        return
    }

    staleGateways := make([]string, 0)
    for _, z := range gatewayMembers {
        gatewayID := z.Member.(string)

        // 检查网关 HASH 是否还存在（TTL 30s，网关心跳续期）
        // Key: conn:gateway:{gateway_id}
        gatewayKey := fmt.Sprintf("conn:gateway:%s", gatewayID)
        exists, err := c.redis.Exists(ctx, gatewayKey).Result()
        if err != nil {
            log.Warn("清理任务: 检查网关存在性失败",
                "gateway_id", gatewayID, "err", err)
            continue
        }

        if exists == 0 {
            // 网关 HASH 已过期（心跳超时），标记为过期
            staleGateways = append(staleGateways, gatewayID)
        }
    }

    // ==================== 2. 清理过期网关及其所有连接 ====================
    totalCleaned := 0
    for _, gatewayID := range staleGateways {
        cleaned := c.cleanupStaleGateway(ctx, gatewayID)
        totalCleaned += cleaned
    }

    // ==================== 3. 清理孤立的用户连接 ====================
    // 扫描部分用户连接 key，检查对应网关是否仍然活跃
    // 使用 SCAN 避免阻塞 Redis
    orphanCleaned := c.cleanupOrphanConnections(ctx)
    totalCleaned += orphanCleaned

    // ==================== 4. 上报清理统计 ====================
    if totalCleaned > 0 || len(staleGateways) > 0 {
        duration := time.Since(startTime)
        log.Info("过期连接清理完成",
            "stale_gateways", len(staleGateways),
            "cleaned_connections", totalCleaned,
            "orphan_cleaned", orphanCleaned,
            "duration_ms", duration.Milliseconds())

        // 投递统计事件
        statsEvent := &kafka_connecte.ConnStatsEvent{
            Header:           buildEventHeader("connecte", c.instanceID),
            Gateway:          c.instanceID,
            TotalDisconnected: int64(totalCleaned),
            StatsTime:        time.Now().UnixMilli(),
            PeriodSeconds:    60,
        }
        c.kafka.Produce(ctx, "conn.stats", c.instanceID, statsEvent)
    }
}

// cleanupStaleGateway 清理过期网关的所有连接
// 当网关 HASH 已过期（心跳超时 30s），说明网关节点已不可用
// 需要清理该网关上注册的所有连接路由
func (c *ConnecteConsumer) cleanupStaleGateway(ctx context.Context, gatewayID string) int {
    // ---- 1. 获取网关上的所有连接 ----
    // Key: conn:gateway_conns:{gateway_id}  SET
    // Members: "user_id:platform"
    gatewayConnsKey := fmt.Sprintf("conn:gateway_conns:%s", gatewayID)
    connMembers, err := c.redis.SMembers(ctx, gatewayConnsKey).Result()
    if err != nil {
        log.Error("清理任务: 查询网关连接列表失败",
            "gateway_id", gatewayID, "err", err)
        return 0
    }

    cleanedCount := 0

    // ---- 2. 逐个清理连接路由 ----
    for _, member := range connMembers {
        // member 格式: "user_id:platform"
        parts := strings.SplitN(member, ":", 2)
        if len(parts) != 2 {
            continue
        }
        userID := parts[0]
        platform := parts[1]

        // 检查用户连接 HASH 是否指向该网关
        connKey := fmt.Sprintf("conn:user:%s:%s", userID, platform)
        storedGateway, err := c.redis.HGet(ctx, connKey, "gateway_id").Result()
        if err != nil || storedGateway != gatewayID {
            // 连接已过期或已被新网关覆盖，跳过
            continue
        }

        // 获取 conn_id 用于清理
        connID, _ := c.redis.HGet(ctx, connKey, "conn_id").Result()
        deviceID, _ := c.redis.HGet(ctx, connKey, "device_id").Result()

        // 清理路由
        pipe := c.redis.Pipeline()
        pipe.Del(ctx, connKey)

        allKey := fmt.Sprintf("conn:user_all:%s", userID)
        if connID != "" {
            cleanMember := fmt.Sprintf("%s:%s:%s", platform, gatewayID, connID)
            pipe.SRem(ctx, allKey, cleanMember)
        }

        pipe.SRem(ctx, gatewayConnsKey, member)
        if _, err := pipe.Exec(ctx); err != nil {
            log.Warn("清理任务: 清理连接路由失败",
                "user_id", userID, "platform", platform, "err", err)
            continue
        }

        cleanedCount++

        // 通知 Presence 用户下线
        platformInt, _ := strconv.Atoi(platform)
        _, presErr := c.presenceClient.UserOffline(ctx, &presence_pb.UserOfflineRequest{
            UserId:   userID,
            Platform: common.PlatformType(platformInt),
        })
        if presErr != nil {
            log.Warn("清理任务: 通知 Presence 下线失败",
                "user_id", userID, "err", presErr)
        }

        // 投递 conn.disconnected 事件
        disconnEvent := &kafka_connecte.DisconnectedEvent{
            Header:      buildEventHeader("connecte", c.instanceID),
            UserId:      userID,
            DeviceId:    deviceID,
            Platform:    common.PlatformType(platformInt),
            Gateway:     gatewayID,
            Reason:      "gateway_heartbeat_timeout",
            DisconnTime: time.Now().UnixMilli(),
        }
        c.kafka.Produce(ctx, "conn.disconnected", userID, disconnEvent)
    }

    // ---- 3. 从全局网关列表移除过期网关 ----
    pipe := c.redis.Pipeline()
    pipe.ZRem(ctx, "conn:gateway_list", gatewayID)
    pipe.Del(ctx, gatewayConnsKey)
    if _, err := pipe.Exec(ctx); err != nil {
        log.Warn("清理任务: 移除过期网关失败",
            "gateway_id", gatewayID, "err", err)
    }

    log.Info("清理任务: 过期网关已清理",
        "gateway_id", gatewayID,
        "cleaned_connections", cleanedCount)

    return cleanedCount
}

// cleanupOrphanConnections 清理孤立连接
// 使用 SCAN 扫描 conn:user:* 类型的 key，检查连接对应的网关是否仍然活跃
// 每次最多扫描 1000 个 key，避免清理任务耗时过长
func (c *ConnecteConsumer) cleanupOrphanConnections(ctx context.Context) int {
    cleanedCount := 0
    scanCount := 0
    maxScan := 1000 // 每次最多扫描 1000 个 key

    var cursor uint64
    for {
        if scanCount >= maxScan {
            break
        }

        // SCAN 匹配 conn:user:*:* 格式的 key（用户平台连接 HASH）
        keys, nextCursor, err := c.redis.Scan(ctx, cursor, "conn:user:*", 100).Result()
        if err != nil {
            log.Warn("清理任务: SCAN 失败", "cursor", cursor, "err", err)
            break
        }

        for _, key := range keys {
            scanCount++
            if scanCount > maxScan {
                break
            }

            // 跳过 conn:user_all:* 格式的 key（SET 类型）
            if strings.Contains(key, "user_all:") {
                continue
            }

            // 检查连接对应的网关是否仍然存活
            gatewayID, err := c.redis.HGet(ctx, key, "gateway_id").Result()
            if err != nil || gatewayID == "" {
                continue
            }

            gatewayKey := fmt.Sprintf("conn:gateway:%s", gatewayID)
            exists, err := c.redis.Exists(ctx, gatewayKey).Result()
            if err != nil {
                continue
            }

            if exists == 0 {
                // 网关已过期，此连接为孤立连接，删除
                userID, _ := c.redis.HGet(ctx, key, "conn_id").Result()
                _ = userID // 用于日志

                c.redis.Del(ctx, key)
                cleanedCount++

                log.Debug("清理任务: 删除孤立连接 key",
                    "key", key, "gateway_id", gatewayID)
            }
        }

        cursor = nextCursor
        if cursor == 0 {
            break // SCAN 完成
        }
    }

    return cleanedCount
}
```

---

## 服务初始化 & Consumer Group 注册

```go
// NewConnecteConsumer 初始化 Connecte Kafka 消费者
func NewConnecteConsumer(cfg *Config, redis *redis.Client, kafka *KafkaClient,
    connecteService *ConnecteService, presenceClient presence_pb.PresenceServiceClient) *ConnecteConsumer {

    consumer := &ConnecteConsumer{
        config:          cfg,
        redis:           redis,
        kafka:           kafka,
        connecteService: connecteService,
        presenceClient:  presenceClient,
        instanceID:      generateInstanceID(),
        stopCh:          make(chan struct{}),
    }

    return consumer
}

// Start 启动所有 Kafka 消费者 + 定时任务
func (c *ConnecteConsumer) Start(ctx context.Context) error {
    // ---- 1. 注册 Kafka Consumer ----
    // Consumer Group: connecte-consumer-group
    handlers := map[string]ConsumerHandler{
        "session.kicked":   {Handler: c.HandleSessionKicked, Concurrency: 4, MaxRetry: 3},
        "user.banned":      {Handler: c.HandleUserBanned, Concurrency: 2, MaxRetry: 3},
        "user.deactivated": {Handler: c.HandleUserDeactivated, Concurrency: 2, MaxRetry: 3},
        "config.changed":   {Handler: c.HandleConfigChanged, Concurrency: 1, MaxRetry: 1},
    }

    for topic, handler := range handlers {
        if err := c.kafka.Subscribe(ctx, "connecte-consumer-group", topic, handler); err != nil {
            return fmt.Errorf("订阅 topic %s 失败: %w", topic, err)
        }
        log.Info("Kafka consumer 已注册",
            "topic", topic,
            "concurrency", handler.Concurrency,
            "max_retry", handler.MaxRetry)
    }

    // ---- 2. 启动定时清理任务 ----
    go c.startStaleConnectionCleanup()
    log.Info("过期连接清理定时任务已启动", "interval", "60s")

    // ---- 3. 启动时加载动态配置 ----
    c.loadDynamicConfig(ctx)

    log.Info("Connecte Kafka 消费者启动完成",
        "instance_id", c.instanceID,
        "topics", []string{"session.kicked", "user.banned", "user.deactivated", "config.changed"})

    return nil
}

// loadDynamicConfig 从 Redis 加载动态配置（启动时 + config.changed 后）
func (c *ConnecteConsumer) loadDynamicConfig(ctx context.Context) {
    // Key: conn:config  HASH
    configKey := "conn:config"
    result, err := c.redis.HGetAll(ctx, configKey).Result()
    if err != nil || len(result) == 0 {
        log.Info("无动态配置，使用默认值")
        return
    }

    if v, ok := result["max_connections_per_gateway"]; ok {
        if val, err := strconv.Atoi(v); err == nil {
            c.config.MaxConnectionsPerGateway = val
        }
    }
    if v, ok := result["heartbeat_timeout_seconds"]; ok {
        if val, err := strconv.Atoi(v); err == nil {
            c.config.HeartbeatTimeoutSeconds = val
        }
    }
    if v, ok := result["heartbeat_interval_seconds"]; ok {
        if val, err := strconv.Atoi(v); err == nil {
            c.config.HeartbeatIntervalSeconds = val
        }
    }
    if v, ok := result["connection_ttl_seconds"]; ok {
        if val, err := strconv.Atoi(v); err == nil {
            c.config.ConnectionTTLSeconds = val
        }
    }
    if v, ok := result["batch_send_concurrency"]; ok {
        if val, err := strconv.Atoi(v); err == nil {
            c.config.BatchSendConcurrency = val
        }
    }
    if v, ok := result["max_connections_per_user"]; ok {
        if val, err := strconv.Atoi(v); err == nil {
            c.config.MaxConnectionsPerUser = val
        }
    }

    log.Info("动态配置已加载", "config", c.config)
}

// Stop 优雅停止
func (c *ConnecteConsumer) Stop() {
    close(c.stopCh)
    log.Info("Connecte Kafka 消费者已停止")
}
```

### buildEventHeader — 构建 Kafka 事件头

```go
func buildEventHeader(source, instanceID string) *common.EventHeader {
    return &common.EventHeader{
        EventId:   uuid.New().String(),
        TraceId:   "",
        SpanId:    "",
        Source:    source,
        SourceId:  instanceID,
        Timestamp: time.Now().UnixMilli(),
        Version:   1,
    }
}
```
