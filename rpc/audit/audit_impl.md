# Audit 审计/内容审核服务 — RPC 接口实现伪代码

## 概述

Audit 服务承担两大核心职责：
1. **审计日志**（Audit Logging）— 记录系统中所有重要操作（登录、消息、群操作、好友操作等），用于合规审查、安全追溯、问题排查
2. **内容审核**（Content Moderation）— 对消息文本/图片/视频/用户资料等内容进行违规检测，支持关键词过滤、外部 AI 审核 API（异步）、人工复审队列三种模式

**核心设计原则：**
- **高吞吐**：审计日志写入以 Kafka 异步为主，RPC 仅用于主动查询和手动补录
- **批量写入**：BatchCreateAuditLog 支持高吞吐 fire-and-forget 场景
- **分区表**：audit_logs 按 `created_at` 按月分区，冷热分离
- **异步审核**：ContentModeration 默认异步，结果通过 Kafka 事件 + Redis 缓存回查
- **多级审核**：关键词过滤（毫秒级）→ AI API（秒级）→ 人工复审队列（分钟~小时级）
- **违规升级**：累计违规次数达到阈值自动触发用户级处罚

## 外部依赖

| 依赖类型 | 依赖项 | 用途 |
|---------|--------|------|
| PgSQL | audit_logs 表（按月分区） | 审计日志持久化 |
| PgSQL | moderation_records 表 | 内容审核记录 |
| Redis | 审核结果缓存/频率限制/违规计数/队列监控 | 高频查询与限流 |
| RPC | User.GetUser | 内容审核时获取用户上下文（昵称/历史违规等） |
| Kafka | audit.moderation.result → Message | 审核违规时通过 Kafka 事件通知 Message 服务自动撤回（不直接 RPC 调用，解耦审核与消息撤回） |
| Kafka | audit.moderation.result | 审核结果 → Message（更新消息状态）、Media（封禁媒体） |
| Kafka | audit.content.violation | 内容违规 → User（封禁）、Group（处理）、Relation |
| Kafka | audit.user.violation | 用户级违规升级 → User（封禁/警告） |
| 外部 | AI 内容审核 API（阿里云/腾讯云等） | 图片/视频/文本 AI 审核 |

## PgSQL 表结构

```sql
-- 审计日志表（核心表，按 created_at 按月分区支撑大规模数据）
CREATE TABLE audit_logs (
    id            BIGSERIAL,
    log_id        VARCHAR(20)   NOT NULL,                     -- Snowflake 全局唯一日志 ID
    user_id       VARCHAR(20)   NOT NULL,                     -- 操作用户 ID
    action        VARCHAR(100)  NOT NULL,                     -- 操作类型（LOGIN / SEND_MESSAGE / CREATE_GROUP 等）
    resource_type VARCHAR(50)   NOT NULL DEFAULT '',           -- 资源类型（message / group / user / config 等）
    resource_id   VARCHAR(100)  NOT NULL DEFAULT '',           -- 资源 ID（msg_id / group_id 等）
    detail        JSONB         NOT NULL DEFAULT '{}',         -- 操作详情（JSON 格式，灵活扩展）
    ip            VARCHAR(45)   NOT NULL DEFAULT '',           -- 客户端 IP（支持 IPv6）
    device_info   VARCHAR(200)  NOT NULL DEFAULT '',           -- 设备信息 / User-Agent
    result        VARCHAR(20)   NOT NULL DEFAULT 'success',    -- 操作结果：success / fail / denied
    created_at    TIMESTAMPTZ   NOT NULL DEFAULT NOW(),        -- 创建时间

    PRIMARY KEY (id, created_at)                               -- 分区键必须包含在主键中
) PARTITION BY RANGE (created_at);

-- 每月自动创建分区（示例）
-- CREATE TABLE audit_logs_2026_01 PARTITION OF audit_logs FOR VALUES FROM ('2026-01-01') TO ('2026-02-01');
-- CREATE TABLE audit_logs_2026_02 PARTITION OF audit_logs FOR VALUES FROM ('2026-02-01') TO ('2026-03-01');
-- ... 建议使用 pg_partman 自动管理

-- 查询索引
CREATE INDEX idx_audit_logs_user_time ON audit_logs (user_id, created_at DESC);
CREATE INDEX idx_audit_logs_action_time ON audit_logs (action, created_at DESC);
CREATE INDEX idx_audit_logs_resource ON audit_logs (resource_type, resource_id);
CREATE UNIQUE INDEX idx_audit_logs_log_id ON audit_logs (log_id, created_at);

-- 内容审核记录表
CREATE TABLE moderation_records (
    id            BIGSERIAL    PRIMARY KEY,
    request_id    VARCHAR(20)  NOT NULL UNIQUE,                -- Snowflake 全局唯一审核请求 ID
    content_type  VARCHAR(20)  NOT NULL,                       -- 内容类型：text / image / video / voice / nickname / group_name / avatar
    content       TEXT         NOT NULL DEFAULT '',             -- 待审核文本内容
    media_url     TEXT         NOT NULL DEFAULT '',             -- 待审核媒体 URL
    user_id       VARCHAR(20)  NOT NULL,                       -- 提交审核的用户 ID
    source        VARCHAR(50)  NOT NULL DEFAULT '',             -- 审核来源：chat / profile / group_name / media_upload
    status        INT          NOT NULL DEFAULT 0,             -- 审核状态：0=PENDING 1=PROCESSING 2=PASS 3=REJECT 4=REVIEW
    result        VARCHAR(20)  NOT NULL DEFAULT '',             -- 审核结果：PASS / SPAM / PORN / VIOLENCE / POLITICAL / AD / OTHER
    reason        TEXT         NOT NULL DEFAULT '',             -- 审核原因说明
    confidence    FLOAT        NOT NULL DEFAULT 0.0,           -- 置信度（0.0 ~ 1.0）
    reviewed_by   VARCHAR(20)  NOT NULL DEFAULT '',             -- 人工审核员 ID（人工审核时有值）
    reviewed_at   TIMESTAMPTZ,                                 -- 人工审核时间
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW()           -- 创建时间
);

CREATE INDEX idx_moderation_user ON moderation_records (user_id, created_at DESC);
CREATE INDEX idx_moderation_status ON moderation_records (status, created_at);
CREATE INDEX idx_moderation_source ON moderation_records (source, created_at DESC);
```

## 审核状态枚举

| 值 | 名称 | 说明 |
|----|------|------|
| 0 | PENDING | 待处理 |
| 1 | PROCESSING | 处理中 |
| 2 | PASS | 审核通过 |
| 3 | REJECT | 审核拒绝 |
| 4 | REVIEW | 需人工复审 |

## 审核结果枚举

| 值 | 说明 |
|----|------|
| PASS | 通过 |
| SPAM | 垃圾信息 |
| PORN | 涉黄 |
| VIOLENCE | 暴力 |
| POLITICAL | 涉政 |
| AD | 广告 |
| OTHER | 其他违规 |

## Redis Key 设计

| Key 格式 | 类型 | 值 | TTL | 说明 |
|----------|------|-----|-----|------|
| `audit:moderation:{request_id}` | HASH | request_id / status / result / reason / confidence / reviewed_by | 24h | 审核结果缓存，供 GetModerationResult 快速查询 |
| `audit:rate:{user_id}:{action}` | STRING | count | 按 action 不同（1min~1h） | 操作频率限制（如发消息/修改资料频率） |
| `audit:user_violation_count:{user_id}` | STRING | count | 30d | 用户累计违规次数（用于违规升级判断） |
| `audit:moderation_queue_size` | STRING | count | 无 TTL（实时更新） | 人工审核队列大小监控 |
| `audit:kafka:dedup:{event_id}` | STRING | "1" | 24h | Kafka 消费幂等去重 |

---

## 接口实现

### 1. CreateAuditLog — 创建单条审计日志

> 手动创建一条审计日志。通常审计日志通过 Kafka 异步写入，此 RPC 用于管理后台补录或低频主动写入场景。

```go
func (s *AuditService) CreateAuditLog(ctx context.Context, req *pb.CreateAuditLogRequest) (*pb.CreateAuditLogResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id is required")
    }
    if req.Action == common.AUDIT_ACTION_UNSPECIFIED {
        return nil, status.Error(codes.InvalidArgument, "action is required")
    }
    if req.ResourceType == "" {
        return nil, status.Error(codes.InvalidArgument, "resource_type is required")
    }

    // ==================== 2. 生成日志 ID — Snowflake ====================
    logID := s.snowflake.Generate().String() // 全局唯一 20 位 ID

    // ==================== 3. 序列化 detail — JSON ====================
    detailJSON := "{}"
    if req.Detail != "" {
        // 校验 detail 是合法 JSON
        if !json.Valid([]byte(req.Detail)) {
            return nil, status.Error(codes.InvalidArgument, "detail must be valid JSON")
        }
        detailJSON = req.Detail
    }

    // ==================== 4. 写入 PgSQL ====================
    _, err := s.db.ExecContext(ctx,
        `INSERT INTO audit_logs (log_id, user_id, action, resource_type, resource_id, detail, ip, device_info, result, created_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW())`,
        logID,
        req.UserId,
        req.Action.String(),    // 枚举转字符串：LOGIN / SEND_MESSAGE 等
        req.ResourceType,
        req.ResourceId,
        detailJSON,
        req.ClientIp,
        req.UserAgent,
        "success",              // RPC 手动创建默认 result=success
    )
    if err != nil {
        log.Error("insert audit_log failed", "log_id", logID, "err", err)
        return nil, status.Error(codes.Internal, "insert audit log failed")
    }

    log.Info("audit log created",
        "log_id", logID, "user_id", req.UserId,
        "action", req.Action.String(), "resource", req.ResourceType+"/"+req.ResourceId)

    return &pb.CreateAuditLogResponse{
        Meta:  successMeta(ctx),
        LogId: logID,
    }, nil
}
```

---

### 2. BatchCreateAuditLog — 批量创建审计日志

> 高吞吐场景批量写入审计日志。适用于 Kafka 消费者批量落库、管理后台批量导入等。  
> 采用 fire-and-forget 模式：尽力写入，部分失败不阻塞整体。

```go
func (s *AuditService) BatchCreateAuditLog(ctx context.Context, req *pb.BatchCreateAuditLogRequest) (*pb.BatchCreateAuditLogResponse, error) {
    // ==================== 1. 参数校验 ====================
    if len(req.Logs) == 0 {
        return nil, status.Error(codes.InvalidArgument, "logs cannot be empty")
    }
    if len(req.Logs) > 500 {
        return nil, status.Error(codes.InvalidArgument, "batch size too large, max 500")
    }

    // ==================== 2. 批量写入 — PgSQL 事务批量 INSERT ====================
    tx, err := s.db.BeginTx(ctx, nil)
    if err != nil {
        return nil, status.Error(codes.Internal, "begin transaction failed")
    }
    defer tx.Rollback()

    // 预编译批量插入语句
    stmt, err := tx.PrepareContext(ctx,
        `INSERT INTO audit_logs (log_id, user_id, action, resource_type, resource_id, detail, ip, device_info, result, created_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW())`)
    if err != nil {
        return nil, status.Error(codes.Internal, "prepare statement failed")
    }
    defer stmt.Close()

    var successIDs []string
    var failedCount int32

    for _, logReq := range req.Logs {
        // 逐条校验
        if logReq.UserId == "" || logReq.Action == common.AUDIT_ACTION_UNSPECIFIED {
            failedCount++
            log.Warn("batch audit log skipped: invalid params",
                "user_id", logReq.UserId, "action", logReq.Action)
            continue
        }

        // 生成 log_id
        logID := s.snowflake.Generate().String()

        // detail 默认空 JSON
        detailJSON := "{}"
        if logReq.Detail != "" && json.Valid([]byte(logReq.Detail)) {
            detailJSON = logReq.Detail
        }

        _, err := stmt.ExecContext(ctx,
            logID,
            logReq.UserId,
            logReq.Action.String(),
            logReq.ResourceType,
            logReq.ResourceId,
            detailJSON,
            logReq.ClientIp,
            logReq.UserAgent,
            "success",
        )
        if err != nil {
            failedCount++
            log.Error("batch insert audit_log failed",
                "log_id", logID, "user_id", logReq.UserId, "err", err)
            continue
        }
        successIDs = append(successIDs, logID)
    }

    // 提交事务
    if err := tx.Commit(); err != nil {
        log.Error("batch audit log commit failed", "err", err)
        return nil, status.Error(codes.Internal, "commit transaction failed")
    }

    log.Info("batch audit logs created",
        "total", len(req.Logs), "success", len(successIDs), "failed", failedCount)

    return &pb.BatchCreateAuditLogResponse{
        Meta:        successMeta(ctx),
        LogIds:      successIDs,
        FailedCount: failedCount,
    }, nil
}
```

---

### 3. QueryAuditLogs — 多条件查询审计日志

> 支持按 user_id、action、resource_type、resource_id、时间范围、关键字等多条件筛选审计日志。  
> 管理后台和合规审查的核心查询接口。

```go
func (s *AuditService) QueryAuditLogs(ctx context.Context, req *pb.QueryAuditLogsRequest) (*pb.QueryAuditLogsResponse, error) {
    // ==================== 1. 分页参数校验 ====================
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
        pageSize = 100 // 防止单次拉取过大
    }

    // ==================== 2. 构建动态查询条件 ====================
    // 基础 WHERE 1=1 方便动态拼接
    baseQuery := `FROM audit_logs WHERE 1=1`
    countQuery := `SELECT COUNT(*) ` + baseQuery
    dataQuery := `SELECT log_id, user_id, action, resource_type, resource_id, detail, ip, device_info, result, created_at ` + baseQuery

    var args []interface{}
    argIdx := 1

    // 按 user_id 筛选
    if req.UserId != "" {
        condition := fmt.Sprintf(` AND user_id = $%d`, argIdx)
        countQuery += condition
        dataQuery += condition
        args = append(args, req.UserId)
        argIdx++
    }

    // 按 action 筛选
    if req.Action != common.AUDIT_ACTION_UNSPECIFIED {
        condition := fmt.Sprintf(` AND action = $%d`, argIdx)
        countQuery += condition
        dataQuery += condition
        args = append(args, req.Action.String())
        argIdx++
    }

    // 按 resource_type 筛选
    if req.ResourceType != "" {
        condition := fmt.Sprintf(` AND resource_type = $%d`, argIdx)
        countQuery += condition
        dataQuery += condition
        args = append(args, req.ResourceType)
        argIdx++
    }

    // 按 resource_id 筛选
    if req.ResourceId != "" {
        condition := fmt.Sprintf(` AND resource_id = $%d`, argIdx)
        countQuery += condition
        dataQuery += condition
        args = append(args, req.ResourceId)
        argIdx++
    }

    // 按时间范围筛选（命中分区裁剪，性能关键）
    if req.TimeRange != nil {
        if req.TimeRange.StartTime > 0 {
            condition := fmt.Sprintf(` AND created_at >= to_timestamp($%d / 1000.0)`, argIdx)
            countQuery += condition
            dataQuery += condition
            args = append(args, req.TimeRange.StartTime)
            argIdx++
        }
        if req.TimeRange.EndTime > 0 {
            condition := fmt.Sprintf(` AND created_at <= to_timestamp($%d / 1000.0)`, argIdx)
            countQuery += condition
            dataQuery += condition
            args = append(args, req.TimeRange.EndTime)
            argIdx++
        }
    }

    // 按 client_ip 筛选
    if req.ClientIp != "" {
        condition := fmt.Sprintf(` AND ip = $%d`, argIdx)
        countQuery += condition
        dataQuery += condition
        args = append(args, req.ClientIp)
        argIdx++
    }

    // 关键字搜索（在 detail JSONB 中模糊匹配）
    if req.Keyword != "" {
        condition := fmt.Sprintf(` AND detail::text ILIKE '%%' || $%d || '%%'`, argIdx)
        countQuery += condition
        dataQuery += condition
        args = append(args, req.Keyword)
        argIdx++
    }

    // ==================== 3. 查询总数 — PgSQL ====================
    var total int64
    err := s.db.QueryRowContext(ctx, countQuery, args...).Scan(&total)
    if err != nil {
        log.Error("query audit_logs count failed", "err", err)
        return nil, status.Error(codes.Internal, "query count failed")
    }

    // ==================== 4. 查询数据 — PgSQL（分页 + 排序） ====================
    offset := (page - 1) * pageSize
    dataQuery += fmt.Sprintf(` ORDER BY created_at DESC LIMIT $%d OFFSET $%d`, argIdx, argIdx+1)
    dataArgs := append(args, pageSize, offset)

    rows, err := s.db.QueryContext(ctx, dataQuery, dataArgs...)
    if err != nil {
        log.Error("query audit_logs failed", "err", err)
        return nil, status.Error(codes.Internal, "query audit logs failed")
    }
    defer rows.Close()

    var logs []*pb.AuditLog
    for rows.Next() {
        var (
            logID, userID, action, resType, resID string
            detail, ip, deviceInfo, result        string
            createdAt                             time.Time
        )
        if err := rows.Scan(&logID, &userID, &action, &resType, &resID,
            &detail, &ip, &deviceInfo, &result, &createdAt); err != nil {
            log.Error("scan audit_log row failed", "err", err)
            continue
        }
        logs = append(logs, &pb.AuditLog{
            LogId:        logID,
            UserId:       userID,
            Action:       common.AuditActionFromString(action), // 字符串转枚举
            ResourceType: resType,
            ResourceId:   resID,
            Detail:       detail,
            ClientIp:     ip,
            UserAgent:    deviceInfo,
            CreatedAt:    createdAt.UnixMilli(),
        })
    }

    // ==================== 5. 返回结果 ====================
    totalPages := int32(total) / pageSize
    if int32(total)%pageSize > 0 {
        totalPages++
    }

    return &pb.QueryAuditLogsResponse{
        Meta: successMeta(ctx),
        Logs: logs,
        Pagination: &common.PaginationResult{
            Page:       page,
            PageSize:   pageSize,
            Total:      total,
            TotalPages: totalPages,
        },
    }, nil
}
```

---

### 4. GetAuditLog — 获取单条审计日志

> 根据 log_id 查询单条审计日志详情。

```go
func (s *AuditService) GetAuditLog(ctx context.Context, req *pb.GetAuditLogRequest) (*pb.GetAuditLogResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.LogId == "" {
        return nil, status.Error(codes.InvalidArgument, "log_id is required")
    }

    // ==================== 2. 查询 PgSQL ====================
    // 注意：log_id 有唯一索引 (log_id, created_at)，跨分区查询可能需全分区扫描
    // 如果调用方能提供大致时间范围可大幅优化，此处做通用查询
    var (
        logID, userID, action, resType, resID string
        detail, ip, deviceInfo, result        string
        createdAt                             time.Time
    )
    err := s.db.QueryRowContext(ctx,
        `SELECT log_id, user_id, action, resource_type, resource_id, detail, ip, device_info, result, created_at
         FROM audit_logs
         WHERE log_id = $1
         LIMIT 1`,
        req.LogId,
    ).Scan(&logID, &userID, &action, &resType, &resID,
        &detail, &ip, &deviceInfo, &result, &createdAt)

    if err == sql.ErrNoRows {
        return nil, status.Error(codes.NotFound, "audit log not found")
    }
    if err != nil {
        log.Error("query audit_log by log_id failed", "log_id", req.LogId, "err", err)
        return nil, status.Error(codes.Internal, "query audit log failed")
    }

    // ==================== 3. 返回 ====================
    return &pb.GetAuditLogResponse{
        Meta: successMeta(ctx),
        Log: &pb.AuditLog{
            LogId:        logID,
            UserId:       userID,
            Action:       common.AuditActionFromString(action),
            ResourceType: resType,
            ResourceId:   resID,
            Detail:       detail,
            ClientIp:     ip,
            UserAgent:    deviceInfo,
            CreatedAt:    createdAt.UnixMilli(),
        },
    }, nil
}
```

---

### 5. GetUserAuditLogs — 获取指定用户的审计日志

> 查询某个用户的所有操作审计日志，支持按 action 类型和时间范围筛选。  
> 典型场景：管理后台查看某用户行为轨迹、合规审查用户操作历史。

```go
func (s *AuditService) GetUserAuditLogs(ctx context.Context, req *pb.GetUserAuditLogsRequest) (*pb.GetUserAuditLogsResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id is required")
    }

    // 分页参数
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

    // ==================== 2. 构建查询条件 ====================
    baseQuery := `FROM audit_logs WHERE user_id = $1`
    var args []interface{}
    args = append(args, req.UserId)
    argIdx := 2

    // 按 action 列表筛选（可选）
    if len(req.Actions) > 0 {
        // 构建 IN 子句
        placeholders := make([]string, len(req.Actions))
        for i, act := range req.Actions {
            placeholders[i] = fmt.Sprintf("$%d", argIdx)
            args = append(args, act.String())
            argIdx++
        }
        baseQuery += fmt.Sprintf(` AND action IN (%s)`, strings.Join(placeholders, ","))
    }

    // 按时间范围筛选
    if req.TimeRange != nil {
        if req.TimeRange.StartTime > 0 {
            baseQuery += fmt.Sprintf(` AND created_at >= to_timestamp($%d / 1000.0)`, argIdx)
            args = append(args, req.TimeRange.StartTime)
            argIdx++
        }
        if req.TimeRange.EndTime > 0 {
            baseQuery += fmt.Sprintf(` AND created_at <= to_timestamp($%d / 1000.0)`, argIdx)
            args = append(args, req.TimeRange.EndTime)
            argIdx++
        }
    }

    // ==================== 3. 查询总数 — PgSQL ====================
    var total int64
    err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) `+baseQuery, args...).Scan(&total)
    if err != nil {
        log.Error("query user audit_logs count failed", "user_id", req.UserId, "err", err)
        return nil, status.Error(codes.Internal, "query count failed")
    }

    // ==================== 4. 查询数据 — PgSQL ====================
    offset := (page - 1) * pageSize
    dataQuery := `SELECT log_id, user_id, action, resource_type, resource_id, detail, ip, device_info, result, created_at ` +
        baseQuery + fmt.Sprintf(` ORDER BY created_at DESC LIMIT $%d OFFSET $%d`, argIdx, argIdx+1)
    dataArgs := append(args, pageSize, offset)

    rows, err := s.db.QueryContext(ctx, dataQuery, dataArgs...)
    if err != nil {
        log.Error("query user audit_logs failed", "user_id", req.UserId, "err", err)
        return nil, status.Error(codes.Internal, "query user audit logs failed")
    }
    defer rows.Close()

    var logs []*pb.AuditLog
    for rows.Next() {
        var (
            logID, userID, action, resType, resID string
            detail, ip, deviceInfo, result        string
            createdAt                             time.Time
        )
        if err := rows.Scan(&logID, &userID, &action, &resType, &resID,
            &detail, &ip, &deviceInfo, &result, &createdAt); err != nil {
            log.Error("scan user audit_log row failed", "err", err)
            continue
        }
        logs = append(logs, &pb.AuditLog{
            LogId:        logID,
            UserId:       userID,
            Action:       common.AuditActionFromString(action),
            ResourceType: resType,
            ResourceId:   resID,
            Detail:       detail,
            ClientIp:     ip,
            UserAgent:    deviceInfo,
            CreatedAt:    createdAt.UnixMilli(),
        })
    }

    // ==================== 5. 返回 ====================
    totalPages := int32(total) / pageSize
    if int32(total)%pageSize > 0 {
        totalPages++
    }

    return &pb.GetUserAuditLogsResponse{
        Meta: successMeta(ctx),
        Logs: logs,
        Pagination: &common.PaginationResult{
            Page:       page,
            PageSize:   pageSize,
            Total:      total,
            TotalPages: totalPages,
        },
    }, nil
}
```

---

### 6. ContentModeration — 提交内容审核

> **内容审核核心接口**。接收文本/图片/视频等内容，执行多级审核流水线：  
> 1. **关键词过滤**（同步，毫秒级）— 命中黑名单关键词直接拒绝  
> 2. **外部 AI 审核 API**（异步，秒级）— 调用第三方 AI 内容安全接口  
> 3. **人工复审队列**（异步，分钟~小时级）— AI 不确定的内容推入人工队列  
>
> 默认返回同步关键词过滤结果，AI 审核结果通过 Kafka 事件 audit.moderation.result 异步通知。  
> 客户端可通过 GetModerationResult 轮询最终结果。

```go
func (s *AuditService) ContentModeration(ctx context.Context, req *pb.ContentModerationRequest) (*pb.ContentModerationResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.ContentType == "" {
        return nil, status.Error(codes.InvalidArgument, "content_type is required")
    }
    if req.ContentId == "" {
        return nil, status.Error(codes.InvalidArgument, "content_id is required")
    }
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id is required")
    }
    // 文本审核必须有内容，媒体审核必须有 URL
    supportedTypes := map[string]bool{"text": true, "image": true, "video": true, "voice": true, "nickname": true, "group_name": true, "avatar": true}
    if !supportedTypes[req.ContentType] {
        return nil, status.Error(codes.InvalidArgument, "unsupported content_type")
    }
    if (req.ContentType == "text" || req.ContentType == "nickname" || req.ContentType == "group_name") && req.Content == "" {
        return nil, status.Error(codes.InvalidArgument, "content is required for text-based moderation")
    }
    if (req.ContentType == "image" || req.ContentType == "video" || req.ContentType == "voice" || req.ContentType == "avatar") && req.Content == "" {
        return nil, status.Error(codes.InvalidArgument, "content (media_url) is required for media moderation")
    }

    // ==================== 2. 频率限制 — Redis ====================
    // 同一用户短时间内不能提交过多审核请求
    rateKey := fmt.Sprintf("audit:rate:%s:moderation", req.UserId)
    count, _ := s.redis.Incr(ctx, rateKey).Result()
    if count == 1 {
        s.redis.Expire(ctx, rateKey, 1*time.Minute)
    }
    if count > 60 { // 每分钟最多 60 次
        return nil, status.Error(codes.ResourceExhausted, "moderation rate limit exceeded")
    }

    // ==================== 3. 生成审核请求 ID — Snowflake ====================
    requestID := s.snowflake.Generate().String()

    // ==================== 4. 获取用户上下文 — RPC User.GetUser ====================
    // 获取用户信息用于辅助审核决策（如历史违规用户需更严格审核）
    var userViolationCount int64
    violationKey := fmt.Sprintf("audit:user_violation_count:%s", req.UserId)
    userViolationCount, _ = s.redis.Get(ctx, violationKey).Int64()

    // 获取用户基本信息（用于审核上下文，非阻塞）
    userInfo, err := s.userClient.GetUser(ctx, &user_pb.GetUserRequest{UserId: req.UserId})
    if err != nil {
        log.Warn("get user info for moderation context failed, continue without it",
            "user_id", req.UserId, "err", err)
        // 非阻塞，获取失败不影响审核流程
    }

    // ==================== 5. 第一级：关键词过滤（同步） ====================
    var syncResult string
    var syncReason string
    var syncConfidence float64
    var hitKeyword bool

    if req.ContentType == "text" || req.ContentType == "nickname" || req.ContentType == "group_name" {
        // 关键词黑名单匹配（内存 Aho-Corasick 算法，毫秒级）
        matched, keyword, category := s.keywordFilter.Match(req.Content)
        if matched {
            hitKeyword = true
            syncResult = category       // SPAM / PORN / VIOLENCE / POLITICAL / AD
            syncReason = fmt.Sprintf("命中关键词: %s", keyword)
            syncConfidence = 1.0        // 关键词匹配置信度 100%
            log.Info("keyword filter hit",
                "request_id", requestID, "user_id", req.UserId,
                "keyword", keyword, "category", category)
        }
    }

    // ==================== 6. 写入审核记录 — PgSQL ====================
    initialStatus := 0 // PENDING
    initialResult := ""
    if hitKeyword {
        initialStatus = 3 // REJECT（关键词命中直接拒绝）
        initialResult = syncResult
    }

    mediaURL := ""
    textContent := ""
    if req.ContentType == "image" || req.ContentType == "video" || req.ContentType == "voice" || req.ContentType == "avatar" {
        mediaURL = req.Content
    } else {
        textContent = req.Content
    }

    _, err = s.db.ExecContext(ctx,
        `INSERT INTO moderation_records (request_id, content_type, content, media_url, user_id, source, status, result, reason, confidence, created_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NOW())`,
        requestID,
        req.ContentType,
        textContent,
        mediaURL,
        req.UserId,
        req.Scene,       // 审核场景：chat / profile / group_name / media_upload
        initialStatus,
        initialResult,
        syncReason,
        syncConfidence,
    )
    if err != nil {
        log.Error("insert moderation_record failed", "request_id", requestID, "err", err)
        return nil, status.Error(codes.Internal, "create moderation record failed")
    }

    // ==================== 7. 关键词命中 → 直接处理违规 ====================
    if hitKeyword {
        // 7a. 写入 Redis 缓存审核结果
        resultKey := fmt.Sprintf("audit:moderation:%s", requestID)
        s.redis.HSet(ctx, resultKey, map[string]interface{}{
            "request_id": requestID,
            "status":     3,               // REJECT
            "result":     syncResult,
            "reason":     syncReason,
            "confidence": syncConfidence,
        })
        s.redis.Expire(ctx, resultKey, 24*time.Hour)

        // 7b. 累计用户违规次数
        s.redis.Incr(ctx, violationKey)
        s.redis.Expire(ctx, violationKey, 30*24*time.Hour)
        newViolationCount := userViolationCount + 1

        // 7c. 投递审核结果事件 — Kafka audit.moderation.result
        moderationResultEvent := &kafka_audit.ModerationResultEvent{
            Header:      buildEventHeader("audit", s.instanceID),
            ModerationId: requestID,
            ContentId:   req.ContentId,
            ContentType: contentTypeToProto(req.ContentType),
            Result:      common.MODERATION_RESULT_REJECT,
            ViolationType: violationTypeFromCategory(syncResult),
            Confidence:  syncConfidence,
            Detail:      syncReason,
            IsAuto:      true,
            AuditTime:   time.Now().UnixMilli(),
            LatencyMs:   0, // 关键词过滤延迟忽略不计
        }
        s.kafka.Produce(ctx, "audit.moderation.result", req.ContentId, moderationResultEvent)

        // 7d. 投递内容违规事件 — Kafka audit.content.violation
        violationEvent := &kafka_audit.ContentViolationEvent{
            Header:         buildEventHeader("audit", s.instanceID),
            ViolationId:    s.snowflake.Generate().String(),
            UserId:         req.UserId,
            ContentId:      req.ContentId,
            ContentType:    contentTypeToProto(req.ContentType),
            ViolationType:  violationTypeFromCategory(syncResult),
            Action:         "delete", // 关键词命中直接删除
            Detail:         syncReason,
            ViolationCount: int32(newViolationCount),
            HandleTime:     time.Now().UnixMilli(),
        }
        s.kafka.Produce(ctx, "audit.content.violation", req.UserId, violationEvent)

        // 7e. 违规次数达阈值 → 投递用户违规升级事件 — Kafka audit.user.violation
        if newViolationCount >= 3 {
            var banAction string
            var banDuration int64
            switch {
            case newViolationCount >= 10:
                banAction = "ban"
                banDuration = 0 // 永久封禁
            case newViolationCount >= 5:
                banAction = "ban"
                banDuration = 7 * 24 * 3600 // 封禁 7 天
            case newViolationCount >= 3:
                banAction = "mute"
                banDuration = 24 * 3600 // 禁言 24 小时
            }

            userViolationEvent := &kafka_audit.UserViolationEvent{
                Header:         buildEventHeader("audit", s.instanceID),
                ViolationId:    s.snowflake.Generate().String(),
                UserId:         req.UserId,
                ViolationType:  violationTypeFromCategory(syncResult),
                Action:         banAction,
                Reason:         fmt.Sprintf("累计违规 %d 次", newViolationCount),
                ViolationCount: int32(newViolationCount),
                BanDuration:    banDuration,
                HandleTime:     time.Now().UnixMilli(),
            }
            if banDuration > 0 {
                userViolationEvent.ExpireAt = time.Now().Add(time.Duration(banDuration) * time.Second).UnixMilli()
            }
            s.kafka.Produce(ctx, "audit.user.violation", req.UserId, userViolationEvent)

            log.Warn("user violation escalated",
                "user_id", req.UserId, "count", newViolationCount, "action", banAction)
        }

        // 返回同步拒绝结果
        return &pb.ContentModerationResponse{
            Meta: successMeta(ctx),
            Record: &pb.ModerationRecord{
                RecordId:    requestID,
                ContentType: req.ContentType,
                ContentId:   req.ContentId,
                Content:     req.Content,
                Result:      common.MODERATION_RESULT_REJECT,
                Reason:      syncReason,
                Labels:      []string{syncResult},
                Confidence:  syncConfidence,
                CreatedAt:   time.Now().UnixMilli(),
            },
        }, nil
    }

    // ==================== 8. 未命中关键词 → 异步 AI 审核 ====================
    // 更新状态为 PROCESSING
    s.db.ExecContext(ctx,
        `UPDATE moderation_records SET status = 1 WHERE request_id = $1`,
        requestID,
    )

    // 写入 Redis 缓存（状态=处理中）
    resultKey := fmt.Sprintf("audit:moderation:%s", requestID)
    s.redis.HSet(ctx, resultKey, map[string]interface{}{
        "request_id": requestID,
        "status":     1, // PROCESSING
        "result":     "",
    })
    s.redis.Expire(ctx, resultKey, 24*time.Hour)

    // 更新审核队列大小监控
    s.redis.Incr(ctx, "audit:moderation_queue_size")

    // 异步提交 AI 审核（不阻塞 RPC 返回）
    go func() {
        asyncCtx := context.Background()
        startTime := time.Now()

        var aiResult *AIModerateResult
        var aiErr error

        switch req.ContentType {
        case "text", "nickname", "group_name":
            // 文本 AI 审核
            aiResult, aiErr = s.aiModerator.ModerateText(asyncCtx, req.Content, &ModerationContext{
                UserID:         req.UserId,
                Scene:          req.Scene,
                ViolationCount: int(userViolationCount),
                UserNickname:   getUserNickname(userInfo),
            })
        case "image", "avatar":
            // 图片 AI 审核
            aiResult, aiErr = s.aiModerator.ModerateImage(asyncCtx, mediaURL, &ModerationContext{
                UserID:         req.UserId,
                Scene:          req.Scene,
                ViolationCount: int(userViolationCount),
            })
        case "video":
            // 视频 AI 审核（截帧 + 逐帧检测）
            aiResult, aiErr = s.aiModerator.ModerateVideo(asyncCtx, mediaURL, &ModerationContext{
                UserID:         req.UserId,
                Scene:          req.Scene,
                ViolationCount: int(userViolationCount),
            })
        case "voice":
            // 语音 AI 审核（语音转文本 + 文本审核）
            aiResult, aiErr = s.aiModerator.ModerateVoice(asyncCtx, mediaURL, &ModerationContext{
                UserID:         req.UserId,
                Scene:          req.Scene,
                ViolationCount: int(userViolationCount),
            })
        }

        latencyMs := time.Since(startTime).Milliseconds()

        // 减少队列计数
        s.redis.Decr(asyncCtx, "audit:moderation_queue_size")

        if aiErr != nil {
            // AI 审核失败 → 标记为需人工复审
            log.Error("AI moderation failed, fallback to manual review",
                "request_id", requestID, "content_type", req.ContentType, "err", aiErr)

            s.db.ExecContext(asyncCtx,
                `UPDATE moderation_records SET status = 4, reason = $1 WHERE request_id = $2`,
                fmt.Sprintf("AI 审核失败: %v", aiErr), requestID,
            )
            // 延迟双删 — 审核状态即将变更，使缓存失效
            s.delayedDoubleDelete(asyncCtx, resultKey)
            s.redis.HSet(asyncCtx, resultKey, map[string]interface{}{
                "status": 4, // REVIEW
                "result": "",
                "reason": fmt.Sprintf("AI 审核失败: %v", aiErr),
            })
            return
        }

        // ---- AI 审核成功，处理结果 ----
        var finalStatus int
        var finalResult string
        var moderationProtoResult common.ModerationResult

        if aiResult.Confidence >= 0.9 && aiResult.Result == "PASS" {
            // 高置信度通过
            finalStatus = 2 // PASS
            finalResult = "PASS"
            moderationProtoResult = common.MODERATION_RESULT_PASS
        } else if aiResult.Confidence >= 0.8 && aiResult.Result != "PASS" {
            // 高置信度违规 → 直接拒绝
            finalStatus = 3 // REJECT
            finalResult = aiResult.Result
            moderationProtoResult = common.MODERATION_RESULT_REJECT
        } else {
            // 低置信度 → 人工复审
            finalStatus = 4 // REVIEW
            finalResult = aiResult.Result
            moderationProtoResult = common.MODERATION_RESULT_REVIEW
        }

        // 延迟双删 — 审核状态即将变更，使缓存失效
        s.delayedDoubleDelete(asyncCtx, resultKey)

        // 更新 PgSQL
        s.db.ExecContext(asyncCtx,
            `UPDATE moderation_records
             SET status = $1, result = $2, reason = $3, confidence = $4
             WHERE request_id = $5`,
            finalStatus, finalResult, aiResult.Reason, aiResult.Confidence, requestID,
        )

        // 更新 Redis 缓存
        s.redis.HSet(asyncCtx, resultKey, map[string]interface{}{
            "status":     finalStatus,
            "result":     finalResult,
            "reason":     aiResult.Reason,
            "confidence": aiResult.Confidence,
        })

        // 投递审核结果事件 — Kafka audit.moderation.result
        resultEvent := &kafka_audit.ModerationResultEvent{
            Header:       buildEventHeader("audit", s.instanceID),
            ModerationId: requestID,
            ContentId:    req.ContentId,
            ContentType:  contentTypeToProto(req.ContentType),
            Result:       moderationProtoResult,
            Confidence:   aiResult.Confidence,
            Detail:       aiResult.Reason,
            IsAuto:       true,
            AuditTime:    time.Now().UnixMilli(),
            LatencyMs:    latencyMs,
        }
        if finalResult != "PASS" {
            resultEvent.ViolationType = violationTypeFromCategory(finalResult)
        }
        s.kafka.Produce(asyncCtx, "audit.moderation.result", req.ContentId, resultEvent)

        // 如果是拒绝 → 投递违规事件 + 累计违规计数
        if finalStatus == 3 {
            s.redis.Incr(asyncCtx, violationKey)
            s.redis.Expire(asyncCtx, violationKey, 30*24*time.Hour)
            newCount, _ := s.redis.Get(asyncCtx, violationKey).Int64()

            violationEvent := &kafka_audit.ContentViolationEvent{
                Header:         buildEventHeader("audit", s.instanceID),
                ViolationId:    s.snowflake.Generate().String(),
                UserId:         req.UserId,
                ContentId:      req.ContentId,
                ContentType:    contentTypeToProto(req.ContentType),
                ViolationType:  violationTypeFromCategory(finalResult),
                Action:         "delete",
                Detail:         aiResult.Reason,
                ViolationCount: int32(newCount),
                HandleTime:     time.Now().UnixMilli(),
            }
            s.kafka.Produce(asyncCtx, "audit.content.violation", req.UserId, violationEvent)

            // 违规升级检查
            if newCount >= 3 {
                s.escalateUserViolation(asyncCtx, req.UserId, finalResult, int(newCount))
            }
        }

        log.Info("AI moderation completed",
            "request_id", requestID, "result", finalResult,
            "confidence", aiResult.Confidence, "latency_ms", latencyMs)
    }()

    // ==================== 9. 返回异步处理中状态 ====================
    return &pb.ContentModerationResponse{
        Meta: successMeta(ctx),
        Record: &pb.ModerationRecord{
            RecordId:    requestID,
            ContentType: req.ContentType,
            ContentId:   req.ContentId,
            Content:     req.Content,
            Result:      common.MODERATION_RESULT_UNSPECIFIED, // 异步审核中，结果待定
            Confidence:  0,
            CreatedAt:   time.Now().UnixMilli(),
        },
    }, nil
}

// escalateUserViolation 违规升级处理（抽取公共方法）
func (s *AuditService) escalateUserViolation(ctx context.Context, userID, category string, count int) {
    var banAction string
    var banDuration int64
    switch {
    case count >= 10:
        banAction = "ban"
        banDuration = 0 // 永久封禁
    case count >= 5:
        banAction = "ban"
        banDuration = 7 * 24 * 3600
    case count >= 3:
        banAction = "mute"
        banDuration = 24 * 3600
    }

    event := &kafka_audit.UserViolationEvent{
        Header:         buildEventHeader("audit", s.instanceID),
        ViolationId:    s.snowflake.Generate().String(),
        UserId:         userID,
        ViolationType:  violationTypeFromCategory(category),
        Action:         banAction,
        Reason:         fmt.Sprintf("累计违规 %d 次", count),
        ViolationCount: int32(count),
        BanDuration:    banDuration,
        HandleTime:     time.Now().UnixMilli(),
    }
    if banDuration > 0 {
        event.ExpireAt = time.Now().Add(time.Duration(banDuration) * time.Second).UnixMilli()
    }
    s.kafka.Produce(ctx, "audit.user.violation", userID, event)
}
```

---

### 7. GetModerationResult — 获取审核结果

> 根据 request_id（即 record_id）查询审核结果。优先从 Redis 缓存读取，缓存未命中回查 PgSQL。  
> 客户端在 ContentModeration 返回异步状态后，通过此接口轮询最终审核结果。

```go
func (s *AuditService) GetModerationResult(ctx context.Context, req *pb.GetModerationResultRequest) (*pb.GetModerationResultResponse, error) {
    // ==================== 1. 参数校验 ====================
    if req.RecordId == "" {
        return nil, status.Error(codes.InvalidArgument, "record_id is required")
    }

    // ==================== 2. 优先读 Redis 缓存 ====================
    resultKey := fmt.Sprintf("audit:moderation:%s", req.RecordId)
    cached, err := s.redis.HGetAll(ctx, resultKey).Result()
    if err == nil && len(cached) > 0 {
        // 缓存命中
        statusVal, _ := strconv.Atoi(cached["status"])
        confidence, _ := strconv.ParseFloat(cached["confidence"], 64)

        // 将 status 转为 ModerationResult 枚举
        var result common.ModerationResult
        switch statusVal {
        case 2: // PASS
            result = common.MODERATION_RESULT_PASS
        case 3: // REJECT
            result = common.MODERATION_RESULT_REJECT
        case 4: // REVIEW
            result = common.MODERATION_RESULT_REVIEW
        default:
            result = common.MODERATION_RESULT_UNSPECIFIED // PENDING / PROCESSING
        }

        // 收集 labels
        var labels []string
        if cached["result"] != "" && cached["result"] != "PASS" {
            labels = []string{cached["result"]}
        }

        return &pb.GetModerationResultResponse{
            Meta: successMeta(ctx),
            Record: &pb.ModerationRecord{
                RecordId:   req.RecordId,
                Result:     result,
                Reason:     cached["reason"],
                Labels:     labels,
                Confidence: confidence,
            },
        }, nil
    }

    // ==================== 3. 缓存未命中 → 回查 PgSQL ====================
    var (
        requestID, contentType, contentID, content string
        statusVal                                  int
        resultStr, reason, reviewedBy              string
        confidence                                 float64
        createdAt                                  time.Time
        reviewedAt                                 sql.NullTime
    )
    err = s.db.QueryRowContext(ctx,
        `SELECT request_id, content_type, content, status, result, reason, confidence, reviewed_by, reviewed_at, created_at
         FROM moderation_records
         WHERE request_id = $1`,
        req.RecordId,
    ).Scan(&requestID, &contentType, &content, &statusVal, &resultStr,
        &reason, &confidence, &reviewedBy, &reviewedAt, &createdAt)

    if err == sql.ErrNoRows {
        return nil, status.Error(codes.NotFound, "moderation record not found")
    }
    if err != nil {
        log.Error("query moderation_record failed", "record_id", req.RecordId, "err", err)
        return nil, status.Error(codes.Internal, "query moderation record failed")
    }

    // ==================== 4. 回填 Redis 缓存 ====================
    s.redis.HSet(ctx, resultKey, map[string]interface{}{
        "request_id":  requestID,
        "status":      statusVal,
        "result":      resultStr,
        "reason":      reason,
        "confidence":  confidence,
        "reviewed_by": reviewedBy,
    })
    s.redis.Expire(ctx, resultKey, 24*time.Hour)

    // ==================== 5. 转换并返回 ====================
    var moderationResult common.ModerationResult
    switch statusVal {
    case 2:
        moderationResult = common.MODERATION_RESULT_PASS
    case 3:
        moderationResult = common.MODERATION_RESULT_REJECT
    case 4:
        moderationResult = common.MODERATION_RESULT_REVIEW
    default:
        moderationResult = common.MODERATION_RESULT_UNSPECIFIED
    }

    var labels []string
    if resultStr != "" && resultStr != "PASS" {
        labels = []string{resultStr}
    }

    return &pb.GetModerationResultResponse{
        Meta: successMeta(ctx),
        Record: &pb.ModerationRecord{
            RecordId:    requestID,
            ContentType: contentType,
            ContentId:   contentID,
            Content:     content,
            Result:      moderationResult,
            Reason:      reason,
            Labels:      labels,
            Confidence:  confidence,
            CreatedAt:   createdAt.UnixMilli(),
        },
    }, nil
}
```

---

## 辅助函数

```go
// delayedDoubleDelete 延迟双删工具函数
// Phase 1: 立即删除缓存（在 DB 写入前调用，防止旧缓存被读取）
// Phase 2: 延迟 500ms 后再次删除缓存（防止并发读在 DB 写入窗口期回填旧数据）
func (s *AuditService) delayedDoubleDelete(ctx context.Context, keys ...string) {
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

// contentTypeToProto 将字符串 content_type 转为 proto 枚举
func contentTypeToProto(ct string) kafka_audit.ContentType {
    switch ct {
    case "text":
        return kafka_audit.CONTENT_TYPE_TEXT
    case "image":
        return kafka_audit.CONTENT_TYPE_IMAGE
    case "video":
        return kafka_audit.CONTENT_TYPE_VIDEO
    case "voice":
        return kafka_audit.CONTENT_TYPE_VOICE
    case "nickname":
        return kafka_audit.CONTENT_TYPE_NICKNAME
    case "group_name":
        return kafka_audit.CONTENT_TYPE_GROUP_NAME
    case "avatar":
        return kafka_audit.CONTENT_TYPE_AVATAR
    default:
        return kafka_audit.CONTENT_TYPE_UNSPECIFIED
    }
}

// violationTypeFromCategory 将审核结果分类转为违规类型枚举
func violationTypeFromCategory(category string) kafka_audit.ViolationType {
    switch category {
    case "SPAM":
        return kafka_audit.VIOLATION_TYPE_SPAM
    case "PORN":
        return kafka_audit.VIOLATION_TYPE_PORN
    case "VIOLENCE":
        return kafka_audit.VIOLATION_TYPE_VIOLENCE
    case "POLITICAL":
        return kafka_audit.VIOLATION_TYPE_POLITICS
    case "AD":
        return kafka_audit.VIOLATION_TYPE_AD
    default:
        return kafka_audit.VIOLATION_TYPE_OTHER
    }
}

// getUserNickname 安全获取用户昵称
func getUserNickname(resp *user_pb.GetUserResponse) string {
    if resp != nil && resp.User != nil {
        return resp.User.Nickname
    }
    return ""
}

// buildEventHeader 构建 Kafka 事件头
func buildEventHeader(source, instanceID string) *common.EventHeader {
    return &common.EventHeader{
        EventId:   uuid.NewString(),
        TraceId:   trace.SpanContextFromContext(ctx).TraceID().String(),
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
        TraceId:    trace.SpanContextFromContext(ctx).TraceID().String(),
    }
}

// AIModerateResult AI 审核结果结构
type AIModerateResult struct {
    Result     string  // PASS / SPAM / PORN / VIOLENCE / POLITICAL / AD / OTHER
    Reason     string  // 审核原因说明
    Confidence float64 // 置信度 0.0~1.0
    Labels     []string // 命中标签
}

// ModerationContext 审核上下文
type ModerationContext struct {
    UserID         string
    Scene          string
    ViolationCount int
    UserNickname   string
}
```

---

## 可观测性接入（OpenTelemetry）

> Audit 服务负责审计日志与内容审核，需重点观测：审核延迟（含外部 AI API 调用）、审核结果分布、批量写入性能、违规检出率。

### 第一步：服务启动时初始化 OTel SDK

```go
func main() {
    ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
    defer cancel()

    otelShutdown := observability.InitOpenTelemetry(ctx, "audit", "v1.0.0")
    defer otelShutdown(ctx)
    observability.SetupLogger("audit")
    // ...
}
```

### 第二步：gRPC Server/Client Interceptor

```go
grpcServer := grpc.NewServer(
    grpc.ChainUnaryInterceptor(otelgrpc.UnaryServerInterceptor()),
    grpc.ChainStreamInterceptor(otelgrpc.StreamServerInterceptor()), // 统一添加 Stream Interceptor
)
// Client — 调用 User.GetUser / Config.GetConfig
userConn := grpc.Dial("user:50051",
    grpc.WithChainUnaryInterceptor(otelgrpc.UnaryClientInterceptor()),
)
```

### 第三步：注册 Audit 专属业务指标

```go
var meter = otel.Meter("im-chat/audit")

var (
    // 内容审核总数
    contentScanned, _ = meter.Int64Counter("audit.content_scanned_total",
        metric.WithDescription("内容审核扫描总数"))

    // 违规检出数
    violationsDetected, _ = meter.Int64Counter("audit.violations_detected_total",
        metric.WithDescription("违规内容检出总数"))

    // AI 审核 API 调用延迟
    aiModerationLatency, _ = meter.Float64Histogram("audit.ai_moderation_latency_ms",
        metric.WithDescription("外部 AI 审核 API 调用延迟"), metric.WithUnit("ms"))

    // 批量审计日志写入延迟
    batchWriteLatency, _ = meter.Float64Histogram("audit.batch_write_latency_ms",
        metric.WithDescription("批量审计日志 PgSQL 写入延迟"), metric.WithUnit("ms"))

    // 审核结果分布
    moderationResult, _ = meter.Int64Counter("audit.moderation_result_total",
        metric.WithDescription("审核结果分布（passed/rejected/review）"))
)
```

在业务代码中埋点：

```go
// ContentModeration 中 — AI API 调用
aiStart := time.Now()
result, err := s.aiClient.Moderate(ctx, content)
aiModerationLatency.Record(ctx, float64(time.Since(aiStart).Milliseconds()))
contentScanned.Add(ctx, 1)
if result.IsViolation {
    violationsDetected.Add(ctx, 1, metric.WithAttributes(
        attribute.String("violation_type", result.Label),
    ))
}
moderationResult.Add(ctx, 1, metric.WithAttributes(
    attribute.String("result", result.Action), // passed / rejected / review
))

// BatchCreateAuditLog 中 — 批量写入
batchStart := time.Now()
err := s.db.BatchInsert(ctx, logs)
batchWriteLatency.Record(ctx, float64(time.Since(batchStart).Milliseconds()))
```

### 第四步：Kafka Producer/Consumer 埋点

发布 `audit.moderation.result` / 消费 `msg.moderation.request` 事件时，创建 span + 注入/提取 trace context。

### 第五步：结构化 JSON 日志 + buildEventHeader 改造

日志设置与所有服务一致。`buildEventHeader` 改为使用共享包：

```go
header := observability.BuildEventHeader(ctx, "audit", instanceID)
```

### 补充：基础设施指标注册

Audit 服务依赖 PgSQL（审计日志存储）+ Redis（缓存），需注册连接池指标：

```go
func main() {
    // ... initOpenTelemetry ...
    observability.RegisterDBPoolMetrics(db, "audit")
    observability.RegisterRedisPoolMetrics(redisClient, "audit")
}
```

### 补充：Span 错误记录

在所有返回 error 的业务路径中记录 span 错误（AI API 调用、DB 批量写入等高错误率操作尤其重要）：

```go
func (s *AuditServer) ContentModeration(ctx context.Context, req *pb.ContentModerationRequest) (*pb.ContentModerationResponse, error) {
    ctx, span := otel.Tracer("audit").Start(ctx, "ContentModeration")
    defer span.End()

    result, err := s.doContentModeration(ctx, req)
    if err != nil {
        observability.RecordError(span, err)
        return nil, err
    }
    return result, nil
}
```

### 接入要点总结

| 步骤 | 改动位置 | 效果 |
|------|---------|------|
| 1. `observability.InitOpenTelemetry` | main() | TracerProvider + MeterProvider + Runtime Metrics |
| 2. gRPC Interceptor | Server(Unary+Stream) + Client | RPC 自动 trace |
| 3. 自定义业务指标 | ContentModeration/BatchCreate | AI 审核延迟、违规检出率、批量写入性能 |
| 4. Kafka 埋点 | 事件发布/消费 | 审核链路追踪 |
| 5. 日志 + EventHeader | `observability.BuildEventHeader` | trace_id 贯穿 |
| 6. 基础设施指标 | `RegisterDBPoolMetrics` + `RegisterRedisPoolMetrics` | DB/Redis 连接池健康 |
| 7. Span 错误记录 | `observability.RecordError` | 错误 trace 在 Tempo 可见 |
