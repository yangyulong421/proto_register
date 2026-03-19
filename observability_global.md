# 可观测性全局接入指南（跨服务公共事项）

> 本文档描述 **除单个服务内部接入外**，还需要完成的所有全局性、跨服务性工作。  
> 各服务内部的 OTel SDK 初始化、gRPC Interceptor、自定义业务指标、Kafka 埋点等步骤请参阅各服务的 `rpc/<service>/<service>_impl.md` 中的"可观测性接入（OpenTelemetry）"章节。

---

## 一、共享 Go 公共包（pkg/observability）

所有 18 个服务的 OTel 初始化、日志包装、Kafka trace 注入逻辑完全一致，**必须抽成共享包**，避免 18 份拷贝。

### 1.1 initOpenTelemetry —— 统一初始化函数

```go
// pkg/observability/otel.go
package observability

import (
    "context"
    "fmt"
    "log/slog"
    "os"
    "time"

    "go.opentelemetry.io/otel"
    "go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
    "go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
    // "go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"  // 启用 LoggerProvider 时取消注释
    "go.opentelemetry.io/otel/propagation"
    "go.opentelemetry.io/otel/sdk/resource"
    sdktrace "go.opentelemetry.io/otel/sdk/trace"
    sdkmetric "go.opentelemetry.io/otel/sdk/metric"
    // sdklog "go.opentelemetry.io/otel/sdk/log"  // 启用 LoggerProvider 时取消注释
    "go.opentelemetry.io/contrib/instrumentation/runtime"
    semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
)

// InitOpenTelemetry 初始化 Trace + Metrics + Logs 三大 Provider
// 所有 18 个服务在 main() 中调用此函数，仅 serviceName 和 version 不同
// 返回 shutdown 函数，在服务退出时调用以刷出剩余数据
func InitOpenTelemetry(ctx context.Context, serviceName, version string) func(context.Context) {
    collectorEndpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
    if collectorEndpoint == "" {
        collectorEndpoint = "otel-agent:4317" // 默认 sidecar agent 地址
    }

    res, err := resource.Merge(
        resource.Default(),
        resource.NewWithAttributes(
            semconv.SchemaURL,
            semconv.ServiceNameKey.String(serviceName),
            semconv.ServiceVersionKey.String(version),
            semconv.DeploymentEnvironmentKey.String(os.Getenv("DEPLOY_ENV")),
            semconv.ServiceInstanceIDKey.String(os.Getenv("HOSTNAME")),
        ),
    )
    if err != nil {
        panic(fmt.Sprintf("failed to create resource: %v", err))
    }

    // ⚠️ 关键：注册 W3C TraceContext + Baggage 传播器
    // 不设置此项，Kafka/HTTP 跨服务 trace 传播将完全失效
    otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
        propagation.TraceContext{},
        propagation.Baggage{},
    ))

    // —— TracerProvider ——
    traceExporter, err := otlptracegrpc.New(ctx,
        otlptracegrpc.WithEndpoint(collectorEndpoint),
        otlptracegrpc.WithInsecure(),
        otlptracegrpc.WithRetry(otlptracegrpc.RetryConfig{
            Enabled:         true,
            InitialInterval: 1 * time.Second,
            MaxInterval:     10 * time.Second,
        }),
    )
    if err != nil {
        panic(fmt.Sprintf("failed to create trace exporter: %v", err))
    }
    tp := sdktrace.NewTracerProvider(
        sdktrace.WithBatcher(traceExporter,
            sdktrace.WithMaxQueueSize(4096),
            sdktrace.WithMaxExportBatchSize(512),
        ),
        sdktrace.WithResource(res),
        // ⚠️ SDK 端使用 AlwaysSample，将采样决策交给 Gateway 的 tail_sampling
        // 若 SDK 端 head sampling 0.1，错误/慢请求有 90% 概率在 SDK 端丢弃，Gateway 永远看不到
        sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.AlwaysSample())),
    )
    otel.SetTracerProvider(tp)

    // —— MeterProvider ——
    metricExporter, err := otlpmetricgrpc.New(ctx,
        otlpmetricgrpc.WithEndpoint(collectorEndpoint),
        otlpmetricgrpc.WithInsecure(),
    )
    if err != nil {
        panic(fmt.Sprintf("failed to create metric exporter: %v", err))
    }
    mp := sdkmetric.NewMeterProvider(
        sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExporter,
            sdkmetric.WithInterval(15*time.Second),
        )),
        sdkmetric.WithResource(res),
        sdkmetric.WithView(CustomHistogramViews()...), // 应用 1.7 定义的 IM 专属 bucket
    )
    otel.SetMeterProvider(mp)

    // —— LoggerProvider（按需启用） ——
    // ⚠️ 架构说明：
    // 当前日志路径：slog → JSON stdout → 容器日志 → OTel Collector filelog receiver → Loki
    // LoggerProvider 用于未来切换到 OTLP 直推模式（通过 otelslog bridge 桥接）
    // 如需启用直推：
    //   1. 取消以下注释创建 LoggerProvider
    //   2. 将 traceLogHandler 的 inner 替换为 otelslog.NewHandler(lp)
    //
    // 当前注释掉以避免创建未使用的 gRPC 连接和 BatchProcessor goroutine
    // logExporter, _ := otlploggrpc.New(ctx, ...)
    // lp := sdklog.NewLoggerProvider(sdklog.WithProcessor(sdklog.NewBatchProcessor(logExporter)), sdklog.WithResource(res))

    // —— Go Runtime 指标自动采集 ——
    // 自动上报: runtime.go.goroutines, runtime.go.mem.heap_alloc,
    //          runtime.go.gc.pause_ns, runtime.uptime 等
    if err := runtime.Start(runtime.WithMinimumReadMemStatsInterval(15 * time.Second)); err != nil {
        slog.Warn("failed to start runtime metrics", "err", err)
    }

    // shutdown 函数：每个 Provider 独立 5s 超时，避免前一个耗尽时间后续无法 flush
    return func(ctx context.Context) {
        tpCtx, tpCancel := context.WithTimeout(ctx, 5*time.Second)
        defer tpCancel()
        if err := tp.Shutdown(tpCtx); err != nil {
            slog.Error("trace provider shutdown failed", "err", err)
        }

        mpCtx, mpCancel := context.WithTimeout(ctx, 5*time.Second)
        defer mpCancel()
        if err := mp.Shutdown(mpCtx); err != nil {
            slog.Error("meter provider shutdown failed", "err", err)
        }

        // 若启用了 LoggerProvider，取消此注释
        // lpCtx, lpCancel := context.WithTimeout(ctx, 5*time.Second)
        // defer lpCancel()
        // if err := lp.Shutdown(lpCtx); err != nil {
        //     slog.Error("log provider shutdown failed", "err", err)
        // }
    }
}
```

### 1.2 traceLogHandler —— 自动注入 trace_id 的 slog Handler

```go
// pkg/observability/logger.go
package observability

import (
    "context"
    "log/slog"
    "os"

    "go.opentelemetry.io/otel/trace"
)

type traceLogHandler struct {
    inner slog.Handler
}

func (h *traceLogHandler) Handle(ctx context.Context, r slog.Record) error {
    spanCtx := trace.SpanContextFromContext(ctx)
    if spanCtx.IsValid() {
        r.AddAttrs(
            slog.String("trace_id", spanCtx.TraceID().String()),
            slog.String("span_id", spanCtx.SpanID().String()),
        )
    }
    return h.inner.Handle(ctx, r)
}

func (h *traceLogHandler) Enabled(ctx context.Context, level slog.Level) bool {
    return h.inner.Enabled(ctx, level)
}

func (h *traceLogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
    return &traceLogHandler{inner: h.inner.WithAttrs(attrs)}
}

func (h *traceLogHandler) WithGroup(name string) slog.Handler {
    return &traceLogHandler{inner: h.inner.WithGroup(name)}
}

// logLevel 全局动态日志级别，可通过 SetLogLevel 在运行时调整
var logLevel = new(slog.LevelVar)

// SetupLogger 设置全局 slog.Default 为结构化 JSON + trace context 注入 + 动态日志级别
func SetupLogger(serviceName string) {
    // 从环境变量读取初始日志级别
    switch os.Getenv("LOG_LEVEL") {
    case "debug":
        logLevel.Set(slog.LevelDebug)
    case "warn":
        logLevel.Set(slog.LevelWarn)
    case "error":
        logLevel.Set(slog.LevelError)
    default:
        logLevel.Set(slog.LevelInfo)
    }

    jsonHandler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
        Level: logLevel, // 动态级别，可通过 SetLogLevel() 运行时调整
    }).WithAttrs([]slog.Attr{
        slog.String("service", serviceName),
    })
    slog.SetDefault(slog.New(&traceLogHandler{inner: jsonHandler}))
}

// SetLogLevel 运行时动态调整日志级别（可通过 admin RPC 或环境变量触发）
func SetLogLevel(level slog.Level) {
    logLevel.Set(level)
}
```

### 1.3 kafkaHeaderCarrier —— Kafka trace context 传播

```go
// pkg/observability/kafka_carrier.go
package observability

import (
    "context"

    "github.com/segmentio/kafka-go"
    "go.opentelemetry.io/otel"
    "go.opentelemetry.io/otel/propagation"
    "go.opentelemetry.io/otel/trace"
    "go.opentelemetry.io/otel/attribute"
)

type KafkaHeaderCarrier []kafka.Header

func (c *KafkaHeaderCarrier) Get(key string) string {
    for _, h := range *c {
        if h.Key == key {
            return string(h.Value)
        }
    }
    return ""
}

func (c *KafkaHeaderCarrier) Set(key, val string) {
    for i, h := range *c {
        if h.Key == key {
            (*c)[i].Value = []byte(val)
            return
        }
    }
    *c = append(*c, kafka.Header{Key: key, Value: []byte(val)})
}

func (c *KafkaHeaderCarrier) Keys() []string {
    keys := make([]string, len(*c))
    for i, h := range *c {
        keys[i] = h.Key
    }
    return keys
}

// InjectKafkaHeaders 在 Kafka produce 前调用，将当前 span context 注入 headers
func InjectKafkaHeaders(ctx context.Context, headers *[]kafka.Header) {
    carrier := KafkaHeaderCarrier(*headers)
    otel.GetTextMapPropagator().Inject(ctx, &carrier)
    *headers = []kafka.Header(carrier)
}

// ExtractKafkaContext 在 Kafka consume 时调用，从 headers 还原 span context
func ExtractKafkaContext(ctx context.Context, headers []kafka.Header) context.Context {
    carrier := KafkaHeaderCarrier(headers)
    return otel.GetTextMapPropagator().Extract(ctx, &carrier)
}

// StartKafkaProducerSpan 创建 PRODUCER span 并注入 headers
func StartKafkaProducerSpan(ctx context.Context, topic string, headers *[]kafka.Header) (context.Context, trace.Span) {
    tracer := otel.Tracer("kafka-producer")
    ctx, span := tracer.Start(ctx, "kafka.produce."+topic,
        trace.WithSpanKind(trace.SpanKindProducer),
        trace.WithAttributes(attribute.String("messaging.system", "kafka"),
            attribute.String("messaging.destination.name", topic)),
    )
    InjectKafkaHeaders(ctx, headers)
    return ctx, span
}

// StartKafkaConsumerSpan 从 headers 还原 context 并创建 CONSUMER span
func StartKafkaConsumerSpan(ctx context.Context, topic string, headers []kafka.Header) (context.Context, trace.Span) {
    ctx = ExtractKafkaContext(ctx, headers)
    tracer := otel.Tracer("kafka-consumer")
    ctx, span := tracer.Start(ctx, "kafka.consume."+topic,
        trace.WithSpanKind(trace.SpanKindConsumer),
        trace.WithAttributes(attribute.String("messaging.system", "kafka"),
            attribute.String("messaging.destination.name", topic)),
    )
    return ctx, span
}
```

### 1.4 buildEventHeader 改造模板

```go
// pkg/observability/event_header.go
package observability

import (
    "context"
    "time"

    "github.com/google/uuid"
    "go.opentelemetry.io/otel/trace"
    "proto_register/common"
)

// BuildEventHeader 替代各服务的 buildEventHeader，从 ctx 提取真实 trace context
func BuildEventHeader(ctx context.Context, source, instanceID string) *common.EventHeader {
    spanCtx := trace.SpanContextFromContext(ctx)
    return &common.EventHeader{
        EventId:   uuid.NewString(),
        TraceId:   spanCtx.TraceID().String(),
        SpanId:    spanCtx.SpanID().String(),
        Source:    source,
        SourceId:  instanceID,
        Timestamp: time.Now().UnixMilli(),
        Version:   1,
    }
}
```

### 1.5 基础设施指标采集（DB / Redis / Kafka 连接池）

> Go runtime 指标已通过 `runtime.Start()` 自动上报（goroutine 数、GC 暂停、堆内存等）。  
> 以下是需要手动注册的 **基础设施层** 指标，所有 18 个服务共享。

```go
// pkg/observability/infra_metrics.go
package observability

import (
    "context"
    "database/sql"

    "github.com/redis/go-redis/v9"
    "go.opentelemetry.io/otel"
    "go.opentelemetry.io/otel/metric"
)

// RegisterDBPoolMetrics 注册 PgSQL 连接池可观测指标
// 通过 sql.DBStats 采集，每 15s 上报一次
func RegisterDBPoolMetrics(db *sql.DB, serviceName string) {
    meter := otel.Meter("im-chat/" + serviceName)

    dbOpenConns, _ := meter.Int64ObservableGauge("db.pool.open_connections",
        metric.WithDescription("当前打开的 DB 连接数"))
    dbMaxConns, _ := meter.Int64ObservableGauge("db.pool.max_connections",
        metric.WithDescription("DB 连接池最大连接数上限"))
    dbIdleConns, _ := meter.Int64ObservableGauge("db.pool.idle_connections",
        metric.WithDescription("当前空闲的 DB 连接数"))
    dbWaitCount, _ := meter.Int64ObservableCounter("db.pool.wait_count_total",
        metric.WithDescription("等待获取连接的累计次数"))
    dbWaitDuration, _ := meter.Float64ObservableCounter("db.pool.wait_duration_seconds_total",
        metric.WithDescription("等待获取连接的累计时间"))

    meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
        stats := db.Stats()
        o.ObserveInt64(dbOpenConns, int64(stats.OpenConnections))
        o.ObserveInt64(dbMaxConns, int64(stats.MaxOpenConnections))
        o.ObserveInt64(dbIdleConns, int64(stats.Idle))
        o.ObserveInt64(dbWaitCount, stats.WaitCount)
        o.ObserveFloat64(dbWaitDuration, stats.WaitDuration.Seconds())
        return nil
    }, dbOpenConns, dbMaxConns, dbIdleConns, dbWaitCount, dbWaitDuration)
}

// RegisterRedisPoolMetrics 注册 Redis 连接池可观测指标
func RegisterRedisPoolMetrics(rdb *redis.Client, serviceName string) {
    meter := otel.Meter("im-chat/" + serviceName)

    redisActiveConns, _ := meter.Int64ObservableGauge("redis.pool.active_connections",
        metric.WithDescription("当前活跃 Redis 连接数"))
    redisIdleConns, _ := meter.Int64ObservableGauge("redis.pool.idle_connections",
        metric.WithDescription("当前空闲 Redis 连接数"))
    redisTimeouts, _ := meter.Int64ObservableCounter("redis.pool.timeouts_total",
        metric.WithDescription("Redis 连接池超时次数"))

    meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
        stats := rdb.PoolStats()
        o.ObserveInt64(redisActiveConns, int64(stats.TotalConns-stats.IdleConns))
        o.ObserveInt64(redisIdleConns, int64(stats.IdleConns))
        o.ObserveInt64(redisTimeouts, int64(stats.Timeouts))
        return nil
    }, redisActiveConns, redisIdleConns, redisTimeouts)
}
```

**各服务 main() 中调用方式**：

```go
func main() {
    // ... initOpenTelemetry ...
    observability.RegisterDBPoolMetrics(db, "message")
    observability.RegisterRedisPoolMetrics(redisClient, "message")
}
```

### 1.6 Span 错误记录与状态设置

```go
// pkg/observability/span_helpers.go
package observability

import (
    "go.opentelemetry.io/otel/codes"
    "go.opentelemetry.io/otel/trace"
)

// RecordError 在当前 span 上记录错误事件并设置状态为 ERROR
// 供所有服务的业务代码使用：observability.RecordError(span, err)
func RecordError(span trace.Span, err error) {
    if err != nil {
        span.RecordError(err)
        span.SetStatus(codes.Error, err.Error())
    }
}
```

在业务代码中使用：

```go
func (s *MessageService) SendMessage(ctx context.Context, req *pb.SendMessageRequest) (*pb.SendMessageResponse, error) {
    ctx, span := otel.Tracer("message").Start(ctx, "SendMessage")
    defer span.End()

    result, err := s.doSendMessage(ctx, req)
    if err != nil {
        observability.RecordError(span, err) // ← 所有返回 error 的路径都应调用
        return nil, err
    }
    return result, nil
}
```

### 1.7 Histogram Bucket 自定义（关键延迟指标）

默认 OTel Histogram bucket 为 `[0, 5, 10, 25, 50, 75, 100, 250, 500, 750, 1000, 2500, 5000, 7500, 10000]`ms，对 IM 场景需要自定义：

```go
// pkg/observability/histogram_views.go
package observability

import (
    sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// CustomHistogramViews 返回 IM 系统专属的 Histogram bucket 配置
// 在创建 MeterProvider 时传入: sdkmetric.WithView(CustomHistogramViews()...)
//
// ⚠️ 注意：聚合配置使用 sdkmetric.AggregationExplicitBucketHistogram（View 配置类型），
//    不要与 metricdata.ExplicitBucketHistogram 混淆（后者是 export 数据结构）
func CustomHistogramViews() []sdkmetric.View {
    return []sdkmetric.View{
        // 极低延迟路径（seq 分配、Redis 操作、缓存查询）：0.5ms 精度
        sdkmetric.NewView(
            sdkmetric.Instrument{Name: "*seq_alloc*"},
            sdkmetric.Stream{Aggregation: sdkmetric.AggregationExplicitBucketHistogram{
                Boundaries: []float64{0.5, 1, 2, 5, 10, 20, 50, 100, 200, 500},
            }},
        ),
        sdkmetric.NewView(
            sdkmetric.Instrument{Name: "*redis*"},
            sdkmetric.Stream{Aggregation: sdkmetric.AggregationExplicitBucketHistogram{
                Boundaries: []float64{0.5, 1, 2, 5, 10, 20, 50, 100, 200, 500},
            }},
        ),
        // 消息端到端延迟（覆盖 50ms~30s 范围）
        sdkmetric.NewView(
            sdkmetric.Instrument{Name: "*e2e_latency*"},
            sdkmetric.Stream{Aggregation: sdkmetric.AggregationExplicitBucketHistogram{
                Boundaries: []float64{50, 100, 200, 500, 1000, 2000, 5000, 10000, 30000},
            }},
        ),
        // S3/外部 API 调用延迟（覆盖 10ms~60s 范围）
        sdkmetric.NewView(
            sdkmetric.Instrument{Name: "*s3_*"},
            sdkmetric.Stream{Aggregation: sdkmetric.AggregationExplicitBucketHistogram{
                Boundaries: []float64{10, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 30000, 60000},
            }},
        ),
        sdkmetric.NewView(
            sdkmetric.Instrument{Name: "*oauth*"},
            sdkmetric.Stream{Aggregation: sdkmetric.AggregationExplicitBucketHistogram{
                Boundaries: []float64{10, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 30000, 60000},
            }},
        ),
        sdkmetric.NewView(
            sdkmetric.Instrument{Name: "*ai_moderation*"},
            sdkmetric.Stream{Aggregation: sdkmetric.AggregationExplicitBucketHistogram{
                Boundaries: []float64{50, 100, 250, 500, 1000, 2000, 5000, 10000, 30000},
            }},
        ),
    }
}
```

**需在 InitOpenTelemetry 的 MeterProvider 创建中加入**：

```go
mp := sdkmetric.NewMeterProvider(
    sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExporter, ...)),
    sdkmetric.WithResource(res),
    sdkmetric.WithView(CustomHistogramViews()...), // ← 加入自定义 bucket
)
```

---

## 二、OTel Collector 部署（双层架构）

采用 **Agent + Gateway** 双层架构：

```
┌───────────────────────────────────────────────────────┐
│  Pod / VM                                             │
│  ┌─────────┐    OTLP gRPC     ┌──────────────────┐   │
│  │ Service  │ ───────────────→ │ OTel Agent       │   │
│  │  (SDK)   │    :4317 本地    │ (Sidecar/DaemonSet)│  │
│  └─────────┘                   └───────┬──────────┘   │
└────────────────────────────────────────┼──────────────┘
                                         │ OTLP gRPC
                                         ▼
                              ┌──────────────────────┐
                              │  OTel Gateway        │
                              │  (独立 Deployment)    │
                              └─────┬──────┬─────┬───┘
                                    │      │     │
                          ┌─────────┘      │     └──────────┐
                          ▼                ▼                 ▼
                    ┌──────────┐   ┌────────────┐   ┌────────────┐
                    │ Prometheus│   │   Loki     │   │   Tempo    │
                    │ (remote  │   │ (OTLP HTTP)│   │ (OTLP gRPC)│
                    │  write)  │   │            │   │            │
                    └──────────┘   └────────────┘   └────────────┘
```

### 2.1 Agent 配置（每个 Pod/VM 一个）

```yaml
# otel-agent-config.yaml
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: "0.0.0.0:4317"

processors:
  batch:
    timeout: 5s
    send_batch_size: 1024
  memory_limiter:
    check_interval: 1s
    limit_mib: 256
    spike_limit_mib: 64
  resource:
    attributes:
      - key: host.name
        from_attribute: host.name
        action: upsert

exporters:
  otlp/gateway:
    endpoint: "otel-gateway:4317"
    tls:
      insecure: true

service:
  pipelines:
    traces:
      receivers: [otlp]
      processors: [memory_limiter, batch]
      exporters: [otlp/gateway]
    metrics:
      receivers: [otlp]
      processors: [memory_limiter, batch]
      exporters: [otlp/gateway]
    logs:
      receivers: [otlp]
      processors: [memory_limiter, batch]
      exporters: [otlp/gateway]
```

### 2.2 Gateway 配置（集中转发）

```yaml
# otel-gateway-config.yaml
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: "0.0.0.0:4317"

processors:
  batch:
    timeout: 10s
    send_batch_size: 2048
  memory_limiter:
    check_interval: 1s
    limit_mib: 1024
    spike_limit_mib: 256
  tail_sampling:
    decision_wait: 10s
    policies:
      - name: error-sampling
        type: status_code
        status_code: { status_codes: [ERROR] }
      - name: slow-sampling
        type: latency
        latency: { threshold_ms: 500 }
      - name: default-sampling
        type: probabilistic
        probabilistic: { sampling_percentage: 10 }

exporters:
  prometheusremotewrite:
    endpoint: "http://prometheus:9090/api/v1/write"
    resource_to_telemetry_conversion:
      enabled: true
  otlphttp/loki:
    endpoint: "http://loki:3100/otlp"
  otlp/tempo:
    endpoint: "tempo:4317"
    tls:
      insecure: true

service:
  pipelines:
    traces:
      receivers: [otlp]
      processors: [memory_limiter, tail_sampling, batch]
      exporters: [otlp/tempo]
    metrics:
      receivers: [otlp]
      processors: [memory_limiter, batch]
      exporters: [prometheusremotewrite]
    logs:
      receivers: [otlp]
      processors: [memory_limiter, batch]
      exporters: [otlphttp/loki]
```

**关键设计点**：
- 服务不暴露 `/metrics` 端口，指标通过 OTLP push 到 Collector → Prometheus remote write
- 日志通过 OTLP push → Collector → Loki，不用 promtail
- Traces 经过 tail_sampling：ERROR / 慢请求 100% 采样，其余 10%
- ⚠️ **tail_sampling 要求同一 trace 的所有 span 到达同一个 Gateway 实例**。当 Gateway 多副本时，必须使用 **trace-ID-based 路由**（如 OTel Collector `loadbalancing` exporter 或 K8s Service 配合 consistent hashing），否则同一 trace 的 span 分散到不同实例，tail_sampling 将无法正确决策
- SDK 端采用 `AlwaysSample()`，将采样决策完全交给 Gateway tail_sampling（若 SDK 端 head sampling 10%，Gateway 将永远无法看到那 90% 的错误/慢请求）

---

## 三、Prometheus 配置

### 3.1 prometheus.yml

```yaml
global:
  scrape_interval: 15s
  evaluation_interval: 15s

# 不需要 scrape_configs！所有指标通过 remote_write 接入
# 仅保留自身 self-scrape 用于监控 Prometheus 自身健康
scrape_configs:
  - job_name: 'prometheus'
    static_configs:
      - targets: ['localhost:9090']

# 开启 remote write receiver
remote_write: []  # 用于联邦/长期存储可扩展

rule_files:
  - "alert_rules.yml"

alerting:
  alertmanagers:
    - static_configs:
        - targets: ['alertmanager:9093']
```

### 3.2 告警规则（alert_rules.yml）

```yaml
groups:
  - name: im-chat-service-alerts
    interval: 30s
    rules:
      # ---- gRPC 层通用告警 ----
      - alert: HighGrpcErrorRate
        expr: |
          sum(rate(rpc_server_duration_milliseconds_count{rpc_grpc_status_code!="0"}[5m])) by (service_name)
          /
          sum(rate(rpc_server_duration_milliseconds_count[5m])) by (service_name)
          > 0.05
        for: 3m
        labels:
          severity: critical
          team: backend
        annotations:
          summary: "{{ $labels.service_name }} gRPC 错误率超过 5%"
          runbook_url: "https://wiki.internal/runbooks/grpc-error-rate"
          dashboard_url: "https://grafana.internal/d/im-overview?var-service={{ $labels.service_name }}"

      - alert: HighGrpcLatencyP99
        expr: |
          histogram_quantile(0.99,
            sum(rate(rpc_server_duration_milliseconds_bucket[5m])) by (service_name, le)
          ) > 500
        for: 5m
        labels:
          severity: warning
          team: backend
        annotations:
          summary: "{{ $labels.service_name }} gRPC P99 延迟超过 500ms"
          runbook_url: "https://wiki.internal/runbooks/grpc-latency-high"

      # ---- 业务告警 ----
      - alert: MessageE2ELatencyHigh
        expr: histogram_quantile(0.99, sum(rate(message_e2e_latency_ms_bucket[5m])) by (le)) > 2000
        for: 3m
        labels:
          severity: critical
          team: messaging
        annotations:
          summary: "消息端到端延迟 P99 超过 2s"
          runbook_url: "https://wiki.internal/runbooks/message-e2e-latency"

      - alert: SearchSlowQuerySpike
        expr: rate(search_slow_query_total[5m]) > 1
        for: 3m
        labels:
          severity: warning
          team: search
        annotations:
          summary: "Search 慢查询频率超过 1 次/秒"
          runbook_url: "https://wiki.internal/runbooks/search-slow-query"

      - alert: PresenceOnlineCountAnomaly
        expr: |
          abs(delta(presence_online_users[10m]))
          / (presence_online_users + 1) > 0.3
        for: 5m
        labels:
          severity: warning
          team: connection
        annotations:
          summary: "在线人数 10 分钟内波动超过 30%"
          runbook_url: "https://wiki.internal/runbooks/presence-anomaly"

      - alert: ConnecteHighDisconnectRate
        expr: rate(connecte_disconnects_total[5m]) > 100
        for: 3m
        labels:
          severity: warning
          team: connection
        annotations:
          summary: "长连接断开速率超过 100/s"
          runbook_url: "https://wiki.internal/runbooks/connecte-disconnect"

      - alert: PushNotificationFailHigh
        expr: |
          sum(rate(push_notifications_failed_total[5m]))
          / (sum(rate(push_notifications_sent_total[5m])) + 1) > 0.1
        for: 5m
        labels:
          severity: critical
          team: messaging
        annotations:
          summary: "离线推送失败率超过 10%"
          runbook_url: "https://wiki.internal/runbooks/push-fail-rate"

      - alert: OfflineQueueDepthHigh
        expr: histogram_quantile(0.95, sum(rate(offline_queue_queue_depth_bucket[5m])) by (le)) > 1000
        for: 5m
        labels:
          severity: warning
          team: messaging
        annotations:
          summary: "离线队列深度 P95 超过 1000"
          runbook_url: "https://wiki.internal/runbooks/offline-queue-depth"

      - alert: AuditAIModerationSlow
        expr: histogram_quantile(0.99, sum(rate(audit_ai_moderation_latency_ms_bucket[5m])) by (le)) > 3000
        for: 3m
        labels:
          severity: warning
          team: safety
        annotations:
          summary: "AI 内容审核延迟 P99 超过 3s"
          runbook_url: "https://wiki.internal/runbooks/audit-ai-slow"

      # ---- 基础设施告警 ----
      - alert: HighGoroutineCount
        expr: runtime_go_goroutines > 10000
        for: 5m
        labels:
          severity: warning
          team: infra
        annotations:
          summary: "{{ $labels.service_name }} goroutine 数超过 10000，可能存在泄漏"
          runbook_url: "https://wiki.internal/runbooks/goroutine-leak"

      - alert: DBConnectionPoolExhausted
        expr: db_pool_open_connections / db_pool_max_connections > 0.9
        for: 3m
        labels:
          severity: critical
          team: infra
        annotations:
          summary: "{{ $labels.service_name }} DB 连接池使用率超过 90%"
          runbook_url: "https://wiki.internal/runbooks/db-pool-exhausted"

      - alert: RedisPoolTimeoutsHigh
        expr: rate(redis_pool_timeouts_total[5m]) > 1
        for: 3m
        labels:
          severity: warning
          team: infra
        annotations:
          summary: "{{ $labels.service_name }} Redis 连接池超时频率 > 1/s"
          runbook_url: "https://wiki.internal/runbooks/redis-pool-timeout"

      - alert: OTelCollectorDroppedSpans
        expr: rate(otelcol_exporter_send_failed_spans[5m]) > 0
        for: 5m
        labels:
          severity: warning
          team: infra
        annotations:
          summary: "OTel Collector 正在丢失 span 数据"
          runbook_url: "https://wiki.internal/runbooks/otel-collector-drop"

      - alert: AuthLoginFailRateHigh
        expr: |
          sum(rate(auth_login_total{result!="success"}[5m]))
          / (sum(rate(auth_login_total[5m])) + 1) > 0.3
        for: 5m
        labels:
          severity: critical
          team: security
        annotations:
          summary: "登录失败率超过 30%，可能遭受暴力破解"
          runbook_url: "https://wiki.internal/runbooks/auth-brute-force"

      - alert: ApiGatewayRateLimitSurge
        expr: rate(apigateway_rate_limit_hit_total[5m]) > 50
        for: 3m
        labels:
          severity: warning
          team: gateway
        annotations:
          summary: "API 网关限流触发频率 > 50/s"
          runbook_url: "https://wiki.internal/runbooks/gateway-rate-limit"
```

---

## 四、Loki 配置

```yaml
# loki-config.yaml
auth_enabled: false

server:
  http_listen_port: 3100

common:
  ring:
    instance_addr: 127.0.0.1
    kvstore:
      store: inmemory
  replication_factor: 1
  path_prefix: /loki

schema_config:
  configs:
    - from: 2024-01-01
      store: tsdb
      object_store: filesystem
      schema: v13
      index:
        prefix: index_
        period: 24h

storage_config:
  filesystem:
    directory: /loki/chunks

limits_config:
  allow_structured_metadata: true     # 允许接收 OTel 结构化日志
  retention_period: 360h              # 15 天
  max_query_parallelism: 32
  otlp_config:
    resource_attributes:
      attributes_config:
        - action: index_label          # service.name 作为 label
          attributes: ["service.name"]

# 日志保留 15 天
compactor:
  retention_enabled: true
  retention_delete_delay: 2h
  delete_request_store: filesystem
```

---

## 五、Tempo 配置

```yaml
# tempo-config.yaml
server:
  http_listen_port: 3200

distributor:
  receivers:
    otlp:
      protocols:
        grpc:
          endpoint: "0.0.0.0:4317"

storage:
  trace:
    backend: local
    local:
      path: /tempo/traces
    wal:
      path: /tempo/wal

metrics_generator:
  registry:
    external_labels:
      source: tempo
  storage:
    path: /tempo/generator/wal
    remote_write:
      - url: http://prometheus:9090/api/v1/write
        send_exemplars: true
  traces_storage:
    path: /tempo/generator/traces
  processor:
    service_graphs:
      dimensions:
        - service.name
    span_metrics:
      dimensions:
        - service.name

overrides:
  defaults:
    metrics_generator:
      processors: [service-graphs, span-metrics]
```

**关键**：开启 `metrics_generator` → 自动生成 Service Graph（服务调用关系图）+ Span Metrics（按 service.name 维度聚合的 RED 指标）→ remote_write 到 Prometheus。

---

## 六、Grafana 配置

### 6.1 数据源配置

```yaml
# grafana/provisioning/datasources/datasources.yaml
apiVersion: 1
datasources:
  - name: Prometheus
    type: prometheus
    url: http://prometheus:9090
    isDefault: true
    jsonData:
      exemplarTraceIdDestinations:
        - name: traceID
          datasourceUid: tempo

  - name: Loki
    type: loki
    url: http://loki:3100
    jsonData:
      derivedFields:
        - datasourceUid: tempo
          matcherRegex: '"trace_id":"(\w+)"'
          name: TraceID
          url: '$${__value.raw}'

  - name: Tempo
    type: tempo
    url: http://tempo:3200
    jsonData:
      tracesToMetrics:
        datasourceUid: prometheus
        tags:
          - key: service.name
            value: service_name
      tracesToLogs:
        datasourceUid: loki
        tags: ['service.name']
        mappedTags:
          - key: service.name
            value: service_name
        filterByTraceID: true
      serviceMap:
        datasourceUid: prometheus
      nodeGraph:
        enabled: true
```

**三柱关联设计**：
- **Prometheus → Tempo**：指标上的 exemplar 直接跳转到对应 trace
- **Loki → Tempo**：日志中的 `trace_id` 字段关联到 Tempo trace 详情
- **Tempo → Prometheus**：trace 详情页跳转到对应服务的指标面板
- **Tempo → Loki**：trace 详情页跳转到同 trace_id 的日志

### 6.2 推荐 Dashboard 列表

| Dashboard | 关注点 | 核心面板 |
|-----------|--------|----------|
| **IM Service Overview** | 全局鸟瞰 | 18 服务 RPC QPS/错误率/P99 热力图、Service Map |
| **Message Pipeline** | 消息全链路 | 发送 → seq分配 → 存储 → 推送端到端延迟瀑布图 |
| **Connection Gateway** | 长连接健康 | 在线数趋势、握手延迟、断连原因分布 |
| **Push Delivery** | 推送投递 | APNs/FCM 投递成功率、离线队列深度、群推扇出 |
| **Kafka Pipeline** | 消息队列 | 各 topic 吞吐、consumer lag、produce/consume 延迟 |
| **Cache Performance** | 缓存效率 | 各服务缓存命中率、Redis 延迟分位数 |
| **Auth & Session** | 安全监控 | 登录成功/失败率、Token 签发速率、设备踢下线 |
| **Search Performance** | 搜索质量 | ES 查询延迟分位、慢查询数、索引重建进度 |

---

## 七、Alertmanager 配置

```yaml
# alertmanager.yml
global:
  resolve_timeout: 5m

route:
  group_by: ['alertname', 'service_name']
  group_wait: 30s
  group_interval: 5m
  repeat_interval: 4h
  receiver: 'default'
  routes:
    - match:
        severity: critical
      receiver: 'oncall'
      repeat_interval: 30m

receivers:
  - name: 'default'
    webhook_configs:
      - url: 'http://alert-webhook:8080/alert'  # 对接企业微信/飞书/钉钉

  - name: 'oncall'
    webhook_configs:
      - url: 'http://alert-webhook:8080/oncall'
```

---

## 八、Docker Compose 部署（开发/测试环境）

```yaml
# docker-compose.observability.yml
version: "3.9"

services:
  otel-agent:
    image: otel/opentelemetry-collector-contrib:0.96.0
    command: ["--config", "/etc/otel/config.yaml"]
    volumes:
      - ./observability/otel-agent-config.yaml:/etc/otel/config.yaml
    ports:
      - "4317"     # 仅容器网络内部可达
    restart: unless-stopped

  otel-gateway:
    image: otel/opentelemetry-collector-contrib:0.96.0
    command: ["--config", "/etc/otel/config.yaml"]
    volumes:
      - ./observability/otel-gateway-config.yaml:/etc/otel/config.yaml
    ports:
      - "4317"
    depends_on:
      - prometheus
      - loki
      - tempo
    restart: unless-stopped

  prometheus:
    image: prom/prometheus:v2.51.0
    volumes:
      - ./observability/prometheus.yml:/etc/prometheus/prometheus.yml
      - ./observability/alert_rules.yml:/etc/prometheus/alert_rules.yml
    command:
      - '--config.file=/etc/prometheus/prometheus.yml'
      - '--web.enable-remote-write-receiver'
      - '--enable-feature=exemplar-storage'
    ports:
      - "9090:9090"
    restart: unless-stopped

  loki:
    image: grafana/loki:3.0.0   # ⚠️ schema v13 需要 Loki 3.x
    volumes:
      - ./observability/loki-config.yaml:/etc/loki/config.yaml
    command: ["-config.file=/etc/loki/config.yaml"]
    ports:
      - "3100:3100"
    restart: unless-stopped

  tempo:
    image: grafana/tempo:2.4.1
    volumes:
      - ./observability/tempo-config.yaml:/etc/tempo/config.yaml
    command: ["-config.file=/etc/tempo/config.yaml"]
    ports:
      - "3200:3200"
      - "4317"
    restart: unless-stopped

  grafana:
    image: grafana/grafana:10.4.0
    volumes:
      - ./observability/grafana/provisioning:/etc/grafana/provisioning
    environment:
      GF_AUTH_ANONYMOUS_ENABLED: "true"
      GF_AUTH_ANONYMOUS_ORG_ROLE: "Viewer"
    ports:
      - "3000:3000"
    depends_on:
      - prometheus
      - loki
      - tempo
    restart: unless-stopped

  alertmanager:
    image: prom/alertmanager:v0.27.0
    volumes:
      - ./observability/alertmanager.yml:/etc/alertmanager/alertmanager.yml
    ports:
      - "9093:9093"
    restart: unless-stopped
```

---

## 九、环境变量清单

每个服务容器需设置以下环境变量：

| 变量名 | 默认值 | 说明 |
|--------|--------|------|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `otel-agent:4317` | OTel Agent 地址 |
| `OTEL_SERVICE_NAME` | （由代码传入） | 服务名（可选，代码中已设置） |
| `DEPLOY_ENV` | `development` | 部署环境标识（dev/staging/prod） |
| `OTEL_TRACES_SAMPLER` | `always_on` | 采样策略（SDK 全量上报，尾部采样由 Gateway 处理） |
| `OTEL_TRACES_SAMPLER_ARG` | — | 与 always_on 搭配时无需此参数 |
| `HOSTNAME` | （K8s 自动注入） | 服务实例 ID，用于 resource attribute |
| `LOG_LEVEL` | `info` | 日志级别（debug/info/warn/error） |

---

## 十、SLI/SLO 定义

> 大型标准化系统必须定义 SLI（Service Level Indicator）和 SLO（Service Level Objective），用于度量服务质量。

### 10.1 全局 SLI 定义

| SLI 名称 | 计算公式 | 说明 |
|----------|---------|------|
| **可用性 (Availability)** | `1 - (gRPC error_count / total_count)` | 排除客户端错误（InvalidArgument 等） |
| **延迟 (Latency)** | `P99(rpc_server_duration_ms)` | 按服务和方法分 |
| **吞吐量 (Throughput)** | `sum(rate(rpc_server_duration_count[5m]))` | 每秒请求数 |
| **消息端到端延迟** | `P99(message.e2e_latency_ms)` | 从发送到接收端收到 |
| **推送投递率** | `1 - (push.failed / push.sent)` | 离线推送投递成功率 |

### 10.2 各服务 SLO

| 服务 | SLO 指标 | 目标值 | 窗口 | Burn Rate 告警 |
|------|---------|--------|------|----------------|
| **apigateway** | 可用性 | ≥ 99.95% | 30 天 | 14.4× → critical, 6× → warning |
| **apigateway** | P99 延迟 | ≤ 200ms | 30 天 | |
| **message** | 可用性 | ≥ 99.99% | 30 天 | 14.4× → critical |
| **message** | E2E 延迟 P99 | ≤ 2000ms | 30 天 | |
| **connecte** | 可用性 | ≥ 99.99% | 30 天 | |
| **connecte** | 握手延迟 P99 | ≤ 500ms | 30 天 | |
| **push** | 投递成功率 | ≥ 99% | 7 天 | |
| **auth** | 可用性 | ≥ 99.99% | 30 天 | |
| **auth** | 登录延迟 P99 | ≤ 300ms | 30 天 | |
| **search** | 查询延迟 P99 | ≤ 500ms | 30 天 | |
| **conversation** | seq 分配延迟 P99 | ≤ 10ms | 30 天 | |

### 10.3 SLO Burn Rate 告警示例

```yaml
# 加入 alert_rules.yml
- alert: SLOBurnRateHigh_ApiGateway
  expr: |
    (
      sum(rate(rpc_server_duration_milliseconds_count{service_name="apigateway",rpc_grpc_status_code!~"0|3|5"}[1h]))
      / sum(rate(rpc_server_duration_milliseconds_count{service_name="apigateway"}[1h]))
    ) > (14.4 * 0.0005)
  for: 5m
  labels:
    severity: critical
    team: gateway
  annotations:
    summary: "ApiGateway SLO burn rate 14.4× 超过预算（1h 窗口）"
    runbook_url: "https://wiki.internal/runbooks/slo-burn-rate"
```

---

## 十一、健康检查与优雅降级

### 11.1 gRPC Health Check

每个服务注册 gRPC Health Checking Protocol，供 K8s/负载均衡器探测：

```go
import "google.golang.org/grpc/health"
import healthpb "google.golang.org/grpc/health/grpc_health_v1"

func main() {
    // ... initOpenTelemetry ...

    grpcServer := grpc.NewServer(...)
    
    // 注册健康检查
    healthServer := health.NewServer()
    healthpb.RegisterHealthServer(grpcServer, healthServer)
    healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
    
    // 在 graceful shutdown 时标记为 NOT_SERVING
    go func() {
        <-ctx.Done()
        healthServer.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
    }()
}
```

### 11.2 OTel Collector 不可用时的降级策略

> ⚠️ `otlptracegrpc.New()` 使用**惰性连接**，创建 exporter 时不会真正建立连接，
> 因此即使 Collector 不可达，`err` 也为 `nil`。真正的连接失败发生在首次 export 时。

**实际降级机制**由以下三层保障：

```go
// 1. SDK BatchSpanProcessor 内置 retry + 丢弃策略
//    - MaxQueueSize=4096：超过后自动丢弃最旧的 span
//    - Collector 恢复后自动重连并恢复 export

// 2. Exporter 级别的 retry 配置（已在 InitOpenTelemetry 中设置）
otlptracegrpc.WithRetry(otlptracegrpc.RetryConfig{
    Enabled:         true,
    InitialInterval: 1 * time.Second,
    MaxInterval:     10 * time.Second,
})

// 3. 应用层健康检查可探测 Collector 状态
//    通过 Collector 的 health_check extension（端口 13133）判断：
//    GET http://otel-agent:13133/ → 200 OK = healthy
//    在 K8s readiness probe 失败时可触发告警
```

**核心原则**：可观测性组件故障不能导致业务服务不可用。SDK 内置的 queue + retry 机制
已确保 Collector 短暂不可用时数据不会丢失（在 queue 容量内），Collector 恢复后自动重连。

### 11.3 日志级别动态调整

> ✅ **已在第 1.2 节 `SetupLogger` 中实现**：通过 `slog.LevelVar` 支持动态日志级别，启动时从 `LOG_LEVEL` 环境变量读取。

运行时调整示例（可通过 admin RPC 或 HTTP endpoint 触发）：

```go
// 临时开启 debug 日志排查问题
observability.SetLogLevel(slog.LevelDebug)

// 排查完成后恢复
observability.SetLogLevel(slog.LevelInfo)
```

---

## 十二、K8s 部署指引（生产环境）

> Docker Compose 仅用于开发/测试。生产环境使用 K8s 部署。

### 12.1 OTel Agent 部署方式：DaemonSet

```yaml
# otel-agent-daemonset.yaml
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: otel-agent
  namespace: observability
spec:
  selector:
    matchLabels:
      app: otel-agent
  template:
    metadata:
      labels:
        app: otel-agent
    spec:
      containers:
        - name: otel-agent
          image: otel/opentelemetry-collector-contrib:0.96.0
          args: ["--config", "/etc/otel/config.yaml"]
          ports:
            - containerPort: 4317
              hostPort: 4317    # 使各 Pod 通过 Node IP:4317 可达
          resources:
            requests:
              cpu: 100m
              memory: 256Mi
            limits:
              cpu: 500m
              memory: 512Mi
          volumeMounts:
            - name: config
              mountPath: /etc/otel
      volumes:
        - name: config
          configMap:
            name: otel-agent-config
```

### 12.2 OTel Gateway 部署方式：Deployment + HPA

```yaml
# otel-gateway-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: otel-gateway
  namespace: observability
spec:
  replicas: 2
  selector:
    matchLabels:
      app: otel-gateway
  template:
    metadata:
      labels:
        app: otel-gateway
    spec:
      containers:
        - name: otel-gateway
          image: otel/opentelemetry-collector-contrib:0.96.0
          args: ["--config", "/etc/otel/config.yaml"]
          ports:
            - containerPort: 4317
          resources:
            requests:
              cpu: 500m
              memory: 1Gi
            limits:
              cpu: 2
              memory: 2Gi
          livenessProbe:
            httpGet:
              path: /
              port: 13133
          readinessProbe:
            httpGet:
              path: /
              port: 13133
---
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: otel-gateway
  namespace: observability
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: otel-gateway
  minReplicas: 2
  maxReplicas: 8
  metrics:
    - type: Resource
      resource:
        name: cpu
        target:
          type: Utilization
          averageUtilization: 70
```

### 12.3 各微服务 Pod 环境变量配置

```yaml
# 各微服务 Deployment 中
env:
  - name: OTEL_EXPORTER_OTLP_ENDPOINT
    value: "$(NODE_IP):4317"    # 指向本节点 DaemonSet Agent
  - name: NODE_IP
    valueFrom:
      fieldRef:
        fieldPath: status.hostIP
  - name: HOSTNAME
    valueFrom:
      fieldRef:
        fieldPath: metadata.name
  - name: DEPLOY_ENV
    value: "production"
```

### 12.4 gRPC 健康检查探针

```yaml
# 各微服务 Deployment 中
livenessProbe:
  grpc:
    port: 50051
  initialDelaySeconds: 10
  periodSeconds: 10
readinessProbe:
  grpc:
    port: 50051
  initialDelaySeconds: 5
  periodSeconds: 5
```

---

## 十三、接入步骤总览（Checklist）

### 全局任务（做一次）

- [ ] 创建 `pkg/observability` 公共包（otel.go + logger.go + kafka_carrier.go + event_header.go + infra_metrics.go + span_helpers.go + histogram_views.go）
- [ ] 编写 OTel Agent 配置文件
- [ ] 编写 OTel Gateway 配置文件（含 tail_sampling）
- [ ] 编写 Prometheus 配置 + 告警规则（含 SLO burn rate 告警）
- [ ] 编写 Loki 配置
- [ ] 编写 Tempo 配置（含 metrics_generator）
- [ ] 编写 Grafana 数据源 provisioning（三柱关联）
- [ ] 编写 Alertmanager 配置
- [ ] 编写 Docker Compose（开发环境）+ K8s manifests（生产环境）
- [ ] 创建 Grafana Dashboard（至少 8 个核心面板）
- [ ] 定义 SLI/SLO 并配置 burn rate 告警
- [ ] 编写 Runbook 文档（每条告警规则对应一份）
- [ ] `common.proto` 中 ResponseMeta 增加 `span_id` 字段（可选）

### 各服务任务（每个服务做一次）

- [ ] `main()` 中调用 `observability.InitOpenTelemetry(ctx, "服务名", "版本")`
- [ ] `main()` 中调用 `observability.SetupLogger("服务名")`
- [ ] `main()` 中调用 `observability.RegisterDBPoolMetrics(db, "服务名")`
- [ ] `main()` 中调用 `observability.RegisterRedisPoolMetrics(rdb, "服务名")`
- [ ] gRPC Server 注册 `otelgrpc.UnaryServerInterceptor()` + `StreamServerInterceptor()`
- [ ] gRPC Client 连接注册 `otelgrpc.UnaryClientInterceptor()`
- [ ] 注册 gRPC Health Check（`health.NewServer()`）
- [ ] 注册服务专属自定义业务指标（见各服务 impl.md）
- [ ] 在业务代码中埋点（Counter.Add / Histogram.Record）
- [ ] 错误路径调用 `observability.RecordError(span, err)`
- [ ] Kafka Producer 调用 `observability.StartKafkaProducerSpan()`
- [ ] Kafka Consumer 调用 `observability.StartKafkaConsumerSpan()`
- [ ] 将 `buildEventHeader` 改为调用 `observability.BuildEventHeader(ctx, ...)`
- [ ] 所有日志改为 `slog.InfoContext(ctx, ...)` 格式

---

## 十四、网络安全设计

**核心原则：所有服务不对外暴露监控端口**

```
                    ┌─ 外部网络 ──────────────────────────┐
                    │                                      │
                    │   仅暴露: Grafana :3000 (带认证)      │
                    │                                      │
                    └──────────────────────────────────────┘
                                     │
                    ┌─ 内部网络 ──────┼──────────────────────┐
                    │                │                       │
                    │   Grafana ─── Prometheus               │
                    │            ─── Loki                    │
                    │            ─── Tempo                   │
                    │                                        │
                    │   OTel Gateway ─── Prometheus          │
                    │                ─── Loki                │
                    │                ─── Tempo               │
                    │                                        │
                    │   OTel Agent ◄── 各微服务 (OTLP:4317)  │
                    │                                        │
                    │   各微服务: 不暴露 /metrics、不暴露     │
                    │            Prometheus scrape 端口       │
                    └────────────────────────────────────────┘
```

- 微服务 → OTel Agent：OTLP gRPC，**容器内通信**
- OTel Agent → OTel Gateway：OTLP gRPC，**内部网络**
- OTel Gateway → Prometheus/Loki/Tempo：内部网络
- Grafana：唯一对外端口，配置 OAuth/LDAP 认证

---

## 十五、18 服务专属指标汇总

| 服务 | 核心自定义指标 |
|------|---------------|
| **message** | `stored_total`, `e2e_latency_ms`, `seq_alloc_duration_ms`, `dedup_hit_total`, `cache_hit/miss` |
| **push** | `kafka_process_duration_ms`, `notifications_sent/failed`, `offline_push_latency_ms`, `group_fanout_duration_ms`, `cache_hit/miss` |
| **connecte** | `active_connections`, `connections_by_platform`, `handshake_duration_ms`, `sync_notify_sent/failed` |
| **ack** | `read_ack_processed_total`, `group_read_fanout_duration_ms`, `delivery_confirmed_total` |
| **apigateway** | `rate_limit_hit_total`, `auth_failed_total`, `api_requests_total`, `downstream_rpc_failed_total` |
| **audit** | `content_scanned_total`, `violations_detected_total`, `ai_moderation_latency_ms`, `batch_write_latency_ms` |
| **auth** | `login_total`, `token_issued_total`, `token_refreshed_total`, `oauth_call_latency_ms` |
| **config** | `changed_total`, `feature_flag_queried_total`, `active_watchers`, `hot_reload_duration_ms` |
| **conversation** | `channel_created_total`, `seq_alloc_total`, `seq_alloc_latency_ms`, `unread_calc_duration_ms` |
| **group** | `created_total`, `member_joined/left_total`, `member_count`, `cache_hit/miss`, `batch_db_write_latency_ms` |
| **media** | `upload_total`, `upload_duration_ms`, `s3_put/get_latency_ms`, `transcode_duration_ms`, `thumbnail_duration_ms` |
| **notification** | `sent_total`, `broadcast_duration_ms`, `broadcast_fanout_size`, `unread_count_duration_ms` |
| **offline_queue** | `signal_enqueued/dequeued_total`, `queue_depth`, `users_with_queue`, `zset_op_duration_ms` |
| **presence** | `online_users`, `heartbeat_processed_total`, `status_changed_total`, `grace_period_expired_total`, `hll_rebuild_duration_ms` |
| **relation** | `friend_request_sent/handled_total`, `block_ops_total`, `friend_list_query_duration_ms`, `double_delete_triggered_total` |
| **search** | `es_query_duration_ms`, `es_query_total`, `slow_query_total`, `rebuild_progress`, `rebuild_duration_ms` |
| **session** | `created_total`, `device_kicked_total`, `active_sessions`, `login_by_platform_total`, `cache_hit/miss` |
| **user** | `registered_total`, `profile_updated_total`, `get_user_duration_ms`, `cache_hit/miss`, `batch_get_size` |

---

## 十六、Redis Cluster 部署规划（MED-02）

> **10M 并发用户场景**下，不同服务的 Redis 访问模式和 QPS 差异极大。
> 为避免 noisy neighbor 问题，高频服务使用独立 Redis Cluster，低频服务共享集群。

### 服务 → Redis Cluster 映射

| Redis Cluster | 服务 | 规格（主+从） | 峰值 QPS | 关键 Key 族群 |
|---|---|---|---|---|
| **redis-conversation** | Conversation | 8 主 8 从 = 16 节点 | 50 万/s | `conv:max_seq:*`（IncrChannelSeq，全系统最热） |
| **redis-connecte** | Connecte | 4 主 4 从 = 8 节点 | 300~500 万/s | `conn:route:*`, `conn:user_conns:*`（WebSocket 路由） |
| **redis-presence** | Presence | 4 主 4 从 = 8 节点 | 100~200 万/s | `presence:status:*`, `presence:subs:*`（在线状态） |
| **redis-message** | Message | 4 主 4 从 = 8 节点 | 100~200 万/s | `msg:detail:*`, `msg:seq_index:*`, `msg:dedup:*`（消息缓存） |
| **redis-push** | Push | 4 主 4 从 = 8 节点 | 50~100 万/s | `push:kafka:dedup:*`, `push:local:*`（推送去重+本地缓存） |
| **redis-offline** | OfflineQueue | 4 主 4 从 = 8 节点 | 10~20 万/s | `offline:queue:*`, `offline:count:*`（离线队列 ZSET） |
| **redis-shared** | Session/Auth/Config/User/Relation/Ack/Audit/Media/Search/Notification/Group | 4 主 4 从 = 8 节点 | 10~50 万/s | 各服务独立 key 前缀，共享集群即可满足 |

### 单节点硬件建议

| 角色 | CPU | 内存 | 网络 | 存储 |
|------|-----|------|------|------|
| Redis 主节点 | 4C | 32 GB（实际使用 < 16 GB，预留 > 50%） | 10 Gbps | SSD（AOF） |
| Redis 从节点 | 2C | 32 GB | 10 Gbps | SSD（RDB） |

### 配置要点

```
# 通用配置（所有 Cluster）
cluster-enabled yes
cluster-require-full-coverage no    # 单主故障不影响其他 slot
maxmemory-policy allkeys-lru        # 内存不足时 LRU 淘汰
appendonly yes
appendfsync everysec                # AOF 持久化
hz 100                              # 提高事件循环频率（高 QPS 场景）

# 慢查询监控
slowlog-log-slower-than 5000        # 5ms 以上记入慢查询
slowlog-max-len 1024
```

### 总节点数

| 集群 | 节点数 |
|------|--------|
| redis-conversation | 16 |
| redis-connecte | 8 |
| redis-presence | 8 |
| redis-message | 8 |
| redis-push | 8 |
| redis-offline | 8 |
| redis-shared | 8 |
| **合计** | **64 节点** |
