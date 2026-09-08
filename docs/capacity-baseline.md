# 容量基线与背压策略（Issue #34）

本文件是平台容量验证的生产基线：负载测试方法、各组件饱和点、背压/拒绝策略
以及自动扩缩容配置。每次容量验收（持续 1000 msg/s、3000 msg/s 60 秒突发、
≥10000 并发执行）后更新实测数据列。

## 1. 验收阈值

| 目标 | 阈值 | 验证方式 |
| --- | --- | --- |
| 持续吞吐 | 1000 msg/s，持续 60s，p99 ≤ 5s | `scripts/load_test.py --profile sustained` |
| 突发吞吐 | 3000 msg/s，持续 60s，p99 ≤ 5s | `scripts/load_test.py --profile burst` |
| 并发执行 | ≥ 10000 个并发 Agent 执行可观测、受控 | 准入控制器 in-flight 上限 + Worker 并发池 |
| 稳态 | 200 msg/s 持续 300s 无衰减 | `scripts/load_test.py --profile soak` |

## 2. 背压与拒绝策略

- 网关对每次提交执行令牌桶准入（`platform.try_admit_execution`）：桶状态在
  PostgreSQL 中由事务 advisory lock 保护，因而持续/突发额度在所有 Gateway 副本间
  共享；桶容量 = 突发速率 × 60s，按持续速率回填。超出后以 **429** 返回稳定错误
  `RATE_EXCEEDED`。同一函数还统计所有 Gateway 副本中尚未终态的执行，达到
  `admission_max_in_flight`（默认 10000）时返回 `INFLIGHT_SATURATED`。成功写入
  Outbox 后不会释放该容量；Worker 将成功或不可恢复的模型失败推进为终态才会腾出槽位。
- `GET /internal/v1/capacity` 暴露当前策略、跨副本 `pending_executions`、待发布的
  `pending_outbox_records`、兼容的 `in_flight` 计数与 shed level。
  （GREEN/YELLOW/RED）。
- 所有准入决策通过 `platform_admission_decisions_total{service,decision,reason}`
  暴露到 Prometheus，并配有 `AdmissionOverload` 告警（见 observability 规则）。
- 执行出队后的一切背压由既有状态机承担：outbox 重试 + SKIP LOCKED 批处理、
  Session lease 串行化、回复投递的 RATE_LIMITED 退避与 DEAD_LETTER 兜底。

## 3. 自动扩缩容

HPA（`deploy/helm/trpc-agent-platform/templates/autoscaling.yaml`）对全部
生产单元生效：CPU 70% / 内存 80% 双目标，扩容 30s 稳定窗口内快速 ×2 翻倍，
缩容 300s 稳定窗口防抖。容量评估期间必须把指标抓取（/metrics）接入
Prometheus，否则 HPA 只依赖 Resource 指标。

## 4. 饱和点基线

下表是生产验收必须采集的可审计基线。Gateway 的容量数值由可重复测试与事务
准入共同约束；其他依赖的行列出报告字段和必须确认的首个饱和阈值，压测 JSON
输出与对应 Grafana 截图是该行的验收证据。

| 组件 | 饱和信号 | 初始告警阈值 | 验收报告字段 |
| --- | --- | --- | --- |
| Agent Gateway | 429 比率、已接受请求 p99、`pending_executions` | burn rate 14.4×（5m）；`AdmissionOverload`；10000 pending | 1000/s、3000/s profile 的 `accepted_rate`、p99、2xx/429 计数 |
| Kafka（Outbox→分区） | 分区积压、生产者延迟 | `ExecutionPipelineFailures` 持续 10m | 每分区积压、发布延迟 p95、重试次数 |
| Agent Worker | 执行结果失败率、lease 等待、PENDING 执行数 | FAILED 结果持续 10m；pending > 8000 | 完成率、lease 等待 p95、pending 峰值 |
| PostgreSQL | 连接占用、事务延迟、锁等待 | 连接池 > 80%、p95 > 500ms | 连接峰值、事务 p95、锁等待 p95 |
| Redis | 内存、逐出、延迟 | 内存 > 80%、逐出 > 0 | 内存峰值、逐出计数、命令延迟 p95（未部署时明确 N/A） |
| LLM/外部依赖 | 网关延迟、熔断次数 | p95 > 5s、fallback 率 > 5% | 依赖 p95、熔断次数、fallback 比率 |

## 5. 验收流程

1. 部署目标环境并确认 `GET /internal/v1/capacity` 反映生产策略
   （1000/3000/60s/10000）。
2. 依次运行 `soak`、`sustained`、`burst`，保留 JSON 输出与 Grafana 截图；仅
   在 profile 截止窗口内完成的 200/202 计入 `accepted_rate`，429 快速拒绝或窗口后
   返回的 2xx 都不能使容量验收通过。
3. 验证扩缩容：突发期间观察 HPA 副本数上升、结束后 300s 内回落。
4. 验证背压：以 >3000/s 压测，确认 429 `RATE_EXCEEDED` 生效且
   `platform_admission_decisions_total` 同步增长，无雪崩（错误率受控）。
5. 将实测饱和点回填第 4 节，并调整告警阈值。
