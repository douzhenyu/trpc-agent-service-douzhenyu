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

- 网关对每次提交执行令牌桶准入（`trpc_service.backpressure.AdmissionController`）：
  桶容量 = 突发速率 × 60s，按持续速率回填。超出后以 **429** 返回稳定错误
  `RATE_EXCEEDED`；并发 in-flight 达到 `admission_max_in_flight`（默认 10000）
  时返回 `INFLIGHT_SATURATED`。
- `GET /internal/v1/capacity` 暴露当前策略、in-flight 与 shed level
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

下表记录理论起点；每次负载验收后以实测值替换（`实测` 列）。

| 组件 | 饱和信号 | 初始告警阈值 | 实测 |
| --- | --- | --- | --- |
| Agent Gateway | 429 比率、请求延迟 | burn rate 14.4×（5m）；`AdmissionOverload` | 待验收 |
| Kafka（Outbox→分区） | 分区积压、生产者延迟 | `ExecutionPipelineFailures` 持续 10m | 待验收 |
| Agent Worker | 执行结果失败率、lease 等待 | FAILED 结果持续 10m | 待验收 |
| PostgreSQL | 连接占用、事务延迟、锁等待 | 连接池 > 80%、p95 > 500ms | 待验收 |
| Redis | 内存、逐出、延迟 | 内存 > 80%、逐出 > 0 | 待验收 |
| LLM/外部依赖 | 网关延迟、熔断次数 | p95 > 5s、fallback 率 > 5% | 待验收 |

## 5. 验收流程

1. 部署目标环境并确认 `GET /internal/v1/capacity` 反映生产策略
   （1000/3000/60s/10000）。
2. 依次运行 `soak`、`sustained`、`burst`，保留 JSON 输出与 Grafana 截图。
3. 验证扩缩容：突发期间观察 HPA 副本数上升、结束后 300s 内回落。
4. 验证背压：以 >3000/s 压测，确认 429 `RATE_EXCEEDED` 生效且
   `platform_admission_decisions_total` 同步增长，无雪崩（错误率受控）。
5. 将实测饱和点回填第 4 节，并调整告警阈值。
