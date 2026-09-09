# 最终公开边界验收记录

本文是 Issue #38 的版本化验收清单。它只声明仓库可从干净环境执行的证据，不替代真实生产演练：不得将未运行的生产演练写为通过。每次代码、配置、依赖或基础设施发生实质变更后，必须重新执行受影响项目并将 CI 运行 URL、提交 SHA、环境、执行人和批准人附到发布记录。

## 可执行的干净环境证据

| 边界 | CI 作业 / 本地复现命令 | 证明内容 |
| --- | --- | --- |
| 代码、迁移与 API 契约 | `uv sync --frozen`；`uv run pytest tests/unit tests/integration --cov=trpc_service --cov=dev.fake_external.scenarios --cov-branch --cov-fail-under=80 -q` | 干净依赖解析、迁移、RLS、协议、治理与集成契约 |
| 文档、图表与 Schema | `Documentation gate / docs-gate`；`Documentation gate / render-mermaid` | README/文档本地链接、JSON Schema 示例和 Mermaid 图源可渲染 |
| 公开 HTTP/UI 边界 | `CI / public-boundary-smoke`；`npm run test:smoke` | Compose 启动后仅通过公开入口验证控制台与 API |
| Kubernetes 生产拓扑 | `CI / kubernetes-production-smoke`；`RUN_KUBERNETES_SMOKE=1 uv run pytest tests/deployment/test_kubernetes_smoke.py -v -s` | Disposable Kind 环境中的 Helm、GitOps、网格、滚动与零信任边界 |
| 供应链 | `CI / supply-chain` | 锁文件、SBOM、严格漏洞审计、秘密扫描和 Web 依赖审计 |

`public-boundary-smoke` 与 `kubernetes-production-smoke` 必须在 `quality` 成功后才可运行；因此失败、跳过或没有对应提交的运行均不是验收证据。

## 风险责任、缓解与演练证据

下表把风险登记中的 16 项风险绑定到可执行的验证边界。责任角色和完整预防/检测/恢复方案以[风险登记](risk-register-and-acceptance.md#2-风险登记)为准；这里不改变其 OPEN/CLOSED 语义。

| 风险 | 责任角色 | 缓解主题 | 演练或自动化证据 |
| --- | --- | --- | --- |
| R-01 | 安全负责人 | RLS、复合键、Storage 隔离 | `tests/integration/test_postgres_tenant_isolation.py` |
| R-02 | Channel 负责人 | 入站 Ledger、顺序与幂等 | `tests/integration/test_channel_bindings.py` |
| R-03 | 分布式执行负责人 | 租约、fence 与 CAS | `tests/integration/test_session_execution_pipeline.py` |
| R-04 | Tool 治理负责人 | 副作用分级与对账 | `tests/integration/test_tool_approval_recovery.py` |
| R-05 | 数据保护负责人 | Filter、DLP 与出口控制 | `tests/unit/test_governance.py` |
| R-06 | SRE 负责人 | Outbox、Kafka 位点、背压与恢复 | `tests/unit/test_execution_bus.py`；`tests/integration/test_session_execution_pipeline.py`；`tests/integration/test_chaos_recovery.py` |
| R-07 | DBA 负责人 | 迁移、RLS 与 PITR | `tests/unit/test_database_migrations.py` |
| R-08 | 执行平台负责人 | 失效租约拒绝提交 | `tests/integration/test_session_execution_pipeline.py` |
| R-09 | 数据平台负责人 | 在线迁移与校验 | `tests/integration/test_storage_migrations.py` |
| R-10 | Channel 负责人 | 投递状态机与人工对账 | `tests/unit/test_channels.py` |
| R-11 | 安全工程负责人 | gVisor、禁网和资源限制 | `tests/deployment/test_sandbox_contract.py` |
| R-12 | IAM 负责人 | OIDC、密钥引用与应急登录 | `tests/integration/test_tenant_management_api.py` |
| R-13 | FinOps 负责人 | 原子预留与不可变账本 | `tests/integration/test_budget_service.py` |
| R-14 | 平台维护负责人 | 锁定依赖与回归门禁 | `tests/unit/test_upstream_upgrade.py` |
| R-15 | 灾备负责人 | fencing、RPO/RTO 与温备切换 | `tests/integration/test_failover_lease.py` |
| R-16 | 可观测性负责人 | allowlist 与秘密清理 | `tests/unit/test_telemetry.py` |

生产等价环境演练（真实 IdP、IM、Kafka、Redis、对象存储、Vault、模型端点与跨地域切换）仍需按照[风险登记的证据要求](risk-register-and-acceptance.md#2-风险登记)执行并保留外部运行记录。仓库测试验证安全默认和失败行为，不得被误读为已完成真实供应商或生产地域演练。
