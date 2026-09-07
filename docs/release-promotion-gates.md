# 发布晋级门禁与上游升级策略（Issue #37）

所有进入生产发布的变更必须逐级通过以下门禁；升级 PR（上游 SDK 或依赖）
没有捷径。

## 1. CI 质量与供应链门禁（`.github/workflows/ci.yml`）

| 门禁 | 阈值 |
| --- | --- |
| 分支覆盖率（整体） | ≥ 80%（`--cov-fail-under=80`） |
| 关键模块覆盖率 | ≥ 90%：auth/preconditions/roles、database、idempotency、migrations、budgets/governance |
| 类型检查 | mypy strict 全通过 |
| API 合约 | OpenAPI 导出 + web-console 生成客户端零 diff |
| Helm/零信任合约 | chart lint + 渲染合约测试 |
| 公开边界 | Playwright 冒烟（compose 全栈） |
| 生产骨架 | k8s 冒烟（真实集群语义） |
| 供应链 | `uv sync --locked` 完整性、CycloneDX SBOM 产物、pip-audit 已知漏洞、gitleaks 秘密扫描、npm audit（high+ 阻断） |

## 2. 上游升级自动化（`.github/workflows/upstream-upgrade.yml`）

- 每日 03:17 UTC 查询 PyPI 上 `trpc-agent-py` 的**稳定版**（X.Y.Z，
  显式排除 rc/beta/dev），发现高于当前 pin 的新稳定版时自动创建
  锁文件升级 PR：`uv add trpc-agent-py==<version>` + 同步
  `PINNED_TRPC_AGENT_VERSION`。
- 候选发现逻辑在 `trpc_service.upstream.latest_stable`（纯函数，单测覆盖）。

## 3. 升级晋级阶梯（生产发布前必须全部通过）

1. **完整回归**：升级 PR 通过上述全部 CI 门禁（含覆盖率、合约、冒烟）。
2. **Staging**：docker compose 全栈冒烟 + k8s 生产骨架冒烟在 Staging
   环境重放；容量基线（`docs/capacity-baseline.md`）抽测 sustained 剖面。
3. **灰度（Canary）**：生产 Rollout 以 `analysis-templates` 暂停窗口推进，
   Eval Suite（#31）评测门禁通过才允许放量；回滚历史保留（#29 模板）。
4. **生产发布**：三段全部通过后 ArgoCD 完成最终 sync；SBOM 与签名验证
   产物随发布归档（cosign verify 在制品晋级到生产时执行）。

备份与温备切换的 RPO（≤ 5 分钟）/RTO（≤ 60 分钟）门槛见
`docs/backup-failover-runbook.md`，容量门槛见 `docs/capacity-baseline.md`。

任何一级失败，升级 PR 关闭并在修复后重新走完整阶梯——禁止跳级。
