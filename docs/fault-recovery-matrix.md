# 故障恢复矩阵与显式降级（Issue #35）

本矩阵覆盖关键依赖故障下的行为契约：恢复机制、用户可见降级、以及
fail-closed / 显式降级策略。Chaos 场景在 `tests/integration/test_chaos_recovery.py`
中持续验证，恢复后不变量：无跨租户泄漏、无丢事件、无重复副作用。

## 1. 故障矩阵

| 依赖 | 故障注入 | 恢复机制 | 用户可见降级 | 策略 | 验证 |
| --- | --- | --- | --- | --- | --- |
| 节点（Runner/Worker） | 执行中途崩溃 | Session lease 过期 + at-least-once 重投递；SUCCEEDED 状态栅栏防重跑 | 无（延迟） | fail closed（未完成不提交） | `test_model_outage_fails_closed_with_zero_partial_state` |
| 模型（LLM） | 上游 5xx / 超时 | 断路器切换到 allowed fallback alias；`agent_execution.model_fallback` 审计 | 显式 fallback 标记（`fallback_used`） | 允许降级，显式+审计 | `test_llm_gateway.py::test_gateway_uses_fallback...`、`test_model_fallback_writes_explicit_audit_evidence` |
| 模型（全部不可用） | 所有候选失败 | 无降级空间：报错上抛，执行零部分状态 | 明确失败（非静默） | fail closed | `test_model_outage_fails_closed_with_zero_partial_state` |
| 消息总线 | Outbox 投递失败 | outbox `PENDING` 重试 + attempts 计数 | 无 | 重试至成功 | `test_session_execution_pipeline.py` |
| 回复投递（IM） | 限流 / 未知结果 | `RATE_LIMITED` 退避重试；`OUTCOME_UNKNOWN` 停靠并对账后不重发；耗尽进 `DEAD_LETTER`，可运营台重试 | 用户看到延迟/重发对账 | 对账优先 | `channels/delivery.py` 状态机 + 投递恢复测试 |
| 数据库 | 事务冲突 / 连接失败 | 事务回滚 + SKIP LOCKED 重新批处理；RLS 恒定生效 | 无部分提交 | fail closed | `test_postgres_tenant_isolation.py` |
| 缓存 / 投影 | Redis / 投影落后 | outbox 事件重放；Memory 以 SQL 权威状态修正 | Memory 最终一致 | 允许降级，显式 | Summary/Memory 投影测试 |
| 密钥 | Secret 解析失败 | 无降级：`SECRET_RESOLUTION_FAILED` 拒绝调用 | 明确失败 | **fail closed** | `test_llm_gateway.py` |
| 租约 | Lease 冲突 | `SESSION_LEASE` 拒绝并发执行，fencing token 防脑裂 | 明确冲突 | **fail closed** | lease/fencing 测试 |
| 副作用治理 | 策略/对账缺失 | `SIDE_EFFECT_GOVERNANCE` 拒绝出站调用 | 明确拒绝 | **fail closed** | 工具治理测试 |
| Knowledge | 构建不可用 | 发布流停在旧 Revision；检索走最后已发布版本 | 显式降级（`KNOWLEDGE` 域） | 允许降级，显式 | knowledge 部署测试 |
| Artifact | 存储不可用 | 上传/下载明确失败；元数据权威在 SQL | 明确失败 | fail closed（存储）+ 显式（生命周期） | artifact 测试 |

## 2. 显式降级注册表

- `trpc_service/degradation.py`：`DegradationRegistry` 记录
  MODEL/KNOWLEDGE/MEMORY/ARTIFACT/IM 五个允许降级域的进入/恢复，
  带 `degradation.entered` / `degradation.restored` 审计钩子。
- `FAIL_CLOSED_CONCERNS`（SECURITY_STATE、SECRET_RESOLUTION、
  SESSION_LEASE、SIDE_EFFECT_GOVERNANCE）不可作为降级域表示——
  只能拒绝。
- 每个生产单元暴露 `GET /internal/v1/degradations`，运营侧可集中查询。
- LLM fallback 触发 `MODEL` 域降级（`LLM_FALLBACK_USED`），主模型恢复
  后自动 `PRIMARY_RECOVERED`。

## 3. Chaos 验收清单

恢复后必须成立的不变量（对应测试断言）：

1. 零部分状态：故障 attempt 不产生 Session 事件 / 版本推进 / outbox 提交。
2. 恰好一次：恢复后的重投递不重复调用模型、不重复提交事件。
3. 无跨租户：共享 worker 恢复后，租户 A 的会话永不出现租户 B 的内容。
4. 降级显式：所有允许的降级（模型 fallback 等）在降级注册表与审计链可见。
