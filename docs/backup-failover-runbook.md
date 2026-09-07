# 备份恢复与跨地域温备运行手册（Issue #36）

本运行手册定义备份/复制范围、温备地域约束、全局 Failover Lease 切换流程和
季度演练的 RPO/RTO 验收（RPO ≤ 5 分钟，RTO ≤ 60 分钟）。

## 1. 备份与复制范围

| 数据平面 | 机制 | 恢复验证 |
| --- | --- | --- |
| PostgreSQL | PITR（WAL 归档，恢复窗口 ≥ 1h）+ 跨地域流复制 | `verify_restore` 的 `pitr_probe`：恢复到目标时间戳并校验 schema 版本 |
| 对象存储 | Bucket Versioning + 跨地域复制 | `object_probe`：按版本恢复指定对象并比对 SHA-256 |
| 消息位点 | Outbox/消费位点随主库复制；Kafka 主题镜像 | `offsets_probe`：从复制位点重放后与权威状态一致 |
| 配置 | Release/Draft/Profile 快照在 SQL 内随库复制 | PITR 后行数与版本比对 |
| 密钥引用 | 仅引用（vault://）随库复制；凭据本体在 OpenBao 按区域注入 | 恢复后 secret_ref 可解析 |

`BackupManifest`（`trpc_service/backup.py`）是暖备区域必须满足的清单；
`check_rpo` 校验复制延迟 ≤ 300s 且对象版本、位点、配置/密钥引用全部捕获。

## 2. 温备地域约束（默认）

- 温备区域以 `failover.standbyMode: true` 部署：agent-gateway 与
  channel-gateway 的入口端点返回 **503 STANDBY_FENCED**，业务消费不启动。
- 复制与备份管道保持运行（数据温热），仅入口与消费被栅栏。

## 3. 全局 Failover Lease 切换流程

1. 值班操作员在管控库执行 `FailoverLeaseManager.promote(region, operator)`：
   - 事务内先 **FENCE 主区域**（旧 PRIMARY 行置 `FENCED` 并释放），
   - 再为提升区域写入 `PRIMARY` 行，fencing token 单调递增。
2. 原子性保证：advisory lock 串行化提升操作 + 部分唯一索引保证任一时刻
   至多一行活跃 PRIMARY——**双主在结构上不可能**。
3. 提升区域的应用实例重启后读取 `STANDBY_MODE=false`，开始服务；
   所有出站写携带新 fencing token，旧区域残留写入因 token 过期被拒绝。
4. 回切（FAILING_BACK）按同一流程反向执行。

## 4. 季度演练

- 运行 `run_failover_drill`（`trpc_service/backup.py`）：注入部署方
  `restore_probes`（Postgres PITR / 对象版本 / 位点重放），传入实测
  `measured_rpo_seconds` 与 `measured_rto_seconds`，结果落
  `platform.failover_drill` 表。
- 通过门槛：`drill_passes(rpo, rto)`——RPO ≤ 300s 且 RTO ≤ 3600s。
- 每次演练保留：演练窗口、注入的故障、恢复步骤计时、探针输出、
  Grafana 截图；未通过必须在下一窗口内复测并记录整改。

## 5. 告警联动

- `PlatformUnitMissingMetrics`（critical）：温备区域指标消失。
- `ExecutionPipelineFailures`：切换期间管线失败。
- 复制延迟建议由部署方在 Postgres exporter 侧配置 `> 300s` 告警，
  与 RPO 门槛一致。
