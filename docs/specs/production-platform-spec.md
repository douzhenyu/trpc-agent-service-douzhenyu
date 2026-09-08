# 完整生产级多租户节点化 Agent 部署平台

## Problem Statement

企业希望把基于 tRPC-Agent-Python 的单点 Agent 从演示脚本提升为可持续运营的平台能力。一个企业内部的多个部门、业务线和子公司需要各自创建 Agent 应用，绑定企业微信或飞书入口，选择模型、工具、Knowledge 和运行时数据后端，同时共享一套由企业控制的基础设施。

现有仓库只有题目说明和空工程骨架，不具备可运行的平台能力。tRPC-Agent-Python 已提供 Agent 编排、Runner、Tool/MCP、Session、Memory、Knowledge、Filter、Telemetry、AG-UI、A2A 和部分 IM 能力，但没有直接提供企业生产平台所需的租户控制面、跨节点一致性、持久幂等、不可变发布、预算、审批、审计、数据生命周期、灾备和完整管理界面。

问题的核心不是增加一个 `tenant_id` 字段，而是确保配置、身份、Session、Memory、Knowledge、工具副作用、密钥、预算、审计和运维操作都以租户为硬边界。平台还必须在重复、乱序、并发、节点故障、存储迁移、外部模型超时和 IM 投递结果不确定时保持可解释、可恢复且不会重复执行高风险副作用。

交付目标是一个可部署、可测试、可观测、可升级和可灾备的完整生产平台，而不是参考实现、伪代码、空壳 Adapter 或只覆盖 happy path 的演示项目。

## Solution

建设一套面向单企业私有化部署的多租户 Agent 平台。平台由 Admin API、Web Console、Agent Gateway、Channel Gateway、Agent Worker 和 Job Worker 六类独立部署单元组成，以 Kubernetes 作为唯一正式生产基座，以 Kafka 兼容执行总线连接数据面，以 PostgreSQL 保存权威状态，以 Redis 加速缓存和租约，以 PGVector 保存默认向量数据，以 S3 兼容对象存储保存 Artifact 和归档。

平台通过企业 OIDC、平台 RBAC、租户上下文、PostgreSQL RLS、复合外键、OPA Policy Bundle、tRPC-Agent Filter、Vault 兼容密钥服务和零信任工作负载身份建立纵深隔离。Agent 应用以 Agent Draft、不可变 Agent Release 和环境 Deployment 管理，生产晋级必须通过四眼审批、Eval Suite、离线 Eval Run 和灰度指标门禁。

企业微信和飞书作为首批正式 Channel Adapter。Channel Gateway 对入站事件验签、解密、规范化、持久去重并通过 Outbox 提交执行；Agent Worker 无状态运行固定 Agent Release，通过 Session 租约、fencing token 和乐观版本控制提交不可变 Session Event；Job Worker 异步生成 Summary、Memory、Knowledge Revision、审计归档、删除和迁移任务；回复使用可对账的至少一次投递状态机。

平台通过 Storage Adapter 支持租户绑定共享或专属运行时后端，并提供版本化在线迁移流程。所有模型调用默认经过 LLM Gateway，预算在调用前预留并在调用后结算。OpenTelemetry 贯穿 IM 回调、执行、模型、工具、存储和回复投递，审计使用独立的不可变证据链。

管理面提供版本化 REST/OpenAPI 和完整 Web Console；Agent Gateway 提供 HTTP、SSE、AG-UI 与 A2A。生产交付包含 Helm、Argo CD、Argo Rollouts、可观测性、备份恢复、灾备演练、容量验证、安全供应链和运行手册。

## User Stories

1. As a 平台管理员, I want to 创建租户, so that 企业内部组织可以拥有独立的平台边界。
2. As a 平台管理员, I want to 将多个租户加入 Tenant Group, so that 我可以汇总成本和 SLO 而不隐式获得成员租户数据权限。
3. As a 平台管理员, I want to 配置企业 OIDC, so that 平台用户使用现有企业身份登录。
4. As a 平台管理员, I want to 使用受审计的本地应急管理员, so that IdP 故障时仍能恢复管理面。
5. As a 租户管理员, I want to 为平台用户分配租户角色, so that 管理、开发、运维和审计职责相互隔离。
6. As a 审计员, I want to 查询权限变更历史, so that 我可以证明谁在何时获得或失去访问权。
7. As a Agent 开发者, I want to 创建和编辑 Agent Draft, so that 我可以在不影响生产流量的情况下配置 Agent。
8. As a Agent 开发者, I want to 为 Agent 应用配置指令、模型别名、工具、Knowledge 和治理策略, so that Agent 行为可以完整表达。
9. As a Agent 开发者, I want to 校验 Agent Draft, so that 无效引用、越权工具和不可用依赖不能进入 Release。
10. As a Agent 开发者, I want to 从 Agent Draft 生成不可变 Agent Release, so that 每次执行都可以精确复现。
11. As a 发布管理员, I want to 将 Agent Release 部署到 Development、Staging 或 Production, so that 环境边界清晰。
12. As a 发布管理员, I want to 按 Session 确定性灰度 Agent Release, so that 同一 Session 不会在一次交互中漂移版本。
13. As a 发布管理员, I want to 移动 Deployment 指针完成回滚, so that 配置回滚快速且不会篡改旧 Release。
14. As a 审批人, I want to 审批高风险生产变更, so that 发起人不能自行批准自己的变更。
15. As a Agent 开发者, I want to 为 Agent Release 绑定 Eval Suite, so that 生产准入标准是版本化和可审计的。
16. As a 发布管理员, I want to 查看 Eval Run 与当前生产 Release 的回归比较, so that 质量退化可以阻断发布。
17. As a 安全管理员, I want to 将跨租户泄露和越权工具用例设为零容忍, so that LLM Judge 不会成为唯一安全证据。
18. As a 租户管理员, I want to 创建模型配置档, so that Agent 应用只依赖稳定模型别名而不接触供应商凭据。
19. As a 租户管理员, I want to 限定模型可处理的数据分级和区域, so that 敏感数据不会被发送到不允许的 Endpoint。
20. As a 平台运维人员, I want to 通过 LLM Gateway 路由、限流和熔断模型调用, so that 供应商故障不会扩散到所有 Worker。
21. As a 财务管理员, I want to 配置租户月度、Agent 应用每日和单次执行预算, so that 成本可以硬性控制。
22. As a 财务管理员, I want to 查看预算预留、结算、释放和拒绝明细, so that 历史费用可以复算和归因。
23. As a IM 用户, I want to 通过企业微信与 Agent 交互, so that 我无需打开独立应用。
24. As a IM 用户, I want to 通过飞书与 Agent 交互, so that 企业可以选择现有协作平台。
25. As a 租户管理员, I want to 将一个 IM 机器人安全绑定到指定 Agent 应用, so that 外部事件只能路由到唯一租户和应用。
26. As a 租户管理员, I want to 为飞书选择 Webhook 或长连接接入, so that 网络条件和部署约束可以被满足。
27. As a 安全管理员, I want to 验证回调签名、时间窗和密文, so that 伪造或重放的 IM 请求被拒绝。
28. As a IM 用户, I want to 让重复投递复用原 Agent 执行, so that 我不会收到重复副作用或重复回复。
29. As a IM 用户, I want to 在单聊中获得稳定 Session, so that 连续对话具有上下文。
30. As a 群聊成员, I want to 让群聊和飞书话题使用确定的共享 Session 规则, so that 上下文不会泄露到其他群或话题。
31. As a 隐私管理员, I want to 禁止群聊注入成员私有 Memory, so that 个人信息不会暴露给群成员。
32. As a 租户管理员, I want to 经验证关联同一自然人的多个 IM 主体, so that 可以在授权后共享租户内跨通道 Memory。
33. As a IM 用户, I want to 接收适应通道能力的流式卡片或最终回复, so that 回复及时且不触发平台频控。
34. As a IM 用户, I want to 获得超长内容的安全分段或 Artifact 链接, so that 通道长度限制不会截断结果。
35. As a 平台运维人员, I want to 对结果不确定的 IM 投递先对账再重试, so that 重复通知风险可控。
36. As a API 客户端, I want to 通过 HTTP、SSE、AG-UI 或 A2A 调用 Agent Gateway, so that 标准协议客户端可以接入平台。
37. As a API 客户端, I want to 获得 execution、session、release 和 trace 标识, so that 异步执行可以查询和关联。
38. As a API 客户端, I want to 使用 Idempotency-Key 和 If-Match, so that 重试和并发更新不会产生重复或丢失变更。
39. As a IM 用户, I want to 在任意 Agent Worker 故障后继续 Session, so that 平台不依赖 Sticky Session。
40. As a 平台运维人员, I want to 通过 fencing token 和预期版本阻止过期 Worker 提交, so that 重平衡不会破坏 Session 顺序。
41. As a 数据工程师, I want to 从不可变 Session Event 重建 Session State 和 Summary, so that 派生状态损坏时可以恢复。
42. As a IM 用户, I want to 让当前回复不等待长期 Memory 写入, so that Memory 延迟不会增加交互尾延迟。
43. As a IM 用户, I want to 在后续节点上及时读取已经生成的 Memory, so that 水平扩展不会丢失长期上下文。
44. As a 隐私管理员, I want to 追踪每条 Memory 的来源 Session 和 Event 范围, so that 内容可以纠正、删除和审计。
45. As a 租户管理员, I want to 创建存储配置档, so that 不同租户可以选择共享或专属 SQL、Redis、向量、对象和外部 Memory 后端。
46. As a 高敏租户管理员, I want to 绑定独立存储和专属 Worker Pool, so that 高敏工作负载获得更强隔离。
47. As a 平台运维人员, I want to 在线迁移租户运行时后端, so that Redis 到 SQL 或本地向量到远端向量的迁移不需要停机式盲目双写。
48. As a 平台运维人员, I want to 在迁移切换前执行回填、追赶和校验, so that 切换不会丢失已提交数据。
49. As a 平台运维人员, I want to 在观察期内回滚存储配置档, so that 新后端异常时仍可恢复到完整源端。
50. As a Knowledge 管理员, I want to 管理 Knowledge Base 和文档 ACL, so that 知识访问遵守租户及文档边界。
51. As a Knowledge 管理员, I want to 构建不可变 Knowledge Revision, so that 来源、切分、Embedding 和索引参数可复现。
52. As a Knowledge 管理员, I want to 独立灰度和回滚 Knowledge Deployment, so that 更新知识无需重新发布 Agent Release。
53. As a Agent 开发者, I want to 为工具声明输入输出、权限和副作用等级, so that 平台可以决定重试、审批和隔离策略。
54. As a IM 用户, I want to 自行确认普通受控工具操作, so that 低风险流程不需要额外审批人。
55. As a 高风险审批人, I want to 查看不可变工具参数预览并批准或拒绝, so that HIGH_RISK 操作满足职责分离。
56. As a Agent Worker, I want to 在等待审批时释放计算资源并持久保存检查点, so that 长时间审批不会占用 Worker。
57. As a 外部系统所有者, I want to 接收稳定的工具幂等键, so that重试不会重复创建业务副作用。
58. As a 平台运维人员, I want to 将结果不确定的非幂等工具调用标记为 OUTCOME_UNKNOWN, so that 平台不会盲目重试。
59. As a 安全管理员, I want to 在 gVisor Sandbox 中执行不可信代码, so that Agent 生成代码不能访问宿主机和集群凭据。
60. As a 安全管理员, I want to 默认禁止 Sandbox 网络和提权能力, so that 代码执行遵循最小权限。
61. As a 租户管理员, I want to 只在平台配置中保存密钥引用, so that IM、模型和数据库秘密不会明文落库。
62. As a 安全管理员, I want to 轮换和吊销租户密钥而不重新发布应用代码, so that 密钥生命周期独立可控。
63. As a 策略管理员, I want to 发布签名且版本化的 Policy Bundle, so that Gateway 和 Worker 使用一致的治理决定。
64. As a 策略管理员, I want to 将治理结果表达为允许、拒绝或需要审批, so that决策具有结构化解释。
65. As a 数据保护人员, I want to 对信息采用 PUBLIC、INTERNAL、CONFIDENTIAL 和 RESTRICTED 分级, so that 模型、工具、日志和存储出口受到一致约束。
66. As a 数据保护人员, I want to 让 DLP 只能提高而不能自动降低数据等级, so that 自动化不会弱化安全边界。
67. As a 数据主体请求处理人员, I want to 发起删除请求并查看跨后端进度, so that Session、Memory、Artifact 和派生数据按期限删除。
68. As a 合规管理员, I want to 对指定数据设置 Legal Hold, so that 法律保留可以暂停正常过期。
69. As a 审计员, I want to 查询只追加的审计事件和签名 Manifest, so that 生产操作证据可验证且难以篡改。
70. As a 审计员, I want to 审计查询和导出行为本身, so that 对敏感证据的访问也有记录。
71. As a SRE, I want to 通过一个 trace 串联 IM 回调、Runner、模型、工具、Session、Memory 和回复投递, so that 跨组件故障可以定位。
72. As a SRE, I want to 查看请求量、延迟、错误、Token、成本、积压、存储和 IM 投递 Dashboard, so that SLO 和容量风险可运营。
73. As a 安全管理员, I want to 从日志、Trace、Kafka 和错误报告中排除秘密和原始身份, so that 可观测性不会成为泄露路径。
74. As a SRE, I want to 独立扩缩容 Gateway、Agent Worker 和 Job Worker, so that 不同负载类型不会相互拖累。
75. As a SRE, I want to 在三个故障域内维持数据面可用, so that Pod、节点或单可用区故障不会中断服务。
76. As a SRE, I want to 使用持久 Outbox 和死信恢复 Kafka 短暂故障, so that 已接收消息不会丢失。
77. As a SRE, I want to 在非关键 Knowledge 或 Memory 故障时执行明确降级, so that 用户知道回答能力受到限制。
78. As a 安全管理员, I want to 在权限、密钥、租约或副作用状态不确定时 fail closed, so that 平台不会绕过关键安全边界。
79. As a 灾备负责人, I want to 将 PostgreSQL、对象、消息位点、配置和密钥引用复制到温备地域, so that 整集群灾难可以恢复。
80. As a 灾备负责人, I want to 先 fencing 主集群再启用备用消费者, so that 跨地域不会出现双写和重复副作用。
81. As a 灾备负责人, I want to 每季度验证五分钟 RPO 和一小时 RTO, so that 灾备目标有实际证据。
82. As a 平台工程师, I want to 通过 Helm 与 Argo CD 管理 Kubernetes 期望状态, so that 生产发布可审计和可回滚。
83. As a 发布工程师, I want to 使用按 digest 固定且签名的镜像, so that 部署内容不会在发布后漂移。
84. As a 发布工程师, I want to 使用 Expand-Migrate-Contract 数据迁移, so that 滚动升级期间新旧版本可以共存。
85. As a 平台工程师, I want to 自动生成 SBOM 并扫描依赖、镜像和密钥, so that 供应链风险阻断生产发布。
86. As a 平台维护者, I want to 精确锁定并持续验证 tRPC-Agent-Python 稳定版, so that 平台不会意外跟踪 main 或未经验证地升级生产。
87. As a 平台维护者, I want to 在出现严重上游安全修复时快速评估升级, so that 已知漏洞不会长期保留。
88. As a Web Console 用户, I want to 管理租户、应用、Release、Deployment、模型、存储、通道、工具、Knowledge 和策略, so that 日常操作无需直接调用内部服务。
89. As a Web Console 用户, I want to 查看审批、执行、Session、Memory、Artifact、成本、审计、迁移、死信和健康状态, so that 平台形成完整运营闭环。
90. As a 平台管理员, I want to 使用默认简体中文且可国际化的控制台, so that 企业用户可以一致理解平台术语。
91. As a 架构评审者, I want to 阅读完整架构设计文档, so that 我可以理解系统边界、组件职责、关键取舍及 tRPC-Agent 复用范围。
92. As a 平台工程师, I want to 查看可版本控制的系统架构图, so that Gateway、Worker、Adapter、Filter、Telemetry、存储和 IM 平台之间的关系没有歧义。
93. As a 集成工程师, I want to 查看企业微信消息全链路时序图, so that 验签、幂等、执行、Tool、Session、Memory 和回复投递的顺序及失败点明确。
94. As a 数据工程师, I want to 获得核心数据模型、关系、索引和事件 Schema, so that 数据库、事件总线及 Adapter 可以按同一契约实现。
95. As a 分布式系统评审者, I want to 阅读数据同步和幂等策略, so that 并发、重试、迁移和最终一致性不会依赖隐含假设。
96. As a 租户运维人员, I want to 比较多后端的职责和一致性取舍, so that 我可以为租户选择合适的存储配置档。
97. As a 风险负责人, I want to 查看有责任人和验证方式的生产风险清单, so that 高影响失败模式在上线前得到缓解和演练。
98. As a 仓库维护者, I want to 获得与设计一致的完整 GitHub 实现代码和自动化资产, so that 平台可以从干净环境构建、测试、部署和恢复。

## Implementation Decisions

1. **交付边界**：交付完整生产平台。文档、伪代码、Mock 或空壳 Adapter 不能替代正式能力；每项生产声明必须有可执行实现、测试和部署资产支撑。
2. **运营模式**：首版是一企业一平台实例的私有化部署。企业内部组织是租户；Tenant Group 只聚合运营信息，不继承成员租户数据权限。
3. **部署单元**：Admin API、Web Console、Agent Gateway、Channel Gateway、Agent Worker 和 Job Worker 独立部署和扩缩容。Channel Adapter 是 Channel Gateway 插件，Storage Adapter 是进程内库，Filter 位于 Agent 执行管线。
4. **技术基线**：后端使用 Python 3.12 和精确锁定的 `trpc-agent-py==1.1.19`，复用其 Runner、Session、Memory、Knowledge、Tool/MCP、Filter、Telemetry、Evaluation、AG-UI 与 A2A 能力；管理前端使用 React、TypeScript 和 Vite。最终验收时如有更新稳定版，必须升级重测或记录可复现阻断。
5. **认证与授权**：平台用户通过企业 OIDC 登录，平台实施 RBAC；保留严格保护的本地应急管理员。IM 主体使用独立身份链路。服务间使用独立工作负载身份、Istio Ambient `STRICT` mTLS、默认拒绝 NetworkPolicy 和 AuthorizationPolicy。
6. **租户数据结构**：租户实体使用应用生成的 UUIDv7。租户表具有 `(tenant_id, id)` 唯一约束，租户内关系使用包含 `tenant_id` 的复合外键；PostgreSQL RLS 在事务租户上下文中强制执行，业务连接不得拥有 Owner、Superuser 或 BYPASSRLS 权限。
7. **核心领域实体**：至少包含租户、Tenant Group、平台成员关系、Agent 应用、Agent Draft、Agent Release、Deployment、模型配置档、存储配置档、通道绑定、IM 主体、主体关联、入站消息、Agent 执行、Session、Session Event、Session State、Summary、Memory、工具、工具调用、审批请求、Knowledge Base、Knowledge Revision、Knowledge Deployment、Artifact、回复投递、预算、成本账本、治理策略、Policy Bundle、审计事件、Outbox、删除请求、Eval Suite 和 Eval Run。
8. **控制面 API**：Admin API 使用 `/api/v1` REST/OpenAPI；写请求支持 Idempotency-Key，更新支持版本或 If-Match，长任务返回 operation ID，列表使用游标分页，并定义稳定错误码和弃用周期。Web Console 只调用公开 Admin API。
9. **Agent 协议**：Agent Gateway 提供 HTTP、SSE、AG-UI 和 A2A；IM 协议只进入 Channel Gateway。执行响应公开 execution、session、release 和 trace 标识，不引入 GraphQL。
10. **执行总线**：生产使用 Apache Kafka 兼容持久总线和至少一次投递。执行按 `(tenant_id, session_id)` 分区；入站、执行结果、Memory、审计、回复投递、重试和死信使用版本化领域事件。事件采用 CloudEvents 1.0 风格信封、JSON Schema 和向后兼容 Schema Registry，不序列化 tRPC-Agent 内部对象。
11. **入站幂等**：Channel Gateway 以租户、通道绑定和外部事件 ID 建立 PostgreSQL 幂等账本；没有稳定 ID 时使用版本化规范字段摘要。在一个事务内创建入站消息、唯一 Agent 执行和 Outbox，提交后才确认外部回调。相同键不同 Payload 哈希必须隔离告警。
12. **无状态执行**：不使用 Sticky Session。Kafka 分区提供正常顺序，fencing 租约和 `expected_version` 处理重平衡及并发；失去租约或版本冲突的 Agent Worker 不得提交结果。单次 Agent 执行固定其 Agent Release、Knowledge Revision、模型、工具和策略版本。
13. **Session 权威性**：Session Event 是只追加权威事实；Session State 和 Summary 是带来源版本的可重建投影。提交在同一事务内验证幂等、fencing 和预期版本，追加事件、递增版本、更新 State 并写 Outbox。旧 Summary 结果不能覆盖新版本。
14. **Memory 一致性**：Job Worker 只从已提交 Session Event 异步生成 Memory，正常跨节点可见目标为 P99 五秒以内。Memory 保留主体、来源 Session、Event 范围和策略版本，并以来源范围幂等；Memory 故障不阻塞当前回复。
15. **默认存储配置档**：PostgreSQL 16 高可用集群保存控制面、Session 权威记录、幂等、Outbox 和审计索引；Redis 7 Cluster 保存缓存、限流、租约和热投影；独立 PostgreSQL/PGVector 保存 Knowledge 和语义 Memory；S3 兼容对象存储保存 Artifact、知识源和归档。InMemory 只用于单元测试。
16. **多后端与迁移**：Storage Adapter 保持统一领域语义。在线迁移执行准备、快照回填、Outbox/CDC 追赶、校验、原子切换、观察和完成状态机；源端在切换前保持权威，业务代码不得无协调双写。向量 Embedding 变化必须重建索引并影子验证。
17. **IM 通道**：首批正式支持企业微信智能机器人和飞书。企业微信可使用 HTTPS 回调或 Bot ID/Secret 认证的 WSS 长连接；长连接配置显式绑定一个租户，并在入站账本前再次解析该租户的 Channel Binding。飞书同时支持 Webhook 与长连接，默认 Webhook；长连接由分布式租约确保单一有效所有者。所有通道实现验签、解密（适用时）、频控、附件检查、消息长度处理和能力协商。
18. **IM 身份与 Session**：IM 主体键为租户、通道绑定和外部用户；单聊 Session 还绑定 Agent 应用与 generation，群聊绑定稳定 chat ID，飞书话题增加 thread ID。跨群、跨绑定和跨租户默认不共享 Session；跨通道 Memory 需要经验证主体关联。
19. **回复投递**：每个逻辑回复具有稳定 delivery ID，每次尝试具有 attempt ID，并按外部会话排序。发送前持久化；超时进入 OUTCOME_UNKNOWN 并优先查询对账；超限退避，超期进入死信且以原 delivery ID 重放。平台不承诺外部 IM Exactly Once。
20. **流式体验**：单聊使用合并增量更新，群聊默认处理中状态加最终回复；企业微信使用其流式能力，飞书优先流式卡片。超长内容分段或 Artifact 化，不逐 Token 调用 IM API。
21. **工具治理**：工具声明 READ_ONLY、IDEMPOTENT_WRITE、NON_IDEMPOTENT_WRITE 或 HIGH_RISK，并声明 Scope、超时、费用、数据分级和主体限制。只读可退避重试，幂等写只在下游支持幂等键时重试，非幂等写结果不确定时禁止盲目重试。
22. **审批与恢复**：普通受控操作可以由具备权限的请求者确认，HIGH_RISK 必须由另一审批角色批准。审批请求绑定 Release、工具版本、参数哈希、请求者、策略版本和十五分钟默认有效期。等待状态不占 Agent Worker，恢复前重新取得 Session 租约并重新校验不可变绑定。
23. **代码沙箱**：实现基于 tRPC-Agent BaseCodeExecutor 的 Kubernetes Sandbox Executor，默认 gVisor。Sandbox 非 root、只读根文件系统、零 capabilities、无 ServiceAccount Token、默认禁网并限制 CPU、内存、时间和输出。生产禁止本地不安全执行器和 Docker Socket。
24. **密钥管理**：生产采用 OpenBao/Vault HA 或兼容服务。平台只保存租户范围 `secret_ref`，Pod 通过 Kubernetes Auth 获取短期凭据，静态秘密使用 KV v2，租户数据使用 Transit 信封加密。密钥正文不得进入 API 响应、日志、Trace、Kafka、Session Event 或错误信息。
25. **治理策略**：结构化治理策略经校验、签名和版本化生成 Policy Bundle，由本地 OPA Sidecar 执行；tRPC-Agent Filter 负责提供规范上下文和落实 allow、deny、needs_approval、脱敏、预算与数据范围。敏感决策默认 fail closed，Filter 不是唯一权限边界。
26. **数据分级**：使用 PUBLIC、INTERNAL、CONFIDENTIAL 和 RESTRICTED。DLP 只能提高等级，聚合内容取最高等级。RESTRICTED 禁止进入外部模型，秘密检测直接阻断；高敏降级和敏感导出需要职责分离审批。
27. **Release 与评测**：Agent Draft 经 Schema、密钥引用、权限、连通性和策略校验后生成不可变 Agent Release。Production Deployment 必须通过 Eval Suite 和可复现 Eval Run；安全确定性断言零容忍，LLM Judge 不作为唯一安全依据。灰度超阈值时停止推广或回滚。
28. **Knowledge 生命周期**：Knowledge Base 是逻辑集合，Knowledge Revision 固定来源、切分、Embedding、索引和 ACL，Knowledge Deployment 决定环境在线版本。Revision 使用蓝绿索引构建、验证和切换；每次 Agent 执行记录实际 Revision。
29. **预算与成本**：在租户月度、Agent 应用每日和单次执行三级实施预算预留与结算。70% 和 90% 告警，100% 拒绝新预留；预算状态无法确认时 fail closed，可为关键应用配置独立应急额度。价格表和成本账本版本化。
30. **可观测性**：交付 OpenTelemetry Collector、Prometheus、Tempo、Loki 和 Grafana。Trace Context 通过 HTTP 和 Kafka 传播；Memory、Summary 和回复投递使用 Span Link。敏感 Attribute 使用允许列表，外发模型前剥离内部 Baggage。
31. **审计**：业务事务同步写 Audit Outbox，异步落 PostgreSQL 在线索引，并按租户和日期生成哈希链与签名 Manifest，归档到 S3 Object Lock。审计默认不含正文和秘密，查询、导出、更正和 Break-glass 均产生审计事件。
32. **保留与删除**：默认原始 IM Payload 七天、Session 内容最后活跃后九十天、Memory 最后使用或验证后一年、普通 Artifact 三十天、幂等墓碑一年、备份三十五天；Knowledge 按显式生命周期。删除请求覆盖 SQL、Redis、向量、对象、缓存和派生数据，主存储二十四小时内完成，备份在三十五天内失效，Legal Hold 可以暂停。
33. **故障语义**：状态持久化、安全、密钥、租约和副作用结果不确定时 fail closed。Kafka 故障由已提交 Outbox 积压，Redis 缓存可绕过但租约不可绕过；Memory 可显式降级到 Session，Knowledge、Artifact 和模型 fallback 只有 Agent Release 允许时才能使用。任何降级均产生用户状态、审计、指标和告警。
34. **容量与 SLO**：单集群持续一千入站消息每秒，三千每秒突发六十秒，至少一万个并发 Agent 执行。数据面月可用性目标 99.95%，控制面 99.9%；至少跨三个故障域部署，并通过负载和故障测试形成资源基线。
35. **灾备**：地域内多可用区 Active，跨地域 Warm Standby。目标 RPO 五分钟、RTO 一小时。备用持续复制 WAL、对象、关键消息位点、配置和密钥引用但不启用业务入口或消费者；切换先取得全局 Failover Lease 并 fencing 主集群。每季度演练。
36. **GitOps 与供应链**：Kubernetes 期望状态由 Helm 与 Argo CD 管理，Argo Rollouts 执行金丝雀。镜像固定 digest、生成 SBOM、扫描并签名；CI 不持有生产集群管理员权限。数据库使用 Expand-Migrate-Contract 和受控迁移 Job，业务配置不进入 Git。
37. **升级策略**：依赖和锁文件精确固定。每日检查 tRPC-Agent-Python 官方稳定 Release 和 PyPI，自动创建升级 PR，但不自动升级生产；普通版本七日内评估，严重安全修复四十八小时内评估，经过完整回归、Staging 和灰度后发布。
38. **交付物即产品能力**：架构文档、架构图、核心时序图、数据模型、同步与幂等策略、多后端适配方案、生产风险清单和 GitHub 实现代码均为强制验收项。文档必须引用已接受 ADR、使用统一语言，并与实际 API、Schema、配置、测试及部署资产相互校验；发现偏差时先修正文档或以新 ADR 修改决策，不能把过期设计作为验收依据。

## Testing Decisions

1. **主要测试接缝**是部署后平台的公开边界。测试通过 Admin API 配置系统，通过 Agent Gateway 或 Channel Gateway 输入请求，通过公开查询 API、事件流和模拟 IM 接收端观察结果，不直接调用内部 Worker、Repository 或私有函数断言业务行为。
2. **真实基础设施**测试使用真实 PostgreSQL、Redis、Kafka、S3 兼容对象存储和 OPA；模型、工具、企业微信及飞书使用可编排 Fake，以稳定制造成功、重复、乱序、限流、超时、断连和结果不确定场景。
3. **单一测试框架**承载端到端、通道回放、多后端一致性、安全攻击、故障注入、升级、回滚和灾备数据集，避免为每个组件形成一套互不一致的顶层接缝。
4. **行为优先**：测试外部可观察的状态转换、响应、投递、审计、指标和不变量，不断言内部函数调用次数、私有对象结构或实现顺序。
5. **单元测试范围**限于规范化摘要、Session ID、参数哈希、预算计算、数据分级合并、重试决策和状态机等纯算法，以及难以从公开边界经济覆盖的错误分支。
6. **Adapter 合约测试**对所有 Channel Adapter、Storage Adapter、模型和工具执行器运行同一行为规范，保证后端替换不会改变租户隔离、幂等、版本和错误语义。
7. **租户安全测试**必须验证 RLS、复合外键、缓存键、Kafka 事件、对象路径、向量过滤、日志和 Trace 均不能跨租户泄露；隔离、授权、幂等、预算、工具和迁移模块分支覆盖率至少 90%。
8. **发布质量门禁**要求整体分支覆盖率至少 80%，并执行类型检查、静态分析、依赖和镜像扫描、秘密扫描、SBOM、签名校验、OpenAPI/JSON Schema/Policy 合约兼容测试。
9. **tRPC-Agent 回归**覆盖 Runner、Session、Memory、Summary、Filter、Tool、Knowledge、Evaluation、Channel、AG-UI 和 A2A，运行时还必须验证实际 SDK 版本与锁定版本一致。
10. **性能验收**验证持续一千消息每秒、三千消息每秒六十秒突发和一万个并发执行，并报告 Gateway、Kafka、Worker、PostgreSQL、Redis、模型和 IM 投递的饱和点与资源曲线。
11. **故障与恢复测试**覆盖 Pod/节点/可用区失效、Kafka 和数据库短暂不可用、Redis 租约故障、模型超时、工具 OUTCOME_UNKNOWN、IM 限流、Outbox 积压、死信重放和 Storage Adapter 迁移回滚。
12. **灾备验收**必须从独立温备环境执行恢复，证明全局 fencing、数据一致性、消息位点、入口切换、RPO 和 RTO，而不是仅验证备份文件存在。
13. **评测门禁测试**固定 Eval Suite、数据集、评分器和所有依赖版本；确定性安全失败直接阻断，质量、成本和延迟与当前生产 Agent Release 比较。
14. 当前仓库没有可复用的既有测试实现，因此首次实现需要建立上述公开边界测试框架；以后新增能力优先扩展该接缝，而不是新增更低层的并行验收体系。
15. **文档一致性测试**在 CI 中校验内部链接、Mermaid 源、OpenAPI、JSON Schema、数据库迁移及配置示例；架构组件、领域实体、事件类型和公开接口必须能够追溯到实现及测试，图表不能引用不存在或已经删除的组件。

## Required Deliverables

以下八项均为项目最终验收的必要交付物。它们可以相互引用，但不能用一个笼统 README 替代，也不能只存在于 GitHub Issue 评论中。

### 1. 架构设计文档

- 提供一份完整架构设计正文，建议为 2000–4000 中文字；ADR、图表、Schema 和附录不计入该建议长度。
- 说明业务目标、私有化及租户边界、控制面与数据面、六类部署单元、关键数据流、扩缩容、隔离、安全、可观测性、故障恢复和灾备。
- 明确列出哪些能力直接复用 tRPC-Agent-Python，哪些由平台层新增，哪些由 PostgreSQL、Kafka、Redis、OPA、Vault、Kubernetes 等基础设施承担。
- 每个关键架构选择链接到对应 Accepted ADR；架构文档不重复创造与 ADR 冲突的决定。
- 包含最小开发拓扑与生产推荐拓扑，并说明二者不能混用的能力边界。

### 2. 系统架构图

- 提供可版本控制的图源，并提供可直接查看的渲染结果；推荐使用 Mermaid 图源并在文档构建中生成 SVG。
- 至少展示 Admin API、Web Console、Agent Gateway、Channel Gateway、Channel Adapter、Agent Worker、Job Worker、Storage Adapter、Filter、LLM Gateway、Telemetry、Kafka、PostgreSQL、Redis、向量库、对象存储、OPA、Vault、企业微信和飞书。
- 使用不同视觉边界标识控制面、数据面、基础设施、企业外部系统、环境和租户隔离范围。
- 用有方向的连线区分同步调用、异步事件、遥测、密钥解析和数据持久化，不能只画无语义连线。
- 图中每个组件名称必须使用统一语言，且与实际部署单元或明确标注的进程内模块一一对应。

### 3. 核心时序图

- 提供“企业微信用户发送消息 → Channel Gateway 验签及去重 → Outbox/Kafka → Agent Worker → tRPC-Agent Runner → Tool → Session Event/State → 异步 Summary/Memory → 回复投递”的完整时序。
- 明确 trace ID、external event ID、inbound message ID、execution ID、session ID、tool invocation ID、idempotency key 和 delivery ID 的创建及传播位置。
- 标出事务提交点、外部回调确认点、Session 租约与 fencing、预算预留与结算、Filter/OPA 决策、Tool 审批等待及恢复。
- 标出数据库、Kafka、模型、Tool 和企业微信超时或结果不确定时的重试、对账、降级和死信分支。
- Memory 必须表现为消费已提交 Session Event 的异步流程，不能画成当前回复事务内的强一致写入。

### 4. 数据模型设计

- 提供核心 ER 图及数据字典，至少覆盖租户、Agent 应用、Agent Release、Deployment、通道绑定、IM 主体、入站消息、Agent 执行、Session、Session Event、Session State、Summary、Memory、工具调用、审批请求、Artifact、Knowledge、预算、成本账本、Outbox、回复投递和审计事件。
- 对每个核心实体说明主键、`tenant_id`、复合外键、唯一约束、版本字段、状态字段、重要索引、保留策略和数据分级。
- 明确全局表与租户表、PostgreSQL RLS 策略、业务角色权限、时间分区和 UUIDv7 生成责任。
- 提供 Kafka CloudEvents 风格信封和关键领域事件的版本化 JSON Schema，说明兼容性、必填字段、内容大小及敏感数据限制。
- 说明权威记录与投影：Session Event、成本账本和审计事件只追加；Session State、Summary、缓存和向量索引可重建。

### 5. 数据同步和幂等策略

- 独立说明同一 Session 多节点并发、Kafka 分区、fencing 租约、乐观版本和过期 Worker 禁止提交的组合语义。
- 说明 Session Event、Session State、Summary、Memory 和 Outbox 的提交顺序、事务边界、失败恢复及跨节点可见性目标。
- 说明入站 IM、Admin API、Agent 请求、工具副作用、Kafka 消费、Memory 生成和回复投递各自使用的幂等键、唯一约束和墓碑保留。
- 说明 READ_ONLY、IDEMPOTENT_WRITE、NON_IDEMPOTENT_WRITE 和 HIGH_RISK 工具的不同重试与 OUTCOME_UNKNOWN 处理。
- 说明 Redis 到 SQL、向量后端迁移和 Embedding 变更时的回填、增量追赶、校验、切换、观察及回滚。
- 提供强一致、最终一致、读写延迟、成本、可用性和运维复杂度的取舍表，不能笼统宣称“最终一致”。

### 6. 多后端适配方案

- 给出 Storage Adapter 的统一契约、租户上下文、能力发现、健康检查、错误分类、幂等、迁移、删除和一致性测试要求。
- 说明 PostgreSQL 适合权威控制面、Session Event、事务幂等、Outbox、预算和在线审计索引。
- 说明 Redis 适合缓存、限流、fencing 租约和可重建热投影，且不得成为不可重建数据的唯一权威来源。
- 说明向量库适合 Knowledge 与语义 Memory 检索，并强制租户、Revision、ACL、Embedding 版本和删除过滤。
- 说明 S3 兼容对象存储适合 Artifact、知识源、导出、备份及 WORM 审计归档，并定义校验和、版本、加密和生命周期。
- 说明外部 Memory 或专属后端的接入条件、能力差异和降级规则；每种实现必须通过相同 Adapter 合约测试。
- 提供共享后端、逻辑隔离、专属后端和专属 Worker Pool 的成本及隔离对比。

### 7. 生产风险清单

- 风险清单至少包含风险标识、触发条件、影响范围、发生可能性、严重度、预防措施、检测信号、恢复措施、责任角色和验证方式。
- 至少覆盖下列初始风险；实施阶段可以新增，但不得无证据删除。

| 风险 | 主要影响 | 必需缓解与验证 |
|---|---|---|
| 跨租户数据泄露 | 合规及业务数据暴露 | 复合外键、RLS、租户上下文、Adapter 过滤和攻击测试双重阻断 |
| IM 重复或乱序投递 | 重复回复、重复 Tool 副作用、Session 顺序损坏 | 持久幂等账本、会话分区、Payload 哈希冲突隔离和回放测试 |
| Session 并发写冲突 | State、Summary 与对话上下文不一致 | fencing 租约、预期版本、过期 Worker 拒绝及重平衡故障测试 |
| 非幂等 Tool 结果不确定 | 外部系统发生重复且不可逆操作 | 持久工具调用、下游幂等键、OUTCOME_UNKNOWN 对账和人工处置演练 |
| 模型或 Tool 泄露敏感数据 | 密钥、个人信息或机密外发 | 数据分级、DLP、OPA/Filter、出站 allowlist 和秘密泄露测试 |
| Kafka 或 Outbox 长时间积压 | 回复、Memory、审计和删除延迟 | 容量水位、分级告警、背压、扩容、死信与积压恢复演练 |
| PostgreSQL 故障或 RLS 误配置 | 权威状态不可用或隔离失效 | 多可用区 HA、非特权角色、迁移门禁、PITR 和 RLS CI 测试 |
| Redis 租约故障 | 并发执行或平台错误降级 | Redis Cluster、fencing token、租约不可绕过和故障注入 |
| 存储在线迁移数据缺失 | 租户历史、向量或 Artifact 不完整 | 版本化状态机、回填、CDC/Outbox 追赶、对账、观察期与回滚 |
| IM 投递 OUTCOME_UNKNOWN | 用户看不到回复或收到重复消息 | 稳定 delivery ID、通道查询对账、保守重试策略和人工重放 |
| Sandbox 逃逸或资源滥用 | 集群、数据或供应链受损 | gVisor、默认禁网、非 root、资源限额、镜像固定及逃逸 Smoke Test |
| 密钥服务或 OIDC 不可用 | 管理、模型、通道或存储访问中断 | 短期缓存边界、应急管理员、密钥轮换、fail closed 和恢复演练 |
| 预算并发超扣 | 租户产生不可接受费用 | 调用前预留、不可变成本账本、最坏用量上限和并发测试 |
| 上游 SDK 升级回归 | Runner、Session、Tool 或协议行为改变 | 精确锁定、自动升级 PR、全量回归、Staging、灰度及指针回滚 |
| 灾备双主 | 跨地域重复消费、双写和重复副作用 | 全局 Failover Lease、主集群 fencing、温备默认禁用及季度演练 |
| 可观测性采集敏感正文 | 日志和 Trace 成为泄露副本 | Attribute allowlist、正文默认关闭、Baggage 清除和遥测扫描测试 |

### 8. GitHub 实现代码

- 仓库必须包含完整后端、Web Console、数据库迁移、事件 Schema、Policy Bundle、Channel Adapter、Storage Adapter、测试、容器构建、Helm、GitOps、可观测性、备份恢复和运维脚本，而不是只有接口或目录占位。
- 依赖必须由锁文件精确固定，构建不得依赖开发者机器上的未声明软件或未提交文件。
- 提供不含真实秘密的开发配置和 Fake 外部服务，使评审者可以从干净环境运行公开边界验收测试。
- CI 必须执行格式化检查、类型检查、单元及集成测试、公开边界端到端测试、覆盖率、安全扫描、Schema/Policy 合约、SBOM 和镜像签名验证。
- 生产配置只保存密钥引用；仓库历史、示例、测试夹具和错误快照不得包含真实凭据或生产数据。
- README 必须给出构建、启动、停止、测试、最小开发拓扑和生产部署入口，并链接全部正式交付文档。
- 完成定义是代码、测试、图表、数据模型、运行手册和实际部署行为一致；单独提交设计文档或单独提交无法部署的代码均不算完成。

## Out of Scope

1. 公有多企业 SaaS 的订阅、计费、合同、区域数据驻留和商业运营能力。
2. 跨地域 Active/Active 写入；首版采用地域内 Active、跨地域 Warm Standby。
3. Kubernetes 之外的生产运行基座；Docker Compose 仅用于本地开发和集成测试。
4. 用 Go 重写 tRPC-Agent-Python 的核心 Runner、Session、Memory、Knowledge 或 Tool 链路。
5. Telegram 作为首批正式 IM 通道；首批只交付企业微信和飞书。
6. 对外部 IM 声称端到端 Exactly Once；平台提供持久、幂等、可对账的至少一次语义。
7. 允许租户父子关系自动继承数据访问权；Tenant Group 不改变硬租户边界。
8. 在生产中使用 InMemory 权威存储、UnsafeLocalCodeExecutor、宿主 Docker Socket 或无 gVisor 的静默降级沙箱。
9. 在平台数据库、Git、日志或 Trace 中保存模型、IM、数据库或对象存储密钥正文。
10. 自动将生产会话正文导入评测集；评测数据必须经过分级、授权和脱敏。
11. 通过回滚应用版本自动撤销已经提交的数据迁移、工具副作用或外部 IM 投递。
12. 未经验证自动合并跨通道主体，或跨租户共享 Session、Memory、Knowledge 和密钥。

## Further Notes

- 项目统一语言、边界和 `_Avoid_` 规则是本规格解释术语的权威来源。
- 已接受的 45 项 ADR 是本规格的架构约束；如果实施发现矛盾，必须以新 ADR 明确替代，不得静默偏离。
- tRPC-Agent-Python v1.1.19 是规格形成时验证过的最新稳定版。版本号是可受控升级的依赖基线，不是放弃跟踪上游的永久冻结。
- 本规格描述完整目标平台。后续应以 tracer-bullet 垂直切片拆分工单，使每个工单在单个上下文内可独立演示和验证，同时保留本规格的全部最终验收目标。
- 规格发布后只进入 `ready-for-agent` 状态，不代表已经拆分工单、开始实现或完成代码审查。
