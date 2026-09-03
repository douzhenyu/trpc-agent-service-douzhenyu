# 强制零信任服务通信

生产环境使用 Istio Ambient Mesh 为平台工作负载建立零信任通信基线：每个服务使用独立 Kubernetes ServiceAccount 和工作负载身份，服务间连接强制 `STRICT` mTLS，并通过默认拒绝的 Kubernetes NetworkPolicy 与 Istio AuthorizationPolicy 仅开放已声明调用路径。用户、租户与权限上下文通过短期签名凭证传播，任何服务不得仅信任调用方提交的 `tenant_id`；外部流量统一经过受控 Ingress/Egress Gateway，并按目的地、协议和数据分级放行。生产、测试及灾备使用独立信任域并自动轮换证书；身份或网格控制能力不可用时，新建关键连接失败关闭，不得回退明文。平台保留 SPIFFE 兼容身份语义及外部企业 CA 接入点，但默认生产拓扑不额外强制部署 SPIRE。

本决策在发布时序上部分取代 [ADR-0037](0037-gitops-with-helm-and-argo.md) 的迁移 `PreSync` 约定。Argo CD 必须先以常驻资源建立 mTLS、Waypoint、namespace 默认拒绝、无 endpoints 的 Service、授权策略与迁移任务网络边界，再以 wave `0` 的 `Sync` hook 执行数据库迁移，最后创建应用工作负载。这样迁移仍是发布阻断门禁，同时首次安装、重试和后续同步都不会出现迁移任务早于安全基线运行或因 hook 重建安全资源而短暂失败开放的窗口。
