# Kubernetes 生产部署骨架

Kubernetes 是唯一正式支持的生产运行基座。Docker Compose 仍只用于本地开发和最小公开边界测试；本页的 Helm、Argo CD 与 Argo Rollouts 资产不读取 Compose 配置，也不依赖 Compose 服务名或网络。

## 六类部署单元

| 单元 | 工作负载 | HPA 最小副本 | 健康路径 | GitOps 应用 |
|---|---|---:|---|---|
| Admin API | Deployment | 2 | `/api/v1/health` | `trpc-agent-platform-admin-api` |
| Web Console | Deployment | 2 | `/` | `trpc-agent-platform-web-console` |
| Agent Gateway | Rollout | 3 | `/health/ready` | `trpc-agent-platform-agent-gateway` |
| Channel Gateway | Rollout | 3 | `/health/ready` | `trpc-agent-platform-channel-gateway` |
| Agent Worker | Rollout | 3 | `/health/ready` | `trpc-agent-platform-agent-worker` |
| Job Worker | Rollout | 2 | `/health/ready` | `trpc-agent-platform-job-worker` |

每个单元有独立的 production values、稳定命名且不共享的 ServiceAccount、Service、HPA、PDB 和 NetworkPolicy。工作负载不声明 `spec.replicas`，副本下限由 HPA 唯一管理，避免 Argo CD `selfHeal` 与 HPA 相互回写。Gateway 与 Worker 使用 Argo Rollouts 金丝雀；Admin API 与 Web Console 使用 Kubernetes Deployment。当前 Gateway/Worker 进程只实现本工单要求的健康边界，后续业务工单在同一部署边界内替换健康骨架。

Admin API release 同时拥有 namespace 级 `ResourceQuota` 和 Argo CD `Sync` 数据库迁移 hook；其余五个 release 显式关闭这两个共享资源，避免重复配额叠加。当前尚无数据库 schema revision，迁移入口会报告零项变更；后续数据库工单在这个受控入口接入 Alembic，迁移失败将阻断应用同步。

## 零信任通信边界

ApplicationSet 创建生产 namespace 时会写入 `istio.io/dataplane-mode=ambient`、`istio.io/use-waypoint=platform-waypoint` 和 `istio.io/ingress-use-waypoint=true`。Admin API release 负责创建 namespace 级 `STRICT` PeerAuthentication 和共享 Waypoint；其他 release 只创建自己单元的策略，避免共享资源被多个 Argo CD Application 争用。所有常驻安全资源都是 Argo CD 正常管理的期望状态，不使用会先删后建的 hook；sync wave 依次落地 mTLS/Waypoint（`-4`），namespace deny-all 与尚无 endpoints 的 Service（`-3`），已被 Istio 接受的授权策略（`-2`）和单元网络放行（`-1`），数据库迁移 Sync hook 位于 `0`，工作负载位于 `1`。首次部署和后续同步都保持失败关闭。

每个单元有两层默认拒绝：共享 NetworkPolicy 先兜底隔离所有平台 Pod（包括 Sync 迁移任务），单元 NetworkPolicy 再在 L4 只开放 Ambient HBONE `15008`、Ambient 固定 kubelet 探针地址、DNS、声明出站所需的 Waypoint HBONE 通道以及受控 egress gateway；Istio AuthorizationPolicy 在 ztunnel 只接受 Waypoint 身份，在 Waypoint 再按调用方 ServiceAccount、HTTP 方法和路径放行。迁移任务在声明数据库出口前保持完全隔离。Web Console 当前只可用 `GET /api/v1/health` 调用 Admin API。平台策略拒绝携带裸 `x-tenant-id` 的内部请求；后续业务接口必须从短期签名凭证验证租户上下文，不能信任调用方自报的 tenant id。

`zeroTrust.egressGateway` 指向唯一允许的外部出口。当前六类骨架没有声明任何外部依赖，因此 Chart 只预留到 gateway 的 L4 通道，不创建宽泛的 ServiceEntry 或透明外部路由；集群必须在外部依赖接入时部署与该 namespace、Pod 标签和 HBONE 端口匹配的 egress gateway，并在平台外的网格基础设施层按获批目的地配置 ServiceEntry/路由和审计策略。缺失这些资源时外部连接按设计失败关闭，直接访问公网始终不在工作负载 NetworkPolicy 的允许列表中。

## 前置条件

- Kubernetes 集群跨至少三个故障域，节点带 `topology.kubernetes.io/zone` 标签。
- 已安装 Gateway API 1.6.0、Istio 1.31.0 Ambient、Argo CD 3.5.2、Argo Rollouts 1.10.0 和 Metrics API；Istiod 已启用 `ENABLE_INGRESS_WAYPOINT_ROUTING`，ApplicationSet Controller 已启用 Progressive Syncs。
- `zeroTrust.trustDomain` 与环境的 Istio trust domain 一致；生产、测试和灾备分别覆盖为各自的值。
- `argo-rollouts` namespace 已加入 Ambient，使金丝雀分析请求持有 `argo-rollouts/argo-rollouts` 工作负载身份。
- 入站 gateway 使用默认的 `istio-ingress/istio-ingress` 身份并通过 Istio mTLS 访问声明路径；外部出口 gateway 与 `zeroTrust.egressGateway` 配置一致。若集群使用不同名称，应在 production values 中显式覆盖调用方或出口配置。
- 六类镜像已经由供应链流程构建；production values 中只允许 `repository@sha256:digest`，不得使用可变 tag。
- Argo CD 的 `argocd` namespace 已存在，并可读取本仓库。

Chart 默认 digest 是不可部署的全零占位值。首次同步前必须把 `deploy/helm/trpc-agent-platform/values.yaml` 中的平台镜像和 Web Console 镜像替换为已扫描、签名的真实 digest。Kind 黑盒测试同样从本地构建镜像的内容 digest 部署，不放宽成可变 tag。

## 渲染与校验

安装固定版本的本地工具：

```bash
scripts/install_kubernetes_tools.sh /tmp/trpc-platform-tools
export PATH="/tmp/trpc-platform-tools:${PATH}"
```

检查完整 Chart：

```bash
helm lint deploy/helm/trpc-agent-platform
helm template platform deploy/helm/trpc-agent-platform >/tmp/trpc-platform-rendered.yaml
uv run pytest tests/deployment/test_helm_contract.py tests/deployment/test_zero_trust_contract.py
```

只渲染一个单元，例如 Agent Worker：

```bash
helm template agent-worker deploy/helm/trpc-agent-platform \
  --values deploy/gitops/production/values/agent-worker.yaml
```

## GitOps 安装与独立变更

更新镜像 digest 并完成评审后，先安装受限 AppProject，再安装 ApplicationSet：

```bash
kubectl apply -f deploy/gitops/production/project.yaml
kubectl apply -f deploy/gitops/production/applicationset.yaml
```

ApplicationSet 为六类单元分别生成一个 Argo CD Application，但部署到同一个 `trpc-agent-platform` namespace，使 Web Console、健康分析和后续内部服务可以使用稳定的 Kubernetes Service 名称。RollingSync 首先同步 Admin API，并在零信任基线落地后等待其 Sync 迁移 Job 与应用全部 Healthy，之后才并行放行其余五个单元；迁移失败会阻断整个生产切片。每个 Application 仍只渲染自己的单元，因此某个单元的镜像、资源或 HPA 范围变化不会重启其他单元。

扩缩容或升级时修改对应的 `deploy/gitops/production/values/<unit>.yaml` 中 HPA 范围和该单元的镜像 digest，经评审合入后由 Argo CD 同步。不要直接长期修改线上 HPA，`selfHeal` 会把未入 Git 的策略漂移恢复为声明状态；HPA 产生的工作负载副本变化不会出现在 Helm 渲染结果里，因此不会被 Argo CD 回写。

## 金丝雀与回滚

Rollout 首先把 10% 副本切到 canary，暂停后调用 canary Service 的健康接口。分析同时校验 HTTP 200、`status=ok` 和响应中的单元名，防止错误 Service 返回 200 被误判为健康。门禁失败时 Rollouts 中止推广并保持 stable Service 指向上一版本。

查看和回滚指定单元：

```bash
kubectl argo rollouts get rollout \
  -n trpc-agent-platform \
  -l app.kubernetes.io/component=agent-gateway

rollout_name="$(kubectl get rollout -n trpc-agent-platform \
  -l app.kubernetes.io/component=agent-gateway \
  -o jsonpath='{.items[0].metadata.name}')"
kubectl argo rollouts undo "${rollout_name}" -n trpc-agent-platform
kubectl argo rollouts status "${rollout_name}" -n trpc-agent-platform --timeout 180s
```

服务回滚只恢复 Pod 模板和流量指针，不撤销数据库迁移、已经执行的工具副作用或外部消息投递。

## 可重复黑盒验证

以下测试创建带三个 worker 故障域的临时 Kind 集群，安装固定版本的 Gateway API、Istio Ambient、Rollouts、Metrics Server 与 Argo CD，通过集群内 Git 服务让 Argo CD 以本地镜像 digest 同步六个独立 release。测试会证明 Web Console 身份可访问声明的 Admin 健康路径，而错误路径、伪造 `x-tenant-id`、未授权 ServiceAccount 和未加入网格的工作负载均被拒绝；随后制造错误 canary 并验证安全回滚。全程不依赖 Compose，结束后自动删除自己的唯一命名集群。

```bash
scripts/install_kubernetes_tools.sh /tmp/trpc-platform-tools
PATH="/tmp/trpc-platform-tools:${PATH}" \
RUN_KUBERNETES_SMOKE=1 \
uv run pytest tests/deployment/test_kubernetes_smoke.py -v -s
```

只有排障时才设置 `KEEP_KUBERNETES_SMOKE_CLUSTER=1`；保留后需要按测试输出中的精确集群名手工删除。
