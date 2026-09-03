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

每个单元有独立的 production values、ServiceAccount、Service、HPA、PDB 和 NetworkPolicy。工作负载不声明 `spec.replicas`，副本下限由 HPA 唯一管理，避免 Argo CD `selfHeal` 与 HPA 相互回写。Gateway 与 Worker 使用 Argo Rollouts 金丝雀；Admin API 与 Web Console 使用 Kubernetes Deployment。当前 Gateway/Worker 进程只实现本工单要求的健康边界，后续业务工单在同一部署边界内替换健康骨架。

Admin API release 同时拥有 namespace 级 `ResourceQuota` 和 Argo CD `PreSync` 数据库迁移 Job；其余五个 release 显式关闭这两个共享资源，避免重复配额叠加。当前尚无数据库 schema revision，迁移入口会报告零项变更；后续数据库工单在这个受控入口接入 Alembic，迁移失败将阻断应用同步。

## 前置条件

- Kubernetes 集群跨至少三个故障域，节点带 `topology.kubernetes.io/zone` 标签。
- 已安装 Argo CD 3.5.2、Argo Rollouts 1.10.0 和 Metrics API；ApplicationSet Controller 已启用 Progressive Syncs。
- Ingress Controller 所在 namespace 带 `trpc-agent-platform.io/ingress=allowed` 标签。
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
uv run pytest tests/deployment/test_helm_contract.py
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

ApplicationSet 为六类单元分别生成一个 Argo CD Application，但部署到同一个 `trpc-agent-platform` namespace，使 Web Console、健康分析和后续内部服务可以使用稳定的 Kubernetes Service 名称。RollingSync 首先同步 Admin API 并等待其 PreSync 迁移 Job 与应用全部 Healthy，之后才并行放行其余五个单元；迁移失败会阻断整个生产切片。每个 Application 仍只渲染自己的单元，因此某个单元的镜像、资源或 HPA 范围变化不会重启其他单元。

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

以下测试创建带三个 worker 故障域的临时 Kind 集群，安装固定版本的 Rollouts、Metrics Server 与 Argo CD，通过集群内 Git 服务让 Argo CD 以本地镜像 digest 同步六个独立 release，验证全部健康边界，再制造错误 canary 并执行回滚。测试结束会自动删除自己的唯一命名集群。

```bash
scripts/install_kubernetes_tools.sh /tmp/trpc-platform-tools
PATH="/tmp/trpc-platform-tools:${PATH}" \
RUN_KUBERNETES_SMOKE=1 \
uv run pytest tests/deployment/test_kubernetes_smoke.py -v -s
```

只有排障时才设置 `KEEP_KUBERNETES_SMOKE_CLUSTER=1`；保留后需要按测试输出中的精确集群名手工删除。
