# 本地开发步行骨架

此开发拓扑只用于本地开发和公开边界集成测试，不是生产部署方式。生产部署从后续 Kubernetes、Helm 与 GitOps 工单进入。

## 前置条件

- Docker 及 Docker Compose
- Python 3.12 与 uv 0.11.28
- Node.js 22.22.2 与 npm

## 构建与启动

```bash
./build.sh
./start.sh
```

启动后可访问：

- Web Console：<http://localhost:4173>
- Admin API 健康接口：<http://localhost:8000/api/v1/health>
- Admin API OpenAPI：<http://localhost:8000/api/docs>
- Fake 外部服务：<http://localhost:8090/health>
- MinIO Console：<http://localhost:9001>

`compose.yml` 启动真实 PostgreSQL/PGVector、Redis、Kafka 兼容 Redpanda、MinIO 和 OPA。`fake-external` 提供确定性的 LLM 与 IM 开发替身。

## 验证

```bash
uv run pytest tests/unit
cd web-console && npm test && npm run typecheck && cd ..
cd web-console && npm run test:smoke && cd ..
```

Smoke Test 通过浏览器访问 Web Console，并通过控制台同源代理验证 `/api/v1/health`，不直接调用后端内部对象。

## 停止与清理

```bash
./stop.sh
./clean.sh
```
