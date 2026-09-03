# 本地开发步行骨架

此开发拓扑只用于本地开发和公开边界集成测试，不是生产部署方式。生产安装、独立扩缩容、金丝雀和回滚入口见 [Kubernetes 生产部署骨架](kubernetes.md)。

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

## 生成 API 客户端

Admin API 的 OpenAPI 合约是 Web Console 客户端类型的唯一来源：

```bash
uv run python scripts/export_openapi.py
npm run generate:api --prefix web-console
```

## 编排 Fake 故障

Fake 外部服务通过 `POST /control/v1/scenarios` 配置 LLM 或 IM 场景。可用值为 `success`、`duplicate`、`out_of_order`、`rate_limit`、`timeout`、`disconnect` 和 `outcome_unknown`。通过 `POST /control/v1/reset` 恢复默认状态并清空已接收消息。

```bash
curl -X POST http://localhost:8090/control/v1/scenarios \
  -H 'Content-Type: application/json' \
  -d '{"llm":"timeout","im":"duplicate"}'
```

## 停止与清理

```bash
./stop.sh
./clean.sh
```
