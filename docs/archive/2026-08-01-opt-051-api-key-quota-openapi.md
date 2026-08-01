# OPT-051 / §12 #5 · API Key 多 Key + 配额 + OpenAPI 文档 · 归档于 2026-08-01

> **关联 todo**：[`docs/todo.md §2 API P0` / `§12 实施清单 #5`](../../todo.md)
> **OpenAPI 人类可读文档**：[`docs/api/openapi.md`](../../docs/api/openapi.md)（**新**）
> **Swagger UI / ReDoc**：服务起来后访问 `/docs` / `/redoc`（FastAPI 默认开启）
> **机器可读入口**：`GET /v1/schema` 或 `GET /openapi.json`

## 当时的目标

§12 #5：让外部 AI 助手**可多 Key 隔离 + 自查配额**；给所有 `/v1/*` endpoint 提供**人类可读的 OpenAPI 文档**。

## 实际做了什么

### A. API Key 多 Key + 配额（OPT-051）

#### A1. 升级 `KARIOS_API_KEYS` 格式

| 格式 | 例子 | 行为 |
|------|------|------|
| 旧（保留）| `sk-abc,sk-xyz` | 扁平 list，无 label 无配额 |
| 新 | `frontend:sk-abc:600:0:0,external-ai:sk-xyz:60:1000:10000` | `label:secret:rpm:rph:rpd`，rpm/rph/rpd=0 表示不限 |

#### A2. 三窗口滑动配额

| 窗口 | 长度 | 用途 |
|------|------|------|
| `rpm` | last 60 s | burst 保护 |
| `rph` | last 3600 s | 持续流量上限 |
| `rpd` | last 86 400 s | 日上限 |

- 内存实现（`api/key_quota.py`）：`_Window` 用单调时间戳的 deque，`try_acquire` 自动 prune 过期项
- 重启清零（homelab 单进程 + 单 worker → 可接受）
- limit=0 的窗口 short-circuit（不消耗 CPU）
- 配额耗尽 → 429 + `Retry-After` + `X-RateLimit-Limit` / `X-RateLimit-Remaining` / `X-RateLimit-Reset`

#### A3. 新 endpoint `GET /v1/quota`

```json
{
  "key_label": "external-ai",
  "auth_enabled": true,
  "windows": {
    "rpm": {"used": 7,  "limit": 60,    "window_seconds": 60},
    "rph": {"used": 142,"limit": 1000,  "window_seconds": 3600},
    "rpd": {"used": 312,"limit": 10000, "window_seconds": 86400}
  },
  "as_of": "2026-08-01T12:34:56.789+00:00"
}
```

只暴露**当前匹配** key 的用量——不暴露其他 key 的预算。

#### A4. 关键设计取舍

| 取舍 | 选择 | 理由 |
|------|------|------|
| 配额状态存储 | 内存 vs Postgres | **内存**——单进程 + 重启频率低；DB write 给每条请求加 IO 不可接受 |
| Admin endpoint | 不做 | admin auth 是单独设计决策（"谁能列所有 key？"）；先靠 log scraping + 编辑 .env |
| Per-route quota override | 不做 | 当前所有 opt 路由共享 key 配额；未来某 endpoint 变热再单独 override |
| 旧 KARIOS_API_KEYS 格式 | **保留兼容** | 旧 key 自动获 `key-<前4字>` label + 无限配额 |

### B. OpenAPI 文档

#### B1. FastAPI metadata

```python
app = FastAPI(
    title="Karios /v1/* API",
    version="0.1.0",  # 实际由 KARIOS_API_VERSION 决定
    description="...",
    openapi_tags=[
        {"name": "v1:discovery", ...},
        {"name": "v1:business",  ...},
        {"name": "v1:explain",   ...},
        {"name": "v1:quota",     ...},
    ],
)
```

#### B2. 三个地方读同一份 schema

| 路径 | 格式 | 受众 |
|------|------|------|
| `GET /v1/schema` | JSON | 外部 AI 助手（discoverable alias）|
| `GET /openapi.json` | JSON | 工具链（Postman / codegen / MCP）|
| `GET /docs` | Swagger UI HTML | 人类交互式调试 |
| `GET /redoc` | ReDoc HTML | 人类只读 review（可打印）|

`/v1/schema` 和 `/openapi.json` 测试断言**字节相等**——零漂移。

#### B3. 人类可读文档 `docs/api/openapi.md`

8 节：why 三处读 / metadata / endpoint 表 / auth + 401/429 / quota（含建议 tier profile）/ versioning / where to find what / known limitations。

## 验证 / 数据

| 测试文件 | tests | 状态 |
|----------|-------|------|
| `test_key_quota.py`（新）| **23** | ✅ |
| `test_openapi_docs.py`（新）| **11** | ✅ |
| `test_discovery_endpoints.py`（回归）| 17 | ✅ |
| `test_v1_business_endpoints.py`（回归）| 18 | ✅ |
| `test_v1_explain_endpoint.py`（回归）| 14 | ✅ |
| `test_paper_trading.py`（回归）| 19 | ✅ |
| `test_tunnel_scripts.py`（回归）| 12 + 1 skip | ✅ |
| `test_data_source_audit.py`（回归）| 13 | ✅ |
| `test_alembic_baseline.py`（回归）| 8 | ✅ |
| `test_api.py`（回归）| 19 | ✅ |
| **总计** | **154 + 1 skip** | ✅ |

### 关键 test 覆盖

- `parse_api_keys`：旧/新格式 / duplicate label/secret / 负数 / 非 int / 多字段 / 空字符串
- `_Window`：under-limit / at-limit（带 retry_after）/ 0-limit short-circuit / prune 过期
- `QuotaTracker`：no-quota short-circuit / rpm enforced / usage snapshot
- `/v1/quota`：匿名模式 / 401 / matched key 快照 / 429 带 headers / per-key 隔离
- OpenAPI：title / version / description / tags / schema == openapi.json / docs HTML / redoc HTML / 所有 /v1/* 在正确 tag 下

## 后续影响 / 留给谁

### 给外部 AI 助手

- **推荐配置**：`external-ai:sk-XXX:60:1000:10000`
  - 60/min 允许密集轮询（盘前 8:30 批量拉 explain）
  - 1000/h 覆盖全天
  - 10000/d 留足冗余
- **集成建议**：
  1. 启动 → `GET /v1/version` + `GET /v1/schema`
  2. 启动 → `GET /v1/quota` 看自己还剩多少
  3. 收到 429 → 读 `Retry-After` header（不要硬编码 sleep）
  4. 接近配额 → 降频轮询

### 给 Karios 本身

- **默认 `.env` 不需要改**——KARIOS_API_KEYS 留空时所有路由都开放（向后兼容现有前端）
- **配置文档**：见 `docs/api/openapi.md §4.1` + `AGENTS.md` 未来补充
- **未来 admin 入口**：`/v1/admin/keys` 需要独立 admin auth（不在本次 scope）

### 给未来 review

| 触发 | 行动 |
|------|------|
| 某个 key 持续 429 | 调高该 key 的 rpm/rph/rpd，或加 backpressure |
| Postgres 配额需求出现 | 把 `_Window` 换成 DB-backed；不要 inline 改内存版 |
| 多 worker 部署 | 内存配额立刻失效，必须 DB-backed |
| /v1/* 新增 endpoint | 加到 `main.py` openapi_tags + `docs/api/openapi.md §3` 表格 |

## 沉淀数据

| 项 | 值 |
|----|----|
| 新增文件 | 4（key_quota.py / v1_quota_routes.py / openapi.md / tests）|
| 改动文件 | 5（auth.py / main.py / v1_business_routes.py / docs/api/README.md / docs/api/openapi.md）|
| 总测试 | 154/154 ✅ + 1 skip（OPT-051 净增 34：23 key_quota + 11 openapi）|
| 工期 | 1 个会话 |
| 预算 | $0 |