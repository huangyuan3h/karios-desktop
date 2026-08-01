# Karios `/v1/*` API · 人类可读文档

> **自动维护**。这些 Markdown 文档与 `services/data-sync-service/src/data_sync_service/api/discovery_routes.py` / `v1_business_routes.py` / `v1_explain_routes.py` 的 Pydantic `description` 字段一一对应。**改 Pydantic 字段 → 同步改本文档**（下次 review 时统一刷）。
>
> **机器可读入口**（AI 助手用）：`GET /v1/schema`（OpenAPI 3.1 JSON） + `GET /v1/errors`（错误码字典） + `GET /v1/changelog?since=X`（接口变更 diff）。

---

## 0. 一图流

```
┌─────────────────────────────────────────────────────────────┐
│  外部 AI 助手（你的独立项目）                                  │
│                                                             │
│   启动  ──► GET /v1/version                                 │
│         ──► GET /v1/schema            ◄── 一次性发现          │
│         ──► GET /v1/errors                                  │
│                                                             │
│   日常  ──► GET /v1/market/snapshot?symbols=...             │
│         ──► GET /v1/watchlist/items                         │
│         ──► GET /v1/decision-journal/query?since=...        │
│         ──► GET /v1/explain/{symbol}                        │
│         ──► GET /v1/paper-trades                            │
│         ──► GET /v1/paper-trades/stats                      │
│         ──► GET /v1/quota                                   │
│                                                             │
│   升级  ──► GET /v1/changelog?since=老版本                  │
│                                                             │
└─────────────────────────────────────────────────────────────┘
                          │
                          │ outbound only · Tunnel
                          ▼
┌─────────────────────────────────────────────────────────────┐
│  Karios（macOS 本地）                                        │
│   - pnpm dev (Next.js) + Python FastAPI                     │
│   - Postgres（本地权威）                                     │
│   - 4 稳定发现性 + 7 业务 endpoint（只读）+ quota            │
│   - 不做：推送 / 日报生成 / 自然语言代理                       │
└─────────────────────────────────────────────────────────────┘
```

> **可视化文档**：[`openapi.md`](./openapi.md) — Swagger UI 在 `/docs`、ReDoc 在 `/redoc`（FastAPI 默认开启）。

---

## 1. 发现性 endpoint（稳定，永不删）

> 4 个 endpoint 路径与基础行为不变。**AI 助手启动第一件事就是调这些**。详见 [`discovery.md`](./discovery.md)。

| Endpoint | 用途 |
|----------|------|
| `GET /v1/version` | 当前 API version + `min_compatible` + `released_at` |
| `GET /v1/schema` | OpenAPI 3.1 JSON（全量 endpoint + 字段 schema + description）|
| `GET /v1/errors` | 错误码字典（含 `recovery_hint`）|
| `GET /v1/changelog?since=X` | 接口变更 diff |

**所有 4 个不要求 API Key**（AI 助手启动时调，没 Key 会死锁）。

---

## 2. 业务 endpoint（只读）

> 7 个 endpoint 都只读。**禁止改仓**（写操作走现有 `/watchlist/*` / `/execution/*`）。详见 [`business.md`](./business.md) + [`explain.md`](./explain.md) + [`openapi.md`](./openapi.md)。

| Endpoint | 用途 | 鉴权 |
|----------|------|------|
| `GET /v1/market/snapshot?symbols=...` | N 个标的的 TrendOK / Score / 当前价 | opt-in |
| `GET /v1/watchlist/items` | 当前 watchlist 全量 | opt-in |
| `GET /v1/decision-journal/query?since=...&limit=...` | 近期决策变更 | opt-in |
| `GET /v1/explain/{symbol}` | 单 symbol 完整上下文包（解释素材）| opt-in |
| `GET /v1/paper-trades?status=&since=&limit=` | paper-trade intake log | opt-in |
| `GET /v1/paper-trades/stats?since=` | win rate / avg pnl / holding 分布 | opt-in |
| `GET /v1/quota` | 当前 API Key 配额用量快照 | opt-in |

---

## 3. 错误码

详见 [`errors.md`](./errors.md)。`/v1/errors` 是机器可读字典；`errors.md` 是人类可读对照表。

---

## 4. 接口变更历史

详见 [`CHANGELOG.md`](./CHANGELOG.md)。每次接口变更（MAJOR / MINOR / PATCH）必须追加一条。

---

## 5. 版本号规则

```
MAJOR.MINOR.PATCH

MAJOR  ↑  删字段 / 改字段名 / 删 endpoint           →  AI 助手必须重新接入
MINOR  ↑  新增 endpoint / 新增可选字段              →  AI 助手可选升级
PATCH  ↑  修描述 / 修默认值 / 加错误码（不删旧的）   →  AI 助手不需要动作
```

完整规则见 [`../../docs/designs/api-contract.md`](../../docs/designs/api-contract.md)。

---

## 6. 鉴权 + 配额（opt-in）

Karios 默认**不要求 API Key**（向后兼容现有前端）。在 `.env` 设置：

```bash
# 旧格式：扁平 list（无配额）
KARIOS_API_KEYS=key-for-ai-assistant,key-for-friend

# 新格式：label:secret:rpm:rph:rpd，rpm/rph/rpd=0 表示不限
KARIOS_API_KEYS="frontend:sk-abc:600:0:0,external-ai:sk-xyz:60:1000:10000"
```

设置后：
- 7 个**业务 + explain + quota** endpoint 要求 `Authorization: Bearer <key>`，并按 rpm/rph/rpd 限速
- 4 个**发现性** endpoint 仍然不要求 Key（AI 助手启动先调这些）
- 配额耗尽 → 429 + `Retry-After` + `X-RateLimit-*` 头部
- 自查用量 → `GET /v1/quota`（仅暴露当前匹配的 Key，不暴露其他 Key）

完整配额 + Key 格式 + 示例档案见 [`openapi.md`](./openapi.md)。

---

## 7. 完整流程示例

### AI 助手启动（一次）

```bash
# 1. 查当前版本
curl http://karios.local/v1/version
# {"version": "0.1.0", "min_compatible": "0.1.0", "released_at": "..."}

# 2. 拿 schema
curl http://karios.local/v1/schema > karios-openapi.json
# 解析 → 生成客户端 SDK

# 3. 拿错误码字典
curl http://karios.local/v1/errors > karios-errors.json
# 存起来用于调错时查询 recovery_hint

# 4. 拿 changelog
curl http://karios.local/v1/changelog?since=0.0.5
# 升级时拿 diff
```

### AI 助手日常

```bash
# 盘前 8:30 — 拉今日待拍板清单（解释素材）
curl -H "Authorization: Bearer $KEY" \
  http://karios.local/v1/explain/CN:000001

# 盘后 17:00 — 拉当日 watchlist + 决策变更
curl -H "Authorization: Bearer $KEY" \
  http://karios.local/v1/watchlist/items
curl -H "Authorization: Bearer $KEY" \
  "http://karios.local/v1/decision-journal/query?since=2026-08-01&limit=50"
```
