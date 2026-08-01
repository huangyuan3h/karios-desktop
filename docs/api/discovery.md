# Discovery Endpoints（稳定发现性）

> 这 4 个 endpoint 是 Karios `/v1/*` 与外部 AI 助手的**唯一桥梁**。路径与基础行为永不变。  
> 详细规则：[`docs/designs/api-contract.md`](../../designs/api-contract.md)

**鉴权**：4 个 endpoint **永远不要求 API Key**（AI 助手启动时调，没 Key 死锁）。

---

## GET /v1/version

返回当前 `/v1/*` API 版本。AI 助手启动**第一件事**。

### Response 200

| 字段 | 类型 | 说明 |
|------|------|------|
| `version` | string | 当前 MAJOR.MINOR.PATCH |
| `min_compatible` | string | 仍受支持的最低版本；低于此应拒绝启动 |
| `released_at` | string | ISO-8601 UTC 发布时间戳 |

### 示例

```bash
$ curl http://karios.local/v1/version
{
  "version": "0.1.0",
  "min_compatible": "0.1.0",
  "released_at": "2026-08-01T07:30:00+00:00"
}
```

---

## GET /v1/schema

返回 FastAPI 自动生成的 OpenAPI 3.1 JSON。**全量**（含现有 16 个 router + 这 4 个 + 4 个业务 endpoint）。

### 用途

- 自动生成客户端 SDK（openapi-generator / openapi-typescript-codegen 等）
- 校验："我想调的 endpoint 还在吗？"
- 字段 `description` 是给 LLM 看的"为什么需要这个字段"

### 示例

```bash
$ curl http://karios.local/v1/schema > karios-openapi.json
$ jq '.paths."/v1/explain/{symbol}".get.summary' karios-openapi.json
"Comprehensive context pack for a single symbol (read-only)."
```

---

## GET /v1/errors

返回错误码字典。**每个错误码必须有 `recovery_hint`**——这是给 LLM 自动修错用的。

### Response 200

| 字段 | 类型 | 说明 |
|------|------|------|
| `version` | string | Karios `/v1/*` 版本（与 `/v1/version.version` 一致） |
| `codes[]` | array | 错误码列表 |
| `codes[].code` | string | 稳定机器可读码（UPPER_SNAKE_CASE，永不复用） |
| `codes[].http_status` | int | HTTP 状态码 |
| `codes[].meaning` | string | 一句话人类解释（避免 jargon） |
| `codes[].recovery_hint` | string | 祈使句的修复建议（LLM 用） |
| `codes[].since` | string | 此 code 首次出现的 API 版本 |
| `codes[].deprecated_since` | string \| null | 被标 deprecated 的 API 版本；null = 仍 active |

### 当前种子错误码（v0.1.0）

| Code | HTTP | 含义 |
|------|------|------|
| `SLEEVE_CAP_BLOCK` | 422 | 卫星仓合计已达 hint 上界 |
| `SECTOR_CONC_BLOCK` | 422 | 行业仓位合计已 ≥ 30% |
| `ENTRY_BELOW_STOP` | 422 | 建议买入区间 ≤ 硬止损 |

完整 `recovery_hint` 调 `GET /v1/errors` 拿。

---

## GET /v1/changelog

返回接口变更历史。AI 助手启动时**比对** `version`，跳 MAJOR → 主动调这个拿 diff。

### Query

| 参数 | 类型 | 默认 | 说明 |
|------|------|------|------|
| `since` | string \| null | null | ISO 日期（YYYY-MM-DD）或版本号（"0.0.5"）；null = 全部 |

### Response 200

| 字段 | 类型 | 说明 |
|------|------|------|
| `since` | string \| null | 回显 `since` 参数；null = 全量 |
| `changes[]` | array | 变更列表（按时间正序，最早在前）|
| `changes[].version` | string | 引入此变更的 API 版本 |
| `changes[].kind` | string | `added` / `changed` / `deprecated` / `removed` / `fixed` |
| `changes[].target` | string | 受影响路径或符号，如 `GET /v1/market/snapshot` |
| `changes[].summary` | string | 一句话变更说明（LLM 写迁移代码用）|

### 现状

**v0.1.0**: 返回 `changes: []`（Phase C 留 stub，由 `bump-api-version.sh` 在每次发版时写入）。

---

## 下一步

- 业务 endpoint：[`business.md`](./business.md)
- 解释素材：[`explain.md`](./explain.md)
- 完整 OpenAPI：`GET /v1/schema`
