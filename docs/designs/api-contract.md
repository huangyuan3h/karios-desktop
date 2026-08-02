# API Contract — 跨项目接口契约

> **关联 todo**：[§3 API 开放](../todo.md) · [`freelancer-architecture.md`](./freelancer-architecture.md)  
> **配套**：OpenAI 兼容 `/v1/*` 的**稳定发现性**端点  
> **决议日**：2026-08-01

---

## 核心问题

Karios 的 API 会**时常改**（schema 改、字段改、新增 endpoint）。外部 AI 助手（个人项目）需要**一个 URL 打开就知道当前如何调**，不靠人维护外部文档。

**结论**：Karios 暴露**稳定发现性 endpoint**，AI 助手启动先查 `/v1/version` + `/v1/schema`，变更时通过 `/v1/changelog` 主动感知。

---

## 4 个"永不变"的稳定 endpoint

这 4 个是**契约级 endpoint**，路径与基础行为不许变：

| Endpoint | 返回 | 用途 |
|----------|------|------|
| `GET /v1/version` | `{ "version": "1.4.2", "min_compatible": "1.3.0", "released_at": "..." }` | AI 助手启动时**第一件事**调这个；version 跳 major → 报警 |
| `GET /v1/schema` | OpenAPI 3.1 JSON（**全量** endpoint + request/response schema）| 机器可读；自动生成客户端代码 |
| `GET /v1/errors` | 错误码字典：`{ code, http_status, meaning, recovery_hint }[]` | 错误时 AI 助手**自己查**怎么修 |
| `GET /v1/changelog?since=1.3.0` | 接口变更历史（diff 视图）| 升级时 AI 助手**主动**告诉用户改了啥 |

**为什么是这 4 个**：

- 业务 endpoint 会加会改会删
- 但**发现性**（怎么知道现在有哪些、现在长啥样）必须稳定
- AI 助手第一行代码就调这 4 个，**业务 endpoint 在 schema 出来后**才调

---

## 版本号规则（必须严格执行）

```
MAJOR.MINOR.PATCH

MAJOR  ↑  删字段 / 改字段名 / 删 endpoint  →  AI 助手必须重新接入
MINOR  ↑  新增 endpoint / 新增可选字段     →  AI 助手可选升级
PATCH  ↑  修描述 / 修默认值 / 加错误码      →  AI 助手不需要动作
```

**写在 schema / changelog / code 三处，必须一致**。CI 校验：发布时检查 `packages/shared` 的 `version` 与 `CHANGELOG.md` 第一行匹配。

---

## Schema 字段的"AI 友好"约束

OpenAPI 规范允许 description 为空——但**Karios 不允许**。每个字段必须给 LLM 看的描述：

```jsonc
{
  "name": "positionPct",
  "type": "number",
  "description": "卫星仓内当前仓位百分比（0-100），用于建议上限 15% 的硬闸校验。null 表示无仓位数字，不计入合计。",
  "example": 8.5
}
```

**description 写法规范**：

- **说人话**，不说内部代号
- 提到**单位 / 范围 / 边界**（"0-100"、"null 表示 X"）
- 提到**与其他字段关系**（"用于 ... 硬闸"）
- 提到**何时为 null**（避免 LLM 误用）

**linter**：CI 跑 `check-schema-descriptions.ts`，缺 description 直接 fail。

---

## 错误码设计

```json
{
  "code": "SLEEVE_CAP_BLOCK",
  "http_status": 422,
  "meaning": "卫星仓合计仓位已达 hint 上界，禁止 BUY/ADD。",
  "recovery_hint": "检查 sleeve 合计或调高 positionRangeHint。",
  "since": "1.2.0",
  "deprecated_since": null
}
```

**规则**：

- `code` 一旦发布**永远不变**（语义稳定）
- `meaning` / `recovery_hint` 可改描述（不破坏契约）
- 新增 `code` 不删旧 `code`；老码** deprecated** 但继续工作
- `recovery_hint` 是给 LLM 看的**修复指南**——比人读的错误页更重要

---

## 变更通知（changelog）

**每个 PR 影响接口 → 必须写 changelog 条目**：

```markdown
## 1.4.2 (2026-08-15)

### Added
- `GET /v1/alpha-radar/recent?grade=S&max_age_days=7` — Alpha S 级近 7 天候选

### Changed
- `GET /v1/watchlist/items`: 新增可选字段 `industryCn`（东财行业名）

### Deprecated
- 字段 `positionPct_old` → 请改用 `positionPct`（since 1.4.2，删除计划 2.0.0）
```

AI 助手侧流程：

1. 启动 → `GET /v1/version` 拿到 "1.4.2"
2. 缓存上次版本 "1.4.0"
3. `GET /v1/changelog?since=1.4.0` → 拿到 diff
4. **主动告诉用户**："Karios API 升级，新增 X、改变 Y"
5. 用户确认 → AI 助手更新客户端代码

---

## 人类可读文档 vs 机器可读 schema

| 谁 | 看什么 | 哪里 |
|----|--------|------|
| 人（开发时）| Markdown 文档 | `docs/api/*.md`（自动生成 + 人工校对）|
| 人（调试时）| Swagger UI | `/docs`（FastAPI 自带）|
| **AI 助手（自动）** | **`/v1/schema`** | **永远走这个** |
| CI 校验 | `tests/test_schema_completeness.py` | — |

**原则**：业务逻辑不能写到 Markdown 里。Markdown 永远只是"schema 的另一种渲染"。

---

## 实现路径

| 任务 | 位置 | 工时 |
|------|------|------|
| Zod schema → OpenAPI 生成 | `services/data-sync-service/src/data_sync_service/api/openapi.py` | 0.5 天 |
| 暴露 4 个稳定 endpoint | `services/data-sync-service/src/data_sync_service/api/discovery_routes.py`（新）| 1 天 |
| 错误码字典 | `packages/shared/src/schemas/errors.ts`（新）| 0.5 天 |
| Version bump 脚本 + CI 校验 | `scripts/bump-api-version.sh` + `tests/test_api_version.py` | 0.5 天 |
| 字段 description 补全 | 跨 5 个 schema 文件 | 1-2 天 |
| `docs/api/` 目录 + 人类可读文档 | 文档侧 | 0.5 天 |
| Schema completeness 测试 | `tests/test_schema_completeness.py` | 0.5 天 |
| **合计** | — | **4-5 天** |

---

## 反原则

- ❌ 业务 endpoint 改路径名（破坏 AI 助手缓存的 schema）
- ❌ 字段 description 写"内部代号"或"待补充"（AI 助手无法判断）
- ❌ 删 endpoint 而不先 deprecate 一个 minor 版本
- ❌ 错误码没有 `recovery_hint`（AI 助手调错不知道怎么修）
- ❌ 在外部仓库写 Karios API 文档（永远从 schema 自动生成）
- ❌ 让 AI 助手"猜"字段含义（必须 description 里写清）

---

## 与 Karios 其他 todo 的关系

- §3 P0 拆分：endpoint 暴露（#1-A） + 稳定发现性（#1-B），**不**单独成条目
- §3 P1 改：人类可读 OpenAPI 文档（`docs/api/` + Swagger UI），AI 友好性已在 #1-B 完成
- §3 P3（MCP）：与本契约同源；MCP server 只是把 `/v1/schema` 包装为 MCP tools