# OPT-045 + 046 + 047 整圈 · `/v1/*` API 端到端  · 归档于 2026-08-01

> **关联 todo**：[`docs/todo.md §3 API 开放 P0`](../../todo.md) · [§12 实施清单 #1](../../todo.md)
> **设计稿**：[`docs/designs/api-contract.md`](../../designs/api-contract.md) · [`docs/designs/freelancer-architecture.md`](../../designs/freelancer-architecture.md)
> **关联 OPT 条目**：[OPT-045 / OPT-046 / OPT-047](../../optimization-checklist.md)

## 当时的目标（todo 链接）

让 Karios 通过 OpenAI 兼容 `/v1/*` API **与外部 AI 助手（用户独立项目）打通**。AI 助手能：
1. **自服务发现**——启动时调 4 个稳定发现性 endpoint，**不靠人手维护外部文档**
2. **日常数据**——调 3 个只读业务 endpoint 拿 trendok / watchlist / decision journal
3. **解释素材**——调 `/v1/explain/{symbol}` 一次拿全一个 symbol 的所有上下文

并且 **Karios 永远不调 LLM**（功能不重合：LLM 解释归外部 AI 助手；Karios 是被动数据 + endpoint 服务）。

## 实际做了什么

### Phase A（OPT-045）：4 稳定发现性 endpoint + API Key 鉴权

| 端点 | 用途 | 鉴权 |
|------|------|------|
| `GET /v1/version` | 当前 API version + min_compatible + released_at | 无 |
| `GET /v1/schema` | OpenAPI 3.1 JSON（FastAPI 自动生成）| 无 |
| `GET /v1/errors` | 错误码字典（含 `recovery_hint`）| 无 |
| `GET /v1/changelog?since=X` | 接口变更 diff | 无 |

+ `require_api_key` 依赖（opt-in：未设 `KARIOS_API_KEYS` 时 no-op）
+ `KARIOS_API_VERSION` / `KARIOS_API_KEYS` env var

### Phase B（OPT-046）：3 只读业务 endpoint

| 端点 | 用途 |
|------|------|
| `GET /v1/market/snapshot?symbols=...` | N 个标的 TrendOK / Score / 当前价 |
| `GET /v1/watchlist/items` | 当前 watchlist 全量 |
| `GET /v1/decision-journal/query?since=...&limit=...` | 近期决策变更 |

### Phase C（OPT-047）：1 综合解释 + 文档 + version 工具

| 端点 / 工具 | 用途 |
|------|------|
| `GET /v1/explain/{symbol}` | 单 symbol 完整上下文包（trendok + watchlist + 最近 5 条 journal）|
| `docs/api/*.md` | 6 份人类可读文档（README / discovery / business / explain / errors / CHANGELOG）|
| `scripts/bump-api-version.sh` | version bump 脚本（major/minor/patch + 校验 git 干净）|

## 验证 / 数据

- **49 个 v1/* 专项测试**（17 discovery + 18 business + 14 explain）全绿
- **76 个测试** 跨 `test_discovery_endpoints.py` + `test_v1_business_endpoints.py` + `test_v1_explain_endpoint.py` + `test_api.py`（无 regression）+ `test_alembic_baseline.py` = **76/76 ✅**
- 8 个 endpoint 全部在 `GET /v1/schema` 中出现
- 所有 Pydantic 字段 `description` 非空（3 个单测守住）
- 错误码 `recovery_hint` 全部非空
- 鉴权：opt-in 默认通过；启用时 401 / 200 行为正确
- `positionPct` / `why` null 保留（LLM 关键字段）
- `recentChanges` cap 5 + 30 天窗口严格守住

## 后续影响 / 留给谁

### 给外部 AI 助手那边（用户独立项目）

- 启动流程：`/v1/version` → `/v1/schema` → `/v1/errors`（3 次调用）
- 日常流程：`/v1/explain/{symbol}` 一次拿全；或 `/v1/market/snapshot` + `/v1/watchlist/items` + `/v1/decision-journal/query` 组合
- 升级流程：`/v1/changelog?since=老版本` 拿 diff，MAJOR 跳变时主动告警用户

### 给 Karios 本身

- **OPT-045/046/047 完整闭合**；todo §12 #1 标 done
- 后续 P1（todo §12 #5：API Key 配额 + 限流 + 人类可读 OpenAPI 文档生成自动化）— 留 OPT-049
- 后续 P2（todo §12 #13：MCP server 暴露）— 留 OPT-050（与 OPT-045 同源）
- **接口契约必须严格**——任何字段 / endpoint 改动 → bump version + 写 changelog

### 留给未来 review

- 当外部 AI 助手项目接入 Karios 后，**第一时间观察**：
  - 字段 description 是否够清楚（LLM 一次读懂 vs 多问）
  - 错误码 recovery_hint 是否真让 LLM 自助修
  - `/v1/changelog` 的 since 参数语义是否合预期
- 这些反馈会触发 docs/api/*.md + Pydantic description 的迭代

## 沉淀数据

| 项 | 值 |
|----|----|
| 新增文件 | 8（4 router + 1 auth + 3 test）|
| 改动文件 | 4（config.py + main.py + AGENTS.md + test_discovery 修 fixture）|
| 人类可读文档 | 6 份 |
| 总测试 | 49 v1/* + 27 integration = 76/76 ✅ |
| 工期 | 半天（agent 集中 4 小时不到）|
| 预算 | $0 |
