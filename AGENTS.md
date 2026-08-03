# Karios Desktop — Agent Guide

> 跨工具约定文件 — Cursor / OpenCode / Codex / Claude Code / Aider 等都会默认读。
> 改 schema / 改 docs / 开 Agent 任务前先读。  
> **本文件是规则真值**；任何 docs/ 下与本文冲突的文档，以本文为准。

## Project layout

| Path | Role |
|------|------|
| `apps/desktop-ui` | Next.js UI (Tauri WebView) |
| `apps/ai-service` | Node/TypeScript AI service (Vercel AI SDK) |
| `services/data-sync-service` | Python FastAPI — data sync, analysis, Postgres |
| `packages/shared` | 跨层 Zod schemas + TS 类型 |
| `docs/todo.md` | **产品路线图（注意力集中地）**，按领域分章 + `[P0..P4]` |
| `docs/optimization-checklist.md` | 工程债 / 性能 / 兼容 / Agent 任务（`OPT-xxx`） |
| `docs/trading-improvement-checklist.md` | 业务规则校准 / 交易闸（`TIP-xxx` / `V6.x`） |
| `docs/modules/` | 业务模块真值（与代码对齐的工作流文档） |
| `docs/designs/` | **未落地 / 拍板中**的设计草稿容器（落地后迁出） |
| `docs/archive/` | 已完成事项快照 + 历史文档（只读，不回写） |
| `docs/README.md` | docs 目录索引 |

Start dev: `pnpm dev` (from repo root). Backend needs root `.env` with `DATABASE_URL`.

---

## Documentation management（Agent 必读）

> docs/ 的**角色分工**写在 `docs/README.md`，本节是 **Agent 操作约束**。

### 分工唯一性

| 角色 | 唯一位置 | 谁来维护 |
|------|----------|----------|
| **做什么、为什么** | `docs/todo.md` | 用户直接 update；Agent 起草 → 用户拍板 |
| **业务规则真值** | `docs/modules/*.md`（5 份） | 与代码同步；与现行不一致的迁至 `docs/archive/modules-legacy/` |
| **工程执行栈** | `docs/optimization-checklist.md`（OPT）/ `docs/trading-improvement-checklist.md`（TIP/V6） | Agent 滚动维护 |
| **未完成设计** | `docs/designs/*.md` | 拍板前停留；落地后迁出 |
| **已沉淀** | `docs/archive/` | 只读，不再回写 |

### Agent 操作清单

1. **不要在 `docs/` 根目录新建规划/计划类 markdown**——`todo.md` 是唯一入口。
2. **不要新建 "会议纪要 / 杂记 / TODO-LIST" 类散点文档**——有想法写 todo；落地后归档到 archive/。
3. **不要删除或回写 `docs/archive/`**——它是历史快照。
4. **改 `docs/modules/*.md` 前**先 grep 代码确认现状；现状脱节就**整篇迁到** `docs/archive/modules-legacy/`，**不要就地矛盾修改**。
5. **完成 todo 的一条**要标 `[done] YYYY-MM-DD` + 在 `todo.md §10` 补一行，并在 `docs/archive/` 起一份摘要（按 `archive/README.md` 的模板）。
6. **新建 schemas/API** 不单写 markdown——在 `packages/shared` 加 Zod，跑到 `docs/optimization-checklist.md` 加一条；不在 docs/ 加实现说明文件。

### 跨工具约定

- `AGENTS.md` 是**唯一 agent 规则文件**。Cursor / OpenCode / Codex / Claude Code 默认都读它。
- 不要为不同工具复制平行文件（`.cursor/rules/AGENTS.mdc` / `.opencode/agent.md` / `.codex/AGENTS.md` / `CLAUDE.md`），分叉维护反而容易漂移。
- 如果想给 Cursor 加可视化 / 工具特定的 hook，可以放 `.cursor/rules/*.mdc`，但**只能引用本文档**，不能复制本文规则。

---

## Database: Postgres + Alembic (required reading)

---

## Database: Postgres + Alembic (required reading)

Schema is **Postgres**. Alembic tracks **versioned migrations**; per-module `ensure_table()` in `db/*.py` remains for local dev convenience only.

**Do not** add ad-hoc runtime `ALTER TABLE` patches in application code for new changes. Use Alembic revisions.

### Commands (run from `services/data-sync-service`)

```bash
cd services/data-sync-service

# Fresh empty Postgres — create all tables
PYTHONPATH=src alembic upgrade head

# Existing DB (tables already created by ensure_table) — mark baseline once, no SQL
PYTHONPATH=src alembic stamp head

# After pulling new migrations
PYTHONPATH=src alembic upgrade head

# New schema change (agent must add revision file + sync CREATE_SQL)
PYTHONPATH=src alembic revision -m "describe_change"
PYTHONPATH=src alembic upgrade head

# Check current revision
PYTHONPATH=src alembic current
```

- Migrations are **not** auto-run on app startup.
- Baseline revision: `0001_baseline` (DDL aggregated in `src/data_sync_service/db/schema_baseline.py`).
- Alembic uses `DATABASE_URL` from repo root `.env` (`postgresql+psycopg://` in `alembic/env.py`).

### When changing schema (agent checklist)

1. Add **`alembic/versions/xxxx_describe_change.py`** with `upgrade()` / optional `downgrade()`.
2. Update matching **`CREATE_SQL` / `CREATE_*_SQL`** in `services/data-sync-service/src/data_sync_service/db/*.py`.
3. Update business code that uses the new columns/tables.
4. Add or extend tests; run `pytest` (DB tests skip if Postgres unavailable).
5. Tell the user to run `PYTHONPATH=src alembic upgrade head` locally (or run it in terminal if allowed).
6. Do **not** only edit `ensure_table()` without a migration.

### Common mistakes

| Mistake | Fix |
|---------|-----|
| Only changed `db/*.py`, no migration | Add Alembic revision |
| Only added migration, not `CREATE_SQL` | Sync `db/*.py` for empty-DB parity |
| Existing dev DB never stamped | Once: `alembic stamp head` or `upgrade head` |
| Used `stamp` after new migrations exist | Use `upgrade head`, not `stamp` |

More detail: `services/data-sync-service/README.md` → **Database Migrations**.

---

## TrendOK / refresh (OPT-006)

- `GET /market/stocks/trendok` is **DB-only** (no `refresh` query param).
- To refresh K-lines from network first: `GET /market/stocks/{symbol}/bars?force=true` (incremental tushare sync per CN symbol), then call trendok.
- Watchlist manual refresh already follows this pattern (`forceMarket`).

---

## TV Capture jobs (OPT-008)

TradingView screener capture is **async** via Postgres job queue `tv_capture_jobs`:

| Endpoint | Behavior |
|----------|----------|
| `POST /integrations/tradingview/screeners/{id}/sync` | **202** — enqueue job, returns `{ jobId, status, screenerId }` |
| `GET /integrations/tradingview/capture-jobs/{job_id}` | Poll job status until `done` / `failed` |
| `GET /integrations/tradingview/capture-jobs?screener_id=` | List recent jobs (optional) |

- In-process worker (`tv_capture_worker.py`): max **2** concurrent captures; dedupe active jobs per screener.
- Dashboard Sync All: enqueue all screeners, then `wait_for_capture_jobs` (SSE emits `jobId` / `jobStatus`).
- Screener UI: POST → poll job → refresh snapshots.
- **AM/PM cron:** `tv_screener_capture_am` (workdays 09:30 Asia/Shanghai) + `tv_screener_capture_pm` (workdays 15:30 Asia/Shanghai) enqueue all enabled screeners daily; matches `docs/modules/screener.md` "AM/PM" intent.
- Migration: `0002_tv_capture_jobs` — run `PYTHONPATH=src alembic upgrade head`.

---

## Scheduler coverage gaps closed

- `index_basic_sync` — weekdays 17:15 Asia/Shanghai (`scheduler/index_basic_job.py`). Independent sync of `index_dailybasic` so `macro_snapshot.market_breadth` is warm without a user clicking "Sync all".
- `cn_industry_post_close_sync` — weekdays 17:35 Asia/Shanghai (`scheduler/cn_industry_post_close_job.py`). Runs `sync_cn_industry_fund_flow` + `sync_cn_industry_mainline` + `sync_cn_sentiment` after `close_sync` (17:10) and `watchlist_automation` (17:30). Aligns implementation with `docs/modules/industry-flow.md` and `market-sentiment.md` "盘后每日更新".
- All three job types added to `SYNC_JOB_TYPES` in `api/sync_routes.py` and to `SCHEDULER_JOB_CATALOG` (with new `cnIndustry` / `tvScreener` groups) in `packages/shared/src/schemas/scheduler.ts`.

---

## Frontend data fetching (OPT-012)

- Polling pages use **`@tanstack/react-query`** via `lib/queries/*` hooks (`useDashboardSummaryQuery`, `useWatchlistMarketQuery`, `useMacroSnapshotQuery`, etc.).
- `QueryClientProvider` wraps the app in `AppShell.tsx`.
- New page data fetch: add a query module under `apps/desktop-ui/src/lib/queries/` rather than raw `setInterval`.

---

## Shared API types (OPT-009)

Cross-layer JSON contracts live in [`packages/shared`](packages/shared) as **Zod schemas** + inferred TS types.

| Schema module | Used for |
|---------------|----------|
| `schemas/trendok.ts` | `GET /market/stocks/trendok` |
| `schemas/watchlist.ts` | `GET/POST /watchlist/registry` |
| `schemas/tvCapture.ts` | TV capture job API (OPT-008) |

**Workflow for new API fields:**

1. Add/update Zod schema in `packages/shared/src/schemas/`.
2. Export from `packages/shared/src/index.ts`; add schema test.
3. Import types in `desktop-ui` via `@karios/shared` (thin re-exports in `lib/api/types.ts` etc. are OK).
4. Align Python Pydantic / dict responses manually; extend `tests/test_api.py` shape assertions.
5. Run `pnpm -C packages/shared build` before first `desktop-ui` dev session (or `turbo build --filter=@karios/shared`).

Python does **not** import `@karios/shared` at runtime. Field-name comments in route modules are the drift guard.

---

## Scoped optimization tasks

For structural work, use `docs/optimization-checklist.md`:

1. Pick one **OPT-xxx** item; do not expand scope beyond listed files.
2. One OPT per agent session when possible.
3. Mark checklist `[x]` and add tests when done.

Template:

```text
Implement OPT-XXX from docs/optimization-checklist.md.
- Only change files listed in that section
- Update checklist status when done
- Add/update tests
```

---

## Tests

| Area | Command |
|------|---------|
| Backend | `cd services/data-sync-service && pytest …` (use `--no-cov` for quick runs) |
| Frontend | `cd apps/desktop-ui && npm run test` |
| Alembic | `pytest tests/test_alembic_baseline.py` |

---

## Language

- User-facing chat: Chinese
- Code, comments, commit messages: English
