# Karios Desktop — Agent Guide

> 跨工具约定文件 — Cursor / OpenCode / Codex / Claude Code / Aider 等都会默认读。
> 改 schema / 改 docs / 开 Agent 任务前先读。  
> **本文件是规则真值**；任何 docs/ 下与本文冲突的文档，以本文为准。

## Project layout

| Path | Role |
|------|------|
| `apps/desktop-ui` | Next.js UI (web-only) |
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
7. **用户要改交易策略 / 仓位 / 退出 / 篮子只数时**：先读 `docs/backtests/SUMMARY.md` 和对应实验文档，再开口或改代码。细则见下文 **Strategy / parameter changes**。

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

## Scheduler coverage gaps closed

- `index_basic_sync` — weekdays 17:15 Asia/Shanghai (`scheduler/index_basic_job.py`). Independent sync of `index_dailybasic` so `macro_snapshot.market_breadth` is warm without a user clicking "Sync all".
- `cn_industry_post_close_sync` — weekdays 17:35 Asia/Shanghai (`scheduler/cn_industry_post_close_job.py`). Runs `sync_cn_industry_fund_flow` + `sync_cn_industry_mainline` + `sync_cn_sentiment` after `close_sync` (17:10) and `watchlist_automation` (17:30). Aligns implementation with `docs/modules/industry-flow.md` and `market-sentiment.md` "盘后每日更新".
- `factor_signals_sync` — weekdays 18:30 Asia/Shanghai (`scheduler/factor_signals_job.py`). Scans the latest open day for `strong_scoop_exhaustion` into `factor_signals` (direction-only; never touches S-3). Manual trigger: `POST /factors/sync`.
- All job types above are added to `SYNC_JOB_TYPES` in `api/sync_routes.py` and to `SCHEDULER_JOB_CATALOG` (groups `cnIndustry` / `tvScreener` / `factors`) in `packages/shared/src/schemas/scheduler.ts`.

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
| `schemas/strategyCatalog.ts` | `GET /api/backtest/strategy-catalog` |
| `schemas/b3State.ts` | `GET /api/backtest/b3-state` |

**Workflow for new API fields:**

1. Add/update Zod schema in `packages/shared/src/schemas/`.
2. Export from `packages/shared/src/index.ts`; add schema test.
3. Import types in `desktop-ui` via `@karios/shared` (thin re-exports in `lib/api/types.ts` etc. are OK).
4. Align Python Pydantic / dict responses manually; extend `tests/test_api.py` shape assertions.
5. Run `pnpm -C packages/shared build` before first `desktop-ui` dev session (or `turbo build --filter=@karios/shared`).

Python does **not** import `@karios/shared` at runtime. Field-name comments in route modules are the drift guard.

---

## Strategy / parameter changes（Agent 必读）

用户问「能不能改策略 / 少买几只 / 加止损 / 赢家拿长一点 / 闲置现金怎么停」时，**先查过往实验，再谈改不改**。禁止凭直觉改 Live 参数。

0. 新想法先过 [`first-principles-2026-09-05.md`](docs/backtests/first-principles-2026-09-05.md) 自查：撞上已杀直觉（§一不变量 / §三死因）的不开诊断；撞上不变量的直接用；只有档里没有的才开预注册。

1. **现行产品基线 = 「港湾」（Harbor）= S-3 股票核心 + 闲置现金 ETF 停车场**（tag `harbor-p1-20260913`；真值 [`docs/modules/pick-strong-track.md`](docs/modules/pick-strong-track.md)）。三窗增量 **+15.5/+11.0/+11.6pt**、long **+125.4pt**（2026-09-13 修正后）；停车场只作用于 S-3 闲置现金（14:30、`mom60+MA200` argmax、因果 trail8）。
   - 停车场真值 → [`etf-parking-baseline-2026-09-13.md`](docs/backtests/stable/etf-parking-baseline-2026-09-13.md)（B11）
   - ETF 基准 / 最佳拟合 → [`etf-benchmark-parking-2026-09-13.md`](docs/backtests/stable/etf-benchmark-parking-2026-09-13.md)（B13；港湾×风险预算 50/50，落地需另起预注册）
   - **产品候选（不进 Live）**：母港 = 港湾×B3 50/50 **PASS**（B15）→ [`harbor-riskbudget-2026-09-13.md`](docs/backtests/stable/harbor-riskbudget-2026-09-13.md)；三腿「**星港**」（Starport = 母港×卫星）**PASS chosen=1/3**（H-B3-SAT，2026-09-14；1/3 踩线、稳健 0.15–0.25）→ [`harbor-b3-sat-2026-09-14.md`](docs/backtests/stable/harbor-b3-sat-2026-09-14.md)；激进档「**星舰**」（Starship = 卫星 standalone，long +463.6%/SR 3.50/MDD −8.4，**未过执行审计，不进 Live**，前置=审计电池+paper 3/20+用户风险授权）→ [`sgap-habit-satellite-standalone-2026-09-14.md`](docs/backtests/stable/sgap-habit-satellite-standalone-2026-09-14.md)
   - S-3 参数真值 → [`docs/modules/strategy-params.md`](docs/modules/strategy-params.md) §1；拒收总表 → [`docs/backtests/SUMMARY.md`](docs/backtests/SUMMARY.md)
   - Live 现状：旧 `twin_star`/择强路径仍在且有前视/账本 bug → **OPT-178**（修完前不按旧卫星指令下单）。
2. **历史（REJECT / 已下线，不要再当实盘方案提出）**：
   - 择强单轨「全资产同权 100% argmax」被 **OPT-177** 证伪（long ≈ +0.8% / MDD −58%）；
   - 机会双子星 / 卫星腿（14:30 名单 + C1 3% + 第 3 日 14:30 卖 + 4×12.5%）审计后死因只剩 valid 单窗（Δ−21.2；旧 boom-bust/long 数字全部作废，卫星 standalone clean long +463.6%）；两腿叠加港湾任何权重也无解（H-SAT-W）→ 只有三腿（母港×卫星）产品候选成立。**2026-09-14 用户决策：双子星保留为「并行对照档」（不退役、catalog 徽章改「并行对照」、Timeline `strategy=twin_star` 已接线），default 数据收集档 = 星港；5 套并行对比，均不进 Live。** 见 [`twin-star-parking-refit-2026-09-13.md`](docs/backtests/stable/twin-star-parking-refit-2026-09-13.md) · [`audit-three-strategy-lookahead-2026-09-14.md`](docs/backtests/audit-three-strategy-lookahead-2026-09-14.md)。
   - 旧卫星 / 14:30 / C1 各专题档（`sat-*`、`clip4-ops-decisions`、`state-bucket-algo` 等）仅作历史参考，索引见 [`docs/backtests/SUMMARY.md`](docs/backtests/SUMMARY.md)。
3. **已 REJECT 的变体不要再当实盘方案提出**（除非新三窗相对冻结基线全过，且文档写明为何值得重开）。
4. **Live 以冻结回测引擎为准**。把 Live 收到已经 PASS 的腿上（例如去掉引擎里没有的 overlay）可以做；把 REJECT 机制写进实盘不行。
5. 任何新参数/机制必须过三窗 walk-forward（下一节）。单窗好看 = 过拟合。
6. 改完把结论写进 `docs/backtests/`（PASS 或 REJECT 都留档），不要只停在对话里。

---

## Backtest walk-forward（S-3 参数验证铁律工具）

任何 S-3 参数/机制改动必须过 **三窗 walk-forward**（单窗好看 = 过拟合，三窗铁律）。
工具：`services/data-sync-service/scripts/run_walk_forward.py`（C1，2026-08-09 交付）：

```bash
cd services/data-sync-service
PYTHONPATH=src python3 scripts/run_walk_forward.py                       # 三窗 vs 固化基线
PYTHONPATH=src python3 scripts/run_walk_forward.py --param score_threshold=70   # 试参数
PYTHONPATH=src python3 scripts/run_walk_forward.py --save-baseline       # 数据/引擎变化后重固化基线
```

- 三窗固定切分与 holdout/long 口径：见 [backtests/README 验证纪律](docs/backtests/README.md)（OOS2/train/valid 日期 + holdout 只读 + long 说明 + no-op 警告）
- 内置 S-3 定案配置（真值在 `docs/modules/strategy-params.md` §1）；`--param k=v` 覆盖任意
  `BacktestConfig` 字段（未知字段告警忽略）
- 基线固化在 `data/backtest_reports/walk_forward_baseline.json`；三窗相对基线 >5pt 劣化
  → 自动判"未通过/拒收"
- 验收口径：改动后跑 `--param ...` 三窗对比，输出表 + 判定随实验记录

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

### DB 集成测试纪律（2026-08-07 教训 · 必须遵守）

**任何 `requires_postgres` 测试插入真实数据后必须清理自己写的行。**

原因：`test_execution_source_db.py` 曾直接往 dev Postgres 插入 `CN:99{uuid}` 假 symbol 的
paper_trades（230+ 条）、`source='manual-test'` 快照（72 条）、假快照 id（`snap-agg`/
`snap-bf`）的 changes 行（67 条）且不清理——污染决策日志、归因统计，掩盖了真实数据。

规则：

1. 测试写入的行必须在 autouse fixture 的 teardown 中按特征删除（参考
   `tests/test_execution_source_db.py::_ensure_tables` 的清理模式）。
2. 生成测试 symbol 一律走带前缀的 helper（如 `CN:99{uuid[:6]}`）并登记到集合，
   teardown 用 `symbol = ANY(%s)` 删除；快照/changes 用本测试专用特征
   （`source` / 假 id）删除。
3. 写完测试后**必须实际跑一遍并确认表保持干净**（`pytest tests/test_xxx.py` 前后
   各查一次相关表计数）。
4. 新增 DB 表相关的集成测试时，先想清楚"我这个测试会往哪个表插什么行、怎么删"，
   再写断言。
5. **集成测试禁止硬编码真实 symbol**（如 `CN:600000`）——曾污染真实决策日志；
   一律走前缀 helper（规则 2）。
6. 动过 requires_postgres 测试后，跑全量并验收：`python3 scripts/db_rows_baseline.py save`
   → 全量 pytest → `python3 scripts/db_rows_baseline.py check` 必须 OK
   （27 张关键表行数零变化）。

---

## New-module conventions (H3 · outbound / errors / testability)

E1/H3 lesson: 26 + 15 old tests broke because services constructed their own
`ts.pro_api(token)` / `psycopg.connect()`. New outbound code must converge:

1. **Single pooled client per vendor.** Tushare → `clients/tushare_pool.get_pool().pro()`
   (multi-token round-robin + quota watchdog); Postgres → `db.get_connection()`
   (`with` only — exit commits, exception rolls back). Never add a second
   `pro_api(` / `connect(` call site; the only legitimate exceptions are
   `realtime_quote` (tk.csv file-token flow) and `bar_5min` (global-token flow).
2. **Missing-credential guard contract.** Gate on the *parsed* setting
   (`settings.tushare_tokens`, not the raw env string) and keep the legacy
   message (`"TU_SHARE_API_KEY is not set"`) — tests and frontend match on it.
   `get_pool()` raising the same message is the backstop, not the gate.
3. **`{"ok", "error"}` dicts, never bare raise** across service boundaries;
   exceptions are for programmer errors and pool-exhaustion backstops.
4. **Singleton + reset for testability.** Process-wide clients expose
   `get_pool()` (rebuilds when settings change) + `reset_pool()`; injectable
   `pro_factory / clock / sleep` (pool) so policy is unit-testable with fakes.
   Tests patch `<module>.get_pool`, never `<module>.ts`.
5. **No network/DB at import.** Lazy imports inside functions; module import
   must succeed (and tests must pass) with no Postgres and no tokens.

---

## Language

- User-facing chat: Chinese
- Code, comments, commit messages: English
