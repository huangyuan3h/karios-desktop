# Karios Desktop — Agent Guide

Instructions for AI coding agents and maintainers. Read this before schema changes, data-sync work, or checklist tasks.

## Project layout

| Path | Role |
|------|------|
| `apps/desktop-ui` | Next.js UI (Tauri WebView) |
| `services/data-sync-service` | Python FastAPI — data sync, analysis, Postgres |
| `docs/modules/` | Business docs (workflows, not implementation) |
| `docs/optimization-checklist.md` | Architecture debt & scoped agent tasks (OPT-xxx) |

Start dev: `pnpm dev` (from repo root). Backend needs root `.env` with `DATABASE_URL`.

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
2. Update matching **`CREATE_SQL` / `CREATE_*_SQL`** in `services/data-sync-service/src/data_sync_service/db/*.py` (and `testback/db.py` if applicable).
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
- Migration: `0002_tv_capture_jobs` — run `PYTHONPATH=src alembic upgrade head`.

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
