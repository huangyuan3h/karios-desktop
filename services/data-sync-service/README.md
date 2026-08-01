# Data Sync Service

FastAPI service for scheduled data synchronization.

## Local Development

1. Ensure the root `.env` contains `DATABASE_URL`.
2. Install dependencies with uv:

```bash
uv sync
```

3. Run the service:

```bash
uv run uvicorn data_sync_service.main:app --app-dir src --reload
```

## Monorepo Dev

From repo root:

```bash
pnpm dev
```

## Database Migrations (Alembic)

Schema baseline lives in `alembic/versions/0001_baseline.py` (DDL aggregated from `db/*.py` via `schema_baseline.py`).

Per-module `ensure_table()` remains for local dev convenience; **new schema changes must add an Alembic revision**.

```bash
cd services/data-sync-service

# Fresh empty Postgres
PYTHONPATH=src alembic upgrade head

# Existing DB (tables already created by ensure_table)
PYTHONPATH=src alembic stamp head

# New change after baseline
PYTHONPATH=src alembic revision -m "describe_change"
PYTHONPATH=src alembic upgrade head
```

Alembic reads `DATABASE_URL` from the repo root `.env` (same as the app). SQLAlchemy uses the `postgresql+psycopg://` driver (psycopg v3).

## Endpoints

- `GET /healthz`
- `GET /quote` — realtime quote from tushare (query params: ts_code or ts_codes)
- `GET /market/stocks/{symbol}/bars` — StockPage-compatible bars (query params: days, force ignored)
- `GET /market/stocks/trendok` — Watchlist TrendOK/score from DB-cached daily bars (query params: symbols, realtime). For network K-line refresh use `/market/stocks/{symbol}/bars?force=true` first. Uses lightweight market regime (`include_breadth=False`) with a 60-second in-process TTL cache (`clear_trendok_cache` on bars force sync); East Money industry is DB-only on this path (no HTTP).
- `GET /market/stocks/resolve` — Resolve symbols to names from stock_basic (query params: symbols)
- `GET /trade-reviews` — list trade review records (query params: limit, offset, symbol)
- `GET /trade-reviews/{review_id}` — get a single trade review
- `POST /trade-reviews` — create a trade review record
- `PUT /trade-reviews/{review_id}` — update a trade review record
- `DELETE /trade-reviews/{review_id}` — delete a trade review record
- `GET /stock-basic` — return all stock_basic rows from DB (~5k)
- `POST /sync/stock-basic` — trigger sync from tushare into DB (upsert by ts_code)
- `GET /daily` — return daily bars from DB (query params: ts_code, start_date, end_date, limit)
- `GET /daily/status` — today's full sync run (success/fail, last_ts_code on failure)
- `POST /sync/daily` — trigger full sync of daily bars (2023-01-01 to today; skip if today ok, resume from failure)
- `GET /adj-factor/status` — today's adj_factor sync run (success/fail, last_ts_code on failure)
- `POST /sync/adj-factor` — trigger full sync of adj_factor into daily table (skip/resume like daily)
- `POST /sync/trade-cal` — manually sync trade calendar into DB (query params: exchange, start_date, end_date)
- `GET /close/status` — close-sync status (today run + last success)
- `POST /sync/close` — close-time sync by trade_date window (daily + adj_factor, paged)

### TradingView screeners (async capture jobs)

Requires migration `0002_tv_capture_jobs` (`PYTHONPATH=src alembic upgrade head`).

- `GET /integrations/tradingview/screeners` — list screeners
- `POST /integrations/tradingview/screeners/{id}/sync` — **202** enqueue capture job (`{ jobId, status, screenerId }`)
- `GET /integrations/tradingview/capture-jobs/{job_id}` — poll job status (`queued` / `running` / `done` / `failed`)
- `GET /integrations/tradingview/capture-jobs?screener_id=` — list recent jobs

Background worker (max 2 concurrent) runs in-process; dedupes active jobs per screener.

## Scheduler

One Python file per cron job under `scheduler/`, with `JOB_ID`, `build_trigger()`, and `run()`. Register in `scheduler/__init__.py`. All triggers use `Asia/Shanghai`; APScheduler itself is anchored in UTC with a 12 h `misfire_grace_time`.

Daily / weekday post-close chain:

- `close_sync_job` — daily 17:10 weekdays; pulls daily + adj_factor by `trade_date`; runs `run_post_close_sync()` (index_daily, macro_daily, etf_fund_flow, option_iv, top_inst, em_industry).
- `close_catchup_job` — every 10 min weekdays 17:00–23:00; heals missed `close_sync` runs.
- `index_daily_job` — weekdays 16:30; `pro.index_daily` for SH/SZ/CSI300.
- `index_basic_job` — weekdays 17:15; `pro.index_dailybasic` (market breadth for `macro_snapshot`).
- `hk_daily_job` — daily 17:30; HK K-line incremental sync (akshare → yfinance → tushare).
- `watchlist_automation_job` — weekdays 17:30; post-close GC + screener import + Alpha Radar add.
- `cn_industry_post_close_job` — weekdays 17:35; `industry_fund_flow` + `industry_mainline` + `cn_sentiment` so Dashboard 顶部 is warm without a user click.
- `eastmoney_industry_job` — weekdays 18:00; incremental East Money industry labels.
- `hk_industry_job` — daily 02:00; Xueqiu mbu HK industry labels (before HK open 09:30 HKT).
- `macro_daily_job` — Tue–Sat 07:00; US indices + FX + A50/HSI futures + COMM series.

Weekly / monthly:

- `stock_basic_job` — Fri 18:00; tushare `pro.stock_basic` for IPOs/delistings/name changes.
- `daily_sync_job` — Fri 17:00; legacy per-stock loop (deprecated; falls through to `close_sync`).
- `adj_factor_job` — Fri 17:00; full `adj_factor` safety net.
- `hk_basic_job` — 1st of month 03:30.
- `fund_basic_job` — 1st of month 04:00.
- `etf_daily_job` — 1st of month 19:00; full ETF K-line backfill.

Continuous (interval triggers):

- `news_fetch_job` — every 4h; RSS fetch + 72h prune.
- `alpha_radar_ingest_job` — every 4h; RSSHub + English RSS ingest.
- `alpha_radar_process_job` — every 1h; LLM batch on raw docs.
- `alpha_radar_fetch_job` — every 12h; full pipeline (cooldown 12h).
- `tv_screener_capture_job` — weekdays 09:30 (AM) + 15:30 (PM); enqueues all enabled TradingView screeners into `tv_capture_jobs`. The in-process `tv_capture_worker` (max 2 concurrent) consumes the queue; results land as `tv_snapshots`.

Always-on background worker (not a cron, but started in `lifespan`):

- `tv_capture_worker` — drains `tv_capture_jobs` table; max 2 concurrent captures; dedupes active jobs per screener.

## Sync job record

Table `sync_job_record` stores each run: job_type, sync_at, success, last_ts_code (on failure), error_message. Used to skip if today already succeeded, or resume from last_ts_code after a failed run.
