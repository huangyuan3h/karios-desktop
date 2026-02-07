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

## Endpoints

- `GET /healthz`
- `GET /sync/stock-basic` — return all stock_basic rows from DB (~5k)
- `POST /sync/stock-basic` — trigger sync from tushare into DB (upsert by ts_code)
- `GET /sync/daily` — return daily bars from DB (query params: ts_code, start_date, end_date, limit)
- `GET /sync/daily/status` — today's full sync run (success/fail, last_ts_code on failure)
- `POST /sync/daily` — trigger full sync of daily bars (2023-01-01 to today; skip if today ok, resume from failure)
- `GET /sync/adj-factor/status` — today's adj_factor sync run (success/fail, last_ts_code on failure)
- `POST /sync/adj-factor` — trigger full sync of adj_factor into daily table (skip/resume like daily)

## Scheduler

One Python file per cron job under `scheduler/`, with `JOB_ID`, `build_trigger()`, and `run()`. Register in `scheduler/__init__.py`.

- `stock_basic_job`: every Friday 18:00 (Asia/Shanghai). Failures are logged only.
- `daily_sync_job`: full daily sync every Friday 17:00 (Asia/Shanghai), fallback only. Failures are logged only.
- `adj_factor_job`: full adj_factor sync every Friday 17:00 (Asia/Shanghai), fallback only. Failures are logged only.

## Sync job record

Table `sync_job_record` stores each run: job_type, sync_at, success, last_ts_code (on failure), error_message. Used to skip if today already succeeded, or resume from last_ts_code after a failed run.
