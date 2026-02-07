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

## Scheduler

One Python file per cron job under `scheduler/`, with `JOB_ID`, `build_trigger()`, and `run()`. Register in `scheduler/__init__.py`.

- `stock_basic_job`: every Friday 18:00 (Asia/Shanghai). Failures are logged only.
