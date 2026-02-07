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
uv run uvicorn data_sync_service.main:app --reload
```

## Monorepo Dev

From repo root:

```bash
pnpm dev
```

## Endpoints

- `GET /healthz`
- `GET /foo`
