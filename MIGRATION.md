# Quant-Service and SQLite Migration Checklist

This file tracks migration from `services/quant-service` (SQLite) to `services/data-sync-service` (Postgres).
Mark items as done as we complete them.

## Data Migration (one-time)
- [x] Trade journals migrated to Postgres.
- [x] TradingView screeners/snapshots migrated to Postgres.
- [x] Industry fund flow migrated to Postgres.
- [x] Market sentiment migrated to Postgres.
- [ ] Broker data migrated to Postgres.
- [ ] System prompt presets migrated to Postgres (if required).

## Service APIs (data-sync-service)
- [x] Broker APIs implemented in data-sync-service.
- [x] Industry fund flow APIs implemented in data-sync-service.
- [x] Market sentiment APIs implemented in data-sync-service.
- [x] Dashboard summary/sync APIs implemented in data-sync-service.
- [x] Stock chips/fund-flow APIs implemented in data-sync-service.
- [x] Global stock search APIs implemented in data-sync-service.
- [x] System prompt APIs implemented in data-sync-service.

## Frontend Switch-over (desktop-ui)
- [x] Broker page uses data-sync-service.
- [x] Industry flow page uses data-sync-service.
- [x] Market sentiment (Dashboard + Chat reference) uses data-sync-service.
- [x] Dashboard summary/sync uses data-sync-service.
- [x] Stock chips/fund-flow uses data-sync-service.
- [x] Global stock search uses data-sync-service.
- [x] System prompt editor uses data-sync-service.
- [x] Chat references for stock/tradingview use data-sync-service.

## Decommission Quant-Service & SQLite
- [x] Remove quant-service sidecar from Tauri startup.
- [x] Remove quant-service bundle config.
- [x] Delete quant-service runtime references in frontend.
- [x] Remove SQLite DB usage (DATABASE_PATH) from app runtime.
