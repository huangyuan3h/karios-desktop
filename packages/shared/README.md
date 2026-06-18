# @karios/shared

Zod schemas and TypeScript types shared across the monorepo (primarily `desktop-ui` API clients).

## Schemas

| Module | Types |
|--------|-------|
| `schemas/trendok.ts` | `TrendOkResult`, `WatchlistRiskAlert` |
| `schemas/watchlist.ts` | `WatchlistItem`, `WatchlistRegistryItem`, `WatchlistRegistryResponse` |
| `schemas/tvCapture.ts` | `TvCaptureJob`, `TvCaptureJobStatus` |
| `schemas/portfolio.ts` | `PortfolioSnapshot` |
| `schemas/artifact.ts` | `Artifact` |
| `schemas/orderRecipe.ts` | `OrderRecipe` |

## Commands

```bash
pnpm -C packages/shared build
pnpm -C packages/shared test
pnpm -C packages/shared typecheck
```

After changing schemas, rebuild before running `desktop-ui` (`transpilePackages` uses compiled `dist/`).

## Consumers

- `apps/desktop-ui` — import types from `@karios/shared`
- `services/data-sync-service` — manual field alignment + `tests/test_api.py` (no runtime dependency)
