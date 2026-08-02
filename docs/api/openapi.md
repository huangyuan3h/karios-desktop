# /v1/* OpenAPI documentation（OPT-051）

> **TL;DR**: Karios serves a fully spec'd OpenAPI 3.1 document. Humans read
> the interactive docs at **`/docs`** (Swagger UI) and **`/redoc`** (ReDoc).
> Machines consume the JSON at **`/openapi.json`** or via the discovery
> wrapper **`GET /v1/schema`**.

---

## 1. Why three places to read the same spec?

| Surface | Format | Audience | Notes |
|---------|--------|----------|-------|
| **`GET /v1/schema`** | JSON (full app) | External AI assistant | Discovery-friendly alias. Body is whatever `app.openapi()` returns. |
| **`GET /openapi.json`** | JSON (full app) | Tooling (Postman, codegen, MCP, Swagger CLI) | FastAPI default. Identical body. |
| **`GET /docs`** | HTML — Swagger UI | Humans (you) | Try-it-out form, auto-refresh on reload. |
| **`GET /redoc`** | HTML — ReDoc | Humans (reviewers) | Read-only, prints well, group-by-tag. |

All three are served by the same `app.openapi()` generator, so there is one
single source of truth and zero drift risk.

---

## 2. OpenAPI metadata (top of `info` block)

```json
{
  "title": "Karios /v1/* API",
  "version": "0.1.0",
  "description": "OpenAI-compatible /v1/* surface ...",
  "openapi_tags": [
    {"name": "v1:discovery", "description": "Stable discovery endpoints (no auth required)."},
    {"name": "v1:business",  "description": "Read-only business data (market, watchlist, journal, paper-trades)."},
    {"name": "v1:explain",   "description": "Comprehensive context pack for a single symbol."},
    {"name": "v1:quota",     "description": "Per-API-key quota usage snapshot."}
  ]
}
```

`version` is driven by the `KARIOS_API_VERSION` env var (default `0.1.0`).
Bump via `scripts/bump-api-version.sh` (see [`bump-api-version.sh`](../../services/data-sync-service/scripts/bump-api-version.sh)).

---

## 3. Endpoint index

| Tag | Path | Method | Auth | Purpose |
|-----|------|--------|------|---------|
| v1:discovery | `/v1/version`              | GET | no  | API version + min compatible |
| v1:discovery | `/v1/schema`               | GET | no  | Full OpenAPI 3.1 JSON |
| v1:discovery | `/v1/errors`               | GET | no  | Error-code dictionary (codes + recovery_hint) |
| v1:discovery | `/v1/changelog`            | GET | no  | Versioned changelog entries |
| v1:business  | `/v1/market/snapshot`      | GET | opt | TrendOK + score + quote for N symbols |
| v1:business  | `/v1/watchlist/items`      | GET | opt | Pool + positions + action cards |
| v1:business  | `/v1/decision-journal/query` | GET | opt | Recent gate / action / why changes |
| v1:business  | `/v1/paper-trades`         | GET | opt | Paper-trade intake log |
| v1:business  | `/v1/paper-trades/stats`   | GET | opt | Win rate / avg pnl / holding distribution |
| v1:explain   | `/v1/explain/{symbol}`     | GET | opt | Single-symbol context pack (no LLM call) |
| v1:quota     | `/v1/quota`                | GET | opt | Current key usage snapshot |

Auth column:

- **no**  — never requires `Authorization`, even when keys are configured (the 4 discovery endpoints must remain reachable before any key can be issued).
- **opt** — requires `Authorization: Bearer <key>` only when `KARIOS_API_KEYS` is non-empty.

---

## 4. Authentication

```http
GET /v1/market/snapshot?symbols=600519 HTTP/1.1
Host: api.example.com
Authorization: Bearer sk-frontend-abc123
```

When `KARIOS_API_KEYS` is unset the header is ignored and every request is
allowed (local dev / first-run mode). When it is set the same header
**must** be present and the secret must match one of the configured keys.

### 4.1 Key format (env var)

```bash
# Legacy (no per-key quota, still supported):
KARIOS_API_KEYS="sk-abc,sk-xyz"

# New (per-key label + quota):
KARIOS_API_KEYS="frontend:sk-abc:600:0:0,external-ai:sk-xyz:60:1000:10000"
```

Fields (colon-separated): `label:secret:rpm:rph:rpd`. `0` means unlimited.
`label` must be unique, `secret` must be unique. See
[`api/key_quota.py`](../../services/data-sync-service/src/data_sync_service/api/key_quota.py).

### 4.2 401 / 429

| Code | When | Headers |
|------|------|---------|
| 401  | Missing / malformed / unknown `Authorization` | `WWW-Authenticate: Bearer` |
| 429  | A quota window is full | `Retry-After: <seconds>`, `X-RateLimit-Limit`, `X-RateLimit-Remaining`, `X-RateLimit-Reset` |

---

## 5. Quota (OPT-051)

### 5.1 What gets counted

Every successful HTTP request to an `opt`-auth route consumes **one hit in
each of three sliding windows** for the matched key:

| Window | Length | Purpose |
|--------|--------|---------|
| `rpm` | last 60 s | Burst protection |
| `rph` | last 3600 s | Sustained-traffic cap |
| `rpd` | last 86 400 s | Daily ceiling |

A window with `limit = 0` is not tracked. A key with **all three** `0` has no
quota and the dependency short-circuits.

### 5.2 Self-inspection: `GET /v1/quota`

```json
{
  "key_label": "external-ai",
  "auth_enabled": true,
  "windows": {
    "rpm": {"used": 7,  "limit": 60,    "window_seconds": 60},
    "rph": {"used": 142,"limit": 1000,  "window_seconds": 3600},
    "rpd": {"used": 312,"limit": 10000, "window_seconds": 86400}
  },
  "as_of": "2026-08-01T12:34:56.789+00:00"
}
```

Anonymous callers (auth disabled) receive `auth_enabled: false` and an
empty `windows` map.

### 5.3 Suggested quota profiles

| Tier | rpm | rph | rpd | Use case |
|------|-----|-----|-----|----------|
| `frontend` | 600 | 0 | 0 | Local desktop UI + dashboard polling |
| `external-ai` | 60 | 1000 | 10000 | External AI assistant polling + bursts |
| `read-only-debug` | 10 | 60 | 200 | Manual curl / jq exploration |

---

## 6. Versioning

`KARIOS_API_VERSION` is bumped via [`bump-api-version.sh`](../../services/data-sync-service/scripts/bump-api-version.sh):

```bash
./scripts/bump-api-version.sh patch   # 0.1.0 → 0.1.1  (default; safe)
./scripts/bump-api-version.sh minor   # 0.1.0 → 0.2.0  (new endpoint)
./scripts/bump-api-version.sh major   # 0.1.0 → 1.0.0  (BREAKING — public review first)
```

`/v1/version` reports the **running** value. `/v1/changelog` lists every
recorded change. See [`docs/designs/api-contract.md`](../../docs/designs/api-contract.md)
for the full MAJOR / MINOR / PATCH semantics.

---

## 7. Where to find what

| What | Where |
|------|-------|
| Field-level schemas + types | `GET /v1/schema` (or `/openapi.json`) |
| Human-readable field guide | [`docs/api/discovery.md`](./discovery.md), [`business.md`](./business.md), [`explain.md`](./explain.md) |
| Error codes + recovery hints | [`docs/api/errors.md`](./errors.md) + `GET /v1/errors` |
| API contract + versioning rules | [`docs/designs/api-contract.md`](../../docs/designs/api-contract.md) |
| Released changes | [`docs/api/CHANGELOG.md`](./CHANGELOG.md) + `GET /v1/changelog` |

---

## 8. Known limitations

- **In-memory state** — quotas reset on process restart. Acceptable for a
  homelab single-process FastAPI; switch to Postgres-backed quotas when
  multiple workers or a multi-region tunnel is introduced.
- **No admin endpoint** — there is no `/v1/admin/keys`. To rotate or list
  keys, edit `.env` and restart. Add admin auth separately if needed.
- **No per-route quota override** — all `opt`-auth routes share the key's
  configured windows. If a single endpoint becomes a hot path, add a
  per-route override.