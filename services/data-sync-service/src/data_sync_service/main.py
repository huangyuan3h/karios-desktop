from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI  # type: ignore[import-not-found]
from fastapi.middleware.cors import CORSMiddleware  # type: ignore[import-not-found]

from .api.alpha_radar_routes import router as alpha_radar_router
from .api.backtest_routes import router as backtest_router
from .api.broker_routes import router as broker_router
from .api.commodity_routes import router as commodity_router
from .api.dashboard_routes import router as dashboard_router
from .api.decision_routes import router as decision_router

# OPT-045 Phase A: 4 stable discovery endpoints (no auth — must be reachable
# before any API key can be issued).
from .api.discovery_routes import router as discovery_router
from .api.execution_journal_routes import router as execution_journal_router
from .api.factor_routes import router as factor_router
from .api.health_routes import router as health_router
from .api.industry_flow_routes import router as industry_flow_router
from .api.journal_routes import router as journal_router
from .api.market_sentiment_routes import router as market_sentiment_router
from .api.news_routes import router as news_router
from .api.notifications_routes import router as notifications_router
from .api.query_routes import router as query_router
from .api.research_routes import router as research_router
from .api.sync_routes import router as sync_router
from .api.system_prompts_routes import router as system_prompts_router
from .api.trade_review_routes import router as trade_review_router
from .api.user_trades_routes import router as user_trades_router

# OPT-045 Phase B / OPT-046: 3 read-only business endpoints under /v1/*.
# Auth is opt-in (no-op when KARIOS_API_KEYS is empty).
from .api.v1_business_routes import router as v1_business_router

# OPT-047 Phase C: /v1/explain/{symbol} — comprehensive context pack.
from .api.v1_explain_routes import router as v1_explain_router

# OPT-051 §12 #5: /v1/quota — per-API-key usage snapshot.
from .api.v1_quota_routes import router as v1_quota_router
from .api.watchlist_routes import router as watchlist_router
from .api.webhook_routes import router as webhook_router
from .scheduler import create_scheduler


@asynccontextmanager
async def lifespan(app: FastAPI):
    scheduler = create_scheduler()
    scheduler.start()
    yield
    scheduler.shutdown(wait=False)


app = FastAPI(
    lifespan=lifespan,
    title="Karios /v1/* API",
    version="0.1.0",
    description=(
        "OpenAI-compatible /v1/* surface for the Karios investment desktop. "
        "Designed for an external AI assistant: discovery endpoints first "
        "(`/v1/version`, `/v1/schema`, `/v1/errors`, `/v1/changelog`), then "
        "read-only business data (`/v1/market/snapshot`, `/v1/watchlist/items`, "
        "`/v1/decision-journal/query`, `/v1/paper-trades`, `/v1/paper-trades/stats`), "
        "comprehensive context (`/v1/explain/{symbol}`), and self-inspection "
        "(`/v1/quota`). Auth is opt-in via `Authorization: Bearer <key>` when "
        "`KARIOS_API_KEYS` is set; otherwise all routes are open. "
        "See `docs/api/README.md` for the human-readable index."
    ),
    openapi_tags=[
        {"name": "v1:discovery", "description": "Stable discovery endpoints (no auth required)."},
        {"name": "v1:business", "description": "Read-only business data (market, watchlist, journal, paper-trades)."},
        {"name": "v1:explain", "description": "Comprehensive context pack for a single symbol."},
        {"name": "v1:quota", "description": "Per-API-key quota usage snapshot."},
    ],
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)
# H10: reject state-changing requests from non-local web origins
# (added after CORSMiddleware → outermost, so it also covers preflight-eligible
#  methods while OPTIONS passes through untouched).
from .api.security import LocalOriginGuardMiddleware  # noqa: E402

app.add_middleware(LocalOriginGuardMiddleware)
# OPT-051: parse KARIOS_API_KEYS once at app load so the quota dependency
# doesn't re-read the env var on every request.
from .api.key_quota import keys_from_env  # noqa: E402

app.state.api_keys = keys_from_env()

app.include_router(query_router)
app.include_router(sync_router)
app.include_router(system_prompts_router)
app.include_router(dashboard_router)
app.include_router(journal_router)
app.include_router(execution_journal_router)
app.include_router(health_router)
app.include_router(trade_review_router)
app.include_router(broker_router)
app.include_router(industry_flow_router)
app.include_router(market_sentiment_router)
app.include_router(news_router)
app.include_router(alpha_radar_router)
app.include_router(watchlist_router)
app.include_router(factor_router)
app.include_router(research_router)
app.include_router(user_trades_router)
# OPT-045 Phase A: discovery router (4 stable endpoints, no auth).
# Phase B will add a separate /v1/* business router that depends on
# api.auth.require_api_key.
app.include_router(discovery_router)
app.include_router(notifications_router)
app.include_router(backtest_router)
app.include_router(decision_router)
app.include_router(webhook_router)
app.include_router(commodity_router)
# OPT-045 Phase B / OPT-046: read-only business endpoints under /v1/*.
app.include_router(v1_business_router)
# OPT-047 Phase C: /v1/explain/{symbol}.
app.include_router(v1_explain_router)
# OPT-051 §12 #5: /v1/quota.
app.include_router(v1_quota_router)
