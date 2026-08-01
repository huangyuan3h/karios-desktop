from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI  # type: ignore[import-not-found]
from fastapi.middleware.cors import CORSMiddleware  # type: ignore[import-not-found]

from .api.alpha_radar_routes import router as alpha_radar_router
from .api.broker_routes import router as broker_router
from .api.dashboard_routes import router as dashboard_router
from .api.execution_journal_routes import router as execution_journal_router
from .api.industry_flow_routes import router as industry_flow_router
from .api.journal_routes import router as journal_router
from .api.market_sentiment_routes import router as market_sentiment_router
from .api.news_routes import router as news_router
from .api.query_routes import router as query_router
from .api.simtrade_routes import router as simtrade_router
from .api.sync_routes import router as sync_router
from .api.system_prompts_routes import router as system_prompts_router
from .api.trade_review_routes import router as trade_review_router
from .api.tv_chrome_routes import router as tv_chrome_router
from .api.tv_routes import router as tv_router
from .api.watchlist_routes import router as watchlist_router
# OPT-045 Phase A: 4 stable discovery endpoints (no auth — must be reachable
# before any API key can be issued).
from .api.discovery_routes import router as discovery_router
# OPT-045 Phase B / OPT-046: 3 read-only business endpoints under /v1/*.
# Auth is opt-in (no-op when KARIOS_API_KEYS is empty).
from .api.v1_business_routes import router as v1_business_router
# OPT-047 Phase C: /v1/explain/{symbol} — comprehensive context pack.
from .api.v1_explain_routes import router as v1_explain_router
from .scheduler import create_scheduler
from .service.tv_capture_worker import start_tv_capture_worker, stop_tv_capture_worker


@asynccontextmanager
async def lifespan(app: FastAPI):
    start_tv_capture_worker()
    scheduler = create_scheduler()
    scheduler.start()
    yield
    scheduler.shutdown(wait=False)
    stop_tv_capture_worker()


app = FastAPI(lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(query_router)
app.include_router(simtrade_router)
app.include_router(sync_router)
app.include_router(system_prompts_router)
app.include_router(dashboard_router)
app.include_router(tv_router)
app.include_router(tv_chrome_router)
app.include_router(journal_router)
app.include_router(execution_journal_router)
app.include_router(trade_review_router)
app.include_router(broker_router)
app.include_router(industry_flow_router)
app.include_router(market_sentiment_router)
app.include_router(news_router)
app.include_router(alpha_radar_router)
app.include_router(watchlist_router)
# OPT-045 Phase A: discovery router (4 stable endpoints, no auth).
# Phase B will add a separate /v1/* business router that depends on
# api.auth.require_api_key.
app.include_router(discovery_router)
# OPT-045 Phase B / OPT-046: read-only business endpoints under /v1/*.
app.include_router(v1_business_router)
# OPT-047 Phase C: /v1/explain/{symbol}.
app.include_router(v1_explain_router)
