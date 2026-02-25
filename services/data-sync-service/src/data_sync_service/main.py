from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI  # type: ignore[import-not-found]
from fastapi.middleware.cors import CORSMiddleware  # type: ignore[import-not-found]

from .api.dashboard_routes import router as dashboard_router
from .api.broker_routes import router as broker_router
from .api.industry_flow_routes import router as industry_flow_router
from .api.market_sentiment_routes import router as market_sentiment_router
from .api.journal_routes import router as journal_router
from .api.query_routes import router as query_router
from .api.simtrade_routes import router as simtrade_router
from .api.system_prompts_routes import router as system_prompts_router
from .api.sync_routes import router as sync_router
from .api.tv_chrome_routes import router as tv_chrome_router
from .api.tv_routes import router as tv_router
from .scheduler import create_scheduler


@asynccontextmanager
async def lifespan(app: FastAPI):
    scheduler = create_scheduler()
    scheduler.start()
    yield
    scheduler.shutdown(wait=False)


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
app.include_router(broker_router)
app.include_router(industry_flow_router)
app.include_router(market_sentiment_router)
