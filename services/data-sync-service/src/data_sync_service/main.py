from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI

from .api.query_routes import router as query_router
from .api.sync_routes import router as sync_router
from .scheduler import create_scheduler


@asynccontextmanager
async def lifespan(app: FastAPI):
    scheduler = create_scheduler()
    scheduler.start()
    yield
    scheduler.shutdown(wait=False)


app = FastAPI(lifespan=lifespan)
app.include_router(query_router)
app.include_router(sync_router)
