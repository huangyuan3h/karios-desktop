from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI

from .api.routes import router
from .scheduler import create_scheduler


@asynccontextmanager
async def lifespan(app: FastAPI):
    scheduler = create_scheduler()
    scheduler.start()
    yield
    scheduler.shutdown(wait=False)


app = FastAPI(lifespan=lifespan)
app.include_router(router)
