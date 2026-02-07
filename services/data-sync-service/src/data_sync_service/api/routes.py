from __future__ import annotations

from fastapi import APIRouter

from data_sync_service.db import check_db
from data_sync_service.scheduler import get_foo_status
from data_sync_service.service import foo

router = APIRouter()


@router.get("/healthz")
def healthz() -> dict:
    ok, error = check_db()
    return {
        "status": "ok" if ok else "degraded",
        "db": ok,
        "db_error": error if not ok else None,
    }


@router.get("/foo")
def foo_endpoint() -> dict:
    return foo()


@router.get("/scheduler/foo")
def foo_schedule_status() -> dict:
    return get_foo_status()
