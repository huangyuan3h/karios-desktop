import pytest
from fastapi.testclient import TestClient  # type: ignore[import-not-found]

from data_sync_service.main import app  # type: ignore[import-not-found]

pytestmark = pytest.mark.requires_postgres

def test_sync_index_daily_no_index_list(monkeypatch) -> None:
    import data_sync_service.service.index_daily as index_daily  # type: ignore[import-not-found]

    monkeypatch.setattr(index_daily, "INDEX_CODES", [])
    result = index_daily.sync_index_daily_full()
    assert result["ok"] is True
    assert result.get("updated", 0) == 0


def test_sync_index_daily_skips_when_today_succeeded(monkeypatch) -> None:
    import data_sync_service.service.index_daily as index_daily  # type: ignore[import-not-found]
    from data_sync_service.db.sync_job_record import (
        ensure_table,
        get_connection,
        insert_record,
    )

    ensure_table()
    insert_record(job_type=index_daily.JOB_TYPE, success=True)

    def _boom(*args, **kwargs):  # noqa: ANN002, ANN003
        raise AssertionError("sync should have been skipped")

    from types import SimpleNamespace

    monkeypatch.setattr(index_daily, "ts", SimpleNamespace(pro_api=_boom))
    try:
        result = index_daily.sync_index_daily_full()
        assert result.get("skipped") is True
    finally:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM sync_job_record WHERE job_type = %s AND sync_at >= now() - interval '5 minutes'",
                    (index_daily.JOB_TYPE,),
                )
            conn.commit()


def test_sync_index_daily_force_clears_today_record(monkeypatch) -> None:
    import data_sync_service.api.sync_routes as sync_routes  # type: ignore[import-not-found]
    from data_sync_service.db.sync_job_record import (
        ensure_table,
        get_today_run,
        insert_record,
    )

    ensure_table()
    insert_record(job_type="index_daily_full", success=True)
    assert get_today_run("index_daily_full") is not None

    called: list[str] = []

    def _sync() -> dict:
        called.append("sync")
        return {"ok": True, "updated": 1}

    monkeypatch.setattr(sync_routes, "sync_index_daily_full", _sync)

    client = TestClient(app)
    resp = client.post("/sync/index-daily?force=true")
    assert resp.status_code == 200
    assert resp.json() == {"ok": True, "updated": 1}
    assert called == ["sync"]
    assert get_today_run("index_daily_full") is None


def test_sync_close_endpoint_includes_index_daily(monkeypatch) -> None:
    import data_sync_service.api.sync_routes as sync_routes  # type: ignore[import-not-found]

    monkeypatch.setattr(sync_routes, "sync_close", lambda exchange, force: {"ok": True, "updated": 1})

    def _post() -> dict:
        return {
            "indexDaily": {"ok": True, "updated": 2},
            "macroDaily": {"ok": True, "updated": 3},
        }

    monkeypatch.setattr(sync_routes, "run_post_close_sync", _post)

    client = TestClient(app)
    resp = client.post("/sync/close")
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["ok"] is True
    assert payload["updated"] == 1
    assert payload["indexDaily"]["ok"] is True
    assert payload["indexDaily"]["updated"] == 2
    assert payload["macroDaily"]["ok"] is True
    assert payload["macroDaily"]["updated"] == 3
