"""TIP-002 N-day funnel history tests: GET /watchlist/automation/runs.

The endpoint is a thin passthrough over ``get_automation_runs`` (which reads
``watchlist_automation_runs.meta.funnel``); these tests mock the service layer
so they run without Postgres. The literal ``runs`` path must win over the
dynamic ``/watchlist/automation/{run_id}`` route.
"""

from __future__ import annotations

from unittest.mock import patch

from fastapi.testclient import TestClient  # type: ignore[import-not-found]

from data_sync_service.main import app  # type: ignore[import-not-found]

client = TestClient(app)


def _fake_run(trade_date: str, tv: int, trend_ok: int) -> dict:
    return {
        "runId": f"run-{trade_date}",
        "tradeDate": trade_date,
        "trigger": "scheduled",
        "skipped": False,
        "screenerAdded": 2,
        "createdAt": f"{trade_date}T09:30:00+00:00",
        "meta": {
            "funnel": {
                "tvHit": tv,
                "passPullback": 3,
                "passTrendOk": trend_ok,
                "addedNew": 2,
                "fallbackUsed": False,
                "fallbackHit": 0,
                "fallbackTrendOk": 0,
                "fallbackAdded": 0,
            }
        },
    }


def test_funnel_history_shape() -> None:
    with patch(
        "data_sync_service.api.watchlist_routes.get_automation_runs",
        return_value=[_fake_run("2026-08-05", 10, 4), _fake_run("2026-08-04", 8, 2)],
    ):
        body = client.get("/watchlist/automation/runs?limit=10").json()
    assert body["ok"] is True
    assert body["asOfDate"]
    assert len(body["runs"]) == 2
    first = body["runs"][0]
    assert first["tradeDate"] == "2026-08-05"
    assert first["meta"]["funnel"]["tvHit"] == 10
    assert first["meta"]["funnel"]["passTrendOk"] == 4


def test_funnel_history_passes_limit() -> None:
    with patch(
        "data_sync_service.api.watchlist_routes.get_automation_runs",
        return_value=[],
    ) as mock_get:
        client.get("/watchlist/automation/runs?limit=5")
    assert mock_get.call_args.kwargs == {"limit": 5}


def test_funnel_history_clamps_limit() -> None:
    resp = client.get("/watchlist/automation/runs?limit=999")
    assert resp.status_code == 422


def test_funnel_history_route_takes_precedence_over_run_id() -> None:
    """`runs` is a literal path, not a run id — must return ok:true, not 404."""
    with (
        patch(
            "data_sync_service.api.watchlist_routes.get_automation_runs",
            return_value=[],
        ),
        patch(
            "data_sync_service.api.watchlist_routes.get_automation_run",
            return_value=None,
        ),
    ):
        resp = client.get("/watchlist/automation/runs")
    assert resp.status_code == 200
    assert resp.json()["ok"] is True
