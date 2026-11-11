"""Agent timeline-brief + file-cache tests (no Postgres, no network, no sim).

Step 3 (2026-11-11): ``GET /v1/agent/timeline-brief`` must answer from the
memory or per-strategy file timeline cache and NEVER run a cold simulate. A
double miss is reported honestly so the judgment brief can degrade instead of
blocking for minutes.
"""

from __future__ import annotations

from typing import Any

import pytest
from fastapi.testclient import TestClient

from data_sync_service.api import backtest_routes as br
from data_sync_service.main import app

client = TestClient(app)


@pytest.fixture(autouse=True)
def _clean_timeline_caches() -> Any:
    br._timeline_cache.clear()
    br._timeline_cache_at.clear()
    br._timeline_engine_cache.clear()
    yield
    br._timeline_cache.clear()
    br._timeline_cache_at.clear()
    br._timeline_engine_cache.clear()


def _payload(*, mode: str = "homeport", label: str = "母港") -> dict[str, Any]:
    return {
        "ok": True,
        "start": "2026-01-01",
        "end": "2026-12-31",
        "mode": mode,
        "strategy": label,
        "summary": {
            "fusedPct": 12.5,
            "maxDdFusedPct": 4.2,
            "parkingTrades": 7,
            "trailExits": 2,
        },
        "parkedHeld": {"key": "BOND10", "name": "10年国债ETF", "ts": "511260.SH", "weight": 0.25},
        "rows": [
            {"date": "2026-12-29", "navMulti": 1.0, "idlePct": 20, "positions": 3},
            {"date": "2026-12-30", "navMulti": 1.05, "idlePct": 20, "positions": 3},
            {
                "date": "2026-12-31",
                "navMulti": 1.10,
                "idlePct": 25,
                "deployedPct": 75,
                "positions": 2,
                "pick": "BOND10",
                "pickTs": "511260.SH",
                "parkedSides": 0,
                "parkedTrail": False,
                "parkedRetPct": 0.1,
            },
        ],
    }


def _no_build(*_: Any, **__: Any) -> Any:
    raise AssertionError("timeline-brief must not run a cold build / simulate")


def test_timeline_brief_reads_file_cache_without_simulating(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Any
) -> None:
    monkeypatch.setattr(br, "TIMELINE_CACHE_DIR", tmp_path)
    monkeypatch.setattr(br, "_get_or_build_timeline", _no_build)
    monkeypatch.setattr(br, "simulate", _no_build)
    monkeypatch.setattr(
        "data_sync_service.api.settings_routes.selected_strategy_mode", lambda: "homeport"
    )
    br._save_timeline_file("2026-01-01", "2026-12-31", _payload(), "homeport_m30")

    resp = client.get(
        "/v1/agent/timeline-brief?strategy=homeport_m30&start=2026-01-01&end=2026-12-31"
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is True
    assert body["available"] is True
    assert body["timelineStrategy"] == "homeport_m30"
    assert body["timelineLabel"] == "母港 M30"
    assert body["window"] == {
        "start": "2026-01-01",
        "end": "2026-12-31",
        "requested": True,
        "source": "requested",
    }
    assert body["cache"]["source"] == "file"
    assert body["cache"]["file"].endswith("_homeport_m30_mom_compare_t8.json")
    assert body["lastDay"]["date"] == "2026-12-31"
    assert body["lastDay"]["returnPct"] == 4.76
    assert body["lastDay"]["drawdownFromPeakPct"] == 0.0
    assert body["parking"]["key"] == "BOND10"
    assert body["parking"]["held"]["weight"] == 0.25
    assert body["summary"]["fusedPct"] == 12.5
    assert body["curve"]["endpoint"] == "/api/backtest/timeline"
    assert "rows" not in body


def test_timeline_brief_defaults_to_settings_mapped_key(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Any
) -> None:
    monkeypatch.setattr(br, "TIMELINE_CACHE_DIR", tmp_path)
    monkeypatch.setattr(br, "_get_or_build_timeline", _no_build)
    monkeypatch.setattr(
        "data_sync_service.api.settings_routes.selected_strategy_mode", lambda: "homeport"
    )
    br._save_timeline_file("2026-01-01", "2026-12-31", _payload(), "homeport_m30")

    resp = client.get("/v1/agent/timeline-brief")
    assert resp.status_code == 200
    body = resp.json()
    # settings 'homeport' -> timeline 'homeport_m30' (NOT the frozen M50 key).
    assert body["settingsMode"] == "homeport"
    assert body["timelineStrategy"] == "homeport_m30"
    assert body["cache"]["source"] == "file"
    # The default trailing-year window moved on, so the newest valid file is served.
    assert body["window"]["source"] == "latest_file"
    assert body["window"]["start"] == "2026-01-01"


def test_timeline_brief_reports_miss_without_blocking(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Any
) -> None:
    monkeypatch.setattr(br, "TIMELINE_CACHE_DIR", tmp_path)
    monkeypatch.setattr(br, "_get_or_build_timeline", _no_build)
    monkeypatch.setattr(
        "data_sync_service.api.settings_routes.selected_strategy_mode", lambda: "homeport"
    )

    resp = client.get("/v1/agent/timeline-brief")
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is True
    assert body["available"] is False
    assert body["cache"]["source"] == "miss"
    assert "cold" in body["reason"]
    assert body["warmup"] == "warm_default_timelines"
    assert body["curve"]["endpoint"] == "/api/backtest/timeline"


def test_timeline_brief_does_not_serve_pre_close_file(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Any
) -> None:
    """Same close-expiry rule as the timeline file loader (no pre-close rows)."""
    import os

    monkeypatch.setattr(br, "TIMELINE_CACHE_DIR", tmp_path)
    monkeypatch.setattr(br, "_get_or_build_timeline", _no_build)
    monkeypatch.setattr(
        "data_sync_service.api.settings_routes.selected_strategy_mode", lambda: "homeport"
    )
    br._save_timeline_file("2026-01-01", "2026-12-31", _payload(), "homeport_m30")
    stale = br._last_session_close_ts() - 60
    os.utime(br._timeline_file("2026-01-01", "2026-12-31", "homeport_m30"), (stale, stale))

    resp = client.get("/v1/agent/timeline-brief?strategy=homeport_m30")
    assert resp.status_code == 200
    assert resp.json()["available"] is False
    assert resp.json()["cache"]["source"] == "miss"


def test_timeline_brief_rejects_unknown_strategy(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "data_sync_service.api.settings_routes.selected_strategy_mode", lambda: "homeport"
    )
    resp = client.get("/v1/agent/timeline-brief?strategy=no_such_strategy")
    assert resp.status_code == 400
    assert "harbor|homeport|homeport_m30" in resp.json()["detail"]


def test_file_cache_keys_distinguish_harbor_from_homeport_m30(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Any
) -> None:
    monkeypatch.setattr(br, "TIMELINE_CACHE_DIR", tmp_path)
    harbor = _payload(mode="harbor", label="港湾")
    m30 = _payload(mode="homeport", label="母港")
    br._save_timeline_file("2026-01-01", "2026-12-31", harbor, "harbor")
    br._save_timeline_file("2026-01-01", "2026-12-31", m30, "homeport_m30")

    assert br._timeline_file("2026-01-01", "2026-12-31", "harbor") != br._timeline_file(
        "2026-01-01", "2026-12-31", "homeport_m30"
    )
    loaded_harbor = br._load_timeline_file("2026-01-01", "2026-12-31", "harbor")
    loaded_m30 = br._load_timeline_file("2026-01-01", "2026-12-31", "homeport_m30")
    assert loaded_harbor is not None and loaded_harbor["mode"] == "harbor"
    assert loaded_m30 is not None and loaded_m30["mode"] == "homeport"
    # Display mode is preserved; the logic version is checked via cacheMode.
    assert loaded_harbor["cacheMode"] == br._TIMELINE_MODE
    assert loaded_m30["cacheMode"] == br._TIMELINE_MODE
    assert br._load_timeline_file("2026-01-01", "2026-12-31", "pick_strong") is None
