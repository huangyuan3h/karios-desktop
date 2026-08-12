"""service/notifications.py + api/notifications_routes.py coverage."""

from __future__ import annotations

from data_sync_service.service import notifications as nf


def test_stop_trail_alerts_extracts_exit_and_near_line(monkeypatch) -> None:
    monkeypatch.setattr(
        nf,
        "_anchor_blocks",
        lambda: {
            "CN": {
                "holdings": [
                    {"symbol": "CN:600000", "name": "浦发银行", "action": "EXIT",
                     "reason": "stop_loss", "pnlPct": -6.0, "stopLossLine": None,
                     "trailingLine": None},
                    {"symbol": "CN:600519", "name": "贵州茅台", "action": "HOLD",
                     "pnlPct": -4.7, "stopLossLine": -5.0, "trailingLine": -3.0},
                ]
            }
        },
    )
    out = nf._stop_trail_alerts()
    types = [x["type"] for x in out]
    assert "exit" in types
    assert "near_line" in types
    exit_item = next(x for x in out if x["type"] == "exit")
    assert exit_item["severity"] == "high"
    assert exit_item["anchor"] == "holdings"
    assert "浦发银行" in exit_item["title"]
    near = [x for x in out if x["type"] == "near_line"]
    assert all(x["anchor"] == "holdings" for x in near)
    # Far-from-line holdings produce no alert.
    assert not any("far" in x["detail"] for x in out)


def test_line_update_and_expire_soon_alerts(monkeypatch) -> None:
    monkeypatch.setattr(
        nf,
        "_anchor_blocks",
        lambda: {
            "CN": {
                "holdings": [
                    {"symbol": "CN:300628", "name": "亿联网络", "action": "HOLD",
                     "pnlPct": 5.0, "stopLossLine": -5.0, "trailingLine": -3.0,
                     "expireDate": "2026-10-03",
                     "lineOps": {"trail_up": [36.828, 37.52], "stop_up": [37.905, 38.1],
                                 "expire_soon": 3, "expireDate": "2026-10-03"}},
                ]
            }
        },
    )
    out = nf._stop_trail_alerts()
    by_type = {x["type"]: x for x in out}
    assert "line_update" in by_type
    lineups = [x for x in out if x["type"] == "line_update"]
    assert len(lineups) == 2
    trail = next(x for x in lineups if "移动线上调" in x["title"])
    assert "36.828 → 37.52" in trail["detail"]
    stop = next(x for x in lineups if "止损线上调" in x["title"])
    assert "37.905 → 38.1" in stop["detail"]
    assert "expire_soon" in by_type
    exp = by_type["expire_soon"]
    assert "剩 3 天" in exp["detail"]
    assert "2026-10-03" in exp["detail"]
    assert all(x["anchor"] == "holdings" for x in out)


def test_recon_alerts_only_when_missing(monkeypatch) -> None:
    monkeypatch.setattr(
        "data_sync_service.db.reconciliation.latest_recon",
        lambda limit=2: [
            {"reconDate": "2026-08-07", "market": "HK", "expected": 19, "actual": 0,
             "missing": 19, "extra": 0},
            {"reconDate": "2026-08-07", "market": "CN", "expected": 0, "actual": 0,
             "missing": 0, "extra": 0},
        ],
    )
    out = nf._recon_alerts()
    assert len(out) == 1
    assert out[0]["type"] == "recon_missing"
    assert out[0]["anchor"] == "recon"
    assert "港股缺 19 只" in out[0]["title"]


def test_cron_failures_filters_trading_jobs(monkeypatch) -> None:
    monkeypatch.setattr(
        "data_sync_service.db.sync_job_record.list_recent_failures",
        lambda hours=24: [
            {"job_type": "paper_trading_update", "sync_at": "2026-08-12T08:00:00Z",
             "error_message": "boom"},
            {"job_type": "some_other_job", "sync_at": "2026-08-12T08:00:00Z",
             "error_message": "ignore me"},
        ],
    )
    out = nf._cron_failures()
    assert len(out) == 1
    assert out[0]["type"] == "cron_failed"
    assert out[0]["severity"] == "high"
    assert "paper_trading_update" in out[0]["title"]


def test_rolling_oos_warning_reads_file(monkeypatch, tmp_path) -> None:
    import json

    p = tmp_path / "rolling_oos_latest.json"
    p.write_text(json.dumps({
        "windowStart": "2026-05-13", "windowEnd": "2026-08-11",
        "warning": True, "warnings": ["HK: -8.5%"],
    }))
    monkeypatch.setattr(nf, "REPORTS_DIR", tmp_path)
    out = nf._rolling_oos_warning()
    assert len(out) == 1
    assert out[0]["type"] == "oos_warning"
    assert "HK: -8.5%" in out[0]["detail"]

    p.write_text(json.dumps({"windowStart": "x", "warning": False}))
    assert nf._rolling_oos_warning() == []


def test_build_notifications_sorts_high_first(monkeypatch) -> None:
    monkeypatch.setattr(nf, "_stop_trail_alerts", lambda: [
        {"id": "a", "type": "near_line", "severity": "medium", "title": "m", "detail": "d",
         "anchor": "holdings", "createdAt": "x"},
    ])
    monkeypatch.setattr(nf, "_cron_failures", lambda: [
        {"id": "b", "type": "cron_failed", "severity": "high", "title": "h", "detail": "d",
         "anchor": "scheduler", "createdAt": "x"},
    ])
    monkeypatch.setattr(nf, "_recon_alerts", lambda: [])
    monkeypatch.setattr(nf, "_rolling_oos_warning", lambda: [])
    out = nf.build_notifications()
    assert [x["severity"] for x in out] == ["high", "medium"]


def test_route_ok(monkeypatch) -> None:
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    import data_sync_service.api.notifications_routes as nr
    from data_sync_service.api.notifications_routes import router

    monkeypatch.setattr(nr, "build_notifications", lambda: [{"id": "x", "severity": "high"}])
    app = FastAPI()
    app.include_router(router)
    r = TestClient(app).get("/api/notifications")
    assert r.status_code == 200
    assert r.json()["items"][0]["severity"] == "high"
