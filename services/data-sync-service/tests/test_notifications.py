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
                     "pnlPct": 2.0, "lastClose": 100, "stopLossLine": 98.5,
                     "trailingLine": 92, "nearStop": True, "nearStopLabel": "止损",
                     "nearStopDistancePct": 1.5},
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


def test_trading_job_types_cover_twin_star_health() -> None:
    assert {"twin_star_intraday", "sleeve_etf_daily_sync", "stock_daily_basic_sync"} <= nf.TRADING_JOB_TYPES


def test_cron_failures_includes_twin_star_snapshot(monkeypatch) -> None:
    monkeypatch.setattr(
        "data_sync_service.db.sync_job_record.list_recent_failures",
        lambda hours=24: [
            {"job_type": "twin_star_intraday", "sync_at": "2026-09-02T04:35:00Z",
             "error_message": "no_session_snapshot"},
        ],
    )
    out = nf._cron_failures()
    assert len(out) == 1
    assert out[0]["lane"] == "system"
    assert "twin_star_intraday" in out[0]["title"]


def test_twin_star_snapshot_alert_system_lane(monkeypatch) -> None:
    from datetime import datetime
    from zoneinfo import ZoneInfo

    monkeypatch.setattr(
        "data_sync_service.service.twin_star_intraday.now_cn",
        lambda: datetime(2026, 9, 2, 13, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
    )
    monkeypatch.setattr(
        "data_sync_service.service.twin_star_intraday.intraday_snapshot_status",
        lambda now=None: {
            "ok": False,
            "missing": True,
            "stale": True,
            "session": "2026-09-02",
            "reason": "no_session_snapshot",
        },
    )
    out = nf._twin_star_snapshot_alert("twin_star")
    assert len(out) == 1
    assert out[0]["type"] == "twin_star_snapshot"
    assert out[0]["lane"] == "system"
    assert out[0]["severity"] == "high"
    assert "不要用 T-1" in out[0]["detail"]
    assert nf._twin_star_snapshot_alert("single_track") == []


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


def test_pyramid_trigger_alert_fires_when_close_crosses_line(monkeypatch) -> None:
    """A held symbol whose close crossed the +2.5% trigger and not added yet -> alert."""
    monkeypatch.setattr(
        nf, "_anchor_blocks",
        lambda: {"CN": {"holdings": [
            {
                "symbol": "CN:300628", "name": "亿联网络", "action": "HOLD",
                "pyramidTriggerLine": 40.897, "pyramidAdded": False, "lastClose": 42.01,
            },
            {
                "symbol": "CN:600000", "name": "未触发", "action": "HOLD",
                "pyramidTriggerLine": 10.0, "pyramidAdded": False, "lastClose": 9.5,
            },
            {
                "symbol": "CN:600001", "name": "已加过", "action": "HOLD",
                "pyramidTriggerLine": 10.0, "pyramidAdded": True, "lastClose": 11.0,
            },
        ]}},
    )
    out = nf._pyramid_trigger_alerts()
    assert len(out) == 1
    assert out[0]["type"] == "pyramid_trigger"
    assert "亿联网络" in out[0]["title"]
    assert "加半仓" in out[0]["detail"]


def test_build_notifications_sorts_high_first(monkeypatch) -> None:
    monkeypatch.setattr(nf, "_stop_trail_alerts", lambda mode="single_track", ctx=None: [
        {"id": "a", "type": "near_line", "severity": "medium", "title": "m", "detail": "d",
         "anchor": "holdings", "createdAt": "x"},
    ])
    monkeypatch.setattr(nf, "_cron_failures", lambda: [
        {"id": "b", "type": "cron_failed", "severity": "high", "title": "h", "detail": "d",
         "anchor": "scheduler", "createdAt": "x"},
    ])
    monkeypatch.setattr(nf, "_recon_alerts", lambda: [])
    monkeypatch.setattr(nf, "_rolling_oos_warning", lambda: [])
    monkeypatch.setattr(nf, "_pyramid_trigger_alerts", lambda mode="single_track", ctx=None: [])
    monkeypatch.setattr(nf, "_third_asset_notification", lambda: [])
    monkeypatch.setattr(nf, "_twin_star_notification", lambda mode="single_track": [])
    monkeypatch.setattr(nf, "_twin_star_snapshot_alert", lambda mode="single_track": [])
    monkeypatch.setattr(nf, "_load_health_ctx", lambda: {"blocks": {}, "pick": None, "tradeDate": None})
    out = nf.build_notifications()
    assert [x["severity"] for x in out] == ["high", "medium"]


def test_third_asset_notification_active(monkeypatch) -> None:
    """Pick-strong sleeve hint becomes a core-book notification."""
    monkeypatch.setattr(
        "data_sync_service.service.portfolio_health.build_portfolio_health",
        lambda **k: {
            "multiAssetSleeve": {
                "active": True,
                "action": "BUY",
                "label": "建议买入 513100",
                "message": "闲置资金 90% → 建议买入",
                "pick": {"key": "NASDAQ"},
            }
        },
    )
    out = nf._third_asset_notification()
    assert len(out) == 1
    assert out[0]["type"] == "pick_strong"
    assert out[0]["book"] == "core"
    assert out[0]["lane"] == "trade"
    assert "建议买入 513100" in out[0]["title"]

    monkeypatch.setattr(
        "data_sync_service.service.portfolio_health.build_portfolio_health",
        lambda **k: {"multiAssetSleeve": {"active": False, "action": "NONE"}},
    )
    assert nf._third_asset_notification() == []


def test_route_ok(monkeypatch) -> None:
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    import data_sync_service.api.notifications_routes as nr
    from data_sync_service.api.notifications_routes import router

    seen: list[str] = []

    def _capture(mode: str = "twin_star") -> list[dict]:
        seen.append(mode)
        return [{"id": "x", "severity": "high"}]

    monkeypatch.setattr(nr, "build_notifications", _capture)
    app = FastAPI()
    app.include_router(router)
    r = TestClient(app).get("/api/notifications")
    assert r.status_code == 200
    assert r.json()["items"][0]["severity"] == "high"
    assert seen == ["twin_star"]


def test_twin_star_does_not_emit_s3_pyramid_or_false_near_line(monkeypatch) -> None:
    ctx = {
        "pick": "OIL",
        "tradeDate": "2026-09-02",
        "blocks": {
            "CN": {
                "holdings": [
                    {
                        "symbol": "CN:600540",
                        "name": "新赛股份",
                        "action": "HOLD",
                        "pyramidAdded": False,
                        "pyramidTriggerLine": 6.57,
                        "lastClose": 6.73,
                        "costPrice": 6.41,
                        "entryDate": "2026-09-02",
                        "pnlPct": 4.99,
                        "stopLossLine": 6.09,
                        "trailingLine": 6.192,
                    }
                ]
            }
        },
    }
    assert nf._pyramid_trigger_alerts("twin_star", ctx) == []
    out = nf._stop_trail_alerts("twin_star", ctx)
    titles = [x["title"] for x in out]
    assert not any("金字塔" in t for t in titles)
    assert not any("接近止损" in t for t in titles)
    assert not any("接近移动" in t for t in titles)
    assert all(x.get("book") == "sat" for x in out)


def test_twin_star_sat_exit_and_protect_stop(monkeypatch) -> None:
    ctx = {
        "pick": "OIL",
        "tradeDate": "2026-09-02",
        "blocks": {
            "CN": {
                "holdings": [
                    {
                        "symbol": "CN:300413",
                        "name": "芒果超媒",
                        "costPrice": 20,
                        "entryDate": "2026-08-31",
                        "lastClose": 18.9,
                    }
                ]
            }
        },
    }
    out = nf._stop_trail_alerts("twin_star", ctx)
    types = {x["type"] for x in out}
    assert "sat_exit" in types
    assert "sat_stop" in types


def test_build_notifications_hides_recon_in_twin_star(monkeypatch) -> None:
    monkeypatch.setattr(nf, "_load_health_ctx", lambda: {"blocks": {}, "pick": "OIL", "tradeDate": "2026-09-02"})
    monkeypatch.setattr(nf, "_stop_trail_alerts", lambda *a, **k: [])
    monkeypatch.setattr(nf, "_pyramid_trigger_alerts", lambda *a, **k: [])
    monkeypatch.setattr(nf, "_cron_failures", lambda: [])
    monkeypatch.setattr(
        nf,
        "_recon_alerts",
        lambda: [{"id": "recon:x", "type": "recon_missing", "severity": "low", "title": "t", "detail": "d", "anchor": "recon"}],
    )
    monkeypatch.setattr(nf, "_rolling_oos_warning", lambda: [])
    monkeypatch.setattr(nf, "_third_asset_notification", lambda: [])
    monkeypatch.setattr(nf, "_twin_star_notification", lambda mode="single_track": [])
    monkeypatch.setattr(nf, "_twin_star_snapshot_alert", lambda mode="single_track": [])
    assert nf.build_notifications("twin_star") == []
    assert len(nf.build_notifications("single_track")) == 1
