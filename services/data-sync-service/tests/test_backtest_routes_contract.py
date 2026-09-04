"""Contract tests for thin backtest route wrappers (mocked services, no DB)."""

from __future__ import annotations

import json

import pytest
from fastapi import HTTPException

from data_sync_service.api import backtest_routes as br


def test_twin_star_action_ok_and_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    import data_sync_service.service.twin_star_daily as daily
    import data_sync_service.service.twin_star_intraday as intra

    monkeypatch.setattr(intra, "maybe_refresh_intraday_sat", lambda *a, **k: {"asOf": "d"})
    monkeypatch.setattr(daily, "build_twin_star_daily_action", lambda: {"action": "BUY"})
    assert br.twin_star_action() == {"ok": True, "action": "BUY"}

    def boom() -> dict:
        raise RuntimeError("no data")

    monkeypatch.setattr(daily, "build_twin_star_daily_action", boom)
    with pytest.raises(HTTPException) as exc:
        br.twin_star_action()
    assert exc.value.status_code == 500


def test_twin_star_refresh_paths(monkeypatch: pytest.MonkeyPatch) -> None:
    import data_sync_service.service.twin_star_daily as daily
    import data_sync_service.service.twin_star_intraday as intra

    monkeypatch.setattr(intra, "maybe_refresh_intraday_sat", lambda *a, **k: {"asOf": "d"})
    monkeypatch.setattr(daily, "build_twin_star_daily_action", lambda: {"action": "HOLD"})
    out = br.twin_star_refresh()
    assert out["ok"] is True and out["refreshed"] is True

    monkeypatch.setattr(intra, "maybe_refresh_intraday_sat", lambda *a, **k: None)
    assert br.twin_star_refresh()["refreshed"] is False

    def boom(*a, **k):
        raise RuntimeError("em down")

    monkeypatch.setattr(intra, "maybe_refresh_intraday_sat", boom)
    with pytest.raises(HTTPException) as exc:
        br.twin_star_refresh()
    assert exc.value.status_code == 500


def test_recon_and_behavior_latest(monkeypatch: pytest.MonkeyPatch) -> None:
    import data_sync_service.db.behavior_audit as ba
    import data_sync_service.db.reconciliation as rec

    monkeypatch.setattr(rec, "latest_recon", lambda limit=4: [{"d": 1}])
    assert br.backtest_recon_latest(limit=2) == {"ok": True, "items": [{"d": 1}]}
    monkeypatch.setattr(ba, "latest_audit", lambda limit=2: [{"m": "CN"}])
    assert br.behavior_audit_latest(limit=3) == {"ok": True, "items": [{"m": "CN"}]}


def test_behavior_refresh_summary_and_error(monkeypatch: pytest.MonkeyPatch) -> None:
    import data_sync_service.db.paper_trading as pt
    import data_sync_service.service.reconciliation as rec

    monkeypatch.setattr(pt, "today_iso", lambda: "2026-09-04")
    monkeypatch.setattr(
        rec,
        "run_registry_and_persist",
        lambda day: {
            "reconDate": day,
            "markets": {
                "CN": {"expected": 5, "actual": 4, "available": True},
                "HK": {"expected": 0, "actual": 0, "available": False},
            },
        },
    )
    out = br.behavior_audit_refresh(tradeDate=None)
    assert out["ok"] is True and set(out["markets"]) == {"CN"}
    assert out["markets"]["CN"]["expected"] == 5

    def boom(day):
        raise RuntimeError("engine down")

    monkeypatch.setattr(rec, "run_registry_and_persist", boom)
    with pytest.raises(HTTPException) as exc:
        br.behavior_audit_refresh(tradeDate="2026-09-03")
    assert exc.value.status_code == 500


def test_paper_vs_backtest_file_paths(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    monkeypatch.setattr(br, "REPORTS_DIR", tmp_path)
    with pytest.raises(HTTPException) as exc:
        br.backtest_paper_vs_backtest()
    assert exc.value.status_code == 404
    (tmp_path / "paper_vs_backtest_latest.json").write_text("{oops", encoding="utf-8")
    with pytest.raises(HTTPException) as exc:
        br.backtest_paper_vs_backtest()
    assert exc.value.status_code == 500
    (tmp_path / "paper_vs_backtest_latest.json").write_text('{"n": 3}', encoding="utf-8")
    assert br.backtest_paper_vs_backtest() == {"ok": True, "report": {"n": 3}}


def test_core_audit_paths(monkeypatch: pytest.MonkeyPatch) -> None:
    import data_sync_service.service.core_holding_audit as audit

    monkeypatch.setattr(audit, "audit_core_holdings", lambda day: {"violations": []})
    assert br.backtest_core_audit(day="2026-09-04")["ok"] is True

    def boom(day):
        raise RuntimeError("audit down")

    monkeypatch.setattr(audit, "audit_core_holdings", boom)
    with pytest.raises(HTTPException) as exc:
        br.backtest_core_audit(day="2026-09-04")
    assert exc.value.status_code == 500


def test_timeline_file_cache_helpers(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    monkeypatch.setattr(br, "TIMELINE_CACHE_DIR", tmp_path)
    assert br._load_timeline_file("2026-01-01", "2026-02-01") is None
    payload = {"mode": br._TIMELINE_MODE, "rows": [1, 2]}
    br._save_timeline_file("2026-01-01", "2026-02-01", payload)
    assert br._load_timeline_file("2026-01-01", "2026-02-01") == payload
    # Wrong mode and corrupt bodies read as missing.
    p = br._timeline_file("2026-01-01", "2026-02-01")
    p.write_text(json.dumps({"mode": "other"}), encoding="utf-8")
    assert br._load_timeline_file("2026-01-01", "2026-02-01") is None
    p.write_text("{oops", encoding="utf-8")
    assert br._load_timeline_file("2026-01-01", "2026-02-01") is None
    # Stale files read as missing.
    p.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setattr(br, "TIMELINE_CACHE_TTL_HOURS", -1)
    assert br._load_timeline_file("2026-01-01", "2026-02-01") is None
