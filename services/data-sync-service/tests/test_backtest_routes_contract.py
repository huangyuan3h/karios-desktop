"""Contract tests for thin backtest route wrappers (mocked services, no DB)."""

from __future__ import annotations

import json

import pytest
from fastapi import HTTPException

from data_sync_service.api import backtest_routes as br


def test_timeline_rejects_unknown_strategy() -> None:
    with pytest.raises(HTTPException) as exc:
        br.backtest_timeline(start="2026-01-01", end="2026-01-31", strategy="no_such_strategy")
    assert exc.value.status_code == 400
    assert "harbor|harbor_h2|homeport|homeport_m30|starport|starship|twin_star|pick_strong|state_bucket" in exc.value.detail


def test_timeline_accepts_twin_star(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = {"ok": True, "strategy": "双子星", "rows": [], "summary": {"fusedPct": 0.0}}
    monkeypatch.setattr(br, "_get_or_build_timeline", lambda s, e, **kw: (payload, None))
    out = br.backtest_timeline(start="2026-01-01", end="2026-01-31", strategy="twin_star")
    assert out["strategy"] == "双子星"


def test_timeline_accepts_harbor(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = {"ok": True, "strategy": "港湾", "rows": [], "summary": {"fusedPct": 0.0}}
    monkeypatch.setattr(br, "_get_or_build_timeline", lambda s, e, **kw: (payload, None))
    out = br.backtest_timeline(start="2026-01-01", end="2026-01-31", strategy="harbor")
    assert out["strategy"] == "港湾"


def test_timeline_accepts_homeport(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = {"ok": True, "strategy": "母港", "rows": [], "summary": {"fusedPct": 0.0}}
    monkeypatch.setattr(br, "_get_or_build_timeline", lambda s, e, **kw: (payload, None))
    out = br.backtest_timeline(start="2026-01-01", end="2026-01-31", strategy="homeport")
    assert out["strategy"] == "母港"


def test_timeline_accepts_starport(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = {"ok": True, "strategy": "星港", "rows": [], "summary": {"fusedPct": 0.0}}
    monkeypatch.setattr(br, "_get_or_build_timeline", lambda s, e, **kw: (payload, None))
    out = br.backtest_timeline(start="2026-01-01", end="2026-01-31", strategy="starport")
    assert out["strategy"] == "星港"


def test_timeline_accepts_starship(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = {"ok": True, "strategy": "星舰", "rows": [], "summary": {"fusedPct": 0.0}}
    monkeypatch.setattr(br, "_get_or_build_timeline", lambda s, e, **kw: (payload, None))
    out = br.backtest_timeline(start="2026-01-01", end="2026-01-31", strategy="starship")
    assert out["strategy"] == "星舰"


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


def test_timeline_mem_cache_ttl_and_force_drop() -> None:
    """2026-09-17 audit: the in-memory timeline cache must expire (a morning
    warmup otherwise serves pre-close rows all day) and support force-drop."""
    key = ("2026-01-01", "2026-01-31", "harbor")
    br._timeline_mem_put(key, {"ok": True}, {"ctx": 1})
    assert br._timeline_mem_get(key) == {"ok": True}
    assert br._timeline_engine_cache[key] == {"ctx": 1}

    # TTL expiry drops the timeline AND its engine ctx (heavy rebuild path).
    br._timeline_cache_at[key] = 0.0
    assert br._timeline_mem_get(key) is None
    assert key not in br._timeline_cache and key not in br._timeline_engine_cache
    assert key not in br._timeline_cache_at

    # force path used by the daily H2 shadow job.
    br._timeline_mem_put(key, {"ok": True}, {"ctx": 2})
    br._timeline_mem_drop(key)
    assert br._timeline_mem_get(key) is None and key not in br._timeline_engine_cache


def test_satellite_live_panel_route_shape(monkeypatch: pytest.MonkeyPatch) -> None:
    """OPT-222: file-backed snapshot route — panel + stale flag, no DB work."""
    panel = {"tradeDate": "2026-09-17", "decisionAvailable": True, "gateOpen": False}
    import data_sync_service.service.satellite_live as sl

    monkeypatch.setattr(sl, "load_live_panel", lambda: panel)
    monkeypatch.setattr(
        "data_sync_service.service.trade_calendar_utils.shanghai_today_iso",
        lambda: "2026-09-17",
    )
    out = br.satellite_signals_live_panel()
    assert out == {"ok": True, "panel": panel, "stale": False}

    monkeypatch.setattr(sl, "load_live_panel", lambda: {**panel, "tradeDate": "2026-09-16"})
    assert br.satellite_signals_live_panel()["stale"] is True

    monkeypatch.setattr(sl, "load_live_panel", lambda: None)
    assert br.satellite_signals_live_panel() == {"ok": True, "panel": None, "stale": False}
