"""Factor live overlay tests (no DB/network; every dependency injected).

Covers:
- frozen prefix is read-only and never mutated;
- live append merge is idempotent and ignores open/unclosed trades;
- last-40 uses the appended closed trades (not just the frozen prefix);
- a failing replay keeps yesterday's appended rows (fail-open/degraded);
- freshness is not stale when the overlay is caught up to the latest
  complete session, and stays stale when an input is missing/behind;
- generate_factor_vault prefers the live blotter over the frozen prefix.
"""

from __future__ import annotations

import importlib.util
import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest

from data_sync_service.service import factor_live as fl
from data_sync_service.service import factor_system

ROOT = Path(__file__).resolve().parents[1]


def _load_generate_vault():
    spec = importlib.util.spec_from_file_location(
        "generate_factor_vault_live", ROOT / "scripts" / "generate_factor_vault.py"
    )
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


def _write(path: Path, payload: Any) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _sgap_frozen(n: int = 39) -> list[dict[str, Any]]:
    rows = []
    for i in range(n):
        day = f"2026-01-{i + 1:02d}" if i < 28 else f"2026-02-{i - 27:02d}"
        rows.append(
            {
                "ts": f"00000{i % 10}.SZ",
                "entry": day,
                "exit": day,
                "contrib": 0.5 if i % 2 else -0.5,
                "win": bool(i % 2),
            }
        )
    return rows


def _harbor_frozen(n: int = 39) -> list[dict[str, Any]]:
    rows = []
    for i in range(n):
        day = f"2026-01-{i + 1:02d}" if i < 28 else f"2026-02-{i - 27:02d}"
        rows.append(
            {
                "ts": f"CN:00000{i % 10}",
                "entry": day,
                "exit": day,
                "contrib": 0.8,
                "win": True,
            }
        )
    return rows


def _live_payload(
    *,
    as_of: str = "2026-11-10",
    sgap_ok: bool = True,
    harbor_ok: bool = True,
    status: str = "ok",
    sgap_reason: str | None = None,
    harbor_reason: str | None = None,
) -> dict[str, Any]:
    return {
        "version": 1,
        "meta": {
            "status": status,
            "asOf": as_of,
            "expectedSession": as_of,
            "updatedAt": "2026-11-10T19:00:00+08:00",
            "reason": None if status == "ok" else "inputs missing",
            "inputs": {
                "sgap": {"ok": sgap_ok, "reason": sgap_reason},
                "harbor": {"ok": harbor_ok, "reason": harbor_reason},
            },
        },
        "appended": {"sgap": [], "harbor": []},
        "parking": {"episodes": []},
    }


def _no_daily_replay(family: str, start: str, end: str) -> dict[str, Any]:
    """Hermetic H01/H02/H03 stand-in (tests inject their own when needed)."""
    return {"ok": True, "asOf": end, "trades": []}


def _write_vault(reports: Path) -> None:
    """Minimal two-factor vault for the factor-system contract."""
    factors = [
        {
            "id": "starship_b",
            "name": "星舰B默认",
            "family": "starship",
            "source": "fixture",
            "unit": "点",
            "status": "cold",
            "badge": "冷库",
            "position": 0,
            "percentile": 2.2,
            "net": -14.79,
            "win_rate": 0.3,
            "trades": 40,
            "rolling_window": "live",
            "rolling_as_of": "2026-11-10",
            "rolling_last_trade_as_of": "2026-09-28",
            "rolling_basis": "live:sgap",
            "confirmations": 0,
            "history": [
                {"window": w, "date": d, "net": -1.0, "trades": 10, "revived": False}
                for w, d in (
                    ("OOS2", "2025-08-07"),
                    ("train", "2026-02-27"),
                    ("valid", "2026-08-07"),
                    ("holdout", "2026-09-30"),
                )
            ],
        },
        {
            "id": "harbor",
            "name": "港湾 (参考)",
            "family": "reference",
            "source": "fixture",
            "unit": "点",
            "status": "watch",
            "badge": "观察",
            "position": 0,
            "percentile": 85.9,
            "net": 11.24,
            "win_rate": 0.425,
            "trades": 40,
            "rolling_window": "live",
            "rolling_as_of": "2026-11-10",
            "rolling_last_trade_as_of": "2026-11-09",
            "rolling_basis": "live:harbor",
            "confirmations": 0,
            "history": [],
        },
    ]
    _write(
        reports / "factor_vault.json",
        {
            "meta": {"generated_at": "2026-10-03"},
            "summary": {"n_factors": 2, "n_cold": 1, "n_watch": 1, "n_revived": 0},
            "factors": factors,
        },
    )
    _write(reports / "factor_vault_history.json", {"history": []})
    _write(reports / "factor_vault_transitions.json", {"transitions": []})
    _write(reports / "sgap_decay.json", {"summary": {"date_end": "2026-09-23"}})


# ---------------------------------------------------------------------------
# Merge / append
# ---------------------------------------------------------------------------


def test_merge_is_idempotent_and_keeps_frozen_prefix_immutable(tmp_path: Path) -> None:
    frozen_path = _write(tmp_path / "h2f_trades.json", _sgap_frozen())
    before = frozen_path.read_text(encoding="utf-8")
    frozen = fl.load_frozen_sgap(frozen_path)
    new = [
        {"ts": "000123.SZ", "entry": "2026-10-09", "exit": "2026-10-13", "contrib": 2.0},
    ]
    first = fl.merge_appended(frozen, new, family="sgap")
    second = fl.merge_appended(first, new, family="sgap")
    assert len(first) == len(frozen) + 1
    assert second == first
    # The audit file is read-only input: no write-back anywhere.
    assert frozen_path.read_text(encoding="utf-8") == before
    assert len(frozen) == 39


def test_refresh_appends_only_closed_trades_after_frozen_entry(tmp_path: Path) -> None:
    frozen_sgap = _write(tmp_path / "h2f_trades.json", _sgap_frozen())
    frozen_harbor = _write(tmp_path / "h2d_blotter_long.json", _harbor_frozen())
    reports = tmp_path / "reports"
    reports.mkdir()

    def sgap_replay(start: str, end: str) -> dict[str, Any]:
        return {
            "ok": True,
            "asOf": end,
            "trades": [
                {"ts": "A", "entry": "2026-10-09", "exit": "2026-10-13", "contrib": 2.0},
                # open position -> ignored
                {"ts": "B", "entry": "2026-10-20", "exit": None, "contrib": 1.0},
                # already in the frozen prefix -> ignored
                {"ts": "000000.SZ", "entry": "2026-01-01", "exit": "2026-01-01", "contrib": 0.5},
                # exits after the watermark -> ignored
                {"ts": "C", "entry": "2026-11-05", "exit": "2026-11-20", "contrib": 3.0},
            ],
        }

    def harbor_replay(start: str, end: str) -> dict[str, Any]:
        return {
            "ok": True,
            "asOf": end,
            "etfAsOf": end,
            "trades": [
                {"ts": "CN:600000", "entry": "2026-06-02", "exit": "2026-06-10", "contrib": 1.1},
            ],
            "parking": [
                {"ts": "518880.SH", "entry": "2026-06-03", "exit": "2026-06-05", "contrib": 0.4},
            ],
        }

    kwargs = dict(
        now=datetime(2026, 11, 10, 19, 0),
        latest_session="2026-11-10",
        etf_session="2026-11-10",
        frozen_sgap_path=frozen_sgap,
        frozen_harbor_path=frozen_harbor,
        sgap_replay=sgap_replay,
        harbor_replay=harbor_replay,
        frozen_daily_prefix={},
        daily_replay=_no_daily_replay,
        session_after=lambda _d: "2026-05-30",
    )
    payload = fl.refresh_live_blotter(reports, **kwargs)
    assert payload["meta"]["status"] == "ok"
    assert payload["meta"]["asOf"] == "2026-11-10"
    assert [t["ts"] for t in payload["appended"]["sgap"]] == ["A"]
    assert [t["ts"] for t in payload["appended"]["harbor"]] == ["CN:600000"]
    assert len(payload["parking"]["episodes"]) == 1
    assert payload["meta"]["inputs"]["sgap"]["newTrades"] == 1

    # Idempotent: a second refresh on the same day appends nothing twice.
    again = fl.refresh_live_blotter(reports, **kwargs)
    assert [t["ts"] for t in again["appended"]["sgap"]] == ["A"]
    assert [t["ts"] for t in again["appended"]["harbor"]] == ["CN:600000"]
    assert len(again["parking"]["episodes"]) == 1
    # Frozen inputs still untouched.
    assert len(fl.load_frozen_sgap(frozen_sgap)) == 39


def test_failed_replay_keeps_yesterday_rows_and_marks_degraded(tmp_path: Path) -> None:
    frozen_sgap = _write(tmp_path / "h2f_trades.json", _sgap_frozen())
    frozen_harbor = _write(tmp_path / "h2d_blotter_long.json", _harbor_frozen())
    reports = tmp_path / "reports"
    reports.mkdir()

    def ok_replay(start: str, end: str) -> dict[str, Any]:
        return {
            "ok": True,
            "asOf": end,
            "trades": [{"ts": "A", "entry": "2026-10-09", "exit": "2026-10-13", "contrib": 2.0}],
            "parking": [],
        }

    base = dict(
        now=datetime(2026, 11, 10, 19, 0),
        latest_session="2026-11-10",
        etf_session="2026-11-10",
        frozen_sgap_path=frozen_sgap,
        frozen_harbor_path=frozen_harbor,
        harbor_replay=ok_replay,
        frozen_daily_prefix={},
        daily_replay=_no_daily_replay,
        session_after=lambda _d: "2026-05-30",
    )
    first = fl.refresh_live_blotter(reports, sgap_replay=ok_replay, **base)
    assert first["meta"]["status"] == "ok"

    def boom(start: str, end: str) -> dict[str, Any]:
        raise RuntimeError("db down")

    degraded = fl.refresh_live_blotter(reports, sgap_replay=boom, **base)
    assert degraded["meta"]["status"] == "degraded"
    assert "sgap" in str(degraded["meta"]["reason"])
    # Yesterday's appended rows survive a failed replay.
    assert [t["ts"] for t in degraded["appended"]["sgap"]] == ["A"]
    # And the file on disk is the degraded payload, not a deletion.
    assert json.loads((reports / fl.LIVE_FILENAME).read_text())["appended"]["sgap"]


def test_parking_episodes_use_harbor_nav_points() -> None:
    rows = [
        {
            "date": "2026-06-03",
            "pick": "GOLD",
            "pickTs": "518880.SH",
            "parkedSides": 1,
            "parkedRetPct": 1.0,
            "idlePct": 50.0,
        },
        {
            "date": "2026-06-04",
            "pick": "GOLD",
            "pickTs": "518880.SH",
            "parkedSides": 0,
            "parkedRetPct": 0.0,
            "idlePct": 50.0,
        },
        {
            "date": "2026-06-05",
            "pick": "REPO",
            "pickTs": "GC001",
            "parkedSides": 1,
            "parkedRetPct": 0.0,
            "idlePct": 50.0,
        },
    ]
    eps = fl.parking_episodes(rows)
    assert len(eps) == 1
    # 0.5*(1% - 5bp) + 0.5*(0 - 0) + 0.5*(-5bp) = 0.45 points
    assert eps[0]["contrib"] == pytest.approx(0.45, abs=1e-6)
    assert eps[0]["entry"] == "2026-06-03" and eps[0]["exit"] == "2026-06-04"
    assert eps[0]["closed"] is True


def test_parking_open_leg_is_mtm_only() -> None:
    rows = [
        {
            "date": "2026-11-11",
            "pick": "BOND10",
            "pickTs": "511260.SH",
            "parkedSides": 1,
            "parkedRetPct": 0.2,
            "idlePct": 100.0,
        }
    ]
    eps = fl.parking_episodes(rows)
    assert len(eps) == 1
    assert eps[0]["closed"] is False
    # Open legs are MTM-only: never counted as a closed trade in the window.
    assert fl.parking_as_trades(eps) == []
    assert fl.parking_as_trades([{**eps[0], "closed": True}])[0]["contrib"] == pytest.approx(
        0.15, abs=1e-6
    )


# ---------------------------------------------------------------------------
# Last-40 uses the live appends
# ---------------------------------------------------------------------------


def test_last_40_uses_appended_trades() -> None:
    from data_sync_service.service import factor_vault as fv

    frozen = _sgap_frozen(39)
    appended = [
        {"ts": "Z", "entry": "2026-10-09", "exit": "2026-10-13", "contrib": 9.0},
    ]
    merged = fl.merge_trades(frozen, appended, family="sgap")
    stats = fl.rolling_stats(
        merged, mu=fv.SGAP_MU, sigma=fv.SGAP_SIGMA, as_of="2026-11-10"
    )
    assert stats["trades"] == 40
    assert stats["net"] > 8.0
    assert stats["as_of"] == "2026-11-10"
    assert stats["last_trade_as_of"] == "2026-10-13"

    frozen_only = fl.rolling_stats(frozen, mu=fv.SGAP_MU, sigma=fv.SGAP_SIGMA)
    assert frozen_only["trades"] == 39
    assert frozen_only["net"] < stats["net"]


def test_live_override_prefers_appended_and_ignores_missing_prefix(tmp_path: Path) -> None:
    from data_sync_service.service import factor_vault as fv

    frozen_path = _write(tmp_path / "h2f_trades.json", _sgap_frozen(39))
    frozen = fl.load_frozen_sgap(frozen_path)
    live = _live_payload()
    live["appended"]["sgap"] = [
        {"ts": "Z", "entry": "2026-10-09", "exit": "2026-10-13", "contrib": 9.0}
    ]
    stats = fl.live_override_stats(
        live, family="sgap", frozen=frozen, mu=fv.SGAP_MU, sigma=fv.SGAP_SIGMA
    )
    assert stats is not None
    assert stats["as_of"] == "2026-11-10"
    assert stats["trades"] == 40
    # No frozen prefix -> no invented window.
    assert (
        fl.live_override_stats(live, family="sgap", frozen=[], mu=0.0, sigma=1.0)
        is None
    )


def test_harbor_rolling_adds_parking_episodes() -> None:
    from data_sync_service.service import factor_vault as fv

    frozen = _harbor_frozen(39)
    live = _live_payload()
    live["appended"]["harbor"] = [
        {"ts": "CN:600000", "entry": "2026-06-02", "exit": "2026-06-10", "contrib": 1.1}
    ]
    live["parking"]["episodes"] = [
        {"ts": "518880.SH", "entry": "2026-06-03", "exit": "2026-06-05", "contrib": -0.4}
    ]
    stats = fl.live_override_stats(
        live, family="harbor", frozen=frozen, mu=fv.HARBOR_MU, sigma=fv.HARBOR_SIGMA
    )
    assert stats is not None
    assert stats["trades"] == 40
    assert stats["last_trade_as_of"] == "2026-06-10"
    assert stats["net"] == pytest.approx(sum(r["contrib"] for r in frozen[-38:]) + 1.1 - 0.4)


# ---------------------------------------------------------------------------
# Freshness contract
# ---------------------------------------------------------------------------


def test_freshness_is_not_stale_when_overlay_is_caught_up(tmp_path: Path) -> None:
    _write_vault(tmp_path)
    _write(tmp_path / fl.LIVE_FILENAME, _live_payload(as_of="2026-11-10"))
    out = factor_system.build_factor_system(
        reports_dir=tmp_path, now="2026-11-11", latest_session="2026-11-10"
    )
    fresh = out["freshness"]
    assert fresh["stale"] is False
    assert fresh["tradeBlotterLastEntry"] == "2026-11-10"
    assert fresh["liveAsOf"] == "2026-11-10"
    # The old frozen generated_at (2026-10-03) must not make it stale.
    assert "2026-10-03" not in (fresh["reason"] or "")
    # recentPerformance follows the live as-of, not the frozen holdout date.
    brick = {b["id"]: b for b in out["bricks"]}["starship_b"]
    assert brick["recentPerformance"]["asOf"] == "2026-11-10"
    assert brick["recentPerformance"]["source"] == "live"
    assert brick["recentPerformance"]["window"] == "live"
    assert brick["recentPerformance"]["lastTradeAsOf"] == "2026-09-28"
    assert brick["windows"]["holdout"]["date"] == "2026-09-30"


def test_freshness_allows_same_day_lag_before_the_19h_job(tmp_path: Path) -> None:
    _write_vault(tmp_path)
    _write(tmp_path / fl.LIVE_FILENAME, _live_payload(as_of="2026-11-10"))
    out = factor_system.build_factor_system(
        reports_dir=tmp_path, now="2026-11-11", latest_session="2026-11-11"
    )
    assert out["freshness"]["stale"] is False


def test_freshness_stale_when_blotter_is_behind(tmp_path: Path) -> None:
    _write_vault(tmp_path)
    _write(tmp_path / fl.LIVE_FILENAME, _live_payload(as_of="2026-11-06"))
    out = factor_system.build_factor_system(
        reports_dir=tmp_path, now="2026-11-11", latest_session="2026-11-10"
    )
    fresh = out["freshness"]
    assert fresh["stale"] is True
    assert "实盘流水止于 2026-11-06" in (fresh["reason"] or "")


def test_freshness_same_day_grace_does_not_mask_a_dead_overlay(tmp_path: Path) -> None:
    _write_vault(tmp_path)
    _write(tmp_path / fl.LIVE_FILENAME, _live_payload(as_of="2026-10-30"))
    out = factor_system.build_factor_system(
        reports_dir=tmp_path, now="2026-11-11", latest_session="2026-11-11"
    )
    assert out["freshness"]["stale"] is True
    assert "实盘流水止于 2026-10-30" in (out["freshness"]["reason"] or "")


def test_freshness_stale_when_an_input_is_missing(tmp_path: Path) -> None:
    _write_vault(tmp_path)
    _write(
        tmp_path / fl.LIVE_FILENAME,
        _live_payload(
            as_of="2026-11-10",
            status="degraded",
            harbor_ok=False,
            harbor_reason="ETF daily ends 2026-11-06",
        ),
    )
    out = factor_system.build_factor_system(
        reports_dir=tmp_path, now="2026-11-11", latest_session="2026-11-10"
    )
    fresh = out["freshness"]
    assert fresh["stale"] is True
    assert "ETF daily ends 2026-11-06" in (fresh["reason"] or "")


def test_freshness_tolerates_a_failed_daily_family(tmp_path: Path) -> None:
    """H01/H02/H03 are additive: one failing pilot never masks a live core."""
    _write_vault(tmp_path)
    live = _live_payload(as_of="2026-11-10", status="degraded")
    live["meta"]["inputs"]["h01"] = {"ok": False, "reason": "no daily bars for 2026-11-10"}
    live["meta"]["inputs"]["h02"] = {"ok": True, "reason": None}
    live["meta"]["inputs"]["h03"] = {"ok": True, "reason": None}
    _write(tmp_path / fl.LIVE_FILENAME, live)
    out = factor_system.build_factor_system(
        reports_dir=tmp_path, now="2026-11-11", latest_session="2026-11-10"
    )
    fresh = out["freshness"]
    assert fresh["stale"] is False
    assert fresh["degradedFamilies"] == ["h01"]
    assert "h01" in (fresh["reason"] or "")
    assert "不影响核心重演" in (fresh["reason"] or "")


# ---------------------------------------------------------------------------
# generate_factor_vault prefers the live file
# ---------------------------------------------------------------------------


def test_generate_factor_vault_prefers_live_override(tmp_path: Path, monkeypatch) -> None:
    gv = _load_generate_vault()
    scratch = tmp_path / "scratch"
    _write(scratch / "h2f_trades.json", _sgap_frozen(39))
    _write(scratch / "h2d_blotter_long.json", _harbor_frozen(39))
    monkeypatch.setattr(gv, "AUDIT_SCRATCH", scratch)

    frozen_only = gv._sgap_override(None)
    assert frozen_only is not None
    assert frozen_only["starship_b"]["trades"] == 39
    assert frozen_only["starship_b"]["basis"] == "frozen:sgap"

    live = _live_payload(as_of="2026-11-10")
    live["appended"]["sgap"] = [
        {"ts": "Z", "entry": "2026-10-09", "exit": "2026-10-13", "contrib": 9.0}
    ]
    live["parking"]["episodes"] = [
        {"ts": "518880.SH", "entry": "2026-06-03", "exit": "2026-06-05", "contrib": 0.2}
    ]
    with_live = gv._sgap_override(live)
    assert with_live is not None
    assert with_live["starship_b"]["trades"] == 40
    assert with_live["starship_b"]["basis"] == "live:sgap"
    assert with_live["starship_b"]["as_of"] == "2026-11-10"

    harbor_frozen = gv._harbor_override(None)
    harbor_live = gv._harbor_override(live)
    assert harbor_frozen is not None and harbor_live is not None
    assert harbor_live["harbor"]["trades"] == 40
    assert harbor_live["harbor"]["basis"] == "live:harbor"
    # Frozen harbor prefix never records the parking leg; the live one adds it.
    assert harbor_frozen["harbor"]["net"] != harbor_live["harbor"]["net"]


# ---------------------------------------------------------------------------
# Replay adapters (frozen helpers are monkeypatched; no DB)
# ---------------------------------------------------------------------------


def test_replay_sgap_live_keeps_only_closed_fills(monkeypatch) -> None:
    from data_sync_service.service import state_bucket_track as sbt

    monkeypatch.setattr(sbt, "load_sgap_context", lambda s, e: {"px_1430": {"A": {e: 1.0}}, "cal": [e]})
    monkeypatch.setattr(
        sbt,
        "replay_sgap_from_context",
        lambda ctx, **kw: {
            "blotter": [
                {"kind": "fill", "ts": "A", "entryDate": "2026-10-01", "exitDate": "2026-10-05", "contribPct": 1.5},
                {"kind": "fill", "ts": "B", "entryDate": "2026-10-08", "exitDate": "2026-12-01", "contribPct": 2.0},
                {"kind": "open", "ts": "C", "entryDate": "2026-10-30", "exitDate": None},
                {"kind": "skip_t1", "ts": "D", "entryDate": "2026-10-30"},
            ],
            "rows": [{"date": "2026-11-10", "gateOpen": False, "gapCount": 0}],
            "openPositions": [],
            "decisionAvailable": True,
            "decisionUnavailableDays": [],
        },
    )
    out = fl.replay_sgap_live("2026-10-01", "2026-11-10")
    assert out["ok"] is True
    assert [t["ts"] for t in out["trades"]] == ["A"]
    assert out["trades"][0]["contrib"] == 1.5
    assert out["panelPrints"] == 1


def test_replay_sgap_live_reports_missing_panel(monkeypatch) -> None:
    from data_sync_service.service import state_bucket_track as sbt

    monkeypatch.setattr(sbt, "load_sgap_context", lambda s, e: {"px_1430": {}, "cal": [e]})
    monkeypatch.setattr(
        sbt,
        "replay_sgap_from_context",
        lambda ctx, **kw: {
            "blotter": [],
            "rows": [],
            "openPositions": [],
            "decisionAvailable": False,
            "decisionUnavailableDays": ["2026-11-10"],
        },
    )
    out = fl.replay_sgap_live("2026-10-01", "2026-11-10")
    assert out["ok"] is False
    assert "no 14:30 bars" in str(out["reason"])


def test_replay_harbor_live_maps_trades_and_parking(monkeypatch) -> None:
    from types import SimpleNamespace

    from data_sync_service.service import backtest_engine as be
    from data_sync_service.service import harbor
    from data_sync_service.service import portfolio_nav_sim as pns

    monkeypatch.setattr(be, "BacktestData", lambda cfg: SimpleNamespace(calendar=["2026-06-01", "2026-11-10"]))
    monkeypatch.setattr(
        be,
        "simulate",
        lambda cfg, data: SimpleNamespace(
            trades=[
                SimpleNamespace(
                    symbol="CN:600000",
                    entry_date="2026-06-02",
                    close_date="2026-06-10",
                    pnl_pct=8.0,
                    position_pct=0.1,
                ),
                SimpleNamespace(
                    symbol="CN:600001",
                    entry_date="2026-11-05",
                    close_date="2026-11-20",
                    pnl_pct=1.0,
                    position_pct=0.1,
                ),
            ],
            positions_by_day=[],
            nav_curve=[1.0, 1.1],
        ),
    )
    monkeypatch.setattr(harbor, "load_etf_closes", lambda: {"518880.SH": {"2026-11-10": 1.0}})
    monkeypatch.setattr(
        harbor,
        "build_harbor_timeline",
        lambda **kw: {
            "rows": [
                {"date": "2026-06-03", "pick": "GOLD", "pickTs": "518880.SH", "parkedSides": 1, "parkedRetPct": 1.0, "idlePct": 100.0},
                {"date": "2026-06-04", "pick": "REPO", "pickTs": "GC001", "parkedSides": 1, "parkedRetPct": 0.0, "idlePct": 100.0},
            ],
            "parkingTrades": 1,
        },
    )
    monkeypatch.setattr(pns, "engine_nav_by_day_from_run", lambda cal, nav: {"2026-11-10": 1.1})
    out = fl.replay_harbor_live("2026-06-01", "2026-11-10")
    assert out["ok"] is True
    assert [t["ts"] for t in out["trades"]] == ["CN:600000"]
    assert out["trades"][0]["contrib"] == pytest.approx(0.8)
    assert out["etfAsOf"] == "2026-11-10"
    assert len(out["parking"]) == 1


# ---------------------------------------------------------------------------
# Session helpers / input failures
# ---------------------------------------------------------------------------


def test_session_helpers_fail_open(monkeypatch) -> None:
    class Boom:
        def __enter__(self):
            raise RuntimeError("no db")

        def __exit__(self, *a):
            return False

    monkeypatch.setattr(fl, "_read_only_conn", lambda: Boom())
    assert fl.latest_complete_session() is None
    assert fl.latest_etf_session() is None
    # Weekday fallback: Fri 2026-11-06 -> Mon 2026-11-09.
    assert fl.first_session_after("2026-11-06") == "2026-11-09"
    assert fl.first_session_after("not-a-date") is None


def test_refresh_marks_missing_frozen_prefix_degraded(tmp_path: Path) -> None:
    payload = fl.refresh_live_blotter(
        tmp_path,
        now=datetime(2026, 11, 10, 19, 0),
        latest_session="2026-11-10",
        etf_session="2026-11-10",
        frozen_sgap_path=tmp_path / "missing_sgap.json",
        frozen_harbor_path=tmp_path / "missing_harbor.json",
        frozen_daily_prefix={},
        daily_replay=_no_daily_replay,
    )
    assert payload["meta"]["status"] == "degraded"
    assert "frozen prefix missing" in str(payload["meta"]["reason"])
    assert payload["appended"]["sgap"] == []
    assert payload["appended"]["harbor"] == []
    assert payload["appended"]["h01"] == []


def test_refresh_marks_etf_lag_degraded(tmp_path: Path) -> None:
    frozen_sgap = _write(tmp_path / "h2f_trades.json", _sgap_frozen())
    frozen_harbor = _write(tmp_path / "h2d_blotter_long.json", _harbor_frozen())
    payload = fl.refresh_live_blotter(
        tmp_path,
        now=datetime(2026, 11, 10, 19, 0),
        latest_session="2026-11-10",
        etf_session="2026-11-06",
        frozen_sgap_path=frozen_sgap,
        frozen_harbor_path=frozen_harbor,
        sgap_replay=lambda s, e: {"ok": True, "trades": [], "asOf": e},
        frozen_daily_prefix={},
        daily_replay=_no_daily_replay,
        session_after=lambda _d: "2026-05-30",
    )
    assert payload["meta"]["status"] == "degraded"
    assert "ETF daily ends 2026-11-06" in str(payload["meta"]["reason"])


def test_refresh_handles_no_session_after_prefix(tmp_path: Path) -> None:
    frozen_sgap = _write(tmp_path / "h2f_trades.json", _sgap_frozen())
    frozen_harbor = _write(tmp_path / "h2d_blotter_long.json", _harbor_frozen())
    payload = fl.refresh_live_blotter(
        tmp_path,
        now=datetime(2026, 11, 10, 19, 0),
        latest_session="2026-11-10",
        etf_session="2026-11-10",
        frozen_sgap_path=frozen_sgap,
        frozen_harbor_path=frozen_harbor,
        sgap_replay=lambda s, e: pytest.fail("must not replay before the frozen exit"),
        harbor_replay=lambda s, e: pytest.fail("must not replay before the frozen exit"),
        daily_replay=lambda f, s, e: pytest.fail("must not replay before the frozen exit"),
        frozen_daily_prefix={},
        session_after=lambda _d: "2027-01-01",
    )
    assert payload["meta"]["status"] == "ok"
    assert payload["meta"]["inputs"]["sgap"]["newTrades"] == 0
    assert payload["meta"]["inputs"]["harbor"]["newTrades"] == 0


def test_refresh_unknown_watermark_keeps_old_rows(tmp_path: Path, monkeypatch) -> None:
    frozen_sgap = _write(tmp_path / "h2f_trades.json", _sgap_frozen())
    frozen_harbor = _write(tmp_path / "h2d_blotter_long.json", _harbor_frozen())
    reports = tmp_path / "reports"
    reports.mkdir()
    _write(reports / fl.LIVE_FILENAME, _live_payload())
    monkeypatch.setattr(fl, "latest_complete_session", lambda: None)
    monkeypatch.setattr(fl, "latest_etf_session", lambda: None)
    payload = fl.refresh_live_blotter(
        reports,
        now=datetime(2026, 11, 10, 19, 0),
        latest_session=None,
        etf_session=None,
        frozen_sgap_path=frozen_sgap,
        frozen_harbor_path=frozen_harbor,
        sgap_replay=lambda s, e: pytest.fail("no watermark -> no replay"),
        harbor_replay=lambda s, e: pytest.fail("no watermark -> no replay"),
        daily_replay=lambda f, s, e: pytest.fail("no watermark -> no replay"),
        frozen_daily_prefix={},
        session_after=lambda _d: "2026-05-30",
    )
    assert payload["meta"]["status"] == "degraded"
    assert "latest complete trading session unknown" in str(payload["meta"]["reason"])
    # Yesterday's payload is preserved (only the metadata degrades).
    assert payload["appended"]["sgap"] == []
    assert payload["appended"]["harbor"] == []
    assert payload["appended"]["h02"] == []
