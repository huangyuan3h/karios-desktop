"""Coverage for pick_strong_track uncovered pure paths (M30 gap-closure).

Exercises the real timeline builders with synthetic calendars:
STOCK vs REPO picks, MA gate, trail exits, exits/market labels,
_pair_nav_to_calendar, _circuit_flags_by_day, twin-star blending
(opportunity/fixed, legacy rows, sim curves) and fetch_etf_closes.
"""

from __future__ import annotations

import copy
from types import SimpleNamespace

import pytest

from data_sync_service.service import pick_strong_track as pst
from data_sync_service.service.pick_strong_track import (
    _circuit_flags_by_day,
    _pair_nav_to_calendar,
    build_mom_compare_timeline,
    build_twin_star_timeline,
)


def _with_multi_ts(mapping, fn):
    old = pst.MULTI_TS
    pst.MULTI_TS = dict(mapping)
    try:
        return fn()
    finally:
        pst.MULTI_TS = old


def _days(n, prefix="D"):
    return [f"{prefix}{i:03d}" for i in range(n)]


def test_fetch_etf_closes_reads_daily(monkeypatch) -> None:
    rows = {"518880.SH": [("2026-01-02", 10.0), ("2026-01-03", 11.0)]}

    class _Cur:
        def __init__(self):
            self.last_ts = None

        def execute(self, sql, params):
            self.last_ts = params[0]

        def fetchall(self):
            return rows.get(self.last_ts, [])

    class _Conn:
        def __init__(self):
            self.cur = _Cur()

        def cursor(self):
            return self.cur

        def close(self):
            return None

    conn = _Conn()
    monkeypatch.setattr(
        "data_sync_service.service.pick_strong_track.psycopg.connect", lambda url: conn
    )
    monkeypatch.setattr(
        "data_sync_service.service.pick_strong_track.get_settings",
        lambda: SimpleNamespace(database_url="postgresql://x"),
    )

    # Only GOLD queried in this mapping
    def run():
        return pst.fetch_etf_closes()

    out = _with_multi_ts({"GOLD": "518880.SH"}, run)
    assert out["GOLD"] == {"2026-01-02": 10.0, "2026-01-03": 11.0}


def test_mom_compare_stock_pick_and_market_labels() -> None:
    # 8-day calendar, lookback 3: stock AAA rises steadily, ETF too thin -> STOCK
    cal = [f"2026-03-{d:02d}" for d in range(1, 10)]
    aaa = {d: 10.0 + i for i, d in enumerate(cal)}
    etf = {"K1": {cal[-1]: 10.0}}  # prev missing -> no ETF candidate
    positions = [
        {
            "date": cal[-2],
            "positions": [{"ts_code": "600001.SH", "symbol": "AAA", "entry_date": cal[0]}],
        },
        {
            "date": cal[-1],
            "positions": [{"ts_code": "600001.SH", "symbol": "AAA", "entry_date": cal[0]}],
        },
    ]
    close_by = {"600001.SH": aaa}

    def run():
        return build_mom_compare_timeline(
            calendar=cal,
            positions_by_day=copy.deepcopy(positions),
            close_by_ts_day=copy.deepcopy(close_by),
            etf_close=copy.deepcopy(etf),
            lookback=3,
            ma_window=3,
            trail_pct=0,
        )

    out = _with_multi_ts({"K1": "K1.TS"}, run)
    last = out["rows"][-1]
    assert last["pick"] == "STOCK"
    assert last["pickTs"] == "STOCK_BASKET"
    assert last["stockMarket"] == "A股"
    assert last["positions"] == 1
    assert last["deployedPct"] == 10.0
    assert out["summary"]["fusedPct"] != 0.0


def test_mom_compare_repo_when_no_candidates() -> None:
    cal = [f"2026-04-{d:02d}" for d in range(1, 8)]
    etf = {"K1": {cal[-1]: 10.0}}

    def run():
        return build_mom_compare_timeline(
            calendar=cal,
            positions_by_day=[],
            close_by_ts_day={},
            etf_close=copy.deepcopy(etf),
            lookback=3,
            ma_window=3,
            trail_pct=0,
        )

    out = _with_multi_ts({"K1": "K1.TS"}, run)
    assert all(r["pick"] == "REPO" for r in out["rows"])
    assert all(r["pickTs"] == "GC001" for r in out["rows"])
    assert out["rows"][-1]["stockMarket"] == "空仓"


def test_mom_compare_etf_below_ma_falls_to_repo() -> None:
    # ETF closes declining: prev below MA -> excluded -> REPO
    cal = [f"2026-05-{d:02d}" for d in range(1, 10)]
    closes = [20.0, 19.0, 18.0, 17.0, 16.0, 15.0, 14.0, 13.0, 12.0]
    etf = {"K1": dict(zip(cal, closes, strict=True))}

    def run():
        return build_mom_compare_timeline(
            calendar=cal,
            positions_by_day=[],
            close_by_ts_day={},
            etf_close=copy.deepcopy(etf),
            lookback=3,
            ma_window=5,
            trail_pct=0,
        )

    out = _with_multi_ts({"K1": "K1.TS"}, run)
    assert out["rows"][-1]["pick"] == "REPO"


def test_mom_compare_trail_exit_turns_repo() -> None:
    # Flat 10 x6, spike 20 x2, then drop to 17 (15% < peak*0.92) with mom still high
    cal = [f"T{i:03d}" for i in range(10)]
    closes = [10.0] * 6 + [20.0, 20.0, 17.0, 17.0]
    etf = {"K1": dict(zip(cal, closes, strict=True))}

    def run():
        return build_mom_compare_timeline(
            calendar=cal,
            positions_by_day=[],
            close_by_ts_day={},
            etf_close=copy.deepcopy(etf),
            lookback=5,
            ma_window=5,
            trail_pct=8.0,
        )

    out = _with_multi_ts({"K1": "K1.TS"}, run)
    picks = [r["pick"] for r in out["rows"]]
    # At least one trail exit happened and the drop day is REPO
    assert out["trailExits"] >= 1
    assert "REPO" in picks


def test_mom_compare_exits_and_hk_market() -> None:
    cal = [f"2026-06-{d:02d}" for d in range(1, 10)]
    hk_closes = {d: 10.0 + i * 0.5 for i, d in enumerate(cal)}
    etf = {"K1": {cal[-1]: 10.0}}
    positions = [
        {
            "date": cal[-2],
            "positions": [
                {"ts_code": "HK:09988", "symbol": "BABA", "entry_date": cal[0]},
                {"ts_code": "600002.SH", "symbol": "BBB", "entry_date": cal[0]},
                # entry today -> excluded (day <= entry)
                {"ts_code": "600003.SH", "symbol": "CCC", "entry_date": cal[-1]},
            ],
        },
        {
            "date": cal[-1],
            "positions": [{"ts_code": "HK:09988", "symbol": "BABA", "entry_date": cal[0]}],
        },
    ]
    close_by = {"HK:09988": hk_closes, "600002.SH": hk_closes}

    def run():
        return build_mom_compare_timeline(
            calendar=cal,
            positions_by_day=copy.deepcopy(positions),
            close_by_ts_day=copy.deepcopy(close_by),
            etf_close=copy.deepcopy(etf),
            lookback=3,
            ma_window=3,
            trail_pct=0,
        )

    out = _with_multi_ts({"K1": "K1.TS"}, run)
    last = out["rows"][-1]
    assert last["pick"] == "STOCK"
    # BBB + CCC sold between prev and cur snaps (CCC held one day then sold)
    assert "BBB" in last["exits"]
    assert last["exitsCount"] == 2


def test_pair_nav_to_calendar_cases() -> None:
    assert _pair_nav_to_calendar(None, ["a"], ["a"]) is None
    assert _pair_nav_to_calendar([1.0], None, ["a"]) is None
    assert _pair_nav_to_calendar([], ["a"], ["a"]) is None
    # Forward-fill missing days + terminal forced-close binding
    out = _pair_nav_to_calendar([1.0, 1.1, 1.2], ["d1", "d2"], ["d1", "d2", "d3"])
    assert out == pytest.approx([1.0, 1.2, 1.2])
    # Normal mapping carries last value
    out2 = _pair_nav_to_calendar([1.0, 2.0], ["d1", "d2"], ["d0", "d1", "d2"])
    assert out2[0] == pytest.approx(1.0)
    assert out2[1] == pytest.approx(1.0)
    assert out2[2] == pytest.approx(2.0)


def test_circuit_flags_by_day_cases() -> None:
    t1 = SimpleNamespace(close_date="2026-03-01", pnl_pct=-10.0)
    t2 = SimpleNamespace(close_date="2026-03-02", pnl_pct=-10.0)
    t3 = SimpleNamespace(close_date="2026-03-03", pnl_pct=-10.0)
    cal = ["2026-03-03", "2026-03-10"]
    out = _circuit_flags_by_day([t1, t2, t3], cal)
    assert out["2026-03-03"] is True
    # Too few trades -> off
    out2 = _circuit_flags_by_day([t1], cal)
    assert out2["2026-03-03"] is False
    # Bad date -> False, never raises
    assert _circuit_flags_by_day([t1, t2, t3], ["bad-date"]) == {"bad-date": False}


def _core_rows(navs):
    rows = []
    for i, nav in enumerate(navs):
        rows.append(
            {
                "date": f"2026-07-{i + 1:02d}",
                "navSingle": nav,
                "pick": "STOCK" if i % 2 == 0 else "REPO",
            }
        )
    return rows


def test_twin_star_opportunity_blend() -> None:
    core = _core_rows([1.0, 1.1, 1.21])
    sat = [
        {"date": "2026-07-01", "satNav": 1.0, "satPositions": 0, "satActive": False},
        {"date": "2026-07-02", "satNav": 1.1, "satPositions": 2, "satActive": True},
        {"date": "2026-07-03", "satNav": 1.21, "satPositions": 0, "satActive": False},
    ]
    out = build_twin_star_timeline(
        core_rows=core,
        core_summary={"fusedPct": 21.0, "basePct": 5.0},
        sat_rows=sat,
        sat_blotter=[{"d": 1}],
    )
    assert out["ok"] is True
    assert len(out["rows"]) == 3
    assert out["summary"]["satActiveDays"] == 1
    assert out["blotter"] == [{"d": 1}]


def test_twin_star_fixed_blend_and_legacy_rows() -> None:
    core = _core_rows([1.0, 1.1])
    # Legacy rows without satActive: positions>0 means active
    sat = [
        {"date": "2026-07-01", "satNav": 1.0, "satPositions": 0},
        {"date": "2026-07-02", "satNav": 1.2, "satPositions": 1},
    ]
    out = build_twin_star_timeline(
        core_rows=core,
        core_summary={"fusedPct": 10.0, "basePct": 2.0},
        sat_rows=sat,
        opportunity=False,
        core_weight=0.5,
        sat_weight=0.5,
    )
    assert out["rows"][1]["satActive"] is True
    assert out["mode"] == "opportunity_twin_star"


def test_twin_star_missing_sat_and_sim_curve() -> None:
    core = _core_rows([1.0, 1.05, 1.1])
    sat = [{"date": "2026-07-02", "satNav": 1.0, "satPositions": 1, "satActive": True}]
    out = build_twin_star_timeline(
        core_rows=core,
        core_summary={"fusedPct": 10.0, "basePct": 1.0},
        sat_rows=sat,
        sim_nav_cn=[1.0, 1.02, 1.03],
        sim_cal_cn=["2026-07-01", "2026-07-02", "2026-07-03"],
        cn_trades=[SimpleNamespace(close_date="2026-07-01", pnl_pct=-30.0)] * 3,
        sentiment_by_day={"2026-07-02": "hot"},
        flow_by_day={"2026-07-02": {"x": 1.0}},
    )
    # First day has no sat row yet -> sat fields None, circuit present
    assert out["rows"][0]["satNav"] is None
    assert out["rows"][1]["sentiment"] == "hot"
    assert out["summary"]["simPct"] is not None
