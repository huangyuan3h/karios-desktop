"""Unit tests for the portfolio NAV simulator (T6 sleeve card)."""

from __future__ import annotations

import pytest

from data_sync_service.service.portfolio_nav_sim import (
    load_third_asset_cache,
    simulate_sleeve_nav,
)


def _bars(dates: list[str], closes: list[float]) -> list[dict]:
    return [{"date": d.replace("-", ""), "close": c} for d, c in zip(dates, closes, strict=False)]


def _run(
    *,
    positions_by_day: list[dict],
    close_by_ts_day: dict[str, dict[str, float]],
    calendar: list[str],
    etf_close_by_day: dict[str, float],
    repo_rate_by_day: dict[str, float],
    min_idle_pct: float = 0.0,
):
    return simulate_sleeve_nav(
        positions_by_day=positions_by_day,
        close_by_ts_day=close_by_ts_day,
        calendar=calendar,
        etf_close_by_day=etf_close_by_day,
        repo_rate_by_day=repo_rate_by_day,
        min_idle_pct=min_idle_pct,
    )


def _days(n: int, start: str = "2025-01-01") -> list[str]:
    from datetime import date, timedelta

    d0 = date.fromisoformat(start)
    return [(d0 + timedelta(days=i)).isoformat() for i in range(n)]


# 220 days: 200 flat at 100 (MA200 signal becomes available), then 20 days
# rising +1/day -> close always above the ~100 MA200.
def _bull_etf(n: int = 220) -> dict[str, float]:
    days = _days(n)
    return {
        d: float(100.0 if i < 200 else 101.0 + (i - 200))
        for i, d in enumerate(days)
    }


def _flat_then_break_etf(n: int = 220) -> dict[str, float]:
    """200 flat, then 10 up (+1/day to 110), then 10 down (-1.5/day to 95)."""
    days = _days(n)
    closes: list[float] = []
    for i in range(n):
        if i < 200:
            closes.append(100.0)
        elif i < 210:
            closes.append(101.0 + (i - 200))
        else:
            closes.append(110.0 - 1.5 * (i - 210))
    return {d: c for d, c in zip(days, closes, strict=False)}


def test_baseline_is_idle_zero():
    """With no sleeve data the NAV equals the deployed-only baseline (idle 0%)."""
    etf = _bull_etf()
    days = list(etf)[:30]
    out = _run(
        positions_by_day=[],
        close_by_ts_day={},
        calendar=days,
        etf_close_by_day=etf,
        repo_rate_by_day={},
    )
    assert out["summary"]["totalSleevePct"] == 0.0
    assert out["summary"]["totalBasePct"] == 0.0
    assert out["summary"]["deltaPct"] == 0.0


def test_idle_cash_earns_etf_when_above_ma200():
    """Fully idle book + ETF rising above MA200 -> sleeve NAV tracks the ETF."""
    etf = _bull_etf()
    days = list(etf)
    out = _run(
        positions_by_day=[],
        close_by_ts_day={},
        calendar=days,
        etf_close_by_day=etf,
        repo_rate_by_day={},
    )
    s = out["summary"]
    # Hold begins once MA200 is computable (day 200): the final 21 days of
    # +1/day moves idle cash from 100 -> 120 (+20%).
    assert s["totalSleevePct"] == pytest.approx(20.0, abs=1.0)
    assert s["totalBasePct"] == pytest.approx(0.0, abs=0.01)
    assert s["holdDays"] == 21
    assert s["avgIdlePct"] == pytest.approx(100.0, abs=0.01)


def test_break_below_ma200_cuts_to_repo():
    """ETF peaks then crashes below MA200 -> sleeve returns to repo growth."""
    etf = _flat_then_break_etf()
    days = list(etf)
    out = _run(
        positions_by_day=[],
        close_by_ts_day={},
        calendar=days,
        etf_close_by_day=etf,
        repo_rate_by_day={},
    )
    rows = out["rows"]
    holding_days = [r for r in rows if r["holding"]]
    repo_after_break = [r for r in rows[-5:] if not r["holding"]]
    assert 10 <= len(holding_days) <= 20  # rising + early-crash segment held
    assert len(repo_after_break) >= 3  # deep-crash days sit in repo
    # Rising days earn, crash days give back most of it.
    assert 0.0 < out["summary"]["totalSleevePct"] < 5.0


def test_deployed_cash_not_charged_to_sleeve():
    """A fully deployed day (idle=0) must not earn sleeve returns."""
    etf = _bull_etf()
    days = list(etf)[195:220]  # MA200 signal available in this tail
    close_by_ts_day = {"600000.SH": {d: 10.0 + i for i, d in enumerate(days)}}
    positions_by_day = [
        {
            "date": d,
            "positions": [
                {"symbol": "CN:600000", "ts_code": "600000.SH", "entry_date": days[0], "position_pct": 0.5}
            ],
        }
        for d in days
    ]
    out = _run(
        positions_by_day=positions_by_day,
        close_by_ts_day=close_by_ts_day,
        calendar=days,
        etf_close_by_day=etf,
        repo_rate_by_day={},
    )
    s = out["summary"]
    # First day is idle-only (position filled at day-0 close, pnl starts day 1).
    assert s["avgIdlePct"] == pytest.approx(52.0, abs=0.5)
    assert s["totalSleevePct"] > s["totalBasePct"]  # idle half earned ETF gains
    assert s["totalBasePct"] > 0.0  # deployed half earned the stock move


def test_ma200_fail_closed_before_200_bars():
    """Not enough ETF history -> no hold, repo-only (fail-closed)."""
    etf = {f"2025-01-{d:02d}": 100.0 for d in range(1, 21)}  # only 20 bars
    days = list(etf)
    out = _run(
        positions_by_day=[],
        close_by_ts_day={},
        calendar=days,
        etf_close_by_day=etf,
        repo_rate_by_day={},
    )
    assert out["summary"]["holdDays"] == 0
    assert all(not r["holding"] for r in out["rows"])


def test_load_cache_flattens_dates():
    cache = {
        "etfs": {"513100.SH": {"name": "x", "rows": _bars(["20240801", "20240802"], [1.4, 1.41])}},
        "repo": [{"date": "20240801", "close": 1.8}, {"date": "20240802", "close": 1.9}],
    }
    etf, repo = load_third_asset_cache(cache)
    assert etf["2024-08-01"] == 1.4
    assert repo["2024-08-02"] == 1.9


def test_min_idle_threshold_gates_engagement():
    """min_idle_pct=101 blocks the sleeve entirely (idle never exceeds 100)."""
    etf = _bull_etf()
    days = list(etf)
    out = _run(
        positions_by_day=[],
        close_by_ts_day={},
        calendar=days,
        etf_close_by_day=etf,
        repo_rate_by_day={},
        min_idle_pct=101.0,
    )
    assert out["summary"]["holdDays"] == 0
    assert out["summary"]["totalSleevePct"] == pytest.approx(0.0, abs=0.1)