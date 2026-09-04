"""In-memory tests for factor_validation math (no DB): forward returns,
momentum/volatility/drawdown factors, IC time series, stratification."""

from __future__ import annotations

import math
from types import SimpleNamespace

from data_sync_service.service.factor_validation import (
    _dd_60,
    _future_return,
    _mom_20,
    _vol_20,
    compute_signal_ic,
    stratified_returns,
)

NDAYS = 100


def _cal() -> list[str]:
    return [f"2026-{m:02d}-{d:02d}" for m in (1, 2, 3, 4) for d in range(1, 26)][:NDAYS]


def _data(n_names: int = 15, drift: float = 0.001):
    cal = _cal()
    close_by_ts_day: dict[str, dict[str, float]] = {}
    for i in range(n_names):
        base = 10.0 + i
        close_by_ts_day[f"TS{i:03d}"] = {
            day: base * (1 + drift * (k + i)) for k, day in enumerate(cal)
        }
    return SimpleNamespace(
        calendar=cal,
        close_by_ts_day=close_by_ts_day,
        ts_codes=sorted(close_by_ts_day),
    )


def test_future_return_edges():
    data = _data()
    day = data.calendar[10]
    assert _future_return(data, "TS000", day, 5) == (
        data.close_by_ts_day["TS000"][data.calendar[15]]
        / data.close_by_ts_day["TS000"][day]
        - 1.0
    ) * 100.0
    assert _future_return(data, "TS000", "2099-01-01", 5) is None  # unknown day
    assert _future_return(data, "TS000", data.calendar[-1], 5) is None  # overflow
    assert _future_return(data, "NOPE", day, 5) is None  # unknown symbol
    data.close_by_ts_day["TS000"][day] = 0.0
    assert _future_return(data, "TS000", day, 5) is None  # zero base


def test_mom_20_edges():
    data = _data()
    assert _mom_20(data, "TS001", data.calendar[5]) is None  # warmup
    assert _mom_20(data, "TS001", "2099-01-01") is None
    assert _mom_20(data, "NOPE", data.calendar[50]) is None
    got = _mom_20(data, "TS001", data.calendar[50])
    assert got is not None and math.isfinite(got)


def test_vol_20_edges():
    data = _data()
    assert _vol_20(data, "TS001", data.calendar[5]) is None
    assert _vol_20(data, "NOPE", data.calendar[50]) is None
    got = _vol_20(data, "TS001", data.calendar[50])
    assert got is not None and got >= 0
    # Gap in closes aborts the window.
    del data.close_by_ts_day["TS001"][data.calendar[40]]
    assert _vol_20(data, "TS001", data.calendar[50]) is None


def test_dd_60_edges():
    data = _data()
    assert _dd_60(data, "TS001", data.calendar[5]) is None
    assert _dd_60(data, "NOPE", data.calendar[80]) is None
    got = _dd_60(data, "TS001", data.calendar[80])
    assert got is not None and got <= 0  # rising closes -> at par
    data.close_by_ts_day["TS001"][data.calendar[80]] = 0.0
    assert _dd_60(data, "TS001", data.calendar[80]) is None


def test_compute_signal_ic_paths():
    data = _data()
    # All-None getter -> empty series path.
    out = compute_signal_ic(data, lambda day, ts: None, horizons=[5])
    assert out[5]["mean_ic"] is None and out[5]["n_days"] == 0
    # Perfect rank signal -> IC -1 here: higher level means lower forward
    # return in this rising synthetic (level-chasing mean-reverts).
    out = compute_signal_ic(
        data,
        lambda day, ts: float(data.close_by_ts_day[ts][day]),
        horizons=[5],
    )
    assert out[5]["n_days"] > 0
    assert out[5]["mean_ic"] is not None and out[5]["mean_ic"] < -0.9
    assert out[5]["hit_rate"] == 0.0
    # Constant IC series has zero variance -> icir None.
    assert out[5]["icir"] is None
    # Regime-flipping signal -> varying IC series -> icir defined.
    mid = data.calendar[50]
    out = compute_signal_ic(
        data,
        lambda day, ts: float(data.close_by_ts_day[ts][day])
        * (1.0 if day < mid else -1.0),
        horizons=[5],
    )
    assert out[5]["icir"] is not None
    # NaN / non-numeric signals are skipped.
    out = compute_signal_ic(
        data,
        lambda day, ts: float("nan") if ts == "TS000" else 1.0,
        horizons=[5],
    )
    assert out[5]["n_days"] >= 0


def test_stratified_paths():
    data = _data()
    out = stratified_returns(data, lambda day, ts: 95.0, horizon=5)
    assert out[">=90"]["n"] > 0 and out["<70"]["n"] == 0
    assert out["<70"]["mean_ret"] is None
    out = stratified_returns(data, lambda day, ts: 10.0, horizon=5)
    assert out["<70"]["n"] > 0 and out[">=90"]["n"] == 0
    out = stratified_returns(data, lambda day, ts: None, horizon=5)
    assert all(v["n"] == 0 for v in out.values())
    out = stratified_returns(
        data, lambda day, ts: 87.0, horizon=5, buckets=[("mid", 80, 90)]
    )
    assert out["mid"]["n"] > 0 and out["mid"]["win_rate"] is not None
