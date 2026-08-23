"""TIP-013 Phase A — signal IC / RankIC / ICIR / stratified + decay.

Uses BacktestData (watchlist_score_daily + RS ranks + daily closes) to
measure predictive power without new DB sync. Watchlist-scale IC is
trend-only (N ~ 5000), not statistically decisive — results carry sample
size warnings per spec.
"""
from __future__ import annotations

import math
from typing import Any

from data_sync_service.service.backtest_engine import BacktestConfig, BacktestData


def _spearman(x: list[float], y: list[float]) -> float | None:
    n = len(x)
    if n < 10:
        return None
    # rank (average for ties)
    def rank_vals(v: list[float]) -> list[float]:
        sorted_idx = sorted(range(n), key=lambda i: v[i])
        ranks = [0.0] * n
        i = 0
        while i < n:
            j = i
            while j + 1 < n and v[sorted_idx[j + 1]] == v[sorted_idx[i]]:
                j += 1
            avg = (i + j) / 2 + 1
            for k in range(i, j + 1):
                ranks[sorted_idx[k]] = avg
            i = j + 1
        return ranks

    rx = rank_vals(x)
    ry = rank_vals(y)
    mx = sum(rx) / n
    my = sum(ry) / n
    num = sum((rx[i] - mx) * (ry[i] - my) for i in range(n))
    den_x = math.sqrt(sum((rx[i] - mx) ** 2 for i in range(n)))
    den_y = math.sqrt(sum((ry[i] - my) ** 2 for i in range(n)))
    if den_x == 0 or den_y == 0:
        return None
    return num / (den_x * den_y)


def _future_return(data: BacktestData, ts_code: str, day: str, horizon: int) -> float | None:
    """N trading-day forward return using data.calendar."""
    try:
        idx = data.calendar.index(day)
    except ValueError:
        return None
    if idx + horizon >= len(data.calendar):
        return None
    future_day = data.calendar[idx + horizon]
    closes = data.close_by_ts_day.get(ts_code)
    if not closes:
        return None
    c0 = closes.get(day)
    c1 = closes.get(future_day)
    if c0 is None or c1 is None or c0 <= 0:
        return None
    # adjust for splits? close_by_ts_day is qfq-adjusted (2026-08-11), so raw ratio is valid
    return (c1 / c0 - 1.0) * 100.0


def _mom_20(data: BacktestData, ts_code: str, day: str) -> float | None:
    try:
        idx = data.calendar.index(day)
    except ValueError:
        return None
    if idx < 20:
        return None
    prev_day = data.calendar[idx - 20]
    closes = data.close_by_ts_day.get(ts_code)
    if not closes:
        return None
    c0 = closes.get(prev_day)
    c1 = closes.get(day)
    if c0 is None or c1 is None or c0 <= 0:
        return None
    return (c1 / c0 - 1.0) * 100.0


def _vol_20(data: BacktestData, ts_code: str, day: str) -> float | None:
    try:
        idx = data.calendar.index(day)
    except ValueError:
        return None
    if idx < 20:
        return None
    closes = data.close_by_ts_day.get(ts_code)
    if not closes:
        return None
    rets: list[float] = []
    for k in range(idx - 20 + 1, idx + 1):
        d0 = data.calendar[k - 1]
        d1 = data.calendar[k]
        c0 = closes.get(d0)
        c1 = closes.get(d1)
        if c0 is None or c1 is None or c0 <= 0:
            return None
        rets.append((c1 / c0 - 1.0) * 100.0)
    if len(rets) < 20:
        return None
    mean = sum(rets) / len(rets)
    var = sum((r - mean) ** 2 for r in rets) / (len(rets) - 1) if len(rets) > 1 else 0
    return math.sqrt(var) if var >= 0 else None


def _dd_60(data: BacktestData, ts_code: str, day: str) -> float | None:
    try:
        idx = data.calendar.index(day)
    except ValueError:
        return None
    if idx < 60:
        return None
    closes = data.close_by_ts_day.get(ts_code)
    if not closes:
        return None
    c_now = closes.get(day)
    if c_now is None or c_now <= 0:
        return None
    window = data.calendar[idx - 60 : idx + 1]
    highs = [closes.get(d) for d in window]
    highs = [h for h in highs if h is not None and h > 0]
    if not highs:
        return None
    mx = max(highs)
    if mx <= 0:
        return None
    return (c_now / mx - 1.0) * 100.0  # negative = drawdown


def compute_signal_ic(
    data: BacktestData,
    signal_getter,  # (day, ts_code) -> float | None
    horizons: list[int] | None = None,
) -> dict[int, dict[str, Any]]:
    """Per-horizon cross-sectional RankIC time series."""
    if horizons is None:
        horizons = [1, 3, 5, 10]
    out: dict[int, dict[str, Any]] = {}
    for h in horizons:
        ics: list[float] = []
        ns: list[int] = []
        for day in data.calendar:
            xs: list[float] = []
            ys: list[float] = []
            for ts in data.ts_codes:
                sig = signal_getter(day, ts)
                if sig is None or not isinstance(sig, (int, float)) or math.isnan(sig):
                    continue
                ret = _future_return(data, ts, day, h)
                if ret is None or math.isnan(ret):
                    continue
                xs.append(float(sig))
                ys.append(float(ret))
            ic = _spearman(xs, ys)
            if ic is not None:
                ics.append(ic)
                ns.append(len(xs))
        if not ics:
            out[h] = {"mean_ic": None, "icir": None, "hit_rate": None, "n_days": 0, "avg_n": 0, "ics": []}
            continue
        mean_ic = sum(ics) / len(ics)
        # sample std
        if len(ics) > 1:
            var = sum((v - mean_ic) ** 2 for v in ics) / (len(ics) - 1)
            std = math.sqrt(var)
            icir = mean_ic / std if std != 0 else None
        else:
            icir = None
        hit_rate = sum(1 for v in ics if v > 0) / len(ics)
        out[h] = {
            "mean_ic": mean_ic,
            "icir": icir,
            "hit_rate": hit_rate,
            "n_days": len(ics),
            "avg_n": sum(ns) / len(ns) if ns else 0,
            "ics": ics,
        }
    return out


def stratified_returns(
    data: BacktestData,
    signal_getter,
    horizon: int = 5,
    buckets: list[tuple[str, float, float]] | None = None,
) -> dict[str, dict[str, Any]]:
    """Bucket future returns by signal quantile. Buckets = [(label, lo, hi)]."""
    if buckets is None:
        buckets = [
            (">=90", 90, 1000),
            ("85-90", 85, 90),
            ("80-85", 80, 85),
            ("70-80", 70, 80),
            ("<70", -1000, 70),
        ]
    bucket_rets: dict[str, list[float]] = {label: [] for label, _, _ in buckets}
    for day in data.calendar:
        for ts in data.ts_codes:
            sig = signal_getter(day, ts)
            if sig is None:
                continue
            ret = _future_return(data, ts, day, horizon)
            if ret is None:
                continue
            for label, lo, hi in buckets:
                if lo <= sig < hi or (label == ">=90" and sig >= 90):
                    # >=90 bucket upper is inclusive
                    if label == ">=90" and sig >= 90:
                        bucket_rets[label].append(ret)
                        break
                    elif lo <= sig < hi:
                        bucket_rets[label].append(ret)
                        break
                    # fallback for <70
                    if label == "<70" and sig < 70:
                        bucket_rets[label].append(ret)
                        break
    out: dict[str, dict[str, Any]] = {}
    for label, rets in bucket_rets.items():
        if not rets:
            out[label] = {"n": 0, "mean_ret": None, "win_rate": None}
        else:
            mean_ret = sum(rets) / len(rets)
            win_rate = sum(1 for r in rets if r > 0) / len(rets)
            out[label] = {"n": len(rets), "mean_ret": mean_ret, "win_rate": win_rate}
    return out


def analyze_signals(
    start_date: str,
    end_date: str,
    signals: list[str] | None = None,
    horizons: list[int] | None = None,
) -> dict[str, Any]:
    """Entry for API / scripts. Signals: score, rs, amount."""
    if signals is None:
        signals = ["score", "rs", "amount"]
    if horizons is None:
        horizons = [1, 3, 5, 10]
    # Enable RS and liquidity so BacktestData loads those signals
    # (defaults disable them; S-3 uses 0.5 / 0.7)
    cfg = BacktestConfig(start_date=start_date, end_date=end_date, rs_rank_min=0.5, min_avg_amount=0.7)
    data = BacktestData(cfg)

    # warn if thin
    n_days = len(data.calendar)
    n_ts = len(data.ts_codes)

    results: dict[str, Any] = {
        "window": {"start": start_date, "end": end_date, "n_days": n_days, "n_ts": n_ts},
        "signals": {},
        "warnings": [],
    }
    if n_days < 30 or n_ts < 100:
        results["warnings"].append(f"thin sample N_days={n_days} N_ts={n_ts} — IC is trend-only, N<100 not decisive (TIP-013 spec)")
    # signal getters
    def make_getter(name: str):
        if name == "score":
            return lambda day, ts: data.scores_by_day.get(day, {}).get(f"CN:{ts.split('.')[0]}")
        if name == "rs":
            return lambda day, ts: data.rs_rank_by_day.get(day, {}).get(ts)
        if name == "amount":
            # log amount to reduce skew, use raw avg_amount
            return lambda day, ts: data.avg_amount_by_day.get(day, {}).get(ts)
        if name == "flow5d":
            # flow5d is per-industry, map via industry_by_ts
            return lambda day, ts: data.flow5d_by_day.get(day, {}).get(data.industry_by_ts.get(ts, "")) if hasattr(data, "flow5d_by_day") else None
        if name == "mom20":
            return lambda day, ts: _mom_20(data, ts, day)
        if name == "vol20":
            return lambda day, ts: _vol_20(data, ts, day)
        if name == "dd60":
            return lambda day, ts: _dd_60(data, ts, day)
        return lambda day, ts: None

    for sig in signals:
        getter = make_getter(sig)
        ic_by_h = compute_signal_ic(data, getter, horizons)
        strat = stratified_returns(data, getter, horizon=5) if sig == "score" else {}
        results["signals"][sig] = {"ic": ic_by_h, "stratified_5d": strat}

    # also compute decay table for score
    if "score" in results["signals"]:
        decay = {h: results["signals"]["score"]["ic"][h] for h in horizons}
        results["signals"]["score"]["decay"] = decay

    return results
