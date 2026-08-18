"""Signal pool · P10/P9 correlation gate — new-factor vs RS rank overlap check.

Planned-doc §3.3 信息独立性: a new factor must have per-window correlation
with the existing 20d RS rank < 0.5, else it is a re-make of RS (collinear =
overfit). This script measures that BEFORE any engine experiment:

- P10: close / 250d-high proximity (continuous 52-week-high distance)
- P9a: 120d cross-sectional momentum (skip last 20d: ret from 20d ago to 120d ago)
- P9b: 250d cross-sectional momentum (skip last 20d)
- P12-ref: ret120/vol60 (vol-adjusted momentum — already rejected; sanity)

Output per window: mean cross-sectional Pearson |r| between the factor and
the 20d RS rank, over (a) the whole market and (b) the score>=65 candidate
pool. Verdict rule (planned-doc §3.3): any window mean |r| >= 0.5 → factor
collinear with RS → do not run the engine experiment (or only run it with
the collinearity documented).

Usage:
  PYTHONPATH=src python3 scripts/signal_p10_p9_correlation.py [--windows OOS2,train,valid]
"""

from __future__ import annotations

import argparse
import json
import math
import sys

from data_sync_service.db import get_connection

WINDOWS = {
    "OOS2": ("2024-08-01", "2025-08-01"),
    "train": ("2025-08-01", "2026-02-01"),
    "valid": ("2026-03-01", "2026-08-07"),
}

FACTOR_NAMES = ("p10_52w", "p9_mom120_skip20", "p9_mom250_skip20", "p12_ref")

SCORE_TABLE = "watchlist_score_daily"


def _load_bars(start: str, end: str) -> tuple[dict[str, list[tuple[str, float]]], dict[str, dict[str, float]]]:
    """closes_by_ts (with lookback) + per-day close maps for the window."""
    from datetime import timedelta

    start_early = max(
        __import__("datetime").date.fromisoformat(start) - timedelta(days=400),
        __import__("datetime").date(1998, 1, 1),
    ).isoformat()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT ts_code, trade_date, close FROM daily
                WHERE trade_date >= %s AND trade_date <= %s AND close > 0
                ORDER BY trade_date
                """,
                (start_early, end),
            )
            raw = cur.fetchall()
    series: dict[str, list[tuple[str, float]]] = {}
    by_day: dict[str, dict[str, float]] = {}
    for ts, d, c in raw:
        ds = str(d)
        series.setdefault(ts, []).append((ds, float(c)))
        if ds >= start:
            by_day.setdefault(ds, {})[ts] = float(c)
    return series, by_day


def _load_scores(start: str, end: str) -> dict[str, dict[str, str]]:
    """{day: {symbol: ts_code}} for score>=65 CN symbols (candidate pool)."""
    prefix = "CN:"
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT trade_date, symbol FROM {SCORE_TABLE}
                WHERE trade_date >= %s AND trade_date <= %s AND score >= 65
                ORDER BY trade_date
                """,
                (start, end),
            )
            raw = cur.fetchall()
    out: dict[str, dict[str, str]] = {}
    for d, sym in raw:
        sym = str(sym or "").upper()
        if not sym.startswith(prefix):
            continue
        ds = d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d)
        code = sym[len(prefix):]
        ts = code + ".SH" if code.startswith(("6", "9")) else code + ".SZ"
        out.setdefault(ds, {})[sym] = ts
    return out


def _pearson(xs: list[float], ys: list[float]) -> float:
    n = len(xs)
    if n < 30:
        return float("nan")
    mx, my = sum(xs) / n, sum(ys) / n
    cov = sum((x - mx) * (y - my) for x, y in zip(xs, ys, strict=False))
    vx = sum((x - mx) ** 2 for x in xs)
    vy = sum((y - my) ** 2 for y in ys)
    if vx <= 0 or vy <= 0:
        return float("nan")
    return cov / math.sqrt(vx * vy)


def factors_for(day: str, ts: str, series: dict[str, list[tuple[str, float]]]) -> dict[str, float]:
    """Compute all candidate factors as-of ``day`` (None when insufficient bars)."""
    closes = series.get(ts)
    if not closes:
        return {}
    idx = None
    for i, (d, _c) in enumerate(closes):
        if d == day:
            idx = i
            break
    if idx is None:
        return {}
    out: dict[str, float] = {}
    c_now = closes[idx][1]
    # RS (20d) — same definition as the engine's rs_rank: ret20 minus the
    # CSI300 20d return. The percentile is computed later cross-sectionally.
    if idx >= 20:
        out["rs_ret20"] = (c_now / closes[idx - 20][1] - 1.0) * 100.0
    # P10: close / 250d-high (continuous proximity, 0..1)
    if idx >= 250:
        hi = max(c for (_d, c) in closes[idx - 250: idx])
        if hi > 0:
            out["p10_52w"] = c_now / hi
    # P9: 120/250d momentum skipping the recent 20d (A股短期反转 → skip)
    if idx >= 120:
        out["p9_mom120_skip20"] = (closes[idx - 20][1] / closes[idx - 120][1] - 1.0) * 100.0
    if idx >= 250:
        out["p9_mom250_skip20"] = (closes[idx - 20][1] / closes[idx - 250][1] - 1.0) * 100.0
    # P12 reference: ret120 / vol60
    if idx >= 180:
        ret120 = (c_now / closes[idx - 120][1] - 1.0)
        rets = [closes[j][1] / closes[j - 1][1] - 1.0 for j in range(idx - 59, idx + 1)]
        mean = sum(rets) / len(rets)
        var = sum((r - mean) ** 2 for r in rets) / len(rets)
        vol = math.sqrt(var)
        if vol > 0:
            out["p12_ref"] = ret120 / vol
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--windows", default="OOS2,train,valid")
    args = ap.parse_args()

    results: dict[str, dict] = {}
    for name in args.windows.split(","):
        if name not in WINDOWS:
            print(f"unknown window {name}", file=sys.stderr)
            return 2
        s, e = WINDOWS[name]
        series, by_day = _load_bars(s, e)
        cand_pool = _load_scores(s, e)
        # CSI300 benchmark 20d returns (as-of) for RS definition
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT trade_date, close FROM index_daily
                    WHERE ts_code = '000300.SH' AND trade_date >= %s AND trade_date <= %s
                    ORDER BY trade_date
                    """,
                    (s, e),
                )
                bench_rows = cur.fetchall()
        bench_close = {str(d): float(c) for d, c in bench_rows}
        bench_dates = sorted(bench_close)
        bench20: dict[str, float] = {}
        for i in range(20, len(bench_dates)):
            d0, d1 = bench_dates[i - 20], bench_dates[i]
            if bench_close[d0] > 0:
                bench20[d1] = (bench_close[d1] / bench_close[d0] - 1.0) * 100.0

        per_factor: dict[str, list[float]] = {f: [] for f in FACTOR_NAMES}
        per_factor_cand: dict[str, list[float]] = {f: [] for f in FACTOR_NAMES}
        days_done = 0
        for day in sorted(by_day):
            bench = bench20.get(day)
            if bench is None:
                continue
            day_data: dict[str, dict[str, float]] = {}
            for ts in by_day[day]:
                f = factors_for(day, ts, series)
                if "rs_ret20" in f:
                    day_data[ts] = f
            if len(day_data) < 100:
                continue
            # whole-market RS percentile (rank by rs_ret20, strongest = 1.0)
            ranked = sorted(day_data.items(), key=lambda kv: -kv[1].get("rs_ret20", -1e9))
            total = len(ranked)
            rs_pct: dict[str, float] = {}
            for i, (ts, _f) in enumerate(ranked, start=1):
                rs_pct[ts] = (total - i + 1) / total
            pairs: dict[str, list[tuple[float, float]]] = {f: [] for f in FACTOR_NAMES}
            for ts, f in day_data.items():
                rs = rs_pct[ts]
                for fn in FACTOR_NAMES:
                    v = f.get(fn)
                    if v is not None:
                        pairs[fn].append((v, rs))
            for fn in FACTOR_NAMES:
                if len(pairs[fn]) >= 100:
                    xs = [p[0] for p in pairs[fn]]
                    ys = [p[1] for p in pairs[fn]]
                    per_factor[fn].append(abs(_pearson(xs, ys)))
                # candidate-pool correlation (score>=65 symbols that day)
                cands = cand_pool.get(day, {})
                cand_pairs: dict[str, list[tuple[float, float]]] = {f: [] for f in FACTOR_NAMES}
                for _sym, ts in cands.items():
                    f = day_data.get(ts)
                    if not f:
                        continue
                    rs = rs_pct.get(ts)
                    if rs is None:
                        continue
                    for fn in FACTOR_NAMES:
                        v = f.get(fn)
                        if v is not None:
                            cand_pairs[fn].append((v, rs))
                for fn in FACTOR_NAMES:
                    if len(cand_pairs[fn]) >= 10:
                        xs = [p[0] for p in cand_pairs[fn]]
                        ys = [p[1] for p in cand_pairs[fn]]
                        per_factor_cand[fn].append(abs(_pearson(xs, ys)))
            days_done += 1

        def _mean(xs: list[float]) -> float:
            xs = [x for x in xs if x == x]  # drop NaN
            return round(sum(xs) / len(xs), 3) if xs else float("nan")

        results[name] = {
            "days": days_done,
            "whole_market": {f: _mean(per_factor[f]) for f in FACTOR_NAMES},
            "candidate_pool": {f: _mean(per_factor_cand[f]) for f in FACTOR_NAMES},
        }
        print(f"\n=== {name} ({WINDOWS[name][0]}..{WINDOWS[name][1]}) days={days_done} ===")
        print(f"  {'factor':22s} {'|r| whole-market':>18s} {'|r| candidate-pool':>20s}")
        for fn in FACTOR_NAMES:
            print(
                f"  {fn:22s} {results[name]['whole_market'][fn]:18.3f} "
                f"{results[name]['candidate_pool'][fn]:20.3f}"
            )

    json.dump(results, open("data/backtest_reports/signal_p10_p9_correlation.json", "w"), indent=1, ensure_ascii=False)
    print("\nreport -> data/backtest_reports/signal_p10_p9_correlation.json")
    return 0


if __name__ == "__main__":
    sys.exit(main())
