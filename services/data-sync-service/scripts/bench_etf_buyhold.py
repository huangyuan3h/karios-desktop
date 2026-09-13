#!/usr/bin/env python3
"""A1/B1: ETF buy-and-hold benchmark vs the twin-star (ruler, not a strategy).

For each window, compare the frozen 机会双子星 and its core against:
  - each available ETF bought and held over the window (上证50/科创50 are not
    in the local daily table; the available set is 300/500/创业板/黄金/十年债/
    纳指/恒科),
  - an equal-weight portfolio of the available ETFs (daily rebalanced),
  - a 60/40 (CSI300 + 10y bond) portfolio.

Outputs total / maxDD / Sharpe plus the correlation of daily returns with the
twin-star. Read-only; Live untouched. Answers "how much is the strategy worth
versus just buying ETFs", and whether the core asset menu is missing a class.

Usage:
  cd services/data-sync-service
  PYTHONPATH=src:scripts python3 scripts/bench_etf_buyhold.py --save-report
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import psycopg

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from data_sync_service.service.ps_g50_blend import blend_nav_opportunity  # noqa: E402
from data_sync_service.service.state_bucket_track import (  # noqa: E402
    FILL_SAME_1430,
    get_settings,
    load_sgap_context,
    replay_sgap_from_context,
)
from pick_strong_grid import build_nav_from_cache, fetch_etf_closes, warm_window  # noqa: E402

WINDOWS = {
    "OOS2": ("2024-08-01", "2025-08-01"),
    "train": ("2025-08-01", "2026-02-01"),
    "valid": ("2026-03-01", "2026-08-07"),
    "past_year": ("2025-08-01", "2026-08-07"),
}
FULL_START = "2024-08-01"
FULL_END = "2026-08-07"
REPORT_DIR = Path(__file__).resolve().parents[1] / "data" / "backtest_reports"

ETF_UNIVERSE = {
    "沪深300": "510300.SH",
    "中证500": "510500.SH",
    "创业板": "159915.SZ",
    "黄金": "518880.SH",
    "十年国债": "511260.SH",
    "纳指100": "513100.SH",
    "恒生科技": "513180.SH",
}


def _stats(nav: list[float]) -> dict[str, float]:
    n = len(nav)
    if n < 2 or not nav[0]:
        return {"n_days": n, "total_pct": 0.0, "max_dd": 0.0, "sharpe": 0.0}
    total = (nav[-1] / nav[0] - 1) * 100
    peak = nav[0]
    mdd = 0.0
    for v in nav:
        if v > peak:
            peak = v
        if peak:
            mdd = max(mdd, (peak - v) / peak * 100)
    rets = [nav[i] / nav[i - 1] - 1 for i in range(1, n) if nav[i - 1] > 0]
    sharpe = 0.0
    if len(rets) > 10:
        std = float(np.std(rets))
        if std > 0:
            sharpe = float(np.mean(rets) / std * (252**0.5))
    return {"n_days": n, "total_pct": round(total, 1), "max_dd": round(mdd, 1), "sharpe": round(sharpe, 2)}


def _fmt(m: dict[str, float]) -> str:
    return f"{m['total_pct']:+.1f}/{m['sharpe']:.2f}/{m['max_dd']:.1f}"


def _load_etf_closes() -> dict[str, dict[str, float]]:
    s = get_settings()
    with psycopg.connect(s.database_url) as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT ts_code, trade_date, close FROM daily WHERE ts_code = ANY(%s) ORDER BY ts_code, trade_date",
            (list(ETF_UNIVERSE.values()),),
        )
        out: dict[str, dict[str, float]] = {}
        for ts, d, c in cur.fetchall():
            out.setdefault(str(ts), {})[str(d)] = float(c)
        return out


def _nav_on(series: dict[str, float], cal: list[str]) -> list[float]:
    last = None
    out: list[float] = []
    for d in cal:
        v = series.get(d)
        if v:
            last = v
        out.append(last if last is not None else float("nan"))
    # forward-fill leading NaN is impossible; return only if covered
    return out


def _rets(nav: list[float]) -> np.ndarray:
    a = np.array(nav, dtype=float)
    r = np.zeros(len(a))
    for i in range(1, len(a)):
        if a[i - 1] > 0 and a[i] == a[i]:
            r[i] = a[i] / a[i - 1] - 1
    return r


def _corr(a: np.ndarray, b: np.ndarray) -> float | None:
    n = min(len(a), len(b))
    a, b = a[:n], b[:n]
    if len(a) < 20 or float(np.std(a)) == 0 or float(np.std(b)) == 0:
        return None
    return float(np.corrcoef(a, b)[0, 1])


def _sat_series(sat: dict) -> tuple[list[str], list[float], list[bool]]:
    rows = sat["rows"]
    dates = [r["date"] for r in rows]
    nav = [float(r["satNav"]) for r in rows]
    active = [bool(r.get("satActive")) for r in rows]
    if nav and nav[0] > 0:
        base = nav[0]
        nav = [v / base for v in nav]
    return dates, nav, active


def _pick_strong_nav(dates: list[str], start: str, end: str, etf_close) -> list[float]:
    cache = warm_window(start, end, etf_close)
    r = build_nav_from_cache(
        cache, lookback=60, ma_window=200, min_hold=1, cost=0.0,
        score="mom", top2=False, trail_pct=8.0,
    )
    pk_map = r["nav"]
    last = 1.0
    out: list[float] = []
    for d in dates:
        v = pk_map.get(d)
        if v is not None:
            last = v
        out.append(last)
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--save-report", action="store_true")
    args = ap.parse_args()

    print("ETF buy-and-hold benchmark vs twin-star\n", flush=True)
    print(f"loading context {FULL_START}~{FULL_END} ...", flush=True)
    ctx = load_sgap_context(FULL_START, FULL_END)
    etf_close = fetch_etf_closes()
    etf_hist = _load_etf_closes()
    print(f"  ETF history: {sorted(etf_hist)}", flush=True)

    results: dict[str, dict] = {}
    for wname, (s, e) in WINDOWS.items():
        print(f"=== {wname} ({s}~{e}) ===", flush=True)
        sat = replay_sgap_from_context(
            ctx, start=s, end=e, skip_t1_limit=True, pool_mode="strict",
            max_pos=4, position_pct=0.25, fill_mode=FILL_SAME_1430,
            fill_hhmm="1430", exit_hhmm="1430", max_open_to_1430_pct=0.03,
            rank_key="amp_1430",
        )
        dates, sat_nav, active = _sat_series(sat)
        core = _pick_strong_nav(dates, s, e, etf_close)
        n = min(len(core), len(sat_nav))
        dates, core, sat_nav, active = dates[:n], core[:n], sat_nav[:n], active[:n]
        twin = blend_nav_opportunity(core, sat_nav, active, sat_weight=0.5)
        twin_m, core_m = _stats(twin), _stats(core)
        twin_r = _rets(twin)
        print(f"  双子星 {_fmt(twin_m)}   核心 {_fmt(core_m)}", flush=True)

        row: dict[str, dict] = {
            "双子星": {"stats": twin_m, "corr": 1.0},
            "核心(择强单轨)": {"stats": core_m, "corr": _corr(_rets(core), twin_r)},
        }
        # per-ETF buy & hold
        avail_series: dict[str, dict[str, float]] = {}
        for label, ts in ETF_UNIVERSE.items():
            ser = etf_hist.get(ts) or {}
            nav = _nav_on(ser, dates)
            if not nav or any(v != v for v in nav):  # any NaN → not covered
                continue
            avail_series[label] = ser
            row[label] = {"stats": _stats(nav), "corr": _corr(_rets(nav), twin_r)}
        # equal-weight daily rebalanced across available ETFs
        if avail_series:
            rets = [_rets(_nav_on(ser, dates)) for ser in avail_series.values()]
            ew = [1.0]
            for i in range(1, len(dates)):
                r = float(np.mean([rr[i] for rr in rets]))
                ew.append(ew[-1] * (1 + r))
            row["等权ETF组合"] = {"stats": _stats(ew), "corr": _corr(_rets(ew), twin_r)}
        # 60/40 CSI300 + bond
        if "沪深300" in avail_series and "十年国债" in avail_series:
            r300 = _rets(_nav_on(avail_series["沪深300"], dates))
            rb = _rets(_nav_on(avail_series["十年国债"], dates))
            p = [1.0]
            for i in range(1, len(dates)):
                p.append(p[-1] * (1 + 0.6 * r300[i] + 0.4 * rb[i]))
            row["60/40(300+债)"] = {"stats": _stats(p), "corr": _corr(_rets(p), twin_r)}

        for label, rec in row.items():
            if label in ("双子星", "核心(择强单轨)"):
                continue
            c = "—" if rec["corr"] is None else f"{rec['corr']:.2f}"
            print(f"    {label:<12} {_fmt(rec['stats'])}  corr {c}", flush=True)
        results[wname] = {"twin": twin_m, "core": core_m, "bench": row}

    # summary table: total return per window for twin vs best benches
    print("\n## 总收益对照（tot% / Sharpe / maxDD）\n")
    labels = ["双子星", "核心(择强单轨)", "沪深300", "中证500", "创业板", "黄金", "十年国债", "纳指100", "恒生科技", "等权ETF组合", "60/40(300+债)"]
    present = [l for l in labels if all(l in results[w]["bench"] for w in WINDOWS)]
    print("| 配置 | " + " | ".join(WINDOWS) + " |")
    print("|" + "|".join(["------"] * (1 + len(WINDOWS))) + "|")
    for l in present:
        cells = []
        for w in WINDOWS:
            m = results[w]["bench"][l]["stats"]
            cells.append(f"{m['total_pct']:+.1f} ({m['sharpe']:.2f}/{m['max_dd']:.1f})")
        print(f"| {l} | " + " | ".join(cells) + " |")

    payload = {
        "tag": "etf-buyhold-bench-2026-09-12",
        "protocol": (
            "frozen twin-star habit sat + pick-strong trail8; ETF buy&hold over each window; "
            "equal-weight and 60/40 daily-rebalanced; corr = daily returns vs twin. Read-only."
        ),
        "etf_universe": ETF_UNIVERSE,
        "windows": results,
        "as_of": datetime.now(UTC).isoformat(timespec="seconds"),
    }
    if args.save_report:
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        path = REPORT_DIR / "etf_buyhold_bench_2026-09-12.json"
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        print(f"\nsaved {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
