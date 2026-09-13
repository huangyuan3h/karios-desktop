#!/usr/bin/env python3
"""ETF benchmark panel vs the new parking baseline (H-BENCH · 2026-09-13).

Baseline = S-3 CN engine + idle parking (P1, B11). Methods:
  - per-ETF buy & hold (incl. 上证50 / 科创50)
  - equal-weight broad portfolio (buy&hold and monthly rebalance)
  - inverse-vol risk parity (300/500/gold/nasdaq/bond, monthly)
  - 60/40 (CSI300 / 10y bond), daily
  - CSI300 x MA200 timing (else 10y bond)
  - parking sleeve standalone (ETF rotation only, no S-3)
  - 50/50 blends: baseline x risk-parity, baseline x EW
Costs: 5bps/side on rebalancing legs; buy&hold and daily-rebalanced are cost-free.
Usage:
  cd services/data-sync-service
  PYTHONPATH=src:scripts python3 scripts/eval_etf_benchmark_parking.py --save-report
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))
REPORT_DIR = ROOT / "data" / "backtest_reports"
ETF_CSV = ROOT / "data" / "etf" / "etf_daily.csv"

from eval_twin_star_parking import _parking_core_by_day  # noqa: E402
from run_walk_forward import WINDOWS  # noqa: E402

# label -> ts_code for the buy&hold universe (long history only)
BUYHOLD = {
    "上证50": "510050.SH",
    "沪深300": "510300.SH",
    "中证500": "510500.SH",
    "创业板": "159915.SZ",
    "科创50": "588000.SH",
    "黄金": "518880.SH",
    "十年国债": "511260.SH",
    "纳指100": "513100.SH",
    "恒生科技": "513180.SH",
}
EW_UNIVERSE = list(BUYHOLD.values())
RP_UNIVERSE = ["510300.SH", "510500.SH", "518880.SH", "513100.SH", "511260.SH"]
COST = 0.0005
WIN_ORDER = ("OOS2", "train", "valid", "long")


def _load_closes() -> dict[str, dict[str, float]]:
    out: dict[str, dict[str, float]] = {}
    wanted = set(BUYHOLD.values()) | {"513350.SH", "513110.SH", "511260.SH"}
    with ETF_CSV.open(newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            ts = row["ts_code"]
            if ts not in wanted:
                continue
            d = str(row["trade_date"])
            d = f"{d[:4]}-{d[4:6]}-{d[6:8]}"
            try:
                c = float(row["close_adj"])
            except (TypeError, ValueError):
                continue
            if c > 0:
                out.setdefault(ts, {})[d] = c
    return out


def _series_on_cal(px: dict[str, float], cal: list[str]) -> list[float]:
    """Forward-filled normalized NAV on the calendar (1.0 before first print)."""
    out, last = [], None
    for d in cal:
        v = px.get(d)
        if v:
            last = v
        out.append(last)
    first = next((v for v in out if v), None)
    return [1.0] * len(cal) if first is None else [(v / first if v else 1.0) for v in out]


def _stats(nav: list[float]) -> dict[str, float]:
    n = len(nav)
    if n < 3 or not nav[0]:
        return {"total_pct": 0.0, "cagr": 0.0, "max_dd": 0.0, "sharpe": 0.0}
    years = (n - 1) / 252.0
    total = (nav[-1] / nav[0] - 1) * 100
    cagr = ((nav[-1] / nav[0]) ** (1 / years) - 1) * 100 if years > 0 and nav[-1] > 0 else 0.0
    peak, mdd = nav[0], 0.0
    for v in nav:
        peak = max(peak, v)
        mdd = min(mdd, v / peak - 1.0)
    rets = [nav[i] / nav[i - 1] - 1 for i in range(1, n) if nav[i - 1] > 0]
    std = float(np.std(rets)) if rets else 0.0
    sharpe = float(np.mean(rets) / std * (252**0.5)) if std > 0 else 0.0
    return {"total_pct": round(total, 1), "cagr": round(cagr, 2), "max_dd": round(100 * mdd, 1), "sharpe": round(sharpe, 2)}


def _fmt(m: dict[str, float]) -> str:
    return f"{m['total_pct']:+.1f}/{m['cagr']:.1f}/{m['max_dd']:.1f}/{m['sharpe']:.2f}"


def _rebal_nav(nav_series: dict[str, list[float]], target: dict[str, float], cal: list[str], *, monthly: bool, cost: float) -> list[float]:
    """Rebalance to fixed target weights at month starts (or daily), growth from nav_series."""
    nav, cur_w = 1.0, {}
    out = [1.0]
    prev_month = cal[0][:7]
    for i in range(1, len(cal)):
        d = cal[i]
        growth = sum(
            w * (nav_series[ts][i] / nav_series[ts][i - 1] - 1.0)
            for ts, w in cur_w.items()
            if nav_series[ts][i - 1]
        )
        nav *= 1.0 + growth
        if (not monthly) or (d[:7] != prev_month):
            cost_frac = sum(abs(target[ts] - cur_w.get(ts, 0.0)) for ts in target) / 2.0 if cur_w else 1.0
            nav *= 1.0 - cost * cost_frac
            cur_w = dict(target)
        prev_month = d[:7]
        out.append(nav)
    return out


def _blend(a: list[float], b: list[float], w: float) -> list[float]:
    n = min(len(a), len(b))
    out = [1.0]
    for i in range(1, n):
        ra = a[i] / a[i - 1] - 1.0 if a[i - 1] else 0.0
        rb = b[i] / b[i - 1] - 1.0 if b[i - 1] else 0.0
        out.append(out[-1] * (1.0 + w * ra + (1.0 - w) * rb))
    return out


def _ma_on_cal(raw: dict[str, float], cal: list[str], window: int = 200) -> list[float | None]:
    """Trailing MA (200) of the raw adjusted series, mapped on the calendar."""
    ds = sorted(raw)
    vals = [raw[d] for d in ds]
    out: list[float | None] = []
    j = 0
    for d in cal:
        while j < len(ds) and ds[j] <= d:
            j += 1
        if j >= window:
            out.append(float(np.mean(vals[j - window:j])))
        else:
            out.append(None)
    return out


def _corr(a: list[float], b: list[float]) -> float:
    n = min(len(a), len(b))
    if n < 10:
        return 0.0
    ra = [a[i] / a[i - 1] - 1 for i in range(1, n)]
    rb = [b[i] / b[i - 1] - 1 for i in range(1, n)]
    return round(float(np.corrcoef(ra, rb)[0, 1]), 2)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--save-report", action="store_true")
    args = ap.parse_args()

    px = _load_closes()
    results: dict[str, dict] = {}
    for w in WIN_ORDER:
        s, e = WINDOWS[w]
        print(f"=== {w} ({s}~{e}) ===", flush=True)
        core_by_day = _parking_core_by_day(px, s, e)
        cal = sorted(core_by_day)
        core = [core_by_day[d] for d in cal]
        base = _stats(core)

        series = {ts: _series_on_cal(px.get(ts) or {}, cal) for ts in BUYHOLD.values()}
        rows: dict[str, dict] = {"停车场基线": {**_stats(core), "corr": 1.0}}

        for label, ts in BUYHOLD.items():
            m = _stats(series[ts])
            rows[label] = {**m, "corr": _corr(series[ts], core)}

        ew_bh = [float(np.mean([series[ts][i] for ts in EW_UNIVERSE])) for i in range(len(cal))]
        rows["等权买持(9只)"] = {**_stats(ew_bh), "corr": _corr(ew_bh, core)}
        ew_reb = _rebal_nav(series, {ts: 1.0 / len(EW_UNIVERSE) for ts in EW_UNIVERSE}, cal, monthly=True, cost=COST)
        rows["等权月再平衡"] = {**_stats(ew_reb), "corr": _corr(ew_reb, core)}

        rp_w = {}
        for i in range(len(cal)):
            if i < 60:
                rp_w[i] = {ts: 1.0 / len(RP_UNIVERSE) for ts in RP_UNIVERSE}
                continue
            if cal[i][:7] == cal[i - 1][:7]:
                rp_w[i] = rp_w[i - 1]
                continue
            vol = {}
            for ts in RP_UNIVERSE:
                r = [series[ts][j] / series[ts][j - 1] - 1 for j in range(i - 60, i) if series[ts][j - 1]]
                vol[ts] = float(np.std(r)) or 1e-9
            inv = {ts: 1.0 / vol[ts] for ts in RP_UNIVERSE}
            tot = sum(inv.values())
            rp_w[i] = {ts: inv[ts] / tot for ts in RP_UNIVERSE}
        rp_nav, cur_w, nav = [1.0], rp_w[0], 1.0
        for i in range(1, len(cal)):
            r = sum(cur_w[ts] * (series[ts][i] / series[ts][i - 1] - 1 if series[ts][i - 1] else 0.0) for ts in RP_UNIVERSE)
            nav *= 1.0 + r
            if rp_w[i] != cur_w:
                turn = sum(abs(rp_w[i][ts] - cur_w[ts]) for ts in RP_UNIVERSE) / 2.0
                nav *= 1.0 - COST * turn
                cur_w = rp_w[i]
            rp_nav.append(nav)
        rows["风险预算(300/500/金/纳/债)"] = {**_stats(rp_nav), "corr": _corr(rp_nav, core)}

        s300, sbond = series["510300.SH"], series["511260.SH"]
        n6040 = _rebal_nav({"510300.SH": s300, "511260.SH": sbond}, {"510300.SH": 0.6, "511260.SH": 0.4}, cal, monthly=False, cost=COST)
        rows["60/40 日再平衡"] = {**_stats(n6040), "corr": _corr(n6040, core)}

        ma200_300 = _ma_on_cal(px.get("510300.SH") or {}, cal)
        nav, hold_300, c300, cbond = 1.0, True, s300, sbond
        ma_nav = [1.0]
        switches = 0
        for i in range(1, len(cal)):
            if ma200_300[i - 1]:
                want = c300[i - 1] >= ma200_300[i - 1]
                if want != hold_300:
                    switches += 1
                    nav *= 1.0 - COST
                    hold_300 = want
            r = (c300[i] / c300[i - 1] - 1) if hold_300 and c300[i - 1] else ((cbond[i] / cbond[i - 1] - 1) if cbond[i - 1] else 0.0)
            nav *= 1.0 + r
            ma_nav.append(nav)
        rows["300×MA200 择时(+债)"] = {**_stats(ma_nav), "corr": _corr(ma_nav, core), "switches": switches}

        sleeve = _sleeve_only_nav(px, cal)
        rows["停车场套筒单独"] = {**_stats(sleeve), "corr": _corr(sleeve, core)}

        rows["基线×风险预算 50/50"] = {**_stats(_blend(core, rp_nav, 0.5)), "corr": _corr(_blend(core, rp_nav, 0.5), core)}
        rows["基线×等权买持 50/50"] = {**_stats(_blend(core, ew_bh, 0.5)), "corr": _corr(_blend(core, ew_bh, 0.5), core)}

        best_ts = max(BUYHOLD.values(), key=lambda ts: series[ts][-1] / series[ts][0])
        rows["最强单只(事后)"] = {**_stats(series[best_ts]), "corr": _corr(series[best_ts], core), "name": next(k for k, v in BUYHOLD.items() if v == best_ts)}
        results[w] = rows
        print(f"  base {_fmt(base)}  (n={len(cal)} days)  done", flush=True)

    print("\n## 最终对照表（total% / CAGR% / maxDD% / Sharpe；括号=与停车场基线的日收益相关）")
    print("| 方法 | " + " | ".join(WIN_ORDER) + " |")
    print("|---|" + "---|" * len(WIN_ORDER))
    keys = ["停车场基线", *BUYHOLD.keys(), "等权买持(9只)", "等权月再平衡", "风险预算(300/500/金/纳/债)", "60/40 日再平衡", "300×MA200 择时(+债)", "停车场套筒单独", "基线×风险预算 50/50", "基线×等权买持 50/50", "最强单只(事后)"]
    for k in keys:
        cells = []
        for w in WIN_ORDER:
            r = results[w].get(k)
            if not r:
                cells.append("—")
                continue
            tag = "" if k == "停车场基线" else f" ({r['corr']:+.2f})"
            extra = f" [{r.get('name')}]" if k == "最强单只(事后)" else ""
            cells.append(_fmt(r) + tag + extra)
        print(f"| {k} | " + " | ".join(cells) + " |")

    if args.save_report:
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        (REPORT_DIR / "etf_benchmark_parking_2026-09-13.json").write_text(
            json.dumps({"tag": "etf-benchmark-parking-2026-09-13", "results": results,
                        "as_of": datetime.now(UTC).isoformat(timespec="seconds")},
                       ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        print("saved report")
    return 0


def _sleeve_only_nav(px: dict[str, dict[str, float]], cal: list[str]) -> list[float]:
    """Parking sleeve alone (ETF rotation, 100% notional, same rule as P1)."""
    import bisect

    GOLD, OIL, BOND10 = "518880.SH", "513350.SH", "511260.SH"
    ALIAS = ("513110.SH", "513100.SH")
    CAND = {"GOLD": GOLD, "OIL": OIL, "NASDAQ": ALIAS[0], "BOND10": BOND10}
    MA, LB, TRAIL, COST = 200, 60, 0.08, 0.0005
    days = {ts: sorted(m) for ts, m in px.items()}

    def idx(ts, d):
        ds = days.get(ts) or []
        i = bisect.bisect_left(ds, d)
        return i if i < len(ds) and ds[i] == d else None

    def ma(ts, d):
        i = idx(ts, d)
        return None if i is None or i < MA - 1 else float(np.mean([px[ts][days[ts][j]] for j in range(i - MA + 1, i + 1)]))

    def mom(ts, d):
        i = idx(ts, d)
        if i is None or i < LB:
            return None
        a = px[ts][days[ts][i - LB]]
        return px[ts][d] / a - 1.0 if a else None

    nav, held_key, held_ts, peak = 1.0, None, None, 0.0
    out = [1.0]
    for i in range(1, len(cal)):
        day, prev = cal[i], cal[i - 1]
        pool_mom, pool_ts = {}, {}
        for key, ts in CAND.items():
            for a in (ALIAS if key == "NASDAQ" else (ts,)):
                m, mm = mom(a, prev), ma(a, prev)
                if m is not None and mm is not None and px.get(a, {}).get(prev) and px[a][prev] >= mm:
                    if m > pool_mom.get(key, -1e9):
                        pool_mom[key], pool_ts[key] = m, a
        best = max(pool_mom, key=pool_mom.get) if pool_mom else None
        best_ts = pool_ts.get(best) if best else None
        sides = 0
        if held_key != best:
            sides += (held_key is not None) + (best is not None)
            held_key, held_ts = best, best_ts
            peak = (px.get(best_ts, {}).get(prev) or 0.0) if best_ts else 0.0
        sr = 0.0
        if held_ts:
            c = px.get(held_ts, {}).get(prev) or 0.0
            peak = max(peak, c)
            if peak > 0 and c and c < peak * (1 - TRAIL):
                held_key = held_ts = None
                peak = 0.0
                sides += 1
            else:
                c2 = px.get(held_ts, {}).get(day)
                sr = c2 / c - 1.0 if c2 and c else 0.0
        nav *= 1.0 + sr - COST * sides
        out.append(nav)
    return out


if __name__ == "__main__":
    raise SystemExit(main())
