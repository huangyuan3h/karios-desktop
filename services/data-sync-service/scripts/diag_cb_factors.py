#!/usr/bin/env python3
"""CB daily cross-section screen (read-only): is any simple CB factor real?

The repo's earlier CB work only tested intraday price micro (T+0 reversal, gap
fade, stock->CB lead-lag) — all REJECT. This screens the DAILY cross-section:
  low / high price, 20d momentum, 5d reversal, 20d low / high volatility,
against the equal-weight CB universe, weekly rebalanced, net 20bp.

The pre-registered primary was H-CB1 (low-price beats EW). The rest are an
exploratory battery (house "diagnostic battery" pattern), not adoption
candidates. Conversion-premium (双低) is a later cut needing PIT 转股价.

Usage:
  cd services/data-sync-service
  PYTHONPATH=src python3 scripts/diag_cb_factors.py --save-report
"""

from __future__ import annotations

import argparse
import csv
import json
import statistics
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path

import numpy as np

CB_DAILY = Path(__file__).resolve().parents[1] / "data" / "cb" / "cb_daily.csv"
REPORT_DIR = Path(__file__).resolve().parents[1] / "data" / "backtest_reports"
WINDOWS = {
    "OOS2": ("2024-08-01", "2025-08-01"),
    "train": ("2025-08-01", "2026-02-01"),
    "valid": ("2026-03-01", "2026-08-07"),
    "past_year": ("2025-08-01", "2026-08-07"),
}
WF = ("OOS2", "train", "valid")
REBAL = 5
Q = 0.20
LIQ_Q = 0.40
COST = 0.0020
# (id, metric, side)  side: "low" = smallest metric first, "high" = largest
VARIANTS = (
    ("low_price", "close", "low"),
    ("high_price", "close", "high"),
    ("mom20", "ret20", "high"),
    ("rev5", "ret5", "low"),
    ("low_vol20", "vol20", "low"),
    ("high_vol20", "vol20", "high"),
    ("ew", None, None),
)


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


def _load() -> tuple[list[str], dict[str, dict[str, tuple[float, float]]]]:
    panel: dict[str, dict[str, tuple[float, float]]] = defaultdict(dict)
    with CB_DAILY.open(encoding="utf-8") as fh:
        for r in csv.DictReader(fh):
            try:
                c, a = float(r["close"]), float(r["amount"])
            except (ValueError, KeyError):
                continue
            if c > 0:
                panel[r["date"]][r["ts_code"]] = (c, a)
    return sorted(panel), panel


def _build_metrics(
    dates: list[str], panel: dict
) -> dict[str, dict[str, dict[str, float]]]:
    """date -> ts -> {close, amount, ret5, ret20, vol20}"""
    hist: dict[str, list[str]] = defaultdict(list)
    for d in dates:
        for ts in panel[d]:
            hist[ts].append(d)
    idx = {ts: {d: i for i, d in enumerate(ds)} for ts, ds in hist.items()}
    out: dict[str, dict[str, dict[str, float]]] = {}
    for d in dates:
        out[d] = {}
        for ts, (c, a) in panel[d].items():
            i = idx[ts][d]
            ds = hist[ts]
            m: dict[str, float] = {"close": c, "amount": a}
            if i >= 5:
                c5 = panel[ds[i - 5]][ts][0]
                m["ret5"] = c / c5 - 1 if c5 else 0.0
            if i >= 20:
                c20 = panel[ds[i - 20]][ts][0]
                m["ret20"] = c / c20 - 1 if c20 else 0.0
                rets = []
                for j in range(i - 19, i + 1):
                    a0 = panel[ds[j - 1]][ts][0]
                    a1 = panel[ds[j]][ts][0]
                    if a0:
                        rets.append(a1 / a0 - 1)
                if len(rets) > 5:
                    m["vol20"] = float(np.std(rets))
            out[d][ts] = m
    return out


def _select(day: dict[str, dict[str, float]], metric: str | None, side: str | None) -> list[str]:
    if not day:
        return []
    amts = sorted(v["amount"] for v in day.values())
    q = amts[int(len(amts) * LIQ_Q)] if amts else 0.0
    liq = {ts: v for ts, v in day.items() if v["amount"] >= q and v["close"] > 0}
    if metric is None:
        return list(liq)
    have = {ts: v for ts, v in liq.items() if metric in v}
    if not have:
        return list(liq)
    ranked = sorted(have.items(), key=lambda kv: kv[1][metric])
    k = max(5, int(len(ranked) * Q))
    return [ts for ts, _ in (ranked[:k] if side == "low" else ranked[-k:])]


def _run(dates: list[str], day_metrics: dict, metric, side) -> list[float]:
    nav = [1.0]
    holdings: list[str] = []
    for i in range(len(dates) - 1):
        d0, d1 = dates[i], dates[i + 1]
        turn = 0.0
        if i % REBAL == 0:
            holdings = _select(day_metrics.get(d0, {}), metric, side)
            turn = COST if holdings else 0.0
        rets = []
        for ts in holdings:
            p0 = day_metrics.get(d0, {}).get(ts, {}).get("close")
            p1 = day_metrics.get(d1, {}).get(ts, {}).get("close")
            if p0 and p1:
                rets.append(p1 / p0 - 1.0)
        r = (sum(rets) / len(rets)) if rets else 0.0
        nav.append(nav[-1] * (1.0 + r - turn))
    return nav


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--save-report", action="store_true")
    args = ap.parse_args()

    dates_all, panel = _load()
    dm = _build_metrics(dates_all, panel)
    print(f"CB daily: {len(dates_all)} days, {sum(len(v) for v in panel.values())} bond-days\n", flush=True)

    results: dict[str, dict] = {}
    for wname, (s, e) in WINDOWS.items():
        dts = [d for d in dates_all if s <= d <= e]
        if len(dts) < 20:
            continue
        row = {}
        for vid, metric, side in VARIANTS:
            row[vid] = _stats(_run(dts, dm, metric, side))
        results[wname] = row
        print(
            f"=== {wname} ({s}~{e}, {len(dts)}d) ===\n   "
            + "  ".join(f"{vid} {_fmt(row[vid])}" for vid, _, _ in VARIANTS),
            flush=True,
        )

    print("\n## Verdict vs ew (Δ total pt)\n")
    print(f"{'variant':<12}" + "".join(f"{w:>12}" for w in WF) + "   verdict")
    verdicts = {}
    for vid, _, _ in VARIANTS:
        if vid == "ew":
            continue
        ds = [round(results[w][vid]["total_pct"] - results[w]["ew"]["total_pct"], 1) for w in WF if w in results]
        if len(ds) < 3:
            continue
        verdicts[vid] = ds
        if all(d > 0 for d in ds):
            tag = "positive all 3"
        elif any(d < -5 for d in ds):
            tag = "REJECT (<=-5pt)"
        elif all(d < 0 for d in ds):
            tag = "NEGATIVE all 3"
        else:
            tag = "mixed / single-window"
        print(f"{vid:<12}" + "".join(f"{d:>+12.1f}" for d in ds) + f"   {tag}")

    payload = {
        "tag": "cb-factors-2026-09-12",
        "protocol": (
            "CB daily from purchased 1-min (data/cb/cb_daily.csv); weekly rebalance, "
            "bottom/top 20% by signal vs equal-weight, liquidity amount>=p40/day, 20bp round trip. "
            "H-CB1 = low_price; rest exploratory. Read-only."
        ),
        "variants": [v[0] for v in VARIANTS],
        "windows": results,
        "verdict_vs_ew": verdicts,
        "as_of": datetime.now(UTC).isoformat(timespec="seconds"),
    }
    if args.save_report:
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        path = REPORT_DIR / "cb_factors_2026-09-12.json"
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        print(f"\nsaved {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
