#!/usr/bin/env python3
"""H-CB3 pre-registered validation: CB 双低 (price + conversion premium).

Prereg: docs/designs/cb-doublelow-prereg-2026-09-12.md (frozen before this run).
Primary: bottom 20% by (price + conv_prem_pct), weekly rebalance, 20bp,
close-to-close. Controls: low-premium only, low-price only, equal-weight.
Robustness: 40bp, next-open, per-year, long window.

Usage:
  cd services/data-sync-service
  PYTHONPATH=src python3 scripts/compare_cb_doublelow.py --save-report
"""

from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path

import numpy as np

CB = Path(__file__).resolve().parents[1] / "data" / "cb" / "cb_daily.csv"
VAL = Path(__file__).resolve().parents[1] / "data" / "cb" / "cb_valuation.csv"
REPORT_DIR = Path(__file__).resolve().parents[1] / "data" / "backtest_reports"
WINDOWS = {
    "OOS2": ("2024-08-01", "2025-08-01"),
    "train": ("2025-08-01", "2026-02-01"),
    "valid": ("2026-03-01", "2026-08-07"),
    "past_year": ("2025-08-01", "2026-08-07"),
    "long": ("2021-01-01", "2026-08-07"),
}
WF = ("OOS2", "train", "valid")
YEARS = ("2021", "2022", "2023", "2024", "2025", "2026")
REBAL = 5
Q = 0.20
LIQ_Q = 0.40
COST = 0.0020
# (id, score_key, side)  side low = smallest score first
VARIANTS = (
    ("doublelow", "double", "low"),
    ("lowprem", "prem", "low"),
    ("lowprice", "price", "low"),
    ("highprem", "prem", "high"),
    ("ew", None, None),
)


def _stats(nav):
    n = len(nav)
    if n < 2 or not nav[0]:
        return {"n_days": n, "total_pct": 0.0, "max_dd": 0.0, "sharpe": 0.0}
    total = (nav[-1] / nav[0] - 1) * 100
    peak, mdd = nav[0], 0.0
    for v in nav:
        peak = max(peak, v)
        if peak:
            mdd = max(mdd, (peak - v) / peak * 100)
    rets = [nav[i] / nav[i - 1] - 1 for i in range(1, n) if nav[i - 1] > 0]
    sharpe = 0.0
    if len(rets) > 10:
        std = float(np.std(rets))
        if std > 0:
            sharpe = float(np.mean(rets)) / std * (252**0.5)
    return {"n_days": n, "total_pct": round(total, 1), "max_dd": round(mdd, 1), "sharpe": round(sharpe, 2)}


def _fmt(m):
    return f"{m['total_pct']:+.1f}/{m['sharpe']:.2f}/{m['max_dd']:.1f}"


def _panel():
    px: dict[str, dict[str, dict[str, float]]] = defaultdict(lambda: defaultdict(dict))
    with CB.open() as fh:
        for r in csv.DictReader(fh):
            try:
                px[r["date"]][r["ts_code"]].update({
                    "close": float(r["close"]), "open": float(r["open"]), "amount": float(r["amount"]),
                })
            except (ValueError, KeyError):
                pass
    prem: dict[str, dict[str, float]] = defaultdict(dict)
    with VAL.open(encoding="utf-8") as fh:
        for r in csv.DictReader(fh):
            try:
                p = float(r["conv_prem_pct"])
                ce = float(r["close_em"])
            except (ValueError, KeyError):
                continue
            prem[r["date"]][r["ts_code"]] = (p, ce)
    panel: dict[str, dict[str, dict[str, float]]] = defaultdict(dict)
    for d, tsmap in px.items():
        for ts, v in tsmap.items():
            pr = prem.get(d, {}).get(ts)
            if not pr:
                continue
            p, ce = pr
            price = ce if ce > 0 else v["close"]
            panel[d][ts] = {"price": price, "amount": v["amount"], "open": v["open"],
                            "prem": p, "double": price + p, "close": v["close"]}
    return sorted(panel), panel


def _select(day, key, side):
    amts = sorted(v["amount"] for v in day.values())
    q = amts[int(len(amts) * LIQ_Q)] if amts else 0.0
    have = {ts: v for ts, v in day.items() if v["amount"] >= q and v["price"] > 0}
    if key is None:
        return list(have)
    ranked = sorted(have.items(), key=lambda kv: kv[1][key])
    k = max(5, int(len(ranked) * Q))
    return [ts for ts, _ in (ranked[:k] if side == "low" else ranked[-k:])]


def _run(dates, panel, key, side, cost, execute):
    nav = [1.0]
    holdings: list[str] = []
    turns = []
    for i in range(len(dates) - 1):
        d0, d1 = dates[i], dates[i + 1]
        rebal = i % REBAL == 0
        tcost = 0.0
        if rebal:
            prev = set(holdings)
            holdings = _select(panel.get(d0, {}), key, side)
            new = set(holdings)
            turn = (len(prev ^ new) / max(1, len(prev | new))) if prev else 1.0
            turns.append(turn)
            tcost = cost * turn
        rets = []
        for ts in holdings:
            a = panel.get(d0, {}).get(ts)
            b = panel.get(d1, {}).get(ts)
            if not a or not b:
                continue
            if execute == "open" and rebal and b["open"] > 0:
                rets.append(b["close"] / b["open"] - 1.0)
            elif a["close"] > 0:
                rets.append(b["close"] / a["close"] - 1.0)
        r = (sum(rets) / len(rets)) if rets else 0.0
        nav.append(nav[-1] * (1.0 + r - tcost))
    return nav, (float(np.mean(turns)) * 100 if turns else 0.0)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--save-report", action="store_true")
    args = ap.parse_args()

    dates_all, panel = _panel()
    print(f"panel: {len(dates_all)} days, {sum(len(v) for v in panel.values())} bond-days\n", flush=True)

    results: dict[str, dict] = {}
    for wname, (s, e) in WINDOWS.items():
        dts = [d for d in dates_all if s <= d <= e]
        if len(dts) < 30:
            continue
        row = {}
        for vid, key, side in VARIANTS:
            nav, turn = _run(dts, panel, key, side, COST, "close")
            row[vid] = {**_stats(nav), "turnover_pct": round(turn, 1)}
        results[wname] = row
        print(
            f"=== {wname} ({s}~{e}, {len(dts)}d) ===  " + "  ".join(f"{v} {_fmt(row[v])}" for v, _, _ in VARIANTS),
            flush=True,
        )

    print("\n## Per-year (双低 vs ew)\n")
    year_rows = {}
    for y in YEARS:
        dts = [d for d in dates_all if d[:4] == y]
        if len(dts) < 20:
            continue
        dl = _stats(_run(dts, panel, "double", "low", COST, "close")[0])
        ew = _stats(_run(dts, panel, None, None, 0.0, "close")[0])
        year_rows[y] = {"doublelow": dl, "ew": ew, "delta": round(dl["total_pct"] - ew["total_pct"], 1)}
        print(f"  {y}: 双低 {_fmt(dl)}  ew {_fmt(ew)}  Δ {year_rows[y]['delta']:+.1f}pt")

    longs = [d for d in dates_all if "2021-01-01" <= d <= "2026-08-07"]
    rb = {}
    for label, cost, ex in (("20bp_close", COST, "close"), ("40bp_close", 0.0040, "close"), ("20bp_open", COST, "open")):
        dl = _stats(_run(longs, panel, "double", "low", cost, ex)[0])
        ew = _stats(_run(longs, panel, None, None, 0.0, "close")[0])
        rb[label] = {**dl, "delta_vs_ew": round(dl["total_pct"] - ew["total_pct"], 1)}
        print(f"  long {label}: 双低 {_fmt(dl)}  Δew {rb[label]['delta_vs_ew']:+.1f}pt")

    wf_delta = [round(results[w]["doublelow"]["total_pct"] - results[w]["ew"]["total_pct"], 1) for w in WF if w in results]
    long_delta = rb["20bp_close"]["delta_vs_ew"]
    stress = rb["40bp_close"]["delta_vs_ew"]
    pos_years = sum(1 for y in year_rows.values() if y["delta"] > 0)
    k1, k2, k3 = long_delta > 0, sum(1 for d in wf_delta if d > 0) >= 2, stress > 0
    verdict = "PASS" if (k1 and k2 and k3) else "REJECT"
    print("\n## H-CB3 verdict\n")
    print(f"  K1 long Δew {long_delta:+.1f} ({'pass' if k1 else 'FAIL'})")
    print(f"  K2 WF wins {sum(1 for d in wf_delta if d > 0)}/3 {wf_delta} ({'pass' if k2 else 'FAIL'})")
    print(f"  K3 40bp long Δew {stress:+.1f} ({'pass' if k3 else 'FAIL'})")
    print(f"  K4 positive years {pos_years}/{len(year_rows)}")
    print(f"  → {verdict}")

    payload = {
        "tag": "cb-doublelow-2026-09-12",
        "prereg": "docs/designs/cb-doublelow-prereg-2026-09-12.md",
        "windows": results,
        "per_year": year_rows,
        "robustness": rb,
        "verdict": {"wf_delta": wf_delta, "long_delta_20bp": long_delta, "long_delta_40bp": stress,
                    "positive_years": pos_years, "tag": verdict},
        "as_of": datetime.now(UTC).isoformat(timespec="seconds"),
    }
    if args.save_report:
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        path = REPORT_DIR / "cb_doublelow_2026-09-12.json"
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        print(f"\nsaved {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
