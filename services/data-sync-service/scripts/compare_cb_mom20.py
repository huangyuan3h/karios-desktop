#!/usr/bin/env python3
"""H-CB2 pre-registered validation: CB 20d cross-sectional momentum.

Prereg: docs/designs/cb-mom20-prereg-2026-09-12.md (frozen before this run).
Primary: top 20% by trailing 20d return vs equal-weight CB, weekly rebalance,
20bp cost, close-to-close. Robustness (platform only): lookback 10/40,
next-open entry, 40bp stress, per-year and long window.

Usage:
  cd services/data-sync-service
  PYTHONPATH=src python3 scripts/compare_cb_mom20.py --save-report
"""

from __future__ import annotations

import argparse
import csv
import json
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
    "long": ("2021-01-01", "2026-08-07"),
}
WF = ("OOS2", "train", "valid")
YEARS = ("2021", "2022", "2023", "2024", "2025", "2026")
REBAL = 5
Q = 0.20
LIQ_Q = 0.40
LOOKBACKS = (10, 20, 40)
PRIMARY_LB = 20


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


def _load():
    panel: dict[str, dict[str, tuple[float, float]]] = defaultdict(dict)  # date -> ts -> (close, amount)
    with CB_DAILY.open(encoding="utf-8") as fh:
        for r in csv.DictReader(fh):
            try:
                c, a = float(r["close"]), float(r["amount"])
            except (ValueError, KeyError):
                continue
            if c > 0:
                panel[r["date"]][r["ts_code"]] = (c, a)
    return sorted(panel), panel


def _metrics(dates, panel):
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
            m: dict[str, float] = {"close": c, "amount": a}
            for lb in LOOKBACKS:
                if i >= lb:
                    c0 = panel[hist[ts][i - lb]][ts][0]
                    m[f"ret{lb}"] = c / c0 - 1 if c0 else 0.0
            out[d][ts] = m
    return out


def _select(day, lb, top=True):
    amts = sorted(v["amount"] for v in day.values())
    q = amts[int(len(amts) * LIQ_Q)] if amts else 0.0
    key = f"ret{lb}"
    have = {ts: v for ts, v in day.items() if v["amount"] >= q and v["close"] > 0 and key in v}
    if not have:
        return []
    ranked = sorted(have.items(), key=lambda kv: kv[1][key])
    k = max(5, int(len(ranked) * Q))
    return [ts for ts, _ in (ranked[-k:] if top else ranked[:k])]


def _run(dates, dm, lb, cost, execute):
    """execute: 'close' or 'open' (rebalance-day next open entry)."""
    nav = [1.0]
    holdings: list[str] = []
    turns = []
    for i in range(len(dates) - 1):
        d0, d1 = dates[i], dates[i + 1]
        rebal = i % REBAL == 0
        tcost = 0.0
        if rebal:
            prev = set(holdings)
            holdings = _select(dm.get(d0, {}), lb)
            new = set(holdings)
            turn = (len(prev ^ new) / max(1, len(prev | new))) if prev else 1.0
            turns.append(turn)
            tcost = cost * turn
        rets = []
        for ts in holdings:
            v0 = dm.get(d0, {}).get(ts)
            v1 = dm.get(d1, {}).get(ts)
            if not v0 or not v1:
                continue
            if execute == "open" and rebal:
                # entered at d1 open; miss the d0 close -> d1 open gap
                o1 = dm.get(d1, {}).get(ts, {}).get("open")
                if o1:
                    rets.append(v1["close"] / o1 - 1.0)
                    continue
            if v0["close"]:
                rets.append(v1["close"] / v0["close"] - 1.0)
        r = (sum(rets) / len(rets)) if rets else 0.0
        nav.append(nav[-1] * (1.0 + r - tcost))
    avg_turn = float(np.mean(turns)) * 100 if turns else 0.0
    return nav, avg_turn


def _with_open(dates, panel, dm):
    """inject 'open' into metrics from the raw panel (needs open column)."""
    opens: dict[str, dict[str, float]] = defaultdict(dict)
    with CB_DAILY.open(encoding="utf-8") as fh:
        for r in csv.DictReader(fh):
            try:
                opens[r["date"]][r["ts_code"]] = float(r["open"])
            except (ValueError, KeyError):
                continue
    for d, tsmap in dm.items():
        for ts, m in tsmap.items():
            o = opens.get(d, {}).get(ts)
            if o:
                m["open"] = o


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--save-report", action="store_true")
    args = ap.parse_args()

    dates_all, panel = _load()
    dm = _metrics(dates_all, panel)
    _with_open(dates_all, panel, dm)
    print(f"CB daily: {len(dates_all)} days, {sum(len(v) for v in panel.values())} bond-days\n", flush=True)

    results: dict[str, dict] = {}
    for wname, (s, e) in WINDOWS.items():
        dts = [d for d in dates_all if s <= d <= e]
        if len(dts) < 30:
            continue
        row = {}
        for lb in LOOKBACKS:
            nav_m, turn_m = _run(dts, dm, lb, 0.0020, "close")
            row[f"mom{lb}"] = {**_stats(nav_m), "turnover_pct": round(turn_m, 1)}
        row["ew"] = _stats(_run_ew(dts, dm))
        results[wname] = row
        print(
            f"=== {wname} ({s}~{e}, {len(dts)}d) ===  ew {_fmt(row['ew'])} | "
            + "  ".join(f"mom{lb} {_fmt(row[f'mom{lb}'])} (turn {row[f'mom{lb}']['turnover_pct']}%)" for lb in LOOKBACKS),
            flush=True,
        )

    # per-year (primary lb, close)
    print("\n## Per-year (mom20 vs ew)\n")
    year_rows = {}
    for y in YEARS:
        dts = [d for d in dates_all if d[:4] == y]
        if len(dts) < 20:
            continue
        mom = _stats(_run(dts, dm, PRIMARY_LB, 0.0020, "close")[0])
        ew = _stats(_run_ew(dts, dm))
        year_rows[y] = {"mom20": mom, "ew": ew, "delta": round(mom["total_pct"] - ew["total_pct"], 1)}
        print(f"  {y}: mom20 {_fmt(mom)}  ew {_fmt(ew)}  Δ {year_rows[y]['delta']:+.1f}pt")

    # cost / execution robustness on long window
    longs = [d for d in dates_all if "2021-01-01" <= d <= "2026-08-07"]
    rb = {}
    for label, cost, ex in (("20bp_close", 0.0020, "close"), ("40bp_close", 0.0040, "close"), ("20bp_open", 0.0020, "open")):
        m = _stats(_run(longs, dm, PRIMARY_LB, cost, ex)[0])
        ew = _stats(_run_ew(longs, dm))
        rb[label] = {**m, "delta_vs_ew": round(m["total_pct"] - ew["total_pct"], 1)}
        print(f"  long {label}: mom20 {_fmt(m)}  Δew {rb[label]['delta_vs_ew']:+.1f}pt")

    # verdict
    print("\n## H-CB2 verdict\n")
    wf_delta = [round(results[w]["mom20"]["total_pct"] - results[w]["ew"]["total_pct"], 1) for w in WF if w in results]
    long_delta = rb["20bp_close"]["delta_vs_ew"]
    stress_delta = rb["40bp_close"]["delta_vs_ew"]
    k1 = long_delta > 0
    k2 = sum(1 for d in wf_delta if d > 0) >= 2
    k3 = stress_delta > 0
    pos_years = sum(1 for y in year_rows.values() if y["delta"] > 0)
    print(f"  K1 long Δew {long_delta:+.1f} ({'pass' if k1 else 'FAIL'})")
    print(f"  K2 WF wins {sum(1 for d in wf_delta if d > 0)}/3 {wf_delta} ({'pass' if k2 else 'FAIL'})")
    print(f"  K3 40bp long Δew {stress_delta:+.1f} ({'pass' if k3 else 'FAIL'})")
    print(f"  K4 positive years {pos_years}/{len(year_rows)}")
    verdict = "PASS" if (k1 and k2 and k3) else "REJECT"
    print(f"  → {verdict}")

    payload = {
        "tag": "cb-mom20-2026-09-12",
        "prereg": "docs/designs/cb-mom20-prereg-2026-09-12.md",
        "windows": results,
        "per_year": year_rows,
        "robustness": rb,
        "verdict": {"wf_delta": wf_delta, "long_delta_20bp": long_delta, "long_delta_40bp": stress_delta, "positive_years": pos_years, "tag": verdict},
        "as_of": datetime.now(UTC).isoformat(timespec="seconds"),
    }
    if args.save_report:
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        path = REPORT_DIR / "cb_mom20_2026-09-12.json"
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        print(f"\nsaved {path}")
    return 0


def _run_ew(dates, dm):
    nav = [1.0]
    for i in range(len(dates) - 1):
        d0, d1 = dates[i], dates[i + 1]
        amts = sorted(v["amount"] for v in dm.get(d0, {}).values())
        q = amts[int(len(amts) * LIQ_Q)] if amts else 0.0
        holdings = [ts for ts, v in dm.get(d0, {}).items() if v["amount"] >= q and v["close"] > 0]
        rets = []
        for ts in holdings:
            p0 = dm.get(d0, {}).get(ts, {}).get("close")
            p1 = dm.get(d1, {}).get(ts, {}).get("close")
            if p0 and p1:
                rets.append(p1 / p0 - 1.0)
        r = (sum(rets) / len(rets)) if rets else 0.0
        nav.append(nav[-1] * (1.0 + r))
    return nav


if __name__ == "__main__":
    raise SystemExit(main())
