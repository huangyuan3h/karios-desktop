#!/usr/bin/env python3
"""H-ETF-B pre-registered diagnostic: sector-ETF relative strength as a signal.

Necessary condition first: does top-3 sector ETF mom20 predict forward sector
returns vs the equal-weight sector universe? Plus a weekly top-3 rotation NAV
as background. Read-only, no grid (mom60/TopK/horizon not scanned).

Prereg & kill lines: docs/designs/etf-sector-momentum-prereg-2026-09-12.md

Usage:
  cd services/data-sync-service
  PYTHONPATH=src:scripts python3 scripts/diag_etf_sector_momentum.py --save-report
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
META = ROOT / "data" / "etf" / "etf_meta.csv"
DAILY = ROOT / "data" / "etf" / "etf_daily.csv"
REPORT_DIR = ROOT / "data" / "backtest_reports"
WINDOWS = {
    "OOS2": ("2024-08-01", "2025-08-01"),
    "train": ("2025-08-01", "2026-02-01"),
    "valid": ("2026-03-01", "2026-08-07"),
    "long": ("2021-08-01", "2026-08-07"),
}
HORIZONS = (5, 10, 20)
TOPK = 3
REBAL = 5
COST = 0.0020


def _load() -> tuple[dict[str, dict[str, float]], dict[str, list[str]], dict[str, dict[str, int]]]:
    sector = {r["ts_code"] for r in csv.DictReader(META.open()) if r["kind"] == "sector"}
    px: dict[str, dict[str, float]] = defaultdict(dict)
    with DAILY.open() as fh:
        for r in csv.DictReader(fh):
            if r["ts_code"] not in sector:
                continue
            d = str(r["trade_date"])
            iso = f"{d[:4]}-{d[4:6]}-{d[6:8]}" if len(d) == 8 else d
            px[r["ts_code"]][iso] = float(r["close_adj"])
    days = {ts: sorted(mp) for ts, mp in px.items()}
    idx = {ts: {d: i for i, d in enumerate(ds)} for ts, ds in days.items()}
    return px, days, idx


def _mom(px, days, idx, ts, t, lb):
    i = idx[ts].get(t)
    if i is None or i < lb:
        return None
    a = px[ts][days[ts][i - lb]]
    if a <= 0:
        return None
    return px[ts][t] / a - 1.0


def _fwd(px, days, idx, ts, t, h):
    i = idx[ts].get(t)
    if i is None or i + h >= len(days[ts]):
        return None
    return px[ts][days[ts][i + h]] / px[ts][t] - 1.0


def _stats(nav):
    n = len(nav)
    if n < 2 or not nav[0]:
        return {"total_pct": 0.0, "sharpe": 0.0, "max_dd": 0.0}
    total = (nav[-1] / nav[0] - 1) * 100
    peak = nav[0]
    mdd = 0.0
    for v in nav:
        peak = max(peak, v)
        if peak:
            mdd = max(mdd, (peak - v) / peak * 100)
    rets = [nav[i] / nav[i - 1] - 1 for i in range(1, n) if nav[i - 1] > 0]
    sr = 0.0
    if len(rets) > 10:
        sd = float(np.std(rets))
        if sd > 0:
            sr = float(np.mean(rets)) / sd * (252**0.5)
    return {"total_pct": round(total, 1), "sharpe": round(sr, 2), "max_dd": round(mdd, 1)}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--save-report", action="store_true")
    args = ap.parse_args()

    px, days, idx = _load()
    calendar = sorted({d for mp in px.values() for d in mp})
    print(f"sector ETFs {len(px)}; calendar {len(calendar)} days {calendar[0]}~{calendar[-1]}\n", flush=True)

    # --- edge: top3 mom20 forward vs EW ---
    edges: dict[int, list[tuple[str, float]]] = {h: [] for h in HORIZONS}
    bmark: dict[int, list[tuple[str, float]]] = {h: [] for h in HORIZONS}
    for t in calendar:
        elig = [ts for ts in px if _mom(px, days, idx, ts, t, 20) is not None]
        if len(elig) < TOPK + 1:
            continue
        top = sorted(elig, key=lambda ts: _mom(px, days, idx, ts, t, 20), reverse=True)[:TOPK]
        for h in HORIZONS:
            fs = [x for x in (_fwd(px, days, idx, ts, t, h) for ts in top) if x is not None]
            es = [x for x in (_fwd(px, days, idx, ts, t, h) for ts in elig) if x is not None]
            if fs and es:
                edges[h].append((t, float(np.mean(fs)) - float(np.mean(es))))
                bmark[h].append((t, float(np.mean(es))))

    def _edge_win(s, e, h):
        xs = [v for d, v in edges[h] if s <= d <= e]
        return (round(100 * float(np.mean(xs)), 2) if xs else None, len(xs))

    print("## Top3 mom20 forward edge vs EW sector (%) [n]")
    for w, (s, e) in WINDOWS.items():
        cells = "  ".join(f"H{h} {_edge_win(s,e,h)}" for h in HORIZONS)
        print(f"  {w:<10} {cells}")
    print()

    # per-year edge_20 over long
    print("## per-year edge_20")
    yrs = sorted({d[:4] for d, _ in edges[20]})
    for y in yrs:
        v, n = _edge_win(f"{y}-01-01", f"{y}-12-31", 20)
        print(f"  {y}: {v}% [n={n}]")
    print()

    # --- rotation NAV (background) ---
    def _rotation(s, e):
        dts = [d for d in calendar if s <= d <= e]
        if len(dts) < 30:
            return None, None
        last: dict[str, float] = {}
        port = [1.0]
        ew = [1.0]
        holds: list[str] = []
        rets_by_ts: dict[str, float] = {}
        for i, d in enumerate(dts):
            rets_by_ts = {}
            present = []
            for ts, mp in px.items():
                if d in mp:
                    if ts in last and last[ts] > 0:
                        rets_by_ts[ts] = mp[d] / last[ts] - 1.0
                    last[ts] = mp[d]
                    present.append(ts)
            if i == 0:
                continue
            if i % REBAL == 1 or not holds:
                prev = dts[i - 1]
                elig = [ts for ts in present if _mom(px, days, idx, ts, prev, 20) is not None]
                if len(elig) >= TOPK:
                    ranked = sorted(elig, key=lambda ts: _mom(px, days, idx, ts, prev, 20), reverse=True)
                    new = ranked[:TOPK]
                    turn = 1.0 if not holds else len(set(holds) ^ set(new)) / max(1, len(set(holds) | set(new)))
                    holds = new
                    c = COST * turn
                else:
                    c = 0.0
            else:
                c = 0.0
            pr = float(np.mean([rets_by_ts.get(ts, 0.0) for ts in holds])) if holds else 0.0
            er = float(np.mean(list(rets_by_ts.values()))) if rets_by_ts else 0.0
            port.append(port[-1] * (1 + pr - c))
            ew.append(ew[-1] * (1 + er))
        return _stats(port), _stats(ew)

    print("## rotation NAV (top3 mom20 weekly, 20bp) vs EW sector (tot/sr/dd)")
    rot = {}
    for w, (s, e) in WINDOWS.items():
        a, b = _rotation(s, e)
        rot[w] = {"top3": a, "ew": b}
        if a:
            print(f"  {w:<10} top3 {a['total_pct']:+.1f}/{a['sharpe']:.2f}/{a['max_dd']:.1f}   "
                  f"ew {b['total_pct']:+.1f}/{b['sharpe']:.2f}/{b['max_dd']:.1f}")
    print()

    wf = ["OOS2", "train", "valid"]
    e20 = {w: _edge_win(*WINDOWS[w], 20)[0] for w in WINDOWS}
    e5 = {w: _edge_win(*WINDOWS[w], 5)[0] for w in WINDOWS}
    e10 = {w: _edge_win(*WINDOWS[w], 10)[0] for w in WINDOWS}
    k1 = (e20["long"] or -1) > 0 and sum(1 for w in wf if (e20[w] or -1) > 0) >= 2
    k2 = not all((e[w] or 0) <= 0 for e in (e5, e10, e20) for w in WINDOWS)
    k3 = (rot["long"]["top3"]["total_pct"] > rot["long"]["ew"]["total_pct"]
          and sum(1 for w in wf if rot[w]["top3"] and rot[w]["top3"]["total_pct"] > rot[w]["ew"]["total_pct"]) >= 2)
    verdict = "OPEN gate design (ETF strength vs mainline)" if (k1 and k2 and k3) else "REJECT / close line"
    print("## H-ETF-B verdict\n")
    print(f"  K1 edge_20 long {e20['long']}% WF {[e20[w] for w in wf]} -> {'pass' if k1 else 'FAIL'}")
    print(f"  K2 horizons not all <=0 -> {'pass' if k2 else 'FAIL'}")
    print(f"  K3 rotation long beat EW + >=2 WF -> {'pass' if k3 else 'FAIL'}")
    print(f"  => {verdict}")

    payload = {"tag": "etf-sector-momentum-2026-09-12",
               "prereg": "docs/designs/etf-sector-momentum-prereg-2026-09-12.md",
               "edge": {str(h): {"long_mean_pct": _edge_win(*WINDOWS["long"], h)[0],
                                 "per_window": {w: _edge_win(*WINDOWS[w], h)[0] for w in WINDOWS}} for h in HORIZONS},
               "rotation": rot, "verdict": {"k1": k1, "k2": k2, "k3": k3, "call": verdict},
               "as_of": datetime.now(UTC).isoformat(timespec="seconds")}
    if args.save_report:
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        (REPORT_DIR / "etf_sector_momentum_2026-09-12.json").write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        print("\nsaved report")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
