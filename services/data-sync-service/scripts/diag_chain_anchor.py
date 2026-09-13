#!/usr/bin/env python3
"""H-CHAIN-A pre-registered diagnostic: overseas anchor -> A-share concept.

For each (board <- anchor) pair, map every board trading day t to the freshest
US anchor session before t, then measure the overnight gap, the intraday move,
and forward drift. Tests whether the transmission is a one-shot overnight gap
(§一.14, untradeable) or a tradeable drift.

Prereg & kill lines: docs/designs/chain-anchor-prereg-2026-09-12.md
Read-only, no parameter search.

Usage:
  cd services/data-sync-service
  PYTHONPATH=src:scripts python3 scripts/diag_chain_anchor.py --save-report
"""

from __future__ import annotations

import argparse
import bisect
import csv
import json
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
CHAIN = ROOT / "data" / "chain"
REPORT_DIR = ROOT / "data" / "backtest_reports"
WINDOWS = {"OOS2": ("2024-08-01", "2025-08-01"), "train": ("2025-08-01", "2026-02-01"),
           "valid": ("2026-03-01", "2026-08-07"), "long": ("2021-01-01", "2026-08-07")}
NS = (5, 10, 20)


def _corr(xs: list[float], ys: list[float]) -> float | None:
    if len(xs) < 30:
        return None
    a = np.asarray(xs, dtype=float)
    b = np.asarray(ys, dtype=float)
    if np.std(a) == 0 or np.std(b) == 0:
        return None
    return round(float(np.corrcoef(a, b)[0, 1]), 3)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--save-report", action="store_true")
    args = ap.parse_args()

    meta: dict[str, str] = {}
    with (CHAIN / "meta.csv").open() as fh:
        for r in csv.DictReader(fh):
            meta[r["board"]] = r["anchor"]

    boards: dict[str, dict[str, tuple[float, float]]] = defaultdict(dict)
    with (CHAIN / "board_index.csv").open() as fh:
        for r in csv.DictReader(fh):
            try:
                boards[r["board"]][r["date"]] = (float(r["open"]), float(r["close"]))
            except (ValueError, KeyError):
                continue

    anchors: dict[str, list[tuple[str, float]]] = defaultdict(list)
    with (CHAIN / "anchor_us.csv").open() as fh:
        for r in csv.DictReader(fh):
            try:
                anchors[r["symbol"]].append((r["date"], float(r["close"])))
            except (ValueError, KeyError):
                continue
    for s in anchors:
        anchors[s] = sorted(set(anchors[s]))

    def anchor_series(sym: str) -> tuple[list[str], list[float], list[float], list[float]]:
        recs = anchors[sym]
        days = [r[0] for r in recs]
        closes = [r[1] for r in recs]
        ret = [0.0] * len(days)
        mom = [None] * len(days)  # type: ignore
        for i in range(1, len(days)):
            ret[i] = closes[i] / closes[i - 1] - 1.0 if closes[i - 1] > 0 else 0.0
        for i in range(20, len(days)):
            mom[i] = closes[i] / closes[i - 20] - 1.0 if closes[i - 20] > 0 else None
        return days, ret, mom, closes

    # records: one per (pair, board_day t)
    recs: list[dict] = []
    per_pair: dict[str, list[dict]] = defaultdict(list)
    acache: dict[str, tuple] = {}
    for board, anchor in meta.items():
        bd = boards.get(board)
        if not bd:
            continue
        if anchor not in acache:
            acache[anchor] = anchor_series(anchor)
        adays, aret, amom, _ = acache[anchor]
        days = sorted(bd)
        for i in range(1, len(days)):
            t = days[i]
            j = bisect.bisect_left(adays, t)
            if j == 0:
                continue
            k = j - 1  # freshest US session < t
            if amom[k] is None:
                continue
            o_t, c_t = bd[t]
            c_prev = bd[days[i - 1]][1]
            if o_t <= 0 or c_prev <= 0:
                continue
            rec = {"board": board, "t": t, "anchor_ret": aret[k], "anchor_mom20": amom[k],
                   "gap": o_t / c_prev - 1.0, "intraday": c_t / o_t - 1.0}
            for n in NS:
                if i + n < len(days):
                    rec[f"fwd{n}"] = bd[days[i + n]][1] / c_t - 1.0
            recs.append(rec)
            per_pair[board].append(rec)

    print(f"pairs {len(per_pair)} records {len(recs)}")

    def arr(rows, key):
        return [r[key] for r in rows if key in r and r[key] is not None]

    # A. reaction decomposition
    print("\n## A. reaction decomposition (pooled corr)")
    r_gap = _corr(arr(recs, "anchor_ret"), arr(recs, "gap"))
    r_intra = _corr(arr(recs, "anchor_ret"), arr(recs, "intraday"))
    print(f"  corr(anchor_ret, gap)      {r_gap}")
    print(f"  corr(anchor_ret, intraday) {r_intra}")

    print("\n## per-pair corr(anchor_ret, gap / intraday)  n")
    pair_tbl = {}
    for b, rows in sorted(per_pair.items(), key=lambda kv: -len(kv[1])):
        g = _corr(arr(rows, "anchor_ret"), arr(rows, "gap"))
        it = _corr(arr(rows, "anchor_ret"), arr(rows, "intraday"))
        pair_tbl[b] = {"gap": g, "intraday": it, "n": len(rows)}
        print(f"  {b:<16} gap {str(g):>7}  intraday {str(it):>7}  n {len(rows)}")

    # B. slow drift: anchor_mom20 terciles -> board fwd20
    m20 = np.asarray(arr(recs, "anchor_mom20"), dtype=float)
    lo_thr, hi_thr = np.percentile(m20, [100 / 3, 200 / 3])

    def tercile(v: float) -> str:
        return "low" if v <= lo_thr else ("high" if v >= hi_thr else "mid")

    fwdN = NS[-1]
    for r in recs:
        r["_tercile"] = tercile(r["anchor_mom20"])
    print(f"\n## B. anchor_mom20 terciles (lo<={lo_thr:.3f}<hi<={hi_thr:.3f}) -> fwd{fwdN}")
    print(f"  {'group':<6}{'fwd20 mean%':>14}{'n':>8}")
    for g in ("low", "mid", "high"):
        vals = [r[f"fwd{fwdN}"] for r in recs if r["_tercile"] == g and f"fwd{fwdN}" in r]
        print(f"  {g:<6}{(100*np.mean(vals) if vals else 0):>14.2f}{len(vals):>8}")
    hi_lo_long = None
    hv = [r[f"fwd{fwdN}"] for r in recs if r["_tercile"] == "high" and f"fwd{fwdN}" in r]
    lv = [r[f"fwd{fwdN}"] for r in recs if r["_tercile"] == "low" and f"fwd{fwdN}" in r]
    if hv and lv:
        hi_lo_long = round(100 * (float(np.mean(hv)) - float(np.mean(lv))), 2)
    print(f"  high-low diff (long window basis: all) {hi_lo_long}")

    print(f"\n## B2. by window: fwd{fwdN} high / low / diff")
    win = {}
    for w, (s, e) in WINDOWS.items():
        dw = [r for r in recs if s <= r["t"] <= e and f"fwd{fwdN}" in r]
        hv = [r[f"fwd{fwdN}"] for r in dw if r["_tercile"] == "high"]
        lv = [r[f"fwd{fwdN}"] for r in dw if r["_tercile"] == "low"]
        h = round(100 * float(np.mean(hv)), 2) if hv else None
        l = round(100 * float(np.mean(lv)), 2) if lv else None
        d = round(h - l, 2) if h is not None and l is not None else None
        win[w] = {"high": h, "low": l, "diff": d, "n": len(dw)}
        print(f"  {w:<8} high {h}  low {l}  diff {d}  n {len(dw)}")

    wf = ["OOS2", "train", "valid"]
    k1 = r_gap is not None and r_gap > 0.2
    k2_intra = r_intra is not None and r_intra >= 0.1
    k2_drift = (hi_lo_long or -99) > 0
    k2 = k2_intra or k2_drift
    verdict = "CANDIDATE (transmission has tradeable part)" if (k1 and k2) else "REJECT (§一.14 gap-consumed)"
    print("\n## H-CHAIN-A verdict\n")
    print(f"  K1 corr(anchor_ret,gap)>0.2 -> {'pass' if k1 else 'FAIL'} ({r_gap})")
    print(f"  K2 intraday corr>=0.1 ({r_intra}) OR mom20 hi-lo fwd20>0 ({hi_lo_long}) -> {'pass' if k2 else 'FAIL'}")
    print(f"  => {verdict}")

    payload = {"tag": "chain-anchor-2026-09-12",
               "prereg": "docs/designs/chain-anchor-prereg-2026-09-12.md",
               "corr_gap": r_gap, "corr_intraday": r_intra,
               "per_pair": pair_tbl, "by_window": win, "tercile_thr": [round(float(lo_thr), 4), round(float(hi_thr), 4)],
               "hi_lo_fwd20": hi_lo_long,
               "verdict": {"k1": k1, "k2": k2, "k2_intra": k2_intra, "k2_drift": k2_drift, "call": verdict},
               "as_of": datetime.now(UTC).isoformat(timespec="seconds")}
    if args.save_report:
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        (REPORT_DIR / "chain_anchor_2026-09-12.json").write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        print("\nsaved report")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
