#!/usr/bin/env python3
"""H-GDHS-A pre-registered diagnostic: 股东户数 concentration -> forward excess.

Event = a 股东户数 disclosure (ann_date). Signal = change vs the stock's previous
end_date count. Tradeable = T+1 open after ann_date. Forward excess vs 中证500.

Prereg & kill lines: docs/designs/gdhs-concentration-prereg-2026-09-12.md
Read-only, no parameter search. Memory-lean via a server-side cursor.

Usage:
  cd services/data-sync-service
  PYTHONPATH=src:scripts python3 scripts/diag_gdhs.py --save-report
"""

from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import psycopg

ROOT = Path(__file__).resolve().parents[1]
GDHS = ROOT / "data" / "gdhs" / "gdhs.csv"
ETF = ROOT / "data" / "etf" / "etf_daily.csv"
REPORT_DIR = ROOT / "data" / "backtest_reports"
WINDOWS = {"OOS2": ("2024-08-01", "2025-08-01"), "train": ("2025-08-01", "2026-02-01"),
           "valid": ("2026-03-01", "2026-08-07"), "long": ("2021-01-01", "2026-08-07")}
NS = (10, 20, 60)
BENCH = "510500.SH"
# bucket by holder-count change
def bucket(chg: float) -> str:
    if chg <= -0.05:
        return "strong_down"
    if chg < 0:
        return "mild_down"
    return "up"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--save-report", action="store_true")
    args = ap.parse_args()

    from data_sync_service.config import get_settings

    # load + dedupe events per ts_code, compute chg vs previous end_date
    per: dict[str, list[tuple[str, str, float]]] = defaultdict(list)  # ts -> [(end_date, ann_date, num)]
    with GDHS.open() as fh:
        for r in csv.DictReader(fh):
            p = str(r["ts_code"]).split(".")[0]
            if not p.startswith(("6", "0", "3")):
                continue
            try:
                per[r["ts_code"]].append((str(r["end_date"]), str(r["ann_date"]), float(r["holder_num"])))
            except (ValueError, KeyError):
                continue
    events: dict[str, list[tuple[str, float]]] = defaultdict(list)  # ts -> [(ann_date, chg)]
    for ts, recs in per.items():
        recs = sorted({(e, a, n) for e, a, n in recs})
        prev = None
        for e, a, n in recs:
            if prev and prev > 0 and n > 0:
                events[ts].append((a, n / prev - 1.0))
            prev = n
    codes = [ts for ts, ev in events.items() if ev and ts.split(".")[0][0] in "603"]
    n_ev = sum(len(events[c]) for c in codes)
    print(f"codes {len(codes)}, events {n_ev}", flush=True)

    bench: dict[str, tuple[float, float]] = {}
    with ETF.open() as fh:
        for r in csv.DictReader(fh):
            if r["ts_code"] != BENCH:
                continue
            d = str(r["trade_date"])
            iso = f"{d[:4]}-{d[4:6]}-{d[6:8]}" if len(d) == 8 else d
            bench[iso] = (float(r["open"]), float(r["close"]))
    bdays = sorted(bench)

    def bench_fwd(d0: str, n: int) -> float | None:
        i = next((k for k, d in enumerate(bdays) if d >= d0), None)
        if i is None or i + n >= len(bdays):
            return None
        bo, bc = bench[bdays[i]][0], bench[bdays[i + n]][1]
        return bc / bo - 1.0 if bo > 0 else None

    grp: dict[str, dict[int, list[float]]] = {b: {n: [] for n in NS} for b in ("strong_down", "mild_down", "up")}
    by_date: dict[str, dict[int, list[float]]] = {b: {n: defaultdict(list) for n in NS} for b in grp}

    with psycopg.connect(get_settings().database_url) as conn:
        with conn.cursor(name="bars") as cur:
            cur.itersize = 200000
            cur.execute("SELECT ts_code, trade_date, open, close FROM daily WHERE ts_code = ANY(%s) ORDER BY ts_code, trade_date", (list(events),))
            buf: list[tuple[str, float, float]] = []
            curts = None
            for ts, d, o, c in cur:
                if ts != curts:
                    if curts is not None:
                        _process(curts, buf, events, grp, by_date, bench_fwd)
                    curts, buf = ts, []
                if o is not None and c is not None:
                    buf.append((str(d), float(o), float(c)))
            if curts is not None:
                _process(curts, buf, events, grp, by_date, bench_fwd)

    def mean(xs):
        return round(100 * float(np.mean(xs)), 2) if xs else None

    def med(xs):
        return round(100 * float(np.median(xs)), 2) if xs else None

    print("\n## forward excess vs 中证500 (%), T+1 open -> close(T+1+N)  [mean / median]")
    print(f"  {'bucket':<12}" + "".join(f"{'N'+str(n):>16}" for n in NS) + "     n")
    for b in ("strong_down", "mild_down", "up"):
        cells = "".join(f"{str(mean(grp[b][n]))+'/'+str(med(grp[b][n])):>16}" for n in NS)
        print(f"  {b:<12}{cells}{len(grp[b][20]):>8}")
    print("  diff(sd-up) " + "".join(
        f"{(mean(grp['strong_down'][n]) or 0)-(mean(grp['up'][n]) or 0):>+16.2f}" for n in NS))

    print("\n## by window (excess_20: strong_down / up / diff)")
    win = {}
    for w, (s, en) in WINDOWS.items():
        def wm(b):
            vals = [v for d, vs in by_date[b][20].items() if s <= d <= en for v in vs]
            return mean(vals)
        win[w] = {b: wm(b) for b in ("strong_down", "up")}
        win[w]["diff"] = round((win[w]["strong_down"] or 0) - (win[w]["up"] or 0), 2)
        print(f"  {w:<10} strong_down {win[w]['strong_down']}  up {win[w]['up']}  diff {win[w]['diff']:+.2f}")

    wf = ["OOS2", "train", "valid"]
    k1 = (win["long"]["strong_down"] or -1) > 0 and sum(1 for w in wf if (win[w]["strong_down"] or -1) > 0) >= 2
    k2 = win["long"]["diff"] > 0 and sum(1 for w in wf if win[w]["diff"] > 0) >= 2
    k3 = bool(grp["strong_down"][20] and grp["mild_down"][20] and grp["up"][20]) and (
        (mean(grp["strong_down"][20]) or -99) > (mean(grp["mild_down"][20]) or -99) > (mean(grp["up"][20]) or -99))
    verdict = "OPEN concentration design" if (k1 and k2 and k3) else "REJECT / close line"
    print("\n## H-GDHS-A verdict\n")
    print(f"  K1 strong_down long>0 & >=2 windows -> {'pass' if k1 else 'FAIL'}")
    print(f"  K2 (sd-up) diff long>0 & >=2 windows -> {'pass' if k2 else 'FAIL'}")
    print(f"  K3 monotone sd>md>up (N20) -> {'pass' if k3 else 'FAIL'}")
    print(f"  => {verdict}")

    payload = {"tag": "gdhs-concentration-2026-09-12", "prereg": "docs/designs/gdhs-concentration-prereg-2026-09-12.md",
               "excess_pct": {b: {n: mean(grp[b][n]) for n in NS} for b in grp},
               "by_window": win, "n_events": n_ev,
               "verdict": {"k1": k1, "k2": k2, "k3": k3, "call": verdict},
               "as_of": datetime.now(UTC).isoformat(timespec="seconds")}
    if args.save_report:
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        (REPORT_DIR / "gdhs_2026-09-12.json").write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        print("\nsaved report")
    return 0


def _process(ts, buf, events, grp, by_date, bench_fwd):
    evs = events.get(ts)
    if not evs or not buf:
        return
    days = [b[0] for b in buf]
    o = {b[0]: b[1] for b in buf}
    c = {b[0]: b[2] for b in buf}
    for ann, chg in evs:
        idx = next((k for k, d in enumerate(days) if d > ann), None)
        if idx is None:
            continue
        t1 = days[idx]
        o1 = o[t1]
        if o1 <= 0:
            continue
        b = bucket(chg)
        for n in NS:
            if idx + n >= len(days):
                continue
            bf = bench_fwd(t1, n)
            if bf is None:
                continue
            ex = c[days[idx + n]] / o1 - 1.0 - bf
            grp[b][n].append(ex)
            by_date[b][n][days[idx + n]].append(ex)


if __name__ == "__main__":
    raise SystemExit(main())
