#!/usr/bin/env python3
"""H-BLK-A pre-registered diagnostic: block-trade premium / institutional seat.

Event = a block trade. Signal = premium vs close (price/close-1) and 机构专用
buyer/seller. Tradeable = T+1 open (block trades disclosed after T close).
Forward excess vs 中证500.

Prereg & kill lines: docs/designs/block-premium-prereg-2026-09-12.md
Read-only, no parameter search. Memory-lean (server-side cursor).

Usage:
  cd services/data-sync-service
  PYTHONPATH=src:scripts python3 scripts/diag_block_trade.py --save-report
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
import psycopg

ROOT = Path(__file__).resolve().parents[1]
BLK = ROOT / "data" / "block" / "block_trade.csv"
ETF = ROOT / "data" / "etf" / "etf_daily.csv"
REPORT_DIR = ROOT / "data" / "backtest_reports"
WINDOWS = {"OOS2": ("2024-08-01", "2025-08-01"), "train": ("2025-08-01", "2026-02-01"),
           "valid": ("2026-03-01", "2026-08-07"), "long": ("2021-01-01", "2026-08-07")}
NS = (5, 10, 20)
BENCH = "510500.SH"
GROUPS = ("premium", "mild_disc", "big_disc", "inst_buy", "inst_sell")


def bucket(prem: float) -> str:
    if prem > 0:
        return "premium"
    if prem <= -0.05:
        return "big_disc"
    return "mild_disc"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--save-report", action="store_true")
    args = ap.parse_args()

    from data_sync_service.config import get_settings

    ev: dict[str, list[tuple[str, float, str]]] = defaultdict(list)  # ts -> [(date, price, buyer)]
    with BLK.open() as fh:
        for r in csv.DictReader(fh):
            ts = r["ts_code"]
            if not str(ts).split(".")[0].startswith(("6", "0", "3")):
                continue
            try:
                price = float(r["price"])
            except (ValueError, KeyError):
                continue
            d = str(r["trade_date"])
            iso = f"{d[:4]}-{d[4:6]}-{d[6:8]}" if len(d) == 8 else d
            ev[ts].append((iso, price, str(r["buyer"]), str(r["seller"])))
    n_ev = sum(len(v) for v in ev.values())
    print(f"codes {len(ev)}, events {n_ev}", flush=True)

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
        i = bisect.bisect_left(bdays, d0)
        if i >= len(bdays) or i + n >= len(bdays):
            return None
        bo, bc = bench[bdays[i]][0], bench[bdays[i + n]][1]
        return bc / bo - 1.0 if bo > 0 else None

    grp: dict[str, dict[int, list[float]]] = {g: {n: [] for n in NS} for g in GROUPS}
    by_date: dict[str, list[float]] = {g: [] for g in GROUPS}
    by_date_d: dict[str, list[str]] = {g: [] for g in GROUPS}

    with psycopg.connect(get_settings().database_url) as conn:
        with conn.cursor(name="bars") as cur:
            cur.itersize = 200000
            cur.execute("SELECT ts_code, trade_date, open, close FROM daily WHERE ts_code = ANY(%s) ORDER BY ts_code, trade_date", (list(ev),))
            curts = None
            buf: list[tuple[str, float, float]] = []
            for ts, d, o, c in cur:
                if ts != curts:
                    if curts is not None:
                        _proc(curts, buf, ev, grp, by_date, by_date_d, bench_fwd, bdays)
                    curts, buf = ts, []
                if o is not None and c is not None:
                    buf.append((str(d), float(o), float(c)))
            if curts is not None:
                _proc(curts, buf, ev, grp, by_date, by_date_d, bench_fwd, bdays)

    def mean(xs):
        return round(100 * float(np.mean(xs)), 2) if xs else None

    def med(xs):
        return round(100 * float(np.median(xs)), 2) if xs else None

    print("\n## forward excess vs 中证500 (%), T+1 open -> close(T+1+N)  [mean / median]")
    print(f"  {'group':<11}" + "".join(f"{'N'+str(n):>16}" for n in NS) + "     n")
    for g in GROUPS:
        cells = "".join(f"{str(mean(grp[g][n]))+'/'+str(med(grp[g][n])):>16}" for n in NS)
        print(f"  {g:<11}{cells}{len(grp[g][10]):>8}")
    print("  prem-bigdisc " + "".join(
        f"{(mean(grp['premium'][n]) or 0)-(mean(grp['big_disc'][n]) or 0):>+16.2f}" for n in NS))
    print("  instbuy-sell " + "".join(
        f"{(mean(grp['inst_buy'][n]) or 0)-(mean(grp['inst_sell'][n]) or 0):>+16.2f}" for n in NS))

    def win(g, s, en, n=10):
        vals = [v for d, v in zip(by_date_d[g], by_date[g]) if s <= d <= en]
        return mean(vals)

    print("\n## by window (excess_10: premium / big_disc / inst_buy / inst_sell)")
    winres = {}
    for w, (s, en) in WINDOWS.items():
        wr = {g: win(g, s, en) for g in ("premium", "big_disc", "inst_buy", "inst_sell")}
        wr["prem_disc"] = round((wr["premium"] or 0) - (wr["big_disc"] or 0), 2)
        wr["ib_is"] = round((wr["inst_buy"] or 0) - (wr["inst_sell"] or 0), 2)
        winres[w] = wr
        print(f"  {w:<10} prem {wr['premium']}  bigdisc {wr['big_disc']}  Δ {wr['prem_disc']:+.2f} | "
              f"ibuy {wr['inst_buy']} isel {wr['inst_sell']} Δ {wr['ib_is']:+.2f}")

    wf = ["OOS2", "train", "valid"]
    k1 = (winres["long"]["premium"] or -1) > 0 and sum(1 for w in wf if (winres[w]["premium"] or -1) > 0) >= 2
    k2 = winres["long"]["prem_disc"] > 0 and sum(1 for w in wf if winres[w]["prem_disc"] > 0) >= 2
    k3 = winres["long"]["ib_is"] > 0 and sum(1 for w in wf if winres[w]["ib_is"] > 0) >= 2
    verdict = "OPEN block-trade design" if (k1 and k2 and k3) else "REJECT / close line"
    print("\n## H-BLK-A verdict\n")
    print(f"  K1 premium long>0 & >=2 windows -> {'pass' if k1 else 'FAIL'}")
    print(f"  K2 premium-bigdisc long>0 & >=2 windows -> {'pass' if k2 else 'FAIL'}")
    print(f"  K3 instbuy-instsell long>0 & >=2 windows -> {'pass' if k3 else 'FAIL'}")
    print(f"  => {verdict}")

    payload = {"tag": "block-premium-2026-09-12", "prereg": "docs/designs/block-premium-prereg-2026-09-12.md",
               "excess_pct": {g: {n: mean(grp[g][n]) for n in NS} for g in GROUPS},
               "median_pct": {g: {n: med(grp[g][n]) for n in NS} for g in GROUPS},
               "by_window": winres, "n_events": n_ev,
               "verdict": {"k1": k1, "k2": k2, "k3": k3, "call": verdict},
               "as_of": datetime.now(UTC).isoformat(timespec="seconds")}
    if args.save_report:
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        (REPORT_DIR / "block_premium_2026-09-12.json").write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        print("\nsaved report")
    return 0




def _proc(ts, buf, ev, grp, by_date, by_date_d, bench_fwd, bdays):
    evs = ev.get(ts)
    if not evs or not buf:
        return
    days = [b[0] for b in buf]
    o = {b[0]: b[1] for b in buf}
    c = {b[0]: b[2] for b in buf}
    for date, price, buyer, seller in evs:
        idx = bisect.bisect_left(days, date)
        if idx >= len(days) or days[idx] != date:
            continue
        close0 = c[date]
        if close0 <= 0:
            continue
        prem = price / close0 - 1.0
        t1 = days[idx + 1] if idx + 1 < len(days) else None
        if t1 is None:
            continue
        o1 = o[t1]
        if o1 <= 0:
            continue
        b = bucket(prem)
        ib = "机构专用" in buyer
        is_ = "机构专用" in seller
        for n in NS:
            if idx + 1 + n >= len(days):
                continue
            bf = bench_fwd(t1, n)
            if bf is None:
                continue
            ex = c[days[idx + 1 + n]] / o1 - 1.0 - bf
            grp[b][n].append(ex)
            by_date[b].append(ex); by_date_d[b].append(days[idx + 1 + n])
            if ib:
                grp["inst_buy"][n].append(ex); by_date["inst_buy"].append(ex); by_date_d["inst_buy"].append(days[idx + 1 + n])
            if is_:
                grp["inst_sell"][n].append(ex); by_date["inst_sell"].append(ex); by_date_d["inst_sell"].append(days[idx + 1 + n])


if __name__ == "__main__":
    raise SystemExit(main())
