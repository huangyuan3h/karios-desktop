#!/usr/bin/env python3
"""H-BLK-B pre-registered diagnostic: block-trade premium vs big-discount,
momentum-neutralized (does the premium edge survive within momentum bins?).

Event = block trade (premium>0 or discount<=-5%). Tradeable = T+1 open.
Forward excess vs 中证500 (primary N=10). Momentum = close(T)/close(T-20)-1.

Prereg & kill lines: docs/designs/block-momneutral-prereg-2026-09-12.md
Read-only, no parameter search. Memory-lean (server-side cursor).

Usage:
  cd services/data-sync-service
  PYTHONPATH=src:scripts python3 scripts/diag_block_momneutral.py --save-report
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
PRIMARY = 10
BENCH = "510500.SH"
BINS = ("down", "mid", "up")


def group_of(prem: float) -> str | None:
    if prem > 0:
        return "premium"
    if prem <= -0.05:
        return "big_disc"
    return None


def mom_bin(m: float) -> str:
    if m < 0:
        return "down"
    if m < 0.10:
        return "mid"
    return "up"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--save-report", action="store_true")
    args = ap.parse_args()

    from data_sync_service.config import get_settings

    ev: dict[str, list[tuple[str, float]]] = defaultdict(list)
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
            ev[ts].append((iso, price))

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

    # cells[(group, bin, n)] = list[(date, ex)]
    cells: dict[tuple[str, str, int], list[tuple[str, float]]] = defaultdict(list)
    with psycopg.connect(get_settings().database_url) as conn:
        with conn.cursor(name="bars") as cur:
            cur.itersize = 200000
            cur.execute("SELECT ts_code, trade_date, open, close FROM daily WHERE ts_code = ANY(%s) ORDER BY ts_code, trade_date", (list(ev),))
            curts = None
            buf: list[tuple[str, float, float]] = []
            for ts, d, o, c in cur:
                if ts != curts:
                    if curts is not None:
                        _proc(curts, buf, ev, cells, bench_fwd)
                    curts, buf = ts, []
                if o is not None and c is not None:
                    buf.append((str(d), float(o), float(c)))
            if curts is not None:
                _proc(curts, buf, ev, cells, bench_fwd)

    def vals(g, b, n, s=None, e=None):
        return [x for d, x in cells[(g, b, n)] if (s is None or s <= d <= e)]

    def m(xs):
        return round(100 * float(np.mean(xs)), 2) if xs else None

    # momentum distribution (the confound)
    print(f"\n## momentum bin distribution (N{PRIMARY} samples)")
    print(f"  {'bin':<6}{'premium n':>12}{'big_disc n':>12}{'prem share':>12}{'disc share':>12}")
    tot_p = sum(len(vals("premium", b, PRIMARY)) for b in BINS)
    tot_d = sum(len(vals("big_disc", b, PRIMARY)) for b in BINS)
    for b in BINS:
        npn = len(vals("premium", b, PRIMARY))
        ndn = len(vals("big_disc", b, PRIMARY))
        sp = npn / tot_p if tot_p else 0
        sd = ndn / tot_d if tot_d else 0
        print(f"  {b:<6}{npn:>12}{ndn:>12}{sp:>12.1%}{sd:>12.1%}")

    # within-bin premium vs big_disc (primary N)
    print(f"\n## within-bin excess (%), N{PRIMARY}  [premium / big_disc / Δ / n]")
    for b in BINS:
        p = m(vals("premium", b, PRIMARY))
        dd = m(vals("big_disc", b, PRIMARY))
        delta = round(p - dd, 2) if p is not None and dd is not None else None
        print(f"  {b:<6} prem {p}  disc {dd}  Δ {delta}  n {len(vals('premium', b, PRIMARY))}/{len(vals('big_disc', b, PRIMARY))}")

    # by window: per-bin delta + n-weighted neutral delta
    print(f"\n## by window (N{PRIMARY}): per-bin Δ, then Δ_neutral")
    win: dict[str, dict] = {}
    for w, (s, e) in WINDOWS.items():
        wr: dict = {"bins": {}}
        num = 0.0
        den = 0
        for b in BINS:
            p = m(vals("premium", b, PRIMARY, s, e))
            dd = m(vals("big_disc", b, PRIMARY, s, e))
            nb = len(vals("premium", b, PRIMARY, s, e)) + len(vals("big_disc", b, PRIMARY, s, e))
            delta = round(p - dd, 2) if p is not None and dd is not None else None
            wr["bins"][b] = {"prem": p, "disc": dd, "delta": delta, "n": nb}
            if delta is not None and nb > 0:
                num += delta * nb
                den += nb
        wr["neutral"] = round(num / den, 2) if den else None
        win[w] = wr
        bins_str = "  ".join(f"{b}:{wr['bins'][b]['delta']}" for b in BINS)
        print(f"  {w:<8} {bins_str}   Δ_neutral {wr['neutral']}")

    wf = ["OOS2", "train", "valid"]
    k1 = (win["long"]["neutral"] or -99) > 0 and sum(1 for w in wf if (win[w]["neutral"] or -99) > 0) >= 2
    pos_bins_long = sum(1 for b in BINS if (win["long"]["bins"][b]["delta"] or -99) > 0)
    k2 = pos_bins_long >= 2
    verdict = "REVIVE (open engine replay)" if (k1 and k2) else "REJECT / close forever"
    print("\n## H-BLK-B verdict\n")
    print(f"  K1 Δ_neutral long>0 & >=2 windows -> {'pass' if k1 else 'FAIL'} (long {win['long']['neutral']})")
    print(f"  K2 >=2/3 bins Δ>0 in long -> {'pass' if k2 else 'FAIL'} ({pos_bins_long}/3)")
    print(f"  => {verdict}")

    payload = {"tag": "block-momneutral-2026-09-12",
               "prereg": "docs/designs/block-momneutral-prereg-2026-09-12.md",
               "mom_dist": {b: {"premium_n": len(vals("premium", b, PRIMARY)),
                                "big_disc_n": len(vals("big_disc", b, PRIMARY))} for b in BINS},
               "within_bin": {b: {"prem": m(vals("premium", b, PRIMARY)), "disc": m(vals("big_disc", b, PRIMARY))}
                              for b in BINS},
               "by_window": win,
               "verdict": {"k1": k1, "k2": k2, "pos_bins_long": pos_bins_long, "call": verdict},
               "as_of": datetime.now(UTC).isoformat(timespec="seconds")}
    if args.save_report:
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        (REPORT_DIR / "block_momneutral_2026-09-12.json").write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        print("\nsaved report")
    return 0


def _proc(ts, buf, ev, cells, bench_fwd):
    evs = ev.get(ts)
    if not evs or not buf:
        return
    days = [b[0] for b in buf]
    o = {b[0]: b[1] for b in buf}
    c = {b[0]: b[2] for b in buf}
    for date, price in evs:
        idx = bisect.bisect_left(days, date)
        if idx >= len(days) or days[idx] != date or idx < 20:
            continue
        close0 = c[date]
        if close0 <= 0 or c[days[idx - 20]] <= 0:
            continue
        g = group_of(price / close0 - 1.0)
        if g is None:
            continue
        b = mom_bin(close0 / c[days[idx - 20]] - 1.0)
        t1 = days[idx + 1] if idx + 1 < len(days) else None
        if t1 is None:
            continue
        o1 = o[t1]
        if o1 <= 0:
            continue
        for n in NS:
            if idx + 1 + n >= len(days):
                continue
            bf = bench_fwd(t1, n)
            if bf is None:
                continue
            ex = c[days[idx + 1 + n]] / o1 - 1.0 - bf
            cells[(g, b, n)].append((days[idx + 1 + n], ex))


if __name__ == "__main__":
    raise SystemExit(main())
