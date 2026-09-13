#!/usr/bin/env python3
"""H-BLK-C pre-registered replay: premium block-trade long portfolio (daily EW).

Signal: a premium block trade (prem>0) on day T -> buy next session open, hold
N=10 sessions, sell at close. Daily-equal-weight across active names, dedup
(no re-entry while holding). Benchmark = 中证500 (510500 ETF). Cost 15bp/side.

Prereg & kill lines: docs/designs/block-premium-replay-prereg-2026-09-12.md
Read-only, no parameter search. Memory-lean (server-side cursor).

Usage:
  cd services/data-sync-service
  PYTHONPATH=src:scripts python3 scripts/replay_block_premium.py --save-report
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
N = 10
COST_SIDE = 0.0015
BENCH = "510500.SH"
GROUPS = ("premium", "big_disc")
BDAYS_PER_YEAR = 242


def group_of(prem: float) -> str | None:
    if prem > 0:
        return "premium"
    if prem <= -0.05:
        return "big_disc"
    return None


def _metrics(rets: list[float]) -> dict:
    if not rets:
        return {"n": 0, "ann": None, "sharpe": None, "mdd": None}
    arr = np.asarray(rets, dtype=float)
    nav = float(np.prod(1.0 + arr))
    ann = nav ** (BDAYS_PER_YEAR / len(arr)) - 1.0 if nav > 0 else None
    sd = float(np.std(arr, ddof=1)) if len(arr) > 1 else 0.0
    sharpe = float(np.mean(arr) / sd * np.sqrt(BDAYS_PER_YEAR)) if sd > 0 else None
    cum = np.cumprod(1.0 + arr)
    peak = np.maximum.accumulate(cum)
    mdd = float(np.min(cum / peak - 1.0))
    return {"n": len(arr), "ann": round(100 * ann, 2) if ann is not None else None,
            "sharpe": round(sharpe, 2) if sharpe is not None else None,
            "mdd": round(100 * mdd, 2)}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--save-report", action="store_true")
    args = ap.parse_args()

    from data_sync_service.config import get_settings

    ev: dict[str, list[tuple[str, float]]] = defaultdict(list)  # ts -> [(date, block_price)]
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
    print(f"codes {len(ev)} events {sum(len(v) for v in ev.values())}", flush=True)

    bench: dict[str, float] = {}
    with ETF.open() as fh:
        for r in csv.DictReader(fh):
            if r["ts_code"] != BENCH:
                continue
            d = str(r["trade_date"])
            iso = f"{d[:4]}-{d[4:6]}-{d[6:8]}" if len(d) == 8 else d
            bench[iso] = float(r["close"])
    bdays = sorted(bench)

    def bench_ret_series() -> dict[str, float]:
        out: dict[str, float] = {}
        for i in range(1, len(bdays)):
            p, c = bench[bdays[i - 1]], bench[bdays[i]]
            out[bdays[i]] = c / p - 1.0 if p > 0 else 0.0
        return out

    bret = bench_ret_series()

    # port[group][day] = list of position daily returns; new[group][day] = #new entries
    port: dict[str, dict[str, list[float]]] = {g: defaultdict(list) for g in GROUPS}
    newc: dict[str, dict[str, int]] = {g: defaultdict(int) for g in GROUPS}

    with psycopg.connect(get_settings().database_url) as conn:
        with conn.cursor(name="bars") as cur:
            cur.itersize = 200000
            cur.execute("SELECT ts_code, trade_date, open, close FROM daily WHERE ts_code = ANY(%s) ORDER BY ts_code, trade_date", (list(ev),))
            curts = None
            buf: list[tuple[str, float, float]] = []
            for ts, d, o, c in cur:
                if ts != curts:
                    if curts is not None:
                        _proc(curts, buf, ev, port, newc, N)
                    curts, buf = ts, []
                if o is not None and c is not None:
                    buf.append((str(d), float(o), float(c)))
            if curts is not None:
                _proc(curts, buf, ev, port, newc, N)

    # daily portfolio series over bench calendar
    def series(g: str) -> dict[str, float]:
        out: dict[str, float] = {}
        for d in bdays:
            pos = port[g].get(d)
            if not pos:
                continue
            r = float(np.mean(pos))
            active = len(pos)
            nw = newc[g].get(d, 0)
            turn = min(1.0, nw / active) if active else 0.0
            out[d] = r - turn * 2 * COST_SIDE
        return out

    s_prem = series("premium")
    s_disc = series("big_disc")

    def in_win(d: str, w: tuple[str, str]) -> bool:
        return w[0] <= d <= w[1]

    print("\n## portfolio vs 中证500 (--) / 0-cost (net)  [ann% / sharpe / mdd%]")
    result: dict[str, dict] = {}
    for w, (s, e) in WINDOWS.items():
        wr: dict = {}
        bw = [bret[d] for d in bdays if in_win(d, (s, e)) and d in bret]
        wr["bench"] = _metrics(bw)
        for g, sr in (("premium", s_prem), ("big_disc", s_disc)):
            rw = [sr[d] for d in bdays if in_win(d, (s, e)) and d in sr]
            wr[g] = _metrics(rw)
            # excess vs bench on common days, arithmetic-annualized
            common = [d for d in bdays if in_win(d, (s, e)) and d in sr and d in bret]
            exc = [sr[d] - bret[d] for d in common]
            wr[g + "_excess_ann"] = round(100 * float(np.mean(exc)) * BDAYS_PER_YEAR, 2) if exc else None
            wr[g + "_hit"] = round(100 * float(np.mean([1 if x > 0 else 0 for x in exc])), 1) if exc else None
        result[w] = wr
        print(f"  {w:<7} bench {wr['bench']['ann']}/{wr['bench']['sharpe']}/{wr['bench']['mdd']} | "
              f"prem {wr['premium']['ann']}/{wr['premium']['sharpe']}/{wr['premium']['mdd']} "
              f"exc {wr['premium_excess_ann']} hit {wr['premium_hit']} | "
              f"disc {wr['big_disc']['ann']}/{wr['big_disc']['sharpe']}")

    # breadth
    print("\n## effective breadth (long window)")
    lw = WINDOWS["long"]
    active_counts = []
    for d in bdays:
        if in_win(d, lw) and d in s_prem:
            active_counts.append(len(port["premium"][d]))
    n_days = sum(1 for d in bdays if in_win(d, lw))
    days_with_pos = len(active_counts)
    avg_active = float(np.mean(active_counts)) if active_counts else 0.0
    print(f"  trading days {n_days} · days with position {days_with_pos} ({days_with_pos/n_days:.0%}) · "
          f"avg active {avg_active:.1f}")

    # verdict
    covers = avg_active >= 5
    wf = ["OOS2", "train", "valid"]
    k1 = (result["long"]["premium_excess_ann"] or -99) > 0 and sum(
        1 for w in wf if (result[w]["premium_excess_ann"] or -99) > 0) >= 2
    k2 = ((result["long"]["premium"]["ann"] or -99) > (result["long"]["big_disc"]["ann"] or 99)) and (
        (result["long"]["premium"]["sharpe"] or -99) > (result["long"]["big_disc"]["sharpe"] or 99)) and (
        sum(1 for w in wf if (result[w]["premium"]["sharpe"] or -99) > (result[w]["big_disc"]["sharpe"] or 99)) >= 2)
    k3 = (result["long"]["premium"]["mdd"] or -999) >= (result["long"]["bench"]["mdd"] or -999) - 10.0
    verdict = "CANDIDATE sleeve (not auto-Live)" if (covers and k1 and k2 and k3) else "REJECT / park"
    print("\n## H-BLK-C verdict\n")
    print(f"  coverage avg active>=5 -> {'pass' if covers else 'FAIL'} ({avg_active:.1f})")
    print(f"  K1 premium excess long>0 & >=2 windows -> {'pass' if k1 else 'FAIL'} "
          f"(long {result['long']['premium_excess_ann']})")
    print(f"  K2 prem>disc ann & sharpe -> {'pass' if k2 else 'FAIL'}")
    print(f"  K3 prem mdd not worse than bench-10pt -> {'pass' if k3 else 'FAIL'} "
          f"({result['long']['premium']['mdd']} vs {result['long']['bench']['mdd']})")
    print(f"  => {verdict}")

    payload = {"tag": "block-premium-replay-2026-09-12",
               "prereg": "docs/designs/block-premium-replay-prereg-2026-09-12.md",
               "by_window": result, "avg_active": round(avg_active, 2),
               "days_with_pos": days_with_pos, "n_days": n_days,
               "verdict": {"covers": covers, "k1": k1, "k2": k2, "k3": k3, "call": verdict},
               "as_of": datetime.now(UTC).isoformat(timespec="seconds")}
    if args.save_report:
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        (REPORT_DIR / "block_premium_replay_2026-09-12.json").write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        print("\nsaved report")
    return 0


def _proc(ts, buf, ev, port, newc, hold_n):
    evs = ev.get(ts)
    if not evs or not buf:
        return
    days = [b[0] for b in buf]
    o = {b[0]: b[1] for b in buf}
    c = {b[0]: b[2] for b in buf}
    last_exit = -1
    for date, price in sorted(evs):
        idx = bisect.bisect_left(days, date)
        if idx >= len(days) or days[idx] != date:
            continue
        close0 = c[days[idx]]
        if close0 <= 0:
            continue
        g = group_of(price / close0 - 1.0)
        if g is None:
            continue
        entry = idx + 1
        exit_ = idx + 1 + hold_n
        if entry <= last_exit:  # dedup: already holding
            continue
        if exit_ >= len(days):
            continue
        o1 = o[days[entry]]
        if o1 <= 0:
            continue
        for k in range(hold_n):
            dd = days[entry + k]
            if k == 0:
                r = c[dd] / o1 - 1.0
            else:
                p = c[days[entry + k - 1]]
                r = c[dd] / p - 1.0 if p > 0 else 0.0
            port[g][dd].append(r)
        newc[g][days[entry]] += 1
        last_exit = exit_


if __name__ == "__main__":
    raise SystemExit(main())
