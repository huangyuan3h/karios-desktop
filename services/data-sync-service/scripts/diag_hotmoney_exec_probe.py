#!/usr/bin/env python3
"""H-EXEC-1 pre-registered probe: is any sliver executable?

E5a reopen-reseal: T day sealed before 14:30, board OPEN in [14:30,15:00) and
     resealed by close -> buy at the last genuinely-open bar close, sell T+1 open
     or T+3 close.
E5b quality next-day: T day top-quality seal (first seal <= 10:00 and tail
     sealed), T+1 open NOT at limit -> buy T+1 open, sell T+3 close.

Costs: 30bp round trip base (15/50 descriptive). Excess vs 中证500 (510500).
Prereg & kill lines: docs/designs/hotmoney-anatomy-prereg-2026-09-15.md (E5)
Read-only, no parameter search.

Usage:
  cd services/data-sync-service
  PYTHONPATH=src python3 scripts/diag_hotmoney_exec_probe.py --save-report
"""

from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import psycopg

from hotmoney_lib import latest_adj

ROOT = Path(__file__).resolve().parents[1]
LIMIT = ROOT / "data" / "limit" / "stk_limit.csv"
ETF = ROOT / "data" / "etf" / "etf_daily.csv"
REPORT_DIR = ROOT / "data" / "backtest_reports"
BENCH = "510500.SH"
DEFAULT_START, DEFAULT_END = "2024-01-01", "2026-09-11"
AMOUNT_FLOOR_QIAN = 70_000.0
MIN_LIST_DAYS = 60
MIN_BARS = 40
EPS_RAW = 0.011
TOPK_SEAL_MIN = 600  # 10:00
WINDOWS = {
    "OOS2": ("2024-08-01", "2025-08-01"),
    "train": ("2025-08-01", "2026-02-01"),
    "valid": ("2026-03-01", "2026-08-07"),
    "long": ("2021-01-01", "2026-08-07"),
}
WF = ("OOS2", "train", "valid")


def _is_cn_stock(ts: str) -> bool:
    code = ts.split(".")[0]
    return ts.endswith((".SH", ".SZ")) and code[:2] in ("60", "00", "30", "68")


def _iso(d: str) -> str:
    s = str(d or "")
    if len(s) == 8 and s.isdigit():
        return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    return s[:10]


def _list_date_iso(raw: Any) -> str | None:
    s = str(raw or "")
    if len(s) == 8 and s.isdigit():
        return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    return s[:10] if len(s) >= 10 else None


def _mean_pct(xs) -> float | None:
    xs = [x for x in xs if x is not None]
    return round(100 * float(np.mean(xs)), 2) if xs else None


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--start", default=DEFAULT_START)
    ap.add_argument("--end", default=DEFAULT_END)
    ap.add_argument("--save-report", action="store_true")
    args = ap.parse_args()
    start, end = args.start, args.end

    lim: dict[tuple[str, str], float] = {}
    with LIMIT.open(encoding="utf-8", newline="") as fh:
        for r in csv.DictReader(fh):
            d = str(r.get("trade_date") or "")
            if start <= d <= end:
                try:
                    lim[(d, str(r["ts_code"]))] = float(r["up_limit"])
                except (KeyError, TypeError, ValueError):
                    continue

    from data_sync_service.config import get_settings

    sb: dict[str, tuple[str, str | None]] = {}
    daily: dict[str, dict[str, tuple]] = defaultdict(dict)
    with psycopg.connect(get_settings().database_url) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT ts_code, name, list_date FROM stock_basic")
            for ts, name, ld in cur.fetchall():
                sb[str(ts)] = (str(name or ""), _list_date_iso(ld))
            cur.execute(
                "SELECT ts_code, trade_date, open, high, low, close, amount, adj_factor FROM daily "
                "WHERE trade_date >= %s AND trade_date <= %s",
                (start, end),
            )
            for ts, d, o, h, low, c, amt, adj in cur.fetchall():
                if o is None or c is None or not adj:
                    continue
                daily[str(ts)][str(d)] = (
                    float(o),
                    float(c),
                    float(amt) if amt is not None else None,
                    float(adj),
                )
        adj_latest = latest_adj(conn)
        bars: dict[tuple[str, str], list[tuple[str, float, float, float, float]]] = defaultdict(list)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT ts_code, trade_date, trade_time, open, high, low, close FROM bar_5min "
                "WHERE source IN ('ext_5min', 'baostock_full') "
                "AND trade_date >= %s AND trade_date <= %s",
                (start, end),
            )
            for ts, d, t, o, h, low, c in cur.fetchall():
                bars[(str(ts), str(d))].append(
                    (str(t), float(o) if o is not None else None, float(h), float(low), float(c))
                )
    for k in bars:
        bars[k].sort()

    bench: dict[str, tuple[float, float]] = {}
    with ETF.open(encoding="utf-8", newline="") as fh:
        for r in csv.DictReader(fh):
            if r.get("ts_code") != BENCH:
                continue
            try:
                bench[_iso(str(r.get("trade_date") or ""))] = (float(r["open"]), float(r["close"]))
            except (KeyError, TypeError, ValueError):
                continue
    bdays = sorted(bench)
    bidx = {d: i for i, d in enumerate(bdays)}

    def bench_ret(d_from: str, d_to: str, *, from_open: bool = False) -> float | None:
        if d_from not in bidx or d_to not in bidx:
            return None
        p0 = bench[d_from][0] if from_open else bench[d_from][1]
        p1 = bench[d_to][1]
        return p1 / p0 - 1.0 if p0 > 0 and p1 > 0 else None

    events_a: list[dict[str, Any]] = []
    events_b: list[dict[str, Any]] = []
    for ts, sdays in daily.items():
        if not _is_cn_stock(ts):
            continue
        name, ld = sb.get(ts, ("", None))
        if "ST" in name or "退" in name:
            continue
        adjl = adj_latest.get(ts)
        if not adjl:
            continue
        for d0 in sorted(sdays):
            up = lim.get((d0, ts))
            if up is None:
                continue
            o0, c0, amt0, adj0 = sdays[d0]
            raw_c0 = float(c0) * adjl / adj0
            if raw_c0 < up - EPS_RAW:
                continue
            if amt0 is not None and amt0 < AMOUNT_FLOOR_QIAN:
                continue
            if ld is not None and ld > (
                datetime.fromisoformat(d0) - timedelta(days=MIN_LIST_DAYS)
            ).date().isoformat():
                continue
            b = bars.get((ts, d0), [])
            if len(b) < MIN_BARS:
                continue
            sealed = [low >= up - 0.005 for (_t, _o, _h, low, _c) in b]
            times_min = [int(t[:2]) * 60 + int(t[2:]) for (t, *_rest) in b]
            first_seal = next((times_min[i] for i, s in enumerate(sealed) if s), None)
            tail_sealed = bool(sealed[-1])
            reopen = sum(1 for i in range(1, len(sealed)) if sealed[i - 1] and not sealed[i])
            days = sorted(sdays)
            idx = next((k for k, x in enumerate(days) if x > d0), None)
            if idx is None or idx + 2 >= len(days):
                continue
            t1, t3 = days[idx], days[idx + 2]
            o1, _c1, _amt1, adj1 = sdays[t1]
            raw_o1 = float(o1) * adjl / adj1
            up1 = lim.get((t1, ts))
            c1 = sdays[t1][1]
            c3 = sdays[t3][1]
            c0_ = float(c0)

            # E5a: sealed before 14:30, open window >= 14:30, resealed at close
            if (
                first_seal is not None
                and first_seal < 870
                and tail_sealed
                and reopen >= 1
            ):
                open_bars = [
                    (times_min[i], float(b[i][4]))
                    for i in range(len(b))
                    if times_min[i] >= 870 and not sealed[i] and b[i][4] <= up - 0.005
                ]
                if open_bars:
                    entry = open_bars[-1][1]
                    if entry > 0:
                        r_overnight = float(o1) / entry - 1.0
                        r_d3 = c3 / entry - 1.0
                        be = bench_ret(d0, t1, from_open=False)
                        bd = bench_ret(d0, t3, from_open=False)
                        events_a.append(
                            {
                                "d0": d0,
                                "n_overnight": r_overnight - 0.0030,
                                "n_d3": r_d3 - 0.0030,
                                "x_overnight": None if be is None else r_overnight - be - 0.0030,
                                "x_d3": None if bd is None else r_d3 - bd - 0.0030,
                                "x_d3_15": r_d3 - (bd or 0.0) - 0.0015 if bd is not None else None,
                                "x_d3_50": r_d3 - (bd or 0.0) - 0.0050 if bd is not None else None,
                                "amt0": amt0,
                            }
                        )

            # E5b: top-quality seal, buy T+1 open if not at limit
            if first_seal is not None and first_seal <= TOPK_SEAL_MIN and tail_sealed:
                if up1 is not None and raw_o1 < up1 - EPS_RAW and o1 > 0:
                    r_d3 = c3 / float(o1) - 1.0
                    bd = bench_ret(t1, t3, from_open=True)
                    events_b.append(
                        {
                            "d0": d0,
                            "n_d3": r_d3 - 0.0030,
                            "x_d3": None if bd is None else r_d3 - bd - 0.0030,
                            "x_d3_15": (r_d3 - bd - 0.0015) if bd is not None else None,
                            "x_d3_50": (r_d3 - bd - 0.0050) if bd is not None else None,
                            "amt0": amt0,
                        }
                    )
    print(f"E5a reopen-reseal events {len(events_a)} | E5b quality next-day events {len(events_b)}", flush=True)

    result: dict[str, Any] = {}
    for tag, evs in (("E5a_reopen_reseal", events_a), ("E5b_quality_nextday", events_b)):
        print(f"\n## {tag}")
        res: dict[str, Any] = {"n": len(evs)}
        for exit_key, label in (("x_overnight", "sell T+1 open"), ("x_d3", "sell T+3 close")):
            if exit_key == "x_overnight" and tag != "E5a_reopen_reseal":
                continue
            rows = [e for e in evs if e.get(exit_key) is not None]
            res[exit_key] = {
                "n": len(rows),
                "net_excess": _mean_pct([e[exit_key] for e in rows]),
            }
            print(f"  {label:<16} n={len(rows):>6} net excess {res[exit_key]['net_excess']}")
        for w in WINDOWS:
            s, e_ = WINDOWS[w]
            rows = [e for e in evs if s <= e["d0"] <= e_ and e["x_d3"] is not None]
            if not rows:
                continue
            res.setdefault("by_window", {})[w] = {
                "n": len(rows),
                "net_d3": _mean_pct([e["n_d3"] for e in rows]),
                "excess_d3": _mean_pct([e["x_d3"] for e in rows]),
                "excess_d3_15bp": _mean_pct([e["x_d3_15"] for e in rows]),
                "excess_d3_50bp": _mean_pct([e["x_d3_50"] for e in rows]),
            }
            print(
                f"  {w:<7} n={len(rows):>6} net {res['by_window'][w]['net_d3']} "
                f"excess {res['by_window'][w]['excess_d3']} (15bp {res['by_window'][w]['excess_d3_15bp']} / "
                f"50bp {res['by_window'][w]['excess_d3_50bp']})"
            )
        # K3: drop smallest amount tercile
        amts = sorted([e["amt0"] for e in evs if e["amt0"] is not None])
        if amts:
            cut = amts[len(amts) // 3]
            kept = [e for e in evs if e["amt0"] is not None and e["amt0"] > cut]
            res["k3_large_amount_only"] = {
                w: _mean_pct(
                    [e["x_d3"] for e in kept if WINDOWS[w][0] <= e["d0"] <= WINDOWS[w][1] and e["x_d3"] is not None]
                )
                for w in WINDOWS
            }
            print(f"  K3 剔最小额三分位后 excess_d3: {res['k3_large_amount_only']}")
        result[tag] = res

    print("\n## H-EXEC-1 verdict")
    for tag in ("E5a_reopen_reseal", "E5b_quality_nextday"):
        res = result[tag]
        bw = res.get("by_window", {})
        wf_ex = [(bw.get(w, {}).get("excess_d3")) for w in WF]
        wf_net = [(bw.get(w, {}).get("net_d3")) for w in WF]
        k1 = (bw.get("long", {}).get("net_d3") or -9) > 0 and sum(
            1 for v in wf_net if v is not None and v > 0
        ) >= 2
        k2 = (bw.get("long", {}).get("excess_d3") or -9) > 0 and sum(
            1 for v in wf_ex if v is not None and v > 0
        ) >= 2
        long_k3 = res.get("k3_large_amount_only", {}).get("long")
        k3 = (long_k3 or -9) > 0
        call = "OPEN (executable sliver)" if (k1 and k2 and k3) else "REJECT"
        print(f"  {tag}: K1 {k1} ({wf_net}) K2 {k2} ({wf_ex}) K3 {k3} -> {call}")
        res["verdict"] = {"k1": k1, "k2": k2, "k3": k3, "call": call}

    payload = {
        "tag": "hotmoney-exec-probe-2026-09-15",
        "prereg": "docs/designs/hotmoney-anatomy-prereg-2026-09-15.md",
        "range": [start, end],
        "results": result,
        "as_of": datetime.now(UTC).isoformat(timespec="seconds"),
    }
    if args.save_report:
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        out = REPORT_DIR / "hotmoney_exec_probe.json"
        out.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        print(f"\nsaved {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
