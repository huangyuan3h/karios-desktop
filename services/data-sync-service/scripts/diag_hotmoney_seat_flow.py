#!/usr/bin/env python3
"""H-FLOW-1 pre-registered diagnostic: seat-level LHB net-buy next-day decomposition.

Event = a CN stock on the daily 龙虎榜 with a positive net-buy from a seat group
(head hot-money / other hot-money / institution / Lhasa retail / Stock Connect).
Tradeable = T+1 open (LHB is disclosed after T close). We decompose forward
returns into gap (close(T) -> open(T+1)) and intraday (open(T+1) -> close(T+1))
plus a 3-day executable drift (open(T+1) -> close(T+3)).

Prereg & kill lines: docs/designs/hotmoney-anatomy-prereg-2026-09-15.md (E1)
Read-only, no parameter search.

Usage:
  cd services/data-sync-service
  PYTHONPATH=src python3 scripts/diag_hotmoney_seat_flow.py --save-report
"""

from __future__ import annotations

import argparse
import csv
import json
import math
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import psycopg

from hotmoney_lib import latest_adj, raw_price

ROOT = Path(__file__).resolve().parents[1]
SEATS = ROOT / "data" / "lhb" / "lhb_seats.csv"
LIMIT = ROOT / "data" / "limit" / "stk_limit.csv"
ETF = ROOT / "data" / "etf" / "etf_daily.csv"
REPORT_DIR = ROOT / "data" / "backtest_reports"
WINDOWS = {
    "OOS2": ("2024-08-01", "2025-08-01"),
    "train": ("2025-08-01", "2026-02-01"),
    "valid": ("2026-03-01", "2026-08-07"),
    "long": ("2021-01-01", "2026-08-07"),
}
WF = ("OOS2", "train", "valid")
BENCH = "510500.SH"
AMOUNT_FLOOR_QIAN = 70_000.0  # 0.7亿元 in tushare 千元
MIN_LIST_DAYS = 60
HEAD_K = 20
GROUPS = ("head", "hot_other", "inst", "lhasa", "hk", "control")


def _seat_group(name: str) -> str:
    n = str(name or "")
    if "机构专用" in n:
        return "inst"
    if "沪股通" in n or "深股通" in n:
        return "hk"
    if "拉萨" in n:
        return "lhasa"
    return "hot"


def _is_cn_stock(ts: str) -> bool:
    code = ts.split(".")[0]
    return ts.endswith((".SH", ".SZ")) and code[:2] in ("60", "00", "30", "68")


def _iso(d: str) -> str:
    s = str(d or "")
    if len(s) == 8 and s.isdigit():
        return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    return s[:10]


def _list_date_iso(raw: Any) -> str | None:
    s = str(raw or "").strip()
    if len(s) == 8 and s.isdigit():
        return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    return s[:10] if len(s) >= 10 else None


def _mean_pct(xs: list[float]) -> float | None:
    return round(100 * float(np.mean(xs)), 2) if xs else None


def _median_pct(xs: list[float]) -> float | None:
    return round(100 * float(np.median(xs)), 2) if xs else None


def _up_limit_proxy(pre_close: float, ts: str) -> float:
    pct = 0.20 if ts.split(".")[0][:2] in ("30", "68") else 0.10
    return math.floor(pre_close * (1.0 + pct) * 100.0 + 0.5) / 100.0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--save-report", action="store_true")
    args = ap.parse_args()

    if not SEATS.exists():
        raise SystemExit(f"missing {SEATS} (run scripts/sync_lhb_seats.py first)")

    # --- seats: aggregate net per (date, ts, seat) ---
    agg: dict[tuple[str, str, str], float] = defaultdict(float)
    with SEATS.open(encoding="utf-8", newline="") as fh:
        for r in csv.DictReader(fh):
            ts = str(r.get("ts_code") or "").strip()
            if not _is_cn_stock(ts):
                continue
            ex = str(r.get("exalter") or "").strip()
            if not ex:
                continue
            try:
                net = float(r.get("net_buy") or 0)
            except (TypeError, ValueError):
                continue
            agg[(str(r.get("trade_date") or "").strip(), ts, ex)] += net

    seat_events: dict[str, int] = defaultdict(int)
    seat_amount: dict[str, float] = defaultdict(float)
    for (_d, _ts, ex), net in agg.items():
        if net > 0 and _seat_group(ex) == "hot":
            seat_events[ex] += 1
            seat_amount[ex] += net
    ranked = sorted(seat_events, key=lambda s: (-seat_events[s], -seat_amount.get(s, 0.0)))
    head_seats = set(ranked[:HEAD_K])
    print(f"seats aggregated {len(agg)}, hot seats {len(seat_events)}, head K={HEAD_K}", flush=True)

    # --- per stock-day group nets ---
    day_net: dict[tuple[str, str], dict[str, float]] = defaultdict(lambda: defaultdict(float))
    for (d0, ts, ex), net in agg.items():
        g = _seat_group(ex)
        if g == "hot":
            g = "head" if ex in head_seats else "hot_other"
        day_net[(d0, ts)][g] += net
    codes = sorted({ts for (_d, ts) in day_net})
    print(f"stock-days {len(day_net)}, codes {len(codes)}", flush=True)

    # --- stock_basic / daily / limit / bench ---
    from data_sync_service.config import get_settings

    sb: dict[str, tuple[str, str | None, str | None]] = {}
    bars: dict[str, dict[str, tuple[float, float, float | None, float | None, float | None]]] = defaultdict(dict)
    adj_lat: dict[str, float] = {}
    with psycopg.connect(get_settings().database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT ts_code, name, list_date, delist_date FROM stock_basic "
                "WHERE ts_code = ANY(%s)",
                (codes,),
            )
            for ts, name, ld, dd in cur.fetchall():
                sb[str(ts)] = (str(name or ""), _list_date_iso(ld), _list_date_iso(dd))
            cur.execute(
                "SELECT ts_code, trade_date, open, close, amount, pre_close, adj_factor FROM daily "
                "WHERE ts_code = ANY(%s) ORDER BY ts_code, trade_date",
                (codes,),
            )
            for ts, d, o, c, amt, pre, adj in cur.fetchall():
                if o is None or c is None:
                    continue
                bars[str(ts)][str(d)] = (
                    float(o),
                    float(c),
                    float(amt) if amt is not None else None,
                    float(pre) if pre is not None else None,
                    float(adj) if adj is not None else None,
                )
        adj_lat = latest_adj(conn, codes)

    lim: dict[tuple[str, str], float] = {}
    if LIMIT.exists():
        with LIMIT.open(encoding="utf-8", newline="") as fh:
            for r in csv.DictReader(fh):
                try:
                    lim[(str(r["trade_date"]), str(r["ts_code"]))] = float(r["up_limit"])
                except (KeyError, TypeError, ValueError):
                    continue
    proxy_used = [0]

    bench: dict[str, tuple[float, float]] = {}
    with ETF.open(encoding="utf-8", newline="") as fh:
        for r in csv.DictReader(fh):
            if r.get("ts_code") != BENCH:
                continue
            d = _iso(str(r.get("trade_date") or ""))
            try:
                bench[d] = (float(r["open"]), float(r["close"]))
            except (KeyError, TypeError, ValueError):
                continue
    bdays = sorted(bench)
    bidx = {d: i for i, d in enumerate(bdays)}

    def bench_gap(t1: str, d0: str) -> float | None:
        if t1 not in bidx:
            return None
        j = next((k for k in range(bidx[t1] - 1, -1, -1) if bdays[k] <= d0), None)
        if j is None:
            return None
        bo = bench[t1][0]
        bc = bench[bdays[j]][1]
        return bo / bc - 1.0 if bo > 0 and bc > 0 else None

    def bench_d3(t1: str) -> float | None:
        if t1 not in bidx or bidx[t1] + 2 >= len(bdays):
            return None
        bo = bench[t1][0]
        bc = bench[bdays[bidx[t1] + 2]][1]
        return bc / bo - 1.0 if bo > 0 and bc > 0 else None

    # --- forward decomposition per group ---
    vals: dict[str, dict[str, list]] = {
        g: {"gap": [], "gap_ex": [], "intraday": [], "d3_ex": [], "d3": []} for g in GROUPS
    }
    unbuyable: dict[str, int] = defaultdict(int)
    by_date: dict[str, dict[str, list]] = {
        g: {"gap_ex": defaultdict(list), "d3_ex": defaultdict(list), "gap_ex_nb": defaultdict(list)}
        for g in GROUPS
    }
    overlap_head_inst = 0
    n_events = defaultdict(int)

    for (d0, ts), gnets in day_net.items():
        info = sb.get(ts)
        if info is None:
            continue
        name, ld, dd = info
        if "ST" in name or "退" in name:
            continue
        if ld is not None and ld > (datetime.fromisoformat(d0) - timedelta(days=MIN_LIST_DAYS)).date().isoformat():
            continue
        if dd is not None and dd < d0:
            continue
        stock_days = bars.get(ts, {})
        if d0 not in stock_days:
            continue
        amt0 = stock_days[d0][2]
        if amt0 is not None and amt0 < AMOUNT_FLOOR_QIAN:
            continue

        pos = {g for g in ("head", "hot_other", "inst", "lhasa", "hk") if gnets.get(g, 0.0) > 0}
        if pos:
            if "head" in pos and "inst" in pos:
                overlap_head_inst += 1
            assign = pos
        else:
            assign = {"control"}

        days = sorted(stock_days)
        idx = next((k for k, d in enumerate(days) if d > d0), None)
        if idx is None:
            continue
        t1 = days[idx]
        o1, c1, _, pre1, adj1 = stock_days[t1]
        c0 = stock_days[d0][1]
        if o1 <= 0 or c0 <= 0:
            continue
        gap = o1 / c0 - 1.0
        intraday = c1 / o1 - 1.0
        bg = bench_gap(t1, d0)
        d3 = None
        bd3 = bench_d3(t1)
        if idx + 2 < len(days):
            d3 = stock_days[days[idx + 2]][1] / o1 - 1.0
        raw_o1 = raw_price(o1, adj1, adj_lat.get(ts))
        raw_pre1 = raw_price(pre1, adj1, adj_lat.get(ts))
        up = lim.get((t1, ts))
        if up is None and raw_pre1 is not None and raw_pre1 > 0:
            up = _up_limit_proxy(raw_pre1, ts)
            proxy_used[0] += 1
        one_line = up is not None and raw_o1 is not None and raw_o1 >= up - 0.011

        for g in assign:
            n_events[g] += 1
            vals[g]["gap"].append(gap)
            vals[g]["intraday"].append(intraday)
            if bg is not None:
                vals[g]["gap_ex"].append(gap - bg)
                by_date[g]["gap_ex"][d0].append(gap - bg)
                if not one_line:
                    by_date[g]["gap_ex_nb"][d0].append(gap - bg)
            if d3 is not None:
                vals[g]["d3"].append(d3)
                if bd3 is not None:
                    vals[g]["d3_ex"].append(d3 - bd3)
                    by_date[g]["d3_ex"][d0].append(d3 - bd3)
            if one_line:
                unbuyable[g] += 1

    # --- report ---
    print("\n## E1 group means (%, T = LHB day, buy at T+1 open)")
    hdr = f"  {'group':<10}{'n':>7}{'gap':>8}{'gap_ex':>8}{'intraday':>9}{'d3':>8}{'d3_ex':>8}{'无买%':>7}"
    print(hdr)
    summary: dict[str, Any] = {}
    for g in GROUPS:
        n = n_events[g]
        row = {
            "n": n,
            "gap": _mean_pct(vals[g]["gap"]),
            "gap_ex": _mean_pct(vals[g]["gap_ex"]),
            "gap_median": _median_pct(vals[g]["gap"]),
            "intraday": _mean_pct(vals[g]["intraday"]),
            "d3": _mean_pct(vals[g]["d3"]),
            "d3_ex": _mean_pct(vals[g]["d3_ex"]),
            "unbuyable_pct": round(100 * unbuyable[g] / n, 2) if n else None,
        }
        summary[g] = row
        print(
            f"  {g:<10}{n:>7}{str(row['gap']):>8}{str(row['gap_ex']):>8}"
            f"{str(row['intraday']):>9}{str(row['d3']):>8}{str(row['d3_ex']):>8}"
            f"{str(row['unbuyable_pct']):>7}"
        )

    def win_mean(g: str, key: str, w: str) -> float | None:
        s, e = WINDOWS[w]
        flat = [v for d, vs in by_date[g][key].items() if s <= d <= e for v in vs]
        return _mean_pct(flat)

    print("\n## by window (excess vs 中证500, %)")
    win: dict[str, dict[str, Any]] = {}
    for w in WINDOWS:
        win[w] = {g: {"gap_ex": win_mean(g, "gap_ex", w), "d3_ex": win_mean(g, "d3_ex", w)} for g in GROUPS}
        print(
            f"  {w:<7} head gap {win[w]['head']['gap_ex']} / d3 {win[w]['head']['d3_ex']}"
            f" | inst gap {win[w]['inst']['gap_ex']} / d3 {win[w]['inst']['d3_ex']}"
        )

    def delta(w: str) -> float:
        h = win[w]["head"]["gap_ex"]
        i = win[w]["inst"]["gap_ex"]
        return round((h or 0.0) - (i or 0.0), 2)

    k1 = (win["long"]["head"]["gap_ex"] or -1) > 0 and sum(
        1 for w in WF if (win[w]["head"]["gap_ex"] or -1) > 0
    ) >= 2
    k2 = delta("long") > 0 and sum(1 for w in WF if delta(w) > 0) >= 2
    head_nb = {w: win_mean("head", "gap_ex_nb", w) for w in WINDOWS}
    head_intra = summary["head"]["intraday"] or 0.0
    inst_intra = summary["inst"]["intraday"] or 0.0
    k3 = (head_nb["long"] or -1) > 0 and sum(1 for w in WF if (head_nb[w] or -1) > 0) >= 2 and head_intra <= inst_intra
    verdict = "OPEN: 隔夜溢价 = 头部游资利润来源" if (k1 and k2 and k3) else "REJECT / close seam"
    print("\n## H-FLOW-1 verdict\n")
    print(f"  K1 head gap_ex long>0 & >=2/3 windows -> {'pass' if k1 else 'FAIL'}")
    print(f"  K2 (head-inst) gap_ex diff -> long {delta('long'):+.2f}, windows {[delta(w) for w in WF]} -> {'pass' if k2 else 'FAIL'}")
    print(
        f"  K3 剔除T+1一字 head gap_ex_nb {head_nb} & intraday asymmetry "
        f"(head {head_intra} vs inst {inst_intra}) -> {'pass' if k3 else 'FAIL'}"
    )
    print(f"  => {verdict}")
    print(
        f"  limit prices: {'exact' if LIMIT.exists() and proxy_used[0] == 0 else 'proxy/partial'}"
        f" (proxy events {proxy_used[0]})"
    )
    print("  note: head seats ranked on the full period (in-sample cohort by construction; descriptive, not a rule)")

    print(f"\n## head seats (top {HEAD_K} by positive-net events)")
    head_table = []
    for ex in ranked[:HEAD_K]:
        print(f"  {seat_events[ex]:>4}  {ex}  net={seat_amount[ex]/1e8:.1f}亿")

    payload = {
        "tag": "hotmoney-seat-flow-2026-09-15",
        "prereg": "docs/designs/hotmoney-anatomy-prereg-2026-09-15.md#e1--h-flow-1席位净买的次日分解gap-vs-盘中",
        "head_seats": ranked[:HEAD_K],
        "summary": summary,
        "by_window": win,
        "head_gap_ex_nb_by_window": {w: win_mean("head", "gap_ex_nb", w) for w in WINDOWS},
        "head_minus_inst_gap_ex": {w: delta(w) for w in WINDOWS},
        "overlap_head_inst_events": overlap_head_inst,
        "limit_source": "exact" if LIMIT.exists() and proxy_used[0] == 0 else "proxy/partial",
        "limit_proxy_events": proxy_used[0],
        "verdict": {"k1": k1, "k2": k2, "k3": k3, "call": verdict},
        "as_of": datetime.now(UTC).isoformat(timespec="seconds"),
    }
    if args.save_report:
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        out = REPORT_DIR / "hotmoney_seat_flow.json"
        out.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        print(f"\nsaved {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
