#!/usr/bin/env python3
"""H-FLOW-2 pre-registered diagnostic: head hot-money seat timing.

For each head-seat positive net-buy event (T, X, S), track whether the SAME seat
reappears for X on T+1 (visible in the disclosed top seats):
  sold  = aggregated net(T+1) < 0  -> "overnight type" behaviour
  added = aggregated net(T+1) > 0  -> "relay type" behaviour
Then compare forward paths (T+1 open -> T+3 close) across seat types and against
institution seats' sell-through.

Prereg & kill lines: docs/designs/hotmoney-anatomy-prereg-2026-09-15.md (E2)
Read-only, no parameter search.

Usage:
  cd services/data-sync-service
  PYTHONPATH=src python3 scripts/diag_hotmoney_seat_timing.py --save-report
"""

from __future__ import annotations

import argparse
import csv
import json
import math
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np

from hotmoney_lib import latest_adj, raw_price

ROOT = Path(__file__).resolve().parents[1]
SEATS = ROOT / "data" / "lhb" / "lhb_seats.csv"
LIMIT = ROOT / "data" / "limit" / "stk_limit.csv"
REPORT_DIR = ROOT / "data" / "backtest_reports"
WINDOWS = {
    "OOS2": ("2024-08-01", "2025-08-01"),
    "train": ("2025-08-01", "2026-02-01"),
    "valid": ("2026-03-01", "2026-08-07"),
    "long": ("2021-01-01", "2026-08-07"),
}
WF = ("OOS2", "train", "valid")
HEAD_K = 20
EPS = 1.0
SELL_THROUGH_CUT = 0.5
ADD_ON_CUT = 0.3


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


def _up_limit_proxy(pre_close: float, ts: str) -> float:
    pct = 0.20 if ts.split(".")[0][:2] in ("30", "68") else 0.10
    return math.floor(pre_close * (1.0 + pct) * 100.0 + 0.5) / 100.0


def _mean_pct(xs: list[float]) -> float | None:
    return round(100 * float(np.mean(xs)), 2) if xs else None


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--save-report", action="store_true")
    args = ap.parse_args()

    if not SEATS.exists():
        raise SystemExit(f"missing {SEATS} (run scripts/sync_lhb_seats.py first)")

    net: dict[tuple[str, str, str], float] = defaultdict(float)
    with SEATS.open(encoding="utf-8", newline="") as fh:
        for r in csv.DictReader(fh):
            ts = str(r.get("ts_code") or "").strip()
            if not _is_cn_stock(ts):
                continue
            ex = str(r.get("exalter") or "").strip()
            if not ex:
                continue
            try:
                v = float(r.get("net_buy") or 0)
            except (TypeError, ValueError):
                continue
            net[(str(r.get("trade_date") or "").strip(), ts, ex)] += v

    seat_events: dict[str, int] = defaultdict(int)
    seat_amount: dict[str, float] = defaultdict(float)
    for (_d, _ts, ex), v in net.items():
        if v > 0 and _seat_group(ex) == "hot":
            seat_events[ex] += 1
            seat_amount[ex] += v
    head_seats = set(sorted(seat_events, key=lambda s: (-seat_events[s], -seat_amount.get(s, 0.0)))[:HEAD_K])
    print(f"head seats {len(head_seats)}, net cells {len(net)}", flush=True)

    from data_sync_service.config import get_settings
    import psycopg

    codes = sorted({ts for (_d, ts, ex) in net if ex in head_seats or _seat_group(ex) == "inst"})
    bars: dict[str, dict[str, tuple[float, float, float | None, float | None]]] = defaultdict(dict)
    adj_lat: dict[str, float] = {}
    with psycopg.connect(get_settings().database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT ts_code, trade_date, open, close, pre_close, adj_factor FROM daily "
                "WHERE ts_code = ANY(%s) ORDER BY ts_code, trade_date",
                (codes,),
            )
            for ts, d, o, c, pre, adj in cur.fetchall():
                if o is None or c is None:
                    continue
                bars[str(ts)][str(d)] = (
                    float(o),
                    float(c),
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

    events: list[dict[str, Any]] = []
    for (d0, ts, ex), v in net.items():
        if v <= 0:
            continue
        if ex not in head_seats and _seat_group(ex) != "inst":
            continue
        stock_days = bars.get(ts, {})
        if d0 not in stock_days:
            continue
        days = sorted(stock_days)
        idx = next((k for k, d in enumerate(days) if d > d0), None)
        if idx is None or idx + 2 >= len(days):
            continue
        t1 = days[idx]
        t2 = days[idx + 2]
        o1, _, pre1, adj1 = stock_days[t1]
        c3 = stock_days[t2][1]
        if o1 <= 0:
            continue
        d3 = c3 / o1 - 1.0
        raw_o1 = raw_price(o1, adj1, adj_lat.get(ts))
        raw_pre1 = raw_price(pre1, adj1, adj_lat.get(ts))
        up = lim.get((t1, ts))
        if up is None and raw_pre1 is not None and raw_pre1 > 0:
            up = _up_limit_proxy(raw_pre1, ts)
        one_line = up is not None and raw_o1 is not None and raw_o1 >= up - 0.011
        net1 = net.get((t1, ts, ex), 0.0)
        events.append(
            {
                "d0": d0,
                "ts": ts,
                "seat": ex,
                "is_head": ex in head_seats,
                "d3": d3,
                "one_line": one_line,
                "sold": net1 < -EPS,
                "added": net1 > EPS,
                "reappear": (t1, ts, ex) in net,
            }
        )
    print(f"events {len(events)}", flush=True)

    # seat typing over all its head events
    seat_tab: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for e in events:
        if e["is_head"]:
            seat_tab[e["seat"]].append(e)
    seat_type: dict[str, str] = {}
    for seat, evs in seat_tab.items():
        n = len(evs)
        st = sum(1 for e in evs if e["sold"]) / n
        ad = sum(1 for e in evs if e["added"]) / n
        seat_type[seat] = "overnight" if st >= SELL_THROUGH_CUT else ("relay" if ad >= ADD_ON_CUT else "mixed")

    def st_rate(evs: list[dict[str, Any]]) -> float | None:
        return round(100 * sum(1 for e in evs if e["sold"]) / len(evs), 1) if evs else None

    def win_vals(evs: list[dict[str, Any]], w: str) -> list[dict[str, Any]]:
        s, e_ = WINDOWS[w]
        return [x for x in evs if s <= x["d0"] <= e_]

    head_evs = [e for e in events if e["is_head"]]
    inst_evs = [e for e in events if not e["is_head"]]
    types = {t: [e for e in head_evs if seat_type.get(e["seat"]) == t] for t in ("overnight", "relay", "mixed")}

    print(f"\n## head seat types (cut st>={SELL_THROUGH_CUT} / add>={ADD_ON_CUT})")
    for t in ("overnight", "relay", "mixed"):
        print(f"  {t:<10} seats={sum(1 for s,v in seat_type.items() if v==t):>3} events={len(types[t]):>5} d3={_mean_pct([e['d3'] for e in types[t]])}")

    print("\n## sell-through (visible on T+1 seats, %)")
    nbc = [e for e in head_evs if not e["one_line"]]
    ibc = [e for e in inst_evs if not e["one_line"]]
    print(f"  head {st_rate(head_evs)} (n={len(head_evs)}) / inst {st_rate(inst_evs)} (n={len(inst_evs)})")
    print(f"  head ex-一字 {st_rate(nbc)} / inst ex-一字 {st_rate(ibc)}")

    print("\n## d3 (T+1 open -> T+3 close, %) by seat type and window")
    win: dict[str, Any] = {}
    for w in WINDOWS:
        row = {}
        for t in ("overnight", "relay", "mixed"):
            row[t] = _mean_pct([e["d3"] for e in win_vals(types[t], w)])
        row["head_all"] = _mean_pct([e["d3"] for e in win_vals(head_evs, w)])
        row["inst"] = _mean_pct([e["d3"] for e in win_vals(inst_evs, w)])
        win[w] = row
        print(f"  {w:<7} overnight {row['overnight']} relay {row['relay']} mixed {row['mixed']} | head {row['head_all']} inst {row['inst']}")

    # windows for sell-through
    st_win: dict[str, Any] = {}
    for w in WINDOWS:
        hv = win_vals(head_evs, w)
        iv = win_vals(inst_evs, w)
        st_win[w] = {"head": st_rate(hv), "inst": st_rate(iv)}
    k1 = sum(1 for w in WF if (st_win[w]["head"] or 0) > (st_win[w]["inst"] or 0)) >= 2
    k2 = sum(
        1 for w in WF if (win[w]["relay"] or -999) > (win[w]["overnight"] or 999)
    ) >= 2
    nb_head = [e for e in head_evs if not e["one_line"]]
    nb_inst = [e for e in inst_evs if not e["one_line"]]
    st_win_nb = {
        w: {
            "head": st_rate([e for e in nb_head if WINDOWS[w][0] <= e["d0"] <= WINDOWS[w][1]]),
            "inst": st_rate([e for e in nb_inst if WINDOWS[w][0] <= e["d0"] <= WINDOWS[w][1]]),
        }
        for w in WINDOWS
    }
    d3_nb = {
        w: {
            t: _mean_pct(
                [
                    e["d3"]
                    for e in types[t]
                    if not e["one_line"] and WINDOWS[w][0] <= e["d0"] <= WINDOWS[w][1]
                ]
            )
            for t in ("overnight", "relay")
        }
        for w in WINDOWS
    }
    k3a = sum(1 for w in WF if (st_win_nb[w]["head"] or 0) > (st_win_nb[w]["inst"] or 0)) >= 2
    k3b = sum(1 for w in WF if (d3_nb[w]["relay"] or -999) > (d3_nb[w]["overnight"] or 999)) >= 2
    k3 = k3a and k3b
    verdict = "OPEN: 席位分型存在" if (k1 and k2 and k3) else "REJECT / seats homogeneous"
    print("\n## H-FLOW-2 verdict\n")
    print(f"  K1 head sell-through > inst in >=2/3 windows -> {[st_win[w] for w in WF]} -> {'pass' if k1 else 'FAIL'}")
    print(f"  K2 relay d3 > overnight d3 in >=2/3 windows -> {'pass' if k2 else 'FAIL'}")
    print(f"  K3 剔除T+1一字 -> sell-through {'pass' if k3a else 'FAIL'}, d3 relay>overnight {'pass' if k3b else 'FAIL'}")
    print(f"  => {verdict}")

    payload = {
        "tag": "hotmoney-seat-timing-2026-09-15",
        "prereg": "docs/designs/hotmoney-anatomy-prereg-2026-09-15.md",
        "head_seats": sorted(head_seats),
        "seat_type": {s: seat_type[s] for s in sorted(seat_tab)},
        "sell_through": {"head": st_rate(head_evs), "inst": st_rate(inst_evs), "by_window": st_win},
        "sell_through_ex_one_line": st_win_nb,
        "d3_ex_one_line_by_type_window": d3_nb,
        "d3_by_type_window": win,
        "verdict": {"k1": k1, "k2": k2, "k3": k3, "call": verdict},
        "as_of": datetime.now(UTC).isoformat(timespec="seconds"),
    }
    if args.save_report:
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        out = REPORT_DIR / "hotmoney_seat_timing.json"
        out.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        print(f"\nsaved {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
