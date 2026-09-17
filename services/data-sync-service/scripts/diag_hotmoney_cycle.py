#!/usr/bin/env python3
"""H-CYCLE-1 pre-registered diagnostic: hot-money head events by sentiment phase.

Phase variables (all known at T close, rebuilt from daily + exact stk_limit):
  ylu_prem(T)   = mean return on T of stocks that hit limit-up on T-1
  lu_count(T)   = number of limit-up stocks on T
  med60(T)      = rolling 60d median of lu_count
  lu_max(T)     = max consecutive limit-up streak on T
  broken(T)     = share of limit-touching stocks that closed below the limit

Phases (frozen, no threshold search):
  ON  = ylu_prem > 0 AND lu_count >= med60
  OFF = ylu_prem < 0
  MID = otherwise

Events = head hot-money seat net-buy stock-days (same cohort as H-FLOW-1).
Forward = T+1 open gap excess vs 中证500 and T+1 open -> T+3 close.

Prereg & kill lines: docs/designs/hotmoney-anatomy-prereg-2026-09-15.md (E4)
Read-only, no parameter search.

Usage:
  cd services/data-sync-service
  PYTHONPATH=src python3 scripts/diag_hotmoney_cycle.py --save-report
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
import pandas as pd
import psycopg

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
HEAD_K = 20
PHASES = ("ON", "MID", "OFF")


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


def _up_limit_proxy(pre_close: float, ts: str) -> float:
    pct = 0.20 if ts.split(".")[0][:2] in ("30", "68") else 0.10
    return math.floor(pre_close * (1.0 + pct) * 100.0 + 0.5) / 100.0


def _mean_pct(xs: list[float] | np.ndarray | pd.Series) -> float | None:
    if xs is None or len(xs) == 0:
        return None
    return round(100 * float(np.mean(xs)), 2)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--save-report", action="store_true")
    args = ap.parse_args()

    if not SEATS.exists():
        raise SystemExit(f"missing {SEATS} (run scripts/sync_lhb_seats.py first)")

    # --- head seat cohort (same as H-FLOW-1) ---
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
                net[(str(r.get("trade_date") or "").strip(), ts, ex)] += float(r.get("net_buy") or 0)
            except (TypeError, ValueError):
                continue
    seat_events: dict[str, int] = defaultdict(int)
    seat_amount: dict[str, float] = defaultdict(float)
    for (_d, _ts, ex), v in net.items():
        if v > 0 and _seat_group(ex) == "hot":
            seat_events[ex] += 1
            seat_amount[ex] += v
    head_seats = set(sorted(seat_events, key=lambda s: (-seat_events[s], -seat_amount.get(s, 0.0)))[:HEAD_K])
    head_days: dict[tuple[str, str], float] = defaultdict(float)
    for (d0, ts, ex), v in net.items():
        if ex in head_seats and v > 0:
            head_days[(d0, ts)] += v
    event_codes = sorted({ts for (_d, ts) in head_days})
    print(f"head seats {len(head_seats)}, head stock-days {len(head_days)}, codes {len(event_codes)}", flush=True)

    # --- cycle variables from full daily panel + exact limits ---
    from data_sync_service.config import get_settings

    with psycopg.connect(get_settings().database_url) as conn:
        daily = pd.read_sql(
            "SELECT ts_code, trade_date, open, close, high, pre_close, adj_factor FROM daily "
            "WHERE trade_date >= '2021-01-01' AND trade_date <= '2026-09-11'",
            conn,
            dtype={"ts_code": str, "trade_date": str},
        )
        adj_latest = pd.read_sql(
            "SELECT DISTINCT ON (ts_code) ts_code, adj_factor AS adj_latest FROM daily "
            "ORDER BY ts_code, trade_date DESC",
            conn,
            dtype={"ts_code": str},
        )
    lim = pd.read_csv(
        LIMIT, dtype={"ts_code": str, "trade_date": str}, usecols=["trade_date", "ts_code", "up_limit"]
    )
    lim = lim[lim["trade_date"] >= "2021-01-01"]
    df = daily.merge(lim, on=["trade_date", "ts_code"], how="left").merge(
        adj_latest, on="ts_code", how="left"
    )
    # lesson 2026-09-15: daily is qfq; limits are raw -> reconstruct raw for limit checks
    for col, raw in (
        ("close", "raw_close"),
        ("high", "raw_high"),
        ("open", "raw_open"),
        ("pre_close", "raw_pre_close"),
    ):
        df[raw] = df[col] * df["adj_latest"] / df["adj_factor"]
    df["is_lu"] = df["up_limit"].notna() & (df["raw_close"] >= df["up_limit"] - 0.011)
    df["touched"] = df["up_limit"].notna() & (df["raw_high"] >= df["up_limit"] - 0.011)
    df["broken"] = df["touched"] & ~df["is_lu"]
    df["ret"] = df["close"] / df["pre_close"] - 1.0
    df = df.sort_values(["ts_code", "trade_date"]).reset_index(drop=True)

    lu = df["is_lu"].astype("int8")
    blk = (lu == 0).groupby(df["ts_code"]).cumsum()
    df["streak"] = lu.groupby([df["ts_code"], blk]).cumsum()
    df["prev_lu"] = df.groupby("ts_code")["is_lu"].shift(1).fillna(False)

    by_date = df.groupby("trade_date")
    cyc = pd.DataFrame(
        {
            "lu_count": by_date["is_lu"].sum(),
            "lu_max": df[df["is_lu"]].groupby("trade_date")["streak"].max(),
            "ylu_prem": df[df["prev_lu"]].groupby("trade_date")["ret"].mean(),
            "broken_rate": df[df["touched"]].groupby("trade_date")["broken"].mean(),
        }
    ).sort_index()
    cyc["med60"] = cyc["lu_count"].rolling(60, min_periods=20).median()
    cyc["lu5_chg"] = cyc["lu_count"] - cyc["lu_count"].shift(5)
    phase = np.where(
        (cyc["ylu_prem"] > 0) & (cyc["lu_count"] >= cyc["med60"]),
        "ON",
        np.where(cyc["ylu_prem"] < 0, "OFF", "MID"),
    )
    cyc["phase"] = phase
    print("phase counts:", cyc["phase"].value_counts().to_dict(), flush=True)

    # --- event bars (subset) + bench ---
    sub = df[df["ts_code"].isin(set(event_codes))]
    bars: dict[str, dict[str, tuple[Any, ...]]] = defaultdict(dict)
    for ts, d, o, c, up, raw_o, raw_pre in zip(
        sub["ts_code"],
        sub["trade_date"],
        sub["open"],
        sub["close"],
        sub["up_limit"],
        sub["raw_open"],
        sub["raw_pre_close"],
        strict=False,
    ):
        bars[str(ts)][str(d)] = (
            float(o) if o is not None else None,
            float(c),
            float(up) if up is not None else None,
            float(raw_o) if raw_o is not None else None,
            float(raw_pre) if raw_pre is not None else None,
        )

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

    def bench_gap(t1: str, d0: str) -> float | None:
        if t1 not in bidx:
            return None
        j = next((k for k in range(bidx[t1] - 1, -1, -1) if bdays[k] <= d0), None)
        if j is None:
            return None
        bo, bc = bench[t1][0], bench[bdays[j]][1]
        return bo / bc - 1.0 if bo > 0 and bc > 0 else None

    def bench_d3(t1: str) -> float | None:
        if t1 not in bidx or bidx[t1] + 2 >= len(bdays):
            return None
        bo, bc = bench[t1][0], bench[bdays[bidx[t1] + 2]][1]
        return bc / bo - 1.0 if bo > 0 and bc > 0 else None

    rows: list[dict[str, Any]] = []
    for (d0, ts) in head_days:
        p = cyc["phase"].get(d0)
        if p is None:
            continue
        sdays = bars.get(ts, {})
        if d0 not in sdays:
            continue
        days = sorted(sdays)
        idx = next((k for k, d in enumerate(days) if d > d0), None)
        if idx is None:
            continue
        t1 = days[idx]
        o1, c1, up1, raw_o1, raw_pre1 = sdays[t1]
        c0 = sdays[d0][1]
        if o1 is None or o1 <= 0 or c0 <= 0:
            continue
        gap = o1 / c0 - 1.0
        bg = bench_gap(t1, d0)
        d3 = None
        bd3 = bench_d3(t1)
        if idx + 2 < len(days):
            d3 = sdays[days[idx + 2]][1] / o1 - 1.0
        up = up1
        if up is None and raw_pre1 is not None and raw_pre1 > 0:
            up = _up_limit_proxy(raw_pre1, ts)
        one_line = up is not None and raw_o1 is not None and raw_o1 >= up - 0.011
        rows.append(
            {
                "d0": d0,
                "ts": ts,
                "phase": p,
                "gap_ex": None if bg is None else gap - bg,
                "d3_ex": None if (d3 is None or bd3 is None) else d3 - bd3,
                "one_line": one_line,
            }
        )
    ev = pd.DataFrame(rows)
    print(f"events with phase: {len(ev)} (one-line {int(ev['one_line'].sum())})", flush=True)

    # --- tables ---
    print("\n## head events by phase (%, excess vs 中证500)")
    summary: dict[str, Any] = {}
    for p in PHASES:
        e = ev[ev["phase"] == p]
        summary[p] = {
            "n": int(len(e)),
            "gap_ex": _mean_pct(e["gap_ex"].dropna()),
            "d3_ex": _mean_pct(e["d3_ex"].dropna()),
            "one_line_pct": round(100 * float(e["one_line"].mean()), 2) if len(e) else None,
        }
        print(
            f"  {p:<4} n={summary[p]['n']:>6} gap_ex {summary[p]['gap_ex']} d3_ex {summary[p]['d3_ex']} "
            f"一字 {summary[p]['one_line_pct']}"
        )

    def win_mean(p: str, col: str, w: str) -> float | None:
        s, e_ = WINDOWS[w]
        subw = ev[(ev["phase"] == p) & (ev["d0"] >= s) & (ev["d0"] <= e_)][col].dropna()
        return _mean_pct(subw)

    print("\n## by window: gap_ex (ON / MID / OFF) and d3_ex (ON)")
    win: dict[str, Any] = {}
    for w in WINDOWS:
        row = {
            "ON": win_mean("ON", "gap_ex", w),
            "MID": win_mean("MID", "gap_ex", w),
            "OFF": win_mean("OFF", "gap_ex", w),
            "ON_d3": win_mean("ON", "d3_ex", w),
            "OFF_d3": win_mean("OFF", "d3_ex", w),
        }
        row["ON_minus_OFF"] = (
            None if row["ON"] is None or row["OFF"] is None else round(row["ON"] - row["OFF"], 2)
        )
        win[w] = row
        print(f"  {w:<7} ON {row['ON']} MID {row['MID']} OFF {row['OFF']} | ON−OFF {row['ON_minus_OFF']} | ON d3 {row['ON_d3']}")

    e_long = ev[(ev["d0"] >= WINDOWS["long"][0]) & (ev["d0"] <= WINDOWS["long"][1])]
    pos = e_long[e_long["gap_ex"].notna() & (e_long["gap_ex"] > 0)]["gap_ex"].sum()
    pos_on = e_long[
        (e_long["phase"] == "ON") & e_long["gap_ex"].notna() & (e_long["gap_ex"] > 0)
    ]["gap_ex"].sum()
    share = round(100 * float(pos_on / pos), 1) if pos > 0 else None

    k1 = sum(1 for w in WF if (win[w]["ON_minus_OFF"] or -999) > 0) >= 2
    signs = [1 if (win[w]["ON_minus_OFF"] or 0) > 0 else -1 for w in WF]
    k2 = len(set(signs)) == 1 and share is not None and share > 50
    k3 = (win["long"]["ON"] or -1) > 0 and (win["long"]["MID"] or -1) > 0
    verdict = "OPEN: 相位解释力" if (k1 and k2 and k3) else "REJECT / phase does not explain"
    print("\n## H-CYCLE-1 verdict\n")
    print(f"  K1 ON−OFF gap_ex >0 in >=2/3 windows -> {[win[w]['ON_minus_OFF'] for w in WF]} -> {'pass' if k1 else 'FAIL'}")
    print(f"  K2 sign-consistent + ON share of positive gap >50% -> signs {signs} share {share}% -> {'pass' if k2 else 'FAIL'}")
    print(f"  K3 ON & MID long gap_ex both >0 -> ON {win['long']['ON']} MID {win['long']['MID']} -> {'pass' if k3 else 'FAIL'}")
    print(f"  => {verdict}")

    payload = {
        "tag": "hotmoney-cycle-2026-09-15",
        "prereg": "docs/designs/hotmoney-anatomy-prereg-2026-09-15.md",
        "phase_counts": cyc["phase"].value_counts().to_dict(),
        "summary": summary,
        "by_window": win,
        "on_share_of_positive_gap_long": share,
        "verdict": {"k1": k1, "k2": k2, "k3": k3, "call": verdict},
        "as_of": datetime.now(UTC).isoformat(timespec="seconds"),
    }
    if args.save_report:
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        out = REPORT_DIR / "hotmoney_cycle.json"
        out.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        print(f"\nsaved {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
