#!/usr/bin/env python3
"""H-BOARD-1 diagnostic: limit-up board quality from full-day 5min.

Phase 1 (2024-2026) = descriptive; phase 2 (2021+, needs baostock_full bars) =
three-window verdict per prereg.

Lesson 2026-09-15: daily stores qfq prices; stk_limit / bar_5min are RAW.
Limit checks reconstruct raw = qfq x adj_latest / adj (hotmoney_lib).

Prereg & kill lines: docs/designs/hotmoney-anatomy-prereg-2026-09-15.md (E3)
Read-only, no parameter search.

Usage:
  cd services/data-sync-service
  PYTHONPATH=src python3 scripts/diag_limitup_board_quality.py --save-report
  PYTHONPATH=src python3 scripts/diag_limitup_board_quality.py --start 2021-01-01 --verdict --save-report
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


def _spearman(xs: list[float], ys: list[float]) -> float | None:
    if len(xs) < 30:
        return None
    rx = np.argsort(np.argsort(xs))
    ry = np.argsort(np.argsort(ys))
    if float(np.std(rx)) == 0 or float(np.std(ry)) == 0:
        return None
    return round(float(np.corrcoef(rx, ry)[0, 1]), 3)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--start", default=DEFAULT_START)
    ap.add_argument("--end", default=DEFAULT_END)
    ap.add_argument("--verdict", action="store_true", help="phase-2 three-window gates")
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
                "SELECT ts_code, trade_date, open, high, low, close, amount, adj_factor, pre_close "
                "FROM daily WHERE trade_date >= %s AND trade_date <= %s",
                (start, end),
            )
            for ts, d, o, h, low, c, amt, adj, pre in cur.fetchall():
                if o is None or c is None or h is None or low is None or not adj:
                    continue
                daily[str(ts)][str(d)] = (
                    float(o),
                    float(h),
                    float(low),
                    float(c),
                    float(amt) if amt is not None else None,
                    float(adj),
                    float(pre) if pre is not None else None,
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

    events: list[dict[str, Any]] = []
    skipped_bars = 0
    for ts, sdays in daily.items():
        if not _is_cn_stock(ts):
            continue
        name, ld = sb.get(ts, ("", None))
        if "ST" in name or "退" in name:
            continue
        adjl = adj_latest.get(ts)
        for d0 in sorted(sdays):
            up = lim.get((d0, ts))
            if up is None:
                continue
            o0, h0, _l0, c0, amt0, adj0, _pre0 = sdays[d0]
            raw_c0 = float(c0) * adjl / adj0 if adjl else None
            if raw_c0 is None or raw_c0 < up - EPS_RAW:
                continue
            if amt0 is not None and amt0 < AMOUNT_FLOOR_QIAN:
                continue
            if ld is not None and ld > (
                datetime.fromisoformat(d0) - timedelta(days=MIN_LIST_DAYS)
            ).date().isoformat():
                continue
            b = bars.get((ts, d0), [])
            if len(b) < MIN_BARS:
                skipped_bars += 1
                continue
            sealed = [low >= up - 0.005 for (_t, _o, _h, low, _c) in b]
            touched = [h >= up - 0.005 for (_t, _o, h, _l, _c) in b]
            if not any(touched):
                continue
            times_min = [int(t[:2]) * 60 + int(t[2:]) for (t, *_rest) in b]
            first_seal = next((times_min[i] for i, s in enumerate(sealed) if s), None)
            reopen = sum(1 for i in range(1, len(sealed)) if sealed[i - 1] and not sealed[i])
            sealed_share = float(np.mean(sealed))
            tail_sealed = bool(sealed[-1])
            days = sorted(sdays)
            idx = next((k for k, x in enumerate(days) if x > d0), None)
            if idx is None:
                continue
            t1 = days[idx]
            o1, _h1, _l1, c1, _amt1, adj1, pre1 = sdays[t1]
            if o1 <= 0 or c0 <= 0:
                continue
            up1 = lim.get((t1, ts))
            raw_o1 = float(o1) * adjl / adj1 if adjl else None
            gap = o1 / c0 - 1.0
            bg = bench_gap(t1, d0)
            d3 = None
            bd3 = bench_d3(t1)
            if idx + 2 < len(days):
                d3 = sdays[days[idx + 2]][3] / o1 - 1.0
            events.append(
                {
                    "ts": ts,
                    "d0": d0,
                    "year": d0[:4],
                    "first_seal_min": first_seal,
                    "sealed_share": sealed_share,
                    "reopen": reopen,
                    "tail_sealed": tail_sealed,
                    "gap_ex": None if bg is None else gap - bg,
                    "unbuyable": up1 is not None and raw_o1 is not None and raw_o1 >= up1 - EPS_RAW,
                    "intraday": c1 / o1 - 1.0,
                    "d3_ex": None if (d3 is None or bd3 is None) else d3 - bd3,
                }
            )
    print(
        f"limit-up events {len(events)} (skipped, <{MIN_BARS} bars: {skipped_bars}) range {start}..{end}",
        flush=True,
    )

    def table(rows: list[dict[str, Any]], label: str) -> dict[str, Any]:
        n = len(rows)
        out = {
            "n": n,
            "gap_ex": _mean_pct([r["gap_ex"] for r in rows]),
            "unbuyable_pct": round(100 * float(np.mean([r["unbuyable"] for r in rows])), 2) if n else None,
            "intraday": _mean_pct([r["intraday"] for r in rows]),
            "d3_ex": _mean_pct([r["d3_ex"] for r in rows]),
        }
        print(
            f"  {label:<24} n={n:>6} gap_ex {out['gap_ex']} 无买 {out['unbuyable_pct']}% "
            f"intraday {out['intraday']} d3_ex {out['d3_ex']}"
        )
        return out

    result: dict[str, Any] = {"range": [start, end], "events": len(events), "skipped_bars": skipped_bars}

    if not args.verdict:
        print("\n## phase-1 descriptive: board-quality terciles (excess vs 中证500)")
        for feat, label in (("first_seal_min", "first_seal(早→晚)"), ("sealed_share", "sealed_share(低→高)")):
            vals = [r[feat] for r in events if r[feat] is not None]
            if not vals:
                continue
            q1, q2 = np.quantile(vals, [1 / 3, 2 / 3])
            groups = {
                "T1": [r for r in events if r[feat] is not None and r[feat] <= q1],
                "T2": [r for r in events if r[feat] is not None and q1 < r[feat] <= q2],
                "T3": [r for r in events if r[feat] is not None and r[feat] > q2],
            }
            print(f"\n### {label} (cuts {round(float(q1), 3)} / {round(float(q2), 3)})")
            result[feat] = {g: table(rows, f"{feat}:{g}") for g, rows in groups.items()}
            xs = [r[feat] for r in events if r[feat] is not None and r["gap_ex"] is not None]
            ys = [r["gap_ex"] for r in events if r[feat] is not None and r["gap_ex"] is not None]
            result[feat]["spearman_vs_gap"] = _spearman(xs, ys)
            print(f"  spearman({feat}, gap_ex) = {result[feat]['spearman_vs_gap']}")

        print("\n### reopen buckets")
        result["reopen"] = {}
        for gname, sel in (
            ("0", lambda r: r["reopen"] == 0),
            ("1-2", lambda r: 1 <= r["reopen"] <= 2),
            ("3+", lambda r: r["reopen"] >= 3),
        ):
            result["reopen"][gname] = table([r for r in events if sel(r)], f"reopen:{gname}")

        print("\n### tail_sealed")
        result["tail_sealed"] = {}
        for gname, sel in (("yes", lambda r: r["tail_sealed"]), ("no", lambda r: not r["tail_sealed"])):
            result["tail_sealed"][gname] = table([r for r in events if sel(r)], f"tail_sealed:{gname}")

        print("\n### by year (all events)")
        result["by_year"] = {}
        for y in sorted({r["year"] for r in events}):
            result["by_year"][y] = table([r for r in events if r["year"] == y], y)
    else:
        # phase-2 frozen cuts: global terciles; expected directions:
        #   first_seal_min: G1 (earliest) > G2 > G3 ; sealed_share: G3 (highest) > G2 > G1
        cuts: dict[str, tuple[float, float]] = {}
        for feat in ("first_seal_min", "sealed_share"):
            vals = [r[feat] for r in events if r[feat] is not None]
            q1, q2 = np.quantile(vals, [1 / 3, 2 / 3])
            cuts[feat] = (float(q1), float(q2))
        print(f"\n## phase-2 gate: global cuts {cuts}")

        def group_rows(feat: str, g: str, rows: list[dict[str, Any]], clean: bool = False) -> list[dict[str, Any]]:
            q1, q2 = cuts[feat]
            base = [r for r in rows if r[feat] is not None and (not clean or not r["unbuyable"])]
            if g == "G1":
                return [r for r in base if r[feat] <= q1]
            if g == "G2":
                return [r for r in base if q1 < r[feat] <= q2]
            return [r for r in base if r[feat] > q2]

        def monotone(feat: str, rows: list[dict[str, Any]], clean: bool = False) -> bool | None:
            m1 = _mean_pct([r["gap_ex"] for r in group_rows(feat, "G1", rows, clean)])
            m2 = _mean_pct([r["gap_ex"] for r in group_rows(feat, "G2", rows, clean)])
            m3 = _mean_pct([r["gap_ex"] for r in group_rows(feat, "G3", rows, clean)])
            if None in (m1, m2, m3):
                return None
            if feat == "first_seal_min":
                return bool(m1 > m2 > m3)
            return bool(m3 > m2 > m1)

        print("\n## per-window group means (gap_ex) and monotonicity")
        win: dict[str, Any] = {}
        for w in WINDOWS:
            s, e_ = WINDOWS[w]
            rows = [r for r in events if s <= r["d0"] <= e_]
            row: dict[str, Any] = {"n": len(rows)}
            for feat in ("first_seal_min", "sealed_share"):
                row[feat] = {
                    g: _mean_pct([r["gap_ex"] for r in group_rows(feat, g, rows)])
                    for g in ("G1", "G2", "G3")
                }
                row[feat]["monotone"] = monotone(feat, rows)
                row[feat]["monotone_clean"] = monotone(feat, rows, clean=True)
            win[w] = row
            print(
                f"  {w:<7} n={row['n']:>6} | first_seal G1/G2/G3 "
                f"{row['first_seal_min']['G1']}/{row['first_seal_min']['G2']}/{row['first_seal_min']['G3']} "
                f"mono={row['first_seal_min']['monotone']} clean={row['first_seal_min']['monotone_clean']} | "
                f"sealed G1/G2/G3 {row['sealed_share']['G1']}/{row['sealed_share']['G2']}/{row['sealed_share']['G3']} "
                f"mono={row['sealed_share']['monotone']} clean={row['sealed_share']['monotone_clean']}"
            )

        k1 = sum(
            1
            for w in WF
            if win[w]["first_seal_min"]["monotone"] and win[w]["sealed_share"]["monotone"]
        ) >= 2
        k3 = sum(
            1
            for w in WF
            if win[w]["first_seal_min"]["monotone_clean"] and win[w]["sealed_share"]["monotone_clean"]
        ) >= 2
        # K2 edibility: top grade (first_seal G1 and sealed G3) gap_ex - 30bp > 0
        tops: dict[str, Any] = {}
        k2 = True
        for w in WINDOWS:
            s, e_ = WINDOWS[w]
            rows = [r for r in events if s <= r["d0"] <= e_]
            m_a = _mean_pct(
                [r["gap_ex"] for r in group_rows("first_seal_min", "G1", rows) if not r["unbuyable"]]
            )
            m_b = _mean_pct(
                [r["gap_ex"] for r in group_rows("sealed_share", "G3", rows) if not r["unbuyable"]]
            )
            tops[w] = {"first_seal_G1_nb": m_a, "sealed_G3_nb": m_b}
        long_ok = (tops["long"]["first_seal_G1_nb"] or -9) - 0.30 > 0 and (
            tops["long"]["sealed_G3_nb"] or -9
        ) - 0.30 > 0
        wf_ok = sum(
            1
            for w in WF
            if (tops[w]["first_seal_G1_nb"] or -9) - 0.30 > 0 and (tops[w]["sealed_G3_nb"] or -9) - 0.30 > 0
        ) >= 2
        k2 = long_ok and wf_ok
        print("\n## H-BOARD-1 phase-2 verdict\n")
        print(f"  K1 gradient monotone (both features) in >=2/3 windows -> {'pass' if k1 else 'FAIL'}")
        print(f"  K2 top grade (ex-一字) gap_ex - 30bp > 0 long & >=2/3 -> {tops} -> {'pass' if k2 else 'FAIL'}")
        print(f"  K3 gradient kept after dropping unbuyable in >=2/3 -> {'pass' if k3 else 'FAIL'}")
        call = "PASS: quality divider real" if (k1 and k3) else "REJECT: divider not robust"
        print(f"  => {call} (K2 = edibility only)")
        result.update({"cuts": cuts, "by_window": win, "tops_ex_unbuyable": tops,
                       "verdict": {"k1": k1, "k2": k2, "k3": k3, "call": call}})

    payload = {
        "tag": "limitup-board-quality-2026-09-15",
        "prereg": "docs/designs/hotmoney-anatomy-prereg-2026-09-15.md",
        "phase": "phase2 verdict" if args.verdict else "phase1 descriptive",
        "results": result,
        "as_of": datetime.now(UTC).isoformat(timespec="seconds"),
    }
    if args.save_report:
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        out = REPORT_DIR / ("limitup_board_quality_phase2.json" if args.verdict else "limitup_board_quality.json")
        out.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        print(f"\nsaved {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
