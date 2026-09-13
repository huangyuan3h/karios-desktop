#!/usr/bin/env python3
"""Idle-cash ETF parking sleeve (causal) on top of S-3 — H-SLEEVE.

Baseline = S-3 CN engine NAV. Idle cash (1 - deployed) earns a causal ETF pick
(decision from t-1 close, return of t): V1 single NASDAQ, V2 multi-asset mo m60
rotation, V3 +BOND ballast, V4 +causal trail8. See
docs/designs/idle-sleeve-prereg-2026-09-12.md

Usage:
  cd services/data-sync-service
  PYTHONPATH=src:scripts python3 scripts/eval_idle_sleeve.py --save-report
"""

from __future__ import annotations

import argparse
import bisect
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import psycopg

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))
REPORT_DIR = ROOT / "data" / "backtest_reports"

from data_sync_service.config import get_settings  # noqa: E402
from data_sync_service.service.backtest_engine import BacktestConfig, BacktestData, simulate  # noqa: E402
from data_sync_service.service.portfolio_nav_sim import engine_nav_by_day_from_run  # noqa: E402
from run_walk_forward import S3_CONFIG, WINDOWS  # noqa: E402

CAND = {"GOLD": "518880.SH", "OIL": "513350.SH", "NASDAQ": "513110.SH", "BOND10": "511260.SH"}
SINGLE = {"NASDAQ": "513100.SH"}
WINS = {k: WINDOWS[k] for k in ("OOS2", "train", "valid")}
WINS["past_year"] = ("2025-08-01", "2026-08-21")
MA, LB, TRAIL = 200, 60, 0.08


def _load_closes(codes: list[str]) -> dict[str, dict[str, float]]:
    s = get_settings()
    out: dict[str, dict[str, float]] = {}
    with psycopg.connect(s.database_url) as conn, conn.cursor() as cur:
        for ts in codes:
            cur.execute("SELECT trade_date, close FROM daily WHERE ts_code=%s ORDER BY trade_date", (ts,))
            out[ts] = {str(d): float(c) for d, c in cur.fetchall() if c is not None}
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--save-report", action="store_true")
    args = ap.parse_args()

    px = _load_closes(sorted(set(CAND.values()) | set(SINGLE.values())))
    days = {ts: sorted(m) for ts, m in px.items()}

    def c_at(ts, d):
        return px[ts].get(d)

    def ma(ts, d):
        ds = days[ts]
        i = bisect.bisect_left(ds, d)
        if i < MA or i >= len(ds) or ds[i] != d:
            return None
        return float(np.mean([px[ts][ds[j]] for j in range(i - MA + 1, i + 1)]))

    def mom(ts, d):
        ds = days[ts]
        i = bisect.bisect_left(ds, d)
        if i < LB or i >= len(ds) or ds[i] != d:
            return None
        a = px[ts][ds[i - LB]]
        return px[ts][d] / a - 1.0 if a else None

    def ret(ts, d, prev):
        c, p = px[ts].get(d), px[ts].get(prev)
        return c / p - 1.0 if c and p else 0.0

    variants = ("V0", "V1", "V2", "V3", "V4")
    res: dict[str, dict] = {}
    for w, (s, e) in WINS.items():
        cfg = BacktestConfig(start_date=s, end_date=e, **S3_CONFIG)
        data = BacktestData(cfg)
        run = simulate(cfg, data)
        cal = list(data.calendar)
        eng = engine_nav_by_day_from_run(cal, run.nav_curve)
        snap_by = {str(x.get("date")): x for x in run.positions_by_day}
        navs = {v: [1.0] for v in variants}
        held = {v: None for v in variants}
        peak = {v: 0.0 for v in variants}
        for idx in range(1, len(cal)):
            day, prev = cal[idx], cal[idx - 1]
            snap = snap_by.get(prev) or {}
            dep = sum(float(p.get("position_pct") or 0.0) for p in (snap.get("positions") or []))
            idle = max(0.0, 1.0 - min(1.0, dep))
            r_eng = eng[day] / eng[prev] - 1.0 if eng.get(prev) else 0.0
            for v in variants:
                sr = 0.0
                if v != "V0":
                    if v == "V1":
                        cand = {}
                        ts = SINGLE["NASDAQ"]
                        m, c = ma(ts, prev), c_at(ts, prev)
                        if m is not None and c is not None and c >= m:
                            cand["NASDAQ"] = ts
                    else:
                        cand = {}
                        for k, ts in CAND.items():
                            if v == "V3" and k == "BOND10":
                                cand[k] = ts
                                continue
                            m = ma(ts, prev)
                            if m is not None and c_at(ts, prev) is not None and c_at(ts, prev) >= m:
                                cand[k] = ts
                    best, bm = None, -1e9
                    for k, ts in cand.items():
                        mom_v = mom(ts, prev)
                        if mom_v is not None and mom_v > bm:
                            bm, best = mom_v, k
                    if best is None:
                        held[v] = None
                        peak[v] = 0.0
                    else:
                        ts = (SINGLE if v == "V1" else CAND)[best]
                        if held[v] != best:
                            held[v] = best
                            peak[v] = c_at(ts, prev) or 0.0
                        else:
                            peak[v] = max(peak[v], c_at(ts, prev) or 0.0)
                        if v == "V4" and peak[v] > 0:
                            c = c_at(ts, prev)
                            if c and c < peak[v] * (1 - TRAIL):
                                held[v] = None
                                best = None
                                peak[v] = 0.0
                        if best is not None:
                            sr = ret(ts, day, prev)
                navs[v].append(navs[v][-1] * (1.0 + r_eng + idle * sr))
        base = navs["V0"][-1] - 1.0
        res[w] = {v: {"total": round(100 * (navs[v][-1] - 1), 1),
                      "delta": round(100 * ((navs[v][-1] - 1) - base), 1)} for v in variants}
        print(f"  {w:<10} base {100*base:+7.1f}%  " + "  ".join(
            f"{v}:{res[w][v]['total']:+.1f}({res[w][v]['delta']:+.1f})" for v in variants[1:]))

    print("\n## verdict (K1 三窗增量≥0 / K2 全窗合计>0)")
    ok = []
    for v in variants[1:]:
        k1 = all(res[w][v]["delta"] >= -0.05 for w in ("OOS2", "train", "valid"))
        k2 = sum(res[w][v]["delta"] for w in WINS) > 0
        if k1 and k2:
            ok.append(v)
        print(f"  {v}: K1 {'ok' if k1 else 'FAIL'} ({[res[w][v]['delta'] for w in ('OOS2','train','valid')]}) K2 {'ok' if k2 else 'FAIL'}")
    print(f"  usable = {ok if ok else 'NONE'}")

    if args.save_report:
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        (REPORT_DIR / "idle_sleeve_2026-09-12.json").write_text(
            json.dumps({"tag": "idle-sleeve-2026-09-12",
                        "prereg": "docs/designs/idle-sleeve-prereg-2026-09-12.md",
                        "results": res, "usable": ok,
                        "as_of": datetime.now(UTC).isoformat(timespec="seconds")},
                       ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        print("saved report")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
