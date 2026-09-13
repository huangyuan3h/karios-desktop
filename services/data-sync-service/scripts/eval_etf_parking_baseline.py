#!/usr/bin/env python3
"""ETF parking layer as the S-3 baseline (H-PARK · 2026-09-13).

Baseline = S-3 CN engine NAV. Idle cash is parked in the mom60+MA200 ETF pick
(satellite 14:30 clock proxied by the close). P1 = park whenever idle>0;
R1 = Live 20% floor; R2 = Live ETF>STOCK-mom gate; R3 = BOND ungated.
See docs/designs/etf-parking-baseline-prereg-2026-09-13.md

Usage:
  cd services/data-sync-service
  PYTHONPATH=src:scripts python3 scripts/eval_etf_parking_baseline.py --save-report
"""

from __future__ import annotations

import argparse
import bisect
import csv
import json
import math
import sys
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import psycopg

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))
REPORT_DIR = ROOT / "data" / "backtest_reports"
ETF_CSV = ROOT / "data" / "etf" / "etf_daily.csv"

from data_sync_service.config import get_settings  # noqa: E402
from data_sync_service.service.backtest_engine import BacktestConfig, BacktestData, simulate  # noqa: E402
from data_sync_service.service.portfolio_nav_sim import engine_nav_by_day_from_run  # noqa: E402
from run_walk_forward import S3_CONFIG, WINDOWS  # noqa: E402

GOLD, OIL, BOND10 = "518880.SH", "513350.SH", "511260.SH"
NASDAQ_ALIAS = ("513110.SH", "513100.SH")
CAND = {"GOLD": GOLD, "OIL": OIL, "NASDAQ": NASDAQ_ALIAS[0], "BOND10": BOND10}
MA, LB, TRAIL, COST, FLOOR = 200, 60, 0.08, 0.0005, 0.20
WINS = {k: WINDOWS[k] for k in ("OOS2", "train", "valid", "long")}
VARIANTS = ("V0", "P1", "R1", "R2", "R3")


def _load_etf_closes() -> dict[str, dict[str, float]]:
    out: dict[str, dict[str, float]] = {}
    wanted = {GOLD, OIL, BOND10, *NASDAQ_ALIAS}
    with ETF_CSV.open(newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            ts = row["ts_code"]
            if ts not in wanted:
                continue
            d = str(row["trade_date"])
            d = f"{d[:4]}-{d[4:6]}-{d[6:8]}"
            try:
                c = float(row["close_adj"])
            except (TypeError, ValueError):
                continue
            if c > 0:
                out.setdefault(ts, {})[d] = c
    return out


def _load_stock_closes(codes: set[str]) -> dict[str, dict[str, float]]:
    if not codes:
        return {}
    s = get_settings()
    out: dict[str, dict[str, float]] = {}
    with psycopg.connect(s.database_url) as conn, conn.cursor() as cur:
        for ts in sorted(codes):
            cur.execute("SELECT trade_date, close FROM daily WHERE ts_code=%s ORDER BY trade_date", (ts,))
            out[ts] = {str(d): float(c) for d, c in cur.fetchall() if c is not None}
    return out


def _metrics(navs: list[float]) -> dict[str, float]:
    if len(navs) < 3:
        return {"cagr": 0.0, "mdd": 0.0, "sharpe": 0.0}
    rets = [navs[i] / navs[i - 1] - 1.0 for i in range(1, len(navs)) if navs[i - 1] > 0]
    years = len(rets) / 252.0
    cagr = (navs[-1] / navs[0]) ** (1 / years) - 1 if years > 0 and navs[0] > 0 and navs[-1] > 0 else 0.0
    peak, mdd = navs[0], 0.0
    for v in navs:
        peak = max(peak, v)
        mdd = min(mdd, v / peak - 1.0)
    mean = float(np.mean(rets)) if rets else 0.0
    std = float(np.std(rets)) if rets else 0.0
    sharpe = mean / std * math.sqrt(252) if std > 0 else 0.0
    return {"cagr": round(100 * cagr, 2), "mdd": round(100 * mdd, 1), "sharpe": round(sharpe, 2)}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--save-report", action="store_true")
    args = ap.parse_args()

    px = _load_etf_closes()
    days = {ts: sorted(m) for ts, m in px.items()}
    # Engine calendar can carry phantom sessions (holidays) with no ETF bar;
    # decisions there are data artifacts. Trade only real sessions.
    etf_days = {d for mp in px.values() for d in mp}

    def c_at(ts: str, d: str) -> float | None:
        return px.get(ts, {}).get(d)

    def index_of(ts: str, d: str) -> int | None:
        ds = days.get(ts) or []
        i = bisect.bisect_left(ds, d)
        if i < len(ds) and ds[i] == d:
            return i
        return None

    def ma(ts: str, d: str) -> float | None:
        i = index_of(ts, d)
        if i is None or i < MA - 1:
            return None
        return float(np.mean([px[ts][days[ts][j]] for j in range(i - MA + 1, i + 1)]))

    def mom(ts: str, d: str) -> float | None:
        i = index_of(ts, d)
        if i is None or i < LB:
            return None
        a = px[ts][days[ts][i - LB]]
        return px[ts][d] / a - 1.0 if a else None

    def ret(ts: str, d: str, prev: str) -> float:
        c, p = c_at(ts, d), c_at(ts, prev)
        return c / p - 1.0 if c and p else 0.0

    res: dict[str, dict] = {}
    for w, (s, e) in WINS.items():
        cfg = BacktestConfig(start_date=s, end_date=e, **S3_CONFIG)
        data = BacktestData(cfg)
        run = simulate(cfg, data)
        cal = [d for d in data.calendar if d in etf_days]
        eng = engine_nav_by_day_from_run(list(data.calendar), run.nav_curve)
        snap_by = {str(x.get("date")): x for x in run.positions_by_day}

        stock_codes: set[str] = set()
        for snap in snap_by.values():
            for p in snap.get("positions") or []:
                ts = str(p.get("ts_code") or "").strip().upper()
                if ts:
                    stock_codes.add(ts)
        for ts, series in _load_stock_closes(stock_codes - set(px)).items():
            px[ts] = series
            days[ts] = sorted(series)

        navs = {v: [1.0] for v in VARIANTS}
        held_key = {v: None for v in VARIANTS}
        held_ts = {v: None for v in VARIANTS}
        peak = {v: 0.0 for v in VARIANTS}
        trades = {v: 0 for v in VARIANTS}

        for idx in range(1, len(cal)):
            day, prev = cal[idx], cal[idx - 1]
            snap = snap_by.get(prev) or {}
            dep = sum(float(p.get("position_pct") or 0.0) for p in (snap.get("positions") or []))
            idle = max(0.0, 1.0 - min(1.0, dep))
            r_eng = eng[day] / eng[prev] - 1.0 if eng.get(prev) else 0.0

            stock_mom = None
            smoms = [m for p in (snap.get("positions") or [])
                     if (m := mom(str(p.get("ts_code") or "").strip().upper(), prev)) is not None]
            if smoms:
                stock_mom = sum(smoms) / len(smoms)

            etf_mom: dict[str, float] = {}
            etf_ts: dict[str, str] = {}
            bond_mom_ungated: tuple[str, float] | None = None
            for key, ts in CAND.items():
                aliases = NASDAQ_ALIAS if key == "NASDAQ" else (ts,)
                ba, bm = None, -1e9
                for a in aliases:
                    m, mm = mom(a, prev), ma(a, prev)
                    if m is None or mm is None or c_at(a, prev) is None:
                        continue
                    if c_at(a, prev) >= mm and m > bm:
                        ba, bm = a, m
                if ba is not None:
                    etf_mom[key], etf_ts[key] = bm, ba
                elif key == "BOND10" and (m := mom(ts, prev)) is not None and c_at(ts, prev) is not None:
                    bond_mom_ungated = (ts, m)

            def pick_for(v: str) -> tuple[str | None, str | None, float]:
                pool_mom = dict(etf_mom)
                pool_ts = dict(etf_ts)
                if v == "R3" and bond_mom_ungated is not None and "BOND10" not in pool_mom:
                    pool_mom["BOND10"], pool_ts["BOND10"] = bond_mom_ungated[1], bond_mom_ungated[0]
                if not pool_mom:
                    return None, None, 0.0
                k = max(pool_mom, key=pool_mom.get)
                return k, pool_ts[k], pool_mom[k]

            for v in VARIANTS:
                sides = 0
                if v != "V0":
                    want_key, want_ts, want_mom = pick_for(v)
                    if v == "R2" and want_key is not None and stock_mom is not None and want_mom <= stock_mom:
                        want_key, want_ts = None, None
                    if v == "R1" and want_key is not None and held_key[v] is None and idle < FLOOR:
                        want_key, want_ts = None, None
                    if held_key[v] != want_key:
                        if held_key[v] is not None:
                            sides += 1
                        if want_key is not None:
                            sides += 1
                        held_key[v], held_ts[v] = want_key, want_ts
                        peak[v] = c_at(want_ts, prev) or 0.0 if want_ts else 0.0
                sr = 0.0
                if held_ts[v] is not None:
                    c = c_at(held_ts[v], prev) or 0.0
                    peak[v] = max(peak[v], c)
                    if peak[v] > 0 and c and c < peak[v] * (1 - TRAIL):
                        held_key[v], held_ts[v], peak[v] = None, None, 0.0
                        sides += 1
                    else:
                        sr = ret(held_ts[v], day, prev)
                if sides:
                    trades[v] += 1
                navs[v].append(navs[v][-1] * (1.0 + r_eng + idle * (sr - COST * sides)))

        base = navs["V0"][-1] - 1.0
        res[w] = {v: {"total": round(100 * (navs[v][-1] - 1), 1),
                      "delta": round(100 * ((navs[v][-1] - 1) - base), 1),
                      "trades": trades[v],
                      **_metrics(navs[v])} for v in VARIANTS}
        print(f"  {w:<10} base {100*base:+7.1f}%  " + "  ".join(
            f"{v}:{res[w][v]['total']:+.1f}({res[w][v]['delta']:+.1f})" for v in VARIANTS[1:]))
        print(f"  {'':<10} V0 {res[w]['V0']} | P1 {res[w]['P1']}")

    print("\n## verdict (P1: K1 三窗增量≥-0.05 / K2 三窗合计>0 / K3 long>0)")
    k1 = all(res[w]["P1"]["delta"] >= -0.05 for w in ("OOS2", "train", "valid"))
    k2 = sum(res[w]["P1"]["delta"] for w in ("OOS2", "train", "valid")) > 0
    k3 = res["long"]["P1"]["delta"] > 0
    print(f"  K1 {'ok' if k1 else 'FAIL'} {[res[w]['P1']['delta'] for w in ('OOS2','train','valid')]}")
    print(f"  K2 {'ok' if k2 else 'FAIL'} sum={sum(res[w]['P1']['delta'] for w in ('OOS2','train','valid')):.1f}")
    print(f"  K3 {'ok' if k3 else 'FAIL'} long={res['long']['P1']['delta']}")
    print(f"  P1 {'PASS → new baseline = S-3 + parking' if (k1 and k2 and k3) else 'NOT PASSED'}")
    for v in ("R1", "R2", "R3"):
        print(f"  {v} delta by window: {{{', '.join(f'{w}: {res[w][v]['delta']}' for w in WINS)}}}")

    if args.save_report:
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        (REPORT_DIR / "etf_parking_baseline_2026-09-13.json").write_text(
            json.dumps({"tag": "etf-parking-baseline-2026-09-13",
                        "prereg": "docs/designs/etf-parking-baseline-prereg-2026-09-13.md",
                        "results": res, "P1_pass": bool(k1 and k2 and k3),
                        "as_of": datetime.now(UTC).isoformat(timespec="seconds")},
                       ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        print("saved report")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
