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

from run_walk_forward import S3_CONFIG, WINDOWS  # noqa: E402

from data_sync_service.config import get_settings  # noqa: E402
from data_sync_service.service.backtest_engine import (  # noqa: E402
    BacktestConfig,
    BacktestData,
    simulate,
)
from data_sync_service.service.harbor import (  # noqa: E402
    MULTI_TS,
    NASDAQ_ALIASES,
    parking_replay,
)
from data_sync_service.service.portfolio_nav_sim import engine_nav_by_day_from_run  # noqa: E402

LB, COST, FLOOR = 60, 0.0005, 0.20
WINS = {k: WINDOWS[k] for k in ("OOS2", "train", "valid", "long")}
VARIANTS = ("V0", "P1", "P1_H2", "R1", "R2", "R3")


def _load_etf_closes() -> dict[str, dict[str, float]]:
    out: dict[str, dict[str, float]] = {}
    wanted = {*MULTI_TS.values(), *NASDAQ_ALIASES}
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


def _mom_at(series: dict[str, float], d: str, lb: int = LB) -> float | None:
    ds = sorted(series)
    i = bisect.bisect_left(ds, d)
    if i >= len(ds) or ds[i] != d or i < lb:
        return None
    a = series[ds[i - lb]]
    return series[d] / a - 1.0 if a else None


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--save-report", action="store_true")
    args = ap.parse_args()

    px = _load_etf_closes()
    etf_days = {d for mp in px.values() for d in mp}

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
        spx = _load_stock_closes(stock_codes - set(px))

        idle_by_day: dict[str, float] = {}
        stock_mom_by_day: dict[str, float | None] = {}
        for idx in range(1, len(cal)):
            prev = cal[idx - 1]
            snap = snap_by.get(prev) or {}
            dep = sum(float(p.get("position_pct") or 0.0) for p in (snap.get("positions") or []))
            idle_by_day[prev] = max(0.0, 1.0 - min(1.0, dep))
            smoms = [
                m
                for p in (snap.get("positions") or [])
                if (m := _mom_at(spx.get(str(p.get("ts_code") or "").strip().upper()) or {}, prev))
                is not None
            ]
            stock_mom_by_day[prev] = sum(smoms) / len(smoms) if smoms else None

        # One shared parking state machine (service.harbor.parking_replay) per
        # variant — same code as the timeline / Live decision.
        # H-HARBOR-H2 (2026-09-16 prereg): P1_H2 = S-3 core + H2 hysteresis
        # sleeve (service/parking_sleeve.py, research-only machine). Frozen
        # P1/R-arms untouched.
        from data_sync_service.service.parking_sleeve import hysteresis_parking_replay
        records = {
            "P1": parking_replay(px, cal, idle_by_day=idle_by_day),
            "P1_H2": hysteresis_parking_replay(px, cal),
            "R1": parking_replay(px, cal, idle_by_day=idle_by_day, min_idle_pct=FLOOR),
            "R2": parking_replay(
                px, cal, idle_by_day=idle_by_day, stock_gate=True, stock_mom_by_day=stock_mom_by_day
            ),
            "R3": parking_replay(px, cal, idle_by_day=idle_by_day, bond_ungated=True),
        }
        parking_variants = [v for v in VARIANTS if v != "V0"]
        navs = {v: [1.0] for v in VARIANTS}
        trades = {v: 0 for v in VARIANTS}
        for recs in zip(*[records[v] for v in parking_variants], strict=True):
            rec0 = recs[0]
            day, prev = str(rec0["date"]), str(rec0["prev"])
            idle = idle_by_day.get(prev, 0.0)
            r_eng = eng[day] / eng[prev] - 1.0 if eng.get(prev) and eng.get(day) else 0.0
            navs["V0"].append(navs["V0"][-1] * (1.0 + r_eng))
            for v, rec in zip(parking_variants, recs, strict=True):
                sides = int(rec["sides"])
                if sides:
                    trades[v] += 1
                navs[v].append(
                    navs[v][-1] * (1.0 + r_eng + idle * (float(rec["parking_ret"]) - COST * sides))
                )

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
    # H-HARBOR-H2 verdict (2026-09-16 prereg §2; frozen block above untouched).
    h = {w: res[w]["P1_H2"] for w in WINS}
    hp = {w: res[w]["P1"] for w in WINS}
    hk1 = all(h[w]["total"] - hp[w]["total"] >= -1.0 for w in ("OOS2", "train", "valid"))
    hk2 = (sum(h[w]["total"] - hp[w]["total"] for w in ("OOS2", "train", "valid")) / 3 > 0
           and h["long"]["total"] - hp["long"]["total"] > 0)
    hk3 = (h["long"]["mdd"] >= hp["long"]["mdd"] - 1.0
           and h["valid"]["mdd"] >= hp["valid"]["mdd"] - 2.0)
    print("## verdict (H-HARBOR-H2: K1 devΔ>=-1 ea / K2 mean>0 & long>0 / K3 MDD guard)")
    print(f"  HK1 {'ok' if hk1 else 'FAIL'} "
          f"{[round(h[w]['total'] - hp[w]['total'], 1) for w in ('OOS2', 'train', 'valid')]}")
    print(f"  HK2 {'ok' if hk2 else 'FAIL'} "
          f"mean={sum(h[w]['total'] - hp[w]['total'] for w in ('OOS2', 'train', 'valid')) / 3:.1f} "
          f"long={h['long']['total'] - hp['long']['total']:+.1f}")
    print(f"  HK3 {'ok' if hk3 else 'FAIL'} "
          f"longMDD {h['long']['mdd']:+.1f} vs {hp['long']['mdd']:+.1f} / "
          f"validMDD {h['valid']['mdd']:+.1f} vs {hp['valid']['mdd']:+.1f}")
    print(f"  P1_H2 {'PASS → harbor-H2 candidate (Live track: paper + auth)' if (hk1 and hk2 and hk3) else 'REJECT → harbor stays canonical'}")
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
