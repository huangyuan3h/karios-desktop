#!/usr/bin/env python3
"""Harbor x B3 risk-budget 50/50 product mix (H-MIX · 2026-09-13).

Primary M50 = monthly rebalance back to 50/50, 5bp/side on the blend.
Records (not selected): M40/M60 weight band, MD = B13 daily cost-free blend,
and a 20bp/side cost sensitivity run.
See docs/designs/harbor-riskbudget-mix-prereg-2026-09-13.md

Usage:
  cd services/data-sync-service
  PYTHONPATH=src:scripts python3 scripts/eval_harbor_riskbudget.py --save-report
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))
REPORT_DIR = ROOT / "data" / "backtest_reports"
ETF_CSV = ROOT / "data" / "etf" / "etf_daily.csv"

from eval_etf_parking_baseline import _metrics  # noqa: E402
from run_walk_forward import S3_CONFIG, WINDOWS  # noqa: E402

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

HARBOR_CODES = {*MULTI_TS.values(), *NASDAQ_ALIASES}
RP_UNIVERSE = ["510300.SH", "510500.SH", "518880.SH", "513100.SH", "511260.SH"]
COST = 0.0005
COST_HIGH = 0.0020
WINS = {k: WINDOWS[k] for k in ("OOS2", "train", "valid", "long")}
STRESS = ("2022-01-01", "2023-12-31")


def _load_panel() -> dict[str, dict[str, float]]:
    wanted = HARBOR_CODES | set(RP_UNIVERSE)
    out: dict[str, dict[str, float]] = {}
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


def _series_on_cal(px: dict[str, float], cal: list[str]) -> list[float]:
    out: list[float] = []
    last = None
    for d in cal:
        v = px.get(d)
        if v:
            last = v
        out.append(last)
    first = next((v for v in out if v), None)
    return [1.0] * len(cal) if first is None else [(v / first if v else 1.0) for v in out]


def _harbor_nav(px: dict[str, dict[str, float]], s: str, e: str):
    cfg = BacktestConfig(start_date=s, end_date=e, **S3_CONFIG)
    data = BacktestData(cfg)
    run = simulate(cfg, data)
    etf_days = {d for mp in px.values() for d in mp}
    cal = [d for d in data.calendar if d in etf_days]
    eng = engine_nav_by_day_from_run(list(data.calendar), run.nav_curve)
    snap_by = {str(x.get("date")): x for x in run.positions_by_day}

    idle_by_day: dict[str, float] = {}
    for idx in range(1, len(cal)):
        prev = cal[idx - 1]
        snap = snap_by.get(prev) or {}
        dep = sum(float(p.get("position_pct") or 0.0) for p in (snap.get("positions") or []))
        idle_by_day[prev] = max(0.0, 1.0 - min(1.0, dep))

    records = parking_replay(px, cal, idle_by_day=idle_by_day)
    nav = [1.0]
    for rec in records:
        day, prev = str(rec["date"]), str(rec["prev"])
        idle = idle_by_day.get(prev, 0.0)
        r_eng = eng[day] / eng[prev] - 1.0 if eng.get(prev) and eng.get(day) else 0.0
        sides = int(rec["sides"])
        nav.append(nav[-1] * (1.0 + r_eng + idle * (float(rec["parking_ret"]) - COST * sides)))
    return cal, nav


def _rp_nav(px: dict[str, dict[str, float]], cal: list[str], cost: float) -> list[float]:
    series = {ts: _series_on_cal(px.get(ts) or {}, cal) for ts in RP_UNIVERSE}
    w_by_i: dict[int, dict[str, float]] = {}
    for i in range(len(cal)):
        if i < 60:
            w_by_i[i] = {ts: 1.0 / len(RP_UNIVERSE) for ts in RP_UNIVERSE}
            continue
        if i > 0 and cal[i][:7] == cal[i - 1][:7]:
            w_by_i[i] = w_by_i[i - 1]
            continue
        vol = {}
        for ts in RP_UNIVERSE:
            r = [
                series[ts][j] / series[ts][j - 1] - 1
                for j in range(max(1, i - 60), i)
                if series[ts][j - 1]
            ]
            vol[ts] = float(np.std(r)) or 1e-9
        inv = {ts: 1.0 / vol[ts] for ts in RP_UNIVERSE}
        tot = sum(inv.values())
        w_by_i[i] = {ts: inv[ts] / tot for ts in RP_UNIVERSE}
    nav, cur_w = [1.0], w_by_i[0]
    for i in range(1, len(cal)):
        r = sum(
            cur_w[ts] * (series[ts][i] / series[ts][i - 1] - 1 if series[ts][i - 1] else 0.0)
            for ts in RP_UNIVERSE
        )
        nav.append(nav[-1] * (1.0 + r))
        if w_by_i[i] != cur_w:
            turn = sum(abs(w_by_i[i][ts] - cur_w[ts]) for ts in RP_UNIVERSE) / 2.0
            nav[-1] *= 1.0 - cost * turn
            cur_w = w_by_i[i]
    return nav


def _blend_daily(a: list[float], b: list[float], w_a: float) -> list[float]:
    """B13 reproduction: daily constant mix, cost-free."""
    n = min(len(a), len(b))
    out = [1.0]
    for i in range(1, n):
        ra = a[i] / a[i - 1] - 1.0 if a[i - 1] else 0.0
        rb = b[i] / b[i - 1] - 1.0 if b[i - 1] else 0.0
        out.append(out[-1] * (1.0 + w_a * ra + (1.0 - w_a) * rb))
    return out


def _blend_monthly(
    a: list[float], b: list[float], w_a: float, cal: list[str], cost: float
) -> list[float]:
    """Monthly rebalance back to ``w_a``; 5bp per side on the traded fraction.

    Cost convention (OPT-211 P2, pinned — mirrors
    ``service/homeport.blend_monthly_nav``): one-sided charge
    ``cost * |w_a - wa|`` on the rebalanced amount. A literal two-ticket
    execution would cost ~2x; the gap is ~0.1%/yr, covered by the 15bp cost
    sensitivity. Keep both copies in sync; do not change one alone.
    """
    n = min(len(a), len(b))
    out = [1.0]
    wa = w_a
    for i in range(1, n):
        ra = a[i] / a[i - 1] - 1.0 if a[i - 1] else 0.0
        rb = b[i] / b[i - 1] - 1.0 if b[i - 1] else 0.0
        port = 1.0 + wa * ra + (1.0 - wa) * rb
        out.append(out[-1] * port)
        if port > 0:
            wa = wa * (1.0 + ra) / port
        if cal[i][:7] != cal[i - 1][:7]:
            out[-1] *= 1.0 - cost * abs(w_a - wa)
            wa = w_a
    return out


def _slice(nav: list[float], cal: list[str], s: str, e: str) -> list[float]:
    return [v for d, v in zip(cal, nav, strict=True) if s <= d <= e]


def _row(nav: list[float]) -> dict:
    total = round(100 * (nav[-1] - 1), 1)
    return {"total": total, **_metrics(nav)}


def _corr(a: list[float], b: list[float]) -> float:
    n = min(len(a), len(b))
    ra = [a[i] / a[i - 1] - 1 for i in range(1, n) if a[i - 1]]
    rb = [b[i] / b[i - 1] - 1 for i in range(1, n) if b[i - 1]]
    return round(float(np.corrcoef(ra, rb)[0, 1]), 2) if len(ra) > 10 else 0.0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--save-report", action="store_true")
    args = ap.parse_args()

    px = _load_panel()
    print(f"panel: {len(px)} series")
    results: dict[str, dict] = {}
    legs: dict[str, tuple] = {}
    for w, (s, e) in WINS.items():
        cal, harbor = _harbor_nav(px, s, e)
        rp = _rp_nav(px, cal, COST)
        m50 = _blend_monthly(harbor, rp, 0.5, cal, COST)
        m40 = _blend_monthly(harbor, rp, 0.6, cal, COST)
        m60 = _blend_monthly(harbor, rp, 0.4, cal, COST)
        # H-MIX-TUNE (2026-09-16 prereg): extend the M-band harbor-ward.
        # M{N} = N% B3 + (100-N)% harbor; M30/M20 buy annualized return
        # with B3 cushion. Cost20 arms mirror m50_cost20 for K5-equivalence.
        m30 = _blend_monthly(harbor, rp, 0.7, cal, COST)
        m20 = _blend_monthly(harbor, rp, 0.8, cal, COST)
        md = _blend_daily(harbor, rp, 0.5)
        rp_hi = _rp_nav(px, cal, COST_HIGH)
        m50_hi = _blend_monthly(harbor, rp_hi, 0.5, cal, COST_HIGH)
        m30_hi = _blend_monthly(harbor, rp_hi, 0.7, cal, COST_HIGH)
        m20_hi = _blend_monthly(harbor, rp_hi, 0.8, cal, COST_HIGH)
        legs[w] = (cal, harbor, rp, m50)
        results[w] = {
            "harbor": _row(harbor),
            "rp": _row(rp),
            "m50": _row(m50),
            "m40": _row(m40),
            "m60": _row(m60),
            "m30": _row(m30),
            "m20": _row(m20),
            "md": _row(md),
            "m50_cost20": _row(m50_hi),
            "m30_cost20": _row(m30_hi),
            "m20_cost20": _row(m20_hi),
            "corr": _corr(harbor, rp),
            "days": len(cal),
        }
        for key in ("harbor", "rp", "m50", "m40", "m60", "m30", "m20", "md",
                    "m50_cost20", "m30_cost20", "m20_cost20"):
            r = results[w][key]
            print(
                f"  {w:<6} {key:<10} {r['total']:+7.1f}  cagr {r['cagr']:>7.2f}  mdd {r['mdd']:>6.1f}  sr {r['sharpe']:.2f}"
            )
        print(f"  {'':<6} corr(harbor,rp)={results[w]['corr']}  n={len(cal)}")

    # frozen verdict (prereg §4)
    k1 = all(results[w]["m50"]["sharpe"] >= results[w]["harbor"]["sharpe"] for w in WINS)
    k2 = all(results[w]["m50"]["mdd"] >= results[w]["harbor"]["mdd"] for w in WINS)
    k3 = results["long"]["m50"]["total"] >= 0.5 * results["long"]["harbor"]["total"] and all(
        results[w]["m50"]["total"] > 0 for w in WINS
    )
    cal_l, harbor_l, _, m50_l = legs["long"]
    sh = _slice(harbor_l, cal_l, *STRESS)
    sm = _slice(m50_l, cal_l, *STRESS)
    stress = {"harbor": _row(sh), "m50": _row(sm)}
    k4 = (
        stress["m50"]["sharpe"] >= stress["harbor"]["sharpe"]
        and stress["m50"]["mdd"] >= stress["harbor"]["mdd"]
    )
    k5 = all(
        results[w]["m50_cost20"]["sharpe"] >= results[w]["harbor"]["sharpe"]
        and results[w]["m50_cost20"]["mdd"] >= results[w]["harbor"]["mdd"]
        for w in WINS
    )
    verdict = {"k1": k1, "k2": k2, "k3": k3, "k4": k4, "k5": k5, "pass": all((k1, k2, k3, k4, k5))}

    print("\n## verdict (H-MIX · frozen K1-K5)")
    for name, val in verdict.items():
        print(f"  {name}: {'ok' if val else 'FAIL'}")
    print(
        f"  K3 check: long mix {results['long']['m50']['total']:+.1f} vs 0.5*harbor {0.5 * results['long']['harbor']['total']:+.1f}"
    )
    print(
        f"  stress 2022-23: harbor sr/mdd {stress['harbor']['sharpe']:.2f}/{stress['harbor']['mdd']:.1f} vs mix {stress['m50']['sharpe']:.2f}/{stress['m50']['mdd']:.1f}"
    )
    print(f"  M50 {'PASS -> product candidate' if verdict['pass'] else 'REJECT'}")

    if args.save_report:
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        (REPORT_DIR / "harbor_riskbudget_2026-09-13.json").write_text(
            json.dumps(
                {
                    "tag": "harbor-riskbudget-2026-09-13",
                    "prereg": "docs/designs/harbor-riskbudget-mix-prereg-2026-09-13.md",
                    "rp_universe": RP_UNIVERSE,
                    "stress": stress,
                    "results": results,
                    "verdict": verdict,
                    "as_of": datetime.now(UTC).isoformat(timespec="seconds"),
                },
                ensure_ascii=False,
                indent=2,
                default=str,
            ),
            encoding="utf-8",
        )
        print("saved report")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
