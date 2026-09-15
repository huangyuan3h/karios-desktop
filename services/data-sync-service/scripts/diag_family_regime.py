#!/usr/bin/env python3
"""Family regime map (read-only): annual / monthly / ex-ante buckets for all 5 strategies.

Strategies (frozen calibers, long window 2021-08-01~2026-08-07):
  harbor   = S-3 + idle-cash ETF parking (P1)
  homeport = M50 (harbor x B3 risk-budget, monthly rebalance)
  starport = homeport x habit satellite, opportunity blend w=1/3
  twin_star= harbor x habit satellite, opportunity blend w=1/2
  starship = v2 (satellite + idle-cash parking, compose_parked_rows)

Buckets use T-1-knowable index (510300) labels: MA200 trend, 20d momentum,
20d realized-vol tercile. Descriptive only.

Usage:
  cd services/data-sync-service
  PYTHONPATH=src:scripts python3 scripts/diag_family_regime.py --save-report
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))
REPORT_DIR = ROOT / "data" / "backtest_reports"

from eval_harbor_riskbudget import (  # noqa: E402
    COST,
    _blend_monthly,
    _harbor_nav,
    _load_panel,
    _rp_nav,
)
from eval_sat_idle_parking import (  # noqa: E402
    _align_day_nav,
    _rets,
    _sat_book,
    _true_cash_share,
)
from eval_twin_star_parking import _load_etf_closes  # noqa: E402

from data_sync_service.service.harbor import parking_replay  # noqa: E402
from data_sync_service.service.ps_g50_blend import blend_nav_opportunity  # noqa: E402
from data_sync_service.service.state_bucket_track import compose_parked_rows  # noqa: E402

START, END = "2021-08-01", "2026-08-07"
IDX_TS = "510300.SH"
LABELS = ("harbor", "homeport", "starport", "twin_star", "starship")


def _mean(xs: list[float]) -> float:
    return float(np.mean(xs)) if xs else 0.0


def _seg(series: list[float], idxs: list[int]) -> dict[str, float]:
    i0, i1 = idxs[0], idxs[-1]
    base = max(0, i0 - 1)
    tot = (series[i1] / series[base] - 1.0) * 100 if series[base] else 0.0
    peak, mdd = series[i0], 0.0
    for i in idxs:
        peak = max(peak, series[i])
        mdd = max(mdd, (peak - series[i]) / peak * 100 if peak else 0.0)
    rets = [series[i] / series[i - 1] - 1 for i in idxs if series[i - 1]]
    sharpe = (
        (_mean(rets) / float(np.std(rets)) * (252**0.5))
        if len(rets) > 10 and float(np.std(rets)) > 0
        else 0.0
    )
    return {"total": round(tot, 1), "mdd": round(mdd, 1), "sharpe": round(sharpe, 2)}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--save-report", action="store_true")
    args = ap.parse_args()

    print(f"loading family {START}~{END} ...", flush=True)
    px = _load_panel()
    cal, harbor_l = _harbor_nav(px, START, END)
    rp_l = _rp_nav(px, cal, COST)
    m50_l = _blend_monthly(harbor_l, rp_l, 0.5, cal, COST)
    harbor_day = dict(zip(cal, harbor_l, strict=True))
    m50_day = dict(zip(cal, m50_l, strict=True))

    sat = _sat_book(START, END)
    dates, sat_nav, pos = sat["dates"], sat["nav"], sat["pos"]
    cash = _true_cash_share(sat)
    n = len(dates)

    px_h = _load_etf_closes()
    recs = parking_replay(px_h, dates, idle_by_day=None)
    sleeve_day: dict[str, float] = {dates[0]: 1.0}
    nav = 1.0
    for r in recs:
        nav *= 1.0 + float(r["parking_ret"]) - COST * int(r["sides"])
        sleeve_day[str(r["date"])] = nav
    sleeve_ret_by_day = {
        str(r["date"]): float(r["parking_ret"]) - COST * int(r["sides"]) for r in recs
    }
    v2 = compose_parked_rows(
        [{**r, "cashShare": cash[i]} for i, r in enumerate(sat["rows"])],
        sleeve_ret_by_day,
        cost_bps=5.0,
    )
    v2_nav = [p["parkedNav"] for p in v2["rows"]]

    active = [bool(r.get("satActive")) for r in sat["rows"]]
    harbor_a = _align_day_nav(harbor_day, dates)[:n]
    m50_a = _align_day_nav(m50_day, dates)[:n]
    twin_nav = blend_nav_opportunity(harbor_a, sat_nav, active, sat_weight=0.5)
    starport_nav = blend_nav_opportunity(m50_a, sat_nav, active, sat_weight=1 / 3)

    series: dict[str, list[float]] = {
        "harbor": harbor_a,
        "homeport": m50_a,
        "starport": starport_nav,
        "twin_star": twin_nav,
        "starship": v2_nav,
    }
    rets = {k: _rets(v) for k, v in series.items()}

    panel = _load_panel()
    idx_map = panel.get(IDX_TS) or {}
    idx_nav = _align_day_nav(idx_map, dates)
    idx_r = _rets(idx_nav)

    # ---- annual
    years: dict[str, list[int]] = {}
    for i, d in enumerate(dates):
        years.setdefault(d[:4], []).append(i)
    annual: dict[str, dict] = {}
    print("\n## 年度 total（括号 MDD/SR）")
    header = "| 年 | " + " | ".join(LABELS) + " | 指数 |"
    print(header)
    print("|" + "---|" * (len(LABELS) + 2))
    for y, idxs in years.items():
        cells = []
        annual[y] = {}
        for k in LABELS:
            st = _seg(series[k], idxs)
            annual[y][k] = st
            cells.append(f"{st['total']:+.1f} ({st['mdd']:.1f}/{st['sharpe']:.2f})")
        ix = _seg(idx_nav, idxs)
        annual[y]["index"] = ix
        print(f"| {y} | " + " | ".join(cells) + f" | {ix['total']:+.1f} |")

    # ---- monthly
    months: dict[str, list[int]] = {}
    for i, d in enumerate(dates):
        months.setdefault(d[:7], []).append(i)
    monthly: dict[str, dict] = {}
    print("\n## 月度胜率 / 中位 / 最差 3 月")
    for k in LABELS:
        rows = []
        for m, idxs in months.items():
            i0, i1 = idxs[0], idxs[-1]
            base = max(0, i0 - 1)
            rows.append((m, (series[k][i1] / series[k][base] - 1) * 100))
        wins = sum(1 for _, v in rows if v > 0)
        worst = sorted(rows, key=lambda x: x[1])[:3]
        monthly[k] = {
            "n": len(rows),
            "win_rate_pct": round(100 * wins / len(rows), 0),
            "median_pct": round(float(np.median([v for _, v in rows])), 1),
            "worst": [[m, round(v, 1)] for m, v in worst],
        }
        print(
            f"  {k:<9} 胜率 {wins}/{len(rows)} = {100 * wins / len(rows):.0f}% · 中位 "
            f"{float(np.median([v for _, v in rows])):+.1f}% · 最差 "
            + " ".join(f"{m} {v:+.1f}%" for m, v in worst)
        )

    # ---- buckets
    vol20 = [0.0] * n
    for i in range(1, n):
        vols = [idx_r[j] for j in range(max(1, i - 19), i + 1)]
        vol20[i] = float(np.std(vols)) * (252**0.5) if len(vols) > 5 else 0.0
    valid_vols = [v for v in vol20[21:] if v > 0]
    q1, q2 = (float(np.quantile(valid_vols, 1 / 3)), float(np.quantile(valid_vols, 2 / 3)))
    labels: dict[str, list[str | None]] = {"trend_ma200": [None] * n, "mom20": [None] * n, "vol": [None] * n}
    for i in range(n):
        ma200 = _mean(idx_nav[max(0, i - 200) : i]) if i > 200 else None
        labels["trend_ma200"][i] = None if ma200 is None else ("above" if idx_nav[i - 1] > ma200 else "below")
        mom = (idx_nav[i - 1] / idx_nav[i - 21] - 1) if i > 21 and idx_nav[i - 21] else None
        labels["mom20"][i] = None if mom is None else ("up" if mom > 0 else "down")
        labels["vol"][i] = (
            None
            if i <= 21 or vol20[i] <= 0
            else ("low" if vol20[i] <= q1 else ("mid" if vol20[i] <= q2 else "high"))
        )
    buckets: dict[str, dict] = {}
    print("\n## 市况分桶：日均收益%（年化 = 日均×243）")
    for name, lab in labels.items():
        print(f"\n### by {name}")
        print("| 状态 | 天数 | " + " | ".join(f"{k} 日均 / 年化" for k in LABELS) + " |")
        print("|" + "---|" * (len(LABELS) + 2))
        buckets[name] = {}
        groups: dict[str, list[int]] = {}
        for i in range(1, n):
            if lab[i]:
                groups.setdefault(str(lab[i]), []).append(i)
        for lg, idxs in sorted(groups.items()):
            cells = []
            row: dict[str, dict] = {"days": len(idxs)}
            for k in LABELS:
                m = _mean([rets[k][i] for i in idxs]) * 100
                row[k] = {"daily_pct": round(m, 3), "annualized_pct": round(m * 243, 1)}
                cells.append(f"{m:+.3f} / {m * 243:+.0f}")
            buckets[name][lg] = row
            print(f"| {lg} | {len(idxs)} | " + " | ".join(cells) + " |")

    if args.save_report:
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        path = REPORT_DIR / "family_regime_2026-09-15.json"
        path.write_text(
            json.dumps(
                {
                    "tag": "family-regime-2026-09-15",
                    "window": [START, END],
                    "annual": annual,
                    "monthly": monthly,
                    "buckets": buckets,
                    "as_of": datetime.now(UTC).isoformat(),
                },
                ensure_ascii=False,
                indent=1,
            ),
            encoding="utf-8",
        )
        print(f"\nreport: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
