#!/usr/bin/env python3
"""Starship v2 regime map (read-only diagnostic): when does satellite + idle-cash parking work?

Covers 2021-08-01~2026-08-07 in one continuous replay (annual + monthly + ex-ante
regime buckets). v2 = compose_parked_rows (codified algorithm); v1 = satellite
standalone; sleeve = the ETF parking leg alone; index = CSI300 ETF (510300.SH).

Ex-ante labels (all computed up to t-1):
  index_trend : 510300 close vs its MA200
  index_mom20 : 510300 20-session return sign
  index_vol   : 510300 20-session realized vol tercile (full-window quantiles)
  sleeve_trend: sleeve NAV vs its MA20

Usage:
  cd services/data-sync-service
  PYTHONPATH=src:scripts python3 scripts/diag_sat_v2_regime.py --save-report
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

from eval_harbor_riskbudget import _load_panel  # noqa: E402
from eval_sat_idle_parking import (  # noqa: E402
    _align_day_nav,
    _rets,
    _sat_book,
    _true_cash_share,
)
from eval_twin_star_parking import COST, _load_etf_closes  # noqa: E402

from data_sync_service.service.harbor import parking_replay  # noqa: E402
from data_sync_service.service.state_bucket_track import (  # noqa: E402
    compose_parked_rows,
)

START, END = "2021-08-01", "2026-08-07"
IDX_TS = "510300.SH"


def _mean(xs: list[float]) -> float:
    return float(np.mean(xs)) if xs else 0.0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--save-report", action="store_true")
    args = ap.parse_args()

    print(f"loading satellite {START}~{END} ...", flush=True)
    sat = _sat_book(START, END)
    rows = sat["rows"]
    dates = sat["dates"]
    sat_nav = sat["nav"]
    cash = _true_cash_share(sat)
    n = len(dates)

    px = _load_etf_closes()
    recs = parking_replay(px, dates, idle_by_day=None)
    rec_by_day = {str(r["date"]): r for r in recs}
    day_nav = {dates[0]: 1.0}
    nav = 1.0
    for r in recs:
        nav *= 1.0 + float(r["parking_ret"]) - COST * int(r["sides"])
        day_nav[str(r["date"])] = nav
    sleeve_nav = _align_day_nav(day_nav, dates)
    sleeve_r = _rets(sleeve_nav)
    sat_r = _rets(sat_nav)

    rows_with_cash = [{**r, "cashShare": cash[i]} for i, r in enumerate(rows)]
    sleeve_ret_by_day = {
        str(r["date"]): float(r["parking_ret"]) - COST * int(r["sides"]) for r in recs
    }
    parked = compose_parked_rows(rows_with_cash, sleeve_ret_by_day, cost_bps=5.0)
    v2_nav = [p["parkedNav"] for p in parked["rows"]]
    v2_r = _rets(v2_nav)
    w = [p["parkedWeight"] for p in parked["rows"]]

    panel = _load_panel()
    idx_map = panel.get(IDX_TS) or {}
    idx_nav = _align_day_nav(idx_map, dates)
    idx_r = _rets(idx_nav)

    # ex-ante regime labels
    labels: dict[str, list[str | None]] = {"index_trend": [], "index_mom20": [], "index_vol": []}
    vol20: list[float] = [0.0] * n
    for i in range(1, n):
        vol20[i] = float(np.std(idx_r[max(1, i - 19) : i + 1])) * (252**0.5) if i > 20 else 0.0
    vols = [v for v in vol20[21:] if v > 0]
    q1, q2 = (np.quantile(vols, 1 / 3), np.quantile(vols, 2 / 3)) if vols else (0.0, 0.0)
    for i in range(n):
        ma200 = _mean([v for v in idx_nav[max(0, i - 200) : i] if v]) if i > 200 else None
        labels["index_trend"].append(
            None if ma200 is None else ("above" if idx_nav[i - 1] > ma200 else "below")
        )
        mom = (idx_nav[i - 1] / idx_nav[i - 21] - 1) if i > 21 and idx_nav[i - 21] else None
        labels["index_mom20"].append(None if mom is None else ("up" if mom > 0 else "down"))
        if i <= 21 or vol20[i] <= 0:
            labels["index_vol"].append(None)
        else:
            labels["index_vol"].append("low" if vol20[i] <= q1 else ("mid" if vol20[i] <= q2 else "high"))
    sleeve_ma20: list[str | None] = [None] * n
    for i in range(21, n):
        ma = _mean(sleeve_nav[i - 20 : i])
        sleeve_ma20[i] = "above" if sleeve_nav[i - 1] > ma else "below"

    # ---- annual table
    years: dict[str, list[int]] = {}
    for i, d in enumerate(dates):
        years.setdefault(d[:4], []).append(i)
    def _seg_stats(series: list[float], idxs: list[int]) -> dict[str, float]:
        i0, i1 = idxs[0], idxs[-1]
        base = max(0, i0 - 1)
        tot = (series[i1] / series[base] - 1.0) * 100 if series[base] else 0.0
        peak, mdd = series[i0], 0.0
        for i in idxs:
            peak = max(peak, series[i])
            mdd = max(mdd, (peak - series[i]) / peak * 100 if peak else 0.0)
        rets = [series[i] / series[i - 1] - 1 for i in idxs if series[i - 1]]
        sharpe = (_mean(rets) / float(np.std(rets)) * (252**0.5)) if len(rets) > 10 and np.std(rets) else 0.0
        return {"total": round(tot, 1), "mdd": round(mdd, 1), "sharpe": round(sharpe, 2)}

    annual: dict[str, dict] = {}
    print("\n## 年度（v1 = 卫星 standalone / v2 = 卫星+停车 / sleeve = 套筒 / 指数 = 510300 买持）")
    print("| 年 | v1 | v2 | v2-v1 | sleeve | 指数 | v2 MDD | v2 SR | 空闲日占比 |")
    print("|---|---|---|---|---|---|---|---|---|")
    for y, idxs in years.items():
        v1 = _seg_stats(sat_nav, idxs)
        v2 = _seg_stats(v2_nav, idxs)
        sl = _seg_stats(sleeve_nav, idxs)
        ix = _seg_stats(idx_nav, idxs)
        idle_share = _mean([1.0 if w[i] > 0.5 else 0.0 for i in idxs]) * 100
        annual[y] = {"v1": v1, "v2": v2, "sleeve": sl, "index": ix, "idle_days_pct": round(idle_share, 1)}
        print(
            f"| {y} | {v1['total']:+.1f} | {v2['total']:+.1f} | {v2['total'] - v1['total']:+.1f} | "
            f"{sl['total']:+.1f} | {ix['total']:+.1f} | {v2['mdd']:.1f} | {v2['sharpe']:.2f} | {idle_share:.0f}% |"
        )

    # ---- monthly distribution
    months: dict[str, list[int]] = {}
    for i, d in enumerate(dates):
        months.setdefault(d[:7], []).append(i)
    m_v2, m_v1 = [], []
    for m, idxs in months.items():
        i0, i1 = idxs[0], idxs[-1]
        base = max(0, i0 - 1)
        m_v2.append((m, (v2_nav[i1] / v2_nav[base] - 1) * 100))
        m_v1.append((m, (sat_nav[i1] / sat_nav[base] - 1) * 100))
    worst = sorted(m_v2, key=lambda x: x[1])[:8]
    best = sorted(m_v2, key=lambda x: -x[1])[:5]
    wins = sum(1 for _, v in m_v2 if v > 0)
    print(
        f"\n## 月度（n={len(m_v2)}）：v2 胜率 {wins}/{len(m_v2)} = {100 * wins / len(m_v2):.0f}% | "
        f"中位 {float(np.median([v for _, v in m_v2])):+.1f}% | v1 胜率 {sum(1 for _, v in m_v1 if v > 0)}/{len(m_v1)}"
    )
    print("  最差月 v2: " + " · ".join(f"{m} {v:+.1f}%" for m, v in worst))
    print("  最好月 v2: " + " · ".join(f"{m} {v:+.1f}%" for m, v in best))

    # ---- regime buckets
    def _bucket(name: str, lab: list[str | None]) -> None:
        groups: dict[str, list[int]] = {}
        for i in range(1, n):
            lg = lab[i]
            if lg is None:
                continue
            groups.setdefault(lg, []).append(i)
        print(f"\n### by {name}")
        print("| 状态 | 天数 | v2 日均为% | v1 日均为% | Δ(v2-v1) 日均 | sleeve 日均为% | 年化 v2% |")
        print("|---|---|---|---|---|---|---|")
        out = {}
        for lg, idxs in sorted(groups.items()):
            v2m = _mean([v2_r[i] for i in idxs]) * 100
            v1m = _mean([sat_r[i] for i in idxs]) * 100
            slm = _mean([sleeve_r[i] for i in idxs]) * 100
            out[lg] = {
                "days": len(idxs),
                "v2_daily_pct": round(v2m, 3),
                "v1_daily_pct": round(v1m, 3),
                "delta_daily_pct": round(v2m - v1m, 3),
                "sleeve_daily_pct": round(slm, 3),
                "v2_annualized_pct": round(v2m * 243, 1),
            }
            print(
                f"| {lg} | {len(idxs)} | {v2m:+.3f} | {v1m:+.3f} | {v2m - v1m:+.3f} | {slm:+.3f} | {v2m * 243:+.1f} |"
            )
        stats[name] = out

    stats: dict[str, dict] = {}
    for name, lab in labels.items():
        _bucket(name, lab)
    _bucket("sleeve_trend", sleeve_ma20)

    # ---- where the parking leg's money is made (sleeve returns by held asset + shell state)
    print("\n### sleeve 日均（按持有资产）")
    print("| 持有 | 天数 | 日均% | 年化% |")
    print("|---|---|---|---|")
    held: dict[str, list[float]] = {}
    for i in range(1, n):
        ts = str((rec_by_day.get(dates[i]) or {}).get("pick_ts") or "REPO")
        held.setdefault(ts, []).append(sleeve_r[i])
    held_stats = {}
    for ts, rs in sorted(held.items(), key=lambda x: -len(x[1])):
        held_stats[ts] = {
            "days": len(rs),
            "daily_pct": round(_mean(rs) * 100, 3),
            "annualized_pct": round(_mean(rs) * 243 * 100, 1),
            "win_rate": round(100 * sum(1 for x in rs if x > 0) / len(rs), 0),
        }
        print(
            f"| {ts} | {len(rs)} | {_mean(rs) * 100:+.3f} | {_mean(rs) * 243 * 100:+.1f} |"
        )

    # ---- v2 delta attribution: parking contribution vs cost, by year
    print("\n### 停车贡献分解（Σ w×sleeve_ret vs 转移成本，pt · 加和口径）")
    print("| 年 | 停车毛贡献 | 转移成本 | 净 |")
    print("|---|---|---|---|")
    delta_years = {}
    for y, idxs in years.items():
        gross = sum(w[i] * sleeve_r[i] for i in idxs) * 100
        cost = sum(5.0 / 1e4 * abs(w[i] - w[i - 1]) for i in idxs if i > 0) * 100
        delta_years[y] = {"gross_pt": round(gross, 1), "cost_pt": round(cost, 2), "net_pt": round(gross - cost, 1)}
        print(f"| {y} | {gross:+.1f} | -{cost:.2f} | {gross - cost:+.1f} |")

    if args.save_report:
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        path = REPORT_DIR / "sat_v2_regime_2026-09-15.json"
        path.write_text(
            json.dumps(
                {
                    "tag": "sat-v2-regime-2026-09-15",
                    "window": [START, END],
                    "annual": annual,
                    "monthly": {"worst": worst, "best": best, "win_rate_pct": round(100 * wins / len(m_v2), 0)},
                    "buckets": stats,
                    "sleeve_by_held": held_stats,
                    "parking_attribution_by_year": delta_years,
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
