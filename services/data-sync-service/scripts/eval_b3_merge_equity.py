#!/usr/bin/env python3
"""B3 execution simplification: merge the 4 sub-5% equity legs into one ETF.

Prereg (frozen, no grid) — user's live-replication problem: B3 inverse-vol gives
the 4 equity legs only 2-4% each (bond ~86%), below practical lot sizes. Merge
the equity sleeve (510300+510500+518880+513100) into ONE broad/semiconductor
ETF held at the combined equity weight, with the bond leg kept separate.

Question (returns-first): does collapsing the equity sleeve cost return?
  V0 incumbent : B3 as-is (5 legs, inverse-vol, monthly).
  V1 equity->510300: equity sum into 沪深300, weight = sum of 4 (recomputed monthly).
  V2 equity->518880: equity sum into 黄金.
  V3 equity->510500: equity sum into 中证500.
  BASE         : pure 510300 buy&hold (reference).

Read-only. Usage:
  cd services/data-sync-service
  PYTHONPATH=src:scripts python3 scripts/eval_b3_merge_equity.py
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from eval_sat_idle_parking import _compose, _repo_nav, _sat_book, _true_cash_share  # noqa: E402
from eval_twin_star_parking import COST, _stats  # noqa: E402
from run_walk_forward import WINDOWS  # noqa: E402

from data_sync_service.service.homeport import (  # noqa: E402
    RISK_UNIVERSE,
    VOL_LOOKBACK,
    _series_on_cal,
    _vol_at,
    inverse_vol_weights,
    load_risk_closes,
)
from data_sync_service.service.state_bucket_track import (  # noqa: E402
    A25_B3_WEIGHT,
    A25_SLEEVE_WEIGHT,
)

STRESS = ("2022-01-01", "2023-12-31")
ALL_WINDOWS = {**WINDOWS, "stress": STRESS}
EQUITY = ("510300.SH", "510500.SH", "518880.SH", "513100.SH")
BOND = "511260.SH"


def _merge_nav(px, cal, target_ts: str, *, cost: float = COST) -> list[float]:
    """Monthly inverse-vol on {EQUITY_SUM, BOND}; EQ_SUM parked in ``target_ts``."""
    series = {ts: _series_on_cal(px.get(ts) or {}, cal) for ts in RISK_UNIVERSE}
    # equity-sum series = renormalised combo of the 4 equity legs (engine basis).
    eq_series: list[float | None] = []
    base = {ts: next((v for v in series[ts] if v), None) for ts in EQUITY}
    for i in range(len(cal)):
        vals = [series[ts][i] for ts in EQUITY]
        if any(v is None for v in vals) or any(base[ts] is None for ts in EQUITY):
            eq_series.append(None)
            continue
        eq_series.append(sum(vals[j] / base[EQUITY[j]] for j in range(len(EQUITY))) / len(EQUITY))
    w_by_i: dict[int, dict[str, float]] = {}
    for i in range(len(cal)):
        if i < VOL_LOOKBACK:
            w_by_i[i] = {target_ts: 0.5, BOND: 0.5}
        elif cal[i][:7] == cal[i - 1][:7]:
            w_by_i[i] = w_by_i[i - 1]
        else:
            ve = _vol_at(eq_series, i, VOL_LOOKBACK) or 1e-9
            vb = _vol_at(series[BOND], i, VOL_LOOKBACK) or 1e-9
            w = inverse_vol_weights({target_ts: ve, BOND: vb})
            w_by_i[i] = w
    tgt_series = series[target_ts]
    nav = [1.0]
    cur = w_by_i[0]
    for i in range(1, len(cal)):
        r = 0.0
        for leg, ser in ((target_ts, tgt_series), (BOND, series[BOND])):
            if ser[i - 1] and ser[i]:
                r += cur.get(leg, 0.0) * (ser[i] / ser[i - 1] - 1.0)
        nav.append(nav[-1] * (1.0 + r))
        if w_by_i[i] != cur:
            turn = sum(abs(w_by_i[i][k] - cur.get(k, 0.0)) for k in w_by_i[i]) / 2.0
            nav[-1] *= 1.0 - cost * turn
            cur = w_by_i[i]
    return nav


def _blend(a_nav, b_nav, a=0.25, b=0.75):
    n = min(len(a_nav), len(b_nav))
    out = [1.0]
    for t in range(1, n):
        ra = a_nav[t] / a_nav[t - 1] - 1 if a_nav[t - 1] else 0.0
        rb = b_nav[t] / b_nav[t - 1] - 1 if b_nav[t - 1] else 0.0
        out.append(out[-1] * (1.0 + a * ra + b * rb))
    return out


def main() -> int:
    px = load_risk_closes()
    print("B3 execution: incumbent 5-leg vs merged equity sleeve\n", flush=True)
    print(f"{'窗口':<9}{'B3 as-is':>22}{'EQ->510300':>22}{'EQ->518880':>22}{'EQ->510500':>22}")
    for wname in ("OOS2", "train", "valid", "long", "stress", "holdout"):
        s, e = ALL_WINDOWS[wname]
        if wname == "holdout":
            pass
        cal = sorted({d for mp in px.values() for d in mp if s <= d <= e})
        # incumbent: reuse homeport risk_budget_nav
        from data_sync_service.service.homeport import risk_budget_nav
        v0 = risk_budget_nav(px, cal)
        v1 = _merge_nav(px, cal, "510300.SH")
        v2 = _merge_nav(px, cal, "518880.SH")
        v3 = _merge_nav(px, cal, "510500.SH")
        fmt = lambda nav: f"{_stats(nav)['total_pct']:+7.1f}/{_stats(nav)['max_dd']:+6.1f}/{_stats(nav)['sharpe']:4.2f}"  # noqa: E731
        print(f"{wname:<9}{fmt(v0):>22}{fmt(v1):>22}{fmt(v2):>22}{fmt(v3):>22}", flush=True)
    print("\n纯 B3 腿（总收益% / maxDD% / Sharpe）。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
