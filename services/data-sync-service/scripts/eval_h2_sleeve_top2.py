#!/usr/bin/env python3
"""H2 sleeve diversification: top-1 (incumbent) vs top-2 equal-weight.

Prereg (frozen, no grid):
  Mechanism: the H2 sleeve can sit 100% in one ETF (the 2026-09 oil case), so a
  single-asset drawdown lands in the portfolio. Splitting the sleeve across the
  mom60 top-2 eligible ETFs should cut the long MDD without giving up the return.
  Causal: rank/trail from the PREV close; earn the prev->day return. Trail8 per
  leg (a trailed leg's share goes to REPO/cash). Cost = COST * sum|dw|.
  Compose: a25 = satellite + cashShare(T-1) x (25% sleeve + 75% B3).
  Verdict: K2 long Dtot vs pure_B3 >= +50; K3 long DMDD vs pure_B3 >= -1.0.

Read-only. Usage:
  cd services/data-sync-service
  PYTHONPATH=src:scripts python3 scripts/eval_h2_sleeve_top2.py
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from eval_sat_idle_parking import (  # noqa: E402
    _compose,
    _repo_nav,
    _sat_book,
    _sleeve_nav,
    _true_cash_share,
)
from eval_twin_star_parking import COST, _stats  # noqa: E402
from run_walk_forward import WINDOWS  # noqa: E402

from data_sync_service.service.harbor import (  # noqa: E402
    HYST_BAND,
    MULTI_TS,
    load_etf_closes,
    pick_parking,
)
from data_sync_service.service.homeport import load_risk_closes, risk_budget_nav  # noqa: E402
from data_sync_service.service.state_bucket_track import (  # noqa: E402
    A25_B3_WEIGHT,
    A25_SLEEVE_WEIGHT,
)

STRESS = ("2022-01-01", "2023-12-31")
ALL_WINDOWS = {**WINDOWS, "stress": STRESS}


def sleeve_topn_nav(px, dates, *, top_n: int, trail_pct: float = 8.0, cost: float = COST):
    days_by_ts = {ts: sorted(mp) for ts, mp in px.items()}
    nav = [1.0]
    prev_w: dict[str, float] = {}
    peaks: dict[str, float] = {}
    for i in range(1, len(dates)):
        day, prev = dates[i], dates[i - 1]
        info = pick_parking(px, prev, days_by_ts=days_by_ts)
        sel: list[str] = []
        if info:
            elig = [k for k, ab in info["all_above"].items()
                    if ab and info["all_mom"].get(k) is not None]
            elig.sort(key=lambda k: info["all_mom"][k], reverse=True)
            sel = elig[:top_n]
        weights: dict[str, float] = {}
        n = len(sel)
        for k in sel:
            ts = MULTI_TS[k]
            c_prev = (px.get(ts) or {}).get(prev)
            if c_prev is None:
                continue
            peak = peaks.get(k, 0.0)
            if peak > 0 and c_prev < peak * (1 - trail_pct / 100):
                peaks[k] = 0.0
                continue  # trailed leg -> its share goes to REPO/cash
            peaks[k] = max(peak, c_prev)
            weights[ts] = 1.0 / n
        for k in list(peaks):
            if k not in sel:
                peaks.pop(k, None)
        r = 0.0
        for ts, wt in weights.items():
            c0 = (px.get(ts) or {}).get(prev)
            c1 = (px.get(ts) or {}).get(day)
            if c0 and c1:
                r += wt * (c1 / c0 - 1.0)
        turnover = sum(abs(weights.get(t, 0.0) - prev_w.get(t, 0.0))
                       for t in set(weights) | set(prev_w))
        nav.append(nav[-1] * (1.0 + r - cost * turnover))
        prev_w = weights
    return nav


def _blend(sleeve, b3, repo, a, b):
    n = min(len(sleeve), len(b3), len(repo))
    out = [1.0]
    for t in range(1, n):
        rs = sleeve[t] / sleeve[t - 1] - 1 if sleeve[t - 1] else 0.0
        rb = b3[t] / b3[t - 1] - 1 if b3[t - 1] else 0.0
        rr = repo[t] / repo[t - 1] - 1 if repo[t - 1] else 0.0
        out.append(out[-1] * (1.0 + a * rs + b * rb + rr * 0.0))
    return out


def main() -> int:
    px = load_etf_closes()
    risk = load_risk_closes()
    a, b = A25_SLEEVE_WEIGHT, A25_B3_WEIGHT
    print("H2-a25 sleeve: top-1 (incumbent) vs top-2 equal\n", flush=True)
    print(f"{'窗口':<9}{'sleeve top1':>24}{'sleeve top2':>24}{'a25 top1':>24}{'a25 top2':>24}{'pureB3':>24}")
    for wname in ("OOS2", "train", "valid", "long", "stress", "holdout"):
        s, e = ALL_WINDOWS[wname]
        print(f"  computing {wname} ...", flush=True)
        sat = _sat_book(s, e)
        dates, sat_nav = sat["dates"], sat["nav"]
        b3 = risk_budget_nav(risk, dates)
        repo = _repo_nav(dates)
        cash = _true_cash_share(sat, from_rows=True)
        w = [0.0] * len(dates)
        for i in range(1, len(dates)):
            w[i] = cash[i - 1]
        sl1 = _sleeve_nav(px, dates, hyst_band=HYST_BAND)
        sl2 = sleeve_topn_nav(px, dates, top_n=2)
        nav1 = _compose(sat_nav, w, _blend(sl1, b3, repo, a, b), 5.0)
        nav2 = _compose(sat_nav, w, _blend(sl2, b3, repo, a, b), 5.0)
        rb = _compose(sat_nav, w, _blend(sl1, b3, repo, 0.0, 1.0), 5.0)
        fmt = lambda nav: f"{_stats(nav)['total_pct']:+7.1f}/{_stats(nav)['max_dd']:+6.1f}/{_stats(nav)['sharpe']:4.2f}"  # noqa: E731
        print(f"{wname:<9}{fmt(sl1):>24}{fmt(sl2):>24}{fmt(nav1):>24}{fmt(nav2):>24}{fmt(rb):>24}", flush=True)
    print("\n列 = 总收益% / maxDD% / Sharpe。sleeve = 纯套筒 NAV；a25 = 组合。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
