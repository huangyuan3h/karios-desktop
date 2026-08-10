#!/usr/bin/env python3
"""T1: joint two-market walk-forward — shared capital pool allocation rules.

The two strategy lines (CN S-3, HK parallel) are independent simulations
(shared rule code, per-market gates). This tool evaluates how to split ONE
capital pool between them when BOTH markets are strong:

  R1  equal 50/50 (constant)
  R2  strength-ratio weighting (weekly rebalance; T2 regime_strength_score)
  R3  relative 20d momentum (CSI300 vs HSI, softmax, weekly)
  R4  fixed 60/40 (CN/HK)

Each market's daily NAV is rebuilt from its simulated trades (per-trade
position_pct mark-to-market along the real close path). The joint NAV is
w_cn(t)*NAV_cn(t) + w_hk(t)*NAV_hk(t); weights are clamped to [0.2, 0.8]
and only matter while both markets actually hold positions (each market's
own regime gate already zeroes out a weak market).

Usage:
  PYTHONPATH=src python3 scripts/run_walk_forward_dual.py
  PYTHONPATH=src python3 scripts/run_walk_forward_dual.py --windows train,valid
  PYTHONPATH=src python3 scripts/run_walk_forward_dual.py --rules R1,R2
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import UTC, date, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from run_walk_forward import HK_S3_CONFIG, S3_CONFIG, WINDOWS  # noqa: E402

from data_sync_service.service.backtest_engine import (  # noqa: E402
    BacktestConfig,
    BacktestData,
    _resolve_ts_code,
    simulate,
)
from data_sync_service.service.market_regime import regime_strength_score  # noqa: E402

RULES = ("R1", "R2", "R3", "R4", "R5a", "R5b")
WEIGHT_CLAMP = (0.2, 0.8)
SHARPE_DAYS = 252


def _mk_config(market: str, start: str, end: str) -> BacktestConfig:
    base = HK_S3_CONFIG if market == "HK" else S3_CONFIG
    return BacktestConfig(start_date=start, end_date=end, **base)


def rebuild_daily_pnl(
    run, data: BacktestData, config: BacktestConfig
) -> dict[str, float]:
    """day -> net portfolio pnl% contribution from closed trades (per-day mark).

    Each round trip contributes (close_d/entry_px - 1) * position_pct on the
    days between entry and close, following the real daily close path. Open
    positions at window end are excluded (same convention as the summary).
    """
    pnl: dict[str, float] = {}
    for tr in run.trades:
        resolved = _resolve_ts_code(tr.symbol)
        if resolved is None:
            continue
        ts = resolved[1]
        closes = data.close_by_ts_day.get(ts, {})
        entry_px = tr.entry_price or 0.0
        if entry_px <= 0:
            continue
        pos = tr.position_pct or config.position_pct
        prev = entry_px
        for day, close in sorted(closes.items()):
            if tr.entry_date <= day <= tr.close_date and close > 0:
                incr = (close / prev - 1.0) * pos
                if day == tr.close_date:
                    incr -= (tr.costs_pct / 100.0) * pos
                pnl[day] = pnl.get(day, 0.0) + incr
                prev = close
    return pnl


def nav_from_pnl(pnl: dict[str, float], calendar: list[str]) -> list[float]:
    nav = [1.0]
    for day in calendar:
        nav.append(nav[-1] * (1.0 + pnl.get(day, 0.0)))
    return nav[1:]


def weekly_weights(
    start: str, end: str, rule: str, market_codes: dict[str, list[str]],
    regimes: dict[str, dict[str, str]] | None = None,
) -> dict[str, tuple[float, float]]:
    """week_start_date -> (w_cn, w_hk). Weights decided on the week's first day.

    R5 rules use each market's real gate regime (engine regime_by_day, same
    source as the live gates): a Weak market is FORCED to 0 weight — its
    capital migrates fully to the other market (substitution), which is the
    core of the dynamic allocation idea.
      R5a  one strong one weak -> 100% the strong one; both strong -> 50/50;
           both weak -> 0/0 (both NAVs flat anyway)
      R5b  same but both-strong uses the 20d momentum rate ratio (rate of
           climb) instead of fixed 50/50
    """
    out: dict[str, tuple[float, float]] = {}
    d = date.fromisoformat(start)
    while d <= date.fromisoformat(end):
        wk = d.isoformat()
        if rule == "R1":
            out[wk] = (0.5, 0.5)
        elif rule == "R4":
            out[wk] = (0.6, 0.4)
        elif rule == "R2":
            s_cn = regime_strength_score(market="CN", as_of_date=wk)["strength"]
            s_hk = regime_strength_score(market="HK", as_of_date=wk)["strength"]
            total = s_cn + s_hk
            w_cn = 0.5 if total <= 0 else s_cn / total
            out[wk] = _clamp_pair(w_cn)
        elif rule in ("R5a", "R5b"):
            # Same-decision-code rule: live path calls allocation.resolve_weights,
            # the backtest replays the SAME function on as-of regimes.
            from data_sync_service.service.allocation import weights_from_regimes

            r_cn = _regime_at(regimes["CN"], wk) if regimes else "Weak"
            r_hk = _regime_at(regimes["HK"], wk) if regimes else "Weak"
            w_cn, w_hk = weights_from_regimes(r_cn, r_hk)
            both_ok = (r_cn in ("Strong", "Diverging")) and (r_hk in ("Strong", "Diverging"))
            if both_ok:
                if rule == "R5a":
                    w_cn, w_hk = (0.5, 0.5)
                else:
                    m_cn = regime_strength_score(market="CN", as_of_date=wk)["components"]["momentum"]
                    m_hk = regime_strength_score(market="HK", as_of_date=wk)["components"]["momentum"]
                    total_m = m_cn + m_hk
                    w_cn = 0.5 if total_m <= 0 else m_cn / total_m
                    w_hk = 1.0 - w_cn
            out[wk] = (w_cn, w_hk)
            m_cn = _index_momentum(market_codes["cn"], wk)
            m_hk = _index_momentum(market_codes["hk"], wk)
            e_cn, e_hk = math.exp(m_cn), math.exp(m_hk)
            out[wk] = _clamp_pair(e_cn / (e_cn + e_hk))
        d = d.fromordinal(d.toordinal() + 7)
    return out


def _regime_at(regime_by_day: dict[str, str], day: str) -> str:
    """Regime of the latest trading day <= day (week starts are calendar days)."""
    keys = sorted(regime_by_day)
    for k in reversed(keys):
        if k <= day:
            return regime_by_day[k]
    return "Weak"


def _clamp_pair(w_cn: float) -> tuple[float, float]:
    lo, hi = WEIGHT_CLAMP
    w_cn = max(lo, min(hi, w_cn))
    return w_cn, 1.0 - w_cn


def _index_momentum(codes: list[str], as_of: str) -> float:
    from data_sync_service.db.index_daily import fetch_last_closes_vol_batch

    raw = fetch_last_closes_vol_batch(codes, days=25, as_of_date=as_of)
    moms = []
    for code in codes:
        closes = [float(c) for _, c, _ in raw.get(code, [])]
        if len(closes) >= 21 and closes[-21] > 0:
            moms.append(closes[-1] / closes[-21] - 1.0)
    return sum(moms) / len(moms) if moms else 0.0


def joint_stats(
    nav_cn: list[float], nav_hk: list[float], calendar: list[str], weights: dict[str, tuple[float, float]]
) -> dict[str, float]:
    """Union calendar + weekly weights -> joint pool NAV.

    IMPORTANT (2026-08-11): with changing weights, NAV-weighted averaging
    (w*NAV_cn + (1-w)*NAV_hk) is WRONG — it resets the pool to the other
    market's 1.0-based NAV at every switch, throwing away accumulated gains.
    Correct: weight the DAILY RETURNS, compounding the single capital pool:
        joint[t] = joint[t-1] * (1 + w(t)*r_cn(t) + (1-w)(t)*r_hk(t))
    NAV-weighting is only equivalent when weights are constant (R1/R4).
    """
    joint = [1.0]
    prev_cn, prev_hk = nav_cn[0], nav_hk[0]
    for i in range(1, len(calendar)):
        w_cn = 0.5
        for wk in sorted(weights):
            if calendar[i] >= wk:
                w_cn = weights[wk][0]
        r_cn = nav_cn[i] / prev_cn - 1.0 if prev_cn > 0 else 0.0
        r_hk = nav_hk[i] / prev_hk - 1.0 if prev_hk > 0 else 0.0
        joint.append(joint[-1] * (1.0 + w_cn * r_cn + (1.0 - w_cn) * r_hk))
        prev_cn, prev_hk = nav_cn[i], nav_hk[i]

    daily_ret = [joint[i] / joint[i - 1] - 1.0 for i in range(1, len(joint)) if joint[i - 1] > 0]
    total = joint[-1] - 1.0
    peak = joint[0]
    max_dd = 0.0
    for v in joint:
        peak = max(peak, v)
        if peak > 0:
            max_dd = max(max_dd, (peak - v) / peak)
    sharpe = 0.0
    if len(daily_ret) > 1:
        mean = sum(daily_ret) / len(daily_ret)
        var = sum((r - mean) ** 2 for r in daily_ret) / (len(daily_ret) - 1)
        if var > 0:
            sharpe = mean / math.sqrt(var) * math.sqrt(SHARPE_DAYS)
    years = max(1, len(calendar)) / 250.0
    return {
        "totalNetPnlPct": total * 100.0,
        "maxDrawdownPct": max_dd * 100.0,
        "sharpe": round(sharpe, 2),
        "annualPct": ((1.0 + total) ** (1.0 / years) - 1.0) * 100.0,
    }


def _md_table(rows: list[list[str]]) -> str:
    out = [rows[0], ["-" * len(c) for c in rows[0]]]
    for r in rows[1:]:
        out.append(r)
    return "\n".join("| " + " | ".join(r) + " |" for r in out)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--windows", default="OOS2,train,valid", help="Comma-separated windows to run")
    ap.add_argument("--rules", default=",".join(RULES), help="Comma-separated rules (R1,R2,R3,R4)")
    ap.add_argument("--json", help="Write the full report to this file (default walk_forward_dual_latest.json)")
    args = ap.parse_args()

    windows = [w.strip() for w in args.windows.split(",") if w.strip()]
    rules = [r.strip().upper() for r in args.rules.split(",") if r.strip().upper() in RULES]
    missing = [w for w in windows if w not in WINDOWS]
    if missing or not rules:
        print(f"ERROR: bad windows {missing} or rules {args.rules} (rules: {RULES})", file=sys.stderr)
        return 2

    report: dict = {"generatedAt": datetime.now(UTC).isoformat(), "rules": rules, "windows": {}}
    for w in windows:
        start, end = WINDOWS[w]
        print(f"\n== {w} {start}..{end} ==")
        runs = {}
        for market in ("CN", "HK"):
            cfg = _mk_config(market, start, end)
            data = BacktestData(cfg)
            run = simulate(cfg, data=data)
            runs[market] = (cfg, data, run)
            print(f"  {market}: closed={run.summary.closed} total={run.summary.total_net_pnl_pct:+.1f}% "
                  f"dd={run.summary.max_drawdown_pct:.1f}% sharpe={run.summary.sharpe}")

        _, data_cn, run_cn = runs["CN"]
        _, data_hk, run_hk = runs["HK"]
        pnl_cn = rebuild_daily_pnl(run_cn, data_cn, runs["CN"][0])
        pnl_hk = rebuild_daily_pnl(run_hk, data_hk, runs["HK"][0])
        union = sorted(set(data_cn.calendar) | set(data_hk.calendar))
        nav_cn = _ffill(pnl_cn, data_cn.calendar, union)
        nav_hk = _ffill(pnl_hk, data_hk.calendar, union)

        market_codes = {
            "cn": ["000300.SH"],
            "hk": ["HSI"],
        }
        regimes = {"CN": data_cn.regime_by_day, "HK": data_hk.regime_by_day}
        rows = [["规则", "联合收益%", "联合回撤%", "夏普", "年化%"]]
        for rule in rules:
            weights = weekly_weights(start, end, rule, market_codes, regimes=regimes)
            stats = joint_stats(nav_cn, nav_hk, union, weights)
            rows.append([rule, f"{stats['totalNetPnlPct']:+.1f}", f"{stats['maxDrawdownPct']:.1f}",
                         f"{stats['sharpe']:.2f}", f"{stats['annualPct']:+.1f}"])
            report.setdefault("windows", {}).setdefault(w, {})[rule] = stats
        print(_md_table(rows))

    out_file = Path(args.json) if args.json else Path(__file__).resolve().parents[1] / "data" / "backtest_reports" / "walk_forward_dual_latest.json"
    out_file.parent.mkdir(parents=True, exist_ok=True)
    out_file.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    print(f"\nreport -> {out_file}")
    return 0


def _ffill(pnl: dict[str, float], own_calendar: list[str], union: list[str]) -> list[float]:
    """NAV on own calendar, forward-filled onto the union calendar."""
    own_nav = nav_from_pnl(pnl, own_calendar)
    own_index = 0
    out: list[float] = []
    for day in union:
        while own_index < len(own_calendar) - 1 and own_calendar[own_index] < day:
            own_index += 1
        out.append(own_nav[min(own_index, len(own_nav) - 1)] if own_calendar[own_index] == day else (out[-1] if out else 1.0))
    return out


if __name__ == "__main__":
    raise SystemExit(main())
