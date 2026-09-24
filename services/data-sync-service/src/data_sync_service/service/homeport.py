"""「母港」(Homeport) = Harbor x B3 risk-budget 50/50 — Timeline research option.

Validated in H-MIX (docs/backtests/stable/harbor-riskbudget-2026-09-13.md):
monthly rebalance back to 50/50 between the Harbor NAV and a 5-asset inverse-vol
risk-budget sleeve (monthly, 5bp/side); 5bp/side on the blend.

Display only: `GET /api/backtest/timeline?strategy=homeport` derives the
Homeport rows from the cached Harbor timeline (no engine re-run, no Live wiring).
"""

from __future__ import annotations

import csv
from pathlib import Path
from statistics import pstdev
from typing import Any

from data_sync_service.service.harbor import merge_recent_db_closes

RISK_UNIVERSE: tuple[str, ...] = (
    "510300.SH",
    "510500.SH",
    "518880.SH",
    "513100.SH",
    "511260.SH",
)
VOL_LOOKBACK = 60
COST = 0.0005
WEIGHT_HARBOR = 0.5
MODE = "homeport"
STRATEGY_LABEL = "母港"


def load_risk_closes() -> dict[str, dict[str, float]]:
    """Adjusted closes for the B3 universe (research panel + fresh DB tail)."""
    csv_path = Path(__file__).resolve().parents[3] / "data" / "etf" / "etf_daily.csv"
    wanted = set(RISK_UNIVERSE)
    out: dict[str, dict[str, float]] = {}
    if csv_path.exists():
        with csv_path.open(newline="", encoding="utf-8") as fh:
            for row in csv.DictReader(fh):
                ts = str(row.get("ts_code") or "")
                if ts not in wanted:
                    continue
                d = str(row.get("trade_date") or "")
                d = f"{d[:4]}-{d[4:6]}-{d[6:8]}"
                try:
                    c = float(row.get("close_adj") or 0)
                except (TypeError, ValueError):
                    continue
                if c > 0:
                    out.setdefault(ts, {})[d] = c
    return merge_recent_db_closes(out, wanted)


def load_starship_b_closes() -> dict[str, dict[str, float]]:
    """Adjusted closes for the 星舰 B park universe (国债/黄金/纳指).

    Reuses the B3 research panel (``data/etf/etf_daily.csv``, adjusted basis)
    and merges the fresh DB tail the same way ``load_risk_closes`` does, so the
    display/backtest leg matches Live. 513100 is present in the panel; its DB
    rows are raw, so ``merge_recent_db_closes`` applies the adj anchor.
    """
    return load_risk_closes()


def inverse_vol_weights(vol: dict[str, float]) -> dict[str, float]:
    """w_i ∝ 1/sigma_i (B3 risk budget); equal weight when vols are unusable."""
    inv = {k: 1.0 / v for k, v in vol.items() if v and v > 0}
    tot = sum(inv.values())
    if tot <= 0:
        return {k: 1.0 / len(vol) for k in vol}
    return {k: inv.get(k, 0.0) / tot for k in vol}


def _series_on_cal(closes: dict[str, float], cal: list[str]) -> list[float | None]:
    out: list[float | None] = []
    last: float | None = None
    for d in cal:
        v = closes.get(d)
        if v:
            last = v
        out.append(last)
    return out


def _vol_at(series: list[float | None], i: int, lookback: int) -> float:
    rets: list[float] = []
    for j in range(max(1, i - lookback), i):
        a, b = series[j - 1], series[j]
        if a and b:
            rets.append(b / a - 1.0)
    return pstdev(rets) if len(rets) >= 2 else 0.0


def risk_budget_run(
    closes: dict[str, dict[str, float]],
    cal: list[str],
    *,
    lookback: int = VOL_LOOKBACK,
    cost: float = COST,
) -> dict[str, Any]:
    """B3 sleeve NAV + monthly rebalance events + per-day weights (all causal).

    Same loop as the legacy ``risk_budget_nav`` (NAV bit-identical); it also
    records every rebalance event (initial + each month turn, with turnover)
    and the weight vector held each day, so Timeline views (母港/星港) can
    render the B3 leg instead of hiding half the portfolio.
    """
    series = {ts: _series_on_cal(closes.get(ts) or {}, cal) for ts in RISK_UNIVERSE}
    w_by_i: dict[int, dict[str, float]] = {}
    for i in range(len(cal)):
        if i < lookback:
            w_by_i[i] = {ts: 1.0 / len(RISK_UNIVERSE) for ts in RISK_UNIVERSE}
        elif cal[i][:7] == cal[i - 1][:7]:
            w_by_i[i] = w_by_i[i - 1]
        else:
            vol = {ts: (_vol_at(series[ts], i, lookback) or 1e-9) for ts in RISK_UNIVERSE}
            w_by_i[i] = inverse_vol_weights(vol)

    def _rounded(w: dict[str, float]) -> dict[str, float]:
        return {ts: round(float(w.get(ts) or 0.0), 4) for ts in RISK_UNIVERSE}

    nav = [1.0]
    cur = w_by_i[0]
    events: list[dict[str, Any]] = [
        {"date": cal[0] if cal else "", "weights": _rounded(cur), "turnover": 0.0}
    ]
    for i in range(1, len(cal)):
        r = sum(
            cur[ts]
            * (
                series[ts][i] / series[ts][i - 1] - 1.0
                if series[ts][i - 1] and series[ts][i]
                else 0.0
            )
            for ts in RISK_UNIVERSE
        )
        nav.append(nav[-1] * (1.0 + r))
        if w_by_i[i] != cur:
            turn = sum(abs(w_by_i[i][ts] - cur[ts]) for ts in RISK_UNIVERSE) / 2.0
            nav[-1] *= 1.0 - cost * turn
            events.append(
                {"date": cal[i], "weights": _rounded(w_by_i[i]), "turnover": round(turn, 4)}
            )
            cur = w_by_i[i]
    return {"nav": nav, "events": events, "weights": [w_by_i[i] for i in range(len(cal))]}


def risk_budget_nav(
    closes: dict[str, dict[str, float]],
    cal: list[str],
    *,
    lookback: int = VOL_LOOKBACK,
    cost: float = COST,
) -> list[float]:
    """B3 sleeve NAV on ``cal`` (monthly inverse-vol rebalance, causal, costed)."""
    return list(risk_budget_run(closes, cal, lookback=lookback, cost=cost)["nav"])


# 星舰 B parking universe (2026-09-24): BOND10 + GOLD + NASDAQ inverse-vol.
# Reuses the B3 risk-budget math (monthly 60d inverse-vol, causal, 5bp/side).
STARSIP_B_UNIVERSE: tuple[str, ...] = ("511260.SH", "518880.SH", "513100.SH")


def starship_b_run(
    closes: dict[str, dict[str, float]],
    cal: list[str],
    *,
    lookback: int = VOL_LOOKBACK,
    cost: float = COST,
) -> dict[str, Any]:
    """星舰 B park-leg NAV (3-leg inverse vol: 国债 + 黄金 + 纳指), causal monthly.

    Single source for the Live/display/backtest parking leg. Same loop shape as
    ``risk_budget_run`` but over ``STARSIP_B_UNIVERSE``. ``closes`` must be the
    adjusted ETF panel (adj basis) merged with the fresh DB tail.
    """
    universe = STARSIP_B_UNIVERSE
    series = {ts: _series_on_cal(closes.get(ts) or {}, cal) for ts in universe}
    w_by_i: dict[int, dict[str, float]] = {}
    for i in range(len(cal)):
        if i < lookback:
            w_by_i[i] = {ts: 1.0 / len(universe) for ts in universe}
        elif cal[i][:7] == cal[i - 1][:7]:
            w_by_i[i] = w_by_i[i - 1]
        else:
            vol = {ts: (_vol_at(series[ts], i, lookback) or 1e-9) for ts in universe}
            w_by_i[i] = inverse_vol_weights(vol)
    nav = [1.0]
    cur = w_by_i[0]
    events: list[dict[str, Any]] = [
        {"date": cal[0] if cal else "", "weights": {ts: round(cur[ts], 4) for ts in universe}, "turnover": 0.0}
    ]
    for i in range(1, len(cal)):
        r = sum(
            cur[ts] * (series[ts][i] / series[ts][i - 1] - 1.0)
            for ts in universe
            if series[ts][i - 1] and series[ts][i]
        )
        nav.append(nav[-1] * (1.0 + r))
        if w_by_i[i] != cur:
            turn = sum(abs(w_by_i[i][ts] - cur[ts]) for ts in universe) / 2.0
            nav[-1] *= 1.0 - cost * turn
            events.append({"date": cal[i], "weights": {ts: round(w_by_i[i][ts], 4) for ts in universe}, "turnover": round(turn, 4)})
            cur = w_by_i[i]
    return {"nav": nav, "events": events, "weights": [w_by_i[i] for i in range(len(cal))]}


def starship_b_nav(
    closes: dict[str, dict[str, float]],
    cal: list[str],
    *,
    lookback: int = VOL_LOOKBACK,
    cost: float = COST,
) -> list[float]:
    """星舰 B park-leg NAV series (see ``starship_b_run``)."""
    return list(starship_b_run(closes, cal, lookback=lookback, cost=cost)["nav"])


def blend_monthly_nav(
    harbor: list[float],
    passive: list[float],
    cal: list[str],
    *,
    w_harbor: float = WEIGHT_HARBOR,
    cost: float = COST,
) -> list[float]:
    """Homeport NAV: monthly reset back to ``w_harbor``, 5bp/side on the trade.

    Cost convention (OPT-211 P2, pinned): the monthly reset is charged
    ``cost * |w_harbor - w|`` — one-sided on the rebalanced amount, matching
    the frozen eval caliber (``scripts/eval_harbor_riskbudget._blend_monthly``,
    same formula). A literal two-ticket execution would cost ~2x; the gap is
    ~0.1%/yr (monthly drift |Δ| is 1-3%), covered by the 15bp cost
    sensitivity in the frozen docs. Do not "fix" one side without the other.
    """
    n = min(len(harbor), len(passive))
    out = [1.0]
    w = w_harbor
    for i in range(1, n):
        ra = harbor[i] / harbor[i - 1] - 1.0 if harbor[i - 1] else 0.0
        rb = passive[i] / passive[i - 1] - 1.0 if passive[i - 1] else 0.0
        port = 1.0 + w * ra + (1.0 - w) * rb
        out.append(out[-1] * port)
        if port > 0:
            w = w * (1.0 + ra) / port
        if cal[i][:7] != cal[i - 1][:7]:
            out[-1] *= 1.0 - cost * abs(w_harbor - w)
            w = w_harbor
    return out


def blend_homeport_timeline(
    harbor_result: dict[str, Any],
    *,
    risk_closes: dict[str, dict[str, float]] | None = None,
    w_harbor: float = WEIGHT_HARBOR,
) -> dict[str, Any]:
    """Derive the Homeport timeline from a built Harbor timeline (rows copy).

    Besides the blended NAV, every B3-leg detail the Timeline needs is
    attached (the B3 sleeve must not stay invisible): ``riskUniverse``
    (ts + display names), ``riskBlotter``
    (monthly rebalance events with weights + turnover), ``riskHeld``
    (weights at the window end), per-row ``riskTop``/``riskTopW`` (top
    holding that day, for the bar strip + day table), and summary
    ``riskPct``/``riskMaxDdPct`` (sleeve total / drawdown).
    """
    out = dict(harbor_result)
    rows = [dict(r) for r in (harbor_result.get("rows") or [])]
    harbor_total = float(((harbor_result.get("summary") or {}).get("fusedPct")) or 0.0)
    out["mode"] = MODE
    out["strategy"] = STRATEGY_LABEL
    out["rows"] = rows
    out["summary"] = {
        **(harbor_result.get("summary") or {}),
        "fusedPct": round(harbor_total, 2),
    }
    if not rows:
        return out
    # Display names stay single-sourced in strategy_today.B3_LABELS; lazy
    # import here because strategy_today imports this module at top level.
    from data_sync_service.service.strategy_today import B3_LABELS

    px = risk_closes if risk_closes is not None else load_risk_closes()
    if not px:
        raise RuntimeError("homeport risk panel unavailable (data/etf/etf_daily.csv missing)")
    cal = [str(rows[0]["prev"])] + [str(r["date"]) for r in rows]
    run = risk_budget_run(px, cal)
    passive = run["nav"]
    harbor_nav = [1.0] + [float(r["navSingle"]) for r in rows]
    mix = blend_monthly_nav(harbor_nav, passive, cal, w_harbor=w_harbor)
    for r, v in zip(rows, mix[1:], strict=True):
        r["navSingle"] = round(v, 6)
        r["navMulti"] = round(v, 6)
        r["navSingleReturnPct"] = round((v - 1) * 100, 2)
        r["navMultiReturnPct"] = round((v - 1) * 100, 2)
    # B3 leg: per-day top holding (row i <-> cal index i+1: weights held that day).
    day_weights = run["weights"][1:]
    for r, w in zip(rows, day_weights, strict=True):
        top = max(RISK_UNIVERSE, key=lambda ts: float(w.get(ts) or 0.0))
        r["riskTop"] = top
        r["riskTopW"] = round(float(w.get(top) or 0.0), 4)
    risk_peak = 0.0
    risk_dd = 0.0
    for v in passive:
        risk_peak = max(risk_peak, v)
        if risk_peak > 0:
            risk_dd = max(risk_dd, (risk_peak - v) / risk_peak)
    last_w = run["weights"][-1] if run["weights"] else {}
    out["riskUniverse"] = [
        {"ts": ts, "name": B3_LABELS.get(ts, ts)} for ts in RISK_UNIVERSE
    ]
    out["riskBlotter"] = run["events"]
    out["riskHeld"] = {
        "date": cal[-1],
        "weights": {ts: round(float(last_w.get(ts) or 0.0), 4) for ts in RISK_UNIVERSE},
    }
    out["summary"] = {
        **out["summary"],
        "fusedPct": round((mix[-1] - 1) * 100, 2),
        "harborPct": round(harbor_total, 2),
        "harborWeight": round(float(w_harbor), 2),
        "riskPct": round((passive[-1] - 1) * 100, 2),
        "riskMaxDdPct": round(risk_dd * 100, 1),
    }
    return out
