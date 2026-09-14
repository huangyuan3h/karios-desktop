"""Per-strategy "today" decision state (display only; never order wiring).

Covers the legs that are not part of the daily Harbor book:
- B3 risk-budget sleeve (母港/星港): monthly inverse-vol target vs drifted
  weights -> rebalance trade list for the current month.
- Satellite leg state is served by the timeline rows (`strategy=starport|starship`,
  satActive/satPositions/slots); today's 14:30 signal list is pending OPT-178/186.

Live stays 港湾 — these numbers are reference decisions only.
"""

from __future__ import annotations

from typing import Any

from data_sync_service.service.homeport import (
    RISK_UNIVERSE,
    VOL_LOOKBACK,
    _series_on_cal,
    _vol_at,
    inverse_vol_weights,
    load_risk_closes,
)

B3_LABELS: dict[str, str] = {
    "510300.SH": "沪深300",
    "510500.SH": "中证500",
    "518880.SH": "黄金",
    "513100.SH": "纳指100",
    "511260.SH": "10年国债",
}
MIN_TRADE_PCT = 0.5  # hide dust trades below this target delta


def b3_state(
    closes: dict[str, dict[str, float]] | None = None,
) -> dict[str, Any]:
    """Current-month B3 target / drift / rebalance trades.

    Same math as ``homeport.risk_budget_nav`` (60d inverse vol recomputed on the
    first trading day of each month, causal). ``closes`` is injectable for tests.
    """
    px = closes if closes is not None else load_risk_closes()
    if not px:
        return {"ok": False, "error": "B3 risk panel unavailable (data/etf/etf_daily.csv missing)"}
    cal = sorted({d for series in px.values() for d in series})
    if len(cal) <= VOL_LOOKBACK:
        return {"ok": False, "error": "B3 history too short"}
    series = {ts: _series_on_cal(px.get(ts) or {}, cal) for ts in RISK_UNIVERSE}

    month = cal[-1][:7]
    rebal_i = next((i for i, d in enumerate(cal) if d[:7] == month), None)
    if rebal_i is None or rebal_i < VOL_LOOKBACK:
        return {"ok": False, "error": "no current-month rebalance anchor"}

    vol = {ts: (_vol_at(series[ts], rebal_i, VOL_LOOKBACK) or 1e-9) for ts in RISK_UNIVERSE}
    target = inverse_vol_weights(vol)
    factor = {
        ts: (
            series[ts][-1] / series[ts][rebal_i]
            if series[ts][rebal_i] and series[ts][-1]
            else 1.0
        )
        for ts in RISK_UNIVERSE
    }
    gross = sum(target[ts] * factor[ts] for ts in RISK_UNIVERSE) or 1.0
    drift = {ts: target[ts] * factor[ts] / gross for ts in RISK_UNIVERSE}

    universe = [
        {
            "symbol": ts,
            "name": B3_LABELS.get(ts, ts),
            "targetPct": round(target[ts] * 100, 1),
            "driftPct": round(drift[ts] * 100, 1),
            "deltaPct": round((target[ts] - drift[ts]) * 100, 1),
        }
        for ts in RISK_UNIVERSE
    ]
    trades = [
        {"symbol": u["symbol"], "name": u["name"], "side": "BUY" if u["deltaPct"] > 0 else "SELL", "deltaPct": u["deltaPct"]}
        for u in universe
        if abs(u["deltaPct"]) >= MIN_TRADE_PCT
    ]
    return {
        "ok": True,
        "asOf": cal[-1],
        "rebalanceDate": cal[rebal_i],
        "month": month,
        "universe": universe,
        "trades": trades,
        "note": "B3 腿内部权重（占组合 50%）；月频再平衡、5bp/边；paper/实盘记账未接线（OPT-186）",
    }
