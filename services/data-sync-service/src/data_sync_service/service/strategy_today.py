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
    STARSIP_B_UNIVERSE,
    VOL_LOOKBACK,
    _series_on_cal,
    _vol_at,
    inverse_vol_weights,
    load_risk_closes,
    load_starship_b_closes,
)

B3_LABELS: dict[str, str] = {
    "510300.SH": "沪深300",
    "510500.SH": "中证500",
    "518880.SH": "黄金",
    "513100.SH": "纳指100",
    "511260.SH": "10年国债",
}
MIN_TRADE_PCT = 0.5  # hide dust trades below this target delta


def _raw_last_closes() -> dict[str, float]:
    """Latest RAW close per B3 ETF (``daily.close`` is raw for ETFs; adj NULL).

    Lot sizing must use the tradable raw price, never the adjusted engine basis
    (510300 engine 5.84 vs raw 4.61 on 2026-09-22). Best-effort: {} on failure.
    """
    try:
        from data_sync_service.db import get_connection

        codes = list(RISK_UNIVERSE)
        with get_connection() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT ts_code, close FROM daily WHERE ts_code = ANY(%s) "
                "AND trade_date = (SELECT max(trade_date) FROM daily WHERE ts_code = ANY(%s))",
                (codes, codes),
            )
            return {str(r[0]): float(r[1]) for r in cur.fetchall()}
    except Exception:  # noqa: BLE001
        return {}


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

    raw_px = _raw_last_closes()
    universe = [
        {
            "symbol": ts,
            "name": B3_LABELS.get(ts, ts),
            "targetPct": round(target[ts] * 100, 1),
            "driftPct": round(drift[ts] * 100, 1),
            "deltaPct": round((target[ts] - drift[ts]) * 100, 1),
            # Tradable (raw) price for lot sizing — never the adjusted basis.
            "px": raw_px.get(ts),
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
        "note": (
            "B3 腿内部权重（月频再平衡、5bp/边）。占组合比例由策略档决定："
            "母港/星港 30%、稳健星舰 H2-a25 = 空槽比例 ×75%。paper/实盘记账未接线（OPT-186）"
        ),
    }


def starship_b_state(
    closes: dict[str, dict[str, float]] | None = None,
) -> dict[str, Any]:
    """Current-month 星舰 B park-leg target / drift / rebalance trades.

    Same math as ``b3_state`` but the 3-leg {国债, 黄金, 纳指} inverse-vol
    universe. ``closes`` injectable for tests.
    """
    px = closes if closes is not None else load_starship_b_closes()
    if not px:
        return {"ok": False, "error": "星舰 B risk panel unavailable"}
    universe_ts = STARSIP_B_UNIVERSE
    cal = sorted({d for series in px.values() for d in series})
    if len(cal) <= VOL_LOOKBACK:
        return {"ok": False, "error": "星舰 B history too short"}
    series = {ts: _series_on_cal(px.get(ts) or {}, cal) for ts in universe_ts}
    month = cal[-1][:7]
    rebal_i = next((i for i, d in enumerate(cal) if d[:7] == month), None)
    if rebal_i is None or rebal_i < VOL_LOOKBACK:
        return {"ok": False, "error": "no current-month rebalance anchor"}
    vol = {ts: (_vol_at(series[ts], rebal_i, VOL_LOOKBACK) or 1e-9) for ts in universe_ts}
    target = inverse_vol_weights(vol)
    factor = {
        ts: (series[ts][-1] / series[ts][rebal_i] if series[ts][rebal_i] and series[ts][-1] else 1.0)
        for ts in universe_ts
    }
    gross = sum(target[ts] * factor[ts] for ts in universe_ts) or 1.0
    drift = {ts: target[ts] * factor[ts] / gross for ts in universe_ts}
    raw_px = _raw_last_closes()
    universe = [
        {
            "symbol": ts,
            "name": B3_LABELS.get(ts, ts),
            "targetPct": round(target[ts] * 100, 1),
            "driftPct": round(drift[ts] * 100, 1),
            "deltaPct": round((target[ts] - drift[ts]) * 100, 1),
            "px": raw_px.get(ts),
        }
        for ts in universe_ts
    ]
    trades = [
        {
            "symbol": u["symbol"],
            "name": u["name"],
            "side": "BUY" if u["deltaPct"] > 0 else "SELL",
            "deltaPct": u["deltaPct"],
        }
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
        "note": (
            "星舰 B 停放腿内部权重 = {国债, 黄金, 纳指} 60d 逆波动率（月频再平衡、5bp/边）；"
            "占组合比例 = 空槽比例（cashShare T−1）。paper/实盘记账未接线（OPT-186）"
        ),
    }
