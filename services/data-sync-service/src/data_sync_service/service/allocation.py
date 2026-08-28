"""Cross-market capital allocation (T3/R5c, 2026-08-11).

The ONLY input is the traffic-light regime of each market — everything is
as-of visible (no momentum/rate lookahead, no strength scoring that cannot be
reproduced on the day). The backtest and the live system share this module,
so the allocation decided in backtests is byte-for-byte the same decision
made on the day (user decision: "只能按照红绿灯系统来判断").

Rule R5c:
  CN tradable (Strong/Diverging)  -> 100% CN
  only HK tradable                -> 100% HK
  both weak                       -> 0/0 (both markets' own regime gates
                                       already keep them out of positions)

T6 sleeve extension (2026-08-21): when BOTH markets are weak, the idle pool
is offered to the third-asset sleeve — 100% Nasdaq ETF while it trades above
its 200-day MA, else stay in cash/repo. The three-window validation lives in
scripts/sleeve_nav_sim.py (OPT-119).
"""

from __future__ import annotations

import datetime
from typing import Any

TRADABLE_REGIMES = ("Strong", "Diverging")


def weights_from_regimes(cn_regime: str | None, hk_regime: str | None) -> tuple[float, float]:
    """(w_cn, w_hk) from the two traffic-light regimes. Pure R5c, no lookahead."""
    cn_ok = (cn_regime or "") in TRADABLE_REGIMES
    hk_ok = (hk_regime or "") in TRADABLE_REGIMES
    if cn_ok:
        return (1.0, 0.0)
    if hk_ok:
        return (0.0, 1.0)
    return (0.0, 0.0)


def weights_with_sleeve_from_regimes(
    cn_regime: str | None,
    hk_regime: str | None,
    etf_above_ma200: bool | None = None,
) -> tuple[float, float, float]:
    """R5c + multi-asset sleeve (2026-08-24 固化): (w_cn, w_hk, w_etf).

    Both markets weak -> idle pool goes to multi-asset sleeve (GOLD/OIL/NASDAQ/BOND
    4选最强，60天动量>MA200，最少持有5天，已在 multi_asset_sleeve 验证三窗全过)。
    当 4资产中最强仍站上MA200则 w_etf=1（含纳指强时自然吃纳指），否则 0 回GC001。
    ``etf_above_ma200`` 仍兼容旧单纳指注入，None时查询多资产 _pick()。
    """
    w_cn, w_hk = weights_from_regimes(cn_regime, hk_regime)
    if w_cn > 0 or w_hk > 0:
        return (w_cn, w_hk, 0.0)
    if etf_above_ma200 is not None:
        return (0.0, 0.0, 1.0 if etf_above_ma200 else 0.0)
    # multi-asset 4选1：任一站上即视为袖子可投
    try:
        from data_sync_service.service.multi_asset_sleeve import _pick

        pick = _pick()
        etf_above = pick is not None and bool(pick.get("above_ma200"))
        return (0.0, 0.0, 1.0 if etf_above else 0.0)
    except Exception:
        # fallback to single NASDAQ
        from data_sync_service.service.third_asset_sleeve import (
            THIRD_ASSET_TS,
            _etf_market_data,
        )

        md = _etf_market_data(THIRD_ASSET_TS)
        etf_above_ma200 = bool(md.get("ok") and md.get("above_ma200"))
        return (0.0, 0.0, 1.0 if etf_above_ma200 else 0.0)


def live_regimes(*, as_of_date: str | None = None) -> dict[str, str | None]:
    """Current traffic lights for both markets (live signal path)."""
    from data_sync_service.service.execution_gate import classify_market_regime
    from data_sync_service.service.market_regime import get_hk_regime, get_index_signals

    signals = get_index_signals(as_of_date=as_of_date, include_breadth=False)
    cn = classify_market_regime(signals)
    hk = str(get_hk_regime(as_of_date=as_of_date).get("regime") or "")
    return {"CN": cn, "HK": hk}


def resolve_weights(*, as_of_date: str | None = None) -> dict[str, Any]:
    """One-call R5c allocation for the live path.

    Returns {"weights": {"CN": w, "HK": w}, "regimes": {"CN": ..., "HK": ...}}.
    """
    regimes = live_regimes(as_of_date=as_of_date)
    w_cn, w_hk = weights_from_regimes(regimes["CN"], regimes["HK"])
    return {"weights": {"CN": w_cn, "HK": w_hk}, "regimes": regimes}


def weights_with_sleeve(
    *, as_of_date: str | None = None, etf_above_ma200: bool | None = None
) -> tuple[float, float, float]:
    """R5c + T6 sleeve: (w_cn, w_hk, w_etf).

    Both markets weak -> the idle pool goes to the Nasdaq sleeve while it
    trades above its 200-day MA, else stays in cash/repo (w_etf = 0).
    ``etf_above_ma200`` is injectable for tests; when omitted it is read
    live from the sleeve state machine.
    """
    base = resolve_weights(as_of_date=as_of_date)
    return weights_with_sleeve_from_regimes(
        base["regimes"]["CN"], base["regimes"]["HK"], etf_above_ma200=etf_above_ma200
    )


def resolve_weights_with_sleeve(*, as_of_date: str | None = None) -> dict[str, Any]:
    """One-call R5c + sleeve allocation.

    Returns {"weights": {"CN", "HK", "ETF"}, "regimes": {...}}.
    """
    w_cn, w_hk, w_etf = weights_with_sleeve(as_of_date=as_of_date)
    regimes = live_regimes(as_of_date=as_of_date)
    return {
        "weights": {"CN": w_cn, "HK": w_hk, "ETF": w_etf},
        "regimes": regimes,
    }


# ---------------------------------------------------------------------------
# T4: weekly persistence — Monday decision recorded, intake reads the week
# ---------------------------------------------------------------------------


def week_start_for(day: str) -> str:
    """ISO Monday of the week containing `day`."""
    d = datetime.date.fromisoformat(day)
    return (d.fromordinal(d.toordinal() - d.weekday())).isoformat()


def decide_week(*, week_start: str, as_of_date: str | None = None) -> dict[str, Any]:
    """Resolve R5c weights for the week (as-of) and persist (first wins)."""
    from data_sync_service.db.allocation import insert_week_decision

    regimes = live_regimes(as_of_date=as_of_date)
    w_cn, w_hk = weights_from_regimes(regimes["CN"], regimes["HK"])
    row = insert_week_decision(
        week_start=week_start,
        cn_regime=str(regimes["CN"] or ""),
        hk_regime=str(regimes["HK"] or ""),
        w_cn=w_cn,
        w_hk=w_hk,
    )
    return {"weekStart": week_start, "decision": row}


def week_weights(day: str) -> dict[str, Any]:
    """The week's persisted weights for `day`; falls back to a same-day
    decision recorded on the spot (so intake never blocks on the Monday job).
    """
    from data_sync_service.db.allocation import get_week_decision

    wk = week_start_for(day)
    row = get_week_decision(wk)
    if row is None:
        return decide_week(week_start=wk, as_of_date=day)
    return {"weekStart": wk, "decision": row}


# ---------------------------------------------------------------------------
# R5CS: selected market internal idle eats sleeve (B-S1, 2026-08-28)
# When CN or HK is tradable, the idle inside the selected market (1 - holdings/10)
# is offered to the multi-asset sleeve if it is above MA200, else GC001.
# Validated in run_walk_forward_dual R5CS: +10.8/+17.0/+30.9pt vs R5C.
# ---------------------------------------------------------------------------


def weights_r5cs(
    cn_regime: str | None,
    hk_regime: str | None,
    cn_holdings: int = 0,
    hk_holdings: int = 0,
    max_positions: int = 10,
    etf_above_ma200: bool | None = None,
) -> tuple[float, float, float]:
    """R5CS: (w_cn, w_hk, w_etf) with internal idle to sleeve."""
    w_cn, w_hk = weights_from_regimes(cn_regime, hk_regime)
    # both weak -> same as before: all to sleeve
    if w_cn == 0 and w_hk == 0:
        return weights_with_sleeve_from_regimes(cn_regime, hk_regime, etf_above_ma200=etf_above_ma200)
    # sleeve availability
    if etf_above_ma200 is None:
        try:
            from data_sync_service.service.multi_asset_sleeve import _pick

            pick = _pick()
            etf_above = pick is not None and bool(pick.get("above_ma200"))
        except Exception:
            etf_above = False
    else:
        etf_above = bool(etf_above_ma200)
    if not etf_above:
        return (w_cn, w_hk, 0.0)
    # internal idle
    if w_cn > 0:
        used = min(1.0, cn_holdings / max_positions) if max_positions else 1.0
        idle = max(0.0, 1.0 - used)
        return (used, 0.0, idle)
    if w_hk > 0:
        used = min(1.0, hk_holdings / max_positions) if max_positions else 1.0
        idle = max(0.0, 1.0 - used)
        return (0.0, used, idle)
    return (w_cn, w_hk, 0.0)
