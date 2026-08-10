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
"""

from __future__ import annotations

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
