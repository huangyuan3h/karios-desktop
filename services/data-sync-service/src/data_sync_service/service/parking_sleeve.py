"""H2 hysteresis parking entry point (thin wrapper, research compatibility).

The ONE state machine is ``harbor.parking_replay(hyst_band=...)``; since
H-H2-UNIFY (2026-09-18) H2 (band 2pt) is the product rule for Live / paper /
Timeline / watchlist / recon, so this module no longer owns any logic. It is
kept so research scripts and the frozen H2 experiments keep importing
``hysteresis_parking_replay`` without touching the unified core.
"""

from __future__ import annotations

from typing import Any

from data_sync_service.service.harbor import HYST_BAND, TRAIL_PCT, parking_replay


def hysteresis_parking_replay(
    etf_close: dict[str, dict[str, float]],
    calendar: list[str],
    *,
    band: float = HYST_BAND,
    trail_pct: float = TRAIL_PCT,
) -> list[dict[str, Any]]:
    """H2 hysteresis parking — thin wrapper over ``harbor.parking_replay``.

    Since H-H2-UNIFY (2026-09-18) the state machine lives in ONE place
    (``harbor.parking_replay`` with ``hyst_band``); this keeps the historical
    ``parking_sleeve`` entry point (and its ``hyst_blocked`` records) working
    for the research lines that still call it.
    """
    return parking_replay(etf_close, calendar, hyst_band=band, trail_pct=trail_pct)
