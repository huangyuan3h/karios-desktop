"""PS-G50-X blend: executable S-gap sleeve, idle slots follow pick-strong core.

Historical PS-G50 is a static 50/50 of core vs satellite daily returns. After
skip_t1_limit the satellite book is often sparsely filled, so 50% of the
portfolio earns ~0 instead of working. Rank-fallback (next S-gap name) made
quality worse.

Fix: keep skip_t1_limit (honest fills) and route unfilled satellite notional
to the core. Slot occupancy includes the body-exit day (same as satActive).

    invested = min(1, satSlots * POSITION_PCT)
    idle     = max(0, 1 - satSlots * POSITION_PCT)
    sleeve   = sat_ret + idle * core_ret   # replace cash-0 with core
    port     = core_w * core_ret + sat_w * sleeve

Empty book (idle=1, sat_ret=0) → 100% core.
Full book (idle=0, ≥4 slots at 25%) → classic PS-G50 50/50.
"""
from __future__ import annotations

from data_sync_service.service.state_bucket_track import POSITION_PCT


def sat_idle_frac(sat_slots: int, position_pct: float = POSITION_PCT) -> float:
    """Fraction of the satellite sleeve that is unfilled cash (0 if levered)."""
    return max(0.0, 1.0 - max(0, int(sat_slots)) * float(position_pct))


def daily_ret_idle_to_core(
    core_ret: float,
    sat_ret: float,
    idle_frac: float,
    *,
    core_weight: float = 0.5,
) -> float:
    sat_weight = 1.0 - core_weight
    idle = min(1.0, max(0.0, float(idle_frac)))
    return core_weight * core_ret + sat_weight * (sat_ret + idle * core_ret)


def blend_nav_idle_to_core(
    core_nav: list[float],
    sat_nav: list[float],
    sat_slots: list[int],
    *,
    core_weight: float = 0.5,
    position_pct: float = POSITION_PCT,
) -> list[float]:
    n = min(len(core_nav), len(sat_nav), len(sat_slots))
    if n == 0:
        return [1.0]
    out = [1.0]
    for i in range(1, n):
        c0, s0 = core_nav[i - 1], sat_nav[i - 1]
        core_ret = core_nav[i] / c0 - 1.0 if c0 > 0 else 0.0
        sat_ret = sat_nav[i] / s0 - 1.0 if s0 > 0 else 0.0
        idle = sat_idle_frac(sat_slots[i], position_pct)
        out.append(out[-1] * (1.0 + daily_ret_idle_to_core(core_ret, sat_ret, idle, core_weight=core_weight)))
    return out


def blend_nav_static(
    core_nav: list[float],
    sat_nav: list[float],
    *,
    core_weight: float = 0.5,
) -> list[float]:
    n = min(len(core_nav), len(sat_nav))
    if n == 0:
        return [1.0]
    sat_weight = 1.0 - core_weight
    out = [1.0]
    for i in range(1, n):
        c0, s0 = core_nav[i - 1], sat_nav[i - 1]
        core_ret = core_nav[i] / c0 - 1.0 if c0 > 0 else 0.0
        sat_ret = sat_nav[i] / s0 - 1.0 if s0 > 0 else 0.0
        out.append(out[-1] * (1.0 + core_weight * core_ret + sat_weight * sat_ret))
    return out


def blend_nav_opportunity(
    core_nav: list[float],
    sat_nav: list[float],
    sat_active: list[bool],
    *,
    sat_weight: float = 0.5,
) -> list[float]:
    """Binary twin-star: 100% core when idle, else core + sat_weight*(sat-core)."""
    n = min(len(core_nav), len(sat_nav), len(sat_active))
    if n == 0:
        return [1.0]
    out = [1.0]
    for i in range(1, n):
        c0, s0 = core_nav[i - 1], sat_nav[i - 1]
        core_ret = core_nav[i] / c0 - 1.0 if c0 > 0 else 0.0
        sat_ret = sat_nav[i] / s0 - 1.0 if s0 > 0 else 0.0
        if sat_active[i]:
            ret = core_ret + sat_weight * (sat_ret - core_ret)
        else:
            ret = core_ret
        out.append(out[-1] * (1.0 + ret))
    return out
