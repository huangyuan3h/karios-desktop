"""「星港」(Starport) = Homeport x habit-satellite overlay — Timeline research option.

Validated in H-B3-SAT (docs/backtests/stable/harbor-b3-sat-2026-09-14.md):
on satellite-active days the portfolio return is ``homeport + w * (satellite -
homeport)``; idle days stay 100% Homeport. Frozen w = 1/3 (chosen by the
pre-registered K1-K3 rule; robust band 0.15-0.25).

Display only: ``GET /api/backtest/timeline?strategy=starport`` derives the rows
from the cached Homeport timeline + the standalone state-bucket satellite
timeline (no engine re-run, no Live wiring; Live stays 港湾).
"""

from __future__ import annotations

from typing import Any

MODE = "starport"
STRATEGY_LABEL = "星港"
SAT_WEIGHT = 1.0 / 3.0


def blend_starport_timeline(
    homeport_result: dict[str, Any],
    satellite_result: dict[str, Any],
    *,
    sat_weight: float = SAT_WEIGHT,
) -> dict[str, Any]:
    """Derive the Starport timeline from Homeport + satellite timeline rows.

    Satellite NAV is aligned to the Homeport date axis (forward-filled) and
    normalized to its first available value; missing satellite days count as
    inactive. Pure function — synthetic rows are enough to unit-test it.
    """
    out = dict(homeport_result)
    rows = [dict(r) for r in (homeport_result.get("rows") or [])]
    src_summary = dict(homeport_result.get("summary") or {})
    out["mode"] = MODE
    out["strategy"] = STRATEGY_LABEL
    out["rows"] = rows
    if not rows:
        out["summary"] = {**src_summary}
        return out

    sat_rows = satellite_result.get("rows") or []
    sat_by_day = {str(r.get("date")): r for r in sat_rows if r.get("date")}
    sat_days = sorted(sat_by_day)

    aligned: list[float | None] = []
    si, cur = 0, None
    for r in rows:
        d = str(r.get("date"))
        while si < len(sat_days) and sat_days[si] <= d:
            cur = float(sat_by_day[sat_days[si]].get("satNav") or 1.0)
            si += 1
        aligned.append(cur)
    base = next((v for v in aligned if v), None)
    sat_nav = [(v / base if (v and base) else 1.0) for v in aligned]

    hp = [float(r.get("navSingle") or 1.0) for r in rows]
    navs = [1.0]
    active_count = 0
    for i in range(1, len(rows)):
        r_hp = hp[i] / hp[i - 1] - 1.0 if hp[i - 1] else 0.0
        sat_row = sat_by_day.get(str(rows[i].get("date"))) or {}
        active = bool(sat_row.get("satActive"))
        if active:
            active_count += 1
            r_sat = sat_nav[i] / sat_nav[i - 1] - 1.0 if sat_nav[i - 1] else 0.0
            ret = r_hp + sat_weight * (r_sat - r_hp)
        else:
            ret = r_hp
        navs.append(navs[-1] * (1.0 + ret))

    peak, mdd = navs[0], 0.0
    for i, r in enumerate(rows):
        v = navs[i]
        peak = max(peak, v)
        mdd = min(mdd, v / peak - 1.0)
        r["navSingle"] = round(v, 6)
        r["navMulti"] = round(v, 6)
        r["navSingleReturnPct"] = round((v - 1.0) * 100, 2)
        r["navMultiReturnPct"] = round((v - 1.0) * 100, 2)
        sat_row = sat_by_day.get(str(r.get("date"))) or {}
        r["satNav"] = round(sat_nav[i], 6)
        r["satActive"] = bool(sat_row.get("satActive"))
    out["summary"] = {
        **src_summary,
        "fusedPct": round((navs[-1] - 1.0) * 100, 2),
        "homeportPct": round(float(src_summary.get("fusedPct") or 0.0), 2),
        "satPct": round(float((satellite_result.get("summary") or {}).get("satPct") or 0.0), 2),
        "maxDdFusedPct": round(mdd * 100, 1),
        "activeDays": active_count,
    }
    return out
