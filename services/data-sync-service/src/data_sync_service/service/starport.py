"""「星港」(Starport) / 「双子星」(Twin Star) overlay blends — Timeline research options.

Both are the same opportunity blend on a different base leg:
- Starport (H-B3-SAT, 2026-09-14): base = Homeport (Harbor x B3 50/50), w = 1/3.
- Twin Star (kept as a parallel comparison entry, 2026-09-14 user decision):
  base = Harbor, w = 1/2.

Blend rule: on satellite-active days ``base + w * (satellite - base)``; idle days
stay 100% base. Display only — no Live wiring; the default data-collection view
is 星港, Live stays 港湾.
"""

from __future__ import annotations

from typing import Any

MODE = "starport"
STRATEGY_LABEL = "星港"
SAT_WEIGHT = 1.0 / 3.0

TWIN_STAR_MODE = "twin_star"
TWIN_STAR_LABEL = "双子星"
TWIN_STAR_WEIGHT = 0.5


def blend_overlay_timeline(
    base_result: dict[str, Any],
    satellite_result: dict[str, Any],
    *,
    sat_weight: float,
    mode: str,
    strategy_label: str,
    base_key: str,
) -> dict[str, Any]:
    """Derive an overlay timeline from base + satellite timeline rows.

    Satellite NAV is aligned to the base date axis (forward-filled) and
    normalized to its first available value; missing satellite days count as
    inactive. Pure function — synthetic rows are enough to unit-test it.
    """
    out = dict(base_result)
    rows = [dict(r) for r in (base_result.get("rows") or [])]
    src_summary = dict(base_result.get("summary") or {})
    out["mode"] = mode
    out["strategy"] = strategy_label
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
        for key in ("satPositions", "satSlots", "filledToday", "idleSlots", "gateOpen"):
            if key in sat_row:
                r[key] = sat_row[key]
    out["summary"] = {
        **src_summary,
        "fusedPct": round((navs[-1] - 1.0) * 100, 2),
        base_key: round(float(src_summary.get("fusedPct") or 0.0), 2),
        "satPct": round(float((satellite_result.get("summary") or {}).get("satPct") or 0.0), 2),
        "maxDdFusedPct": round(mdd * 100, 1),
        "activeDays": active_count,
    }
    return out


def blend_starport_timeline(
    homeport_result: dict[str, Any],
    satellite_result: dict[str, Any],
    *,
    sat_weight: float = SAT_WEIGHT,
) -> dict[str, Any]:
    """Starport = Homeport x satellite overlay (w=1/3, H-B3-SAT chosen)."""
    return blend_overlay_timeline(
        homeport_result,
        satellite_result,
        sat_weight=sat_weight,
        mode=MODE,
        strategy_label=STRATEGY_LABEL,
        base_key="homeportPct",
    )


def blend_twin_star_timeline(
    harbor_result: dict[str, Any],
    satellite_result: dict[str, Any],
    *,
    sat_weight: float = TWIN_STAR_WEIGHT,
) -> dict[str, Any]:
    """Twin Star = Harbor x satellite overlay (w=1/2, parallel comparison entry)."""
    return blend_overlay_timeline(
        harbor_result,
        satellite_result,
        sat_weight=sat_weight,
        mode=TWIN_STAR_MODE,
        strategy_label=TWIN_STAR_LABEL,
        base_key="harborPct",
    )
