"""Sector Rotation Index (SRV): 3-day Top5 industry fund-flow overlap."""

from __future__ import annotations

from typing import Any

SRV_LEVEL_STABLE = "Stable"
SRV_LEVEL_ELEVATED = "Elevated"
SRV_LEVEL_EXTREME_HIGH = "Extreme_High"

_LABEL_ZH: dict[str, str] = {
    SRV_LEVEL_STABLE: "主线共识极强",
    SRV_LEVEL_ELEVATED: "高热震荡分歧",
    SRV_LEVEL_EXTREME_HIGH: "恶性电风扇绞肉机",
}


def _top_set_for_date(top_by_date: list[dict[str, Any]], date_str: str) -> set[str] | None:
    for item in top_by_date:
        if str(item.get("date") or "") != date_str:
            continue
        top = item.get("top")
        if not isinstance(top, list) or not top:
            return None
        names = {str(x).strip() for x in top if str(x).strip()}
        return names if names else None
    return None


def classify_srv_level(overlap_count: int) -> str:
    if overlap_count >= 3:
        return SRV_LEVEL_STABLE
    if overlap_count == 2:
        return SRV_LEVEL_ELEVATED
    return SRV_LEVEL_EXTREME_HIGH


def compute_srv_index(*, top_by_date: list[dict[str, Any]], as_of_date: str) -> dict[str, Any]:
    """
    Compute SRV from Top5 industry inflow sets on T, T-1, T-2 (last 3 dates <= as_of_date).
    """
    as_of = str(as_of_date or "").strip()
    if not as_of:
        return _empty_srv_index("")

    dated_entries: list[tuple[str, dict[str, Any]]] = []
    for item in top_by_date:
        d = str(item.get("date") or "").strip()
        if d and d <= as_of:
            dated_entries.append((d, item))
    dated_entries.sort(key=lambda x: x[0])

    unique_dates = []
    seen: set[str] = set()
    for d, _ in dated_entries:
        if d not in seen:
            seen.add(d)
            unique_dates.append(d)

    if len(unique_dates) < 3:
        return _empty_srv_index(as_of)

    last3 = unique_dates[-3:]
    sets: list[set[str]] = []
    for d in last3:
        s = _top_set_for_date(top_by_date, d)
        if s is None:
            return _empty_srv_index(as_of)
        sets.append(s)

    overlap = sets[0] & sets[1] & sets[2]
    overlap_count = len(overlap)
    level = classify_srv_level(overlap_count)

    return {
        "asOfDate": as_of,
        "dates": last3,
        "overlapCount": overlap_count,
        "overlapSectors": sorted(overlap),
        "level": level,
        "labelZh": _LABEL_ZH.get(level, ""),
    }


def _empty_srv_index(as_of_date: str) -> dict[str, Any]:
    return {
        "asOfDate": as_of_date,
        "dates": [],
        "overlapCount": None,
        "overlapSectors": [],
        "level": None,
        "labelZh": None,
    }
