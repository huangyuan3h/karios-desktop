from __future__ import annotations

from typing import Any


def series_map_from_rows(
    rows: list[dict[str, Any]],
    dates: list[str],
) -> dict[str, list[dict[str, Any]]]:
    """Group rows into per-industry series ordered by date ASC (dates filter only)."""
    if not dates:
        return {}
    allowed = set(dates)
    by_name: dict[str, dict[str, float]] = {}
    for row in rows:
        d = str(row.get("date") or "")
        if d not in allowed:
            continue
        name = str(row.get("industry_name") or "")
        if not name:
            continue
        by_name.setdefault(name, {})[d] = float(row.get("net_inflow") or 0.0)
    out: dict[str, list[dict[str, Any]]] = {}
    for name, per_date in by_name.items():
        out[name] = [{"date": d, "net_inflow": per_date[d]} for d in dates if d in per_date]
    return out


def sum_by_industry_from_rows(
    rows: list[dict[str, Any]],
    dates_subset: list[str],
) -> dict[str, float]:
    """Per-industry sum of net_inflow over dates_subset."""
    if not dates_subset:
        return {}
    allowed = set(dates_subset)
    sums: dict[str, float] = {}
    for row in rows:
        d = str(row.get("date") or "")
        if d not in allowed:
            continue
        name = str(row.get("industry_name") or "")
        if not name:
            continue
        sums[name] = sums.get(name, 0.0) + float(row.get("net_inflow") or 0.0)
    return sums


def positive_days_from_rows(
    rows: list[dict[str, Any]],
    dates_subset: list[str],
) -> dict[str, int]:
    """Count days with net_inflow > 0 per industry within dates_subset."""
    if not dates_subset:
        return {}
    allowed = set(dates_subset)
    pos: dict[str, int] = {}
    for row in rows:
        d = str(row.get("date") or "")
        if d not in allowed:
            continue
        name = str(row.get("industry_name") or "")
        if not name:
            continue
        if float(row.get("net_inflow") or 0.0) > 0:
            pos[name] = pos.get(name, 0) + 1
    return pos
