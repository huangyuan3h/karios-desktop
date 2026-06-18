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


def _rows_for_date(rows: list[dict[str, Any]], as_of: str) -> list[dict[str, Any]]:
    return [r for r in rows if str(r.get("date") or "") == as_of]


def build_trendok_flow_context_from_rows(
    *,
    flow_date: str,
    dates_5: list[str],
    rows: list[dict[str, Any]],
) -> dict[str, Any]:
    """Build TrendOK industry-flow context from a batch of industry fund-flow rows."""
    dates_2 = dates_5[-2:] if dates_5 else []
    today = dates_2[-1] if dates_2 else flow_date
    yesterday = dates_2[-2] if len(dates_2) >= 2 else None

    rows_today = _rows_for_date(rows, today)
    rows_yesterday = _rows_for_date(rows, yesterday) if yesterday else []

    top_today = sorted(rows_today, key=lambda x: float(x.get("net_inflow") or 0.0), reverse=True)
    top_today_5 = [str(x.get("industry_name") or "") for x in top_today[:5] if x.get("industry_name")]
    top_today_3 = top_today_5[:3]

    top_yesterday = sorted(rows_yesterday, key=lambda x: float(x.get("net_inflow") or 0.0), reverse=True)
    top_yesterday_3 = [str(x.get("industry_name") or "") for x in top_yesterday[:3] if x.get("industry_name")]

    net_today = {str(x.get("industry_name") or ""): float(x.get("net_inflow") or 0.0) for x in rows_today}
    net_yesterday = {
        str(x.get("industry_name") or ""): float(x.get("net_inflow") or 0.0) for x in rows_yesterday
    }

    sums_5d_dict = sum_by_industry_from_rows(rows, dates_5)
    sums_5d_sorted = sorted(sums_5d_dict.items(), key=lambda x: (-x[1], x[0]))
    top_5d_3 = [name for name, _ in sums_5d_sorted[:3]]
    bottom_5d_5 = [name for name, _ in reversed(sums_5d_sorted[-5:])]

    return {
        "ok": True,
        "asOfDate": flow_date,
        "today": today,
        "yesterday": yesterday,
        "top_today_3": set(top_today_3),
        "top_today_5": set(top_today_5),
        "top_yesterday_3": set(top_yesterday_3),
        "net_today": net_today,
        "net_yesterday": net_yesterday,
        "top_5d_3": set(top_5d_3),
        "bottom_5d_5": set(bottom_5d_5),
    }
