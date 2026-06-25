from __future__ import annotations

from typing import Any

from data_sync_service.service.industry_taxonomy import row_is_sw_l1


def filter_sw_l1_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [row for row in rows if isinstance(row, dict) and row_is_sw_l1(row)]


def _dedupe_rows_by_date_name(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    best: dict[tuple[str, str], dict[str, Any]] = {}
    for row in filter_sw_l1_rows(rows):
        d = str(row.get("date") or "")
        name = str(row.get("industry_name") or "").strip()
        if not d or not name:
            continue
        key = (d, name)
        try:
            v = float(row.get("net_inflow") or 0.0)
        except Exception:
            v = 0.0
        prev = best.get(key)
        if prev is None or v > float(prev.get("net_inflow") or 0.0):
            best[key] = row
    return list(best.values())


def series_map_from_rows(
    rows: list[dict[str, Any]],
    dates: list[str],
) -> dict[str, list[dict[str, Any]]]:
    """Group rows into per-industry series ordered by date ASC (dates filter only)."""
    if not dates:
        return {}
    rows = _dedupe_rows_by_date_name(rows)
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
    rows = _dedupe_rows_by_date_name(rows)
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
    rows = _dedupe_rows_by_date_name(rows)
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
    rows = _dedupe_rows_by_date_name(rows)
    dates_2 = dates_5[-2:] if dates_5 else []
    today = dates_2[-1] if dates_2 else flow_date
    yesterday = dates_2[-2] if len(dates_2) >= 2 else None

    rows_today = _rows_for_date(rows, today)
    rows_yesterday = _rows_for_date(rows, yesterday) if yesterday else []

    top_today = sorted(rows_today, key=lambda x: float(x.get("net_inflow") or 0.0), reverse=True)
    top_today_5 = [str(x.get("industry_name") or "") for x in top_today[:5] if x.get("industry_name")]
    top_today_3 = top_today_5[:3]

    # Calculate top 3 outflow industries today (lowest net_inflow = largest outflow)
    outflow_today = sorted(rows_today, key=lambda x: float(x.get("net_inflow") or 0.0))
    outflow_today_3 = [str(x.get("industry_name") or "") for x in outflow_today[:3] if x.get("industry_name")]

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
        "outflow_today_3": set(outflow_today_3),
        "top_yesterday_3": set(top_yesterday_3),
        "net_today": net_today,
        "net_yesterday": net_yesterday,
        "top_5d_3": set(top_5d_3),
        "bottom_5d_5": set(bottom_5d_5),
    }


def flow_items_from_rows(
    rows: list[dict[str, Any]],
    dates_sorted: list[str],
) -> list[dict[str, Any]]:
    """Build dashboard 5D flow items (per industry series + sum5d) from batch rows."""
    if not dates_sorted:
        return []
    rows = _dedupe_rows_by_date_name(rows)
    last_date = dates_sorted[-1]
    by_name: dict[str, dict[str, Any]] = {}
    allowed = set(dates_sorted)
    for row in rows:
        d = str(row.get("date") or "")
        if d not in allowed:
            continue
        code = str(row.get("industry_code") or "")
        name = str(row.get("industry_name") or "").strip()
        if not name:
            continue
        try:
            v = float(row.get("net_inflow") or 0.0)
        except Exception:
            v = 0.0
        rec = by_name.setdefault(name, {"industryCode": code, "industryName": name, "perDate": {}})
        if code and not rec.get("industryCode"):
            rec["industryCode"] = code
        rec["perDate"][d] = v

    items: list[dict[str, Any]] = []
    for rec in by_name.values():
        per: dict[str, float] = rec.get("perDate") or {}
        series = [{"date": d, "netInflow": float(per.get(d, 0.0) or 0.0)} for d in dates_sorted]
        sum5d = sum(float(p.get("netInflow") or 0.0) for p in series)
        items.append(
            {
                "industryCode": rec["industryCode"],
                "industryName": str(rec.get("industryName") or ""),
                "sum5d": sum5d,
                "netInflow": float(per.get(last_date, 0.0) or 0.0),
                "series": series,
            }
        )
    return items


def daily_rankings_from_flow_items(
    items: list[dict[str, Any]],
    dates: list[str],
) -> list[dict[str, Any]]:
    """Full net-inflow rankings per date for hot-industry rank-delta logic."""
    out: list[dict[str, Any]] = []
    for d in dates:
        scored: list[dict[str, Any]] = []
        for it in items:
            name = str(it.get("industryName") or "").strip()
            if not name:
                continue
            series = it.get("series") if isinstance(it.get("series"), list) else []
            v = 0.0
            for p in series:
                if not isinstance(p, dict) or str(p.get("date") or "") != d:
                    continue
                try:
                    v = float(p.get("netInflow") or 0.0)
                except Exception:
                    v = 0.0
                break
            scored.append({"industryName": name, "value": v})
        scored.sort(key=lambda x: float(x.get("value") or 0.0), reverse=True)
        ranked = [
            {"industryName": x["industryName"], "value": x["value"], "rank": i + 1}
            for i, x in enumerate(scored)
        ]
        out.append({"date": d, "ranked": ranked})
    return out


def top_by_date_from_rows(
    rows: list[dict[str, Any]],
    dates_sorted: list[str],
    *,
    top_k: int = 5,
) -> list[dict[str, Any]]:
    """Top-K industry names per date by net_inflow DESC."""
    rows = _dedupe_rows_by_date_name(rows)
    topk2 = max(1, min(int(top_k), 20))
    by_date: dict[str, list[tuple[str, float]]] = {d: [] for d in dates_sorted}
    allowed = set(dates_sorted)
    for row in rows:
        d = str(row.get("date") or "")
        if d not in allowed:
            continue
        name = str(row.get("industry_name") or "").strip()
        if not name:
            continue
        try:
            v = float(row.get("net_inflow") or 0.0)
        except Exception:
            v = 0.0
        by_date.setdefault(d, []).append((name, v))
    out: list[dict[str, Any]] = []
    for d in dates_sorted:
        ranked = sorted(by_date.get(d, []), key=lambda x: (-x[1], x[0]))
        out.append({"date": d, "top": [name for name, _ in ranked[:topk2]]})
    return out


def build_dashboard_industry_bundle(
    *,
    as_of_date: str,
    dates: list[str],
    rows: list[dict[str, Any]],
    days: int = 5,
    top_k: int = 5,
) -> dict[str, Any]:
    """Build dashboard industryFundFlow block from a single batch read."""
    days2 = max(1, min(int(days), 30))
    topk2 = max(1, min(int(top_k), 20))
    dates_sorted = list(dates)
    top_by_date = top_by_date_from_rows(rows, dates_sorted, top_k=topk2)
    base = {
        "asOfDate": as_of_date,
        "days": days2,
        "topK": topk2,
        "dates": dates_sorted,
        "topByDate": top_by_date,
    }
    items = flow_items_from_rows(rows, dates_sorted)
    daily_rankings = daily_rankings_from_flow_items(items, dates_sorted) if dates_sorted else []
    if not dates_sorted:
        empty = {"asOfDate": as_of_date, "days": days2, "topN": 10, "dates": [], "top": []}
        return {**base, "dailyRankings": daily_rankings, "flow5d": empty, "flow5dOut": empty}
    top_in = sorted(items, key=lambda x: float(x.get("sum5d") or 0.0), reverse=True)[:10]
    top_out = sorted(items, key=lambda x: float(x.get("sum5d") or 0.0))[:10]
    flow5d = {"asOfDate": as_of_date, "days": days2, "topN": 10, "dates": dates_sorted, "top": top_in}
    flow5d_out = {
        "asOfDate": as_of_date,
        "days": days2,
        "topN": 10,
        "dates": dates_sorted,
        "top": top_out,
    }
    return {
        **base,
        "dailyRankings": daily_rankings,
        "flow5d": flow5d,
        "flow5dOut": flow5d_out,
    }
