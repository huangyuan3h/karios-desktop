from __future__ import annotations

import math
from datetime import UTC, date, datetime, timedelta
from typing import Any

from data_sync_service.db import get_connection
from data_sync_service.db.daily import ensure_table as ensure_daily
from data_sync_service.db.industry_fund_flow import (
    get_dates_upto as flow_dates_upto,
    get_latest_date as flow_latest_date,
    get_rows_by_date as flow_rows_by_date,
    get_series_for_industry,
    get_sum_by_industry_for_dates,
)
from data_sync_service.db.industry_mainline_metrics import (
    get_dates_upto as metrics_dates_upto,
    list_rows_by_date as metrics_rows_by_date,
    list_rows_for_dates as metrics_rows_for_dates,
    upsert_daily_rows as metrics_upsert_rows,
)
from data_sync_service.db.industry_mainline_scores import (
    list_rows_by_date as scores_rows_by_date,
    list_rows_for_dates as scores_rows_for_dates,
    upsert_daily_rows as scores_upsert_rows,
)
from data_sync_service.db.stock_basic import ensure_table as ensure_stock_basic
from data_sync_service.db.trade_calendar import get_open_dates, is_trading_day

SURGE_PCT = 5.0
LIMIT_UP_PCT = 9.8
FLOW_SCORE_MAX = 40.0
BREADTH_SCORE_MAX = 40.0
TREND_SCORE_MAX = 20.0
MAINLINE_THRESHOLD = 80.0
MAINLINE_STREAK_DAYS = 3


def _now_iso() -> str:
    return datetime.now(tz=UTC).isoformat()


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        f = float(v)
        return f if math.isfinite(f) else default
    except Exception:
        return default


def _limit_pct_for(ts_code: str, name: str | None) -> float:
    n = (name or "").upper()
    if "ST" in n:
        return 5.0
    t = (ts_code or "").upper()
    if t.endswith(".BJ"):
        return 30.0
    code = t.split(".", 1)[0]
    if code.startswith(("300", "301", "688")):
        return 20.0
    return 10.0


def _is_limit_up(
    *,
    ts_code: str,
    pre_close: float | None,
    close: float | None,
    pct_chg: float | None,
    name: str | None,
) -> bool:
    if pre_close is None or close is None:
        return False
    pre = _safe_float(pre_close, 0.0)
    c = _safe_float(close, 0.0)
    if not (pre > 0.0 and math.isfinite(pre) and math.isfinite(c)):
        return False
    limit_pct = _limit_pct_for(ts_code, name)
    limit_price = pre * (1.0 + limit_pct / 100.0)
    tol = max(0.01, abs(limit_price) * 0.0015)
    if abs(c - limit_price) <= tol:
        return True
    p = _safe_float(pct_chg, 0.0)
    return p >= (limit_pct - 0.2)


def _prev_open_date(exchange: str, d0: date) -> date | None:
    if is_trading_day(exchange, d0) is not None:
        xs = get_open_dates(exchange=exchange, start_date=d0 - timedelta(days=40), end_date=d0)
        xs2 = [x for x in xs if x < d0]
        if xs2:
            return xs2[-1]
    ensure_daily()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT MAX(trade_date) FROM daily WHERE trade_date < %s", (d0.isoformat(),))
            row = cur.fetchone()
    return row[0] if row and row[0] else None


def _trade_dates_upto(d0: str, days: int) -> list[str]:
    d = date.fromisoformat(d0)
    if is_trading_day("SSE", d) is not None:
        xs = get_open_dates(exchange="SSE", start_date=d - timedelta(days=120), end_date=d)
        if xs:
            xs2 = [x.isoformat() for x in xs][-max(1, min(int(days), 60)) :]
            return xs2
    return flow_dates_upto(d0, days)


def _fetch_daily_rows_by_dates(dates: list[str]) -> dict[str, list[dict[str, Any]]]:
    ensure_daily()
    ensure_stock_basic()
    if not dates:
        return {}
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT d.trade_date, d.ts_code, d.pre_close, d.close, d.pct_chg, b.name, b.industry
                FROM daily d
                LEFT JOIN stock_basic b ON b.ts_code = d.ts_code
                WHERE d.trade_date = ANY(%s)
                """,
                (dates,),
            )
            rows = cur.fetchall()
    out: dict[str, list[dict[str, Any]]] = {}
    for r in rows:
        d = r[0].strftime("%Y-%m-%d") if hasattr(r[0], "strftime") else str(r[0])
        out.setdefault(d, []).append(
            {
                "ts_code": str(r[1] or ""),
                "pre_close": r[2],
                "close": r[3],
                "pct_chg": r[4],
                "name": str(r[5]) if r[5] is not None else None,
                "industry": str(r[6]) if r[6] is not None else None,
            }
        )
    return out


def _compute_industry_metrics_for_date(d0: str) -> list[dict[str, Any]]:
    d = date.fromisoformat(d0)
    prev = _prev_open_date("SSE", d)
    dates = [d0]
    if prev:
        dates.append(prev.isoformat())
    rows_by_date = _fetch_daily_rows_by_dates(dates)
    rows_today = rows_by_date.get(d0, [])
    rows_prev = rows_by_date.get(prev.isoformat(), []) if prev else []
    limit_today: set[str] = set()
    limit_prev: set[str] = set()
    for row in rows_today:
        if _is_limit_up(
            ts_code=row["ts_code"],
            pre_close=row["pre_close"],
            close=row["close"],
            pct_chg=row["pct_chg"],
            name=row["name"],
        ):
            limit_today.add(row["ts_code"])
    for row in rows_prev:
        if _is_limit_up(
            ts_code=row["ts_code"],
            pre_close=row["pre_close"],
            close=row["close"],
            pct_chg=row["pct_chg"],
            name=row["name"],
        ):
            limit_prev.add(row["ts_code"])

    stats: dict[str, dict[str, Any]] = {}
    for row in rows_today:
        industry = row.get("industry") or ""
        if not industry:
            continue
        st = stats.setdefault(
            industry,
            {
                "total_count": 0,
                "limit_up_count": 0,
                "limit_up_2d_count": 0,
                "surge_count": 0,
                "close_sum": 0.0,
                "pct_sum": 0.0,
            },
        )
        st["total_count"] += 1
        close = _safe_float(row.get("close"), 0.0)
        pct = _safe_float(row.get("pct_chg"), 0.0)
        st["close_sum"] += close
        st["pct_sum"] += pct
        if row["ts_code"] in limit_today or pct >= LIMIT_UP_PCT:
            st["limit_up_count"] += 1
        if row["ts_code"] in limit_today and row["ts_code"] in limit_prev:
            st["limit_up_2d_count"] += 1
        if pct > SURGE_PCT:
            st["surge_count"] += 1

    out: list[dict[str, Any]] = []
    for industry, st in stats.items():
        total = int(st["total_count"])
        avg_close = (float(st["close_sum"]) / total) if total > 0 else 0.0
        avg_pct = (float(st["pct_sum"]) / total) if total > 0 else 0.0
        surge_ratio = (float(st["surge_count"]) / total) if total > 0 else 0.0
        out.append(
            {
                "date": d0,
                "industry_name": industry,
                "total_count": total,
                "limit_up_count": int(st["limit_up_count"]),
                "limit_up_2d_count": int(st["limit_up_2d_count"]),
                "surge_count": int(st["surge_count"]),
                "surge_ratio": float(surge_ratio),
                "avg_close": float(avg_close),
                "avg_pct": float(avg_pct),
                "updated_at": _now_iso(),
                "raw": {"prevDate": prev.isoformat() if prev else None},
            }
        )
    return out


def ensure_metrics_for_dates(dates: list[str]) -> dict[str, Any]:
    ensured = 0
    for d in dates:
        existing = metrics_rows_by_date(d)
        if existing:
            continue
        rows = _compute_industry_metrics_for_date(d)
        if rows:
            metrics_upsert_rows(rows)
            ensured += 1
    return {"ensured": ensured}


def _rank_map(values: dict[str, float], *, desc: bool = True) -> dict[str, int]:
    items = sorted(values.items(), key=lambda x: x[1], reverse=desc)
    out: dict[str, int] = {}
    for idx, (k, _v) in enumerate(items):
        out[k] = idx + 1
    return out


def _flow_context(as_of_date: str) -> dict[str, Any]:
    dates_20 = flow_dates_upto(as_of_date, 20)
    dates_10 = flow_dates_upto(as_of_date, 10)
    dates_5 = flow_dates_upto(as_of_date, 5)
    sums_20 = {r["industry_name"]: float(r["sum_inflow"]) for r in get_sum_by_industry_for_dates(dates_20)}
    sums_5 = {r["industry_name"]: float(r["sum_inflow"]) for r in get_sum_by_industry_for_dates(dates_5)}
    rank_20 = _rank_map(sums_20, desc=True)
    rank_5 = _rank_map(sums_5, desc=True)

    pos_days: dict[str, int] = {}
    for d in dates_10:
        for row in flow_rows_by_date(d):
            name = str(row.get("industry_name") or "")
            if not name:
                continue
            if float(row.get("net_inflow") or 0.0) > 0:
                pos_days[name] = pos_days.get(name, 0) + 1

    return {
        "dates_20": dates_20,
        "dates_10": dates_10,
        "dates_5": dates_5,
        "sum20": sums_20,
        "sum5": sums_5,
        "rank20": rank_20,
        "rank5": rank_5,
        "pos10": pos_days,
    }


def _breadth_context(as_of_date: str) -> dict[str, Any]:
    rows = metrics_rows_by_date(as_of_date)
    limit_counts = {r["industry_name"]: int(r["limit_up_count"]) for r in rows}
    limit_rank = _rank_map({k: float(v) for k, v in limit_counts.items()}, desc=True)
    return {
        "rows": {r["industry_name"]: r for r in rows},
        "limit_rank": limit_rank,
    }


def _trend_context(as_of_date: str) -> dict[str, Any]:
    dates_21 = _trade_dates_upto(as_of_date, 21)
    rows = metrics_rows_for_dates(dates_21)
    by_industry: dict[str, list[tuple[str, float]]] = {}
    market_close: dict[str, tuple[float, int]] = {}
    for r in rows:
        d = r["date"]
        industry = r["industry_name"]
        avg_close = float(r["avg_close"])
        by_industry.setdefault(industry, []).append((d, avg_close))
        total_count = int(r["total_count"] or 0)
        sum_close, cnt = market_close.get(d, (0.0, 0))
        market_close[d] = (sum_close + avg_close * total_count, cnt + total_count)

    market_avg_close = {
        d: (sum_close / cnt if cnt > 0 else 0.0) for d, (sum_close, cnt) in market_close.items()
    }
    return {"dates": dates_21, "series": by_industry, "market_avg_close": market_avg_close}


def _score_flow(industry: str, ctx: dict[str, Any]) -> tuple[float, dict[str, Any]]:
    rank20 = int(ctx["rank20"].get(industry, 10_000))
    rank5 = int(ctx["rank5"].get(industry, 10_000))
    pos10 = int(ctx["pos10"].get(industry, 0))
    mid_ok = rank20 <= 5
    short_ok = rank5 <= 3
    cons_ok = pos10 >= 6
    score = (15.0 if mid_ok else 0.0) + (15.0 if short_ok else 0.0) + (10.0 if cons_ok else 0.0)
    return (
        score,
        {
            "sum20d": float(ctx["sum20"].get(industry, 0.0)),
            "sum5d": float(ctx["sum5"].get(industry, 0.0)),
            "rank20d": rank20,
            "rank5d": rank5,
            "positiveDays10d": pos10,
            "midAccumulation": mid_ok,
            "shortIntensity": short_ok,
            "consistency": cons_ok,
        },
    )


def _score_breadth(industry: str, ctx: dict[str, Any]) -> tuple[float, dict[str, Any]]:
    row = ctx["rows"].get(industry) or {}
    limit_up = int(row.get("limit_up_count") or 0)
    limit_rank = int(ctx["limit_rank"].get(industry, 10_000))
    limit_ok = limit_up >= 3 and limit_rank <= 3
    dragon = int(row.get("limit_up_2d_count") or 0)
    dragon_ok = dragon >= 1
    surge_ratio = float(row.get("surge_ratio") or 0.0)
    surge_ok = surge_ratio >= 0.05
    score = (15.0 if limit_ok else 0.0) + (15.0 if dragon_ok else 0.0) + (10.0 if surge_ok else 0.0)
    return (
        score,
        {
            "limitUpCount": limit_up,
            "limitUpRank": limit_rank,
            "limitUpQualified": limit_ok,
            "dragonCount": dragon,
            "dragonQualified": dragon_ok,
            "surgeRatio": surge_ratio,
            "surgeQualified": surge_ok,
        },
    )


def _score_trend(industry: str, ctx: dict[str, Any]) -> tuple[float, dict[str, Any]]:
    dates = ctx["dates"]
    series = sorted(ctx["series"].get(industry, []), key=lambda x: x[0])
    if len(dates) < 21 or len(series) < 21:
        return 0.0, {
            "indexAboveMa20": False,
            "ma20Up": False,
            "rps": 0.0,
            "rpsQualified": False,
        }

    series_map = {d: v for d, v in series}
    closes = [series_map.get(d, 0.0) for d in dates]
    if len(closes) < 21:
        return 0.0, {
            "indexAboveMa20": False,
            "ma20Up": False,
            "rps": 0.0,
            "rpsQualified": False,
        }
    ma20 = sum(closes[-20:]) / 20.0
    ma20_prev = sum(closes[-21:-1]) / 20.0
    close_today = closes[-1]
    index_ok = close_today > ma20 and ma20 > ma20_prev

    market_series = ctx["market_avg_close"]
    market_closes = [market_series.get(d, 0.0) for d in dates]
    def _ret5(xs: list[float]) -> float:
        if len(xs) < 6 or xs[-6] <= 0:
            return 0.0
        return xs[-1] / xs[-6] - 1.0

    rps = _ret5(closes) - _ret5(market_closes)
    rps_ok = rps > 0.02
    score = (10.0 if index_ok else 0.0) + (10.0 if rps_ok else 0.0)
    return (
        score,
        {
            "indexAboveMa20": index_ok,
            "ma20Up": ma20 > ma20_prev,
            "rps": rps,
            "rpsQualified": rps_ok,
        },
    )


def compute_scores_for_date(as_of_date: str) -> list[dict[str, Any]]:
    flow_ctx = _flow_context(as_of_date)
    breadth_ctx = _breadth_context(as_of_date)
    trend_ctx = _trend_context(as_of_date)

    industries = set(flow_ctx["sum20"].keys()) | set(breadth_ctx["rows"].keys())
    out: list[dict[str, Any]] = []
    for industry in industries:
        flow_score, flow_flags = _score_flow(industry, flow_ctx)
        breadth_score, breadth_flags = _score_breadth(industry, breadth_ctx)
        trend_score, trend_flags = _score_trend(industry, trend_ctx)
        total = flow_score + breadth_score + trend_score
        out.append(
            {
                "date": as_of_date,
                "industry_name": industry,
                "flow_score": float(flow_score),
                "breadth_score": float(breadth_score),
                "trend_score": float(trend_score),
                "total_score": float(total),
                "updated_at": _now_iso(),
                "flags": {"flow": flow_flags, "breadth": breadth_flags, "trend": trend_flags},
            }
        )
    return out


def ensure_scores_for_dates(dates: list[str]) -> dict[str, Any]:
    ensured = 0
    for d in dates:
        existing = scores_rows_by_date(d)
        if existing:
            continue
        rows = compute_scores_for_date(d)
        if rows:
            scores_upsert_rows(rows)
            ensured += 1
    return {"ensured": ensured}


def _is_mainline(industries_scores: list[dict[str, Any]], recent_scores: dict[str, dict[str, float]]) -> bool:
    if not industries_scores:
        return False
    for d in recent_scores.values():
        if d.get("total_score", 0.0) <= MAINLINE_THRESHOLD:
            return False
    return True


def get_cn_industry_mainline(*, as_of_date: str | None = None) -> dict[str, Any]:
    d = (as_of_date or "").strip() or (flow_latest_date() or "")
    if not d:
        return {"asOfDate": "", "dates": [], "allScores": [], "currentMainline": []}
    dates_for_trend = _trade_dates_upto(d, 21)
    ensure_metrics_for_dates(dates_for_trend)
    ensure_scores_for_dates(dates_for_trend[-max(MAINLINE_STREAK_DAYS, 1) :])
    scores_today = scores_rows_by_date(d)

    recent_dates = _trade_dates_upto(d, MAINLINE_STREAK_DAYS)
    recent_rows = scores_rows_for_dates(recent_dates)
    recent_by_industry: dict[str, dict[str, dict[str, float]]] = {}
    for r in recent_rows:
        industry = r["industry_name"]
        d2 = r["date"]
        recent_by_industry.setdefault(industry, {})[d2] = {"total_score": float(r["total_score"])}

    def _to_row(r: dict[str, Any], *, is_mainline: bool) -> dict[str, Any]:
        return {
            "industryName": r["industry_name"],
            "flowScore": float(r["flow_score"]),
            "breadthScore": float(r["breadth_score"]),
            "trendScore": float(r["trend_score"]),
            "totalScore": float(r["total_score"]),
            "isMainline": bool(is_mainline),
            "flags": r.get("flags") if isinstance(r.get("flags"), dict) else {},
        }

    current_mainline: list[dict[str, Any]] = []
    for r in scores_today:
        industry = r["industry_name"]
        recent = recent_by_industry.get(industry, {})
        if len(recent) < MAINLINE_STREAK_DAYS:
            continue
        is_mainline = _is_mainline([r], recent)
        if is_mainline:
            current_mainline.append(_to_row(r, is_mainline=True))

    mainline_names = {r["industryName"] for r in current_mainline}
    all_scores = [_to_row(r, is_mainline=(r["industry_name"] in mainline_names)) for r in scores_today]
    return {
        "asOfDate": d,
        "dates": dates_for_trend,
        "allScores": all_scores,
        "currentMainline": current_mainline,
    }


def sync_cn_industry_mainline(*, as_of_date: str | None = None, force: bool = False) -> dict[str, Any]:
    d = (as_of_date or "").strip() or (flow_latest_date() or "")
    if not d:
        return {"ok": False, "error": "no_industry_flow_data"}
    dates_for_trend = _trade_dates_upto(d, 21)
    metrics = ensure_metrics_for_dates(dates_for_trend) if force else ensure_metrics_for_dates(dates_for_trend)
    scores = ensure_scores_for_dates(dates_for_trend[-max(MAINLINE_STREAK_DAYS, 1) :])
    return {
        "ok": True,
        "asOfDate": d,
        "metricsEnsured": int(metrics.get("ensured") or 0),
        "scoresEnsured": int(scores.get("ensured") or 0),
    }
