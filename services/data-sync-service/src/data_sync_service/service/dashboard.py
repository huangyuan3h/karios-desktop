from __future__ import annotations

import time
from datetime import UTC, datetime
from typing import Any

from data_sync_service.db import get_connection
from data_sync_service.db.industry_fund_flow import ensure_table as ensure_industry
from data_sync_service.db.market_sentiment import get_latest_date as get_latest_sentiment_date
from data_sync_service.db.market_sentiment import list_days as list_sentiment_days
from data_sync_service.db.tv import list_snapshots_for_screener_full
from data_sync_service.service.industry_fund_flow import get_cn_industry_fund_flow, sync_cn_industry_fund_flow
from data_sync_service.service.market_regime import get_index_signals
from data_sync_service.service.market_sentiment import sync_cn_sentiment
from data_sync_service.service.tv import list_screeners, sync_screener


def _now_iso() -> str:
    return datetime.now(tz=UTC).isoformat()


def _today_iso_date() -> str:
    return datetime.now(tz=UTC).date().isoformat()


def _industry_top_by_date(*, as_of_date: str, days: int = 5, top_k: int = 5) -> dict[str, Any]:
    """
    Return TopK industry names per date for the last N days (<= as_of_date).

    Shape:
      { asOfDate, days, topK, dates, topByDate: [{date, top:[name...]}] }
    """
    ensure_industry()
    days2 = max(1, min(int(days), 30))
    topk2 = max(1, min(int(top_k), 20))
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH base AS (
                  SELECT date, industry_name, net_inflow
                  FROM market_cn_industry_fund_flow_daily
                  WHERE date <= %s
                ),
                ranked AS (
                  SELECT
                    date,
                    industry_name,
                    ROW_NUMBER() OVER (PARTITION BY date ORDER BY net_inflow DESC) AS rn
                  FROM base
                ),
                dates AS (
                  SELECT DISTINCT date
                  FROM base
                  ORDER BY date DESC
                  LIMIT %s
                )
                SELECT r.date, r.rn, r.industry_name
                FROM ranked r
                JOIN dates d ON d.date = r.date
                WHERE r.rn <= %s
                ORDER BY r.date ASC, r.rn ASC
                """,
                (as_of_date, days2, topk2),
            )
            rows = cur.fetchall()

    by_date: dict[str, list[str]] = {}
    for r in rows:
        d = str(r[0])
        name = str(r[2] or "")
        if not name:
            continue
        by_date.setdefault(d, []).append(name)
    dates_sorted = sorted(by_date.keys())
    top_by_date = [{"date": d, "top": by_date.get(d, [])[:topk2]} for d in dates_sorted]
    return {
        "asOfDate": as_of_date,
        "days": days2,
        "topK": topk2,
        "dates": dates_sorted,
        "topByDate": top_by_date,
    }


def _industry_flow_5d_items(*, as_of_date: str) -> tuple[list[str], list[dict[str, Any]]]:
    """
    Compute 5D aggregated flow items from DB for the last 5 cached dates (<= as_of_date).
    """
    ensure_industry()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH dates AS (
                  SELECT DISTINCT date
                  FROM market_cn_industry_fund_flow_daily
                  WHERE date <= %s
                  ORDER BY date DESC
                  LIMIT 5
                )
                SELECT d.date, b.industry_code, b.industry_name, b.net_inflow
                FROM market_cn_industry_fund_flow_daily b
                JOIN dates d ON d.date = b.date
                ORDER BY d.date ASC
                """,
                (as_of_date,),
            )
            rows = cur.fetchall()

    dates_sorted: list[str] = sorted({str(r[0]) for r in rows if r and r[0]})
    if not dates_sorted:
        return [], []

    last_date = dates_sorted[-1]
    by_code: dict[str, dict[str, Any]] = {}
    for r in rows:
        d = str(r[0] or "")
        code = str(r[1] or "")
        name = str(r[2] or "")
        try:
            v = float(r[3] or 0.0)
        except Exception:
            v = 0.0
        if not code:
            continue
        rec = by_code.setdefault(code, {"industryCode": code, "industryName": name, "perDate": {}})
        if name and not rec.get("industryName"):
            rec["industryName"] = name
        rec["perDate"][d] = v

    items: list[dict[str, Any]] = []
    for code, rec in by_code.items():
        per: dict[str, float] = rec.get("perDate") or {}
        series = [{"date": d, "netInflow": float(per.get(d, 0.0) or 0.0)} for d in dates_sorted]
        sum5d = 0.0
        for p in series:
            try:
                sum5d += float(p.get("netInflow") or 0.0)
            except Exception:
                sum5d += 0.0
        items.append(
            {
                "industryCode": code,
                "industryName": str(rec.get("industryName") or ""),
                "sum5d": sum5d,
                "netInflow": float(per.get(last_date, 0.0) or 0.0),
                "series": series,
            }
        )
    return dates_sorted, items


def _industry_flow_5d(*, as_of_date: str) -> dict[str, Any]:
    """
    Numeric 5D inflow block used by Dashboard under industryFundFlow.flow5d.
    """
    dates_sorted, items = _industry_flow_5d_items(as_of_date=as_of_date)
    if not dates_sorted:
        return {"asOfDate": as_of_date, "days": 5, "topN": 10, "dates": [], "top": []}
    top_in = sorted(items, key=lambda x: float(x.get("sum5d") or 0.0), reverse=True)[:10]
    return {"asOfDate": as_of_date, "days": 5, "topN": 10, "dates": dates_sorted, "top": top_in}


def _industry_flow_5d_out(*, as_of_date: str) -> dict[str, Any]:
    """
    5D outflow block used by Dashboard under industryFundFlow.flow5dOut.
    """
    dates_sorted, items = _industry_flow_5d_items(as_of_date=as_of_date)
    if not dates_sorted:
        return {"asOfDate": as_of_date, "days": 5, "topN": 10, "dates": [], "top": []}
    top_out = sorted(items, key=lambda x: float(x.get("sum5d") or 0.0))[:10]
    return {"asOfDate": as_of_date, "days": 5, "topN": 10, "dates": dates_sorted, "top": top_out}


def _screeners_status(limit: int = 50) -> list[dict[str, Any]]:
    """
    Return enabled screeners + latest snapshot meta.
    """
    scr = list_screeners()
    items = scr.get("items") if isinstance(scr, dict) else []
    rows: list[dict[str, Any]] = []
    for it in (items if isinstance(items, list) else [])[: max(1, min(int(limit), 200))]:
        if not isinstance(it, dict):
            continue
        if not bool(it.get("enabled")):
            continue
        sid = str(it.get("id") or "").strip()
        if not sid:
            continue
        latest = list_snapshots_for_screener_full(sid, limit=1)
        meta = latest[0] if latest else {}
        filters = meta.get("filters") if isinstance(meta, dict) else []
        filters_count = len(filters) if isinstance(filters, list) else 0
        rows.append(
            {
                "id": sid,
                "name": str(it.get("name") or sid),
                "enabled": True,
                "updatedAt": it.get("updatedAt"),
                "capturedAt": meta.get("capturedAt") if isinstance(meta, dict) else None,
                "rowCount": int(meta.get("rowCount") or 0) if isinstance(meta, dict) else 0,
                "filtersCount": int(filters_count),
            }
        )
    return rows


def _index_signal_items(*, as_of_date: str) -> list[dict[str, Any]]:
    """
    Build index traffic-light signals for selected indices using MA20/MA5.
    During trading hours, try to use realtime quotes from tushare.
    """
    _ = as_of_date
    return get_index_signals(as_of_date=as_of_date)


def dashboard_summary() -> dict[str, Any]:
    """
    Minimal Dashboard summary for UI:
      - asOfDate
      - industryFundFlow: {dates, topByDate, flow5d}
      - marketSentiment: {asOfDate, days, items}
      - screeners: list
    """
    # Prefer sentiment latest date as asOfDate, otherwise today.
    as_of = get_latest_sentiment_date() or _today_iso_date()

    industry_daily = _industry_top_by_date(as_of_date=as_of, days=5, top_k=5)
    flow5d = _industry_flow_5d(as_of_date=as_of)
    flow5d_out = _industry_flow_5d_out(as_of_date=as_of)
    industry = {**industry_daily, "flow5d": flow5d, "flow5dOut": flow5d_out}

    sentiment_items = list_sentiment_days(as_of_date=as_of, days=5)
    market_sentiment = {
        "asOfDate": as_of,
        "days": 5,
        "items": sentiment_items,
        "indexSignals": _index_signal_items(as_of_date=as_of),
    }

    screeners = _screeners_status(limit=50)
    return {
        "asOfDate": as_of,
        "industryFundFlow": industry,
        "marketSentiment": market_sentiment,
        "screeners": screeners,
    }


def dashboard_sync(*, force: bool = True, screeners: bool = True) -> dict[str, Any]:
    """
    Minimal Dashboard sync:
      - industry fund flow sync
      - market sentiment sync
      - TradingView screeners sync (all enabled)
    """
    started_at = _now_iso()
    steps: list[dict[str, Any]] = []

    def step(name: str, fn) -> None:
        st = time.perf_counter()
        ok = True
        msg: str | None = None
        meta: dict[str, Any] = {}
        try:
            out = fn()
            if isinstance(out, dict):
                meta = out
        except Exception as exc:  # noqa: BLE001
            ok = False
            msg = str(exc)
        dur = int((time.perf_counter() - st) * 1000)
        steps.append({"name": name, "ok": ok, "durationMs": dur, "message": msg, "meta": meta})

    # 1) Industry
    def _sync_industry() -> dict[str, Any]:
        out = sync_cn_industry_fund_flow(days=10, top_n=10)
        return out if isinstance(out, dict) else {"ok": True}

    step("industryFundFlow", _sync_industry)

    # 2) Sentiment
    def _sync_sentiment() -> dict[str, Any]:
        d = datetime.now(tz=UTC).date().isoformat()
        out = sync_cn_sentiment(date_str=d, force=bool(force))
        items = out.get("items") if isinstance(out, dict) else []
        last = items[-1] if isinstance(items, list) and items else {}
        return {
            "asOfDate": out.get("asOfDate") if isinstance(out, dict) else d,
            "riskMode": str((last or {}).get("riskMode") or ""),
            "premium": (last or {}).get("yesterdayLimitUpPremium"),
            "failedRate": (last or {}).get("failedLimitUpRate"),
        }

    step("marketSentiment", _sync_sentiment)

    # 3) Screeners
    screener_failed: list[str] = []
    screener_missing: list[str] = []

    def _sync_screeners() -> dict[str, Any]:
        scr = list_screeners()
        items = scr.get("items") if isinstance(scr, dict) else []
        items_list = items if isinstance(items, list) else []
        enabled = [x for x in items_list if isinstance(x, dict) and bool(x.get("enabled"))]
        if not bool(screeners):
            return {"enabled": len(enabled), "skipped": True, "failed": 0, "missing": 0}
        for sc in enabled:
            sid = str(sc.get("id") or "").strip()
            if not sid:
                continue
            try:
                res = sync_screener(screener_id=sid)
                rc = int(res.get("rowCount") or 0) if isinstance(res, dict) else 0
                if rc <= 0:
                    screener_missing.append(sid)
            except Exception:
                screener_failed.append(sid)
        return {"enabled": len(enabled), "skipped": False, "failed": len(screener_failed), "missing": len(screener_missing)}

    step("screeners", _sync_screeners)

    finished_at = _now_iso()
    ok = all(bool(s.get("ok")) for s in steps)
    return {
        "ok": ok,
        "startedAt": started_at,
        "finishedAt": finished_at,
        "steps": steps,
        "screener": {"failed": screener_failed, "missing": screener_missing},
    }
