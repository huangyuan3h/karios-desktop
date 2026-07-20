"""Watchlist post-close automation orchestration."""

from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Any

from data_sync_service.db.industry_fund_flow import (
    get_dates_upto,
    get_sum_by_industry_for_dates,
)
from data_sync_service.db.industry_fund_flow import (
    get_latest_date as get_latest_industry_date,
)
from data_sync_service.db.sync_job_record import get_today_run
from data_sync_service.db.trade_calendar import get_open_dates, is_trading_day
from data_sync_service.db.watchlist_automation import (
    get_pending_run,
    get_run_by_id,
    get_scores_for_symbol,
    insert_automation_run,
    list_registry,
    upsert_score_daily,
)
from data_sync_service.service.alpha_radar_catalyst import list_catalyst_stocks
from data_sync_service.service.close_sync import JOB_TYPE as CLOSE_JOB_TYPE
from data_sync_service.service.close_sync import _cn_today
from data_sync_service.service.dashboard import _sync_screeners_step
from data_sync_service.service.industry_fund_flow import sync_cn_industry_fund_flow
from data_sync_service.service.trade_calendar_utils import resolve_effective_as_of, trade_dates_upto
from data_sync_service.service.trendok import compute_trendok_for_symbols

logger = logging.getLogger(__name__)

SCORE_REMOVAL_THRESHOLD = 30.0
CATALYST_SCORE_MIN = 85.0
CONSECUTIVE_LOW_SCORE_DAYS = 3


def _normalize_trade_date(value: str | None) -> str | None:
    if not value:
        return None
    text = str(value).strip()
    if len(text) == 8 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:8]}"
    if len(text) >= 10 and text[4] == "-" and text[7] == "-":
        return text[:10]
    return text


def _to_yyyymmdd(d: date) -> str:
    return d.strftime("%Y-%m-%d")


def _shanghai_today_iso() -> str:
    return _to_yyyymmdd(_cn_today())


def get_top_5d_industry_names(as_of_date: str | None = None) -> set[str]:
    flow_date = resolve_effective_as_of((as_of_date or "").strip() or get_latest_industry_date())
    if not flow_date:
        return set()
    dates_5 = trade_dates_upto(flow_date, 5, fallback_dates_fn=get_dates_upto)
    if not dates_5:
        return set()
    sums = get_sum_by_industry_for_dates(dates_5)
    return {str(x.get("industry_name") or "") for x in sums[:5] if x.get("industry_name")}


def get_last_n_trading_dates(n: int, *, end: date | None = None) -> list[str]:
    end_d = end or _cn_today()
    start_d = end_d - timedelta(days=max(n * 4, 14))
    opens = get_open_dates("SSE", start_d, end_d)
    if len(opens) < n:
        return [_to_yyyymmdd(d) for d in opens]
    return [_to_yyyymmdd(d) for d in opens[-n:]]


def _industry_from_trendok(row: dict[str, Any]) -> str | None:
    values = row.get("values") if isinstance(row.get("values"), dict) else {}
    em = values.get("emIndustry") or values.get("em_industry")
    if em and str(em).strip():
        return str(em).strip()
    ind = row.get("industry")
    if ind and str(ind).strip():
        return str(ind).strip()
    return None


def record_score_snapshots(symbols: list[str]) -> tuple[str | None, int, list[dict[str, Any]]]:
    if not symbols:
        return None, 0, []
    rows_out = compute_trendok_for_symbols(symbols, realtime=False)
    score_rows: list[dict[str, Any]] = []
    trade_date: str | None = None
    for row in rows_out:
        if not isinstance(row, dict):
            continue
        sym = str(row.get("symbol") or "").strip()
        if not sym:
            continue
        td = _normalize_trade_date(str(row.get("asOfDate") or ""))
        if td:
            trade_date = trade_date or td
        score = row.get("score")
        score_val = float(score) if score is not None else None
        score_rows.append(
            {
                "symbol": sym,
                "trade_date": td or _shanghai_today_iso(),
                "score": score_val,
                "industry": _industry_from_trendok(row),
            }
        )
    count = upsert_score_daily(score_rows)
    return trade_date, count, rows_out


def should_remove_symbol(
    *,
    symbol: str,
    source: str,
    trade_dates: list[str],
    top_5d_industries: set[str],
    current_industry: str | None,
    position_pct: float | None = None,
) -> tuple[bool, str]:
    if source == "alpha_radar":
        return False, "alpha_radar_exempt"
    # Do not GC held names (missing/None position treated as flat = 0).
    if position_pct is not None and float(position_pct) > 0:
        return False, "held_position"
    if len(trade_dates) < CONSECUTIVE_LOW_SCORE_DAYS:
        return False, "insufficient_history"

    history = get_scores_for_symbol(symbol, trade_dates)
    by_date = {h["trade_date"]: h for h in history}
    for td in trade_dates:
        rec = by_date.get(td)
        if not rec:
            return False, "missing_score_day"
        score = rec.get("score")
        if score is None:
            return False, "null_score_breaks_streak"
        if float(score) >= SCORE_REMOVAL_THRESHOLD:
            return False, "score_not_low_enough"
    industry = current_industry
    if not industry:
        last = history[-1] if history else None
        industry = last.get("industry") if last else None
    if not industry:
        return False, "missing_industry"
    if industry in top_5d_industries:
        return False, "industry_still_in_top5"
    return True, "score_low_3d_and_industry_outside_top5"


def compute_removals(
    registry: list[dict[str, Any]],
    *,
    trade_dates: list[str],
    top_5d_industries: set[str],
    trendok_by_symbol: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for item in registry:
        sym = str(item.get("symbol") or "").strip()
        if not sym:
            continue
        source = str(item.get("source") or "manual")
        trend = trendok_by_symbol.get(sym) or {}
        industry = _industry_from_trendok(trend)
        pos_raw = item.get("positionPct")
        try:
            position_pct = float(pos_raw) if pos_raw is not None else None
        except (TypeError, ValueError):
            position_pct = None
        ok, reason = should_remove_symbol(
            symbol=sym,
            source=source,
            trade_dates=trade_dates,
            top_5d_industries=top_5d_industries,
            current_industry=industry,
            position_pct=position_pct,
        )
        if ok:
            out.append({"symbol": sym, "reason": reason})
    return out


def compute_alpha_additions(limit: int = 200) -> list[dict[str, Any]]:
    payload = list_catalyst_stocks(limit=limit)
    items = payload.get("items") if isinstance(payload, dict) else []
    if not isinstance(items, list):
        return []
    out: list[dict[str, Any]] = []
    for row in items:
        if not isinstance(row, dict):
            continue
        score = float(row.get("catalystScore") or 0.0)
        if score <= CATALYST_SCORE_MIN:
            continue
        articles = row.get("articles") if isinstance(row.get("articles"), list) else []
        has_s = any(str(a.get("catalystGrade") or "").upper() == "S" for a in articles if isinstance(a, dict))
        if not has_s:
            continue
        sym_raw = str(row.get("symbol") or "").strip()
        if not sym_raw:
            continue
        sym = sym_raw if sym_raw.startswith("CN:") else f"CN:{sym_raw}"
        out.append(
            {
                "symbol": sym,
                "name": str(row.get("name") or sym),
                "catalystScore": score,
            }
        )
    return out


def _precheck(*, force: bool) -> tuple[bool, str | None]:
    if force:
        return False, None
    today = _cn_today()
    open_flag = is_trading_day("SSE", today)
    if open_flag is False:
        return True, "not_trading_day"
    today_run = get_today_run(CLOSE_JOB_TYPE)
    if not today_run or not today_run.get("success"):
        return True, "close_sync_not_ready"
    return False, None


def run_watchlist_automation(*, trigger: str = "scheduled", force: bool = False) -> dict[str, Any]:
    trade_date = _shanghai_today_iso()
    skipped, skip_reason = _precheck(force=force)
    meta: dict[str, Any] = {"trigger": trigger, "force": force}

    if skipped:
        run_id = insert_automation_run(
            trade_date=trade_date,
            trigger_type=trigger,
            skipped=True,
            skip_reason=skip_reason,
            remove_items=[],
            alpha_add=[],
            meta=meta,
        )
        return {
            "runId": run_id,
            "tradeDate": trade_date,
            "skipped": True,
            "skipReason": skip_reason,
            "remove": [],
            "alphaAdd": [],
            "meta": meta,
        }

    try:
        industry_result = sync_cn_industry_fund_flow(days=10, top_n=10)
        meta["industrySync"] = industry_result if isinstance(industry_result, dict) else {"ok": True}
    except Exception as exc:  # noqa: BLE001
        logger.warning("watchlist automation industry sync failed: %s", exc)
        meta["industrySync"] = {"ok": False, "error": str(exc)}

    try:
        screener_result = _sync_screeners_step(screeners_enabled=True)
        meta["screenerSync"] = screener_result if isinstance(screener_result, dict) else {}
    except Exception as exc:  # noqa: BLE001
        logger.warning("watchlist automation screener sync failed: %s", exc)
        meta["screenerSync"] = {"ok": False, "error": str(exc)}

    registry = list_registry()
    symbols = [str(x.get("symbol") or "").strip() for x in registry if x.get("symbol")]
    symbols = [s for s in symbols if s]

    score_trade_date, score_count, trendok_rows = record_score_snapshots(symbols)
    if score_trade_date:
        trade_date = score_trade_date
    meta["scoreSnapshots"] = score_count

    trendok_by_symbol = {
        str(r.get("symbol")): r for r in trendok_rows if isinstance(r, dict) and r.get("symbol")
    }

    top_5d = get_top_5d_industry_names()
    meta["top5dIndustries"] = sorted(top_5d)

    streak_dates = get_last_n_trading_dates(CONSECUTIVE_LOW_SCORE_DAYS)
    meta["streakTradeDates"] = streak_dates

    remove_items = compute_removals(
        registry,
        trade_dates=streak_dates,
        top_5d_industries=top_5d,
        trendok_by_symbol=trendok_by_symbol,
    )
    alpha_add = compute_alpha_additions()
    meta["alphaCandidates"] = len(alpha_add)

    run_id = insert_automation_run(
        trade_date=trade_date,
        trigger_type=trigger,
        skipped=False,
        skip_reason=None,
        remove_items=remove_items,
        alpha_add=alpha_add,
        meta=meta,
    )

    return {
        "runId": run_id,
        "tradeDate": trade_date,
        "skipped": False,
        "skipReason": None,
        "remove": remove_items,
        "alphaAdd": alpha_add,
        "meta": meta,
    }


def get_automation_pending(trade_date: str | None = None) -> dict[str, Any] | None:
    return get_pending_run(trade_date)


def get_automation_latest() -> dict[str, Any] | None:
    from data_sync_service.db.watchlist_automation import get_latest_run

    return get_latest_run()


def ack_automation_run(run_id: str, screener_added: int | None = None) -> dict[str, Any] | None:
    from data_sync_service.db.watchlist_automation import ack_run

    return ack_run(run_id, screener_added=screener_added)


def get_automation_run(run_id: str) -> dict[str, Any] | None:
    return get_run_by_id(run_id)
