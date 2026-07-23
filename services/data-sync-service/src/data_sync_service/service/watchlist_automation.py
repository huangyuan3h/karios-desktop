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
from data_sync_service.service.alpha_radar_catalyst import (
    aggregate_catalyst_stocks,
    default_max_age_days,
    list_catalyst_stocks,
)
from data_sync_service.service.close_sync import JOB_TYPE as CLOSE_JOB_TYPE
from data_sync_service.service.close_sync import _cn_today
from data_sync_service.service.dashboard import _sync_screeners_step
from data_sync_service.service.industry_fund_flow import sync_cn_industry_fund_flow
from data_sync_service.service.industry_taxonomy import is_sw_l1_industry_name
from data_sync_service.service.trade_calendar_utils import resolve_effective_as_of, trade_dates_upto
from data_sync_service.service.trendok import compute_trendok_for_symbols

logger = logging.getLogger(__name__)

SCORE_REMOVAL_THRESHOLD = 30.0
CATALYST_SCORE_MIN = 85.0
CONSECUTIVE_LOW_SCORE_DAYS = 3
# Alpha entry light gate: wider than BUY mainline Top3 (see TIP-004).
ALPHA_ENTRY_TOP_INDUSTRIES = 10
# TIP-003 empty-window fallback universe.
FALLBACK_TOP_INDUSTRIES = 5
FALLBACK_PER_INDUSTRY = 30
FALLBACK_MAX_TOTAL = 80
# Mirror apps/desktop-ui execution-action DEFENSE_SECTOR_KEYWORDS.
DEFENSE_SECTOR_KEYWORDS = (
    "银行",
    "电力",
    "公用事业",
    "中药",
    "煤炭",
    "高速公路",
)


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


def get_top_5d_industry_names(as_of_date: str | None = None, *, top_n: int = 5) -> set[str]:
    flow_date = resolve_effective_as_of((as_of_date or "").strip() or get_latest_industry_date())
    if not flow_date:
        return set()
    dates_5 = trade_dates_upto(flow_date, 5, fallback_dates_fn=get_dates_upto)
    if not dates_5:
        return set()
    n = max(1, int(top_n))
    sums = get_sum_by_industry_for_dates(dates_5)
    return {str(x.get("industry_name") or "") for x in sums[:n] if x.get("industry_name")}


def get_top_5d_industry_names_ordered(as_of_date: str | None = None, *, top_n: int = 5) -> list[str]:
    """Same as get_top_5d_industry_names but preserves inflow rank order."""
    flow_date = resolve_effective_as_of((as_of_date or "").strip() or get_latest_industry_date())
    if not flow_date:
        return []
    dates_5 = trade_dates_upto(flow_date, 5, fallback_dates_fn=get_dates_upto)
    if not dates_5:
        return []
    n = max(1, int(top_n))
    sums = get_sum_by_industry_for_dates(dates_5)
    out: list[str] = []
    for x in sums[:n]:
        name = str(x.get("industry_name") or "").strip()
        if name:
            out.append(name)
    return out


def list_fallback_universe_symbols(
    *,
    max_total: int = FALLBACK_MAX_TOTAL,
    per_industry: int = FALLBACK_PER_INDUSTRY,
    top_n: int = FALLBACK_TOP_INDUSTRIES,
    as_of_date: str | None = None,
) -> dict[str, Any]:
    """
    TIP-003 empty-window fallback universe.

    5D TopN SW L1 industries (non-defense) → EM industry_name LIKE match → capped symbols.
    """
    from data_sync_service.db.stock_eastmoney_industry import search_stocks_by_industry_keyword

    cap = max(1, min(int(max_total), 200))
    per = max(1, min(int(per_industry), 40))
    ranked = get_top_5d_industry_names_ordered(as_of_date, top_n=top_n)
    industries_raw = list(ranked)
    industries = [name for name in ranked if not is_defense_sector(name)]
    skipped_defense = [name for name in industries_raw if name not in industries]

    symbols: list[str] = []
    names_by_symbol: dict[str, str] = {}
    seen: set[str] = set()
    truncated = False
    for industry in industries:
        if len(symbols) >= cap:
            truncated = True
            break
        room = cap - len(symbols)
        rows = search_stocks_by_industry_keyword(industry, limit=min(per, room))
        for row in rows:
            sym = str(row.get("symbol") or "").strip()
            if not sym or sym in seen:
                continue
            seen.add(sym)
            symbols.append(sym)
            names_by_symbol[sym] = str(row.get("name") or sym)
            if len(symbols) >= cap:
                truncated = True
                break

    return {
        "industries": industries,
        "industriesRaw": industries_raw,
        "skippedDefense": skipped_defense,
        "symbols": symbols,
        "namesBySymbol": names_by_symbol,
        "truncated": truncated,
        "maxTotal": cap,
        "count": len(symbols),
    }


def is_defense_sector(industry_name: str | None) -> bool:
    """True when EM/SW industry should be blocked from alpha entry (mirrors FE BUY gate).

    Special-case: keyword ``电力`` must not match growth board ``电力设备``.
    """
    name = str(industry_name or "").strip()
    if not name:
        return False
    for kw in DEFENSE_SECTOR_KEYWORDS:
        if kw == "电力":
            if name == "电力" or ("电力" in name and "电力设备" not in name):
                return True
            continue
        if kw in name:
            return True
    return False


def _cn_symbol_to_ts_code(symbol: str) -> str | None:
    s = _normalize_cn_watchlist_symbol(symbol)
    if not s.startswith("CN:"):
        return None
    ticker = s.split(":", 1)[1].strip()
    if len(ticker) != 6 or not ticker.isdigit():
        return None
    suffix = "SH" if ticker.startswith("6") else "SZ"
    return f"{ticker}.{suffix}"


def _resolve_em_industries_for_symbols(symbols: list[str]) -> dict[str, str]:
    """Map CN:xxxxxx → East Money industry name (DB only)."""
    from data_sync_service.service.eastmoney_industry import lookup_em_industries_for_ts_codes

    ts_by_sym: dict[str, str] = {}
    for sym in symbols:
        ts = _cn_symbol_to_ts_code(sym)
        if ts:
            ts_by_sym[sym] = ts
    if not ts_by_sym:
        return {}
    by_ts = lookup_em_industries_for_ts_codes(list(ts_by_sym.values()))
    out: dict[str, str] = {}
    for sym, ts in ts_by_sym.items():
        name = by_ts.get(ts)
        if name and str(name).strip():
            out[sym] = str(name).strip()
    return out

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


def _normalize_cn_watchlist_symbol(symbol: str) -> str:
    text = str(symbol or "").strip().upper()
    if not text:
        return ""
    if text.startswith("CN:"):
        return text
    if text.startswith("HK:"):
        return text
    return f"CN:{text}"


def symbols_with_max_grade_s(catalyst_payload: dict[str, Any] | None) -> set[str]:
    """Symbols whose catalyst window still includes at least one grade-S article."""
    items = catalyst_payload.get("items") if isinstance(catalyst_payload, dict) else None
    if not isinstance(items, list):
        return set()
    out: set[str] = set()
    for row in items:
        if not isinstance(row, dict):
            continue
        articles = row.get("articles") if isinstance(row.get("articles"), list) else []
        has_s = any(
            str(a.get("catalystGrade") or "").upper() == "S" for a in articles if isinstance(a, dict)
        )
        if not has_s:
            continue
        sym = _normalize_cn_watchlist_symbol(str(row.get("symbol") or ""))
        if sym:
            out.add(sym)
    return out


def load_catalyst_window(*, add_limit: int = 200) -> tuple[dict[str, Any], set[str]]:
    """
    Load full catalyst aggregation for Max Grade=S GC exemption, while alpha-add
    still uses the score-ranked top ``add_limit`` slice (TIP-005 / TIP-004).
    """
    from data_sync_service.db.alpha_radar import fetch_trends_for_catalyst

    age_days = default_max_age_days()
    trends = fetch_trends_for_catalyst(max_age_days=age_days)
    all_items = aggregate_catalyst_stocks(trends)
    full_payload = {
        "stalenessBasis": "published_then_fetched",
        "maxAgeDays": age_days,
        "total": len(all_items),
        "items": all_items,
    }
    alpha_s = symbols_with_max_grade_s(full_payload)
    lim = max(1, min(int(add_limit), 200))
    add_payload = {
        "stalenessBasis": full_payload["stalenessBasis"],
        "maxAgeDays": age_days,
        "total": len(all_items),
        "items": all_items[:lim],
    }
    return add_payload, alpha_s


def should_remove_symbol(
    *,
    symbol: str,
    source: str,
    trade_dates: list[str],
    top_5d_industries: set[str],
    current_industry: str | None,
    position_pct: float | None = None,
    alpha_s_symbols: set[str] | None = None,
) -> tuple[bool, str]:
    # Align with WATCH_SILENT: only Max Grade=S stays exempt from 3-day Score GC.
    if source == "alpha_radar":
        norm = _normalize_cn_watchlist_symbol(symbol)
        if norm and alpha_s_symbols and norm in alpha_s_symbols:
            return False, "alpha_s_exempt"
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
    alpha_s_symbols: set[str] | None = None,
) -> list[dict[str, Any]]:
    s_set = alpha_s_symbols or set()
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
            alpha_s_symbols=s_set,
        )
        if ok:
            out.append({"symbol": sym, "reason": reason})
    return out


def compute_alpha_additions(
    limit: int = 200,
    *,
    catalyst_payload: dict[str, Any] | None = None,
    industry_by_symbol: dict[str, str] | None = None,
    top_industries: set[str] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """
    Alpha Radar → Watchlist candidates with light entry gates (TIP-004).

    Gates (after score>85 and grade S):
    - defense sector → reject
    - missing EM industry → reject (fail-closed per symbol)
    - Top10 (SW L1 5D flow): only enforced when the stock industry label is itself
      an SW L1 name (exact match). Granular EM boards (e.g. 半导体 vs 电子) skip
      Top10 rather than false-reject (taxonomy mismatch fail-open).
    - if Top10 set empty (flow data missing) → skip Top10 gate entirely
    """
    payload = catalyst_payload if catalyst_payload is not None else list_catalyst_stocks(limit=limit)
    items = payload.get("items") if isinstance(payload, dict) else []
    if not isinstance(items, list):
        return [], {}

    rejected: dict[str, int] = {}

    def _bump(reason: str) -> None:
        rejected[reason] = int(rejected.get(reason) or 0) + 1

    prelim: list[dict[str, Any]] = []
    for row in items:
        if not isinstance(row, dict):
            continue
        score = float(row.get("catalystScore") or 0.0)
        if score <= CATALYST_SCORE_MIN:
            _bump("low_score")
            continue
        articles = row.get("articles") if isinstance(row.get("articles"), list) else []
        has_s = any(str(a.get("catalystGrade") or "").upper() == "S" for a in articles if isinstance(a, dict))
        if not has_s:
            _bump("no_s_grade")
            continue
        sym = _normalize_cn_watchlist_symbol(str(row.get("symbol") or ""))
        if not sym:
            _bump("bad_symbol")
            continue
        prelim.append(
            {
                "symbol": sym,
                "name": str(row.get("name") or sym),
                "catalystScore": score,
            }
        )

    if not prelim:
        return [], rejected

    industries = industry_by_symbol
    if industries is None:
        industries = _resolve_em_industries_for_symbols([str(x["symbol"]) for x in prelim])

    top_set = top_industries
    if top_set is None:
        top_set = get_top_5d_industry_names(top_n=ALPHA_ENTRY_TOP_INDUSTRIES)
    top_ready = bool(top_set)

    out: list[dict[str, Any]] = []
    for row in prelim:
        sym = str(row["symbol"])
        industry = industries.get(sym) if industries else None
        if not industry:
            _bump("missing_industry")
            continue
        if is_defense_sector(industry):
            _bump("defense_sector")
            continue
        if top_ready:
            # Fund-flow Top10 is SW L1; EM stock boards are often finer-grained.
            if is_sw_l1_industry_name(industry):
                if industry not in top_set:
                    _bump("not_in_top10")
                    continue
            # else: non-SW EM label → skip Top10 (fail-open), do not reject
        out.append(row)
    return out, rejected


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
        if isinstance(industry_result, dict):
            meta["industrySync"] = {**industry_result, "ok": True}
        else:
            meta["industrySync"] = {"ok": True}
    except Exception as exc:  # noqa: BLE001
        logger.warning("watchlist automation industry sync failed: %s", exc)
        meta["industrySync"] = {"ok": False, "error": str(exc)}

    try:
        screener_result = _sync_screeners_step(screeners_enabled=True)
        if isinstance(screener_result, dict):
            failed = int(screener_result.get("failed") or 0)
            meta["screenerSync"] = {**screener_result, "ok": failed == 0}
        else:
            meta["screenerSync"] = {"ok": True}
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

    catalyst_payload, alpha_s_symbols = load_catalyst_window(add_limit=200)
    meta["alphaSSymbols"] = len(alpha_s_symbols)
    meta["catalystWindowTotal"] = int(catalyst_payload.get("total") or 0)

    remove_items = compute_removals(
        registry,
        trade_dates=streak_dates,
        top_5d_industries=top_5d,
        trendok_by_symbol=trendok_by_symbol,
        alpha_s_symbols=alpha_s_symbols,
    )
    top10 = get_top_5d_industry_names(top_n=ALPHA_ENTRY_TOP_INDUSTRIES)
    meta["top10dIndustries"] = sorted(top10)
    alpha_add, alpha_rejected = compute_alpha_additions(
        catalyst_payload=catalyst_payload,
        top_industries=top10,
    )
    meta["alphaCandidates"] = len(alpha_add)
    meta["alphaRejected"] = alpha_rejected

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


def ack_automation_run(
    run_id: str,
    screener_added: int | None = None,
    funnel: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    from data_sync_service.db.watchlist_automation import ack_run

    return ack_run(run_id, screener_added=screener_added, funnel=funnel)


def get_automation_run(run_id: str) -> dict[str, Any] | None:
    return get_run_by_id(run_id)
