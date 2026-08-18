"""Watchlist post-close automation orchestration."""

from __future__ import annotations

import logging
import threading
from datetime import date, timedelta
from typing import Any

from data_sync_service.db import get_connection
from data_sync_service.db.daily import list_hk_universe_symbols
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
from data_sync_service.service.industry_fund_flow import sync_cn_industry_fund_flow
from data_sync_service.service.industry_taxonomy import is_sw_l1_industry_name
from data_sync_service.service.research import (
    RESEARCH_MAX_CANDIDATES,
    RESEARCH_SCORE_MIN,
    build_research_catalyst_payload,
)
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
    # strip() mirrors the ordered variant below — unstripped names would
    # silently break the exact-match GC comparison in should_remove_symbol.
    return {str(x.get("industry_name") or "").strip() for x in sums[:n] if x.get("industry_name")}


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
    from data_sync_service.service.market_quotes import normalize_market_symbol

    s = _normalize_cn_watchlist_symbol(normalize_market_symbol(symbol))
    if s.startswith("CN:"):
        ticker = s.split(":", 1)[1].strip()
        if len(ticker) != 6 or not ticker.isdigit():
            return None
        suffix = "SH" if ticker.startswith("6") else "SZ"
        return f"{ticker}.{suffix}"
    if s.startswith("HK:"):
        ticker = s.split(":", 1)[1].strip()
        if not (1 <= len(ticker) <= 5 and ticker.isdigit()):
            return None
        padded = ticker.zfill(5)
        return f"{padded}.HK"
    if s.startswith("ETF:"):
        ticker = s.split(":", 1)[1].strip()
        if len(ticker) != 6 or not ticker.isdigit():
            return None
        suffix = "SH" if ticker[0] in ("5", "6", "9") else "SZ"
        return f"{ticker}.{suffix}"
    return None


# 52W pullback window constants (mirror FE importFromScreener thresholds).
PULLBACK_WINDOW_MIN = -0.15
PULLBACK_WINDOW_MAX = -0.05
# Bars needed for a meaningful 52W window; fewer → candidate marked missing.
PULLBACK_MIN_BARS = 60
PULLBACK_LOOKBACK_BARS = 300


def filter_pullback_window(
    symbols: list[str],
    *,
    as_of: str | None = None,
    min_ratio: float = PULLBACK_WINDOW_MIN,
    max_ratio: float = PULLBACK_WINDOW_MAX,
) -> dict[str, Any]:
    """
    Screen symbols by 52-week pullback using DB K-lines (daily table).

    Why not the TV snapshot column? ``High.Interval52Week`` from the Scanner API
    returns empty for virtually every row (observed 2026-08-02+), which silently
    zeroed the funnel's pullback gate and forced the fallback universe path.

    Window: latest close vs max(high) over the last ``PULLBACK_LOOKBACK_BARS``
    bars (clamped to the trailing 52 trading weeks). Candidate passes when
    ``min_ratio <= (price - high52w) / high52w <= max_ratio``.

    Returns:
      {"ok", "results": [{symbol, tsCode, price, high52w, pullbackRatio,
                          inWindow, windowBars, missing}], "asOf", "unparsed"}
    """
    from data_sync_service.db.daily import fetch_last_ohlcv_batch
    from data_sync_service.service.market_quotes import symbol_to_ts_code

    results: list[dict[str, Any]] = []
    ts_by_symbol: dict[str, str] = {}
    unparsed: list[str] = []
    for sym in symbols:
        s = str(sym or "").strip()
        if not s:
            continue
        ts = symbol_to_ts_code(s)
        if not ts:
            unparsed.append(s)
            continue
        ts_by_symbol[s] = ts

    if ts_by_symbol:
        bars = fetch_last_ohlcv_batch(list(dict.fromkeys(ts_by_symbol.values())), days=PULLBACK_LOOKBACK_BARS)
        as_of_date = max(
            (b[0] for rows in bars.values() for b in rows),
            default=as_of or "",
        )
        for sym, ts in ts_by_symbol.items():
            rows = bars.get(ts) or []
            # Tuples are (date, open, high, low, close, volume).
            parsed_highs = []
            parsed_closes = []
            for b in rows:
                try:
                    parsed_highs.append(float(b[2] or ""))
                except ValueError:
                    parsed_highs.append(None)
                try:
                    parsed_closes.append(float(b[4] or ""))
                except ValueError:
                    parsed_closes.append(None)
            valid = [h for h in parsed_highs if h is not None and h > 0]
            latest_close = next((c for c in reversed(parsed_closes) if c is not None), None)
            if latest_close is None or not valid or len(valid) < PULLBACK_MIN_BARS:
                results.append(
                    {
                        "symbol": sym,
                        "tsCode": ts,
                        "price": latest_close,
                        "high52w": None,
                        "pullbackRatio": None,
                        "inWindow": False,
                        "windowBars": len(valid),
                        "missing": True,
                    }
                )
                continue
            high52w = max(valid)
            ratio = (latest_close - high52w) / high52w
            results.append(
                {
                    "symbol": sym,
                    "tsCode": ts,
                    "price": latest_close,
                    "high52w": high52w,
                    "pullbackRatio": round(ratio, 6),
                    "inWindow": min_ratio <= ratio <= max_ratio,
                    "windowBars": len(valid),
                    "missing": False,
                }
            )
    else:
        as_of_date = as_of or ""

    return {
        "ok": True,
        "results": results,
        "asOf": as_of_date or None,
        "unparsed": unparsed,
    }


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


def record_score_snapshots(
    symbols: list[str], *, realtime: bool = False,
) -> tuple[str | None, int, list[dict[str, Any]]]:
    if not symbols:
        return None, 0, []
    # compute_trendok_for_symbols caps at 200 symbols per call; chunk so the
    # full CN screener universe (~700) and HK vol-top-N universe (500) all
    # get scored (2026-08-10: HK line was truncated to 200 — score gap).
    # realtime=True merges live quotes into the last bar so intraday runs
    # write scores under TODAY (asOfDate = today); EOD runs keep close prices.
    rows_out: list[dict[str, Any]] = []
    for i in range(0, len(symbols), 200):
        chunk = symbols[i : i + 200]
        rows_out.extend(compute_trendok_for_symbols(chunk, realtime=realtime))
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


def _is_cn_b_share(symbol: str) -> bool:
    """CN B shares live on SSE/SZSE but trade in foreign currency and carry
    no trendOK score — 900xxx (SH B) / 200xxx (SZ B)."""
    t = str(symbol or "").strip().upper()
    if not t.startswith("CN:"):
        return False
    code = t[3:]
    return len(code) == 6 and (code.startswith("900") or code.startswith("200"))


def _normalize_cn_watchlist_symbol(symbol: str) -> str:
    text = str(symbol or "").strip().upper()
    if not text:
        return ""
    if text.startswith("CN:"):
        return text
    if text.startswith("HK:"):
        return text
    if text.startswith("ETF:"):
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
    auto_qa_penalties: dict[str, dict[str, Any]] | None = None,
    score_min: float | None = None,
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
    - TIP-009 auto-QA penalty: catalystScore is multiplied by (1 - penalty)
      before the SCORE_MIN check; entries that fall under the floor are
      counted under ``auto_qa_penalty``.

    V7.0/TIP-012: ``score_min`` overrides the 85 floor — the research channel
    passes 70 (RESEARCH_SCORE_MIN) because a fresh 买入 rating is a weaker
    signal than an S-grade news catalyst but still pool-worthy.
    """
    payload = catalyst_payload if catalyst_payload is not None else list_catalyst_stocks(limit=limit)
    items = payload.get("items") if isinstance(payload, dict) else []
    if not isinstance(items, list):
        return [], {}

    score_floor = CATALYST_SCORE_MIN if score_min is None else float(score_min)

    penalties = auto_qa_penalties
    if penalties is None:
        try:
            from data_sync_service.service.alpha_radar_qa import (
                compute_auto_qa_penalty_for_catalyst,
            )
            penalties = compute_auto_qa_penalty_for_catalyst(items)
        except Exception as exc:  # noqa: BLE001
            logger.warning("auto_qa_penalty compute failed: %s", exc)
            penalties = {}

    rejected: dict[str, int] = {}

    def _bump(reason: str) -> None:
        rejected[reason] = int(rejected.get(reason) or 0) + 1

    prelim: list[dict[str, Any]] = []
    for row in items:
        if not isinstance(row, dict):
            continue
        score = float(row.get("catalystScore") or 0.0)
        if score <= score_floor:
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
        penalty_info = penalties.get(sym) or {}
        penalty = float(penalty_info.get("penalty") or 0.0)
        adjusted_score = score * (1.0 - penalty)
        if adjusted_score <= score_floor:
            _bump("auto_qa_penalty")
            continue
        prelim.append(
            {
                "symbol": sym,
                "name": str(row.get("name") or sym),
                "catalystScore": adjusted_score,
                "rawCatalystScore": score,
                "autoQaPenalty": round(penalty, 3),
                "autoQaSignals": penalty_info.get("signals") or {},
                # TIP-012: research-channel candidates keep their channel tag so
                # the frontend can mark registry source='research'.
                "channel": str(row.get("channel") or "") or None,
                # TIP-011: tag ALPHA provenance so downstream journal changes
                # and paper_trades can attribute win-rate by source.
                "source": "ALPHA",
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
        # OPT-052: HK pure-plays do not carry EM (东方财富) industry labels —
        # the EM industry pipeline is CN-only. Skipping the industry gates
        # for HK is the right behavior because (a) we have no HK fund-flow
        # data, and (b) HK Alpha S entries are already gated by score + grade
        # upstream. The only failure mode we accept is "false positive on a
        # HK name with no HK pure-play" — same as the CN knowledge-fallback
        # path, and the catalystScore gate keeps that bounded.
        is_hk = sym.startswith("HK:")
        industry = industries.get(sym) if industries else None
        if not industry and not is_hk:
            _bump("missing_industry")
            continue
        if industry and is_defense_sector(industry):
            _bump("defense_sector")
            continue
        if top_ready and industry:
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


def _score_universe_symbols() -> tuple[list[str], list[str], list[dict[str, Any]]]:
    """CN + HK score universes shared by the EOD (17:30) and intraday passes.

    Returns ``(cn_symbols, hk_symbols, registry)``. CN: **whole-market A-share
    universe from the daily table** (registry ∪ full market — 2026-08-12
    universe unification: the backtest now scores the whole market (5226),
    so the live line must match; the TV api-screener pool is retired, the
    daily compute for 5226 names is ~5s). B shares (900xxx/200xxx) get no
    score. HK: vol-top-N proxy (500) ∪ registry HK symbols (2026-08-10 HK
    parallel line).
    """
    registry = list_registry()
    symbols = [str(x.get("symbol") or "").strip() for x in registry if x.get("symbol")]
    symbols = [s for s in symbols if s]

    cn_full: list[str] = []
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT ts_code FROM daily WHERE ts_code ~ '^(6\\d{5}\\.SH|(0|3)\\d{5}\\.SZ)$' ORDER BY ts_code"
            )
            for (ts,) in cur.fetchall():
                code = str(ts)
                if _is_cn_b_share(code):
                    continue
                ticker = code.split(".")[0]
                cn_full.append(f"CN:{ticker}")

    merged: list[str] = []
    seen: set[str] = set()
    for s in symbols + cn_full:
        if s and s not in seen:
            seen.add(s)
            merged.append(s)
    hk_symbols = list_hk_universe_symbols(500)
    for row in registry:
        sym = str(row.get("symbol") or "").upper()
        if sym.startswith("HK:") and sym not in hk_symbols:
            hk_symbols.append(sym)
    return merged, hk_symbols, registry


def run_intraday_scores(*, trigger: str = "scheduled", force: bool = False) -> dict[str, Any]:
    """Intraday (trading hours) score refresh with realtime quotes merged into
    the last bar, so the S-3 health card shows TODAY's candidates during the
    session (2026-08-11: the EOD chain only wrote post-close, leaving the
    intraday decision surface empty).

    Scores are written under asOfDate — today when realtime quotes are
    available (``_merge_realtime_bar`` appends a bar dated today), otherwise
    the last bar date (harmless upsert of the previous session's values).
    The EOD pass (17:30) overwrites the same rows with close prices, so the
    paper intake at 17:42 still sees faithful EOD scores.
    """
    today = _shanghai_today_iso()
    if not force and is_trading_day("SSE", today) is False:
        return {"ok": False, "skipped": True, "skipReason": "not_trading_day", "tradeDate": today}
    cn_symbols, hk_symbols, _registry = _score_universe_symbols()
    summary: dict[str, Any] = {
        "ok": True,
        "tradeDate": today,
        "trigger": trigger,
        "realtime": True,
    }
    # Sync today's INTRADAY sentiment FIRST so the S-3 panic gate sees
    # today's state (breadth can flip intraday — e.g. 2026-08-11 morning
    # flipped to extreme_caution at ~11:00; without this the gate falls back
    # to the previous session's row). Idempotent: skips when today exists.
    try:
        from data_sync_service.service.market_sentiment import sync_cn_sentiment

        s = sync_cn_sentiment(date_str=today, force=False)
        summary["sentimentSync"] = bool(s.get("ok")) if isinstance(s, dict) else False
        summary["sentimentAsOf"] = s.get("asOfDate") if isinstance(s, dict) else None
    except Exception as exc:  # noqa: BLE001
        logger.warning("intraday sentiment sync failed: %s", exc)
        summary["sentimentSync"] = False
    try:
        cn_td, cn_count, _rows = record_score_snapshots(cn_symbols, realtime=True)
        summary["cnScoreSnapshots"] = int(cn_count or 0)
        if cn_td:
            summary["tradeDate"] = cn_td
    except Exception as exc:  # noqa: BLE001
        logger.warning("intraday scores CN failed: %s", exc)
        summary["ok"] = False
        summary["cnError"] = str(exc)
    try:
        hk_td, hk_count, _rows = record_score_snapshots(hk_symbols, realtime=True)
        summary["hkScoreSnapshots"] = int(hk_count or 0)
        if hk_td:
            summary["tradeDate"] = summary["tradeDate"] or hk_td
    except Exception as exc:  # noqa: BLE001
        logger.warning("intraday scores HK failed: %s", exc)
        summary["ok"] = False
        summary["hkError"] = str(exc)
    return summary


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

    symbols, hk_symbols, registry = _score_universe_symbols()

    score_trade_date, score_count, trendok_rows = record_score_snapshots(symbols)
    if score_trade_date:
        trade_date = score_trade_date
    meta["scoreSnapshots"] = score_count

    # 2026-08-10 (HK parallel line): score the HK strategy-line universe too
    # (vol-top-N proxy + registry HK union) — the HK paper intake needs fresh
    # daily scores; without this the HK line would be frozen at the backfill.
    hk_score_count = 0
    try:
        if hk_symbols:
            hk_td, hk_count, _hk_rows = record_score_snapshots(hk_symbols)
            hk_score_count = int(hk_count or 0)
            if hk_td:
                trade_date = trade_date or hk_td
    except Exception as exc:  # noqa: BLE001
        logger.warning("watchlist automation HK scoring failed: %s", exc)
    meta["hkScoreSnapshots"] = hk_score_count

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

    # TIP-012: research channel (研报 → α) — same entry gates, lower floor.
    research_add: list[dict[str, Any]] = []
    research_rejected: dict[str, int] = {}
    try:
        research_payload = build_research_catalyst_payload(limit=100)
        # Research reports carry their own East Money industry label (same
        # taxonomy as the EM cache); prefer it over the DB cache so fresh
        # names without a warm cache row still pass the defense/Top10 gates.
        research_industries = {
            str(x.get("symbol")): str(x["industryName"])
            for x in (research_payload.get("items") or [])
            if isinstance(x, dict) and x.get("symbol") and x.get("industryName")
        }
        research_add, research_rejected = compute_alpha_additions(
            catalyst_payload=research_payload,
            industry_by_symbol=research_industries,
            top_industries=top10,
            score_min=RESEARCH_SCORE_MIN,
        )
        # Attention budget: cap research-channel additions per run (best
        # scores first — payload is already sorted descending).
        research_add = research_add[:RESEARCH_MAX_CANDIDATES]
    except Exception as exc:  # noqa: BLE001
        logger.warning("research channel candidate build failed: %s", exc)
        research_rejected = {"research_channel_error": 1}
    alpha_add = alpha_add + research_add
    meta["researchCandidates"] = len(research_add)
    meta["researchRejected"] = research_rejected

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


def get_automation_runs(limit: int = 10) -> list[dict[str, Any]]:
    """Recent acknowledged runs (one per trade_date, newest first) for the
    TIP-002 N-day funnel history table."""
    from data_sync_service.db.watchlist_automation import list_recent_runs

    return list_recent_runs(limit=limit)


def ack_automation_run(
    run_id: str,
    screener_added: int | None = None,
    funnel: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    from data_sync_service.db.watchlist_automation import ack_run

    return ack_run(run_id, screener_added=screener_added, funnel=funnel)


def get_automation_run(run_id: str) -> dict[str, Any] | None:
    return get_run_by_id(run_id)

# ---------------------------------------------------------------------------
# RS whole-market rank (S-2 / OPT-073): 20-day return percentile vs ALL stocks
# ---------------------------------------------------------------------------

_rs_rank_cache: dict[str, float] = {}
_rs_rank_cache_date: str | None = None
_rs_rank_cache_lock = threading.Lock()


def compute_rs_ranks(symbols: list[str], as_of_date: str | None = None) -> dict[str, float]:
    """Return {symbol: percentile} of 20-day relative strength vs ALL stocks.

    Percentile 0-1 (strongest = 1.0). Ranking pool = every stock with a bar
    on the as-of date; benchmark subtraction is skipped (constant shift does
    not change ranks). Results cached per as-of date.
    """
    global _rs_rank_cache, _rs_rank_cache_date
    from data_sync_service.service.trendok import _symbol_to_ts_code

    resolved: dict[str, str] = {}
    for sym in symbols:
        parsed = _symbol_to_ts_code(str(sym))
        if parsed:
            resolved[str(sym)] = parsed[2]

    # resolve as-of: latest trade date in daily <= as_of_date
    with get_connection() as conn:
        with conn.cursor() as cur:
            if as_of_date:
                cur.execute(
                    "SELECT MAX(trade_date) FROM daily WHERE trade_date <= %s",
                    (as_of_date,),
                )
            else:
                cur.execute("SELECT MAX(trade_date) FROM daily")
            latest = cur.fetchone()
            latest = str(latest[0]) if latest and latest[0] else None
    if not latest:
        return {}
    with _rs_rank_cache_lock:
        if _rs_rank_cache_date == latest:
            pass
        else:
            # rank all stocks on `latest` by 20d return
            rows: list[tuple[str, float]] = []
            with get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT ts_code, ret20 FROM (
                            SELECT ts_code, close,
                                (close / lag(close, 20) OVER (PARTITION BY ts_code ORDER BY trade_date) - 1) * 100 AS ret20
                            FROM daily
                            WHERE trade_date <= %s
                              AND trade_date >= (
                                  SELECT MIN(trade_date) FROM (
                                      SELECT DISTINCT trade_date FROM daily
                                      WHERE trade_date <= %s ORDER BY trade_date DESC LIMIT 21
                                  ) w
                              )
                              AND close > 0
                        ) t
                        WHERE ret20 IS NOT NULL
                        """,
                        (latest, latest),
                    )
                    rows = cur.fetchall()
            ranked = sorted(rows, key=lambda kv: -kv[1])
            total = len(ranked)
            pos: dict[str, float] = {}
            for i, (ts, _ret) in enumerate(ranked, start=1):
                pos[ts] = (total - i + 1) / total if total else 0.0
            _rs_rank_cache.clear()
            _rs_rank_cache.update(pos)
            _rs_rank_cache_date = latest

    out: dict[str, Any] = {}
    for sym, ts in resolved.items():
        pct = _rs_rank_cache.get(ts)
        if pct is not None:
            out[sym] = pct
    out["_asOf"] = latest  # sentinel key stripped by the caller
    return out
