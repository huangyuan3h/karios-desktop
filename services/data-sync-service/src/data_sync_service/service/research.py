"""Research report → Alpha channel (研报 → α 通道).

Pipeline:
  1. sync_research_reports()   — pull recent sell-side reports from East
                                 Money report center (reportapi.eastmoney.com)
                                 into research_reports (dedup by info_code).
  2. compute_report_score()    — deterministic rating/target-price score.
  3. build_research_catalyst_payload() — aggregate per symbol over a window,
                                 shaped like Alpha Radar's catalyst payload so
                                 watchlist_automation.compute_alpha_additions
                                 can reuse the exact TIP-004 entry gates.

Scoring (14-day half-life, same decay shape as Alpha Radar catalyst):
  score = (rating_base × 80 + target_space × 20) × recency
  rating_base: 买入=1.0 增持=0.75 中性=0.5 减持=0.2 卖出=0.1 (unknown=0.5)
  target_space: clamp(target_price/current − 1, 0, 0.5) / 0.5  (0..20 pts)
  → a fresh 买入 report alone scores 80; 增持 scores 60 (below the 70
    research entry floor, intentionally — only strong ratings reach the pool).
Per-symbol aggregation adds +5 per extra confirming report (cap +10).
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Any

from data_sync_service.service.em_push2_http import em_get_json

logger = logging.getLogger(__name__)

REPORT_API_URL = "https://reportapi.eastmoney.com/report/list"
REPORT_API_REFERER = "https://data.eastmoney.com/report/zw_stock.jshtml"

RESEARCH_SCORE_MIN = 70.0
RESEARCH_MAX_CANDIDATES = 10
RESEARCH_WINDOW_DAYS = 14
RESEARCH_HALF_LIFE_DAYS = 14.0
RATING_BASE = {
    "买入": 1.0,
    "增持": 0.75,
    "中性": 0.5,
    "减持": 0.2,
    "卖出": 0.1,
}
TARGET_SPACE_MAX = 0.5
CONFIRM_BONUS_PER_REPORT = 5.0
CONFIRM_BONUS_CAP = 10.0

# East Money exchanges we track. BEIJING (北交所 920xxx) is skipped: the
# CN TrendOK / paper-trade pipeline is SH/SZ oriented.
SUPPORTED_MARKETS = ("SHANGHAI", "SHENZHEN")


def _rating_base(rating: str | None) -> float:
    key = str(rating or "").strip()
    return RATING_BASE.get(key, 0.5)


def _recency(publish_date: date, today: date | None = None) -> float:
    today = today or date.today()
    days = max(0, (today - publish_date).days)
    return 0.5 ** (days / RESEARCH_HALF_LIFE_DAYS)


def compute_report_score(
    *,
    rating: str | None,
    target_price: float | None,
    current_close: float | None,
    publish_date: date,
    today: date | None = None,
) -> float:
    """Deterministic per-report alpha score, 0-100."""
    score = _rating_base(rating) * 80.0
    if (
        target_price is not None
        and current_close is not None
        and current_close > 0
        and target_price > current_close
    ):
        space = min((target_price / current_close) - 1.0, TARGET_SPACE_MAX)
        score += (space / TARGET_SPACE_MAX) * 20.0
    score *= _recency(publish_date, today)
    return round(min(max(score, 0.0), 100.0), 2)


def _symbol_from_code(stock_code: str, market: str) -> str | None:
    """CN: prefix normalized symbol; None for unsupported markets/codes."""
    code = str(stock_code or "").strip()
    if not code or len(code) != 6 or not code.isdigit():
        return None
    if market == "SHANGHAI":
        return f"CN:{code}.SH"
    if market == "SHENZHEN":
        return f"CN:{code}.SZ"
    return None


def sync_research_reports(
    *,
    days: int = 3,
    page_size: int = 100,
    max_pages: int = 3,
) -> dict[str, Any]:
    """Fetch recent East Money individual-stock reports and upsert them.

    Returns {ok, fetched, inserted, pages, error}.
    """
    from data_sync_service.db import research as db

    today = date.today()
    begin = today - timedelta(days=max(1, int(days)))
    params = {
        "industryCode": "*",
        "pageSize": int(page_size),
        "pageNo": 1,
        "qType": 0,  # 0 = individual-stock reports
        "code": "*",
        "beginTime": begin.isoformat(),
        "endTime": today.isoformat(),
        "sortType": 1,
        "sortColumn": "noticeDate",
    }
    rows: list[dict[str, Any]] = []
    fetched = 0
    error: str | None = None
    for page in range(1, max_pages + 1):
        try:
            params["pageNo"] = page
            payload = em_get_json(
                REPORT_API_URL,
                params=params,
                referer=REPORT_API_REFERER,
            )
        except Exception as exc:  # noqa: BLE001
            error = f"page {page}: {exc}"
            logger.warning("research report fetch failed (%s)", error)
            break
        data = payload.get("data") if isinstance(payload, dict) else None
        if not isinstance(data, list) or not data:
            break
        fetched += len(data)
        for r in data:
            if not isinstance(r, dict):
                continue
            market = str(r.get("market") or "").strip()
            if market not in SUPPORTED_MARKETS:
                continue
            publish = str(r.get("publishDate") or "").strip()[:10]
            if not publish:
                continue
            rows.append(
                {
                    "infoCode": r.get("infoCode"),
                    "stockCode": r.get("stockCode"),
                    "stockName": r.get("stockName"),
                    "title": r.get("title"),
                    "orgName": r.get("orgSName"),
                    "rating": r.get("emRatingName"),
                    "targetPrice": r.get("indvAimPriceT") or r.get("indvAimPriceL"),
                    "epsThisYear": r.get("predictThisYearEps"),
                    "peThisYear": r.get("predictThisYearPe"),
                    "industryName": r.get("indvInduName"),
                    "market": market,
                    "publishDate": publish,
                    "encodeUrl": r.get("encodeUrl"),
                }
            )
        if len(data) < page_size:
            break
    inserted = db.upsert_research_reports(rows) if rows else 0
    try:
        refresh_report_scores()
    except Exception as exc:  # noqa: BLE001
        logger.warning("research report score refresh failed: %s", exc)
    return {"ok": error is None and (fetched > 0 or inserted >= 0), "fetched": fetched,
            "inserted": inserted, "pages": (params["pageNo"] - 1), "error": error}


def _current_closes(symbols: list[str]) -> dict[str, float]:
    """Latest close per CN symbol from the daily table (best effort)."""
    from data_sync_service.db.daily import fetch_daily

    out: dict[str, float] = {}
    for sym in symbols:
        ts_code = sym.split(":", 1)[-1] if ":" in sym else sym
        try:
            rows = fetch_daily(ts_code=ts_code, limit=1)
            if rows and rows[0].get("close") is not None:
                out[sym] = float(rows[0]["close"])
        except Exception as exc:  # noqa: BLE001
            logger.debug("research current close lookup failed for %s: %s", sym, exc)
    return out


def build_research_catalyst_payload(
    *,
    limit: int = 100,
    window_days: int = RESEARCH_WINDOW_DAYS,
) -> dict[str, Any]:
    """Aggregate recent reports per symbol into Alpha-Radar-shaped payload.

    Items carry catalystScore (research score), articles with a pseudo
    catalystGrade 'S' (to pass compute_alpha_additions' grade-S gate) plus
    the report meta (title / rating / org / targetPrice) for downstream UI.
    """
    from data_sync_service.db import research as db

    reports = db.fetch_reports_for_score_window(window_days=window_days)
    if not reports:
        return {"stalenessBasis": "report_window", "maxAgeDays": window_days,
                "total": 0, "items": []}

    today = date.today()
    by_symbol: dict[str, list[dict[str, Any]]] = {}
    for rep in reports:
        market = str(rep.get("market") or "").strip()
        code = str(rep.get("stock_code") or "").strip()
        sym = _symbol_from_code(code, market)
        if not sym:
            continue
        by_symbol.setdefault(sym, []).append(rep)

    if not by_symbol:
        return {"stalenessBasis": "report_window", "maxAgeDays": window_days,
                "total": 0, "items": []}

    closes = _current_closes(sorted(by_symbol.keys()))
    items: list[dict[str, Any]] = []
    for sym, reps in by_symbol.items():
        current = closes.get(sym)
        scored: list[tuple[float, dict[str, Any]]] = []
        for rep in reps:
            publish = rep.get("publish_date") or today.isoformat()
            try:
                publish_date = date.fromisoformat(str(publish)[:10])
            except ValueError:
                publish_date = today
            score = compute_report_score(
                rating=rep.get("rating"),
                target_price=rep.get("target_price"),
                current_close=current,
                publish_date=publish_date,
                today=today,
            )
            scored.append((score, rep))
        if not scored:
            continue
        best_score = max(s for s, _ in scored)
        confirmed = min(len(scored) - 1, int(CONFIRM_BONUS_CAP / CONFIRM_BONUS_PER_REPORT))
        agg_score = min(best_score + confirmed * CONFIRM_BONUS_PER_REPORT, 100.0)
        best_rep = max(scored, key=lambda x: x[0])[1]
        latest = max(rep.get("publish_date") or today.isoformat() for rep in reps)
        articles = [
            {
                "catalystGrade": "S",
                "title": rep.get("title") or "",
                "rating": rep.get("rating"),
                "orgName": rep.get("org_name"),
                "targetPrice": rep.get("target_price"),
            }
            for rep in sorted(reps, key=lambda r: str(r.get("publish_date") or ""), reverse=True)
        ]
        items.append(
            {
                "symbol": sym,
                "name": best_rep.get("stock_name") or sym,
                "catalystScore": round(agg_score, 2),
                "articleCount": len(reps),
                "latestArticleAt": latest,
                "channel": "research",
                # East Money industry from the report API itself (same taxonomy
                # as the EM industry cache) — lets the entry gates resolve
                # defense/Top10 without a warm EM cache row for fresh names.
                "industryName": best_rep.get("industry_name"),
                "articles": articles,
            }
        )
    items.sort(key=lambda x: float(x["catalystScore"]), reverse=True)
    return {
        "stalenessBasis": "report_window",
        "maxAgeDays": window_days,
        "total": len(items),
        "items": items[: int(limit)],
    }


def refresh_report_scores(*, window_days: int = RESEARCH_WINDOW_DAYS) -> int:
    """Recompute and persist alpha_score for reports in the window.

    Called after every sync so the /api/research/reports list and the
    frontend always see fresh scores without re-aggregating on read.
    """
    from data_sync_service.db import research as db

    reports = db.fetch_reports_for_score_window(window_days=window_days)
    if not reports:
        return 0
    today = date.today()
    closes = _current_closes(
        [
            f"CN:{r.get('stock_code')}.{'SH' if r.get('market') == 'SHANGHAI' else 'SZ'}"
            for r in reports
            if r.get("stock_code") and r.get("market") in SUPPORTED_MARKETS
        ]
    )
    updates: list[tuple[float, int]] = []
    for rep in reports:
        if not rep.get("stock_code") or rep.get("market") not in SUPPORTED_MARKETS:
            continue
        market = "SH" if rep.get("market") == "SHANGHAI" else "SZ"
        sym = f"CN:{rep.get('stock_code')}.{market}"
        publish = rep.get("publish_date") or today.isoformat()
        try:
            publish_date = date.fromisoformat(str(publish)[:10])
        except ValueError:
            publish_date = today
        score = compute_report_score(
            rating=rep.get("rating"),
            target_price=rep.get("target_price"),
            current_close=closes.get(sym),
            publish_date=publish_date,
            today=today,
        )
        updates.append((score, int(rep["id"])))
    return db.update_report_scores(updates)


def list_research_reports(*, limit: int = 50, window_days: int = 7) -> list[dict[str, Any]]:
    from data_sync_service.db import research as db

    return db.list_recent_reports(limit=limit, window_days=window_days)


def research_stats() -> dict[str, int]:
    from data_sync_service.db import research as db

    return db.research_stats()


__all__ = [
    "RESEARCH_SCORE_MIN",
    "build_research_catalyst_payload",
    "compute_report_score",
    "list_research_reports",
    "refresh_report_scores",
    "research_stats",
    "sync_research_reports",
]
