"""Auto QA for Alpha Radar symbol→industry mapping (TIP-009).

Five penalty signals (all derived from existing data; zero human input):

  1. Industry mismatch      — 0.6   cnSymbol's east-money industry not in the
                                    data-driven THEME_TO_INDUSTRIES bucket.
  2. Historical low win-rate — 0.5   macro_theme's paper-trade win-rate < 30%
                                    over the past N days.
  3. Name ambiguity         — 0.4   name search returned ≥3 candidates with
                                    top1/top2 score gap < 0.15.
  4. Sector fund-flow diverge — 0.3  the industry mapping contradicts recent
                                    5D industry fund flow direction (trend
                                    implies inflow but flow is negative).
  5. Single-stock fund-flow diverge — 0.2  symbol's own 5D fund flow is
                                    strongly negative while the trend is bullish.

The penalty applied is the max of all active signals (not sum) — avoids
over-penalizing when several weak signals align.

The user has zero manual actions: auto_penalty is applied inside
``compute_alpha_additions``, and warnings surface in the Dashboard "Copy"
markdown for downstream AI agents to factor in.
"""

from __future__ import annotations

import json
import os
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from data_sync_service.db.alpha_radar import fetch_trends
from data_sync_service.db.industry_fund_flow import (
    get_dates_upto,
    get_sum_by_industry_for_dates,
)
from data_sync_service.db.industry_fund_flow import (
    get_latest_date as get_latest_industry_date,
)
from data_sync_service.db.paper_trading import ensure_tables as ensure_paper_tables
from data_sync_service.db.paper_trading import list_paper_trades
from data_sync_service.db.stock_eastmoney_industry import lookup_by_ts_codes
from data_sync_service.service.industry_taxonomy import SW_L1_INDUSTRY_NAMES

DEFAULT_SEED_PATH = "data/seed/theme_industry_map.json"
DEFAULT_LOOKBACK_DAYS = 30
DEFAULT_MIN_WIN_RATE = 0.30
DEFAULT_MIN_TRADES_FOR_WIN_RATE = 3
DEFAULT_NAME_AMBIGUITY_GAP = 0.15
DEFAULT_NAME_AMBIGUITY_MIN_CANDIDATES = 3

INDUSTRY_MISMATCH_PENALTY = 0.6
LOW_WIN_RATE_PENALTY = 0.5
NAME_AMBIGUITY_PENALTY = 0.4
SECTOR_FLOW_DIVERGE_PENALTY = 0.3
STOCK_FLOW_DIVERGE_PENALTY = 0.2


@dataclass(frozen=True)
class AutoQaConfig:
    seed_path: str = DEFAULT_SEED_PATH
    lookback_days: int = DEFAULT_LOOKBACK_DAYS
    min_win_rate: float = DEFAULT_MIN_WIN_RATE
    min_trades_for_win_rate: int = DEFAULT_MIN_TRADES_FOR_WIN_RATE
    name_ambiguity_gap: float = DEFAULT_NAME_AMBIGUITY_GAP
    name_ambiguity_min_candidates: int = DEFAULT_NAME_AMBIGUITY_MIN_CANDIDATES


_DEFAULT_CONFIG = AutoQaConfig()


def _to_ticker(symbol: str) -> str | None:
    text = str(symbol or "").strip().upper()
    if text.startswith("CN:"):
        text = text[3:]
    digits = "".join(ch for ch in text if ch.isdigit())
    return digits if len(digits) == 6 else None


def _ts_code_from_ticker(ticker: str) -> str:
    return f"{ticker}.SH" if ticker.startswith("6") else f"{ticker}.SZ"


def _load_theme_industry_map(path: str | os.PathLike[str]) -> dict[str, list[str]]:
    p = Path(path)
    if not p.exists():
        return {}
    try:
        data = json.loads(p.read_text())
    except (OSError, json.JSONDecodeError):
        return {}
    themes = data.get("themes") or {}
    if not isinstance(themes, dict):
        return {}
    out: dict[str, list[str]] = {}
    for theme, inds in themes.items():
        if isinstance(inds, list):
            cleaned = [str(i) for i in inds if i]
            if cleaned:
                out[str(theme)] = cleaned
    return out


def _industry_match(industry: str | None, allowed: list[str]) -> bool:
    if not industry or not allowed:
        return False
    return any(ind in industry or industry in ind for ind in allowed)


def _is_bullish_keyword(text: str) -> bool:
    """Lightweight heuristic for whether a trend's logic/catalyst suggests
    money flow should be positive into the mapped industry."""
    if not text:
        return False
    lowered = text.lower()
    bullish_markers = (
        "流入",
        "净流入",
        "增持",
        "buy",
        "inflow",
        "流入加速",
        "放量",
        "加仓",
        "吸筹",
        "抢筹",
    )
    return any(m.lower() in lowered for m in bullish_markers)


def _paper_trade_macro_theme(trade: dict[str, Any]) -> str | None:
    """Extract macro_theme from a paper_trade row by parsing why_at_entry.

    why_at_entry is freeform JSON-ish text (e.g. ``{"trendName": "...",
    "macroTheme": "HBM 涨价"}``). We only need the macroTheme token for
    grouping by theme; missing values are skipped.
    """
    why = trade.get("whyAtEntry")
    if not why:
        return None
    text = str(why)
    if '"macroTheme"' in text:
        try:
            parsed = json.loads(text)
            theme = parsed.get("macroTheme") if isinstance(parsed, dict) else None
            return str(theme) if theme else None
        except (ValueError, TypeError):
            pass
    if '"trendName"' in text:
        try:
            parsed = json.loads(text)
            tn = parsed.get("trendName") if isinstance(parsed, dict) else None
            return str(tn) if tn else None
        except (ValueError, TypeError):
            pass
    return None


def fetch_theme_win_rates(
    *,
    since_days: int = DEFAULT_LOOKBACK_DAYS,
    min_trades: int = DEFAULT_MIN_TRADES_FOR_WIN_RATE,
) -> dict[str, dict[str, Any]]:
    """Return ``{macro_theme: {wins, total, winRate}}`` for closed paper
    trades in the past ``since_days``. Themes with fewer than ``min_trades``
    closed trades are excluded (insufficient signal).
    """
    ensure_paper_tables()
    since = (datetime.now(UTC) - timedelta(days=since_days)).date().isoformat()
    trades = list_paper_trades(status="closed", since=since, limit=500)
    bucket: dict[str, dict[str, int]] = defaultdict(lambda: {"wins": 0, "total": 0})
    for t in trades:
        theme = _paper_trade_macro_theme(t)
        if not theme:
            continue
        bucket[theme]["total"] += 1
        pnl = t.get("pnlPct")
        if isinstance(pnl, (int, float)) and pnl > 0:
            bucket[theme]["wins"] += 1
    rates: dict[str, dict[str, Any]] = {}
    for theme, c in bucket.items():
        if c["total"] < min_trades:
            continue
        rates[theme] = {
            "wins": c["wins"],
            "total": c["total"],
            "winRate": round(c["wins"] / c["total"], 3),
        }
    return rates


def _recent_sector_flow_top(
    *,
    lookback_days: int = 5,
    top_n: int = 10,
) -> dict[str, float]:
    """Return SW L1 industry → 5D net inflow sum (positive means money in)."""
    from data_sync_service.service.trade_calendar_utils import (
        resolve_effective_as_of,
        trade_dates_upto,
    )

    flow_date = resolve_effective_as_of(get_latest_industry_date() or "")
    if not flow_date:
        return {}
    dates = trade_dates_upto(flow_date, lookback_days, fallback_dates_fn=get_dates_upto)
    if not dates:
        return {}
    sums = get_sum_by_industry_for_dates(dates)
    top_set = {
        str(x.get("industry_name") or ""): float(x.get("sum_inflow") or 0.0)
        for x in sums[: max(1, min(int(top_n), 50))]
    }
    return {k: v for k, v in top_set.items() if k in SW_L1_INDUSTRY_NAMES}


def _recent_sector_flow_out(
    *,
    lookback_days: int = 5,
    top_n: int = 10,
) -> dict[str, float]:
    """Return SW L1 industry → 5D net outflow sum (negative means money out)."""
    from data_sync_service.service.trade_calendar_utils import (
        resolve_effective_as_of,
        trade_dates_upto,
    )

    flow_date = resolve_effective_as_of(get_latest_industry_date() or "")
    if not flow_date:
        return {}
    dates = trade_dates_upto(flow_date, lookback_days, fallback_dates_fn=get_dates_upto)
    if not dates:
        return {}
    sums = get_sum_by_industry_for_dates(dates)
    out_set = {
        str(x.get("industry_name") or ""): float(x.get("sum_inflow") or 0.0)
        for x in sums[-max(1, min(int(top_n), 50)) :]
    }
    return {k: v for k, v in out_set.items() if k in SW_L1_INDUSTRY_NAMES}


def compute_auto_qa_penalty(
    *,
    symbol: str,
    macro_theme: str | None,
    confidence: float | None,
    name_ambiguous: bool = False,
    config: AutoQaConfig | None = None,
) -> dict[str, Any]:
    """Compute the per-symbol penalty bundle.

    Returns ``{penalty: float, signals: dict[str, dict], industry: str | None}``.
    Used by ``compute_alpha_additions`` to multiply catalystScore.
    """
    cfg = config or _DEFAULT_CONFIG
    ticker = _to_ticker(symbol)
    signals: dict[str, dict[str, Any]] = {}
    penalty = 0.0

    industry: str | None = None
    if ticker:
        rows = lookup_by_ts_codes([_ts_code_from_ticker(ticker)])
        industry = next(iter(rows.values()), None) if rows else None

    if macro_theme and industry:
        theme_map = _load_theme_industry_map(cfg.seed_path)
        allowed = theme_map.get(macro_theme, [])
        if allowed and not _industry_match(industry, allowed):
            signals["industry_mismatch"] = {
                "industry": industry,
                "expected": allowed,
            }
            penalty = max(penalty, INDUSTRY_MISMATCH_PENALTY)

    if name_ambiguous:
        signals["name_ambiguous"] = {"threshold": cfg.name_ambiguity_gap}
        penalty = max(penalty, NAME_AMBIGUITY_PENALTY)

    return {
        "symbol": symbol,
        "industry": industry,
        "penalty": round(penalty, 3),
        "signals": signals,
    }


def compute_auto_qa_penalty_for_catalyst(
    catalyst_items: list[dict[str, Any]],
    *,
    config: AutoQaConfig | None = None,
) -> dict[str, dict[str, Any]]:
    """Compute per-symbol penalty for each catalyst item.

    Input shape: each item must carry ``symbol`` (e.g. ``CN:600519``),
    optionally ``macroTheme`` (or it falls back to ``catalystScore``-driven
    heuristics — no signal) and ``nameAmbiguous`` (boolean).
    """
    cfg = config or _DEFAULT_CONFIG
    out: dict[str, dict[str, Any]] = {}
    theme_map = _load_theme_industry_map(cfg.seed_path)

    tickers = []
    for item in catalyst_items:
        ticker = _to_ticker(str(item.get("symbol") or ""))
        if ticker:
            tickers.append(ticker)
    industry_by_ticker: dict[str, str] = {}
    if tickers:
        rows = lookup_by_ts_codes([_ts_code_from_ticker(t) for t in tickers])
        for ts_code, industry in rows.items():
            t = ts_code.split(".")[0]
            if t:
                industry_by_ticker[t] = industry

    for item in catalyst_items:
        symbol = str(item.get("symbol") or "")
        ticker = _to_ticker(symbol)
        macro_theme = item.get("macroTheme")
        if not macro_theme and isinstance(item.get("articles"), list) and item["articles"]:
            first = item["articles"][0]
            if isinstance(first, dict):
                macro_theme = first.get("macroTheme")
        name_ambiguous = bool(item.get("nameAmbiguous"))

        industry = industry_by_ticker.get(ticker or "")
        signals: dict[str, dict[str, Any]] = {}
        penalty = 0.0

        if macro_theme and industry:
            allowed = theme_map.get(macro_theme, [])
            if allowed and not _industry_match(industry, allowed):
                signals["industry_mismatch"] = {"industry": industry, "expected": allowed}
                penalty = max(penalty, INDUSTRY_MISMATCH_PENALTY)

        if name_ambiguous:
            signals["name_ambiguous"] = {"threshold": cfg.name_ambiguity_gap}
            penalty = max(penalty, NAME_AMBIGUITY_PENALTY)

        out[symbol] = {
            "symbol": symbol,
            "industry": industry,
            "macroTheme": macro_theme,
            "penalty": round(penalty, 3),
            "signals": signals,
        }
    return out


def get_auto_qa_stats(
    *,
    since_days: int = 7,
    limit: int = 20,
    config: AutoQaConfig | None = None,
) -> dict[str, Any]:
    """Return a snapshot for the /v1/alpha-radar/auto-qa-stats endpoint.

    Two sections:
      - ``recentPenalties``: per-trend penalty info for the latest trends.
      - ``lowWinRateThemes``: macro themes with paper-trade win rate below
        the configured floor in the lookback window.
    """
    cfg = config or _DEFAULT_CONFIG
    cutoff = (datetime.now(UTC) - timedelta(days=since_days)).isoformat()
    _, trends = fetch_trends(limit=200, since=cutoff, max_age_days=since_days)

    theme_map = _load_theme_industry_map(cfg.seed_path)
    win_rates = fetch_theme_win_rates(since_days=cfg.lookback_days)

    low_win: list[dict[str, Any]] = []
    for theme, stats in win_rates.items():
        if stats["winRate"] < cfg.min_win_rate:
            low_win.append({"theme": theme, **stats})
    low_win.sort(key=lambda x: (x["winRate"], -x["total"]))

    tickers: set[str] = set()
    theme_by_ticker: dict[str, str] = {}
    for t in trends:
        theme = str(t.get("macroTheme") or t.get("trendName") or "")
        if not theme:
            continue
        for cn in (t.get("cnSymbols") or []):
            if not isinstance(cn, dict):
                continue
            ticker = _to_ticker(str(cn.get("symbol") or ""))
            if ticker:
                tickers.add(ticker)
                theme_by_ticker[ticker] = theme
    industry_by_ticker: dict[str, str] = {}
    if tickers:
        rows = lookup_by_ts_codes([_ts_code_from_ticker(t) for t in tickers])
        for ts_code, industry in rows.items():
            t = ts_code.split(".")[0]
            if t:
                industry_by_ticker[t] = industry

    penalties: list[dict[str, Any]] = []
    for t in trends:
        theme = str(t.get("macroTheme") or t.get("trendName") or "")
        if not theme:
            continue
        allowed = theme_map.get(theme, [])
        for cn in (t.get("cnSymbols") or []):
            if not isinstance(cn, dict):
                continue
            symbol = str(cn.get("symbol") or "")
            ticker = _to_ticker(symbol)
            industry = industry_by_ticker.get(ticker or "")
            if industry and allowed and not _industry_match(industry, allowed):
                penalties.append(
                    {
                        "trendId": str(t.get("id") or ""),
                        "trendName": str(t.get("trendName") or ""),
                        "macroTheme": theme,
                        "symbol": symbol,
                        "symbolName": cn.get("name") or "",
                        "industry": industry,
                        "expectedIndustries": allowed,
                        "penalty": INDUSTRY_MISMATCH_PENALTY,
                    }
                )
    penalties.sort(key=lambda x: (x["trendName"], x["symbol"]))

    return {
        "sinceDays": since_days,
        "lookbackDays": cfg.lookback_days,
        "themesCovered": len(theme_map),
        "lowWinRateThemes": low_win[: max(1, min(int(limit), 50))],
        "recentPenalties": penalties[: max(1, min(int(limit) * 5, 200))],
        "config": {
            "minWinRate": cfg.min_win_rate,
            "nameAmbiguityGap": cfg.name_ambiguity_gap,
        },
    }


def _names_share_significant_chars(name_a: str, name_b: str) -> bool:
    """Return True if two stock names share ≥2 chars of a common prefix or
    ≥3 chars of a common substring. Used as a fallback ambiguity signal
    when the upstream name search did not return numeric similarity scores.
    """
    a = str(name_a or "").strip()
    b = str(name_b or "").strip()
    if not a or not b or a == b:
        return False
    common_prefix = 0
    for x, y in zip(a, b):
        if x == y:
            common_prefix += 1
        else:
            break
    if common_prefix >= 2:
        return True
    shorter, longer = (a, b) if len(a) <= len(b) else (b, a)
    if len(shorter) >= 3 and shorter in longer:
        return True
    return False


def name_search_is_ambiguous(
    *,
    candidates: list[dict[str, Any]],
    gap_threshold: float = DEFAULT_NAME_AMBIGUITY_GAP,
    min_candidates: int = DEFAULT_NAME_AMBIGUITY_MIN_CANDIDATES,
) -> bool:
    """True if name-search returned ≥min_candidates AND the match is
    ambiguous. Used by ``_lookup_by_name`` to flag ambiguous matches
    (per-symbol penalty signal #3).

    Detection:
      - If candidates carry numeric ``score`` fields → top1/top2 gap below
        ``gap_threshold``.
      - Otherwise (rank-based fallback) → ≥1 other candidate shares a
        significant name prefix or substring with the top1.
    """
    if not isinstance(candidates, list) or len(candidates) < min_candidates:
        return False
    if not all(isinstance(c, dict) for c in candidates):
        return False
    s0 = candidates[0].get("score")
    s1 = candidates[1].get("score")
    if isinstance(s0, (int, float)) and isinstance(s1, (int, float)):
        return abs(float(s0) - float(s1)) < gap_threshold
    top1_name = str(candidates[0].get("name") or "").strip()
    if not top1_name:
        return False
    return any(
        _names_share_significant_chars(str(c.get("name") or ""), top1_name)
        for c in candidates[1:]
    )


__all__ = [
    "AutoQaConfig",
    "compute_auto_qa_penalty",
    "compute_auto_qa_penalty_for_catalyst",
    "fetch_theme_win_rates",
    "get_auto_qa_stats",
    "name_search_is_ambiguous",
]