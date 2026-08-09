#!/usr/bin/env python3
"""Build theme → industry map from historical Alpha Radar trends (TIP-009).

The mapping is purely data-driven — we look at every (macro_theme, cnSymbol)
pair ever produced, join to ``stock_eastmoney_industry`` for the stock's
real industry board, and pick the industries that account for ≥70% of the
theme's mapped symbols. No LLM, no hand-curated seed list.

Output is committed to ``data/seed/theme_industry_map.json`` so the
production code never recomputes it. Quarterly re-run is sufficient.

Usage:
    PYTHONPATH=src python scripts/build_theme_industry_map.py
    PYTHONPATH=src python scripts/build_theme_industry_map.py --dry-run
    PYTHONPATH=src python scripts/build_theme_industry_map.py --output data/seed/theme_industry_map.json
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from pathlib import Path

from data_sync_service.db.alpha_radar import fetch_trends
from data_sync_service.db.stock_eastmoney_industry import lookup_by_ts_codes

DEFAULT_OUTPUT = "data/seed/theme_industry_map.json"
DEFAULT_LOOKBACK_DAYS = 90
INDUSTRY_COVERAGE_THRESHOLD = 0.7
MIN_SYMBOL_COUNT = 3  # need at least 3 mapped symbols to trust a theme


def _to_ticker(symbol: str) -> str | None:
    text = str(symbol or "").strip().upper()
    if text.startswith("CN:"):
        text = text[3:]
    digits = "".join(ch for ch in text if ch.isdigit())
    return digits if len(digits) == 6 else None


def _ts_code_from_ticker(ticker: str) -> str:
    # Karios stores ts_code as {ticker}.SH or .SZ; default to .SH for 6xxxxx,
    # .SZ otherwise. This is a heuristic — eastmoney_industry lookup tolerates
    # miss, the QA logic treats unmapped symbols as no-signal.
    return f"{ticker}.SH" if ticker.startswith("6") else f"{ticker}.SZ"


def build_theme_industry_map(
    *,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    min_symbol_count: int = MIN_SYMBOL_COUNT,
    coverage_threshold: float = INDUSTRY_COVERAGE_THRESHOLD,
) -> dict[str, object]:
    """Scan recent Alpha Radar trends and group each macro_theme's cnSymbols by
    their east-money industry board.

    Returns a dict with three keys:
      - ``themes``: {macro_theme: [industry_name, ...]} — only themes with ≥3
        mapped symbols AND a clear dominant industry (≥70% coverage) make the
        cut. Used by auto_qa_alpha_mapping to penalize wrong-industry mappings.
      - ``unmapped_themes``: [macro_theme, ...] — themes with too few mapped
        symbols or no clear industry. Default penalty 0 (no signal).
      - ``stats``: coverage/coverage stats for the run (debug).
    """
    cutoff = (datetime.now(UTC) - timedelta(days=lookback_days)).isoformat()

    themes_to_industries: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    themes_total: dict[str, int] = defaultdict(int)

    _, trends = fetch_trends(limit=200, since=cutoff, max_age_days=lookback_days)

    all_tickers: set[str] = set()
    for t in trends:
        theme = str(t.get("macroTheme") or t.get("trendName") or "").strip()
        if not theme:
            continue
        cn_symbols = t.get("cnSymbols") or []
        if not isinstance(cn_symbols, list):
            continue
        for cn in cn_symbols:
            if not isinstance(cn, dict):
                continue
            sym = cn.get("symbol")
            ticker = _to_ticker(str(sym or ""))
            if ticker:
                all_tickers.add(ticker)
                themes_total[theme] += 1

    if not all_tickers:
        return {"themes": {}, "unmapped_themes": [], "stats": {"lookback_days": lookback_days, "mappedTrends": 0}}

    industry_by_ticker = lookup_by_ts_codes([_ts_code_from_ticker(t) for t in all_tickers])
    ticker_to_industry: dict[str, str] = {}
    for ts_code, industry in industry_by_ticker.items():
        ticker = ts_code.split(".")[0]
        if ticker:
            ticker_to_industry[ticker] = industry

    for t in trends:
        theme = str(t.get("macroTheme") or t.get("trendName") or "").strip()
        if not theme:
            continue
        cn_symbols = t.get("cnSymbols") or []
        for cn in cn_symbols:
            if not isinstance(cn, dict):
                continue
            ticker = _to_ticker(str(cn.get("symbol") or ""))
            industry = ticker_to_industry.get(ticker or "")
            if industry:
                themes_to_industries[theme][industry] += 1

    themes: dict[str, list[str]] = {}
    unmapped: list[str] = []
    for theme, ind_counts in themes_to_industries.items():
        total = sum(ind_counts.values())
        if total < min_symbol_count:
            unmapped.append(theme)
            continue
        dominant = [ind for ind, cnt in ind_counts.items() if cnt / total >= coverage_threshold]
        if not dominant:
            unmapped.append(theme)
            continue
        themes[theme] = sorted(dominant)

    stats = {
        "lookback_days": lookback_days,
        "mappedTrends": len(trends),
        "themesCovered": len(themes),
        "themesUnmapped": len(unmapped),
        "coveragePct": round(100.0 * len(themes) / max(1, len(themes) + len(unmapped)), 1),
        "minSymbolCount": min_symbol_count,
        "coverageThreshold": coverage_threshold,
        "generatedAt": datetime.now(UTC).isoformat(),
    }

    return {"themes": themes, "unmapped_themes": sorted(unmapped), "stats": stats}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Print stats, do not write file")
    parser.add_argument("--output", default=DEFAULT_OUTPUT, help="Output JSON path")
    parser.add_argument("--lookback-days", type=int, default=DEFAULT_LOOKBACK_DAYS)
    parser.add_argument("--min-symbol-count", type=int, default=MIN_SYMBOL_COUNT)
    parser.add_argument(
        "--coverage-threshold",
        type=float,
        default=INDUSTRY_COVERAGE_THRESHOLD,
        help="Min share of a theme's symbols that must land on one industry",
    )
    args = parser.parse_args()

    result = build_theme_industry_map(
        lookback_days=args.lookback_days,
        min_symbol_count=args.min_symbol_count,
        coverage_threshold=args.coverage_threshold,
    )

    stats = result.get("stats") or {}
    print(f"Lookback: {args.lookback_days}d, trends scanned: {stats.get('mappedTrends', 0)}")
    print(f"Themes covered: {stats.get('themesCovered', 0)}")
    print(f"Themes unmapped: {stats.get('themesUnmapped', 0)}")
    print(f"Coverage: {stats.get('coveragePct', 0)}%")
    print()

    themes = result.get("themes") or {}
    if themes:
        print(f"Top {min(15, len(themes))} themes by industry coverage:")
        for theme, inds in list(themes.items())[:15]:
            print(f"  {theme}: {', '.join(inds)}")
        if len(themes) > 15:
            print(f"  ... and {len(themes) - 15} more")
    else:
        print("No themes covered (insufficient history).")

    if args.dry_run:
        return 0

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "themes": themes,
        "unmapped_themes": result.get("unmapped_themes") or [],
        "stats": stats,
    }
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2))
    print(f"\nWrote {output_path} ({output_path.stat().st_size} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main())