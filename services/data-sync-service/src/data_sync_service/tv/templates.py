"""Built-in screener templates (OPT-057 Phase 5).

Templates live here as the source of truth for `mode='api'` screener
defaults — they map 1-1 with the friendly filter pills documented in
docs/modules/screener.md (Strategy Contract, TIP-006).

The filter JSON format is the request body field `filter` of TV Scanner API
(see tv/scanner_api.py and docs/designs/ego-lite-spike-2026-08.md §2).

NOTE: TV Scanner API expects `filter` to be an array of conditions (not a
dict with "and" key). Multiple conditions in the array are ANDed together.

Caveat (TODO OPT-057.x):
The spike proved the Scanner API endpoint is reachable and returns 30+ fields,
but did NOT exercise every filter expression we want. Nested arithmetic
expressions like `{left: High.Interval52Week, operation: mult, right: 0.85}`
are NOT yet validated against the live API. Templates that rely on them
are marked `# NESTED-FILTER-NOT-VALIDATED` and should be re-tested via
`scripts/preview_screener_template.py` once the docker stack is running.

When a user picks a template in SettingsPage, the template's id, name,
filter_json, and api_columns are written to tv_screeners and `mode` is set
to 'api'. The dispatcher then routes through Scanner API directly.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ScreenerTemplate:
    template_id: str
    display_name: str
    screen_title_substr: str  # for TIP-006 contract matching in catalyst code
    market: str  # 'cn' | 'hk' | 'us'
    filter_json: dict[str, Any] | list[dict[str, Any]]
    api_columns: list[str]
    description: str
    nested_filter_validated: bool  # False = filter not yet exercised against live API


# Default columns for new template registrations (mirrors scanner_api.COLUMN_MAP values).
# NOTE: High.Interval52Week returns null for most stocks and is NOT usable in
# filters — it's only included here for downstream TrendOK processing.
_DEFAULT_API_COLUMNS: list[str] = [
    "name",          # Symbol / Ticker
    "description",   # Name
    "close",         # Price
    "change",        # Change %
    "volume",        # Volume
    "market_cap_basic",  # Market Cap
    "sector",
    "industry",
    "country",
    "price_earnings_ttm",  # P/E
    "RSI",
    "MACD.macd",
    "High.Interval52Week",  # High 52W (for downstream TrendOK)
]


def _karios_pullback_filter_cn() -> list[dict[str, Any]]:
    """Karios Pullback v3 — A 股主合同 (TIP-006).

    Strategy Contract: 趋势回踩 5-15%，市值 ≥ 30B，EMA 多头，RSI 45-75。

    Scanner API pre-filter (coarse — TrendOK handles fine-grained EMA
    crossover + pullback window + pullback_pct downstream):

    - exchange ∈ [SSE, SZSE] (A 股 only)
    - market_cap_basic ≥ 30B (large-cap universe)
    - price_earnings_ttm > 0 (exclude negative-PE / loss-making)
    - RSI ∈ [45, 75] (not overbought, not oversold)

    NOTE: TV Scanner API expects `filter` to be an array of conditions.
    Multiple conditions in the array are ANDed together.
    """
    return [
        {"left": "exchange", "operation": "in_range", "right": ["SSE", "SZSE"]},
        {"left": "market_cap_basic", "operation": "greater", "right": 30_000_000_000},
        {"left": "price_earnings_ttm", "operation": "greater", "right": 0},
        {"left": "RSI", "operation": "in_range", "right": [45, 75]},
    ]


def _karios_pullback_filter_hk() -> list[dict[str, Any]]:
    """Karios Pullback v3 (HK) — 港股主合同。

    - exchange = HKEX (港股 only)
    - market_cap_basic ≥ 30B
    - price_earnings_ttm > 0
    - RSI ∈ [45, 75]
    """
    return [
        {"left": "exchange", "operation": "equal", "right": "HKEX"},
        {"left": "market_cap_basic", "operation": "greater", "right": 30_000_000_000},
        {"left": "price_earnings_ttm", "operation": "greater", "right": 0},
        {"left": "RSI", "operation": "in_range", "right": [45, 75]},
    ]


def _karios_pullback_filter_us() -> list[dict[str, Any]]:
    """Karios Pullback v3 (US) — 美股主合同。

    - exchange ∈ [NASDAQ, NYSE, AMEX] (美股 only)
    - market_cap_basic ≥ 30B
    - price_earnings_ttm > 0
    - RSI ∈ [45, 75]
    """
    return [
        {"left": "exchange", "operation": "in_range", "right": ["NASDAQ", "NYSE", "AMEX"]},
        {"left": "market_cap_basic", "operation": "greater", "right": 30_000_000_000},
        {"left": "price_earnings_ttm", "operation": "greater", "right": 0},
        {"left": "RSI", "operation": "in_range", "right": [45, 75]},
    ]


def _falcon_launch_filter_cn() -> list[dict[str, Any]]:
    """Falcon Launch v2 — momentum / 主线动量日内 (TIP-007).

    - exchange ∈ [SSE, SZSE] (A 股 only)
    - market_cap_basic ≥ 10B
    - MACD > 0 (positive momentum)
    - RSI < 80 (not overbought)
    """
    return [
        {"left": "exchange", "operation": "in_range", "right": ["SSE", "SZSE"]},
        {"left": "market_cap_basic", "operation": "greater", "right": 10_000_000_000},
        {"left": "MACD.macd", "operation": "greater", "right": 0},
        {"left": "RSI", "operation": "less", "right": 80},
    ]


def _industry_top5_filter_cn() -> list[dict[str, Any]]:
    """Industry Top5 Fallback — TIP-003 empty-window fallback.

    - exchange ∈ [SSE, SZSE] (A 股 only)
    - market_cap_basic ≥ 20B
    - RSI ∈ [50, 90] (uptrend, not overbought)
    """
    return [
        {"left": "exchange", "operation": "in_range", "right": ["SSE", "SZSE"]},
        {"left": "market_cap_basic", "operation": "greater", "right": 20_000_000_000},
        {"left": "RSI", "operation": "in_range", "right": [50, 90]},
    ]


SCREENER_TEMPLATES: tuple[ScreenerTemplate, ...] = (
    ScreenerTemplate(
        template_id="karios_pullback_v3_cn",
        display_name="Karios Pullback v3 (CN)",
        screen_title_substr="karios pullback",
        market="cn",
        filter_json=_karios_pullback_filter_cn(),
        api_columns=list(_DEFAULT_API_COLUMNS),
        description="A 股主合同 — 趋势回踩，市值 ≥30B、PE>0、RSI 45-75（TIP-006）。",
        nested_filter_validated=True,
    ),
    ScreenerTemplate(
        template_id="karios_pullback_v3_hk",
        display_name="Karios Pullback v3 (HK)",
        screen_title_substr="karios pullback",
        market="hk",
        filter_json=_karios_pullback_filter_hk(),
        api_columns=list(_DEFAULT_API_COLUMNS),
        description="港股主合同 — 同 CN 逻辑，仅 universe 限制为 HK（TV API: country='China'）。",
        nested_filter_validated=True,
    ),
    ScreenerTemplate(
        template_id="karios_pullback_v3_us",
        display_name="Karios Pullback v3 (US)",
        screen_title_substr="karios pullback",
        market="us",
        filter_json=_karios_pullback_filter_us(),
        api_columns=list(_DEFAULT_API_COLUMNS),
        description="美股主合同 — 同 CN 逻辑，仅 universe 限制为 US（TV API: country='United States'）。",
        nested_filter_validated=True,
    ),
    ScreenerTemplate(
        template_id="falcon_launch_v2_cn",
        display_name="Falcon Launch v2 (CN)",
        screen_title_substr="falcon launch",
        market="cn",
        filter_json=_falcon_launch_filter_cn(),
        api_columns=list(_DEFAULT_API_COLUMNS),
        description="A 股动量 — EMA 多头、MACD > 0、RSI < 80（TIP-007）。",
        nested_filter_validated=True,
    ),
    ScreenerTemplate(
        template_id="industry_top5_fallback_cn",
        display_name="Industry Top5 Fallback (CN)",
        screen_title_substr="industry top5",
        market="cn",
        filter_json=_industry_top5_filter_cn(),
        api_columns=list(_DEFAULT_API_COLUMNS),
        description="TIP-003 空窗降级 — 市值 ≥20B、Close > EMA60、RSI 50-90。",
        nested_filter_validated=True,
    ),
)


def get_template(template_id: str) -> ScreenerTemplate | None:
    for t in SCREENER_TEMPLATES:
        if t.template_id == template_id:
            return t
    return None


def list_templates() -> tuple[ScreenerTemplate, ...]:
    return SCREENER_TEMPLATES


__all__ = [
    "SCREENER_TEMPLATES",
    "ScreenerTemplate",
    "get_template",
    "list_templates",
]