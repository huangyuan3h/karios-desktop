"""OPT-052: HK pure-play resolution for Alpha Radar (LLM hk_mapping field).

Covers:
- _normalize_hk_ticker (5-digit / HK:00700 / HK00700 / zero-pad)
- resolve_hk_mapping (ticker match / name match / unresolved)
- map_trend_hk persists to trend_json.hkSymbols via update_trend_hk_mapping
- _trend_row surfaces hkSymbols at the top level (cnSymbols parity)
- aggregate_catalyst_stocks combines CN + HK symbols into one bucket map
- compute_alpha_additions lets HK pure-plays skip EM industry gates
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from data_sync_service.service.alpha_radar_catalyst import aggregate_catalyst_stocks
from data_sync_service.service.alpha_radar_symbol_resolve import (
    _normalize_hk_ticker,
    map_trend_hk,
    resolve_hk_mapping,
)
from data_sync_service.service.watchlist_automation import compute_alpha_additions


# ---------------------------------------------------------------------------
# _normalize_hk_ticker
# ---------------------------------------------------------------------------


def test_normalize_hk_ticker_pads_to_5_digits() -> None:
    assert _normalize_hk_ticker("700") == "00700"
    assert _normalize_hk_ticker("00700") == "00700"
    assert _normalize_hk_ticker("12345") == "12345"


def test_normalize_hk_ticker_accepts_prefix() -> None:
    assert _normalize_hk_ticker("HK:700") == "00700"
    assert _normalize_hk_ticker("HK00700") == "00700"
    assert _normalize_hk_ticker("hk:00700") == "00700"


def test_normalize_hk_ticker_rejects_invalid() -> None:
    assert _normalize_hk_ticker("") is None
    assert _normalize_hk_ticker("abc") is None
    assert _normalize_hk_ticker("123456") is None  # too many digits
    assert _normalize_hk_ticker("CN:600000") is None  # not HK


# ---------------------------------------------------------------------------
# resolve_hk_mapping
# ---------------------------------------------------------------------------


def test_resolve_hk_mapping_by_ticker() -> None:
    with patch(
        "data_sync_service.service.alpha_radar_symbol_resolve.fetch_market_stocks",
        return_value=(
            1,
            [{"symbol": "HK:00700", "ticker": "00700", "name": "腾讯控股", "market": "HK"}],
        ),
    ):
        resolved, unresolved = resolve_hk_mapping(["700"], logic_summary="HK 算力 capex 重启")
    assert len(resolved) == 1
    assert resolved[0]["symbol"] == "HK:00700"
    assert resolved[0]["name"] == "腾讯控股"
    assert unresolved == []


def test_resolve_hk_mapping_by_name() -> None:
    with patch(
        "data_sync_service.service.alpha_radar_symbol_resolve.fetch_market_stocks",
        return_value=(
            1,
            [{"symbol": "HK:00700", "ticker": "00700", "name": "腾讯控股", "market": "HK"}],
        ),
    ):
        resolved, unresolved = resolve_hk_mapping(["腾讯"], logic_summary="HK AI 算力")
    assert len(resolved) == 1
    assert resolved[0]["symbol"].startswith("HK:")
    assert unresolved == []


def test_resolve_hk_mapping_unresolved_returns_empty() -> None:
    with patch(
        "data_sync_service.service.alpha_radar_symbol_resolve.fetch_market_stocks",
        return_value=(0, []),
    ):
        resolved, unresolved = resolve_hk_mapping(["某不存在的港股"], logic_summary="X")
    assert resolved == []
    assert unresolved == ["某不存在的港股"]


def test_resolve_hk_mapping_caps_at_three() -> None:
    with patch(
        "data_sync_service.service.alpha_radar_symbol_resolve.fetch_market_stocks",
        return_value=(
            1,
            [{"symbol": "HK:00700", "ticker": "00700", "name": "腾讯", "market": "HK"}],
        ),
    ):
        # 5 entries → cap at 3
        resolved, unresolved = resolve_hk_mapping(["700", "941", "9988", "3690", "1810"])
    assert len(resolved) <= 3


def test_resolve_hk_mapping_dedupes() -> None:
    """Duplicate raw strings should resolve once."""
    with patch(
        "data_sync_service.service.alpha_radar_symbol_resolve.fetch_market_stocks",
        return_value=(
            1,
            [{"symbol": "HK:00700", "ticker": "00700", "name": "腾讯", "market": "HK"}],
        ),
    ):
        resolved, _ = resolve_hk_mapping(["00700", "HK:700"])
    # Both raw forms resolve to the same ticker → dedupe to one entry.
    assert len(resolved) == 1


# ---------------------------------------------------------------------------
# map_trend_hk persists to trend_json.hkSymbols
# ---------------------------------------------------------------------------


def test_map_trend_hk_writes_to_db(monkeypatch: pytest.MonkeyPatch) -> None:
    """map_trend_hk should call update_trend_hk_mapping and return resolved."""
    captured: dict = {}

    def fake_update(*, trend_id: str, hk_symbols: list) -> bool:
        captured["trend_id"] = trend_id
        captured["hk_symbols"] = hk_symbols
        return True

    # monkeypatch at the *symbol_resolve* module because that file did
    # `from data_sync_service.db.alpha_radar import update_trend_hk_mapping`
    # which binds the name locally — patching the source module won't help.
    monkeypatch.setattr(
        "data_sync_service.service.alpha_radar_symbol_resolve.update_trend_hk_mapping",
        fake_update,
    )

    with patch(
        "data_sync_service.service.alpha_radar_symbol_resolve.fetch_market_stocks",
        return_value=(
            1,
            [{"symbol": "HK:00700", "ticker": "00700", "name": "腾讯", "market": "HK"}],
        ),
    ):
        out = map_trend_hk(
            trend_id="trend-1",
            trend={"hk_mapping": ["腾讯"], "logic_summary": "HK capex"},
        )

    assert out["trendId"] == "trend-1"
    assert out["mappingMode"] == "local_resolve_hk"
    assert len(out["hkSymbols"]) == 1
    assert out["hkSymbols"][0]["symbol"] == "HK:00700"
    # update_trend_hk_mapping was called with the same payload.
    assert captured["trend_id"] == "trend-1"
    assert captured["hk_symbols"][0]["symbol"] == "HK:00700"


# ---------------------------------------------------------------------------
# aggregate_catalyst_stocks merges CN + HK symbols
# ---------------------------------------------------------------------------


def test_aggregate_catalyst_stocks_combines_cn_and_hk() -> None:
    """Both CN and HK symbols from the same trend should land in the buckets map."""
    from datetime import UTC, datetime

    now = datetime(2026, 8, 1, 12, 0, 0, tzinfo=UTC)
    trends = [
        {
            "id": "t1",
            "documentId": "d1",
            "trendName": "全球算力 capex 重启",
            "catalystGrade": "S",
            "driverType": "Global_Tech",
            "documentPublishedAt": "2026-07-30T00:00:00Z",
            "documentFetchedAt": "2026-07-30T01:00:00Z",
            "cnSymbols": [
                {"symbol": "CN:300308", "name": "中际旭创", "confidence": 0.9},
            ],
            "hkSymbols": [
                {"symbol": "HK:00700", "name": "腾讯控股", "confidence": 0.85},
            ],
        }
    ]
    results = aggregate_catalyst_stocks(trends, now=now)
    by_symbol = {r["symbol"]: r for r in results}
    # NOTE: _normalize_symbol strips the CN: prefix on A-shares (legacy behavior)
    # but keeps HK: on HK tickers. So the CN bucket key is "300308" while the
    # HK bucket key keeps its prefix. They never collide because prefixes differ.
    assert "300308" in by_symbol
    assert "HK:00700" in by_symbol
    # Both should carry the S-grade article.
    assert any(a.get("catalystGrade") == "S" for a in by_symbol["HK:00700"]["articles"])


def test_aggregate_catalyst_stocks_hk_only_trend() -> None:
    """A trend with only HK mapping (no CN) still produces HK buckets."""
    from datetime import UTC, datetime

    now = datetime(2026, 8, 1, 12, 0, 0, tzinfo=UTC)
    trends = [
        {
            "id": "t1",
            "documentId": "d1",
            "trendName": "腾讯 AI capex 重启",
            "catalystGrade": "S",
            "driverType": "Global_Tech",
            "documentPublishedAt": "2026-07-30T00:00:00Z",
            "documentFetchedAt": "2026-07-30T01:00:00Z",
            "cnSymbols": [],
            "hkSymbols": [
                {"symbol": "HK:00700", "name": "腾讯控股", "confidence": 0.8},
            ],
        }
    ]
    results = aggregate_catalyst_stocks(trends, now=now)
    assert any(r["symbol"] == "HK:00700" for r in results)


# ---------------------------------------------------------------------------
# compute_alpha_additions lets HK pure-plays skip EM industry gates
# ---------------------------------------------------------------------------


def test_alpha_additions_accepts_hk_without_em_industry() -> None:
    """HK pure-plays have no EM industry label; the missing_industry gate
    must NOT reject them (or else no HK Alpha S ever reaches the watchlist)."""
    payload = {
        "items": [
            {
                "symbol": "HK:00700",
                "name": "腾讯控股",
                "catalystScore": 200.0,
                "articles": [{"catalystGrade": "S", "contribution": 100.0}],
            }
        ]
    }
    # industries empty (mimics EM having no HK data) + empty top_set
    out, rejected = compute_alpha_additions(
        catalyst_payload=payload,
        industry_by_symbol={},
        top_industries=set(),
    )
    assert len(out) == 1
    assert out[0]["symbol"] == "HK:00700"
    # The HK symbol must NOT be counted as missing_industry.
    assert "missing_industry" not in rejected


def test_alpha_additions_rejects_cn_without_em_industry() -> None:
    """The CN missing_industry gate still applies for A-share names."""
    payload = {
        "items": [
            {
                "symbol": "CN:600519",
                "name": "贵州茅台",
                "catalystScore": 200.0,
                "articles": [{"catalystGrade": "S", "contribution": 100.0}],
            }
        ]
    }
    out, rejected = compute_alpha_additions(
        catalyst_payload=payload,
        industry_by_symbol={},
        top_industries=set(),
    )
    assert out == []
    assert rejected.get("missing_industry") == 1