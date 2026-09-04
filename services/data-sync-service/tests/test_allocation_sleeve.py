"""Tests for the R5c + T6 sleeve allocation extension (2026-08-21)."""

from __future__ import annotations

from unittest.mock import patch

from data_sync_service.service.allocation import (
    resolve_weights_with_sleeve,
    weights_from_regimes,
    weights_with_sleeve,
)


def test_weights_from_regimes_unchanged():
    assert weights_from_regimes("Strong", "Weak") == (1.0, 0.0)
    assert weights_from_regimes("Weak", "Strong") == (0.0, 1.0)
    assert weights_from_regimes("Weak", "Weak") == (0.0, 0.0)
    assert weights_from_regimes("Diverging", None) == (1.0, 0.0)


def test_sleeve_ignored_when_a_market_is_tradable():
    with patch("data_sync_service.service.allocation.live_regimes",
               return_value={"CN": "Strong", "HK": "Weak"}):
        w = weights_with_sleeve(as_of_date="2026-08-01", etf_above_ma200=True)
    assert w == (1.0, 0.0, 0.0)


def test_sleeve_takes_idle_when_both_weak_and_above_ma():
    with patch("data_sync_service.service.allocation.live_regimes",
               return_value={"CN": "Weak", "HK": "Weak"}):
        w = weights_with_sleeve(as_of_date="2026-08-21", etf_above_ma200=True)
    assert w == (0.0, 0.0, 1.0)


def test_no_sleeve_when_both_weak_and_below_ma():
    with patch("data_sync_service.service.allocation.live_regimes",
               return_value={"CN": "Weak", "HK": "Weak"}):
        w = weights_with_sleeve(as_of_date="2026-08-21", etf_above_ma200=False)
    assert w == (0.0, 0.0, 0.0)


def test_resolve_weights_with_sleeve_shape():
    # Hermetic: stub the multi-asset _pick() instead of reading live ETF bars.
    # (CI runs on a fresh-migrated empty DB where _pick() finds no bars.)
    with patch("data_sync_service.service.allocation.live_regimes",
               return_value={"CN": "Weak", "HK": "Weak"}):
        with patch(
            "data_sync_service.service.multi_asset_sleeve._pick",
            return_value={"key": "NASDAQ", "above_ma200": True},
        ):
            out = resolve_weights_with_sleeve(as_of_date="2026-08-21")
    assert out["weights"] == {"CN": 0.0, "HK": 0.0, "ETF": 1.0}
    assert out["regimes"] == {"CN": "Weak", "HK": "Weak"}
