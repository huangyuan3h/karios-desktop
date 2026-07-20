from __future__ import annotations

from data_sync_service.service.trendok import _block_buy_if_entry_at_or_below_stop


def test_block_buy_when_entry_trigger_at_or_below_hard_stop() -> None:
    res = {
        "buyZoneHigh": 56.89,
        "stopLossPrice": 57.6,
        "buyAction": "buy",
        "buyWhy": "模式A：回踩买入",
        "buyChecks": {},
    }
    _block_buy_if_entry_at_or_below_stop(res)
    assert res["buyAction"] == "avoid"
    assert "狙击位" in str(res["buyWhy"])
    assert res["buyChecks"]["blocked_entry_vs_stop"] is True


def test_block_buy_when_entry_equals_hard_stop() -> None:
    res = {
        "buyZoneHigh": 10.0,
        "stopLossPrice": 10.0,
        "buyAction": "buy",
        "buyChecks": {},
    }
    _block_buy_if_entry_at_or_below_stop(res)
    assert res["buyAction"] == "avoid"
    assert res["buyChecks"]["blocked_entry_vs_stop"] is True


def test_allows_buy_when_entry_above_hard_stop() -> None:
    res = {
        "buyZoneHigh": 58.0,
        "stopLossPrice": 57.6,
        "buyAction": "buy",
        "buyWhy": "ok",
        "buyChecks": {},
    }
    _block_buy_if_entry_at_or_below_stop(res)
    assert res["buyAction"] == "buy"
    assert res["buyWhy"] == "ok"
    assert "blocked_entry_vs_stop" not in res["buyChecks"]


def test_noop_when_prices_missing() -> None:
    res = {"buyAction": "buy", "buyChecks": {}}
    _block_buy_if_entry_at_or_below_stop(res)
    assert res["buyAction"] == "buy"
