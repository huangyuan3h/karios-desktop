import pytest
from data_sync_service.testback.engine import (
    _apply_daily_rules,
    _score_bar,
    _pick_top_n,
    _execute_order,
    DailyRuleFilter,
)
from data_sync_service.testback.strategies.base import Bar, Order, ScoreConfig


def test_apply_daily_rules_no_rules():
    bar = Bar(
        ts_code="000001.SZ",
        trade_date="2024-01-15",
        open=10.0,
        high=11.0,
        low=9.0,
        close=10.5,
        avg_price=10.25,
        volume=1000000,
        amount=10000000,
    )
    rules = DailyRuleFilter()
    assert _apply_daily_rules(bar, rules) is True


def test_apply_daily_rules_min_price():
    bar = Bar(
        ts_code="000001.SZ",
        trade_date="2024-01-15",
        open=10.0,
        high=11.0,
        low=9.0,
        close=10.5,
        avg_price=10.25,
        volume=1000000,
        amount=10000000,
    )
    rules = DailyRuleFilter(min_price=5.0)
    assert _apply_daily_rules(bar, rules) is True
    rules_fail = DailyRuleFilter(min_price=15.0)
    assert _apply_daily_rules(bar, rules_fail) is False


def test_apply_daily_rules_max_price():
    bar = Bar(
        ts_code="000001.SZ",
        trade_date="2024-01-15",
        open=10.0,
        high=11.0,
        low=9.0,
        close=10.5,
        avg_price=10.25,
        volume=1000000,
        amount=10000000,
    )
    rules = DailyRuleFilter(max_price=20.0)
    assert _apply_daily_rules(bar, rules) is True
    rules_fail = DailyRuleFilter(max_price=5.0)
    assert _apply_daily_rules(bar, rules_fail) is False


def test_apply_daily_rules_min_volume():
    bar = Bar(
        ts_code="000001.SZ",
        trade_date="2024-01-15",
        open=10.0,
        high=11.0,
        low=9.0,
        close=10.5,
        avg_price=10.25,
        volume=1000000,
        amount=10000000,
    )
    rules = DailyRuleFilter(min_volume=500000)
    assert _apply_daily_rules(bar, rules) is True
    rules_fail = DailyRuleFilter(min_volume=2000000)
    assert _apply_daily_rules(bar, rules_fail) is False


def test_apply_daily_rules_max_volume():
    bar = Bar(
        ts_code="000001.SZ",
        trade_date="2024-01-15",
        open=10.0,
        high=11.0,
        low=9.0,
        close=10.5,
        avg_price=10.25,
        volume=1000000,
        amount=10000000,
    )
    rules = DailyRuleFilter(max_volume=2000000)
    assert _apply_daily_rules(bar, rules) is True
    rules_fail = DailyRuleFilter(max_volume=500000)
    assert _apply_daily_rules(bar, rules_fail) is False


def test_apply_daily_rules_min_amount():
    bar = Bar(
        ts_code="000001.SZ",
        trade_date="2024-01-15",
        open=10.0,
        high=11.0,
        low=9.0,
        close=10.5,
        avg_price=10.25,
        volume=1000000,
        amount=10000000,
    )
    rules = DailyRuleFilter(min_amount=5000000)
    assert _apply_daily_rules(bar, rules) is True
    rules_fail = DailyRuleFilter(min_amount=20000000)
    assert _apply_daily_rules(bar, rules_fail) is False


def test_apply_daily_rules_max_amount():
    bar = Bar(
        ts_code="000001.SZ",
        trade_date="2024-01-15",
        open=10.0,
        high=11.0,
        low=9.0,
        close=10.5,
        avg_price=10.25,
        volume=1000000,
        amount=10000000,
    )
    rules = DailyRuleFilter(max_amount=20000000)
    assert _apply_daily_rules(bar, rules) is True
    rules_fail = DailyRuleFilter(max_amount=5000000)
    assert _apply_daily_rules(bar, rules_fail) is False


def test_score_bar_positive_momentum():
    bar = Bar(
        ts_code="000001.SZ",
        trade_date="2024-01-15",
        open=10.0,
        high=11.0,
        low=9.0,
        close=10.5,
        avg_price=10.25,
        volume=1000000,
        amount=10000000,
    )
    prev_close = 10.0
    score_cfg = ScoreConfig(momentum_weight=1.0, volume_weight=0.0, amount_weight=0.0, top_n=5)
    score = _score_bar(bar, prev_close, score_cfg)
    assert score > 0


def test_score_bar_negative_momentum():
    bar = Bar(
        ts_code="000001.SZ",
        trade_date="2024-01-15",
        open=10.0,
        high=11.0,
        low=9.0,
        close=9.5,
        avg_price=9.875,
        volume=1000000,
        amount=10000000,
    )
    prev_close = 10.0
    score_cfg = ScoreConfig(momentum_weight=1.0, volume_weight=0.0, amount_weight=0.0, top_n=5)
    score = _score_bar(bar, prev_close, score_cfg)
    assert score < 0


def test_score_bar_zero_prev_close():
    bar = Bar(
        ts_code="000001.SZ",
        trade_date="2024-01-15",
        open=10.0,
        high=11.0,
        low=9.0,
        close=10.5,
        avg_price=10.25,
        volume=1000000,
        amount=10000000,
    )
    prev_close = 0.0
    score_cfg = ScoreConfig(momentum_weight=1.0, volume_weight=0.0, amount_weight=0.0, top_n=5)
    score = _score_bar(bar, prev_close, score_cfg)
    assert score == 0.0


def test_pick_top_n_empty():
    bars = {}
    prev_close_map = {}
    rules = DailyRuleFilter()
    score_cfg = ScoreConfig(momentum_weight=1.0, volume_weight=0.0, amount_weight=0.0, top_n=5)
    selected, scored = _pick_top_n(bars, prev_close_map, rules, score_cfg)
    assert selected == {}
    assert scored == []


def test_pick_top_n_single():
    bar = Bar(
        ts_code="000001.SZ",
        trade_date="2024-01-15",
        open=10.0,
        high=11.0,
        low=9.0,
        close=10.5,
        avg_price=10.25,
        volume=1000000,
        amount=10000000,
    )
    bars = {"000001.SZ": bar}
    prev_close_map = {"000001.SZ": 10.0}
    rules = DailyRuleFilter()
    score_cfg = ScoreConfig(momentum_weight=1.0, volume_weight=0.0, amount_weight=0.0, top_n=5)
    selected, scored = _pick_top_n(bars, prev_close_map, rules, score_cfg)
    assert "000001.SZ" in selected
    assert len(scored) == 1


def test_pick_top_n_sorted_by_score():
    bar1 = Bar(
        ts_code="000001.SZ",
        trade_date="2024-01-15",
        open=10.0,
        high=11.0,
        low=9.0,
        close=11.0,
        avg_price=10.0,
        volume=1000000,
        amount=10000000,
    )
    bar2 = Bar(
        ts_code="000002.SZ",
        trade_date="2024-01-15",
        open=10.0,
        high=11.0,
        low=9.0,
        close=9.0,
        avg_price=10.0,
        volume=1000000,
        amount=10000000,
    )
    bars = {"000001.SZ": bar1, "000002.SZ": bar2}
    prev_close_map = {"000001.SZ": 10.0, "000002.SZ": 10.0}
    rules = DailyRuleFilter()
    score_cfg = ScoreConfig(momentum_weight=1.0, volume_weight=0.0, amount_weight=0.0, top_n=5)
    selected, scored = _pick_top_n(bars, prev_close_map, rules, score_cfg)
    assert scored[0][0] == "000001.SZ"
    assert scored[0][1] > scored[1][1]


def test_execute_order_buy():
    bar = Bar(
        ts_code="000001.SZ",
        trade_date="2024-01-15",
        open=10.0,
        high=11.0,
        low=9.0,
        close=10.5,
        avg_price=10.0,
        volume=1000000,
        amount=10000000,
    )
    order = Order(ts_code="000001.SZ", action="buy", qty=100, reason="test")
    cash = 100000.0
    positions = {}
    fee_rate = 0.0
    slippage_rate = 0.0
    equity = 100000.0
    cash_after, positions_after, trade = _execute_order(order, bar, cash, positions, fee_rate, slippage_rate, equity)
    assert trade is not None
    assert trade["action"] == "buy"
    assert positions_after.get("000001.SZ") == 100
    assert cash_after < cash


def test_execute_order_sell():
    bar = Bar(
        ts_code="000001.SZ",
        trade_date="2024-01-15",
        open=10.0,
        high=11.0,
        low=9.0,
        close=10.5,
        avg_price=10.0,
        volume=1000000,
        amount=10000000,
    )
    order = Order(ts_code="000001.SZ", action="sell", qty=100, reason="test")
    cash = 1000.0
    positions = {"000001.SZ": 200}
    fee_rate = 0.0
    slippage_rate = 0.0
    equity = 3000.0
    cash_after, positions_after, trade = _execute_order(order, bar, cash, positions, fee_rate, slippage_rate, equity)
    assert trade is not None
    assert trade["action"] == "sell"
    assert positions_after.get("000001.SZ") == 100
    assert cash_after > cash


def test_execute_order_sell_all():
    bar = Bar(
        ts_code="000001.SZ",
        trade_date="2024-01-15",
        open=10.0,
        high=11.0,
        low=9.0,
        close=10.5,
        avg_price=10.0,
        volume=1000000,
        amount=10000000,
    )
    order = Order(ts_code="000001.SZ", action="sell", qty=200, reason="test")
    cash = 1000.0
    positions = {"000001.SZ": 200}
    fee_rate = 0.0
    slippage_rate = 0.0
    equity = 3000.0
    cash_after, positions_after, trade = _execute_order(order, bar, cash, positions, fee_rate, slippage_rate, equity)
    assert trade is not None
    assert trade["action"] == "sell"
    assert "000001.SZ" not in positions_after


def test_execute_order_invalid_action():
    bar = Bar(
        ts_code="000001.SZ",
        trade_date="2024-01-15",
        open=10.0,
        high=11.0,
        low=9.0,
        close=10.5,
        avg_price=10.0,
        volume=1000000,
        amount=10000000,
    )
    order = Order(ts_code="000001.SZ", action="invalid", qty=100, reason="test")
    cash = 100000.0
    positions = {}
    fee_rate = 0.0
    slippage_rate = 0.0
    equity = 100000.0
    cash_after, positions_after, trade = _execute_order(order, bar, cash, positions, fee_rate, slippage_rate, equity)
    assert trade is None
    assert positions_after == {}
    assert cash_after == cash


def test_execute_order_with_fee():
    bar = Bar(
        ts_code="000001.SZ",
        trade_date="2024-01-15",
        open=10.0,
        high=11.0,
        low=9.0,
        close=10.5,
        avg_price=10.0,
        volume=1000000,
        amount=10000000,
    )
    order = Order(ts_code="000001.SZ", action="buy", qty=100, reason="test")
    cash = 100000.0
    positions = {}
    fee_rate = 0.01
    slippage_rate = 0.0
    equity = 100000.0
    cash_after, positions_after, trade = _execute_order(order, bar, cash, positions, fee_rate, slippage_rate, equity)
    assert trade is not None
    assert trade["fee"] > 0


def test_execute_order_target_pct():
    bar = Bar(
        ts_code="000001.SZ",
        trade_date="2024-01-15",
        open=10.0,
        high=11.0,
        low=9.0,
        close=10.5,
        avg_price=10.0,
        volume=1000000,
        amount=10000000,
    )
    order = Order(ts_code="000001.SZ", action="buy", target_pct=0.5, reason="test")
    cash = 10000.0
    positions = {}
    fee_rate = 0.0
    slippage_rate = 0.0
    equity = 10000.0
    cash_after, positions_after, trade = _execute_order(order, bar, cash, positions, fee_rate, slippage_rate, equity)
    assert trade is not None
    assert trade["action"] == "buy"