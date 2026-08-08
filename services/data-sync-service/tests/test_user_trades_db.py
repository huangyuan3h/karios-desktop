"""user_trades db + routes + stats tests.

Integration tests write rows with the `CN:99` test-symbol prefix and MUST
clean them up (AGENTS.md DB hygiene discipline): the autouse fixture removes
any row whose symbol matches the prefix.
"""

from __future__ import annotations

import pytest

from data_sync_service.db import user_trades as ut

TEST_PREFIX = "CN:99"
TEST_SYMBOL = "CN:99ut1"

pytestmark = pytest.mark.requires_postgres


@pytest.fixture(autouse=True)
def _cleanup_test_rows():
    yield
    ut.ensure_tables()
    with ut.get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            "DELETE FROM user_trades WHERE symbol LIKE %s",
            (f"{TEST_PREFIX}%",),
        )


def test_insert_and_list_roundtrip() -> None:
    ut.ensure_tables()
    row = ut.insert_trade(
        symbol=TEST_SYMBOL,
        side="SELL",
        trade_date="2026-08-08",
        price=10.5,
        position_pct=5.0,
        cost_basis=10.0,
        entry_date="2026-08-01",
        pnl_pct=5.0,
        holding_days=7,
        source="ALPHA",
        market="CN",
    )
    assert row["side"] == "SELL"
    assert row["symbol"] == TEST_SYMBOL
    rows = ut.list_trades()
    assert any(r["id"] == row["id"] for r in rows)


def test_insert_rejects_invalid_side() -> None:
    ut.ensure_tables()
    with pytest.raises(ValueError):
        ut.insert_trade(
            symbol=TEST_SYMBOL,
            side="HOLD",
            trade_date="2026-08-08",
            price=1.0,
            position_pct=1.0,
        )


def test_delete_trade() -> None:
    ut.ensure_tables()
    row = ut.insert_trade(
        symbol=TEST_SYMBOL,
        side="BUY",
        trade_date="2026-08-08",
        price=10.0,
        position_pct=5.0,
    )
    assert ut.delete_trade(row["id"]) is True
    assert ut.delete_trade(row["id"]) is False


def test_list_filters_by_symbol_and_limit() -> None:
    ut.ensure_tables()
    for i in range(3):
        ut.insert_trade(
            symbol=f"{TEST_PREFIX}list{i}",
            side="BUY",
            trade_date=f"2026-08-0{i + 1}",
            price=10.0 + i,
            position_pct=5.0,
        )
    rows = ut.list_trades(symbol=f"{TEST_PREFIX}list1")
    assert len(rows) == 1
    rows2 = ut.list_trades(limit=2)
    assert len(rows2) <= 2


def test_fetch_sell_rows_only() -> None:
    ut.ensure_tables()
    ut.insert_trade(
        symbol=TEST_SYMBOL,
        side="BUY",
        trade_date="2026-08-01",
        price=10.0,
        position_pct=5.0,
    )
    ut.insert_trade(
        symbol=TEST_SYMBOL,
        side="SELL",
        trade_date="2026-08-08",
        price=11.0,
        position_pct=5.0,
        cost_basis=10.0,
        entry_date="2026-08-01",
        pnl_pct=10.0,
        holding_days=7,
    )
    sells = ut.fetch_sell_rows()
    assert len(sells) == 1
    assert sells[0]["symbol"] == TEST_SYMBOL
    assert sells[0]["pnlPct"] == 10.0
