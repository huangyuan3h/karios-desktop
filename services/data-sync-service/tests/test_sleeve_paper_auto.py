"""Tests for the paper-book sleeve auto-configuration (T6 · 2026-08-21)."""

from __future__ import annotations

import uuid
from unittest.mock import patch

import pytest

from data_sync_service.db.paper_trading import (
    CLOSE_REASON_SLEEVE_EXIT,
    close_paper_trade,
    list_paper_trades,
)
from data_sync_service.service.sleeve_paper_auto import apply_sleeve_to_paper

TEST_SYMBOL = "ETF:513100"


def _mk_open_leg(day: str = "2026-08-01") -> dict:
    """Insert an open 513100 leg directly (test-only, cleaned in teardown)."""
    from data_sync_service.db.paper_trading import insert_paper_trade

    return insert_paper_trade(
        symbol=TEST_SYMBOL,
        entry_date=day,
        side="BUY",
        entry_price=2.0,
        why_at_entry="test sleeve leg",
        sleeve_pct=50.0,
        source="S3",
        market="CN",
    )


@pytest.fixture(autouse=True)
def _cleanup():
    yield
    from data_sync_service.db import get_connection

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM paper_trades WHERE symbol = %s AND why_at_entry LIKE %s",
                (TEST_SYMBOL, "test sleeve leg%"),
            )
        conn.commit()


@pytest.mark.requires_postgres
def test_buy_opens_sleeve_leg():
    with patch(
        "data_sync_service.service.sleeve_paper_auto.build_third_asset_sleeve_for_paper",
        return_value={
            "action": "BUY_513100", "price": 2.25, "idlePct": 60.0,
            "etf": "ETF:513100", "holding": False,
        },
    ):
        out = apply_sleeve_to_paper(day="2026-08-20")
    assert out["changed"] is True
    assert out["reason"] == "opened"
    open_legs = [
        t for t in list_paper_trades(status="open")
        if str(t.get("symbol") or "").upper() == TEST_SYMBOL
    ]
    assert len(open_legs) == 1
    assert float(open_legs[0]["sleeve_pct"] or 0) == pytest.approx(60.0, abs=0.1)


@pytest.mark.requires_postgres
def test_buy_is_idempotent():
    _mk_open_leg()
    with patch(
        "data_sync_service.service.sleeve_paper_auto.build_third_asset_sleeve_for_paper",
        return_value={
            "action": "BUY_513100", "price": 2.25, "idlePct": 60.0,
            "etf": "ETF:513100", "holding": True,
        },
    ):
        out = apply_sleeve_to_paper(day="2026-08-20")
    assert out["changed"] is False  # already holding -> no-op
    assert out["reason"] == "no-op"


@pytest.mark.requires_postgres
def test_sell_to_repo_closes_leg():
    _mk_open_leg(day="2026-08-01")
    with patch(
        "data_sync_service.service.sleeve_paper_auto.build_third_asset_sleeve_for_paper",
        return_value={
            "action": "SELL_TO_REPO", "price": 2.1, "idlePct": 60.0,
            "etf": "ETF:513100", "holding": True,
        },
    ):
        out = apply_sleeve_to_paper(day="2026-08-20")
    assert out["changed"] is True
    assert out["reason"] == "closed 1"
    open_legs = [
        t for t in list_paper_trades(status="open")
        if str(t.get("symbol") or "").upper() == TEST_SYMBOL
    ]
    assert len(open_legs) == 0


@pytest.mark.requires_postgres
def test_hold_is_noop():
    with patch(
        "data_sync_service.service.sleeve_paper_auto.build_third_asset_sleeve_for_paper",
        return_value={
            "action": "HOLD", "price": 2.25, "idlePct": 10.0,
            "etf": "ETF:513100", "holding": True,
        },
    ):
        out = apply_sleeve_to_paper(day="2026-08-20")
    assert out["changed"] is False
    assert out["reason"] == "no-op"


@pytest.mark.requires_postgres
def test_close_reason_is_sleeve_exit():
    leg = _mk_open_leg(day="2026-08-01")
    with patch(
        "data_sync_service.service.sleeve_paper_auto.build_third_asset_sleeve_for_paper",
        return_value={
            "action": "SELL_TO_REPO", "price": 2.1, "idlePct": 60.0,
            "etf": "ETF:513100", "holding": True,
        },
    ):
        apply_sleeve_to_paper(day="2026-08-20")
    rows = [
        t for t in list_paper_trades(status="closed")
        if str(t.get("id")) == str(leg["id"])
    ]
    assert rows and rows[0]["close_reason"] == CLOSE_REASON_SLEEVE_EXIT