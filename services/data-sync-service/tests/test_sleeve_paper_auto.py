"""Tests for the paper-book Harbor parking sleeve auto-configuration."""

from __future__ import annotations

from contextlib import ExitStack
from unittest.mock import patch

import pytest

import data_sync_service.service.multi_asset_sleeve as mas
import data_sync_service.service.sleeve_paper_auto as sa
from data_sync_service.db.paper_trading import (
    CLOSE_REASON_SLEEVE_EXIT,
    list_paper_trades,
)
from data_sync_service.service.sleeve_paper_auto import (
    apply_sleeve_to_paper,
    paper_sleeve_holdings,
)

# Fake candidate symbols — DB tests must never touch real book rows (AGENTS.md).
TEST_SYMBOL = "ETF:995100"
TEST_SYMBOL_B = "ETF:995101"
TEST_TS = "995100.SH"
FAKE_CANDIDATES = {TEST_SYMBOL, TEST_SYMBOL_B}


def _mk_open_leg(day: str = "2026-08-01", symbol: str = TEST_SYMBOL) -> dict:
    from data_sync_service.db.paper_trading import insert_paper_trade

    return insert_paper_trade(
        symbol=symbol,
        entry_date=day,
        side="BUY",
        entry_price=2.0,
        why_at_entry="test sleeve leg",
        sleeve_pct=50.0,
        source="S3",
        market="CN",
    )


def _multi(*, action: str, idle: float = 60.0, holding: bool = False) -> dict:
    return {
        "action": action,
        "idlePct": idle,
        "holding": holding,
        "pick": {
            "key": "GOLD",
            "symbol": TEST_SYMBOL,
            "ts": TEST_TS,
            "mom60": 12.0,
            "close": 2.25,
        },
    }


def _fill(day: str = "2026-08-20") -> dict:
    return {
        "entry_date": "2026-08-21",
        "entry_price": 2.30,
        "pending_open_fill": False,
        "signal_snapshot": {
            "entryMode": "next_open",
            "signalDate": day,
            "pendingOpenFill": False,
        },
    }


def _purge_test_rows() -> None:
    from data_sync_service.db import get_connection

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM paper_trades WHERE symbol = ANY(%s)",
                (sorted(FAKE_CANDIDATES),),
            )
        conn.commit()


@pytest.fixture(autouse=True)
def _cleanup():
    # Purge before AND after so a leftover fake leg cannot flip a BUY branch.
    _purge_test_rows()
    yield
    _purge_test_rows()


def _patches(stack: ExitStack, *, multi: dict, fill: dict | None = None) -> None:
    """Patch the candidate set (fake symbol only) + decision + next-open fills."""
    stack.enter_context(
        patch("data_sync_service.service.sleeve_paper_auto.CANDIDATE_SYMBOLS", FAKE_CANDIDATES)
    )
    stack.enter_context(
        patch(
            "data_sync_service.service.sleeve_paper_auto._build_multi_for_paper",
            return_value=multi,
        )
    )
    if fill is not None:
        stack.enter_context(
            patch(
                "data_sync_service.service.sleeve_paper_auto.resolve_next_open_fill",
                return_value=fill,
            )
        )


@pytest.mark.requires_postgres
def test_buy_opens_sleeve_leg():
    with ExitStack() as stack:
        _patches(stack, multi=_multi(action="BUY"), fill=_fill())
        out = apply_sleeve_to_paper(day="2026-08-20")
    assert out.get("reason") != "no next_open fill", out
    assert out["changed"] is True
    assert out["reason"] == "multi opened"
    open_legs = [
        t
        for t in list_paper_trades(status="open")
        if str(t.get("symbol") or "").upper() == TEST_SYMBOL
    ]
    assert len(open_legs) == 1
    assert float(open_legs[0]["sleevePct"] or 0) == pytest.approx(60.0, abs=0.1)
    assert open_legs[0]["entryDate"] == "2026-08-21"
    assert float(open_legs[0]["entryPrice"] or 0) == pytest.approx(2.30)


@pytest.mark.requires_postgres
def test_buy_is_idempotent():
    _mk_open_leg()
    with ExitStack() as stack:
        _patches(stack, multi=_multi(action="BUY", holding=True))
        out = apply_sleeve_to_paper(day="2026-08-20")
    # already have an open sleeve leg → BUY requires not open_multi
    assert out["changed"] is False


@pytest.mark.requires_postgres
def test_sell_to_repo_closes_leg():
    _mk_open_leg(day="2026-08-01")
    with ExitStack() as stack:
        _patches(stack, multi=_multi(action="SELL_TO_REPO", holding=True), fill=_fill())
        out = apply_sleeve_to_paper(day="2026-08-20")
    assert out["changed"] is True
    assert "multi closed" in out["reason"]
    open_legs = [
        t
        for t in list_paper_trades(status="open")
        if str(t.get("symbol") or "").upper() == TEST_SYMBOL
    ]
    assert len(open_legs) == 0


@pytest.mark.requires_postgres
def test_sell_books_next_open_pnl():
    """pnl/holding days come from the camelCase row fields (not恒 0)."""
    leg = _mk_open_leg(day="2026-08-01")
    with ExitStack() as stack:
        _patches(stack, multi=_multi(action="SELL_TO_REPO", holding=True), fill=_fill())
        apply_sleeve_to_paper(day="2026-08-20")
    rows = [t for t in list_paper_trades(status="closed") if str(t.get("id")) == str(leg["id"])]
    assert rows
    row = rows[0]
    assert row["closeDate"] == "2026-08-21"
    assert float(row["closePrice"] or 0) == pytest.approx(2.30)
    assert float(row["grossPnlPct"] or 0) == pytest.approx(15.0, abs=0.01)
    assert float(row["costsPct"] or 0) == pytest.approx(0.1, abs=0.001)
    assert float(row["pnlPct"] or 0) == pytest.approx(14.9, abs=0.01)
    assert int(row["holdingDays"] or 0) >= 20


@pytest.mark.requires_postgres
def test_pending_exit_marks_placeholder_for_later_patch():
    """Same-evening exit: T+1 open not printed yet → placeholder + pending flag."""
    leg = _mk_open_leg(day="2026-08-01")
    pending = {**_fill(), "pending_open_fill": True}
    with ExitStack() as stack:
        _patches(stack, multi=_multi(action="SELL_TO_REPO", holding=True), fill=pending)
        apply_sleeve_to_paper(day="2026-08-20")
    rows = [t for t in list_paper_trades(status="closed") if str(t.get("id")) == str(leg["id"])]
    assert rows
    snap = rows[0].get("signalSnapshot") or {}
    assert snap.get("exitPendingOpenFill") is True
    assert snap.get("exitSignalDate") == "2026-08-20"
    assert float(snap.get("exitPlaceholderClose") or 0) == pytest.approx(2.30)


def test_exit_fill_marks_pending_tuple():
    leg = {"symbol": "ETF:513100", "entryDate": "2026-01-01"}
    pending = {**_fill(), "pending_open_fill": True}
    with patch(
        "data_sync_service.service.sleeve_paper_auto.resolve_next_open_fill",
        return_value=pending,
    ):
        out = sa._exit_fill(leg, "2026-08-20")
    assert out is not None
    px, exit_day, extra = out
    assert (px, exit_day) == (2.30, "2026-08-21")
    assert extra["exitPendingOpenFill"] is True
    assert extra["exitPlaceholderClose"] == pytest.approx(2.30)


@pytest.mark.requires_postgres
def test_hold_is_noop():
    with ExitStack() as stack:
        _patches(stack, multi=_multi(action="HOLD", idle=10.0, holding=True))
        out = apply_sleeve_to_paper(day="2026-08-20")
    assert out["changed"] is False
    assert out["reason"] == "multi no-op"


@pytest.mark.requires_postgres
def test_close_reason_is_sleeve_exit():
    leg = _mk_open_leg(day="2026-08-01")
    with ExitStack() as stack:
        _patches(stack, multi=_multi(action="SELL_TO_REPO", holding=True), fill=_fill())
        apply_sleeve_to_paper(day="2026-08-20")
    rows = [t for t in list_paper_trades(status="closed") if str(t.get("id")) == str(leg["id"])]
    assert rows and rows[0]["closeReason"] == CLOSE_REASON_SLEEVE_EXIT


@pytest.mark.requires_postgres
def test_rotate_aborts_when_exit_fill_missing():
    """Old leg not closable → no new leg may be opened (double-hold guard)."""
    _mk_open_leg(day="2026-08-01", symbol=TEST_SYMBOL_B)
    multi = _multi(action="ROTATE")

    def _fill_by_ts(ts, day, signal_close=None):
        if ts == TEST_TS:
            return _fill()
        return None

    with ExitStack() as stack:
        _patches(stack, multi=multi)
        stack.enter_context(
            patch(
                "data_sync_service.service.sleeve_paper_auto.resolve_next_open_fill",
                side_effect=_fill_by_ts,
            )
        )
        out = apply_sleeve_to_paper(day="2026-08-20")
    assert out["changed"] is False
    assert "rotate aborted" in out["reason"]
    still_open = [
        t
        for t in list_paper_trades(status="open")
        if str(t.get("symbol") or "").upper() == TEST_SYMBOL_B
    ]
    assert len(still_open) == 1
    new_open = [
        t
        for t in list_paper_trades(status="open")
        if str(t.get("symbol") or "").upper() == TEST_SYMBOL
    ]
    assert new_open == []


def test_paper_sleeve_holdings_normalizes_units_and_keeps_entry_date():
    rows = [
        {"symbol": "CN:600000", "sleevePct": 0.1, "entryDate": "2026-08-01"},
        {"symbol": "ETF:513100", "sleevePct": 40.0, "entryDate": "2026-09-01"},
        {"symbol": "HK:00700", "sleevePct": 0.1, "entryDate": "2026-08-01"},
        {"symbol": "CN:600001", "sleeve_pct": None, "entryDate": None},
    ]
    out = paper_sleeve_holdings(rows)
    assert [h["symbol"] for h in out] == ["CN:600000", "ETF:513100", "CN:600001"]
    assert out[0]["sleeve_pct"] == pytest.approx(10.0)
    assert out[0]["entryDate"] == "2026-08-01"
    assert out[1]["sleeve_pct"] == pytest.approx(40.0)
    assert out[2]["sleeve_pct"] == pytest.approx(0.0)
    assert mas._idle_pct(out) == pytest.approx(50.0)


def test_build_multi_for_paper_passes_shaped_holdings(monkeypatch):
    rows = [{"symbol": "CN:600000", "sleevePct": 0.1, "entryDate": "2026-08-01"}]
    captured: dict = {}

    def _fake_build(*, day, cn_block, holdings_override=None):
        captured["holdings"] = holdings_override
        return {"action": "NONE"}

    monkeypatch.setattr(sa, "list_paper_trades", lambda **kw: rows)
    monkeypatch.setattr(sa, "_health_block", lambda market, day: {"holdings": []})
    monkeypatch.setattr(sa, "build_multi_asset_sleeve", _fake_build)
    out = sa._build_multi_for_paper("2026-09-01")
    assert out == {"action": "NONE"}
    assert captured["holdings"][0]["sleeve_pct"] == pytest.approx(10.0)
    assert captured["holdings"][0]["entryDate"] == "2026-08-01"


def test_paper_holdings_feed_causal_trail(monkeypatch):
    """entryDate in the paper holdings must reach the trail8 exit."""
    pick = {
        "key": "GOLD",
        "symbol": "ETF:518880",
        "name": "gold",
        "mom60": 3.0,
        "above_ma200": True,
    }
    rows = [{"symbol": "ETF:518880", "sleevePct": 50.0, "entryDate": "2026-01-01"}]
    bars = [
        {"date": "2026-01-02", "trade_date": "2026-01-02", "close": 100.0},
        {"date": "2026-02-16", "trade_date": "2026-02-16", "close": 110.0},
        {"date": "2026-03-01", "trade_date": "2026-03-01", "close": 99.0},
    ]
    cn = {
        "regime": "Weak",
        "panicCooldown": {"active": False},
        "circuitBlocked": False,
        "s3Candidates": [],
    }
    monkeypatch.setattr(mas, "_pick", lambda *, as_of=None: pick)
    monkeypatch.setattr(
        mas,
        "_adjusted_series",
        lambda ts: {b["date"]: float(b["close"]) for b in bars},
    )
    out = mas.build_multi_asset_sleeve(
        day="2026-03-01",
        cn_block=cn,
        holdings_override=paper_sleeve_holdings(rows),
    )
    assert out["action"] == "SELL_TO_REPO"
    assert "峰值回撤" in out["message"]
