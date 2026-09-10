"""Tests for the core-leg sleeve paper recon (OPT-151).

Unit tests patch the paper snapshot / decision rebuild / user trades, so no
Postgres is needed for the recon logic itself. The integration test mirrors
``test_sleeve_paper_auto.py``: real paper rows, patched decision, cleanup in
teardown (AGENTS.md DB discipline).
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

import data_sync_service.service.trading_brief as tb
from data_sync_service.service.sleeve_paper_recon import (
    _pre_decision_holdings,
    sleeve_paper_recon,
)

TEST_SYMBOL = "ETF:513100"
DAY = "2026-08-20"

_RECON = "data_sync_service.service.sleeve_paper_recon"


def _open_row(*, symbol: str = TEST_SYMBOL, created: str | None = None) -> dict:
    return {
        "symbol": symbol,
        "whyAtEntry": "multi-sleeve: NASDAQ mom60 12% 60/200+5d" if created else "test leg",
        "signalSnapshot": {"entryMode": "next_open", "signalDate": created},
        "createdAt": created,
        "sleevePct": 50.0,
        "tsCode": None,
    }


def _closed_row(*, symbol: str = TEST_SYMBOL, day: str = DAY) -> dict:
    return {
        "symbol": symbol,
        "closeReason": "sleeve_exit",
        "closeDate": day,
        "sleevePct": 50.0,
        "tsCode": None,
    }


def _user_row(symbol: str, side: str, day: str) -> dict:
    return {"symbol": symbol, "side": side, "trade_date": day}


def _multi(*, action: str, pick_symbol: str = TEST_SYMBOL, idle: float = 60.0) -> dict:
    return {
        "action": action,
        "label": "test",
        "message": "test message",
        "idlePct": idle,
        "pick": {"key": "NASDAQ", "symbol": pick_symbol, "ts": "513100.SH"},
    }


def _run_recon(
    *,
    open_rows: list[dict],
    closed_rows: list[dict],
    multi: dict,
    user_rows: list[dict] | None = None,
) -> dict:
    with (
        patch(f"{_RECON}._paper_snapshot", return_value=(open_rows, closed_rows)),
        patch(f"{_RECON}._rebuild_decision", return_value=multi),
        patch(f"{_RECON}._user_core_trades", return_value=user_rows or []),
    ):
        return sleeve_paper_recon(day=DAY)


def test_pre_decision_holdings_excludes_todays_decision_rows() -> None:
    rows = [
        _open_row(created=DAY),  # the job's own BUY today — excluded
        _open_row(symbol="ETF:518880"),  # pre-existing sleeve leg — kept
        _open_row(symbol="CN:600000", created=DAY),  # S-3 intake today — kept
        {"symbol": "HK:00700", "whyAtEntry": "", "signalSnapshot": {}, "createdAt": None, "sleevePct": 0},
    ]
    closed = [_closed_row(symbol="ETF:513350")]
    holdings = _pre_decision_holdings(rows, closed, DAY)
    syms = [str(h["symbol"]).upper() for h in holdings]
    assert "ETF:513100" not in syms
    assert "ETF:518880" in syms
    assert "CN:600000" in syms
    assert "ETF:513350" in syms
    assert "HK:00700" not in syms


def test_buy_recorded_ok_and_user_pending() -> None:
    recon = _run_recon(
        open_rows=[_open_row(created=DAY)],
        closed_rows=[],
        multi=_multi(action="BUY"),
    )
    assert recon["ok"] is True
    assert recon["expectedBuys"] == [TEST_SYMBOL]
    assert recon["paperBuysToday"] == [TEST_SYMBOL]
    assert recon["missedBuys"] == []
    # BUY fills next open (Fri after Thu): user pending, not missing.
    assert recon["userAlignment"] == "pending"


def test_buy_missed_flips_ok() -> None:
    recon = _run_recon(open_rows=[], closed_rows=[], multi=_multi(action="BUY"))
    assert recon["ok"] is False
    assert recon["missedBuys"] == [TEST_SYMBOL]


def test_extra_open_flagged() -> None:
    recon = _run_recon(
        open_rows=[_open_row(created=DAY)],
        closed_rows=[],
        multi=_multi(action="HOLD", pick_symbol="ETF:518880"),
    )
    assert recon["ok"] is False
    assert recon["extraOpens"] == [TEST_SYMBOL]


def test_sell_recorded_ok_from_closed_leg_prestate() -> None:
    recon = _run_recon(
        open_rows=[],
        closed_rows=[_closed_row()],
        multi=_multi(action="SELL_TO_REPO"),
    )
    assert recon["ok"] is True
    assert recon["expectedSells"] == [TEST_SYMBOL]
    assert recon["paperSellsToday"] == [TEST_SYMBOL]
    assert recon["userAlignment"] == "missing"  # engine sold today, user did not


def test_rotate_expects_sell_old_buy_new() -> None:
    recon = _run_recon(
        open_rows=[_open_row(symbol="ETF:518880"), _open_row(symbol=TEST_SYMBOL, created=DAY)],
        closed_rows=[_closed_row(symbol="ETF:518880")],
        multi=_multi(action="ROTATE", pick_symbol=TEST_SYMBOL),
    )
    assert recon["expectedSells"] == ["ETF:518880"]
    assert recon["expectedBuys"] == [TEST_SYMBOL]
    assert recon["ok"] is True


def test_no_decision_available_is_idle_ok() -> None:
    multi = {"action": "NONE", "idlePct": 0.0, "pick": None, "note": "候选数据不足"}
    recon = _run_recon(open_rows=[], closed_rows=[], multi=multi)
    assert recon["ok"] is True
    assert recon["decisionAvailable"] is False
    assert recon["userAlignment"] == "idle"


def test_user_aligned_when_bought_on_fill_day() -> None:
    recon = _run_recon(
        open_rows=[_open_row(created=DAY)],
        closed_rows=[],
        multi=_multi(action="BUY"),
        user_rows=[_user_row("ETF:513100", "BUY", "2026-08-21")],
    )
    assert recon["userAlignment"] == "aligned"
    assert recon["userBuys"] == [TEST_SYMBOL]


def test_error_snapshot_surfaces() -> None:
    with patch(f"{_RECON}._paper_snapshot", side_effect=RuntimeError("db down")):
        recon = sleeve_paper_recon(day=DAY)
    assert recon["ok"] is False
    assert "db down" in (recon.get("error") or "")


# ---------------------------------------------------------------------------
# trading_brief wiring (mirror the satellite recon section tests)
# ---------------------------------------------------------------------------


def test_sleeve_recon_section_mismatch_emits() -> None:
    recon = {
        "day": DAY,
        "ok": False,
        "action": "BUY",
        "expectedBuys": [TEST_SYMBOL],
        "expectedSells": [],
        "paperBuysToday": [],
        "paperSellsToday": [],
        "missedBuys": [TEST_SYMBOL],
        "missedSells": [],
        "extraOpens": [],
        "userAlignment": "missing",
    }
    emit = MagicMock()
    with (
        patch(f"{_RECON}.sleeve_paper_recon", return_value=recon),
        patch("data_sync_service.db.webhook.emit_event", emit),
    ):
        sections = tb._sleeve_recon_section()
    assert sections[0]["type"] == "sleeve_recon"
    assert emit.call_count == 1
    assert emit.call_args.args[0] == "sleeve_recon_mismatch"
    assert emit.call_args.kwargs["dedupe_key"] == f"sleeve_recon:{DAY}"


def test_sleeve_recon_section_clean_no_emit() -> None:
    recon = {
        "day": DAY,
        "ok": True,
        "userAlignment": "aligned",
        "expectedBuys": [],
        "expectedSells": [],
    }
    emit = MagicMock()
    with (
        patch(f"{_RECON}.sleeve_paper_recon", return_value=recon),
        patch("data_sync_service.db.webhook.emit_event", emit),
    ):
        tb._sleeve_recon_section()
    assert emit.call_count == 0


def test_sleeve_recon_markdown_renders() -> None:
    sections = [{
        "type": "sleeve_recon",
        "day": DAY,
        "ok": False,
        "action": "BUY",
        "expectedBuys": [TEST_SYMBOL],
        "expectedSells": [],
        "missedBuys": [TEST_SYMBOL],
        "missedSells": [],
        "extraOpens": [],
        "userAlignment": "missing",
    }]
    md = tb.render_markdown(sections, "action")
    assert f"**核心纸账对账 {DAY}** 🔴有差异 · 你未执行" in md
    assert f"缺 {TEST_SYMBOL}" in md


# ---------------------------------------------------------------------------
# DB integration: real paper rows through the recon path
# ---------------------------------------------------------------------------


@pytest.mark.requires_postgres
class TestSleeveReconEndToEnd:
    # Fake candidate symbol (AGENTS.md: no real symbols in DB tests); the
    # recon module's candidate set is patched so recon treats it as a sleeve
    # leg and never collides with real book rows.
    FAKE_SYMBOL = "ETF:995131"

    @pytest.fixture(autouse=True)
    def _cleanup(self):
        yield
        from data_sync_service.db import get_connection

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM paper_trades WHERE symbol = %s "
                    "AND (why_at_entry LIKE %s OR why_at_entry LIKE %s)",
                    (self.FAKE_SYMBOL, "test sleeve leg%", "multi-sleeve%"),
                )
            conn.commit()

    def test_apply_then_recon_ok_end_to_end(self) -> None:
        from data_sync_service.db.paper_trading import insert_paper_trade
        from data_sync_service.service.sleeve_paper_auto import apply_sleeve_to_paper

        insert_paper_trade(
            symbol=self.FAKE_SYMBOL,
            entry_date="2026-08-01",
            side="BUY",
            entry_price=2.0,
            why_at_entry="test sleeve leg",
            sleeve_pct=50.0,
            source="S3",
            market="CN",
        )
        multi = _multi(action="SELL_TO_REPO", pick_symbol=self.FAKE_SYMBOL)
        with (
            patch(
                "data_sync_service.service.sleeve_paper_auto._build_multi_for_paper",
                return_value=multi,
            ),
            patch(
                "data_sync_service.service.sleeve_paper_auto.CANDIDATE_SYMBOLS",
                {self.FAKE_SYMBOL},
            ),
            patch(f"{_RECON}.CANDIDATE_SYMBOLS", {self.FAKE_SYMBOL}),
        ):
            apply_sleeve_to_paper(day=DAY)
        # Real paper rows; only the decision + candidate set are stubbed.
        # Pre-state = the leg closed today → SELL_TO_REPO reproduced, and the
        # close today is the actual → ok.
        with (
            patch(f"{_RECON}.CANDIDATE_SYMBOLS", {self.FAKE_SYMBOL}),
            patch(f"{_RECON}._rebuild_decision", return_value=multi),
            patch(f"{_RECON}._user_core_trades", return_value=[]),
        ):
            recon = sleeve_paper_recon(day=DAY)
        assert recon["ok"] is True, recon
        assert recon["expectedSells"] == [self.FAKE_SYMBOL]
        assert recon["paperSellsToday"] == [self.FAKE_SYMBOL]
