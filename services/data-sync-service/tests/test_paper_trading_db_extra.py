"""db/paper_trading.py remaining branches (mocked DB)."""

from __future__ import annotations

from unittest.mock import Mock

from data_sync_service.db import paper_trading as pt


def _fake_conn(cur: Mock) -> Mock:
    conn = Mock()
    conn.cursor.return_value = cur
    conn.__enter__ = Mock(return_value=conn)
    conn.__exit__ = Mock(return_value=False)
    cur.__enter__ = Mock(return_value=cur)
    cur.__exit__ = Mock(return_value=False)
    return conn


class TestInsertPaperTrade:
    def test_bad_side(self) -> None:

        try:
            pt.insert_paper_trade(symbol="CN:600519", entry_date="2026-08-07", side="SELL", entry_price=10.0, market="CN")
            raise AssertionError("should raise")
        except ValueError as e:
            assert "side must be one of" in str(e)

    def test_bad_source(self) -> None:
        try:
            pt.insert_paper_trade(symbol="CN:600519", entry_date="2026-08-07", side="BUY", entry_price=10.0, source="hack", market="CN")
            raise AssertionError("should raise")
        except ValueError as e:
            assert "source must be one of" in str(e)

    def test_bad_market(self) -> None:
        try:
            pt.insert_paper_trade(symbol="CN:600519", entry_date="2026-08-07", side="BUY", entry_price=10.0, market="US")
            raise AssertionError("should raise")
        except ValueError as e:
            assert "market must be one of" in str(e)

    def test_row_none_returns_none(self, monkeypatch) -> None:
        cur = Mock()
        cur.fetchone.return_value = None
        monkeypatch.setattr(pt, "get_connection", lambda: _fake_conn(cur))
        assert pt.insert_paper_trade(symbol="CN:600519", entry_date="2026-08-07", side="BUY", entry_price=10.0, market="CN") is None

    def test_ok(self, monkeypatch) -> None:
        cur = Mock()
        cur.fetchone.return_value = {
            "id": "x", "symbol": "CN:600519", "entry_date": "2026-08-07", "side": "BUY",
            "entry_price": 10.0, "status": "open", "source": None, "market": "CN",
            "pnl_pct": None, "gross_pnl_pct": None, "costs_pct": None, "close_reason": None,
            "holding_days": None, "close_date": None, "close_price": None,
            "score_at_entry": None, "why_at_entry": None, "sleeve_pct": None,
            "created_at": None, "updated_at": None,
        }
        monkeypatch.setattr(pt, "get_connection", lambda: _fake_conn(cur))
        out = pt.insert_paper_trade(symbol="CN:600519", entry_date="2026-08-07", side="BUY", entry_price=10.0, market="CN")
        assert out["symbol"] == "CN:600519"


class TestListPaperTrades:
    def test_market_filter(self, monkeypatch) -> None:
        cur = Mock()
        cur.fetchall.return_value = []
        monkeypatch.setattr(pt, "get_connection", lambda: _fake_conn(cur))
        pt.list_paper_trades(market="HK")
        sql = cur.execute.call_args_list[-1][0][0]
        assert "market = %s" in sql

    def test_bad_market(self) -> None:
        try:
            pt.list_paper_trades(market="US")
            raise AssertionError("should raise")
        except ValueError as e:
            assert "market must be one of" in str(e)

    def test_no_filters(self, monkeypatch) -> None:
        cur = Mock()
        cur.fetchall.return_value = []
        monkeypatch.setattr(pt, "get_connection", lambda: _fake_conn(cur))
        pt.list_paper_trades()
        sql = cur.execute.call_args_list[-1][0][0]
        assert "market" not in sql


class TestGetOpenPaperTrades:
    def test_ok(self, monkeypatch) -> None:
        cur = Mock()
        cur.fetchall.return_value = [{"id": "t1", "symbol": "CN:600519", "status": "open"}]
        monkeypatch.setattr(pt, "get_connection", lambda: _fake_conn(cur))
        out = pt.get_open_paper_trades()
        assert out[0]["id"] == "t1"

    def test_empty(self, monkeypatch) -> None:
        cur = Mock()
        cur.fetchall.return_value = []
        monkeypatch.setattr(pt, "get_connection", lambda: _fake_conn(cur))
        assert pt.get_open_paper_trades() == []


class TestUpdateAndClose:
    def test_update_returns_none_when_not_open(self, monkeypatch) -> None:
        cur = Mock()
        cur.fetchone.return_value = None
        monkeypatch.setattr(pt, "get_connection", lambda: _fake_conn(cur))
        assert pt.update_paper_trade_price(trade_id="t1", close_price=10.0, pnl_pct=1.0, holding_days=1) is None

    def test_close_bad_reason(self) -> None:
        try:
            pt.close_paper_trade(trade_id="t1", close_date="2026-08-07", close_price=10.0, pnl_pct=1.0, holding_days=1, close_reason="hack")
            raise AssertionError("should raise")
        except ValueError as e:
            assert "close_reason must be one of" in str(e)

    def test_close_ok(self, monkeypatch) -> None:
        cur = Mock()
        cur.fetchone.return_value = {"id": "t1", "symbol": "CN:600519", "status": "closed"}
        monkeypatch.setattr(pt, "get_connection", lambda: _fake_conn(cur))
        out = pt.close_paper_trade(trade_id="t1", close_date="2026-08-07", close_price=9.0, pnl_pct=-5.0, holding_days=6, close_reason="stop_hit", gross_pnl_pct=-4.0, costs_pct=1.0)
        assert out["status"] == "closed"


class TestStats:
    def test_count_since_empty(self, monkeypatch) -> None:
        cur = Mock()
        cur.fetchone.return_value = None
        monkeypatch.setattr(pt, "get_connection", lambda: _fake_conn(cur))
        assert pt.count_since("2026-08-01") == (0, 0)

    def test_count_since_ok(self, monkeypatch) -> None:
        cur = Mock()
        cur.fetchone.return_value = {"total": 10, "wins": 7}
        monkeypatch.setattr(pt, "get_connection", lambda: _fake_conn(cur))
        assert pt.count_since("2026-08-01") == (10, 7)

    def test_avg_none(self, monkeypatch) -> None:
        cur = Mock()
        cur.fetchone.return_value = None
        monkeypatch.setattr(pt, "get_connection", lambda: _fake_conn(cur))
        assert pt.avg_pnl_pct_since("2026-08-01") is None

    def test_avg_ok(self, monkeypatch) -> None:
        cur = Mock()
        cur.fetchone.return_value = {"avg": 2.5}
        monkeypatch.setattr(pt, "get_connection", lambda: _fake_conn(cur))
        assert pt.avg_pnl_pct_since("2026-08-01") == 2.5


class TestRowShaping:
    def test_float_bad(self) -> None:
        assert pt._float("abc") is None
        assert pt._float(None) is None

    def test_int_bad(self) -> None:
        assert pt._int("abc") is None
        assert pt._int(None) is None

    def test_iso_timestamp(self) -> None:
        assert pt._iso_timestamp(None) is None
        assert pt._iso_timestamp("2026-08-07T00:00:00") == "2026-08-07T00:00:00"
