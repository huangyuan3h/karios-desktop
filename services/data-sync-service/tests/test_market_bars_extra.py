"""service/market_bars.py coverage."""

from __future__ import annotations

from unittest.mock import Mock

from data_sync_service.service import market_bars as mb


class TestParseSymbolRemaining:
    def test_etf_sh(self) -> None:
        assert mb._parse_symbol("ETF:510300") == ("ETF", "510300", "510300.SH")

    def test_etf_sz(self) -> None:
        assert mb._parse_symbol("ETF:159915") == ("ETF", "159915", "159915.SZ")

    def test_etf_invalid(self) -> None:
        assert mb._parse_symbol("ETF:12345") is None
        assert mb._parse_symbol("ETF:") is None

    def test_cn_short_ticker(self) -> None:
        assert mb._parse_symbol("CN:123") is None

    def test_hk_too_long(self) -> None:
        assert mb._parse_symbol("HK:123456") is None

    def test_ts_code_non_alpha_suffix(self) -> None:
        assert mb._parse_symbol("000001.9Z") is None

    def test_hk_ts_code_short(self) -> None:
        assert mb._parse_symbol("0700.HK") is None


class TestLookupName:
    def test_found(self, monkeypatch) -> None:
        cur = Mock()
        cur.fetchone.return_value = ("平安银行",)
        conn = Mock()
        conn.cursor.return_value = cur
        conn.__enter__ = Mock(return_value=conn)
        conn.__exit__ = Mock(return_value=False)
        cur.__enter__ = Mock(return_value=cur)
        cur.__exit__ = Mock(return_value=False)
        monkeypatch.setattr("data_sync_service.db.get_connection", lambda: conn)
        assert mb._lookup_name("000001.SZ") == "平安银行"

    def test_empty_name(self, monkeypatch) -> None:
        cur = Mock()
        cur.fetchone.return_value = (None,)
        conn = Mock()
        conn.cursor.return_value = cur
        conn.__enter__ = Mock(return_value=conn)
        conn.__exit__ = Mock(return_value=False)
        cur.__enter__ = Mock(return_value=cur)
        cur.__exit__ = Mock(return_value=False)
        monkeypatch.setattr("data_sync_service.db.get_connection", lambda: conn)
        assert mb._lookup_name("000001.SZ") is None

    def test_db_error(self, monkeypatch) -> None:
        monkeypatch.setattr(mb, "ensure_stock_basic", lambda: (_ for _ in ()).throw(RuntimeError("boom")))
        assert mb._lookup_name("000001.SZ") is None


class TestGetMarketBars:
    def test_invalid_symbol(self, monkeypatch) -> None:
        out = mb.get_market_bars("BOGUS", force=True)
        assert out == {"symbol": "BOGUS", "market": "", "ticker": "", "name": "", "currency": "", "bars": []}

    def test_cn_no_force(self, monkeypatch) -> None:
        called = []
        monkeypatch.setattr(mb, "_lookup_name", lambda ts_code: called.append(ts_code) or "平安银行")
        monkeypatch.setattr(mb, "fetch_last_bars", lambda ts_code, days: [{"date": "2024-01-01"}])
        out = mb.get_market_bars("CN:000001", days=30)
        assert out["market"] == "CN" and out["currency"] == "CNY" and out["name"] == "平安银行"
        assert out["bars"] == [{"date": "2024-01-01"}] and called == ["000001.SZ"]

    def test_cn_force_sync(self, monkeypatch) -> None:
        called = []
        monkeypatch.setattr(mb, "sync_daily_for_ts_code", lambda ts_code: called.append(ts_code))
        monkeypatch.setattr(mb, "_lookup_name", lambda ts_code: None)
        monkeypatch.setattr(mb, "fetch_last_bars", lambda ts_code, days: [])
        out = mb.get_market_bars("CN:000001", force=True)
        assert called == ["000001.SZ"] and out["name"] == "000001"

    def test_hk_force_yf_ok(self, monkeypatch) -> None:
        calls = []
        monkeypatch.setattr(mb, "sync_hk_daily_for_ts_code_yf", lambda ts_code: calls.append("yf") or {"ok": True, "updated": 5})
        monkeypatch.setattr(mb, "sync_hk_daily_for_ts_code", lambda ts_code: calls.append("ts"))
        monkeypatch.setattr(mb, "_lookup_name", lambda ts_code: None)
        monkeypatch.setattr(mb, "fetch_last_bars", lambda ts_code, days: [])
        out = mb.get_market_bars("HK:00700", force=True)
        assert calls == ["yf"] and out["currency"] == "HKD"

    def test_hk_force_yf_empty_falls_back(self, monkeypatch) -> None:
        calls = []
        monkeypatch.setattr(mb, "sync_hk_daily_for_ts_code_yf", lambda ts_code: calls.append("yf") or {"ok": True, "updated": 0})
        monkeypatch.setattr(mb, "sync_hk_daily_for_ts_code", lambda ts_code: calls.append("ts") or {"ok": False})
        monkeypatch.setattr(mb, "_lookup_name", lambda ts_code: None)
        monkeypatch.setattr(mb, "fetch_last_bars", lambda ts_code, days: [])
        mb.get_market_bars("HK:00700", force=True)
        assert calls == ["yf", "ts"]

    def test_hk_force_yf_error_falls_back(self, monkeypatch) -> None:
        calls = []
        monkeypatch.setattr(mb, "sync_hk_daily_for_ts_code_yf", lambda ts_code: calls.append("yf") or {"ok": False})
        monkeypatch.setattr(mb, "sync_hk_daily_for_ts_code", lambda ts_code: calls.append("ts") or {"ok": True, "updated": 3})
        monkeypatch.setattr(mb, "_lookup_name", lambda ts_code: None)
        monkeypatch.setattr(mb, "fetch_last_bars", lambda ts_code, days: [])
        out = mb.get_market_bars("HK:00700", force=True)
        assert calls == ["yf", "ts"] and out["ticker"] == "00700"

    def test_etf_force_sync(self, monkeypatch) -> None:
        called = []
        monkeypatch.setattr(mb, "sync_etf_daily_for_ts_code", lambda ts_code: called.append(ts_code))
        monkeypatch.setattr(mb, "_lookup_name", lambda ts_code: "沪深300ETF")
        monkeypatch.setattr(mb, "fetch_last_bars", lambda ts_code, days: [])
        out = mb.get_market_bars("ETF:510300", force=True)
        assert called == ["510300.SH"] and out["currency"] == "CNY"
