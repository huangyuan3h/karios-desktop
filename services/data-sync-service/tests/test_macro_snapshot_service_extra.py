"""service/macro_snapshot.py coverage."""

from __future__ import annotations

import pytest

from data_sync_service.service import macro_snapshot as ms
from data_sync_service.service.macro_daily import (
    SID_510300_PUT_IV,
    SID_IXIC,
    SID_USDCNH,
)


class TestHelpers:
    def test_safe_float(self) -> None:
        assert ms._safe_float(1.5) == 1.5
        assert ms._safe_float("2.5") == 2.5
        assert ms._safe_float(float("nan")) is None
        assert ms._safe_float(float("inf")) is None
        assert ms._safe_float("bad") is None
        assert ms._safe_float(None) is None

    def test_ma(self) -> None:
        assert ms._ma([1.0, 2.0], 5) is None
        assert ms._ma([1.0, 2.0, 3.0, 4.0, 5.0], 5) == 3.0
        assert ms._ma([1.0] * 10, 5) == 1.0

    def test_stale(self, monkeypatch) -> None:
        monkeypatch.setattr("data_sync_service.service.macro_snapshot_on_demand._is_data_stale", lambda d: True)
        assert ms._macro_as_of_stale("2020-01-01")


class TestItemFromDb:
    def test_from_db(self, monkeypatch) -> None:
        closes = [("2026-08-06", 99.0), ("2026-08-07", 100.0)]
        latest = {"pct_chg": "2.5", "source": "src", "underlying_ts_code": "IXIC"}
        item = ms._macro_item_from_db({"seriesId": SID_IXIC, "name": "N", "category": "us_tech", "why": "w"}, closes, latest)
        assert item["close"] == 100.0
        assert item["pctChg"] == 2.5
        assert item["ma5"] is None
        assert item["source"] == "src"

    def test_computed_pct(self, monkeypatch) -> None:
        closes = [("a", 100.0), ("b", 110.0)]
        monkeypatch.setattr(ms, "get_latest_row", lambda sid: {"pct_chg": None, "source": "s", "underlying_ts_code": "u"})
        item = ms._macro_item_from_db({"seriesId": "X", "name": "N", "category": "c"}, closes)
        assert item["pctChg"] == pytest.approx(10.0)

    def test_fetch_latest(self, monkeypatch) -> None:
        closes = [("a", 1.0), ("b", 2.0)]
        monkeypatch.setattr(ms, "get_latest_row", lambda sid: None)
        item = ms._macro_item_from_db({"seriesId": "X", "name": "N", "category": "c"}, closes)
        assert item["source"] is None

    def test_bad_closes(self) -> None:
        closes = [("a", float("nan")), ("b", 2.0), ("c", None)]
        item = ms._macro_item_from_db({"seriesId": "X", "name": "N", "category": "c"}, closes, {"pct_chg": None, "source": "s", "underlying_ts_code": "u"})
        assert item["close"] == 2.0
        assert item["ma5"] is None and item["ma20"] is None


class TestBackfill:
    def test_has_pct(self) -> None:
        m = {"pctChg": 1.0, "seriesId": "X"}
        ms._backfill_macro_pct_chg(m, [("a", 1.0), ("b", 2.0)])
        assert m["pctChg"] == 1.0

    def test_no_close(self) -> None:
        m = {"pctChg": None, "seriesId": "X", "close": None}
        ms._backfill_macro_pct_chg(m, [("a", 1.0)])
        assert m.get("pctChg") is None

    def test_short_hist(self, monkeypatch) -> None:
        m = {"pctChg": None, "seriesId": "X", "close": 1.0, "realtime": False}
        monkeypatch.setattr(ms, "fetch_last_closes", lambda sid, days: [("a", 1.0)])
        ms._backfill_macro_pct_chg(m)
        assert m.get("pctChg") is None

    def test_bad_hist(self, monkeypatch) -> None:
        m = {"pctChg": None, "seriesId": "X", "close": 1.0, "realtime": False}
        monkeypatch.setattr(ms, "fetch_last_closes", lambda sid, days: [("a", float("nan")), ("b", 0.0)])
        ms._backfill_macro_pct_chg(m)
        assert m.get("pctChg") is None

    def test_realtime_pct(self, monkeypatch) -> None:
        m = {"pctChg": None, "seriesId": "X", "close": 110.0, "realtime": True}
        monkeypatch.setattr(ms, "fetch_last_closes", lambda sid, days: [("a", 100.0), ("b", 105.0)])
        ms._backfill_macro_pct_chg(m)
        assert m["pctChg"] == pytest.approx(5.0 / 105.0 * 100.0)

    def test_eod_pct(self, monkeypatch) -> None:
        m = {"pctChg": None, "seriesId": "X", "close": 110.0, "realtime": False}
        monkeypatch.setattr(ms, "fetch_last_closes", lambda sid, days: [("a", 100.0), ("b", 110.0)])
        ms._backfill_macro_pct_chg(m)
        assert m["pctChg"] == pytest.approx(10.0)


class TestBuild:
    def _patch(self, monkeypatch, *, closes=None, latest=None, resolved=None, quotes=None, enrich=None, window=False, warning=None):
        monkeypatch.setattr(ms, "ensure_table", lambda: None)
        monkeypatch.setattr(ms, "get_index_signals", lambda **kw: [{"k": "v"}])
        monkeypatch.setattr(ms, "fetch_last_closes_batch", lambda sids, days: closes or {})
        monkeypatch.setattr(ms, "get_latest_rows_batch", lambda sids: latest or {})
        monkeypatch.setattr(ms, "resolve_put_iv_for_snapshot", lambda **kw: resolved or {"close": 20.0, "pctChg": 1.0, "asOfDate": "d", "source": "s", "underlyingTsCode": "u", "realtime": False, "signal": "neutral", "signalLabel": "N", "warning": warning, "diagnostics": {}})
        monkeypatch.setattr(ms, "fetch_realtime_quotes", lambda codes: quotes if quotes is not None else {"ok": True, "items": []})
        monkeypatch.setattr(ms, "_trade_date_from_trade_time", lambda tt: "2026-08-07" if tt else None)
        monkeypatch.setattr(ms, "enrich_macro_items_on_demand", lambda items: enrich(items) if enrich else items)
        monkeypatch.setattr(ms, "shanghai_today", lambda: __import__("datetime").date(2026, 8, 7))
        monkeypatch.setattr(ms, "_is_shanghai_sync_window", lambda: window)
        monkeypatch.setattr(ms, "get_latest_etf_flow_date", lambda: "2026-08-07")
        monkeypatch.setattr(ms, "build_etf_fund_flow_bundle", lambda **kw: {"items": []})
        monkeypatch.setattr(ms, "build_etf_flow_signal", lambda **kw: {"verdict": "neutral"})
        monkeypatch.setattr(ms, "macro_snapshot_warning", lambda: None)
        monkeypatch.setattr(ms, "_macro_as_of_stale", lambda d: False)
        monkeypatch.setattr(ms, "classify_iv_signal", lambda **kw: ("neutral", "N"))
        monkeypatch.setattr(ms, "PUT_IV_LIVE_FETCH_FAILED_USING_DB", "LIVE_FAIL")

    def test_empty_db(self, monkeypatch) -> None:
        self._patch(monkeypatch)
        out = ms.build_macro_snapshot()
        empty = [m for m in out["macro"] if m["close"] is None]
        assert len(empty) == len(ms.MACRO_CARDS) - 1
        assert out["cnIndexSignals"] == [{"k": "v"}]
        assert out["etfFundFlow"] == {"items": []}

    def test_with_data_and_realtime(self, monkeypatch) -> None:
        closes = {
            SID_IXIC: [("2026-08-06", 100.0), ("2026-08-07", 105.0)],
            SID_USDCNH: [("2026-08-06", 7.0), ("2026-08-07", 7.1)],
            SID_510300_PUT_IV: [("2026-08-07", 20.0)],
        }
        latest = {
            SID_IXIC: {"pct_chg": None, "source": "index_global", "underlying_ts_code": "IXIC"},
            SID_USDCNH: {"pct_chg": None, "source": "fx", "underlying_ts_code": "USDCNH.FXCM"},
            SID_510300_PUT_IV: {"pct_chg": None, "source": "opt", "underlying_ts_code": "opt"},
            "A50": {"pct_chg": None, "source": "fut", "underlying_ts_code": "FTXA50"},
            "COMM_ENERGY": {"pct_chg": None, "source": "fut", "underlying_ts_code": ""},
        }
        quotes = {"ok": True, "items": [
            {"ts_code": "IXIC", "price": 110.0, "pct_chg": 1.5, "trade_time": "2026-08-07 04:00:00"},
            {"ts_code": "USDCNH.FXCM", "price": 7.2, "pct_chg": None, "pre_close": 7.1, "trade_time": None},
            {"ts_code": "unknown", "price": 1.0, "pct_chg": 1.0},
            "not-a-dict",
        ]}
        self._patch(monkeypatch, closes=closes, latest=latest, quotes=quotes)
        out = ms.build_macro_snapshot(cn_index_signals=[{"a": 1}])
        ix = next(m for m in out["macro"] if m["seriesId"] == SID_IXIC)
        assert ix["realtime"] is True
        assert ix["close"] == 110.0
        assert ix["quotePrice"] == 110.0
        assert ix["asOfDate"] == "2026-08-07"
        iv = next(m for m in out["macro"] if m["seriesId"] == SID_510300_PUT_IV)
        assert iv["signal"] == "neutral"
        assert iv["unit"] == "%"
        assert iv["warning"] is None
        usdcnh = next(m for m in out["macro"] if m["seriesId"] == SID_USDCNH)
        assert usdcnh["realtime"] is True
        assert usdcnh["pctChg"] == pytest.approx(0.1 / 7.1 * 100.0)

    def test_put_iv_warning_filtered(self, monkeypatch) -> None:
        self._patch(monkeypatch, resolved={"close": 20.0, "pctChg": None, "asOfDate": "d", "source": "s", "underlyingTsCode": "u", "realtime": True, "signal": None, "signalLabel": None, "warning": "LIVE_FAIL", "diagnostics": "not-dict"})
        out = ms.build_macro_snapshot()
        assert "warnings" not in out

    def test_put_iv_other_warning(self, monkeypatch) -> None:
        self._patch(monkeypatch, resolved={"close": 20.0, "pctChg": None, "asOfDate": "d", "source": "s", "underlyingTsCode": "u", "realtime": True, "signal": None, "signalLabel": None, "warning": "other warning", "diagnostics": {}})
        out = ms.build_macro_snapshot()
        assert "other warning" in out["warning"]

    def test_stale_warning(self, monkeypatch) -> None:
        closes = {SID_IXIC: [("2026-08-06", 100.0), ("2026-08-07", 105.0)]}
        latest = {SID_IXIC: {"pct_chg": None, "source": "s", "underlying_ts_code": "IXIC"}}
        self._patch(monkeypatch, closes=closes, latest=latest)
        monkeypatch.setattr(ms, "_macro_as_of_stale", lambda d: True)
        out = ms.build_macro_snapshot()
        assert "macro_data_stale: IXIC" in out["warning"]

    def test_warning_from_macro(self, monkeypatch) -> None:
        self._patch(monkeypatch)
        monkeypatch.setattr(ms, "macro_snapshot_warning", lambda: "no tushare key")
        out = ms.build_macro_snapshot()
        assert "no tushare key" in out["warning"]

    def test_quote_pct_fallback(self, monkeypatch) -> None:
        closes = {SID_IXIC: [("a", 100.0), ("b", 105.0)]}
        latest = {SID_IXIC: {"pct_chg": None, "source": "s", "underlying_ts_code": "IXIC"}}
        quotes = {"ok": True, "items": [{"ts_code": "IXIC", "price": 110.0, "pct_chg": None, "pre_close": 100.0, "trade_time": None}]}
        self._patch(monkeypatch, closes=closes, latest=latest, quotes=quotes)
        out = ms.build_macro_snapshot()
        ix = next(m for m in out["macro"] if m["seriesId"] == SID_IXIC)
        assert ix["pctChg"] == pytest.approx(10.0)
        assert ix["tradeTime"] is None

    def test_enrich(self, monkeypatch) -> None:
        def enrich_fn(items):
            for m in items:
                if m["seriesId"] == SID_IXIC:
                    m["close"] = 1.0
            return items

        self._patch(monkeypatch, enrich=enrich_fn)
        out = ms.build_macro_snapshot()
        ix = next(m for m in out["macro"] if m["seriesId"] == SID_IXIC)
        assert ix["close"] == 1.0

    def test_macro_as_of_none_no_stale(self, monkeypatch) -> None:
        self._patch(monkeypatch)
        out = ms.build_macro_snapshot()
        assert "warnings" not in out
