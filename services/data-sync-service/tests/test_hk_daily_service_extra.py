"""service/hk_daily.py coverage."""

from __future__ import annotations

from datetime import date
from unittest.mock import Mock

import pandas as pd

from data_sync_service.service import hk_daily as hd


class TestHelpers:
    def test_dates(self) -> None:
        assert hd._today_yyyymmdd().isdigit()
        assert hd._date_to_yyyymmdd(date(2026, 8, 7)) == "20260807"
        start = hd._backfill_start_yyyymmdd()
        assert len(start) == 8 and int(start[:4]) >= 2020


class TestTushareSyncOne:
    def test_no_code(self) -> None:
        assert hd._tushare_sync_one("") == {"ok": False, "error": "ts_code is required"}

    def test_no_key(self, monkeypatch) -> None:
        monkeypatch.setattr(hd, "get_settings", lambda: Mock(tu_share_api_key=""))
        assert hd._tushare_sync_one("00700.HK")["error"] == "TU_SHARE_API_KEY is not set"

    def test_up_to_date(self, monkeypatch) -> None:
        monkeypatch.setattr(hd, "get_settings", lambda: Mock(tu_share_api_key="k"))
        monkeypatch.setattr(hd, "get_last_trade_date", lambda code: date.today())
        out = hd._tushare_sync_one("00700.HK")
        assert out["skipped"] is True and out["updated"] == 0

    def test_backfill(self, monkeypatch) -> None:
        monkeypatch.setattr(hd, "get_settings", lambda: Mock(tu_share_api_key="k"))
        monkeypatch.setattr(hd, "get_last_trade_date", lambda code: None)
        pro = Mock()
        pro.hk_daily.return_value = pd.DataFrame([{"trade_date": "20260807", "close": 1.0}])
        monkeypatch.setattr(hd.ts, "pro_api", lambda k: pro)
        monkeypatch.setattr(hd, "upsert_from_dataframe", lambda df: 1)
        out = hd._tushare_sync_one("00700.HK")
        assert out["ok"] is True and out["updated"] == 1 and out["source"] == "tushare"
        pro.hk_daily.assert_called_once()
        assert pro.hk_daily.call_args.kwargs["ts_code"] == "00700.HK"

    def test_incremental(self, monkeypatch) -> None:
        monkeypatch.setattr(hd, "get_settings", lambda: Mock(tu_share_api_key="k"))
        monkeypatch.setattr(hd, "get_last_trade_date", lambda code: date(2026, 8, 6))
        pro = Mock()
        pro.hk_daily.return_value = pd.DataFrame()
        monkeypatch.setattr(hd.ts, "pro_api", lambda k: pro)
        out = hd._tushare_sync_one("00700.HK")
        assert out["ok"] is True and out["updated"] == 0
        assert pro.hk_daily.call_args.kwargs["start_date"] == "20260807"

    def test_error(self, monkeypatch) -> None:
        monkeypatch.setattr(hd, "get_settings", lambda: Mock(tu_share_api_key="k"))
        monkeypatch.setattr(hd, "get_last_trade_date", lambda code: None)
        pro = Mock()
        pro.hk_daily.side_effect = RuntimeError("boom")
        monkeypatch.setattr(hd.ts, "pro_api", lambda k: pro)
        out = hd._tushare_sync_one("00700.HK")
        assert out["ok"] is False and out["error"] == "boom"


class TestFallback:
    def _tx(self, monkeypatch, ok=True, updated=0):
        monkeypatch.setattr("data_sync_service.service.hk_daily_tx.sync_hk_daily_for_ts_code_tx", lambda code: {"ok": ok, "updated": updated, "error": None if ok else "tx_err", "source": "tencent"})

    def _ak(self, monkeypatch, ok=True, updated=0):
        monkeypatch.setattr("data_sync_service.service.hk_daily_ak.sync_hk_daily_for_ts_code_ak", lambda code: {"ok": ok, "updated": updated, "error": None if ok else "ak_err", "source": "akshare"})

    def _yf(self, monkeypatch, ok=True, updated=0):
        monkeypatch.setattr("data_sync_service.service.hk_daily_yf.sync_hk_daily_for_ts_code_yf", lambda code: {"ok": ok, "updated": updated, "error": None if ok else "yf_err", "source": "yfinance"})

    def test_tencent_first(self, monkeypatch) -> None:
        self._tx(monkeypatch, updated=5)
        out = hd._sync_one_with_fallback("00700.HK")
        assert out["source"] == "tencent" and out["updated"] == 5

    def test_akshare_darwin_disabled(self, monkeypatch) -> None:
        self._tx(monkeypatch, ok=False)
        self._yf(monkeypatch, updated=3)
        monkeypatch.setattr(hd.sys, "platform", "darwin")
        monkeypatch.setattr(hd, "_tushare_sync_one", lambda code: {"ok": False, "error": "no key"})
        out = hd._sync_one_with_fallback("00700.HK")
        assert out["source"] == "yfinance"

    def test_akshare_linux(self, monkeypatch) -> None:
        self._tx(monkeypatch, ok=False)
        self._ak(monkeypatch, updated=4)
        monkeypatch.setattr(hd.sys, "platform", "linux")
        out = hd._sync_one_with_fallback("00700.HK")
        assert out["source"] == "akshare" and out["updated"] == 4

    def test_tushare_last(self, monkeypatch) -> None:
        self._tx(monkeypatch, ok=False)
        self._ak(monkeypatch, ok=False)
        self._yf(monkeypatch, ok=False)
        monkeypatch.setattr(hd.sys, "platform", "linux")
        monkeypatch.setattr(hd, "_tushare_sync_one", lambda code: {"ok": True, "updated": 2, "source": "tushare", "ts_code": code})
        out = hd._sync_one_with_fallback("00700.HK")
        assert out["source"] == "tushare" and out["updated"] == 2

    def test_nothing_tencent_failed(self, monkeypatch) -> None:
        self._tx(monkeypatch, ok=False)
        self._ak(monkeypatch, ok=False)
        self._yf(monkeypatch, ok=False)
        monkeypatch.setattr(hd.sys, "platform", "linux")
        monkeypatch.setattr(hd, "_tushare_sync_one", lambda code: {"ok": False, "error": "tushare_err"})
        out = hd._sync_one_with_fallback("00700.HK")
        assert out["skipped"] is True and out["source"] == "tencent"
        assert "tencent failed: tx_err" in out["message"]

    def test_nothing_tencent_ok_ak_failed(self, monkeypatch) -> None:
        self._tx(monkeypatch, ok=True, updated=0)
        self._ak(monkeypatch, ok=False)
        self._yf(monkeypatch, ok=False)
        monkeypatch.setattr(hd.sys, "platform", "linux")
        monkeypatch.setattr(hd, "_tushare_sync_one", lambda code: {"ok": False, "error": "x"})
        out = hd._sync_one_with_fallback("00700.HK")
        assert out["skipped"] is True and out["source"] == "akshare"
        assert "akshare failed: ak_err" in out["message"]

    def test_nothing_all_ok_empty(self, monkeypatch) -> None:
        self._tx(monkeypatch, ok=True, updated=0)
        self._ak(monkeypatch, ok=True, updated=0)
        self._yf(monkeypatch, ok=True, updated=0)
        monkeypatch.setattr(hd.sys, "platform", "linux")
        monkeypatch.setattr(hd, "_tushare_sync_one", lambda code: {"ok": True, "updated": 0})
        out = hd._sync_one_with_fallback("00700.HK")
        assert out["message"] == "no source delivered new bars"


class TestFull:
    def test_skipped(self, monkeypatch) -> None:
        monkeypatch.setattr(hd, "get_today_run", lambda job: {"success": True})
        assert hd.sync_hk_daily_full() == {"ok": True, "skipped": True, "message": "already synced today"}

    def test_no_stocks(self, monkeypatch) -> None:
        monkeypatch.setattr(hd, "get_today_run", lambda job: None)
        monkeypatch.setattr(hd, "fetch_ts_codes_by_market", lambda market: [])
        assert hd.sync_hk_daily_full() == {"ok": True, "updated": 0, "message": "no HK stock list"}

    def test_full(self, monkeypatch) -> None:
        monkeypatch.setattr(hd, "get_today_run", lambda job: None)
        monkeypatch.setattr(hd, "fetch_ts_codes_by_market", lambda market: ["00700.HK", "00941.HK", "09988.HK"])
        results = [
            {"ok": True, "updated": 5, "source": "tencent"},
            {"ok": True, "updated": 0, "source": "tencent"},
            {"ok": True, "updated": 2, "source": "yfinance"},
        ]
        monkeypatch.setattr(hd, "_sync_one_with_fallback", lambda code: results.pop(0))
        monkeypatch.setattr(hd.time, "sleep", lambda s: None)
        seen = {}
        monkeypatch.setattr(hd, "insert_record", lambda **kw: seen.update(kw))
        out = hd.sync_hk_daily_full()
        assert out["ok"] is True and out["updated"] == 7
        assert out["skipped_count"] == 1 and out["failed_count"] == 0
        assert seen["success"] is True

    def test_full_resume(self, monkeypatch) -> None:
        monkeypatch.setattr(hd, "get_today_run", lambda job: {"success": False, "last_ts_code": "00700.HK"})
        monkeypatch.setattr(hd, "fetch_ts_codes_by_market", lambda market: ["00700.HK", "00941.HK"])
        called = []
        monkeypatch.setattr(hd, "_sync_one_with_fallback", lambda code: called.append(code) or {"ok": True, "updated": 0})
        monkeypatch.setattr(hd.time, "sleep", lambda s: None)
        monkeypatch.setattr(hd, "insert_record", lambda **kw: None)
        hd.sync_hk_daily_full()
        assert called == ["00941.HK"]

    def test_full_exception(self, monkeypatch) -> None:
        monkeypatch.setattr(hd, "get_today_run", lambda job: None)
        monkeypatch.setattr(hd, "fetch_ts_codes_by_market", lambda market: ["00700.HK", "00941.HK"])
        calls = {"n": 0}
        def fallback(code):
            calls["n"] += 1
            if calls["n"] == 1:
                return {"ok": True, "updated": 0}
            raise RuntimeError("boom")

        monkeypatch.setattr(hd, "_sync_one_with_fallback", fallback)
        monkeypatch.setattr(hd.time, "sleep", lambda s: None)
        monkeypatch.setattr(hd, "insert_record", lambda **kw: None)
        out = hd.sync_hk_daily_full()
        assert out["failed_count"] == 1

    def test_resume_unknown_code(self, monkeypatch) -> None:
        monkeypatch.setattr(hd, "get_today_run", lambda job: {"success": False, "last_ts_code": "NOPE.HK"})
        monkeypatch.setattr(hd, "fetch_ts_codes_by_market", lambda market: ["00700.HK"])
        called = []
        monkeypatch.setattr(hd, "_sync_one_with_fallback", lambda code: called.append(code) or {"ok": True, "updated": 0})
        monkeypatch.setattr(hd.time, "sleep", lambda s: None)
        monkeypatch.setattr(hd, "insert_record", lambda **kw: None)
        hd.sync_hk_daily_full()
        assert called == ["00700.HK"]

    def test_status(self, monkeypatch) -> None:
        monkeypatch.setattr(hd, "get_today_run", lambda job: None)
        assert hd.get_hk_daily_sync_status() == {"job_type": "hk_daily_full", "today_run": None}
        monkeypatch.setattr(hd, "get_today_run", lambda job: {"success": True})
        assert hd.get_hk_daily_sync_status()["today_run"] == {"success": True}

    def test_single_ts_code(self, monkeypatch) -> None:
        monkeypatch.setattr(hd, "_sync_one_with_fallback", lambda code: {"ok": True, "updated": 1})
        assert hd.sync_hk_daily_for_ts_code("00700.HK")["updated"] == 1
