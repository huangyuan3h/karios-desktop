"""service/index_daily.py, index_basic.py, stock_basic.py coverage (pure mocks)."""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

import pandas as pd

from data_sync_service.service import index_basic, index_daily, stock_basic


def _today() -> str:
    return datetime.now(UTC).strftime("%Y%m%d")


def _future_date() -> str:
    return datetime.now(UTC).strftime("%Y1231")


def _settings() -> SimpleNamespace:
    return SimpleNamespace(tu_share_api_key="test-key")


class TestIndexDaily:
    def test_skips_when_today_succeeded(self, monkeypatch) -> None:
        monkeypatch.setattr(index_daily, "get_today_run", lambda job_type: {"success": True})
        monkeypatch.setattr(index_daily, "get_settings", _settings)
        out = index_daily.sync_index_daily_full()
        assert out == {"ok": True, "skipped": True, "message": "already synced today"}

    def test_no_index_list(self, monkeypatch) -> None:
        monkeypatch.setattr(index_daily, "get_today_run", lambda job_type: None)
        monkeypatch.setattr(index_daily, "INDEX_CODES", [])
        out = index_daily.sync_index_daily_full()
        assert out["ok"] is True and out["updated"] == 0

    def test_missing_api_key(self, monkeypatch) -> None:
        monkeypatch.setattr(index_daily, "get_today_run", lambda job_type: None)
        monkeypatch.setattr(index_daily, "get_settings", lambda: SimpleNamespace(tu_share_api_key=None))
        out = index_daily.sync_index_daily_full()
        assert out["ok"] is False and "TU_SHARE_API_KEY" in out["error"]

    def test_full_success(self, monkeypatch) -> None:
        monkeypatch.setattr(index_daily, "get_today_run", lambda job_type: None)
        monkeypatch.setattr(index_daily, "get_settings", _settings)
        monkeypatch.setattr(index_daily, "get_last_trade_date", lambda ts_code: None)
        monkeypatch.setattr(index_daily, "upsert_from_dataframe", lambda df: len(df))
        pro = SimpleNamespace(index_daily=lambda **kw: pd.DataFrame([{"ts_code": "000001.SH", "trade_date": _today()}]))
        monkeypatch.setattr(index_daily.ts, "pro_api", lambda key: pro)
        records = []
        monkeypatch.setattr(index_daily, "insert_record", lambda **kw: records.append(kw))
        out = index_daily.sync_index_daily_full()
        assert out["ok"] is True and out["updated"] == len(index_daily.INDEX_CODES)
        assert records[-1]["job_type"] == index_daily.JOB_TYPE and records[-1]["success"] is True

    def test_empty_df_no_rows(self, monkeypatch) -> None:
        monkeypatch.setattr(index_daily, "get_today_run", lambda job_type: None)
        monkeypatch.setattr(index_daily, "get_settings", _settings)
        monkeypatch.setattr(index_daily, "get_last_trade_date", lambda ts_code: None)
        monkeypatch.setattr(index_daily, "upsert_from_dataframe", lambda df: len(df))
        pro = SimpleNamespace(index_daily=lambda **kw: pd.DataFrame())
        monkeypatch.setattr(index_daily.ts, "pro_api", lambda key: pro)
        monkeypatch.setattr(index_daily, "insert_record", lambda **kw: None)
        out = index_daily.sync_index_daily_full()
        assert out == {"ok": True, "updated": 0}

    def test_resume_from_failed_run(self, monkeypatch) -> None:
        calls = []
        monkeypatch.setattr(index_daily, "get_today_run", lambda job_type: {"success": False, "last_ts_code": "000001.SH"})
        monkeypatch.setattr(index_daily, "get_settings", _settings)
        monkeypatch.setattr(index_daily, "get_last_trade_date", lambda ts_code: None)
        monkeypatch.setattr(index_daily, "upsert_from_dataframe", lambda df: len(df))
        pro = SimpleNamespace(index_daily=lambda **kw: calls.append(kw["ts_code"]) or pd.DataFrame())
        monkeypatch.setattr(index_daily.ts, "pro_api", lambda key: pro)
        monkeypatch.setattr(index_daily, "insert_record", lambda **kw: None)
        index_daily.sync_index_daily_full()
        assert "000001.SH" not in calls and len(calls) == len(index_daily.INDEX_CODES) - 1

    def test_resume_unknown_last_code(self, monkeypatch) -> None:
        calls = []
        monkeypatch.setattr(index_daily, "get_today_run", lambda job_type: {"success": False, "last_ts_code": "nope.SH"})
        monkeypatch.setattr(index_daily, "get_settings", _settings)
        monkeypatch.setattr(index_daily, "get_last_trade_date", lambda ts_code: None)
        monkeypatch.setattr(index_daily, "upsert_from_dataframe", lambda df: len(df))
        pro = SimpleNamespace(index_daily=lambda **kw: calls.append(kw["ts_code"]) or pd.DataFrame())
        monkeypatch.setattr(index_daily.ts, "pro_api", lambda key: pro)
        monkeypatch.setattr(index_daily, "insert_record", lambda **kw: None)
        index_daily.sync_index_daily_full()
        assert len(calls) == len(index_daily.INDEX_CODES)

    def test_skip_when_up_to_date(self, monkeypatch) -> None:
        monkeypatch.setattr(index_daily, "get_today_run", lambda job_type: None)
        monkeypatch.setattr(index_daily, "get_settings", _settings)
        monkeypatch.setattr(index_daily, "get_last_trade_date", lambda ts_code: datetime(2030, 1, 1).date())
        pro = SimpleNamespace(index_daily=lambda **kw: (_ for _ in ()).throw(AssertionError("should be skipped")))
        monkeypatch.setattr(index_daily.ts, "pro_api", lambda key: pro)
        monkeypatch.setattr(index_daily, "insert_record", lambda **kw: None)
        out = index_daily.sync_index_daily_full()
        assert out == {"ok": True, "updated": 0}

    def test_failure_records_and_returns(self, monkeypatch) -> None:
        monkeypatch.setattr(index_daily, "get_today_run", lambda job_type: None)
        monkeypatch.setattr(index_daily, "get_settings", _settings)
        monkeypatch.setattr(index_daily, "get_last_trade_date", lambda ts_code: None)
        pro = SimpleNamespace(index_daily=lambda **kw: (_ for _ in ()).throw(RuntimeError("boom")))
        monkeypatch.setattr(index_daily.ts, "pro_api", lambda key: pro)
        records = []
        monkeypatch.setattr(index_daily, "insert_record", lambda **kw: records.append(kw))
        out = index_daily.sync_index_daily_full()
        assert out["ok"] is False and "boom" in out["error"]
        assert records[0]["job_type"] == index_daily.JOB_TYPE and records[0]["success"] is False


class TestIndexBasic:
    def test_skips_when_today_succeeded(self, monkeypatch) -> None:
        monkeypatch.setattr(index_basic, "get_today_run", lambda job_type: {"success": True})
        out = index_basic.sync_index_basic_full()
        assert out.get("skipped") is True

    def test_missing_api_key(self, monkeypatch) -> None:
        monkeypatch.setattr(index_basic, "get_today_run", lambda job_type: None)
        monkeypatch.setattr(index_basic, "get_settings", lambda: SimpleNamespace(tu_share_api_key=None))
        out = index_basic.sync_index_basic_full()
        assert out["ok"] is False and "TU_SHARE_API_KEY" in out["error"]

    def test_full_success(self, monkeypatch) -> None:
        monkeypatch.setattr(index_basic, "get_today_run", lambda job_type: None)
        monkeypatch.setattr(index_basic, "get_settings", _settings)
        monkeypatch.setattr(index_basic, "get_last_trade_date", lambda ts_code: None)
        monkeypatch.setattr(index_basic, "upsert_from_dataframe", lambda df: len(df))
        pro = SimpleNamespace(index_dailybasic=lambda **kw: pd.DataFrame([{"ts_code": "000001.SH", "trade_date": _today()}]))
        monkeypatch.setattr(index_basic.ts, "pro_api", lambda key: pro)
        records = []
        monkeypatch.setattr(index_basic, "insert_record", lambda **kw: records.append(kw))
        out = index_basic.sync_index_basic_full()
        assert out["ok"] is True and out["updated"] == len(index_basic.INDEX_CODES)
        assert records[-1]["success"] is True

    def test_empty_df(self, monkeypatch) -> None:
        monkeypatch.setattr(index_basic, "get_today_run", lambda job_type: None)
        monkeypatch.setattr(index_basic, "get_settings", _settings)
        monkeypatch.setattr(index_basic, "get_last_trade_date", lambda ts_code: None)
        pro = SimpleNamespace(index_dailybasic=lambda **kw: pd.DataFrame())
        monkeypatch.setattr(index_basic.ts, "pro_api", lambda key: pro)
        monkeypatch.setattr(index_basic, "insert_record", lambda **kw: None)
        assert index_basic.sync_index_basic_full() == {"ok": True, "updated": 0}

    def test_skip_when_up_to_date(self, monkeypatch) -> None:
        monkeypatch.setattr(index_basic, "get_today_run", lambda job_type: None)
        monkeypatch.setattr(index_basic, "get_settings", _settings)
        monkeypatch.setattr(index_basic, "get_last_trade_date", lambda ts_code: datetime(2030, 1, 1).date())
        pro = SimpleNamespace(index_dailybasic=lambda **kw: (_ for _ in ()).throw(AssertionError("should be skipped")))
        monkeypatch.setattr(index_basic.ts, "pro_api", lambda key: pro)
        monkeypatch.setattr(index_basic, "insert_record", lambda **kw: None)
        assert index_basic.sync_index_basic_full() == {"ok": True, "updated": 0}

    def test_failure(self, monkeypatch) -> None:
        monkeypatch.setattr(index_basic, "get_today_run", lambda job_type: None)
        monkeypatch.setattr(index_basic, "get_settings", _settings)
        monkeypatch.setattr(index_basic, "get_last_trade_date", lambda ts_code: None)
        pro = SimpleNamespace(index_dailybasic=lambda **kw: (_ for _ in ()).throw(RuntimeError("boom")))
        monkeypatch.setattr(index_basic.ts, "pro_api", lambda key: pro)
        records = []
        monkeypatch.setattr(index_basic, "insert_record", lambda **kw: records.append(kw))
        out = index_basic.sync_index_basic_full()
        assert out["ok"] is False and records[0]["success"] is False


class TestStockBasic:
    def test_get_list(self, monkeypatch) -> None:
        monkeypatch.setattr(stock_basic, "fetch_all", lambda: [{"ts_code": "000001.SZ"}])
        assert stock_basic.get_stock_basic_list() == [{"ts_code": "000001.SZ"}]

    def test_skips_when_today_succeeded(self, monkeypatch) -> None:
        monkeypatch.setattr(stock_basic, "get_today_run", lambda job_type: {"success": True})
        assert stock_basic.sync_stock_basic().get("skipped") is True

    def test_missing_api_key_records_failure(self, monkeypatch) -> None:
        monkeypatch.setattr(stock_basic, "get_today_run", lambda job_type: None)
        monkeypatch.setattr(stock_basic, "get_settings", lambda: SimpleNamespace(tu_share_api_key=None))
        records = []
        monkeypatch.setattr(stock_basic, "insert_record", lambda **kw: records.append(kw))
        out = stock_basic.sync_stock_basic()
        assert out["ok"] is False and records[0]["success"] is False

    def test_empty_df(self, monkeypatch) -> None:
        monkeypatch.setattr(stock_basic, "get_today_run", lambda job_type: None)
        monkeypatch.setattr(stock_basic, "get_settings", _settings)
        pro = SimpleNamespace(stock_basic=lambda **kw: pd.DataFrame())
        monkeypatch.setattr(stock_basic.ts, "pro_api", lambda key: pro)
        records = []
        monkeypatch.setattr(stock_basic, "insert_record", lambda **kw: records.append(kw))
        out = stock_basic.sync_stock_basic()
        assert out == {"ok": True, "updated": 0, "message": "no data from tushare"}
        assert records[0]["success"] is True

    def test_success(self, monkeypatch) -> None:
        monkeypatch.setattr(stock_basic, "get_today_run", lambda job_type: None)
        monkeypatch.setattr(stock_basic, "get_settings", _settings)
        pro = SimpleNamespace(stock_basic=lambda **kw: pd.DataFrame([{"ts_code": "000001.SZ"}]))
        monkeypatch.setattr(stock_basic.ts, "pro_api", lambda key: pro)
        monkeypatch.setattr(stock_basic, "upsert_from_dataframe", lambda df: 3)
        records = []
        monkeypatch.setattr(stock_basic, "insert_record", lambda **kw: records.append(kw))
        assert stock_basic.sync_stock_basic() == {"ok": True, "updated": 3}
        assert records[-1]["success"] is True

    def test_exception(self, monkeypatch) -> None:
        monkeypatch.setattr(stock_basic, "get_today_run", lambda job_type: None)
        monkeypatch.setattr(stock_basic, "get_settings", _settings)
        monkeypatch.setattr(stock_basic.ts, "pro_api", lambda key: (_ for _ in ()).throw(RuntimeError("boom")))
        records = []
        monkeypatch.setattr(stock_basic, "insert_record", lambda **kw: records.append(kw))
        out = stock_basic.sync_stock_basic()
        assert out["ok"] is False and "boom" in out["error"]
        assert records[0]["success"] is False

    def test_sync_status_none(self, monkeypatch) -> None:
        monkeypatch.setattr(stock_basic, "get_today_run", lambda job_type: None)
        assert stock_basic.get_stock_basic_sync_status() == {"job_type": stock_basic.JOB_TYPE, "today_run": None}

    def test_sync_status_with_run(self, monkeypatch) -> None:
        monkeypatch.setattr(stock_basic, "get_today_run", lambda job_type: {"success": True})
        assert stock_basic.get_stock_basic_sync_status()["today_run"]["success"] is True
