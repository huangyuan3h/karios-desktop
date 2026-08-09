"""etf_daily service coverage (tushare fund_daily → daily table)."""

from __future__ import annotations

from datetime import date

import pandas as pd

from data_sync_service.service import etf_daily as ed


class _Settings:
    tu_share_api_key = "TEST_KEY"


class _Pro:
    def __init__(self) -> None:
        self.calls = []

    def fund_daily(self, **kw) -> pd.DataFrame:
        self.calls.append(kw)
        return pd.DataFrame(
            {"ts_code": ["510300.SH"], "trade_date": ["20260807"], "close": [4.0]}
        )


def _patch(monkeypatch, pro=None, ts_codes=None, last=None, run=None):
    state = {"upserted": 0}
    monkeypatch.setattr(ed, "get_settings", lambda: _Settings())
    monkeypatch.setattr(ed, "ts", type("ts", (), {"pro_api": staticmethod(lambda k: pro)}))
    monkeypatch.setattr(ed, "get_today_run", lambda jt: run)
    monkeypatch.setattr(ed, "_fetch_etf_ts_codes", lambda: ts_codes or [])
    monkeypatch.setattr(ed, "get_last_trade_date", lambda code: last)
    monkeypatch.setattr(ed, "upsert_from_dataframe", lambda df: state.__setitem__("upserted", state["upserted"] + len(df)) or len(df))
    monkeypatch.setattr(ed, "insert_record", lambda **kw: None)
    return state


def test_today_yyyymmdd() -> None:
    s = ed._today_yyyymmdd()
    assert len(s) == 8 and s.isdigit()


def test_date_to_yyyymmdd() -> None:
    assert ed._date_to_yyyymmdd(date(2026, 8, 7)) == "20260807"


def test_fetch_etf_ts_codes(monkeypatch) -> None:
    monkeypatch.setattr(ed, "fetch_ts_codes_by_market", lambda m: ["510300.SH"])
    assert ed._fetch_etf_ts_codes() == ["510300.SH"]


def test_sync_skips_when_already_succeeded(monkeypatch) -> None:
    _patch(monkeypatch, run={"success": True})
    assert ed.sync_etf_daily_full()["skipped"] is True


def test_sync_no_etf_list(monkeypatch) -> None:
    _patch(monkeypatch, run=None, ts_codes=[])
    out = ed.sync_etf_daily_full()
    assert out["message"] == "no ETF stock list"


def test_sync_resumes_from_last_ts_code(monkeypatch) -> None:
    pro = _Pro()
    _patch(monkeypatch, pro=pro, ts_codes=["510300.SH", "510500.SH", "588000.SH"],
           last=date(2026, 8, 6), run={"success": False, "last_ts_code": "510500.SH"})
    out = ed.sync_etf_daily_full()
    assert out["ok"] is True and out["updated"] == 1
    assert pro.calls[0]["ts_code"] == "588000.SH"
    assert pro.calls[0]["start_date"] == "20260807"


def test_sync_resume_unknown_code_restarts(monkeypatch) -> None:
    pro = _Pro()
    _patch(monkeypatch, pro=pro, ts_codes=["510300.SH"],
           run={"success": False, "last_ts_code": "unknown.UN"})
    out = ed.sync_etf_daily_full()
    assert out["ok"] is True
    assert pro.calls[0]["ts_code"] == "510300.SH"


def test_sync_missing_api_key(monkeypatch) -> None:
    _patch(monkeypatch, run=None, ts_codes=["510300.SH"])
    monkeypatch.setattr(ed, "get_settings", lambda: type("S", (), {"tu_share_api_key": ""})())
    out = ed.sync_etf_daily_full()
    assert out["ok"] is False and "API_KEY" in out["error"]


def test_sync_fresh_no_last_date(monkeypatch) -> None:
    pro = _Pro()
    _patch(monkeypatch, pro=pro, ts_codes=["510300.SH"], last=None)
    out = ed.sync_etf_daily_full()
    assert out["ok"] is True
    assert pro.calls[0]["start_date"] == ed.FULL_START_DATE


def test_sync_skips_uptodate_stock(monkeypatch) -> None:
    pro = _Pro()
    _patch(monkeypatch, pro=pro, ts_codes=["510300.SH"], last=date(2026, 8, 8))
    out = ed.sync_etf_daily_full()
    assert out["ok"] is True and out["updated"] == 0
    assert pro.calls == []  # start_date > end_date → never fetched


def test_sync_empty_df_no_upsert(monkeypatch) -> None:
    class P2(_Pro):
        def fund_daily(self, **kw):
            return pd.DataFrame()

    _patch(monkeypatch, pro=P2(), ts_codes=["510300.SH"], last=date(2026, 8, 6))
    out = ed.sync_etf_daily_full()
    assert out["ok"] is True and out["updated"] == 0


def test_sync_failure_records_and_stops(monkeypatch) -> None:
    class P3:
        def fund_daily(self, **kw):
            raise RuntimeError("boom")

    rec = {}

    def fake_record(**kw):
        rec.update(kw)

    _patch(monkeypatch, pro=P3(), ts_codes=["510300.SH", "510500.SH"], last=None)
    monkeypatch.setattr(ed, "insert_record", fake_record)
    out = ed.sync_etf_daily_full()
    assert out["ok"] is False and out["error"] == "boom"
    assert rec["success"] is False and rec["job_type"] == ed.JOB_TYPE


def test_sync_success_records_run(monkeypatch) -> None:
    rec = {}
    _patch(monkeypatch, pro=_Pro(), ts_codes=["510300.SH"], last=date(2026, 8, 6))
    monkeypatch.setattr(ed, "insert_record", lambda **kw: rec.update(kw))
    assert ed.sync_etf_daily_full()["ok"] is True
    assert rec["success"] is True and rec["last_ts_code"] is None


def test_sync_failure_sets_last_successful_code(monkeypatch) -> None:
    class P4:
        def __init__(self) -> None:
            self.n = 0

        def fund_daily(self, **kw):
            self.n += 1
            if self.n == 1:
                return pd.DataFrame({"ts_code": ["a"], "trade_date": ["20260807"], "close": [1.0]})
            raise RuntimeError("boom2")

    rec = {}
    _patch(monkeypatch, pro=P4(), ts_codes=["510300.SH", "510500.SH"], last=date(2026, 8, 6))
    monkeypatch.setattr(ed, "insert_record", lambda **kw: rec.update(kw))
    out = ed.sync_etf_daily_full()
    assert out["ok"] is False and out["last_ts_code"] == "510300.SH"
    assert rec["last_ts_code"] == "510300.SH"


def test_get_etf_daily_sync_status(monkeypatch) -> None:
    monkeypatch.setattr(ed, "get_today_run", lambda jt: None)
    assert ed.get_etf_daily_sync_status()["today_run"] is None
    monkeypatch.setattr(ed, "get_today_run", lambda jt: {"success": True})
    out = ed.get_etf_daily_sync_status()
    assert out["today_run"] == {"success": True}


def test_sync_single_requires_code(monkeypatch) -> None:
    _patch(monkeypatch)
    assert ed.sync_etf_daily_for_ts_code("  ")["error"] == "ts_code is required"
    assert ed.sync_etf_daily_for_ts_code(None)["ok"] is False


def test_sync_single_missing_api_key(monkeypatch) -> None:
    _patch(monkeypatch)
    monkeypatch.setattr(ed, "get_settings", lambda: type("S", (), {"tu_share_api_key": ""})())
    out = ed.sync_etf_daily_for_ts_code("510300.sh")
    assert out["ok"] is False and "API_KEY" in out["error"]


def test_sync_single_uptodate_skips(monkeypatch) -> None:
    _patch(monkeypatch, last=date(2026, 8, 8))
    out = ed.sync_etf_daily_for_ts_code(" 510300.sh ")
    assert out["ok"] is True and out["skipped"] is True
    assert out["ts_code"] == "510300.SH"


def test_sync_single_success_clears_trendok_cache(monkeypatch) -> None:
    cleared = {"n": 0}

    class P5:
        def fund_daily(self, **kw):
            return pd.DataFrame({"ts_code": ["510300.SH"], "trade_date": ["20260807"], "close": [4.0]})

    import data_sync_service.service.trendok as tk

    monkeypatch.setattr(tk, "clear_trendok_cache", lambda: cleared.__setitem__("n", cleared["n"] + 1))
    _patch(monkeypatch, pro=P5(), last=date(2026, 8, 6))
    out = ed.sync_etf_daily_for_ts_code("510300.SH")
    assert out["ok"] is True and out["updated"] == 1
    assert cleared["n"] == 1


def test_sync_single_no_upsert_no_cache_clear(monkeypatch) -> None:
    class P6:
        def fund_daily(self, **kw):
            return pd.DataFrame()

    monkeypatch.setattr(ed, "get_settings", lambda: _Settings())
    monkeypatch.setattr(ed, "ts", type("ts", (), {"pro_api": staticmethod(lambda k: P6())}))
    monkeypatch.setattr(ed, "get_last_trade_date", lambda code: date(2026, 8, 6))
    monkeypatch.setattr(ed, "upsert_from_dataframe", lambda df: 0)
    out = ed.sync_etf_daily_for_ts_code("510300.SH")
    assert out["ok"] is True and out["updated"] == 0


def test_sync_single_failure(monkeypatch) -> None:
    class P7:
        def fund_daily(self, **kw):
            raise ValueError("bad data")

    monkeypatch.setattr(ed, "get_settings", lambda: _Settings())
    monkeypatch.setattr(ed, "ts", type("ts", (), {"pro_api": staticmethod(lambda k: P7())}))
    monkeypatch.setattr(ed, "get_last_trade_date", lambda code: date(2026, 8, 6))
    out = ed.sync_etf_daily_for_ts_code("510300.SH")
    assert out["ok"] is False and out["error"] == "bad data"
