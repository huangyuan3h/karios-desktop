"""daily service coverage (per-stock / full daily sync)."""

from __future__ import annotations

import warnings
from datetime import date

import pandas as pd

from data_sync_service.service import daily as dl


class _Settings:
    tu_share_api_key = "TEST_KEY"


class _Pro:
    def __init__(self, df=None) -> None:
        self.df = df
        self.calls = []

    def daily(self, **kw):
        self.calls.append(kw)
        return self.df if self.df is not None else pd.DataFrame()


def _patch(monkeypatch, pro=None, ts_codes=None, last=None, run=None):
    if pro is None:
        pro = _Pro()
    monkeypatch.setattr(dl, "get_settings", lambda: _Settings())
    monkeypatch.setattr(dl, "ts", type("ts", (), {"pro_api": staticmethod(lambda k: pro)}))
    monkeypatch.setattr(dl, "get_today_run", lambda jt: run)
    monkeypatch.setattr(dl, "fetch_ts_codes", lambda: ts_codes or [])
    monkeypatch.setattr(dl, "get_last_trade_date", lambda code: last)
    monkeypatch.setattr(dl, "upsert_from_dataframe", lambda df: len(df))
    monkeypatch.setattr(dl, "insert_record", lambda **kw: None)
    return pro


def test_today_yyyymmdd() -> None:
    s = dl._today_yyyymmdd()
    assert len(s) == 8 and s.isdigit()


def test_date_to_yyyymmdd() -> None:
    assert dl._date_to_yyyymmdd(date(2026, 8, 7)) == "20260807"


def test_incremental_start_date_fresh(monkeypatch) -> None:
    _patch(monkeypatch, last=None)
    s, e = dl._incremental_start_date("600000.SH")
    assert s == dl.FULL_START_DATE and len(e) == 8


def test_incremental_start_date_resume(monkeypatch) -> None:
    _patch(monkeypatch, last=date(2026, 8, 6))
    s, e = dl._incremental_start_date("600000.SH")
    assert s == "20260807"


def test_sync_single_requires_code(monkeypatch) -> None:
    _patch(monkeypatch)
    out = dl.sync_daily_for_ts_code("  ")
    assert out["ok"] is False and "required" in out["error"]
    out2 = dl.sync_daily_for_ts_code(None)
    assert out2["ok"] is False


def test_sync_single_missing_api_key(monkeypatch) -> None:
    _patch(monkeypatch)
    monkeypatch.setattr(dl, "get_settings", lambda: type("S", (), {"tu_share_api_key": ""})())
    out = dl.sync_daily_for_ts_code("600000.sh")
    assert out["ok"] is False and "API_KEY" in out["error"]


def test_sync_single_uptodate(monkeypatch) -> None:
    _patch(monkeypatch, last=date.today())
    out = dl.sync_daily_for_ts_code("600000.sh")
    assert out["ok"] is True and out["skipped"] is True
    assert out["ts_code"] == "600000.SH"


def test_sync_single_success_clears_cache(monkeypatch) -> None:
    cleared = {"n": 0}
    from data_sync_service.service import trendok as tk

    monkeypatch.setattr(tk, "clear_trendok_cache", lambda: cleared.__setitem__("n", cleared["n"] + 1))
    pro = _patch(monkeypatch, last=date(2026, 8, 6))
    pro.df = pd.DataFrame({"ts_code": ["600000.SH"], "trade_date": ["20260807"]})
    out = dl.sync_daily_for_ts_code("600000.SH")
    assert out["ok"] is True and out["updated"] == 1
    assert cleared["n"] == 1


def test_sync_single_no_data(monkeypatch) -> None:
    pro = _patch(monkeypatch, last=date(2026, 8, 6))
    pro.df = pd.DataFrame()
    out = dl.sync_daily_for_ts_code("600000.SH")
    assert out["ok"] is True and out["updated"] == 0


def test_sync_single_failure(monkeypatch) -> None:
    class P:
        def daily(self, **kw):
            raise ValueError("bad")

    _patch(monkeypatch, pro=P(), last=date(2026, 8, 6))
    out = dl.sync_daily_for_ts_code("600000.SH")
    assert out["ok"] is False and out["error"] == "bad"


def test_sync_full_deprecation_warns(monkeypatch) -> None:
    _patch(monkeypatch, run=None)
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        dl.sync_daily_full()
        assert any(issubclass(x.category, DeprecationWarning) for x in w)


def test_sync_full_skips_already_done(monkeypatch) -> None:
    _patch(monkeypatch, run={"success": True})
    out = dl.sync_daily_full()
    assert out["skipped"] is True


def test_sync_full_no_list(monkeypatch) -> None:
    _patch(monkeypatch, ts_codes=[])
    out = dl.sync_daily_full()
    assert out["message"] == "no stock list"


def test_sync_full_resume(monkeypatch) -> None:
    pro = _patch(monkeypatch, ts_codes=["600000.SH", "600001.SH", "600002.SH"],
                 run={"success": False, "last_ts_code": "600001.SH"}, last=date(2026, 8, 6))
    out = dl.sync_daily_full()
    assert out["ok"] is True
    assert pro.calls[0]["ts_code"] == "600002.SH"


def test_sync_full_resume_unknown(monkeypatch) -> None:
    pro = _patch(monkeypatch, ts_codes=["600000.SH"],
                 run={"success": False, "last_ts_code": "x.X"}, last=date(2026, 8, 6))
    assert dl.sync_daily_full()["ok"] is True
    assert pro.calls[0]["ts_code"] == "600000.SH"


def test_sync_full_missing_api_key(monkeypatch) -> None:
    _patch(monkeypatch, ts_codes=["600000.SH"])
    monkeypatch.setattr(dl, "get_settings", lambda: type("S", (), {"tu_share_api_key": ""})())
    out = dl.sync_daily_full()
    assert out["ok"] is False and "API_KEY" in out["error"]


def test_sync_full_failure(monkeypatch) -> None:
    class P:
        def daily(self, **kw):
            raise RuntimeError("boom")

    rec = {}
    _patch(monkeypatch, pro=P(), ts_codes=["600000.SH"], last=None)
    monkeypatch.setattr(dl, "insert_record", lambda **kw: rec.update(kw))
    out = dl.sync_daily_full()
    assert out["ok"] is False and out["error"] == "boom"
    assert rec["success"] is False and rec["job_type"] == dl.JOB_TYPE


def test_sync_full_success_records(monkeypatch) -> None:
    rec = {}
    _patch(monkeypatch, ts_codes=["600000.SH"], last=date(2026, 8, 6))
    monkeypatch.setattr(dl, "insert_record", lambda **kw: rec.update(kw))
    assert dl.sync_daily_full()["ok"] is True
    assert rec["success"] is True and rec["last_ts_code"] is None


def test_get_daily_from_db(monkeypatch) -> None:
    from data_sync_service.db import daily as dbd

    monkeypatch.setattr(dbd, "fetch_daily", lambda **kw: [{"trade_date": "20260807"}])
    out = dl.get_daily_from_db(ts_code="600000.SH", limit=10)
    assert out == [{"trade_date": "20260807"}]


def test_get_daily_sync_status(monkeypatch) -> None:
    monkeypatch.setattr(dl, "get_today_run", lambda jt: None)
    assert dl.get_daily_sync_status()["today_run"] is None
    monkeypatch.setattr(dl, "get_today_run", lambda jt: {"success": True})
    out = dl.get_daily_sync_status()
    assert out["today_run"]["success"] is True and out["job_type"] == dl.JOB_TYPE
