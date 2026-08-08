"""adj_factor service coverage (per-stock adj_factor full sync)."""

from __future__ import annotations

from datetime import date

import pandas as pd

from data_sync_service.service import adj_factor as af


class _Settings:
    tu_share_api_key = "TEST_KEY"


class _Pro:
    def __init__(self, df=None) -> None:
        self.df = df
        self.calls = []

    def adj_factor(self, **kw):
        self.calls.append(kw)
        return self.df if self.df is not None else pd.DataFrame()


def _patch(monkeypatch, pro=None, ts_codes=None, last=None, run=None):
    if pro is None:
        pro = _Pro()
    monkeypatch.setattr(af, "get_settings", lambda: _Settings())
    monkeypatch.setattr(af, "ts", type("ts", (), {"pro_api": staticmethod(lambda k: pro)}))
    monkeypatch.setattr(af, "get_today_run", lambda jt: run)
    monkeypatch.setattr(af, "fetch_ts_codes", lambda: ts_codes or [])
    monkeypatch.setattr(af, "get_last_adj_factor_date", lambda code: last)
    monkeypatch.setattr(af, "update_adj_factor_from_dataframe", lambda df: len(df))
    monkeypatch.setattr(af, "insert_record", lambda **kw: None)
    return pro


def test_today_yyyymmdd() -> None:
    s = af._today_yyyymmdd()
    assert len(s) == 8 and s.isdigit()


def test_date_to_yyyymmdd() -> None:
    assert af._date_to_yyyymmdd(date(2026, 8, 7)) == "20260807"


def test_sync_skips_already_done(monkeypatch) -> None:
    _patch(monkeypatch, run={"success": True})
    out = af.sync_adj_factor_full()
    assert out["skipped"] is True


def test_sync_no_stock_list(monkeypatch) -> None:
    _patch(monkeypatch, ts_codes=[])
    out = af.sync_adj_factor_full()
    assert out["message"] == "no stock list"


def test_sync_resumes_after_marker(monkeypatch) -> None:
    pro = _patch(monkeypatch, ts_codes=["600000.SH", "600001.SH", "600002.SH"],
                 run={"success": False, "last_ts_code": "600001.SH"}, last=date(2026, 8, 6))
    out = af.sync_adj_factor_full()
    assert out["ok"] is True
    assert pro.calls[0]["ts_code"] == "600002.SH"
    assert pro.calls[0]["start_date"] == "20260807"


def test_sync_resume_unknown_marker_restarts(monkeypatch) -> None:
    pro = _patch(monkeypatch, ts_codes=["600000.SH"],
                 run={"success": False, "last_ts_code": "nope.UN"}, last=date(2026, 8, 6))
    assert af.sync_adj_factor_full()["ok"] is True
    assert pro.calls[0]["ts_code"] == "600000.SH"


def test_sync_missing_api_key(monkeypatch) -> None:
    _patch(monkeypatch, ts_codes=["600000.SH"])
    monkeypatch.setattr(af, "get_settings", lambda: type("S", (), {"tu_share_api_key": ""})())
    out = af.sync_adj_factor_full()
    assert out["ok"] is False and "API_KEY" in out["error"]


def test_sync_fresh_uses_full_start(monkeypatch) -> None:
    pro = _patch(monkeypatch, ts_codes=["600000.SH"], last=None)
    assert af.sync_adj_factor_full()["ok"] is True
    assert pro.calls[0]["start_date"] == af.FULL_START_DATE


def test_sync_uptodate_skips_fetch(monkeypatch) -> None:
    pro = _patch(monkeypatch, ts_codes=["600000.SH"], last=date(2026, 8, 8))
    out = af.sync_adj_factor_full()
    assert out["ok"] is True and out["updated"] == 0
    assert pro.calls == []


def test_sync_success_accumulates(monkeypatch) -> None:
    pro = _patch(monkeypatch, ts_codes=["600000.SH", "600001.SH"], last=date(2026, 8, 6))
    pro.df = pd.DataFrame({"ts_code": ["a", "b"]})
    out = af.sync_adj_factor_full()
    assert out["ok"] is True and out["updated"] == 4


def test_sync_failure_records(monkeypatch) -> None:
    class P:
        def adj_factor(self, **kw):
            raise RuntimeError("boom")

    rec = {}
    _patch(monkeypatch, pro=P(), ts_codes=["600000.SH", "600001.SH"], last=None)
    monkeypatch.setattr(af, "insert_record", lambda **kw: rec.update(kw))
    out = af.sync_adj_factor_full()
    assert out["ok"] is False and out["error"] == "boom"
    assert rec["success"] is False and rec["job_type"] == af.JOB_TYPE


def test_sync_failure_tracks_last_success(monkeypatch) -> None:
    class P:
        def __init__(self) -> None:
            self.n = 0

        def adj_factor(self, **kw):
            self.n += 1
            if self.n == 1:
                return pd.DataFrame({"ts_code": ["a"]})
            raise RuntimeError("boom2")

    rec = {}
    _patch(monkeypatch, pro=P(), ts_codes=["600000.SH", "600001.SH"], last=date(2026, 8, 6))
    monkeypatch.setattr(af, "insert_record", lambda **kw: rec.update(kw))
    out = af.sync_adj_factor_full()
    assert out["ok"] is False and out["last_ts_code"] == "600000.SH"
    assert rec["last_ts_code"] == "600000.SH"


def test_sync_success_records(monkeypatch) -> None:
    rec = {}
    _patch(monkeypatch, ts_codes=["600000.SH"], last=date(2026, 8, 6))
    monkeypatch.setattr(af, "insert_record", lambda **kw: rec.update(kw))
    assert af.sync_adj_factor_full()["ok"] is True
    assert rec["success"] is True and rec["last_ts_code"] is None


def test_get_adj_factor_sync_status(monkeypatch) -> None:
    monkeypatch.setattr(af, "get_today_run", lambda jt: None)
    assert af.get_adj_factor_sync_status()["today_run"] is None
    monkeypatch.setattr(af, "get_today_run", lambda jt: {"success": False})
    out = af.get_adj_factor_sync_status()
    assert out["today_run"] == {"success": False} and out["job_type"] == af.JOB_TYPE
