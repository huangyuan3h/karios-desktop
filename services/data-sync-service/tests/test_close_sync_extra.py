"""close_sync service coverage (trade-calendar driven market-wide close sync)."""

from __future__ import annotations

from datetime import date, datetime
from zoneinfo import ZoneInfo

import pandas as pd

from data_sync_service.service import close_sync as cs


class _Settings:
    tu_share_api_key = "TEST_KEY"


class _Pro:
    def __init__(self, daily_df=None, factor_df=None, fail_on=None) -> None:
        self.daily_df = daily_df
        self.factor_df = factor_df
        self.fail_on = fail_on
        self.daily_calls = []
        self.factor_calls = []

    def daily(self, **kw):
        self.daily_calls.append(kw)
        if self.fail_on == "daily":
            raise RuntimeError("daily boom")
        return self.daily_df if self.daily_df is not None else pd.DataFrame()

    def adj_factor(self, **kw):
        self.factor_calls.append(kw)
        if self.fail_on == "factor":
            raise RuntimeError("factor boom")
        return self.factor_df if self.factor_df is not None else pd.DataFrame()


def _df(n):
    return pd.DataFrame({"ts_code": [f"600000.SH{i}" for i in range(n)]})


def _patch(monkeypatch, pro=None, today_run=None, last_ok=None, open_flag=True,
           open_dates=None, last_open=None, counts=None):
    if pro is None:
        pro = _Pro()
    monkeypatch.setattr(cs, "get_settings", lambda: _Settings())
    monkeypatch.setattr(cs, "ts", type("ts", (), {"pro_api": staticmethod(lambda k: pro)}))
    monkeypatch.setattr(cs, "get_today_run", lambda jt: today_run)
    monkeypatch.setattr(cs, "get_last_success", lambda jt: last_ok)
    monkeypatch.setattr(cs, "is_trading_day", lambda exc, d: open_flag)
    monkeypatch.setattr(cs, "get_open_dates", lambda **kw: [d for d in (open_dates if open_dates is not None else [date(2026, 8, 7)])
                                                             if kw["start_date"] <= d <= kw["end_date"]])
    monkeypatch.setattr(cs, "last_open_date_on_or_before", lambda d, exchange=None: last_open)
    monkeypatch.setattr(cs, "count_rows_for_trade_date", lambda d: (counts or {}).get(d, 5000))
    monkeypatch.setattr(cs, "upsert_daily", lambda df: len(df))
    monkeypatch.setattr(cs, "update_adj_factor_from_dataframe", lambda df: len(df))
    monkeypatch.setattr(cs, "insert_record", lambda *a, **k: None)
    monkeypatch.setattr(cs, "sync_trade_calendar", lambda **kw: {"ok": True})
    return pro


def test_cn_today() -> None:
    d = cs._cn_today()
    assert isinstance(d, date)


def test_cn_now() -> None:
    assert isinstance(cs._cn_now(), datetime)


def test_parse_yyyymmdd() -> None:
    assert cs._parse_yyyymmdd("20260807") == date(2026, 8, 7)


def test_to_yyyymmdd() -> None:
    assert cs._to_yyyymmdd(date(2026, 8, 7)) == "20260807"


def test_fetch_paged_daily_single_page(monkeypatch) -> None:
    pro = _Pro(daily_df=_df(3))
    monkeypatch.setattr(cs, "upsert_daily", lambda df: len(df))
    assert cs._fetch_paged_daily(pro, "20260807") == 3
    assert pro.daily_calls == [{"trade_date": "20260807", "limit": 5000, "offset": 0, "fields": cs.DAILY_FIELDS_STR if hasattr(cs, "DAILY_FIELDS_STR") else ",".join(cs.DAILY_FIELDS)}]


def test_fetch_paged_daily_multiple_pages(monkeypatch) -> None:
    big = pd.DataFrame({"ts_code": [f"t{i}" for i in range(5000)]})
    calls = {"n": 0}

    def paged_daily(**kw):
        calls["n"] += 1
        return big if calls["n"] == 1 else pd.DataFrame()

    pro = _Pro(daily_df=big)
    monkeypatch.setattr(cs, "upsert_daily", lambda df: len(df))
    monkeypatch.setattr(pro, "daily", paged_daily)
    n = cs._fetch_paged_daily(pro, "20260807", limit=5000)
    assert n == 5000
    assert calls["n"] == 2


def test_fetch_paged_daily_empty(monkeypatch) -> None:
    pro = _Pro(daily_df=pd.DataFrame())
    monkeypatch.setattr(cs, "upsert_daily", lambda df: len(df))
    assert cs._fetch_paged_daily(pro, "20260807") == 0


def test_fetch_paged_adj_factor(monkeypatch) -> None:
    pro = _Pro(factor_df=_df(4))
    monkeypatch.setattr(cs, "update_adj_factor_from_dataframe", lambda df: len(df))
    assert cs._fetch_paged_adj_factor(pro, "20260807") == 4
    assert pro.factor_calls[0]["limit"] == 5000


def test_fetch_paged_adj_factor_empty(monkeypatch) -> None:
    pro = _Pro(factor_df=pd.DataFrame())
    monkeypatch.setattr(cs, "update_adj_factor_from_dataframe", lambda df: len(df))
    assert cs._fetch_paged_adj_factor(pro, "20260807") == 0


def test_resolve_start_date_default_today(monkeypatch) -> None:
    monkeypatch.setattr(cs, "get_last_success", lambda jt: None)
    assert cs._resolve_start_date(date(2026, 8, 8), None) == date(2026, 8, 8)


def test_resolve_start_date_resume_marker() -> None:
    run = {"success": False, "last_ts_code": "20260805"}
    assert cs._resolve_start_date(date(2026, 8, 8), run) == date(2026, 8, 6)


def test_resolve_start_date_last_success_marker(monkeypatch) -> None:
    monkeypatch.setattr(cs, "get_last_success", lambda jt: {"last_ts_code": "20260806"})
    assert cs._resolve_start_date(date(2026, 8, 8), None) == date(2026, 8, 7)


def test_resolve_start_date_last_success_time(monkeypatch) -> None:
    monkeypatch.setattr(cs, "get_last_success", lambda jt: {"last_ts_code": "", "sync_at": "2026-08-06T17:00:00+08:00"})
    assert cs._resolve_start_date(date(2026, 8, 8), None) == date(2026, 8, 7)


def test_sync_close_skips_already_done(monkeypatch) -> None:
    _patch(monkeypatch, today_run={"success": True})
    out = cs.sync_close()
    assert out["skipped"] is True and "already synced" in out["message"]


def test_sync_close_force_reruns(monkeypatch) -> None:
    pro = _patch(monkeypatch, today_run={"success": True})
    pro.daily_df = _df(2)
    monkeypatch.setattr(cs, "_cn_today", lambda: date(2026, 8, 8))
    monkeypatch.setattr(cs, "_cn_now", lambda: datetime(2026, 8, 8, 18, 0, tzinfo=ZoneInfo("Asia/Shanghai")))
    monkeypatch.setattr(cs, "get_last_success", lambda jt: {"last_ts_code": "20260807"})
    monkeypatch.setattr(cs, "get_open_dates", lambda **kw: [date(2026, 8, 8)])
    out = cs.sync_close(force=True)
    assert out["ok"] is True and out["updated_daily_rows"] == 2


def test_sync_close_calendar_missing_auto_heal_fail(monkeypatch) -> None:
    monkeypatch.setattr(cs, "get_today_run", lambda jt: None)
    monkeypatch.setattr(cs, "is_trading_day", lambda exc, d: None)
    monkeypatch.setattr(cs, "sync_trade_calendar", lambda **kw: {"ok": False, "error": "net down"})
    monkeypatch.setattr(cs, "get_settings", lambda: _Settings())
    monkeypatch.setattr(cs, "insert_record", lambda *a, **k: None)
    monkeypatch.setattr(cs, "_cn_today", lambda: date(2026, 8, 8))
    monkeypatch.setattr(cs, "_cn_now", lambda: datetime(2026, 8, 8, 18, 0, tzinfo=ZoneInfo("Asia/Shanghai")))
    out = cs.sync_close()
    assert out["ok"] is False and "auto sync trade_cal failed" in out["error"]


def test_sync_close_calendar_still_missing(monkeypatch) -> None:
    monkeypatch.setattr(cs, "get_today_run", lambda jt: None)
    monkeypatch.setattr(cs, "is_trading_day", lambda exc, d: None)
    monkeypatch.setattr(cs, "sync_trade_calendar", lambda **kw: {"ok": True})
    monkeypatch.setattr(cs, "get_settings", lambda: _Settings())
    monkeypatch.setattr(cs, "insert_record", lambda *a, **k: None)
    monkeypatch.setattr(cs, "_cn_today", lambda: date(2026, 8, 8))
    monkeypatch.setattr(cs, "_cn_now", lambda: datetime(2026, 8, 8, 18, 0, tzinfo=ZoneInfo("Asia/Shanghai")))
    out = cs.sync_close()
    assert out["ok"] is False and "still missing" in out["error"]


def test_sync_close_non_trading_day_no_latest_open(monkeypatch) -> None:
    monkeypatch.setattr(cs, "get_today_run", lambda jt: None)
    monkeypatch.setattr(cs, "is_trading_day", lambda exc, d: False)
    monkeypatch.setattr(cs, "last_open_date_on_or_before", lambda d, exchange=None: None)
    monkeypatch.setattr(cs, "get_settings", lambda: _Settings())
    monkeypatch.setattr(cs, "insert_record", lambda *a, **k: None)
    monkeypatch.setattr(cs, "_cn_today", lambda: date(2026, 8, 8))
    monkeypatch.setattr(cs, "_cn_now", lambda: datetime(2026, 8, 8, 18, 0, tzinfo=ZoneInfo("Asia/Shanghai")))
    out = cs.sync_close()
    assert out["ok"] is False and "cannot determine last trading day" in out["error"]


def test_sync_close_uptodate_non_trading_day(monkeypatch) -> None:
    _patch(monkeypatch, open_flag=False, last_open=date(2026, 8, 7),
           last_ok={"last_ts_code": "20260807"})
    monkeypatch.setattr(cs, "_cn_today", lambda: date(2026, 8, 8))
    monkeypatch.setattr(cs, "_cn_now", lambda: datetime(2026, 8, 8, 18, 0, tzinfo=ZoneInfo("Asia/Shanghai")))
    out = cs.sync_close()
    assert out["skipped"] is True and "not a trading day" in out["message"]
    assert out["meta"]["endDateDailyRows"] == 5000


def test_sync_close_uptodate_force_heals_partial(monkeypatch) -> None:
    pro = _patch(monkeypatch, open_flag=False, last_open=date(2026, 8, 7),
                 last_ok={"last_ts_code": "20260807"}, counts={"2026-08-07": 100})
    pro.daily_df = _df(2)
    monkeypatch.setattr(cs, "_cn_today", lambda: date(2026, 8, 8))
    monkeypatch.setattr(cs, "_cn_now", lambda: datetime(2026, 8, 8, 18, 0, tzinfo=ZoneInfo("Asia/Shanghai")))
    out = cs.sync_close(force=True)
    assert out["ok"] is True and out["updated_daily_rows"] == 2


def test_sync_close_too_early(monkeypatch) -> None:
    _patch(monkeypatch, open_flag=True)
    monkeypatch.setattr(cs, "_cn_today", lambda: date(2026, 8, 8))
    monkeypatch.setattr(cs, "_cn_now", lambda: datetime(2026, 8, 8, 10, 0, tzinfo=ZoneInfo("Asia/Shanghai")))
    out = cs.sync_close()
    assert out["skipped"] is True and "too early" in out["message"]


def test_sync_close_missing_api_key(monkeypatch) -> None:
    _patch(monkeypatch, open_flag=True)
    monkeypatch.setattr(cs, "get_settings", lambda: type("S", (), {"tu_share_api_key": ""})())
    monkeypatch.setattr(cs, "_cn_today", lambda: date(2026, 8, 8))
    monkeypatch.setattr(cs, "_cn_now", lambda: datetime(2026, 8, 8, 18, 0, tzinfo=ZoneInfo("Asia/Shanghai")))
    out = cs.sync_close()
    assert out["ok"] is False and "API_KEY" in out["error"]


def test_sync_close_no_trade_dates(monkeypatch) -> None:
    _patch(monkeypatch, open_flag=True, open_dates=[], last_open=None)
    monkeypatch.setattr(cs, "sync_trade_calendar", lambda **kw: {"ok": True})
    monkeypatch.setattr(cs, "_cn_today", lambda: date(2026, 8, 8))
    monkeypatch.setattr(cs, "_cn_now", lambda: datetime(2026, 8, 8, 18, 0, tzinfo=ZoneInfo("Asia/Shanghai")))
    out = cs.sync_close()
    assert out["ok"] is False and "trade calendar missing for requested range" in out["error"]


def test_sync_close_success_trading_day(monkeypatch) -> None:
    pro = _patch(monkeypatch, open_flag=True, open_dates=[date(2026, 8, 7), date(2026, 8, 8)],
                 last_ok={"last_ts_code": "20260806"})
    pro.daily_df = _df(2)
    pro.factor_df = _df(1)
    records = []
    monkeypatch.setattr(cs, "insert_record", lambda *a, **k: records.append((a, k)))
    monkeypatch.setattr(cs, "_cn_today", lambda: date(2026, 8, 8))
    monkeypatch.setattr(cs, "_cn_now", lambda: datetime(2026, 8, 8, 18, 0, tzinfo=ZoneInfo("Asia/Shanghai")))
    out = cs.sync_close()
    assert out["ok"] is True and out["updated_daily_rows"] == 4
    assert out["trade_dates"] == ["2026-08-07", "2026-08-08"]
    assert records and records[-1][0] == (cs.JOB_TYPE,) and records[-1][1]["success"] is True
    assert records[-1][1]["last_ts_code"] == "20260808"


def test_sync_close_success_non_trading_day(monkeypatch) -> None:
    pro = _patch(monkeypatch, open_flag=False, last_open=date(2026, 8, 7), open_dates=[date(2026, 8, 7)],
                 last_ok={"last_ts_code": "20260806"})
    pro.daily_df = _df(1)
    records = []
    monkeypatch.setattr(cs, "insert_record", lambda *a, **k: records.append((a, k)))
    monkeypatch.setattr(cs, "_cn_today", lambda: date(2026, 8, 8))
    monkeypatch.setattr(cs, "_cn_now", lambda: datetime(2026, 8, 8, 18, 0, tzinfo=ZoneInfo("Asia/Shanghai")))
    out = cs.sync_close()
    assert out["ok"] is True and "non-trading day catchup" in out["message"]
    assert records[-1][1]["last_ts_code"] == "20260807"


def test_sync_close_failure_records(monkeypatch) -> None:
    pro = _patch(monkeypatch, open_flag=True, open_dates=[date(2026, 8, 7), date(2026, 8, 8)])
    pro.fail_on = "daily"
    records = []
    monkeypatch.setattr(cs, "insert_record", lambda *a, **k: records.append((a, k)))
    monkeypatch.setattr(cs, "_cn_today", lambda: date(2026, 8, 8))
    monkeypatch.setattr(cs, "_cn_now", lambda: datetime(2026, 8, 8, 18, 0, tzinfo=ZoneInfo("Asia/Shanghai")))
    out = cs.sync_close()
    assert out["ok"] is False and out["last_marker"] is None
    assert records[0][1]["success"] is False


def test_sync_close_failure_after_one_day(monkeypatch) -> None:
    pro = _patch(monkeypatch, open_flag=True, open_dates=[date(2026, 8, 7), date(2026, 8, 8)],
                 last_ok={"last_ts_code": "20260806"})
    calls = {"n": 0}

    def paged_factor(**kw):
        calls["n"] += 1
        if calls["n"] == 1:
            return _df(1)
        raise RuntimeError("factor boom")

    monkeypatch.setattr(pro, "adj_factor", paged_factor)
    records = []
    monkeypatch.setattr(cs, "insert_record", lambda *a, **k: records.append((a, k)))
    monkeypatch.setattr(cs, "_cn_today", lambda: date(2026, 8, 8))
    monkeypatch.setattr(cs, "_cn_now", lambda: datetime(2026, 8, 8, 18, 0, tzinfo=ZoneInfo("Asia/Shanghai")))
    out = cs.sync_close()
    assert out["ok"] is False and out["last_marker"] == "20260807"


def test_sync_close_today_empty_raises(monkeypatch) -> None:
    pro = _patch(monkeypatch, open_flag=True, open_dates=[date(2026, 8, 8)],
                 last_ok={"last_ts_code": "20260807"})
    pro.daily_df = pd.DataFrame()  # empty daily for today
    records = []
    monkeypatch.setattr(cs, "insert_record", lambda *a, **k: records.append((a, k)))
    monkeypatch.setattr(cs, "_cn_today", lambda: date(2026, 8, 8))
    monkeypatch.setattr(cs, "_cn_now", lambda: datetime(2026, 8, 8, 18, 0, tzinfo=ZoneInfo("Asia/Shanghai")))
    out = cs.sync_close()
    assert out["ok"] is False and "empty for today" in out["error"]


def test_sync_close_pre_close_catchup_partial(monkeypatch) -> None:
    pro = _patch(monkeypatch, open_flag=True, open_dates=[date(2026, 8, 7), date(2026, 8, 8)],
                 last_ok={"last_ts_code": "20260806"})
    pro.daily_df = _df(1)
    monkeypatch.setattr(cs, "_cn_today", lambda: date(2026, 8, 8))
    monkeypatch.setattr(cs, "_cn_now", lambda: datetime(2026, 8, 8, 9, 0, tzinfo=ZoneInfo("Asia/Shanghai")))
    out = cs.sync_close()
    assert out["ok"] is True and out["partial"] is True
    assert out["trade_dates"] == ["2026-08-07"]  # today excluded before close


def test_get_close_sync_status(monkeypatch) -> None:
    monkeypatch.setattr(cs, "get_today_run", lambda jt: {"success": True})
    monkeypatch.setattr(cs, "get_last_success", lambda jt: {"last_ts_code": "20260807"})
    out = cs.get_close_sync_status()
    assert out["job_type"] == cs.JOB_TYPE
    assert out["today_run"]["success"] is True and out["last_success"]["last_ts_code"] == "20260807"
