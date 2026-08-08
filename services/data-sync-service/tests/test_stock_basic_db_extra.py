"""db/stock_basic coverage."""

from __future__ import annotations

import pandas as pd

from data_sync_service.db import stock_basic as sb


class _Col:
    def __init__(self, name) -> None:
        self.name = name


class _Cur:
    def __init__(self, fetchall=None, fetchone=None, cols=None, rowcount=1) -> None:
        self._all = fetchall or []
        self._one = fetchone
        self.description = cols or [_Col("c")]
        self.rowcount = rowcount
        self.sql = None
        self.params = None
        self.executemany_args = None
        self.executed = []

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return None

    def execute(self, sql, params=None):
        self.sql = sql
        self.params = params
        self.executed.append(sql)
        return self

    def executemany(self, sql, args):
        self.executemany_args = args
        self.executed.append(sql)
        return self

    def fetchall(self):
        return self._all

    def fetchone(self):
        return self._one


class _Conn:
    def __init__(self, cur) -> None:
        self._cur = cur

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return None

    def cursor(self):
        return self._cur

    def commit(self) -> None:
        pass


def _patch(monkeypatch, cur=None):
    if cur is None:
        cur = _Cur()
    monkeypatch.setattr(sb, "ensure_table", lambda: None)
    monkeypatch.setattr(sb, "get_connection", lambda: _Conn(cur))
    return cur


def test_scalar() -> None:
    assert sb._scalar(None) is None
    assert sb._scalar(float("nan")) is None
    assert sb._scalar(" 600000 ") == "600000"
    assert sb._scalar("") is None


def test_date() -> None:
    import datetime as dt

    assert sb._date(None) is None
    assert sb._date(float("nan")) is None
    assert sb._date(dt.date(2026, 8, 7)) == "2026-08-07"
    assert sb._date("20260807") == "20260807"


def test_upsert_from_dataframe(monkeypatch) -> None:
    cur = _patch(monkeypatch)
    df = pd.DataFrame({
        "ts_code": ["600000.SH", "000001.SZ"],
        "symbol": ["600000", "1"],
        "name": ["浦发", None],
        "industry": ["银行", "保险"],
        "market": ["主板", "主板"],
        "list_date": [None, "19910403"],
        "delist_date": [None, None],
    })
    n = sb.upsert_from_dataframe(df)
    assert n == 2
    assert cur.executemany_args[0][4] == "主板"
    assert cur.executemany_args[1][1] == "1"
    assert sb.UPSERT_SQL in cur.executed[0]


def test_upsert_from_dataframe_keep_industry(monkeypatch) -> None:
    cur = _patch(monkeypatch)
    df = pd.DataFrame({"ts_code": ["600000.SH"], "symbol": ["600000"], "name": ["x"],
                       "industry": [None], "market": ["HK"], "list_date": [None], "delist_date": [None]})
    sb.upsert_from_dataframe(df, keep_industry=True)
    assert sb.UPSERT_KEEP_INDUSTRY_SQL in cur.executed[0]


def test_upsert_from_dataframe_empty(monkeypatch) -> None:
    cur = _patch(monkeypatch)
    assert sb.upsert_from_dataframe(pd.DataFrame()) == 0
    assert cur.executed == []


def test_update_industry(monkeypatch) -> None:
    cur = _patch(monkeypatch)
    n = sb.update_industry({"600000.SH": " 银行 ", "000001.SZ": "", "x": None, "y": "  "})
    assert n == 1
    assert cur.executemany_args == [("银行", "600000.SH")]
    assert "UPDATE" in cur.executed[0]


def test_update_industry_empty(monkeypatch) -> None:
    cur = _patch(monkeypatch)
    assert sb.update_industry({}) == 0
    assert sb.update_industry({"a": "  "}) == 0
    assert cur.executed == []


def test_fetch_ts_codes(monkeypatch) -> None:
    cur = _patch(monkeypatch, _Cur(fetchall=[("600000.SH",), (None,)]))
    assert sb.fetch_ts_codes() == ["600000.SH"]
    assert cur.executed[0].startswith("SELECT ts_code")


def test_fetch_stock_ts_codes(monkeypatch) -> None:
    cur = _patch(monkeypatch, _Cur(fetchall=[("600000.SH",)]))
    assert sb.fetch_stock_ts_codes() == ["600000.SH"]
    assert "market IN" in cur.sql and "delist_date IS NULL" in cur.sql


def test_fetch_ts_codes_by_market(monkeypatch) -> None:
    cur = _patch(monkeypatch, _Cur(fetchall=[("510300.SH",)]))
    assert sb.fetch_ts_codes_by_market(" etf ") == ["510300.SH"]
    assert cur.params == ("ETF",)
    cur2 = _patch(monkeypatch, _Cur(fetchall=[]))
    sb.fetch_ts_codes_by_market("")
    assert cur2.executed[0].startswith("SELECT ts_code") and "WHERE" not in cur2.executed[0].split("FROM")[1]


def test_fetch_all(monkeypatch) -> None:
    import datetime as dt

    cols = [_Col("ts_code"), _Col("name"), _Col("list_date")]
    cur = _patch(monkeypatch, _Cur(fetchall=[("600000.SH", "浦发", dt.date(2026, 8, 7))], cols=cols))
    out = sb.fetch_all()
    assert out == [{"ts_code": "600000.SH", "name": "浦发", "list_date": "2026-08-07"}]


def test_fetch_market_stocks_cn(monkeypatch) -> None:
    cur = _patch(monkeypatch, _Cur(fetchone=(2,), fetchall=[
        ("000001.SZ", "1", "平安", "主板", "2026-01-01", None),
        ("00700.HK", "00700", "腾讯", "HK", None, None),
    ], cols=[_Col("ts_code")]))
    quotes = {"000001.SZ": {"price": 10.0, "changePct": 1.5, "volume": 100, "turnover": 1000}}

    def fake_quotes(codes, use_realtime=False):
        return {c: quotes.get(c, {}) for c in codes}

    from data_sync_service.service import market_quotes as mq

    monkeypatch.setattr(mq, "get_market_quotes_batch", fake_quotes)
    total, items = sb.fetch_market_stocks(market="CN", q="平安", offset=0, limit=50)
    assert total == 2
    assert items[0]["symbol"] == "CN:000001"
    assert items[0]["market"] == "CN"
    assert items[0]["currency"] == "CNY"
    assert items[0]["price"] == 10.0
    assert items[0]["updatedAt"] == "2026-01-01"
    assert items[1]["symbol"] == "HK:00700"
    assert items[1]["currency"] == "HKD"
    assert items[0]["marketCap"] is None


def test_fetch_market_stocks_hk(monkeypatch) -> None:
    cur = _patch(monkeypatch, _Cur(fetchone=(1,), fetchall=[], cols=[_Col("ts_code")]))
    from data_sync_service.service import market_quotes as mq

    monkeypatch.setattr(mq, "get_market_quotes_batch", lambda codes, use_realtime=False: {})
    total, items = sb.fetch_market_stocks(market="HK", limit=10)
    assert total == 1 and items == []
    assert cur.executed[0].endswith("WHERE market = %s") or "market = %s" in cur.executed[0]


def test_fetch_market_stocks_limit_clamped(monkeypatch) -> None:
    cur = _patch(monkeypatch, _Cur(fetchone=(0,), fetchall=[], cols=[_Col("ts_code")]))
    from data_sync_service.service import market_quotes as mq

    monkeypatch.setattr(mq, "get_market_quotes_batch", lambda codes, use_realtime=False: {})
    sb.fetch_market_stocks(limit=99999, offset=-5)
    assert "LIMIT %s OFFSET %s" in cur.executed[1]
    assert cur.params[-2] == 200 and cur.params[-1] == 0


def test_get_market_status(monkeypatch) -> None:
    cur = _patch(monkeypatch, _Cur(fetchone=(5000,)))
    from data_sync_service.db import sync_job_record as sjr

    monkeypatch.setattr(sjr, "get_last_successful_run", lambda jt: {"sync_at": "2026-08-07T10:00:00"})
    out = sb.get_market_status()
    assert out["stocks"] == 5000 and out["lastSyncAt"] == "2026-08-07T10:00:00"
    monkeypatch.setattr(sjr, "get_last_successful_run", lambda jt: None)
    assert sb.get_market_status()["lastSyncAt"] is None
