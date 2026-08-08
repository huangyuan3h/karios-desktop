"""db/stock_eastmoney_industry coverage."""

from __future__ import annotations

from data_sync_service.db import stock_eastmoney_industry as sei


class _Cur:
    def __init__(self, fetchall=None, fetchone=None) -> None:
        self._all = fetchall or []
        self._one = fetchone
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
    monkeypatch.setattr(sei, "ensure_table", lambda: None)
    monkeypatch.setattr(sei, "get_connection", lambda: _Conn(cur))
    monkeypatch.setattr(sei, "ensure_sb", lambda: None) if hasattr(sei, "ensure_sb") else None
    return cur


def test_upsert_rows_filters_bad(monkeypatch) -> None:
    cur = _patch(monkeypatch)
    n = sei.upsert_rows([
        {"ts_code": "600000.SH", "industry_name": "银行", "industry_code": "BK0475", "updated_at": "u"},
        {"ts_code": " ", "industry_name": "银行"},  # no ts_code → skipped
        {"ts_code": "600001.SH", "industry_name": "  "},  # no name → skipped
    ])
    assert n == 1
    assert cur.executemany_args == [("600000.SH", "银行", "BK0475", "u")]


def test_upsert_rows_empty(monkeypatch) -> None:
    cur = _patch(monkeypatch)
    assert sei.upsert_rows([]) == 0
    assert sei.upsert_rows([{"ts_code": "", "industry_name": ""}]) == 0
    assert cur.executemany_args is None


def test_lookup_by_ts_codes(monkeypatch) -> None:
    cur = _patch(monkeypatch, _Cur(fetchall=[("600000.SH", "银行"), ("600001.SH", None), (None, "x")]))
    out = sei.lookup_by_ts_codes(["600000.SH", "600001.SH"])
    assert out == {"600000.SH": "银行"}
    assert cur.params == (["600000.SH", "600001.SH"],)


def test_lookup_by_ts_codes_empty(monkeypatch) -> None:
    cur = _patch(monkeypatch)
    assert sei.lookup_by_ts_codes([]) == {}
    assert sei.lookup_by_ts_codes(["  "]) == {}
    assert cur.executed == []


def test_lookup_by_ts_codes_error_returns_empty(monkeypatch) -> None:
    _patch(monkeypatch)
    monkeypatch.setattr(sei, "get_connection", lambda: (_ for _ in ()).throw(RuntimeError("db down")))
    assert sei.lookup_by_ts_codes(["600000.SH"]) == {}


def test_count_rows(monkeypatch) -> None:
    _patch(monkeypatch, _Cur(fetchone=(42,)))
    assert sei.count_rows() == 42
    _patch(monkeypatch, _Cur(fetchone=None))
    assert sei.count_rows() == 0
    _patch(monkeypatch, _Cur(fetchone=(None,)))
    assert sei.count_rows() == 0


def test_coverage_stats(monkeypatch) -> None:
    rows = iter([(120,), (80,)])
    cur = _Cur(fetchall=[], fetchone=None)
    cur.fetchone = lambda: next(rows)
    _patch(monkeypatch, cur)
    out = sei.coverage_stats()
    assert out == {"totalCnStocks": 120, "emMapped": 80, "missingCount": 40}


def test_list_missing_cn_ts_codes(monkeypatch) -> None:
    cur = _patch(monkeypatch, _Cur(fetchall=[("600000.SH",), (None,)]))
    out = sei.list_missing_cn_ts_codes()
    assert out == ["600000.SH"]
    assert cur.params == (500,)


def test_list_missing_cn_ts_codes_after(monkeypatch) -> None:
    cur = _patch(monkeypatch, _Cur(fetchall=[]))
    sei.list_missing_cn_ts_codes(after_ts_code="600000.SH", limit=10)
    assert cur.params == ("600000.SH", 10)


def test_list_missing_cn_ts_codes_limit_clamped(monkeypatch) -> None:
    cur = _patch(monkeypatch, _Cur(fetchall=[]))
    sei.list_missing_cn_ts_codes(limit=99999)
    assert cur.params == (5000,)


def test_list_stale_cn_ts_codes(monkeypatch) -> None:
    cur = _patch(monkeypatch, _Cur(fetchall=[("600000.SH",)]))
    out = sei.list_stale_cn_ts_codes(after_ts_code="600001.SH", limit=7, max_stale_days=0)
    assert out == ["600000.SH"]
    assert cur.params == (1, "600001.SH", 7)  # days clamped to >= 1


def test_list_stale_cn_ts_codes_defaults(monkeypatch) -> None:
    cur = _patch(monkeypatch, _Cur(fetchall=[]))
    sei.list_stale_cn_ts_codes()
    assert cur.params == (30, 500)


def test_search_stocks_by_industry_keyword(monkeypatch) -> None:
    cur = _patch(monkeypatch, _Cur(fetchall=[
        ("600000.SH", "600000", "浦发银行"),
        ("000001.SZ", "bad", "x"),  # non-6-digit symbol → skipped
    ]))
    out = sei.search_stocks_by_industry_keyword("银行")
    assert out == [{"symbol": "CN:600000", "ticker": "600000", "name": "浦发银行", "market": "CN", "source": "emIndustry"}]
    assert cur.params == ("%银行%", 12)


def test_search_stocks_by_industry_keyword_empty_kw(monkeypatch) -> None:
    cur = _patch(monkeypatch)
    assert sei.search_stocks_by_industry_keyword("  ") == []
    assert cur.executed == []


def test_search_stocks_by_industry_keyword_error(monkeypatch) -> None:
    _patch(monkeypatch)
    monkeypatch.setattr(sei, "get_connection", lambda: (_ for _ in ()).throw(RuntimeError("db down")))
    assert sei.search_stocks_by_industry_keyword("银行") == []
