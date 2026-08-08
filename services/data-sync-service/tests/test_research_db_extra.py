"""db/research coverage (research reports storage)."""

from __future__ import annotations

from datetime import date

from data_sync_service.db import research as rs


class _Col:
    def __init__(self, name) -> None:
        self.name = name


class _Cur:
    def __init__(self, fetchall=None, fetchone=None, rowcount=1, cols=None) -> None:
        self._all = fetchall or []
        self._one = fetchone
        self.rowcount = rowcount
        self.description = cols or [_Col(f"c{i}") for i in range(8)]
        self.sql = None
        self.params = None
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
    monkeypatch.setattr(rs, "ensure_table", lambda: None)
    monkeypatch.setattr(rs, "get_connection", lambda: _Conn(cur))
    return cur


def test_numeric() -> None:
    assert rs._numeric(None) is None
    assert rs._numeric("12.5") == 12.5
    assert rs._numeric("abc") is None
    assert rs._numeric(3) == 3.0


def test_upsert_empty(monkeypatch) -> None:
    _patch(monkeypatch)
    assert rs.upsert_research_reports([]) == 0


def test_upsert_full_mapping(monkeypatch) -> None:
    cur = _patch(monkeypatch, _Cur(rowcount=1))
    n = rs.upsert_research_reports([{
        "infoCode": " 1 ",
        "stockCode": "600000.SH",
        "stockName": "浦发",
        "title": "深度报告",
        "orgName": "中金",
        "rating": "买入",
        "targetPrice": "12.5",
        "epsThisYear": "1.2",
        "peThisYear": "bad",
        "industryName": "银行",
        "market": " CN ",
        "publishDate": "2026-08-07",
        "encodeUrl": "http://x",
        "source": "eastmoney",
    }])
    assert n == 1
    assert cur.params[0] == "1"
    assert cur.params[1] == "600000.SH"
    assert cur.params[6] == 12.5
    assert cur.params[7] == 1.2
    assert cur.params[8] is None  # bad pe → None
    assert cur.params[11] == "2026-08-07"
    assert cur.params[13] == "eastmoney"


def test_upsert_org_sname_fallback(monkeypatch) -> None:
    cur = _patch(monkeypatch, _Cur(rowcount=0))
    rs.upsert_research_reports([{
        "infoCode": "2", "stockCode": "s", "stockName": "n", "title": "t",
        "orgSName": "国泰君安", "publishDate": "2026-08-07",
    }])
    assert cur.params[4] == "国泰君安"
    assert cur.params[6] is None and cur.params[8] is None
    assert cur.params[9] is None and cur.params[12] is None


def test_upsert_counts_inserted(monkeypatch) -> None:
    cur = _patch(monkeypatch, _Cur(rowcount=0))
    assert rs.upsert_research_reports([{
        "infoCode": "3", "stockCode": "s", "stockName": "n", "title": "t",
        "publishDate": "2026-08-07",
    }]) == 0


def test_list_recent_no_filters(monkeypatch) -> None:
    cols = [_Col("id"), _Col("title"), _Col("publish_date")]
    cur = _patch(monkeypatch, _Cur(fetchall=[(1, "t", date(2026, 8, 7))], cols=cols))
    out = rs.list_recent_reports(limit=50, window_days=None)
    assert out == [{"id": 1, "title": "t", "publish_date": "2026-08-07"}]
    assert cur.params == [50]


def test_list_recent_filters(monkeypatch) -> None:
    cols = [_Col("id"), _Col("alpha_score")]
    cur = _patch(monkeypatch, _Cur(fetchall=[(1, None)], cols=cols))
    out = rs.list_recent_reports(window_days=7, min_score=70.0, limit=10)
    assert out == [{"id": 1, "alpha_score": None}]
    assert cur.params == [7, 70.0, 10]
    assert "alpha_score IS NOT NULL" in cur.sql


def test_list_recent_window_days_zero(monkeypatch) -> None:
    cur = _patch(monkeypatch, _Cur(fetchall=[], cols=[_Col("id")]))
    rs.list_recent_reports(window_days=0, limit=5)
    assert cur.params == [5]
    assert "WHERE" not in cur.sql.split("ORDER BY")[0].split("FROM")[1][:0] or "publish_date >=" not in cur.sql


def test_fetch_reports_for_score_window(monkeypatch) -> None:
    cols = [_Col("id"), _Col("publish_date")]
    cur = _patch(monkeypatch, _Cur(fetchall=[(2, date(2026, 8, 1))], cols=cols))
    out = rs.fetch_reports_for_score_window(window_days=14)
    assert out == [{"id": 2, "publish_date": "2026-08-01"}]
    assert cur.params == (14,)


def test_update_report_scores(monkeypatch) -> None:
    cur = _patch(monkeypatch)
    n = rs.update_report_scores([(85.5, 1), (70.0, 2)])
    assert n == 2
    assert cur.executed == [f"UPDATE {rs.TABLE_NAME} SET alpha_score = %s WHERE id = %s"] * 2


def test_update_report_scores_empty(monkeypatch) -> None:
    _patch(monkeypatch)
    assert rs.update_report_scores([]) == 0


def test_research_stats(monkeypatch) -> None:
    cur = _Cur(fetchall=[])
    vals = iter([(100,), (5,), (20,), (15,)])
    cur.fetchone = lambda: next(vals)
    _patch(monkeypatch, cur)
    out = rs.research_stats()
    assert out == {"total": 100, "last24h": 5, "last7d": 20, "stocks7d": 15}
