"""db/alpha_radar coverage: timezone/iso helpers + connection drivers."""

from __future__ import annotations

import datetime

from data_sync_service.db import alpha_radar as ard


def test_shanghai_today_and_day_start() -> None:
    assert len(ard.shanghai_today()) == 10
    iso = ard.shanghai_day_start_iso("2026-08-04")
    dt = datetime.datetime.fromisoformat(iso)
    assert dt.tzinfo is not None
    # +08:00 day start == 16:00 UTC previous day
    assert dt.hour == 16 and dt.day == 3


def test_meta_roundtrip_via_mock(monkeypatch) -> None:
    rows = {"k": "v"}

    class _Cur:
        def __init__(self):
            self.sql = None

        def execute(self, sql, params=None):
            self.sql = sql

        def fetchone(self):
            key = rows["__last_key"] if "__last_key" in rows else None
            return (rows.get(key),)

    cur = _Cur()
    calls = {"get": 0}

    def fake_execute(sql, params=None):
        cur.sql = sql
        if "SELECT" in sql:
            rows["__last_key"] = params[0]
            calls["get"] += 1

    class _Cur2:
        def execute(self, sql, params=None):
            fake_execute(sql, params)

        def fetchone(self):
            key = rows.get("__last_key")
            return (rows.get(key),)

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return None

    class _Conn:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return None

        def cursor(self):
            return _Cur2()

        def commit(self):
            return None

    monkeypatch.setattr(ard, "ensure_tables", lambda: None)
    monkeypatch.setattr(ard, "get_connection", lambda: _Conn())

    assert ard.get_meta("k") == "v"
    ard.set_meta("k2", "v2")  # insert path exercised
    assert calls["get"] == 1


def test_disable_sources_except_empty() -> None:
    assert ard.disable_sources_except(set()) == 0


def test_disable_sources_except_updates(monkeypatch) -> None:
    class _Cur:
        def execute(self, sql, params):
            pass

        rowcount = 3

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return None

    class _Conn:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return None

        def cursor(self):
            return _Cur()

        def commit(self):
            return None

    monkeypatch.setattr(ard, "ensure_tables", lambda: None)
    monkeypatch.setattr(ard, "get_connection", lambda: _Conn())
    assert ard.disable_sources_except({"a", "b"}) == 3


def test_upsert_document_builds_query(monkeypatch) -> None:
    captured: list[tuple] = []

    class _Cur:
        def execute(self, sql, params):
            captured.append((sql, params))

        def fetchone(self):
            return ("doc-1", "src-1", "T", "u", "news", None, None, "2026-08-04", "2026-08-04T00:00:00+00:00", "raw")

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return None

    class _Conn:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return None

        def cursor(self):
            return _Cur()

        def commit(self):
            return None

    monkeypatch.setattr(ard, "ensure_tables", lambda: None)
    monkeypatch.setattr(ard, "get_connection", lambda: _Conn())

    out = ard.upsert_document(
        doc_id="doc-1",
        source_id="src-1",
        title="T",
        url="u",
        category="news",
        summary=None,
        full_text_md=None,
        published_at="2026-08-04",
        fetched_at="2026-08-04T00:00:00+00:00",
        processing_status="raw",
    )
    assert out["id"] == "doc-1"
    assert out["processingStatus"] == "raw"
    assert out.get("_requeued") is True
    sql, params = captured[0]
    assert "ON CONFLICT" in sql
    assert params[0] == "doc-1"
"""db/alpha_radar wave-2: fetch/delete/trend read-write drivers via fake conn."""



class _FakeCur:
    def __init__(self, fetchone=None, fetchall=None, rowcount=0):
        self._one = fetchone
        self._all = fetchall
        self.rowcount = rowcount
        self.executed: list[tuple] = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        return self

    def fetchone(self):
        return self._one

    def fetchall(self):
        return self._all

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return None


class _FakeConn:
    def __init__(self, cur):
        self._cur = cur

    def cursor(self):
        return self._cur

    def commit(self):
        return None

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return None


def _monkey(monkeypatch, cur):
    monkeypatch.setattr(ard, "ensure_tables", lambda: None)
    monkeypatch.setattr(ard, "get_connection", lambda: _FakeConn(cur))


def test_fetch_sources_filters(monkeypatch) -> None:
    cur = _FakeCur(fetchall=[("s1", "N", "u", "news", True, "2026-08-04T00:00:00+00:00", "2026-08-01T00:00:00+00:00")])
    _monkey(monkeypatch, cur)
    out = ard.fetch_sources(enabled_only=True, category="news")
    assert out[0]["id"] == "s1" and out[0]["enabled"] is True
    sql, params = cur.executed[0]
    assert "enabled = TRUE" in sql and "category = %s" in sql and params == ["news"]

    cur2 = _FakeCur(fetchall=[])
    _monkey(monkeypatch, cur2)
    ard.fetch_sources(enabled_only=False, category=None)
    sql2, params2 = cur2.executed[0]
    assert "WHERE" not in sql2


def test_update_source_last_fetch(monkeypatch) -> None:
    cur = _FakeCur()
    _monkey(monkeypatch, cur)
    ard.update_source_last_fetch("s1", "2026-08-04T00:00:00+00:00")
    assert cur.executed[0][1] == ("2026-08-04T00:00:00+00:00", "s1")


def test_fetch_documents_filters(monkeypatch) -> None:
    cur = _FakeCur(fetchone=("2",), fetchall=[("d1", "s1", "T", "u", "news", None, None, None, "2026-08-04T00:00:00+00:00", "raw")])
    _monkey(monkeypatch, cur)
    total, items = ard.fetch_documents(limit=999, offset=-3, category="news", processing_status="raw", hours=24)
    assert total == 2 and items[0]["id"] == "d1"
    sql, params = cur.executed[1]
    assert "category = %s" in sql and "processing_status = %s" in sql and "fetched_at >= %s" in sql
    assert params[:2] == ["news", "raw"]
    assert params[-2:] == [200, 0]  # clamped

    cur2 = _FakeCur(fetchone=(0,), fetchall=[])
    _monkey(monkeypatch, cur2)
    total2, items2 = ard.fetch_documents()
    assert total2 == 0 and items2 == []


def test_fetch_documents_by_status(monkeypatch) -> None:
    cur = _FakeCur(fetchall=[("d1", "s1", "T", "u", "news", None, None, None, "2026-08-04T00:00:00+00:00", "raw")])
    _monkey(monkeypatch, cur)
    out = ard.fetch_documents_by_status(processing_status="raw", limit=1, enabled_sources_only=True)
    assert out[0]["processingStatus"] == "raw"
    sql, params = cur.executed[0]
    assert "JOIN" in sql and "s.enabled = TRUE" in sql


def test_delete_trends_family(monkeypatch) -> None:
    cur = _FakeCur(rowcount=4)
    _monkey(monkeypatch, cur)
    assert ard.delete_trends_before("2026-08-04T00:00:00+00:00") == 4
    assert "DELETE FROM" in cur.executed[-1][0]

    cur2 = _FakeCur(rowcount=0)
    _monkey(monkeypatch, cur2)
    assert ard.delete_trends_older_than_days(0) == 0
    sql2, _ = cur2.executed[0]
    assert "USING" in sql2 and "COALESCE" in sql2

    cur3 = _FakeCur(rowcount=2)
    _monkey(monkeypatch, cur3)
    assert ard.delete_trends_since("2026-08-04T00:00:00+00:00") == 2
    assert ard.delete_all_trends() == 2

    cur4 = _FakeCur(rowcount=7)
    _monkey(monkeypatch, cur4)
    assert ard.delete_trends_for_day("2026-08-04") == 7
    sql4, params4 = cur4.executed[0]
    assert len(params4) == 2


def test_insert_trend_and_row(monkeypatch) -> None:
    row = (
        "t1", "d1", "Trend A", None, None, "催化剂", "1T", "high",
        "policy", "focus", "logic", '["kw"]', '[{"symbol":"600000.SH"}]',
        0.9, "pending", '{"driver_type":"policy"}', "2026-08-04T00:00:00+00:00",
    )
    cur = _FakeCur(fetchone=row)
    _monkey(monkeypatch, cur)
    out = ard.insert_trend(
        trend_id="t1", document_id="d1", trend_name="Trend A",
        macro_theme=None, catalyst_grade=None, catalyst="催化剂",
        global_target="1T", urgency_level="high", driver_type="policy",
        event_focus="focus", logic_summary="logic", keywords_for_mapping=["kw"],
        cn_symbols=[{"symbol": "600000.SH"}], mapping_confidence=0.9,
        risk_status="pending", trend_json={"driver_type": "policy"},
    )
    assert out["id"] == "t1"
    assert out["driverType"] == "policy"
    assert out["cnSymbols"][0]["symbol"] == "600000.SH"
    sql, params = cur.executed[0]
    assert "RETURNING" in sql and len(params) == 17


def test_fetch_trend_by_id_and_delete(monkeypatch) -> None:
    doc = ("DOC T", "http://doc", "news", "2026-08-03T00:00:00+00:00", "2026-08-03T01:00:00+00:00", "sum")
    trend17 = (
        "t1", "d1", "Trend A", "催化剂", "1T", "high", "主题", "grade",
        "policy", "focus", "logic", '["kw"]', '[{"symbol":"600000.SH"}]',
        0.9, "pending", '{"hkSymbols":[{"symbol":"00700.HK"}]}', "2026-08-04T00:00:00+00:00",
    )
    cur = _FakeCur(fetchone=trend17 + doc)
    _monkey(monkeypatch, cur)
    item = ard.fetch_trend_by_id("t1")
    assert item is not None
    assert item["trendName"] == "Trend A"
    assert item["hkSymbols"][0]["symbol"] == "00700.HK"
    assert item.get("documentTitle") or item.get("sourceName")  # doc fields attached

    cur2 = _FakeCur(fetchone=None)
    _monkey(monkeypatch, cur2)
    assert ard.fetch_trend_by_id("missing") is None

    cur3 = _FakeCur(rowcount=1)
    _monkey(monkeypatch, cur3)
    assert ard.delete_trend_by_id("t1") is True
    cur3b = _FakeCur(rowcount=0)
    _monkey(monkeypatch, cur3b)
    assert ard.delete_trend_by_id("t1") is False
    assert ard.delete_trends_for_document("d1") == 0


def test_fetch_trends_filters(monkeypatch) -> None:
    doc = ("DOC T", "http://doc", "news", "2026-08-03T00:00:00+00:00", "2026-08-03T01:00:00+00:00", "sum")
    trend17 = ("t1", "d1", "Trend A", "催化剂", "1T", "high", "主题", "grade",
               "policy", "focus", "logic", "[]", "null", None, "pending",
               "{}", "2026-08-04T00:00:00+00:00")
    cur = _FakeCur(fetchone=(1,), fetchall=[trend17 + doc])
    _monkey(monkeypatch, cur)
    total, items = ard.fetch_trends(
        document_id="d1", risk_status="pending", day="2026-08-04",
        since="2026-08-01T00:00:00+00:00", max_age_days=30, limit=999, offset=-1,
    )
    assert total == 1 and items[0]["id"] == "t1"
    sql, params = cur.executed[1]
    assert params[-2:] == [200, 0]


def test_update_trend_status_mapping_hk(monkeypatch) -> None:
    cur = _FakeCur(rowcount=1)
    _monkey(monkeypatch, cur)
    assert ard.update_trend_risk_status("t1", "high") is True
    assert ard.update_trend_mapping(trend_id="t1", cn_symbols=[{"symbol": "x"}], mapping_confidence=0.5, risk_status="mapped") is True

    cur2 = _FakeCur(fetchone=('{"a":1}',), rowcount=1)
    _monkey(monkeypatch, cur2)
    assert ard.update_trend_hk_mapping(trend_id="t1", hk_symbols=[{"symbol": "00700.HK"}]) is True
    assert cur2.executed[1][1][0] == '{"a": 1, "hkSymbols": [{"symbol": "00700.HK"}]}'

    cur3 = _FakeCur(fetchone=None)
    _monkey(monkeypatch, cur3)
    assert ard.update_trend_hk_mapping(trend_id="t1", hk_symbols=[]) is False

    cur4 = _FakeCur(fetchone=("not-json",), rowcount=1)
    _monkey(monkeypatch, cur4)
    assert ard.update_trend_hk_mapping(trend_id="t1", hk_symbols=[{"s": 1}]) is True


def test_trend_row_legacy_layout() -> None:
    legacy = ("t1", "d1", "Trend A", "催化剂", "1T", "high", "主题", "grade",
              "[]", "null", 0.7, "pending", '{"driverType":"AI"}', "2026-08-04T00:00:00+00:00")
    out = ard._trend_row(legacy)
    assert out["driverType"] == "AI"
    assert out["mappingConfidence"] == 0.7
    assert out["riskStatus"] == "pending"
    assert out["eventFocus"] == "催化剂"

    bad_json = ("t1", "d1", "Trend A", "催化剂", "1T", "high", "主题", "grade",
                "bad-json", "bad-json", 0.7, "pending", "bad-json", "2026-08-04T00:00:00+00:00")
    out2 = ard._trend_row(bad_json)
    assert out2["cnSymbols"] == []
    assert out2["keywordsForMapping"] == []
    assert out2["trendJson"] == {}
