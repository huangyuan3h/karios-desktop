"""db/news.py coverage with fake connection."""

from __future__ import annotations

from data_sync_service.db import news as nw

SRC_COLS = ["id", "name", "url", "enabled", "last_fetch", "created_at", "tier", "category"]
ITEM_COLS = [
    "id", "source_id", "title", "link", "summary", "published_at", "fetched_at",
    "is_read", "is_important", "tickers", "sectors", "event_type", "importance",
    "relevance_score", "ai_summary", "enrichment_status", "enriched_at", "enrichment_model",
    "actionability",
]


class _Cur:
    def __init__(self, rows=None, description=None) -> None:
        self._rows = rows or []
        self._desc = description or [type("C", (), {"name": n})() for n in ITEM_COLS]
        self.rowcount = len(self._rows)
        self.executed: list[tuple] = []
        self.queries: list[str] = []

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return None

    def execute(self, sql, params=None):
        self.queries.append(sql)
        self.executed.append((sql, params))
        return self

    def fetchone(self):
        if self.queries and "COUNT(" in self.queries[-1]:
            return (len(self._rows),)
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return self._rows

    @property
    def description(self):
        return self._desc


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


def _patch(monkeypatch, rows=None, description=None):
    cur = _Cur(rows, description)
    monkeypatch.setattr(nw, "ensure_tables", lambda: None)
    monkeypatch.setattr(nw, "get_connection", lambda: _Conn(cur))
    return cur


def _src_row(*vals):
    return vals


def _item_row(*vals):
    return vals


# ---- sources ---------------------------------------------------------------

def test_fetch_sources_enabled_and_all(monkeypatch) -> None:
    rows = [
        ("s1", "财经网", "http://a.com", True, "2026-08-07T10:00:00", "2026-08-01T00:00:00", "B", "macro"),
        ("s2", "科创", "http://b.com", False, None, "2026-08-01T00:00:00", None, None),
    ]
    cur = _patch(monkeypatch, rows, [type("C", (), {"name": n})() for n in SRC_COLS])
    out = nw.fetch_sources(enabled_only=True)
    assert len(out) == 2
    assert out[0]["id"] == "s1" and out[0]["enabled"] is True
    assert out[0]["lastFetch"] == "2026-08-07T10:00:00"
    assert out[1]["tier"] == "D" and out[1]["category"] is None
    assert "enabled = TRUE" in cur.queries[0]

    cur = _patch(monkeypatch, [], [type("C", (), {"name": n})() for n in SRC_COLS])
    nw.fetch_sources(enabled_only=False)
    assert "enabled = TRUE" not in cur.queries[0]


def test_create_source(monkeypatch) -> None:
    row = ("s1", "财经网", "http://a.com", True, None, "2026-08-01T00:00:00", "C", "sector")
    cur = _patch(monkeypatch, [row], [type("C", (), {"name": n})() for n in SRC_COLS])
    out = nw.create_source(source_id="s1", name="财经网", url="http://a.com", tier="C", category="sector")
    assert out["id"] == "s1"
    assert out["tier"] == "C"
    assert out["category"] == "sector"
    assert cur.executed[0][1][2] == "http://a.com"


def test_update_source_all_fields(monkeypatch) -> None:
    row = ("s1", "新名", "http://a.com", False, None, "2026-08-01T00:00:00", "A", "x")
    cur = _patch(monkeypatch, [row], [type("C", (), {"name": n})() for n in SRC_COLS])
    out = nw.update_source(source_id="s1", name="新名", enabled=False, tier="A", category="x")
    assert out["name"] == "新名" and out["enabled"] is False
    assert cur.executed[0][0].count("%s") == 5


def test_update_source_no_fields_returns_none(monkeypatch) -> None:
    cur = _patch(monkeypatch, [])
    assert nw.update_source(source_id="s1") is None
    assert cur.queries == []


def test_update_source_missing_row_returns_none(monkeypatch) -> None:
    _ = _patch(monkeypatch, [])
    out = nw.update_source(source_id="ghost", name="x")
    assert out is None


def test_delete_source(monkeypatch) -> None:
    cur = _patch(monkeypatch, [])
    cur.rowcount = 1
    assert nw.delete_source("s1") is True
    cur2 = _patch(monkeypatch, [])
    cur2.rowcount = 0
    assert nw.delete_source("s1") is False


# ---- items -----------------------------------------------------------------

def test_fetch_items_all_filters(monkeypatch) -> None:
    rows = [
        ("i1", "s1", "标题 <b>x</b>", "http://l", "<p>摘要</p>", "2026-08-07T09:00:00", "2026-08-07T09:00:00",
         False, True, ["600000"], ["银行"], "宏观", 3, 80, "AI 摘要", "done", "2026-08-07T09:00:00", "gpt-4o", "执行"),
    ]
    cur = _patch(monkeypatch, rows)
    cur.rowcount = 1
    total, items = nw.fetch_items(limit=10, offset=5, source_id="s1", is_read=False, hours=24)
    assert total == 1
    assert items[0]["title"] == "标题 x"  # HTML stripped
    assert items[0]["summary"] == "摘要"
    assert items[0]["tickers"] == ["600000"]
    assert items[0]["isRead"] is False and items[0]["isImportant"] is True
    assert items[0]["actionability"] == "执行"
    assert cur.executed[0][0].count("%s") == 3  # count query
    assert cur.executed[1][0].count("%s") == 5  # select query


def test_fetch_items_no_filters(monkeypatch) -> None:
    cur = _patch(monkeypatch, [])
    total, items = nw.fetch_items(limit=100)
    assert total == 0 and items == []
    assert "WHERE" not in cur.executed[0][0]
    assert cur.executed[1][0].count("%s") == 2  # limit + offset


def test_upsert_item_strips_html(monkeypatch) -> None:
    row = ("i1", "s1", "标题 x", "http://l", "摘要 y", None, "2026-08-07T09:00:00",
           False, False, None, None, None, None, None, None, None, None, None)
    cur = _patch(monkeypatch, [row])
    out = nw.upsert_item(
        item_id="i1", source_id="s1", title="标题 <b>x</b>", link="http://l", summary="摘要 <i>y</i>",
        fetched_at="2026-08-07T09:00:00",
    )
    assert out["title"] == "标题 x"
    assert out["summary"] == "摘要 y"
    assert out["tickers"] is None
    assert cur.executed[0][1][2] == "标题 x"  # stripped at write time


def test_mark_item_read_important(monkeypatch) -> None:
    cur = _patch(monkeypatch, [])
    cur.rowcount = 1
    assert nw.mark_item_read("i1") is True
    assert nw.mark_item_important("i1", True) is True
    cur2 = _patch(monkeypatch, [])
    cur2.rowcount = 0
    assert nw.mark_item_read("i2") is False
    assert nw.mark_item_important("i2", False) is False


def test_update_source_last_fetch_and_delete_old(monkeypatch) -> None:
    cur = _patch(monkeypatch, [])
    nw.update_source_last_fetch("s1", "2026-08-07T10:00:00")
    assert "last_fetch" in cur.executed[0][0]
    cur2 = _patch(monkeypatch, [])
    cur2.rowcount = 5
    assert nw.delete_old_items(hours=72) == 5


# ---- enrichment helpers ----------------------------------------------------

def test_fetch_pending_enrichment(monkeypatch) -> None:
    rows = [
        ("i1", "s1", "t", "http://l", "s", None, "2026-08-07T09:00:00",
         False, False, None, None, None, None, None, "AI", "failed", None, "gpt"),
    ]
    cur = _patch(monkeypatch, rows)
    out = nw.fetch_pending_enrichment(limit=50)
    assert out[0]["id"] == "i1"
    assert out[0]["enrichmentStatus"] == "failed"
    assert out[0]["aiSummary"] == "AI"
    assert cur.executed[0][1] == (50,)


def test_update_item_enrichment(monkeypatch) -> None:
    cur = _patch(monkeypatch, [])
    cur.rowcount = 1
    ok = nw.update_item_enrichment(
        item_id="i1",
        tickers=["600000"],
        sectors=["银行"],
        event_type="宏观",
        importance=3,
        relevance_score=80,
        ai_summary="摘要",
        actionability="执行",
        enrichment_model="gpt-4o",
    )
    assert ok is True
    params = cur.executed[0][1]
    assert params[0] == ["600000"]
    assert params[7] == "done"
    assert params[10] == "i1"

    cur2 = _patch(monkeypatch, [])
    cur2.rowcount = 0
    assert nw.update_item_enrichment(item_id="ghost") is False


def test_count_by_enrichment_status(monkeypatch) -> None:
    _ = _patch(monkeypatch, [("done", 10), ("failed", 2), ("pending", 3)])
    assert nw.count_by_enrichment_status() == {"done": 10, "failed": 2, "pending": 3}


def test_strip_html_from_existing_items(monkeypatch) -> None:
    rows = [("i1", "<p>old</p>"), ("i2", "clean"), ("i3", "<div>a</div>")]
    cur = _patch(monkeypatch, rows)
    assert nw.strip_html_from_existing_items() == 2
    assert cur.executed[1][1] == ("old", "i1")
    assert cur.executed[2][1] == ("a", "i3")
