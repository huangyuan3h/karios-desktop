"""service/morning_brief.py coverage: freshness, watchlist context, scoring, selection."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from data_sync_service.service import morning_brief as mb


class _Cur:
    def __init__(self, row_sets):
        self._row_sets = list(row_sets or [[]])
        self._idx = 0
        self.executed = []

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return None

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        return self

    def fetchall(self):
        rows = self._row_sets[self._idx] if self._idx < len(self._row_sets) else []
        self._idx += 1
        return rows


class _Conn:
    def __init__(self, row_sets):
        self._row_sets = list(row_sets)
        self.cursors = []

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return None

    def cursor(self):
        row_sets = self._row_sets[len(self.cursors)] if len(self.cursors) < len(self._row_sets) else []
        c = _Cur(row_sets)
        self.cursors.append(c)
        return c

    def commit(self):
        pass


def _item(**kw) -> dict:
    base = {
        "id": "n1",
        "title": "某公司业绩超预期",
        "sourceId": "src1",
        "publishedAt": (datetime.now(UTC) - timedelta(hours=1)).isoformat(),
        "fetchedAt": datetime.now(UTC).isoformat(),
        "tickers": ["600519.SH"],
        "sectors": ["白酒"],
        "eventType": "earnings",
        "importance": 4,
        "relevanceScore": 70,
        "aiSummary": "业绩增长",
        "actionability": "actionable",
        "link": "http://x",
        "enrichmentStatus": "done",
    }
    base.update(kw)
    return base


class TestFreshness:
    def test_freshness_tiers(self) -> None:
        now = datetime.now(UTC)
        assert mb._freshness_bonus((now - timedelta(hours=1)).isoformat(), "") == 100
        assert mb._freshness_bonus((now - timedelta(hours=4)).isoformat(), "") == 70
        assert mb._freshness_bonus((now - timedelta(hours=8)).isoformat(), "") == 40
        assert mb._freshness_bonus((now - timedelta(hours=20)).isoformat(), "") == 10

    def test_freshness_fallback_and_bad(self) -> None:
        assert mb._freshness_bonus(None, (datetime.now(UTC) - timedelta(hours=1)).isoformat()) == 100
        assert mb._freshness_bonus("garbage", "") == 10

    def test_freshness_z_suffix(self) -> None:
        now = datetime.now(UTC)
        assert mb._freshness_bonus((now - timedelta(hours=1)).isoformat().replace("+00:00", "Z"), "") == 100


class TestWatchlist:
    def test_load_watchlist_context(self, monkeypatch) -> None:
        from data_sync_service import db as dbmod

        conn = _Conn([
            [[("CN:600519", {"positionPct": 50}), ("CN:000001", {"positionPct": 0}), ("CN:600000", "notdict")],
             [("白酒",), ("银行",)]],
        ])
        monkeypatch.setattr(dbmod, "get_connection", lambda: conn)
        held, sectors = mb._load_watchlist_context()
        assert held == {"CN:600519"}
        assert "CN:000001" not in held
        assert sectors == {"白酒", "银行"}

    def test_load_watchlist_context_error(self, monkeypatch) -> None:
        from data_sync_service import db as dbmod

        monkeypatch.setattr(dbmod, "get_connection", lambda: (_ for _ in ()).throw(RuntimeError("db down")))
        assert mb._load_watchlist_context() == (set(), set())

    def test_load_watched_symbols(self, monkeypatch) -> None:
        from data_sync_service import db as dbmod

        conn = _Conn([[[("CN:600519",), ("HK:700",)]]])
        monkeypatch.setattr(dbmod, "get_connection", lambda: conn)
        out = mb._load_watched_symbols()
        assert "CN:600519" in out and "600519" in out and "HK:700" in out and "700" in out

    def test_load_watched_symbols_error(self, monkeypatch) -> None:
        from data_sync_service import db as dbmod

        monkeypatch.setattr(dbmod, "get_connection", lambda: (_ for _ in ()).throw(RuntimeError("down")))
        assert mb._load_watched_symbols() == set()

    def test_watchlist_boost_held_full(self) -> None:
        assert mb._watchlist_boost(_item(tickers=["600519.SH"]), {"600519.SH"}, set()) == 50

    def test_watchlist_boost_held_bare(self) -> None:
        assert mb._watchlist_boost(_item(tickers=["600519.SH"]), {"600519"}, set()) == 50

    def test_watchlist_boost_watched(self) -> None:
        assert mb._watchlist_boost(_item(tickers=["600519.SH"]), set(), set(), {"600519"}) == 30

    def test_watchlist_boost_sector(self) -> None:
        assert mb._watchlist_boost(_item(sectors=["白酒"]), set(), {"白酒"}) == 20

    def test_watchlist_boost_none(self) -> None:
        assert mb._watchlist_boost(_item(tickers=["300001.SZ"], sectors=["光伏"]), set(), set()) == 0


class TestCategory:
    def test_category_watchlist(self) -> None:
        assert mb._assign_category(_item(tickers=["600519.SH"]), {"600519"}) == "watchlist"

    def test_category_risk(self) -> None:
        assert mb._assign_category(_item(title="美国对华加征关税"), set()) == "risk"

    def test_category_macro(self) -> None:
        assert mb._assign_category(_item(title="央行宣布降准"), set()) == "macro"

    def test_category_sector(self) -> None:
        assert mb._assign_category(_item(title="半导体板块走强"), set()) == "sector"

    def test_category_default_macro(self) -> None:
        assert mb._assign_category(_item(title="随便什么新闻"), set()) == "macro"

    def test_category_ai_summary(self) -> None:
        assert mb._assign_category(_item(title="x", aiSummary="涉及战争风险"), set()) == "risk"


class TestScoring:
    def test_score_item(self) -> None:
        item = _item(importance=4, relevanceScore=70)
        score = mb._score_item(item, {"600519.SH"}, set())
        assert score == pytest.approx(4 * 0.3 + 70 * 0.3 + 100 * 0.2 + 50 * 0.2 + 5)

    def test_score_item_no_actionability(self) -> None:
        item = _item(actionability="informational")
        assert mb._score_item(item, set(), set()) > 0

    def test_is_excluded(self) -> None:
        assert mb._is_excluded(_item(title="2026年度回顾")) is True
        assert mb._is_excluded(_item(title="正常新闻")) is False


class TestSelectAndGenerate:
    def test_select_brief_items(self, monkeypatch) -> None:
        monkeypatch.setattr(mb, "fetch_items", lambda limit, hours: (10, [
            _item(id="a", enrichmentStatus="done", importance=5),
            _item(id="b", enrichmentStatus="done", importance=3, title="某公司大跌"),
            _item(id="c", enrichmentStatus="done", importance=0, title="噪音"),
            _item(id="d", enrichmentStatus="done", importance=4, title="2026年度回顾"),
            _item(id="e", enrichmentStatus="pending"),
            _item(id="f", enrichmentStatus="done", importance=4, actionability="historical"),
            _item(id="g", enrichmentStatus="done", importance=4, tickers=["300001.SZ"]),
            _item(id="h", enrichmentStatus="done", importance=4, tickers=["000002.SZ"]),
        ]))
        monkeypatch.setattr(mb, "_load_watchlist_context", lambda: ({"600519.SH"}, {"白酒"}))
        monkeypatch.setattr(mb, "_load_watched_symbols", lambda: {"600519", "300001.SZ"})
        out = mb.select_brief_items()
        assert len(out) <= mb.BRIEF_SIZE
        ids = {x["id"] for x in out}
        assert "c" not in ids and "d" not in ids and "e" not in ids and "f" not in ids
        assert out[0]["id"] == "a"
        assert out[0]["category"] == "watchlist"

    def test_select_brief_items_empty(self, monkeypatch) -> None:
        monkeypatch.setattr(mb, "fetch_items", lambda limit, hours: (0, []))
        monkeypatch.setattr(mb, "_load_watchlist_context", lambda: (set(), set()))
        monkeypatch.setattr(mb, "_load_watched_symbols", lambda: set())
        assert mb.select_brief_items() == []

    def test_generate_brief(self, monkeypatch) -> None:
        monkeypatch.setattr(mb, "select_brief_items", lambda hours=24: [
            _item(id="a", score=80.0, category="macro", sectors=["白酒"], eventType="earnings"),
            _item(id="b", score=70.0, category="watchlist", sectors=["白酒"], eventType="earnings"),
        ])
        stored = {}

        def upsert(**kw):
            stored.update(kw)
            return {"briefDate": kw["brief_date"], "items": kw["items"]}

        monkeypatch.setattr(mb, "upsert_brief", upsert)
        out = mb.generate_brief(brief_type="morning")
        assert out["briefDate"] == datetime.now(UTC).strftime("%Y-%m-%d")
        assert stored["brief_type"] == "morning"
        assert "分类" in stored["macro_overview"]
        assert "热门板块" in stored["macro_overview"]
        assert "事件类型" in stored["macro_overview"]
        assert stored["source_item_ids"] == ["a", "b"]

    def test_generate_brief_empty(self, monkeypatch) -> None:
        monkeypatch.setattr(mb, "select_brief_items", lambda hours=24: [])
        stored = {}

        def upsert(**kw):
            stored.update(kw)
            return {}

        monkeypatch.setattr(mb, "upsert_brief", upsert)
        mb.generate_brief()
        assert stored["macro_overview"] is None
