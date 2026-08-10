"""db/tv.py coverage with fake connection."""

from __future__ import annotations

from data_sync_service.db import tv as tvdb

SCREENER_COLS = ["id", "name", "url", "enabled", "updated_at", "mode", "market", "filter_json", "api_columns"]
SNAP_COLS = ["id", "screener_id", "captured_at", "row_count", "payload"]


class _Cur:
    def __init__(self, rows=None, description=None) -> None:
        self._rows = rows or []
        self._desc = description or [type("C", (), {"name": n})() for n in SNAP_COLS]
        self.rowcount = len(self._rows)
        self.executed: list[tuple] = []

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return None

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        return self

    def fetchone(self):
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


def _patch(monkeypatch, rows=None):
    cur = _Cur(rows)
    monkeypatch.setattr(tvdb, "ensure_tables", lambda: None)
    monkeypatch.setattr(tvdb, "get_connection", lambda: _Conn(cur))
    return cur


def _screener_row(*vals):
    return vals


def _snap_row(*vals):
    return vals


# ---- screeners -------------------------------------------------------------

def test_fetch_screeners(monkeypatch) -> None:
    rows = [
        ("s1", "龙头池", "http://tv/1", True, "2026-08-07T10:00:00", "chrome", "CN",
         {"filters": []}, ["symbol", "name"]),
    ]
    cur = _patch(monkeypatch, rows)
    out = tvdb.fetch_screeners()
    assert out[0]["id"] == "s1"
    assert out[0]["mode"] == "chrome"
    assert out[0]["filterJson"] == {"filters": []}
    assert out[0]["apiColumns"] == ["symbol", "name"]
    assert "ORDER BY updated_at DESC" in cur.executed[0][0]


def test_fetch_screener_by_id_hit_miss(monkeypatch) -> None:
    row = ("s1", "名", "http://u", False, "t", "api", "HK", None, None)
    cur = _patch(monkeypatch, [_screener_row(*row)])
    out = tvdb.fetch_screener_by_id("s1")
    assert out is not None
    assert out["enabled"] is False
    assert out["filterJson"] is None and out["apiColumns"] is None
    assert cur.executed[0][1] == ("s1",)

    _ = _patch(monkeypatch, [])
    assert tvdb.fetch_screener_by_id("s1") is None
    assert tvdb.fetch_screener_by_id("") is None
    assert tvdb.fetch_screener_by_id("  ") is None


def test_upsert_screener_mode_clamp_and_json(monkeypatch) -> None:
    cur = _patch(monkeypatch)
    tvdb.upsert_screener(
        screener_id="s1", name="n", url="", enabled=True,
        created_at="c", updated_at="u", mode="BOGUS",
        market="  ", filter_json={"a": 1}, api_columns=["x"],
    )
    params = cur.executed[0][1]
    assert params[3] is True
    assert params[6] == "chrome"  # invalid mode clamped
    assert params[7] is None  # blank market -> None
    assert params[8] == '{"a": 1}'  # json serialized
    assert params[9] == '["x"]'


def test_update_screener(monkeypatch) -> None:
    cur = _patch(monkeypatch, [])
    cur.rowcount = 1
    ok = tvdb.update_screener(
        screener_id="s1", name="n", url="u", enabled=True,
        updated_at="t", mode="api", market="CN", filter_json=None, api_columns=None,
    )
    assert ok is True
    params = cur.executed[0][1]
    assert params[4] == "api"
    assert params[6] is None and params[7] is None  # filter_json/api_columns
    assert params[8] == "s1"

    cur2 = _patch(monkeypatch, [])
    cur2.rowcount = 0
    assert tvdb.update_screener(screener_id="ghost", name="n", url="u", enabled=True, updated_at="t") is False


def test_delete_screener(monkeypatch) -> None:
    cur = _patch(monkeypatch, [])
    cur.rowcount = 1
    assert tvdb.delete_screener("s1") is True
    cur2 = _patch(monkeypatch, [])
    cur2.rowcount = 0
    assert tvdb.delete_screener("s1") is False


def test_count_screeners(monkeypatch) -> None:
    _ = _patch(monkeypatch, [(5,)])
    assert tvdb.count_screeners() == 5
    _ = _patch(monkeypatch, [(None,)])
    assert tvdb.count_screeners() == 0


# ---- snapshots -------------------------------------------------------------

def test_upsert_snapshot(monkeypatch) -> None:
    cur = _patch(monkeypatch)
    tvdb.upsert_snapshot(snapshot_id="sn1", screener_id="s1", captured_at="t", row_count=3, payload={"rows": []})
    params = cur.executed[0][1]
    assert params[3] == 3
    assert params[4] == '{"rows": []}'


def test_list_snapshots_for_screener(monkeypatch) -> None:
    rows = [("sn1", "s1", "t1", 5)]
    cur = _patch(monkeypatch, rows)
    out = tvdb.list_snapshots_for_screener("s1", limit=10)
    assert out == [{"id": "sn1", "screenerId": "s1", "capturedAt": "t1", "rowCount": 5}]
    assert cur.executed[0][1] == ("s1", 10)


def test_list_snapshots_for_screener_full(monkeypatch) -> None:
    rows = [
        ("sn1", "s1", "t1", 5, {"screenTitle": "龙头池", "filters": ["涨>5", ""], "rows": []}),
        ("sn2", "s1", "t2", 0, None),
    ]
    cur = _patch(monkeypatch, rows)
    out = tvdb.list_snapshots_for_screener_full("s1", limit=200)
    assert out[0]["snapshotId"] == "sn1"
    assert out[0]["screenTitle"] == "龙头池"
    assert out[0]["filters"] == ["涨>5"]
    assert out[1]["screenTitle"] is None and out[1]["filters"] == []
    assert cur.executed[0][1] == ("s1", 200)


def test_list_latest_snapshots_for_screeners(monkeypatch) -> None:
    assert tvdb.list_latest_snapshots_for_screeners([]) == {}
    assert tvdb.list_latest_snapshots_for_screeners(["", " "]) == {}
    rows = [("sn1", "s1", "t1", 5, {"screenTitle": "池"})]
    _ = _patch(monkeypatch, rows)
    out = tvdb.list_latest_snapshots_for_screeners(["s1", "s1"])
    assert out == {"s1": {"snapshotId": "sn1", "screenerId": "s1", "capturedAt": "t1", "rowCount": 5, "screenTitle": "池", "filters": []}}


def test_snapshot_detail_from_row_variants() -> None:
    row = (
        "sn1", "s1", "t1", 2,
        {
            "screenTitle": "标题",
            "filters": ["a", ""],
            "headers": ["symbol", "name"],
            "rows": [{"symbol": "600000", "name": 1}, None],
            "url": "http://tv/x",
        },
    )
    d = tvdb._snapshot_detail_from_row(row)
    assert d["headers"] == ["symbol", "name"]
    assert d["rows"] == [{"symbol": "600000", "name": "1"}, {}]  # None row -> empty dict
    assert d["url"] == "http://tv/x"
    bad = ("sn2", "s2", "t2", 0, None)
    d2 = tvdb._snapshot_detail_from_row(bad)
    assert d2["rows"] == [] and d2["screenTitle"] is None


def test_list_latest_snapshot_details_for_screeners(monkeypatch) -> None:
    assert tvdb.list_latest_snapshot_details_for_screeners([]) == {}
    rows = [("sn1", "s1", "t1", 1, {"rows": [{"a": 1}]})]
    _ = _patch(monkeypatch, rows)
    out = tvdb.list_latest_snapshot_details_for_screeners(["s1", "s2"])
    assert out["s1"]["id"] == "sn1"
    assert out["s2"] is None  # no snapshot -> None
    assert out["s1"]["rows"] == [{"a": "1"}]


def test_fetch_snapshot_detail(monkeypatch) -> None:
    assert tvdb.fetch_snapshot_detail("") is None
    assert tvdb.fetch_snapshot_detail("  ") is None
    _ = _patch(monkeypatch, [])
    assert tvdb.fetch_snapshot_detail("ghost") is None
    row = ("sn1", "s1", "t1", 3, {"rows": [{"x": "y"}]})
    _ = _patch(monkeypatch, [_snap_row(*row)])
    out = tvdb.fetch_snapshot_detail("sn1")
    assert out["id"] == "sn1" and out["rowCount"] == 3


def test_list_enabled_api_screener_symbols(monkeypatch) -> None:
    """2026-08-09: enabled api screener snapshots → CN:XXXXXX symbol list."""
    payload = {
        "rows": [
            {"Symbol": "601088", "Name": "a"},
            {"Symbol": "300001", "Name": "b"},
            {"Symbol": "", "Name": "c"},
            {"Name": "no-symbol"},
        ]
    }
    cur = _patch(monkeypatch, [(payload,), (payload,)])
    out = tvdb.list_enabled_api_screener_symbols(market="cn")
    assert out == ["CN:601088", "CN:300001"]
    assert len(cur.executed) == 1


def test_list_enabled_api_screener_symbols_empty(monkeypatch) -> None:
    cur = _patch(monkeypatch, [])
    assert tvdb.list_enabled_api_screener_symbols(market="cn") == []
