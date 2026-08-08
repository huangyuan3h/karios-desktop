"""db/broker coverage (accounts / state / snapshots)."""

from __future__ import annotations

from data_sync_service.db import broker as bk


class _Cur:
    def __init__(self, fetchall=None, fetchone=None, rowcount=1) -> None:
        self._all = fetchall or []
        self._one = fetchone
        self.rowcount = rowcount
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
        self.commits = 0

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return None

    def cursor(self):
        return self._cur

    def commit(self) -> None:
        self.commits += 1


def _patch(monkeypatch, cur=None):
    if cur is None:
        cur = _Cur()
    monkeypatch.setattr(bk, "ensure_tables", lambda: None)
    monkeypatch.setattr(bk, "get_connection", lambda: _Conn(cur))
    return cur


def test_list_accounts_all(monkeypatch) -> None:
    cur = _patch(monkeypatch, _Cur(fetchall=[("a1", "citic", "主账户", None, "2026-08-01T00:00:00")]))
    out = bk.list_accounts()
    assert out[0]["id"] == "a1" and out[0]["accountMasked"] is None
    assert cur.params is None


def test_list_accounts_filtered(monkeypatch) -> None:
    cur = _patch(monkeypatch, _Cur(fetchall=[]))
    bk.list_accounts(broker="  CITIC ")
    assert cur.params == ("citic",)


def test_create_account(monkeypatch) -> None:
    cur = _patch(monkeypatch)
    out = bk.create_account(account_id="a1", broker="citic", title="t", account_masked="1234", created_at="c", updated_at="u")
    assert out["accountMasked"] == "1234" and out["id"] == "a1"
    assert cur.params == ("a1", "citic", "t", "1234", "c", "u")


def test_update_account_title(monkeypatch) -> None:
    _ = _patch(monkeypatch, _Cur(rowcount=1))
    assert bk.update_account_title(account_id="a1", title="t2", updated_at="u2") is True
    cur2 = _patch(monkeypatch, _Cur(rowcount=0))
    assert bk.update_account_title(account_id="a1", title="t2", updated_at="u2") is False
    assert cur2.executed[0].startswith("UPDATE")


def test_delete_account(monkeypatch) -> None:
    _ = _patch(monkeypatch, _Cur(rowcount=1))
    assert bk.delete_account(account_id="a1") is True
    _patch(monkeypatch, _Cur(rowcount=0))
    assert bk.delete_account(account_id="a1") is False


def test_get_account_state_row_none(monkeypatch) -> None:
    _patch(monkeypatch, _Cur(fetchone=None))
    assert bk.get_account_state_row("a1") is None


def test_get_account_state_row_parses_json(monkeypatch) -> None:
    row = ("a1", "citic", "u", '{"k": 1}', '[{"s": 1}]', "[]", "[]")
    _patch(monkeypatch, _Cur(fetchone=row))
    out = bk.get_account_state_row("a1")
    assert out["overview"] == {"k": 1} and out["positions"] == [{"s": 1}]
    assert out["conditionalOrders"] == [] and out["trades"] == []


def test_get_account_state_row_dict_direct(monkeypatch) -> None:
    row = ("a1", "citic", "u", {"k": 1}, [], [1], '[{"t": 1}]')
    _patch(monkeypatch, _Cur(fetchone=row))
    out = bk.get_account_state_row("a1")
    assert out["overview"] == {"k": 1} and out["trades"] == [{"t": 1}]


def test_ensure_account_state_inserts(monkeypatch) -> None:
    cur = _patch(monkeypatch, _Cur(fetchone=None))
    bk.ensure_account_state(account_id="a1", broker="citic", updated_at="u")
    assert "INSERT INTO" in cur.executed[-1]
    assert len(cur.params) == 7


def test_ensure_account_state_exists_early_return(monkeypatch) -> None:
    cur = _patch(monkeypatch, _Cur(fetchone=("a1",)))
    bk.ensure_account_state(account_id="a1", broker="citic", updated_at="u")
    assert len(cur.executed) == 1  # SELECT only


def test_upsert_account_state_partial_update(monkeypatch) -> None:
    calls = []
    monkeypatch.setattr(bk, "ensure_tables", lambda: None)
    monkeypatch.setattr(bk, "ensure_account_state", lambda **kw: calls.append(("ensure", kw)))
    monkeypatch.setattr(bk, "get_account_state_row", lambda aid: {
        "overview": {"old": 1}, "positions": [{"p": 1}], "conditionalOrders": [{"c": 1}], "trades": [{"t": 1}],
    })

    class C2(_Cur):
        def __init__(self) -> None:
            super().__init__()
            self.all_params = []

        def execute(self, sql, params=None):
            self.executed.append(sql)
            self.params = params
            self.all_params.append(params)
            return self

    cur = C2()
    monkeypatch.setattr(bk, "get_connection", lambda: _Conn(cur))
    bk.upsert_account_state(account_id="a1", broker="citic", updated_at="u2",
                            overview=None, positions=None, conditional_orders=None, trades=None)
    assert "UPDATE broker_account_state" in cur.executed[0]
    assert len(cur.all_params) == 2  # state update + accounts touch
    assert cur.all_params[0][1].obj == {"old": 1}  # merged from current
    assert cur.all_params[0][4].obj == [{"t": 1}]
    assert cur.all_params[1] == ("u2", "a1")


def test_upsert_account_state_full_new(monkeypatch) -> None:
    monkeypatch.setattr(bk, "ensure_tables", lambda: None)
    monkeypatch.setattr(bk, "ensure_account_state", lambda **kw: None)
    monkeypatch.setattr(bk, "get_account_state_row", lambda aid: None)

    class C3(_Cur):
        def __init__(self) -> None:
            super().__init__()
            self.all_params = []

        def execute(self, sql, params=None):
            self.executed.append(sql)
            self.all_params.append(params)
            return self

    cur = C3()
    monkeypatch.setattr(bk, "get_connection", lambda: _Conn(cur))
    bk.upsert_account_state(account_id="a1", broker="citic", updated_at="u",
                            overview={"k": 1}, positions=[{"p": 1}],
                            conditional_orders=[], trades=[])
    assert "UPDATE broker_account_state" in cur.executed[0]
    assert cur.all_params[0][0] == "u"
    assert cur.all_params[0][1].obj == {"k": 1}
    assert cur.all_params[0][2].obj == [{"p": 1}]
    assert cur.all_params[1] == ("u", "a1")


def test_insert_snapshot(monkeypatch) -> None:
    cur = _patch(monkeypatch)
    bk.insert_snapshot(snapshot_id="s1", broker="citic", account_id="a1", captured_at="c",
                       kind="positions", sha256="h", image_bytes=b"\x89PNG", image_type="png",
                       image_name="shot.png", extracted={"k": 1}, created_at="c2")
    assert cur.params[9].obj == {"k": 1}
    assert "ON CONFLICT" in cur.sql


def test_list_snapshots(monkeypatch) -> None:
    cur = _patch(monkeypatch, _Cur(fetchall=[("s1", "citic", None, "c", "positions", "c2")]))
    out = bk.list_snapshots(broker="citic", account_id=None)
    assert out[0]["id"] == "s1" and out[0]["accountId"] is None
    assert cur.params == ("citic", None, None, 20)


def test_list_snapshots_limit_clamped(monkeypatch) -> None:
    cur = _patch(monkeypatch, _Cur(fetchall=[]))
    bk.list_snapshots(broker="citic", account_id="a1", limit=9999)
    assert cur.params == ("citic", "a1", "a1", 200)
    bk.list_snapshots(broker="citic", account_id="a1", limit=0)
    assert cur.params == ("citic", "a1", "a1", 1)


def test_get_snapshot(monkeypatch) -> None:
    _patch(monkeypatch, _Cur(fetchone=None))
    assert bk.get_snapshot("") is None
    assert bk.get_snapshot("  ") is None
    _patch(monkeypatch, _Cur(fetchone=None))
    assert bk.get_snapshot("s1") is None
    row = ("s1", "citic", "a1", "c", "positions", "png", "n.png", '{"k": 1}', "c2")
    _patch(monkeypatch, _Cur(fetchone=row))
    out = bk.get_snapshot("s1")
    assert out["extracted"] == {"k": 1}
    row2 = ("s1", "citic", None, "c", "positions", "png", "n.png", "[1,2]", "c2")
    _patch(monkeypatch, _Cur(fetchone=row2))
    out = bk.get_snapshot("s1")
    assert out["extracted"] == {"raw": [1, 2]}


def test_get_snapshot_image(monkeypatch) -> None:
    _patch(monkeypatch, _Cur(fetchone=None))
    assert bk.get_snapshot_image("") is None
    _patch(monkeypatch, _Cur(fetchone=None))
    assert bk.get_snapshot_image("s1") is None
    _patch(monkeypatch, _Cur(fetchone=(b"\x89PNG", "png", "n.png")))
    out = bk.get_snapshot_image("s1")
    assert out["bytes"] == b"\x89PNG" and out["mediaType"] == "png"
