"""db/stoploss coverage (monotonic ratchet storage)."""

from __future__ import annotations

from datetime import datetime

from data_sync_service.db import stoploss as sl


class _Cur:
    def __init__(self, fetchone=None, fetchall=None, rowcount=1) -> None:
        self._one = fetchone
        self._all = fetchall or []
        self.rowcount = rowcount
        self.sql = None
        self.params = None
        self.executemany_args = None

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return None

    def execute(self, sql, params=None):
        self.sql = sql
        self.params = params
        return self

    def executemany(self, sql, args):
        self.executemany_args = args
        return self

    def fetchone(self):
        return self._one

    def fetchall(self):
        return self._all


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
    monkeypatch.setattr(sl, "get_connection", lambda: _Conn(cur))
    return cur


def test_get_stoploss_none(monkeypatch) -> None:
    cur = _patch(monkeypatch, _Cur(fetchone=None))
    assert sl.get_stoploss("600000.SH") is None
    assert cur.params == ("600000.SH",)


def test_get_stoploss_row(monkeypatch) -> None:
    now = datetime(2026, 8, 7, 12, 0)
    cur = _patch(monkeypatch, _Cur(fetchone=("600000.SH", 10.5, now, "2026-08-07")))
    out = sl.get_stoploss("600000.SH")
    assert out["ts_code"] == "600000.SH"
    assert out["stop_loss_price"] == 10.5
    assert out["updated_at"] == "2026-08-07T12:00:00"
    assert out["as_of_date"] == "2026-08-07"


def test_get_stoploss_null_fields(monkeypatch) -> None:
    cur = _patch(monkeypatch, _Cur(fetchone=("600000.SH", None, None, None)))
    out = sl.get_stoploss("600000.SH")
    assert out["stop_loss_price"] is None
    assert out["updated_at"] is None and out["as_of_date"] is None


def test_get_stoploss_batch_empty(monkeypatch) -> None:
    _patch(monkeypatch)
    assert sl.get_stoploss_batch([]) == {}


def test_get_stoploss_batch(monkeypatch) -> None:
    cur = _patch(monkeypatch, _Cur(fetchall=[("600000.SH", 10.5, None, "d"), (None, 1.0, None, None)]))
    out = sl.get_stoploss_batch(["600000.SH", "000001.SZ"])
    assert list(out) == ["600000.SH"]
    assert out["600000.SH"]["stop_loss_price"] == 10.5
    assert cur.params == (["600000.SH", "000001.SZ"],)


def test_upsert_stoploss(monkeypatch) -> None:
    cur = _patch(monkeypatch)
    sl.upsert_stoploss("600000.SH", 9.8, "2026-08-07")
    assert cur.params[0] == "600000.SH"
    assert cur.params[1] == 9.8
    assert cur.params[3] == "2026-08-07"
    assert "ON CONFLICT" in cur.sql


def test_upsert_stoploss_no_date(monkeypatch) -> None:
    cur = _patch(monkeypatch)
    sl.upsert_stoploss("600000.SH", 9.8)
    assert cur.params[3] is None


def test_upsert_stoploss_batch(monkeypatch) -> None:
    cur = _patch(monkeypatch)
    n = sl.upsert_stoploss_batch([
        {"ts_code": "600000.SH", "stop_loss_price": 10.0, "as_of_date": "d1"},
        {"ts_code": " ", "stop_loss_price": 5.0},  # skipped
        {"ts_code": "600001.SH", "stop_loss_price": None},  # skipped
        {"ts_code": "600002.SH", "stop_loss_price": "11.5"},
    ])
    assert n == 2
    assert cur.executemany_args[0][:2] == ("600000.SH", 10.0)
    assert cur.executemany_args[1][1] == 11.5


def test_upsert_stoploss_batch_empty(monkeypatch) -> None:
    cur = _patch(monkeypatch)
    assert sl.upsert_stoploss_batch([]) == 0
    assert sl.upsert_stoploss_batch([{"ts_code": "", "stop_loss_price": None}]) == 0
    assert cur.executemany_args is None


def test_delete_stoploss(monkeypatch) -> None:
    cur = _patch(monkeypatch)
    sl.delete_stoploss("600000.SH")
    assert cur.params == ("600000.SH",)
    assert "DELETE" in cur.sql


def test_delete_stoploss_batch(monkeypatch) -> None:
    cur = _patch(monkeypatch, _Cur(rowcount=3))
    assert sl.delete_stoploss_batch(["600000.SH", "  ", "000001.SZ"]) == 3
    assert cur.params == (["600000.SH", "000001.SZ"],)


def test_delete_stoploss_batch_empty(monkeypatch) -> None:
    _patch(monkeypatch)
    assert sl.delete_stoploss_batch([]) == 0
    assert sl.delete_stoploss_batch(["  "]) == 0


def test_compute_effective_no_stored(monkeypatch) -> None:
    upserted = {}

    def fake_get(code):
        return None

    def fake_upsert(code, price, as_of_date):
        upserted.update(code=code, price=price, d=as_of_date)

    monkeypatch.setattr(sl, "get_stoploss", fake_get)
    monkeypatch.setattr(sl, "upsert_stoploss", fake_upsert)
    eff, upgraded = sl.compute_effective_stoploss("600000.SH", 9.5, "2026-08-07")
    assert eff == 9.5 and upgraded is False
    assert upserted == {"code": "600000.SH", "price": 9.5, "d": "2026-08-07"}


def test_compute_effective_upgrade(monkeypatch) -> None:
    monkeypatch.setattr(sl, "get_stoploss", lambda code: {"stop_loss_price": 9.0})
    upserted = {}
    monkeypatch.setattr(sl, "upsert_stoploss", lambda code, price, as_of_date=None: upserted.update(price=price))
    eff, upgraded = sl.compute_effective_stoploss("600000.SH", 9.5)
    assert eff == 9.5 and upgraded is False
    assert upserted == {"price": 9.5}


def test_compute_effective_keeps_stored(monkeypatch) -> None:
    monkeypatch.setattr(sl, "get_stoploss", lambda code: {"stop_loss_price": 10.0})
    monkeypatch.setattr(sl, "upsert_stoploss", lambda *a, **k: (_ for _ in ()).throw(AssertionError("no upsert")))
    eff, upgraded = sl.compute_effective_stoploss("600000.SH", 9.5)
    assert eff == 10.0 and upgraded is True
