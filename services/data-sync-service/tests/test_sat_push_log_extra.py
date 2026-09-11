"""db/sat_push_log.py coverage (fake conn, no Postgres writes)."""

from __future__ import annotations

from data_sync_service.db import sat_push_log


class _Cur:
    def __init__(self):
        self.executemany_calls = []
        self.executed = []

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return None

    def executemany(self, sql, values):
        self.executemany_calls.append((sql, list(values)))
        return self

    def execute(self, sql, params=None):
        self.executed.append(sql)
        return self


class _Conn:
    instances: list = []

    def __init__(self):
        self.cur = _Cur()
        _Conn.instances.append(self)

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return None

    def cursor(self):
        return self.cur

    def commit(self):
        pass


def _patch(monkeypatch):
    _Conn.instances.clear()
    monkeypatch.setattr(sat_push_log, "get_connection", _Conn)
    monkeypatch.setattr(sat_push_log, "ensure_table", lambda: None)


def _screen():
    return {
        "asOf": "2026-09-09",
        "gateOpen": True,
        "breadth": 0.519,
        "snapshotAt": "2026-09-09T14:30:00+08:00",
        "candidates": [{"ts": "300308.SZ", "amp": 3.68, "gapPct": 3.34}],
        "alternates": [{"ts": "600903.SH", "amp": 4.43, "gapPct": 2.1}],
        "blocked": [],
        "skippedC1": [{"ts": "999999.SZ", "amp": 9.9, "gapPct": 9.0}],
    }


def test_log_push_writes_all_slots(monkeypatch):
    _patch(monkeypatch)
    out = sat_push_log.log_push(_screen())
    assert out == {"ok": 3}
    vals = _Conn.instances[-1].cur.executemany_calls[0][1]
    slots = sorted(v[1] for v in vals)
    assert slots == ["alternates", "candidates", "skippedC1"]
    assert vals[0][0] == "2026-09-09" and vals[0][5] is True


def test_log_push_never_raises(monkeypatch):
    _patch(monkeypatch)
    assert sat_push_log.log_push({}) == {"ok": 0}
    out = sat_push_log.log_push(None)
    assert out == {"ok": 0}
