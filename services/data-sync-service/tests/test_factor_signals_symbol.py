"""Factor signal HK symbol disambiguation + backfill repair (OPT-146). No real DB."""

from __future__ import annotations

from datetime import date, timedelta

import numpy as np

from data_sync_service.service import factor_signals_service as fss


def test_symbol_for_ts() -> None:
    assert fss._symbol_for_ts("00004.HK") == "HK:00004"
    assert fss._symbol_for_ts("00700.hk") == "HK:00700"
    assert fss._symbol_for_ts("600000.SH") == "CN:600000"
    assert fss._symbol_for_ts("000001.SZ") == "CN:000001"
    assert fss._symbol_for_ts("600000") == "CN:600000"  # legacy no-suffix → CN
    assert fss._symbol_for_ts("") == "CN:"


class _FakeCur:
    def __init__(self, script: list) -> None:
        self._script = list(script)
        self.statements: list[tuple] = []

    def execute(self, sql, params=None):
        self.statements.append((sql, params))

    def fetchall(self):
        return self._script.pop(0)

    def fetchone(self):
        rows = self._script.pop(0)
        return rows[0] if rows else None

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _FakeConn:
    def __init__(self, cur: _FakeCur) -> None:
        self._cur = cur
        self.committed = False

    def cursor(self):
        return self._cur

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def commit(self):
        self.committed = True


def _patch_conn(monkeypatch, cur: _FakeCur):
    conn = _FakeConn(cur)
    monkeypatch.setattr(fss, "get_connection", lambda: conn)
    return conn


def test_repair_updates_mislabeled(monkeypatch) -> None:
    cur = _FakeCur([
        [("2025-06-16", "CN:00004", "strong_scoop_exhaustion")],  # bad rows
        [],  # no HK twin → UPDATE
    ])
    conn = _patch_conn(monkeypatch, cur)
    out = fss.repair_hk_symbols()
    assert out == {"repaired": 1, "dropped_duplicates": 0}
    assert conn.committed is True
    update = [s for s in cur.statements if s[0].startswith("UPDATE")][0]
    assert update[1][0] == "HK:00004"


def test_repair_drops_duplicate_when_twin_exists(monkeypatch) -> None:
    cur = _FakeCur([
        [("2025-06-16", "CN:00004", "strong_scoop_exhaustion")],
        [(1,)],  # HK twin exists → DELETE duplicate
    ])
    _patch_conn(monkeypatch, cur)
    out = fss.repair_hk_symbols()
    assert out == {"repaired": 0, "dropped_duplicates": 1}
    assert any(s[0].startswith("DELETE") for s in cur.statements)
    assert not any(s[0].startswith("UPDATE") for s in cur.statements)


def test_repair_nothing_to_do(monkeypatch) -> None:
    cur = _FakeCur([[]])
    _patch_conn(monkeypatch, cur)
    assert fss.repair_hk_symbols() == {"repaired": 0, "dropped_duplicates": 0}


def _scoop_rows(ts_code: str, end: date, n: int = 110) -> list[tuple]:
    """Synthetic scoop: steady climb with a 10% mid dip (passes all gates).

    n=110 so the t-30 uptrend check sits on a valid MA60 (needs index >= 59).
    """
    rows = []
    for i in range(n):
        d = end - timedelta(days=n - 1 - i)
        trend = 10.0 * (1.005**i)
        dip = 1.0
        if 92 <= i <= 107:
            dip = 1.0 - 0.10 * float(np.sin(np.pi * (i - 92) / 15))
        close = trend * dip
        rows.append((ts_code, d, close * 0.999, close * 1.005, close * 0.995, close, 1e6, 1e8))
    return rows


def test_scan_emits_hk_prefixed_symbol(monkeypatch) -> None:
    end = date(2026, 9, 2)
    rows = _scoop_rows("00004.HK", end) + _scoop_rows("600000.SH", end)
    basic = [("00004.HK", "HK名", "金融", "主板"), ("600000.SH", "浦发", "银行", "主板")]
    cur = _FakeCur([rows, basic])
    _patch_conn(monkeypatch, cur)
    saved: list[dict] = []
    monkeypatch.setattr(fss, "upsert_rows", lambda rs: saved.extend(rs) or len(rs))
    n = fss.scan_strong_scoop_exhaustion("2026-09-02")
    assert n == 2
    by_symbol = {r["symbol"]: r for r in saved}
    assert set(by_symbol) == {"HK:00004", "CN:600000"}
    assert by_symbol["HK:00004"]["direction"] == "short"
