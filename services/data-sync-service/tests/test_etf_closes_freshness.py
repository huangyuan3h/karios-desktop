"""Freshness merge for the research ETF panel (2026-09-15).

`data/etf/etf_daily.csv` is a monthly snapshot; the daily sleeve sync writes
the same ts_codes into `daily`. `merge_recent_db_closes` appends only the DB
tail after each series' last CSV date (frozen history stays byte-identical).
"""

from __future__ import annotations

from datetime import date
from typing import Any
from unittest.mock import patch

from data_sync_service.service import harbor


class _FakeCursor:
    def __init__(self, rows: dict[str, list[tuple[Any, ...]]]) -> None:
        self._rows = rows
        self._current: list[tuple[Any, ...]] = []

    def execute(self, _sql: str, params: tuple[Any, ...]) -> None:
        self._current = self._rows.get(str(params[0]), [])

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self._current

    def __enter__(self) -> _FakeCursor:
        return self

    def __exit__(self, *_exc: Any) -> bool:
        return False


class _FakeConn:
    def __init__(self, rows: dict[str, list[tuple[Any, ...]]]) -> None:
        self._rows = rows

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self._rows)

    def __enter__(self) -> _FakeConn:
        return self

    def __exit__(self, *_exc: Any) -> bool:
        return False


def test_merge_appends_only_newer_dates() -> None:
    out = {"518880.SH": {"2026-09-10": 9.0, "2026-09-11": 8.9}}
    rows = {
        "518880.SH": [(date(2026, 9, 10), 9.05), (date(2026, 9, 14), 9.2)],
    }
    with patch("data_sync_service.db.get_connection", return_value=_FakeConn(rows)):
        merged = harbor.merge_recent_db_closes(out, {"518880.SH"})
    assert merged["518880.SH"]["2026-09-11"] == 8.9  # frozen CSV value kept
    assert merged["518880.SH"]["2026-09-14"] == 9.2
    assert len(merged["518880.SH"]) == 3  # older DB row not re-inserted


def test_merge_skips_bad_values_and_survives_db_error() -> None:
    out = {"513350.SH": {"2026-09-11": 1.39}}
    rows = {"513350.SH": [(date(2026, 9, 14), None), (date(2026, 9, 15), 0.0)]}
    with patch("data_sync_service.db.get_connection", return_value=_FakeConn(rows)):
        merged = harbor.merge_recent_db_closes(out, {"513350.SH"})
    assert merged == {"513350.SH": {"2026-09-11": 1.39}}

    with patch("data_sync_service.db.get_connection", side_effect=RuntimeError("db down")):
        assert harbor.merge_recent_db_closes(out, {"513350.SH"}) == out


def test_load_etf_closes_merges_the_db_tail() -> None:
    with patch.object(harbor, "merge_recent_db_closes", side_effect=lambda o, _w: o) as spy:
        out = harbor.load_etf_closes()
    assert spy.call_count >= 1
    assert out  # research panel history still loaded first
