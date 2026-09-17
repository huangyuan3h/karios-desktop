"""Freshness merge for the research ETF panel (2026-09-15).

`data/etf/etf_daily.csv` is a monthly snapshot; the daily sleeve sync writes
the same ts_codes into `daily`. `merge_recent_db_closes` appends only the DB
tail after each series' last CSV date (frozen history stays byte-identical),
scaled onto the CSV `close_adj` basis (2026-09-17 audit: the DB stores raw
ETF closes, so the tail must be multiplied by the overlap ratio).
"""

from __future__ import annotations

from datetime import date
from typing import Any
from unittest.mock import patch

from data_sync_service.service import harbor


class _FakeCursor:
    """Serves the anchor row (`ORDER BY ... DESC` → fetchone) and the tail.

    The merge issues two queries per ts_code: the basis-anchor row at/before
    the series' last CSV date, then the newer tail rows.
    """

    def __init__(
        self,
        anchors: dict[str, tuple[Any, ...] | None],
        tails: dict[str, list[tuple[Any, ...]]],
    ) -> None:
        self._anchors = anchors
        self._tails = tails
        self._mode = ""
        self._ts = ""

    def execute(self, sql: str, params: tuple[Any, ...]) -> None:
        self._ts = str(params[0])
        self._mode = "anchor" if "DESC" in sql else "tail"

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._anchors.get(self._ts) if self._mode == "anchor" else None

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self._tails.get(self._ts, []) if self._mode == "tail" else []

    def __enter__(self) -> _FakeCursor:
        return self

    def __exit__(self, *_exc: Any) -> bool:
        return False


class _FakeConn:
    def __init__(
        self,
        anchors: dict[str, tuple[Any, ...] | None],
        tails: dict[str, list[tuple[Any, ...]]],
    ) -> None:
        self._anchors = anchors
        self._tails = tails

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self._anchors, self._tails)

    def __enter__(self) -> _FakeConn:
        return self

    def __exit__(self, *_exc: Any) -> bool:
        return False


def test_merge_appends_only_newer_dates() -> None:
    out = {"518880.SH": {"2026-09-10": 9.0, "2026-09-11": 8.9}}
    anchors = {"518880.SH": (date(2026, 9, 11), 8.9, None)}  # ratio = 1.0
    tails = {
        "518880.SH": [
            (date(2026, 9, 10), 9.05, None),  # older DB row must not be re-inserted
            (date(2026, 9, 14), 9.2, None),
        ],
    }
    with patch(
        "data_sync_service.db.get_connection", return_value=_FakeConn(anchors, tails)
    ):
        merged = harbor.merge_recent_db_closes(out, {"518880.SH"})
    assert merged["518880.SH"]["2026-09-11"] == 8.9  # frozen CSV value kept
    assert merged["518880.SH"]["2026-09-14"] == 9.2
    assert len(merged["518880.SH"]) == 3  # older DB row not re-inserted


def test_merge_scales_tail_onto_the_csv_basis() -> None:
    """Raw DB close 2.2 vs CSV 11.0 must NOT append a −80% fake day."""
    out = {"513100.SH": {"2026-09-11": 11.0}}
    anchors = {"513100.SH": (date(2026, 9, 11), 2.2, None)}  # ratio 5.0
    tails = {"513100.SH": [(date(2026, 9, 14), 2.191, None)]}
    with patch(
        "data_sync_service.db.get_connection", return_value=_FakeConn(anchors, tails)
    ):
        merged = harbor.merge_recent_db_closes(out, {"513100.SH"})
    assert merged["513100.SH"]["2026-09-14"] == 2.191 * 5.0


def test_merge_skips_bad_values_and_survives_db_error() -> None:
    out = {"513350.SH": {"2026-09-11": 1.39}}
    anchors = {"513350.SH": (date(2026, 9, 11), 1.39, None)}
    tails = {"513350.SH": [(date(2026, 9, 14), None, None), (date(2026, 9, 15), 0.0, None)]}
    with patch(
        "data_sync_service.db.get_connection", return_value=_FakeConn(anchors, tails)
    ):
        merged = harbor.merge_recent_db_closes(out, {"513350.SH"})
    assert merged == {"513350.SH": {"2026-09-11": 1.39}}

    with patch("data_sync_service.db.get_connection", side_effect=RuntimeError("db down")):
        assert harbor.merge_recent_db_closes(out, {"513350.SH"}) == out


def test_load_etf_closes_merges_the_db_tail() -> None:
    with patch.object(harbor, "merge_recent_db_closes", side_effect=lambda o, _w: o) as spy:
        out = harbor.load_etf_closes()
    assert spy.call_count >= 1
    assert out  # research panel history still loaded first
