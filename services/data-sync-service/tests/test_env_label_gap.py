"""DB-backed branches of env_label.load_env_by_day (M30 gap-closure).

Uses faked connections (no Postgres) to exercise the real per-day
bucket logic: weak / uptrend / fan-by-churn / fan-by-ratio / neutral /
unknown + mainline loader success/failure paths.
"""

from __future__ import annotations

import pytest

from data_sync_service.service import env_label as el


class _FakeCur:
    def __init__(self, rows):
        self._rows = rows

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, *a, **k):
        return None

    def fetchall(self):
        return list(self._rows)


class _FakeConn:
    def __init__(self, rows):
        self._rows = rows

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def cursor(self):
        return _FakeCur(self._rows)


def _patch_sentiment(monkeypatch: pytest.MonkeyPatch, rows):
    monkeypatch.setattr(el, "get_connection", lambda: _FakeConn(rows))


def test_load_env_weak_via_mode(monkeypatch) -> None:
    rows = [("2026-03-02", 100, 40, 1.0, "extreme_caution")]
    _patch_sentiment(monkeypatch, rows)
    monkeypatch.setattr(el, "_load_mainline_top3", lambda s, e: {})
    out = el.load_env_by_day("2026-03-02", "2026-03-02")
    assert out["2026-03-02"] == "weak"


def test_load_env_weak_via_ratio(monkeypatch) -> None:
    # ratio 10/30 = 0.33 < 0.5 -> implicit weak even with normal mode
    rows = [("2026-03-03", 10, 30, 1.0, "normal")]
    _patch_sentiment(monkeypatch, rows)
    monkeypatch.setattr(el, "_load_mainline_top3", lambda s, e: {})
    out = el.load_env_by_day("2026-03-03", "2026-03-03")
    assert out["2026-03-03"] == "weak"


def test_load_env_uptrend(monkeypatch) -> None:
    # ratio 100/40 = 2.5 >= 2.0, premium >= 0, hot mode -> uptrend
    rows = [("2026-03-04", 100, 40, 0.5, "hot")]
    _patch_sentiment(monkeypatch, rows)
    monkeypatch.setattr(el, "_load_mainline_top3", lambda s, e: {"2026-03-04": ["a", "b", "c"]})
    out = el.load_env_by_day("2026-03-04", "2026-03-04")
    assert out["2026-03-04"] == "uptrend"


def test_load_env_fan_via_churn(monkeypatch) -> None:
    # ratio high but mode normal + full churn -> fan (churn >= 2/3)
    rows = [("2026-03-05", 100, 40, -1.0, "normal")]
    _patch_sentiment(monkeypatch, rows)

    # Need prev day to establish prev_top3, then churn 3/3 on second day
    def fake_top3(s, e):
        return {
            "2026-03-04": ["a", "b", "c"],
            "2026-03-05": ["x", "y", "z"],
        }

    monkeypatch.setattr(el, "_load_mainline_top3", fake_top3)
    # First day establishes prev; second day churns
    rows2 = [
        ("2026-03-04", 60, 50, 0.0, "normal"),
        ("2026-03-05", 100, 40, -1.0, "normal"),
    ]
    _patch_sentiment(monkeypatch, rows2)
    out = el.load_env_by_day("2026-03-04", "2026-03-05")
    assert out["2026-03-05"] == "fan"


def test_load_env_fan_via_ratio(monkeypatch) -> None:
    # ratio 60/50 = 1.2 in [0.5, 1.5], no churn -> fan
    rows = [("2026-03-06", 60, 50, 0.0, "normal")]
    _patch_sentiment(monkeypatch, rows)
    monkeypatch.setattr(el, "_load_mainline_top3", lambda s, e: {"2026-03-06": ["a", "b", "c"]})
    # Force no-churn by making prev identical: single day -> churn None -> ratio fan
    out = el.load_env_by_day("2026-03-06", "2026-03-06")
    assert out["2026-03-06"] == "fan"


def test_load_env_neutral_when_full_data_but_no_signal(monkeypatch) -> None:
    # ratio 80/40 = 2.0 but premium negative and ratio outside fan band,
    # mainline present -> TRUE neutral
    rows = [("2026-03-07", 80, 40, -2.0, "caution")]
    _patch_sentiment(monkeypatch, rows)
    monkeypatch.setattr(el, "_load_mainline_top3", lambda s, e: {"2026-03-07": ["a", "b", "c"]})
    out = el.load_env_by_day("2026-03-07", "2026-03-07")
    assert out["2026-03-07"] == "neutral"


def test_load_env_unknown_when_mainline_missing(monkeypatch) -> None:
    # ratio 80/40 = 2.0 but premium negative -> not uptrend, not fan band,
    # no mainline -> absent (unknown, not neutral)
    rows = [("2026-03-08", 80, 40, -2.0, "caution")]
    _patch_sentiment(monkeypatch, rows)
    monkeypatch.setattr(el, "_load_mainline_top3", lambda s, e: {})
    out = el.load_env_by_day("2026-03-08", "2026-03-08")
    assert "2026-03-08" not in out


def test_load_mainline_top3_success(monkeypatch) -> None:
    rows = [
        ("2026-03-04", "银行", 9.0),
        ("2026-03-04", "电力", 8.0),
    ]
    monkeypatch.setattr(el, "get_connection", lambda: _FakeConn(rows))
    out = el._load_mainline_top3("2026-03-04", "2026-03-04")
    assert out == {"2026-03-04": ["银行", "电力"]}


def test_load_mainline_top3_degrades_on_missing_table(monkeypatch) -> None:
    class _BoomConn:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def cursor(self):
            raise RuntimeError("relation does not exist")

    monkeypatch.setattr(el, "get_connection", lambda: _BoomConn())
    assert el._load_mainline_top3("2026-03-04", "2026-03-04") == {}
