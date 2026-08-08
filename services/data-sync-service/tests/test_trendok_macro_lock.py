from __future__ import annotations

from unittest.mock import patch

from data_sync_service.service.trendok import (
    MACRO_LOCK_DOWN_THRESHOLD,
    _read_latest_sentiment_for_macro_lock,
    apply_macro_override_lock,
    clear_trendok_cache,
    macro_override_lock_active,
)


def _sample_result(*, buy_action: str = "buy", buy_mode: str = "B_momentum") -> dict:
    return {
        "symbol": "CN:600000",
        "buyAction": buy_action,
        "buyMode": buy_mode,
        "buyWhy": "test",
        "buyChecks": {},
        "riskAlerts": [],
    }


def test_macro_lock_active_on_extreme_caution() -> None:
    assert macro_override_lock_active("extreme_caution", 1000) is True


def test_macro_lock_active_on_down_threshold() -> None:
    assert macro_override_lock_active("normal", MACRO_LOCK_DOWN_THRESHOLD) is True
    assert macro_override_lock_active("normal", MACRO_LOCK_DOWN_THRESHOLD - 1) is False


def test_macro_lock_inactive_on_capitulation() -> None:
    assert macro_override_lock_active("capitulation_v_bottom", 4600) is False


def test_macro_lock_inactive_on_confirmed_uptrend() -> None:
    assert macro_override_lock_active("confirmed_uptrend", 800) is False


def test_apply_macro_lock_forces_all_buy_actions_to_avoid() -> None:
    rows = [
        _sample_result(buy_action="buy", buy_mode="B_momentum"),
        _sample_result(buy_action="B_pullback", buy_mode="B_momentum"),
        _sample_result(buy_action="wait", buy_mode="A_pullback"),
    ]
    out = apply_macro_override_lock(rows, "extreme_caution", 4600)
    for row in out:
        assert row["buyAction"] == "avoid"
        assert row["buyMode"] == "none"
        assert row["macroLock"]["active"] is True
        assert row["buyChecks"]["blocked_macro_lock"] is True
        assert any(a.get("code") == "macro_override_lock" for a in row["riskAlerts"])


def test_apply_macro_lock_skips_when_inactive() -> None:
    row = _sample_result()
    out = apply_macro_override_lock([row], "normal", 1000)
    assert out[0]["buyAction"] == "buy"
    assert "macroLock" not in out[0]


def test_macro_lock_reader_uses_cache_without_repeat_db_calls() -> None:
    clear_trendok_cache()
    with patch(
        "data_sync_service.service.trendok.list_days",
        return_value=[{"riskMode": "extreme_caution", "downCount": 4000}],
    ) as list_days:
        first = _read_latest_sentiment_for_macro_lock()
        second = _read_latest_sentiment_for_macro_lock()
    assert first == ("extreme_caution", 4000)
    assert second == first
    assert list_days.call_count == 1



def test_macro_lock_fails_closed_on_sentiment_read_failure(monkeypatch) -> None:
    """H5 (2026-08-08): a sentiment read failure must not disable the crash
    lock — the read degrades to extreme_caution (lock active) and is NOT
    cached, so recovery re-locks/unlocks correctly."""
    from data_sync_service.service.trendok import _read_latest_sentiment_for_macro_lock

    def boom() -> None:
        raise RuntimeError("db down")

    monkeypatch.setattr("data_sync_service.service.trendok._shanghai_today_iso", boom)
    monkeypatch.setattr("data_sync_service.service.trendok._macro_lock_cache", {})

    risk_mode, down_count = _read_latest_sentiment_for_macro_lock()
    assert risk_mode == "extreme_caution"
    assert down_count == 3500
    # failure is not cached — the next call retries the read
    from data_sync_service.service import trendok as trendok

    assert "latest" not in trendok._macro_lock_cache


def test_macro_lock_read_failure_not_cached() -> None:
    """The failed read must not poison the cache with a permanent (None, None)
    unlock; a subsequent successful read replaces it."""

    cache = {}
    import data_sync_service.service.trendok as trendok

    original = trendok._macro_lock_cache
    try:
        trendok._macro_lock_cache = cache

        def boom() -> None:
            raise RuntimeError("db down")

        trendok._shanghai_today_iso = boom
        trendok._read_latest_sentiment_for_macro_lock()
        assert "latest" not in cache

        # simulate recovery: empty table is a valid no-lock state
        trendok._shanghai_today_iso = lambda: "2026-08-08"
        trendok.list_days = lambda as_of_date=None, days=1: []
        trendok.get_latest_date = lambda: None
        risk_mode, down_count = trendok._read_latest_sentiment_for_macro_lock()
        assert risk_mode is None
        assert down_count is None
        assert "latest" in cache
    finally:
        trendok._macro_lock_cache = original
