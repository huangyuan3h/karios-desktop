"""Tests for the pooled Tushare client (OPT-124).

All policy (rotation, sliding window, daily budget, back-off) is exercised
with a fake pro factory + fake clock/sleep — no network, no Postgres.
"""

from __future__ import annotations

from datetime import date
from types import SimpleNamespace

import pytest

from data_sync_service.clients import tushare_pool as tp
from data_sync_service.clients.tushare_pool import (
    ALL_KEYS_HOT_SLEEP_SECONDS,
    DailyQuotaExhausted,
    TusharePool,
    is_rate_limit_error,
)


class FakeClock:
    def __init__(self) -> None:
        self.now = 1000.0
        self.sleeps: list[float] = []

    def clock(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


class FakePro:
    """Duck-typed ts.pro_api stand-in with per-endpoint scripted behavior."""

    def __init__(self, token: str, behaviors: dict) -> None:
        self.token = token
        self.behaviors = behaviors
        self.calls: list[tuple[str, tuple, dict]] = []

    def __getattr__(self, endpoint: str):
        def _fn(*args, **kwargs):
            self.calls.append((endpoint, args, kwargs))
            behavior = self.behaviors.get(endpoint, "ok")
            if callable(behavior):
                return behavior()
            if isinstance(behavior, Exception):
                raise behavior
            return {"token": self.token, "endpoint": endpoint}

        return _fn


def _make_pool(tokens, behaviors=None, *, clock=None, **kwargs):
    clock = clock or FakeClock()
    behaviors = behaviors or {}
    made: dict[str, FakePro] = {}

    def factory(token: str) -> FakePro:
        pro = FakePro(token, behaviors.get(token, behaviors))
        made[token] = pro
        return pro

    pool = TusharePool(tokens, pro_factory=factory, clock=clock.clock, sleep=clock.sleep, **kwargs)
    return pool, made, clock


# -- signal sniffing ----------------------------------------------------------


def test_rate_limit_signals() -> None:
    assert is_rate_limit_error(Exception("抱歉，您每分钟最多访问200次，频率超限"))
    assert is_rate_limit_error(Exception("429 Too Many Requests"))
    assert is_rate_limit_error(Exception("daily 调用次数超限"))
    assert not is_rate_limit_error(ValueError("boom"))
    assert not is_rate_limit_error(Exception("tushare daily returned empty"))


# -- rotation -----------------------------------------------------------------


def test_round_robin_across_keys() -> None:
    pool, made, _ = _make_pool(["A", "B"])
    got = [pool.call("daily", trade_date="20260904")["token"] for _ in range(5)]
    assert got == ["A", "B", "A", "B", "A"]
    # One pro client per key, lazily built and reused.
    assert set(made) == {"A", "B"}
    assert len(made["A"].calls) == 3


def test_rate_limit_rotates_without_sleep() -> None:
    pool, made, clock = _make_pool(
        ["A", "B"],
        {"A": {"fund_daily": Exception("频率超限")}},
    )
    out = pool.call("fund_daily", ts_code="518880.SH")
    assert out["token"] == "B"
    assert clock.sleeps == []
    assert pool.rotation_count == 1
    assert len(made["A"].calls) == 1


def test_all_keys_hot_sleeps_once_then_retries() -> None:
    attempts = {"A": 0, "B": 0}

    def flaky(token: str):
        def _run():
            attempts[token] += 1
            if attempts[token] == 1:
                raise Exception("频率超限")
            return {"token": token}

        return _run

    pool, _, clock = _make_pool(
        ["A", "B"], {"A": {"daily": flaky("A")}, "B": {"daily": flaky("B")}}
    )
    out = pool.call("daily", trade_date="20260904")
    assert out["token"] in ("A", "B")
    assert clock.sleeps == [ALL_KEYS_HOT_SLEEP_SECONDS]
    assert pool.rotation_count == 2


def test_persistent_limit_reraises_after_two_rounds() -> None:
    pool, _, clock = _make_pool(
        ["A", "B"],
        {"daily": Exception("频率超限")},
    )
    with pytest.raises(Exception, match="频率超限"):
        pool.call("daily", trade_date="20260904")
    # One back-off between the two rounds, then give up to the caller's retry
    # (no point sleeping after the final round fails).
    assert clock.sleeps == [ALL_KEYS_HOT_SLEEP_SECONDS]


def test_non_rate_error_reraises_immediately() -> None:
    pool, _, clock = _make_pool(["A", "B"], {"daily": ValueError("boom")})
    with pytest.raises(ValueError, match="boom"):
        pool.call("daily", trade_date="20260904")
    assert clock.sleeps == []
    assert pool.rotation_count == 0


# -- minute sliding window ----------------------------------------------------


def test_minute_window_waits_for_slot(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(tp, "DEFAULT_CALLS_PER_MINUTE", 3)
    pool, _, clock = _make_pool(["only"])
    for _ in range(3):
        pool.call("daily", trade_date="20260904")
    assert clock.sleeps == []
    pool.call("daily", trade_date="20260904")
    # Oldest hit was at t=1000 → one 60s wait frees a slot.
    assert clock.sleeps == [pytest.approx(60.0)]


def test_minute_window_prefers_key_with_capacity(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(tp, "DEFAULT_CALLS_PER_MINUTE", 2)
    pool, made, clock = _make_pool(["A", "B"])
    pool.call("daily")  # A
    pool.call("daily")  # B
    pool.call("daily")  # A (full now)
    pool.call("daily")  # B has capacity → no sleep
    assert clock.sleeps == []
    assert [c[0] for c in made["A"].calls] == ["daily"] * 2
    assert [c[0] for c in made["B"].calls] == ["daily"] * 2


def test_hk_daily_one_per_minute_spacing() -> None:
    pool, _, clock = _make_pool(["only"])
    pool.call("hk_daily", ts_code="0700.HK")
    pool.call("hk_daily", ts_code="0700.HK")
    assert clock.sleeps == [pytest.approx(60.0)]


# -- daily budget -------------------------------------------------------------


def test_daily_quota_exhaustion_and_degrade(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(tp.DAILY_LIMITED_ENDPOINTS, "probe_ep", 1)
    pool, _, clock = _make_pool(["A", "B"], today=lambda: date(2026, 9, 6))
    assert pool.call("probe_ep")["token"] == "A"
    # A spent → rotate to B, no sleep (a retry could never succeed on A today).
    assert pool.call("probe_ep")["token"] == "B"
    assert clock.sleeps == []
    with pytest.raises(DailyQuotaExhausted) as exc_info:
        pool.call("probe_ep")
    assert exc_info.value.endpoint == "probe_ep"
    # Other endpoints are unaffected by the probe_ep budget.
    assert pool.call("daily")["token"] in ("A", "B")


def test_daily_quota_resets_next_day(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(tp.DAILY_LIMITED_ENDPOINTS, "probe_ep", 1)
    day = [date(2026, 9, 6)]
    pool, _, _ = _make_pool(["only"], today=lambda: day[0])
    pool.call("probe_ep")
    with pytest.raises(DailyQuotaExhausted):
        pool.call("probe_ep")
    day[0] = date(2026, 9, 7)
    assert pool.call("probe_ep")["token"] == "only"


# -- facade / snapshot / singleton --------------------------------------------


def test_pooled_pro_facade_routes_endpoints() -> None:
    pool, made, _ = _make_pool(["A"])
    pro = pool.pro()
    out = pro.fund_daily(ts_code="518880.SH", start_date="20260904", end_date="20260904")
    assert out["token"] == "A"
    assert made["A"].calls[0][0] == "fund_daily"
    assert made["A"].calls[0][2]["ts_code"] == "518880.SH"


def test_snapshot_masks_tokens_and_never_raises() -> None:
    pool, _, _ = _make_pool(["secretAAA", "secretBBB"])
    pool.call("daily")
    snap = pool.snapshot()
    assert snap["configured"] is True
    assert snap["keyCount"] == 2
    assert [k["suffix"] for k in snap["keys"]] == ["tAAA", "tBBB"]
    assert all("secret" not in str(k) for k in snap["keys"])
    assert snap["keys"][0]["minuteUsed"] == 1
    assert "index_global" in snap["daily"]


def test_empty_tokens_rejected() -> None:
    with pytest.raises(ValueError):
        TusharePool([])


def test_get_pool_rebuilds_when_tokens_change(monkeypatch: pytest.MonkeyPatch) -> None:
    import data_sync_service.config as config_mod

    tp.reset_pool()
    try:
        monkeypatch.setattr(
            config_mod, "get_settings", lambda: SimpleNamespace(tushare_tokens=("A",))
        )
        assert tp.get_pool().tokens == ("A",)
        monkeypatch.setattr(
            config_mod, "get_settings", lambda: SimpleNamespace(tushare_tokens=("A", "B"))
        )
        assert tp.get_pool().tokens == ("A", "B")
        monkeypatch.setattr(config_mod, "get_settings", lambda: SimpleNamespace(tushare_tokens=()))
        with pytest.raises(RuntimeError, match="TU_SHARE_API_KEY is not set"):
            tp.get_pool()
    finally:
        tp.reset_pool()


# -- config parsing (OPT-124 backward compat) ----------------------------------


@pytest.fixture()
def _no_dotenv(monkeypatch: pytest.MonkeyPatch):
    """Neutralize repo-root .env loading so process env fully controls settings."""
    import data_sync_service.config as config_mod

    monkeypatch.setattr(config_mod, "load_dotenv", lambda *a, **k: None)
    config_mod.get_settings.cache_clear()
    yield
    config_mod.get_settings.cache_clear()


def test_config_multi_token_parsing(_no_dotenv, monkeypatch: pytest.MonkeyPatch) -> None:
    import data_sync_service.config as config_mod

    monkeypatch.setenv("TUSHARE_TOKEN", " keyA ,keyB,, keyC ")
    monkeypatch.setenv("TU_SHARE_API_KEY", "legacy")
    settings = config_mod.get_settings()
    assert settings.tushare_tokens == ("keyA", "keyB", "keyC")
    assert settings.tu_share_api_key == "legacy"


def test_config_single_key_backward_compat(_no_dotenv, monkeypatch: pytest.MonkeyPatch) -> None:
    import data_sync_service.config as config_mod

    monkeypatch.delenv("TUSHARE_TOKEN", raising=False)
    monkeypatch.setenv("TU_SHARE_API_KEY", "solo")
    settings = config_mod.get_settings()
    assert settings.tushare_tokens == ("solo",)


def test_config_no_token(_no_dotenv, monkeypatch: pytest.MonkeyPatch) -> None:
    import data_sync_service.config as config_mod

    monkeypatch.delenv("TUSHARE_TOKEN", raising=False)
    monkeypatch.delenv("TU_SHARE_API_KEY", raising=False)
    settings = config_mod.get_settings()
    assert settings.tushare_tokens == ()


# -- service wiring (no DB touched: guards fire before any query) --------------


def test_services_no_longer_import_tushare_directly() -> None:
    from data_sync_service.service import adj_factor, close_sync, etf_daily, hk_daily, macro_daily

    for mod in (adj_factor, close_sync, etf_daily, hk_daily, macro_daily):
        assert not hasattr(mod, "ts"), f"{mod.__name__} still imports tushare"


def test_missing_token_guard_keeps_legacy_message(monkeypatch: pytest.MonkeyPatch) -> None:
    from data_sync_service.service import hk_daily, macro_daily

    empty = SimpleNamespace(tushare_tokens=())
    monkeypatch.setattr(hk_daily, "get_settings", lambda: empty)
    out = hk_daily._tushare_sync_one("0700.HK")
    assert out == {"ok": False, "error": "TU_SHARE_API_KEY is not set"}

    import data_sync_service.config as config_mod

    tp.reset_pool()
    try:
        monkeypatch.setattr(config_mod, "get_settings", lambda: empty)
        assert macro_daily.try_tushare_pro() is None
        with pytest.raises(RuntimeError, match="TU_SHARE_API_KEY is not set"):
            macro_daily._tushare_pro()
    finally:
        tp.reset_pool()


def test_health_datasources_exposes_quota(monkeypatch: pytest.MonkeyPatch) -> None:
    from data_sync_service.api import health_routes as hr

    monkeypatch.setattr(hr, "datasource_freshness", lambda: [])
    body = hr.datasources_endpoint()
    assert body["ok"] is True
    assert isinstance(body.get("tushare_quota"), dict)
    assert "configured" in body["tushare_quota"]
