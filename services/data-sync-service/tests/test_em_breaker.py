"""Path-scoped EM breaker (2026-11-06): list failures must not ban healthy routes."""

from __future__ import annotations

import time

import pytest

from data_sync_service.service import em_breaker
from data_sync_service.service import em_push2_http as eh


@pytest.fixture(autouse=True)
def _clean(monkeypatch):
    em_breaker.reset()
    monkeypatch.setattr(eh, "_EM_BLOCKED", False)
    monkeypatch.setattr(eh, "_EM_BLOCKED_AT", 0.0)
    monkeypatch.setattr(eh, "_EM_FAIL_STREAK", 0)
    monkeypatch.setattr(eh, "_PROXY", "")
    monkeypatch.setattr(eh, "_PROXY_DEGRADED", False)
    yield
    em_breaker.reset()


def test_scope_is_host_plus_path() -> None:
    assert (
        em_breaker.scope_for("https://push2.eastmoney.com/api/qt/clist/get?pn=1")
        == "push2.eastmoney.com/api/qt/clist/get"
    )
    assert em_breaker.scope_for("https://push2.eastmoney.com/api/qt/stock/get") != (
        em_breaker.scope_for("https://push2.eastmoney.com/api/qt/clist/get")
    )
    assert em_breaker.scope_for("https://reportapi.eastmoney.com/report/list") == (
        "reportapi.eastmoney.com/report/list"
    )


def test_list_failures_do_not_ban_stock_get() -> None:
    clist = em_breaker.scope_for("https://push2.eastmoney.com/api/qt/clist/get")
    quote = em_breaker.scope_for("https://push2.eastmoney.com/api/qt/stock/get")
    for _ in range(3):
        em_breaker.record_failure(clist)
    with pytest.raises(RuntimeError, match="eastmoney_ip_ban_latched"):
        em_breaker.check(clist)
    # A different endpoint on the same host is still allowed.
    em_breaker.check(quote)
    assert em_breaker.snapshot()["latched_scopes"] == [clist]


def test_success_resets_streak_and_expired_cooldown_unlatches() -> None:
    scope = "push2.eastmoney.com/api/qt/clist/get"
    em_breaker.record_failure(scope)
    em_breaker.record_failure(scope)
    em_breaker.record_success(scope)
    em_breaker.record_failure(scope)
    em_breaker.check(scope)  # not latched (streak restarted after success)

    em_breaker.record_failure(scope)
    em_breaker.record_failure(scope)
    with pytest.raises(RuntimeError):
        em_breaker.check(scope)
    # Cooldown already elapsed: check clears the latch and allows the retry.
    em_breaker.check(scope, cooldown_s=0)


def test_em_get_json_keeps_healthy_route_after_list_outage(monkeypatch) -> None:
    calls: list[str] = []

    def fake_direct(url, *, params, referer, timeout):
        calls.append(url)
        if "clist" in url:
            raise RuntimeError("Remote end closed connection without response")
        return {"data": {"f43": 10.0}}

    monkeypatch.setattr(eh, "_em_get_json_no_proxy", fake_direct)

    for _ in range(3):
        with pytest.raises(RuntimeError):
            eh.em_get_json(
                "https://push2.eastmoney.com/api/qt/clist/get",
                params={},
                referer="https://quote.eastmoney.com/",
            )

    # Third clist call latched that scope; the quote and report routes still work.
    with pytest.raises(RuntimeError, match="eastmoney_ip_ban_latched"):
        eh.em_get_json(
            "https://push2.eastmoney.com/api/qt/clist/get",
            params={},
            referer="https://quote.eastmoney.com/",
        )
    assert eh.em_get_json(
        "https://push2.eastmoney.com/api/qt/stock/get",
        params={},
        referer="https://quote.eastmoney.com/",
    ) == {"data": {"f43": 10.0}}
    assert eh.em_get_json(
        "https://reportapi.eastmoney.com/report/list",
        params={},
        referer="https://data.eastmoney.com/",
    ) == {"data": {"f43": 10.0}}
    assert calls[-2:] == [
        "https://push2.eastmoney.com/api/qt/stock/get",
        "https://reportapi.eastmoney.com/report/list",
    ]


def test_legacy_global_latch_still_honoured(monkeypatch) -> None:
    monkeypatch.setattr(eh, "_EM_BLOCKED", True)
    monkeypatch.setattr(eh, "_EM_BLOCKED_AT", time.time())
    with pytest.raises(RuntimeError, match="eastmoney_ip_ban_latched"):
        eh.em_get_json(
            "https://push2.eastmoney.com/api/qt/stock/get",
            params={},
            referer="https://quote.eastmoney.com/",
        )


def test_breaker_status_contract_unchanged_and_scoped_view_added() -> None:
    assert eh.breaker_status() == {
        "ban_latched": False,
        "cooldown_remaining_s": 0,
        "fail_streak": 0,
        "proxy_degraded": False,
    }
    scope = "push2his.eastmoney.com/api/qt/stock/fflow/daykline/get"
    for _ in range(3):
        em_breaker.record_failure(scope)
    scoped = eh.breaker_scope_status()
    assert scoped["ban_latched"] is True
    assert scoped["latched_scopes"] == [scope]
    assert scoped["scopes"][scope]["latched"] is True
