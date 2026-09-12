"""East Money egress probe + breaker exposure (OPT-126). No network, no DB."""

from __future__ import annotations

import time

import pytest

from data_sync_service.scheduler import em_probe_job as job
from data_sync_service.service import em_probe as probe
from data_sync_service.service import em_push2_http as emhttp


@pytest.fixture(autouse=True)
def _clean(monkeypatch):
    probe.reset_probe_state()
    monkeypatch.setattr(emhttp, "_EM_BLOCKED", False)
    monkeypatch.setattr(emhttp, "_EM_BLOCKED_AT", 0.0)
    monkeypatch.setattr(emhttp, "_EM_FAIL_STREAK", 0)
    monkeypatch.setattr(emhttp, "_PROXY_DEGRADED", False)
    try:
        yield
    finally:
        probe.reset_probe_state()


def _patch_fetch(monkeypatch, fail_hosts=()):
    def _fetch(url, *, params, referer, timeout, use_proxy=True):
        for target in probe.PROBE_TARGETS:
            if target["url"] == url and target["host"] in fail_hosts:
                raise RuntimeError("RemoteDisconnected")
        return {"ok": True}

    monkeypatch.setattr(emhttp, "_urllib_get_json", _fetch)


def test_probe_host_ok_and_fail(monkeypatch) -> None:
    _patch_fetch(monkeypatch)
    out = probe._probe_host(probe.PROBE_TARGETS[0])
    assert out["ok"] is True and out["error"] is None and out["ms"] >= 0
    _patch_fetch(monkeypatch, fail_hosts={"push2.eastmoney.com"})
    out = probe._probe_host(probe.PROBE_TARGETS[0])
    assert out["ok"] is False and "RemoteDisconnected" in (out["error"] or "")


def test_probe_targets_cover_all_hosts() -> None:
    assert {t["host"] for t in probe.PROBE_TARGETS} == {
        "push2.eastmoney.com",
        "data.eastmoney.com",
        "push2his.eastmoney.com",
    }
    for t in probe.PROBE_TARGETS:
        assert t["params"] and t["referer"]


def test_run_all_ok_no_alert(monkeypatch) -> None:
    _patch_fetch(monkeypatch)
    out = probe.run_em_probe()
    assert out["failed"] == [] and out["alerted"] == []


def test_streak_alerts_once_on_third(monkeypatch) -> None:
    _patch_fetch(monkeypatch, fail_hosts={"push2.eastmoney.com"})
    events: list[dict] = []
    emits: list[dict] = []
    monkeypatch.setattr(
        "data_sync_service.db.system_events.insert_event", lambda **k: events.append(k) or True
    )
    monkeypatch.setattr(
        "data_sync_service.db.webhook.emit_event", lambda *a, **k: emits.append(a) or True
    )
    r1 = probe.run_em_probe()
    r2 = probe.run_em_probe()
    assert r1["alerted"] == [] and r2["alerted"] == [] and events == []
    r3 = probe.run_em_probe()
    assert r3["alerted"] == ["push2.eastmoney.com"]
    assert len(events) == 1 and events[0]["severity"] == "high"
    assert events[0]["dedupe_key"] == "em_probe:push2.eastmoney.com"
    assert len(emits) == 1
    # Fourth failure: emit again (deduped downstream) but no new system event.
    r4 = probe.run_em_probe()
    assert r4["alerted"] == [] and len(events) == 1 and len(emits) == 2


def test_success_resets_streak(monkeypatch) -> None:
    _patch_fetch(monkeypatch, fail_hosts={"push2.eastmoney.com"})
    monkeypatch.setattr("data_sync_service.db.system_events.insert_event", lambda **k: True)
    monkeypatch.setattr("data_sync_service.db.webhook.emit_event", lambda *a, **k: True)
    probe.run_em_probe()
    probe.run_em_probe()
    _patch_fetch(monkeypatch)  # recovers
    out = probe.run_em_probe()
    assert out["streaks"].get("push2.eastmoney.com") == 0
    _patch_fetch(monkeypatch, fail_hosts={"push2.eastmoney.com"})
    assert probe.run_em_probe()["alerted"] == []  # streak restarted, not instant alert


def test_probe_observability_failure_survives(monkeypatch) -> None:
    _patch_fetch(monkeypatch, fail_hosts={"data.eastmoney.com"})

    def _boom(**k):
        raise RuntimeError("events table down")

    monkeypatch.setattr("data_sync_service.db.system_events.insert_event", _boom)
    monkeypatch.setattr("data_sync_service.db.webhook.emit_event", _boom)
    probe.run_em_probe()
    probe.run_em_probe()
    out = probe.run_em_probe()  # third: alert path throws internally, run survives
    assert out["failed"] == ["data.eastmoney.com"]


def test_probe_status_snapshot(monkeypatch) -> None:
    _patch_fetch(monkeypatch)
    monkeypatch.setattr("data_sync_service.db.system_events.insert_event", lambda **k: True)
    monkeypatch.setattr("data_sync_service.db.webhook.emit_event", lambda *a, **k: True)
    assert probe.probe_status() == {"at": None, "checks": [], "streaks": {}, "failing": []}
    probe.run_em_probe()
    st = probe.probe_status()
    assert st["at"] is not None and len(st["checks"]) == 3 and st["failing"] == []


# -- breaker ----------------------------------------------------------------------


def test_breaker_default_unlatched() -> None:
    assert emhttp.breaker_status() == {
        "ban_latched": False,
        "cooldown_remaining_s": 0,
        "fail_streak": 0,
        "proxy_degraded": False,
    }


def test_breaker_latched_and_cooldown(monkeypatch) -> None:
    monkeypatch.setattr(emhttp, "_EM_BLOCKED", True)
    monkeypatch.setattr(emhttp, "_EM_BLOCKED_AT", time.time())
    monkeypatch.setattr(emhttp, "_EM_FAIL_STREAK", 5)
    st = emhttp.breaker_status()
    assert st["ban_latched"] is True
    assert 0 < st["cooldown_remaining_s"] <= 15 * 60
    assert st["fail_streak"] == 5
    # Expired cooldown reads as unlatched.
    monkeypatch.setattr(emhttp, "_EM_BLOCKED_AT", time.time() - 3600)
    assert emhttp.breaker_status()["ban_latched"] is False


# -- job -----------------------------------------------------------------------------


def test_job_ok_failure_exception(monkeypatch) -> None:
    rows: list[tuple] = []
    monkeypatch.setattr(
        job,
        "insert_record",
        lambda jt, success, **k: rows.append((jt, success, k.get("error_message"))),
    )
    monkeypatch.setattr(job, "run_em_probe", lambda: {"failed": [], "checks": []})
    job.run()
    assert rows == [("em_probe", True, None)]

    monkeypatch.setattr(
        job, "run_em_probe", lambda: {"failed": ["push2.eastmoney.com"], "checks": []}
    )
    job.run()
    assert rows[-1][1] is False and "push2.eastmoney.com" in (rows[-1][2] or "")

    monkeypatch.setattr(job, "run_em_probe", lambda: (_ for _ in ()).throw(RuntimeError("boom")))
    job.run()
    assert rows[-1] == ("em_probe", False, "boom")


def test_job_trigger_interval() -> None:
    trig = job.build_trigger()
    assert job.JOB_ID == "em_probe" and job.INTERVAL_MINUTES == 10
    assert trig.interval.total_seconds() == 10 * 60


# -- health source ------------------------------------------------------------------------


def test_health_source_never_ran_is_stale() -> None:
    from data_sync_service.api import health_routes as hr

    out = hr._em_probe_source({"source": "eastmoney_probe", "label": "x", "thresholdMinutes": 20})
    assert out["stale"] is True and out["banLatched"] is False


def test_health_source_fresh_ok(monkeypatch) -> None:
    from data_sync_service.api import health_routes as hr

    monkeypatch.setattr(
        "data_sync_service.service.em_probe.probe_status",
        lambda: {
            "at": time.time(),
            "checks": [{"host": "h", "ok": True}],
            "streaks": {},
            "failing": [],
        },
    )
    out = hr._em_probe_source({"source": "eastmoney_probe", "label": "x", "thresholdMinutes": 20})
    assert out["stale"] is False and out["hosts"] is not None


def test_health_source_failing_and_ban_stale(monkeypatch) -> None:
    from data_sync_service.api import health_routes as hr

    monkeypatch.setattr(
        "data_sync_service.service.em_probe.probe_status",
        lambda: {
            "at": time.time(),
            "checks": [{"host": "h", "ok": False}],
            "streaks": {},
            "failing": ["h"],
        },
    )
    out = hr._em_probe_source({"source": "eastmoney_probe", "label": "x", "thresholdMinutes": 20})
    assert out["stale"] is True and out["failingHosts"] == ["h"]

    monkeypatch.setattr(
        "data_sync_service.service.em_probe.probe_status",
        lambda: {"at": time.time(), "checks": [], "streaks": {}, "failing": []},
    )
    monkeypatch.setattr(
        "data_sync_service.service.em_push2_http.breaker_status",
        lambda: {
            "ban_latched": True,
            "cooldown_remaining_s": 100,
            "fail_streak": 3,
            "proxy_degraded": False,
        },
    )
    out = hr._em_probe_source({"source": "eastmoney_probe", "label": "x", "thresholdMinutes": 20})
    assert out["stale"] is True and out["banLatched"] is True and out["cooldownRemainingS"] == 100
