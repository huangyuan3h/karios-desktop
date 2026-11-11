"""Factor / S-gap 必达 monitor tests (2026-10-25 MVP)."""

from __future__ import annotations

import json

from data_sync_service.api.webhook_routes import KNOWN_EVENT_TYPES
from data_sync_service.db.system_events import HIGH_JOB_TYPES
from data_sync_service.scheduler import factor_vault_job, sgap_decay_job
from data_sync_service.service import factor_vault as fv
from data_sync_service.service.notifications import TRADING_JOB_TYPES


def test_diff_factor_transitions_upgrade_and_revival() -> None:
    prev = {"a": "cold", "b": "watch", "c": "cold"}
    factors = [
        {"id": "a", "name": "A", "status": "watch", "percentile": 60.0, "net": 1.5},
        {"id": "b", "name": "B", "status": "revived", "percentile": 80.0, "net": 2.0},
        {"id": "c", "name": "C", "status": "cold", "percentile": 10.0, "net": -5.0},
    ]
    out = fv.diff_factor_transitions(prev, factors, "2026-10-26")
    assert [(t["factor_id"], t["event_type"]) for t in out] == [
        ("a", "factor_status_up"),
        ("b", "factor_revived"),
    ]
    assert out[0]["dedupe_key"] == "factor_status_up:a:watch:2026-10-26"
    assert out[1]["dedupe_key"] == "factor_revived:b:2026-10-26"


def test_diff_factor_transitions_skips_fades_and_unknown() -> None:
    prev = {"a": "watch", "b": "revived"}
    factors = [
        {"id": "a", "name": "A", "status": "cold", "percentile": 10.0, "net": -1.0},
        {"id": "b", "name": "B", "status": "watch", "percentile": 60.0, "net": 1.0},
        {"id": "zzz", "name": "Z", "status": "revived", "percentile": 90.0, "net": 1.0},
    ]
    assert fv.diff_factor_transitions(prev, factors, "2026-10-26") == []
    assert fv.diff_factor_transitions(None, factors, "2026-10-26") == []
    assert fv.diff_factor_transitions({}, factors, "2026-10-26") == []


def test_event_types_and_job_severity_wiring() -> None:
    assert {"factor_status_up", "factor_revived", "sgap_revival"} <= KNOWN_EVENT_TYPES
    assert {"factor_vault_refresh", "sgap_decay_refresh"} <= HIGH_JOB_TYPES
    # Research monitor jobs page Bark but stay out of the Hub trade/system cron list.
    assert "factor_vault_refresh" not in TRADING_JOB_TYPES
    assert "sgap_decay_refresh" not in TRADING_JOB_TYPES


class _Proc:
    def __init__(self, returncode=0, stdout="", stderr="") -> None:
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


def test_factor_vault_job_emits_on_upgrade(monkeypatch, tmp_path) -> None:
    (tmp_path / "factor_vault_history.json").write_text(
        json.dumps({"history": [{"date": "2026-10-23", "states": {"a": "cold"}}]}),
        encoding="utf-8",
    )
    fresh = {
        "factors": [{"id": "a", "name": "A", "status": "watch", "percentile": 60.0, "net": 1.0}]
    }

    def _fake_run(*args, **kwargs):
        (tmp_path / "factor_vault.json").write_text(json.dumps(fresh), encoding="utf-8")
        return _Proc(stdout="wrote vault")

    monkeypatch.setattr(factor_vault_job, "_reports_dir", lambda: tmp_path)
    monkeypatch.setattr("subprocess.run", _fake_run)
    emitted: list[tuple] = []
    monkeypatch.setattr(
        "data_sync_service.db.webhook.emit_event",
        lambda *a, **k: emitted.append((a, k)) or True,
    )
    records: list[tuple] = []
    monkeypatch.setattr(
        "data_sync_service.scheduler._job_guard.insert_record",
        lambda jt, success, **k: records.append((jt, success)),
    )
    factor_vault_job.run()
    assert records == [("factor_vault_refresh", True)]
    assert len(emitted) == 1
    (etype, payload), kw = emitted[0]
    assert etype == "factor_status_up"
    assert kw["dedupe_key"].startswith("factor_status_up:a:watch:")
    trans = json.loads((tmp_path / "factor_vault_transitions.json").read_text())
    assert len(trans["transitions"]) == 1


def test_factor_vault_job_records_failure(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(factor_vault_job, "_reports_dir", lambda: tmp_path)
    monkeypatch.setattr("subprocess.run", lambda *a, **k: _Proc(1, stderr="boom"))
    emitted: list = []
    monkeypatch.setattr(
        "data_sync_service.db.webhook.emit_event", lambda *a, **k: emitted.append(a) or True
    )
    records: list[tuple] = []
    monkeypatch.setattr(
        "data_sync_service.scheduler._job_guard.insert_record",
        lambda jt, success, **k: records.append((jt, success)),
    )
    factor_vault_job.run()  # must not raise; yesterday's files untouched
    assert records and records[0][1] is False
    assert emitted == []
    assert not (tmp_path / "factor_vault_transitions.json").exists()


def test_sgap_decay_job_emits_on_step_up(monkeypatch, tmp_path) -> None:
    (tmp_path / "sgap_decay.json").write_text(
        json.dumps({"summary": {"revival_pos": 10}}), encoding="utf-8"
    )

    def _fake_run(*args, **kwargs):
        (tmp_path / "sgap_decay.json").write_text(
            json.dumps({"summary": {"revival_pos": 20}}), encoding="utf-8"
        )
        return _Proc(stdout="wrote sgap")

    monkeypatch.setattr(sgap_decay_job, "_reports_dir", lambda: tmp_path)
    monkeypatch.setattr("subprocess.run", _fake_run)
    emitted: list[tuple] = []
    monkeypatch.setattr(
        "data_sync_service.db.webhook.emit_event",
        lambda *a, **k: emitted.append((a, k)) or True,
    )
    records: list[tuple] = []
    monkeypatch.setattr(
        "data_sync_service.scheduler._job_guard.insert_record",
        lambda jt, success, **k: records.append((jt, success)),
    )
    sgap_decay_job.run()
    assert records == [("sgap_decay_refresh", True)]
    assert len(emitted) == 1
    (etype, payload), kw = emitted[0]
    assert etype == "sgap_revival"
    assert payload == {"old_pos": 10, "new_pos": 20, "date": payload["date"]}
    assert kw["dedupe_key"].startswith("sgap_revival:20:")
    trans = json.loads((tmp_path / "factor_vault_transitions.json").read_text())
    assert trans["transitions"][0]["kind"] == "sgap"


def test_sgap_decay_job_no_emit_without_step(monkeypatch, tmp_path) -> None:
    (tmp_path / "sgap_decay.json").write_text(
        json.dumps({"summary": {"revival_pos": 0}}), encoding="utf-8"
    )
    monkeypatch.setattr(sgap_decay_job, "_reports_dir", lambda: tmp_path)
    monkeypatch.setattr("subprocess.run", lambda *a, **k: _Proc(stdout="same"))
    emitted: list = []
    monkeypatch.setattr(
        "data_sync_service.db.webhook.emit_event", lambda *a, **k: emitted.append(a) or True
    )
    records: list[tuple] = []
    monkeypatch.setattr(
        "data_sync_service.scheduler._job_guard.insert_record",
        lambda jt, success, **k: records.append((jt, success)),
    )
    sgap_decay_job.run()
    assert records == [("sgap_decay_refresh", True)]
    assert emitted == []


def test_sgap_decay_job_missing_input_records_failure(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(sgap_decay_job, "_reports_dir", lambda: tmp_path)
    monkeypatch.setattr("subprocess.run", lambda *a, **k: _Proc(1, stderr="missing input"))
    records: list[tuple] = []
    monkeypatch.setattr(
        "data_sync_service.scheduler._job_guard.insert_record",
        lambda jt, success, **k: records.append((jt, success)),
    )
    sgap_decay_job.run()  # must not raise
    assert records and records[0][1] is False


def test_scheduler_registers_both_jobs() -> None:
    from data_sync_service.scheduler import create_scheduler

    sched = create_scheduler()
    ids = {j.id for j in sched.get_jobs()}
    assert "factor_vault_refresh" in ids
    assert "sgap_decay_refresh" in ids
    by_id = {j.id: j for j in sched.get_jobs()}
    assert "19" in str(by_id["factor_vault_refresh"].trigger)
    assert "19" in str(by_id["sgap_decay_refresh"].trigger)


def test_sync_job_types_cover_monitor_jobs() -> None:
    from data_sync_service.api.sync_routes import SYNC_JOB_TYPES

    assert "factor_vault_refresh" in SYNC_JOB_TYPES
    assert "sgap_decay_refresh" in SYNC_JOB_TYPES
