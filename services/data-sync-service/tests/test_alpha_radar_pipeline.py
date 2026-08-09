"""alpha_radar_pipeline drivers: cooldown, backlog rounds, ingest/process/pipeline."""

from __future__ import annotations

import json

from data_sync_service.service import alpha_radar_pipeline as ap


def test_config_getters_with_env(monkeypatch) -> None:
    monkeypatch.setenv("ALPHA_RADAR_PIPELINE_COOLDOWN_HOURS", "6")
    assert ap.pipeline_cooldown_hours() == 6
    monkeypatch.setenv("ALPHA_RADAR_PIPELINE_COOLDOWN_HOURS", "bad")
    assert ap.pipeline_cooldown_hours() == ap.DEFAULT_COOLDOWN_HOURS
    monkeypatch.delenv("ALPHA_RADAR_PIPELINE_COOLDOWN_HOURS")

    monkeypatch.setenv("ALPHA_RADAR_DAILY_BATCH_ROUNDS", "7")
    assert ap.max_batch_rounds() == 7
    monkeypatch.setenv("ALPHA_RADAR_DAILY_BATCH_ROUNDS", "99")
    assert ap.max_batch_rounds() == 10
    monkeypatch.setenv("ALPHA_RADAR_DAILY_BATCH_ROUNDS", "x")
    assert ap.max_batch_rounds() == 3

    monkeypatch.setenv("ALPHA_RADAR_PROCESS_BATCH_SIZE", "9")
    assert ap.process_batch_size() == 9
    monkeypatch.setenv("ALPHA_RADAR_PROCESS_MAX_ROUNDS", "bad")
    assert ap.process_max_rounds() == ap.DEFAULT_PROCESS_MAX_ROUNDS

    monkeypatch.setenv("ALPHA_RADAR_TREND_RETENTION_DAYS", "14")
    assert ap.trend_retention_days() == 14
    monkeypatch.setenv("ALPHA_RADAR_TREND_RETENTION_DAYS", "bad")
    assert ap.trend_retention_days() == 0


def test_parse_iso_variants() -> None:
    from datetime import UTC

    assert ap._parse_iso(None) is None
    assert ap._parse_iso("") is None
    assert ap._parse_iso("not-a-date") is None
    d = ap._parse_iso("2026-08-07T09:00:00Z")
    assert d is not None and d.tzinfo == UTC
    d2 = ap._parse_iso("2026-08-07T09:00:00")
    assert d2 is not None and d2.tzinfo == UTC


def test_within_cooldown(monkeypatch) -> None:
    from datetime import UTC, datetime, timedelta

    now = datetime.now(UTC)
    monkeypatch.setenv("ALPHA_RADAR_PIPELINE_COOLDOWN_HOURS", "6")
    assert ap._within_cooldown((now - timedelta(hours=1)).isoformat()) is True
    assert ap._within_cooldown((now - timedelta(hours=7)).isoformat()) is False
    assert ap._within_cooldown(None) is False
    assert ap._within_cooldown("garbage") is False


def test_load_ingest_stats(monkeypatch) -> None:
    monkeypatch.setattr(ap, "get_meta", lambda k: None)
    assert ap._load_ingest_stats() is None
    monkeypatch.setattr(ap, "get_meta", lambda k: "{bad json")
    assert ap._load_ingest_stats() is None
    monkeypatch.setattr(ap, "get_meta", lambda k: json.dumps({"stored": 3}))
    assert ap._load_ingest_stats() == {"stored": 3}


def test_rounds_for_raw_backlog(monkeypatch) -> None:
    monkeypatch.setattr(ap, "process_batch_size", lambda: 4)
    monkeypatch.setattr(ap, "process_max_rounds", lambda: 10)
    assert ap._rounds_for_raw_backlog(raw_count=0) == 1
    assert ap._rounds_for_raw_backlog(raw_count=9) == 3  # ceil(9/4)
    assert ap._rounds_for_raw_backlog(raw_count=100, max_rounds=2) == 2


def test_run_process_loops_breaks_when_done(monkeypatch) -> None:
    calls = {"raw": 1, "process": 0}
    monkeypatch.setattr(ap, "count_documents_by_status", lambda s: calls["raw"])
    monkeypatch.setattr(ap, "process_batch_size", lambda: 5)
    monkeypatch.setattr(
        ap,
        "process_pending_documents",
        lambda limit, map_cn, mode: (
            calls.__setitem__("raw", 0) or {"processed": 3, "trends": [{"id": "t1"}], "errors": []}
        ),
    )
    processed, trends, errors = ap._run_process_loops(max_rounds=5)
    assert processed == 3
    assert len(trends) == 1
    assert errors == []


def test_run_process_loops_stops_on_processed_lt_2(monkeypatch) -> None:
    monkeypatch.setattr(ap, "count_documents_by_status", lambda s: 1)
    monkeypatch.setattr(ap, "process_batch_size", lambda: 5)
    calls = {"n": 0}

    def fake_process(**kw):
        calls["n"] += 1
        return {"processed": 1, "trends": [], "errors": []}

    monkeypatch.setattr(ap, "process_pending_documents", fake_process)
    processed, _, _ = ap._run_process_loops(max_rounds=5)
    assert calls["n"] == 1


def test_run_alpha_radar_ingest_success(monkeypatch) -> None:
    monkeypatch.setattr(ap, "add_default_sources", lambda: None)
    monkeypatch.setattr(
        ap,
        "fetch_all_sources",
        lambda **kw: {"ingestStats": {"stored": 5}, "sourceErrors": {}},
    )
    monkeypatch.setattr(ap, "set_meta", lambda *a, **k: None)
    monkeypatch.setattr(ap, "insert_record", lambda *a, **k: None)
    monkeypatch.setattr(ap, "count_documents_by_status", lambda s: 2)

    out = ap.run_alpha_radar_ingest(trigger="manual")
    assert out["ok"] is True
    assert out["ingestStats"]["stored"] == 5
    assert out["rawBacklogCount"] == 2


def test_run_alpha_radar_ingest_source_errors(monkeypatch) -> None:
    monkeypatch.setattr(ap, "add_default_sources", lambda: None)
    monkeypatch.setattr(
        ap,
        "fetch_all_sources",
        lambda **kw: {"ingestStats": {"stored": 0}, "sourceErrors": {"rss1": "timeout"}},
    )
    monkeypatch.setattr(ap, "set_meta", lambda *a, **k: None)
    monkeypatch.setattr(ap, "insert_record", lambda *a, **k: None)
    monkeypatch.setattr(ap, "count_documents_by_status", lambda s: 0)

    out = ap.run_alpha_radar_ingest(trigger="manual")
    assert out["ok"] is False
    assert "rss1" in out["errors"][0]["error"]


def test_run_alpha_radar_process_drives_loops(monkeypatch) -> None:
    monkeypatch.setattr(ap, "count_documents_by_status", lambda s: 5)
    monkeypatch.setattr(ap, "_rounds_for_raw_backlog", lambda **kw: 2)
    monkeypatch.setattr(
        ap,
        "_run_process_loops",
        lambda max_rounds: (4, [{"id": "t1"}], []),
    )
    monkeypatch.setattr(ap, "set_meta", lambda *a, **k: None)
    monkeypatch.setattr(ap, "insert_record", lambda *a, **k: None)
    monkeypatch.setattr(ap, "count_trends_total", lambda: 3)

    out = ap.run_alpha_radar_process(trigger="manual")
    assert out["ok"] is True
    assert out["processedHeadlines"] == 4
    assert out["trendsProduced"] == 1


def test_run_alpha_radar_pipeline_cooldown_skips(monkeypatch) -> None:
    from datetime import UTC, datetime, timedelta

    monkeypatch.setattr(ap, "get_meta", lambda k: (datetime.now(UTC) - timedelta(minutes=5)).isoformat() if k == "lastRunAt" else None)
    monkeypatch.setattr(ap, "_within_cooldown", lambda last: True)
    monkeypatch.setattr(ap, "fetch_trends", lambda limit=50, since=None: ([{"id": "t1"}], [{"id": "t1"}]))
    monkeypatch.setattr(ap, "count_trends_total", lambda: 1)
    monkeypatch.setattr(ap, "count_documents_by_status", lambda s: 0)

    out = ap.run_alpha_radar_pipeline(trigger="scheduled")
    assert out["skipped"] is True
    assert out["trendCount"] == 1


def test_run_alpha_radar_pipeline_no_stored_docs(monkeypatch) -> None:
    monkeypatch.setattr(ap, "get_meta", lambda k: None)
    monkeypatch.setattr(ap, "_within_cooldown", lambda last: False)
    monkeypatch.setattr(
        ap,
        "run_alpha_radar_ingest",
        lambda trigger="manual": {"ok": True, "ingest": {"sourceErrors": {}}, "ingestStats": {"stored": 0}},
    )
    monkeypatch.setattr(ap, "insert_record", lambda *a, **k: None)
    monkeypatch.setattr(ap, "fetch_trends", lambda limit=50: ([{"id": "t1"}], [{"id": "t1"}]))

    out = ap.run_alpha_radar_pipeline(trigger="scheduled")
    assert out["ok"] is False
    assert out["processedHeadlines"] == 0
