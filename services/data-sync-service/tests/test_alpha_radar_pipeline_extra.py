"""service/alpha_radar_pipeline.py coverage."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

from data_sync_service.service import alpha_radar_pipeline as ap


class TestConfig:
    def test_cooldown(self, monkeypatch) -> None:
        monkeypatch.setenv("ALPHA_RADAR_PIPELINE_COOLDOWN_HOURS", "7")
        assert ap.pipeline_cooldown_hours() == 7
        monkeypatch.setenv("ALPHA_RADAR_PIPELINE_COOLDOWN_HOURS", "0")
        assert ap.pipeline_cooldown_hours() == 1
        monkeypatch.setenv("ALPHA_RADAR_PIPELINE_COOLDOWN_HOURS", "abc")
        assert ap.pipeline_cooldown_hours() == ap.DEFAULT_COOLDOWN_HOURS
        monkeypatch.delenv("ALPHA_RADAR_PIPELINE_COOLDOWN_HOURS")
        assert ap.pipeline_cooldown_hours() == ap.DEFAULT_COOLDOWN_HOURS

    def test_max_batch_rounds(self, monkeypatch) -> None:
        monkeypatch.setenv("ALPHA_RADAR_DAILY_BATCH_ROUNDS", "5")
        assert ap.max_batch_rounds() == 5
        monkeypatch.setenv("ALPHA_RADAR_DAILY_BATCH_ROUNDS", "20")
        assert ap.max_batch_rounds() == 10
        monkeypatch.setenv("ALPHA_RADAR_DAILY_BATCH_ROUNDS", "abc")
        assert ap.max_batch_rounds() == 3

    def test_process_max_rounds(self, monkeypatch) -> None:
        monkeypatch.setenv("ALPHA_RADAR_PROCESS_MAX_ROUNDS", "7")
        assert ap.process_max_rounds() == 7
        monkeypatch.setenv("ALPHA_RADAR_PROCESS_MAX_ROUNDS", "30")
        assert ap.process_max_rounds() == 20
        monkeypatch.setenv("ALPHA_RADAR_PROCESS_MAX_ROUNDS", "abc")
        assert ap.process_max_rounds() == ap.DEFAULT_PROCESS_MAX_ROUNDS

    def test_process_batch_size(self, monkeypatch) -> None:
        monkeypatch.setenv("ALPHA_RADAR_PROCESS_BATCH_SIZE", "7")
        assert ap.process_batch_size() == 7
        monkeypatch.setenv("ALPHA_RADAR_PROCESS_BATCH_SIZE", "1")
        assert ap.process_batch_size() == 2
        monkeypatch.setenv("ALPHA_RADAR_PROCESS_BATCH_SIZE", "abc")
        assert ap.process_batch_size() == ap.DEFAULT_PROCESS_BATCH_SIZE

    def test_retention(self, monkeypatch) -> None:
        monkeypatch.setenv("ALPHA_RADAR_TREND_RETENTION_DAYS", "30")
        assert ap.trend_retention_days() == 30
        monkeypatch.setenv("ALPHA_RADAR_TREND_RETENTION_DAYS", "-5")
        assert ap.trend_retention_days() == 0
        monkeypatch.setenv("ALPHA_RADAR_TREND_RETENTION_DAYS", "abc")
        assert ap.trend_retention_days() == 0


class TestParse:
    def test_parse_iso(self) -> None:
        assert ap._parse_iso(None) is None
        assert ap._parse_iso("") is None
        dt = ap._parse_iso("2026-08-07T10:00:00Z")
        assert dt.tzinfo is not None
        dt2 = ap._parse_iso("2026-08-07T10:00:00")
        assert dt2.tzinfo is UTC
        assert ap._parse_iso("bad") is None

    def test_within_cooldown(self, monkeypatch) -> None:
        assert not ap._within_cooldown(None)
        now = datetime.now(UTC).isoformat()
        assert ap._within_cooldown(now)
        old = (datetime.now(UTC) - timedelta(hours=48)).isoformat()
        assert not ap._within_cooldown(old)

    def test_load_ingest_stats(self, monkeypatch) -> None:
        monkeypatch.setattr(ap, "get_meta", lambda k: None)
        assert ap._load_ingest_stats() is None
        monkeypatch.setattr(ap, "get_meta", lambda k: "{bad")
        assert ap._load_ingest_stats() is None
        monkeypatch.setattr(ap, "get_meta", lambda k: json.dumps({"stored": 3}))
        assert ap._load_ingest_stats() == {"stored": 3}

    def test_rounds(self, monkeypatch) -> None:
        monkeypatch.setattr(ap, "process_max_rounds", lambda: 8)
        monkeypatch.setattr(ap, "process_batch_size", lambda: 10)
        assert ap._rounds_for_raw_backlog(raw_count=0) == 1
        assert ap._rounds_for_raw_backlog(raw_count=5) == 1
        assert ap._rounds_for_raw_backlog(raw_count=25) == 3
        assert ap._rounds_for_raw_backlog(raw_count=500) == 8
        assert ap._rounds_for_raw_backlog(raw_count=25, max_rounds=2) == 2


class TestProcessLoops:
    def test_empty_raw(self, monkeypatch) -> None:
        monkeypatch.setattr(ap, "count_documents_by_status", lambda s: 0)
        total, trends, errors = ap._run_process_loops(max_rounds=3)
        assert total == 0 and trends == [] and errors == []

    def test_loop_done(self, monkeypatch) -> None:
        counts = iter([3, 3, 0])
        monkeypatch.setattr(ap, "count_documents_by_status", lambda s: next(counts))
        monkeypatch.setattr(ap, "process_pending_documents", lambda **kw: {"processed": 2, "trends": [{"id": 1}], "errors": [{"error": "e1"}, "e2"]})
        total, trends, errors = ap._run_process_loops(max_rounds=3)
        assert total == 4 and len(trends) == 2
        assert errors == [{"error": "e1"}, {"error": "e2"}, {"error": "e1"}, {"error": "e2"}]

    def test_loop_processed_less_than_two(self, monkeypatch) -> None:
        monkeypatch.setattr(ap, "count_documents_by_status", lambda s: 3)
        monkeypatch.setattr(ap, "process_pending_documents", lambda **kw: {"processed": 1, "trends": [], "errors": []})
        total, trends, errors = ap._run_process_loops(max_rounds=3)
        assert total == 1

    def test_loop_exception(self, monkeypatch) -> None:
        monkeypatch.setattr(ap, "count_documents_by_status", lambda s: 3)
        monkeypatch.setattr(ap, "process_pending_documents", lambda **kw: (_ for _ in ()).throw(RuntimeError("boom")))
        total, trends, errors = ap._run_process_loops(max_rounds=3)
        assert errors == [{"error": "boom"}]


class TestStatus:
    def test_pipeline_status(self, monkeypatch) -> None:
        meta = {
            ap.META_LAST_RUN_AT: "2026-08-07T10:00:00+00:00",
            ap.META_LAST_BATCH_STARTED_AT: "2026-08-07T09:00:00+00:00",
            ap.META_LAST_TREND_COUNT: "5",
            ap.META_LAST_INGEST_AT: "x",
            ap.META_LAST_PROCESS_AT: "y",
        }
        monkeypatch.setattr(ap, "get_meta", lambda k: meta.get(k))
        monkeypatch.setattr(ap, "fetch_trends", lambda **kw: (3, []))
        monkeypatch.setattr(ap, "count_trends_total", lambda: 10)
        monkeypatch.setattr(ap, "count_documents_by_status", lambda s: 2)
        monkeypatch.setattr(ap, "trend_retention_days", lambda: 0)
        monkeypatch.setattr(ap, "_load_ingest_stats", lambda: {"stored": 1})
        monkeypatch.setattr(ap, "_within_cooldown", lambda d: True)
        monkeypatch.setattr(ap, "pipeline_cooldown_hours", lambda: 12)
        monkeypatch.setattr(ap, "get_today_run", lambda j: {"success": True})
        monkeypatch.setattr(ap, "get_last_success", lambda j: "t")
        out = ap.pipeline_status()
        assert out["lastTrendCount"] == 5
        assert out["currentTrendCount"] == 3
        assert out["lastIngestStats"] == {"stored": 1}

    def test_pipeline_status_bad_count(self, monkeypatch) -> None:
        monkeypatch.setattr(ap, "get_meta", lambda k: "not-a-number" if k == ap.META_LAST_TREND_COUNT else None)
        monkeypatch.setattr(ap, "fetch_trends", lambda **kw: (0, []))
        monkeypatch.setattr(ap, "count_trends_total", lambda: 0)
        monkeypatch.setattr(ap, "count_documents_by_status", lambda s: 0)
        monkeypatch.setattr(ap, "trend_retention_days", lambda: 0)
        monkeypatch.setattr(ap, "get_today_run", lambda j: None)
        monkeypatch.setattr(ap, "get_last_success", lambda j: None)
        out = ap.pipeline_status()
        assert out["lastTrendCount"] == 0


class TestIngest:
    def test_ingest_success(self, monkeypatch) -> None:
        monkeypatch.setattr(ap, "add_default_sources", lambda: None)
        monkeypatch.setattr(ap, "fetch_all_sources", lambda **kw: {"ingestStats": {"stored": 3}, "sourceErrors": {}})
        monkeypatch.setattr(ap, "set_meta", lambda k, v: None)
        seen = {}
        monkeypatch.setattr(ap, "insert_record", lambda job, **kw: seen.update(job=job, **kw))
        monkeypatch.setattr(ap, "count_documents_by_status", lambda s: 3)
        out = ap.run_alpha_radar_ingest(trigger="cron")
        assert out["ok"] is True
        assert seen["success"] is True and seen["last_ts_code"] == "3"

    def test_ingest_failure(self, monkeypatch) -> None:
        monkeypatch.setattr(ap, "add_default_sources", lambda: None)
        monkeypatch.setattr(ap, "fetch_all_sources", lambda **kw: {"ingestStats": {"stored": 0}, "sourceErrors": {"cnrss": "err"}})
        monkeypatch.setattr(ap, "set_meta", lambda k, v: None)
        seen = {}
        monkeypatch.setattr(ap, "insert_record", lambda job, **kw: seen.update(job=job, **kw))
        monkeypatch.setattr(ap, "count_documents_by_status", lambda s: 0)
        out = ap.run_alpha_radar_ingest()
        assert out["ok"] is False
        assert "cnrss" in out["errors"][0]["error"]


class TestProcess:
    def test_process_ok(self, monkeypatch) -> None:
        monkeypatch.setattr(ap, "count_documents_by_status", lambda s: 3)
        monkeypatch.setattr(ap, "_run_process_loops", lambda **kw: (2, [{"id": 1}], []))
        monkeypatch.setattr(ap, "set_meta", lambda k, v: None)
        seen = {}
        monkeypatch.setattr(ap, "insert_record", lambda job, **kw: seen.update(job=job, **kw))
        monkeypatch.setattr(ap, "count_trends_total", lambda: 5)
        out = ap.run_alpha_radar_process()
        assert out["ok"] is True and out["processedHeadlines"] == 2
        assert seen["success"] is True

    def test_process_errors(self, monkeypatch) -> None:
        monkeypatch.setattr(ap, "count_documents_by_status", lambda s: 3)
        monkeypatch.setattr(ap, "_run_process_loops", lambda **kw: (0, [], [{"error": "boom"}]))
        monkeypatch.setattr(ap, "set_meta", lambda k, v: None)
        seen = {}
        monkeypatch.setattr(ap, "insert_record", lambda job, **kw: seen.update(job=job, **kw))
        monkeypatch.setattr(ap, "count_trends_total", lambda: 5)
        out = ap.run_alpha_radar_process()
        assert out["ok"] is False and out["errors"][0]["error"] == "boom"
        assert seen["success"] is False and seen["error_message"] == "boom"


class TestPipeline:
    def test_cooldown_skip(self, monkeypatch) -> None:
        monkeypatch.setattr(ap, "get_meta", lambda k: "2026-08-07T10:00:00+00:00")
        monkeypatch.setattr(ap, "_within_cooldown", lambda d: True)
        monkeypatch.setattr(ap, "fetch_trends", lambda **kw: (0, [{"id": 1}]))
        monkeypatch.setattr(ap, "_load_ingest_stats", lambda: None)
        monkeypatch.setattr(ap, "count_trends_total", lambda: 5)
        monkeypatch.setattr(ap, "count_documents_by_status", lambda s: 2)
        out = ap.run_alpha_radar_pipeline(force=False)
        assert out["skipped"] is True and out["trendCount"] == 1

    def test_pipeline_force(self, monkeypatch) -> None:
        monkeypatch.setattr(ap, "get_meta", lambda k: "2026-08-07T10:00:00+00:00")
        monkeypatch.setattr(ap, "_within_cooldown", lambda d: False)
        monkeypatch.setattr(ap, "run_alpha_radar_ingest", lambda **kw: {"ok": True, "ingest": {"sourceErrors": {}}, "ingestStats": {"stored": 3}})
        monkeypatch.setattr(ap, "_run_process_loops", lambda **kw: (2, [{"id": 1}], []))
        monkeypatch.setattr(ap, "fetch_trends", lambda **kw: (0, [{"id": 1}]))
        monkeypatch.setattr(ap, "set_meta", lambda k, v: None)
        monkeypatch.setattr(ap, "trend_retention_days", lambda: 0)
        monkeypatch.setattr(ap, "insert_record", lambda job, **kw: None)
        monkeypatch.setattr(ap, "count_trends_total", lambda: 5)
        monkeypatch.setattr(ap, "count_documents_by_status", lambda s: 0)
        out = ap.run_alpha_radar_pipeline(force=True)
        assert out["ok"] is True and out["trendCount"] == 1
        assert out["processedHeadlines"] == 2

    def test_pipeline_retention(self, monkeypatch) -> None:
        monkeypatch.setattr(ap, "get_meta", lambda k: None)
        monkeypatch.setattr(ap, "run_alpha_radar_ingest", lambda **kw: {"ok": True, "ingest": {"sourceErrors": {}}, "ingestStats": {"stored": 3}})
        monkeypatch.setattr(ap, "_run_process_loops", lambda **kw: (2, [], []))
        monkeypatch.setattr(ap, "fetch_trends", lambda **kw: (0, [{"id": 1}]))
        monkeypatch.setattr(ap, "set_meta", lambda k, v: None)
        monkeypatch.setattr(ap, "trend_retention_days", lambda: 30)
        monkeypatch.setattr(ap, "delete_trends_older_than_days", lambda d: 7)
        monkeypatch.setattr(ap, "insert_record", lambda job, **kw: None)
        monkeypatch.setattr(ap, "count_trends_total", lambda: 5)
        monkeypatch.setattr(ap, "count_documents_by_status", lambda s: 0)
        out = ap.run_alpha_radar_pipeline(force=True)
        assert out["prunedOldTrends"] == 7

    def test_pipeline_no_stored(self, monkeypatch) -> None:
        monkeypatch.setattr(ap, "get_meta", lambda k: None)
        monkeypatch.setattr(ap, "run_alpha_radar_ingest", lambda **kw: {"ok": False, "ingest": {"sourceErrors": {"rss": "err"}}, "ingestStats": {"stored": 0}})
        monkeypatch.setattr(ap, "fetch_trends", lambda **kw: (0, [{"id": 1}]))
        monkeypatch.setattr(ap, "insert_record", lambda job, **kw: None)
        monkeypatch.setattr(ap, "count_trends_total", lambda: 5)
        monkeypatch.setattr(ap, "count_documents_by_status", lambda s: 0)
        out = ap.run_alpha_radar_pipeline(force=True)
        assert out["ok"] is False and out["keptPreviousTrends"] is True
        assert "No documents stored" in out["errors"][0]["error"]

    def test_pipeline_zero_trends_with_errors(self, monkeypatch) -> None:
        monkeypatch.setattr(ap, "get_meta", lambda k: None)
        monkeypatch.setattr(ap, "run_alpha_radar_ingest", lambda **kw: {"ok": True, "ingest": {"sourceErrors": {}}, "ingestStats": {"stored": 3}})
        monkeypatch.setattr(ap, "_run_process_loops", lambda **kw: (0, [], [{"error": "ai-service LLM returned 0 trends"}]))
        monkeypatch.setattr(ap, "fetch_trends", lambda **kw: (0, []))
        monkeypatch.setattr(ap, "delete_trends_since", lambda d: None)
        monkeypatch.setattr(ap, "set_meta", lambda k, v: None)
        monkeypatch.setattr(ap, "insert_record", lambda job, **kw: None)
        monkeypatch.setattr(ap, "count_trends_total", lambda: 5)
        monkeypatch.setattr(ap, "count_documents_by_status", lambda s: 0)
        out = ap.run_alpha_radar_pipeline(force=True)
        assert out["ok"] is False and out["keptPreviousTrends"] is True
        assert "LLM returned 0 trends" in out["errors"][0]["error"]

    def test_pipeline_zero_trends_no_errors(self, monkeypatch) -> None:
        monkeypatch.setattr(ap, "get_meta", lambda k: None)
        monkeypatch.setattr(ap, "run_alpha_radar_ingest", lambda **kw: {"ok": True, "ingest": {"sourceErrors": {}}, "ingestStats": {"stored": 3}})
        monkeypatch.setattr(ap, "_run_process_loops", lambda **kw: (0, [], []))
        monkeypatch.setattr(ap, "fetch_trends", lambda **kw: (0, []))
        monkeypatch.setattr(ap, "delete_trends_since", lambda d: None)
        monkeypatch.setattr(ap, "set_meta", lambda k, v: None)
        monkeypatch.setattr(ap, "insert_record", lambda job, **kw: None)
        monkeypatch.setattr(ap, "count_trends_total", lambda: 5)
        monkeypatch.setattr(ap, "count_documents_by_status", lambda s: 0)
        out = ap.run_alpha_radar_pipeline(force=True)
        assert out["ok"] is False
        assert out["errors"][0]["error"] == "LLM produced 0 trends; kept previous cards"

    def test_daily_generation_and_status(self, monkeypatch) -> None:
        monkeypatch.setattr(ap, "run_alpha_radar_pipeline", lambda **kw: {"ok": True})
        assert ap.run_daily_generation(force=True) == {"ok": True}
        monkeypatch.setattr(ap, "pipeline_status", lambda: {"lastRunAt": "x"})
        assert ap.daily_status()["generatedToday"] is True
