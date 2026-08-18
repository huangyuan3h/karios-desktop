"""scheduler/weekly_review_job.py coverage."""

from __future__ import annotations

from data_sync_service.scheduler import weekly_review_job as wj


def test_prev_friday(monkeypatch) -> None:
    from datetime import UTC, datetime

    monkeypatch.setattr(
        "data_sync_service.scheduler.weekly_review_job.datetime",
        type("FakeDT", (), {"now": staticmethod(lambda tz=UTC: datetime(2026, 8, 17, 12, 0, tzinfo=tz))}),
    )
    assert wj._prev_friday() == "2026-08-14"


def test_run_generates_and_stores(monkeypatch) -> None:
    calls: dict[str, list] = {}

    def fake_build(end_date: str) -> dict:
        calls["build"] = [end_date]
        return {"markdown": "# 周报", "stats": []}

    def fake_upsert(**kw) -> dict:
        calls["upsert"] = kw
        return {"id": f"{kw['brief_date']}-{kw['brief_type']}"}

    monkeypatch.setattr("data_sync_service.service.weekly_review.build_weekly_review", fake_build)
    monkeypatch.setattr("data_sync_service.db.morning_brief.upsert_brief", fake_upsert)
    monkeypatch.setattr("data_sync_service.scheduler.weekly_review_job.insert_record",
                        lambda *a, **k: calls.setdefault("record", (a, k)))
    monkeypatch.setattr(wj, "_prev_friday", lambda: "2026-08-14")

    out = wj.run()
    assert out == {"endDate": "2026-08-14", "briefId": "2026-08-14-weekly-review", "markdownChars": 4}
    assert calls["build"] == ["2026-08-14"]
    assert calls["upsert"]["brief_type"] == "weekly-review"
    assert calls["record"][0] == (wj.JOB_ID, True, None)


def test_run_failure_records_false(monkeypatch) -> None:
    def fake_build(end_date: str) -> dict:
        raise RuntimeError("boom")

    monkeypatch.setattr("data_sync_service.service.weekly_review.build_weekly_review", fake_build)
    recorded: list = []

    def fake_record(*a, **k):
        recorded.append((a, k))

    monkeypatch.setattr("data_sync_service.scheduler.weekly_review_job.insert_record", fake_record)
    monkeypatch.setattr(wj, "_prev_friday", lambda: "2026-08-14")

    out = wj.run()
    assert out is None
    assert recorded[0][0] == (wj.JOB_ID, False, None)
    assert "boom" in recorded[0][1]["error_message"]
