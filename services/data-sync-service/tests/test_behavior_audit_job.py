"""behavior_audit_job tests (OPT-112) — pure unit (no DB)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from data_sync_service.scheduler import behavior_audit_job as job


def _out(extra: list | None = None, missing: list | None = None) -> dict:
    return {
        "reconDate": "2026-08-14",
        "markets": {
            "CN": {
                "available": True,
                "expected": 0,
                "actual": 1,
                "extraList": extra or [],
                "missingList": missing or [],
            },
            "HK": {
                "available": True,
                "expected": 19,
                "actual": 0,
                "extraList": [],
                "missingList": missing or [],
            },
        },
    }


def test_run_persists_and_records_success() -> None:
    run_persist = MagicMock(return_value=_out())
    emit = MagicMock()
    insert = MagicMock()
    with (
        patch("data_sync_service.db.paper_trading.today_iso", return_value="2026-08-14"),
        patch(
            "data_sync_service.service.reconciliation.run_registry_and_persist",
            run_persist,
        ),
        patch("data_sync_service.scheduler.behavior_audit_job.emit_event", emit),
        patch("data_sync_service.scheduler.behavior_audit_job.insert_record", insert),
    ):
        job.run()

    run_persist.assert_called_once_with("2026-08-14")
    insert.assert_called_once()
    assert insert.call_args.args[0] == "behavior_audit"
    assert insert.call_args.kwargs["success"] is True
    emit.assert_not_called()


def test_run_emits_audit_issues_when_found() -> None:
    run_persist = MagicMock(return_value=_out(
        extra=[{"symbol": "CN:300628", "kind": "never_entered"}],
        missing=[{"symbol": "HK:02099"}],
    ))
    emit = MagicMock()
    with (
        patch("data_sync_service.db.paper_trading.today_iso", return_value="2026-08-14"),
        patch(
            "data_sync_service.service.reconciliation.run_registry_and_persist",
            run_persist,
        ),
        patch("data_sync_service.scheduler.behavior_audit_job.emit_event", emit),
        patch("data_sync_service.scheduler.behavior_audit_job.insert_record"),
        # STOCK pick → missing counts as an issue (non-STOCK pick suppresses it)
        patch(
            "data_sync_service.service.multi_asset_sleeve._pick",
            return_value={"key": "STOCK"},
        ),
    ):
        job.run()

    emit.assert_called_once()
    event_type = emit.call_args.args[0]
    payload = emit.call_args.args[1]
    dedupe = emit.call_args.kwargs["dedupe_key"]
    assert event_type == "audit_issues"
    assert dedupe == "audit_issues:2026-08-14"
    assert payload["markets"]["CN"]["extra"][0]["symbol"] == "CN:300628"
    assert payload["markets"]["HK"]["missing"] == ["HK:02099"]


def test_run_records_failure_without_event() -> None:
    emit = MagicMock()
    insert = MagicMock()
    with (
        patch("data_sync_service.db.paper_trading.today_iso", return_value="2026-08-14"),
        patch(
            "data_sync_service.service.reconciliation.run_registry_and_persist",
            side_effect=RuntimeError("simulate boom"),
        ),
        patch("data_sync_service.scheduler.behavior_audit_job.emit_event", emit),
        patch("data_sync_service.scheduler.behavior_audit_job.insert_record", insert),
    ):
        job.run()

    insert.assert_called_once()
    assert insert.call_args.kwargs["success"] is False
    assert "simulate boom" in insert.call_args.kwargs["error_message"]
    emit.assert_not_called()
