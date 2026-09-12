"""OPT-151: sleeve_paper_job recon wiring — mismatch lands in sync_job_record
(and a job_failed event), clean run records success."""

from __future__ import annotations

from unittest.mock import patch

from data_sync_service.api.sync_routes import SYNC_JOB_TYPES
from data_sync_service.scheduler.sleeve_paper_job import _run_recon

_RECON = "data_sync_service.service.sleeve_paper_recon"


def test_recon_ok_records_success() -> None:
    rec = []
    with (
        patch(f"{_RECON}.sleeve_paper_recon", return_value={"day": "d", "ok": True}),
        patch(
            "data_sync_service.scheduler.sleeve_paper_job.insert_record",
            lambda *a, **k: rec.append((a, k)),
        ),
    ):
        _run_recon("2026-09-09")
    args, kwargs = rec[0]
    assert args[0] == "sleeve_paper_recon" and kwargs.get("success") is True


def test_recon_mismatch_records_failure() -> None:
    recon = {
        "day": "d",
        "ok": False,
        "missedBuys": ["ETF:513100"],
        "missedSells": [],
        "extraOpens": [],
    }
    rec = []
    with (
        patch(f"{_RECON}.sleeve_paper_recon", return_value=recon),
        patch(
            "data_sync_service.scheduler.sleeve_paper_job.insert_record",
            lambda *a, **k: rec.append((a, k)),
        ),
    ):
        _run_recon("2026-09-09")
    args, kwargs = rec[0]
    assert args[0] == "sleeve_paper_recon"
    assert kwargs.get("success") is False
    assert "ETF:513100" in (kwargs.get("error_message") or "")


def test_recon_exception_records_failure() -> None:
    rec = []
    with (
        patch(f"{_RECON}.sleeve_paper_recon", side_effect=RuntimeError("db down")),
        patch(
            "data_sync_service.scheduler.sleeve_paper_job.insert_record",
            lambda *a, **k: rec.append((a, k)),
        ),
    ):
        _run_recon("2026-09-09")
    args, kwargs = rec[0]
    assert kwargs.get("success") is False
    assert "db down" in (kwargs.get("error_message") or "")


def test_recon_job_in_sync_catalog() -> None:
    assert "sleeve_paper_recon" in SYNC_JOB_TYPES
