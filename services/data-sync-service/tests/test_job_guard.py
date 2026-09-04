"""scheduler/_job_guard.py tests (OPT-139) — pure unit, no DB."""

from __future__ import annotations

import logging
from unittest.mock import MagicMock, patch

from data_sync_service.scheduler import _job_guard as guard


def _patch_insert():
    return patch("data_sync_service.scheduler._job_guard.insert_record")


def test_run_guarded_returns_fn_value_without_record() -> None:
    with _patch_insert() as insert:
        out = guard.run_guarded("some_job", lambda: {"ok": True})
    assert out == {"ok": True}
    insert.assert_not_called()


def test_run_guarded_records_failure_and_returns_none() -> None:
    def boom():
        raise RuntimeError("upstream down")

    log = logging.getLogger("test-guard")
    with _patch_insert() as insert, patch.object(log, "exception") as exc_log:
        out = guard.run_guarded("some_job", boom, log=log)
    assert out is None
    insert.assert_called_once()
    assert insert.call_args.args[0] == "some_job"
    assert insert.call_args.kwargs["success"] is False
    assert "upstream down" in insert.call_args.kwargs["error_message"]
    exc_log.assert_called_once()


def test_record_success_and_failure_shapes() -> None:
    with _patch_insert() as insert:
        guard.record_success("j1", last_ts_code="ts-1")
    insert.assert_called_once_with("j1", success=True, last_ts_code="ts-1")

    with _patch_insert() as insert:
        guard.record_failure("j2", ValueError("bad"))
    assert insert.call_args.args[0] == "j2"
    assert insert.call_args.kwargs["success"] is False
    assert insert.call_args.kwargs["error_message"] == "ValueError: bad"

    with _patch_insert() as insert:
        guard.record_failure("j3", "plain message")
    assert insert.call_args.kwargs["error_message"] == "plain message"

    # long errors are truncated to 500 chars
    with _patch_insert() as insert:
        guard.record_failure("j4", "x" * 900)
    assert len(insert.call_args.kwargs["error_message"]) == 500


def test_record_failure_never_raises() -> None:
    with _patch_insert() as insert:
        insert.side_effect = RuntimeError("db down")
        guard.record_failure("j5", "boom")  # must not raise


def test_record_dict_result_branches() -> None:
    ok_log = MagicMock()
    fail_log = MagicMock()
    with _patch_insert() as insert:
        out = guard.record_dict_result(
            "j6", {"ok": True, "updated": 3}, ok_log=ok_log, fail_log=fail_log
        )
    assert out == {"ok": True, "updated": 3}
    assert insert.call_args.kwargs["success"] is True
    ok_log.assert_called_once()
    fail_log.assert_not_called()

    ok_log.reset_mock()
    with _patch_insert() as insert:
        out = guard.record_dict_result(
            "j7", {"ok": False, "error": "quota"}, ok_log=ok_log, fail_log=fail_log
        )
    assert out == {"ok": False, "error": "quota"}
    assert insert.call_args.kwargs["success"] is False
    assert insert.call_args.kwargs["error_message"] == "quota"
    fail_log.assert_called_once()
    ok_log.assert_not_called()

    # legacy non-dict success + missing ok defaults to success
    with _patch_insert() as insert:
        assert guard.record_dict_result("j8", None) is None
    assert insert.call_args.kwargs["success"] is True
