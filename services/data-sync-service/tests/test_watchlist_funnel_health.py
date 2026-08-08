"""watchlist_funnel_health: 3-day consecutive pullback-gate-zero anomaly monitor."""

from __future__ import annotations

from contextlib import ExitStack
from unittest.mock import patch

from data_sync_service.service.watchlist_funnel_health import (
    JOB_TYPE,
    check_funnel_health,
)

SCREENER = {"id": "s1", "name": "pb", "enabled": True}


_REC_DAY = 0


def _rec(success: bool, error_message: str, last_ts_code: str | None = None, sync_at: str | None = None) -> dict:
    global _REC_DAY
    if sync_at is None:
        _REC_DAY += 1
        sync_at = f"2026-08-{8 - _REC_DAY:02d}T10:00:00+00:00"
    return {
        "id": "x",
        "job_type": JOB_TYPE,
        "sync_at": sync_at,
        "success": success,
        "last_ts_code": last_ts_code,
        "error_message": error_message,
    }


def _patch_deps(snapshot_rows, pullback_results, history, insert, collect_error=None):
    def _collect():
        if collect_error:
            raise RuntimeError(collect_error)
        return {
            "tvHit": sum(len(v) for v in snapshot_rows.values()),
            "passPullback": sum(1 for r in pullback_results if r.get("inWindow")),
            "missing": sum(1 for r in pullback_results if r.get("missing")),
            "fallbackWouldTrigger": not any(r.get("inWindow") for r in pullback_results),
            "snapshotCount": len(snapshot_rows),
        }

    return [
        patch(
            "data_sync_service.service.watchlist_funnel_health.collect_funnel_metrics",
            side_effect=_collect,
        ),
        patch(
            "data_sync_service.service.watchlist_funnel_health.list_recent_runs",
            return_value=history,
        ),
        patch(
            "data_sync_service.service.watchlist_funnel_health.insert_record",
            side_effect=insert,
        ),
    ]


class TestCheckFunnelHealth:
    def test_healthy_day_writes_success(self):
        inserted = []

        def insert(job_type, success, last_ts_code=None, error_message=None):
            inserted.append((success, last_ts_code, error_message))

        history = [
            _rec(True, "tvHit=100 passPullback=31", "streak:0"),
        ]
        with ExitStack() as stack:
            for _p in _patch_deps(
            snapshot_rows={"s1": [{"Ticker": "601088"}]},
            pullback_results=[{"inWindow": True, "missing": False}],
            history=history,
            insert=insert,
        ):
                stack.enter_context(_p)
            res = check_funnel_health()

        assert res["ok"] is True
        assert res["streak"] == 0
        assert inserted[0][0] is True
        assert "tvHit=1" in inserted[0][2]

    def test_first_anomaly_day_writes_success_with_streak_1(self):
        inserted = []

        def insert(job_type, success, last_ts_code=None, error_message=None):
            inserted.append((success, last_ts_code, error_message))

        history = [_rec(True, "tvHit=100 passPullback=31", "streak:0")]
        with ExitStack() as stack:
            for _p in _patch_deps(
            snapshot_rows={"s1": [{"Ticker": "601088"}]},
            pullback_results=[{"inWindow": False, "missing": False}],
            history=history,
            insert=insert,
        ):
                stack.enter_context(_p)
            res = check_funnel_health()

        assert res["ok"] is True
        assert res["streak"] == 1
        assert inserted[0][1] == "streak:1"

    def test_three_consecutive_anomaly_days_fails(self):
        inserted = []

        def insert(job_type, success, last_ts_code=None, error_message=None):
            inserted.append((success, last_ts_code, error_message))

        history = [
            _rec(True, "tvHit=100 passPullback=0", "streak:2"),
            _rec(True, "tvHit=99 passPullback=0", "streak:1"),
        ]
        with ExitStack() as stack:
            for _p in _patch_deps(
            snapshot_rows={"s1": [{"Ticker": "601088"}]},
            pullback_results=[{"inWindow": False, "missing": False}],
            history=history,
            insert=insert,
        ):
                stack.enter_context(_p)
            res = check_funnel_health()

        assert res["ok"] is False
        assert res["streak"] == 3
        assert inserted[0][0] is False
        assert "funnel anomaly 3+ days" in inserted[0][2]

    def test_streak_breaks_on_healthy_day(self):
        inserted = []

        def insert(job_type, success, last_ts_code=None, error_message=None):
            inserted.append((success, last_ts_code, error_message))

        history = [
            _rec(True, "tvHit=100 passPullback=31", "streak:0"),  # day before: healthy
            _rec(True, "tvHit=99 passPullback=0", "streak:1"),  # day -2: anomaly
        ]
        with ExitStack() as stack:
            for _p in _patch_deps(
            snapshot_rows={"s1": [{"Ticker": "601088"}]},
            pullback_results=[{"inWindow": False, "missing": False}],
            history=history,
            insert=insert,
        ):
                stack.enter_context(_p)
            res = check_funnel_health()

        assert res["ok"] is True
        assert res["streak"] == 1

    def test_anomaly_through_failed_history_records(self):
        # Failure records (written on >=3-day streaks) still count toward the
        # streak — their metrics are embedded in error_message.
        inserted = []

        def insert(job_type, success, last_ts_code=None, error_message=None):
            inserted.append((success, last_ts_code, error_message))

        history = [
            _rec(
                False,
                "funnel anomaly 3+ days (4): TV hit 100 but pullback gate 0. metrics: tvHit=100 passPullback=0",
                "streak:4",
            ),
            _rec(True, "tvHit=100 passPullback=0", "streak:3"),
            _rec(True, "tvHit=99 passPullback=0", "streak:2"),
        ]
        with ExitStack() as stack:
            for _p in _patch_deps(
            snapshot_rows={"s1": [{"Ticker": "601088"}]},
            pullback_results=[{"inWindow": False, "missing": False}],
            history=history,
            insert=insert,
        ):
                stack.enter_context(_p)
            res = check_funnel_health()

        assert res["ok"] is False
        assert res["streak"] == 4

    def test_same_day_duplicate_runs_do_not_inflate_streak(self):
        # Two runs on the SAME day (scheduled + manual) must count once.
        inserted = []

        def insert(job_type, success, last_ts_code=None, error_message=None):
            inserted.append((success, last_ts_code, error_message))

        history = [
            # Two runs on the SAME day (scheduled + manual) — must count once.
            _rec(True, "tvHit=100 passPullback=0", "streak:1", sync_at="2026-08-07T10:00:00+00:00"),
            _rec(True, "tvHit=100 passPullback=0", "streak:1", sync_at="2026-08-07T18:00:00+00:00"),
            # Day before was healthy → streak capped at 2 (today + 08-07).
            _rec(True, "tvHit=100 passPullback=31", "streak:0", sync_at="2026-08-06T10:00:00+00:00"),
        ]
        with ExitStack() as stack:
            for _p in _patch_deps(
                snapshot_rows={"s1": [{"Ticker": "601088"}]},
                pullback_results=[{"inWindow": False, "missing": False}],
                history=history,
                insert=insert,
            ):
                stack.enter_context(_p)
            res = check_funnel_health()

        assert res["ok"] is True
        assert res["streak"] == 2

    def test_collect_error_writes_failure(self):
        inserted = []

        def insert(job_type, success, last_ts_code=None, error_message=None):
            inserted.append((success, last_ts_code, error_message))

        with ExitStack() as stack:
            for _p in _patch_deps(
            snapshot_rows={},
            pullback_results=[],
            history=[],
            insert=insert,
            collect_error="boom",
        ):
                stack.enter_context(_p)
            res = check_funnel_health()

        assert res["ok"] is False
        assert "collect_error" in inserted[0][2]
