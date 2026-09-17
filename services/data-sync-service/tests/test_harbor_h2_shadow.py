"""Unit tests for the H2 shadow ledger (OPT-216, display-only).

Synthetic Harbor/H2 timelines, hand-computed expectations. No DB, no network:
the scheduler job test monkeypatches the timeline builder, the calendar and
the report path.
"""

from __future__ import annotations

from datetime import date

from data_sync_service.service import harbor_h2_shadow as sh


def _results(days: list[tuple[str, str, float, float, str]]) -> tuple[dict, dict]:
    """Build (harbor, h2) timeline results from (date, prev, nav_live, nav_h2, pick_h2)."""
    h_rows = [
        {
            "date": d, "prev": p, "navBase": nl, "navSingle": nl,
            "idlePct": 100.0, "deployedPct": 0.0, "pick": "OIL", "pickTs": "513350.SH",
        }
        for d, p, nl, _nh, _pk in days
    ]
    b_rows = [
        {
            "date": d, "prev": p, "navBase": nl, "navSingle": nh,
            "idlePct": 100.0, "deployedPct": 0.0, "pick": pk, "pickTs": "513350.SH",
            "parkedSides": 0, "parkedTrail": False,
        }
        for d, p, nl, nh, pk in days
    ]
    harbor = {"ok": True, "rows": h_rows, "summary": {"fusedPct": 0.0}}
    h2 = {"ok": True, "rows": b_rows, "summary": {"fusedPct": 0.0}}
    return harbor, h2


def test_status_thresholds() -> None:
    assert sh.status_for(0.0, 0.0) == "tracking"
    assert sh.status_for(1.5, 0.0) == "tracking"
    assert sh.status_for(-1.0, 0.0) == "watch"
    assert sh.status_for(-1.5, 0.0) == "watch"
    # Paper MDD gap line (stable doc): watch at 1.0pt, rollback at 2.0pt.
    assert sh.status_for(0.0, 0.5) == "tracking"
    assert sh.status_for(0.0, 1.0) == "watch"
    assert sh.status_for(0.0, 1.5) == "watch"
    assert sh.status_for(-2.0, 0.0) == "rollback"
    assert sh.status_for(-2.5, 0.0) == "rollback"
    assert sh.status_for(0.0, 2.0) == "rollback"
    assert sh.status_for(0.5, 2.2) == "rollback"


def test_first_run_rebases_inception_even_mid_series() -> None:
    """2026-09-17 audit: a fresh ledger must rebase to 1.0 at inception.

    The old code applied the window's day return when the inception day was
    not the series' first row (production: a trailing-year window), so the
    first ledger row started at e.g. 1.0258 instead of 1.0.
    """
    harbor, h2 = _results([
        ("2026-03-02", "2026-03-01", 1.010, 1.020, "OIL"),
        ("2026-03-03", "2026-03-02", 1.026, 1.026, "OIL"),
        ("2026-03-04", "2026-03-03", 1.030, 1.031, "OIL"),
    ])
    out = sh.build_shadow_report("2026-03-04", harbor, h2, None)
    assert len(out["rows"]) == 1  # only the inception day
    row = out["latest"]
    assert (row["navLive"], row["navH2"]) == (1.0, 1.0)
    assert (row["dayLivePct"], row["dayH2Pct"]) == (0.0, 0.0)
    assert row["status"] == "tracking"


def test_first_run_seeds_inception_row() -> None:
    harbor, h2 = _results([
        ("2026-03-03", "2026-03-02", 1.001, 1.002, "OIL"),
        ("2026-03-04", "2026-03-03", 1.002, 1.003, "OIL"),
    ])
    out = sh.build_shadow_report("2026-03-03", harbor, h2, None)
    assert out["ok"] is True and out["inception"] == "2026-03-03"
    assert len(out["rows"]) == 1  # no fake history: paper starts now
    row = out["latest"]
    assert (row["navLive"], row["navH2"]) == (1.0, 1.0)
    assert (row["dayLivePct"], row["dayH2Pct"]) == (0.0, 0.0)
    assert row["spreadPt"] == 0.0 and row["status"] == "tracking"
    assert row["actionH2"] == "enter"  # first sighting of the held leg
    assert out["thresholds"]["spreadRollbackPt"] == -2.0


def test_append_compounds_window_returns() -> None:
    harbor, h2 = _results([
        ("2026-03-03", "2026-03-02", 1.001, 1.002, "OIL"),
        ("2026-03-04", "2026-03-03", 1.002, 1.003, "OIL"),
    ])
    prev = sh.build_shadow_report("2026-03-03", harbor, h2, None)
    harbor2, h2_2 = _results([
        ("2026-03-03", "2026-03-02", 1.000, 1.000, "OIL"),
        # Live +2.0% on the day, H2 +1.0% -> spread -1.0pt -> watch.
        ("2026-03-04", "2026-03-03", 1.020, 1.010, "GOLD"),
    ])
    out = sh.build_shadow_report("2026-03-04", harbor2, h2_2, prev)
    assert out["appended"] == 1 and len(out["rows"]) == 2
    row = out["latest"]
    assert row["navLive"] == 1.02 and row["navH2"] == 1.01
    assert row["dayLivePct"] == 2.0 and row["dayH2Pct"] == 1.0
    assert row["spreadPt"] == -1.0 and row["status"] == "watch"
    assert row["actionH2"] == "rotate" and row["pickH2"] == "GOLD"
    assert row["ddLivePct"] == 0.0 and row["ddH2Pct"] == 0.0


def test_drawdown_and_rollback() -> None:
    harbor, h2 = _results([
        ("2026-03-03", "2026-03-02", 1.001, 1.002, "OIL"),
        ("2026-03-04", "2026-03-03", 1.002, 1.003, "OIL"),
    ])
    prev = sh.build_shadow_report("2026-03-03", harbor, h2, None)
    # Live flat, H2 -3% in one day: spread -3pt -> rollback.
    harbor2, h2_2 = _results([
        ("2026-03-03", "2026-03-02", 1.000, 1.000, "OIL"),
        ("2026-03-04", "2026-03-03", 1.000, 0.970, "OIL"),
    ])
    out = sh.build_shadow_report("2026-03-04", harbor2, h2_2, prev)
    row = out["latest"]
    assert row["navH2"] == 0.97 and row["ddH2Pct"] == 3.0
    assert row["spreadPt"] == -3.0 and row["status"] == "rollback"


def test_up_to_date_returns_prev_untouched() -> None:
    harbor, h2 = _results([("2026-03-03", "2026-03-02", 1.001, 1.002, "OIL")])
    prev = sh.build_shadow_report("2026-03-03", harbor, h2, None)
    out = sh.build_shadow_report("2026-03-03", harbor, h2, prev)
    assert out["rows"] == prev["rows"] and "up to date" in out["note"]


def test_end_not_covered_raises() -> None:
    import pytest

    harbor, h2 = _results([("2026-03-03", "2026-03-02", 1.001, 1.002, "OIL")])
    with pytest.raises(ValueError, match="does not cover"):
        sh.build_shadow_report("2026-03-05", harbor, h2, None)


def test_save_load_roundtrip(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(sh, "report_path", lambda: tmp_path / "shadow.json")
    harbor, h2 = _results([("2026-03-03", "2026-03-02", 1.001, 1.002, "OIL")])
    report = sh.build_shadow_report("2026-03-03", harbor, h2, None)
    sh.save_shadow_report(report)
    loaded = sh.load_shadow_report()
    assert loaded is not None and loaded["latest"]["date"] == "2026-03-03"


def test_job_run_appends_without_db_or_network(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    from data_sync_service.api import backtest_routes as br
    from data_sync_service.scheduler import harbor_h2_shadow_job as job
    from data_sync_service.service import trade_calendar_utils as tcu

    harbor, h2 = _results([
        ("2026-03-03", "2026-03-02", 1.000, 1.000, "OIL"),
        ("2026-03-04", "2026-03-03", 1.010, 1.005, "OIL"),
    ])
    monkeypatch.setattr(
        br, "_get_or_build_timeline", lambda *a, **k: (harbor, None)
    )
    import data_sync_service.service.parking_sleeve as ps

    monkeypatch.setattr(ps, "blend_harbor_h2_timeline", lambda harbor_result: h2)
    monkeypatch.setattr(sh, "report_path", lambda: tmp_path / "shadow.json")
    monkeypatch.setattr(
        tcu, "last_open_date_on_or_before", lambda d, **k: date(2026, 3, 4)
    )
    monkeypatch.setattr(tcu, "shanghai_today", lambda: date(2026, 3, 4))
    records: list = []
    monkeypatch.setattr(
        job, "insert_record",
        lambda job_type, success, **kw: records.append((job_type, success, kw)),
    )

    # First run seeds inception; second run for the same day is a no-op.
    first = job.run()
    assert first["ok"] is True and first["end"] == "2026-03-04"
    second = job.run()
    assert second["ok"] is True and second.get("upToDate") == "2026-03-04"
    assert [r[0] for r in records] == [job.JOB_ID, job.JOB_ID]
    assert all(r[1] for r in records)
    assert records[0][2]["last_ts_code"] == "2026-03-04"
    loaded = sh.load_shadow_report()
    assert loaded is not None and len(loaded["rows"]) == 1


class TestShadowEndpoint:
    def test_latest_returns_report(self, tmp_path, monkeypatch) -> None:  # noqa: ANN001
        from data_sync_service.api import backtest_routes as br

        monkeypatch.setattr(sh, "report_path", lambda: tmp_path / "shadow.json")
        harbor, h2 = _results([("2026-03-03", "2026-03-02", 1.001, 1.002, "OIL")])
        sh.save_shadow_report(sh.build_shadow_report("2026-03-03", harbor, h2, None))
        out = br.backtest_harbor_h2_shadow_latest()
        assert out["ok"] is True and out["latest"]["date"] == "2026-03-03"

    def test_latest_404_before_first_run(self, tmp_path, monkeypatch) -> None:  # noqa: ANN001
        import pytest
        from fastapi import HTTPException

        from data_sync_service.api import backtest_routes as br

        monkeypatch.setattr(sh, "report_path", lambda: tmp_path / "missing.json")
        with pytest.raises(HTTPException) as exc_info:
            br.backtest_harbor_h2_shadow_latest()
        assert exc_info.value.status_code == 404
