"""Data health check script: structure and no-crash smoke test."""

from __future__ import annotations

import runpy

from pathlib import Path

SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "data_healthcheck.py"


def test_healthcheck_script_runs_without_crash() -> None:
    """Every check must return a dict with status in {ok, warn, fail}."""
    ns = runpy.run_path(str(SCRIPT))
    results = [fn() for _, fn in ns["CHECKS"]]
    assert results, "no checks defined"
    for result in results:
        assert set(result.keys()) >= {"status", "message"}
        assert result["status"] in {"ok", "warn", "fail"}
        assert isinstance(result["message"], str)


def test_healthcheck_exit_code_ranges() -> None:
    """Exit code must be 0 (ok) / 1 (warn) / 2 (fail)."""
    ns = runpy.run_path(str(SCRIPT))
    assert ns["_STATUS_RANK"] == {"ok": 0, "warn": 1, "fail": 2}
