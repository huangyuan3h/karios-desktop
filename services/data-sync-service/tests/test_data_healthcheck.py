"""Data health check script: structure and no-crash smoke test."""

from __future__ import annotations

import importlib.util
import os
import runpy
import sys
import time
from datetime import UTC, datetime, timedelta
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


def _load_module():
    """Load the script as a real module (runpy returns a globals *copy*,
    so monkeypatching requires the live module object)."""
    name = "data_healthcheck_under_test"
    sys.modules.pop(name, None)
    spec = importlib.util.spec_from_file_location(name, str(SCRIPT))
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


def test_severity_tags_present() -> None:
    """Every check result carries an explicit OK/WARN/CRITICAL severity."""
    mod = _load_module()
    assert mod._STATUS_TAG == {"ok": "OK  ", "warn": "WARN", "fail": "CRIT"}
    assert mod._ok("m")["severity"] == "OK"
    assert mod._warn("m")["severity"] == "WARN"
    assert mod._fail("m")["severity"] == "CRITICAL"


def _make_dump(tmp_path: Path, name: str, age_hours: float) -> None:
    p = tmp_path / name
    p.write_bytes(b"x")
    ts = time.time() - age_hours * 3600
    os.utime(p, (ts, ts))


def test_backup_age_thresholds(tmp_path: Path) -> None:
    """>26h FAIL (CRITICAL), 20-26h WARN, fresh OK."""
    mod = _load_module()
    mod.BACKUP_DIR = tmp_path
    _make_dump(tmp_path, "karios-20261020-030000.dump", 30)
    res = mod.check_backup_age()
    assert res["status"] == "fail", res
    assert res["severity"] == "CRITICAL"
    for f in tmp_path.glob("karios-*.dump"):
        f.unlink()
    _make_dump(tmp_path, "karios-20261023-030000.dump", 22)
    res = mod.check_backup_age()
    assert res["status"] == "warn", res
    assert res["severity"] == "WARN"
    for f in tmp_path.glob("karios-*.dump"):
        f.unlink()
    _make_dump(tmp_path, "karios-20261024-030000.dump", 5)
    assert mod.check_backup_age()["status"] == "ok"


def test_tv_snapshots_never_critical() -> None:
    """TV snapshots stale ~1700h (since 2026-08) must WARN, never FAIL."""
    mod = _load_module()

    class FakeCur:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def execute(self, *a, **k):
            pass

        def fetchone(self):
            return (datetime.now(UTC) - timedelta(hours=1700),)

    class FakeConn:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def cursor(self):
            return FakeCur()

    mod.get_connection = lambda: FakeConn()
    res = mod.check_tv_snapshots()
    assert res["status"] == "warn", res
    assert res["severity"] == "WARN"


def _fake_scheduler_mod(mod, rows: dict) -> None:
    """rows: job -> None (no record) or (success, err, age_days)."""

    class FakeCur:
        def __init__(self):
            self._row = None

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def execute(self, _sql, params):
            self._row = rows.get(params[0], "missing")

        def fetchone(self):
            if self._row == "missing":
                return None
            success, err, age_days = self._row
            return (datetime.now(UTC) - timedelta(days=age_days), success, err)

    class FakeConn:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def cursor(self):
            return FakeCur()

    mod.get_connection = lambda: FakeConn()


def test_scheduler_env_owned_failures_are_warn_only() -> None:
    """alpha_radar/hk_basic/xq_follow last-run failures must not FAIL the check."""
    mod = _load_module()
    mod.SYNC_JOB_TYPES = (
        "stock_close_sync",
        "watchlist_automation",
        "alpha_radar_pipeline",
        "hk_basic_sync",
        "xq_follow_snapshot",
    )
    _fake_scheduler_mod(
        mod,
        {
            "stock_close_sync": (True, "", 0),
            "watchlist_automation": (True, "", 0),
            "alpha_radar_pipeline": (False, "model not found", 0),
            "hk_basic_sync": (False, "TU_SHARE_API_KEY is not set", 0),
            "xq_follow_snapshot": (False, "JSON parse error", 0),
        },
    )
    res = mod.check_scheduler_jobs()
    assert res["status"] == "warn", res
    assert res["severity"] == "WARN"
    assert "alpha_radar_pipeline" in res["message"]
    assert "hk_basic_sync" in res["message"]


def test_scheduler_noncritical_failure_is_warn() -> None:
    """A failed news_fetch_job (not in the trading-critical set) must WARN, not FAIL."""
    mod = _load_module()
    mod.SYNC_JOB_TYPES = (
        "stock_close_sync",
        "news_fetch_job",
    )
    _fake_scheduler_mod(
        mod,
        {
            "stock_close_sync": (True, "", 0),
            "news_fetch_job": (False, "1 source(s) failed; fetched=158", 0),
        },
    )
    res = mod.check_scheduler_jobs()
    assert res["status"] == "warn", res
    assert res["severity"] == "WARN"
    assert "news_fetch_job" in res["message"]


def test_scheduler_trading_critical_failure_is_critical() -> None:
    """A failed watchlist_automation run must still FAIL (CRITICAL)."""
    mod = _load_module()
    mod.SYNC_JOB_TYPES = (
        "stock_close_sync",
        "watchlist_automation",
        "alpha_radar_pipeline",
    )
    _fake_scheduler_mod(
        mod,
        {
            "stock_close_sync": (True, "", 0),
            "watchlist_automation": (False, "sync blew up", 0),
            "alpha_radar_pipeline": (False, "model not found", 0),
        },
    )
    res = mod.check_scheduler_jobs()
    assert res["status"] == "fail", res
    assert res["severity"] == "CRITICAL"
    assert "watchlist_automation" in res["message"]
    # env-owned failure still visible alongside the critical line
    assert "alpha_radar_pipeline" in res["message"]
