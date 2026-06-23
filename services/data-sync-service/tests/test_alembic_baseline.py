from __future__ import annotations

import os
from pathlib import Path

import pytest

from data_sync_service.db import check_db, get_connection  # type: ignore[import-not-found]
from data_sync_service.db.schema_baseline import BASELINE_REVISION, baseline_ddl_statements

HEAD_REVISION = "0007_industry_fund_flow_taxonomy"


def _postgres_available() -> bool:
    if os.getenv("SKIP_DB_TESTS", "").lower() in {"1", "true", "yes"}:
        return False
    ok, _ = check_db()
    return ok


def test_baseline_ddl_includes_core_tables() -> None:
    ddl = "\n".join(baseline_ddl_statements()).lower()
    for name in (
        "daily",
        "stock_basic",
        "alpha_radar_trends",
        "backtest_run",
        "watchlist_registry",
        "tv_screeners",
    ):
        assert name in ddl


def test_baseline_ddl_includes_industry_fund_flow_taxonomy_fields() -> None:
    ddl = "\n".join(baseline_ddl_statements()).lower()
    assert "market_cn_industry_fund_flow_daily" in ddl
    assert "taxonomy" in ddl
    assert "industry_level" in ddl
    assert "idx_cn_industry_fund_flow_taxonomy_level_date" in ddl


@pytest.mark.skipif(not _postgres_available(), reason="Postgres not available")
def test_alembic_baseline_revision_applied() -> None:
    from alembic import command
    from alembic.config import Config

    service_root = Path(__file__).resolve().parents[1]
    cfg = Config(str(service_root / "alembic.ini"))
    cfg.set_main_option("script_location", str(service_root / "alembic"))
    cfg.set_main_option("prepend_sys_path", str(service_root / "src"))
    cfg.set_main_option("path_separator", "os")
    command.upgrade(cfg, "head")
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT version_num
                FROM alembic_version
                LIMIT 1
                """
            )
            row = cur.fetchone()
    assert row is not None
    assert str(row[0]) == HEAD_REVISION


@pytest.mark.skipif(not _postgres_available(), reason="Postgres not available")
def test_alembic_baseline_core_tables_exist() -> None:
    expected = {
        "daily",
        "stock_basic",
        "alpha_radar_trends",
        "backtest_run",
        "watchlist_registry",
    }
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                  AND table_type = 'BASE TABLE'
                """
            )
            names = {str(r[0]) for r in cur.fetchall()}
    assert expected.issubset(names)


@pytest.mark.skipif(not _postgres_available(), reason="Postgres not available")
def test_alembic_upgrade_head_is_idempotent() -> None:
    from alembic import command
    from alembic.config import Config

    service_root = Path(__file__).resolve().parents[1]
    cfg = Config(str(service_root / "alembic.ini"))
    cfg.set_main_option("script_location", str(service_root / "alembic"))
    cfg.set_main_option("prepend_sys_path", str(service_root / "src"))
    cfg.set_main_option("path_separator", "os")
    command.upgrade(cfg, "head")
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT version_num FROM alembic_version LIMIT 1")
            row = cur.fetchone()
    assert row is not None
    assert str(row[0]) == HEAD_REVISION
