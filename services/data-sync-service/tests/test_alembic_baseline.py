from __future__ import annotations

import os
from pathlib import Path

import pytest

from data_sync_service.db import check_db, get_connection  # type: ignore[import-not-found]
from data_sync_service.db.schema_baseline import baseline_ddl_statements

HEAD_REVISION = "0017_drop_backtest_tables"


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


def test_baseline_ddl_includes_brin_daily_indexes() -> None:
    # OPT-009 follow-up: B-tree idx_daily_trade_date/idx_index_daily_trade_date were
    # replaced with BRIN variants in 0009. Empty-DB parity must reflect the change.
    ddl = "\n".join(baseline_ddl_statements()).lower()
    assert "idx_daily_trade_date_brin" in ddl
    assert "idx_index_daily_trade_date_brin" in ddl


def test_baseline_ddl_statements_are_executable_sql() -> None:
    """Comments with semicolons must not become standalone Alembic statements."""
    from data_sync_service.db.schema_baseline import _split_sql

    messy = """
    CREATE TABLE IF NOT EXISTS t (id INT PRIMARY KEY);
    -- note: removed old index (redundant; PK already covers scans).
    CREATE INDEX IF NOT EXISTS idx_t ON t USING BRIN (id);
    """
    stmts = _split_sql(messy)
    assert len(stmts) == 2
    assert all(s.lstrip().upper().startswith("CREATE") for s in stmts)
    joined = "\n".join(baseline_ddl_statements())
    assert "PK already covers" not in joined


@pytest.mark.skipif(not _postgres_available(), reason="Postgres not available")
def test_brin_daily_indexes_exist_and_btree_gone() -> None:
    from alembic.config import Config

    from alembic import command

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
                SELECT indexname
                FROM pg_indexes
                WHERE schemaname = 'public'
                  AND indexname IN (
                    'idx_daily_trade_date',
                    'idx_index_daily_trade_date',
                    'idx_daily_trade_date_brin',
                    'idx_index_daily_trade_date_brin'
                  )
                """
            )
            names = {str(r[0]) for r in cur.fetchall()}
    assert "idx_daily_trade_date" not in names
    assert "idx_index_daily_trade_date" not in names
    assert "idx_daily_trade_date_brin" in names
    assert "idx_index_daily_trade_date_brin" in names


@pytest.mark.skipif(not _postgres_available(), reason="Postgres not available")
def test_alembic_baseline_revision_applied() -> None:
    from alembic.config import Config

    from alembic import command

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
    from alembic.config import Config

    from alembic import command

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
