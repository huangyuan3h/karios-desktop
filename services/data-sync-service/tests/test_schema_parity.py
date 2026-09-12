"""Schema parity guards (OPT-165).

AGENTS requires every ``db/*.py`` ``CREATE_*_SQL`` table to be covered by the
Alembic chain (baseline or a later revision). This file turns that manual
contract into tests:

1. the CREATE TABLE scanner is sane (pure);
2. every table defined in ``db/*.py`` is reachable from the migration chain
   (pure — catches "added table, forgot a migration");
3. after ``alembic upgrade head`` every ``db/*.py`` column exists in Postgres
   (DB — catches "added a column, forgot a migration").
"""

from __future__ import annotations

import importlib
import os
import re
from pathlib import Path

import pytest

from data_sync_service.db import check_db, get_connection
from data_sync_service.db.schema_baseline import baseline_ddl_statements

SERVICE_ROOT = Path(__file__).resolve().parents[1]
DB_DIR = SERVICE_ROOT / "src" / "data_sync_service" / "db"
VERSIONS_DIR = SERVICE_ROOT / "alembic" / "versions"

_CREATE_RE = re.compile(
    r"create\s+table\s+(?:if\s+not\s+exists\s+)?([a-z_][\w]*)\s*\(", re.IGNORECASE
)
_CONSTRAINT_HEADS = {"primary", "unique", "constraint", "foreign", "check", "exclude"}


def _strip_line_comments(sql: str) -> str:
    return "\n".join(line.split("--", 1)[0] for line in sql.splitlines())


def _split_top_level(body: str) -> list[str]:
    parts: list[str] = []
    depth = 0
    current: list[str] = []
    for ch in body:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if ch == "," and depth == 0:
            parts.append("".join(current))
            current = []
        else:
            current.append(ch)
    parts.append("".join(current))
    return parts


def _extract_columns(sql: str) -> dict[str, set[str]]:
    """{table: {column, ...}} for every CREATE TABLE in ``sql``."""
    sql = _strip_line_comments(sql)
    out: dict[str, set[str]] = {}
    for match in _CREATE_RE.finditer(sql):
        table = match.group(1).lower()
        open_paren = match.end() - 1
        depth = 0
        i = open_paren
        while i < len(sql):
            if sql[i] == "(":
                depth += 1
            elif sql[i] == ")":
                depth -= 1
                if depth == 0:
                    break
            i += 1
        body = sql[open_paren + 1 : i]
        cols: set[str] = set()
        for part in _split_top_level(body):
            stripped = part.strip()
            if not stripped:
                continue
            name = re.split(r"[\s(]", stripped, maxsplit=1)[0].strip('"').lower()
            if name and name not in _CONSTRAINT_HEADS:
                cols.add(name)
        out[table] = cols
    return out


def _db_module_columns() -> dict[str, dict[str, set[str]]]:
    """{module_stem: {table: columns}} across db/*.py CREATE TABLE constants.

    Imports each module and reads its *resolved* SQL constants: most modules
    build DDL from f-string table-name constants, so a text scan of the source
    cannot see the table names.
    """
    out: dict[str, dict[str, set[str]]] = {}
    for path in sorted(DB_DIR.glob("*.py")):
        if path.stem.startswith("_"):
            continue
        module = importlib.import_module(f"data_sync_service.db.{path.stem}")
        tables: dict[str, set[str]] = {}
        for attr in dir(module):
            value = getattr(module, attr)
            if isinstance(value, str) and "create table" in value.lower():
                for table, cols in _extract_columns(value).items():
                    tables.setdefault(table, set()).update(cols)
        if tables:
            out[path.stem] = tables
    return out


def _db_tables() -> dict[str, set[str]]:
    """{table: columns} across every db/*.py CREATE TABLE."""
    tables: dict[str, set[str]] = {}
    for cols_by_table in _db_module_columns().values():
        for table, cols in cols_by_table.items():
            tables.setdefault(table, set()).update(cols)
    return tables


def _migration_covered_tables() -> set[str]:
    """Tables reachable via baseline DDL, literal migration DDL, or an import."""
    module_columns = _db_module_columns()
    covered = set(_extract_columns("\n".join(baseline_ddl_statements())))
    for path in sorted(VERSIONS_DIR.glob("*.py")):
        text = path.read_text(encoding="utf-8")
        covered.update(_extract_columns(text))
        # tables declared through the Alembic op API rather than raw DDL
        covered.update(re.findall(r"op\.create_table\(\s*[\"']([a-z_][\w]*)[\"']", text))
        for module in re.findall(r"from\s+data_sync_service\.db\.(\w+)\s+import", text):
            covered.update(module_columns.get(module, {}))
    return covered


# -- pure -----------------------------------------------------------------


def test_scanner_finds_known_columns() -> None:
    tables = _db_tables()
    assert "daily" in tables
    assert {"ts_code", "trade_date", "close"} <= tables["daily"]
    assert len(tables) > 40


def test_every_db_table_has_a_migration() -> None:
    missing = sorted(set(_db_tables()) - _migration_covered_tables())
    assert not missing, f"db/*.py tables with no alembic revision: {missing}"


# -- postgres -------------------------------------------------------------


def _postgres_available() -> bool:
    if os.getenv("SKIP_DB_TESTS", "").lower() in {"1", "true", "yes"}:
        return False
    ok, _ = check_db()
    return ok


@pytest.mark.skipif(not _postgres_available(), reason="Postgres not available")
def test_migrated_schema_covers_db_columns() -> None:
    from alembic.config import Config

    from alembic import command

    cfg = Config(str(SERVICE_ROOT / "alembic.ini"))
    cfg.set_main_option("script_location", str(SERVICE_ROOT / "alembic"))
    cfg.set_main_option("prepend_sys_path", str(SERVICE_ROOT / "src"))
    cfg.set_main_option("path_separator", "os")
    command.upgrade(cfg, "head")

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT table_name, column_name FROM information_schema.columns "
                "WHERE table_schema = 'public'"
            )
            actual: dict[str, set[str]] = {}
            for table, column in cur.fetchall():
                actual.setdefault(str(table), set()).add(str(column))

    problems: list[str] = []
    for table, cols in sorted(_db_tables().items()):
        if table not in actual:
            problems.append(f"{table}: table missing after migrate")
            continue
        missing = sorted(cols - actual[table])
        if missing:
            problems.append(f"{table}: columns missing after migrate: {missing}")
    assert not problems, "\n".join(problems)
