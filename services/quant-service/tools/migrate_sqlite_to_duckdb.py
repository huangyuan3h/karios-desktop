from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path
from typing import Any

import duckdb


FUND_FLOW_COLS = [
    "symbol",
    "date",
    "close",
    "change_pct",
    "main_net_amount",
    "main_net_ratio",
    "super_net_amount",
    "super_net_ratio",
    "large_net_amount",
    "large_net_ratio",
    "medium_net_amount",
    "medium_net_ratio",
    "small_net_amount",
    "small_net_ratio",
    "updated_at",
    "raw_json",
]

SCREENER_COLS = ["id", "name", "url", "enabled", "created_at", "updated_at"]


def _ensure_tables(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS market_fund_flow (
          symbol TEXT NOT NULL,
          date TEXT NOT NULL,
          close TEXT,
          change_pct TEXT,
          main_net_amount TEXT,
          main_net_ratio TEXT,
          super_net_amount TEXT,
          super_net_ratio TEXT,
          large_net_amount TEXT,
          large_net_ratio TEXT,
          medium_net_amount TEXT,
          medium_net_ratio TEXT,
          small_net_amount TEXT,
          small_net_ratio TEXT,
          updated_at TEXT NOT NULL,
          raw_json TEXT NOT NULL,
          PRIMARY KEY(symbol, date)
        )
        """,
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS tv_screeners (
          id TEXT PRIMARY KEY,
          name TEXT NOT NULL,
          url TEXT NOT NULL,
          enabled INTEGER NOT NULL DEFAULT 1,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        )
        """,
    )


def _sqlite_rows(conn: sqlite3.Connection, table: str, cols: list[str]) -> list[dict[str, Any]]:
    cur = conn.execute(f"SELECT {', '.join(cols)} FROM {table}")
    rows = cur.fetchall()
    out: list[dict[str, Any]] = []
    for r in rows:
        out.append({cols[i]: r[i] for i in range(len(cols))})
    return out


def _merge_fund_flow(conn: duckdb.DuckDBPyConnection, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    inserted = 0
    for r in rows:
        raw = r.get("raw_json")
        if raw is None or str(raw).strip() == "":
            r["raw_json"] = "{}"
        conn.execute(
            """
            MERGE INTO market_fund_flow AS t
            USING (
              SELECT
                ? AS symbol,
                ? AS date,
                ? AS close,
                ? AS change_pct,
                ? AS main_net_amount,
                ? AS main_net_ratio,
                ? AS super_net_amount,
                ? AS super_net_ratio,
                ? AS large_net_amount,
                ? AS large_net_ratio,
                ? AS medium_net_amount,
                ? AS medium_net_ratio,
                ? AS small_net_amount,
                ? AS small_net_ratio,
                ? AS updated_at,
                ? AS raw_json
            ) AS s
            ON t.symbol = s.symbol AND t.date = s.date
            WHEN MATCHED THEN UPDATE SET
              close = s.close,
              change_pct = s.change_pct,
              main_net_amount = s.main_net_amount,
              main_net_ratio = s.main_net_ratio,
              super_net_amount = s.super_net_amount,
              super_net_ratio = s.super_net_ratio,
              large_net_amount = s.large_net_amount,
              large_net_ratio = s.large_net_ratio,
              medium_net_amount = s.medium_net_amount,
              medium_net_ratio = s.medium_net_ratio,
              small_net_amount = s.small_net_amount,
              small_net_ratio = s.small_net_ratio,
              updated_at = s.updated_at,
              raw_json = s.raw_json
            WHEN NOT MATCHED THEN INSERT (
              symbol, date, close, change_pct,
              main_net_amount, main_net_ratio,
              super_net_amount, super_net_ratio,
              large_net_amount, large_net_ratio,
              medium_net_amount, medium_net_ratio,
              small_net_amount, small_net_ratio,
              updated_at, raw_json
            ) VALUES (
              s.symbol, s.date, s.close, s.change_pct,
              s.main_net_amount, s.main_net_ratio,
              s.super_net_amount, s.super_net_ratio,
              s.large_net_amount, s.large_net_ratio,
              s.medium_net_amount, s.medium_net_ratio,
              s.small_net_amount, s.small_net_ratio,
              s.updated_at, s.raw_json
            )
            """,
            (
                r.get("symbol"),
                r.get("date"),
                r.get("close"),
                r.get("change_pct"),
                r.get("main_net_amount"),
                r.get("main_net_ratio"),
                r.get("super_net_amount"),
                r.get("super_net_ratio"),
                r.get("large_net_amount"),
                r.get("large_net_ratio"),
                r.get("medium_net_amount"),
                r.get("medium_net_ratio"),
                r.get("small_net_amount"),
                r.get("small_net_ratio"),
                r.get("updated_at"),
                r.get("raw_json"),
            ),
        )
        inserted += 1
    return inserted


def _merge_screeners(conn: duckdb.DuckDBPyConnection, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    inserted = 0
    for r in rows:
        conn.execute(
            """
            MERGE INTO tv_screeners AS t
            USING (
              SELECT
                ? AS id,
                ? AS name,
                ? AS url,
                ? AS enabled,
                ? AS created_at,
                ? AS updated_at
            ) AS s
            ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET
              name = s.name,
              url = s.url,
              enabled = s.enabled,
              created_at = s.created_at,
              updated_at = s.updated_at
            WHEN NOT MATCHED THEN INSERT (
              id, name, url, enabled, created_at, updated_at
            ) VALUES (
              s.id, s.name, s.url, s.enabled, s.created_at, s.updated_at
            )
            """,
            (
                r.get("id"),
                r.get("name"),
                r.get("url"),
                int(r.get("enabled") or 0),
                r.get("created_at"),
                r.get("updated_at"),
            ),
        )
        inserted += 1
    return inserted


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name = ?",
        (name,),
    ).fetchone()
    return bool(row and row[0])


def migrate(sqlite_path: Path, duckdb_path: Path) -> int:
    if not sqlite_path.exists():
        raise FileNotFoundError(f"SQLite DB not found: {sqlite_path}")
    duckdb_path.parent.mkdir(parents=True, exist_ok=True)

    sqlite_conn = sqlite3.connect(str(sqlite_path))
    duck_conn = duckdb.connect(str(duckdb_path))
    _ensure_tables(duck_conn)

    total = 0
    if _table_exists(sqlite_conn, "market_fund_flow"):
        rows = _sqlite_rows(sqlite_conn, "market_fund_flow", FUND_FLOW_COLS)
        total += _merge_fund_flow(duck_conn, rows)
    if _table_exists(sqlite_conn, "tv_screeners"):
        rows = _sqlite_rows(sqlite_conn, "tv_screeners", SCREENER_COLS)
        total += _merge_screeners(duck_conn, rows)

    duck_conn.commit()
    duck_conn.close()
    sqlite_conn.close()
    return total


def main() -> None:
    parser = argparse.ArgumentParser(description="Migrate selected tables from SQLite to DuckDB.")
    parser.add_argument("--sqlite", dest="sqlite_path", required=True, help="Path to SQLite DB.")
    parser.add_argument("--duckdb", dest="duckdb_path", required=True, help="Path to DuckDB file.")
    args = parser.parse_args()

    total = migrate(Path(args.sqlite_path), Path(args.duckdb_path))
    print(f"Done. Upserted {total} rows into DuckDB.")


if __name__ == "__main__":
    main()
