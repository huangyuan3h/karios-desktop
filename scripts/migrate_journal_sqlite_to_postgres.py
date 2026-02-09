#!/usr/bin/env python3
"""Migrate trade_journals from quant-service SQLite to data-sync-service Postgres."""

from __future__ import annotations

import os
import sqlite3
import sys
from pathlib import Path

# Add data-sync-service src to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "services" / "data-sync-service" / "src"))

from data_sync_service.db import get_connection
from data_sync_service.db.journal import ensure_table


def main() -> None:
    # SQLite source
    sqlite_path = Path(__file__).resolve().parent.parent / "services" / "quant-service" / "karios.sqlite3"
    sqlite_path_env = os.getenv("SQLITE_DB_PATH")
    if sqlite_path_env:
        sqlite_path = Path(sqlite_path_env)
    if not sqlite_path.exists():
        print(f"ERROR: SQLite database not found: {sqlite_path}")
        sys.exit(1)

    print(f"Reading from SQLite: {sqlite_path}")

    # Read from SQLite
    conn_sqlite = sqlite3.connect(str(sqlite_path))
    rows = conn_sqlite.execute(
        """
        SELECT id, title, content_md, created_at, updated_at
        FROM trade_journals
        ORDER BY created_at ASC
        """
    ).fetchall()
    conn_sqlite.close()

    if not rows:
        print("No journals found in SQLite database.")
        return

    print(f"Found {len(rows)} journal entries in SQLite.")

    # Ensure Postgres table exists
    ensure_table()

    # Write to Postgres
    conn_pg = get_connection()
    inserted = 0
    skipped = 0
    errors = 0

    with conn_pg.cursor() as cur:
        for r in rows:
            jid, title, content_md, created_at, updated_at = r
            try:
                cur.execute(
                    """
                    INSERT INTO trade_journals(id, title, content_md, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO UPDATE SET
                        title = EXCLUDED.title,
                        content_md = EXCLUDED.content_md,
                        updated_at = EXCLUDED.updated_at
                    """,
                    (str(jid), str(title), str(content_md), str(created_at), str(updated_at)),
                )
                if cur.rowcount and cur.rowcount > 0:
                    inserted += 1
                else:
                    skipped += 1
            except Exception as e:
                print(f"ERROR inserting journal {jid}: {e}")
                errors += 1
        conn_pg.commit()
    conn_pg.close()

    print(f"\nMigration complete:")
    print(f"  Inserted/updated: {inserted}")
    print(f"  Skipped (already exists): {skipped}")
    print(f"  Errors: {errors}")


if __name__ == "__main__":
    main()
