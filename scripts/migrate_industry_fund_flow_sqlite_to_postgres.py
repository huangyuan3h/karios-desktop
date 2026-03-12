from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from data_sync_service.db import get_connection
from data_sync_service.db.industry_fund_flow import ensure_table

ROOT = Path(__file__).resolve().parents[1]
SQLITE_PATH = ROOT / "services" / "quant-service" / "karios.sqlite3"


def load_rows() -> list[dict]:
    if not SQLITE_PATH.exists():
        raise FileNotFoundError(f"SQLite DB not found: {SQLITE_PATH}")
    conn = sqlite3.connect(str(SQLITE_PATH))
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT date, industry_code, industry_name, net_inflow, updated_at, raw_json
            FROM market_cn_industry_fund_flow_daily
            """
        )
        rows = cur.fetchall()
    finally:
        conn.close()
    out: list[dict] = []
    for r in rows:
        raw_txt = r[5]
        try:
            raw = json.loads(raw_txt) if raw_txt else {}
        except Exception:
            raw = {"raw": raw_txt}
        out.append(
            {
                "date": str(r[0]),
                "industry_code": str(r[1]),
                "industry_name": str(r[2]),
                "net_inflow": float(r[3] or 0.0),
                "updated_at": str(r[4]),
                "raw": raw,
            }
        )
    return out


def main() -> None:
    ensure_table()
    rows = load_rows()
    if not rows:
        print("No rows found in SQLite table market_cn_industry_fund_flow_daily.")
        return
    with get_connection() as conn:
        with conn.cursor() as cur:
            for r in rows:
                cur.execute(
                    """
                    INSERT INTO market_cn_industry_fund_flow_daily(
                        date, industry_code, industry_name, net_inflow, updated_at, raw_json
                    )
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT(date, industry_code) DO UPDATE SET
                        industry_name = excluded.industry_name,
                        net_inflow = excluded.net_inflow,
                        updated_at = excluded.updated_at,
                        raw_json = excluded.raw_json
                    """,
                    (
                        r["date"],
                        r["industry_code"],
                        r["industry_name"],
                        r["net_inflow"],
                        r["updated_at"],
                        json.dumps(r["raw"], ensure_ascii=False),
                    ),
                )
        conn.commit()
    print(f"Migrated {len(rows)} rows to Postgres.")


if __name__ == "__main__":
    main()
