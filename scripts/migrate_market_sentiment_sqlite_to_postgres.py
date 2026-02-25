from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from data_sync_service.db import get_connection
from data_sync_service.db.market_sentiment import ensure_table

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
            SELECT date, as_of_date,
                   up_count, down_count, flat_count, total_count, up_down_ratio,
                   market_turnover_cny, market_volume,
                   yesterday_limitup_premium, failed_limitup_rate,
                   risk_mode, rules_json, updated_at, raw_json
            FROM market_cn_sentiment_daily
            """
        )
        rows = cur.fetchall()
    finally:
        conn.close()
    out: list[dict] = []
    for r in rows:
        try:
            rules = json.loads(r[12]) if r[12] else []
        except Exception:
            rules = []
        try:
            raw = json.loads(r[14]) if r[14] else {}
        except Exception:
            raw = {"raw": r[14]}
        out.append(
            {
                "date": str(r[0]),
                "as_of_date": str(r[1]),
                "up_count": int(r[2] or 0),
                "down_count": int(r[3] or 0),
                "flat_count": int(r[4] or 0),
                "total_count": int(r[5] or 0),
                "up_down_ratio": float(r[6] or 0.0),
                "market_turnover_cny": float(r[7] or 0.0),
                "market_volume": float(r[8] or 0.0),
                "yesterday_limitup_premium": float(r[9] or 0.0),
                "failed_limitup_rate": float(r[10] or 0.0),
                "risk_mode": str(r[11] or "normal"),
                "rules": rules if isinstance(rules, list) else [],
                "updated_at": str(r[13]),
                "raw": raw,
            }
        )
    return out


def _sanitize_json(value):
    if isinstance(value, float):
        if value != value or value in (float("inf"), float("-inf")):
            return None
        return value
    if isinstance(value, dict):
        return {str(k): _sanitize_json(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_sanitize_json(v) for v in value]
    return value


def main() -> None:
    ensure_table()
    rows = load_rows()
    if not rows:
        print("No rows found in SQLite table market_cn_sentiment_daily.")
        return
    with get_connection() as conn:
        with conn.cursor() as cur:
            for r in rows:
                safe_rules = _sanitize_json(r["rules"]) or []
                safe_raw = _sanitize_json(r["raw"]) or {}
                cur.execute(
                    """
                    INSERT INTO market_cn_sentiment_daily(
                        date, as_of_date,
                        up_count, down_count, flat_count, total_count, up_down_ratio,
                        market_turnover_cny, market_volume,
                        yesterday_limitup_premium, failed_limitup_rate,
                        risk_mode, rules_json, updated_at, raw_json
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT(date) DO UPDATE SET
                        as_of_date = excluded.as_of_date,
                        up_count = excluded.up_count,
                        down_count = excluded.down_count,
                        flat_count = excluded.flat_count,
                        total_count = excluded.total_count,
                        up_down_ratio = excluded.up_down_ratio,
                        market_turnover_cny = excluded.market_turnover_cny,
                        market_volume = excluded.market_volume,
                        yesterday_limitup_premium = excluded.yesterday_limitup_premium,
                        failed_limitup_rate = excluded.failed_limitup_rate,
                        risk_mode = excluded.risk_mode,
                        rules_json = excluded.rules_json,
                        updated_at = excluded.updated_at,
                        raw_json = excluded.raw_json
                    """,
                    (
                        r["date"],
                        r["as_of_date"],
                        r["up_count"],
                        r["down_count"],
                        r["flat_count"],
                        r["total_count"],
                        r["up_down_ratio"],
                        r["market_turnover_cny"],
                        r["market_volume"],
                        r["yesterday_limitup_premium"],
                        r["failed_limitup_rate"],
                        r["risk_mode"],
                        json.dumps(safe_rules, ensure_ascii=False, allow_nan=False),
                        r["updated_at"],
                        json.dumps(safe_raw, ensure_ascii=False, allow_nan=False),
                    ),
                )
        conn.commit()
    print(f"Migrated {len(rows)} rows to Postgres.")


if __name__ == "__main__":
    main()
