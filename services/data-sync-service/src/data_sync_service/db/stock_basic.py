"""Stock basic list table: schema and upsert from tushare DataFrame."""

from __future__ import annotations

import pandas as pd  # type: ignore[import-not-found, import-untyped]

from data_sync_service.db import get_connection

TABLE_NAME = "stock_basic"

CREATE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    ts_code     TEXT PRIMARY KEY,
    symbol      TEXT NOT NULL,
    name        TEXT,
    industry    TEXT,
    market      TEXT,
    list_date   DATE,
    delist_date DATE
);
"""

UPSERT_SQL = f"""
INSERT INTO {TABLE_NAME} (ts_code, symbol, name, industry, market, list_date, delist_date)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (ts_code) DO UPDATE SET
    symbol = EXCLUDED.symbol,
    name = EXCLUDED.name,
    industry = EXCLUDED.industry,
    market = EXCLUDED.market,
    list_date = EXCLUDED.list_date,
    delist_date = EXCLUDED.delist_date;
"""


def ensure_table() -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_SQL)
        conn.commit()


def _scalar(val: object) -> str | None:
    if pd.isna(val) or val is None:
        return None
    return str(val).strip() or None


def _date(val: object) -> str | None:
    if pd.isna(val) or val is None:
        return None
    if hasattr(val, "strftime"):
        return val.strftime("%Y-%m-%d")
    return str(val).strip() or None


def upsert_from_dataframe(df: pd.DataFrame) -> int:
    """Upsert rows from tushare stock_basic DataFrame. Returns number of rows upserted."""
    ensure_table()
    rows = []
    for _, row in df.iterrows():
        rows.append((
            _scalar(row.get("ts_code")),
            _scalar(row.get("symbol")),
            _scalar(row.get("name")),
            _scalar(row.get("industry")),
            _scalar(row.get("market")),
            _date(row.get("list_date")),
            _date(row.get("delist_date")),
        ))
    with get_connection() as conn:
        with conn.cursor() as cur:
            for r in rows:
                cur.execute(UPSERT_SQL, r)
        conn.commit()
    return len(rows)


def fetch_ts_codes() -> list[str]:
    """Return ordered list of ts_code from stock_basic (for sync loops)."""
    ensure_table()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT ts_code FROM {TABLE_NAME} ORDER BY ts_code")
            rows = cur.fetchall()
    return [r[0] for r in rows if r and r[0]]


def fetch_ts_codes_by_market(market: str) -> list[str]:
    """Return ordered ts_codes filtered by market."""
    ensure_table()
    market2 = (market or "").strip().upper()
    if not market2:
        return fetch_ts_codes()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT ts_code FROM {TABLE_NAME} WHERE market = %s ORDER BY ts_code",
                (market2,),
            )
            rows = cur.fetchall()
    return [r[0] for r in rows if r and r[0]]


def fetch_all() -> list[dict]:
    """Return all stock_basic rows from DB as list of dicts. Dates as YYYY-MM-DD strings."""
    ensure_table()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT ts_code, symbol, name, industry, market, list_date, delist_date FROM {TABLE_NAME} ORDER BY ts_code"
            )
            rows = cur.fetchall()
            columns = [d.name for d in cur.description]
    out = []
    for row in rows:
        obj = {}
        for col, val in zip(columns, row):
            if hasattr(val, "strftime"):
                obj[col] = val.strftime("%Y-%m-%d")
            else:
                obj[col] = val
        out.append(obj)
    return out


def fetch_market_stocks(
    market: str | None = None,
    q: str | None = None,
    offset: int = 0,
    limit: int = 50,
    use_realtime: bool = False,
) -> tuple[int, list[dict]]:
    """
    Fetch market stocks with filters and pagination.
    Returns: (total_count, items) where items are MarketStockRow-compatible dicts.
    Converts ts_code (e.g., "000001.SZ") to symbol format (e.g., "CN:000001").
    """
    ensure_table()
    market2 = (market or "").strip().upper()
    q2 = (q or "").strip()
    offset2 = max(0, int(offset))
    limit2 = max(1, min(int(limit), 200))

    where: list[str] = []
    params: list[object] = []

    # Filter by market (CN/HK)
    # Note: Tushare uses "主板", "中小板", "创业板", "科创板" for CN stocks
    if market2 == "CN":
        where.append("market IN ('主板', '中小板', '创业板', '科创板', 'CN')")
    elif market2 == "HK":
        where.append("market = %s")
        params.append(market2)
    # If market is "ALL" or empty, don't filter by market (return all)

    # Filter by delist_date (exclude delisted stocks)
    where.append("(delist_date IS NULL OR delist_date > CURRENT_DATE)")

    # Search by ticker or name
    if q2:
        where.append("(symbol LIKE %s OR name LIKE %s)")
        params.extend([f"%{q2}%", f"%{q2}%"])

    where_sql = "WHERE " + " AND ".join(where) if where else ""

    with get_connection() as conn:
        with conn.cursor() as cur:
            # Get total count
            cur.execute(f"SELECT COUNT(*) FROM {TABLE_NAME} {where_sql}", tuple(params))
            total = int(cur.fetchone()[0] or 0)

            # Get paginated results
            cur.execute(
                f"""
                SELECT ts_code, symbol, name, market, list_date, delist_date
                FROM {TABLE_NAME}
                {where_sql}
                ORDER BY market ASC, symbol ASC
                LIMIT %s OFFSET %s
                """,
                tuple(params + [limit2, offset2]),
            )
            rows = cur.fetchall()

    # Collect ts_codes for batch quote fetch
    ts_codes_list: list[str] = []
    row_data: list[tuple[str, str, str, str, str | None]] = []
    
    for r in rows:
        ts_code = str(r[0])
        ticker = str(r[1])
        name = str(r[2]) or ""
        market_val = str(r[3]) or "CN"
        list_date = r[4]
        ts_codes_list.append(ts_code)
        row_data.append((ts_code, ticker, name, market_val, list_date))

    # Fetch quotes in batch
    from data_sync_service.service.market_quotes import get_market_quotes_batch
    quotes_map = get_market_quotes_batch(ts_codes_list, use_realtime=use_realtime)
    
    # Build items with quote data
    items = []
    for ts_code, ticker, name, market_val, list_date in row_data:
        # Normalize market value: Tushare uses "主板", "中小板", "创业板" etc., map to "CN"
        market_normalized = "CN" if market_val in ("主板", "中小板", "创业板", "科创板", "CN") else market_val
        
        # Convert ts_code to symbol format: "000001.SZ" -> "CN:000001"
        if market_normalized == "CN" and "." in ts_code:
            ticker_part = ts_code.split(".")[0]
            symbol = f"CN:{ticker_part}"
        else:
            symbol = f"{market_normalized}:{ticker}"
        
        quote = quotes_map.get(ts_code, {})
        
        items.append({
            "symbol": symbol,
            "market": market_normalized,  # Use normalized market value
            "ticker": ticker,
            "name": name,
            "currency": "CNY" if market_normalized == "CN" else "HKD",
            "price": quote.get("price"),
            "changePct": quote.get("changePct"),
            "volume": quote.get("volume"),
            "turnover": quote.get("turnover"),
            "marketCap": None,  # Market cap not available in daily table
            "updatedAt": str(list_date) if list_date else "",  # Use list_date as fallback
        })

    return total, items


def get_market_status() -> dict:
    """Return market status: total stocks count and last sync time."""
    ensure_table()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {TABLE_NAME} WHERE delist_date IS NULL OR delist_date > CURRENT_DATE")
            total = int(cur.fetchone()[0] or 0)

    # Get last sync time from sync_job_record
    from data_sync_service.db.sync_job_record import get_last_successful_run
    last_run = get_last_successful_run("stock_basic_sync")
    last_sync_at = last_run.get("sync_at") if last_run else None

    return {
        "stocks": total,
        "lastSyncAt": last_sync_at,
    }
