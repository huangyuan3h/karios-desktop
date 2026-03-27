"""Trade review table (Postgres) and CRUD helpers."""

from __future__ import annotations

import json
from typing import Any

from data_sync_service.db import get_connection

TABLE_NAME = "trade_reviews"

CREATE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    id                           TEXT PRIMARY KEY,
    symbol                       TEXT NOT NULL,
    stock_name                   TEXT,
    buy_date                     DATE,
    sell_date                    DATE,
    holding_days                 INTEGER,
    pnl_amount                   NUMERIC,
    pnl_pct                      NUMERIC,
    total_capital_impact_pct     NUMERIC,
    max_loss_guardrail_pct       NUMERIC NOT NULL DEFAULT 2.0,
    market_light_entry           TEXT,
    market_light_exit            TEXT,
    buy_logic_fund_resonance     BOOLEAN NOT NULL DEFAULT FALSE,
    buy_logic_pattern_breakout   BOOLEAN NOT NULL DEFAULT FALSE,
    buy_logic_macro_sentiment    BOOLEAN NOT NULL DEFAULT FALSE,
    buy_logic_notes              TEXT,
    position_pct                 NUMERIC,
    buy_avg_price                NUMERIC,
    initial_defense_price        NUMERIC,
    sell_avg_price               NUMERIC,
    sell_reason                  TEXT,
    execution_notes              TEXT,
    good_actions                 TEXT,
    improvement_areas            TEXT,
    custom_payload               TEXT NOT NULL DEFAULT '{{}}',
    created_at                   TEXT NOT NULL,
    updated_at                   TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_trade_reviews_updated_at ON {TABLE_NAME}(updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_trade_reviews_symbol ON {TABLE_NAME}(symbol);
"""


def ensure_table() -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_SQL)
        conn.commit()


def _to_float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _to_int(v: Any) -> int | None:
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _to_bool(v: Any) -> bool:
    return bool(v)


def _to_date(v: Any) -> str | None:
    if v is None:
        return None
    if hasattr(v, "strftime"):
        return v.strftime("%Y-%m-%d")
    s = str(v).strip()
    return s or None


def _to_json_obj(v: Any) -> dict[str, Any]:
    if isinstance(v, dict):
        return v
    if v is None:
        return {}
    try:
        decoded = json.loads(str(v))
        return decoded if isinstance(decoded, dict) else {}
    except Exception:  # noqa: BLE001
        return {}


def _row_to_dict(row: Any) -> dict[str, Any]:
    return {
        "id": str(row[0]),
        "symbol": str(row[1]),
        "stockName": str(row[2]) if row[2] is not None else None,
        "buyDate": _to_date(row[3]),
        "sellDate": _to_date(row[4]),
        "holdingDays": _to_int(row[5]),
        "pnlAmount": _to_float(row[6]),
        "pnlPct": _to_float(row[7]),
        "totalCapitalImpactPct": _to_float(row[8]),
        "maxLossGuardrailPct": _to_float(row[9]),
        "marketLightEntry": str(row[10]) if row[10] is not None else None,
        "marketLightExit": str(row[11]) if row[11] is not None else None,
        "buyLogicFundResonance": _to_bool(row[12]),
        "buyLogicPatternBreakout": _to_bool(row[13]),
        "buyLogicMacroSentiment": _to_bool(row[14]),
        "buyLogicNotes": str(row[15]) if row[15] is not None else None,
        "positionPct": _to_float(row[16]),
        "buyAvgPrice": _to_float(row[17]),
        "initialDefensePrice": _to_float(row[18]),
        "sellAvgPrice": _to_float(row[19]),
        "sellReason": str(row[20]) if row[20] is not None else None,
        "executionNotes": str(row[21]) if row[21] is not None else None,
        "goodActions": str(row[22]) if row[22] is not None else None,
        "improvementAreas": str(row[23]) if row[23] is not None else None,
        "customPayload": _to_json_obj(row[24]),
        "createdAt": str(row[25]),
        "updatedAt": str(row[26]),
    }


def fetch_all(limit: int = 50, offset: int = 0, symbol: str | None = None) -> tuple[int, list[dict[str, Any]]]:
    """Return total and paginated trade reviews."""
    ensure_table()
    lim = max(1, min(int(limit), 200))
    off = max(0, int(offset))
    symbol2 = (symbol or "").strip()

    where_sql = ""
    count_params: tuple[Any, ...] = ()
    list_params: tuple[Any, ...]
    if symbol2:
        where_sql = " WHERE symbol = %s"
        count_params = (symbol2,)
        list_params = (symbol2, lim, off)
    else:
        list_params = (lim, off)

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}{where_sql}", count_params)
            total = int(cur.fetchone()[0] or 0)
            cur.execute(
                f"""
                SELECT
                    id, symbol, stock_name, buy_date, sell_date, holding_days,
                    pnl_amount, pnl_pct, total_capital_impact_pct, max_loss_guardrail_pct,
                    market_light_entry, market_light_exit,
                    buy_logic_fund_resonance, buy_logic_pattern_breakout, buy_logic_macro_sentiment,
                    buy_logic_notes, position_pct, buy_avg_price, initial_defense_price, sell_avg_price,
                    sell_reason, execution_notes, good_actions, improvement_areas, custom_payload,
                    created_at, updated_at
                FROM {TABLE_NAME}
                {where_sql}
                ORDER BY updated_at DESC
                LIMIT %s OFFSET %s
                """,
                list_params,
            )
            rows = cur.fetchall()
    return total, [_row_to_dict(r) for r in rows]


def fetch_by_id(review_id: str) -> dict[str, Any] | None:
    """Return one trade review by id."""
    ensure_table()
    rid = (review_id or "").strip()
    if not rid:
        return None
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT
                    id, symbol, stock_name, buy_date, sell_date, holding_days,
                    pnl_amount, pnl_pct, total_capital_impact_pct, max_loss_guardrail_pct,
                    market_light_entry, market_light_exit,
                    buy_logic_fund_resonance, buy_logic_pattern_breakout, buy_logic_macro_sentiment,
                    buy_logic_notes, position_pct, buy_avg_price, initial_defense_price, sell_avg_price,
                    sell_reason, execution_notes, good_actions, improvement_areas, custom_payload,
                    created_at, updated_at
                FROM {TABLE_NAME}
                WHERE id = %s
                """,
                (rid,),
            )
            row = cur.fetchone()
    if not row:
        return None
    return _row_to_dict(row)


def create_review(
    *,
    review_id: str,
    payload: dict[str, Any],
    created_at: str,
    updated_at: str,
) -> dict[str, Any]:
    """Create a trade review."""
    ensure_table()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {TABLE_NAME} (
                    id, symbol, stock_name, buy_date, sell_date, holding_days, pnl_amount, pnl_pct,
                    total_capital_impact_pct, max_loss_guardrail_pct, market_light_entry, market_light_exit,
                    buy_logic_fund_resonance, buy_logic_pattern_breakout, buy_logic_macro_sentiment,
                    buy_logic_notes, position_pct, buy_avg_price, initial_defense_price, sell_avg_price,
                    sell_reason, execution_notes, good_actions, improvement_areas, custom_payload,
                    created_at, updated_at
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s
                )
                """,
                (
                    review_id,
                    payload.get("symbol"),
                    payload.get("stockName"),
                    payload.get("buyDate"),
                    payload.get("sellDate"),
                    payload.get("holdingDays"),
                    payload.get("pnlAmount"),
                    payload.get("pnlPct"),
                    payload.get("totalCapitalImpactPct"),
                    payload.get("maxLossGuardrailPct"),
                    payload.get("marketLightEntry"),
                    payload.get("marketLightExit"),
                    payload.get("buyLogicFundResonance"),
                    payload.get("buyLogicPatternBreakout"),
                    payload.get("buyLogicMacroSentiment"),
                    payload.get("buyLogicNotes"),
                    payload.get("positionPct"),
                    payload.get("buyAvgPrice"),
                    payload.get("initialDefensePrice"),
                    payload.get("sellAvgPrice"),
                    payload.get("sellReason"),
                    payload.get("executionNotes"),
                    payload.get("goodActions"),
                    payload.get("improvementAreas"),
                    json.dumps(payload.get("customPayload") or {}),
                    created_at,
                    updated_at,
                ),
            )
        conn.commit()
    return fetch_by_id(review_id) or {}


def update_review(*, review_id: str, payload: dict[str, Any], updated_at: str) -> dict[str, Any] | None:
    """Patch-update trade review. Only fields present in payload are updated."""
    ensure_table()
    existing = fetch_by_id(review_id)
    if not existing:
        return None

    merged = {**existing, **payload}
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE {TABLE_NAME}
                SET
                    symbol = %s,
                    stock_name = %s,
                    buy_date = %s,
                    sell_date = %s,
                    holding_days = %s,
                    pnl_amount = %s,
                    pnl_pct = %s,
                    total_capital_impact_pct = %s,
                    max_loss_guardrail_pct = %s,
                    market_light_entry = %s,
                    market_light_exit = %s,
                    buy_logic_fund_resonance = %s,
                    buy_logic_pattern_breakout = %s,
                    buy_logic_macro_sentiment = %s,
                    buy_logic_notes = %s,
                    position_pct = %s,
                    buy_avg_price = %s,
                    initial_defense_price = %s,
                    sell_avg_price = %s,
                    sell_reason = %s,
                    execution_notes = %s,
                    good_actions = %s,
                    improvement_areas = %s,
                    custom_payload = %s,
                    updated_at = %s
                WHERE id = %s
                """,
                (
                    merged.get("symbol"),
                    merged.get("stockName"),
                    merged.get("buyDate"),
                    merged.get("sellDate"),
                    merged.get("holdingDays"),
                    merged.get("pnlAmount"),
                    merged.get("pnlPct"),
                    merged.get("totalCapitalImpactPct"),
                    merged.get("maxLossGuardrailPct"),
                    merged.get("marketLightEntry"),
                    merged.get("marketLightExit"),
                    merged.get("buyLogicFundResonance"),
                    merged.get("buyLogicPatternBreakout"),
                    merged.get("buyLogicMacroSentiment"),
                    merged.get("buyLogicNotes"),
                    merged.get("positionPct"),
                    merged.get("buyAvgPrice"),
                    merged.get("initialDefensePrice"),
                    merged.get("sellAvgPrice"),
                    merged.get("sellReason"),
                    merged.get("executionNotes"),
                    merged.get("goodActions"),
                    merged.get("improvementAreas"),
                    json.dumps(merged.get("customPayload") or {}),
                    updated_at,
                    review_id,
                ),
            )
        conn.commit()
    return fetch_by_id(review_id)


def delete_review(review_id: str) -> bool:
    """Delete one trade review by id."""
    ensure_table()
    rid = (review_id or "").strip()
    if not rid:
        return False
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM {TABLE_NAME} WHERE id = %s", (rid,))
            ok = (cur.rowcount or 0) > 0
        conn.commit()
    return ok
