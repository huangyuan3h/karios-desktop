"""Coverage for small db/* CRUD modules (allocation, behavior_audit, factor_signals,
holder/flow/financial upserts, dailybasic/forecast reads).

All keys are far-future fakes (trade_date 2099-01-05, ts 990001.SZ) removed in
teardown; tables are verified clean afterwards (DB test discipline).
"""

from __future__ import annotations

import pytest

from data_sync_service.db import get_connection

pytestmark = pytest.mark.requires_postgres

WEEK = "2099-01-05"
TS = "990001.SZ"
SYM = "CN:990001"


@pytest.fixture(autouse=True)
def _cleanup():
    yield
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM allocation_weights WHERE week_start = %s", (WEEK,))
            cur.execute("DELETE FROM behavior_audit WHERE audit_date = %s", (WEEK,))
            cur.execute("DELETE FROM factor_signals WHERE trade_date = %s", (WEEK,))
            cur.execute("DELETE FROM cn_holder_number WHERE ts_code = %s", (TS,))
            cur.execute("DELETE FROM cn_hk_hold WHERE ts_code = %s", (TS,))
            cur.execute("DELETE FROM cn_margin_detail WHERE ts_code = %s", (TS,))
            cur.execute("DELETE FROM cn_moneyflow WHERE ts_code = %s", (TS,))
            cur.execute("DELETE FROM cn_financial WHERE ts_code = %s", (TS,))
            cur.execute("DELETE FROM stock_dailybasic WHERE ts_code = %s", (TS,))
            cur.execute("DELETE FROM stock_forecast WHERE ts_code = %s", (TS,))
        conn.commit()
    with get_connection() as conn:
        with conn.cursor() as cur:
            for table, col, val in [
                ("allocation_weights", "week_start", WEEK),
                ("behavior_audit", "audit_date", WEEK),
                ("factor_signals", "trade_date", WEEK),
                ("stock_forecast", "ts_code", TS),
            ]:
                cur.execute(f"SELECT COUNT(*) FROM {table} WHERE {col} = %s", (val,))
                assert cur.fetchone()[0] == 0, f"{table} not clean"


def test_allocation_week_decision_roundtrip():
    from data_sync_service.db.allocation import get_week_decision, insert_week_decision

    assert get_week_decision(WEEK) is None
    row = insert_week_decision(
        week_start=WEEK, cn_regime="Strong", hk_regime="Weak", w_cn=1.0, w_hk=0.0
    )
    assert row["week_start"] == WEEK
    assert (row["cn_regime"], row["hk_regime"]) == ("Strong", "Weak")
    assert (row["w_cn"], row["w_hk"]) == (1.0, 0.0)
    # Idempotent: second insert keeps the first decision.
    again = insert_week_decision(
        week_start=WEEK, cn_regime="Weak", hk_regime="Weak", w_cn=0.0, w_hk=0.0
    )
    assert (again["cn_regime"], again["w_cn"]) == ("Strong", 1.0)
    assert get_week_decision(WEEK)["hk_regime"] == "Weak"


def test_behavior_audit_insert_and_latest():
    from data_sync_service.db.behavior_audit import insert_audit, latest_audit

    res = insert_audit(
        audit_date=WEEK, market="TEST99", expected=10, actual=8, extra=1, missing=3,
        extra_list=[{"symbol": SYM}], sat_expected=4, sat_actual=4,
    )
    assert res["id"] is not None
    rows = latest_audit(limit=50)
    mine = [r for r in rows if r["market"] == "TEST99" and r["auditDate"] == WEEK]
    assert len(mine) == 1
    assert (mine[0]["expected"], mine[0]["actual"]) == (10, 8)
    assert mine[0]["extraList"] == [{"symbol": SYM}]
    assert mine[0]["satExpected"] == 4
    # Re-run updates the same day+market row.
    insert_audit(
        audit_date=WEEK, market="TEST99", expected=11, actual=9, extra=0, missing=2
    )
    rows = latest_audit(limit=50)
    mine = [r for r in rows if r["market"] == "TEST99" and r["auditDate"] == WEEK]
    assert len(mine) == 1 and mine[0]["expected"] == 11
    # Corrupt JSON in list columns degrades to [] instead of raising.
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO behavior_audit (audit_date, market, expected, actual, extra, missing,"
                " extra_list, missing_list) VALUES (%s, %s, 0, 0, 0, 0, %s, %s)"
                " ON CONFLICT (audit_date, market) DO UPDATE SET extra_list=excluded.extra_list,"
                " missing_list=excluded.missing_list",
                (WEEK, "TEST98", "{not-json", None),
            )
        conn.commit()
    rows = latest_audit(limit=50)
    bad = [r for r in rows if r["market"] == "TEST98"]
    assert len(bad) == 1 and bad[0]["extraList"] == [] and bad[0]["missingList"] == []
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM behavior_audit WHERE market = 'TEST98' AND audit_date = %s", (WEEK,))
        conn.commit()


def test_factor_signals_upsert_and_fetch():
    from data_sync_service.db.factor_signals import fetch_by_date, upsert_rows

    assert upsert_rows([]) == 0
    assert upsert_rows([{"symbol": SYM}]) == 0  # missing date/factor
    assert upsert_rows([
        {"trade_date": WEEK, "symbol": SYM, "factor_name": "strong_scoop_exhaustion",
         "probability": 0.83, "ret60": 0.45},
        {"trade_date": WEEK, "symbol": "CN:990002", "factor_name": "strong_scoop_exhaustion",
         "probability": 0.92},
    ]) == 2
    # Same key replaces.
    assert upsert_rows([
        {"trade_date": WEEK, "symbol": SYM, "factor_name": "strong_scoop_exhaustion",
         "probability": 0.95},
    ]) == 1
    rows = fetch_by_date(WEEK)
    assert [r["symbol"] for r in rows] == [SYM, "CN:990002"]  # prob DESC
    assert rows[0]["probability"] == pytest.approx(0.95)
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM factor_signals WHERE symbol = 'CN:990002' AND trade_date = %s", (WEEK,))
        conn.commit()


def test_holder_flow_financial_upserts():
    from data_sync_service.db import (
        cn_financial,
        cn_hk_hold,
        cn_holder,
        cn_margin_detail,
        cn_moneyflow,
    )

    for mod in (cn_holder, cn_hk_hold, cn_margin_detail, cn_moneyflow, cn_financial):
        assert mod.upsert_rows([]) == 0
        assert mod.upsert_rows([{"ts_code": ""}]) == 0
    assert cn_holder.upsert_rows([
        {"ts_code": TS, "ann_date": "20990105", "end_date": "2098-12-31", "holder_num": 12345}
    ]) == 1
    assert cn_hk_hold.upsert_rows([
        {"trade_date": WEEK, "ts_code": TS, "vol": 100.0, "ratio": 0.5}
    ]) == 1
    assert cn_margin_detail.upsert_rows([
        {"trade_date": WEEK, "ts_code": TS, "rzye": 1.0, "rqye": 2.0}
    ]) == 1
    assert cn_moneyflow.upsert_rows([
        {"trade_date": WEEK, "ts_code": TS, "net_mf_amount": 3.0}
    ]) == 1
    assert cn_financial.upsert_rows([
        {"ts_code": TS, "ann_date": "20990105", "end_date": "2098-12-31", "eps": 1.5}
    ]) == 1
    # Idempotent re-upserts.
    assert cn_holder.upsert_rows([
        {"ts_code": TS, "ann_date": "2099-01-05", "end_date": "2098-12-31", "holder_num": 999}
    ]) == 1


def test_dailybasic_market_cap_empty_and_row():
    from data_sync_service.db.stock_dailybasic import ensure_table, market_cap_by_date

    ensure_table()
    assert market_cap_by_date("2098-01-01") == {}
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO stock_dailybasic (ts_code, trade_date, total_mv) VALUES (%s, %s, %s)",
                (TS, WEEK, 123.5),
            )
        conn.commit()
    assert market_cap_by_date(WEEK) == {TS: pytest.approx(123.5)}


def test_forecast_positive_dates():
    from data_sync_service.db.stock_forecast import ensure_table, positive_forecast_dates

    ensure_table()
    assert positive_forecast_dates("2099-01-01", "2099-01-31") == {}
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO stock_forecast (ts_code, ann_date, forecast_type) VALUES "
                "(%s, %s, %s), (%s, %s, %s)",
                (TS, "2099-01-05", "预增", TS, "2099-01-06", "预减"),
            )
        conn.commit()
    got = positive_forecast_dates("2099-01-01", "2099-01-31")
    assert got == {TS: {"2099-01-05"}}
