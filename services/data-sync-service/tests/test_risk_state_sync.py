"""TIP-017 risk-state sync tests — fake pro factory (H3) + DB round-trips.

DB rows use sentinel dates (1999-01-01) and are deleted in teardown — never
pollutes live tables (AGENTS.md DB test discipline).
"""

from __future__ import annotations

import pandas as pd
import pytest

from data_sync_service.db import get_connection
from data_sync_service.service import cn_risk_state_sync as sync_mod

SENTINEL = "1999-01-01"
SENTINEL_COMPACT = "19990101"


class _FakePro:
    def fund_share(self, **kw):
        ts = kw.get("ts_code", "510300.SH")
        return pd.DataFrame(
            [
                {
                    "ts_code": ts,
                    "trade_date": SENTINEL_COMPACT,
                    "fd_share": 100.0,
                    "fund_type": "ETF",
                    "market": "SH",
                },
                {
                    "ts_code": ts,
                    "trade_date": "19990102",
                    "fd_share": 101.0,
                    "fund_type": "ETF",
                    "market": "SH",
                },
            ]
        )

    def margin(self, trade_date: str):
        return pd.DataFrame(
            [
                {
                    "trade_date": trade_date,
                    "exchange_id": "SSE",
                    "rzye": 1.0,
                    "rzmre": 2.0,
                    "rzche": 3.0,
                    "rqye": 4.0,
                    "rqmcl": 5.0,
                    "rzrqye": 6.0,
                    "rqyl": 7.0,
                },
            ]
        )

    def moneyflow_hsgt(self, **kw):
        return pd.DataFrame(
            [
                {
                    "trade_date": SENTINEL_COMPACT,
                    "ggt_ss": 1.0,
                    "ggt_sz": 2.0,
                    "hgt": 3.0,
                    "sgt": 4.0,
                    "north_money": 7.0,
                    "south_money": 6.0,
                },
            ]
        )

    def index_global(self, **kw):
        ts = kw.get("ts_code", "HSI")
        return pd.DataFrame(
            [
                {
                    "ts_code": ts,
                    "trade_date": SENTINEL_COMPACT,
                    "open": 1.0,
                    "high": 2.0,
                    "low": 0.5,
                    "close": 1.5,
                },
            ]
        )

    def trade_cal(self, **kw):
        return pd.DataFrame(
            [
                {"cal_date": SENTINEL_COMPACT, "is_open": "1"},
                {"cal_date": "19990102", "is_open": "1"},
            ]
        )


@pytest.fixture()
def _clean_sentinels():
    yield
    tables = ["cn_etf_share", "cn_margin_total", "cn_moneyflow_hsgt", "global_index_daily"]
    with get_connection() as conn:
        with conn.cursor() as cur:
            for t in tables:
                cur.execute(f"DELETE FROM {t} WHERE trade_date <= '1999-12-31'")
            cur.execute("DELETE FROM cn_flow_daily WHERE trade_date <= '1999-12-31'")
            cur.execute("DELETE FROM cn_moneyflow WHERE ts_code = '999999.SH'")
        conn.commit()


@pytest.mark.requires_postgres
class TestRiskStateSync:
    def test_etf_share_roundtrip(self, _clean_sentinels) -> None:
        out = sync_mod.sync_etf_share(
            SENTINEL, "1999-01-02", pro_factory=_FakePro, sleep=lambda s: None
        )
        assert out["ok"] is True
        assert out["updated"] == 8  # 2 rows x 4 codes
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT count(*) FROM cn_etf_share WHERE trade_date <= '1999-12-31'")
                assert cur.fetchone()[0] == 8

    def test_margin_total_roundtrip(self, _clean_sentinels) -> None:
        out = sync_mod.sync_margin_total(
            SENTINEL, "1999-01-02", pro_factory=_FakePro, sleep=lambda s: None
        )
        assert out["ok"] is True
        assert out["updated"] == 2  # 2 days x 1 exchange
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT count(*) FROM cn_margin_total WHERE trade_date <= '1999-12-31'")
                assert cur.fetchone()[0] == 2

    def test_hsgt_roundtrip(self, _clean_sentinels) -> None:
        out = sync_mod.sync_moneyflow_hsgt(
            SENTINEL, "1999-01-02", pro_factory=_FakePro, sleep=lambda s: None
        )
        assert out["ok"] is True
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT north_money FROM cn_moneyflow_hsgt WHERE trade_date = %s", (SENTINEL,)
                )
                assert float(cur.fetchone()[0]) == 7.0

    def test_global_index_alias(self, _clean_sentinels) -> None:
        """HSTECH is fetched via tushare code HKTECH but stored as HSTECH."""
        out = sync_mod.sync_global_index(
            SENTINEL, "1999-01-02", pro_factory=_FakePro, sleep=lambda s: None
        )
        assert out["ok"] is True
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT DISTINCT ts_code FROM global_index_daily WHERE trade_date <= '1999-12-31'"
                )
                codes = {r[0] for r in cur.fetchall()}
        assert codes == {"HSI", "HSTECH"}

    def test_pro_error_is_dict_not_raise(self, _clean_sentinels) -> None:
        def _bad_factory():
            raise RuntimeError("TU_SHARE_API_KEY is not set")

        out = sync_mod.sync_etf_share(SENTINEL, None, pro_factory=_bad_factory)
        assert out["ok"] is False
        assert "error" in out

    def test_flow_daily_aggregation(self, _clean_sentinels) -> None:
        """cn_moneyflow per-stock rows roll up to daily size-class nets."""
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    """INSERT INTO cn_moneyflow
                    (trade_date, ts_code, buy_sm_amount, sell_sm_amount, buy_md_amount,
                     sell_md_amount, buy_lg_amount, sell_lg_amount, buy_elg_amount, sell_elg_amount)
                    VALUES (%s, '999999.SH', %s, %s, %s, %s, %s, %s, %s, %s)""",
                    [
                        (SENTINEL, 100.0, 10.0, 50.0, 20.0, 200.0, 0.0, 0.0, 0.0),
                        ("1999-01-02", 50.0, 60.0, 10.0, 10.0, 0.0, 100.0, 0.0, 0.0),
                    ],
                )
            conn.commit()
        out = sync_mod.sync_flow_daily(SENTINEL, "1999-01-02")
        assert out["ok"] is True
        assert out["updated"] == 2
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """SELECT trade_date, sm_net, lg_net, elg_net, turnover FROM cn_flow_daily
                    WHERE trade_date <= '1999-12-31' ORDER BY trade_date"""
                )
                rows = cur.fetchall()
        d1, d2 = rows
        assert str(d1[0]) == SENTINEL
        assert float(d1[1]) == pytest.approx(90.0)  # sm net day1: 100-10
        assert float(d1[2]) == pytest.approx(200.0)
        assert float(d1[4]) == pytest.approx((100 + 10 + 50 + 20 + 200) / 2)
