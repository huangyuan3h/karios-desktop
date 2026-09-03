"""Vendor 5/15-minute CSV import helpers — no DB required except ranked upsert."""

from __future__ import annotations

from pathlib import Path

import pytest

from data_sync_service.service import ext_minute_csv as ext


def test_filename_to_ts_code() -> None:
    assert ext.filename_to_ts_code("sz000001_2025.csv") == "000001.SZ"
    assert ext.filename_to_ts_code("sh600000_2025.csv") == "600000.SH"
    assert ext.filename_to_ts_code("sz300750.csv") == "300750.SZ"
    assert ext.filename_to_ts_code("bj430047_2025.csv") is None
    assert ext.filename_to_ts_code("readme.txt") is None


def test_detect_freq() -> None:
    assert ext.detect_freq(Path("data/2025_15min")) == 15
    assert ext.detect_freq(Path("data/2024_5min")) == 5
    assert ext.detect_freq(Path("/tmp/5分钟_按年汇总/2024")) == 5
    assert ext.detect_freq(Path("/tmp/15分钟_按年汇总")) == 15


def test_parse_vendor_csv_keeps_1430_and_1500(tmp_path: Path) -> None:
    p = tmp_path / "sz000001_2025.csv"
    p.write_text(
        "\ufeff时间,代码,名称,开盘价,收盘价,最高价,最低价,成交量,成交额,涨幅,振幅\n"
        "2025-01-02 09:45:00,sz000001,平安银行,11.73,11.74,11.76,11.7,1,1,0,0\n"
        "2025-01-02 14:30:00,sz000001,平安银行,11.42,11.43,11.43,11.41,2,2,0,0\n"
        "2025-01-02 14:45:00,sz000001,平安银行,11.43,11.42,11.43,11.41,3,3,0,0\n"
        "2025-01-02 15:00:00,sz000001,平安银行,11.42,11.41,11.43,11.41,4,4,0,0\n",
        encoding="utf-8",
    )
    ts, rows = ext.parse_vendor_csv(p, keep_times=ext.TIMES_15MIN)
    assert ts == "000001.SZ"
    assert [r["time"] for r in rows] == ["1430", "1500"]
    assert rows[0]["close"] == 11.43
    assert rows[1]["close"] == 11.41


def test_parse_keep_times() -> None:
    assert ext.parse_keep_times(None) is None
    assert ext.parse_keep_times("1330,1400") == frozenset({"1330", "1400"})
    try:
        ext.parse_keep_times("14:30")
    except ValueError:
        pass
    else:
        raise AssertionError("expected ValueError")


def test_parse_vendor_ts_iso_and_slash() -> None:
    assert ext.parse_vendor_ts("2024-01-02 14:30:00") == ("2024-01-02", "1430")
    assert ext.parse_vendor_ts("2026/09/02 14:45") == ("2026-09-02", "1445")
    assert ext.parse_vendor_ts("2026/09/02 15:00:00") == ("2026-09-02", "1500")
    assert ext.parse_vendor_ts("bad") is None


def test_parse_vendor_csv_slash_date_2026(tmp_path: Path) -> None:
    p = tmp_path / "sz000001_2026.csv"
    p.write_text(
        "时间,代码,名称,开盘价,收盘价,最高价,最低价,成交量,成交额\n"
        "2026/09/02 14:25,sz000001,x,1,1,1,1,1,1\n"
        "2026/09/02 14:30,sz000001,x,1,11.9,11.9,1,1,1\n"
        "2026/09/02 15:00,sz000001,x,1,11.91,11.91,1,1,1\n",
        encoding="utf-8",
    )
    ts, rows = ext.parse_vendor_csv(p, keep_times=ext.times_for_freq(5))
    assert ts == "000001.SZ"
    assert rows[0]["trade_date"] == "2026-09-02"
    assert [r["time"] for r in rows] == ["1430", "1500"]
    assert rows[0]["close"] == 11.9
    p = tmp_path / "sz000001_2025.csv"
    p.write_text(
        "时间,代码,名称,开盘价,收盘价,最高价,最低价,成交量,成交额\n"
        "2025-01-02 14:25:00,sz000001,x,1,1,1,1,1,1\n"
        "2025-01-02 14:30:00,sz000001,x,1,1.1,1.1,1,1,1\n"
        "2025-01-02 14:35:00,sz000001,x,1,1.2,1.2,1,1,1\n"
        "2025-01-02 15:00:00,sz000001,x,1,1.3,1.3,1,1,1\n",
        encoding="utf-8",
    )
    ts, rows = ext.parse_vendor_csv(p, keep_times=ext.times_for_freq(5))
    assert ts == "000001.SZ"
    assert [r["time"] for r in rows] == ["1430", "1435", "1500"]


@pytest.mark.requires_postgres
def test_15min_does_not_overwrite_5min_source() -> None:
    from data_sync_service.db.bar_5min import TABLE_NAME, ensure_table, upsert_5min_bars
    from data_sync_service.db import get_connection

    ts = "999991.SZ"
    day = "2025-01-02"
    ensure_table()
    five = [
        {
            "trade_date": day,
            "time": "1430",
            "open": 10.0,
            "high": 10.1,
            "low": 9.9,
            "close": 10.05,
            "vol": 1,
            "amount": 1,
        }
    ]
    fifteen = [
        {
            "trade_date": day,
            "time": "1430",
            "open": 9.0,
            "high": 11.0,
            "low": 8.0,
            "close": 10.05,
            "vol": 9,
            "amount": 9,
        }
    ]
    try:
        upsert_5min_bars(ts, five, source=ext.SOURCE_EXT_5MIN)
        upsert_5min_bars(ts, fifteen, source=ext.SOURCE_EXT_15MIN)
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT high, source FROM {TABLE_NAME} WHERE ts_code=%s AND trade_date=%s AND trade_time='1430'",
                    (ts, day),
                )
                high, source = cur.fetchone()
        assert float(high) == 10.1
        assert source == ext.SOURCE_EXT_5MIN
        close_bar = [
            {
                "trade_date": day,
                "time": "1500",
                "open": 9.0,
                "high": 20.0,
                "low": 8.0,
                "close": 11.0,
                "vol": 9,
                "amount": 9,
            }
        ]
        upsert_5min_bars(ts, close_bar, source=ext.SOURCE_EXT_15MIN)
        upsert_5min_bars(
            ts,
            [{**close_bar[0], "high": 11.2, "close": 11.1}],
            source=ext.SOURCE_EXT_5MIN,
        )
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT high, close, source FROM {TABLE_NAME} "
                    f"WHERE ts_code=%s AND trade_date=%s AND trade_time='1500'",
                    (ts, day),
                )
                high, close, source = cur.fetchone()
        assert float(high) == 11.2
        assert float(close) == 11.1
        assert source == ext.SOURCE_EXT_5MIN
    finally:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"DELETE FROM {TABLE_NAME} WHERE ts_code=%s", (ts,))
            conn.commit()
