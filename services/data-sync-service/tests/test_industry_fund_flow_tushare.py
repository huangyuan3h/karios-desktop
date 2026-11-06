"""Non-EM industry fund-flow fallback: Tushare moneyflow aggregated to SW L1."""

from __future__ import annotations

from datetime import date

import pytest

from data_sync_service.service import industry_fund_flow as iff
from data_sync_service.service import industry_fund_flow_tushare as tsf


@pytest.fixture(autouse=True)
def _clean():
    tsf.reset_cache()
    yield
    tsf.reset_cache()


def test_membership_cached_per_day(monkeypatch) -> None:
    calls: list[bool] = []

    def fake_fetch():
        calls.append(True)
        return {"600000.SH": ("801780.SI", "银行")}

    monkeypatch.setattr(tsf, "_fetch_sw_l1_membership", fake_fetch)
    monkeypatch.setattr(tsf, "_shanghai_today_iso", lambda: "2026-11-06")
    assert tsf.sw_l1_membership() == {"600000.SH": ("801780.SI", "银行")}
    assert tsf.sw_l1_membership() == {"600000.SH": ("801780.SI", "银行")}
    assert len(calls) == 1


def test_aggregate_converts_wan_to_cny_and_tags_source(monkeypatch) -> None:
    monkeypatch.setattr(
        tsf, "sw_l1_membership", lambda: {"600000.SH": ("801780.SI", "银行")}
    )
    monkeypatch.setattr(
        tsf,
        "_ensure_moneyflow_ingested",
        lambda day: [
            {
                "ts_code": "600000.SH",
                "buy_lg_amount": 100.0,
                "sell_lg_amount": 40.0,
                "buy_elg_amount": 30.0,
                "sell_elg_amount": 10.0,
            },
            {
                "ts_code": "999999.SZ",  # not in the SW membership
                "buy_lg_amount": 1e9,
                "sell_lg_amount": 0.0,
                "buy_elg_amount": 0.0,
                "sell_elg_amount": 0.0,
            },
        ],
    )
    rows = tsf.aggregate_for_date("2026-11-05")
    assert len(rows) == 1
    row = rows[0]
    assert row["industry_name"] == "银行"
    assert row["industry_code"] == "801780.SI"
    # (100 + 30 - 40 - 10) 万元 = 80 * 10,000 CNY
    assert row["net_inflow"] == pytest.approx(800_000.0)
    assert row["source"] == tsf.SOURCE
    assert row["taxonomy"] == "SW"
    assert row["industry_level"] == 1
    assert row["raw"]["metric"] == tsf.METRIC
    assert row["raw"]["unmappedRows"] == 1


def test_aggregate_raises_when_source_empty(monkeypatch) -> None:
    monkeypatch.setattr(tsf, "sw_l1_membership", lambda: {"600000.SH": ("801780.SI", "银行")})

    def _raise(day):
        raise RuntimeError("tushare: no moneyflow rows for 2026-11-05")

    monkeypatch.setattr(tsf, "_ensure_moneyflow_ingested", _raise)
    with pytest.raises(RuntimeError, match="tushare:"):
        tsf.aggregate_for_date("2026-11-05")


def test_aggregate_for_dates_isolates_failures(monkeypatch) -> None:
    def fake_agg(day):
        if day == "2026-11-04":
            raise RuntimeError("tushare: missing")
        return [{"date": day, "industry_name": "银行", "net_inflow": 1.0}]

    monkeypatch.setattr(tsf, "aggregate_for_date", fake_agg)
    rows, errors = tsf.aggregate_for_dates(["2026-11-03", "2026-11-04"])
    assert [r["date"] for r in rows] == ["2026-11-03"]
    assert errors and "2026-11-04" in errors[0]


def _em_row() -> dict:
    return {
        "date": "2026-11-06",
        "industry_code": "BK1",
        "industry_name": "电子",
        "net_inflow": 1.0,
        "taxonomy": "SW",
        "industry_level": 1,
        "source": "eastmoney_bkzj",
    }


def test_sync_uses_tushare_when_em_empty_and_labels_degraded(monkeypatch) -> None:
    written: list[list[dict]] = []
    monkeypatch.setattr(iff, "shanghai_today", lambda: date(2026, 11, 6))
    monkeypatch.setattr(iff, "_resolve_sync_as_of", lambda **kw: (date(2026, 11, 6), None))
    monkeypatch.setattr(iff, "fetch_cn_industry_fund_flow_eod", lambda as_of: [])
    monkeypatch.setattr(iff, "get_last_eod_error", lambda: "eastmoney_bkzj: RemoteDisconnected")
    monkeypatch.setattr(iff, "row_is_sw_l1", lambda it: True)
    monkeypatch.setattr(iff, "_now_iso", lambda: "now")
    monkeypatch.setattr(iff, "upsert_daily_rows", lambda rows: written.append(list(rows)) or len(rows))
    monkeypatch.setattr(iff, "trade_dates_upto", lambda *a, **k: ["2026-11-06"])
    monkeypatch.setattr(iff, "get_rows_for_dates", lambda dates: written[-1] if written else [])

    fb = [
        {
            "date": "2026-11-06",
            "industry_code": "801780.SI",
            "industry_name": "银行",
            "net_inflow": 852_000_000.0,
            "taxonomy": "SW",
            "industry_level": 1,
            "source": tsf.SOURCE,
            "raw": {"provider": "tushare"},
        }
    ]
    monkeypatch.setattr(tsf, "aggregate_for_dates", lambda days: (fb, []))

    out = iff.sync_cn_industry_fund_flow(force=True)
    assert out["ok"] is True
    assert out["degraded"] is True
    assert out["fallbackUsed"] is True
    assert out["source"] == tsf.SOURCE
    assert out["rows"] == 1
    assert out["rawError"] == "eastmoney_bkzj: RemoteDisconnected"
    assert out["warnings"]
    # The genuinely computed number is written — not a zero placeholder.
    assert written[0][0]["net_inflow"] == pytest.approx(852_000_000.0)
    assert written[0][0]["source"] == tsf.SOURCE


def test_sync_fails_when_em_and_tushare_both_unavailable(monkeypatch) -> None:
    written: list[list[dict]] = []
    monkeypatch.setattr(iff, "shanghai_today", lambda: date(2026, 11, 6))
    monkeypatch.setattr(iff, "_resolve_sync_as_of", lambda **kw: (date(2026, 11, 6), None))
    monkeypatch.setattr(iff, "fetch_cn_industry_fund_flow_eod", lambda as_of: [])
    monkeypatch.setattr(iff, "get_last_eod_error", lambda: "eastmoney_bkzj: RemoteDisconnected")
    monkeypatch.setattr(iff, "upsert_daily_rows", lambda rows: written.append(list(rows)) or len(rows))
    monkeypatch.setattr(
        tsf,
        "aggregate_for_dates",
        lambda days: ([], ["2026-11-06: tushare: no moneyflow rows"]),
    )

    out = iff.sync_cn_industry_fund_flow(force=True)
    assert out["ok"] is False
    assert out["rows"] == 0
    assert "eastmoney_bkzj" in out["error"] and "tushare" in out["error"]
    assert written == []  # never write fabricated zeros


def test_sync_does_not_fabricate_success_on_zero_rows(monkeypatch) -> None:
    monkeypatch.setattr(iff, "shanghai_today", lambda: date(2026, 11, 6))
    monkeypatch.setattr(iff, "_resolve_sync_as_of", lambda **kw: (date(2026, 11, 6), None))
    monkeypatch.setattr(iff, "fetch_cn_industry_fund_flow_eod", lambda as_of: [])
    monkeypatch.setattr(iff, "get_last_eod_error", lambda: None)
    monkeypatch.setattr(tsf, "aggregate_for_dates", lambda days: ([], []))
    out = iff.sync_cn_industry_fund_flow(force=True)
    assert out["ok"] is False
    assert "no rows" in out["error"] or "no industry fund-flow rows" in out["error"]
