"""Unit tests for return_attribution — identity + edge cases."""

from __future__ import annotations

import math

from data_sync_service.service import return_attribution as ra


def _nav_rows(seq: list[tuple[str, str, float]]) -> list[dict]:
    """(date, pick, dayRet) → cumulative navSingle rows."""
    nav = 1.0
    rows = []
    for date, pick, day_ret in seq:
        nav *= 1.0 + day_ret
        rows.append({"date": date, "pick": pick, "navSingle": nav, "dayRet": day_ret})
    return rows


def test_day_returns_from_nav_identity():
    rows = [
        {"date": "2026-01-01", "pick": "NASDAQ", "navSingle": 1.0},
        {"date": "2026-01-02", "pick": "NASDAQ", "navSingle": 1.1},
        {"date": "2026-01-03", "pick": "GOLD", "navSingle": 1.045},
    ]
    days = ra.day_returns_from_nav(rows)
    assert len(days) == 2
    assert days[0]["pick"] == "NASDAQ"
    assert math.isclose(days[0]["dayRet"], 0.1)
    assert days[1]["pick"] == "GOLD"
    assert math.isclose(days[1]["dayRet"], -0.05)


def test_day_returns_prefer_explicit_day_ret():
    rows = [
        {"date": "2026-01-01", "pick": "REPO", "navSingle": 1.0, "dayRet": 0.0},
        {"date": "2026-01-02", "pick": "REPO", "navSingle": 1.0, "dayRet": 0.0},
    ]
    days = ra.day_returns_from_nav(rows)
    assert len(days) == 2
    assert all(d["dayRet"] == 0.0 for d in days)


def test_additive_sum_equals_sum_day_rets():
    seq = [
        ("2026-01-02", "NASDAQ", 0.02),
        ("2026-01-03", "NASDAQ", 0.01),
        ("2026-01-06", "GOLD", -0.03),
        ("2026-01-07", "REPO", 0.0),
        ("2026-01-08", "STOCK", 0.015),
    ]
    day_rows = [{"date": d, "pick": p, "dayRet": r} for d, p, r in seq]
    out = ra.attribute_by_pick(day_rows)
    add_sum = sum(st["contribAddPct"] for st in out["byPick"].values())
    expected = sum(r for _, _, r in seq) * 100.0
    assert math.isclose(add_sum, expected, abs_tol=1e-6)
    assert math.isclose(out["totalAddPct"], expected, abs_tol=1e-6)
    assert out["totalDays"] == 5
    assert sum(st["days"] for st in out["byPick"].values()) == 5


def test_geometric_total_matches_product():
    seq = [
        ("2026-01-02", "NASDAQ", 0.10),
        ("2026-01-03", "GOLD", -0.05),
        ("2026-01-06", "REPO", 0.0),
    ]
    day_rows = [{"date": d, "pick": p, "dayRet": r} for d, p, r in seq]
    out = ra.attribute_by_pick(day_rows)
    geo = 1.0
    for _, _, r in seq:
        geo *= 1.0 + r
    assert math.isclose(out["totalGeoPct"], (geo - 1.0) * 100.0, abs_tol=1e-6)
    assert math.isclose(out["byPick"]["NASDAQ"]["contribGeoPct"], 10.0, abs_tol=1e-6)
    assert math.isclose(out["byPick"]["GOLD"]["contribGeoPct"], -5.0, abs_tol=1e-6)
    assert out["byPick"]["REPO"]["contribGeoPct"] == 0.0


def test_single_leg_full_window():
    seq = [(f"2026-03-0{i}", "OIL", 0.01) for i in range(1, 6)]
    day_rows = [{"date": d, "pick": p, "dayRet": r} for d, p, r in seq]
    out = ra.attribute_by_pick(day_rows)
    assert out["byPick"]["OIL"]["days"] == 5
    assert out["byPick"]["OIL"]["pctDays"] == 100.0
    assert out["byPick"]["NASDAQ"]["days"] == 0
    geo = (1.01**5 - 1.0) * 100.0
    assert math.isclose(out["totalGeoPct"], geo, abs_tol=1e-4)
    assert math.isclose(out["byPick"]["OIL"]["contribGeoPct"], geo, abs_tol=1e-4)


def test_repo_zero_and_trail_cut():
    seq = [
        ("2026-04-01", "NASDAQ", 0.05),
        ("2026-04-02", "NASDAQ", -0.09),
        ("2026-04-03", "REPO", 0.0),
        ("2026-04-04", "REPO", 0.0),
    ]
    day_rows = [{"date": d, "pick": p, "dayRet": r} for d, p, r in seq]
    out = ra.attribute_by_pick(day_rows)
    assert out["byPick"]["REPO"]["days"] == 2
    assert out["byPick"]["REPO"]["contribAddPct"] == 0.0
    assert out["byPick"]["REPO"]["contribGeoPct"] == 0.0
    assert out["byPick"]["NASDAQ"]["days"] == 2
    assert math.isclose(out["byPick"]["NASDAQ"]["contribAddPct"], -4.0, abs_tol=1e-6)


def test_by_month_and_top_days():
    seq = [
        ("2026-01-02", "GOLD", 0.02),
        ("2026-01-15", "GOLD", -0.01),
        ("2026-02-03", "NASDAQ", 0.08),
        ("2026-02-04", "NASDAQ", -0.02),
    ]
    day_rows = [{"date": d, "pick": p, "dayRet": r} for d, p, r in seq]
    months = ra.attribute_by_month(day_rows)
    assert [m["month"] for m in months] == ["2026-01", "2026-02"]
    assert math.isclose(months[0]["byPick"]["GOLD"], 1.0, abs_tol=1e-6)
    assert math.isclose(months[1]["byPick"]["NASDAQ"], 6.0, abs_tol=1e-6)
    tops = ra.top_days(day_rows, k=2)
    assert tops[0]["date"] == "2026-02-03"
    assert tops[0]["pick"] == "NASDAQ"
    assert tops[0]["dayRetPct"] == 8.0


def test_stock_equal_weight_split_sums_to_basket():
    day_rows = [
        {"date": "2026-05-02", "pick": "STOCK", "dayRet": 0.03},
        {"date": "2026-05-03", "pick": "NASDAQ", "dayRet": 0.01},
    ]
    legs = {
        "2026-05-02": [
            {"symbol": "CN:AAA", "dayRet": 0.06},
            {"symbol": "CN:BBB", "dayRet": 0.00},
        ]
    }
    bd = ra.attribute_stock_symbols(day_rows, legs)
    assert bd is not None
    assert bd["stockDays"] == 1
    assert math.isclose(bd["bySymbol"]["CN:AAA"]["contribAddPct"], 3.0, abs_tol=1e-6)
    assert math.isclose(bd["bySymbol"]["CN:BBB"]["contribAddPct"], 0.0, abs_tol=1e-6)
    total = sum(v["contribAddPct"] for v in bd["bySymbol"].values())
    assert math.isclose(total, 3.0, abs_tol=1e-6)


def test_stock_breakdown_null_without_legs():
    day_rows = [{"date": "2026-05-02", "pick": "STOCK", "dayRet": 0.01}]
    assert ra.attribute_stock_symbols(day_rows, None) is None
    assert ra.attribute_stock_symbols(day_rows, {}) is None


def test_build_stock_legs_by_day():
    calendar = ["2026-05-01", "2026-05-02", "2026-05-03"]
    day_rows = [
        {"date": "2026-05-02", "pick": "STOCK", "dayRet": 0.05},
        {"date": "2026-05-03", "pick": "REPO", "dayRet": 0.0},
    ]
    positions_by_day = [
        {
            "date": "2026-05-01",
            "positions": [
                {"symbol": "CN:111111", "ts_code": "111111.SH", "entry_date": "2026-04-01"},
                {"symbol": "CN:222222", "ts_code": "222222.SH", "entry_date": "2026-04-01"},
            ],
        }
    ]
    closes = {
        "111111.SH": {"2026-05-01": 10.0, "2026-05-02": 11.0},
        "222222.SH": {"2026-05-01": 20.0, "2026-05-02": 20.0},
    }
    legs = ra.build_stock_legs_by_day(
        day_rows=day_rows,
        positions_by_day=positions_by_day,
        close_by_ts_day=closes,
        calendar=calendar,
    )
    assert "2026-05-02" in legs
    assert "2026-05-03" not in legs
    assert len(legs["2026-05-02"]) == 2
    by_sym = {x["symbol"]: x["dayRet"] for x in legs["2026-05-02"]}
    assert math.isclose(by_sym["CN:111111"], 0.1)
    assert math.isclose(by_sym["CN:222222"], 0.0)


def test_attribute_pick_strong_package_matches_fused_pct():
    seq = [
        ("2026-06-02", "NASDAQ", 0.02),
        ("2026-06-03", "GOLD", -0.01),
        ("2026-06-04", "STOCK", 0.03),
    ]
    rows = _nav_rows(seq)
    fused = (rows[-1]["navSingle"] - 1.0) * 100.0
    pkg = ra.attribute_pick_strong(rows)
    assert math.isclose(pkg["totalGeoPct"], fused, abs_tol=0.02)
    assert pkg["totalDays"] == 3
    assert len(pkg["topDays"]) >= 1
    assert pkg["stockBreakdown"] is None


def test_classify_and_user_trades():
    assert ra.classify_user_symbol("ETF:513100") == "NASDAQ"
    assert ra.classify_user_symbol("ETF:518880") == "GOLD"
    assert ra.classify_user_symbol("CN:600519") == "STOCK_CN"
    assert ra.classify_user_symbol("HK:00700") == "STOCK_HK"
    sells = [
        {"symbol": "ETF:513100", "tradeDate": "2026-03-01", "pnlPct": 5.0},
        {"symbol": "CN:600519", "tradeDate": "2026-03-10", "pnlPct": -2.0},
        {"symbol": "ETF:518880", "tradeDate": "2025-01-01", "pnlPct": 99.0},
    ]
    out = ra.attribute_user_trades(sells, start="2026-01-01", end="2026-06-01")
    assert out["closedCount"] == 2
    assert out["insufficient"] is True
    assert out["byBucket"]["NASDAQ"]["sumPnlPct"] == 5.0
    assert out["byBucket"]["STOCK_CN"]["sumPnlPct"] == -2.0
    assert "GOLD" not in out["byBucket"]


def test_nav_chain_without_explicit_day_ret():
    nav = 1.0
    rows = [{"date": "2026-01-01", "pick": "REPO", "navSingle": 1.0}]
    for i, (pick, r) in enumerate(
        [("NASDAQ", 0.05), ("NASDAQ", -0.02), ("GOLD", 0.01)], start=2
    ):
        nav *= 1.0 + r
        rows.append({"date": f"2026-01-0{i}", "pick": pick, "navSingle": nav})
    days = ra.day_returns_from_nav(rows)
    assert len(days) == 3
    pkg = ra.attribute_by_pick(days)
    assert math.isclose(pkg["totalGeoPct"], (nav - 1.0) * 100.0, abs_tol=0.02)
    assert sum(st["days"] for st in pkg["byPick"].values()) == 3


def test_first_timeline_row_above_unit_nav():
    """Timeline omits unit seed — first row nav already includes day-1 return."""
    rows = [
        {"date": "2026-01-02", "pick": "NASDAQ", "navSingle": 1.1},
        {"date": "2026-01-03", "pick": "GOLD", "navSingle": 1.045},
    ]
    days = ra.day_returns_from_nav(rows)
    assert len(days) == 2
    assert math.isclose(days[0]["dayRet"], 0.1)
    assert math.isclose(days[1]["dayRet"], -0.05)
    pkg = ra.attribute_by_pick(days)
    assert math.isclose(pkg["totalGeoPct"], 4.5, abs_tol=0.02)
