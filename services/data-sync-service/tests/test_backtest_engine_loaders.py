"""Loader + simulate branch coverage for backtest_engine (pure unit tests).

No DB, no network. DB loaders are exercised hermetically by patching
``backtest_engine.get_connection`` with a FakeConn whose cursor returns
canned ``fetchall()`` rows. Simulate branches use small synthetic
calendars in the style of test_backtest_engine_simflags.
"""

from __future__ import annotations

import datetime as _dt

import pytest

import data_sync_service.service.backtest_engine as be
from data_sync_service.service.backtest_engine import (
    BacktestConfig,
    BacktestData,
    _last_close_before,
    _load_avg_amount,
    _load_calendar,
    _load_delist_dates,
    _load_flow_mainline_data,
    _load_industries,
    _load_industry_data,
    _load_light_red_days,
    _load_market_caps,
    _load_mom_ranks,
    _load_regime_by_day,
    _load_rs_ranks,
    _load_scores,
    _load_sentiment_risk,
    _load_st_names,
    _trend_score,
    load_benchmarks,
    simulate,
)

CN1 = "CN:600001"
TS1 = "600001.SH"
CN2 = "CN:600002"
TS2 = "600002.SH"


# ---------------------------------------------------------------------------
# Fake DB plumbing
# ---------------------------------------------------------------------------


class FakeCur:
    """Cursor stub dispatching fetchall rows from a handler(sql, params)."""

    def __init__(self, handler) -> None:
        self._handler = handler
        self._rows: list = []

    def __enter__(self) -> FakeCur:
        return self

    def __exit__(self, *args: object) -> bool:
        return False

    def execute(self, sql: object, params: object = None) -> None:
        self._rows = self._handler(str(sql), params)

    def fetchall(self) -> list:
        return self._rows


class FakeConn:
    def __init__(self, handler) -> None:
        self._handler = handler

    def __enter__(self) -> FakeConn:
        return self

    def __exit__(self, *args: object) -> bool:
        return False

    def cursor(self) -> FakeCur:
        return FakeCur(self._handler)


class BoomConn:
    """Simulates a missing table: entering the connection raises."""

    def __enter__(self) -> BoomConn:
        raise RuntimeError("no such table")

    def __exit__(self, *args: object) -> bool:
        return False


def _fixed(rows: list) -> FakeConn:
    return FakeConn(lambda _sql, _params: rows)


def _cfg(**kw: object) -> BacktestConfig:
    base: dict[str, object] = {"start_date": "2026-06-18", "end_date": "2026-06-19"}
    base.update(kw)
    return BacktestConfig(**base)  # type: ignore[arg-type]


def _data(
    calendar: list[str],
    scores: dict[str, dict[str, float]],
    prices: dict[str, dict[str, float]],
    *,
    regime: str = "Strong",
    flow_any_positive: bool = True,
    mainline_allow: set[str] | None = None,
    industry_by_ts: dict[str, str] | None = None,
) -> BacktestData:
    data = BacktestData.__new__(BacktestData)
    data.config = None
    data.calendar = calendar
    data.scores_by_day = scores
    data.ts_codes = []
    data.bars_by_ts = {}
    data.close_by_ts_day = {ts: {d: float(px) for d, px in m.items()} for ts, m in prices.items()}
    data.regime_by_day = {d: regime for d in calendar}
    data.flow_any_positive_by_day = {d: flow_any_positive for d in calendar}
    data.mainline_allow_by_day = {d: set(mainline_allow or {"\u8ba1\u7b97\u673a"}) for d in calendar}
    data.flow5d_by_day = {}
    if industry_by_ts is None:
        industry_by_ts = {ts: "\u8ba1\u7b97\u673a" for ts in prices}
    data.industry_by_ts = dict(industry_by_ts)
    data.sentiment_risk_by_day = {}
    data.light_red_by_day = set()
    data.env_by_day = {d: "unknown" for d in calendar}
    data.closes_by_ts = {
        ts: [(d, float(px)) for d, px in sorted(m.items())] for ts, m in prices.items()
    }
    data.rs_rank_by_day = {}
    data.mv_by_day = {}
    data.avg_amount_by_day = {}
    data.ind_industry_rank_by_day = {}
    data.ind_within_rank_by_day = {}
    data.mom_rank_by_day = {}
    data.st_ts_codes = set()
    data.pead_events = {}
    data.delist_by_ts = {}
    return data


def _days(start: str, n: int) -> list[str]:
    d0 = _dt.date.fromisoformat(start)
    return [(d0 + _dt.timedelta(days=i)).isoformat() for i in range(n)]


def _flat(calendar: list[str], px: float = 10.0) -> dict[str, float]:
    return {d: px for d in calendar}


# ---------------------------------------------------------------------------
# _load_calendar
# ---------------------------------------------------------------------------


def test_loader_calendar_mixes_date_objects_and_strings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rows = [(_dt.date(2026, 6, 18),), ("2026-06-19",)]
    monkeypatch.setattr(be, "get_connection", lambda: _fixed(rows))
    assert _load_calendar("2026-06-18", "2026-06-19") == ["2026-06-18", "2026-06-19"]


def test_loader_calendar_empty_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(be, "get_connection", lambda: _fixed([]))
    assert _load_calendar("2026-06-18", "2026-06-19") == []


# ---------------------------------------------------------------------------
# load_benchmarks: happy + <2 rows skip + start_px<=0 skip
# ---------------------------------------------------------------------------


def test_loader_benchmarks_happy_and_skips(monkeypatch: pytest.MonkeyPatch) -> None:
    def handler(sql: str, params: object) -> list:
        code = params[0] if isinstance(params, (list, tuple)) else ""  # type: ignore[index]
        if code == "000001.SH":
            return [("2026-06-18", 100.0), ("2026-06-19", 110.0)]
        if code == "399006.SZ":
            return [("2026-06-18", 100.0)]  # <2 rows -> skip
        if code == "000300.SH":
            return [("2026-06-18", 0.0), ("2026-06-19", 110.0)]  # start<=0 -> skip
        return []

    monkeypatch.setattr(be, "get_connection", lambda: FakeConn(handler))
    out = load_benchmarks("2026-06-18", "2026-06-19")
    by_code = {r["ts_code"]: r for r in out}
    assert by_code["000001.SH"]["total_return_pct"] == pytest.approx(10.0)
    assert "399006.SZ" not in by_code
    assert "000300.SH" not in by_code
    assert "000905.SH" not in by_code


def test_loader_benchmarks_all_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(be, "get_connection", lambda: _fixed([]))
    assert load_benchmarks("2026-06-18", "2026-06-19") == []


# ---------------------------------------------------------------------------
# _load_scores: prefix filter, bad-score skip, date objects vs strings
# ---------------------------------------------------------------------------


def test_loader_scores_prefix_and_bad_score_skip(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rows = [
        ("cn:600001", _dt.date(2026, 6, 18), 90.0),  # lower-case -> upper passes
        ("CN:600002", "2026-06-18", "BAD"),  # bad score -> skip
        ("CN:600003", "2026-06-18", None),  # None score -> skip
        ("HK:00700", "2026-06-18", 95.0),  # wrong market -> skip
        ("CN:600004", _dt.date(2026, 6, 19), 80.0),  # date object branch
        (None, "2026-06-19", 90.0),  # empty symbol -> skip
    ]
    monkeypatch.setattr(be, "get_connection", lambda: _fixed(rows))
    out = _load_scores("2026-06-18", "2026-06-19", "CN")
    assert out == {"2026-06-18": {"CN:600001": 90.0}, "2026-06-19": {"CN:600004": 80.0}}


def test_loader_scores_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(be, "get_connection", lambda: _fixed([]))
    assert _load_scores("2026-06-18", "2026-06-19", "CN") == {}


# ---------------------------------------------------------------------------
# _load_delist_dates: empty set, happy, table-missing fail-open
# ---------------------------------------------------------------------------


def test_loader_delist_empty_set_returns_empty() -> None:
    assert _load_delist_dates(set()) == {}


def test_loader_delist_happy_and_unrelated_filtered(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rows = [
        ("600001.SH", _dt.date(2026, 6, 19)),  # date object formatting
        ("600002.SH", "2026-06-20"),  # string formatting
        ("999999.SH", "2026-06-21"),  # not in universe -> filtered
    ]
    monkeypatch.setattr(be, "get_connection", lambda: _fixed(rows))
    out = _load_delist_dates({"600001.SH", "600002.SH"})
    assert out == {"600001.SH": "2026-06-19", "600002.SH": "2026-06-20"}


def test_loader_delist_table_missing_fail_open(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(be, "get_connection", lambda: BoomConn())
    assert _load_delist_dates({"600001.SH"}) == {}


# ---------------------------------------------------------------------------
# _last_close_before: break branch + None edges
# ---------------------------------------------------------------------------


def test_loader_last_close_before_break_and_edges() -> None:
    data = _data(
        ["2026-06-18", "2026-06-19", "2026-06-22"],
        {},
        {TS1: {"2026-06-18": 10.0, "2026-06-19": 11.0, "2026-06-22": 12.0}},
    )
    assert _last_close_before(data, TS1, "2026-06-19") == 10.0  # hits break
    assert _last_close_before(data, TS1, "2026-06-23") == 12.0
    assert _last_close_before(data, "NOPE.SH", "2026-06-19") is None
    data.closes_by_ts = {TS1: []}
    assert _last_close_before(data, TS1, "2026-06-19") is None


# ---------------------------------------------------------------------------
# _load_regime_by_day: CN happy, HK, exception
# ---------------------------------------------------------------------------


def test_loader_regime_cn_happy(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(be, "get_index_signals", lambda **kw: [{"name": "x"}])
    monkeypatch.setattr(be, "classify_market_regime", lambda signals: "Strong")
    out = _load_regime_by_day(_cfg(market="CN"), ["2026-06-18", "2026-06-19"])
    assert out == {"2026-06-18": "Strong", "2026-06-19": "Strong"}


def test_loader_regime_hk_branch(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(be, "get_hk_regime", lambda **kw: {"regime": "Weak"})
    out = _load_regime_by_day(_cfg(market="HK"), ["2026-06-18"])
    assert out == {"2026-06-18": "Weak"}


def test_loader_regime_exception_day_absent(monkeypatch: pytest.MonkeyPatch) -> None:
    def _boom(**kw: object) -> list:
        raise RuntimeError("no data")

    monkeypatch.setattr(be, "get_index_signals", _boom)
    out = _load_regime_by_day(_cfg(market="CN"), ["2026-06-18"])
    assert out == {}


# ---------------------------------------------------------------------------
# _load_light_red_days: red, non-red, exception
# ---------------------------------------------------------------------------


def _lights(signals: list[dict]) -> object:
    return lambda **kw: signals  # type: ignore[return-value]


def test_loader_light_red_days_mixed_and_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    red_day = [
        {"name": "\u6caa\u6df1300", "signal": "red"},
        {"name": "\u4e2d\u8bc1500", "signal": "green"},
        {"name": "\u521b\u4e1a\u677f\u6307", "signal": "green"},
    ]
    monkeypatch.setattr(be, "get_index_signals", _lights(red_day))
    assert _load_light_red_days(_cfg(), ["2026-06-18"]) == {"2026-06-18"}
    green_day = [
        {"name": "\u6caa\u6df1300", "signal": "green"},
        {"name": "\u4e2d\u8bc1500", "signal": "green"},
        {"name": "\u521b\u4e1a\u677f\u6307", "signal": "green"},
    ]
    monkeypatch.setattr(be, "get_index_signals", _lights(green_day))
    assert _load_light_red_days(_cfg(), ["2026-06-18"]) == set()

    def _boom(**kw: object) -> list:
        raise RuntimeError("no data")

    monkeypatch.setattr(be, "get_index_signals", _boom)
    assert _load_light_red_days(_cfg(), ["2026-06-18"]) == set()


def test_loader_light_red_ignores_unknown_names(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        be, "get_index_signals", _lights([{"name": "\u4e0a\u8bc1\u6307\u6570", "signal": "red"}])
    )
    assert _load_light_red_days(_cfg(), ["2026-06-18"]) == set()


# ---------------------------------------------------------------------------
# _load_flow_mainline_data: bad inflow, momentum add, empty lookback
# ---------------------------------------------------------------------------


def test_loader_flow_bad_inflow_degrades_to_zero(monkeypatch: pytest.MonkeyPatch) -> None:
    cal = ["2026-06-18"]
    monkeypatch.setattr(be, "get_dates_upto", lambda day, n: [day])
    monkeypatch.setattr(
        be,
        "get_rows_for_dates",
        lambda dates: [
            {"date": "2026-06-18", "industry_name": "A", "net_inflow": "BAD"},
            {"date": "2026-06-18", "industry_name": "B", "net_inflow": 5.0},
            {"date": "2026-06-18", "industry_name": "", "net_inflow": 9.0},
        ],
    )
    monkeypatch.setattr(be, "top_by_date_from_rows", lambda rows, lookback, top_k=3: [])
    cfg = _cfg(mainline_top_k=3)
    any_pos, allow, five = _load_flow_mainline_data(cfg, cal)
    assert any_pos == {"2026-06-18": True}
    assert allow["2026-06-18"] == set()
    assert five["2026-06-18"] == {"A": 0.0, "B": 5.0}


def test_loader_flow_momentum_breakout_adds_allow(monkeypatch: pytest.MonkeyPatch) -> None:
    d1, d2 = "2026-06-18", "2026-06-19"
    cal = [d1, d2]

    def _dates(day: str, n: int) -> list[str]:
        if n == 2:
            # 2-day lookback stub returns the prior session so yesterday[-1]
            # is the previous day (exercises the rank-improvement branch).
            idx = cal.index(day)
            return [cal[idx - 1]] if idx > 0 else [day]
        return [d for d in cal if d <= day][-n:]

    monkeypatch.setattr(be, "get_dates_upto", _dates)
    inds = [f"IND{i}" for i in range(12)]
    rows = []
    for i, name in enumerate(inds):
        rows.append({"date": d1, "industry_name": name, "net_inflow": float(100 - i * 5)})
    # On d2 reverse the order so IND11 jumps from last to first.
    for i, name in enumerate(reversed(inds)):
        rows.append({"date": d2, "industry_name": name, "net_inflow": float(100 - i * 5)})
    monkeypatch.setattr(be, "get_rows_for_dates", lambda dates: rows)
    monkeypatch.setattr(be, "top_by_date_from_rows", lambda rows, lookback, top_k=3: [])
    monkeypatch.setattr(be, "MOMENTUM_THRESHOLD_YI", 1.0)
    monkeypatch.setattr(be, "MOMENTUM_RANK_CHANGE", 5)
    cfg = _cfg(mainline_top_k=3)
    _any_pos, allow, _five = _load_flow_mainline_data(cfg, cal)
    assert "IND11" in allow[d2]


def test_loader_flow_empty_lookback_continue(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(be, "get_dates_upto", lambda day, n: [])
    monkeypatch.setattr(be, "get_rows_for_dates", lambda dates: [])
    monkeypatch.setattr(be, "top_by_date_from_rows", lambda rows, lookback, top_k=3: [])
    _any_pos, _allow, five = _load_flow_mainline_data(_cfg(), ["2026-06-18"])
    assert five == {}


# ---------------------------------------------------------------------------
# _load_industry_data: disabled, happy mom+neutral, thin edges
# ---------------------------------------------------------------------------


def _ind_handler(
    ind_rows: list, daily_rows: list
) -> object:
    def handler(sql: str, params: object) -> list:
        if "stock_eastmoney_industry" in sql:
            return ind_rows
        return daily_rows

    return handler


def test_loader_industry_disabled_returns_empty() -> None:
    cfg = _cfg(ind_mom_days=0, ind_neutral_days=0)
    assert _load_industry_data(cfg, ["2026-06-18"], {TS1}) == ({}, {})


def test_loader_industry_happy_mom_and_neutral(monkeypatch: pytest.MonkeyPatch) -> None:
    d1, d2 = "2026-06-18", "2026-06-19"
    industries = [f"IND{i}" for i in range(6)]
    codes = [f"{i + 1:06d}.SH" for i in range(60)]
    ind_rows = [(ts, industries[i % 6]) for i, ts in enumerate(codes)]
    ind_rows.append((None, None))  # falsy row skipped
    daily_rows = [(d1, ts, float(i)) for i, ts in enumerate(codes)]
    daily_rows.append((d1, codes[0], None))  # None ret skipped
    daily_rows.append((d1, "999999.SH", 999.0))  # no industry mapping skipped
    monkeypatch.setattr(
        be, "get_connection", lambda: FakeConn(_ind_handler(ind_rows, daily_rows))
    )
    cfg = _cfg(
        start_date=d1, end_date=d2, ind_mom_days=20, ind_neutral_days=20
    )
    universe = set(codes[:10])
    ind_rank, within = _load_industry_data(cfg, [d1, d2], universe)
    assert set(ind_rank[d1].keys()) == set(industries)
    assert ind_rank[d1][industries[5]] == pytest.approx(1.0)  # strongest avg
    assert set(within[d1].keys()) <= universe
    assert len(within[d1]) == 10
    assert d2 not in ind_rank  # thin day skipped


def test_loader_industry_thin_market_skips(monkeypatch: pytest.MonkeyPatch) -> None:
    d1 = "2026-06-18"
    codes = [f"{i + 1:06d}.SH" for i in range(10)]
    ind_rows = [(ts, "IND0") for ts in codes]
    daily_rows = [(d1, ts, float(i)) for i, ts in enumerate(codes)]
    monkeypatch.setattr(
        be, "get_connection", lambda: FakeConn(_ind_handler(ind_rows, daily_rows))
    )
    cfg = _cfg(start_date=d1, end_date=d1, ind_mom_days=20, ind_neutral_days=20)
    ind_rank, within = _load_industry_data(cfg, [d1], set(codes))
    assert ind_rank == {} and within == {}


def test_loader_industry_few_industries_skips_mom(monkeypatch: pytest.MonkeyPatch) -> None:
    d1 = "2026-06-18"
    codes = [f"{i + 1:06d}.SH" for i in range(60)]
    ind_rows = [(ts, f"IND{i % 3}") for i, ts in enumerate(codes)]
    daily_rows = [(d1, ts, float(i)) for i, ts in enumerate(codes)]
    monkeypatch.setattr(
        be, "get_connection", lambda: FakeConn(_ind_handler(ind_rows, daily_rows))
    )
    cfg = _cfg(start_date=d1, end_date=d1, ind_mom_days=20, ind_neutral_days=0)
    ind_rank, within = _load_industry_data(cfg, [d1], set(codes[:5]))
    assert ind_rank == {} and within == {}


def test_loader_industry_mom_thin_after_min_members(monkeypatch: pytest.MonkeyPatch) -> None:
    """Six industries present but only two have >=3 members -> 1106 continue."""
    d1 = "2026-06-18"
    big = [f"{i + 1:06d}.SH" for i in range(60)]
    small = [f"9{i:05d}.SH" for i in range(4)]
    ind_rows = [(ts, f"IND{i % 2}") for i, ts in enumerate(big)]
    ind_rows += [(ts, f"SMALL{i}") for i, ts in enumerate(small)]
    daily_rows = [(d1, ts, float(i)) for i, ts in enumerate(big + small)]
    monkeypatch.setattr(
        be, "get_connection", lambda: FakeConn(_ind_handler(ind_rows, daily_rows))
    )
    cfg = _cfg(start_date=d1, end_date=d1, ind_mom_days=20, ind_neutral_days=0)
    ind_rank, within = _load_industry_data(cfg, [d1], set(big[:5]))
    assert ind_rank == {} and within == {}


def test_loader_industry_small_within_group_skipped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A 2-member industry hits the 1121 continue; larger groups still rank."""
    d1 = "2026-06-18"
    codes = [f"{i + 1:06d}.SH" for i in range(50)]
    tiny = ["900001.SH", "900002.SH"]
    ind_rows = [(ts, f"IND{i % 5}") for i, ts in enumerate(codes)]
    ind_rows += [(ts, "TINY") for ts in tiny]
    daily_rows = [(d1, ts, float(i)) for i, ts in enumerate(codes + tiny)]
    monkeypatch.setattr(
        be, "get_connection", lambda: FakeConn(_ind_handler(ind_rows, daily_rows))
    )
    cfg = _cfg(start_date=d1, end_date=d1, ind_mom_days=0, ind_neutral_days=20)
    _ind_rank, within = _load_industry_data(cfg, [d1], set(codes[:5] + tiny))
    assert d1 in within
    assert "900001.SH" not in within[d1]  # tiny group skipped
    assert any(ts in within[d1] for ts in codes[:5])


# ---------------------------------------------------------------------------
# _load_avg_amount: disabled, happy, universe filter + thin symbol
# ---------------------------------------------------------------------------


def test_loader_avg_amount_disabled_returns_empty() -> None:
    assert _load_avg_amount(_cfg(min_avg_amount=0.0), ["2026-06-18"], {TS1}) == {}


def test_loader_avg_amount_happy_and_thin(monkeypatch: pytest.MonkeyPatch) -> None:
    hist = _days("2026-05-01", 35)
    cal = [hist[-2], hist[-1]]
    rows = [(d, TS1, 1000000.0) for d in hist]  # 35 sessions -> >=30 kept
    rows += [(d, TS2, 1000000.0) for d in hist[-5:]]  # 5 sessions -> too thin
    rows.append((hist[-1], "999999.SH", 1000000.0))  # outside universe skipped
    monkeypatch.setattr(be, "get_connection", lambda: _fixed(rows))
    out = _load_avg_amount(_cfg(min_avg_amount=1.0), cal, {TS1, TS2})
    assert out[cal[-1]][TS1] == pytest.approx(10.0)
    assert TS2 not in out.get(cal[-1], {})
    assert cal[-1] in out


def test_loader_avg_amount_window_pop_oldest(monkeypatch: pytest.MonkeyPatch) -> None:
    """65 sessions overflow the 60-window deque (1187 pop branch)."""
    hist = _days("2026-03-01", 65)
    cal = [hist[-2], hist[-1]]
    rows = [(d, TS1, 100000.0) for d in hist[:-1]] + [(hist[-1], TS1, 700000.0)]
    monkeypatch.setattr(be, "get_connection", lambda: _fixed(rows))
    out = _load_avg_amount(_cfg(min_avg_amount=1.0), cal, {TS1})
    expect = (59 * 100000.0 + 700000.0) / 60 / 100000.0
    assert out[cal[-1]][TS1] == pytest.approx(round(expect, 4))


# ---------------------------------------------------------------------------
# _load_mom_ranks: disabled, happy, thin day
# ---------------------------------------------------------------------------


def test_loader_mom_disabled_returns_empty() -> None:
    assert _load_mom_ranks(_cfg(mom_ret_days=0), ["2026-06-18"], {TS1}) == {}


def test_loader_mom_happy_and_thin(monkeypatch: pytest.MonkeyPatch) -> None:
    d1, d2 = "2026-06-18", "2026-06-19"
    codes = [f"{i + 1:06d}.SH" for i in range(35)]
    rows = [(d1, ts, float(i)) for i, ts in enumerate(codes)]
    rows.append((d1, "999999.SH", 9999.0))  # ranking pool only, not universe
    rows += [(d2, codes[i], float(i)) for i in range(5)]  # thin day skipped
    monkeypatch.setattr(be, "get_connection", lambda: _fixed(rows))
    cfg = _cfg(mom_ret_days=60)
    out = _load_mom_ranks(cfg, [d1, d2], set(codes[:5]))
    assert d1 in out and d2 not in out
    assert "999999.SH" not in out[d1]
    assert set(out[d1].keys()) == set(codes[:5])


# ---------------------------------------------------------------------------
# _load_rs_ranks: disabled, happy, thin + missing bench
# ---------------------------------------------------------------------------


def _rs_handler(ret_rows: list, bench_rows: list) -> object:
    def handler(sql: str, params: object) -> list:
        if "ret20" in sql:
            return ret_rows
        return bench_rows

    return handler


def test_loader_rs_disabled_returns_empty() -> None:
    assert _load_rs_ranks(_cfg(rs_rank_min=0.0), ["2026-06-18"], {TS1}) == {}


def test_loader_rs_happy_thin_and_missing_bench(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(be, "RS_LOOKBACK_DAYS", 2)
    b0, b1, b2, b3 = "2026-06-16", "2026-06-17", "2026-06-18", "2026-06-19"
    missing = "2026-06-22"
    codes = [f"{i + 1:06d}.SH" for i in range(35)]
    ret_rows = [(b3, ts, float(i)) for i, ts in enumerate(codes)]
    ret_rows.append((b3, "999999.SH", 9999.0))
    ret_rows += [(b2, codes[i], float(i)) for i in range(5)]  # thin day
    ret_rows += [(missing, codes[i], float(i)) for i in range(35)]  # no bench
    bench_rows = [(b0, 100.0), (b1, 101.0), (b2, 102.0), (b3, 103.0)]
    monkeypatch.setattr(
        be, "get_connection", lambda: FakeConn(_rs_handler(ret_rows, bench_rows))
    )
    cfg = _cfg(start_date=b0, end_date=missing, rs_rank_min=0.5)
    out = _load_rs_ranks(cfg, [b3, b2, missing], set(codes[:4]))
    assert b3 in out
    assert b2 not in out  # thin market skipped
    assert missing not in out  # bench missing skipped
    assert set(out[b3].keys()) == set(codes[:4])


def test_loader_rs_day_without_rows_skipped(monkeypatch: pytest.MonkeyPatch) -> None:
    """Calendar day with zero ret rows hits the 1329 continue."""
    monkeypatch.setattr(be, "RS_LOOKBACK_DAYS", 2)
    b0, b1, b2, b3 = "2026-06-16", "2026-06-17", "2026-06-18", "2026-06-19"
    empty = "2026-06-20"
    codes = [f"{i + 1:06d}.SH" for i in range(35)]
    ret_rows = [(b3, ts, float(i)) for i, ts in enumerate(codes)]
    bench_rows = [(b0, 100.0), (b1, 101.0), (b2, 102.0), (b3, 103.0)]
    monkeypatch.setattr(
        be, "get_connection", lambda: FakeConn(_rs_handler(ret_rows, bench_rows))
    )
    cfg = _cfg(start_date=b0, end_date=empty, rs_rank_min=0.5)
    out = _load_rs_ranks(cfg, [b3, empty], set(codes[:4]))
    assert b3 in out and empty not in out


# ---------------------------------------------------------------------------
# _trend_score: none, alignment states, distance bands
# ---------------------------------------------------------------------------


def _closes_flat(n: int, px: float, day0: str = "2026-01-02") -> list[tuple[str, float]]:
    base = _dt.date.fromisoformat(day0)
    return [((base + _dt.timedelta(days=i)).isoformat(), px) for i in range(n)]


def test_loader_trend_score_no_closes_and_short_history() -> None:
    assert _trend_score(0.9, None, "2026-06-18") is None
    assert _trend_score(0.9, [], "2026-06-18") is None
    assert _trend_score(0.9, _closes_flat(10, 10.0), "2026-06-19") is None


def test_loader_trend_score_full_alignment_near_high() -> None:
    closes = [(f"2026-01-{i + 1:02d}" if i < 30 else f"2026-02-{(i - 30) + 1:02d}", 10.0 + i * 0.1) for i in range(70)]
    asof = closes[-1][0]
    score = _trend_score(0.9, closes, asof)
    assert score == pytest.approx(0.9 * 40.0 + 30.0 + 30.0)


def test_loader_trend_score_partial_alignment() -> None:
    base = _dt.date(2026, 1, 1)
    closes = [((base + _dt.timedelta(days=i)).isoformat(), 10.0) for i in range(70)]
    for i in range(55, 60):  # spike in the middle -> MA20 > MA60 only
        closes[i] = (closes[i][0], 20.0)
    score = _trend_score(0.9, closes, closes[-1][0])
    # partial mult 0.5, deep drawdown from spike -> distance band 0
    assert score == pytest.approx(0.9 * 40.0 + 15.0 + 0.0)


def test_loader_trend_score_no_alignment() -> None:
    closes = _closes_flat(70, 10.0)
    score = _trend_score(0.9, closes, closes[-1][0])
    assert score == pytest.approx(0.9 * 40.0 + 0.0 + 30.0)


def test_loader_trend_score_distance_bands() -> None:
    base = _dt.date(2026, 1, 1)

    def _with_spike(spike: float) -> list[tuple[str, float]]:
        # Spike placed outside the MA60 window (index 5 of 70 bars) so the
        # MA-alignment mult stays 0.0 and only the distance band varies.
        closes = [((base + _dt.timedelta(days=i)).isoformat(), 10.0) for i in range(70)]
        closes[5] = (closes[5][0], spike)
        return closes

    asof = (base + _dt.timedelta(days=69)).isoformat()
    assert _trend_score(None, _with_spike(10.3), asof) == pytest.approx(0.0 + 0.0 + 30.0)
    assert _trend_score(None, _with_spike(10.75), asof) == pytest.approx(20.0)
    assert _trend_score(None, _with_spike(11.36), asof) == pytest.approx(10.0)
    assert _trend_score(None, _with_spike(20.0), asof) == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# _load_sentiment_risk / _load_market_caps / _load_industries / _load_st_names
# ---------------------------------------------------------------------------


def test_loader_sentiment_happy_and_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    rows = [(_dt.date(2026, 6, 18), "extreme_caution"), ("2026-06-19", None)]
    monkeypatch.setattr(be, "get_connection", lambda: _fixed(rows))
    out = _load_sentiment_risk(_cfg())
    assert out == {"2026-06-18": "extreme_caution", "2026-06-19": ""}
    monkeypatch.setattr(be, "get_connection", lambda: _fixed([]))
    assert _load_sentiment_risk(_cfg()) == {}


def test_loader_market_caps_empty_and_happy(monkeypatch: pytest.MonkeyPatch) -> None:
    assert _load_market_caps(_cfg(), set()) == {}
    rows = [(_dt.date(2026, 6, 18), TS1, 500000.0), ("2026-06-18", TS2, 250000.0)]
    monkeypatch.setattr(be, "get_connection", lambda: _fixed(rows))
    out = _load_market_caps(_cfg(), {TS1, TS2})
    assert out == {"2026-06-18": {TS1: 50.0, TS2: 25.0}}


def test_loader_industries_empty_and_filtered(monkeypatch: pytest.MonkeyPatch) -> None:
    assert _load_industries([]) == {}
    rows = [(TS1, "\u8ba1\u7b97\u673a"), (None, "X"), ("", ""), (TS2, None)]
    monkeypatch.setattr(be, "get_connection", lambda: _fixed(rows))
    assert _load_industries([TS1, TS2]) == {TS1: "\u8ba1\u7b97\u673a"}


def test_loader_st_names_happy_and_fail_open(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(be, "get_connection", lambda: _fixed([(TS1,), (None,), ("",)]))
    assert _load_st_names() == {TS1}
    monkeypatch.setattr(be, "get_connection", lambda: BoomConn())
    assert _load_st_names() == set()


# ---------------------------------------------------------------------------
# simulate() branches
# ---------------------------------------------------------------------------


def _sim_cfg(calendar: list[str], **kw: object) -> BacktestConfig:
    base: dict[str, object] = {
        "start_date": calendar[0],
        "end_date": calendar[-1],
        "gates": "none",
        "score_threshold": 65.0,
    }
    base.update(kw)
    return BacktestConfig(**base)  # type: ignore[arg-type]


def test_simflag_time_stop_cuts_underwater_holding() -> None:
    calendar = ["2026-06-18", "2026-06-19", "2026-06-22", "2026-06-23"]
    scores = {"2026-06-18": {CN1: 90.0}}
    prices = {
        TS1: {
            "2026-06-18": 10.0,
            "2026-06-19": 9.9,
            "2026-06-22": 9.8,
            "2026-06-23": 9.8,
        }
    }
    data = _data(calendar, scores, prices)
    run = simulate(
        _sim_cfg(
            calendar,
            stop_loss_pct=-15.0,
            target_pnl_pct=30.0,
            max_hold_days=20,
            trailing_stop_pct=0.0,
            max_hold_unprofitable_days=2,
        ),
        data=data,
    )
    assert run.summary.closed == 1
    assert run.trades[0].close_reason == "time_stop"
    assert run.trades[0].close_date == "2026-06-22"


def test_simflag_profit_trail_unarmed_holds() -> None:
    calendar = ["2026-06-18", "2026-06-19", "2026-06-22"]
    scores = {"2026-06-18": {CN1: 90.0}}
    prices = {TS1: {"2026-06-18": 10.0, "2026-06-19": 10.5, "2026-06-22": 10.2}}
    data = _data(calendar, scores, prices)
    run = simulate(
        _sim_cfg(
            calendar,
            stop_loss_pct=-15.0,
            target_pnl_pct=30.0,
            max_hold_days=10,
            trailing_stop_pct=-8.0,
            profit_trail_trigger_pct=10.0,
            profit_trail_pct=-6.0,
        ),
        data=data,
    )
    assert run.trades[0].close_reason == "end_of_window"


def test_simflag_profit_trail_armed_small_pullback_holds() -> None:
    calendar = ["2026-06-18", "2026-06-19", "2026-06-22", "2026-06-23"]
    scores = {"2026-06-18": {CN1: 90.0}}
    prices = {
        TS1: {
            "2026-06-18": 10.0,
            "2026-06-19": 11.0,
            "2026-06-22": 12.5,
            "2026-06-23": 12.25,  # -2% from peak, tighter than -6% trail
        }
    }
    data = _data(calendar, scores, prices)
    run = simulate(
        _sim_cfg(
            calendar,
            stop_loss_pct=-15.0,
            target_pnl_pct=30.0,
            max_hold_days=10,
            trailing_stop_pct=-8.0,
            profit_trail_trigger_pct=10.0,
            profit_trail_pct=-6.0,
        ),
        data=data,
    )
    assert run.trades[0].close_reason == "end_of_window"


def test_simflag_flow_streak_none_resets_then_exits() -> None:
    calendar = ["2026-06-18", "2026-06-19", "2026-06-22", "2026-06-23", "2026-06-24"]
    scores = {"2026-06-18": {CN1: 90.0}}
    prices = {TS1: {d: 10.0 for d in calendar}}
    data = _data(calendar, scores, prices, industry_by_ts={TS1: "\u901a\u4fe1"})
    data.flow5d_by_day = {
        "2026-06-18": {"\u901a\u4fe1": -1.0},
        # 2026-06-19 missing -> v is None -> streak reset
        "2026-06-22": {"\u901a\u4fe1": -1.0},
        "2026-06-23": {"\u901a\u4fe1": -1.0},
        "2026-06-24": {"\u901a\u4fe1": -1.0},
    }
    run = simulate(
        _sim_cfg(
            calendar,
            stop_loss_pct=-15.0,
            target_pnl_pct=30.0,
            max_hold_days=20,
            industry_flow_exit_days=3,
        ),
        data=data,
    )
    assert run.trades[0].close_reason == "flow_exit"
    assert run.trades[0].close_date == "2026-06-24"


def test_simflag_flow_streak_positive_break_holds() -> None:
    calendar = ["2026-06-18", "2026-06-19", "2026-06-22", "2026-06-23", "2026-06-24"]
    scores = {"2026-06-18": {CN1: 90.0}}
    prices = {TS1: {d: 10.0 for d in calendar}}
    data = _data(calendar, scores, prices, industry_by_ts={TS1: "\u901a\u4fe1"})
    data.flow5d_by_day = {
        "2026-06-18": {"\u901a\u4fe1": -1.0},
        "2026-06-19": {"\u901a\u4fe1": -1.0},
        "2026-06-22": {"\u901a\u4fe1": 5.0},  # positive breaks the streak
        "2026-06-23": {"\u901a\u4fe1": -1.0},
        "2026-06-24": {"\u901a\u4fe1": -1.0},
    }
    run = simulate(
        _sim_cfg(
            calendar,
            stop_loss_pct=-15.0,
            target_pnl_pct=30.0,
            max_hold_days=20,
            industry_flow_exit_days=3,
        ),
        data=data,
    )
    assert run.trades[0].close_reason == "end_of_window"


def test_simflag_cash_cap_pyramid_blocked() -> None:
    calendar = ["2026-06-18", "2026-06-19", "2026-06-22"]
    scores = {"2026-06-18": {CN1: 90.0}}
    prices = {TS1: {"2026-06-18": 10.0, "2026-06-19": 10.6, "2026-06-22": 10.6}}
    data = _data(calendar, scores, prices)
    run = simulate(
        _sim_cfg(
            calendar,
            stop_loss_pct=-15.0,
            target_pnl_pct=100.0,
            max_hold_days=60,
            trailing_stop_pct=0.0,
            position_pct=0.8,
            pyramid_trigger_pct=5.0,
            pyramid_add_scale=0.5,
            pyramid_max_adds=1,
        ),
        data=data,
    )
    assert run.summary.gated_blocks.get("cash_cap_pyramid", 0) >= 1
    assert run.summary.closed == 1  # only the main leg, no add leg


def test_simflag_window_end_settles_pyramid_adds() -> None:
    calendar = ["2026-06-18", "2026-06-19", "2026-06-22", "2026-06-23"]
    scores = {"2026-06-18": {CN1: 90.0}}
    prices = {
        TS1: {
            "2026-06-18": 10.0,
            "2026-06-19": 10.6,  # +6% triggers one pyramid add
            "2026-06-22": 10.6,
            "2026-06-23": 10.6,
        }
    }
    data = _data(calendar, scores, prices)
    run = simulate(
        _sim_cfg(
            calendar,
            stop_loss_pct=-15.0,
            target_pnl_pct=100.0,
            max_hold_days=60,
            trailing_stop_pct=0.0,
            pyramid_trigger_pct=5.0,
            pyramid_add_scale=0.5,
            pyramid_max_adds=1,
        ),
        data=data,
    )
    assert len(run.trades) == 2
    assert {t.close_reason for t in run.trades} == {"end_of_window"}
    entries = sorted(t.entry_date for t in run.trades)
    assert entries == ["2026-06-18", "2026-06-19"]


def test_simflag_window_end_unpriceable_holds_open() -> None:
    calendar = ["2026-06-18", "2026-06-19"]
    scores = {"2026-06-18": {CN1: 90.0}}
    prices = {TS1: {"2026-06-18": 10.0, "2026-06-19": 0.0}}
    data = _data(calendar, scores, prices)
    run = simulate(
        _sim_cfg(
            calendar,
            stop_loss_pct=-15.0,
            target_pnl_pct=100.0,
            max_hold_days=60,
            trailing_stop_pct=0.0,
        ),
        data=data,
    )
    assert run.summary.closed == 0
    assert run.summary.open_at_end == 1
