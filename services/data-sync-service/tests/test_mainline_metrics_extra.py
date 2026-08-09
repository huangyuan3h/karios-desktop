"""mainline metrics computation coverage (industry metrics for date)."""

from __future__ import annotations

from datetime import date

from data_sync_service.service import mainline as ml


def _row(ts, pre, close, pct, name=None, industry=None) -> dict:
    return {"ts_code": ts, "pre_close": pre, "close": close, "pct_chg": pct,
            "name": name, "industry": industry}


def test_fetch_daily_rows_by_dates_empty(monkeypatch) -> None:
    monkeypatch.setattr(ml, "ensure_daily", lambda: None)
    monkeypatch.setattr(ml, "ensure_stock_basic", lambda: None)
    assert ml._fetch_daily_rows_by_dates([]) == {}


def test_fetch_daily_rows_by_dates_groups(monkeypatch) -> None:
    from datetime import datetime

    rows = [
        (datetime(2026, 8, 7), "600000.SH", 10.0, 11.0, 10.0, "浦发", "银行"),
        ("2026-08-06", "000001.SZ", 5.0, 5.5, 10.0, None, None),
        (datetime(2026, 8, 7), "300001.SZ", 1.0, 1.2, 20.0, "特锐德", "电气"),
    ]
    cur = _FakeCur(rows)
    monkeypatch.setattr(ml, "ensure_daily", lambda: None)
    monkeypatch.setattr(ml, "ensure_stock_basic", lambda: None)
    monkeypatch.setattr(ml, "get_connection", lambda: _FakeConn(cur))

    out = ml._fetch_daily_rows_by_dates(["2026-08-06", "2026-08-07"])
    assert set(out) == {"2026-08-06", "2026-08-07"}
    assert out["2026-08-07"][0]["industry"] == "银行"
    assert out["2026-08-06"][0]["name"] is None and out["2026-08-06"][0]["industry"] is None
    assert cur.params[0] == (["2026-08-06", "2026-08-07"],)


class _FakeCur:
    def __init__(self, rows) -> None:
        self._rows = rows
        self.params = []

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return None

    def execute(self, sql, params=None):
        self.params.append(params)
        return self

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._rows[0] if self._rows else None


class _FakeConn:
    def __init__(self, cur) -> None:
        self._cur = cur

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return None

    def cursor(self):
        return self._cur

    def commit(self) -> None:
        pass


def test_prev_open_date_via_calendar(monkeypatch) -> None:
    monkeypatch.setattr(ml, "is_trading_day", lambda exc, d: True)
    monkeypatch.setattr(
        ml, "get_open_dates",
        lambda exchange, start_date, end_date: [date(2026, 8, 5), date(2026, 8, 6), date(2026, 8, 7)],
    )
    assert ml._prev_open_date("SSE", date(2026, 8, 7)) == date(2026, 8, 6)


def test_prev_open_date_fallback_to_daily_table(monkeypatch) -> None:
    monkeypatch.setattr(ml, "is_trading_day", lambda exc, d: None)
    monkeypatch.setattr(ml, "ensure_daily", lambda: None)
    cur = _FakeCur([(date(2026, 8, 6),)])
    monkeypatch.setattr(ml, "get_connection", lambda: _FakeConn(cur))
    assert ml._prev_open_date("SSE", date(2026, 8, 7)) == date(2026, 8, 6)


def test_prev_open_date_no_prev(monkeypatch) -> None:
    monkeypatch.setattr(ml, "is_trading_day", lambda exc, d: None)
    monkeypatch.setattr(ml, "ensure_daily", lambda: None)
    cur = _FakeCur([(None,)])
    monkeypatch.setattr(ml, "get_connection", lambda: _FakeConn(cur))
    assert ml._prev_open_date("SSE", date(2026, 8, 7)) is None


def test_compute_industry_metrics_full(monkeypatch) -> None:
    monkeypatch.setattr(ml, "_prev_open_date", lambda exc, d: date(2026, 8, 6))
    monkeypatch.setattr(
        ml,
        "_fetch_daily_rows_by_dates",
        lambda dates: {
            "2026-08-07": [
                _row("600000.SH", 10.0, 11.0, 10.0, "浦发", "银行"),   # limit up
                _row("600001.SH", 10.0, 10.5, 5.0, "工行", "银行"),    # normal
                _row("300001.SZ", 1.0, 1.21, 21.0, "特锐德", "电气"),  # surge
                _row("300002.SZ", 1.0, 1.05, 5.0, "x", ""),           # no industry
            ],
            "2026-08-06": [
                _row("600000.SH", 9.0, 10.0, 11.0, "浦发", "银行"),   # limit up prev
                _row("600001.SH", 10.0, 10.0, 0.0, "工行", "银行"),
            ],
        },
    )
    out = ml._compute_industry_metrics_for_date("2026-08-07")
    bank = next(r for r in out if r["industry_name"] == "银行")
    elec = next(r for r in out if r["industry_name"] == "电气")
    assert bank["total_count"] == 2
    assert bank["limit_up_count"] == 1
    assert bank["limit_up_2d_count"] == 1  # 600000 limit up both days
    assert bank["avg_pct"] == 7.5
    assert bank["surge_count"] == 1  # 600000 pct 10.0 > 5.0
    assert bank["surge_ratio"] == 0.5
    assert bank["raw"] == {"prevDate": "2026-08-06"}
    assert elec["surge_count"] == 1
    assert elec["surge_ratio"] == 1.0
    assert len(out) == 2  # no-industry row excluded


def test_compute_industry_metrics_limit_by_pct_threshold(monkeypatch) -> None:
    monkeypatch.setattr(ml, "_prev_open_date", lambda exc, d: None)
    monkeypatch.setattr(
        ml,
        "_fetch_daily_rows_by_dates",
        lambda dates: {
            "2026-08-07": [_row("000002.SZ", 1.0, 1.098, 9.9, "万科", "地产")],
        },
    )
    out = ml._compute_industry_metrics_for_date("2026-08-07")
    assert out[0]["limit_up_count"] == 1  # pct >= 9.8 counts even if price not exact
    assert out[0]["raw"]["prevDate"] is None


def test_ensure_metrics_for_dates(monkeypatch) -> None:
    computed: list[str] = []
    upserted: list[list] = []

    def fake_rows_by_date(d):
        return [{"date": d}] if d == "2026-08-06" else []

    monkeypatch.setattr(ml, "metrics_rows_by_date", fake_rows_by_date)
    monkeypatch.setattr(ml, "_compute_industry_metrics_for_date", lambda d: computed.append(d) or [{"a": 1}])
    monkeypatch.setattr(ml, "metrics_upsert_rows", lambda rows: upserted.append(rows))
    out = ml.ensure_metrics_for_dates(["2026-08-06", "2026-08-07"])
    assert out == {"ensured": 1}
    assert computed == ["2026-08-07"]
    assert upserted == [[{"a": 1}]]


def test_ensure_metrics_empty_rows_no_upsert(monkeypatch) -> None:
    monkeypatch.setattr(ml, "metrics_rows_by_date", lambda d: [])
    monkeypatch.setattr(ml, "_compute_industry_metrics_for_date", lambda d: [])
    monkeypatch.setattr(ml, "metrics_upsert_rows", lambda rows: (_ for _ in ()).throw(AssertionError("no upsert")))
    assert ml.ensure_metrics_for_dates(["2026-08-07"]) == {"ensured": 0}


def test_ensure_scores_for_dates(monkeypatch) -> None:
    upserted: list[list] = []
    monkeypatch.setattr(ml, "scores_rows_by_date", lambda d: [{"x": 1}] if d == "2026-08-06" else [])
    monkeypatch.setattr(ml, "compute_scores_for_date", lambda d: [{"s": 1}])
    monkeypatch.setattr(ml, "scores_upsert_rows", lambda rows: upserted.append(rows))
    out = ml.ensure_scores_for_dates(["2026-08-06", "2026-08-07"])
    assert out == {"ensured": 1}
    assert upserted == [[{"s": 1}]]


def test_score_trend_short_series() -> None:
    ctx = {"dates": ["2026-08-01"], "series": {"银行": [("2026-08-01", 1.0)]}, "market_avg_close": {}}
    score, flags = ml._score_trend("银行", ctx)
    assert score == 0.0
    assert flags["rpsQualified"] is False


def test_get_cn_industry_mainline_scores_not_ready(monkeypatch) -> None:
    monkeypatch.setattr(ml, "flow_latest_date", lambda: "2026-08-07")
    monkeypatch.setattr(ml, "_trade_dates_upto", lambda d, days: [d])
    monkeypatch.setattr(ml, "scores_rows_by_date", lambda d: [])
    out = ml.get_cn_industry_mainline()
    assert out["warning"] == "scores_not_ready"
    assert out["asOfDate"] == "2026-08-07"


def test_get_cn_industry_mainline_no_date(monkeypatch) -> None:
    monkeypatch.setattr(ml, "flow_latest_date", lambda: "")
    out = ml.get_cn_industry_mainline(as_of_date="  ")
    assert out["asOfDate"] == "" and out["dates"] == []
