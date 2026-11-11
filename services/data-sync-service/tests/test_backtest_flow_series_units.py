"""TIP-017 flow-series units: tushare hsgt money is 百万元, not 万元.

``cn_moneyflow_hsgt.north_money`` (and hgt/sgt) are published in 百万元, so a
20-session cumulative must divide by 1e2 to land in 亿元. The 2024-04-26
single-day record (22448.99 → 224.49 亿元) is the canonical regression case.
"""

from __future__ import annotations

from data_sync_service.api import backtest_routes as br


def test_hsgt_money_to_yi_record_day() -> None:
    assert br._hsgt_money_to_yi(22448.99) == 224.5


def test_hsgt_money_to_yi_negative() -> None:
    assert br._hsgt_money_to_yi(-6774.99) == -67.7


def test_hsgt_money_to_yi_zero() -> None:
    assert br._hsgt_money_to_yi(0.0) == 0.0


class _FakeCursor:
    def __init__(self, hsgt: list[tuple[str, float]]) -> None:
        self._hsgt = hsgt
        self._rows: list[tuple] = []

    def __enter__(self):
        return self

    def __exit__(self, *exc) -> bool:
        return False

    def execute(self, sql: str, params=None) -> None:
        flat = " ".join(sql.split())
        if "COUNT(DISTINCT exchange_id) < 3" in flat:
            self._rows = []
        elif "SUM(rzye) FROM cn_margin_total" in flat:
            self._rows = []
        elif "north_money FROM cn_moneyflow_hsgt" in flat:
            self._rows = list(self._hsgt)
        elif "FROM cn_flow_daily" in flat:
            self._rows = []
        elif "FROM daily WHERE ts_code = ANY" in flat:
            self._rows = []
        else:  # pragma: no cover - unexpected SQL surfaces as empty rows
            self._rows = []

    def fetchall(self):
        return list(self._rows)


class _FakeConn:
    def __init__(self, hsgt: list[tuple[str, float]]) -> None:
        self._hsgt = hsgt

    def __enter__(self):
        return self

    def __exit__(self, *exc) -> bool:
        return False

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self._hsgt)


def _patch_flow_deps(monkeypatch, hsgt: list[tuple[str, float]]) -> None:
    from data_sync_service import db
    from data_sync_service.service import risk_state_gate as rsg

    monkeypatch.setattr(db, "get_connection", lambda: _FakeConn(hsgt))
    monkeypatch.setattr(rsg, "load_etf_share_series", lambda *a, **kw: {})
    monkeypatch.setattr(rsg, "load_close_series", lambda *a, **kw: [])
    monkeypatch.setattr(rsg, "national_team_state_by_day", lambda *a, **kw: {})


def test_load_flow_rows_north_daily_and_d20_are_yi(monkeypatch) -> None:
    """20 pre-break sessions of 100 百万元 → 1.0 亿元/day, 20.0 亿元 cumulative."""
    days = [
        "2024-04-01",
        "2024-04-02",
        "2024-04-03",
        "2024-04-08",
        "2024-04-09",
        "2024-04-10",
        "2024-04-11",
        "2024-04-12",
        "2024-04-15",
        "2024-04-16",
        "2024-04-17",
        "2024-04-18",
        "2024-04-19",
        "2024-04-22",
        "2024-04-23",
        "2024-04-24",
        "2024-04-25",
        "2024-04-26",
        "2024-04-29",
        "2024-04-30",
    ]
    hsgt = [(d, 100.0) for d in days]
    hsgt[17] = ("2024-04-26", 22448.99)  # record day replaces the 100
    _patch_flow_deps(monkeypatch, hsgt)

    rows = br._load_flow_rows("2024-04-01", "2024-04-30")
    by_date = {r["date"]: r for r in rows}

    assert len(rows) == 20
    assert by_date["2024-04-26"]["northDailyYi"] == 224.5
    assert by_date["2024-04-01"]["northDailyYi"] == 1.0
    # First emitted 20-session window (2024-04-30) = 19×1.0 + 224.5.
    assert by_date["2024-04-30"]["northD20Yi"] == 243.5
    assert by_date["2024-04-01"]["northIsTurnover"] is False


def test_load_flow_rows_north_d20_all_hundreds(monkeypatch) -> None:
    days = [
        "2024-04-01",
        "2024-04-02",
        "2024-04-03",
        "2024-04-08",
        "2024-04-09",
        "2024-04-10",
        "2024-04-11",
        "2024-04-12",
        "2024-04-15",
        "2024-04-16",
        "2024-04-17",
        "2024-04-18",
        "2024-04-19",
        "2024-04-22",
        "2024-04-23",
        "2024-04-24",
        "2024-04-25",
        "2024-04-26",
        "2024-04-29",
        "2024-04-30",
    ]
    _patch_flow_deps(monkeypatch, [(d, 100.0) for d in days])

    rows = br._load_flow_rows("2024-04-01", "2024-04-30")
    assert rows[-1]["date"] == "2024-04-30"
    assert rows[-1]["northDailyYi"] == 1.0
    assert rows[-1]["northD20Yi"] == 20.0
