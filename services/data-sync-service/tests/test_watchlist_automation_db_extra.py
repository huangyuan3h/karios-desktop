"""db/watchlist_automation.py coverage with fake connection."""

from __future__ import annotations

from datetime import datetime

from data_sync_service.db import watchlist_automation as wa

RUN_COLS = ["id", "trade_date", "trigger_type", "skipped", "skip_reason",
            "remove_items", "alpha_add", "meta", "created_at", "applied_at", "screener_added"]


class _Cur:
    def __init__(self, rows=None) -> None:
        self._rows = rows or []
        self.rowcount = len(self._rows)
        self.executed: list[tuple] = []
        self.fetchone_calls = 0

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return None

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        return self

    def executemany(self, sql, rows):
        self.executed.append((sql, rows))
        return self

    def fetchone(self):
        self.fetchone_calls += 1
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return self._rows


class _Conn:
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


def _patch(monkeypatch, rows=None):
    cur = _Cur(rows)
    monkeypatch.setattr(wa, "ensure_tables", lambda: None)
    monkeypatch.setattr(wa, "get_connection", lambda: _Conn(cur))
    return cur


def _run_row(meta: dict | None = None, applied: datetime | None = None, screener_added=3) -> tuple:
    return (
        "run-1", "2026-08-07", "scheduled", False, None,
        [{"symbol": "CN:600000"}], [{"symbol": "CN:600519"}], meta,
        datetime(2026, 8, 7, 9, 0, 0), applied, screener_added,
    )


# ---- registry --------------------------------------------------------------

def test_upsert_registry_empty_clears(monkeypatch) -> None:
    cur = _patch(monkeypatch)
    assert wa.upsert_registry([]) == 0
    assert "DELETE FROM" in cur.executed[0][0]


def test_upsert_registry_items(monkeypatch) -> None:
    import data_sync_service.service.market_quotes as mq

    monkeypatch.setattr(mq, "normalize_market_symbol", lambda s: s if s else None)
    cur = _patch(monkeypatch)
    n = wa.upsert_registry(
        [
            {"symbol": "CN:600000", "source": "alpha", "addedAt": "2026-08-01"},
            {"symbol": "", "source": "x"},  # invalid -> skipped
            {"symbol": None},
        ]
    )
    assert n == 1
    row = cur.executed[0][1][0]
    assert row[0] == "CN:600000"
    assert row[1] == "alpha"
    assert row[3].obj["symbol"] == "CN:600000"  # psycopg Json wrapper
    assert "DELETE FROM" in cur.executed[1][0]


def test_upsert_registry_all_invalid(monkeypatch) -> None:
    import data_sync_service.service.market_quotes as mq

    monkeypatch.setattr(mq, "normalize_market_symbol", lambda s: None)
    cur = _patch(monkeypatch)
    assert wa.upsert_registry([{"symbol": "x"}]) == 0
    assert cur.executed == []


def test_list_registry(monkeypatch) -> None:
    rows = [
        ("CN:600000", "alpha", "2026-08-01", {"symbol": "CN:600000", "score": 80}),
        ("CN:600519", "manual", "", "not-a-dict"),
        ("HK:00700", "manual", None, None),
    ]
    _ = _patch(monkeypatch, rows)
    out = wa.list_registry()
    assert out[0]["symbol"] == "CN:600000" and out[0]["score"] == 80
    assert out[1]["source"] == "manual"
    assert out[2]["addedAt"] == ""


# ---- scores ----------------------------------------------------------------

def test_upsert_score_daily(monkeypatch) -> None:
    cur = _patch(monkeypatch)
    n = wa.upsert_score_daily(
        [
            {"symbol": "CN:600000", "trade_date": "2026-08-07", "score": 75.5, "industry": "银行"},
            {"symbol": "", "trade_date": "x"},  # skipped
            {"symbol": "CN:600519", "trade_date": "2026-08-07", "score": None, "industry": None},
        ]
    )
    assert n == 2
    assert cur.executed[0][1][0][2] == 75.5
    assert cur.executed[0][1][1][2] is None


def test_upsert_score_daily_empty(monkeypatch) -> None:
    cur = _patch(monkeypatch)
    assert wa.upsert_score_daily([]) == 0
    assert wa.upsert_score_daily([{"symbol": "x"}]) == 0
    assert cur.executed == []


def test_get_scores_for_symbol(monkeypatch) -> None:
    assert wa.get_scores_for_symbol("CN:600000", []) == []
    rows = [("2026-08-06", 70.0, "银行"), ("2026-08-07", None, None)]
    cur = _patch(monkeypatch, rows)
    out = wa.get_scores_for_symbol("CN:600000", ["2026-08-06", "2026-08-07"])
    assert out[0] == {"trade_date": "2026-08-06", "score": 70.0, "industry": "银行"}
    assert out[1]["score"] is None and out[1]["industry"] is None
    assert cur.executed[0][1] == ("CN:600000", ["2026-08-06", "2026-08-07"])


def test_fetch_latest_score_since(monkeypatch) -> None:
    _ = _patch(monkeypatch, [(80.0,)])
    assert wa.fetch_latest_score_since("CN:600000", "2026-08-01") == 80.0
    _ = _patch(monkeypatch, [(None,)])
    assert wa.fetch_latest_score_since("CN:600000", "2026-08-01") is None
    _ = _patch(monkeypatch, [])
    assert wa.fetch_latest_score_since("CN:600000", "2026-08-01") is None


# ---- automation runs -------------------------------------------------------

def test_insert_automation_run(monkeypatch) -> None:
    cur = _patch(monkeypatch)
    run_id = wa.insert_automation_run(
        trade_date="2026-08-07",
        trigger_type="scheduled",
        skipped=False,
        skip_reason=None,
        remove_items=[{"symbol": "CN:600000"}],
        alpha_add=[],
        meta={"funnel": {"x": 1}},
    )
    assert run_id
    params = cur.executed[0][1]
    assert params[1] == "2026-08-07"
    assert params[5].__class__.__name__ == "Json"


def test_get_run_by_id(monkeypatch) -> None:
    cur = _patch(monkeypatch, [_run_row(meta={"funnel": {"a": 1}}, applied=datetime(2026, 8, 7, 12, 0))])
    out = wa.get_run_by_id("run-1")
    assert out["runId"] == "run-1"
    assert out["tradeDate"] == "2026-08-07"
    assert out["trigger"] == "scheduled"
    assert out["skipped"] is False
    assert out["remove"] == [{"symbol": "CN:600000"}]
    assert out["appliedAt"] is not None
    assert out["screenerAdded"] == 3
    assert cur.executed[0][1] == ("run-1",)

    _ = _patch(monkeypatch, [])
    assert wa.get_run_by_id("ghost") is None


def test_get_latest_run(monkeypatch) -> None:
    _ = _patch(monkeypatch, [_run_row(meta=None, applied=None)])
    out = wa.get_latest_run()
    assert out["runId"] == "run-1"
    assert out["appliedAt"] is None and out["meta"] == {}
    _ = _patch(monkeypatch, [])
    assert wa.get_latest_run() is None


def test_list_recent_runs(monkeypatch) -> None:
    rows = [_run_row(meta={"funnel": {"x": 1}}, applied=datetime(2026, 8, 7, 12, 0))]
    cur = _patch(monkeypatch, rows)
    out = wa.list_recent_runs(limit=10)
    assert len(out) == 1
    assert cur.executed[0][1] == (10,)


def test_get_pending_run_with_and_without_date(monkeypatch) -> None:
    row = _run_row(meta=None, applied=None)
    cur = _patch(monkeypatch, [row])
    out = wa.get_pending_run("2026-08-07")
    assert out is not None
    assert "trade_date = %s" in cur.executed[0][0]

    cur2 = _patch(monkeypatch, [row])
    out2 = wa.get_pending_run()
    assert out2 is not None
    assert "trade_date = %s" not in cur2.executed[0][0]

    _ = _patch(monkeypatch, [])
    assert wa.get_pending_run() is None


def test_merge_funnel_into_meta() -> None:
    assert wa.merge_funnel_into_meta(None, None) == {}
    assert wa.merge_funnel_into_meta({"a": 1}, {"f": 2}) == {"a": 1, "funnel": {"f": 2}}
    assert wa.merge_funnel_into_meta({"a": 1}, None) == {"a": 1}
    assert wa.merge_funnel_into_meta({"a": 1}, "junk") == {"a": 1}


def test_ack_run(monkeypatch) -> None:
    cur = _patch(
        monkeypatch,
        [_run_row(meta={"a": 1}, applied=datetime(2026, 8, 7, 12, 0), screener_added=5)],
    )
    out = wa.ack_run("run-1", screener_added=5, funnel={"f": 9})
    assert out is not None
    assert out["screenerAdded"] == 5
    assert "UPDATE" in cur.executed[1][0]
    assert cur.executed[1][1][1].__class__.__name__ == "Json"

    cur2 = _patch(monkeypatch, [])
    assert wa.ack_run("ghost") is None
    assert len(cur2.executed) == 1  # SELECT only
