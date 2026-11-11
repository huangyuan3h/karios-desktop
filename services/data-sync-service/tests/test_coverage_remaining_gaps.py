"""Cover remaining branches in fund-flow fallback, factor-vault job, and daily adjust."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from data_sync_service.scheduler import factor_vault_job as fv_job
from data_sync_service.service import daily_adjust
from data_sync_service.service import industry_fund_flow_tushare as tsf


class _Df:
    def __init__(self, recs: list[dict[str, Any]] | None, *, empty: bool | None = None) -> None:
        self._recs = recs or []
        self.empty = (not self._recs) if empty is None else empty

    def to_dict(self, _orient: str) -> list[dict[str, Any]]:
        return self._recs


class _Pool:
    def __init__(self, df: Any) -> None:
        self._df = df

    def pro(self) -> Any:
        return self

    def index_member_all(self, is_new: str = "Y") -> Any:
        return self._df


class _Cur:
    def __init__(self, rows: list[Any] | None) -> None:
        self._rows = rows

    def __enter__(self) -> _Cur:
        return self

    def __exit__(self, *a: Any) -> None:
        return None

    def execute(self, *a: Any, **k: Any) -> None:
        return None

    def fetchall(self) -> list[Any] | None:
        return self._rows


class _Conn:
    def __init__(self, rows: list[Any] | None) -> None:
        self._rows = rows

    def __enter__(self) -> _Conn:
        return self

    def __exit__(self, *a: Any) -> None:
        return None

    def cursor(self) -> _Cur:
        return _Cur(self._rows)


def test_shanghai_today_iso_and_fetch_membership(monkeypatch: pytest.MonkeyPatch) -> None:
    assert isinstance(tsf._shanghai_today_iso(), str)

    monkeypatch.setattr(
        "data_sync_service.clients.tushare_pool.get_pool",
        lambda: _Pool(None),
    )
    with pytest.raises(RuntimeError, match="no rows"):
        tsf._fetch_sw_l1_membership()

    monkeypatch.setattr(
        "data_sync_service.clients.tushare_pool.get_pool",
        lambda: _Pool(_Df([], empty=True)),
    )
    with pytest.raises(RuntimeError, match="no rows"):
        tsf._fetch_sw_l1_membership()

    monkeypatch.setattr(
        "data_sync_service.clients.tushare_pool.get_pool",
        lambda: _Pool(
            _Df(
                [
                    {"ts_code": "", "l1_code": "801780.SI", "l1_name": "银行"},
                    {"ts_code": "600000.SH", "l1_code": "", "l1_name": "银行"},
                    {"ts_code": "600001.SH", "l1_code": "801780.SI", "l1_name": ""},
                ]
            )
        ),
    )
    with pytest.raises(RuntimeError, match="empty SW L1"):
        tsf._fetch_sw_l1_membership()

    monkeypatch.setattr(
        "data_sync_service.clients.tushare_pool.get_pool",
        lambda: _Pool(
            _Df(
                [
                    {"ts_code": "600000.SH", "l1_code": "801780.SI", "l1_name": "银行"},
                    {"ts_code": "", "l1_code": "x", "l1_name": "y"},
                ]
            )
        ),
    )
    assert tsf._fetch_sw_l1_membership() == {"600000.SH": ("801780.SI", "银行")}


def test_moneyflow_db_and_ingest_paths(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        tsf,
        "get_connection",
        lambda: _Conn(
            [
                ("600000.SH", 1, 2, 3, 4),
                (None, None, None, None, None),
            ]
        ),
    )
    rows = tsf._moneyflow_rows_from_db("2026-11-05")
    assert rows[0]["ts_code"] == "600000.SH"
    assert rows[1]["ts_code"] == ""

    monkeypatch.setattr(tsf, "get_connection", lambda: _Conn(None))
    assert tsf._moneyflow_rows_from_db("2026-11-05") == []

    monkeypatch.setattr(tsf, "_moneyflow_rows_from_db", lambda day: [{"ts_code": "x"}])
    assert tsf._ensure_moneyflow_ingested("2026-11-05") == [{"ts_code": "x"}]

    calls = {"n": 0}

    def _empty_then_still(_day: str) -> list[dict[str, Any]]:
        return []

    monkeypatch.setattr(tsf, "_moneyflow_rows_from_db", _empty_then_still)
    monkeypatch.setattr(
        "data_sync_service.service.cn_extra_sync.sync_moneyflow_for_dates",
        lambda dates: (_ for _ in ()).throw(RuntimeError("quota")),
    )
    with pytest.raises(RuntimeError, match="ingest failed"):
        tsf._ensure_moneyflow_ingested("2026-11-05")

    def _empty_then_filled(day: str) -> list[dict[str, Any]]:
        calls["n"] += 1
        return [] if calls["n"] == 1 else []

    calls["n"] = 0
    monkeypatch.setattr(tsf, "_moneyflow_rows_from_db", _empty_then_filled)
    monkeypatch.setattr(
        "data_sync_service.service.cn_extra_sync.sync_moneyflow_for_dates",
        lambda dates: None,
    )
    with pytest.raises(RuntimeError, match="still empty"):
        tsf._ensure_moneyflow_ingested("2026-11-05")

    calls["n"] = 0

    def _empty_then_ok(day: str) -> list[dict[str, Any]]:
        calls["n"] += 1
        return [] if calls["n"] == 1 else [{"ts_code": "600000.SH"}]

    monkeypatch.setattr(tsf, "_moneyflow_rows_from_db", _empty_then_ok)
    assert tsf._ensure_moneyflow_ingested("2026-11-05") == [{"ts_code": "600000.SH"}]

    assert tsf._to_float("nope") == 0.0
    assert tsf._to_float(None) == 0.0


def test_aggregate_raises_when_no_industry_matches(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(tsf, "sw_l1_membership", lambda: {"600000.SH": ("801780.SI", "银行")})
    monkeypatch.setattr(
        tsf,
        "_ensure_moneyflow_ingested",
        lambda day: [{"ts_code": "999999.SZ", "buy_lg_amount": 1}],
    )
    with pytest.raises(RuntimeError, match="no SW L1 industry matched"):
        tsf.aggregate_for_date("2026-11-05")


def test_factor_vault_job_helpers_and_run(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    assert fv_job._reports_dir().name == "backtest_reports"

    hist_path = tmp_path / "factor_vault_history.json"
    hist_path.write_text("not-json", encoding="utf-8")
    assert fv_job._load_prev_states(tmp_path) == {}

    hist_path.write_text(json.dumps({"history": []}), encoding="utf-8")
    assert fv_job._load_prev_states(tmp_path) == {}

    hist_path.write_text(json.dumps({"history": "nope"}), encoding="utf-8")
    assert fv_job._load_prev_states(tmp_path) == {}

    # empty entries: early return, no file written
    fv_job.append_transitions(tmp_path, [], "2026-11-10")
    assert not (tmp_path / fv_job.TRANSITIONS_FILENAME).exists()

    bad = tmp_path / fv_job.TRANSITIONS_FILENAME
    bad.write_text("{not json", encoding="utf-8")
    fv_job.append_transitions(
        tmp_path,
        [{"date": "2026-11-10", "factor_id": "a"}],
        "2026-11-10",
    )
    assert (tmp_path / fv_job.TRANSITIONS_FILENAME).exists()

    # existing not a list + invalid cutoff date + skip junk / old / bad dates
    (tmp_path / fv_job.TRANSITIONS_FILENAME).write_text(
        json.dumps(
            {
                "transitions": "nope",
            }
        ),
        encoding="utf-8",
    )
    fv_job.append_transitions(
        tmp_path,
        [
            "skip-me",
            {"date": "1999-01-01", "factor_id": "old"},
            {"date": "not-a-date", "factor_id": "bad"},
            {"date": "2026-11-10", "factor_id": "ok"},
        ],
        "not-iso",
    )
    kept = json.loads((tmp_path / fv_job.TRANSITIONS_FILENAME).read_text(encoding="utf-8"))
    # cutoff falls back to 0 on ValueError, so 1999 is kept; bad date is skipped
    ids = [e.get("factor_id") for e in kept["transitions"] if isinstance(e, dict)]
    assert "ok" in ids
    assert "bad" not in ids

    # write OSError is fail-open
    class _BoomPath(type(tmp_path)):
        def write_text(self, *a: Any, **k: Any) -> int:  # type: ignore[override]
            raise OSError("disk full")

    # Patch Path.write_text on the transitions path by making mkdir succeed
    # and write_text fail via monkeypatch of Path.write_text is too broad.
    # Instead patch json.dumps? Better: make reports_dir a file so mkdir/write fails.
    blocked = tmp_path / "blocked_dir"
    blocked.write_text("i-am-a-file", encoding="utf-8")
    fv_job.append_transitions(blocked, [{"date": "2026-11-10", "factor_id": "x"}], "2026-11-10")

    recorded: list[str] = []
    monkeypatch.setattr(fv_job, "_reports_dir", lambda: tmp_path)
    monkeypatch.setattr(fv_job, "_load_prev_states", lambda d: {})
    monkeypatch.setattr(
        "data_sync_service.scheduler._job_guard.record_failure",
        lambda *a, **k: recorded.append("fail"),
    )
    monkeypatch.setattr(
        "data_sync_service.scheduler._job_guard.record_success",
        lambda *a, **k: recorded.append("ok"),
    )

    def _boom(*a: Any, **k: Any) -> Any:
        raise RuntimeError("spawn failed")

    monkeypatch.setattr(fv_job.subprocess, "run", _boom)
    fv_job.run()
    assert "fail" in recorded

    class _Proc:
        def __init__(self, code: int, stdout: str = "", stderr: str = "") -> None:
            self.returncode = code
            self.stdout = stdout
            self.stderr = stderr

    recorded.clear()
    monkeypatch.setattr(fv_job.subprocess, "run", lambda *a, **k: _Proc(0, stdout="ok"))
    # missing vault file → transition OSError branch
    fv_job.run()
    assert "ok" in recorded

    (tmp_path / "factor_vault.json").write_text(
        json.dumps({"factors": [{"id": "f1", "status": "revived"}]}),
        encoding="utf-8",
    )
    recorded.clear()
    monkeypatch.setattr(
        "data_sync_service.service.factor_vault.diff_factor_transitions",
        lambda *a, **k: [
            {
                "event_type": "factor_revived",
                "factor_id": "f1",
                "dedupe_key": "f1-2026-11-10",
                "name": "F1",
                "from": "watch",
                "to": "revived",
            }
        ],
    )
    monkeypatch.setattr(
        "data_sync_service.db.webhook.emit_event",
        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("webhook down")),
    )
    fv_job.run()
    assert "ok" in recorded


def test_daily_adjust_remaining_actions(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "data_sync_service.db.watchlist_automation.list_registry",
        lambda: (_ for _ in ()).throw(RuntimeError("db down")),
    )
    fail = daily_adjust.load_live_sleeve("2026-11-10")
    assert fail["active"] is False
    assert "不可用" in fail["message"]

    registry = [
        {
            "symbol": "ETF:511260",
            "payload": {"positionPct": 29, "costPrice": 134.46, "name": "国债"},
            "ts_code": "511260.SH",
        },
        {"symbol": "IGNORE:X", "payload": {}},
    ]
    monkeypatch.setattr(
        "data_sync_service.db.watchlist_automation.list_registry",
        lambda: registry,
    )
    monkeypatch.setattr(
        "data_sync_service.service.multi_asset_sleeve.build_multi_asset_sleeve",
        lambda **k: {"action": "HOLD", "symbol": "ETF:511260"},
    )
    ok = daily_adjust.load_live_sleeve("2026-11-10")
    assert ok["action"] == "HOLD"

    sell_a = daily_adjust._sleeve_lines(
        {
            "action": "SELL_TO_A_SHARE",
            "symbol": "ETF:513350",
            "message": "让位",
            "executeHint": "次日开盘",
        }
    )
    assert any("股票篮" in ln for ln in sell_a[0])
    assert sell_a[1][0]["side"] == "SELL"

    buy = daily_adjust._sleeve_lines(
        {"action": "BUY", "pick": {"symbol": "ETF:511260"}, "message": "", "executeHint": ""}
    )
    assert any("买入" in ln for ln in buy[0])
    assert buy[1][0]["side"] == "BUY"

    hold_pending = daily_adjust._sleeve_lines(
        {
            "action": "HOLD",
            "symbol": "ETF:513350",
            "message": "峰值回撤触发",
            "pendingOpenSell": True,
            "signalDate": "2026-11-09",
        }
    )
    assert any("卖出" in ln for ln in hold_pending[0])
    assert hold_pending[1][0]["side"] == "SELL"

    none_msg = daily_adjust._sleeve_lines({"action": "NONE", "message": "今日观望"})
    assert none_msg[0] == ["今日观望"]

    monkeypatch.setattr(
        daily_adjust,
        "compose_satellite_action",
        lambda panel, mode, parking=None: {
            "lines": ["买"],
            "exits": [],
            "buys": [{"ts": "512000"}],
            "proceeds": "逆回购",
        },
    )
    sat = daily_adjust.compose_daily_adjust(
        "starship",
        trade_date="2026-11-10",
        panel={"decisionAvailable": True},
    )
    assert any(a.get("side") == "BUY" and a.get("symbol") == "512000" for a in sat["adjustments"])

    # Trail alert
    monkeypatch.setattr(
        "data_sync_service.db.webhook.emit_event",
        lambda *a, **k: None,
    )
    payload = daily_adjust.emit_trail_if_active(
        "2026-11-10",
        sleeve={
            "action": "SELL_TO_REPO",
            "symbol": "ETF:513350",
            "pendingOpenSell": True,
            "signalDate": "2026-11-09",
            "executeHint": "次日开盘卖出转逆回购",
            "message": "峰值回撤 8%",
        },
    )
    assert payload is not None and payload["mustAct"] is True
    assert daily_adjust.emit_trail_if_active("2026-11-10", sleeve={"action": "HOLD"}) is None
    monkeypatch.setattr(
        "data_sync_service.db.webhook.emit_event",
        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("down")),
    )
    assert (
        daily_adjust.emit_trail_if_active(
            "2026-11-10", sleeve={"action": "SELL_TO_REPO", "symbol": "ETF:513350"}
        )
        is None
    )


def test_factor_vault_job_success_paths(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    trigger = fv_job.build_trigger()
    assert trigger is not None

    hist = {
        "history": [
            {"states": {"f1": "cold"}},
            {"states": {"f1": "watch", "f2": "revived"}},
        ]
    }
    (tmp_path / "factor_vault_history.json").write_text(json.dumps(hist), encoding="utf-8")
    states = fv_job._load_prev_states(tmp_path)
    assert states["f1"] == "watch"
    assert states["f2"] == "revived"

    recorded: list[str] = []
    monkeypatch.setattr(fv_job, "_reports_dir", lambda: tmp_path)
    monkeypatch.setattr(fv_job, "_load_prev_states", lambda d: {})
    monkeypatch.setattr(
        "data_sync_service.scheduler._job_guard.record_failure",
        lambda *a, **k: recorded.append("fail"),
    )
    monkeypatch.setattr(
        "data_sync_service.scheduler._job_guard.record_success",
        lambda *a, **k: recorded.append("ok"),
    )

    class _Proc:
        def __init__(self) -> None:
            self.returncode = 1
            self.stdout = ""
            self.stderr = "generate failed"

    monkeypatch.setattr(fv_job.subprocess, "run", lambda *a, **k: _Proc())
    fv_job.run()
    assert "fail" in recorded


def test_overlay_today_iso_uses_calendar() -> None:
    from data_sync_service.service import agent_overlays

    today = agent_overlays._today_iso()
    assert len(today) == 10


def test_contract_today_iso_and_non_dict_report(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    from data_sync_service.service import agent_contract

    assert len(agent_contract._today_iso()) == 10
    monkeypatch.setattr("data_sync_service.api.backtest_routes.REPORTS_DIR", tmp_path)
    (tmp_path / "factor_vault.json").write_text("[1, 2, 3]", encoding="utf-8")
    assert agent_contract._read_report_json("factor_vault.json") is None
