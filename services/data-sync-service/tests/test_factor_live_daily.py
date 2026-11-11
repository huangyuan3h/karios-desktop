"""H01/H02/H03 daily live replay tests (no DB, no network, no pkl required).

Covers the frozen ``h2_daily.py`` recipe port:
- each of the three signals on a synthetic panel (entry next open, 5-session
  hold, 32.28bp cost);
- entry-day limit-up skip and the amount / pct / ST / listing-age filters;
- no append for a signal whose exit bar is not complete yet;
- the frozen-prefix sidecar (compact tail, stable keys, integrity check);
- last-40 is the **mean % per trade**, never the 40-trade sum;
- idempotent append + per-family fail-open (previous rows survive);
- the vault override prefers live rows and never touches the four frozen
  audit windows.
"""

from __future__ import annotations

import importlib.util
import json
import pickle
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pytest

from data_sync_service.service import factor_live as fl
from data_sync_service.service import factor_live_daily as fld
from data_sync_service.service import factor_vault as fv

ROOT = Path(__file__).resolve().parents[1]


def _load_generate_vault():
    spec = importlib.util.spec_from_file_location(
        "generate_factor_vault_daily", ROOT / "scripts" / "generate_factor_vault.py"
    )
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


def _day(i: int) -> str:
    return (date(2026, 1, 5) + timedelta(days=i)).isoformat()


def _series(
    closes: list[float],
    *,
    pcts: list[float] | None = None,
    amounts: list[float] | None = None,
    amount: float = 50000.0,
    split: int | None = None,
    split_open: float | None = None,
) -> list[fld.Bar]:
    """Bars with open=close (so a trade's buy price is the entry close)."""
    bars: list[fld.Bar] = []
    prev = closes[0]
    for i, close in enumerate(closes):
        pct = pcts[i] if pcts is not None else ((close / prev - 1.0) * 100.0 if i else 0.0)
        amt = amounts[i] if amounts is not None else amount
        open_ = close
        if split is not None and i == split and split_open is not None:
            open_ = split_open
        bars.append(fld.Bar(_day(i), open_, close, pct, amt))
        prev = close
    return bars


def _h01_panel() -> list[fld.Bar]:
    closes = [10.0] * 100 + [9.9, 9.8, 9.7, 9.6, 9.5, 9.5, 9.5, 9.5, 9.5, 10.0] + [10.0] * 10
    return _series(closes)


def _h02_panel() -> list[fld.Bar]:
    closes = [10.0] * 100 + [9.2, 9.384, 9.384, 9.384, 9.384, 9.6] + [9.6] * 10
    return _series(closes)


def _h03_panel() -> list[fld.Bar]:
    closes = [10.0 + 0.01 * t for t in range(100)]
    drop = closes[-1] - 0.5
    bounce = drop + 0.8
    closes += [drop, bounce, bounce, bounce, bounce, bounce + 0.3] + [bounce + 0.3] * 10
    return _series(closes)


# ---------------------------------------------------------------------------
# Recipe
# ---------------------------------------------------------------------------


def test_rsi_matches_pandas_rolling_semantics() -> None:
    # Monotonic up -> 100, monotonic down -> 0, flat -> None (NaN in pandas).
    assert fld.compute_rsi([10.0] * 20)[-1] is None
    assert fld.compute_rsi([10.0 + 0.1 * i for i in range(20)])[-1] == pytest.approx(100.0)
    assert fld.compute_rsi([10.0 - 0.1 * i for i in range(20)])[-1] == pytest.approx(0.0)
    # min_periods=n: no value before the n-th delta.
    out = fld.compute_rsi([10.0 - 0.1 * i for i in range(20)])
    assert out[13] is None and out[14] is not None


def test_h01_five_down_hold5_next_open_entry() -> None:
    bars = _h01_panel()
    events = fld.panel_events(
        "h01", "CN:000001", bars, start=_day(100), end=_day(109), list_date="2020-01-01"
    )
    assert len(events) == 1
    ev = events[0]
    assert ev["signal"] == _day(104)
    assert ev["entry"] == _day(105)
    assert ev["exit"] == _day(109)
    assert ev["net"] == pytest.approx(10.0 / 9.5 - 1.0 - fld.COST, abs=1e-6)
    assert ev["win"] is True
    assert ev["ts"] == "CN:000001"


def test_h02_bias6_hold5() -> None:
    bars = _h02_panel()
    events = fld.panel_events(
        "h02", "CN:000002", bars, start=_day(100), end=_day(109), list_date="2020-01-01"
    )
    assert len(events) == 1
    ev = events[0]
    assert ev["signal"] == _day(100)
    assert ev["entry"] == _day(101)
    assert ev["exit"] == _day(105)
    assert ev["net"] == pytest.approx(9.6 / 9.384 - 1.0 - fld.COST, abs=1e-6)


def test_h03_rsi30_hold5() -> None:
    bars = _h03_panel()
    events = fld.panel_events(
        "h03", "CN:000003", bars, start=_day(100), end=_day(109), list_date="2020-01-01"
    )
    assert len(events) == 1
    ev = events[0]
    assert ev["signal"] == _day(100)
    assert ev["entry"] == _day(101)
    assert ev["exit"] == _day(105)
    assert ev["net"] == pytest.approx((bars[101].close + 0.3) / bars[101].close - 1.0 - fld.COST, abs=1e-6)


def test_entry_day_limit_up_is_skipped() -> None:
    bars = _h01_panel()
    bars[105] = bars[105]._replace(pct=9.9)
    events = fld.panel_events(
        "h01", "CN:000001", bars, start=_day(100), end=_day(109), list_date="2020-01-01"
    )
    assert events == []


def test_amount_and_signal_pct_filters() -> None:
    low_amount = _h01_panel()
    low_amount[104] = low_amount[104]._replace(amount=19999.0)
    assert (
        fld.panel_events(
            "h01", "CN:000001", low_amount, start=_day(100), end=_day(109), list_date="2020-01-01"
        )
        == []
    )

    hot_signal = _h03_panel()
    hot_signal[100] = hot_signal[100]._replace(pct=9.6)
    assert (
        fld.panel_events(
            "h03", "CN:000003", hot_signal, start=_day(100), end=_day(109), list_date="2020-01-01"
        )
        == []
    )


def test_universe_filters_st_and_delisted_and_market() -> None:
    rows = [
        ("CN:1", "主板", "平安银行", "2010-01-01", None),
        ("CN:2", "创业板", "ST中葡", "2010-01-01", None),
        ("CN:3", "科创板", "正常股", "2010-01-01", None),
        ("CN:4", "主板", "退市股", "2010-01-01", "2025-01-01"),
        ("HK:5", "HK", "腾讯控股", "2010-01-01", None),
        ("CN:6", "北交所", "北证股", "2010-01-01", None),
    ]
    codes, list_dates = fld.universe_from_rows(rows)
    assert codes == ["CN:1", "CN:3"]
    assert list_dates["CN:1"] == "2010-01-01"


def test_listing_age_filter_follows_signal_date() -> None:
    assert fld.listed_enough("2020-01-01", "2026-10-15") is True
    assert fld.listed_enough("2026-09-01", "2026-10-15") is False
    # Unknown listing date is treated as ok (the audit's fallback).
    assert fld.listed_enough(None, "2026-10-15") is True

    bars = _h01_panel()
    recent = fld.panel_events(
        "h01", "CN:000001", bars, start=_day(100), end=_day(109), list_date="2026-04-01"
    )
    assert recent == []
    old = fld.panel_events(
        "h01", "CN:000001", bars, start=_day(100), end=_day(109), list_date="2020-01-01"
    )
    assert len(old) == 1


def test_signal_without_complete_exit_is_not_appended() -> None:
    bars = _h01_panel()[:106]
    # Signal on the 2nd-from-last bar: the 5-session exit does not exist yet.
    events = fld.panel_events(
        "h01", "CN:000001", bars, start=_day(104), end=_day(114), list_date="2020-01-01"
    )
    assert events == []
    # Same panel, but only the (complete) signal day is inside the window.
    done = fld.panel_events(
        "h01", "CN:000001", bars, start=_day(100), end=_day(104), list_date="2020-01-01"
    )
    assert done == []  # the exit bar is missing from this truncated frame


# ---------------------------------------------------------------------------
# Frozen prefix sidecar
# ---------------------------------------------------------------------------


def _pkl_payload() -> dict[str, Any]:
    return {
        "toprec": {
            "H01": {
                "2026-08-10": [(1.0, 10.0, 10.5, 50000.0), (1.0, 20.0, 19.8, 60000.0)],
                "2026-09-22": [(1.0, 10.0, 10.2, 50000.0)],
                "2026-09-25": [(1.0, 10.0, 10.0, 50000.0)],
            },
            "H02": {},
            "H03": {},
        }
    }


def _sessions() -> list[str]:
    return [
        "2026-08-10",
        "2026-09-22",
        "2026-09-23",
        "2026-09-24",
        "2026-09-25",
        "2026-09-28",
        "2026-09-29",
        "2026-09-30",
        "2026-10-08",
        "2026-10-09",
        "2026-10-12",
    ]


def test_build_frozen_prefix_extracts_compact_tail_and_keys(tmp_path: Path) -> None:
    pkl = tmp_path / fld.FROZEN_PKL
    with pkl.open("wb") as fh:
        pickle.dump(_pkl_payload(), fh)
    (tmp_path / fld.FROZEN_JSON).write_text(
        json.dumps(
            {
                "events": {
                    "H01": {
                        "holdout": {
                            "n": 4,
                            "mean": 0.015 - fld.COST,
                        }
                    },
                    "H02": {"holdout": {"n": 0, "mean": 0.0}},
                    "H03": {"holdout": {"n": 0, "mean": 0.0}},
                }
            }
        ),
        encoding="utf-8",
    )
    prefix = fld.build_frozen_prefix(scratch_dir=tmp_path, sessions=_sessions())
    h01 = prefix["families"]["h01"]
    assert h01["total"] == 4
    assert h01["lastSignal"] == "2026-09-25"
    assert h01["holdout"]["n"] == 4
    assert h01["holdout"]["ok"] is True
    rows = h01["rows"]
    assert [r["key"] for r in rows] == [
        "h01|2026-08-10|0",
        "h01|2026-08-10|1",
        "h01|2026-09-22|0",
        "h01|2026-09-25|0",
    ]
    assert rows[-1]["net"] == pytest.approx(10.0 / 10.0 - 1.0 - fld.COST)
    assert prefix["source"]["pklSha256"]
    assert prefix["source"]["counts"]["h01"] == 4
    # entry/exit are derived with the standard hold when the calendar has them.
    assert rows[-1]["entry"] == "2026-09-28"
    assert rows[-1]["exit"] == "2026-10-09"


def test_load_frozen_prefix_rebuilds_only_when_source_changes(tmp_path: Path) -> None:
    reports = tmp_path / "reports"
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    with (scratch / fld.FROZEN_PKL).open("wb") as fh:
        pickle.dump(_pkl_payload(), fh)
    (scratch / fld.FROZEN_JSON).write_text("{}", encoding="utf-8")

    first = fld.load_frozen_prefix(reports, scratch_dir=scratch)
    assert first is not None
    path = reports / fld.PREFIX_FILENAME
    assert path.exists()
    stamp = json.loads(path.read_text(encoding="utf-8"))["generatedAt"]
    second = fld.load_frozen_prefix(reports, scratch_dir=scratch)
    assert second is not None
    assert json.loads(path.read_text(encoding="utf-8"))["generatedAt"] == stamp

    # A changed source invalidates the cached sidecar.
    with (scratch / fld.FROZEN_PKL).open("wb") as fh:
        pickle.dump({"toprec": {"H01": {"2026-09-30": [(1.0, 1.0, 1.1, 1.0)]}}}, fh)
    third = fld.load_frozen_prefix(reports, scratch_dir=scratch)
    assert third is not None
    assert third["families"]["h01"]["total"] == 1


def test_load_frozen_prefix_is_none_without_pkl_or_sidecar(tmp_path: Path) -> None:
    assert fld.load_frozen_prefix(tmp_path, scratch_dir=tmp_path / "nope") is None


# ---------------------------------------------------------------------------
# Last-40 unit: mean %/笔, not a 40-trade sum
# ---------------------------------------------------------------------------


def test_last_40_is_mean_percent_per_trade_not_sum() -> None:
    frozen = [
        {"key": f"h01|2026-09-{i:02d}|0", "signal": f"2026-09-{i:02d}", "entry": None,
         "exit": None, "net": 0.01, "ts": ""}
        for i in range(1, 11)
    ]
    appended = [
        {
            "ts": f"CN:{i:06d}",
            "signal": "2026-10-09",
            "entry": "2026-10-12",
            "exit": "2026-10-16",
            "net": 0.02,
        }
        for i in range(30)
    ]
    stats = fld.rolling_stats(frozen, appended, as_of="2026-11-11")
    assert stats is not None
    assert stats["trades"] == 40
    # 10 x 0.01 + 30 x 0.02 -> mean 0.0175 -> +1.75 %/笔 (a sum would be 70).
    assert stats["net"] == pytest.approx(1.75)
    assert stats["percentile"] is None
    assert stats["win_rate"] == 1.0
    assert stats["as_of"] == "2026-11-11"
    assert stats["last_trade_as_of"] == "2026-10-16"


def test_last_40_windows_the_tail_only() -> None:
    def day(i: int) -> str:
        return (date(2026, 1, 1) + timedelta(days=i)).isoformat()

    rows = [
        {"ts": f"CN:{i:06d}", "entry": day(i), "exit": day(i),
         "net": 0.10 if i < 5 else -0.01}
        for i in range(45)
    ]
    stats = fld.rolling_stats([], rows, as_of="2026-11-11")
    assert stats is not None
    assert stats["trades"] == 40
    assert stats["total_trades"] == 45
    assert stats["net"] == pytest.approx(-1.0)


# ---------------------------------------------------------------------------
# Overlay integration (injected prefix + replay, no DB)
# ---------------------------------------------------------------------------


def _frozen_rows(family: str, n: int = 45, *, last_signal: str = "2026-09-22") -> list[dict]:
    rows = []
    for i in range(n):
        signal = f"2026-08-{(i % 28) + 1:02d}"
        rows.append(
            {
                "key": f"{family}|{signal}|{i}",
                "signal": signal,
                "entry": "2026-09-23",
                "exit": "2026-09-30",
                "net": 0.01,
            }
        )
    rows[-1].update(
        key=f"{family}|{last_signal}|0",
        signal=last_signal,
        entry="2026-09-23",
        exit="2026-09-30",
    )
    return rows


def _prefix(families: tuple[str, ...] = fld.FAMILIES) -> dict[str, Any]:
    return {
        "version": 1,
        "source": {"pkl": "/tmp/h2_records.pkl", "pklSha256": "abc", "counts": {}},
        "families": {
            fam: {
                "label": fld.LABELS[fam],
                "rows": _frozen_rows(fam),
                "total": 45,
                "lastSignal": "2026-09-22",
                "lastEntry": "2026-09-23",
                "lastExit": "2026-09-30",
            }
            for fam in families
        },
    }


def _sgap_frozen(n: int = 39) -> list[dict[str, Any]]:
    return [
        {"ts": f"00000{i % 10}.SZ", "entry": f"2026-01-{i + 1:02d}", "exit": f"2026-01-{i + 1:02d}",
         "contrib": 0.5, "win": True}
        for i in range(n)
    ]


def _refresh_kwargs(tmp_path: Path) -> dict[str, Any]:
    frozen_sgap = tmp_path / "h2f_trades.json"
    frozen_sgap.write_text(json.dumps(_sgap_frozen()), encoding="utf-8")
    frozen_harbor = tmp_path / "h2d_blotter_long.json"
    frozen_harbor.write_text(
        json.dumps(
            [
                {"ts": "CN:600000", "entry": f"2026-01-{i + 1:02d}", "exit": f"2026-01-{i + 1:02d}",
                 "contrib": 0.8}
                for i in range(39)
            ]
        ),
        encoding="utf-8",
    )
    return {
        "now": __import__("datetime").datetime(2026, 11, 10, 19, 0),
        "latest_session": "2026-11-10",
        "etf_session": "2026-11-10",
        "frozen_sgap_path": frozen_sgap,
        "frozen_harbor_path": frozen_harbor,
        "frozen_daily_prefix": _prefix(),
        "sgap_replay": lambda s, e: {"ok": True, "asOf": e, "trades": [], "parking": []},
        "harbor_replay": lambda s, e: {"ok": True, "asOf": e, "etfAsOf": e, "trades": [], "parking": []},
        "session_after": lambda _d: "2026-09-23",
    }


def test_refresh_appends_closed_daily_trades_idempotently(tmp_path: Path) -> None:
    def daily_replay(family: str, start: str, end: str) -> dict[str, Any]:
        assert (family, start, end) == ("h01", "2026-09-23", "2026-11-10") or family != "h01"
        return {
            "ok": True,
            "asOf": end,
            "trades": [
                # new closed trade -> appended
                {"ts": "CN:600001", "signal": "2026-10-09", "entry": "2026-10-12",
                 "exit": "2026-10-16", "net": 0.05, "win": True},
                # open leg (no exit) -> never appended
                {"ts": "CN:600002", "signal": "2026-11-05", "entry": "2026-11-06",
                 "exit": None, "net": 0.02},
                # exit after the watermark -> never appended
                {"ts": "CN:600003", "signal": "2026-11-06", "entry": "2026-11-09",
                 "exit": "2026-11-13", "net": 0.01},
                # inside the frozen prefix -> never duplicated
                {"ts": "CN:600004", "signal": "2026-09-22", "entry": "2026-09-23",
                 "exit": "2026-09-30", "net": 0.03},
            ],
        }

    reports = tmp_path / "reports"
    kwargs = _refresh_kwargs(tmp_path)
    first = fl.refresh_live_blotter(reports, daily_replay=daily_replay, **kwargs)
    assert first["meta"]["status"] == "ok"
    assert [t["ts"] for t in first["appended"]["h01"]] == ["CN:600001"]
    assert first["meta"]["inputs"]["h01"]["newTrades"] == 1
    assert first["meta"]["frozenPrefix"]["h01"]["sha256"] == "abc"

    again = fl.refresh_live_blotter(reports, daily_replay=daily_replay, **kwargs)
    assert [t["ts"] for t in again["appended"]["h01"]] == ["CN:600001"]


def test_daily_failure_keeps_prior_rows_and_does_not_block_core(tmp_path: Path) -> None:
    def good(family: str, start: str, end: str) -> dict[str, Any]:
        return {
            "ok": True,
            "asOf": end,
            "trades": [
                {"ts": "CN:600001", "signal": "2026-10-09", "entry": "2026-10-12",
                 "exit": "2026-10-16", "net": 0.05, "win": True}
            ],
        }

    reports = tmp_path / "reports"
    kwargs = _refresh_kwargs(tmp_path)
    first = fl.refresh_live_blotter(reports, daily_replay=good, **kwargs)
    assert [t["ts"] for t in first["appended"]["h01"]] == ["CN:600001"]

    def flaky(family: str, start: str, end: str) -> dict[str, Any]:
        if family == "h02":
            raise RuntimeError("db down")
        return good(family, start, end)

    degraded = fl.refresh_live_blotter(reports, daily_replay=flaky, **kwargs)
    assert degraded["meta"]["status"] == "degraded"
    assert "h02" in str(degraded["meta"]["reason"])
    # S-gap / Harbor still ok and yesterday's H01 rows survive untouched.
    assert degraded["meta"]["inputs"]["sgap"]["ok"] is True
    assert degraded["meta"]["inputs"]["harbor"]["ok"] is True
    assert [t["ts"] for t in degraded["appended"]["h01"]] == ["CN:600001"]
    # The failed family keeps the rows appended by the previous run.
    assert [t["ts"] for t in degraded["appended"]["h02"]] == ["CN:600001"]
    assert json.loads((reports / fl.LIVE_FILENAME).read_text())["appended"]["h01"]


def test_daily_replay_without_prefix_falls_back_to_holdout_edge(tmp_path: Path) -> None:
    starts: list[str] = []

    def daily_replay(family: str, start: str, end: str) -> dict[str, Any]:
        starts.append(start)
        return {"ok": True, "asOf": end, "trades": []}

    reports = tmp_path / "reports"
    kwargs = _refresh_kwargs(tmp_path)
    kwargs["frozen_daily_prefix"] = {}
    fl.refresh_live_blotter(reports, daily_replay=daily_replay, **kwargs)
    # No prefix -> replay starts after the frozen holdout edge (2026-09-30).
    assert starts
    payload = json.loads((reports / fl.LIVE_FILENAME).read_text())
    assert payload["meta"]["inputs"]["h03"]["frozenLastSignal"] is None
    assert payload["meta"]["inputs"]["h03"]["ok"] is True


# ---------------------------------------------------------------------------
# Vault override
# ---------------------------------------------------------------------------


def test_vault_override_prefers_live_daily_rows() -> None:
    gv = _load_generate_vault()
    prefix = _prefix()
    live = {
        "meta": {"asOf": "2026-11-10", "status": "ok"},
        "appended": {
            "h01": [
                {"ts": "CN:600001", "signal": "2026-10-09", "entry": "2026-10-12",
                 "exit": "2026-10-16", "net": 0.05}
            ],
            "h02": [],
            "h03": [],
        },
    }
    ov = gv._daily_override(live, prefix)
    assert ov is not None
    h01 = ov["h01"]
    assert h01["basis"] == "live:h01"
    assert h01["window"] == "live"
    assert h01["as_of"] == "2026-11-10"
    assert h01["last_trade_as_of"] == "2026-10-16"
    assert h01["trades"] == 40
    # Tail = 39 frozen rows (net 0.01) + the live 0.05 -> mean 0.011 %.
    assert h01["net"] == pytest.approx((39 * 0.01 + 0.05) / 40 * 100)
    assert h01["percentile"] is None

    frozen_only = gv._daily_override(None, prefix)
    assert frozen_only is not None
    assert frozen_only["h02"]["basis"] == "frozen:h02"
    assert frozen_only["h02"]["window"] == "holdout"
    assert frozen_only["h02"]["net"] == pytest.approx(1.0)

    # No prefix and no rows -> no invented window.
    assert gv._daily_override(None, None) is None


def test_frozen_audit_windows_stay_untouched_by_overrides() -> None:
    gv = _load_generate_vault()
    before = {f["id"]: json.loads(json.dumps(f)) for f in fv.FACTOR_DEFS}
    ov = gv._daily_override(
        {"meta": {"asOf": "2026-11-10"}, "appended": {}},
        _prefix(),
    )
    assert ov
    rows = {r["id"]: r for r in fv.build_factors(overrides=ov)}
    for fid in ("h01", "h02", "h03"):
        after = next(f for f in fv.FACTOR_DEFS if f["id"] == fid)
        assert after["windows"] == before[fid]["windows"]
        history = {h["window"]: h for h in rows[fid]["history"]}
        for label, window in after["windows"].items():
            if label not in history:
                continue
            assert history[label]["net"] == pytest.approx(window["net"])
            assert history[label]["trades"] == window["n"]
        # Only the rolling window moves; no percentile -> never revived.
        assert rows[fid]["percentile"] is None
        assert rows[fid]["status"] in ("cold", "watch")
