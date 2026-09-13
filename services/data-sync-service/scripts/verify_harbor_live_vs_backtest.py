#!/usr/bin/env python3
"""Verify Live Harbor decisions == frozen backtest decisions (PIT replay).

Live side : `multi_asset_sleeve.build_multi_asset_sleeve` replayed day by day
            with `fetch_last_bars` patched to a point-in-time cut (bars <= T),
            maintaining the sleeve state (held leg / entry date) exactly like
            `sleeve_paper_auto` would.
Backtest  : `service.harbor.build_harbor_timeline` rows over the same engine
            calendar, with the SAME price series (DB `daily`) so differences
            are logic/clock, not data source.

Clock mapping: Live decision at T close -> held during T+1; backtest row
(day = T+1).pick was also decided at T close -> held during T+1.

Usage:
  cd services/data-sync-service
  PYTHONPATH=src:scripts python3 scripts/verify_harbor_live_vs_backtest.py
  PYTHONPATH=src:scripts python3 scripts/verify_harbor_live_vs_backtest.py --windows OOS2,train,valid,long
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from run_walk_forward import S3_CONFIG, WINDOWS  # noqa: E402

from data_sync_service.service import multi_asset_sleeve as mas  # noqa: E402
from data_sync_service.service.backtest_engine import (  # noqa: E402
    BacktestConfig,
    BacktestData,
    simulate,
)
from data_sync_service.service.harbor import (  # noqa: E402
    MULTI_TS,
    NASDAQ_ALIASES,
    build_harbor_timeline,
)
from data_sync_service.service.pick_strong_track import fetch_etf_closes  # noqa: E402
from data_sync_service.service.portfolio_nav_sim import engine_nav_by_day_from_run  # noqa: E402

NEUTRAL_BLOCK = {
    "regime": "Weak",
    "panicCooldown": {"active": False},
    "circuitBlocked": False,
    "s3Candidates": [],
    "holdings": [],
}


def _daily_series() -> dict[str, dict[str, float]]:
    """Same source the Live sleeve reads (`daily` table)."""
    base = fetch_etf_closes()  # {key: {date: close}} for the 4 menu keys
    out = {MULTI_TS[key]: {d: float(c) for d, c in series.items()} for key, series in base.items()}
    for alias in NASDAQ_ALIASES:
        if alias not in out:
            import psycopg

            from data_sync_service.config import get_settings

            with psycopg.connect(get_settings().database_url) as conn, conn.cursor() as cur:
                cur.execute(
                    "SELECT trade_date, close FROM daily WHERE ts_code=%s ORDER BY trade_date",
                    (alias,),
                )
                out[alias] = {str(d): float(c) for d, c in cur.fetchall() if c is not None}
    return out


def _bars_cut(series: dict[str, dict[str, float]], as_of: str) -> dict[str, list[dict[str, object]]]:
    """Per-ts bars with date <= as_of (PIT cut), oldest first."""
    cut: dict[str, list[dict[str, object]]] = {}
    for ts, mp in series.items():
        days = sorted(d for d in mp if d <= as_of)
        cut[ts] = [{"date": d, "trade_date": d, "close": mp[d]} for d in days]
    return cut


def _key_for_symbol(symbol: str) -> str:
    bare = symbol.replace("ETF:", "").split(".")[0]
    for key, ts in MULTI_TS.items():
        if bare == ts.split(".")[0]:
            return key
    if bare in {a.split(".")[0] for a in NASDAQ_ALIASES}:
        return "NASDAQ"
    return symbol


def replay_window(
    name: str, start: str, end: str, series: dict[str, dict[str, float]], *, entry_clock: str = "next"
) -> dict:
    cfg = BacktestConfig(start_date=start, end_date=end, **S3_CONFIG)
    data = BacktestData(cfg)
    run = simulate(cfg, data)
    session_days = {d for mp in series.values() for d in mp}
    cal = [d for d in data.calendar if d in session_days]
    eng = engine_nav_by_day_from_run(cal, run.nav_curve)
    bt = build_harbor_timeline(
        calendar=cal,
        positions_by_day=run.positions_by_day,
        engine_nav_by_day=eng,
        etf_close=series,
    )
    bt_pick = {str(r["date"]): str(r.get("pick") or "REPO") for r in bt.get("rows") or []}

    live_hold: dict[str, object] | None = None
    skipped_non_session = 0
    checks: list[tuple[str, str, str]] = []  # (day, live_pick, bt_pick)
    mismatches: list[dict[str, str]] = []
    for i in range(1, len(cal)):
        day = cal[i]
        cut = _bars_cut(series, day)

        def _fb(ts: str, days: int = 260, _cut=cut):
            return _cut.get(ts, [])[-days:]

        with patch.object(mas, "fetch_last_bars", _fb):
            out = mas.build_multi_asset_sleeve(
                day=day,
                cn_block=NEUTRAL_BLOCK,
                holdings_override=[live_hold] if live_hold else [],
            )
        action = str(out.get("action") or "NONE")
        pick = out.get("pick") or {}
        next_day = cal[i + 1] if i + 1 < len(cal) else None
        if action in ("BUY", "ROTATE"):
            live_hold = {
                "symbol": pick.get("symbol"),
                "ts_code": pick.get("ts"),
                "entryDate": next_day if entry_clock == "next" else day,
                "positionPct": out.get("idlePct") or 0,
            }
        elif action in ("SELL_TO_REPO", "DONT_BUY") or pick is None:
            live_hold = None
        # HOLD keeps the leg

        if next_day is None:
            continue
        live_pick = _key_for_symbol(str(live_hold.get("symbol") or "")) if live_hold else "REPO"
        expected = bt_pick.get(next_day, "?")
        checks.append((next_day, live_pick, expected))
        if live_pick != expected:
            mismatches.append({"day": next_day, "live": live_pick, "backtest": expected, "action": action})

    total = len(checks)
    matched = total - len(mismatches)
    rate = 100.0 * matched / total if total else 0.0
    print(f"  {name:<6} days {total:<4} matched {matched:<4} ({rate:.1f}%)  mismatches {len(mismatches)}  non-session-skipped {skipped_non_session}")
    for m in mismatches[:5]:
        print(f"    ✗ {m['day']} live={m['live']} backtest={m['backtest']} (live action {m['action']})")
    return {"window": name, "days": total, "matched": matched, "mismatches": mismatches, "skipped": skipped_non_session}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--windows", default="OOS2,train,valid")
    ap.add_argument("--entry-clock", choices=("next", "decision"), default="next")
    ap.add_argument("--single-nasdaq", action="store_true", help="use only 513110 for NASDAQ on both sides")
    args = ap.parse_args()

    series = _daily_series()
    if args.single_nasdaq:
        series.pop("513100.SH", None)
        mas.CANDIDATES = [c for c in mas.CANDIDATES if c["ts"] != "513100.SH"]
        import data_sync_service.service.harbor as _harbor

        _harbor.NASDAQ_ALIASES = ("513110.SH",)
    print(f"PIT live-vs-backtest replay (daily closes; entry-clock={args.entry_clock})\n")
    results = []
    for name in [w.strip() for w in args.windows.split(",") if w.strip()]:
        start, end = WINDOWS[name]
        results.append(replay_window(name, start, end, series, entry_clock=args.entry_clock))

    total = sum(r["days"] for r in results)
    mism = sum(len(r["mismatches"]) for r in results)
    print(f"\n## verdict: {total - mism}/{total} pick-days matched ({100.0 * (total - mism) / total:.1f}%)")
    return 0 if mism == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
