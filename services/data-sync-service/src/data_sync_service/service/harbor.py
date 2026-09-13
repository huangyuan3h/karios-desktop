"""港湾 (Harbor) baseline: S-3 stock core + idle-cash ETF parking.

Validated in B11 (`docs/backtests/stable/etf-parking-baseline-2026-09-13.md`):
park the whole idle fraction in the mom60+MA200 argmax ETF (GOLD/OIL/NASDAQ/
BOND10), causal trail8 exit, 0.05%/side cost; no idle floor, no STOCK gate.

`build_harbor_timeline` mirrors the row shape of
`pick_strong_track.build_mom_compare_timeline` so the UI chart keeps working:
  navBase   = S-3 engine NAV (stock core alone)
  navSingle = Harbor NAV (engine return + idle * parking return)
Used by GET /api/backtest/timeline?strategy=harbor.
"""

from __future__ import annotations

import bisect
import csv
from pathlib import Path
from typing import Any

LOOKBACK = 60
MA_WINDOW = 200
TRAIL_PCT = 8.0
COST = 0.0005
MODE = "harbor"
STRATEGY_LABEL = "港湾"

MULTI_TS: dict[str, str] = {
    "GOLD": "518880.SH",
    "OIL": "513350.SH",
    "NASDAQ": "513110.SH",
    "BOND10": "511260.SH",
}
NASDAQ_ALIASES = ("513110.SH", "513100.SH")


def load_etf_closes() -> dict[str, dict[str, float]]:
    """Adjusted ETF closes from the research panel; fall back to `daily`."""
    wanted = {*MULTI_TS.values(), *NASDAQ_ALIASES}
    csv_path = Path(__file__).resolve().parents[3] / "data" / "etf" / "etf_daily.csv"
    out: dict[str, dict[str, float]] = {}
    if csv_path.exists():
        with csv_path.open(newline="", encoding="utf-8") as fh:
            for row in csv.DictReader(fh):
                ts = str(row.get("ts_code") or "")
                if ts not in wanted:
                    continue
                d = str(row.get("trade_date") or "")
                d = f"{d[:4]}-{d[4:6]}-{d[6:8]}"
                try:
                    c = float(row.get("close_adj") or 0)
                except (TypeError, ValueError):
                    continue
                if c > 0:
                    out.setdefault(ts, {})[d] = c
    if out:
        return out
    from data_sync_service.service.pick_strong_track import fetch_etf_closes

    for key, ts in MULTI_TS.items():
        out[ts] = fetch_etf_closes().get(key) or {}
    return out


NAMES: dict[str, str] = {
    "GOLD": "华安黄金ETF",
    "OIL": "富国油气QDII",
    "NASDAQ": "纳指100QDII",
    "BOND10": "10年国债ETF",
}


def pick_parking(
    etf_close: dict[str, dict[str, float]],
    as_of: str,
    *,
    bond_ungated: bool = False,
    days_by_ts: dict[str, list[str]] | None = None,
    exclude_keys: set[str] | None = None,
) -> dict[str, Any] | None:
    """Canonical Harbor pick as of the ``as_of`` close (single source).

    - NASDAQ: best alias among 513110/513100; the returned ``ts`` IS that alias
    - momentum = close(as_of) / close(LOOKBACK sessions earlier) - 1
    - gate: close(as_of) >= MA200 (BOND10 may bypass with ``bond_ungated``)
    - coverage: at least 3 of the 4 keys need >= MA_WINDOW bars
    - ``exclude_keys``: keys made ineligible (used by the cooldown timer);
      ``all_mom``/``all_above`` still report their raw data.
    """
    days_by_ts = days_by_ts or {ts: sorted(mp.keys()) for ts, mp in etf_close.items()}
    all_mom: dict[str, float | None] = {}
    all_above: dict[str, bool] = {}
    chosen: dict[str, tuple[float, str]] = {}
    covered = 0
    for key, ts in MULTI_TS.items():
        aliases = NASDAQ_ALIASES if key == "NASDAQ" else (ts,)
        rows_alias: list[tuple[float, str, bool]] = []
        for a in aliases:
            mp = etf_close.get(a) or {}
            ds = days_by_ts.get(a) or []
            i = bisect.bisect_right(ds, as_of) - 1
            if i < MA_WINDOW - 1:
                continue
            ago_i = i - LOOKBACK
            if ago_i < 0 or not mp[ds[ago_i]]:
                continue
            close = mp[ds[i]]
            ma200 = sum(mp[ds[j]] for j in range(i - MA_WINDOW + 1, i + 1)) / MA_WINDOW
            mom = close / mp[ds[ago_i]] - 1.0
            above = close >= ma200 or (bond_ungated and key == "BOND10")
            rows_alias.append((mom, a, above))
        if rows_alias:
            covered += 1
        above_rows = [r for r in rows_alias if r[2]]
        disp = max(above_rows or rows_alias, key=lambda r: r[0]) if rows_alias else None
        if disp is None:
            all_mom[key] = None
            all_above[key] = False
            continue
        all_mom[key] = round(disp[0] * 100, 2)
        all_above[key] = disp[2]
        eligible = [r for r in above_rows if not (exclude_keys and key in exclude_keys)]
        if eligible:
            best = max(eligible, key=lambda r: r[0])
            chosen[key] = (best[0], best[1])

    if covered < 3 or not chosen:
        return None
    pick_key = max(chosen, key=lambda k: chosen[k][0])
    mom, pick_ts = chosen[pick_key]
    mp = etf_close.get(pick_ts) or {}
    ds = days_by_ts.get(pick_ts) or []
    i = bisect.bisect_right(ds, as_of) - 1
    close = mp[ds[i]]
    ma200 = sum(mp[ds[j]] for j in range(i - MA_WINDOW + 1, i + 1)) / MA_WINDOW
    return {
        "key": pick_key,
        "ts": pick_ts,
        "symbol": f"ETF:{pick_ts.split('.')[0]}",
        "name": NAMES.get(pick_key, pick_key),
        "mom60": round(mom * 100, 2),
        "close": round(close, 3),
        "ma200": round(ma200, 3),
        "above_ma200": True,
        "all_mom": all_mom,
        "all_above": all_above,
    }


def parking_replay(
    etf_close: dict[str, dict[str, float]],
    calendar: list[str],
    *,
    idle_by_day: dict[str, float] | None = None,
    stock_mom_by_day: dict[str, float | None] | None = None,
    min_idle_pct: float = 0.0,
    stock_gate: bool = False,
    bond_ungated: bool = False,
    trail_pct: float = TRAIL_PCT,
    cooldown_days: int = 0,
) -> list[dict[str, Any]]:
    """Canonical Harbor parking state machine (single source).

    Live order: trail the held leg first (a trail day goes to REPO, no same-day
    re-entry), then rotate on a TS change (an alias switch is a different fund
    -> fresh peak). Non-session days (holidays in the engine calendar) are
    dropped. ``parking_ret`` is the held leg's close-to-close return, NOT
    idle-scaled (callers multiply by the idle fraction).

    Optional robustness gates (default off = canonical P1):
    ``min_idle_pct`` (fresh-entry floor), ``stock_gate`` (skip when the S-3
    stock basket mom >= ETF mom), ``bond_ungated`` (BOND10 eligible below MA).

    ``cooldown_days`` (H-PARK-C candidate, default 0 = incumbent): after a
    trail exit, the exited *key* is ineligible for ``cooldown_days`` sessions
    (by key, so NASDAQ aliases share the cooldown); the parking sleeve then
    rotates to the next eligible candidate or sits in REPO.
    """
    sessions = {d for mp in etf_close.values() for d in mp}
    cal = [d for d in calendar if d in sessions]
    days_by_ts = {ts: sorted(mp.keys()) for ts, mp in etf_close.items()}
    cooldown_days = max(0, int(cooldown_days))
    cooldown_until: dict[str, int] = {}
    out: list[dict[str, Any]] = []
    held_key: str | None = None
    held_ts: str | None = None
    peak = 0.0
    for i in range(1, len(cal)):
        day, prev = cal[i], cal[i - 1]
        active_exclude = {k for k, until in cooldown_until.items() if i <= until}
        want = pick_parking(
            etf_close,
            prev,
            bond_ungated=bond_ungated,
            days_by_ts=days_by_ts,
            exclude_keys=active_exclude or None,
        )
        want_key = want["key"] if want else None
        want_ts = want["ts"] if want else None
        if want and stock_gate and stock_mom_by_day is not None:
            sm = stock_mom_by_day.get(prev)
            if sm is not None and want["mom60"] / 100.0 <= sm:
                want_key = want_ts = None
        if want_ts and held_ts is None and min_idle_pct > 0:
            if (idle_by_day or {}).get(prev, 1.0) < min_idle_pct:
                want_key = want_ts = None

        sides = 0
        parking_ret = 0.0
        trail_exit = False
        # 1) trail the held leg at the prev close (exit -> REPO that day)
        if held_ts is not None:
            c = (etf_close.get(held_ts) or {}).get(prev)
            if c:
                peak = max(peak, c)
                if peak > 0 and c < peak * (1.0 - trail_pct / 100.0):
                    if cooldown_days > 0 and held_key:
                        cooldown_until[held_key] = i + cooldown_days
                    held_key = held_ts = None
                    peak = 0.0
                    sides += 1
                    trail_exit = True
        # 2) rotate on a TS change (alias switch = different fund -> fresh peak)
        if not trail_exit and (held_key != want_key or held_ts != want_ts):
            sides += int(held_ts is not None) + int(want_ts is not None)
            held_key, held_ts = want_key, want_ts
            peak = ((etf_close.get(held_ts) or {}).get(prev) or 0.0) if held_ts else 0.0
        # 3) day return of the (possibly new) held leg
        if held_ts is not None:
            c0 = (etf_close.get(held_ts) or {}).get(prev)
            c1 = (etf_close.get(held_ts) or {}).get(day)
            parking_ret = c1 / c0 - 1.0 if c0 and c1 else 0.0
        out.append(
            {
                "date": day,
                "prev": prev,
                "pick_key": held_key or "REPO",
                "pick_ts": held_ts or "GC001",
                "want_key": want_key,
                "want_ts": want_ts,
                "parking_ret": parking_ret,
                "sides": sides,
                "trail_exit": trail_exit,
                "cooldown_active": bool(active_exclude),
            }
        )
    return out


def build_harbor_timeline(
    *,
    calendar: list[str],
    positions_by_day: list[dict[str, Any]],
    engine_nav_by_day: dict[str, float],
    etf_close: dict[str, dict[str, float]] | None = None,
) -> dict[str, Any]:
    """Replay Harbor NAV (engine + idle parking) with UI rows."""
    etf_close = etf_close or load_etf_closes()
    snap_by_day = {str(s.get("date")): s for s in positions_by_day}
    records = parking_replay(etf_close, calendar)

    nav_base = 1.0
    nav_harbor = 1.0
    peak = 1.0
    max_dd = 0.0
    trail_exits = 0
    trades = 0
    rows: list[dict[str, Any]] = []
    prev_syms: set[str] = set()
    prev_map: dict[str, str] = {}

    for rec in records:
        day, prev = str(rec["date"]), str(rec["prev"])
        snap_prev = snap_by_day.get(prev) or {}
        stock_poses: list[dict[str, Any]] = []
        for pos in snap_prev.get("positions") or []:
            entry = str(pos.get("entry_date") or "")
            if entry and day <= entry:
                continue
            stock_poses.append(pos)

        deployed = sum(float(p.get("position_pct") or 0.0) for p in stock_poses)
        idle = max(0.0, 1.0 - min(1.0, deployed))

        cn_cnt = hk_cnt = 0
        stock_syms: list[str] = []
        for pos in stock_poses:
            ts = str(pos.get("ts_code") or "")
            sym = str(pos.get("symbol") or ts)
            if len(stock_syms) < 3:
                stock_syms.append(sym)
            if ts.endswith(".HK") or ts.startswith("HK"):
                hk_cnt += 1
            else:
                cn_cnt += 1

        parking_ret = float(rec["parking_ret"])
        sides = int(rec["sides"])
        if sides:
            trades += 1
        if rec["trail_exit"]:
            trail_exits += 1

        eng_prev = engine_nav_by_day.get(prev)
        eng_day = engine_nav_by_day.get(day)
        r_eng = eng_day / eng_prev - 1.0 if eng_prev and eng_day else 0.0
        nav_base *= 1.0 + r_eng
        nav_harbor *= 1.0 + r_eng + idle * (parking_ret - COST * sides)
        peak = max(peak, nav_harbor)
        max_dd = max(max_dd, (peak - nav_harbor) / peak if peak > 0 else 0.0)

        cur_snapshot = snap_by_day.get(day) or snap_prev
        cur_syms = {
            str(p.get("ts_code") or p.get("symbol") or "")
            for p in (cur_snapshot.get("positions") or [])
        }
        cur_syms = {s for s in cur_syms if s}
        sold = sorted(prev_syms - cur_syms)
        sold_labels = [prev_map.get(ts, ts) for ts in sold]
        prev_syms = cur_syms
        prev_map = {}
        for pos in cur_snapshot.get("positions") or []:
            ts = str(pos.get("ts_code") or pos.get("symbol") or "")
            if ts:
                prev_map[ts] = str(pos.get("symbol") or ts)

        if cn_cnt and hk_cnt:
            stock_market = "A+H"
        elif hk_cnt:
            stock_market = "HK"
        elif cn_cnt:
            stock_market = "A股"
        else:
            stock_market = "空仓"

        rows.append(
            {
                "date": day,
                "deployedPct": round(min(1.0, deployed) * 100, 1),
                "idlePct": round(idle * 100, 1),
                "positions": len(stock_poses),
                "cnPositions": cn_cnt,
                "hkPositions": hk_cnt,
                "stockMarket": stock_market,
                "stockSymbols": stock_syms,
                "exits": sold_labels,
                "exitsCount": len(sold_labels),
                "pick": rec["pick_key"],
                "pickTs": rec["pick_ts"],
                "stockMom": None,
                "parkingPct": round(idle * parking_ret * 100, 2),
                "navBase": round(nav_base, 6),
                "navSingle": round(nav_harbor, 6),
                "navMulti": round(nav_harbor, 6),
                "navBaseReturnPct": round((nav_base - 1) * 100, 2),
                "navSingleReturnPct": round((nav_harbor - 1) * 100, 2),
                "navMultiReturnPct": round((nav_harbor - 1) * 100, 2),
            }
        )

    return {
        "ok": True,
        "mode": MODE,
        "strategy": STRATEGY_LABEL,
        "lookback": LOOKBACK,
        "maWindow": MA_WINDOW,
        "trailPct": TRAIL_PCT,
        "trailExits": trail_exits,
        "parkingTrades": trades,
        "rows": rows,
        "summary": {
            "fusedPct": round((nav_harbor - 1) * 100, 2),
            "basePct": round((nav_base - 1) * 100, 2),
            "maxDdFusedPct": round(max_dd * 100, 1),
            "parkingTrades": trades,
            "trailExits": trail_exits,
        },
    }
