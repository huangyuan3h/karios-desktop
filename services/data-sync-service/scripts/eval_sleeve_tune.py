#!/usr/bin/env python3
"""Starship-idle sleeve tune (H-SLEEVE-TUNE prereg 2026-09-16).

Experiment-ONLY variant state machine for the parking sleeve. The shared
``harbor.parking_replay`` is NEVER touched (it feeds Live harbor); this file
re-implements the canonical rule (pick/rotate/trail/REPO) with hooks, and
locks the replica to the original (``--parity`` asserts identical NAV).

Arms (single-mechanism each; rotate-gating only, trail untouched):
  V   volume-confirmed momentum: rotate only if entered ETF amt20 > amt60
  B   breakout: rotate only if prev close > prior 20-session high
  H2/H3/H5 rotation hysteresis: rotate only if want.mom - held.mom >= band
      (H arms run the SERVICE machine ``parking_sleeve.hysteresis_parking_replay``
      so eval/product can never drift again; 2026-09-17 audit fix: a blocked
      rotation KEEPS the incumbent leg and never sells to cash)

Diagnostics first (DV/DB gates, DH context); grid/blend per prereg.
Verdict: leg stage (sleeve standalone) then blend stage (A2 vs A2_true).

Usage:
  PYTHONPATH=src:scripts python3 scripts/eval_sleeve_tune.py --save-report
  PYTHONPATH=src:scripts python3 scripts/eval_sleeve_tune.py --windows OOS2,train --parity
"""

from __future__ import annotations

import argparse
import bisect
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))
REPORT_DIR = ROOT / "data" / "backtest_reports"

from eval_twin_star_parking import _stats  # noqa: E402
from run_walk_forward import WINDOWS  # noqa: E402

from data_sync_service.service.harbor import (  # noqa: E402
    LOOKBACK,
    MA_WINDOW,
    MULTI_TS,
    NASDAQ_ALIASES,
    TRAIL_PCT,
)

WINS = ("OOS2", "train", "valid", "long")
DESCRIPTIVE = ("holdout",)
COST = 0.0005
REPO_ANNUAL = 0.007
KEYS = tuple(MULTI_TS)
TS_ALL = tuple({*MULTI_TS.values(), *NASDAQ_ALIASES})


def _load_ohlcv(since: str = "2020-01-01") -> dict[str, dict[str, dict]]:
    from data_sync_service.db import get_connection

    out: dict[str, dict[str, dict]] = {}
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT ts_code, trade_date, open, high, low, close, pre_close, amount "
                "FROM daily WHERE ts_code = ANY(%s) AND trade_date >= %s ORDER BY 1, 2",
                (list(TS_ALL), since),
            )
            for ts, d, o, h, lo, c, pc, amt in cur.fetchall():
                out.setdefault(ts, {})[str(d)[:10]] = {
                    "open": float(o or 0), "high": float(h or 0), "low": float(lo or 0),
                    "close": float(c or 0), "pre": float(pc or 0), "amt": float(amt or 0),
                }
    return out


def _load_px() -> dict[str, dict[str, float]]:
    """Authoritative closes: fund-adj CSV (frozen sleeve basis) + daily tail.

    Tail goes through ``harbor.merge_recent_db_closes`` (single source): the
    DB stores RAW ETF closes while the CSV is adjusted, so appending raw
    closes created basis breaks (2026-09-17 audit). Daily-only OHLCV is used
    for amount/high features only — never for prices (2026-09-16 lesson: a
    2.2% bond-distribution basis break between pipelines flips argmax picks).
    """
    from eval_twin_star_parking import _load_etf_closes

    from data_sync_service.service.harbor import merge_recent_db_closes

    return merge_recent_db_closes(_load_etf_closes(), TS_ALL)


def _mom(closes: dict[str, float], days: list[str], i: int) -> float | None:
    ago = i - LOOKBACK
    if ago < 0 or not closes.get(days[ago]):
        return None
    return closes[days[i]] / closes[days[ago]] - 1.0


def _ma(closes: dict[str, float], days: list[str], i: int, n: int) -> float | None:
    if i < n - 1:
        return None
    return sum(closes[days[j]] for j in range(i - n + 1, i + 1)) / n


def sleeve_replay(
    px: dict[str, dict[str, float]],
    ohlcv: dict,
    calendar: list[str],
    *,
    mode: str = "canonical",
    trail_pct: float = TRAIL_PCT,
) -> list[dict]:
    """Variant state machine for the V/B arms. ``canonical`` must equal
    parking_replay NAV.

    H arms are NOT here: they call the service machine
    (``parking_sleeve.hysteresis_parking_replay``, ``_hyst_recs`` below) so the
    prereg semantics (blocked rotation KEEPS the incumbent leg) can never drift
    between the eval and the product/shadow code paths.

    Prices (mom/MA/trail/returns) ALWAYS come from ``px`` (fund-adj CSV =
    frozen sleeve basis); ``ohlcv`` (daily) feeds amount/high features only.
    """
    closes = px
    sessions = {d for m in closes.values() for d in m}
    cal = [d for d in calendar if d in sessions]
    out: list[dict] = []
    held_key: str | None = None
    held_ts: str | None = None
    peak = 0.0
    for i in range(1, len(cal)):
        day, prev = cal[i], cal[i - 1]
        want = _pick_day(closes, prev)
        sides = 0
        ret = 0.0
        trail_exit = False
        if held_ts is not None:
            c = closes.get(held_ts, {}).get(prev)
            if c:
                peak = max(peak, c)
                if peak > 0 and c < peak * (1.0 - trail_pct / 100.0):
                    held_key = held_ts = None
                    peak = 0.0
                    sides += 1
                    trail_exit = True
        if not trail_exit and (held_key != (want["key"] if want else None)
                               or held_ts != (want["ts"] if want else None)):
            allow = True
            if want is not None and not _allow_switch(ohlcv, want, prev, mode=mode):
                # Variant gate fired: skip the rotation — KEEP the incumbent
                # leg (prereg semantics), never sell to cash.
                allow = False
            if allow:
                sides += int(held_ts is not None) + int(want is not None)
                if want is not None:
                    held_key, held_ts = want["key"], want["ts"]
                    peak = closes.get(held_ts, {}).get(prev) or 0.0
                else:
                    held_key, held_ts = None, None
                    peak = 0.0
        if held_ts is not None:
            c0 = closes.get(held_ts, {}).get(prev)
            c1 = closes.get(held_ts, {}).get(day)
            ret = c1 / c0 - 1.0 if c0 and c1 else 0.0
        out.append({"date": day, "prev": prev, "pick_key": held_key or "REPO",
                    "pick_ts": held_ts or "GC001",
                    "want_key": (want or {}).get("key") if isinstance(want, dict) else want,
                    "parking_ret": ret, "sides": sides, "trail_exit": trail_exit})
    return out


def _hyst_recs(
    px: dict[str, dict[str, float]], calendar: list[str], *, band: float
) -> list[dict]:
    """H arms: single-source service machine (prereg semantics, 2026-09-17 fix)."""
    from data_sync_service.service.parking_sleeve import hysteresis_parking_replay

    return hysteresis_parking_replay(px, calendar, band=band)


def _pick_day(closes: dict[str, dict[str, float]], day: str) -> dict | None:
    chosen: dict[str, tuple[float, str]] = {}
    covered = 0
    for key in KEYS:
        aliases = NASDAQ_ALIASES if key == "NASDAQ" else (MULTI_TS[key],)
        rows = []
        for a in aliases:
            mp = closes.get(a) or {}
            ds = sorted(mp)
            i = bisect.bisect_right(ds, day) - 1
            if i < MA_WINDOW - 1:
                continue
            ma200 = _ma(mp, ds, i, MA_WINDOW)
            mom = _mom(mp, ds, i)
            if ma200 is None or mom is None:
                continue
            rows.append((mom, a, mp[ds[i]] >= ma200))
        if rows:
            covered += 1
        above = [r for r in rows if r[2]]
        if not above:
            continue
        best = max(above, key=lambda r: r[0])
        chosen[key] = (best[0], best[1])
    if covered < 3 or not chosen:
        return None
    k = max(chosen, key=lambda kk: chosen[kk][0])
    return {"key": k, "mom": chosen[k][0], "ts": chosen[k][1]}


def _amt_regime(ohlcv: dict, ts: str, prev: str) -> float | None:
    m = ohlcv.get(ts) or {}
    ds = sorted(m)
    i = bisect.bisect_right(ds, prev) - 1
    if i < 60:
        return None
    a20 = sum(m[ds[j]]["amt"] for j in range(i - 19, i + 1)) / 20
    a60 = sum(m[ds[j]]["amt"] for j in range(i - 59, i + 1)) / 60
    return a20 / a60 if a60 > 0 else None


def _breakout(ohlcv: dict, ts: str, prev: str) -> bool | None:
    m = ohlcv.get(ts) or {}
    ds = sorted(m)
    i = bisect.bisect_right(ds, prev) - 1
    if i < 21:
        return None
    return m[ds[i]]["close"] > max(m[ds[j]]["high"] for j in range(i - 20, i))


def _allow_switch(ohlcv: dict, want: dict, prev: str, *, mode: str) -> bool:
    if mode == "volume":
        r = _amt_regime(ohlcv, want["ts"], prev)
        return bool(r is not None and r > 1.0)
    if mode == "breakout":
        b = _breakout(ohlcv, want["ts"], prev)
        return bool(b)
    return True


def _nav_from_recs(recs: list[dict], cost: float = COST) -> list[float]:
    # Eval convention (matches _sleeve_nav/_align_day_nav): the window opens
    # FLAT at 1.0 and returns accrue from the first rec. Dropping day 0
    # silently eats the first day's return (here a +10% spike) and flips
    # window totals (2026-09-16 lesson: always open flat, like the engine).
    nav = 1.0
    out = [1.0]
    for r in recs:
        nav *= 1.0 + float(r["parking_ret"]) - cost * int(r["sides"])
        out.append(nav)
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--windows", default=",".join((*WINS, *DESCRIPTIVE)))
    ap.add_argument("--save-report", action="store_true")
    ap.add_argument("--parity", action="store_true",
                    help="assert canonical replica NAV == parking_replay NAV, then exit")
    args = ap.parse_args()
    wins = [w.strip() for w in args.windows.split(",") if w.strip()]

    ohlcv = _load_ohlcv()
    px = _load_px()
    results: dict[str, dict] = {}
    for wname in wins:
        s, e = WINDOWS[wname]
        print(f"=== {wname} ({s}~{e}) ===", flush=True)
        cal = [d for d in sorted(
            {dd for m in px.values() for dd in m}) if s <= d <= e]
        canon = sleeve_replay(px, ohlcv, cal)
        if args.parity:
            from data_sync_service.service.harbor import parking_replay as _ref_replay

            ref_recs = _ref_replay(px, cal)
            a = _nav_from_recs(canon)
            b = _nav_from_recs([{"parking_ret": r["parking_ret"], "sides": r["sides"]}
                                for r in ref_recs])
            assert len(a) == len(b), (len(a), len(b))
            assert max(abs(x - y) for x, y in zip(a, b, strict=True)) < 1e-9
            print(f"  parity OK ({len(a)} sessions)", flush=True)
            continue
        nav_c = _nav_from_recs(canon)
        diag = _diagnose(px, ohlcv, canon)
        print(f"  canonical {_stats(nav_c)}", flush=True)
        print(f"  DV vol Hi {diag['vol_hi']} Lo {diag['vol_lo']} | "
              f"DB bo {diag['bo_yes']} nbo {diag['bo_no']} | "
              f"DH gaps p50 {diag['gap_p50']} n={diag['gap_n']}", flush=True)
        v_open = diag["vol_lo"][0] - diag["vol_hi"][0] < -1.0 and min(
            diag["vol_lo"][1], diag["vol_hi"][1]) >= 20
        b_open = diag["bo_no"][0] - diag["bo_yes"][0] < -1.0 and min(
            diag["bo_no"][1], diag["bo_yes"][1]) >= 20
        print(f"  gates: Arm-V {'OPEN' if v_open else 'VOID'} | "
              f"Arm-B {'OPEN' if b_open else 'VOID'} | Arm-H OPEN (grid)", flush=True)
        variants: dict[str, list[float]] = {}
        if v_open:
            variants["V"] = _nav_from_recs(sleeve_replay(
                px, ohlcv, cal, mode="volume"))
        if b_open:
            variants["B"] = _nav_from_recs(sleeve_replay(
                px, ohlcv, cal, mode="breakout"))
        for band, aid in ((0.02, "H2"), (0.03, "H3"), (0.05, "H5")):
            variants[aid] = _nav_from_recs(_hyst_recs(px, cal, band=band))
        base = _stats(nav_c)
        rec: dict[str, dict] = {"_diag": diag, "_base": base}
        for aid, nav in variants.items():
            m = _stats(nav)
            rec[aid] = {**m, "delta_total": round(m["total_pct"] - base["total_pct"], 1),
                        "delta_mdd": round(m["max_dd"] - base["max_dd"], 1),
                        "delta_sharpe": round(m["sharpe"] - base["sharpe"], 2)}
            print(f"  {aid:<4} {_fmt_leg(m)}  dTot {rec[aid]['delta_total']:+.1f} "
                  f"dSr {rec[aid]['delta_sharpe']:+.2f} dDd {rec[aid]['delta_mdd']:+.1f}",
                  flush=True)
        results[wname] = rec
    if args.parity:
        return 0
    _verdict(results)
    _blend_stage(results, px, ohlcv, wins)
    if args.save_report:
        import json
        from datetime import UTC, datetime

        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        out = {"generatedAt": datetime.now(UTC).isoformat(), "windows": results,
               "prereg": "docs/designs/sleeve-tune-prereg-2026-09-16.md"}
        (REPORT_DIR / "sleeve_tune_2026-09-16.json").write_text(
            json.dumps(out, ensure_ascii=False, indent=2, default=str))
        print("report -> sleeve_tune_2026-09-16.json")
    return 0


def _blend_stage(
    results: dict[str, dict], px: dict, ohlcv: dict, wins: list[str],
) -> None:
    """A2(variant sleeve) vs A2_true blend check (prereg blend stage).

    Satellite leg + cash-share weights are identical for every arm; only the
    parked leg's NAV series changes. 15bps sensitivity included.
    """
    from eval_sat_idle_parking import (
        TRANSFER_BPS,
        _compose,
        _sat_book,
        _true_cash_share,
    )

    print("\n## blend stage (A2_Hx vs A2_true: long dTot>=+10 & valid>=-5 & 15bps>0)")
    for wname in wins:
        s, e = WINDOWS[wname]
        rec = results.get(wname) or {}
        arms_here = [a for a in ("H2", "H3", "H5") if a in rec]
        if not arms_here:
            continue
        print(f"=== blend {wname} ===", flush=True)
        sat = _sat_book(s, e)
        dates = sat["dates"]
        sat_nav = list(sat["nav"])
        base0 = sat_nav[0] if sat_nav and sat_nav[0] else 1.0
        sat_nav = [v / base0 for v in sat_nav]
        n = len(dates)
        cash_true = _true_cash_share({**sat, "dates": dates, "nav": sat_nav})
        w_true = [0.0] * n
        for i in range(1, n):
            w_true[i] = cash_true[i - 1]
        blend_recs: dict[str, list[float]] = {}
        for aid in arms_here:
            blend_recs[aid] = _nav_from_recs(_hyst_recs(
                px, dates, band={"H2": 0.02, "H3": 0.03, "H5": 0.05}[aid]))
        base_recs = sleeve_replay(px, ohlcv, dates)
        blend_recs["_true"] = _nav_from_recs(base_recs)
        assert set(len(v) for v in blend_recs.values()) == {n}, \
            {k: len(v) for k, v in blend_recs.items()}
        a0 = _stats(_compose(sat_nav, w_true, blend_recs["_true"], TRANSFER_BPS[0]))
        for aid in arms_here:
            for bps, suffix in ((TRANSFER_BPS[0], ""), (TRANSFER_BPS[1], "_15bps")):
                nav = _compose(sat_nav, w_true, blend_recs[aid], bps)
                m = _stats(nav)
                key = f"A2_{aid}{suffix}"
                rec[key] = {**m,
                            "delta_total": round(m["total_pct"] - a0["total_pct"], 1),
                            "delta_mdd": round(m["max_dd"] - a0["max_dd"], 1),
                            "delta_sharpe": round(m["sharpe"] - a0["sharpe"], 2)}
                print(f"  {key:<10} {_fmt_leg(m)}  dTot {rec[key]['delta_total']:+.1f} "
                      f"dSr {rec[key]['delta_sharpe']:+.2f} dDd {rec[key]['delta_mdd']:+.1f}",
                      flush=True)
        print(f"  (A2_true ref {_fmt_leg(a0)})", flush=True)


def _fmt_leg(m: dict) -> str:
    return (f"{m['total_pct']:+.1f}%/{m['cagr']:.1f}%/{m['max_dd']:.1f}%/{m['sharpe']:.2f}")


def _diagnose(px: dict, ohlcv: dict, canon: list[dict]) -> dict:
    """Entry-day regimes vs forward 20-session raw ETF returns + rot gaps."""
    vol_hi: list[float] = []
    vol_lo: list[float] = []
    bo_yes: list[float] = []
    bo_no: list[float] = []
    for r in canon:
        if r["sides"] == 0:
            continue
        ts = r["pick_ts"]
        prev = r["prev"]
        if ts == "GC001" or r["pick_key"] == "REPO":
            continue
        m = ohlcv.get(ts) or {}
        ds = sorted(m)
        i = bisect.bisect_right(ds, prev) - 1
        if i < 60 or i + 20 >= len(ds):
            continue
        pm = px.get(ts) or {}
        pds = sorted(pm)
        j = bisect.bisect_right(pds, prev) - 1
        if j < 0 or j + 20 >= len(pds) or not pm[pds[j]]:
            continue
        fwd = pm[pds[j + 20]] / pm[pds[j]] - 1.0
        a20 = sum(m[ds[j]]["amt"] for j in range(i - 19, i + 1)) / 20
        a60 = sum(m[ds[j]]["amt"] for j in range(i - 59, i + 1)) / 60
        (vol_hi if a20 > a60 else vol_lo).append(fwd)
        is_bo = m[ds[i]]["close"] > max(m[ds[j]]["high"] for j in range(i - 20, i))
        (bo_yes if is_bo else bo_no).append(fwd)
    gaps = _rot_gaps(px, canon)

    def _agg(xs: list[float]) -> tuple[float, int]:
        return (round(sum(xs) * 100, 1), len(xs)) if xs else (0.0, 0)

    import numpy as np

    gs = sorted(gaps)
    return {
        "vol_hi": _agg(vol_hi), "vol_lo": _agg(vol_lo),
        "bo_yes": _agg(bo_yes), "bo_no": _agg(bo_no),
        "gap_p50": round(float(np.median(gs)) * 100, 2) if gs else 0.0,
        "gap_n": len(gs),
    }


def _rot_gaps(closes: dict[str, dict[str, float]], canon: list[dict]) -> list[float]:
    """mom(new) - mom(held) at rotation days (both legs ETFs)."""

    def _mom(ts: str, prev: str) -> float | None:
        mp = closes.get(ts) or {}
        ds = sorted(mp)
        i = bisect.bisect_right(ds, prev) - 1
        if i < LOOKBACK or not mp[ds[i - LOOKBACK]]:
            return None
        return mp[ds[i]] / mp[ds[i - LOOKBACK]] - 1.0

    gaps: list[float] = []
    held: str | None = None
    for r in canon:
        if r["sides"] > 0:
            if held is not None and r["pick_ts"] not in (None, "GC001", held):
                wm = _mom(r["pick_ts"], r["prev"])
                hm = _mom(held, r["prev"])
                if wm is not None and hm is not None:
                    gaps.append(wm - hm)
            held = r["pick_ts"] if r["pick_ts"] != "GC001" else None
    return gaps


def _verdict(results: dict[str, dict]) -> None:
    print("\n## verdict (H-SLEEVE-TUNE: leg long dTot>=+5 & MDD>=-2 & valid dTot>=-3 & dev>=0)")
    for aid in ("V", "B", "H2", "H3", "H5"):
        ds = []
        ok = True
        detail = []
        for w in ("OOS2", "train", "valid", "long"):
            rec = (results.get(w) or {}).get(aid)
            if rec is None:
                ok = False
                detail.append(f"{w}:VOID")
                continue
            ds.append(rec["delta_total"])
            detail.append(f"{w}:{rec['delta_total']:+.1f}")
        base = (results.get("long") or {}).get(aid, {})
        leg = (ok and base.get("delta_total", -99) >= 5.0
               and base.get("delta_mdd", -99) >= -2.0
               and (results.get("valid") or {}).get(aid, {}).get("delta_total", -99) >= -3.0
               and min(ds[0], ds[1]) >= 0)
        print(f"  {aid:<3} {'PASS-leg' if leg else 'REJECT'}  {' '.join(detail)}")


if __name__ == "__main__":
    raise SystemExit(main())
