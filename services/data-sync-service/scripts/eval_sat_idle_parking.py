#!/usr/bin/env python3
"""H-SAT-IDLE: causal parking of the habit satellite's idle cash (prereg 2026-09-15).

Sat   = habit S-gap standalone (amp_1430 + C1 3% + same_1430 + gate_1430 + 30bps RT,
        100% notional, 4x25% slots) — frozen, untouched.
Idle  = w_t = clamp(1 - 0.25 * satPositions[t-1], 0, 1) — yesterday's close, causal.
X arms:
  A1  Harbor core (S-3 + idle-cash parking, canonical `parking_replay`, internal costs)
  A2  parking sleeve only (mom60+MA200 argmax + causal trail8, 5bps/side)
  A3  REPO 0.7%/yr (control)
  A1_legacy  same-day label (legacy blend_nav_opportunity convention) — descriptive only
port_ret_t = sat_ret_t + w_t * x_ret_t - transfer_bps/1e4 * |w_t - w_{t-1}|

Verdict (frozen, returns-only premise): K1 = min(dTot over OOS2/train/valid) >= 0
vs satellite standalone; K2 = long dTot >= +100pt. A1 is the only gated arm.

Read-only; Live untouched.
See docs/designs/sat-idle-parking-prereg-2026-09-15.md

Usage:
  cd services/data-sync-service
  PYTHONPATH=src:scripts python3 scripts/eval_sat_idle_parking.py --save-report
  PYTHONPATH=src:scripts python3 scripts/eval_sat_idle_parking.py --windows long
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))
REPORT_DIR = ROOT / "data" / "backtest_reports"

from eval_twin_star_parking import (  # noqa: E402
    COST,
    _fmt,
    _load_etf_closes,
    _parking_core_by_day,
    _stats,
)
from run_walk_forward import WINDOWS  # noqa: E402

from data_sync_service.service.harbor import parking_replay  # noqa: E402
from data_sync_service.service.state_bucket_track import (  # noqa: E402
    FILL_SAME_1430,
    load_sgap_context,
    replay_sgap_from_context,
)

WINS = ("OOS2", "train", "valid", "long")
DESCRIPTIVE = ("holdout",)
SLOT_PCT = 0.25
TRANSFER_BPS = (5.0, 15.0)
REPO_ANNUAL = 0.007
GATED_ARM = "A1"


def _sat_book(start: str, end: str) -> dict:
    ctx = load_sgap_context(start, end)
    sat = replay_sgap_from_context(
        ctx,
        start=start,
        end=end,
        skip_t1_limit=True,
        pool_mode="strict",
        max_pos=4,
        position_pct=SLOT_PCT,
        body=3,
        fill_mode=FILL_SAME_1430,
        fill_hhmm="1430",
        exit_hhmm="1430",
        max_open_to_1430_pct=0.03,
        rank_key="amp_1430",
        gate_1430=True,
    )
    rows = sat.get("rows") or []
    dates = [str(r["date"]) for r in rows]
    nav = [float(r.get("satNav") or 1.0) for r in rows]
    pos = [int(r.get("satPositions") or 0) for r in rows]
    if nav and nav[0] > 0:
        base = nav[0]
        nav = [v / base for v in nav]
    return {"dates": dates, "nav": nav, "pos": pos, "summary": sat.get("summary") or {}}


def _idle_prev(pos: list[int], *, lag: bool = True) -> list[float]:
    """Cash share of the satellite NAV (0 when the book is levered/full)."""
    n = len(pos)
    out = [0.0] * n
    for t in range(1, n):
        p = pos[t - 1] if lag else pos[t]
        out[t] = min(1.0, max(0.0, 1.0 - SLOT_PCT * p))
    return out


def _align_day_nav(day_nav: dict[str, float], dates: list[str]) -> list[float]:
    cal = sorted(day_nav)
    out: list[float] = []
    last, j = 1.0, 0
    for d in dates:
        while j < len(cal) and cal[j] <= d:
            last = day_nav[cal[j]]
            j += 1
        out.append(last)
    return out


def _sleeve_nav(px: dict[str, dict[str, float]], dates: list[str]) -> list[float]:
    """Parking sleeve only: idle = 1 every session, canonical state machine."""
    recs = parking_replay(px, dates, idle_by_day=None)
    day_nav = {dates[0]: 1.0} if dates else {}
    nav = 1.0
    for rec in recs:
        nav *= 1.0 + (float(rec["parking_ret"]) - COST * int(rec["sides"]))
        day_nav[str(rec["date"])] = nav
    return _align_day_nav(day_nav, dates)


def _repo_nav(dates: list[str]) -> list[float]:
    daily = REPO_ANNUAL / 252.0
    out = [1.0]
    for _ in range(1, len(dates)):
        out.append(out[-1] * (1.0 + daily))
    return out


def _compose(
    sat_nav: list[float],
    w: list[float],
    x_nav: list[float],
    transfer_bps: float,
) -> list[float]:
    n = min(len(sat_nav), len(w), len(x_nav))
    out = [1.0]
    for t in range(1, n):
        r_sat = sat_nav[t] / sat_nav[t - 1] - 1.0 if sat_nav[t - 1] else 0.0
        r_x = x_nav[t] / x_nav[t - 1] - 1.0 if x_nav[t - 1] else 0.0
        cost = transfer_bps / 1e4 * abs(w[t] - w[t - 1])
        out.append(out[-1] * (1.0 + r_sat + w[t] * r_x - cost))
    return out


def _corr(a: list[float], b: list[float], mask: list[bool] | None = None) -> float | None:
    pairs = [
        (x, y)
        for i, (x, y) in enumerate(zip(a, b))
        if (mask is None or mask[i]) and x is not None and y is not None
    ]
    if len(pairs) < 3:
        return None
    x = np.array([p[0] for p in pairs])
    y = np.array([p[1] for p in pairs])
    if x.std() == 0 or y.std() == 0:
        return None
    return round(float(np.corrcoef(x, y)[0, 1]), 2)


def _rets(nav: list[float]) -> list[float]:
    return [0.0] + [
        (nav[i] / nav[i - 1] - 1.0) if nav[i - 1] else 0.0 for i in range(1, len(nav))
    ]


def _year_deltas(dates: list[str], a: list[float], b: list[float]) -> dict[str, float]:
    """Per-year (a - b) total-return delta, in pt (a = variant, b = base)."""
    out: dict[str, list[int]] = {}
    for i, d in enumerate(dates):
        out.setdefault(d[:4], []).append(i)
    res: dict[str, float] = {}
    for y, idx in out.items():
        if len(idx) < 2:
            continue
        i0, i1 = idx[0], idx[-1]
        base = i0 - 1 if i0 > 0 else i0
        a_ret = (a[i1] / a[base] - 1.0) * 100 if a[base] else 0.0
        b_ret = (b[i1] / b[base] - 1.0) * 100 if b[base] else 0.0
        res[y] = round(a_ret - b_ret, 1)
    return res


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--windows", default=",".join((*WINS, *DESCRIPTIVE)))
    ap.add_argument("--save-report", action="store_true")
    args = ap.parse_args()
    wins = [w.strip() for w in args.windows.split(",") if w.strip()]

    px = _load_etf_closes()
    results: dict[str, dict] = {}
    for wname in wins:
        s, e = WINDOWS[wname]
        print(f"=== {wname} ({s}~{e}) ===", flush=True)
        sat = _sat_book(s, e)
        dates, sat_nav, pos = sat["dates"], sat["nav"], sat["pos"]
        core_by_day = _parking_core_by_day(px, s, e)
        core_nav = _align_day_nav(core_by_day, dates)
        sleeve_nav = _sleeve_nav(px, dates)
        repo_nav = _repo_nav(dates)
        n = min(len(dates), len(sat_nav), len(core_nav), len(sleeve_nav))
        dates, sat_nav, pos = dates[:n], sat_nav[:n], pos[:n]
        core_nav, sleeve_nav, repo_nav = core_nav[:n], sleeve_nav[:n], repo_nav[:n]

        w_causal = _idle_prev(pos, lag=True)
        w_same = _idle_prev(pos, lag=False)
        arms: dict[str, dict] = {}
        a0 = _stats(sat_nav)
        for bps in TRANSFER_BPS:
            suffix = "" if bps == TRANSFER_BPS[0] else f"_{int(bps)}bps"
            arms[f"A1{suffix}"] = _compose(sat_nav, w_causal, core_nav, bps)
        arms["A1_legacy"] = _compose(sat_nav, w_same, core_nav, TRANSFER_BPS[0])
        arms["A2"] = _compose(sat_nav, w_causal, sleeve_nav, TRANSFER_BPS[0])
        arms["A2_15bps"] = _compose(sat_nav, w_causal, sleeve_nav, TRANSFER_BPS[1])
        arms["A3"] = _compose(sat_nav, w_causal, repo_nav, TRANSFER_BPS[0])

        row: dict[str, dict] = {
            "satellite": {**_stats(sat_nav), "delta_total": 0.0, "delta_sharpe": 0.0, "delta_mdd": 0.0},
            "_x_standalone": {
                "core": _stats(core_nav),
                "sleeve": _stats(sleeve_nav),
                "repo": _stats(repo_nav),
            },
        }
        sat_r = _rets(sat_nav)
        core_r = _rets(core_nav)
        idle_mask = [w_causal[i] > 0 for i in range(n)]
        for aid, nav in arms.items():
            m = _stats(nav)
            row[aid] = {
                **m,
                "delta_total": round(m["total_pct"] - a0["total_pct"], 1),
                "delta_sharpe": round(m["sharpe"] - a0["sharpe"], 2),
                "delta_mdd": round(m["max_dd"] - a0["max_dd"], 1),
                "year_deltas": _year_deltas(dates, nav, sat_nav) if wname == "long" else None,
            }
        active_days = sum(1 for p in pos if p > 0)
        w_avg = float(np.mean(w_causal[1:])) if n > 1 else 0.0
        turns = float(np.mean([abs(w_causal[i] - w_causal[i - 1]) for i in range(1, n)])) if n > 1 else 0.0
        row["_occupancy"] = {
            "days": n,
            "pct_active": round(100 * active_days / max(1, n), 1),
            "pct_flat": round(100 * (n - active_days) / max(1, n), 1),
            "avg_positions_active": round(
                float(np.mean([p for p in pos if p > 0])) if active_days else 0.0, 2
            ),
            "avg_idle_share": round(w_avg, 3),
            "avg_daily_turnover": round(turns, 4),
            "fills": sat["summary"].get("fillCount"),
            "corr_core_sat_all": _corr(core_r, sat_r),
            "corr_core_sat_idle": _corr(core_r, sat_r, idle_mask),
        }
        results[wname] = row

        print(f"  satellite       {_fmt(a0)}", flush=True)
        print(
            "  X standalone    core "
            f"{_fmt(row['_x_standalone']['core'])} | sleeve {_fmt(row['_x_standalone']['sleeve'])}",
            flush=True,
        )
        for aid, rec in row.items():
            if aid.startswith("_"):
                continue
            if aid == "satellite":
                continue
            yd = rec.get("year_deltas")
            extra = f"  years {yd}" if yd else ""
            print(
                f"  {aid:<14} {_fmt(rec)}  Δtot {rec['delta_total']:+7.1f} "
                f"Δsr {rec['delta_sharpe']:+.2f} Δdd {rec['delta_mdd']:+.1f}{extra}",
                flush=True,
            )
        occ = row["_occupancy"]
        print(
            f"  occupancy active {occ['pct_active']}% / avg pos {occ['avg_positions_active']} "
            f"/ idle {occ['avg_idle_share'] * 100:.1f}% / turn {occ['avg_daily_turnover'] * 100:.1f}%/d "
            f"/ corr all {occ['corr_core_sat_all']} idle {occ['corr_core_sat_idle']}",
            flush=True,
        )
        del core_by_day

    verdict: dict[str, object] = {}
    if all(w in results for w in ("OOS2", "train", "valid")):
        d3 = [results[w][GATED_ARM]["delta_total"] for w in ("OOS2", "train", "valid")]
        k1 = min(d3) >= 0
        d_long = results["long"][GATED_ARM]["delta_total"] if "long" in results else None
        k2 = d_long is not None and d_long >= 100.0
        verdict = {
            "arm": GATED_ARM,
            "deltas_3w": d3,
            "k1_no_window_dilution": k1,
            "long_delta": d_long,
            "k2_long_ge_100": k2,
            "pass": bool(k1 and k2),
            "note": (
                "returns-only premise; K3 (MDD/Sharpe) recorded, non-binding; "
                "PASS = satellite-anchored capital structure candidate only "
                "(NOT Harbor+satellite, H-SAT-W stays REJECT)"
            ),
        }
        print("\n## Verdict (frozen prereg)\n")
        print(f"  K1 (3w Δtot all ≥ 0): {k1} {d3}")
        print(f"  K2 (long Δtot ≥ +100): {k2} ({d_long})")
        print(f"  → {'PASS' if verdict['pass'] else 'REJECT'}")

    if args.save_report:
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        path = REPORT_DIR / "sat_idle_parking_2026-09-15.json"
        path.write_text(
            json.dumps(
                {
                    "tag": "sat-idle-2026-09-15",
                    "protocol": (
                        "frozen habit sat (amp_1430 + C1 3% + same_1430 + gate_1430 + 30bps RT, "
                        "4x25% slots) + causal idle parking w_t=1-0.25*satPositions[t-1]; "
                        "A1=Harbor core (parking_replay), A2=sleeve only, A3=REPO, "
                        "A1_legacy=same-day label (descriptive); transfer 5bps/side "
                        "(15bps sensitivity); prereg docs/designs/sat-idle-parking-prereg-2026-09-15.md"
                    ),
                    "windows": results,
                    "verdict": verdict,
                    "as_of": datetime.now(UTC).isoformat(),
                },
                ensure_ascii=False,
                indent=1,
            ),
            encoding="utf-8",
        )
        print(f"\nreport: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
