#!/usr/bin/env python3
"""H-SAT-IDLE-INDEXPOOL: index rotation pool joins the satellite parking pool.

Prereg docs/designs/sat-idle-indexpool-prereg-2026-09-21.md (frozen 2026-09-21).

Park asset = union pool {GOLD, OIL, NASDAQ, BOND10, SSE300 000300, STAR50 000688}
through the SAME canonical `harbor.parking_replay` (mom60+MA200 argmax, causal
trail8, next-day, 5bps/side, coverage gate >=3). Index closes from `index_daily`.
Satellite frozen; w_t = cashShare(T-1) causal.
port_ret_t = sat_ret_t + w_t * park_ret_t - 5bps/1e4 * |w_t - w_{t-1}|

Verdict (frozen):
  K1 A_ip 3w dTot vs standalone all >= 0
  K2 A_ip long dTot vs standalone >= +100pt
  K3 A_ip long MDD >= A2 long MDD (not deeper)
  K4 A_ip long Sharpe > A2 long Sharpe
  PASS = K1 & K2 & K3 & K4

Read-only; Live untouched.

Usage:
  cd services/data-sync-service
  PYTHONPATH=src:scripts python3 scripts/eval_sat_idle_indexpool.py --save-report
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))
REPORT_DIR = ROOT / "data" / "backtest_reports"

from eval_sat_idle_parking import (  # noqa: E402
    DESCRIPTIVE,
    TRANSFER_BPS,
    WINS,
    _align_day_nav,
    _compose,
    _fmt,
    _sat_book,
    _sleeve_nav,
    _stats,
    _true_cash_share,
    _year_deltas,
)
from run_walk_forward import WINDOWS  # noqa: E402

from data_sync_service.db import get_connection  # noqa: E402
from data_sync_service.service import harbor  # noqa: E402

INDEX_KEYS = {"SSE300": "000300.SH", "STAR50": "000688.SH"}
BASE_MULTI_TS = dict(harbor.MULTI_TS)
GATED_ARM = "A_ip_true"
REF_ARM = "A2_true"


def _load_index_closes(code: str) -> dict[str, float]:
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT trade_date, close FROM index_daily WHERE ts_code=%s ORDER BY trade_date",
            (code,),
        )
        return {str(r[0]): float(r[1]) for r in cur.fetchall() if r[1] is not None}


def _union_sleeve_nav(px: dict, dates: list[str]) -> list[float]:
    """Union-pool parking NAV on ``dates`` via the canonical state machine."""
    from eval_twin_star_parking import COST

    harbor.MULTI_TS = {**BASE_MULTI_TS, **INDEX_KEYS}
    try:
        recs = harbor.parking_replay(px, dates, idle_by_day=None)
    finally:
        harbor.MULTI_TS = dict(BASE_MULTI_TS)
    day_nav = {dates[0]: 1.0} if dates else {}
    nav = 1.0
    for rec in recs:
        nav *= 1.0 + (float(rec["parking_ret"]) - COST * int(rec["sides"]))
        day_nav[str(rec["date"])] = nav
    return _align_day_nav(day_nav, dates)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--windows", default=",".join((*WINS, *DESCRIPTIVE)))
    ap.add_argument("--save-report", action="store_true")
    args = ap.parse_args()
    wins = [w.strip() for w in args.windows.split(",") if w.strip()]

    from eval_twin_star_parking import _load_etf_closes

    base_px = _load_etf_closes()
    idx_px = {ts: _load_index_closes(ts) for ts in INDEX_KEYS.values()}
    for ts, mp in idx_px.items():
        ds = sorted(mp)
        print(f"[universe] {ts}: {len(mp)} closes {ds[0]}..{ds[-1]}", flush=True)
    union_px = {**base_px, **idx_px}

    results: dict[str, dict] = {}
    for wname in wins:
        s, e = WINDOWS[wname]
        print(f"=== {wname} ({s}~{e}) ===", flush=True)
        sat = _sat_book(s, e)
        dates, sat_nav, pos = sat["dates"], sat["nav"], sat["pos"]
        sleeve_nav = _sleeve_nav(base_px, dates)
        union_nav = _union_sleeve_nav(union_px, dates)
        n = min(len(dates), len(sleeve_nav), len(union_nav))
        dates, sat_nav, pos = dates[:n], sat_nav[:n], pos[:n]
        sleeve_nav, union_nav = sleeve_nav[:n], union_nav[:n]

        cash_true = _true_cash_share(sat)
        w_true = [0.0] * n
        for i in range(1, n):
            w_true[i] = cash_true[i - 1]

        a0 = _stats(sat_nav)
        arms = {
            "A2_true": _compose(sat_nav, w_true, sleeve_nav, TRANSFER_BPS[0]),
            "A_ip_true": _compose(sat_nav, w_true, union_nav, TRANSFER_BPS[0]),
            "A_ip_true_15bps": _compose(sat_nav, w_true, union_nav, TRANSFER_BPS[1]),
        }
        row: dict[str, dict] = {
            "satellite": {**_stats(sat_nav), "delta_total": 0.0},
            "_x_standalone": {"sleeve": _stats(sleeve_nav), "union": _stats(union_nav)},
        }
        for aid, nav in arms.items():
            m = _stats(nav)
            row[aid] = {
                **m,
                "delta_total": round(m["total_pct"] - a0["total_pct"], 1),
                "year_deltas": _year_deltas(dates, nav, sat_nav) if wname == "long" else None,
            }
        a_ip, a2 = row[GATED_ARM], row[REF_ARM]
        row["_vs_A2"] = {
            "delta_total": round(a_ip["total_pct"] - a2["total_pct"], 1),
            "delta_mdd": round(a_ip["max_dd"] - a2["max_dd"], 1),
            "delta_sharpe": round(a_ip["sharpe"] - a2["sharpe"], 2),
        }
        results[wname] = row

        print(f"  satellite  {_fmt(a0)}", flush=True)
        xs = row["_x_standalone"]
        print(f"  X alone    sleeve {_fmt(xs['sleeve'])} | union {_fmt(xs['union'])}", flush=True)
        for aid, rec in row.items():
            if aid.startswith("_") or aid == "satellite":
                continue
            yd = rec.get("year_deltas")
            extra = f"  years {yd}" if yd else ""
            print(
                f"  {aid:<16} {_fmt(rec)}  Δtot {rec['delta_total']:+7.1f}{extra}",
                flush=True,
            )
        v = row["_vs_A2"]
        print(
            f"  A_ip_true vs A2_true: Δtot {v['delta_total']:+.1f} Δmdd {v['delta_mdd']:+.1f} "
            f"Δsr {v['delta_sharpe']:+.2f}",
            flush=True,
        )

    verdict: dict[str, object] = {}
    if all(w in results for w in ("OOS2", "train", "valid", "long")):
        d3 = [results[w][GATED_ARM]["delta_total"] for w in ("OOS2", "train", "valid")]
        dl = results["long"][GATED_ARM]["delta_total"]
        mdd_gain = results["long"]["_vs_A2"]["delta_mdd"]
        sr_gain = results["long"]["_vs_A2"]["delta_sharpe"]
        k1 = min(d3) >= 0
        k2 = dl >= 100.0
        k3 = mdd_gain >= 0
        k4 = sr_gain > 0
        verdict = {
            "arm": GATED_ARM,
            "deltas_3w": d3,
            "k1_no_window_dilution": k1,
            "long_delta": dl,
            "k2_long_ge_100": k2,
            "long_mdd_vs_A2": mdd_gain,
            "k3_mdd_not_deeper": k3,
            "long_sharpe_vs_A2": sr_gain,
            "k4_sharpe_better": k4,
            "pass": bool(k1 and k2 and k3 and k4),
            "note": "index rotation pool joins the parking pool; candidate only, Live unchanged",
        }
        print("\n## Verdict (frozen prereg)\n")
        print(f"  K1 (3w Δtot vs standalone all ≥ 0): {k1} {d3}")
        print(f"  K2 (long Δtot vs standalone ≥ +100): {k2} ({dl})")
        print(f"  K3 (long MDD ≥ A2): {k3} ({mdd_gain})")
        print(f"  K4 (long Sharpe > A2): {k4} ({sr_gain})")
        print(f"  → {'PASS' if verdict['pass'] else 'REJECT'}")

    if args.save_report:
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        path = REPORT_DIR / "sat_idle_indexpool_2026-09-21.json"
        path.write_text(
            json.dumps(
                {
                    "tag": "sat-idle-indexpool-2026-09-21",
                    "protocol": (
                        "union parking pool {4 ETFs + SSE300 000300 + STAR50 000688} through "
                        "harbor.parking_replay; satellite frozen; w_t=cashShare(T-1); "
                        "transfer 5bps/side; prereg "
                        "docs/designs/sat-idle-indexpool-prereg-2026-09-21.md"
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
