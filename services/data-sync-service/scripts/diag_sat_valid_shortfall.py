#!/usr/bin/env python3
"""Valid-window shortfall diagnosis (H-SAT-DIAG) — description only, no tuning.

Three questions, frozen caliber (amp_1430/C1/14:30/gate_1430/raw basis/clean
calendar):
  1. Same-day comparison: on the satellite's active days, how did the Harbor
     core do on the SAME days? (all four windows)
  2. Valid monthly path: gate-open days, active days, fills, sat NAV.
  3. Counterfactual: valid with the R-wide gate effectively off (r_wide=0.01)
     — was the gate protection or the problem? (diagnostic only)

Read-only; prints tables, saves nothing.
"""

from __future__ import annotations

import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from eval_twin_star_parking import _load_etf_closes, _parking_core_by_day  # noqa: E402
from run_walk_forward import WINDOWS  # noqa: E402

from data_sync_service.service.state_bucket_track import (  # noqa: E402
    FILL_SAME_1430,
    load_sgap_context,
    replay_sgap_from_context,
)

BASE = dict(
    skip_t1_limit=True, pool_mode="strict", max_pos=4, position_pct=0.25,
    body=3, fill_mode=FILL_SAME_1430, fill_hhmm="1430", exit_hhmm="1430",
    max_open_to_1430_pct=0.03, rank_key="amp_1430", gate_1430=True,
)


def _replay(s: str, e: str, **over):
    ctx = load_sgap_context(s, e)
    return ctx, replay_sgap_from_context(ctx, start=s, end=e, **{**BASE, **over})


def _core_nav_aligned(core_by_day: dict[str, float], rows: list[dict]) -> list[float]:
    cal = sorted(core_by_day)
    out: list[float] = []
    last, j = 1.0, 0
    for r in rows:
        d = str(r["date"])
        while j < len(cal) and cal[j] <= d:
            last = core_by_day[cal[j]]
            j += 1
        out.append(last)
    return out


def main() -> int:
    px = _load_etf_closes()

    print("## 1) 有仓日同日对照（daily 收益加和，未复合）")
    print(f"{'win':<7}{'active':<8}{'satAct%':<10}{'coreAct%':<10}{'coreInact%':<12}{'coreTot%':<10}{'satTot%'}")
    for w in ("OOS2", "train", "valid", "long"):
        s, e = WINDOWS[w]
        _, sat = _replay(s, e)
        rows = sat["rows"]
        core = _core_nav_aligned(_parking_core_by_day(px, s, e), rows)
        sat_nav = [float(r.get("satNav") or 1.0) for r in rows]
        active = 0
        sa = ca = ci = 0.0
        for i in range(1, len(rows)):
            cs = sat_nav[i] / sat_nav[i - 1] - 1 if sat_nav[i - 1] else 0.0
            cc = core[i] / core[i - 1] - 1 if core[i - 1] else 0.0
            if rows[i].get("satActive"):
                active += 1
                sa += cs
                ca += cc
            else:
                ci += cc
        print(f"{w:<7}{active:<8}{sa*100:<+10.1f}{ca*100:<+10.1f}{ci*100:<+12.1f}"
              f"{(core[-1]-1)*100:<+10.1f}{(sat_nav[-1]-1)*100:+.1f}", flush=True)

    print("\n## 2) valid 月度路径（gate 开 / 有仓 / fills / sat 月收益）")
    s, e = WINDOWS["valid"]
    ctx, sat = _replay(s, e)
    rows = sat["rows"]
    mon: dict[str, dict[str, int]] = defaultdict(lambda: {"days": 0, "open": 0, "act": 0})
    fills = [b for b in (sat.get("blotter") or []) if b.get("kind") == "fill"]
    fmon: dict[str, list[float]] = defaultdict(list)
    for r in rows:
        m = str(r["date"])[:7]
        mon[m]["days"] += 1
        mon[m]["open"] += 1 if r.get("gateOpen") else 0
        mon[m]["act"] += 1 if r.get("satActive") else 0
    for b in fills:
        fmon[str(b["exitDate"])[:7]].append(float(b["pnlPct"]))
    nav = [float(r.get("satNav") or 1.0) for r in rows]
    mnav: dict[str, list[float]] = defaultdict(list)
    for r in rows:
        mnav[str(r["date"])[:7]].append(float(r.get("satNav") or 1.0))
    print(f"{'month':<9}{'days':<6}{'gateOpen':<10}{'active':<8}{'fills':<7}{'avgPnl%':<9}{'satRet%'}")
    prev = None
    for m in sorted(mon):
        fs = fmon.get(m, [])
        first, lastv = mnav[m][0], mnav[m][-1]
        base = first if prev is None else prev
        ret = (lastv / base - 1) * 100 if base else 0.0
        print(f"{m:<9}{mon[m]['days']:<6}{mon[m]['open']:<10}{mon[m]['act']:<8}{len(fs):<7}"
              f"{(sum(fs)/len(fs) if fs else 0):<+9.2f}{ret:+.1f}")
        prev = lastv

    print("\n## 3) valid 反事实：闸门基本常开（r_wide=0.01，仅诊断）")
    sat2 = replay_sgap_from_context(ctx, start=s, end=e, **{**BASE, "r_wide": 0.01})
    rows2 = sat2["rows"]
    nav2 = [float(r.get("satNav") or 1.0) for r in rows2]
    print(f"gate~off: total {(nav2[-1]-1)*100:+.1f}%  fills {sat2['summary'].get('fillCount')}  "
          f"active {100*sum(1 for r in rows2 if r.get('satActive'))/len(rows2):.0f}%")
    print(f"default gate: total {(nav[-1]-1)*100:+.1f}%  fills {sat['summary'].get('fillCount')}  "
          f"active {100*sum(1 for r in rows if r.get('satActive'))/len(rows):.0f}%")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
