#!/usr/bin/env python3
"""B3 single-asset weight cap (H-B3-CAP · 2026-09-13).

Caps the inverse-vol risk-budget weights at 50% / 60% (water-filling the
excess to the uncapped assets) to stop the capsize into BOND10 (currently
86%). Standalone + embedded in Homeport M50.
See docs/designs/b3-cap-prereg-2026-09-13.md

Usage:
  cd services/data-sync-service
  PYTHONPATH=src:scripts python3 scripts/eval_b3_cap.py --save-report
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

from eval_harbor_riskbudget import (  # noqa: E402
    COST,
    RP_UNIVERSE,
    WINS,
    _blend_monthly,
    _harbor_nav,
    _load_panel,
    _row,
    _series_on_cal,
    _slice,
)

CAPS = (None, 0.50, 0.60)
STRESS = ("2022-01-01", "2023-12-31")


def _capped_inv_vol(vol: dict[str, float], cap: float) -> dict[str, float]:
    """Inverse-vol weights with a per-asset cap; excess water-fills by 1/sigma."""
    inv = {k: 1.0 / v for k, v in vol.items()}
    out: dict[str, float] = {}
    free = set(vol)
    left = 1.0
    while free:
        tot = sum(inv[k] for k in free)
        raw = {k: inv[k] / tot * left for k in free}
        over = [k for k in free if raw[k] > cap + 1e-12]
        if not over:
            out.update(raw)
            break
        for k in over:
            out[k] = cap
            left -= cap
            free.discard(k)
    return out


def _rp_nav_capped(
    px: dict[str, dict[str, float]], cal: list[str], cost: float, cap: float | None
) -> list[float]:
    series = {ts: _series_on_cal(px.get(ts) or {}, cal) for ts in RP_UNIVERSE}
    w_by_i: dict[int, dict[str, float]] = {}
    for i in range(len(cal)):
        if i < 60:
            w_by_i[i] = {ts: 1.0 / len(RP_UNIVERSE) for ts in RP_UNIVERSE}
            continue
        if cal[i][:7] == cal[i - 1][:7]:
            w_by_i[i] = w_by_i[i - 1]
            continue
        vol = {}
        for ts in RP_UNIVERSE:
            r = [
                series[ts][j] / series[ts][j - 1] - 1
                for j in range(max(1, i - 60), i)
                if series[ts][j - 1]
            ]
            vol[ts] = float(np.std(r)) or 1e-9
        if cap is None:
            inv = {ts: 1.0 / vol[ts] for ts in RP_UNIVERSE}
            tot = sum(inv.values())
            w_by_i[i] = {ts: inv[ts] / tot for ts in RP_UNIVERSE}
        else:
            w_by_i[i] = _capped_inv_vol(vol, cap)
    nav, cur = [1.0], w_by_i[0]
    for i in range(1, len(cal)):
        r = sum(
            cur[ts] * (series[ts][i] / series[ts][i - 1] - 1 if series[ts][i - 1] else 0.0)
            for ts in RP_UNIVERSE
        )
        nav.append(nav[-1] * (1.0 + r))
        if w_by_i[i] != cur:
            turn = sum(abs(w_by_i[i][ts] - cur[ts]) for ts in RP_UNIVERSE) / 2.0
            nav[-1] *= 1.0 - cost * turn
            cur = w_by_i[i]
    return nav


def _key(cap: float | None) -> str:
    return "base" if cap is None else f"cap{int(round(cap * 100))}"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--save-report", action="store_true")
    args = ap.parse_args()

    px = _load_panel()
    res: dict[str, dict] = {}
    legs: dict[str, tuple] = {}
    for w, (s, e) in WINS.items():
        cal, harbor = _harbor_nav(px, s, e)
        rps = {cap: _rp_nav_capped(px, cal, COST, cap) for cap in CAPS}
        m50s = {cap: _blend_monthly(harbor, rps[cap], 0.5, cal, COST) for cap in CAPS}
        legs[w] = (cal, harbor, rps, m50s)
        res[w] = {}
        for cap in CAPS:
            k = _key(cap)
            res[w][f"b3_{k}"] = _row(rps[cap])
            res[w][f"m50_{k}"] = _row(m50s[cap])
        row = "  ".join(
            f"b3[{k}] {res[w][f'b3_{k}']['cagr']:.1f}/{res[w][f'b3_{k}']['mdd']:.1f}/"
            f"{res[w][f'b3_{k}']['sharpe']:.2f}"
            for k in ("base", "cap50", "cap60")
        )
        print(f"  {w:<6} {row}")
        row2 = "  ".join(
            f"m50[{k}] {res[w][f'm50_{k}']['total']:+.1f}/{res[w][f'm50_{k}']['mdd']:.1f}/"
            f"{res[w][f'm50_{k}']['sharpe']:.2f}"
            for k in ("base", "cap50", "cap60")
        )
        print(f"  {'':<6} {row2}")

    def _standalone_pass(k: str) -> bool:
        b, c = res["long"]["b3_base"], res["long"][f"b3_{k}"]
        return (
            c["sharpe"] >= b["sharpe"]
            and c["cagr"] >= b["cagr"] - 0.5
            and c["mdd"] >= b["mdd"] - 3.0
            and all(
                res[w][f"b3_{k}"]["sharpe"] >= res[w]["b3_base"]["sharpe"] - 0.15
                for w in ("OOS2", "train", "valid")
            )
        )

    def _mix_pass(k: str) -> bool:
        b, c = res["long"]["m50_base"], res["long"][f"m50_{k}"]
        return (
            c["sharpe"] >= b["sharpe"]
            and c["mdd"] >= b["mdd"] - 1.0
            and all(
                res[w][f"m50_{k}"]["cagr"] >= res[w]["m50_base"]["cagr"] - 3.0
                for w in ("OOS2", "train", "valid", "long")
            )
        )

    cal_l, _, _, m50s_l = legs["long"]
    stresses = {}
    for cap in CAPS:
        k = _key(cap)
        stresses[k] = _row(_slice(m50s_l[cap], cal_l, *STRESS))
    verdicts = {}
    for k in ("cap50", "cap60"):
        s_ok, m_ok = _standalone_pass(k), _mix_pass(k)
        stress_ok = stresses[k]["mdd"] >= stresses["base"]["mdd"] - 2.0
        verdicts[k] = {
            "standalone": s_ok,
            "mix": m_ok,
            "stress": stress_ok,
            "stress_row": stresses[k],
            "pass": s_ok and m_ok and stress_ok,
        }

    print("\n## verdict (H-B3-CAP · frozen K1-K7)")
    for k, v in verdicts.items():
        flags = " ".join(f"{n}{'ok' if v[n] else 'X'}" for n in ("standalone", "mix", "stress"))
        print(
            f"  {k}: {flags}  stress mdd {v['stress_row']['mdd']:.1f} (base {stresses['base']['mdd']:.1f})"
        )
    both = all(v["pass"] for v in verdicts.values())
    chosen = None
    if both:
        chosen = "cap50"
    elif verdicts["cap50"]["pass"]:
        chosen = "cap50"
    elif verdicts["cap60"]["pass"]:
        chosen = "cap60"
    print(f"  chosen: {chosen or 'none -> incumbent B3 (no cap) stays'}")

    if args.save_report:
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        (REPORT_DIR / "b3_cap_2026-09-13.json").write_text(
            json.dumps(
                {
                    "tag": "b3-cap-2026-09-13",
                    "prereg": "docs/designs/b3-cap-prereg-2026-09-13.md",
                    "caps": [0.5, 0.6],
                    "results": res,
                    "stress": stresses,
                    "verdicts": verdicts,
                    "chosen": chosen,
                    "as_of": datetime.now(UTC).isoformat(timespec="seconds"),
                },
                ensure_ascii=False,
                indent=2,
                default=str,
            ),
            encoding="utf-8",
        )
        print("saved report")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
