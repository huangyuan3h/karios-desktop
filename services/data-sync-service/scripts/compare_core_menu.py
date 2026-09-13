#!/usr/bin/env python3
"""Twin-star residual direction 3: core multi-asset menu expansion.

The core (pick-strong) argmaxes mom60 over its STOCK basket plus an ETF menu
of only four assets (GOLD / OIL / NASDAQ / BOND10). More candidate assets =
a larger opportunity set for "the strongest asset", and a deeper bad-regime
menu. This is the one genuinely untested alpha direction on the return engine
(the satellite signal space is frozen).

Only CN-listed ETFs with enough history are testable; the available extras are
equity index ETFs (CSI300 / CSI500 / ChiNext / HSTECH), which overlap the STOCK
leg heavily, so expectations are low. Read-only diagnostic — no Live change.

Bar: three-window core total within 5pt of base; adoption needs all three > base.

Usage:
  cd services/data-sync-service
  PYTHONPATH=src:scripts python3 scripts/compare_core_menu.py --save-report
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import pick_strong_grid as psg  # noqa: E402
from pick_strong_grid import build_nav_from_cache, fetch_etf_closes, warm_window  # noqa: E402

WINDOWS = {
    "OOS2": ("2024-08-01", "2025-08-01"),
    "train": ("2025-08-01", "2026-02-01"),
    "valid": ("2026-03-01", "2026-08-07"),
}
WF_WINDOWS = ("OOS2", "train", "valid")
REPORT_DIR = Path(__file__).resolve().parents[1] / "data" / "backtest_reports"

BASE_MENU = {"GOLD": "518880.SH", "OIL": "513350.SH", "NASDAQ": "513110.SH", "BOND10": "511260.SH"}
EXTRAS = {
    "CSI300": "510300.SH",
    "CSI500": "510500.SH",
    "CHINEXT": "159915.SZ",
    "HSTECH": "513180.SH",
}
UNION = {**BASE_MENU, **EXTRAS}

VARIANTS = (
    {"id": "base", "label": "GOLD/OIL/NASDAQ/BOND (frozen)", "extra": ()},
    {"id": "m_csi", "label": "+CSI300+CSI500", "extra": ("CSI300", "CSI500")},
    {"id": "m_hstech", "label": "+HSTECH", "extra": ("HSTECH",)},
    {"id": "m_all", "label": "+CSI500+ChiNext+HSTECH", "extra": ("CSI500", "CHINEXT", "HSTECH")},
)


def _stats(nav: list[float]) -> dict[str, float]:
    n = len(nav)
    if n < 2 or not nav[0]:
        return {"n_days": n, "total_pct": 0.0, "max_dd": 0.0, "sharpe": 0.0}
    total = (nav[-1] / nav[0] - 1) * 100
    peak = nav[0]
    mdd = 0.0
    for v in nav:
        if v > peak:
            peak = v
        if peak:
            mdd = max(mdd, (peak - v) / peak * 100)
    rets = [nav[i] / nav[i - 1] - 1 for i in range(1, n) if nav[i - 1] > 0]
    sharpe = 0.0
    if len(rets) > 10:
        std = float(np.std(rets))
        if std > 0:
            sharpe = float(np.mean(rets) / std * (252**0.5))
    return {"n_days": n, "total_pct": round(total, 1), "max_dd": round(mdd, 1), "sharpe": round(sharpe, 2)}


def _fmt(m: dict[str, float]) -> str:
    return f"{m['total_pct']:+.1f}/{m['sharpe']:.2f}/{m['max_dd']:.1f}"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--save-report", action="store_true")
    args = ap.parse_args()

    print("Core menu expansion (pick-strong argmax over STOCK + ETF menu)\n", flush=True)
    psg.MULTI_TS = dict(UNION)
    etf_close = fetch_etf_closes()
    print(f"  loaded closes: {sorted(etf_close)}", flush=True)

    results: dict[str, dict] = {}
    for wname, (s, e) in WINDOWS.items():
        print(f"=== {wname} ({s}~{e}) warming S-3 core ... ===", flush=True)
        cache = warm_window(s, e, etf_close)
        out: dict[str, dict] = {}
        for var in VARIANTS:
            menu = dict(BASE_MENU)
            for k in var["extra"]:
                menu[k] = EXTRAS[k]
            psg.MULTI_TS = menu
            r = build_nav_from_cache(
                cache, lookback=60, ma_window=200, min_hold=1, cost=0.0,
                score="mom", top2=False, trail_pct=8.0,
            )
            nav = [float(r["nav"].get(d, 1.0)) for d in cache["calendar"]]
            m = _stats(nav)
            picks = Counter(v for v in r["pick_map"].values() if v not in ("REPO", "STOCK"))
            out[str(var["id"])] = {
                "label": var["label"],
                "stats": m,
                "switches": r["switches"],
                "stockDayPct": r["stockDayPct"],
                "etfPicks": dict(picks),
            }
            print(
                f"  {var['id']:<10} core {_fmt(m)}  switches {r['switches']}  "
                f"stockDays {r['stockDayPct']}%  etfPicks {dict(picks)}",
                flush=True,
            )
        base = out["base"]["stats"]
        for rec in out.values():
            rec["delta_base_pt"] = round(rec["stats"]["total_pct"] - base["total_pct"], 1)
            rec["delta_base_sharpe"] = round(rec["stats"]["sharpe"] - base["sharpe"], 2)
            rec["delta_base_dd"] = round(rec["stats"]["max_dd"] - base["max_dd"], 1)
        results[wname] = out

    print("\n## Core NAV tot/sr/dd (Δ vs base in parens)\n")
    print("| 窗口 | " + " | ".join(v["id"] for v in VARIANTS) + " |")
    print("|" + "|".join(["------"] * len(VARIANTS)) + "|")
    for wname in WINDOWS:
        cells = []
        for v in VARIANTS:
            rec = results[wname][str(v["id"])]
            extra = f" ({rec['delta_base_pt']:+.1f})" if v["id"] != "base" else ""
            cells.append(f"{_fmt(rec['stats'])}{extra}")
        print(f"| {wname} | " + " | ".join(cells) + " |")

    print("\n## Verdict (vs base)\n")
    for v in VARIANTS:
        vid = str(v["id"])
        if vid == "base":
            continue
        d_tot = [results[w][vid]["delta_base_pt"] for w in WF_WINDOWS]
        d_sr = [results[w][vid]["delta_base_sharpe"] for w in WF_WINDOWS]
        d_dd = [results[w][vid]["delta_base_dd"] for w in WF_WINDOWS]
        flags = []
        if any(d < -5.0 for d in d_tot):
            flags.append("REJECT/total")
        if any(s < 0 for s in d_sr):
            flags.append("worse_sharpe")
        if any(d > 0 for d in d_dd):
            flags.append("worse_dd")
        tag = "+".join(flags) if flags else ("PASS+" if all(d > 0 for d in d_tot) else "PASS")
        print(
            f"- {vid} ({v['label']}): tot "
            + ", ".join(f"{w} {d:+.1f}" for w, d in zip(WF_WINDOWS, d_tot, strict=True))
            + "  sr " + ", ".join(f"{w} {d:+.2f}" for w, d in zip(WF_WINDOWS, d_sr, strict=True))
            + "  dd " + ", ".join(f"{w} {d:+.1f}" for w, d in zip(WF_WINDOWS, d_dd, strict=True))
            + f" → {tag}"
        )

    payload = {
        "tag": "core-menu-expand-2026-09-12",
        "protocol": (
            "frozen pick-strong trail8 (LB60/MA200/hold1/mom). Variants differ ONLY in the ETF "
            "candidate menu added to the STOCK leg. Read-only; Live unchanged."
        ),
        "variants": [{k: v for k, v in var.items()} for var in VARIANTS],
        "windows": results,
        "as_of": datetime.now(UTC).isoformat(timespec="seconds"),
    }
    if args.save_report:
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        path = REPORT_DIR / "core_menu_expand_2026-09-12.json"
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        print(f"\nsaved {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
