#!/usr/bin/env python3
"""Verify the trail8 look-ahead: current (same-day close) vs causal (t-1 close).

Current build_nav_from_cache triggers the ETF trail on TODAY's close and credits
TODAY's return as REPO (0%) -> avoids the trigger-day loss using same-day info.
`trail_causal=True` decides the exit from the PREVIOUS close (applied today).

Usage:
  cd services/data-sync-service
  PYTHONPATH=src:scripts python3 scripts/audit_trail8_lookahead.py
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from pick_strong_grid import build_nav_from_cache, fetch_etf_closes, warm_window  # noqa: E402

START, END = "2021-01-04", "2026-08-07"
WINDOWS = {"OOS2": ("2024-08-01", "2025-08-01"), "train": ("2025-08-01", "2026-02-01"),
           "valid": ("2026-03-01", "2026-08-07"), "long": (START, END)}
OPT = dict(lookback=60, ma_window=200, min_hold=1, cost=0.0, score="mom", top2=False)


def _win(nav_map: dict, s: str, e: str) -> float | None:
    days = [d for d in sorted(nav_map) if s <= d <= e]
    if len(days) < 2:
        return None
    return round(100 * (nav_map[days[-1]] / nav_map[days[0]] - 1), 1)


def main() -> int:
    etf_close = fetch_etf_closes()
    print(f"warm_window {START}~{END} ...", flush=True)
    cache = warm_window(START, END, etf_close)

    runs = {
        "trail8 现行(当日收盘)": build_nav_from_cache(cache, trail_pct=8.0, trail_causal=False, **OPT),
        "trail8 因果(t-1 收盘)": build_nav_from_cache(cache, trail_pct=8.0, trail_causal=True, **OPT),
        "无trail": build_nav_from_cache(cache, trail_pct=0.0, **OPT),
    }
    print(f"\n{'配置':<22}{'OOS2':>9}{'train':>9}{'valid':>9}{'long':>9}   trailExits / switches")
    for name, r in runs.items():
        nm = r["nav"]
        cells = "".join(f"{str(_win(nm, *WINDOWS[w])):>9}" for w in ("OOS2", "train", "valid", "long"))
        print(f"{name:<22}{cells}   {r['trailExits']} / {r['switches']}")
    print("\n注：现行=当日收盘触发并记 0；因果=t-1 收盘触发、当日按实际持仓收益。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
