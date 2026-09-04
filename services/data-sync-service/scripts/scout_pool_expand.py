#!/usr/bin/env python3
"""Pool expansion test for Scout B (amp_and_gap 10d breadth>0.5)."""
import sys
from datetime import date, timedelta
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from data_sync_service.db import get_connection

WINDOWS = {
    "valid": ("2026-03-01","2026-08-07"),
    "long": ("2021-08-01","2026-08-07"),
    "OOS2": ("2024-08-01","2025-08-01"),
    "train": ("2025-08-01","2026-02-01"),
}

def run_pool(min_mv, max_mv, wname):
    # patch global for this run
    import scripts.scout_combine_momentum as cm
    # set universe
    orig_min = cm.UNIVERSE_MIN_MV if hasattr(cm, 'UNIVERSE_MIN_MV') else 20.0
    orig_max = cm.UNIVERSE_MAX_MV if hasattr(cm, 'UNIVERSE_MAX_MV') else 80.0
    # Actually scout_combine uses 20-80 hardcoded via UNIVERSE_MIN/MAX at import, but we can monkey patch its _get_factors via closure?
    # Instead we will directly call a custom simulate that respects min/max
    # For now, we will run via scout_hold75_breadth's simulate with patched globals
    import scripts.scout_hold75_breadth as hb
    # patch hb
    hb.UNIVERSE_MIN_MV = min_mv
    hb.UNIVERSE_MAX_MV = max_mv
    # clear cache for hb
    if hasattr(hb, 'CACHE'):
        hb.CACHE.clear()
    # also need to patch combine's cache
    if hasattr(cm, 'CACHE'):
        cm.CACHE.clear()
    # For Scout B, we will use combine's simulate_one with amp_and_gap
    # But combine's UNIVERSE is also 20-80 via its own constants, need to patch both
    cm.UNIVERSE_MIN_MV = min_mv
    cm.UNIVERSE_MAX_MV = max_mv
    # Now call combine's simulate_one for wname
    # It uses get_window_data which uses per_ts filtering via _get_factors which checks mv range via closure's UNIVERSE_MIN/MAX?
    # In combine, _get_factors checks UNIVERSE_MIN_MV via global, so patching works
    from scripts.scout_combine_momentum import simulate_one
    # Need to ensure WINDOWS includes wname
    cm.WINDOWS[wname] = {"valid": ("2026-03-01","2026-08-07"), "long": ("2021-08-01","2026-08-07"), "OOS2": ("2024-08-01","2025-08-01"), "train": ("2025-08-01","2026-02-01")}[wname] if wname in ["valid","long","OOS2","train"] else (None,None)
    # Actually simulate_one uses get_window_data which uses WINDOWS[wname] to load calendar, so we need WINDOWS to have wname
    # So set it
    orig_win = cm.WINDOWS.get(wname)
    cm.WINDOWS[wname] = {"valid": ("2026-03-01","2026-08-07"), "long": ("2021-08-01","2026-08-07"), "OOS2": ("2024-08-01","2025-08-01"), "train": ("2025-08-01","2026-02-01")}[wname]
    cm.CACHE.clear()
    res = simulate_one(wname, "amp_and_gap", 10)
    # restore
    cm.UNIVERSE_MIN_MV = orig_min
    cm.UNIVERSE_MAX_MV = orig_max
    hb.UNIVERSE_MIN_MV = orig_min
    hb.UNIVERSE_MAX_MV = orig_max
    if orig_win:
        cm.WINDOWS[wname] = orig_win
    return res

pools = [(20,80),(20,150),(20,300),(50,300),(0,300),(20,500)]

for min_mv, max_mv in pools:
    print(f"\n=== pool {min_mv}-{max_mv} ===")
    for w in ["valid","long","OOS2","train"]:
        res = run_pool(min_mv, max_mv, w)
        print(f"{w:6s} total {res['total_pnl']:+6.1f}% daily {res['daily_avg']:+.4f}% hold {res['hold_ratio']:4.1f}% win {res['win_rate']*100:4.1f}%")
