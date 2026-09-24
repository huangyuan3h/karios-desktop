#!/usr/bin/env python3
"""H-SGAP-ALT-1 Stage 2: A/B alternative 14:30 rank features through the real engine.

The frozen S-gap leg ranks the gap pool by ``amp_1430 = (hi-lo)/px`` ascending
(lowest amplitude = least chased). Here we test whether *other* 14:30-knowable
"position" features are a better ranking of the SAME pool / execution, by feeding
each feature's within-day percentile rank as the engine's proxy amplitude
(swap ``ctx['px_hl_1430']``) and replaying the frozen habit params.

No core engine edits. Read-only. Dev windows OOS2+train; valid only printed for
the survivor (Stage-2 confirmation).
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from data_sync_service.service.state_bucket_track import (  # noqa: E402
    FILL_SAME_1430,
    _cached_day_features,
    _intraday_px,
    load_sgap_context,
    replay_sgap_from_context,
)

WINDOWS = {
    "OOS2": ("2024-08-01", "2025-08-01"),
    "train": ("2025-08-01", "2026-02-01"),
    "valid": ("2026-03-01", "2026-08-07"),
}
# arm -> (feature, direction). "asc" = engine buys lowest feature values.
ARMS = {
    "amp_1430:base": (None, None),
    "gap_pct:asc": ("gap_pct", "asc"),
    "range_pos:asc": ("range_pos", "asc"),
    "range_pos:desc": ("range_pos", "desc"),
    "low_recovery:asc": ("low_recovery", "asc"),
    "low_recovery:desc": ("low_recovery", "desc"),
    "high_giveback:asc": ("high_giveback", "asc"),
    "high_giveback:desc": ("high_giveback", "desc"),
}


def feature_values(ctx):
    """{day: {ts: feature}} for gap names, from <=14:30 data only."""
    per_ts = ctx["per_ts"]
    date_idx = ctx["date_idx"]
    cal = ctx["cal"]
    hl_map = ctx.get("px_hl_1430") or {}
    out = {}
    for day in cal:
        feat_all, _ = _cached_day_features(ctx, day)
        d_row = {}
        for ts, d in feat_all.items():
            if not d.get("is_gap"):
                continue
            di = date_idx.get(ts, {}).get(day, -1)
            if di < 0:
                continue
            px = _intraday_px(ctx, ts, day, "1430")
            hl = (hl_map.get(ts) or {}).get(day)
            if not px or px <= 0 or not hl:
                continue
            hi, lo = float(hl[0]), float(hl[1])
            bar = (per_ts.get(ts) or [{}])[di]
            o = bar.get("open") or 0
            pc = bar.get("pre_close") or 0
            if hi <= lo or lo <= 0 or o <= 0 or pc <= 0:
                continue
            d_row[ts] = {
                "range_pos": (px - lo) / (hi - lo),
                "low_recovery": (px - lo) / lo,
                "high_giveback": px / hi - 1.0,
                "amp_1430": (hi - lo) / px,
                "gap_pct": o / pc - 1.0,
            }
        if d_row:
            out[day] = d_row
    return out


def proxy_hl(day_feats: dict, feature: str, direction: str) -> dict:
    """Build a px_hl_1430-shaped dict whose amp ordering = feature ordering.

    Engine amp = (hi - lo)/px, so set hi - lo = pct * px to make amp == pct.
    """
    order = sorted(day_feats, key=lambda ts: day_feats[ts][feature])
    n = len(order)
    proxy = {}
    for i, ts in enumerate(order):
        pct = (i + 0.5) / n  # 0..1 ascending
        if direction == "desc":
            pct = 1.0 - pct
        px = day_feats[ts].get("_px") or 0.0
        proxy[ts] = (pct * px, 0.0)
    return proxy


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--save-report", action="store_true")
    ap.add_argument("--windows", default="OOS2,train")
    args = ap.parse_args()
    wins = [w.strip() for w in args.windows.split(",") if w.strip()]

    report = {"windows": {}, "arms": {}}
    for w in wins:
        s, e = WINDOWS[w]
        print(f"\n=== {w} ({s}~{e}) loading ctx ...", flush=True)
        ctx = load_sgap_context(s, e)
        feats = feature_values(ctx)
        orig_hl = {ts: dict(m) for ts, m in (ctx.get("px_hl_1430") or {}).items()}
        # stash px for proxy build
        for day, row in feats.items():
            for ts in row:
                row[ts]["_px"] = _intraday_px(ctx, ts, day, "1430")

        for arm, (feat, direction) in ARMS.items():
            if feat is None:
                ctx["px_hl_1430"] = orig_hl
            else:
                ph = {}
                for day, row in feats.items():
                    for ts, hl in proxy_hl(row, feat, direction).items():
                        ph.setdefault(ts, {})[day] = hl
                ctx["px_hl_1430"] = ph
            sat = replay_sgap_from_context(
                ctx, start=s, end=e, skip_t1_limit=True, pool_mode="strict",
                max_pos=4, position_pct=0.25, body=3, fill_mode=FILL_SAME_1430,
                fill_hhmm="1430", exit_hhmm="1430", max_open_to_1430_pct=0.03,
                rank_key="amp_1430", gate_1430=True,
            )
            sm = sat.get("summary") or {}
            fills = [b for b in (sat.get("blotter") or [])
                     if b.get("kind") == "fill" and b.get("pnlPct") is not None]
            pn = [b["pnlPct"] for b in fills]
            row = {
                "total": float(sm.get("satPct") or 0.0),
                "fills": sm.get("fillCount"),
                "win": float(np.mean([1 if x > 0 else 0 for x in pn]) * 100) if pn else None,
                "mean_trade": float(np.mean(pn)) if pn else None,
            }
            report["arms"].setdefault(arm, {})[w] = row
            print(f"  {arm:20s} total={row['total']:+7.1f}%  fills={row['fills']}  "
                  f"win={row['win']}  mean/trade={row['mean_trade']}", flush=True)
        report["windows"][w] = {"start": s, "end": e}
        del ctx

    base = {w: report["arms"]["amp_1430:base"].get(w, {}).get("total") for w in wins}
    print("\n=== Δtotal vs amp_1430:base (dev windows) ===")
    print("| arm | " + " | ".join(wins) + " | pass(both ≥ +5pt)? |")
    print("|-----|" + "|".join(["------"] * len(wins)) + "|------|")
    survivors = []
    for arm in ARMS:
        if arm == "amp_1430:base":
            continue
        cells, deltas = [], []
        for w in wins:
            t = report["arms"][arm].get(w, {}).get("total")
            d = (t - base[w]) if (t is not None and base[w] is not None) else float("nan")
            deltas.append(d)
            cells.append(f"{t:+.1f}% ({d:+.1f})")
        ok = all(d >= 5.0 for d in deltas)
        if ok:
            survivors.append(arm)
        print(f"| {arm:20s} | " + " | ".join(cells) + f" | {'**PASS**' if ok else 'no'} |")

    print(f"\nStage-2 dev survivors: {survivors or 'NONE'}")
    if survivors:
        best = max(survivors, key=lambda a: min(report['arms'][a][w]['total'] - base[w] for w in wins))
        report["valid_candidate"] = best
        print(f"→ confirm on valid: {best}")
    else:
        report["valid_candidate"] = None
        print("→ REJECT family (no arm beats amp_1430 on both dev windows).")

    if args.save_report:
        out = Path(__file__).resolve().parents[1] / "data" / "backtest_reports" / "sgap_alt_engine.json"
        out.write_text(json.dumps(report, indent=2, default=float))
        print(f"saved {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
