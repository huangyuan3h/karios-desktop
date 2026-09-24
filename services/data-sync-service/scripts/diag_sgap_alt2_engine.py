#!/usr/bin/env python3
"""H-SGAP-ALT-2: engine A/B of bar-level VWAP/volume features vs amp_1430.

Same method as H-SGAP-ALT-1 Stage 2: feed each 14:30-knowable feature's
within-day percentile as the engine's proxy amplitude (swap ctx['px_hl_1430'])
and replay the frozen habit params. Adds a read-only bar_5min aggregate query
(no core loader change).

Features (bars <= 14:30, vol>0):
  F1 vwap_dev  = 1430 print / VWAP - 1
  F2 pm_share  = vol(afternoon) / vol(<=14:30)
  F3 vwpos     = sum(vol*(c-l)/(h-l)) / sum(vol)   (volume-weighted bar position)

Usage: PYTHONPATH=src python3 scripts/diag_sgap_alt2_engine.py --windows OOS2,train --save-report
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import psycopg

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from data_sync_service.config import get_settings  # noqa: E402
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
ARMS = {
    "amp_1430:base": (None, None),
    "vwap_dev:asc": ("vwap_dev", "asc"),
    "vwap_dev:desc": ("vwap_dev", "desc"),
    "pm_share:asc": ("pm_share", "asc"),
    "pm_share:desc": ("pm_share", "desc"),
    "vwpos:asc": ("vwpos", "asc"),
    "vwpos:desc": ("vwpos", "desc"),
}


def load_bar_agg(start: str, end: str) -> dict:
    """{ts: {day: {vwap, pm_share, vwpos}}} from bar_5min bars <= 14:30."""
    conn = psycopg.connect(get_settings().database_url)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT ts_code, trade_date,
            sum(((high+low+close)/3.0)*vol) AS num,
            sum(vol) AS den,
            sum(CASE WHEN trade_time > '1130' THEN vol ELSE 0 END) AS pm_vol,
            sum(CASE WHEN high > low THEN vol*(close-low)/(high-low) ELSE 0 END) AS vwpos_num
        FROM bar_5min
        WHERE trade_time <= '1430' AND trade_date >= %s AND trade_date <= %s
          AND high IS NOT NULL AND low IS NOT NULL AND close IS NOT NULL AND vol > 0
        GROUP BY ts_code, trade_date
        """,
        (start, end),
    )
    out: dict[str, dict[str, dict[str, float]]] = {}
    for ts, d, num, den, pm_vol, vwpos_num in cur.fetchall():
        if not den:
            continue
        day = d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d)
        out.setdefault(str(ts), {})[day] = {
            "vwap": float(num) / float(den),
            "pm_share": float(pm_vol) / float(den),
            "vwpos": float(vwpos_num) / float(den),
        }
    conn.close()
    return out


def feature_values(ctx, agg):
    per_ts = ctx["per_ts"]
    date_idx = ctx["date_idx"]
    hl_map = ctx.get("px_hl_1430") or {}
    out = {}
    for day in ctx["cal"]:
        feat_all, _ = _cached_day_features(ctx, day)
        row = {}
        for ts, d in feat_all.items():
            if not d.get("is_gap"):
                continue
            di = date_idx.get(ts, {}).get(day, -1)
            if di < 0:
                continue
            px = _intraday_px(ctx, ts, day, "1430")
            hl = (hl_map.get(ts) or {}).get(day)
            a = (agg.get(ts) or {}).get(day)
            if not px or px <= 0 or not hl or not a or not a["vwap"]:
                continue
            row[ts] = {
                "vwap_dev": px / a["vwap"] - 1.0,
                "pm_share": a["pm_share"],
                "vwpos": a["vwpos"],
            }
            row[ts]["_px"] = px
        if row:
            out[day] = row
    return out


def proxy_hl(day_feats: dict, feature: str, direction: str) -> dict:
    order = sorted(day_feats, key=lambda ts: day_feats[ts][feature])
    n = len(order)
    proxy = {}
    for i, ts in enumerate(order):
        pct = (i + 0.5) / n
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
        print(f"\n=== {w} ({s}~{e}) loading ctx + bar agg ...", flush=True)
        ctx = load_sgap_context(s, e)
        agg = load_bar_agg(s, e)
        feats = feature_values(ctx, agg)
        orig_hl = {ts: dict(m) for ts, m in (ctx.get("px_hl_1430") or {}).items()}
        print(f"  gap-days with bar features: {len(feats)}", flush=True)

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
            print(f"  {arm:16s} total={row['total']:+7.1f}%  fills={row['fills']}  "
                  f"win={row['win']}  mean/trade={row['mean_trade']}", flush=True)
        report["windows"][w] = {"start": s, "end": e, "feature_days": len(feats)}
        del ctx, agg, feats

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
        print(f"| {arm:16s} | " + " | ".join(cells) + f" | {'**PASS**' if ok else 'no'} |")

    print(f"\nStage-2 dev survivors: {survivors or 'NONE'}")
    report["valid_candidate"] = survivors[0] if survivors else None
    if not survivors:
        print("→ REJECT family (no arm beats amp_1430 on both dev windows).")

    if args.save_report:
        out = Path(__file__).resolve().parents[1] / "data" / "backtest_reports" / "sgap_alt2_engine.json"
        out.write_text(json.dumps(report, indent=2, default=float))
        print(f"saved {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
