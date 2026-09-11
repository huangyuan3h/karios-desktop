"""H-SAT-RANK: stage re-rank inside the primary bucket (pre-registered 2026-09-11).

Hypothesis: keep the primary bucket (top-1/3 by amp, same size, fill
rate ~=100%) but fill it in stage order — easy risers first.

Rank key (frozen): (0 if S2&climax else 1 if S2|climax else 2, amp).
Labels from diag_sat_stage._labels (trailing-only at entry).

Pass bar (frozen): variant satPct > baseline satPct in >= 2/3 windows.

Method: exact mirror of replay_sgap_from_context's same_1430 loop
(same ctx, same helpers, same costs/clip/body/limit-guard); only the
pool order changes. Self-check first: baseline replica must match the
engine blotter fills EXACTLY per window, else the variant is untrusted.

Usage:
    PYTHONPATH=src:scripts python3 scripts/compare_sat_rank.py
"""

from __future__ import annotations

import json
import sys
from datetime import UTC, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from diag_sat_stage import WINDOWS, _labels  # noqa: E402

REPORT_DIR = Path(__file__).resolve().parents[1] / "data" / "backtest_reports"


def _run(ctx, *, start, end, rank_fn, cal, idx_by_day, px_hist):
    from data_sync_service.service.state_bucket_track import (
        BODY,
        BUCKET_Q,
        COSTS_ROUNDTRIP,
        MAX_POS,
        POSITION_PCT,
        SAME_DAY_FILL_MODES,
        FILL_SAME_1430,
        R_WIDE_THRESHOLD,
        _cached_day_features,
        _intraday_px,
        sat_exit_decision,
    )
    per_ts, date_idx, close_by_ts = ctx["per_ts"], ctx["date_idx"], ctx["close_by_ts"]
    body, clip, q = BODY, POSITION_PCT, BUCKET_Q
    positions: dict[str, dict] = {}
    realized = 0.0
    fills: list[tuple[str, str, float]] = []
    for day in cal:
        if day < start or day > end:
            continue
        for ts, p in list(positions.items()):
            ei, ci = idx_by_day.get(p["entry_date"], -1), idx_by_day.get(day, -1)
            held = ci - ei + 1 if ei >= 0 and ci >= 0 else 999
            cc = close_by_ts.get(ts, {}).get(day)
            reason = sat_exit_decision(held=held, body=body, close=cc,
                                       entry=float(p["entry_price"] or 0.0),
                                       peak=float(p.get("peak") or p["entry_price"] or 0.0),
                                       protect_stop_pct=None, trail_after_body_pct=None)
            if reason:
                positions.pop(ts)
                if cc and p["entry_price"]:
                    realized += ((cc / p["entry_price"] - 1) - COSTS_ROUNDTRIP) * clip
                    fills.append((p["entry_date"], ts, (cc / p["entry_price"] - 1) * 100))
        if day <= start or day not in idx_by_day or idx_by_day[day] <= 0:
            continue
        feat_all, breadth = _cached_day_features(ctx, day)
        if not (breadth > R_WIDE_THRESHOLD):
            continue
        gap_stocks = [ts for ts, d in feat_all.items() if d["is_gap"]]
        ranked = sorted(gap_stocks, key=lambda ts: feat_all[ts]["amp"])
        qn = max(1, len(ranked) // q) if ranked else 0
        pool = rank_fn(ranked, qn, feat_all, day, px_hist, cal)
        ei_today = idx_by_day.get(day, -1)
        for ts in pool:
            if ts in positions or len(positions) >= MAX_POS:
                continue
            series, di = per_ts.get(ts), date_idx.get(ts, {}).get(day, -1)
            if di < 0 or not series:
                continue
            bar = series[di]
            px = _intraday_px(ctx, ts, day, "1430")
            if not px or px <= 0:
                continue
            pc = bar.get("pre_close")
            lim = 0.20 if str(ts).startswith(("3", "68")) else 0.10
            if pc and pc > 0 and px >= pc * (1 + lim - 0.004):
                continue
            positions[ts] = {"entry_date": day, "entry_price": px}
    return round(realized * 100, 2), fills


def _base_rank(ranked, qn, feat_all, day, px_hist, cal):
    return ranked[:qn]


def _stage_rank(ranked, qn, feat_all, day, px_hist, cal):
    bucket = ranked[:qn]

    def key(ts):
        h = px_hist.get(ts)
        lab = None
        if h:
            ei = cal.index(day) if day in cal else -1
            vals = h[: ei + 1] if ei >= 0 else []
            if len(vals) >= 61:
                lab = _labels(vals)
        if lab:
            s2, cx = lab["wein"] == "S2-advance", lab["runup5"] == "climax"
            tier = 0 if (s2 and cx) else (1 if (s2 or cx) else 2)
        else:
            tier = 3
        return (tier, feat_all[ts]["amp"])

    return sorted(bucket, key=key)


def main() -> int:
    from data_sync_service.service.fin_panel import load_price_map, load_trade_calendar
    from data_sync_service.service.state_bucket_track import (
        FILL_SAME_1430,
        load_sgap_context,
        replay_sgap_from_context,
    )

    cal, _ = load_trade_calendar()
    pmap = load_price_map()
    px_hist = {ts: [pmap[ts][d] for d in cal if d in pmap[ts]] for ts in pmap}
    # align histories to cal once (positional index == cal index only if no gaps;
    # _stage_rank slices by cal.index(day) so rebuild per-ts aligned lists)
    px_hist = {}
    for ts, days in pmap.items():
        px_hist[ts] = [days.get(d) for d in cal]
    # compact: drop leading Nones per ts is unnecessary; _labels needs 61 vals.
    # Build trailing-close lists ignoring missing (same as diag script).
    px_hist2 = {}
    for ts, days in pmap.items():
        px_hist2[ts] = [days[d] for d in cal if d in days]

    summary = {}
    for w, (s, e) in WINDOWS.items():
        ctx = load_sgap_context(s, e)
        eng = replay_sgap_from_context(ctx, start=s, end=e,
                                       fill_mode=FILL_SAME_1430, fill_hhmm="1430", body=3)
        eng_fills = sorted((b["entryDate"], b["ts"]) for b in eng["blotter"]
                           if b.get("kind") == "fill" and b.get("pnlPct") is not None)
        idx_by_day = ctx["idx_by_day"]
        # _stage_rank needs per-ts trailing closes; use compact lists + date cutoff
        hist = {}
        for ts, days in pmap.items():
            hist[ts] = [(d, days[d]) for d in cal if d in days]

        def stage_rank(ranked, qn, feat_all, day, _px=None, _cal=None, _h=hist):
            bucket = ranked[:qn]

            def key(ts):
                seq = [v for d, v in _h.get(ts, []) if d <= day]
                lab = _labels(seq) if len(seq) >= 61 else None
                if lab:
                    s2 = lab["wein"] == "S2-advance"
                    cx = lab["runup5"] == "climax"
                    tier = 0 if (s2 and cx) else (1 if (s2 or cx) else 2)
                else:
                    tier = 3
                return (tier, feat_all[ts]["amp"])

            return sorted(bucket, key=key)

        base_pct, base_fills = _run(ctx, start=s, end=e, rank_fn=_base_rank,
                                    cal=cal, idx_by_day=idx_by_day, px_hist={})
        var_pct, var_fills = _run(ctx, start=s, end=e, rank_fn=stage_rank,
                                  cal=cal, idx_by_day=idx_by_day, px_hist={})
        replica = sorted((d, t) for d, t, _ in base_fills)
        match = replica == eng_fills
        vp = [p for _, _, p in var_fills]
        summary[w] = {
            "engine_satPct": eng["summary"]["satPct"],
            "replica_satPct": base_pct,
            "replica_match": match,
            "base_fills": len(eng_fills),
            "var_fills": len(var_fills),
            "var_satPct": var_pct,
            "var_avg": round(sum(vp) / len(vp), 2) if vp else None,
            "var_hit": round(sum(1 for p in vp if p > 0) / len(vp) * 100, 1) if vp else None,
        }
        print(w, json.dumps(summary[w], ensure_ascii=False))
    if not all(v["replica_match"] for v in summary.values()):
        print("SELF-CHECK FAILED: replica != engine, variant untrusted")
        return 2
    wins = sum(1 for v in summary.values() if v["var_satPct"] > v["engine_satPct"])
    verdict = "PASS" if wins >= 2 else "REJECT"
    print(f"wins={wins}/3 VERDICT: {verdict}")
    out = {"id": "h-sat-rank", "hypothesis": "stage re-rank in primary bucket",
           "rank_key": "(0 if S2&climax else 1 if S2|climax else 2, amp)",
           "frozen": "2026-09-11", "windows": summary, "wins": wins,
           "verdict": verdict, "created_at": datetime.now(UTC).isoformat()}
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    (REPORT_DIR / "h_sat_rank.json").write_text(
        json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print("report: data/backtest_reports/h_sat_rank.json")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
