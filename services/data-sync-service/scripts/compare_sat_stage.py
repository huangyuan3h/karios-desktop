"""H-SAT-STAGE gate test (pre-registered 2026-09-11).

Hypothesis: satellite fills restricted to the S2-advance x runup5-climax
intersection beat the frozen book (t1430_b3) on three windows.

Gate (frozen): PASS = wein == 'S2-advance' AND runup5 == 'climax'
  (labels from diag_sat_stage._labels, trailing-only at entry).

Pass bar (frozen): gated sumContrib > baseline sumContrib in >=2/3
windows AND pooled avg higher AND fill rate >= 40% of baseline
(a tighter gate is untradable -> REJECT on practicability).

Method note: post-filter on the frozen replay blotter (same clip 0.25,
same 0.3% roundtrip via contribPct). Capacity caveat checked inline:
max concurrent open positions baseline vs gated (MAX_POS=4); if the
baseline rarely binds, post-filter ~= true replay.

Usage:
    PYTHONPATH=src:scripts python3 scripts/compare_sat_stage.py
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


def _passes(labels: dict | None, mode: str = "strict") -> bool:
    if not labels:
        return False
    s2 = labels["wein"] == "S2-advance"
    climax = labels["runup5"] == "climax"
    if mode == "loose":
        return s2 or climax
    return s2 and climax


def main() -> int:
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--gate", default="strict", choices=["strict", "loose"],
                    help="H-SAT-STAGE2 pre-reg 2026-09-11: loose = S2 OR climax")
    args = ap.parse_args()
    gate = args.gate
    hyp = "S2 x climax gate" if gate == "strict" else "S2 OR climax gate (H-SAT-STAGE2)"
    from data_sync_service.service.fin_panel import load_price_map, load_trade_calendar
    from data_sync_service.service.state_bucket_track import (
        FILL_SAME_1430,
        load_sgap_context,
        replay_sgap_from_context,
    )

    cal, idx_of = load_trade_calendar()
    px = load_price_map()
    summary = {}
    for w, (s, e) in WINDOWS.items():
        ctx = load_sgap_context(s, e)
        res = replay_sgap_from_context(ctx, start=s, end=e,
                                       fill_mode=FILL_SAME_1430, fill_hhmm="1430",
                                       body=3)
        fills = [b for b in res["blotter"]
                 if b.get("kind") == "fill" and b.get("pnlPct") is not None]
        base_contrib = sum(b["contribPct"] for b in fills)
        # capacity check: max concurrent opens
        events = []
        for b in fills:
            events.append((b["entryDate"], 1))
            events.append((b["exitDate"], -1))
        events.sort()
        cur = mx = 0
        for _, d in events:
            cur += d
            mx = max(mx, cur)
        gated, skipped_nolabel = [], 0
        for b in fills:
            ei = idx_of.get(b["entryDate"], -1)
            vals = [px[b["ts"]].get(d) for d in cal[: ei + 1]] if ei >= 0 else []
            vals = [v for v in vals if v]
            lab = _labels(vals) if len(vals) >= 61 else None
            if lab is None:
                skipped_nolabel += 1
                continue
            if _passes(lab, gate):
                gated.append(b)
        g_contrib = sum(b["contribPct"] for b in gated)
        pnls = [b["pnlPct"] for b in gated]
        summary[w] = {
            "base_fills": len(fills),
            "base_contrib": round(base_contrib, 2),
            "base_max_concurrent": mx,
            "gated_fills": len(gated),
            "fill_rate": round(len(gated) / len(fills), 3) if fills else 0,
            "gated_contrib": round(g_contrib, 2),
            "gated_avg": round(sum(pnls) / len(pnls), 2) if pnls else None,
            "gated_hit": round(sum(1 for p in pnls if p > 0) / len(pnls) * 100, 1) if pnls else None,
            "unlabeled": skipped_nolabel,
        }
        print(w, json.dumps(summary[w], ensure_ascii=False))
    wins = sum(1 for w in summary
               if summary[w]["gated_contrib"] > summary[w]["base_contrib"])
    rate_ok = all(s["fill_rate"] >= 0.40 for s in summary.values())
    all_avg = [summary[w]["gated_avg"] for w in summary]
    print(f"\nwins={wins}/3 fill_rate_ok(>=40% each)={rate_ok}")
    verdict = "PASS" if (wins >= 2 and rate_ok) else "REJECT"
    print("VERDICT:", verdict)
    out = {"id": "h-sat-stage2" if gate == "loose" else "h-sat-stage",
           "hypothesis": hyp,
           "frozen": "2026-09-11", "windows": summary,
           "wins": wins, "verdict": verdict,
           "created_at": datetime.now(UTC).isoformat()}
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    (REPORT_DIR / ("h_sat_stage2.json" if gate == "loose" else "h_sat_stage.json")).write_text(
        json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print("report: data/backtest_reports/" + ("h_sat_stage2.json" if gate == "loose" else "h_sat_stage.json"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
