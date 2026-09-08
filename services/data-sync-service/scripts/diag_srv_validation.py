#!/usr/bin/env python3
"""SRV validation: does the Sector Rotation Index predict anything?

Q1 predictive power (no trading data): group each day T by its SRV level
  (known at T close, EOD industry flow) and report T+1~T+3 forward market
  stats: 000001.SH forward return + A-share breadth drift. If Extreme is
  not worse out-of-sample-of-definition, SRV is a thermometer, not a forecast.
Q2 strategy relevance: base-habit satellite fills (C1 3% same_1430, body=3,
  day-3 1430 exit, strict 4 slots) grouped by ENTRY-day SRV level, over the
  SRV-covered overlap (entry >= 2025-12-17): n fills, mean net pnl, hit rate.
Q3 structure: threshold slide (45/65 +/-10) + four-dimension contribution
  split (which dimension is always maxed out = dead weight).
Read-only. Prints tables, saves nothing.
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from data_sync_service.db.industry_fund_flow import (  # noqa: E402
    get_dates_upto,
    get_rows_for_dates,
)
from data_sync_service.service.industry_fund_flow_read import (  # noqa: E402
    top_by_date_from_rows,
)
from data_sync_service.service.sector_rotation_index import (  # noqa: E402
    _score_leader_stability,
    _score_pairwise_overlap,
    _score_triple_overlap,
    _score_unique_industries,
    compute_srv_index,
)
from data_sync_service.service.state_bucket_track import (  # noqa: E402
    FILL_SAME_1430,
    load_sgap_context,
    replay_sgap_from_context,
)

SRV_START = "2025-12-17"  # first date with a 3-day flow window
Q2_SEGS = {
    "trainTail": ("2025-12-17", "2026-02-01"),
    "valid": ("2026-03-01", "2026-08-07"),
    "holdout": ("2026-08-10", "2026-09-03"),
}

THRESHOLDS = {
    "loose(35/55)": (35.0, 55.0),
    "frozen(45/65)": (45.0, 65.0),
    "strict(55/75)": (55.0, 75.0),
}


def _classify(score: float, lo: float, hi: float) -> str:
    if score < lo:
        return "Stable"
    if score < hi:
        return "Elevated"
    return "Extreme_High"


def build_srv_daily() -> dict[str, dict]:
    import psycopg

    from data_sync_service.config import get_settings

    conn = psycopg.connect(get_settings().database_url)
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT date FROM market_cn_industry_fund_flow_daily ORDER BY date")
    dates = [str(r[0])[:10] for r in cur.fetchall()]
    conn.close()
    rows = get_rows_for_dates(dates)
    out: dict[str, dict] = {}
    for i in range(2, len(dates)):
        w = dates[max(0, i - 4): i + 1]
        tops = top_by_date_from_rows(rows, w, top_k=5)
        s = compute_srv_index(top_by_date=tops, as_of_date=dates[i])
        if s.get("score") is None:
            continue
        sets = []
        leaders = []
        for item in tops:
            if str(item.get("date") or "") in dates[max(0, i - 2): i + 1]:
                sets.append(set(item.get("top") or []))
                if item.get("top"):
                    leaders.append(item["top"][0])
        # sub-scores recomputed from the last-3 sets for the decomposition
        triple = len(sets[0] & sets[1] & sets[2]) if len(sets) == 3 else -1
        pair = ((len(sets[0] & sets[1]) + len(sets[1] & sets[2])) / 2.0) if len(sets) == 3 else -1
        uniq = len(sets[0] | sets[1] | sets[2]) if len(sets) == 3 else -1
        from collections import Counter

        lead = max(Counter(leaders).values()) if leaders else -1
        out[dates[i]] = {
            "score": float(s["score"]),
            "level": str(s["level"]),
            "overlap": int(s["overlapCount"]),
            "sub": {
                "triple": _score_triple_overlap(triple),
                "pair": _score_pairwise_overlap(pair),
                "unique": _score_unique_industries(uniq),
                "leader": _score_leader_stability(lead),
            },
        }
    return out


def _load_index_closes() -> dict[str, float]:
    import psycopg

    from data_sync_service.config import get_settings

    out: dict[str, float] = {}
    conn = psycopg.connect(get_settings().database_url)
    cur = conn.cursor()
    cur.execute(
        "SELECT trade_date, close FROM index_daily "
        "WHERE ts_code = '000001.SH' AND trade_date >= '2025-11-01' ORDER BY trade_date"
    )
    for d, c in cur.fetchall():
        if c and c > 0:
            out[str(d)[:10]] = float(c)
    conn.close()
    return out


def q1_forward_market(srv: dict[str, dict]) -> None:
    idx = _load_index_closes()
    days = sorted(idx)
    px = {d: idx[d] for d in days}
    print("\n## Q1 forward T+1~T+3 by SRV[T] level (000001.SH, no lookahead)")
    for half, lo, hi in (("2025-12~2026-04", "2025-12-17", "2026-04-30"), ("2026-05~2026-09", "2026-05-01", "2026-09-08")):
        acc: dict[str, list[float]] = {}
        for t, v in srv.items():
            if not (lo <= t <= hi):
                continue
            if t not in px:
                continue
            i = days.index(t)
            if i + 3 >= len(days):
                continue
            fwd = px[days[i + 3]] / px[t] - 1
            acc.setdefault(v["level"], []).append(fwd)
        print(f"  [{half}]")
        tot = sum(len(x) for x in acc.values())
        for lv in ("Stable", "Elevated", "Extreme_High"):
            x = acc.get(lv, [])
            m = float(np.mean(x)) * 100 if x else float("nan")
            hit = float(np.mean([1.0 if r > 0 else 0.0 for r in x])) * 100 if x else float("nan")
            mag = float(np.mean([abs(r) for r in x])) * 100 if x else float("nan")
            print(f"    {lv:<13} mean {m:+6.2f}% / hit {hit:4.0f}% / |m| {mag:5.2f}% / n={len(x):3d}")
        print(f"    total n={tot}")


def q2_sat_fills(srv: dict[str, dict]) -> dict[str, dict[str, list[float]]]:
    print("\nloading sgap context ...", flush=True)
    ctx = load_sgap_context("2024-08-01", "2026-09-03")
    acc: dict[str, dict[str, list[float]]] = {}
    for seg, (s, e) in Q2_SEGS.items():
        sat = replay_sgap_from_context(
            ctx, start=s, end=e, skip_t1_limit=True, pool_mode="strict",
            max_pos=4, position_pct=0.25, fill_mode=FILL_SAME_1430, fill_hhmm="1430",
            exit_hhmm="1430", max_open_to_1430_pct=0.03,
        )
        for b in sat.get("blotter") or []:
            if b.get("kind") != "fill" or b.get("pnlPct") is None:
                continue
            ed = str(b.get("entryDate") or "")
            v = srv.get(ed)
            if v is None:
                continue
            acc.setdefault(seg, {}).setdefault(v["level"], []).append(float(b["pnlPct"]))
    print("\n## Q2 habit fills by entry-day SRV (mean net pnl / hit / n)")
    for seg in Q2_SEGS:
        print(f"  [{seg}]")
        tot = sum(len(x) for x in acc.get(seg, {}).values())
        for lv in ("Stable", "Elevated", "Extreme_High"):
            x = acc.get(seg, {}).get(lv, [])
            m = float(np.mean(x)) if x else float("nan")
            hit = float(np.mean([1.0 if r > 0 else 0.0 for r in x])) * 100 if x else float("nan")
            print(f"    {lv:<13} {m:+6.2f}% / {hit:4.0f}% / n={len(x):4d}")
        print(f"    total fills n={tot}")
    return acc


def q3_structure(srv: dict[str, dict], acc: dict[str, dict[str, list[float]]]) -> None:
    print("\n## Q3a threshold slide: level mix over all SRV days")
    for name, (lo, hi) in THRESHOLDS.items():
        from collections import Counter

        c = Counter(_classify(v["score"], lo, hi) for v in srv.values())
        print(f"  {name:<15} Stable {c['Stable']:3d} / Elevated {c['Elevated']:3d} / Extreme {c['Extreme_High']:3d}")
    print("\n## Q3b threshold slide: Extreme-vs-rest fill spread (see Q3c regroup below)")
    print("\n## Q3c dimension decomposition (mean share of total score)")
    keys = ("triple", "pair", "unique", "leader")
    tot = np.array([[srv[d]["sub"][k] for k in keys] for d in sorted(srv)])
    mean = tot.mean(axis=0)
    sm = mean.sum()
    maxfreq = [(tot[:, j] >= [30.0, 25.0, 25.0, 20.0][j] - 1e-9).mean() * 100 for j in range(4)]
    for j, k in enumerate(keys):
        print(f"  {k:<7} mean {mean[j]:5.2f} ({mean[j]/sm*100:4.1f}%) / maxed-out {maxfreq[j]:4.1f}% of days")


def q3c_regroup(srv: dict[str, dict]) -> None:
    # regroup valid-window fills under each threshold pair for the spread check
    print("\nloading sgap context (regroup) ...", flush=True)
    ctx = load_sgap_context("2024-08-01", "2026-08-07")
    sat = replay_sgap_from_context(
        ctx, start="2026-03-01", end="2026-08-07", skip_t1_limit=True, pool_mode="strict",
        max_pos=4, position_pct=0.25, fill_mode=FILL_SAME_1430, fill_hhmm="1430",
        exit_hhmm="1430", max_open_to_1430_pct=0.03,
    )
    fills = [(str(b.get("entryDate")), float(b["pnlPct"]))
             for b in (sat.get("blotter") or [])
             if b.get("kind") == "fill" and b.get("pnlPct") is not None and str(b.get("entryDate")) in srv]
    print(f"  valid fills with SRV n={len(fills)}")
    print("\n## Q3c valid-window Extreme-vs-rest spread under each threshold")
    for name, (lo, hi) in THRESHOLDS.items():
        ex = [p for d, p in fills if _classify(srv[d]["score"], lo, hi) == "Extreme_High"]
        re = [p for d, p in fills if _classify(srv[d]["score"], lo, hi) != "Extreme_High"]
        me = float(np.mean(ex)) if ex else float("nan")
        mr = float(np.mean(re)) if re else float("nan")
        print(f"  {name:<15} Extreme {me:+6.2f}% n={len(ex):4d} / rest {mr:+6.2f}% n={len(re):4d} / spread {me-mr:+6.2f}pt")


def main() -> int:
    srv = build_srv_daily()
    print(f"srv days={len(srv)}")
    q1_forward_market(srv)
    acc = q2_sat_fills(srv)
    q3_structure(srv, acc)
    q3c_regroup(srv)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
