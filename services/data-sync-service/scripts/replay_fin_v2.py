"""V2 replay (P0-12): slow-value standalone sleeve — pre-registered.

Frozen spec (agent draft, user asked to proceed):
  - Rebalance: first trading day on/after May 1 (post-annual-report), 2021..2025
  - Selection: value composite (EP/BP/SP/FCF equal-weight rank, PiT) top-5
    per circ_mv quintile = 25 names, equal-weight
  - Baseline: equal-weight ALL scored stocks, same dates (NOT the S-3 baseline)
  - Hold 250 sessions; costs 30bps round-trip per rebalance; dividends ignored
    (no local table — disclosed drag on value names)
  - Universe: non-financials, 4 legs non-null, circ_mv asof day

PASS bar (frozen): sleeve beats EW baseline in >= 4/5 vintages AND mean
excess > 0. Else CLOSE. 2025 vintage is partial (~110 sessions) — counted
with note.

Usage:
    PYTHONPATH=src python3 scripts/replay_fin_v2.py
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

COST_RT = 0.003  # 30bps round-trip per rebalance
REBALANCE_YEARS = [2021, 2022, 2023, 2024, 2025]
TOP_PER_BUCKET = 5


def main() -> int:
    import pandas as pd

    from data_sync_service.service.fin_panel import (
        load_mv_map,
        load_price_map,
        load_trade_calendar,
        mv_asof,
        shift_return,
        value_panel,
    )

    panel = value_panel()
    base = panel[~panel["is_fin"]].copy()
    legs = ["n_income_attr_p_sq_ttm", "total_revenue_sq_ttm",
            "total_hldr_eqy_inc_min_int", "free_cashflow_sq_ttm"]
    panel = base[[c for c in legs]].notna().all(axis=1)
    base = base[panel].copy()
    cal, idx_of = load_trade_calendar()
    px = load_price_map()
    tmv = load_mv_map("total_mv")
    cmv = load_mv_map("circ_mv")

    vintages = []
    # A-share session count per day (cal mixes HK-only holidays; trade_calendar
    # SSE starts 2023 — derive A-share opens from price availability instead)
    a_count: dict[str, int] = {}
    for ts, days in px.items():
        if ts.endswith((".SH", ".SZ", ".BJ")):
            for d in days:
                a_count[d] = a_count.get(d, 0) + 1
    for y in REBALANCE_YEARS:
        # first A-share session on/after May 1 (>=3000 names)
        r = next((d for d in cal if d >= f"{y}-05-01" and a_count.get(d, 0) >= 3000), None)
        if r is None or r not in idx_of:
            continue
        asof = base[base["ann_date"].dt.strftime("%Y-%m-%d") <= r].copy()
        asof = asof.sort_values("end_date").drop_duplicates("ts_code", keep="last")
        asof["tmv"] = [mv_asof(tmv, cal, idx_of, t, r, lookback=120) for t in asof["ts_code"]]
        asof["cmv"] = [mv_asof(cmv, cal, idx_of, t, r, lookback=120) for t in asof["ts_code"]]
        asof = asof[asof["tmv"].notna() & (asof["tmv"] > 0) & asof["cmv"].notna()].copy()
        if len(asof) < 100:
            continue
        asof["EP"] = asof["n_income_attr_p_sq_ttm"] / asof["tmv"]
        asof["BP"] = asof["total_hldr_eqy_inc_min_int"] / asof["tmv"]
        asof["SP"] = asof["total_revenue_sq_ttm"] / asof["tmv"]
        asof["FCFP"] = asof["free_cashflow_sq_ttm"] / asof["tmv"]
        for c in ("EP", "BP", "SP", "FCFP"):
            lo, hi = asof[c].quantile([0.01, 0.99])
            asof[c + "w"] = asof[c].clip(lo, hi)
        asof["comp"] = asof[[c + "w" for c in ("EP", "BP", "SP", "FCFP")]].rank().mean(axis=1)
        asof["size_q"] = pd.qcut(asof["cmv"], 5, labels=False, duplicates="drop")
        picks = []
        for _, b in asof.groupby("size_q"):
            picks.extend(b.nlargest(TOP_PER_BUCKET, "comp")["ts_code"].tolist())
        i = idx_of[r]
        # exit: first A-share session on/after +250 mixed-cal sessions
        # (landing on an HK-only holiday = silent all-None, 2022-vintage trap)
        j = next((k for k in range(i + 250, len(cal)) if a_count.get(cal[k], 0) >= 3000),
                 len(cal) - 1)
        sess = j - i
        sleeve_rets, base_rets = [], []
        for t in picks:
            x = shift_return(px, cal, idx_of, t, r, sess)
            if x is not None:
                sleeve_rets.append(x)
        for t in asof["ts_code"]:
            x = shift_return(px, cal, idx_of, t, r, sess)
            if x is not None:
                base_rets.append(x)
        if not sleeve_rets or not base_rets:
            continue
        s = sum(sleeve_rets) / len(sleeve_rets) - COST_RT * 100
        b = sum(base_rets) / len(base_rets) - COST_RT * 100
        vintages.append({"vintage": r, "sess": sess, "n_pick": len(sleeve_rets),
                         "n_base": len(base_rets), "sleeve": round(s, 2),
                         "ew_base": round(b, 2), "excess": round(s - b, 2)})
    res = pd.DataFrame(vintages)
    print(res.to_string(index=False))
    ok1 = ((res["excess"] > 0).sum() >= 4) if len(res) else False
    ok2 = (res["excess"].mean() > 0) if len(res) else False
    print(f"\nvintages={len(res)} win={int((res['excess'] > 0).sum())}/{len(res)} "
          f"meanExcess={res['excess'].mean():.2f}pt")
    print(f"PASS bar: >=4/5 vintages [{ok1}]  meanExcess>0 [{ok2}]")
    print("VERDICT:", "SLEEVE VIABLE" if (ok1 and ok2) else "CLOSE sleeve, keep diagnostic")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
