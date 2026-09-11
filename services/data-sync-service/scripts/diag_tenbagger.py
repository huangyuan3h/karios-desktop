"""L1 + L2 pilot: long-horizon fundamental "tenbagger" screen (P0-12, read-only).

Prereg:
  L1 (naive 1y growth)  docs/designs/longhold-tenbagger-prereg-2026-09-11.md
  L2 (durable min-2y)   same doc §8
Hypothesis: sustained fundamental growth x quality predicts multi-year
outperformance (2-3x), above the cohort base rate. Own baseline = cohort
equal-weight. NOT an S-3 gate, NEVER touches Live.

L1 naive GQ  = rank(current YoY growth) + quality
L2 durable DQ = rank(min(two consecutive YoY growths)) + quality
Head-to-head to test whether "growth durability" fixes the L1 peak-earnings trap.

Usage:
    PYTHONPATH=src python3 scripts/diag_tenbagger.py
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import pandas as pd

FORM_YEARS = [2020, 2021, 2022, 2023, 2024]
H2, H3 = 504, 756
LIQ_MIN = 70_000.0  # 0.7e8 yuan = 7e4 thousand-yuan (tushare amount unit)


def _load_annual(table: str, cols: tuple[str, ...]) -> pd.DataFrame:
    from data_sync_service.db import get_connection

    use = ", ".join(["ts_code", "ann_date", "end_date", "comp_type", *cols])
    with get_connection() as conn:
        df = pd.read_sql(
            f"SELECT {use} FROM {table} WHERE report_type='1' "
            "AND end_date >= '2017-01-01' AND EXTRACT(MONTH FROM end_date)=12",
            conn,
        )
    df["ann_date"] = pd.to_datetime(df["ann_date"])
    df["end_date"] = pd.to_datetime(df["end_date"])
    df["fy"] = df["end_date"].dt.year
    df = df.sort_values(["ts_code", "end_date", "ann_date"]).drop_duplicates(
        ["ts_code", "end_date"], keep="last"
    )
    return df


def _load_calendar() -> list[str]:
    from data_sync_service.db import get_connection

    with get_connection() as conn:
        df = pd.read_sql(
            "SELECT trade_date FROM daily WHERE (ts_code LIKE '%.SH' OR ts_code LIKE '%.SZ') "
            "GROUP BY trade_date HAVING count(*) >= 3000 ORDER BY trade_date",
            conn,
            parse_dates=["trade_date"],
        )
    return df["trade_date"].dt.strftime("%Y-%m-%d").tolist()


def _load_daily() -> pd.DataFrame:
    from data_sync_service.db import get_connection

    with get_connection() as conn:
        return pd.read_sql(
            "SELECT ts_code, trade_date, close, amount FROM daily "
            "WHERE trade_date >= '2018-06-01' AND close > 0 "
            "AND (ts_code LIKE '%.SH' OR ts_code LIKE '%.SZ' OR ts_code LIKE '%.BJ')",
            conn,
            parse_dates=["trade_date"],
        )


def _first_session_on_or_after(cal: list[str], date: str) -> str | None:
    for d in cal:
        if d >= date:
            return d
    return None


def _fwd_return(days: dict[str, float], cal: list[str], idx: dict[str, int],
                base: str, horizon: int, delist: str | None) -> tuple[float | None, bool]:
    """Total return % base->base+horizon, delisting-aware (see prereg §2)."""
    i = idx.get(base)
    if i is None:
        return None, False
    c0 = None
    for k in range(i, max(i - 11, -1), -1):
        c0 = days.get(cal[k])
        if c0:
            break
    if not c0:
        return None, False
    j = min(i + horizon, len(cal) - 1)
    exit_px = None
    exit_k = None
    for k in range(j, i, -1):
        v = days.get(cal[k])
        if v:
            exit_px, exit_k = v, k
            break
    if exit_px is None:
        if delist and delist <= cal[j]:
            return -100.0, True
        return None, False
    return (exit_px / c0 - 1.0) * 100.0, (j - exit_k) > 20


def _back_return(days: dict[str, float], cal: list[str], idx: dict[str, int],
                 base: str, horizon: int) -> float | None:
    i = idx.get(base)
    if i is None or i - horizon < 0:
        return None
    c0, c1 = days.get(cal[i - horizon]), days.get(base)
    if not c0 or not c1:
        return None
    return (c1 / c0 - 1.0) * 100.0


def _rank(s: pd.Series) -> pd.Series:
    return s.rank(pct=True)


def _quality(g: pd.DataFrame) -> pd.Series:
    return (_rank(g["roew"]) + _rank(g["cfo_niw"].clip(-2, 3)) + _rank(-g["debtw"])) / 3


def main() -> int:
    from data_sync_service.db import get_connection

    cal = _load_calendar()
    idx = {d: i for i, d in enumerate(cal)}
    print(f"A-share sessions: {len(cal)} ({cal[0]}..{cal[-1]})")

    inc = _load_annual("cn_income_stmt", ("total_revenue", "n_income_attr_p"))
    bal = _load_annual("cn_balance_sheet",
                       ("total_hldr_eqy_inc_min_int", "total_assets", "total_liab"))
    cf = _load_annual("cn_cashflow_stmt", ("n_cashflow_act",))

    prev = inc[["ts_code", "fy", "total_revenue", "n_income_attr_p", "ann_date"]].copy()
    prev["fy"] = prev["fy"] + 1
    prev = prev.rename(columns={"total_revenue": "rev_prev",
                                "n_income_attr_p": "ni_prev", "ann_date": "ann_prev"})
    inc = inc.merge(prev, on=["ts_code", "fy"], how="left")
    fin = inc.merge(
        bal[["ts_code", "fy", "total_hldr_eqy_inc_min_int", "total_assets", "total_liab",
             "ann_date"]].rename(columns={"ann_date": "ann_bal"}),
        on=["ts_code", "fy"], how="inner",
    ).merge(
        cf[["ts_code", "fy", "n_cashflow_act", "ann_date"]].rename(
            columns={"ann_date": "ann_cf"}),
        on=["ts_code", "fy"], how="inner",
    )
    fin["knowable"] = fin[["ann_date", "ann_prev", "ann_bal", "ann_cf"]].max(axis=1)
    fin = fin[fin["comp_type"] == "1"].copy()
    fin = fin.sort_values(["ts_code", "fy"])
    fin["rev_yoy"] = (fin["total_revenue"] / fin["rev_prev"] - 1) * 100
    fin["ni_yoy"] = (fin["n_income_attr_p"] / fin["ni_prev"] - 1) * 100
    pri = fin[["ts_code", "fy", "rev_yoy", "ni_yoy"]].copy()
    pri["fy"] = pri["fy"] + 1
    pri = pri.rename(columns={"rev_yoy": "rev_yoy_p", "ni_yoy": "ni_yoy_p"})
    fin = fin.merge(pri, on=["ts_code", "fy"], how="left")

    with get_connection() as conn:
        basic = pd.read_sql("SELECT ts_code, name, list_date, delist_date FROM stock_basic", conn)
        mv = pd.read_sql("SELECT ts_code, trade_date, total_mv FROM stock_dailybasic", conn,
                         parse_dates=["trade_date"])
    basic["list_date"] = pd.to_datetime(basic["list_date"])
    basic["delist_date"] = pd.to_datetime(basic["delist_date"]).dt.strftime("%Y-%m-%d")
    name_map = dict(zip(basic["ts_code"], basic["name"]))
    list_map = dict(zip(basic["ts_code"], basic["list_date"]))
    delist_map = dict(zip(basic["ts_code"], basic["delist_date"]))
    mv["trade_date"] = mv["trade_date"].dt.strftime("%Y-%m-%d")
    mv_map: dict[str, dict[str, float]] = {}
    for t, d, v in zip(mv["ts_code"], mv["trade_date"], mv["total_mv"]):
        if v and v > 0:
            mv_map.setdefault(t, {})[d] = float(v)

    px = _load_daily()
    px["trade_date"] = px["trade_date"].dt.strftime("%Y-%m-%d")
    px = px.sort_values(["ts_code", "trade_date"])
    px_map: dict[str, dict[str, float]] = {}
    amt_map: dict[str, dict[str, float]] = {}
    for t, d, c, a in zip(px["ts_code"], px["trade_date"], px["close"], px["amount"]):
        px_map.setdefault(t, {})[d] = float(c)
        amt_map.setdefault(t, {})[d] = float(a or 0.0)

    all_rows = []
    for y in FORM_YEARS:
        form = _first_session_on_or_after(cal, f"{y}-04-30")
        fi = idx[form]
        g = fin[fin["end_date"] < f"{y}-01-01"].copy()
        g = g[g["knowable"] <= form]
        g = g.sort_values(["ts_code", "end_date"]).drop_duplicates("ts_code", keep="last")
        g = g[g["rev_prev"].notna() & (g["rev_prev"] > 0)].copy()
        g = g[(g["total_hldr_eqy_inc_min_int"] > 0) & (g["total_assets"] > 0)]

        keep = []
        for ts in g["ts_code"]:
            ld = list_map.get(ts)
            if pd.isna(ld) or (pd.Timestamp(form) - ld).days < 365:
                continue
            days = amt_map.get(ts, {})
            win = [days.get(cal[k]) for k in range(max(fi - 59, 0), fi + 1)]
            win = [w for w in win if w]
            if len(win) < 30 or sum(win) / len(win) < LIQ_MIN:
                continue
            keep.append(ts)
        g = g[g["ts_code"].isin(keep)].copy()
        if len(g) < 300:
            print(f"[{y}] too few after filters: {len(g)}")
            continue

        g["rev_min"] = g[["rev_yoy", "rev_yoy_p"]].min(axis=1, skipna=False)
        g["ni_min"] = g[["ni_yoy", "ni_yoy_p"]].min(axis=1, skipna=False)
        g["roe"] = g["n_income_attr_p"] / g["total_hldr_eqy_inc_min_int"]
        g["cfo_ni"] = (g["n_cashflow_act"] / g["n_income_attr_p"]).where(
            g["n_income_attr_p"] > 0)
        g["debt"] = g["total_liab"] / g["total_assets"]
        g["mom252"] = [_back_return(px_map.get(t, {}), cal, idx, form, 252)
                       for t in g["ts_code"]]
        ni_map = dict(zip(g["ts_code"], g["n_income_attr_p"]))
        g["ep"] = [(ni_map[t] / mv_map[t][form]) if mv_map.get(t, {}).get(form)
                   else float("nan") for t in g["ts_code"]]

        f2, f3, stale2 = [], [], 0
        for t in g["ts_code"]:
            r2, s2 = _fwd_return(px_map.get(t, {}), cal, idx, form, H2, delist_map.get(t))
            r3, _ = _fwd_return(px_map.get(t, {}), cal, idx, form, H3, delist_map.get(t))
            f2.append(r2); f3.append(r3)
            stale2 += bool(s2)
        g["fwd2"] = f2
        g["fwd3"] = f3
        g = g[g["fwd2"].notna()].copy()

        for c in ("rev_yoy", "ni_yoy", "rev_min", "ni_min", "roe", "cfo_ni",
                  "debt", "mom252", "ep"):
            lo, hi = g[c].quantile([0.01, 0.99])
            g[c + "w"] = g[c].clip(lo, hi)
        g["Q"] = _quality(g)
        g["GQ"] = ((_rank(g["rev_yoyw"]) + _rank(g["ni_yoyw"])) / 2 + g["Q"]) / 2
        g["GQd"] = ((_rank(g["rev_minw"]) + _rank(g["ni_minw"])) / 2 + g["Q"]) / 2
        g["MOM"] = _rank(g["mom252w"])

        gd = g[g["GQd"].notna()].copy()
        top = g.nlargest(20, "GQ")
        topd = gd.nlargest(20, "GQd")
        ew2, ew3 = g["fwd2"].mean(), g["fwd3"].mean()
        ewd2, ewd3 = gd["fwd2"].mean(), gd["fwd3"].mean()
        cov = {
            "year": y, "form": form, "univ": len(g), "univ_d": len(gd),
            "cov_d": len(gd) / len(g), "delist_fallback": stale2,
            "base_2x2": (g["fwd2"] >= 100).mean(),
            "GQ20_2": top["fwd2"].mean(), "EW_2": ew2,
            "GQ20_3": top["fwd3"].mean(), "EW_3": ew3,
            "DQ20_2": topd["fwd2"].mean(), "EWd_2": ewd2,
            "DQ20_3": topd["fwd3"].mean(), "EWd_3": ewd3,
            "GQ20_2x": (top["fwd2"] >= 100).mean(),
            "DQ20_2x": (topd["fwd2"] >= 100).mean(),
            "corr_GQ_MOM": g[["GQ", "MOM"]].corr().iloc[0, 1],
        }
        all_rows.append(cov)

        print(f"\n===== cohort {y} (form {form}) univ={len(g)} "
              f"durable_sub={len(gd)} ({cov['cov_d']:.0%}) delist_fb={stale2} =====")
        print(f"base ≥2x/2y {cov['base_2x2']:.1%} | corr(GQ,MOM) {cov['corr_GQ_MOM']:.2f}")
        print(f"  L1 naive  GQ20: fwd2 {top['fwd2'].mean():7.1f} vs EW {ew2:7.1f} | "
              f"fwd3 {top['fwd3'].mean():7.1f} vs EW {ew3:7.1f} | 2x {cov['GQ20_2x']:.0%}")
        print(f"  L2 durable DQ20: fwd2 {topd['fwd2'].mean():7.1f} vs EW {ewd2:7.1f} | "
              f"fwd3 {topd['fwd3'].mean():7.1f} vs EW {ewd3:7.1f} | 2x {cov['DQ20_2x']:.0%}")
        show = topd.copy()
        show["name"] = show["ts_code"].map(name_map)
        print(show[["ts_code", "name", "rev_min", "ni_min", "roe", "fwd2", "fwd3"]]
              .head(10).round(2).to_string(index=False))

    res = pd.DataFrame(all_rows)
    if not res.empty:
        print("\n===== pooled summary =====")
        num = ["univ", "univ_d", "cov_d", "GQ20_2", "EW_2", "DQ20_2", "EWd_2",
               "GQ20_3", "EW_3", "DQ20_3", "EWd_3", "GQ20_2x", "DQ20_2x"]
        print(res[num].round(3).to_string(index=False))
        k1_naive = ((res["GQ20_2"] > res["EW_2"]) & (res["GQ20_3"] > res["EW_3"])).mean()
        k1_dura = ((res["DQ20_2"] > res["EWd_2"]) & (res["DQ20_3"] > res["EWd_3"])).mean()
        k2_dura = (res["DQ20_2x"] > res["base_2x2"]).mean()
        k3_fix = (res["DQ20_2"] > res["GQ20_2"]).mean()
        print(f"\ncohorts={len(res)}")
        print(f"L1 naive  both-horizon beat EW : {k1_naive:.0%}")
        print(f"L2 durable both-horizon beat EW: {k1_dura:.0%}  "
              f"[{'PASS' if k1_dura >= 0.75 else 'FAIL'}]")
        print(f"L2 durable 2x-hit > base      : {k2_dura:.0%}  "
              f"[{'PASS' if k2_dura >= 0.75 else 'FAIL'}]")
        print(f"L2 durable > L1 naive (fix)   : {k3_fix:.0%}  "
              f"[{'PASS' if k3_fix >= 0.75 else 'FAIL'}]")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
