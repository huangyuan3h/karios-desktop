"""Phase 0 diagnostic for candidate C (TIP-017) — READ ONLY, no output files.

Pre-registration: docs/backtests/factors/candidate-c-flow-resonance-2026-09-10.md §4.

Screens market-level sentiment/flow signals across the three fixed walk-forward
windows (OOS2/train/valid) with the pre-frozen columns:
  1. coverage  — share of days in each extreme tail (rolling 250d percentile)
  2. forward   — forward 1/5/20d return by tail, per window
  3. collinear — Spearman corr with existing structures (idx trend / breadth / risk mode)
  4. 3-window  — sign consistency of (low - high) forward spread
No PnL, no replay, no output files (stdout only).

Run:  PYTHONPATH=src python3 scripts/diag_candidate_c.py
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from data_sync_service import db

if TYPE_CHECKING:
    import pandas as pd

WINDOWS: dict[str, tuple[str, str]] = {
    "OOS2": ("2024-08-01", "2025-08-01"),
    "train": ("2025-08-01", "2026-02-01"),
    "valid": ("2026-03-01", "2026-08-07"),
}
LOAD_START = "2021-06-01"  # warm lookback for MA200 / rolling percentiles
LOAD_END = "2026-08-07"

BROAD_ETF = ("510300.SH", "510500.SH", "510510.SH", "159915.SZ")


def _rows(sql: str, params: tuple = ()) -> list[tuple]:
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()


def build_signals() -> pd.DataFrame:
    import pandas as pd

    cal = [
        str(d)
        for (d,) in _rows(
            "SELECT trade_date FROM index_daily WHERE ts_code='000300.SH' "
            "AND trade_date BETWEEN %s AND %s ORDER BY trade_date",
            (LOAD_START, LOAD_END),
        )
    ]
    df = pd.DataFrame(index=pd.Index(cal, name="date"))

    # 000300 closes (target + trend structures)
    idx = {str(d): float(c) for d, c in _rows(
        "SELECT trade_date, close FROM index_daily WHERE ts_code='000300.SH' "
        "AND trade_date BETWEEN %s AND %s ORDER BY trade_date", (LOAD_START, LOAD_END))}
    df["idx"] = [idx.get(d) for d in cal]
    s = df["idx"]
    df["idx_ret20"] = s / s.shift(20) - 1.0
    df["idx_above_ma200"] = (s > s.rolling(200, min_periods=120).mean()).astype(float)

    # national-team ETF share 20d delta (4 codes, ffilled then summed)
    share = {c: {} for c in BROAD_ETF}
    for d, ts, v in _rows(
        "SELECT trade_date, ts_code, fd_share FROM cn_etf_share WHERE ts_code = ANY(%s) "
        "AND trade_date BETWEEN %s AND %s ORDER BY trade_date", (list(BROAD_ETF), LOAD_START, LOAD_END)):
        share[str(ts)][str(d)] = float(v)
    all_days = sorted({d for m in share.values() for d in m})
    last: dict[str, float] = {}
    tot: list[float] = []
    for d in all_days:
        for c, m in share.items():
            if d in m:
                last[c] = m[d]
        tot.append(sum(last.values()) if last else float("nan"))
    share_s = pd.Series(tot, index=pd.Index(all_days, name="date")).reindex(cal).ffill()
    df["natD20"] = share_s - share_s.shift(20)

    # margin balance 20d delta (sum exchanges)
    mz = {str(d): float(v) for d, v in _rows(
        "SELECT trade_date, SUM(rzye) FROM cn_margin_total WHERE trade_date BETWEEN %s AND %s "
        "GROUP BY trade_date ORDER BY trade_date", (LOAD_START, LOAD_END))}
    ms = pd.Series(mz).reindex(cal)
    df["marginD20"] = (ms - ms.shift(20)) / 1e8  # 亿元

    # northbound 20d sum (万元 -> 亿元)
    nz = {str(d): float(v) for d, v in _rows(
        "SELECT trade_date, SUM(north_money) FROM cn_moneyflow_hsgt WHERE trade_date BETWEEN %s AND %s "
        "GROUP BY trade_date ORDER BY trade_date", (LOAD_START, LOAD_END))}
    ns = pd.Series(nz).reindex(cal)
    df["northD20"] = ns.rolling(20, min_periods=20).sum() / 1e4

    # retail small-order net-buy share (20d mean of sm_net/turnover)
    fz = {str(d): (float(a), float(b)) for d, a, b in _rows(
        "SELECT trade_date, sm_net, turnover FROM cn_flow_daily WHERE trade_date BETWEEN %s AND %s "
        "ORDER BY trade_date", (LOAD_START, LOAD_END))}
    sm = pd.Series({d: (a / b if b else float("nan")) for d, (a, b) in fz.items()}).reindex(cal)
    df["smNetPct20"] = sm.rolling(20, min_periods=10).mean()

    # limit-up count + 2-day consecutive (speculative heat), market-wide
    lu = {str(d): (int(a), int(b)) for d, a, b in _rows(
        """
        WITH f AS (
          SELECT trade_date, ts_code, pct_chg,
            CASE WHEN ts_code LIKE '%%.BJ' THEN 29.8
                 WHEN split_part(ts_code,'.',1) LIKE '300%%'
                   OR split_part(ts_code,'.',1) LIKE '301%%'
                   OR split_part(ts_code,'.',1) LIKE '688%%' THEN 19.8
                 ELSE 9.8 END AS lim
          FROM daily WHERE trade_date BETWEEN %s AND %s
        ), g AS (
          SELECT trade_date, CASE WHEN pct_chg >= lim THEN 1 ELSE 0 END AS is_lu,
                 LAG(CASE WHEN pct_chg >= lim THEN 1 ELSE 0 END)
                   OVER (PARTITION BY ts_code ORDER BY trade_date) AS prev_lu
          FROM f
        )
        SELECT trade_date, SUM(is_lu),
               SUM(CASE WHEN is_lu=1 AND prev_lu=1 THEN 1 ELSE 0 END)
        FROM g GROUP BY trade_date ORDER BY trade_date
        """, (LOAD_START, LOAD_END))}
    df["limitUpCount"] = [lu.get(d, (float("nan"),))[0] for d in cal]
    df["limitUp2d"] = [lu.get(d, (float("nan"), float("nan")))[1] for d in cal]

    # turnover 60d percentile (market median turnover_rate)
    tz: dict[str, list[float]] = {}
    for d, tr in _rows(
        "SELECT trade_date, turnover_rate FROM stock_dailybasic WHERE trade_date BETWEEN %s AND %s",
        (LOAD_START, LOAD_END)):
        tz.setdefault(str(d), []).append(float(tr))
    tsr = pd.Series({d: (sorted(v)[len(v) // 2] if v else float("nan")) for d, v in tz.items()}).reindex(cal)
    df["turnover60Pct"] = tsr.rolling(60, min_periods=40).rank(pct=True)

    # sentiment: up/down ratio + failed-limitup rate + risk mode
    sz: dict[str, tuple] = {}
    for d, r, f, m in _rows(
        "SELECT date, up_down_ratio, failed_limitup_rate, risk_mode FROM market_cn_sentiment_daily "
        "WHERE date BETWEEN %s AND %s ORDER BY date", (LOAD_START, LOAD_END)):
        sz[str(d)] = (r, f, m)
    df["upDownRatio"] = [sz.get(d, (None,))[0] for d in cal]
    df["failedLimit"] = [sz.get(d, (None, None))[1] for d in cal]
    df["riskCaution"] = [1.0 if str(sz.get(d, (None, None, ""))[2]) in
                         ("extreme_caution", "no_new_positions") else 0.0 for d in cal]

    # forward returns (target = 沪深300 as market/core proxy)
    for h in (1, 5, 20):
        df[f"fwd{h}"] = s.shift(-h) / s - 1.0
    return df


SIGNALS = [
    ("natD20", "国家队份额20dΔ"),
    ("marginD20", "两融余额20dΔ"),
    ("northD20", "北向20d累计"),
    ("smNetPct20", "小单净买占比20d"),
    ("limitUpCount", "涨停家数"),
    ("limitUp2d", "2连板家数"),
    ("turnover60Pct", "换手中位60d分位"),
    ("upDownRatio", "涨跌家数比"),
    ("failedLimit", "炸板率"),
    ("riskCaution", "risk_mode谨慎"),
]


def screen() -> None:
    import numpy as np
    import pandas as pd

    df = build_signals()
    pct = df.copy()
    for col, _ in SIGNALS:
        pct[col + "_p"] = df[col].rolling(250, min_periods=120).rank(pct=True)

    STRUCT = {
        "idx_ret20": "指数20d动量",
        "idx_above_ma200": "指数>MA200",
        "upDownRatio": "涨跌比",
        "riskCaution": "谨慎态",
    }

    print("=" * 96)
    print("Phase 0 候选 C 诊断 — 市场级情绪/资金流（预注册 §4）")
    print("目标收益 = 沪深300 前瞻；极端态 = 250d 滚动分位 ≤0.2(低) / ≥0.8(高)")
    print("=" * 96)
    print("\n[3] 共线性（Spearman，long 窗 2021-06~2026-08，|·|>0.7 触发 kill #2）")
    print(f"{'信号':<20}" + "".join(f"{v:>14}" for v in STRUCT.values()))
    for col, label in SIGNALS:
        line = f"{label:<20}"
        for scol in STRUCT:
            sub = pd.concat([df[col], df[scol]], axis=1).dropna()
            c = sub.corr(method="spearman").iloc[0, 1] if len(sub) > 30 else float("nan")
            line += f"{c:>+14.2f}"
        print(line)

    print("\n[1/2/4] 三窗：极端态覆盖 / 前瞻 1·5·20 日均值 / 方向一致性")
    for win, (ws, we) in WINDOWS.items():
        m = (df.index >= ws) & (df.index <= we)
        print(f"\n--- {win} ({ws}~{we}, n={int(m.sum())}) ---")
        print(f"{'信号':<20}{'cov低/高':>12}" + "".join(f"{('fwd'+str(h)+'低/高'):>18}" for h in (1, 5, 20)) + "  判定")
        for col, label in SIGNALS:
            sub = pct.loc[m]
            p = sub[col + "_p"]
            lo = (p <= 0.2)
            hi = (p >= 0.8)
            cov = f"{lo.mean():.0%}/{hi.mean():.0%}"
            cells, signs = "", []
            for h in (1, 5, 20):
                f = df.loc[m, f"fwd{h}"]
                lm = (f[lo].mean() if lo.any() else float("nan")) * 100
                hm = (f[hi].mean() if hi.any() else float("nan")) * 100
                cells += f"{lm:>+8.2f}/{hm:>+8.2f}  "
                if pd.notna(lm) and pd.notna(hm) and abs(lm - hm) > 1e-9:
                    signs.append(np.sign(lm - hm))
            # coverage kill (#7): both tails <1%
            if lo.mean() < 0.01 and hi.mean() < 0.01:
                verdict = "KILL 覆盖<1%"
            elif len(signs) == 3 and len(set(signs)) == 1:
                verdict = "方向一致"
            else:
                verdict = "方向不一致→kill#3?"
            print(f"{label:<20}{cov:>12}{cells}  {verdict}")


def phase1() -> None:
    """Phase 1 (pre-reg §5): fixed resonance / divergence states + the one
    Phase-0 survivor (国家队 low tail) controlled for trend state.

    Read-only, stdout only.
    """
    import numpy as np

    df = build_signals()
    nat_p = df["natD20"].rolling(250, min_periods=120).rank(pct=True)

    def fmean(mask: pd.Series, h: int) -> tuple[int, float]:
        sub = df.loc[mask, f"fwd{h}"].dropna()
        return len(sub), (sub.mean() * 100 if len(sub) else float("nan"))

    print("\n" + "=" * 96)
    print("Phase 1 共振/背离（预注册 §5，单一定义，不扫组合）")
    print("=" * 96)
    for win, (ws, we) in WINDOWS.items():
        m = (df.index >= ws) & (df.index <= we)
        sign = df[["natD20", "marginD20", "northD20"]].apply(np.sign)
        up = (sign == 1).all(axis=1)
        dn = (sign == -1).all(axis=1)
        mixed = ~(up | dn) & sign.notna().all(axis=1)
        div_bear = (df["marginD20"] > 0) & (df["natD20"] < 0)  # 杠杆进 + 护盘撤
        div_bull = (df["marginD20"] < 0) & (df["natD20"] > 0)  # 杠杆撤 + 护盘进
        print(f"\n--- {win} ---")
        print(f"{'状态':<26}{'n':>5}{'fwd5':>10}{'fwd20':>10}")
        for label, mask in [
            ("共振三路全正", up), ("共振三路全负", dn), ("混合/背离", mixed),
            ("背离看空(杠杆↑护盘↓)", div_bear), ("背离看多(杠杆↓护盘↑)", div_bull),
        ]:
            mm = mask & m
            n5, f5 = fmean(mm, 5)
            _, f20 = fmean(mm, 20)
            print(f"{label:<26}{n5:>5}{f5:>+10.2f}{f20:>+10.2f}")

    print("\n" + "=" * 96)
    print("Phase 1C 幸存者趋势控制：国家队份额20dΔ 低分位（≤0.2）的前瞻")
    print("=" * 96)
    for win, (ws, we) in WINDOWS.items():
        m = (df.index >= ws) & (df.index <= we)
        low = (nat_p <= 0.2) & m
        up = m & (df["idx_above_ma200"] > 0.5)
        print(f"\n--- {win} ---")
        for label, mask in [
            ("全部低分位", low),
            ("低分位 & 指数>MA200", low & up),
            ("低分位 & 指数<MA200", low & ~up),
        ]:
            n5, f5 = fmean(mask, 5)
            _, f20 = fmean(mask, 20)
            print(f"{label:<26}{n5:>5}{f5:>+10.2f}{f20:>+10.2f}")


if __name__ == "__main__":
    screen()
    phase1()
