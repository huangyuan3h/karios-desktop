"""Core S-3 entry-execution microstructure diagnostic — READ ONLY, no output files.

For frozen CN core (S-3) entry signals, quantify fill-price alternatives vs the
frozen ``next_open`` baseline. All comparisons are done as ADJUSTMENT-INVARIANT
ratios: ``daily`` is 前复权 (qfq) while ``bar_5min`` is raw, so cross-source
levels are meaningless; ratios within a day cancel the factor.

Zero PnL, zero replay, stdout only.

Run:  PYTHONPATH=src:scripts python3 scripts/diag_core_execution.py
"""

from __future__ import annotations

import statistics as st

from data_sync_service import db
from data_sync_service.service.backtest_engine import (
    BacktestConfig,
    BacktestData,
    _resolve_ts_code,
    simulate,
)

WINDOW_LONG = ("2021-08-01", "2026-08-07")
WINDOWS = {
    "OOS2": ("2024-08-01", "2025-08-01"),
    "train": ("2025-08-01", "2026-02-01"),
    "valid": ("2026-03-01", "2026-08-07"),
    "long": WINDOW_LONG,
}


def _load_5min(ts_code: str, day: str) -> dict[str, float]:
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT trade_time, close FROM bar_5min WHERE ts_code=%s AND trade_date=%s "
                "AND trade_time IN ('1430','1500')",
                (ts_code, day),
            )
            return {str(t): float(c) for t, c in cur.fetchall() if c is not None}


def _impr(price: float | None, base: float) -> float | None:
    """Price improvement % vs ``base`` (positive = cheaper)."""
    return (base / price - 1.0) * 100.0 if price and price > 0 else None


def _ratio_1430(b5: dict[str, float]) -> float | None:
    """14:30 price relative to the same-day 15:00 print (raw ratio; adj cancels)."""
    if b5.get("1430") and b5.get("1500"):
        return b5["1430"] / b5["1500"]
    return None


def main() -> None:
    from run_walk_forward import S3_CONFIG

    s, e = WINDOW_LONG
    cfg = BacktestConfig(start_date=s, end_date=e, **S3_CONFIG)
    data = BacktestData(cfg)
    run = simulate(cfg, data=data)
    cal = list(data.calendar)
    idx = {d: i for i, d in enumerate(cal)}

    rows: list[dict] = []
    for t in run.trades:
        resolved = _resolve_ts_code(t.symbol)
        if resolved is None or resolved[0] != "CN":
            continue
        ts = resolved[1]
        sd, ep = t.entry_date, t.entry_price
        i = idx.get(sd)
        if i is None or i + 1 >= len(cal):
            continue
        fd = cal[i + 1]
        bars = {str(b[0]): b for b in data.bars_by_ts.get(ts, [])}
        if sd not in bars or fd not in bars:
            continue
        sig, fl = bars[sd], bars[fd]
        f_open, f_low, f_close = float(fl[1]), float(fl[3]), float(fl[4])
        sig_close = float(sig[4])
        if not (f_open > 0 and ep > 0):
            continue
        w = next((w for w, (a, b) in WINDOWS.items() if a <= fd <= b), None)
        if w is None:
            continue
        b5f, b5s = _load_5min(ts, fd), _load_5min(ts, sd)

        # All comparisons as ratios to ep (next_open); adjustment cancels within a day.
        r_close = f_close / f_open  # fill-day close relative to open
        r1430f = _ratio_1430(b5f)  # fill-day 14:30 relative to close
        r1430s = _ratio_1430(b5s)  # signal-day 14:30 relative to close
        rec: dict = {
            "window": w,
            "base": ep,
            "f_close": _impr(ep * r_close, ep),
            "f_1430": _impr(ep * r1430f * r_close, ep) if r1430f else None,
            # live proxy: signal-day close & 14:30 vs next_open
            "s_close": _impr(sig_close, ep),
            "s_1430": _impr(sig_close * r1430s, ep) if r1430s else None,
            # daily low-touch limits on the fill session (all daily qfq)
            "L_prev": (min(f_open, sig_close), f_low <= sig_close),
            "L_op05": (f_open * 0.995, f_low <= f_open * 0.995),
            "L_op1": (f_open * 0.99, f_low <= f_open * 0.99),
            "miss_fwd": (f_close / f_open - 1.0) * 100.0,  # open->close move (miss proxy)
        }
        rows.append(rec)

    print("=" * 104)
    print("核心 S-3 入场执行微观诊断 — 冻结 CN 核心，long 2021-08~2026-08（仅诊断，无 PnL）")
    print("基准 = next_open 成交价；正值 = 该方式成交更便宜（复权在比值中抵消）")
    print("=" * 104)
    print(f"\n总样本: {len(rows)} 笔")

    alts = [
        ("f_close", "成交日收盘"),
        ("f_1430", "成交日14:30"),
        ("s_close", "信号日收盘"),
        ("s_1430", "信号日14:30(Live代理)"),
    ]
    for w in ("OOS2", "train", "valid", "long"):
        sub = [r for r in rows if r["window"] == w]
        if not sub:
            continue
        print(f"\n--- {w} (n={len(sub)}) ---")
        print(f"{'入场方式':<24}{'有效n':>7}{'均值改善%':>11}{'中位改善%':>11}{'更便宜占比':>10}")
        for key, label in alts:
            vals = [r[key] for r in sub if r.get(key) is not None]
            if not vals:
                continue
            better = sum(1 for v in vals if v > 0) / len(vals)
            print(f"{label:<24}{len(vals):>7}{st.mean(vals):>+11.2f}{st.median(vals):>+11.2f}{better:>9.0%}")
        print(f"{'限价(成交日低触)':<24}{'成交率':>9}{'成交时均值改善%':>16}")
        for key, label in (("L_prev", "=信号日收盘"), ("L_op05", "=开盘-0.5%"), ("L_op1", "=开盘-1%")):
            pairs = [(r, r[key]) for r in sub]
            rate = sum(1 for _, (_, f) in pairs if f) / len(pairs)
            imp = [(r["base"] - px) / r["base"] * 100.0 for r, (px, f) in pairs if f]
            m = st.mean(imp) if imp else float("nan")
            print(f"{label:<24}{rate:>8.1%}{m:>+16.2f}")
        miss = [r["miss_fwd"] for r in sub]
        print(f"参考: 成交日 open→close 漂移 均值 {st.mean(miss):+.2f}% / 中位 {st.median(miss):+.2f}%")


if __name__ == "__main__":
    main()
