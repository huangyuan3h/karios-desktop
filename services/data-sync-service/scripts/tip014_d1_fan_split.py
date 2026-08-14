"""D1 · fan-day split — does dip work better when a mainline EXISTS?

Attribution over the valid window: for every CLOSED trade whose entry day
was labelled fan, split by whether the day's mainline top-3 total_score was
HIGH (a strong mainline exists, just rotating) vs LOW (no real mainline —
random rotation), then compare dip vs momentum realised PnL inside each.

Question: should the dip filter be stricter (or the whole fan bucket behave
differently) when there is no credible mainline?
"""

from __future__ import annotations

import json
import sys
from collections import defaultdict

from data_sync_service.service import backtest_engine as be
from data_sync_service.service.env_label import (
    ENV_FAN,
    load_env_by_day,
    _load_mainline_top3,
)
from data_sync_service.service.trendok import _symbol_to_ts_code

S3_CONFIG = {
    "score_threshold": 65.0,
    "max_hold_days": 60,
    "stop_loss_pct": -5.0,
    "target_pnl_pct": 100.0,
    "score_floor": 0.0,
    "market": "CN",
    "gates": "full",
    "trailing_stop_pct": -8.0,
    "position_pct": 0.10,
    "max_positions": 20,
    "rs_rank_min": 0.5,
    "diverging_scale": 1.0,
    "panic_cooldown_days": 3,
    "drawdown_circuit_pct": -25.0,
    "slippage_pct": 0.05,
    "pyramid_trigger_pct": 2.5,
    "pyramid_add_scale": 0.5,
    "pyramid_max_adds": 1,
    "exclude_boards": "300",
    "atr_stop_mult": 2.0,
    "atr_stop_strong_only": True,
    "neutral_block": True,
    "entry_style": "auto",
    "entry_style_rs_min": 0.7,
    "entry_style_dip_min": 3.0,
}

WINDOWS = {"OOS2": ("2024-08-01", "2025-08-01"), "train": ("2025-08-01", "2026-02-01"), "valid": ("2026-03-01", "2026-08-07")}

# A mainline top-3 with avg score >= 70 counts as "credible mainline".
MAINLINE_SCORE_MIN = 70.0


def _ts_of(symbol: str) -> str | None:
    parsed = _symbol_to_ts_code(symbol)
    return parsed[2] if parsed else None


def ret5_of(closes: list, day: str) -> float | None:
    idx = None
    for i, (d, _c) in enumerate(closes):
        if str(d) == day:
            idx = i
            break
    if idx is None or idx < 5:
        return None
    c_prev = closes[idx - 5][1]
    c_now = closes[idx][1]
    if c_prev <= 0:
        return None
    return (c_now / c_prev - 1.0) * 100.0


def classify(rs, ret5):
    if rs is None or rs < 0.7:
        return "low_rs"
    if ret5 is None:
        return "no_ret5"
    if ret5 <= -3.0:
        return "dip"
    return "momentum"


def main() -> int:
    out: dict = {}
    for name, (s, e) in WINDOWS.items():
        cfg = be.BacktestConfig(start_date=s, end_date=e, **S3_CONFIG)
        data = be.BacktestData(cfg)
        env = load_env_by_day(s, e)
        mainline_scores = _load_mainline_top3_scores(s, e)
        run = be.simulate(cfg, data=data)

        # fan days split: credible-mainline vs no-credible-mainline
        buckets = {"fan_with_mainline": defaultdict(list), "fan_no_mainline": defaultdict(list)}
        for t in run.trades:
            if env.get(t.entry_date) != ENV_FAN:
                continue
            scores = mainline_scores.get(t.entry_date)
            credible = bool(scores) and (sum(scores) / len(scores)) >= MAINLINE_SCORE_MIN
            b = buckets["fan_with_mainline" if credible else "fan_no_mainline"]
            ts = _ts_of(t.symbol)
            rs = data.rs_rank_by_day.get(t.entry_date, {}).get(ts) if ts else None
            closes = data.closes_by_ts.get(ts) if ts else None
            ret5 = ret5_of(closes, t.entry_date) if closes else None
            b[classify(rs, ret5)].append(t.pnl_pct)

        print(f"=== {name} · fan days ===")
        for bk, styles in buckets.items():
            if not styles:
                continue
            tot = sum(v for vv in styles.values() for v in vv)
            n = sum(len(v) for v in styles.values())
            line = f"  {bk:22s} n={n:3d} avg={tot/n:+.2f}%"
            for st in ("dip", "momentum", "low_rs"):
                if styles.get(st):
                    v = styles[st]
                    line += f"   {st}={sum(v)/len(v):+.2f}%({len(v)})"
            print(line)
            out[f"{name}:{bk}"] = {
                st: {"n": len(v), "avg": round(sum(v) / len(v), 2) if v else 0}
                for st, v in styles.items()
                if v
            }
    json.dump(out, open("data/backtest_reports/tip014_d1_fan_split.json", "w"), indent=1, ensure_ascii=False)
    print("\nreport -> data/backtest_reports/tip014_d1_fan_split.json")
    return 0


def _load_mainline_top3_scores(start_date: str, end_date: str) -> dict[str, list[float]]:
    """{date: [top3 total_scores]} — reuses env_label's loader table."""
    try:
        from data_sync_service.db import get_connection

        out: dict[str, list[float]] = {}
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT date, total_score FROM (
                        SELECT date, total_score,
                               ROW_NUMBER() OVER (PARTITION BY date ORDER BY total_score DESC) AS rn
                        FROM market_cn_industry_mainline_scores_daily
                        WHERE date >= %s AND date <= %s
                    ) t WHERE rn <= 3
                    """,
                    (start_date, end_date),
                )
                for d, sc in cur.fetchall():
                    out.setdefault(str(d), []).append(float(sc))
        return out
    except Exception:
        return {}


if __name__ == "__main__":
    sys.exit(main())
