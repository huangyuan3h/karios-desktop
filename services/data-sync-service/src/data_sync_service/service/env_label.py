"""TIP-014 · Phase 1a — daily market-environment labels.

Goal: classify each trading day into an environment bucket so the backtest
engine can apply the right entry *style* (momentum / dip / neutral).

Buckets:
- ``uptrend``   — 主升: breadth strong + limit-up premium positive + riskMode hot/normal
- ``fan``       — 电风扇: mixed breadth (ratio ~0.5..1.5) OR mainline churns fast
- ``weak``      — 弱势/恐慌: riskMode in {extreme_caution, no_new_positions}
                  OR breadth ratio < 0.5 (implicit-weak, TIP-014 finding #3)
- ``neutral``   — TRUE neutral: sentiment + mainline data both present but the
                  day is neither uptrend nor fan (blockable — 16/16 losing
                  trades in the valid window). Never assigned from ratio alone.
- ``unknown``   — no sentiment data (pre-2026-01 / missing) → engine keeps
                  entries open (OOS2/train windows).

IMPORTANT (TIP-014, 2026-08-14): these labels are INDUSTRY-INDEPENDENT —
they classify the MARKET day, not any sector. The industry restriction is a
SEPARATE layer (backtest mainline gate / live execution gate mainline
whitelist), recomputed daily from 5D net inflow; it never hardcodes a sector.

Data sources (all DB-local, no new sync):
- ``market_cn_sentiment_daily`` — up/down counts, up_down_ratio,
  yesterday_limitup_premium, failed_limitup_rate, risk_mode (from 2026-01-05)
- ``market_cn_industry_mainline_scores_daily`` — per-day industry total_score
  (top-3 churn rate = fan/uptrend discriminator)

Caveats (recorded in docs/trading-improvement-checklist.md TIP-014):
- sentiment data starts 2026-01-05 → OOS2 (2024-08..2025-08) has no labels;
  engine falls back to UNKNOWN there.
- mainline scores start 2026-02-10; before that churn is unavailable and
  neutral is never assigned (ratio-only days → unknown), which fixed the
  train-window noise (-15pt mislabels).
"""

from __future__ import annotations

from data_sync_service.db import get_connection

# Bucket constants — also used by the backtest engine to pick entry styles.
ENV_UPTREND = "uptrend"
ENV_FAN = "fan"
ENV_WEAK = "weak"
ENV_NEUTRAL = "neutral"
# No sentiment data for the day (pre-2026-01 / missing) — NOT the same as
# neutral: blocking those days would kill OOS2/train entries.
ENV_UNKNOWN = "unknown"

_WEAK_MODES = frozenset({"extreme_caution", "no_new_positions"})
_HOT_MODES = frozenset({"hot", "euphoric", "normal"})

# Breadth ratio thresholds (up_count / down_count).
UPTREND_RATIO_MIN = 2.0
FAN_RATIO_LOW = 0.5
FAN_RATIO_HIGH = 1.5
# TIP-014 finding #3: ratio < 0.5 = 跌家数 > 2× 涨家数 → implicit weak day
# even when risk_mode is only normal/caution (16/16 losing trades in valid
# window, avg -6.1%). These must be WEAK (blocked), not neutral.
WEAK_RATIO_MAX = 0.5
# Limit-up premium > 0 = positive momentum carry-over.
UPTREND_PREMIUM_MIN = 0.0
# Mainline top-3 churn: >= 2/3 names differ from the previous day → fan.
MAINLINE_CHURN_RATIO = 2.0 / 3.0
MAINLINE_TOP_N = 3


def load_env_by_day(start_date: str, end_date: str) -> dict[str, str]:
    """Per-day environment bucket for [start_date, end_date].

    Days without sentiment data → neutral (OOS2 window).
    """
    ratios: dict[str, float] = {}
    premiums: dict[str, float] = {}
    modes: dict[str, str] = {}
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT date, up_count, down_count, yesterday_limitup_premium, risk_mode
                FROM market_cn_sentiment_daily
                WHERE date >= %s AND date <= %s
                """,
                (start_date, end_date),
            )
            for d, up, down, premium, mode in cur.fetchall():
                d = str(d)
                ratios[d] = float(up) / float(down) if down and float(down) > 0 else 1.0
                premiums[d] = float(premium or 0.0)
                modes[d] = str(mode or "")
    mainline_top3 = _load_mainline_top3(start_date, end_date)

    out: dict[str, str] = {}
    prev_top3: set[str] | None = None
    for d, ratio in ratios.items():
        mode = modes.get(d, "")
        if mode in _WEAK_MODES or ratio < WEAK_RATIO_MAX:
            out[d] = ENV_WEAK
            continue
        churn = _churn_ratio(mainline_top3.get(d), prev_top3)
        if (
            ratio >= UPTREND_RATIO_MIN
            and premiums.get(d, 0.0) >= UPTREND_PREMIUM_MIN
            and mode in _HOT_MODES
        ):
            out[d] = ENV_UPTREND
        elif churn is not None and churn >= MAINLINE_CHURN_RATIO:
            out[d] = ENV_FAN
        elif FAN_RATIO_LOW <= ratio <= FAN_RATIO_HIGH:
            out[d] = ENV_FAN
        elif mainline_top3.get(d) is not None:
            # Full data present (sentiment + mainline) but the day is neither
            # uptrend nor fan → TRUE neutral. Only these days are blockable
            # (TIP-014 finding #3); ratio-only days (no mainline yet, e.g.
            # pre-2026-02) are UNKNOWN — blocking them was noise (train -15pt).
            out[d] = ENV_NEUTRAL
        # else: sentiment present but mainline missing → UNKNOWN (absent)
        if mainline_top3.get(d) is not None:
            prev_top3 = set(mainline_top3[d])
    return out


def _load_mainline_top3(start_date: str, end_date: str) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT date, industry_name, total_score FROM (
                        SELECT date, industry_name, total_score,
                               ROW_NUMBER() OVER (PARTITION BY date ORDER BY total_score DESC) AS rn
                        FROM market_cn_industry_mainline_scores_daily
                        WHERE date >= %s AND date <= %s
                    ) t WHERE rn <= %s
                    """,
                    (start_date, end_date, MAINLINE_TOP_N),
                )
                for d, name, _score in cur.fetchall():
                    out.setdefault(str(d), []).append(str(name))
    except Exception:  # table may not exist in fresh DBs — degrade to ratio-only
        return {}
    return out


def _churn_ratio(today: list[str] | None, prev: set[str] | None) -> float | None:
    """Fraction of today's top-3 that differs from yesterday's top-3.

    None when either day is missing (no churn signal → ratio-only fan).
    """
    if today is None or prev is None:
        return None
    if not prev:
        return 0.0
    today_set = set(today)
    changed = len(today_set - prev)
    return changed / max(1, len(today_set))
