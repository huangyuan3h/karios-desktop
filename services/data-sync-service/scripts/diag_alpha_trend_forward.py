#!/usr/bin/env python3
"""Alpha Incubator 趋势卡前瞻性诊断（预注册见 docs/designs/alpha-trend-forward-prereg-2026-09-07.md）.

问题：趋势卡分级（S/A/B）+ A 股映射，对映射票未来 10/20 日有没有相对全市场的超额？
as-of 口径：信号时刻 = trends.created_at，入场 = 下一交易日 open；
映射 = trend_json.a_share_mapping 出生中文名（防 Remap 前视）。
Read-only vs Postgres. Prints a table + K1/K2 verdict, saves nothing.
"""

from __future__ import annotations

import json
import sys
from collections import defaultdict
from datetime import date
from pathlib import Path

import psycopg

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from data_sync_service.config import get_settings  # noqa: E402

COSTS_ROUNDTRIP = 0.003
HORIZONS = (10, 20)
PRIMARY_HORIZON = 10


# ---------------------------------------------------------------------------
# Pure helpers (unit-tested without DB)
# ---------------------------------------------------------------------------


def parse_birth_names(trend_json: object) -> list[str]:
    """Birth Chinese names from the frozen creation-time mapping."""
    if isinstance(trend_json, str):
        try:
            trend_json = json.loads(trend_json)
        except (json.JSONDecodeError, TypeError):
            return []
    if not isinstance(trend_json, dict):
        return []
    names = trend_json.get("a_share_mapping") or []
    return [str(n).strip() for n in names if str(n).strip()]


def parse_current_names(cn_symbols: object) -> list[str]:
    if isinstance(cn_symbols, str):
        try:
            cn_symbols = json.loads(cn_symbols)
        except (json.JSONDecodeError, TypeError):
            return []
    if not isinstance(cn_symbols, list):
        return []
    out = []
    for row in cn_symbols:
        if isinstance(row, dict) and row.get("name"):
            out.append(str(row["name"]).strip())
    return out


def net_relative(gross: float, market: float) -> float:
    """Net-of-costs market-relative spread for one fill."""
    return (gross - COSTS_ROUNDTRIP) - market


def grade_verdict(means: dict[str, float]) -> dict[str, object]:
    """K1/K2 against pre-registered kill lines. Ordered grades S>A>B(>C)."""
    order = [g for g in ("S", "A", "B", "C") if g in means]
    mono = all(means[order[i]] > means[order[i + 1]] for i in range(len(order) - 1))
    s_nonpos = means.get("S", float("nan")) <= 0
    killed = (not mono) or bool(s_nonpos)
    reasons = []
    if not mono:
        reasons.append("K1: no monotonic S>A>B gradient")
    if s_nonpos:
        reasons.append("K2: S-leg net market-relative <= 0")
    return {"monotonic": mono, "killed": killed, "reasons": reasons, "order": order}


def batch_clustered_mean(by_batch: dict[str, list[float]]) -> tuple[float, float, int]:
    """Mean-of-batch-means + naive SE across batches (cluster guard)."""
    means = [sum(v) / len(v) for v in by_batch.values() if v]
    n = len(means)
    if not n:
        return float("nan"), float("nan"), 0
    m = sum(means) / n
    var = sum((x - m) ** 2 for x in means) / n
    return m, (var / n) ** 0.5, n


# ---------------------------------------------------------------------------
# DB readers (read-only)
# ---------------------------------------------------------------------------


def _conn():
    return psycopg.connect(get_settings().database_url)


def load_trends() -> list[dict]:
    with _conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, trend_name, catalyst_grade, driver_type,
                   created_at, trend_json, cn_symbols
            FROM alpha_radar_trends
            ORDER BY created_at
            """
        )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r, strict=False)) for r in cur.fetchall()]


def load_name_map() -> dict[str, list[str]]:
    """Chinese name -> A-share ts_codes (SH/SZ only)."""
    with _conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT name, ts_code FROM stock_basic
            WHERE ts_code LIKE '%%.SH' OR ts_code LIKE '%%.SZ'
            """
        )
        out: dict[str, list[str]] = defaultdict(list)
        for name, ts in cur.fetchall():
            out[str(name)].append(str(ts))
    return out


def load_sessions(start: str, end: str) -> list[str]:
    with _conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT cal_date FROM trade_calendar
            WHERE exchange = 'SSE' AND is_open = 1 AND cal_date BETWEEN %s AND %s
            ORDER BY cal_date
            """,
            (start, end),
        )
        return [str(r[0]) for r in cur.fetchall()]


def load_bars(ts_codes: list[str], start: str, end: str) -> dict[str, dict[str, tuple]]:
    """{(ts): {date: (open, close)}} — one batched read."""
    out: dict[str, dict[str, tuple]] = defaultdict(dict)
    if not ts_codes:
        return out
    with _conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT ts_code, trade_date, open, close FROM daily
            WHERE ts_code = ANY(%s) AND trade_date BETWEEN %s AND %s
            """,
            (ts_codes, start, end),
        )
        for ts, td, o, c in cur.fetchall():
            if o and c:
                out[str(ts)][str(td)] = (float(o), float(c))
    return out


def market_mean(entry: str, exit_: str) -> float | None:
    """Equal-weight full-A mean(exit_close / entry_open - 1) for one window."""
    with _conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT AVG(d2.close / d1.open - 1)
            FROM daily d1 JOIN daily d2
              ON d1.ts_code = d2.ts_code
             AND d1.trade_date = %s AND d2.trade_date = %s
             AND (d1.ts_code LIKE '%%.SH' OR d1.ts_code LIKE '%%.SZ')
            WHERE d1.open > 0 AND d2.close > 0
            """,
            (entry, exit_),
        )
        v = cur.fetchone()[0]
        return float(v) if v is not None else None


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> dict:
    trends = load_trends()
    name_map = load_name_map()

    # --- audit: remap drift / unmapped / ambiguous -------------------------
    n_drift = n_unmap = n_ambig = 0
    fills: list[dict] = []
    for t in trends:
        birth = parse_birth_names(t.get("trend_json"))
        current = parse_current_names(t.get("cn_symbols"))
        if set(birth) != set(current):
            n_drift += 1
        for nm in birth:
            codes = name_map.get(nm, [])
            if not codes:
                n_unmap += 1
            elif len(codes) > 1:
                n_ambig += 1
            fills.append(
                {
                    "trend_id": str(t["id"]),
                    "grade": str(t.get("catalyst_grade") or "?"),
                    "driver": str(t.get("driver_type") or "?"),
                    "created": str(t["created_at"])[:10],
                    "name": nm,
                    "ts": codes[0] if codes else None,
                }
            )
    fills = [f for f in fills if f["ts"]]
    if not fills:
        print("no mappable fills")
        return {"killed": True, "reasons": ["no mappable fills"]}

    lo = min(f["created"] for f in fills)
    sessions = load_sessions(lo, date.today().isoformat())
    sidx = {d: i for i, d in enumerate(sessions)}
    max_h = max(HORIZONS)

    # keep births whose longest forward window is complete (as-of safe)
    kept = [
        f
        for f in fills
        if f["created"] in sidx and sidx[f["created"]] + 1 + max_h < len(sessions)
    ]
    if not kept:
        print("no complete-window fills")
        return {"killed": True, "reasons": ["no complete windows"]}

    ts_codes = sorted({f["ts"] for f in kept})
    first_entry = sessions[sidx[min(f["created"] for f in kept)] + 1]
    bars = load_bars(ts_codes, first_entry, sessions[-1])

    mkt_cache: dict[tuple[str, str], float | None] = {}
    rows: list[dict] = []
    for f in kept:
        entry = sessions[sidx[f["created"]] + 1]
        b0 = bars.get(f["ts"], {}).get(entry)
        if not b0:
            continue
        for h in HORIZONS:
            exit_ = sessions[sidx[f["created"]] + 1 + h]
            b1 = bars.get(f["ts"], {}).get(exit_)
            if not b1:
                continue
            key = (entry, exit_)
            if key not in mkt_cache:
                mkt_cache[key] = market_mean(entry, exit_)
            mkt = mkt_cache[key]
            if mkt is None:
                continue
            gross = b1[1] / b0[0] - 1
            rows.append({**f, "h": h, "gross": gross, "rel": net_relative(gross, mkt)})

    # --- report -------------------------------------------------------------
    by_gh: dict[tuple[str, int], list[float]] = defaultdict(list)
    by_gh_batch: dict[tuple[str, int], dict[str, list[float]]] = defaultdict(
        lambda: defaultdict(list)
    )
    for r in rows:
        by_gh[(r["grade"], r["h"])].append(r["rel"])
        by_gh_batch[(r["grade"], r["h"])][r["created"]].append(r["rel"])

    print(f"trends={len(trends)} fills={len(rows)} "
          f"drift={n_drift}/{len(trends)} unmapped_names={n_unmap} ambiguous={n_ambig}")
    print(f"{'grade':<6}{'h':<4}{'n':<7}{'mean_rel':<10}{'hit%':<8}{'batches':<9}{'batch_mean':<11}{'batch_se'}")
    verdicts = {}
    for h in HORIZONS:
        means: dict[str, float] = {}
        for g in ("S", "A", "B", "C"):
            v = by_gh.get((g, h), [])
            if not v:
                continue
            means[g] = sum(v) / len(v)
            hit = sum(1 for x in v if x > 0) / len(v) * 100
            bm, se, nb = batch_clustered_mean(by_gh_batch[(g, h)])
            print(f"{g:<6}{h:<4}{len(v):<7}{means[g] * 100:<10.2f}{hit:<8.1f}"
                  f"{nb:<9}{bm * 100:<11.2f}{se * 100:.2f}")
        if h == PRIMARY_HORIZON:
            verdicts = grade_verdict(means)
    # --- driver split (descriptive only; pre-reg kills rest on grades) -----
    by_drv: dict[tuple[str, int], list[float]] = defaultdict(list)
    for r in rows:
        drv = r["driver"] if r["driver"] not in ("?", "None", "") else "(none)"
        by_drv[(drv, r["h"])].append(r["rel"])
    print(f"{'driver':<16}{'h':<4}{'n':<7}{'mean_rel':<10}{'hit%':<8}")
    for (drv, h), v in sorted(by_drv.items(), key=lambda kv: (kv[0][1], kv[0][0])):
        hit = sum(1 for x in v if x > 0) / len(v) * 100
        print(f"{drv:<16}{h:<4}{len(v):<7}{sum(v) / len(v) * 100:<10.2f}{hit:<8.1f}")
    print("VERDICT:", verdicts.get("reasons") or ["PASS (limited) -> forward paper log, never Live"])
    print("killed:", verdicts.get("killed", True))
    return verdicts


if __name__ == "__main__":
    main()
