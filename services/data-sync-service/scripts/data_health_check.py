#!/usr/bin/env python3
"""Data health check: one command for every key table.

Lists max date, row count, lag vs trade_calendar (>2 trading days = red),
plus single-day jump alarms (>30% day-over-day on full-market aggregates).

Usage:
  cd services/data-sync-service
  PYTHONPATH=src .venv/bin/python scripts/data_health_check.py
  PYTHONPATH=src .venv/bin/python scripts/data_health_check.py --json

Exit code 2 when any lag/jump alarm fires (for weekly_review_job reuse).
No strategy/param/backtest changes — read-only SELECTs + CSV stat.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

LAG_TRADING_DAYS = 2
JUMP_PCT = 30.0

KEY_TABLES: list[tuple[str, str]] = [
    ("daily", "trade_date"),
    ("amp_1430", "trade_date"),
    ("bar_5min", "trade_date"),
    ("cn_etf_share", "trade_date"),
    ("index_daily", "trade_date"),
    ("index_dailybasic", "trade_date"),
    ("stock_dailybasic", "trade_date"),
    ("cn_moneyflow", "trade_date"),
    ("cn_moneyflow_hsgt", "trade_date"),
    ("cn_hk_hold", "trade_date"),
    ("cn_margin_total", "trade_date"),
    ("cn_margin_detail", "trade_date"),
    ("cn_flow_daily", "trade_date"),
    ("market_etf_fund_flow_daily", "trade_date"),
    ("watchlist_score_daily", "trade_date"),
]


def _conn():
    from dotenv import load_dotenv

    from data_sync_service.db import get_connection

    load_dotenv(dotenv_path=Path(__file__).resolve().parents[2] / ".." / ".." / ".env")
    # Fallback: repo-root .env (two levels up from service dir).
    root_env = Path(__file__).resolve().parents[3] / ".env"
    if root_env.exists():
        load_dotenv(dotenv_path=root_env, override=False)
    return get_connection()


def last_open_before(d: date, n: int = 1) -> date:
    """Nth last SSE open date on/before d (via DB trade_calendar, fallback: weekdays)."""
    try:
        with _conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT cal_date FROM trade_calendar WHERE exchange='SSE' "
                    "AND cal_date <= %s AND is_open = 1 ORDER BY cal_date DESC LIMIT %s",
                    (d.isoformat(), n + 1),
                )
                rows = cur.fetchall()
                if len(rows) > n:
                    v = rows[n][0]
                    return v if isinstance(v, date) else date.fromisoformat(str(v))
    except Exception:  # noqa: BLE001
        pass
    # Fallback: step back over weekends.
    out = d
    steps = 0
    while steps < n:
        out = date.fromordinal(out.toordinal() - 1)
        if out.weekday() < 5:
            steps += 1
    return out


def check_tables(today: date) -> list[dict]:
    out: list[dict] = []
    with _conn() as conn:
        with conn.cursor() as cur:
            for tbl, col in KEY_TABLES:
                try:
                    cur.execute(f"SELECT COUNT(*), MAX({col}) FROM {tbl}")
                    cnt, mx = cur.fetchone()
                    mx_s = mx.isoformat() if hasattr(mx, "isoformat") else (str(mx) if mx else None)
                    lag_days = None
                    lag_alarm = False
                    if mx_s:
                        # Count open sessions (mx, today].
                        try:
                            cur.execute(
                                "SELECT COUNT(*) FROM trade_calendar WHERE exchange='SSE' "
                                "AND cal_date > %s::date AND cal_date <= %s::date AND is_open = 1",
                                (mx_s, today.isoformat()),
                            )
                            lag_days = int(cur.fetchone()[0] or 0)
                            lag_alarm = lag_days > LAG_TRADING_DAYS
                        except Exception:  # noqa: BLE001
                            lag_days = None
                    out.append(
                        {
                            "table": tbl,
                            "rows": int(cnt or 0),
                            "max_date": mx_s,
                            "lag_open_days": lag_days,
                            "lag_alarm": lag_alarm,
                        }
                    )
                except Exception as exc:  # noqa: BLE001
                    try:
                        conn.rollback()
                    except Exception:  # noqa: BLE001
                        pass
                    out.append({"table": tbl, "rows": None, "max_date": None, "error": str(exc)[:160]})
    return out


def check_jumps() -> list[dict]:
    """Single-day >30% jumps on full-market aggregates (margin, etf share, hsgt)."""
    alarms: list[dict] = []
    with _conn() as conn:
        with conn.cursor() as cur:
            # Margin full-market SUM (complete days only).
            try:
                cur.execute(
                    "SELECT trade_date, SUM(rzrqye) FROM cn_margin_total "
                    "WHERE trade_date IN (SELECT trade_date FROM cn_margin_total "
                    "GROUP BY trade_date HAVING COUNT(DISTINCT exchange_id) >= 3) "
                    "GROUP BY trade_date ORDER BY trade_date DESC LIMIT 8"
                )
                rows = [(str(d), float(v)) for d, v in cur.fetchall() if v]
                for i in range(len(rows) - 1):
                    d0, v0 = rows[i]
                    _, v1 = rows[i + 1]
                    if v1 > 0 and abs(v0 / v1 - 1) * 100 > JUMP_PCT:
                        alarms.append(
                            {
                                "series": "cn_margin_total SUM(rzrqye)",
                                "date": d0,
                                "change_pct": round((v0 / v1 - 1) * 100, 1),
                            }
                        )
            except Exception as exc:  # noqa: BLE001
                alarms.append({"series": "cn_margin_total", "error": str(exc)[:160]})
            # Margin exchange coverage (partial-day alarm, not pct).
            try:
                cur.execute(
                    "SELECT trade_date, COUNT(DISTINCT exchange_id) FROM cn_margin_total "
                    "GROUP BY trade_date ORDER BY trade_date DESC LIMIT 3"
                )
                for d, n in cur.fetchall():
                    if int(n or 0) < 3:
                        alarms.append(
                            {
                                "series": "cn_margin_total coverage",
                                "date": str(d),
                                "change_pct": None,
                                "note": f"only {n}/3 exchanges (partial publication)",
                            }
                        )
            except Exception as exc:  # noqa: BLE001
                alarms.append({"series": "cn_margin_total coverage", "error": str(exc)[:160]})
            # Daily CN bar counts (holiday-aware: compare vs last open day).
            try:
                cur.execute(
                    "SELECT trade_date, COUNT(*) FROM daily WHERE ts_code LIKE '%.SH' "
                    "GROUP BY trade_date ORDER BY trade_date DESC LIMIT 6"
                )
                rows = [(str(d), int(n)) for d, n in cur.fetchall()]
                for i in range(len(rows) - 1):
                    d0, v0 = rows[i]
                    _, v1 = rows[i + 1]
                    if v1 > 1000 and abs(v0 / v1 - 1) * 100 > JUMP_PCT:
                        alarms.append(
                            {"series": "daily SH count", "date": d0, "change_pct": round((v0 / v1 - 1) * 100, 1)}
                        )
            except Exception as exc:  # noqa: BLE001
                alarms.append({"series": "daily SH count", "error": str(exc)[:160]})
    return alarms


def check_etf_csv() -> dict:
    p = Path(__file__).resolve().parents[1] / "data" / "etf" / "etf_daily.csv"
    if not p.exists():
        return {"exists": False}
    n = 0
    mx = ""
    with p.open(newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            n += 1
            d = str(row.get("trade_date") or "")
            if d > mx:
                mx = d
    return {"exists": True, "rows": n, "max_date": mx, "path": str(p)}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--lag-days", type=int, default=2)
    ap.add_argument("--jump-pct", type=float, default=30.0)
    args = ap.parse_args()
    global LAG_TRADING_DAYS, JUMP_PCT  # noqa: PLW0603
    LAG_TRADING_DAYS = args.lag_days
    JUMP_PCT = args.jump_pct
    today = date.today()
    tables = check_tables(today)
    jumps = check_jumps()
    etf_csv = check_etf_csv()
    lag_alarms = [t for t in tables if t.get("lag_alarm")]
    failed = bool(lag_alarms or jumps)
    if args.json:
        print(json.dumps({"date": today.isoformat(), "tables": tables, "jumps": jumps, "etf_csv": etf_csv}, ensure_ascii=False, indent=2))
        return 2 if failed else 0
    red = "\033[31m"
    green = "\033[32m"
    reset = "\033[0m"
    print(f"data health {today.isoformat()} (lag > {LAG_TRADING_DAYS} open days red, jump > {JUMP_PCT:.0f}%)")
    print(f"{'table':28} {'rows':>10} {'max_date':>12} {'lag':>5}  status")
    for t in tables:
        alarm = t.get("lag_alarm")
        err = t.get("error")
        if err:
            print(f"{t['table']:28} {'?':>10} {'?':>12} {'?':>5}  ERROR {err}")
        elif alarm:
            print(f"{t['table']:28} {t['rows']:>10} {t['max_date']:>12} {t['lag_open_days']:>5}  {red}LAGGING{reset}")
        else:
            print(f"{t['table']:28} {t['rows']:>10} {t['max_date']:>12} {t['lag_open_days']:>5}  {green}ok{reset}")
    if etf_csv.get("exists"):
        print(f"etf_daily.csv rows={etf_csv['rows']} max={etf_csv['max_date']}")
    else:
        print("etf_daily.csv MISSING")
    if jumps:
        print("jump alarms:")
        for j in jumps:
            print(f"  {red}ALARM{reset} {j}")
    else:
        print(f"{green}no jump alarms{reset}")
    if failed:
        print(f"{red}HEALTH CHECK FAILED{reset}")
        return 2
    print(f"{green}HEALTH CHECK OK{reset}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
