"""Monthly ETF research-panel snapshot (data/etf/etf_daily.csv).

The research panel (tushare fund_daily + fund_adj, close_adj basis) is a
manual script today (scripts/sync_etf_daily.py) — no scheduler owns it, so
it went stale (2026-09-11, 22 calendar days behind on 2026-10-03). The
display path already survives via harbor.merge_recent_db_closes (DB tail),
but frozen backtests and the B3 card deserve a fresh monthly anchor.

This service is the schedulable equivalent of the script: same UNIVERSE,
same close_adj math, throttled (0.4s/call, ~80 calls) to stay under the
tushare 200/min limit, incremental merge (only rewrites rows for the
requested window, keeps history byte-identical).
"""

from __future__ import annotations

import csv
import logging
import time
from pathlib import Path

logger = logging.getLogger(__name__)

CSV_PATH = Path(__file__).resolve().parents[3] / "data" / "etf" / "etf_daily.csv"
META_PATH = Path(__file__).resolve().parents[3] / "data" / "etf" / "etf_meta.csv"

# Same universe as scripts/sync_etf_daily.py (ts_code -> (label, kind)).
UNIVERSE: dict[str, tuple[str, str]] = {
    "510050.SH": ("上证50", "broad"),
    "510300.SH": ("沪深300", "broad"),
    "510500.SH": ("中证500", "broad"),
    "159915.SZ": ("创业板", "broad"),
    "588000.SH": ("科创50", "broad"),
    "159949.SZ": ("创业板50", "broad"),
    "518880.SH": ("黄金", "broad"),
    "513350.SH": ("原油", "broad"),
    "513100.SH": ("纳指100", "broad"),
    "513110.SH": ("纳指100b", "broad"),
    "513180.SH": ("恒生科技", "broad"),
    "511260.SH": ("十年国债", "broad"),
    "512880.SH": ("证券", "sector"),
    "512800.SH": ("银行", "sector"),
    "512480.SH": ("半导体", "sector"),
    "512760.SH": ("芯片", "sector"),
    "159995.SZ": ("半导体芯片", "sector"),
    "515880.SH": ("通信", "sector"),
    "515000.SH": ("科技龙头", "sector"),
    "512720.SH": ("计算机", "sector"),
    "512010.SH": ("医药", "sector"),
    "512170.SH": ("医疗", "sector"),
    "159928.SZ": ("消费", "sector"),
    "512690.SH": ("酒", "sector"),
    "515170.SH": ("食品饮料", "sector"),
    "515030.SH": ("新能源车", "sector"),
    "515790.SH": ("光伏", "sector"),
    "516160.SH": ("新能源", "sector"),
    "159611.SZ": ("电力公用", "sector"),
    "512660.SH": ("军工", "sector"),
    "512400.SH": ("有色金属", "sector"),
    "515220.SH": ("煤炭", "sector"),
    "515210.SH": ("钢铁", "sector"),
    "512200.SH": ("房地产", "sector"),
    "512980.SH": ("传媒", "sector"),
    "512580.SH": ("环保", "sector"),
    "159865.SZ": ("畜牧养殖", "sector"),
    "512070.SH": ("非银金融", "sector"),
    "516110.SH": ("汽车", "sector"),
    "159766.SZ": ("旅游", "sector"),
    "159869.SZ": ("动漫游戏", "sector"),
}

FIELDS = ["ts_code", "trade_date", "open", "high", "low", "close", "pre_close", "vol", "amount", "adj_factor", "close_adj"]


def _default_pro_factory():
    from data_sync_service.clients.tushare_pool import get_pool

    return get_pool().pro()


def refresh_window(
    start_yyyymmdd: str,
    end_yyyymmdd: str,
    *,
    pro_factory=None,
    sleep=time.sleep,
) -> dict:
    """Fetch fund_daily+fund_adj for UNIVERSE in [start, end] and merge into CSV.

    Returns {"ok", "updated", "codes", "csv_max"} — merge keeps rows outside
    the window untouched, so history stays byte-identical.
    """
    try:
        pro = (pro_factory or _default_pro_factory)()
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": str(exc)}
    # Load existing rows.
    existing: dict[tuple[str, str], dict] = {}
    if CSV_PATH.exists():
        with CSV_PATH.open(newline="", encoding="utf-8") as fh:
            for row in csv.DictReader(fh):
                existing[(str(row.get("ts_code")), str(row.get("trade_date")))] = row
    updated = 0
    failed: list[str] = []
    for ts in UNIVERSE:
        try:
            px = pro.fund_daily(ts_code=ts, start_date=start_yyyymmdd, end_date=end_yyyymmdd)
            adj = pro.fund_adj(ts_code=ts, start_date=start_yyyymmdd, end_date=end_yyyymmdd)
            sleep(0.4)
        except Exception as exc:  # noqa: BLE001
            logger.warning("etf_snapshot %s failed: %s", ts, exc)
            failed.append(ts)
            sleep(1.0)
            continue
        try:
            adj_map = {str(r.trade_date): float(r.adj_factor) for r in adj.itertuples()} if adj is not None and not adj.empty else {}
        except Exception:  # noqa: BLE001
            adj_map = {}
        if px is None or px.empty:
            continue
        for r in px.itertuples():
            d = str(r.trade_date)
            f = adj_map.get(d)
            if f is None:
                continue
            try:
                close = float(r.close)
            except (TypeError, ValueError):
                continue
            existing[(ts, d)] = {
                "ts_code": ts,
                "trade_date": d,
                "open": str(float(r.open)),
                "high": str(float(r.high)),
                "low": str(float(r.low)),
                "close": str(close),
                "pre_close": str(float(r.pre_close)),
                "vol": str(float(r.vol)),
                "amount": str(float(r.amount)),
                "adj_factor": str(f),
                "close_adj": str(round(close * f, 6)),
            }
            updated += 1
    if updated:
        CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
        rows = [existing[k] for k in sorted(existing)]
        with CSV_PATH.open("w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=FIELDS)
            w.writeheader()
            w.writerows(rows)
    csv_max = max((d for _, d in existing), default="")
    out: dict = {"ok": True, "updated": updated, "codes": len(UNIVERSE), "csv_max": csv_max}
    if failed:
        out["failed"] = failed
    return out


def csv_status() -> dict:
    """Current CSV max date / row count (for health checks)."""
    if not CSV_PATH.exists():
        return {"exists": False, "max_date": None, "rows": 0}
    n = 0
    mx = ""
    with CSV_PATH.open(newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            n += 1
            d = str(row.get("trade_date") or "")
            if d > mx:
                mx = d
    return {"exists": True, "max_date": mx, "rows": n, "path": str(CSV_PATH)}
