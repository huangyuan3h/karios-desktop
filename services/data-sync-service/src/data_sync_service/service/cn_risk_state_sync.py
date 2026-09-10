"""TIP-017 risk-state sync — ETF shares / margin total / north-bound / global index.

Backfill + daily catch-up for the risk-state sensor family (pre-registration:
docs/backtests/risk-state-sensors-2026-09-09.md). H3 conventions: single
pooled client via ``get_pool()``, injectable ``pro_factory`` for tests,
``{"ok", "error"}`` dicts across service boundaries, no network/DB at import.
"""

from __future__ import annotations

import logging
import time
from datetime import date

from data_sync_service.db import cn_risk_state
from data_sync_service.db.trade_calendar import get_open_dates

logger = logging.getLogger(__name__)

# 宽基 ETF 池（国家队代理 · 预注册固定）：沪深300 / 中证500 / 上证50 / 创业板
BROAD_ETF_CODES: tuple[str, ...] = ("510300.SH", "510500.SH", "510510.SH", "159915.SZ")
# Global index codes: tushare index_global names HSTECH "HKTECH"; store as HSTECH.
GLOBAL_INDEX_CODES: tuple[str, ...] = ("HSI", "HSTECH")
GLOBAL_INDEX_TUSHARE_ALIAS: dict[str, str] = {"HSTECH": "HKTECH"}


def _default_pro_factory():
    from data_sync_service.clients.tushare_pool import get_pool

    return get_pool().pro()


def _iso(d) -> str:
    if isinstance(d, date):
        return d.isoformat()
    return str(d)


def _open_dates(start: str, end: str, *, pro_factory=None) -> list[str]:
    """SSE open dates in [start, end] — tushare trade_cal first (full history,
    the DB trade_calendar table starts 2023), DB calendar as fallback."""
    try:
        pro = (pro_factory or _default_pro_factory)()
        cal = pro.trade_cal(
            exchange="SSE",
            start_date=start.replace("-", ""),
            end_date=end.replace("-", ""),
            is_open="1",
        )
        days = sorted(str(d) for d in cal["cal_date"].tolist())
        return [f"{d[:4]}-{d[4:6]}-{d[6:8]}" for d in days]
    except Exception:
        try:
            return [d.isoformat() for d in get_open_dates("SSE", date.fromisoformat(start), date.fromisoformat(end))]
        except Exception:  # noqa: BLE001 — no calendar either → caller narrows
            return []


def sync_etf_share(
    start_date: str,
    end_date: str | None = None,
    *,
    pro_factory=None,
    sleep=time.sleep,
) -> dict:
    """Broad-ETF daily share counts (tushare fund_share). One call per code."""
    try:
        pro = (pro_factory or _default_pro_factory)()
        codes = BROAD_ETF_CODES
        total = 0
        for ts in codes:
            kwargs: dict = {"ts_code": ts}
            if end_date:
                kwargs["start_date"] = start_date.replace("-", "")
                kwargs["end_date"] = end_date.replace("-", "")
            df = pro.fund_share(**kwargs)
            if df is not None and not df.empty:
                rows = [
                    {
                        "trade_date": getattr(r, "trade_date", None),
                        "ts_code": getattr(r, "ts_code", None),
                        "fd_share": getattr(r, "fd_share", None),
                        "fund_type": getattr(r, "fund_type", None),
                        "market": getattr(r, "market", None),
                    }
                    for r in df.itertuples()
                ]
                total += cn_risk_state.upsert_etf_share(rows)
            sleep(0.3)
        return {"ok": True, "updated": total, "codes": list(codes)}
    except Exception as exc:  # noqa: BLE001
        logger.warning("sync_etf_share failed: %s", exc)
        return {"ok": False, "error": str(exc)}


def sync_margin_total(
    start_date: str,
    end_date: str,
    *,
    pro_factory=None,
    sleep=time.sleep,
) -> dict:
    """Market-wide margin balance per exchange (tushare margin, per trade_date)."""
    try:
        pro = (pro_factory or _default_pro_factory)()
        days = _open_dates(start_date, end_date, pro_factory=pro_factory)
        if not days:
            days = [start_date.replace("-", "")] if start_date else []
        days = [d.replace("-", "") for d in days]
        total = 0
        for td in days:
            df = pro.margin(trade_date=td)
            if df is not None and not df.empty:
                rows = [
                    {
                        "trade_date": getattr(r, "trade_date", None),
                        "exchange_id": getattr(r, "exchange_id", None),
                        "rzye": getattr(r, "rzye", None),
                        "rzmre": getattr(r, "rzmre", None),
                        "rzche": getattr(r, "rzche", None),
                        "rqye": getattr(r, "rqye", None),
                        "rqmcl": getattr(r, "rqmcl", None),
                        "rzrqye": getattr(r, "rzrqye", None),
                        "rqyl": getattr(r, "rqyl", None),
                    }
                    for r in df.itertuples()
                ]
                total += cn_risk_state.upsert_margin_total(rows)
            sleep(0.25)
        return {"ok": True, "updated": total, "days": len(days)}
    except Exception as exc:  # noqa: BLE001
        logger.warning("sync_margin_total failed: %s", exc)
        return {"ok": False, "error": str(exc)}


def sync_moneyflow_hsgt(
    start_date: str,
    end_date: str,
    *,
    pro_factory=None,
    sleep=time.sleep,
) -> dict:
    """North/south-bound flow (tushare moneyflow_hsgt; range call, chunked by year)."""
    try:
        pro = (pro_factory or _default_pro_factory)()
        total = 0
        y0, y1 = int(start_date[:4]), int(end_date[:4])
        for year in range(y0, y1 + 1):
            s = max(start_date, f"{year}-01-01")
            e = min(end_date, f"{year}-12-31")
            if s > e:
                continue
            df = pro.moneyflow_hsgt(start_date=s.replace("-", ""), end_date=e.replace("-", ""))
            if df is not None and not df.empty:
                rows = [
                    {
                        "trade_date": getattr(r, "trade_date", None),
                        "ggt_ss": getattr(r, "ggt_ss", None),
                        "ggt_sz": getattr(r, "ggt_sz", None),
                        "hgt": getattr(r, "hgt", None),
                        "sgt": getattr(r, "sgt", None),
                        "north_money": getattr(r, "north_money", None),
                        "south_money": getattr(r, "south_money", None),
                    }
                    for r in df.itertuples()
                ]
                total += cn_risk_state.upsert_hsgt(rows)
            sleep(0.3)
        return {"ok": True, "updated": total}
    except Exception as exc:  # noqa: BLE001
        logger.warning("sync_moneyflow_hsgt failed: %s", exc)
        return {"ok": False, "error": str(exc)}


def sync_global_index(
    start_date: str,
    end_date: str | None = None,
    *,
    pro_factory=None,
    sleep=time.sleep,
) -> dict:
    """Global index bars (HSI/HSTECH via tushare index_global). One call per code."""
    try:
        pro = (pro_factory or _default_pro_factory)()
        total = 0
        for ts in GLOBAL_INDEX_CODES:
            kwargs: dict = {"ts_code": GLOBAL_INDEX_TUSHARE_ALIAS.get(ts, ts)}
            if end_date:
                kwargs["start_date"] = start_date.replace("-", "")
                kwargs["end_date"] = end_date.replace("-", "")
            df = pro.index_global(**kwargs)
            if df is not None and not df.empty:
                rows = [
                    {
                        "trade_date": getattr(r, "trade_date", None),
                        "ts_code": ts,
                        "open": getattr(r, "open", None),
                        "high": getattr(r, "high", None),
                        "low": getattr(r, "low", None),
                        "close": getattr(r, "close", None),
                    }
                    for r in df.itertuples()
                ]
                total += cn_risk_state.upsert_global_index(rows)
            sleep(0.3)
        return {"ok": True, "updated": total, "codes": list(GLOBAL_INDEX_CODES)}
    except Exception as exc:  # noqa: BLE001
        logger.warning("sync_global_index failed: %s", exc)
        return {"ok": False, "error": str(exc)}


def sync_flow_daily(start_date: str, end_date: str) -> dict:
    """Daily size-class flow rollup from cn_moneyflow (pure DB aggregate)."""
    try:
        updated = cn_risk_state.aggregate_flow_daily(start_date, end_date)
        return {"ok": True, "updated": updated}
    except Exception as exc:  # noqa: BLE001
        logger.warning("sync_flow_daily failed: %s", exc)
        return {"ok": False, "error": str(exc)}


def sync_etf_daily(
    start_date: str,
    end_date: str,
    *,
    pro_factory=None,
    sleep=time.sleep,
) -> dict:
    """Broad-ETF daily bars → `daily` table (same store as sleeve ETFs).

    Gives the flow panorama the NAV side of share Δ (万份×元 → 亿元).
    """
    try:
        from data_sync_service.db.daily import upsert_from_dataframe

        pro = (pro_factory or _default_pro_factory)()
        total = 0
        for ts in BROAD_ETF_CODES:
            df = pro.fund_daily(
                ts_code=ts,
                start_date=start_date.replace("-", ""),
                end_date=end_date.replace("-", ""),
            )
            if df is not None and not df.empty:
                total += upsert_from_dataframe(df)
            sleep(0.3)
        return {"ok": True, "updated": total, "codes": list(BROAD_ETF_CODES)}
    except Exception as exc:  # noqa: BLE001
        logger.warning("sync_etf_daily failed: %s", exc)
        return {"ok": False, "error": str(exc)}


def backfill_all(
    etf_from: str = "2018-01-01",
    flow_from: str = "2021-01-01",
    end_date: str | None = None,
    *,
    pro_factory=None,
    sleep=time.sleep,
) -> dict:
    """Full backfill for all series (idempotent upserts)."""
    end = end_date or _iso(date.today())
    out = {
        "etf_share": sync_etf_share(etf_from, end, pro_factory=pro_factory, sleep=sleep),
        "margin_total": sync_margin_total(flow_from, end, pro_factory=pro_factory, sleep=sleep),
        "moneyflow_hsgt": sync_moneyflow_hsgt(flow_from, end, pro_factory=pro_factory, sleep=sleep),
        "global_index": sync_global_index(etf_from, end, pro_factory=pro_factory, sleep=sleep),
        "flow_daily": sync_flow_daily("2023-01-01", end),
        "etf_daily": sync_etf_daily(etf_from, end, pro_factory=pro_factory, sleep=sleep),
    }
    ok = all(r.get("ok") for r in out.values())
    return {"ok": ok, **out}


def catch_up(days: int = 10, *, pro_factory=None, sleep=time.sleep) -> dict:
    """Daily job body: re-sync the last ``days`` open dates (margin publishes T+1)."""
    end = _iso(date.today())
    start_dt = date.today() - _timedelta(days=days * 2 + 5)
    start = _iso(start_dt)
    dates = _open_dates(start, end)
    if dates:
        start = dates[0]
    out = {
        "etf_share": sync_etf_share(start, end, pro_factory=pro_factory, sleep=sleep),
        "margin_total": sync_margin_total(start, end, pro_factory=pro_factory, sleep=sleep),
        "moneyflow_hsgt": sync_moneyflow_hsgt(start, end, pro_factory=pro_factory, sleep=sleep),
        "global_index": sync_global_index(start, end, pro_factory=pro_factory, sleep=sleep),
        "margin_detail": _catch_up_margin_detail(dates, pro_factory=pro_factory, sleep=sleep),
        "moneyflow": _catch_up_moneyflow(dates, pro_factory=pro_factory, sleep=sleep),
        "flow_daily": sync_flow_daily(start, end),
        "etf_daily": sync_etf_daily(start, end, pro_factory=pro_factory, sleep=sleep),
    }
    return {"ok": all(r.get("ok") for r in out.values()), **out}


def _timedelta(days: int):
    from datetime import timedelta

    return timedelta(days=days)


def _catch_up_margin_detail(dates: list[str], *, pro_factory=None, sleep=time.sleep) -> dict:
    try:
        from data_sync_service.service.cn_extra_sync import sync_margin_detail_for_dates

        if not dates:
            return {"ok": True, "updated": 0}
        res = sync_margin_detail_for_dates(dates)
        return {"ok": True, "updated": res}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": str(exc)}


def _catch_up_moneyflow(dates: list[str], *, pro_factory=None, sleep=time.sleep) -> dict:
    try:
        from data_sync_service.service.cn_extra_sync import sync_moneyflow_for_dates

        if not dates:
            return {"ok": True, "updated": 0}
        res = sync_moneyflow_for_dates(dates)
        return {"ok": True, "updated": res}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": str(exc)}
