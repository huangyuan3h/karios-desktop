"""Macro series daily sync: global index, US equity, FX, futures via Tushare."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta
from typing import Any

import pandas as pd  # type: ignore[import-not-found, import-untyped]
import tushare as ts  # type: ignore[import-not-found]

from data_sync_service.config import get_settings
from data_sync_service.db.macro_daily import get_last_trade_date, upsert_from_dataframe
from data_sync_service.db.sync_job_record import get_today_run, insert_record

JOB_TYPE = "macro_daily_full"
FULL_START_DATE = "20230101"

# Logical series ids (stable keys in macro_daily.series_id)
SID_IXIC = "IXIC"
SID_DJI = "DJI"
SID_SPX = "SPX"
SID_USDCNH = "USDCNH.FXCM"
SID_A50 = "A50"
SID_COMM_ENERGY = "COMM_ENERGY"
SID_COMM_GOLD = "COMM_GOLD"
SID_COMM_COPPER = "COMM_COPPER"
SID_HSI = "HSI"

SERIES_ORDER: list[str] = [
    SID_IXIC,
    SID_DJI,
    SID_SPX,
    SID_USDCNH,
    SID_A50,
    SID_COMM_ENERGY,
    SID_COMM_GOLD,
    SID_COMM_COPPER,
    SID_HSI,
]


def _today_yyyymmdd() -> str:
    return datetime.now(UTC).strftime("%Y%m%d")


def _date_to_yyyymmdd(d: date) -> str:
    return d.strftime("%Y%m%d")


def _tushare_pro() -> Any:
    settings = get_settings()
    if not settings.tu_share_api_key:
        raise RuntimeError("TU_SHARE_API_KEY is not set")
    ts.set_token(settings.tu_share_api_key)
    return ts.pro_api(settings.tu_share_api_key)


def try_tushare_pro() -> Any | None:
    """Return pro API client or None if token missing (non-raising)."""
    settings = get_settings()
    if not settings.tu_share_api_key:
        return None
    try:
        ts.set_token(settings.tu_share_api_key)
        return ts.pro_api(settings.tu_share_api_key)
    except Exception:
        return None


def _normalize_us_daily_df(df: pd.DataFrame | None) -> pd.DataFrame | None:
    if df is None or df.empty:
        return df
    out = df.copy()
    if "pct_change" in out.columns and "pct_chg" not in out.columns:
        out["pct_chg"] = out["pct_change"]
    return out


def _normalize_fx_daily_df(df: pd.DataFrame | None) -> pd.DataFrame | None:
    if df is None or df.empty:
        return df
    out = df.copy()
    if "bid_close" in out.columns:
        out["close"] = out["bid_close"]
    if "bid_open" in out.columns:
        out["open"] = out["bid_open"]
    if "bid_high" in out.columns:
        out["high"] = out["bid_high"]
    if "bid_low" in out.columns:
        out["low"] = out["bid_low"]
    return out


def _paged_index_global(
    pro: Any, ts_code: str, start_date: str, end_date: str
) -> pd.DataFrame | None:
    """index_global returns max ~4000 rows; loop by year if needed."""
    chunks: list[pd.DataFrame] = []
    start = datetime.strptime(start_date, "%Y%m%d").date()
    end = datetime.strptime(end_date, "%Y%m%d").date()
    cursor = start
    while cursor <= end:
        chunk_end = min(cursor + timedelta(days=380), end)
        sd = _date_to_yyyymmdd(cursor)
        ed = _date_to_yyyymmdd(chunk_end)
        try:
            df = pro.index_global(ts_code=ts_code, start_date=sd, end_date=ed)
        except Exception:
            df = None
        if df is not None and not df.empty:
            chunks.append(df)
        cursor = chunk_end + timedelta(days=1)
    if not chunks:
        return None
    merged = pd.concat(chunks, ignore_index=True)
    return merged.drop_duplicates(subset=["trade_date"], keep="last")


def _paged_fut_daily(pro: Any, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame | None:
    chunks: list[pd.DataFrame] = []
    start = datetime.strptime(start_date, "%Y%m%d").date()
    end = datetime.strptime(end_date, "%Y%m%d").date()
    cursor = start
    while cursor <= end:
        chunk_end = min(cursor + timedelta(days=380), end)
        sd = _date_to_yyyymmdd(cursor)
        ed = _date_to_yyyymmdd(chunk_end)
        try:
            df = pro.fut_daily(ts_code=ts_code, start_date=sd, end_date=ed)
        except Exception:
            df = None
        if df is not None and not df.empty:
            chunks.append(df)
        cursor = chunk_end + timedelta(days=1)
    if not chunks:
        return None
    merged = pd.concat(chunks, ignore_index=True)
    return merged.drop_duplicates(subset=["trade_date"], keep="last")


def resolve_sgx_a50_main(pro: Any) -> str | None:
    """Best-effort SGX FTSE China A50 futures main contract."""
    try:
        df = pro.fut_basic(exchange="SGX", fut_type="1", fields="ts_code,name,list_date,delist_date")
    except Exception:
        return None
    if df is None or df.empty:
        return None
    name_col = df["name"].astype(str) if "name" in df.columns else None
    if name_col is None:
        return None
    mask = name_col.str.contains("A50", case=False, na=False) | name_col.str.contains(
        "CN", case=False, na=False
    )
    sub = df[mask]
    if sub.empty:
        sub = df
    # Prefer rows whose name mentions China / FTSE
    if "list_date" in sub.columns:
        try:
            sub = sub.sort_values("list_date", ascending=False)
        except Exception:
            pass
    ts_c = sub.iloc[0].get("ts_code")
    if ts_c is None or (isinstance(ts_c, float) and pd.isna(ts_c)):
        return None
    return str(ts_c).strip() or None


def resolve_ine_sc_main(pro: Any) -> str | None:
    """
    INE crude oil (SC) main contract. fut_basic may list SCxxxx.INE; fall back to name filter.
    """
    und = resolve_main_fut_by_prefix(pro, "INE", "SC")
    if und:
        return und
    try:
        df = pro.fut_basic(exchange="INE", fut_type="1", fields="ts_code,name,list_date")
    except Exception:
        return None
    if df is None or df.empty or "ts_code" not in df.columns:
        return None
    tc = df["ts_code"].astype(str)
    sub = df[tc.str.upper().str.startswith("SC")]
    if sub.empty and "name" in df.columns:
        sub = df[df["name"].astype(str).str.contains("原油", na=False)]
    if sub.empty:
        return None
    if "list_date" in sub.columns:
        try:
            sub = sub.sort_values("list_date", ascending=False)
        except Exception:
            pass
    ts_c = sub.iloc[0].get("ts_code")
    return str(ts_c).strip() if ts_c else None


def resolve_main_fut_by_prefix(pro: Any, exchange: str, symbol_prefix: str) -> str | None:
    """Pick latest listed main contract whose ts_code starts with symbol_prefix (e.g. CU, AU, SC)."""
    try:
        df = pro.fut_basic(exchange=exchange, fut_type="1", fields="ts_code,name,list_date")
    except Exception:
        return None
    if df is None or df.empty or "ts_code" not in df.columns:
        return None
    prefix = symbol_prefix.upper()
    sub = df[df["ts_code"].astype(str).str.upper().str.startswith(prefix)]
    if sub.empty:
        return None
    if "list_date" in sub.columns:
        try:
            sub = sub.sort_values("list_date", ascending=False)
        except Exception:
            pass
    ts_c = sub.iloc[0].get("ts_code")
    if ts_c is None:
        return None
    return str(ts_c).strip() or None


def sync_macro_daily_full() -> dict[str, Any]:
    """
    Full macro sync for configured series.
    Skip-if-today-ok and resume-from-last_ts_code (series id) on failure.
    """
    run = get_today_run(JOB_TYPE)
    if run and run.get("success"):
        return {"ok": True, "skipped": True, "message": "already synced today"}

    start_index = 0
    if run and run.get("success") is False and run.get("last_ts_code"):
        try:
            idx = SERIES_ORDER.index(str(run["last_ts_code"]))
            start_index = idx + 1
        except ValueError:
            pass

    settings = get_settings()
    if not settings.tu_share_api_key:
        return {"ok": False, "error": "TU_SHARE_API_KEY is not set"}

    pro = _tushare_pro()
    total_rows = 0
    last_successful: str | None = None

    def sync_ixic() -> int:
        last = get_last_trade_date(SID_IXIC)
        start = FULL_START_DATE if last is None else _date_to_yyyymmdd(last + timedelta(days=1))
        end = _today_yyyymmdd()
        if start > end:
            return 0
        df = _paged_index_global(pro, "IXIC", start, end)
        if df is None or df.empty:
            return 0
        return upsert_from_dataframe(df, series_id=SID_IXIC, source="index_global", underlying_ts_code="IXIC")

    def sync_dji() -> int:
        last = get_last_trade_date(SID_DJI)
        start = FULL_START_DATE if last is None else _date_to_yyyymmdd(last + timedelta(days=1))
        end = _today_yyyymmdd()
        if start > end:
            return 0
        df = _paged_index_global(pro, "DJI", start, end)
        if df is None or df.empty:
            return 0
        return upsert_from_dataframe(df, series_id=SID_DJI, source="index_global", underlying_ts_code="DJI")

    def sync_spx() -> int:
        last = get_last_trade_date(SID_SPX)
        start = FULL_START_DATE if last is None else _date_to_yyyymmdd(last + timedelta(days=1))
        end = _today_yyyymmdd()
        if start > end:
            return 0
        df = _paged_index_global(pro, "SPX", start, end)
        if df is None or df.empty:
            return 0
        return upsert_from_dataframe(df, series_id=SID_SPX, source="index_global", underlying_ts_code="SPX")

    def sync_fx_usdcnh() -> int:
        last = get_last_trade_date(SID_USDCNH)
        start = FULL_START_DATE if last is None else _date_to_yyyymmdd(last + timedelta(days=1))
        end = _today_yyyymmdd()
        if start > end:
            return 0
        try:
            df = pro.fx_daily(ts_code="USDCNH.FXCM", start_date=start, end_date=end)
        except Exception:
            df = None
        df = _normalize_fx_daily_df(df)
        if df is None or df.empty:
            return 0
        return upsert_from_dataframe(df, series_id=SID_USDCNH, source="fx_daily", underlying_ts_code="USDCNH.FXCM")

    def sync_a50() -> int:
        last = get_last_trade_date(SID_A50)
        start = FULL_START_DATE if last is None else _date_to_yyyymmdd(last + timedelta(days=1))
        end = _today_yyyymmdd()
        if start > end:
            return 0
        fut_code = resolve_sgx_a50_main(pro)
        if fut_code:
            df = _paged_fut_daily(pro, fut_code, start, end)
            if df is not None and not df.empty:
                return upsert_from_dataframe(df, series_id=SID_A50, source="fut_daily", underlying_ts_code=fut_code)
        df2 = _paged_index_global(pro, "XIN9", start, end)
        if df2 is None or df2.empty:
            return 0
        return upsert_from_dataframe(df2, series_id=SID_A50, source="index_global", underlying_ts_code="XIN9")

    def sync_comm(exchange: str, prefix: str, source_label: str) -> int:
        series_id = (
            SID_COMM_ENERGY
            if exchange == "INE"
            else SID_COMM_GOLD
            if prefix.upper() == "AU"
            else SID_COMM_COPPER
        )
        last = get_last_trade_date(series_id)
        start = FULL_START_DATE if last is None else _date_to_yyyymmdd(last + timedelta(days=1))
        end = _today_yyyymmdd()
        if start > end:
            return 0
        und = resolve_ine_sc_main(pro) if exchange == "INE" else resolve_main_fut_by_prefix(pro, exchange, prefix)
        if not und:
            return 0
        df = _paged_fut_daily(pro, und, start, end)
        if df is None or df.empty:
            return 0
        return upsert_from_dataframe(df, series_id=series_id, source=source_label, underlying_ts_code=und)

    def sync_hsi() -> int:
        last = get_last_trade_date(SID_HSI)
        start = FULL_START_DATE if last is None else _date_to_yyyymmdd(last + timedelta(days=1))
        end = _today_yyyymmdd()
        if start > end:
            return 0
        df = _paged_index_global(pro, "HSI", start, end)
        if df is None or df.empty:
            return 0
        return upsert_from_dataframe(df, series_id=SID_HSI, source="index_global", underlying_ts_code="HSI")

    sync_funcs: dict[str, Callable[[], int]] = {
        SID_IXIC: sync_ixic,
        SID_DJI: sync_dji,
        SID_SPX: sync_spx,
        SID_USDCNH: sync_fx_usdcnh,
        SID_A50: sync_a50,
        SID_COMM_ENERGY: lambda: sync_comm("INE", "SC", "fut_daily"),
        SID_COMM_GOLD: lambda: sync_comm("SHFE", "AU", "fut_daily"),
        SID_COMM_COPPER: lambda: sync_comm("SHFE", "CU", "fut_daily"),
        SID_HSI: sync_hsi,
    }

    for i in range(start_index, len(SERIES_ORDER)):
        sid = SERIES_ORDER[i]
        fn = sync_funcs.get(sid)
        if not fn:
            continue
        try:
            n = int(fn())
            total_rows += n
            last_successful = sid
        except Exception as e:  # noqa: BLE001
            insert_record(
                job_type=JOB_TYPE,
                success=False,
                last_ts_code=last_successful,
                error_message=f"{sid}: {e}",
            )
            return {"ok": False, "error": str(e), "last_ts_code": last_successful, "series": sid}

    insert_record(job_type=JOB_TYPE, success=True, last_ts_code=None, error_message=None)
    return {"ok": True, "updated": total_rows}
