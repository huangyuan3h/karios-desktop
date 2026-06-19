"""300ETF ATM Put implied volatility sync via AkShare."""

from __future__ import annotations

import sys
from datetime import UTC, date, datetime
from typing import Any

import pandas as pd  # type: ignore[import-not-found, import-untyped]

from data_sync_service.db.macro_daily import get_last_trade_date, get_latest_row, upsert_from_dataframe
from data_sync_service.db.sync_job_record import get_today_run, insert_record
from data_sync_service.db.trade_calendar import is_trading_day
from data_sync_service.service.macro_daily import SID_510300_PUT_IV

JOB_TYPE = "option_iv_daily"
UNDERLYING_TS_CODE = "510300.SH"
OPTION_BOARD_NAME = "华泰柏瑞沪深300ETF期权"


def _now_iso() -> str:
    return datetime.now(tz=UTC).isoformat()


def _today_yyyymmdd() -> str:
    return datetime.now(tz=UTC).strftime("%Y%m%d")


def _today_iso() -> str:
    return datetime.now(tz=UTC).strftime("%Y-%m-%d")


def _akshare() -> Any:
    if sys.platform == "darwin":
        raise RuntimeError("akshare_disabled_on_darwin")
    try:
        import akshare as ak  # type: ignore[import-not-found]
    except ImportError as e:
        raise RuntimeError(
            "akshare is not installed. cd services/data-sync-service && uv add akshare"
        ) from e
    return ak


def _parse_strike_from_name(name: str) -> float | None:
    """Parse strike from names like '300ETF沽6月4000' (strike 4.000)."""
    s = str(name or "").strip()
    digits = ""
    for ch in reversed(s):
        if ch.isdigit():
            digits = ch + digits
        elif digits:
            break
    if not digits:
        return None
    try:
        val = float(digits)
        if val >= 1000:
            return val / 1000.0
        return val
    except (TypeError, ValueError):
        return None


def _is_510300_put_row(name: str) -> bool:
    s = str(name or "")
    if "300ETF" not in s:
        return False
    return "沽" in s or "认沽" in s


def select_atm_put_iv(df: pd.DataFrame) -> dict[str, Any] | None:
    """Pick nearest-expiry ATM put IV for 510300 from option_value_analysis_em."""
    if df is None or df.empty:
        return None
    name_col = "期权名称" if "期权名称" in df.columns else None
    iv_col = "隐含波动率" if "隐含波动率" in df.columns else None
    expiry_col = "到期日" if "到期日" in df.columns else None
    spot_col = "标的最新价" if "标的最新价" in df.columns else None
    if not name_col or not iv_col:
        return None

    subset = df[df[name_col].astype(str).map(_is_510300_put_row)].copy()
    if subset.empty:
        return None

    if expiry_col and expiry_col in subset.columns:
        subset[expiry_col] = pd.to_datetime(subset[expiry_col], errors="coerce")
        subset = subset[subset[expiry_col].notna()]
        if subset.empty:
            return None
        nearest_expiry = subset[expiry_col].min()
        subset = subset[subset[expiry_col] == nearest_expiry]

    spot = None
    if spot_col and spot_col in subset.columns:
        try:
            spot = float(subset.iloc[0][spot_col])
        except (TypeError, ValueError):
            spot = None

    best: dict[str, Any] | None = None
    best_dist = float("inf")
    for _, row in subset.iterrows():
        try:
            iv = float(row[iv_col])
        except (TypeError, ValueError):
            continue
        if iv != iv or iv <= 0:
            continue
        strike = _parse_strike_from_name(str(row.get(name_col) or ""))
        dist = abs(strike - spot) if strike is not None and spot is not None else float("inf")
        if dist < best_dist:
            best_dist = dist
            best = {
                "ivPct": iv,
                "contractName": str(row.get(name_col) or ""),
                "expiry": str(row.get(expiry_col) or "") if expiry_col else None,
                "strike": strike,
                "spot": spot,
            }
    return best


def fetch_510300_atm_put_iv_live() -> dict[str, Any] | None:
    ak = _akshare()
    df = ak.option_value_analysis_em()
    picked = select_atm_put_iv(df)
    if not picked:
        return None
    return picked


def compute_iv_pct_chg(iv_pct: float, prev_close: float | None) -> float | None:
    if prev_close is None or prev_close <= 0:
        return None
    try:
        return round((float(iv_pct) - float(prev_close)) / float(prev_close) * 100.0, 3)
    except (TypeError, ValueError, ZeroDivisionError):
        return None


def _parse_cal_date(s: str) -> date:
    s2 = str(s).strip()
    if len(s2) == 8 and s2.isdigit():
        return date(int(s2[:4]), int(s2[4:6]), int(s2[6:8]))
    if len(s2) == 10 and s2[4] == "-":
        y, m, d = s2.split("-")
        return date(int(y), int(m), int(d))
    raise ValueError(f"invalid cal_date: {s}")


def classify_iv_signal(*, iv_pct: float, pct_chg: float | None) -> tuple[str, str]:
    """Return (signal, signalLabel) for macro volatility card."""
    chg = float(pct_chg) if pct_chg is not None else None
    if iv_pct >= 28.0:
        return "red", "Deep Panic"
    if 20.0 <= iv_pct < 28.0 and chg is not None and chg >= 10.0:
        return "yellow", "Elevated Fear"
    if iv_pct >= 20.0:
        return "yellow", "Elevated Fear"
    if iv_pct >= 15.0:
        return "green", "Normal"
    return "light_green", "Complacent"


def sync_option_iv_daily(*, force: bool = False, trade_date: str | None = None) -> dict[str, Any]:
    """Fetch 510300 ATM put IV and upsert into macro_daily."""
    if not force:
        existing = get_today_run(JOB_TYPE)
        if existing and existing.get("success"):
            return {"ok": True, "skipped": True, "reason": "already_synced_today", "jobType": JOB_TYPE}

    td_yyyymmdd = str(trade_date or _today_yyyymmdd()).strip()
    td_iso = f"{td_yyyymmdd[:4]}-{td_yyyymmdd[4:6]}-{td_yyyymmdd[6:8]}"
    if len(td_yyyymmdd) == 10 and td_yyyymmdd[4] == "-":
        td_iso = td_yyyymmdd
        td_yyyymmdd = td_iso.replace("-", "")

    if not is_trading_day(exchange="SSE", cal_date=_parse_cal_date(td_yyyymmdd)):
        return {"ok": True, "skipped": True, "reason": "not_trading_day", "tradeDate": td_iso}

    try:
        picked = fetch_510300_atm_put_iv_live()
    except Exception as e:
        insert_record(
            job_type=JOB_TYPE,
            success=False,
            last_ts_code=None,
            error_message=str(e)[:500],
        )
        return {"ok": False, "error": str(e), "jobType": JOB_TYPE}

    if not picked:
        insert_record(
            job_type=JOB_TYPE,
            success=False,
            last_ts_code=None,
            error_message="no_iv_data",
        )
        return {"ok": False, "error": "no_iv_data", "jobType": JOB_TYPE}

    iv_pct = float(picked["ivPct"])
    prev_row = get_latest_row(SID_510300_PUT_IV)
    prev_close = None
    if prev_row and prev_row.get("close") is not None:
        try:
            prev_close = float(prev_row["close"])
        except (TypeError, ValueError):
            prev_close = None
    pct_chg = compute_iv_pct_chg(iv_pct, prev_close)

    row_df = pd.DataFrame(
        [
            {
                "trade_date": td_yyyymmdd,
                "close": iv_pct,
                "pre_close": prev_close,
                "pct_chg": pct_chg,
            }
        ]
    )
    n = upsert_from_dataframe(
        row_df,
        series_id=SID_510300_PUT_IV,
        source="akshare",
        underlying_ts_code=UNDERLYING_TS_CODE,
    )
    signal, signal_label = classify_iv_signal(iv_pct=iv_pct, pct_chg=pct_chg)
    insert_record(
        job_type=JOB_TYPE,
        success=True,
        last_ts_code=None,
        error_message=None,
    )
    return {
        "ok": True,
        "tradeDate": td_iso,
        "ivPct": iv_pct,
        "pctChg": pct_chg,
        "signal": signal,
        "signalLabel": signal_label,
        "contractName": picked.get("contractName"),
        "rowsUpserted": n,
    }
