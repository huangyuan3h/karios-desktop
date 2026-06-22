"""300ETF ATM Put implied volatility sync via East Money HTTP (AkShare fallback)."""

from __future__ import annotations

import math
import sys
import time
from datetime import UTC, date, datetime
from typing import Any

import pandas as pd  # type: ignore[import-not-found, import-untyped]

from data_sync_service.db.macro_daily import get_latest_row, upsert_from_dataframe
from data_sync_service.db.sync_job_record import get_today_run, insert_record
from data_sync_service.db.trade_calendar import is_trading_day
from data_sync_service.service.em_push2_http import em_get_json
from data_sync_service.service.macro_daily import SID_510300_PUT_IV

JOB_TYPE = "option_iv_daily"
UNDERLYING_TS_CODE = "510300.SH"
EM_OPTION_VALUE_URL = "https://push2.eastmoney.com/api/qt/clist/get"
EM_OPTION_PAGE_SIZE = 100
EM_OPTION_MAX_PAGES = 20
PUT_IV_SNAPSHOT_CACHE_TTL_SECONDS = 120.0
_PUT_IV_SNAPSHOT_CACHE: dict[str, Any] = {"ts": 0.0, "value": None}
_LAST_PUT_IV_DIAGNOSTICS: dict[str, Any] = {}


def _now_iso() -> str:
    return datetime.now(tz=UTC).isoformat()


def _today_yyyymmdd() -> str:
    return datetime.now(tz=UTC).strftime("%Y%m%d")


def _akshare() -> Any:
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


def _em_option_value_request(params: dict[str, str]) -> dict[str, Any]:
    return em_get_json(
        EM_OPTION_VALUE_URL,
        params=params,
        referer="https://data.eastmoney.com/other/valueAnal.html",
    )


def _fetch_em_option_value_rows() -> list[dict[str, Any]]:
    """Paginated fetch matching akshare option_value_analysis_em field layout."""
    fields = (
        "f1,f2,f3,f12,f13,f14,f298,f299,f249,f300,f330,f331,f332,f333,f334,f335,f336,f301,f152"
    )
    rows: list[dict[str, Any]] = []
    page_number = 1
    total_pages = 1
    while page_number <= total_pages and page_number <= EM_OPTION_MAX_PAGES:
        params = {
            "fid": "f301",
            "po": "1",
            "pz": str(EM_OPTION_PAGE_SIZE),
            "pn": str(page_number),
            "np": "1",
            "fltt": "2",
            "invt": "2",
            "ut": "b2884a393a59ad64002292a3e90d46a5",
            "fields": fields,
            "fs": "m:10",
        }
        j = _em_option_value_request(params)
        data = j.get("data") if isinstance(j, dict) else None
        diff = data.get("diff") if isinstance(data, dict) else None
        if not isinstance(diff, list) or not diff:
            break
        for row in diff:
            if isinstance(row, dict):
                rows.append(row)
        try:
            total = int((data or {}).get("total") or 0)
            total_pages = max(1, math.ceil(total / EM_OPTION_PAGE_SIZE))
        except (TypeError, ValueError):
            total_pages = page_number
        page_number += 1
    return rows


def _em_rows_to_analysis_df(rows: list[dict[str, Any]]) -> pd.DataFrame:
    out_rows: list[dict[str, Any]] = []
    for row in rows:
        name = str(row.get("f14") or "")
        if not name:
            continue
        try:
            iv = float(row.get("f249"))
        except (TypeError, ValueError):
            iv = float("nan")
        expiry_raw = row.get("f301")
        expiry: str | None = None
        if expiry_raw is not None:
            s = str(expiry_raw).strip()
            if len(s) == 8 and s.isdigit():
                expiry = f"{s[:4]}-{s[4:6]}-{s[6:8]}"
            else:
                expiry = s or None
        spot = None
        for key in ("f334", "f335"):
            try:
                spot = float(row.get(key))
                if spot == spot and spot > 0:
                    break
            except (TypeError, ValueError):
                spot = None
        out_rows.append(
            {
                "期权名称": name,
                "隐含波动率": iv,
                "到期日": expiry,
                "标的最新价": spot,
            }
        )
    return pd.DataFrame(out_rows)


def select_atm_put_iv(df: pd.DataFrame) -> dict[str, Any] | None:
    """Pick nearest-expiry ATM put IV for 510300 from option value analysis rows."""
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
    fallback_candidates: list[dict[str, Any]] = []
    for _, row in subset.iterrows():
        try:
            iv = float(row[iv_col])
        except (TypeError, ValueError):
            continue
        if iv != iv or iv <= 0:
            continue
        strike = _parse_strike_from_name(str(row.get(name_col) or ""))
        candidate = {
            "ivPct": iv,
            "contractName": str(row.get(name_col) or ""),
            "expiry": str(row.get(expiry_col) or "") if expiry_col else None,
            "strike": strike,
            "spot": spot,
        }
        fallback_candidates.append(candidate)
        dist = abs(strike - spot) if strike is not None and spot is not None else float("inf")
        if dist < best_dist:
            best_dist = dist
            best = candidate

    if best is None and fallback_candidates:
        with_strike = [c for c in fallback_candidates if c.get("strike") is not None]
        pool = with_strike if with_strike else fallback_candidates
        pool.sort(key=lambda c: float(c.get("strike") or 0.0))
        best = pool[len(pool) // 2]
    return best


def _count_510300_put_rows(df: pd.DataFrame) -> int:
    if df is None or df.empty or "期权名称" not in df.columns:
        return 0
    try:
        return int(df["期权名称"].astype(str).map(_is_510300_put_row).sum())
    except Exception:
        return 0


def _compact_diagnostics(diag: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in diag.items() if v is not None}


def fetch_510300_atm_put_iv_live(*, source: str | None = None) -> dict[str, Any] | None:
    """Fetch ATM put IV; prefer East Money HTTP, AkShare as fallback."""
    global _LAST_PUT_IV_DIAGNOSTICS
    picked: dict[str, Any] | None = None
    used_source = "eastmoney"
    diagnostics: dict[str, Any] = {
        "eastmoneyRows": 0,
        "eastmoneyPutRows": 0,
        "eastmoneySelected": False,
        "akshareAttempted": False,
        "akshareRows": None,
        "aksharePutRows": None,
        "akshareSelected": False,
        "akshareSkippedReason": None,
        "error": None,
    }
    try:
        em_rows = _fetch_em_option_value_rows()
        diagnostics["eastmoneyRows"] = len(em_rows)
        em_df = _em_rows_to_analysis_df(em_rows)
        diagnostics["eastmoneyPutRows"] = _count_510300_put_rows(em_df)
        picked = select_atm_put_iv(em_df)
        diagnostics["eastmoneySelected"] = picked is not None
    except Exception as e:
        diagnostics["eastmoneyError"] = str(e)[:200]
        picked = None

    if picked is None:
        if sys.platform == "darwin":
            diagnostics["akshareSkippedReason"] = "akshare_disabled_on_darwin"
        else:
            diagnostics["akshareAttempted"] = True
            try:
                ak = _akshare()
                df = ak.option_value_analysis_em()
                diagnostics["akshareRows"] = int(len(df)) if hasattr(df, "__len__") else None
                diagnostics["aksharePutRows"] = _count_510300_put_rows(df)
                picked = select_atm_put_iv(df)
                diagnostics["akshareSelected"] = picked is not None
                used_source = "akshare"
            except Exception as e:
                diagnostics["akshareError"] = str(e)[:200]
                picked = None

    if picked is None:
        diagnostics["error"] = "no_510300_put_iv_candidate"
        _LAST_PUT_IV_DIAGNOSTICS = _compact_diagnostics(diagnostics)
        return None

    data_source = source if source is not None else used_source
    _LAST_PUT_IV_DIAGNOSTICS = _compact_diagnostics(diagnostics)
    return {**picked, "source": data_source, "diagnostics": _LAST_PUT_IV_DIAGNOSTICS}


def _shanghai_today_iso() -> str:
    from zoneinfo import ZoneInfo

    return datetime.now(tz=ZoneInfo("Asia/Shanghai")).date().isoformat()


def resolve_put_iv_for_snapshot(*, write_db: bool = True, use_cache: bool = True) -> dict[str, Any]:
    """
    Live-first Put IV for macro snapshot; optional best-effort DB upsert.
    Falls back to latest macro_daily row when live fetch fails.
    """
    now = time.monotonic()
    cached = _PUT_IV_SNAPSHOT_CACHE.get("value")
    cached_ts = float(_PUT_IV_SNAPSHOT_CACHE.get("ts") or 0.0)
    if use_cache and isinstance(cached, dict) and now - cached_ts < PUT_IV_SNAPSHOT_CACHE_TTL_SECONDS:
        return {**cached, "cached": True}

    prev_row = get_latest_row(SID_510300_PUT_IV)
    prev_close = None
    if prev_row and prev_row.get("close") is not None:
        try:
            prev_close = float(prev_row["close"])
        except (TypeError, ValueError):
            prev_close = None

    picked: dict[str, Any] | None = None
    fetch_error: str | None = None
    diagnostics: dict[str, Any] | None = None
    try:
        picked = fetch_510300_atm_put_iv_live()
        if picked and isinstance(picked.get("diagnostics"), dict):
            diagnostics = picked.get("diagnostics")
        elif _LAST_PUT_IV_DIAGNOSTICS:
            diagnostics = dict(_LAST_PUT_IV_DIAGNOSTICS)
    except Exception as e:
        fetch_error = str(e)[:200]
        if _LAST_PUT_IV_DIAGNOSTICS:
            diagnostics = dict(_LAST_PUT_IV_DIAGNOSTICS)

    if picked:
        iv_pct = float(picked["ivPct"])
        data_source = str(picked.get("source") or "eastmoney")
        pct_chg = compute_iv_pct_chg(iv_pct, prev_close)
        signal, signal_label = classify_iv_signal(iv_pct=iv_pct, pct_chg=pct_chg)
        as_of = _shanghai_today_iso()
        if write_db:
            try:
                td_yyyymmdd = as_of.replace("-", "")
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
                upsert_from_dataframe(
                    row_df,
                    series_id=SID_510300_PUT_IV,
                    source=data_source,
                    underlying_ts_code=UNDERLYING_TS_CODE,
                )
            except Exception:
                pass
        out = {
            "close": iv_pct,
            "asOfDate": as_of,
            "pctChg": pct_chg,
            "source": data_source,
            "signal": signal,
            "signalLabel": signal_label,
            "underlyingTsCode": UNDERLYING_TS_CODE,
            "realtime": True,
            "warning": None,
            "diagnostics": diagnostics or {},
            "cached": False,
        }
        _PUT_IV_SNAPSHOT_CACHE.update({"ts": now, "value": out})
        return out

    warning = fetch_error or "put_iv_fetch_failed"
    if prev_row and prev_row.get("close") is not None:
        try:
            iv_pct = float(prev_row["close"])
        except (TypeError, ValueError):
            iv_pct = None
        if iv_pct is not None and iv_pct == iv_pct:
            pct_chg = _safe_float(prev_row.get("pct_chg"))
            if pct_chg is None:
                pct_chg = compute_iv_pct_chg(iv_pct, prev_close)
            signal, signal_label = classify_iv_signal(
                iv_pct=iv_pct,
                pct_chg=float(pct_chg) if pct_chg is not None else None,
            )
            out = {
                "close": iv_pct,
                "asOfDate": str(prev_row.get("trade_date") or ""),
                "pctChg": pct_chg,
                "source": str(prev_row.get("source") or "macro_daily"),
                "signal": signal,
                "signalLabel": signal_label,
                "underlyingTsCode": UNDERLYING_TS_CODE,
                "realtime": False,
                "warning": "put_iv_live_fetch_failed_using_db" if warning == "put_iv_fetch_failed" else warning,
                "diagnostics": diagnostics or {},
                "cached": False,
            }
            _PUT_IV_SNAPSHOT_CACHE.update({"ts": now, "value": out})
            return out

    out = {
        "close": None,
        "asOfDate": None,
        "pctChg": None,
        "source": None,
        "signal": None,
        "signalLabel": None,
        "underlyingTsCode": UNDERLYING_TS_CODE,
        "realtime": False,
        "warning": warning,
        "diagnostics": diagnostics or {},
        "cached": False,
    }
    _PUT_IV_SNAPSHOT_CACHE.update({"ts": now, "value": out})
    return out


def _safe_float(v: Any) -> float | None:
    try:
        f = float(v)
        return f if f == f else None
    except (TypeError, ValueError):
        return None


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
        diag = dict(_LAST_PUT_IV_DIAGNOSTICS)
        insert_record(
            job_type=JOB_TYPE,
            success=False,
            last_ts_code=None,
            error_message="no_iv_data",
        )
        return {"ok": False, "error": "no_iv_data", "jobType": JOB_TYPE, "diagnostics": diag}

    iv_pct = float(picked["ivPct"])
    data_source = str(picked.get("source") or "eastmoney")
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
        source=data_source,
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
        "source": data_source,
        "rowsUpserted": n,
        "diagnostics": picked.get("diagnostics") if isinstance(picked.get("diagnostics"), dict) else {},
    }
