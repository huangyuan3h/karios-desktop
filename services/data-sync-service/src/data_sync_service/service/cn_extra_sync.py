"""CN extra data sync — fina/holder/margin/moneyflow/hk_hold/top via tushare + akshare fallback."""

from __future__ import annotations

import logging
import time
from datetime import date

import tushare as ts  # type: ignore[import-not-found]

from data_sync_service.config import get_settings
from data_sync_service.db import cn_financial, cn_hk_hold, cn_holder, cn_margin_detail, cn_moneyflow
from data_sync_service.db.trade_calendar import get_open_dates

logger = logging.getLogger(__name__)


def _pro():
    try:
        return ts.pro_api(get_settings().tu_share_api_key)
    except Exception:
        ts.set_token(get_settings().tu_share_api_key)
        return ts.pro_api()


def _with_retry(fn, tries=3, base=1.5):
    last = None
    for i in range(tries):
        try:
            return fn()
        except Exception as e:
            last = e
            if i < tries - 1:
                time.sleep(base * (2**i))
    raise last  # type: ignore[misc]


# ---- financial (fina_indicator) per ts_code (market-wide requires loop) ----
FINA_FIELDS = "ts_code,ann_date,end_date,eps,dt_eps,bps,roe,roa,gross_margin,netprofit_margin,profit_dedt,op_income,debt_to_assets,ocf_to_or,netprofit_yoy,tr_yoy,or_yoy,basic_eps_yoy,q_netprofit_yoy,q_sales_yoy,q_profit_yoy,q_roe,update_flag"


def _get_all_ts_codes() -> list[str]:
    from data_sync_service.db import get_connection

    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT ts_code FROM stock_basic WHERE market IN ('主板','创业板','科创板','中小板') OR ts_code LIKE '%.SH' OR ts_code LIKE '%.SZ' LIMIT 6000")
                rows = cur.fetchall()
                codes = [str(r[0]) for r in rows if r[0]]
                if codes:
                    return codes
    except Exception:
        pass
    # fallback: ask tushare stock_basic
    pro = _pro()
    try:
        df = pro.stock_basic(exchange="", list_status="L", fields="ts_code")
        return df["ts_code"].tolist() if df is not None and not df.empty else []
    except Exception:
        return []


def sync_financial_for_range(start_date: str, end_date: str, *, limit_codes: int | None = None) -> int:
    """Sync fina_indicator per ts_code, filter ann_date in range. One call per stock."""
    pro = _pro()
    cn_financial.ensure_table()
    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    codes = _get_all_ts_codes()
    if limit_codes:
        codes = codes[:limit_codes]
    total = 0
    for ts_code in codes:
        try:
            df = _with_retry(lambda ts_code=ts_code: pro.fina_indicator(ts_code=ts_code, fields=FINA_FIELDS))
            if df is None or df.empty:
                time.sleep(0.12)
                continue
            rows = []
            for r in df.itertuples():
                ann = str(getattr(r, "ann_date", "") or "").strip()
                if len(ann) == 8 and ann.isdigit():
                    ann_iso = f"{ann[:4]}-{ann[4:6]}-{ann[6:8]}"
                else:
                    ann_iso = ann
                try:
                    ann_d = date.fromisoformat(ann_iso) if ann_iso else None
                except ValueError:
                    continue
                if ann_d is None or ann_d < start or ann_d > end:
                    continue
                rows.append(
                    {
                        "ts_code": getattr(r, "ts_code", None),
                        "ann_date": getattr(r, "ann_date", None),
                        "end_date": getattr(r, "end_date", None),
                        "eps": getattr(r, "eps", None),
                        "dt_eps": getattr(r, "dt_eps", None),
                        "bps": getattr(r, "bps", None),
                        "roe": getattr(r, "roe", None),
                        "roa": getattr(r, "roa", None),
                        "gross_margin": getattr(r, "gross_margin", None),
                        "netprofit_margin": getattr(r, "netprofit_margin", None),
                        "profit_dedt": getattr(r, "profit_dedt", None),
                        "op_income": getattr(r, "op_income", None),
                        "debt_to_assets": getattr(r, "debt_to_assets", None),
                        "ocf_to_or": getattr(r, "ocf_to_or", None),
                        "netprofit_yoy": getattr(r, "netprofit_yoy", None),
                        "tr_yoy": getattr(r, "tr_yoy", None),
                        "or_yoy": getattr(r, "or_yoy", None),
                        "basic_eps_yoy": getattr(r, "basic_eps_yoy", None),
                        "q_netprofit_yoy": getattr(r, "q_netprofit_yoy", None),
                        "q_sales_yoy": getattr(r, "q_sales_yoy", None),
                        "q_profit_yoy": getattr(r, "q_profit_yoy", None),
                        "q_roe": getattr(r, "q_roe", None),
                        "update_flag": getattr(r, "update_flag", None),
                        "extra": {},
                    }
                )
            if rows:
                total += cn_financial.upsert_rows(rows)
            time.sleep(0.35)
        except Exception as e:
            logger.warning("fina %s failed: %s", ts_code, e)
            if "频率超限" in str(e):
                time.sleep(35)
            else:
                time.sleep(0.6)
    return total


# ---- holder per ts_code ----
def sync_holder_for_range(start_date: str, end_date: str, *, limit_codes: int | None = None) -> int:
    pro = _pro()
    cn_holder.ensure_table()
    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    codes = _get_all_ts_codes()
    if limit_codes:
        codes = codes[:limit_codes]
    total = 0
    for ts_code in codes:
        try:
            df = _with_retry(lambda ts_code=ts_code: pro.stk_holdernumber(ts_code=ts_code, fields="ts_code,ann_date,end_date,holder_num"))
            if df is None or df.empty:
                time.sleep(0.12)
                continue
            rows = []
            for r in df.itertuples():
                ann = str(getattr(r, "ann_date", "") or "").strip()
                if len(ann) == 8 and ann.isdigit():
                    ann_iso = f"{ann[:4]}-{ann[4:6]}-{ann[6:8]}"
                else:
                    ann_iso = ann
                try:
                    ann_d = date.fromisoformat(ann_iso) if ann_iso else None
                except ValueError:
                    continue
                if ann_d is None or ann_d < start or ann_d > end:
                    continue
                rows.append({"ts_code": getattr(r, "ts_code", None), "ann_date": getattr(r, "ann_date", None), "end_date": getattr(r, "end_date", None), "holder_num": getattr(r, "holder_num", None)})
            if rows:
                total += cn_holder.upsert_rows(rows)
            time.sleep(0.35)
        except Exception as e:
            logger.warning("holder %s failed: %s", ts_code, e)
            if "频率超限" in str(e):
                time.sleep(35)
            else:
                time.sleep(0.6)
    return total


# ---- margin_detail per trade_date ----
def sync_margin_detail_for_dates(trade_dates: list[str]) -> int:
    pro = _pro()
    cn_margin_detail.ensure_table()
    total = 0
    for td in trade_dates:
        day = td.replace("-", "")
        try:
            df = _with_retry(lambda day=day: pro.margin_detail(trade_date=day, fields="trade_date,ts_code,rzye,rqye,rzmre,rqyl,rzche,rqchl,rqmcl,rzrqye"))
            if df is not None and not df.empty:
                rows = [
                    {
                        "trade_date": getattr(r, "trade_date", None),
                        "ts_code": getattr(r, "ts_code", None),
                        "rzye": getattr(r, "rzye", None),
                        "rqye": getattr(r, "rqye", None),
                        "rzmre": getattr(r, "rzmre", None),
                        "rqyl": getattr(r, "rqyl", None),
                        "rzche": getattr(r, "rzche", None),
                        "rqchl": getattr(r, "rqchl", None),
                        "rqmcl": getattr(r, "rqmcl", None),
                        "rzrqye": getattr(r, "rzrqye", None),
                    }
                    for r in df.itertuples()
                ]
                total += cn_margin_detail.upsert_rows(rows)
            time.sleep(0.35)
        except Exception as e:
            logger.warning("margin_detail %s failed: %s", td, e)
            time.sleep(1.0)
    return total


# ---- moneyflow per trade_date ----
def sync_moneyflow_for_dates(trade_dates: list[str]) -> int:
    pro = _pro()
    cn_moneyflow.ensure_table()
    total = 0
    for td in trade_dates:
        day = td.replace("-", "")
        try:
            df = _with_retry(lambda day=day: pro.moneyflow(trade_date=day))
            if df is not None and not df.empty:
                rows = []
                for r in df.itertuples():
                    rows.append(
                        {
                            "trade_date": getattr(r, "trade_date", None),
                            "ts_code": getattr(r, "ts_code", None),
                            "buy_sm_amount": getattr(r, "buy_sm_amount", None),
                            "sell_sm_amount": getattr(r, "sell_sm_amount", None),
                            "buy_md_amount": getattr(r, "buy_md_amount", None),
                            "sell_md_amount": getattr(r, "sell_md_amount", None),
                            "buy_lg_amount": getattr(r, "buy_lg_amount", None),
                            "sell_lg_amount": getattr(r, "sell_lg_amount", None),
                            "buy_elg_amount": getattr(r, "buy_elg_amount", None),
                            "sell_elg_amount": getattr(r, "sell_elg_amount", None),
                            "net_mf_amount": getattr(r, "net_mf_amount", None),
                            "net_mf_vol": getattr(r, "net_mf_vol", None),
                        }
                    )
                total += cn_moneyflow.upsert_rows(rows)
            time.sleep(0.35)
        except Exception as e:
            logger.warning("moneyflow %s failed: %s", td, e)
            time.sleep(1.0)
    return total


# ---- hk_hold per trade_date ----
def sync_hk_hold_for_dates(trade_dates: list[str]) -> int:
    pro = _pro()
    cn_hk_hold.ensure_table()
    total = 0
    for td in trade_dates:
        day = td.replace("-", "")
        try:
            df = _with_retry(lambda day=day: pro.hk_hold(trade_date=day))
            if df is not None and not df.empty:
                rows = [
                    {"trade_date": getattr(r, "trade_date", None), "ts_code": getattr(r, "ts_code", None), "vol": getattr(r, "vol", None), "ratio": getattr(r, "ratio", None)}
                    for r in df.itertuples()
                ]
                total += cn_hk_hold.upsert_rows(rows)
            time.sleep(0.35)
        except Exception as e:
            logger.warning("hk_hold %s failed: %s", td, e)
            time.sleep(1.0)
    return total


# ---- top_list/top_inst ----
def sync_top_for_dates(trade_dates: list[str]) -> dict:
    from data_sync_service.service.top_inst_flow import sync_for_trade_dates as sync_top

    # reuse existing service which already does top_list+top_inst per date
    total = 0
    for td in trade_dates:
        try:
            res = sync_top([td])
            total += int(res.get("updated", 0) or 0)
            time.sleep(0.35)
        except Exception as e:
            logger.warning("top %s failed: %s", td, e)
    return {"updated": total}


def sync_all_for_range(start_date: str, end_date: str, *, daily_only: bool = False) -> dict:
    """Sync extra data for trade dates in range. If daily_only, skip quarterly (financial/holder)."""
    trade_dates = [d.isoformat() for d in get_open_dates("SSE", date.fromisoformat(start_date), date.fromisoformat(end_date))]
    out: dict = {}
    if not daily_only:
        out["financial"] = sync_financial_for_range(start_date, end_date)
        out["holder"] = sync_holder_for_range(start_date, end_date)
    out["margin"] = sync_margin_detail_for_dates(trade_dates)
    out["moneyflow"] = sync_moneyflow_for_dates(trade_dates)
    out["hk_hold"] = sync_hk_hold_for_dates(trade_dates)
    out["top"] = sync_top_for_dates(trade_dates)
    return out
