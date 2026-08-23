"""Backfill fund_daily for commodity/bond target ETFs with rate limit."""
import time
from datetime import date, timedelta, datetime, UTC
import tushare as ts
import pandas as pd
from data_sync_service.config import get_settings
from data_sync_service.db.daily import get_last_trade_date, upsert_from_dataframe
from data_sync_service.db.trade_calendar import last_trading_day_str

TARGETS = [
    "518880.SH", "518800.SH", "159934.SZ", "159937.SZ",
    "511010.SH", "511260.SH", "511130.SH", "511380.SH", "511360.SH",
    "561570.SH", "513350.SH", "159518.SZ", "159321.SZ", "159322.SZ",
    "159026.SZ", # short history, but refill will extend if possible
]

FULL_START = "20230101"
SLEEP = 0.5  # 120/min < 200 limit

def sync_one(pro, ts_code, end_date):
    last = get_last_trade_date(ts_code)
    if last is None:
        start = FULL_START
    else:
        start = (last + timedelta(days=1)).strftime("%Y%m%d")
    if start > end_date:
        print(f"{ts_code} up-to-date {start}>{end_date}")
        return 0
    print(f"{ts_code} {start}->{end_date} ...", flush=True)
    df = pro.fund_daily(ts_code=ts_code, start_date=start, end_date=end_date, fields="ts_code,trade_date,open,high,low,close,pre_close,change,pct_chg,vol,amount")
    if df is None or df.empty:
        print(f"  empty")
        return 0
    n = upsert_from_dataframe(df)
    print(f"  upsert {n} rows {df['trade_date'].min()}->{df['trade_date'].max()}")
    return n

def main():
    s=get_settings()
    pro=ts.pro_api(s.tu_share_api_key)
    # end date per exchange
    end_sse = last_trading_day_str("SSE", datetime.now(UTC).date())
    end_szse = last_trading_day_str("SZSE", datetime.now(UTC).date())
    total=0
    for ts_code in TARGETS:
        end = end_sse if ts_code.endswith(".SH") else end_szse
        try:
            total+=sync_one(pro, ts_code, end)
        except Exception as e:
            print(f"ERROR {ts_code}: {e}")
            # if rate limit, sleep longer
            if "频率超限" in str(e) or "rate" in str(e).lower():
                print(" rate limit hit, sleep 60s")
                time.sleep(60)
            else:
                time.sleep(2)
        time.sleep(SLEEP)
    print(f"DONE total {total}")

if __name__=="__main__":
    main()
