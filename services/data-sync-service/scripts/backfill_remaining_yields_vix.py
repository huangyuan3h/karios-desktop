"""Backfill remaining yields (CN5Y/CN30Y/US5Y/US30Y) + VIX via yfinance -> macro_daily."""
import akshare as ak, pandas as pd, psycopg, yfinance as yf
from data_sync_service.config import get_settings

def upsert_series(series_id, df, date_col, close_col, source):
    s=get_settings()
    conn=psycopg.connect(s.database_url)
    cur=conn.cursor()
    cnt=0
    for _, row in df.iterrows():
        d=pd.to_datetime(row[date_col]).date()
        try: close=float(row[close_col])
        except: continue
        if pd.isna(close): continue
        cur.execute("""
            INSERT INTO macro_daily (series_id, trade_date, source, underlying_ts_code, close)
            VALUES (%s,%s,%s,%s,%s)
            ON CONFLICT (series_id, trade_date) DO UPDATE SET close=EXCLUDED.close, source=EXCLUDED.source
        """, (series_id, d, source, series_id, close))
        cnt+=1
    conn.commit()
    cur.execute("select count(*), min(trade_date), max(trade_date) from macro_daily where series_id=%s", (series_id,))
    print(series_id, cur.fetchone(), f"inserted {cnt}")
    conn.close()

# 1. Remaining yields via akshare
print("fetch bond_zh_us_rate ...")
df=ak.bond_zh_us_rate()
print(df.columns.tolist())
mapping={
    "中国国债收益率5年":"CN5Y",
    "中国国债收益率30年":"CN30Y",
    "美国国债收益率5年":"US5Y",
    "美国国债收益率30年":"US30Y",
}
for col, sid in mapping.items():
    sub=df[["日期",col]].dropna()
    upsert_series(sid, sub, "日期", col, "akshare.bond_zh_us_rate")

# 2. VIX via yfinance
print("fetch VIX via yfinance ...")
vix=yf.download("^VIX", period="10y", progress=False, auto_adjust=False)
# yfinance returns multi-index columns if auto_adjust False? Handle
if isinstance(vix.columns, pd.MultiIndex):
    vix.columns=vix.columns.get_level_values(0)
vix=vix.reset_index()
# Date column is "Date"
vix["Date"]=pd.to_datetime(vix["Date"])
# Use Close
vix_sub=vix[["Date","Close"]].dropna()
# rename to match
vix_sub=vix_sub.rename(columns={"Close":"close"})
print(vix_sub.tail())
# Insert as VIX
# yfinance may have duplicate dates, keep last
vix_sub=vix_sub.drop_duplicates(subset=["Date"])
# Need to convert to format expected: date_col "Date", close_col "close"
upsert_series("VIX", vix_sub, "Date", "close", "yfinance.VIX")

print("done")
