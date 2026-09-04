"""Fetch CN/US 10Y yields via akshare bond_zh_us_rate -> macro_daily."""
import akshare as ak
import pandas as pd
import psycopg
from data_sync_service.config import get_settings

def main():
    print("fetch bond_zh_us_rate ...")
    df = ak.bond_zh_us_rate()
    # columns: 日期, 中国国债收益率10年, 美国国债收益率10年 etc
    print(df.columns.tolist())
    print(df.tail(3))
    # normalize
    df['date'] = pd.to_datetime(df['日期'])
    # Map to series
    # CN10Y: 中国国债收益率10年, US10Y: 美国国债收益率10年
    rename = {
        "中国国债收益率10年": "CN10Y",
        "美国国债收益率10年": "US10Y",
        "中国国债收益率2年": "CN2Y",
        "美国国债收益率2年": "US2Y",
        "美国国债收益率10年-2年": "US10Y2Y",
    }
    s=get_settings()
    conn=psycopg.connect(s.database_url)
    cur=conn.cursor()
    for src_col, series_id in rename.items():
        if src_col not in df.columns:
            continue
        sub = df[['date', src_col]].dropna()
        sub = sub.rename(columns={src_col: 'close'})
        # macro_daily expects: series_id, trade_date, source, underlying_ts_code, close etc
        # we store yield as close (percentage)
        print(f"insert {series_id} rows {len(sub)} example {sub.tail(1).to_dict(orient='records')}")
        for _, row in sub.iterrows():
            d = row['date'].date()
            close = float(row['close'])
            cur.execute("""
                INSERT INTO macro_daily (series_id, trade_date, source, underlying_ts_code, close)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (series_id, trade_date) DO UPDATE SET close=EXCLUDED.close, source=EXCLUDED.source
            """, (series_id, d, 'akshare.bond_zh_us_rate', series_id, close))
    conn.commit()
    print("done")
    # verify
    cur.execute("select series_id, count(*), min(trade_date), max(trade_date) from macro_daily where series_id in ('CN10Y','US10Y') group by series_id")
    for r in cur.fetchall():
        print(r)
    conn.close()

if __name__=="__main__":
    main()
