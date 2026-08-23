"""G1 commodity pattern scan: MA/Donchian/RSI for gold/oil/bond."""
import psycopg, pandas as pd, numpy as np
from data_sync_service.config import get_settings

def fetch_daily(ts_code):
    s=get_settings()
    conn=psycopg.connect(s.database_url)
    cur=conn.cursor()
    cur.execute("select trade_date, open, high, low, close, vol from daily where ts_code=%s order by trade_date", (ts_code,))
    df=pd.DataFrame(cur.fetchall(), columns=["date","open","high","low","close","vol"])
    conn.close()
    if df.empty: return df
    df["date"]=pd.to_datetime(df["date"])
    df=df.set_index("date").sort_index()
    df["close"]=df["close"].astype(float)
    df["high"]=df["high"].astype(float)
    df["low"]=df["low"].astype(float)
    return df

def fetch_yield(series):
    s=get_settings()
    conn=psycopg.connect(s.database_url)
    cur=conn.cursor()
    cur.execute("select trade_date, close from macro_daily where series_id=%s order by trade_date", (series,))
    df=pd.DataFrame(cur.fetchall(), columns=["date","close"])
    conn.close()
    df["date"]=pd.to_datetime(df["date"])
    df=df.set_index("date").sort_index()
    df["close"]=df["close"].astype(float)
    return df

def backtest(df, entry_cond, exit_days=10, cost=0.0005):
    """Simple: enter at close when cond true, exit after exit_days, compute pnl."""
    if df.empty or len(df)<60: return None
    df=df.copy()
    # forward return
    df["ret_fwd"] = df["close"].shift(-exit_days)/df["close"] -1 - cost*2  # round trip
    # win
    sig = entry_cond(df)
    trades = df[sig & df["ret_fwd"].notna()]
    if len(trades)<30:
        return {"n": len(trades), "mean": np.nan, "win": np.nan, "sharpe": np.nan, "note":"underpowered"}
    mean = trades["ret_fwd"].mean()*100
    win = (trades["ret_fwd"]>0).mean()*100
    # sharpe approx: mean / std * sqrt(252/exit_days)
    sharpe = (trades["ret_fwd"].mean()/trades["ret_fwd"].std()* np.sqrt(252/exit_days)) if trades["ret_fwd"].std()!=0 else 0
    # vs base
    base = df["ret_fwd"].dropna()
    base_mean = base.mean()*100
    excess = mean - base_mean
    return {"n": len(trades), "mean": mean, "win": win, "sharpe": sharpe, "base": base_mean, "excess": excess}

def cond_ma(df, n): return df["close"] > df["close"].rolling(n).mean()
def cond_ma_below(df, n): return df["close"] < df["close"].rolling(n).mean()
def cond_donchian_break(df, n): return df["close"] == df["high"].rolling(n).max()
def cond_rsi(df, period=14, thresh=30): 
    delta=df["close"].diff()
    gain=delta.where(delta>0,0).rolling(period).mean()
    loss=(-delta.where(delta<0,0)).rolling(period).mean()
    rs=gain/loss.replace(0, np.nan)
    rsi=100 - 100/(1+rs)
    df["_rsi"]=rsi
    return rsi < thresh

assets = {
    "518880.SH": "Gold",
    "511010.SH": "Bond5Y",
    "511260.SH": "Bond10Y",
    "513350.SH": "OilQDII_513350",
    "159518.SZ": "OilQDII_159518",
    "561570.SH": "OilEquity_561570",
    "513100.SH": "Nasdaq",
}

for ts, name in assets.items():
    df=fetch_daily(ts)
    print(f"\n=== {name} {ts} {len(df)} bars {df.index.min().date()}~{df.index.max().date()} ===")
    if len(df)<100:
        print(" short, skip detailed")
        continue
    for n in [10,20,60,200]:
        res=backtest(df, lambda d, n=n: cond_ma(d,n), exit_days=10)
        print(f" MA{n} above 10d: n={res['n']} mean {res['mean']:.2f}% base {res['base']:.2f}% excess {res['excess']:.2f}% win {res['win']:.1f}% sharpe {res['sharpe']:.2f} {res.get('note','')}")
    for n in [20,55]:
        res=backtest(df, lambda d, n=n: cond_donchian_break(d,n), exit_days=10)
        if res is None: continue
        if np.isnan(res['mean']): print(f" Donchian{n} breakout 10d: underpowered n={res['n']}")
        else: print(f" Donchian{n} breakout 10d: n={res['n']} mean {res['mean']:.2f}% win {res['win']:.1f}% excess {res['excess']:.2f}%")
    # RSI oversold bounce 5d
    def rsi_cond(d): return cond_rsi(d,14,30)
    res=backtest(df, rsi_cond, exit_days=10)
    print(f" RSI<30 10d: n={res['n']} mean {res['mean']:.2f}% win {res['win']:.1f}% excess {res['excess']:.2f}%")
    # MA10 below mean reversion 10d
    res=backtest(df, lambda d: cond_ma_below(d,10), exit_days=10)
    print(f" MA10 below 10d (mean reversion): n={res['n']} mean {res['mean']:.2f}% win {res['win']:.1f}% excess {res['excess']:.2f}%")
    # MA60 slope? simple: MA20>MA60
    def slope(d):
        ma20=d["close"].rolling(20).mean()
        ma60=d["close"].rolling(60).mean()
        return ma20 > ma60
    res=backtest(df, slope, exit_days=20)
    print(f" MA20>MA60 20d: n={res['n']} mean {res['mean']:.2f}% win {res['win']:.1f}% excess {res['excess']:.2f}%")

# yield vs bond
print("\n=== Yields vs Bond ETF ===")
us10y=fetch_yield("US10Y")
cn10y=fetch_yield("CN10Y")
bond=fetch_daily("511260.SH")
# correlate yield change vs bond return next 10d
# align
if not us10y.empty and not bond.empty:
    # yield down should mean bond up
    us10y_chg = us10y["close"].diff(5)  # 5d change
    # join
    df=bond.join(us10y_chg.rename("us10y_5d_chg"), how="inner")
    df["bond_ret10"] = df["close"].shift(-10)/df["close"]-1
    # when US10Y down 5d (rates falling) -> bond long?
    cond = df["us10y_5d_chg"] < -0.05  # down 5bp
    sub=df[cond & df["bond_ret10"].notna()]
    base=df["bond_ret10"].dropna()
    print(f"US10Y down 5d >5bp -> bond10Y next10d n={len(sub)} mean {sub['bond_ret10'].mean()*100:.3f}% base {base.mean()*100:.3f}% win {(sub['bond_ret10']>0).mean()*100:.1f}%")
    cond2 = df["us10y_5d_chg"] > 0.05
    sub2=df[cond2 & df["bond_ret10"].notna()]
    print(f"US10Y up 5d >5bp -> bond10Y next10d n={len(sub2)} mean {sub2['bond_ret10'].mean()*100:.3f}%")

print("\nDONE")
