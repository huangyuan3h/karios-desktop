"""抛弃S-3 极简 alt 回测 · 模糊找方向 · 日线close→close 无TrendOK"""
import sys
from pathlib import Path

sys.path.insert(0, str((Path(__file__).parent.parent / "src").resolve()))

from data_sync_service.service.backtest_engine import BacktestConfig, BacktestData


def simulate_simple(data: BacktestData, signal_fn, max_pos=20, hold_days=20, pos_pct=0.1):
    """signal_fn(day, ts) -> bool 产生候选集合，按候选数等权买入，hold_days后平"""
    calendar = data.calendar
    # map day -> closes
    # simulation: 每日检查持仓到期平仓，再按信号买入新仓（现金约束 sum<=1.0）
    positions = []  # list of {ts, entry_day, entry_price, pos_pct}
    cash = 1.0
    equity_curve = [1.0]
    trades=[]
    # precompute daily closes for quick
    close_map = data.close_by_ts_day
    for idx, day in enumerate(calendar):
        # 平仓：到期或止损-5（简化固定-5）
        new_positions=[]
        for p in positions:
            ts=p["ts"]
            entry_price=p["entry_price"]
            c0=entry_price
            c_now=close_map.get(ts,{}).get(day)
            if c_now is None:
                new_positions.append(p)
                continue
            held = idx - data.calendar.index(p["entry_day"])
            pnl = (c_now/c0-1)*100
            if held>=hold_days or pnl <= -5:
                # close
                (c_now/c0 -1)*p["pos_pct"]
                cash += p["pos_pct"] * (1+ pnl/100)  # simplified
                # for equity, we track realized
                trades.append({"pnl":pnl, "held":held})
                # cash already adjusted via position release? simplified equity calc uses daily mark
            else:
                new_positions.append(p)
        positions=new_positions
        # 计算当前权益（持仓按市价 + 现金）
        pos_val=sum(close_map.get(p["ts"],{}).get(day, p["entry_price"])/p["entry_price"]*p["pos_pct"] for p in positions)
        equity = cash - sum(p["pos_pct"] for p in positions) + pos_val if positions else cash
        # 归一化：初始1.0，cash初始1.0，pos_pct sum <=1.0
        # 简化：equity_curve 用 (1 + 累计已平盈亏 + 浮动盈亏)
        # 这里近似用 pos_val + cash
        # cash 初始1.0，每次买入扣 pos_pct，卖出返还
        # 上面 cash 已处理，需同步扣
        # 为简化，equity = 1 + sum(已平pnl*pos_pct) + sum(浮动pnl*pos_pct)
        # 用 trades + positions 计算
        # 先实现买入
        # 候选
        cands=[ts for ts in data.ts_codes if signal_fn(day, ts)]
        # 过滤已有持仓
        held_ts={p["ts"] for p in positions}
        cands=[ts for ts in cands if ts not in held_ts]
        # 可买数量
        slots = max_pos - len(positions)
        if slots>0 and cands:
            # 按候选数取前 slots（随机或按信号强度，这里取前 slots）
            take=cands[:slots]
            for ts in take:
                price=close_map.get(ts,{}).get(day)
                if price is None or price<=0:
                    continue
                # 现金约束
                if sum(p["pos_pct"] for p in positions) + pos_pct > 1.0+1e-9:
                    break
                positions.append({"ts":ts,"entry_day":day,"entry_price":price,"pos_pct":pos_pct})
                cash -= pos_pct
        # 重算权益 after buy
        pos_val=sum(close_map.get(p["ts"],{}).get(day, p["entry_price"])/p["entry_price"]*p["pos_pct"] for p in positions)
        equity = cash + pos_val
        equity_curve.append(equity)
    # stats
    total = (equity_curve[-1]-1)*100
    # max DD
    peak=equity_curve[0]
    max_dd=0
    for v in equity_curve:
        if v>peak: peak=v
        dd=(v/peak-1)*100
        if dd<max_dd: max_dd=dd
    # trades stats from closed trades (approx)
    n=len(trades)
    wins=sum(1 for t in trades if t["pnl"]>0)
    win_rate=wins/n if n else None
    avg=sum(t["pnl"] for t in trades)/n if n else None
    return {"total":total,"dd":max_dd,"n":n,"win_rate":win_rate,"avg":avg}

def small_cap_signal(data: BacktestData, thresh=50):
    """mv < thresh 亿"""
    def fn(day, ts):
        mv=data.mv_by_day.get(day,{}).get(ts)
        return mv is not None and mv < thresh and mv>0
    return fn

def large_cap_signal(data: BacktestData, thresh=500):
    def fn(day, ts):
        mv=data.mv_by_day.get(day,{}).get(ts)
        return mv is not None and mv > thresh
    return fn

def mean_reversion_signal(data: BacktestData):
    """收盘 < MA20*0.92 简易超跌"""
    # 预计算 MA20
    closes=data.closes_by_ts
    ma20={}
    for ts, series in closes.items():
        # series sorted asc, build dict date->close
        d2c={d:c for d,c in series}
        for day in data.calendar:
            idx=None
            try: idx=data.calendar.index(day)
            except: continue
            if idx<20: continue
            window=data.calendar[idx-20:idx]
            vals=[d2c.get(d) for d in window]
            if any(v is None for v in vals): continue
            ma=sum(vals)/len(vals)
            ma20.setdefault(day,{})[ts]=ma
    def fn(day, ts):
        c=data.close_by_ts_day.get(ts,{}).get(day)
        ma=ma20.get(day,{}).get(ts)
        if c is None or ma is None: return False
        return c < ma*0.92
    return fn

def breakout_signal(data: BacktestData):
    """20日新高"""
    def fn(day, ts):
        c=data.close_by_ts_day.get(ts,{}).get(day)
        if c is None: return False
        try: idx=data.calendar.index(day)
        except: return False
        if idx<20: return False
        window=data.calendar[idx-20:idx]
        closes_w=[data.close_by_ts_day.get(ts,{}).get(d) for d in window]
        closes_w=[v for v in closes_w if v is not None]
        if len(closes_w)<20: return False
        return c > max(closes_w)
    return fn

if __name__=="__main__":
    import argparse
    p=argparse.ArgumentParser()
    p.add_argument("--style", choices=["small","large","mean","breakout","concentrated"], required=True)
    p.add_argument("--start", default="2025-08-01")
    p.add_argument("--end", default="2026-08-07")
    a=p.parse_args()
    cfg=BacktestConfig(start_date=a.start, end_date=a.end)
    # need mv for small/large
    if a.style in ("small","large"):
        cfg=BacktestConfig(start_date=a.start, end_date=a.end, min_avg_amount=0.0)
        # mv will be loaded lazily? Need to force mv load: set min_mv>0 to trigger? Actually mv_by_day loaded always? Check _load_market_caps: loaded always if ts_codes non-empty? It loads regardless of min_mv? In BacktestData, mv_by_day loaded unconditionally? Check: self.mv_by_day = _load_market_caps(...) always (line 619) yes always.
    data=BacktestData(cfg)
    print(f"data {a.start}~{a.end} n_days {len(data.calendar)} n_ts {len(data.ts_codes)}", flush=True)
    if a.style=="small":
        for thresh in [30,50,100]:
            fn=small_cap_signal(data, thresh)
            res=simulate_simple(data, fn, max_pos=20, hold_days=20, pos_pct=0.05)
            print(f"small mv<{thresh} 20d hold5% max20 total {res['total']:.1f}% dd {res['dd']:.1f}% n {res['n']} win {res['win_rate']} avg {res['avg']}", flush=True)
    elif a.style=="large":
        fn=large_cap_signal(data, 500)
        res=simulate_simple(data, fn, max_pos=20, hold_days=20, pos_pct=0.05)
        print(f"large mv>500 total {res['total']:.1f}% dd {res['dd']:.1f}% n {res['n']}", flush=True)
    elif a.style=="mean":
        fn=mean_reversion_signal(data)
        for hold in [5,10,20]:
            res=simulate_simple(data, fn, max_pos=20, hold_days=hold, pos_pct=0.05)
            print(f"mean MA20-8% hold{hold} total {res['total']:.1f}% dd {res['dd']:.1f}% n {res['n']} win {res['win_rate']:.2f}" if res["win_rate"] else f"mean hold{hold} total {res['total']:.1f}% n {res['n']}", flush=True)
    elif a.style=="breakout":
        fn=breakout_signal(data)
        res=simulate_simple(data, fn, max_pos=20, hold_days=20, pos_pct=0.05)
        print(f"breakout 20d high total {res['total']:.1f}% dd {res['dd']:.1f}% n {res['n']}", flush=True)
    elif a.style=="concentrated":
        # 集中 mp5 30% vs mp20 5%
        fn=breakout_signal(data)  # 用突破作信号，集中度差异
        for mp,pos in [(5,0.3),(20,0.05)]:
            res=simulate_simple(data, fn, max_pos=mp, hold_days=20, pos_pct=pos)
            print(f"concentrated mp{mp} {pos*100:.0f}% total {res['total']:.1f}% dd {res['dd']:.1f}% n {res['n']}", flush=True)
