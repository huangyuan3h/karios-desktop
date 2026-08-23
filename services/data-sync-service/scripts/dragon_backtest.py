"""D1 龙头一号 · 抛S-3重拼 6条 主线Top1+RS8+新高+量1.5+50-300亿+5票20%"""
import sys
from pathlib import Path

sys.path.insert(0, str((Path(__file__).parent.parent / "src").resolve()))

from data_sync_service.service.backtest_engine import BacktestConfig, BacktestData


def dragon_signal(data: BacktestData, day: str, ts: str) -> bool:
    # 1. 主线 Top1
    flow = data.flow5d_by_day.get(day, {})
    if not flow:
        return False
    # Top1 industry by flow amount
    top1 = max(flow.items(), key=lambda x: x[1])[0] if flow else None
    ind = data.industry_by_ts.get(ts)
    if ind != top1:
        return False
    # 2. RS>0.8
    rs = data.rs_rank_by_day.get(day, {}).get(ts)
    if rs is None or rs < 0.8:
        return False
    # 3. 20日新高 ±2%
    c = data.close_by_ts_day.get(ts, {}).get(day)
    if c is None: return False
    try: idx = data.calendar.index(day)
    except: return False
    if idx < 20: return False
    window = data.calendar[idx-20:idx]
    closes = [data.close_by_ts_day.get(ts, {}).get(d) for d in window]
    closes = [v for v in closes if v is not None]
    if len(closes)<20: return False
    mx = max(closes)
    if c < mx*0.98: return False
    # 4. 量 1.5× (vol)
    # need vol: from bars_by_ts
    bars = data.bars_by_ts.get(ts, [])
    # bars are list of tuples (date, open, high, low, close, vol) as strings
    # Build date->vol map
    vol_map = {}
    for b in bars:
        d=b[0]; v=b[5]
        try: vol_map[d]=float(v)
        except: continue
    today_vol = vol_map.get(day)
    if today_vol is None: return False
    avg_vol_vals=[]
    for d in window:
        v=vol_map.get(d)
        if v is not None: avg_vol_vals.append(v)
    if len(avg_vol_vals)<20: return False
    avg_vol=sum(avg_vol_vals)/len(avg_vol_vals)
    if avg_vol<=0 or today_vol < avg_vol*1.5:
        return False
    # 5. 市值 50-300亿
    mv = data.mv_by_day.get(day, {}).get(ts)
    if mv is None or not (50 <= mv <= 300):
        return False
    return True

def simulate_d1(data: BacktestData, hold_days=30, pos_pct=0.2, max_pos=5, stop=-8):
    calendar=data.calendar
    close_map=data.close_by_ts_day
    positions=[]
    cash=1.0
    trades=[]
    equity_curve=[1.0]
    for idx, day in enumerate(calendar):
        # 平仓：到期或止损-8
        new_positions=[]
        for p in positions:
            ts=p["ts"]; entry_price=p["entry_price"]
            c_now=close_map.get(ts,{}).get(day)
            if c_now is None:
                new_positions.append(p); continue
            held=idx - data.calendar.index(p["entry_day"])
            pnl=(c_now/entry_price-1)*100
            if held>=hold_days or pnl <= stop:
                # close
                (c_now/entry_price-1)*p["pos_pct"]
                cash += p["pos_pct"]*(1+pnl/100)
                trades.append({"pnl":pnl,"held":held})
            else:
                new_positions.append(p)
        positions=new_positions
        # 买入
        cands=[ts for ts in data.ts_codes if dragon_signal(data, day, ts)]
        held_ts={p["ts"] for p in positions}
        cands=[ts for ts in cands if ts not in held_ts]
        slots=max_pos - len(positions)
        if slots>0 and cands:
            # 按RS排序取前slots
            cands_sorted=sorted(cands, key=lambda ts: data.rs_rank_by_day.get(day,{}).get(ts,0), reverse=True)
            take=cands_sorted[:slots]
            for ts in take:
                price=close_map.get(ts,{}).get(day)
                if price is None or price<=0: continue
                if sum(p["pos_pct"] for p in positions)+pos_pct>1.0+1e-9: break
                positions.append({"ts":ts,"entry_day":day,"entry_price":price,"pos_pct":pos_pct})
                cash-=pos_pct
        pos_val=sum(close_map.get(p["ts"],{}).get(day, p["entry_price"])/p["entry_price"]*p["pos_pct"] for p in positions)
        equity=cash+pos_val
        equity_curve.append(equity)
    total=(equity_curve[-1]-1)*100
    peak=equity_curve[0]; max_dd=0
    for v in equity_curve:
        if v>peak: peak=v
        dd=(v/peak-1)*100
        if dd<max_dd: max_dd=dd
    n=len(trades)
    win=sum(1 for t in trades if t["pnl"]>0)/n if n else None
    avg=sum(t["pnl"] for t in trades)/n if n else None
    return {"total":total,"dd":max_dd,"n":n,"win":win,"avg":avg}

if __name__=="__main__":
    import argparse
    p=argparse.ArgumentParser()
    p.add_argument("--start", default="2025-08-01")
    p.add_argument("--end", default="2026-08-07")
    p.add_argument("--windows", nargs="*", default=None)
    a=p.parse_args()
    if a.windows:
        # walk-forward style: run each window
        windows={"OOS2":("2024-08-01","2025-08-01"),"train":("2025-08-01","2026-02-01"),"valid":("2026-03-01","2026-08-07")}
        for w in a.windows:
            s,e=windows[w]
            cfg=BacktestConfig(start_date=s,end_date=e, rs_rank_min=0.5, min_avg_amount=0.7)
            data=BacktestData(cfg)
            print(f"loading {w} {s}~{e} n_days {len(data.calendar)}", flush=True)
            res=simulate_d1(data)
            print(f"[{w}] D1 Top1+RS8+新高+量1.5+50-300 5*20% hold30 stop-8 total {res['total']:.1f}% dd {res['dd']:.1f}% n {res['n']} win {res['win']:.2f} avg {res['avg']:.1f}" if res["win"] else f"[{w}] total {res['total']:.1f}% n {res['n']}", flush=True)
    else:
        cfg=BacktestConfig(start_date=a.start,end_date=a.end, rs_rank_min=0.5, min_avg_amount=0.7)
        data=BacktestData(cfg)
        print(f"data {a.start}~{a.end} n {len(data.calendar)}", flush=True)
        res=simulate_d1(data)
        print(f"D1 total {res['total']:.1f}% dd {res['dd']:.1f}% n {res['n']} win {res['win']:.2f} avg {res['avg']:.2f}" if res["win"] else f"D1 total {res['total']:.1f}%", flush=True)
