#!/usr/bin/env python3
"""Sleeve exit variants: 20d -10% hard cut and trailing -8%"""
import json, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]/"src"))
from run_walk_forward import S3_CONFIG, WINDOWS
from data_sync_service.service.backtest_engine import BacktestConfig, BacktestData, simulate
from data_sync_service.service.portfolio_nav_sim import load_third_asset_cache, _sma, _daily_ret, GC001_DAYS

CACHE_FILE = Path(__file__).resolve().parents[1]/"data/third_asset_cache.json"

def simulate_with_exit(positions_by_day, close_by_ts_day, calendar, etf_close_by_day, repo_rate_by_day, min_idle_pct=0.0, exit_mode="base"):
    from data_sync_service.service.portfolio_nav_sim import MA_WINDOW
    etf_days = sorted(etf_close_by_day)
    etf_ret={}
    for i,d in enumerate(etf_days):
        prev=etf_close_by_day[etf_days[i-1]] if i>0 else None
        etf_ret[d]=_daily_ret(etf_close_by_day[d], prev) if prev else 0.0
    repo_ret={d: float(r)/100/GC001_DAYS for d,r in repo_rate_by_day.items()}
    ma200_by_day={}
    for i in range(len(etf_days)):
        lo=max(0,i-MA_WINDOW+1)
        ma=_sma([etf_close_by_day[etf_days[j]] for j in range(lo,i+1)], MA_WINDOW)
        if ma is not None:
            ma200_by_day[etf_days[i]]=ma
    # precompute 20d high and peak
    high20={}
    for i,d in enumerate(etf_days):
        lo=max(0,i-19)
        high20[d]=max(etf_close_by_day[etf_days[j]] for j in range(lo,i+1))
    snap_by_day={str(s.get("date")): s for s in positions_by_day}
    day_idx={d:i for i,d in enumerate(calendar)}
    holding=False
    peak=0.0
    nav_base=1.0; nav_sleeve=1.0
    base_peak=1.0; sleeve_peak=1.0
    max_dd_base=0.0; max_dd_sleeve=0.0
    for day in calendar:
        snap=snap_by_day.get(day)
        deployed_ret=0.0; deployed_pct=0.0
        if snap:
            for pos in snap.get("positions") or []:
                try: pct=float(pos.get("position_pct") or 0.0)
                except: continue
                if pct<=0: continue
                entry=str(pos.get("entry_date") or "")
                if entry and day<=entry: continue
                closes=close_by_ts_day.get(str(pos.get("ts_code") or "")) or {}
                today=closes.get(day)
                idx=day_idx.get(day)
                prev=closes.get(calendar[idx-1]) if idx and idx>0 else None
                if today is not None and prev:
                    deployed_ret+=pct*(today/prev-1.0)
                deployed_pct+=pct
        deployed_pct=min(1.0, deployed_pct); idle_pct=max(0.0,1-deployed_pct)
        close=etf_close_by_day.get(day); ma=ma200_by_day.get(day)
        above=close is not None and ma is not None and close>=ma
        # exit logic
        if holding:
            should_exit=False
            if not above:
                should_exit=True
            elif exit_mode=="hard20":
                # 20d -10% hard cut
                if close is not None and high20.get(day) and close < high20[day]*0.90:
                    should_exit=True
            elif exit_mode=="trail8":
                if close is not None and peak>0 and close < peak*0.92:
                    should_exit=True
            if should_exit:
                holding=False
                peak=0.0
            else:
                if close and close>peak:
                    peak=close
        else:
            if above and idle_pct*100>=min_idle_pct:
                holding=True
                peak=close if close else 0.0
        sleeve_ret=0.0
        if idle_pct>0:
            if holding:
                sleeve_ret=etf_ret.get(day,0.0)
            else:
                sleeve_ret=repo_ret.get(day,0.0)
        nav_base*=1+deployed_ret
        nav_sleeve*=1+deployed_ret+idle_pct*sleeve_ret
        base_peak=max(base_peak, nav_base); sleeve_peak=max(sleeve_peak, nav_sleeve)
        if base_peak>0: max_dd_base=max(max_dd_base,(base_peak-nav_base)/base_peak)
        if sleeve_peak>0: max_dd_sleeve=max(max_dd_sleeve,(sleeve_peak-nav_sleeve)/sleeve_peak)
    total_base=(nav_base-1)*100; total_sleeve=(nav_sleeve-1)*100
    return dict(totalBasePct=round(total_base,1), totalSleevePct=round(total_sleeve,1), deltaPct=round(total_sleeve-total_base,1), maxDdBasePct=round(max_dd_base*100,1), maxDdSleevePct=round(max_dd_sleeve*100,1))

def main():
    cache=json.loads(Path(CACHE_FILE).read_text())
    etf_close, repo_rate=load_third_asset_cache(cache)
    for mode in ["base","hard20","trail8"]:
        print(f"\n=== {mode} ===")
        print("| 窗口 | 基线 | 套筒 | 增量 | 基线DD | 套筒DD |")
        for w in ["OOS2","train","valid"]:
            start,end=WINDOWS[w]
            cfg=BacktestConfig(start_date=start,end_date=end,**S3_CONFIG)
            data=BacktestData(cfg)
            run=simulate(cfg,data)
            s=simulate_with_exit(run.positions_by_day, data.close_by_ts_day, data.calendar, etf_close, repo_rate, min_idle_pct=0.0, exit_mode=mode)
            print(f"| {w:5s} | {s['totalBasePct']:5.1f} | {s['totalSleevePct']:5.1f} | {s['deltaPct']:+5.1f} | {s['maxDdBasePct']:5.1f} | {s['maxDdSleevePct']:5.1f} |")

if __name__=="__main__": main()
