'use client';

import * as React from 'react';

import { useQuery } from '@tanstack/react-query';
import { BarChart3 } from 'lucide-react';

import { format } from 'date-fns';
import { CalendarIcon } from 'lucide-react';

import { Button } from '@/components/ui/button';
import { Calendar } from '@/components/ui/calendar';
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover';
import { DATA_SYNC_BASE_URL } from '@/lib/endpoints';
import { cn } from '@/lib/utils';
import { useTimelineQuery } from '@/lib/queries/backtest';
import { fetchPortfolioHealth } from '@/lib/queries/portfolioHealth';

function tone(v: number | null | undefined): string {
  if (v == null) return 'text-[var(--k-muted)]';
  return v >= 0 ? 'text-emerald-700 dark:text-emerald-300' : 'text-red-700 dark:text-red-400';
}

export function RecentDailyCompareCard() {
  const today = new Date().toISOString().slice(0, 10);
  const [start, setStart] = React.useState(() => {
    const d = new Date();
    d.setMonth(d.getMonth() - 2);
    return d.toISOString().slice(0, 10);
  });
  const q = useTimelineQuery(start, today, true);
  const ph = useQuery({ queryKey: ['portfolio-health', 'watchlist-yield'], queryFn: () => fetchPortfolioHealth(), staleTime: 60_000 });
  const rows = q.data?.rows ?? [];
  const show = rows.slice(-60);
  const holdings = React.useMemo(() => {
    const a = ph.data?.holdings ?? [];
    const b = ph.data?.hkHealth?.holdings ?? [];
    const c = (ph.data?.multiAssetHoldings ?? []).map((m) => ({ symbol: m.symbol, name: m.name, positionPct: m.positionPct, costPrice: m.costPrice, pnlPct: undefined as unknown as number }));
    const d = ph.data?.thirdAssetHolding?.symbol ? [{ symbol: ph.data.thirdAssetHolding.symbol, name: ph.data.thirdAssetHolding.name ?? undefined, positionPct: ph.data.thirdAssetHolding.positionPct ?? 0, costPrice: ph.data.thirdAssetHolding.costPrice ?? 0, pnlPct: ph.data.thirdAssetHolding.pnlPct ?? 0 } as never] : [];
    const e = ph.data?.thirdAssetSleeve?.holding513100 ? [] as never[] : [];
    const all = [...a, ...b, ...c, ...d];
    // 去重
    const seen = new Set<string>();
    return all.filter((h) => {
      if (seen.has(h.symbol)) return false;
      seen.add(h.symbol);
      return true;
    });
  }, [ph.data]);
  const avgWatchPnl = holdings.length ? holdings.reduce((s, h) => s + (h.pnlPct ?? 0) * ((h.positionPct ?? 0) / 100), 0) / (holdings.reduce((s, h) => s + (h.positionPct ?? 0), 0) / 100 || 1) : null;

  // 逐日 Watchlist 收益率：用持仓成本 + 日线收盘加权（前端即算，无需新后端）
  const barQs = useQuery({
    queryKey: ['watchlist-bars', start, today, holdings.map((h) => h.symbol).join(',')],
    queryFn: async () => {
      if (!holdings.length) return {} as Record<string, Record<string, number>>;
      const out: Record<string, Record<string, number>> = {};
      await Promise.all(
        holdings.map(async (h) => {
          try {
            const res = await fetch(`${DATA_SYNC_BASE_URL}/market/stocks/${encodeURIComponent(h.symbol)}/bars?days=70`, { cache: 'no-store' });
            if (!res.ok) return;
            const j = (await res.json()) as { bars?: Array<{ date: string; close: number }>; data?: Array<{ date: string; close: number }> };
            const bars = (j.bars ?? j.data ?? []) as Array<{ date: string; close: number }>;
            const map: Record<string, number> = {};
            bars.forEach((b) => {
              const d = (b.date ?? '').slice(0, 10);
              if (d) map[d] = b.close;
            });
            out[h.symbol] = map;
          } catch {
            /* ignore */
          }
        }),
      );
      return out;
    },
    enabled: holdings.length > 0,
    staleTime: 60_000,
  });
  const barsMap = barQs.data ?? {};
  const watchDaily: Record<string, number | null> = {};
  // 计算每日加权累计收益（相对成本）
  for (const r of show) {
    let wsum = 0;
    let wtot = 0;
    for (const h of holdings) {
      const w = h.positionPct ?? 0;
      if (!w) continue;
      const cost = h.costPrice ?? 0;
      if (!cost) continue;
      const close = barsMap[h.symbol]?.[r.date];
      if (close == null) continue;
      wsum += ((close - cost) / cost) * 100 * (w / 100);
      wtot += w / 100;
    }
    watchDaily[r.date] = wtot ? wsum / wtot : avgWatchPnl;
  }

  if (q.isError) return <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] p-3 text-xs text-red-700">{String(q.error)}</div>;
  if (!rows.length) return <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] p-3 text-xs text-[var(--k-muted)]">加载中…（首次 ~50s）</div>;

  const last = rows[rows.length - 1];
  const first = rows[0];

  const firstNav = first ? (first.navSingleReturnPct ?? first.navMultiReturnPct) : 0;
  const lastNav = last.navSingleReturnPct ?? last.navMultiReturnPct;
  return (
    <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] p-3">
      <div className="mb-2 flex items-center gap-2 text-[12px] font-medium">
        <BarChart3 className="size-3.5" />
        最近操作 vs 回测三线逐日对比
        <span className="ml-auto flex items-center gap-1 text-[10px] font-normal tabular-nums text-[var(--k-muted)]">
          <Popover>
            <PopoverTrigger asChild>
              <Button variant="outline" data-empty={!start} className="h-7 justify-start text-left text-[11px] font-normal data-[empty=true]:text-muted-foreground">
                <CalendarIcon className="mr-1.5 h-3.5 w-3.5" />
                {start ? format(new Date(start), 'PPP') : <span>Pick a date</span>}
              </Button>
            </PopoverTrigger>
            <PopoverContent className="w-auto p-0" align="end">
              <Calendar mode="single" selected={start ? new Date(start) : undefined} onSelect={(d) => d && setStart(format(d, 'yyyy-MM-dd'))} />
            </PopoverContent>
          </Popover>
          ~ {today} · {rows.length} 日 · {holdings.map((h) => h.symbol).join(' / ') || '—'}
        </span>
      </div>
      <div className="mb-2 grid grid-cols-3 gap-2 text-[11px]">
        <div className="rounded border border-[var(--k-accent)]/30 bg-[var(--k-accent)]/5 px-2 py-1.5">
          <div className="text-[var(--k-muted)]">当前回测收益率</div>
          <div className={cn('font-semibold tabular-nums', tone(lastNav))}>{lastNav.toFixed(2)}%</div>
          <div className="text-[10px] text-[var(--k-muted)]">最优单轨 · {last.pick ?? '—'} {last.pickTs ?? ''}</div>
        </div>
        <div className="rounded border border-[var(--k-border)] px-2 py-1.5">
          <div className="text-[var(--k-muted)]">Watchlist 收益率</div>
          <div className={cn('font-semibold tabular-nums', tone(avgWatchPnl))}>{avgWatchPnl != null ? `${avgWatchPnl.toFixed(2)}%` : '—'}</div>
          <div className="text-[10px] text-[var(--k-muted)]">持仓 {holdings.length} 票 · {holdings.map((h) => h.symbol).join(', ')}</div>
        </div>
        <div className="rounded border border-[var(--k-border)] px-2 py-1.5">
          <div className="text-[var(--k-muted)]">差距</div>
          <div className={cn('font-semibold tabular-nums', tone(avgWatchPnl != null ? lastNav - avgWatchPnl : null))}>{avgWatchPnl != null ? `${(lastNav - avgWatchPnl).toFixed(2)}%` : '—'}</div>
          <div className="text-[10px] text-[var(--k-muted)]">回测 - Watchlist</div>
        </div>
      </div>
      <div className="max-h-[360px] overflow-auto rounded border border-[var(--k-border)]">
        <table className="w-full text-left text-xs tabular-nums">
          <thead className="sticky top-0 bg-[var(--k-surface)]">
            <tr className="text-[10px] text-[var(--k-muted)]">
              <th className="py-1 pl-2 pr-2">日期</th>
              <th className="py-1 pr-2">回测最佳应该持有</th>
              <th className="py-1 pr-2">实际持仓</th>
              <th className="py-1 pr-2">最优NAV%</th>
              <th className="py-1 pr-2">Watchlist持有收益率</th>
            </tr>
          </thead>
          <tbody>
            {show.map((r) => {
              const single = (r as unknown as { navSingleReturnPct?: number }).navSingleReturnPct ?? r.navMultiReturnPct;
              const isStock = r.pick === 'STOCK';
              const pickSym = r.pick === 'GOLD' ? '518880' : r.pick === 'OIL' ? '513350' : r.pick === 'NASDAQ' ? '513110' : r.pick === 'BOND10' ? '511260' : 'GC001';
              const optimalHolding = isStock ? ((r as unknown as { stockSymbols?: string[] }).stockSymbols ?? []).join(' ') || '—' : `${r.pick} ${pickSym}`;
              const actualHolding = holdings.map((h) => h.symbol).join(', ') || '—';
              const watchYield = watchDaily[r.date] ?? avgWatchPnl;
              return (
                <tr key={r.date} className="border-t border-[var(--k-border)]/60">
                  <td className="py-1 pl-2 pr-2 font-mono">{r.date}</td>
                  <td className="max-w-[200px] truncate py-1 pr-2 text-[11px]" title={optimalHolding}>{optimalHolding}</td>
                  <td className="min-w-[220px] whitespace-normal break-words py-1 pr-2 text-[11px] text-[var(--k-muted)]" title={actualHolding}>{actualHolding}</td>
                  <td className={cn('py-1 pr-2 font-medium', tone(single))}>{single.toFixed(2)}%</td>
                  <td className={cn('py-1 pr-2', tone(watchYield))}>{watchYield != null ? `${watchYield.toFixed(2)}%` : '—'}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
      <p className="mt-1.5 text-[10px] text-[var(--k-muted)]">最近 60 日逐日；最优单轨=每日 mom60+MA200 择强 100%，Watchlist 列=当前持仓按成本加权的逐日收盘收益（日线来自 /market/stocks/&#123;symbol&#125;/bars）。</p>
    </div>
  );
}
