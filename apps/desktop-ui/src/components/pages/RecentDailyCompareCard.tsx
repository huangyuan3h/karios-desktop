'use client';

import * as React from 'react';

import { useQuery } from '@tanstack/react-query';
import { BarChart3, CalendarIcon } from 'lucide-react';
import { format } from 'date-fns';

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

const PICK_TS: Record<string, string> = {
  GOLD: '518880',
  OIL: '513350',
  NASDAQ: '513110',
  BOND10: '511260',
  REPO: 'GC001',
};

/**
 * 最近操作 vs Timeline 定案逐日对比。
 *
 * 左/定案列：GET /api/backtest/timeline（可切 机会双子星 / 单轨择强）
 * 右/操作列：portfolio-health 当前持仓 + bars 相对成本加权收益
 *            （当前快照套到每一行，非逐日账本回放）
 */
export function RecentDailyCompareCard() {
  const today = new Date().toISOString().slice(0, 10);
  const [start, setStart] = React.useState(() => {
    const d = new Date();
    d.setMonth(d.getMonth() - 2);
    return d.toISOString().slice(0, 10);
  });
  const [strategy, setStrategy] = React.useState<'twin_star' | 'pick_strong'>('twin_star');
  const isTwin = strategy === 'twin_star';
  const [habit, setHabit] = React.useState(true);
  const timelineQ = useTimelineQuery(
    start,
    today,
    strategy,
    true,
    isTwin && habit ? { satFill: 'same_1430', satExit: '1430', c1Pct: 0.03 } : undefined,
  );
  const healthQ = useQuery({
    queryKey: ['portfolio-health', 'recent-ops-vs-pick-strong'],
    queryFn: () => fetchPortfolioHealth(),
    staleTime: 60_000,
  });

  const rows = timelineQ.data?.rows ?? [];
  const mode = (timelineQ.data as { mode?: string } | undefined)?.mode ?? 'mom_compare';
  const show = rows.slice(-60);

  const holdings = React.useMemo(() => {
    const cn = healthQ.data?.holdings ?? [];
    const hk = healthQ.data?.hkHealth?.holdings ?? [];
    const multi = (healthQ.data?.multiAssetHoldings ?? []).map((m) => ({
      symbol: m.symbol,
      name: m.name,
      positionPct: m.positionPct,
      costPrice: m.costPrice,
      pnlPct: undefined as number | undefined,
    }));
    const all = [...cn, ...hk, ...multi];
    const seen = new Set<string>();
    return all.filter((h) => {
      if (seen.has(h.symbol)) return false;
      seen.add(h.symbol);
      return true;
    });
  }, [healthQ.data]);

  const avgOpsPnl = holdings.length
    ? holdings.reduce((s, h) => s + (h.pnlPct ?? 0) * ((h.positionPct ?? 0) / 100), 0) /
      (holdings.reduce((s, h) => s + (h.positionPct ?? 0), 0) / 100 || 1)
    : null;

  const barQs = useQuery({
    queryKey: ['recent-ops-bars', start, today, holdings.map((h) => h.symbol).join(',')],
    queryFn: async () => {
      if (!holdings.length) return {} as Record<string, Record<string, number>>;
      const out: Record<string, Record<string, number>> = {};
      await Promise.all(
        holdings.map(async (h) => {
          try {
            const res = await fetch(
              `${DATA_SYNC_BASE_URL}/market/stocks/${encodeURIComponent(h.symbol)}/bars?days=70`,
              { cache: 'no-store' },
            );
            if (!res.ok) return;
            const j = (await res.json()) as {
              bars?: Array<{ date: string; close: number }>;
              data?: Array<{ date: string; close: number }>;
            };
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
  const opsDaily: Record<string, number | null> = {};
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
    opsDaily[r.date] = wtot ? wsum / wtot : avgOpsPnl;
  }

  if (timelineQ.isError) {
    return (
      <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] p-3 text-xs text-red-700">
        {String(timelineQ.error)}
      </div>
    );
  }
  if (!rows.length) {
    return (
      <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] p-3 text-xs text-[var(--k-muted)]">
        加载中…（首次 Timeline ~50s）
      </div>
    );
  }

  const last = rows[rows.length - 1];
  const lastNav = last.navSingleReturnPct ?? last.navMultiReturnPct;
  const livePick = healthQ.data?.multiAssetSleeve?.pick?.key;

  return (
    <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] p-3">
      <div className="mb-2 flex items-center gap-2 text-[12px] font-medium">
        <BarChart3 className="size-3.5" />
        最近操作明细（现仓快照 · 非逐日账本）
        <span className="rounded bg-[var(--k-bg)] px-1.5 py-0.5 text-[10px] font-normal text-[var(--k-muted)]">
          次级表 · 复刻差距见上方
        </span>
        <span className="flex overflow-hidden rounded border border-[var(--k-border)] text-[10px] font-normal">
          <button
            type="button"
            onClick={() => setStrategy('twin_star')}
            className={cn('px-2 py-0.5', isTwin ? 'bg-[var(--k-accent)] text-white' : 'text-[var(--k-muted)]')}
          >
            机会双子星
          </button>
          <button
            type="button"
            onClick={() => setStrategy('pick_strong')}
            className={cn('px-2 py-0.5', !isTwin ? 'bg-[var(--k-accent)] text-white' : 'text-[var(--k-muted)]')}
          >
            单轨择强
          </button>
        </span>
        <span className="rounded bg-emerald-500/10 px-1.5 py-0.5 text-[10px] font-normal text-emerald-800 dark:text-emerald-200">
          {mode}
        </span>
        {isTwin ? (
          <button
            type="button"
            onClick={() => setHabit((v) => !v)}
            title="习惯对照：same_1430 + C1 3% + 第3日14:30卖（Live配方）；关=冻结T开盘收盘卖"
            className={cn(
              'rounded border px-1.5 py-0.5 text-[10px] font-normal',
              habit ? 'border-emerald-500/50 bg-emerald-500/10 text-emerald-700 dark:text-emerald-200' : 'border-[var(--k-border)] text-[var(--k-muted)]',
            )}
          >
            {habit ? '习惯C1+14:30卖·开' : '习惯对照·关'}
          </button>
        ) : null}
        <span className="ml-auto flex items-center gap-1 text-[10px] font-normal tabular-nums text-[var(--k-muted)]">
          <Popover>
            <PopoverTrigger asChild>
              <Button
                variant="outline"
                data-empty={!start}
                className="h-7 justify-start text-left text-[11px] font-normal data-[empty=true]:text-muted-foreground"
              >
                <CalendarIcon className="mr-1.5 h-3.5 w-3.5" />
                {start ? format(new Date(start), 'PPP') : <span>起始日</span>}
              </Button>
            </PopoverTrigger>
            <PopoverContent className="w-auto p-0" align="end">
              <Calendar
                mode="single"
                selected={start ? new Date(start) : undefined}
                onSelect={(d) => d && setStart(format(d, 'yyyy-MM-dd'))}
              />
            </PopoverContent>
          </Popover>
          ~ {today} · 近 {show.length} 日 / 共 {rows.length}
        </span>
      </div>

      <div className="mb-2 grid grid-cols-3 gap-2 text-[11px]">
        <div className="rounded border border-emerald-500/30 bg-emerald-500/5 px-2 py-1.5">
          <div className="text-[var(--k-muted)]">{isTwin ? '机会双子星累计' : '单轨择优累计'}</div>
          <div className={cn('font-semibold tabular-nums', tone(lastNav))}>{lastNav.toFixed(2)}%</div>
          <div className="text-[10px] text-[var(--k-muted)]">
            Timeline pick={last.pick ?? '—'}
            {isTwin ? ` · 目标${last.satActive ? 50 : 100}%` : ''} · live={livePick ?? '—'}
          </div>
        </div>
        <div className="rounded border border-[var(--k-border)] px-2 py-1.5">
          <div className="text-[var(--k-muted)]">最近操作（现仓）</div>
          <div className={cn('font-semibold tabular-nums', tone(avgOpsPnl))}>
            {avgOpsPnl != null ? `${avgOpsPnl.toFixed(2)}%` : '—'}
          </div>
          <div className="truncate text-[10px] text-[var(--k-muted)]" title={holdings.map((h) => h.symbol).join(', ')}>
            {holdings.length} 票 · {holdings.map((h) => h.symbol).join(', ') || '无持仓'}
          </div>
        </div>
        <div className="rounded border border-[var(--k-border)] px-2 py-1.5">
          <div className="text-[var(--k-muted)]">差距</div>
          <div
            className={cn(
              'font-semibold tabular-nums',
              tone(avgOpsPnl != null ? lastNav - avgOpsPnl : null),
            )}
          >
            {avgOpsPnl != null ? `${(lastNav - avgOpsPnl).toFixed(2)}%` : '—'}
          </div>
          <div className="text-[10px] text-[var(--k-muted)]">{isTwin ? '双子星' : '单轨'} − 最近操作</div>
        </div>
      </div>

      <div className="max-h-[360px] overflow-auto rounded border border-[var(--k-border)]">
        <table className="w-full text-left text-xs tabular-nums">
          <thead className="sticky top-0 bg-[var(--k-surface)]">
            <tr className="text-[10px] text-[var(--k-muted)]">
              <th className="py-1 pl-2 pr-2">日期</th>
              <th className="py-1 pr-2">{isTwin ? '核心该买' : '单轨应持（pick）'}</th>
              {isTwin ? <th className="py-1 pr-2">核心目标%</th> : null}
              <th className="py-1 pr-2">最近操作（现仓快照）</th>
              <th className="py-1 pr-2">{isTwin ? '双子星NAV%' : '单轨NAV%'}</th>
              <th className="py-1 pr-2">操作收益%</th>
            </tr>
          </thead>
          <tbody>
            {show.map((r) => {
              const single = r.navSingleReturnPct ?? r.navMultiReturnPct;
              const isStock = r.pick === 'STOCK';
              const pickSym = PICK_TS[r.pick ?? ''] ?? '';
              const optimalHolding = isStock
                ? (r.stockSymbols ?? []).join(' ') || 'STOCK'
                : `${r.pick ?? 'REPO'}${pickSym ? ` ${pickSym}` : ''}`;
              const actualHolding = holdings.map((h) => h.symbol).join(', ') || '—';
              const opsYield = opsDaily[r.date] ?? avgOpsPnl;
              return (
                <tr key={r.date} className="border-t border-[var(--k-border)]/60">
                  <td className="py-1 pl-2 pr-2 font-mono">{r.date}</td>
                  <td className="max-w-[200px] truncate py-1 pr-2 text-[11px]" title={optimalHolding}>
                    {optimalHolding}
                  </td>
                  {isTwin ? (
                    <td className="py-1 pr-2 text-[10px] text-[var(--k-muted)]">{r.satActive ? '50' : '100'}%</td>
                  ) : null}
                  <td
                    className="min-w-[220px] whitespace-normal break-words py-1 pr-2 text-[11px] text-[var(--k-muted)]"
                    title={`${actualHolding}（当前快照，非当日账本）`}
                  >
                    {actualHolding}
                  </td>
                  <td className={cn('py-1 pr-2 font-medium', tone(single))}>{single.toFixed(2)}%</td>
                  <td className={cn('py-1 pr-2', tone(opsYield))}>
                    {opsYield != null ? `${opsYield.toFixed(2)}%` : '—'}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
      <p className="mt-1.5 text-[10px] text-[var(--k-muted)]">
        <strong>{isTwin ? '机会双子星' : '单轨择优'}</strong> = <code>GET /api/backtest/timeline?strategy={strategy}</code>
        {isTwin
          ? `（opportunity v3 · satActive→50/50 · idle→核心100% · 与 Watchlist 同源${habit ? ' · 习惯C1+14:30卖（Live）' : ' · 冻结T开盘收盘卖'}）。`
          : '（pick_strong_track · mom_compare · 100% 硬切）。'}
        <strong>最近操作</strong> = 当前 Watchlist/体检持仓（含多资产 ETF）相对成本的加权收益（bars）。
        「现仓快照」每日行相同——不是历史逐日持仓回放。
      </p>
    </div>
  );
}
