'use client';

import * as React from 'react';

import type { TwinStarTradePlan, TwinStarTradeRow } from '@/lib/twin-star-trade-plan';

function SideTag({ side }: { side: TwinStarTradeRow['side'] }) {
  if (side === 'BUY') {
    return <span className="w-8 shrink-0 text-[11px] font-bold text-emerald-700 dark:text-emerald-300">买</span>;
  }
  if (side === 'SELL') {
    return <span className="w-8 shrink-0 text-[11px] font-bold text-red-600 dark:text-red-400">卖</span>;
  }
  return <span className="w-8 shrink-0 text-[11px] font-semibold text-[var(--k-muted)]">留</span>;
}

function ActionRow({
  r,
  onAct,
  done,
}: {
  r: TwinStarTradeRow;
  onAct?: (row: TwinStarTradeRow) => void;
  done: boolean;
}) {
  const canAct = r.side === 'BUY' || r.side === 'SELL';
  return (
    <div className="flex items-center gap-2 border-b border-[var(--k-border)]/60 py-1.5 last:border-0">
      <SideTag side={r.side} />
      <span className="min-w-0 flex-1 truncate font-mono text-[12px]">
        {r.name && r.name !== r.symbol ? r.name : r.symbol}
      </span>
      <span className="font-mono text-[12px] font-semibold tabular-nums">{r.navPct}%</span>
      {r.swapFrom ? (
        <span className="hidden text-[10px] text-amber-700 sm:inline dark:text-amber-300">涨停换</span>
      ) : null}
      {canAct && onAct ? (
        done ? (
          <span className="w-10 text-right text-[10px] text-[var(--k-muted)]">已记</span>
        ) : (
          <button
            type="button"
            onClick={() => onAct(r)}
            className={
              r.side === 'BUY'
                ? 'w-10 rounded bg-emerald-600 px-1.5 py-0.5 text-[11px] font-semibold text-white'
                : 'w-10 rounded bg-red-600 px-1.5 py-0.5 text-[11px] font-semibold text-white'
            }
          >
            {r.side === 'BUY' ? '买入' : '卖出'}
          </button>
        )
      ) : (
        <span className="w-10 text-right text-[10px] text-[var(--k-muted)]">{r.side === 'HOLD' ? '暂留' : ''}</span>
      )}
    </div>
  );
}

function AssetBlock({
  title,
  subtitle,
  rows,
  onAct,
  doneSymbols,
  empty,
}: {
  title: string;
  subtitle: string;
  rows: TwinStarTradeRow[];
  onAct?: (row: TwinStarTradeRow) => void;
  doneSymbols: Set<string>;
  empty: string;
}) {
  const ordered = [
    ...rows.filter((r) => r.side === 'SELL'),
    ...rows.filter((r) => r.side === 'BUY'),
    ...rows.filter((r) => r.side === 'HOLD'),
  ];
  return (
    <div className="rounded-md border border-[var(--k-border)]/80 px-2.5 py-2">
      <div className="mb-1 flex items-baseline gap-2">
        <span className="text-[12px] font-semibold">{title}</span>
        <span className="min-w-0 truncate text-[10px] text-[var(--k-muted)]">{subtitle}</span>
      </div>
      {ordered.length === 0 ? (
        <div className="py-1 text-[12px] text-[var(--k-muted)]">{empty}</div>
      ) : (
        ordered.map((r) => (
          <ActionRow key={`${r.side}-${r.sleeve}-${r.symbol}`} r={r} onAct={onAct} done={doneSymbols.has(r.symbol)} />
        ))
      )}
    </div>
  );
}

export function TwinStarTradePlanPanel({
  plan,
  onAct,
  doneSymbols,
  snapshotAt,
  onRefresh,
  refreshing,
}: {
  plan: TwinStarTradePlan;
  onAct?: (row: TwinStarTradeRow) => void;
  doneSymbols: Set<string>;
  snapshotAt?: string | null;
  onRefresh?: () => void;
  refreshing?: boolean;
}) {
  const snapLabel = snapshotAt
    ? snapshotAt.includes('T')
      ? snapshotAt.slice(11, 16)
      : snapshotAt
    : null;
  const stocks = [...plan.buys, ...plan.sells, ...plan.holds].filter((r) => r.kind === 'stock');
  const etfs = [...plan.buys, ...plan.sells, ...plan.holds].filter((r) => r.kind === 'etf');
  const stockActs = stocks.filter((r) => r.side !== 'HOLD').length;
  const etfActs = etfs.filter((r) => r.side !== 'HOLD').length;

  return (
    <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] px-3 py-2">
      <div className="mb-1.5 flex items-center gap-2 text-[12px] font-semibold">
        <span>今日下单</span>
        <span className="font-mono text-[10px] font-normal text-[var(--k-muted)]">
          {snapLabel ? `${snapLabel} 行情` : '待 14:20 刷新'}
          {` · 股票${stockActs} 笔 · ETF${etfActs} 笔`}
        </span>
        {onRefresh ? (
          <button
            type="button"
            onClick={onRefresh}
            disabled={refreshing}
            className="ml-auto rounded border border-[var(--k-border)] px-1.5 py-0.5 text-[10px] font-normal text-[var(--k-muted)] hover:border-[var(--k-accent)]/60"
          >
            {refreshing ? '拉取中…' : '刷新当日行情'}
          </button>
        ) : null}
      </div>
      <div className="mb-2 text-[10px] leading-snug text-[var(--k-muted)]">{plan.bookNote}</div>
      <div className="flex flex-col gap-2">
        <AssetBlock
          title="股票"
          subtitle={plan.satHeadline}
          rows={stocks}
          onAct={onAct}
          doneSymbols={doneSymbols}
          empty="今日股票不买不卖"
        />
        <AssetBlock
          title="ETF"
          subtitle={plan.etfHeadline}
          rows={etfs}
          onAct={onAct}
          doneSymbols={doneSymbols}
          empty="今日 ETF 不调仓"
        />
      </div>
      {plan.recipeNames.length > 0 ? (
        <details className="mt-2 text-[10px] text-[var(--k-muted)]">
          <summary className="cursor-pointer">
            策略回放卫星仓 {plan.recipeSatHeld}/15（模拟，不是你的券商仓）
          </summary>
          <div className="mt-1 font-mono leading-relaxed">
            {plan.recipeNames.map((h) => `${h.ts}${h.daysLeft != null ? `(剩${h.daysLeft}d)` : ''}`).join(' · ')}
          </div>
        </details>
      ) : null}
    </div>
  );
}
