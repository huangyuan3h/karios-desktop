'use client';

import * as React from 'react';

import type { TwinStarTradePlan, TwinStarTradeRow } from '@/lib/twin-star-trade-plan';

function ActionRow({
  r,
  onAct,
  done,
}: {
  r: TwinStarTradeRow;
  onAct?: (row: TwinStarTradeRow) => void;
  done: boolean;
}) {
  const isBuy = r.side === 'BUY';
  return (
    <div className="flex items-center gap-2 border-b border-[var(--k-border)]/60 py-1.5 last:border-0">
      <span
        className={
          isBuy
            ? 'w-8 shrink-0 text-[11px] font-bold text-emerald-700 dark:text-emerald-300'
            : 'w-8 shrink-0 text-[11px] font-bold text-red-600 dark:text-red-400'
        }
      >
        {isBuy ? '买' : '卖'}
      </span>
      <span className="min-w-0 flex-1 truncate font-mono text-[12px]">{r.symbol}</span>
      <span className="font-mono text-[12px] font-semibold tabular-nums">{r.navPct}%</span>
      {r.swapFrom ? (
        <span className="hidden text-[10px] text-amber-700 sm:inline dark:text-amber-300">涨停换</span>
      ) : null}
      {onAct ? (
        done ? (
          <span className="w-10 text-right text-[10px] text-[var(--k-muted)]">已记</span>
        ) : (
          <button
            type="button"
            onClick={() => onAct(r)}
            className={
              isBuy
                ? 'w-10 rounded bg-emerald-600 px-1.5 py-0.5 text-[11px] font-semibold text-white'
                : 'w-10 rounded bg-red-600 px-1.5 py-0.5 text-[11px] font-semibold text-white'
            }
          >
            {isBuy ? '买入' : '卖出'}
          </button>
        )
      ) : null}
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

  return (
    <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] px-3 py-2">
      <div className="mb-1 flex items-center gap-2 text-[12px] font-semibold">
        <span>今日动作</span>
        <span className="font-mono text-[10px] font-normal text-[var(--k-muted)]">
          {snapLabel ? `${snapLabel} 行情` : '待 14:20 刷新'}
          {` · 买${plan.buys.length} 卖${plan.sells.length}`}
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
      {plan.buys.length === 0 && plan.sells.length === 0 ? (
        <div className="py-2 text-[12px] text-[var(--k-muted)]">今日无买卖</div>
      ) : (
        <div>
          {plan.sells.map((r) => (
            <ActionRow key={`s-${r.sleeve}-${r.symbol}`} r={r} onAct={onAct} done={doneSymbols.has(r.symbol)} />
          ))}
          {plan.buys.map((r) => (
            <ActionRow key={`b-${r.sleeve}-${r.symbol}`} r={r} onAct={onAct} done={doneSymbols.has(r.symbol)} />
          ))}
        </div>
      )}
    </div>
  );
}
