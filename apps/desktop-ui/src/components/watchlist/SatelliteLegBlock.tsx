'use client';

import type { TimelineRow } from '@/lib/queries/backtest';
import { cn } from '@/lib/utils';

/** Satellite leg state from the research replay rows (last session). */
export function SatelliteLegBlock({ row }: { row?: TimelineRow | null }) {
  if (!row) {
    return (
      <div
        data-testid="satellite-leg-block"
        className="mt-2 rounded-md border border-[var(--k-border)] px-2.5 py-1.5 text-[10px] text-[var(--k-muted)]"
      >
        卫星腿：最近交易日状态不可用
      </div>
    );
  }
  const active = row.satActive === true;
  const pos = row.satPositions ?? 0;
  const slots = row.satSlots ?? 0;
  const holdingLabel = active
    ? pos > 0
      ? `有仓 ${pos}/${slots} 槽`
      : `今日有成交（现持 0/${slots} 槽）`
    : '空仓';
  return (
    <div
      data-testid="satellite-leg-block"
      className="mt-2 rounded-md border border-violet-500/30 bg-violet-500/5 px-2.5 py-1.5 text-[10px]"
    >
      <div className="flex flex-wrap items-center gap-2">
        <span className="font-medium">卫星腿</span>
        <span
          className={cn(
            'rounded px-1.5 py-0.5',
            active
              ? 'bg-violet-500/15 text-violet-700 dark:text-violet-300'
              : 'bg-[var(--k-surface-2)] text-[var(--k-muted)]',
          )}
        >
          {holdingLabel}
        </span>
        <span className="text-[var(--k-muted)]">
          最近交易日 {row.date} · 闸 {row.gateOpen === false ? '关' : '开'}
          {typeof row.filledToday === 'number' ? ` · 当日成交 ${row.filledToday}` : ''}
        </span>
      </div>
      <div className="mt-1 text-[var(--k-muted)]">
        回测口径（研究 replay）。今日 14:30 信号名单/成交记录组件待 OPT-178 重接 + OPT-186；未过执行审计。
      </div>
    </div>
  );
}
