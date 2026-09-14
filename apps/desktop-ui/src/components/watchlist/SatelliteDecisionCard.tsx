'use client';

import * as React from 'react';

import { useTimelineQuery, type TimelineRow } from '@/lib/queries/backtest';
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
          {active ? `有仓 ${row.satPositions ?? 0}/${row.satSlots ?? 0} 槽` : '空仓'}
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

/** 星舰 view: satellite-only decision surface. */
export function SatelliteDecisionCard() {
  const today = new Date().toISOString().slice(0, 10);
  const start = (() => {
    const d = new Date();
    d.setFullYear(d.getFullYear() - 1);
    return d.toISOString().slice(0, 10);
  })();
  const q = useTimelineQuery(start, today, 'starship', true);
  const rows = q.data?.rows ?? [];
  const last = rows[rows.length - 1];
  return (
    <div className="mb-4 rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] px-3 py-2.5">
      <div className="flex flex-wrap items-baseline gap-2">
        <span className="text-[13px] font-semibold">星舰 · 今日决策</span>
        <span className="text-[10px] text-[var(--k-muted)]">
          卫星 standalone（100% 曝露）· 未过执行审计，不进 Live
        </span>
      </div>
      <SatelliteLegBlock row={last} />
    </div>
  );
}
