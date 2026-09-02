'use client';

import * as React from 'react';

import type { TwinStarTradePlan } from '@/lib/twin-star-trade-plan';
import { twinStarRecipeLine } from '@/lib/twin-star-trade-plan';

export function TwinStarTradePlanPanel({
  plan,
  snapshotAt,
  frozen,
  onRefresh,
  refreshing,
}: {
  plan: TwinStarTradePlan;
  snapshotAt?: string | null;
  frozen?: boolean;
  onRefresh?: () => void;
  refreshing?: boolean;
}) {
  const snapLabel = snapshotAt
    ? snapshotAt.includes('T')
      ? snapshotAt.slice(11, 16)
      : snapshotAt
    : null;
  const stockBuys = plan.buys.filter((r) => r.kind === 'stock').length;
  const summary =
    stockBuys > 0
      ? `买股票 ${stockBuys} 只 × 总资产 ${plan.satSlotNavPct}%（共 ${plan.stockBuyNavPct}%）${
          plan.etfTrimPct > 0 ? ` · 先砍弱 ETF 腾 ${plan.etfTrimPct}%` : ''
        }`
      : plan.satHeadline;

  return (
    <div className="flex flex-wrap items-center gap-x-2 gap-y-1 rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] px-3 py-1.5 text-[11px]">
      <span className="font-semibold">今日</span>
      <span className="font-mono text-[10px] text-[var(--k-muted)]">
        {snapLabel ? `${snapLabel} 行情` : '等待快照'}
        {frozen ? ' · 收盘冻结至次日 09:00' : ''}
      </span>
      <span className="min-w-0 truncate text-[var(--k-fg)]">{summary}</span>
      {onRefresh ? (
        <button
          type="button"
          onClick={onRefresh}
          disabled={refreshing}
          className="ml-auto rounded border border-[var(--k-border)] px-1.5 py-0.5 text-[10px] text-[var(--k-muted)] hover:border-[var(--k-accent)]/60"
        >
          {refreshing ? '拉取中…' : '刷新行情'}
        </button>
      ) : null}
      <span className="w-full text-[10px] leading-snug text-[var(--k-muted)]" title="与冻结引擎 state_bucket_track / pick_strong 对齐的实盘映射">
        {twinStarRecipeLine(plan.satSlotNavPct)}
      </span>
    </div>
  );
}
