import * as React from 'react';

import { cn } from '@/lib/utils';
import type { PortfolioHealthResponse } from '@/lib/queries/portfolioHealth';

/**
 * T6 (2026-08-20) — NASDAQ-100 ETF held as a separate "third asset / US"
 * region, tracked by the sleeve rules (200d MA line) — NOT the CN S-3 exit
 * rules. Shows the held ETF's price vs MA200 and the actionable T6 status.
 */
export function ThirdAssetHealthBlock({
  holding,
  onOpen,
}: {
  holding: PortfolioHealthResponse['thirdAssetHolding'];
  onOpen?: (symbol: string) => void;
}) {
  if (!holding?.active) {
    return null;
  }
  const actionStyle = ({
    HOLD: 'border-emerald-500/40 bg-emerald-500/10 text-emerald-700 dark:text-emerald-300',
    SELL_TO_A_SHARE: 'border-amber-500/40 bg-amber-500/10 text-amber-700 dark:text-amber-300',
    SELL_TO_REPO: 'border-red-500/40 bg-red-500/10 text-red-700 dark:text-red-300',
  } as Record<string, string>)[holding.action ?? 'HOLD'] ?? 'border-[var(--k-border)] bg-[var(--k-surface)] text-[var(--k-muted)]';

  const icon = ({
    HOLD: '✅',
    SELL_TO_A_SHARE: '🔔',
    SELL_TO_REPO: '⚠️',
  } as Record<string, string>)[holding.action ?? 'HOLD'] ?? '💼';
  const details = [
    holding.price != null ? `现价 ${holding.price}` : null,
    holding.ma200 != null ? `MA200 ${holding.ma200}` : null,
    holding.positionPct != null ? `仓位 ${holding.positionPct}%` : null,
    holding.pnlPct != null ? `盈亏 ${holding.pnlPct >= 0 ? '+' : ''}${holding.pnlPct}%` : null,
    holding.asOfDate ? `asOf ${holding.asOfDate}` : null,
  ]
    .filter(Boolean)
    .join(' · ');

  return (
    <div className="flex min-w-0 flex-col gap-2 rounded-lg border border-[var(--k-border)] bg-[var(--k-surface-2)]/60 p-2.5">
      <div className="flex items-center gap-2 text-[11px] font-semibold">
        <span className="rounded border border-[var(--k-border)] bg-[var(--k-surface)] px-1.5 py-0.5">US</span>
        第三资产 · 纳指ETF（200日线规则）
        <span className="ml-auto text-[10px] font-normal tabular-nums text-[var(--k-muted)]">{holding.asOfDate ?? '—'}</span>
      </div>
      <div className="flex flex-wrap items-center gap-2 text-[11px]">
        <button
          type="button"
          onClick={() => onOpen?.(holding.symbol ?? '')}
          className="font-medium hover:underline"
          title="点击查看行情"
        >
          {icon} {holding.name ?? holding.symbol}
        </button>
        <span className={cn('rounded border px-1.5 py-0.5 font-medium', actionStyle)}>
          {holding.label ?? holding.action}
        </span>
        <span className="text-[var(--k-muted)]">
          {holding.aboveMa200 ? '站上 200 日线' : '跌破 200 日线'}
        </span>
      </div>
      {details ? <div className="text-[11px] tabular-nums text-[var(--k-muted)]">{details}</div> : null}
      {holding.message ? <div className="text-[11px] opacity-90">{holding.message}</div> : null}
      {holding.note ? <div className="text-[11px] text-amber-700 dark:text-amber-300">{holding.note}</div> : null}
    </div>
  );
}