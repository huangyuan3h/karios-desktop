'use client';

import { useB3StateQuery } from '@/lib/queries/backtest';
import { cn } from '@/lib/utils';

/** B3 risk-budget leg: current-month target / drift / rebalance trades. */
export function B3LegBlock() {
  const q = useB3StateQuery();
  const state = q.data;
  if (!state) return null;
  if (!state.ok) {
    return (
      <div
        data-testid="b3-leg-block"
        className="mt-2 rounded-md border border-[var(--k-border)] px-2.5 py-1.5 text-[10px] text-[var(--k-muted)]"
      >
        B3 腿：{state.error ?? '状态不可用'}
      </div>
    );
  }
  return (
    <div
      data-testid="b3-leg-block"
      className="mt-2 rounded-md border border-[var(--k-border)] bg-[var(--k-bg)]/40 px-2.5 py-1.5 text-[10px]"
    >
      <div className="flex flex-wrap items-center gap-2">
        <span className="font-medium">B3 月再平衡</span>
        <span className="text-[var(--k-muted)]">
          本月基准 {state.rebalanceDate} · 截至 {state.asOf}
        </span>
        {state.trades && state.trades.length ? (
          <span className="flex flex-wrap gap-1">
            {state.trades.map((t) => (
              <span
                key={t.symbol}
                className={cn(
                  'rounded px-1 py-0.5 font-mono',
                  t.side === 'BUY'
                    ? 'bg-emerald-500/10 text-emerald-700 dark:text-emerald-300'
                    : 'bg-red-500/10 text-red-700 dark:text-red-300',
                )}
              >
                {t.side === 'BUY' ? '买' : '卖'} {t.name} {Math.abs(t.deltaPct).toFixed(1)}%
              </span>
            ))}
          </span>
        ) : (
          <span className="text-[var(--k-muted)]">本月无需调整（漂移 &lt; 0.5%）</span>
        )}
      </div>
      {state.universe ? (
        <div className="mt-1 flex flex-wrap gap-1 text-[var(--k-muted)]">
          {state.universe.map((u) => (
            <span
              key={u.symbol}
              className="rounded border border-[var(--k-border)] px-1 py-0.5 font-mono"
            >
              {u.name} {u.targetPct.toFixed(1)}%
            </span>
          ))}
        </div>
      ) : null}
      <div className="mt-1 text-[var(--k-muted)]">{state.note}</div>
    </div>
  );
}
