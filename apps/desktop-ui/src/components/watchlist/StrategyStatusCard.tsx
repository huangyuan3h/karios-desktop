'use client';

import { useStrategyCatalogQuery } from '@/lib/queries/backtest';
import type { StrategyMode } from '@/lib/strategy-settings';
import { cn } from '@/lib/utils';

const WIRING_NOTE: Record<StrategyMode, string> = {
  harbor: '组件已全量接线（港湾视图不显示本卡）。',
  homeport:
    '核心腿 = 港湾（上方决策卡仍按港湾配方）；B3 被动腿按月再平衡——paper/实盘记账未接线（OPT-186）。',
  starport:
    '母港底仓 + 卫星 1/3 overlay（K1 余量薄）。卫星信号/成交组件待 OPT-178 重接 + 分腿记账（OPT-186）。',
  starship:
    '卫星 standalone；Live 路径已随 OPT-178 退役——本视图当前仅展示回测口径，数据收集组件待 OPT-186。未过执行审计，不进 Live。',
};

function fmtPct(v: number): string {
  return `${v > 0 ? '+' : ''}${v.toFixed(1)}%`;
}

/** Compact strategy context card shown when the selected view is not plain 港湾. */
export function StrategyStatusCard({ mode }: { mode: StrategyMode }) {
  const q = useStrategyCatalogQuery();
  const entry = (q.data?.strategies ?? []).find((s) => s.key === mode);
  if (!entry) return null;
  return (
    <section
      data-testid="strategy-status-card"
      className="mb-4 rounded-lg border border-sky-500/30 bg-sky-500/5 px-3 py-2"
    >
      <div className="flex flex-wrap items-baseline gap-2">
        <span className="text-[12px] font-medium">{entry.name}</span>
        <span
          className={cn(
            'rounded px-1.5 py-0.5 text-[10px]',
            entry.status === 'rejected'
              ? 'bg-red-500/10 text-red-700 dark:text-red-300'
              : entry.status === 'aggressive_unaudited'
                ? 'bg-amber-500/10 text-amber-700 dark:text-amber-300'
                : 'bg-sky-500/10 text-sky-700 dark:text-sky-300',
          )}
        >
          {entry.statusLabel}
        </span>
        <span className="text-[10px] text-[var(--k-muted)]">
          {entry.tag} · clean {entry.updated}
        </span>
      </div>
      <p className="mt-1 text-xs text-[var(--k-muted)]">{entry.structure}</p>
      <p className="mt-1 text-xs tabular-nums">
        长窗 {fmtPct(entry.windows.long.total)} / SR {entry.windows.long.sharpe.toFixed(2)} / MDD{' '}
        {fmtPct(entry.windows.long.mdd)} · valid {fmtPct(entry.windows.valid.total)} · OOS2{' '}
        {fmtPct(entry.windows.OOS2.total)}
      </p>
      <div className="mt-1 text-[11px] text-[var(--k-muted)]">
        <span className="text-emerald-700 dark:text-emerald-300">优：</span>
        {entry.pros.join('；')}
      </div>
      <div className="mt-0.5 text-[11px] text-[var(--k-muted)]">
        <span className="text-red-700 dark:text-red-300">劣：</span>
        {entry.cons.join('；')}
      </div>
      <p className="mt-1.5 text-[11px] text-amber-700 dark:text-amber-300">
        {WIRING_NOTE[mode]}
      </p>
    </section>
  );
}
