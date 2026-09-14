'use client';

import {
  STRATEGY_MODE_LABELS,
  STRATEGY_MODES,
  STRATEGY_MODE_TAGS,
  setStrategyMode,
  useStrategyMode,
} from '@/lib/strategy-settings';
import { cn } from '@/lib/utils';

/** Strategy view switcher (Watchlist). Synced with Settings' 默认策略. */
export function StrategyModeBar() {
  const mode = useStrategyMode();
  return (
    <div
      data-testid="strategy-mode-bar"
      className="mb-4 rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] px-3 py-2"
    >
      <div className="flex flex-wrap items-baseline gap-2">
        <span className="text-[12px] font-medium">策略视图</span>
        <span className="text-[10px] text-[var(--k-muted)]">
          选中哪个策略，本页就显示对应组件；Live 恒为港湾
        </span>
      </div>
      <div className="mt-2 flex flex-wrap gap-1">
        {STRATEGY_MODES.map((s) => (
          <button
            key={s}
            type="button"
            aria-pressed={s === mode}
            onClick={() => setStrategyMode(s)}
            className={cn(
              'rounded border px-2 py-1 text-[11px]',
              s === mode
                ? 'border-sky-500/50 bg-sky-500/10 text-sky-800 dark:text-sky-200'
                : 'border-[var(--k-border)] text-[var(--k-muted)]',
            )}
          >
            {STRATEGY_MODE_LABELS[s]}
            <span className="ml-1.5 opacity-70">{STRATEGY_MODE_TAGS[s]}</span>
          </button>
        ))}
      </div>
    </div>
  );
}
