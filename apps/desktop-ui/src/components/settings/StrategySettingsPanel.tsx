'use client';

import {
  STRATEGY_MODE_DESCRIPTIONS,
  STRATEGY_MODE_LABELS,
  STRATEGY_MODE_TAGS,
  STRATEGY_MODES,
  setStrategyMode,
  useStrategyMode,
} from '@/lib/strategy-settings';
import { cn } from '@/lib/utils';

export function StrategySettingsPanel() {
  const mode = useStrategyMode();
  return (
    <div className="space-y-4">
      <div>
        <h3 className="text-sm font-semibold">默认策略</h3>
        <p className="mt-1 text-xs text-[var(--k-muted)]">
          只影响本机展示（Timeline 默认档），不改变实盘下单——Live 恒为港湾。存储在本机浏览器。
        </p>
      </div>
      <div className="space-y-2">
        {STRATEGY_MODES.map((s) => {
          const selected = s === mode;
          return (
            <button
              key={s}
              type="button"
              onClick={() => setStrategyMode(s)}
              className={cn(
                'w-full rounded-lg border px-3 py-2.5 text-left',
                selected
                  ? 'border-sky-500/50 bg-sky-500/5'
                  : 'border-[var(--k-border)] hover:border-sky-500/30',
              )}
            >
              <div className="flex items-center gap-2">
                <span
                  className={cn(
                    'h-3 w-3 shrink-0 rounded-full border',
                    selected ? 'border-sky-500 bg-sky-500' : 'border-[var(--k-muted)]',
                  )}
                />
                <span className="text-[13px] font-medium">{STRATEGY_MODE_LABELS[s]}</span>
                <span
                  className={cn(
                    'rounded px-1.5 py-0.5 text-[10px]',
                    s === 'harbor'
                      ? 'bg-emerald-500/10 text-emerald-700 dark:text-emerald-300'
                      : s === 'starport'
                        ? 'bg-sky-500/10 text-sky-700 dark:text-sky-300'
                        : 'bg-[var(--k-border)] text-[var(--k-muted)]',
                  )}
                >
                  {STRATEGY_MODE_TAGS[s]}
                </span>
              </div>
              <div className="mt-1 pl-5 text-xs text-[var(--k-muted)]">
                {STRATEGY_MODE_DESCRIPTIONS[s]}
              </div>
            </button>
          );
        })}
      </div>
      <p className="text-[11px] text-[var(--k-muted)]">当前：{STRATEGY_MODE_LABELS[mode]}</p>
    </div>
  );
}
