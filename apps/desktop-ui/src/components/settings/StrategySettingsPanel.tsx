'use client';

import * as React from 'react';

import {
  STRATEGY_MODE_LABELS,
  useStrategyMode,
  type StrategyMode,
} from '@/lib/strategy-settings';
import { cn } from '@/lib/utils';

const OPTIONS: { value: StrategyMode; title: string; desc: string }[] = [
  {
    value: 'twin_star',
    title: '双子星 (Twin-Star)',
    desc: '择强单轨核心 50% + S-gap 卫星 50%（R12 冻结默认）。今日操作卡显示卫星开闸/候选，14:20 推送调整提醒。',
  },
  {
    value: 'single_track',
    title: '单轨择强',
    desc: 'mom60+MA200+trail8 满仓切换口径。显示择强日对齐 / ETF 执行卡 / 第三资产袖横幅。',
  },
];

export function StrategySettingsPanel() {
  const [mode, setMode] = useStrategyMode();

  return (
    <div className="space-y-4">
      <div>
        <h3 className="text-sm font-semibold">默认策略</h3>
        <p className="mt-1 text-xs text-[var(--k-muted)]">
          决定 Watchlist 今日操作区展示哪套组件（隐藏另一套）。存储在本机浏览器。
        </p>
      </div>
      <div className="space-y-2">
        {OPTIONS.map((opt) => {
          const active = mode === opt.value;
          return (
            <button
              key={opt.value}
              type="button"
              onClick={() => setMode(opt.value)}
              className={cn(
                'w-full rounded-lg border px-3 py-2.5 text-left transition-colors',
                active
                  ? 'border-sky-500/50 bg-sky-500/5'
                  : 'border-[var(--k-border)] bg-[var(--k-surface)] hover:border-[var(--k-border-strong)]',
              )}
            >
              <div className="flex items-center gap-2">
                <span
                  className={cn(
                    'h-3 w-3 shrink-0 rounded-full border',
                    active ? 'border-sky-500 bg-sky-500' : 'border-[var(--k-border-strong)]',
                  )}
                />
                <span className="text-[13px] font-medium">{opt.title}</span>
                {opt.value === 'twin_star' ? (
                  <span className="rounded bg-sky-500/10 px-1.5 py-0.5 text-[10px] text-sky-700 dark:text-sky-300">
                    推荐
                  </span>
                ) : null}
              </div>
              <div className="mt-1 pl-5 text-xs text-[var(--k-muted)]">{opt.desc}</div>
            </button>
          );
        })}
      </div>
      <p className="text-[11px] text-[var(--k-muted)]">
        当前：{STRATEGY_MODE_LABELS[mode]} · 切换后 Watchlist 立即生效，无需刷新。
      </p>
    </div>
  );
}