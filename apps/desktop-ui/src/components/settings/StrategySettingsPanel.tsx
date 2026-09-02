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
    title: '机会双子星（默认）',
    desc: '择强 trail8 核心 + strict S-gap 卫星。无仓 100% 核心；开闸可买或持仓中切 50%。卫星最多 4 只、每只总资产 12.5%。过去一年 +194.9 vs 单轨 +190.6（回撤同 12.6）。',
  },
  {
    value: 'single_track',
    title: '单轨择强',
    desc: 'mom60+MA200+trail8 满仓切换，无卫星。对照 / 回退模式，不是实盘默认。',
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
          const isDefault = opt.value === 'twin_star';
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
                {isDefault ? (
                  <span className="rounded bg-emerald-500/10 px-1.5 py-0.5 text-[10px] text-emerald-700 dark:text-emerald-300">
                    默认
                  </span>
                ) : (
                  <span className="rounded bg-[var(--k-surface)] px-1.5 py-0.5 text-[10px] text-[var(--k-muted)]">
                    对照
                  </span>
                )}
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
