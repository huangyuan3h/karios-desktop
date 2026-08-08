'use client';

import * as React from 'react';

import { cn } from '@/lib/utils';

import { AlphaIncubatorPage } from './AlphaIncubatorPage';
import { ResearchPage } from './ResearchPage';

type AlphaTab = 'incubator' | 'research';

/**
 * Alpha 区域：Alpha Incubator（Alpha Radar）+ Research · 研报 α 合并为单 tab
 * 入口（2026-08-08 用户反馈：左侧 tab 太多）。两个子页保持挂载，切换用
 * display 隐藏，避免轮询/查询状态在切换时重置。
 */
export function AlphaTabsPage() {
  const [tab, setTab] = React.useState<AlphaTab>('incubator');

  return (
    <div className="flex flex-col">
      <div className="sticky top-0 z-10 border-b border-[var(--k-border)] bg-[var(--k-surface)]">
        <div className="mx-auto flex max-w-6xl flex-wrap items-center gap-2 px-4 pt-3 md:px-6">
          <button
            type="button"
            className={cn(
              'rounded-md px-3 py-1.5 text-sm font-medium transition-colors',
              tab === 'incubator'
                ? 'bg-[var(--k-surface-2)] text-[var(--k-text)]'
                : 'text-[var(--k-muted)] hover:text-[var(--k-text)]',
            )}
            onClick={() => setTab('incubator')}
            aria-pressed={tab === 'incubator'}
          >
            Alpha Incubator
          </button>
          <button
            type="button"
            className={cn(
              'rounded-md px-3 py-1.5 text-sm font-medium transition-colors',
              tab === 'research'
                ? 'bg-[var(--k-surface-2)] text-[var(--k-text)]'
                : 'text-[var(--k-muted)] hover:text-[var(--k-text)]',
            )}
            onClick={() => setTab('research')}
            aria-pressed={tab === 'research'}
          >
            Research · 研报 α
          </button>
          <span className="ml-auto hidden text-xs text-[var(--k-muted)] md:inline">
            S 级 / 高分信号在 Run Automation 时进入 Watchlist 监控池
          </span>
        </div>
      </div>

      <div className={tab === 'incubator' ? '' : 'hidden'}>
        <AlphaIncubatorPage />
      </div>
      <div className={tab === 'research' ? '' : 'hidden'}>
        <ResearchPage />
      </div>
    </div>
  );
}
