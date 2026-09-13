'use client';

import { STRATEGY_MODE_LABELS } from '@/lib/strategy-settings';

export function StrategySettingsPanel() {
  return (
    <div className="space-y-4">
      <div>
        <h3 className="text-sm font-semibold">默认策略</h3>
        <p className="mt-1 text-xs text-[var(--k-muted)]">
          港湾是当前唯一实盘基线。存储在本机浏览器，旧策略偏好会自动迁移。
        </p>
      </div>
      <div className="space-y-2">
        <div className="w-full rounded-lg border border-sky-500/50 bg-sky-500/5 px-3 py-2.5 text-left">
          <div className="flex items-center gap-2">
            <span className="h-3 w-3 shrink-0 rounded-full border border-sky-500 bg-sky-500" />
            <span className="text-[13px] font-medium">{STRATEGY_MODE_LABELS.harbor}</span>
            <span className="rounded bg-emerald-500/10 px-1.5 py-0.5 text-[10px] text-emerald-700 dark:text-emerald-300">
              默认
            </span>
          </div>
          <div className="mt-1 pl-5 text-xs text-[var(--k-muted)]">
            S-3 择强核心 + 闲置现金 ETF 停车场。核心按 mom60+MA200+trail8 满仓切换；无股票候选时现金停进核心 ETF。
          </div>
        </div>
      </div>
      <p className="text-[11px] text-[var(--k-muted)]">当前：{STRATEGY_MODE_LABELS.harbor}</p>
    </div>
  );
}
