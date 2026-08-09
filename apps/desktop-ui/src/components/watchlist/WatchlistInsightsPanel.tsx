'use client';

import * as React from 'react';

import { Switch } from '@/components/ui/switch';

/**
 * 诊断面板容器：交易期望值看板 · Funnel History · Import debug table 三块
 * 放一起，用一个总开关统一显隐（默认收起——这些是复盘/调试信息，大部分
 * 时间不该占据视线）。内容保持挂载，切换不重置内部查询状态。
 */
export function WatchlistInsightsPanel({ children }: { children: React.ReactNode }) {
  const [open, setOpen] = React.useState(false);

  return (
    <section className="mb-4 min-w-0 rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] p-4">
      <div className="flex items-center justify-between gap-2">
        <div className="flex items-center gap-2">
          <div className="text-sm font-medium">诊断面板</div>
          <Switch
            checked={open}
            onCheckedChange={setOpen}
            aria-label="Toggle insights panel"
          />
        </div>
        <div className="text-[11px] text-[var(--k-muted)]">
          交易期望值 · 漏斗转化率 · 导入调试
        </div>
      </div>
      <div className={open ? 'mt-4 space-y-4' : 'hidden'}>
        {children}
      </div>
    </section>
  );
}
