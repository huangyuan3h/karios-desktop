'use client';

import { GitCompareArrows } from 'lucide-react';

import { useBacktestReconQuery } from '@/lib/queries/backtest';

/**
 * 回测 vs Paper 对账摘要（2026-08-11）：每周一 cron 自动对账上周五——
 * S-3 回测"应持有" vs paper 实持。差异即矫正信号（缺票/多票/入场偏移），
 * 喂给周度复盘 / 决策 Agent，而不是等它悄悄漂移。
 */
export function BacktestReconCard() {
  const { data, isLoading } = useBacktestReconQuery(4);
  const items = data?.items ?? [];

  return (
    <div className="flex flex-col gap-1.5 rounded-lg border border-[var(--k-border)] bg-[var(--k-surface-2)]/60 p-2.5 text-xs">
      <div className="flex items-center gap-2 text-[11px] font-semibold">
        <GitCompareArrows size={12} className="text-[var(--k-muted)]" />
        回测 vs Paper 对账
        <span className="ml-auto text-[10px] font-normal text-[var(--k-muted)]">
          每周一自动对账上周五
        </span>
      </div>
      {isLoading && items.length === 0 ? (
        <div className="text-[var(--k-muted)]">加载中…</div>
      ) : items.length === 0 ? (
        <div className="text-[var(--k-muted)]">暂无对账快照（周一 07:30 cron 生成）</div>
      ) : (
        items.map((r) => {
          const clean = r.missing === 0 && r.extra === 0 && Math.abs(r.alignedReturnDiffPct ?? 0) < 2;
          const market = r.market === 'HK' ? '港股' : 'A股';
          const diff = r.alignedReturnDiffPct;
          return (
            <div
              key={`${r.reconDate}-${r.market}`}
              className="flex items-center gap-2 tabular-nums"
            >
              <span
                className={
                  clean
                    ? 'text-emerald-700 dark:text-emerald-300'
                    : 'text-amber-700 dark:text-amber-300'
                }
              >
                {clean ? '✓' : '⚠'}
              </span>
              <span className="text-[var(--k-muted)]">{r.reconDate}</span>
              <span className="font-medium">{market}</span>
              <span className="ml-auto">
                回测 {r.expected} · 实持 {r.actual} · 一致 {r.aligned}
              </span>
              <span
                className={
                  r.missing + r.extra > 0
                    ? 'text-amber-700 dark:text-amber-300'
                    : 'text-[var(--k-muted)]'
                }
              >
                缺 {r.missing} · 多 {r.extra}
              </span>
              {diff !== null && diff !== undefined && (
                <span
                  className={
                    Math.abs(diff) < 2
                      ? 'text-[var(--k-muted)]'
                      : 'text-amber-700 dark:text-amber-300'
                  }
                  title="收益偏差中位数：paper − 回测（一致票）"
                >
                  偏差 {diff > 0 ? '+' : ''}
                  {diff}pt
                </span>
              )}
            </div>
          );
        })
      )}
    </div>
  );
}
