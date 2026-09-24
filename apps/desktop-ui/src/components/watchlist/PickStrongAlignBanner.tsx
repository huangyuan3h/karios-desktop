'use client';

/**
 * Watchlist · daily align of live book vs today's harbor core recipe.
 */

import * as React from 'react';
import { useQuery } from '@tanstack/react-query';

import { fetchPortfolioHealth } from '@/lib/queries/portfolioHealth';
import { detectReplicaGaps, type GapSeverity, type HoldingSnap } from '@/lib/replica-gap';
import { STRATEGY_MODE_LABELS, type StrategyMode } from '@/lib/strategy-settings';
import { cn } from '@/lib/utils';

const SEV_CLS: Record<GapSeverity, string> = {
  block: 'border-red-500/40 bg-red-500/10 text-red-900 dark:text-red-100',
  warn: 'border-amber-500/40 bg-amber-500/10 text-amber-900 dark:text-amber-100',
  info: 'border-[var(--k-border)] bg-[var(--k-surface-2)]/50 text-[var(--k-muted)]',
};

const VERDICT_CLS = {
  aligned: 'text-emerald-700 dark:text-emerald-300',
  partial: 'text-amber-700 dark:text-amber-300',
  diverged: 'text-red-700 dark:text-red-300',
} as const;

/**
 * Always-visible daily align (complement to BehaviorAuditBanner / S-3).
 * Harbor keeps the S-3 core instruction (100% switch, idle cash parks in ETF).
 */
export function PickStrongAlignBanner({ mode = 'harbor' }: { mode?: StrategyMode } = {}) {
  const strategyName = STRATEGY_MODE_LABELS[mode];
  const healthQ = useQuery({
    queryKey: ['portfolio-health'],
    queryFn: ({ signal }) => fetchPortfolioHealth(undefined, signal),
    refetchInterval: 5 * 60_000,
  });

  const report = React.useMemo(() => {
    const data = healthQ.data;
    const pick = data?.multiAssetSleeve?.pick?.key ?? 'REPO';
    const cn = data?.holdings ?? [];
    const hk = data?.hkHealth?.holdings ?? [];
    const multi = data?.multiAssetHoldings ?? [];
    const holdings: HoldingSnap[] = [...cn, ...hk, ...multi].map((h) => ({
      symbol: h.symbol,
      positionPct: h.positionPct,
      name: h.name,
    }));
    return detectReplicaGaps({ pick, holdings });
  }, [healthQ.data]);

  const actionable = report.reasons.filter((r) => r.severity !== 'info');
  const infos = report.reasons.filter((r) => r.severity === 'info');
  // 加载中 vs 已对齐必须可区分：否则用户分不清"没事"还是"没加载完"。
  if (healthQ.isPending) {
    return (
      <div className="mb-4 rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] px-4 py-3 text-sm">
        <div className="text-[12px] font-medium">{strategyName}日对齐</div>
        <p className="mt-1 text-[11px] text-[var(--k-muted)]">对齐检查加载中…</p>
      </div>
    );
  }
  // Aligned days stay quiet but visible; the banner surfaces only when the
  // live book diverges from the core recipe (rendered above the strategy
  // decision card).
  if (report.verdict === 'aligned')
    return (
      <div className="mb-4 rounded-lg border border-emerald-500/30 bg-emerald-500/5 px-4 py-2 text-sm">
        <div className="flex flex-wrap items-center gap-2 text-[12px] font-medium">
          <span className={cn('text-[11px] font-semibold', VERDICT_CLS.aligned)}>✓ 已对齐</span>
          <span className="rounded border border-[var(--k-border)] bg-[var(--k-surface)] px-1.5 py-0.5 font-mono text-[10px]">
            pick={report.pick}
          </span>
          <span className="ml-auto font-mono text-[10px] font-normal text-[var(--k-muted)]">
            目标腿 {report.targetWeightPct}% · 股 {report.stockWeightPct}% · ETF{' '}
            {report.etfWeightPct}% · 闲置 {report.idlePct}%
          </span>
        </div>
      </div>
    );
  const verdictLabel = report.verdict === 'partial' ? '部分偏离' : `偏离${strategyName}`;

  return (
    <div
      className={cn(
        'mb-4 rounded-lg border px-4 py-3 text-sm',
        report.verdict === 'partial'
          ? 'border-amber-500/40 bg-amber-500/5'
          : 'border-red-500/40 bg-red-500/5',
      )}
    >
      <div className="flex flex-wrap items-center gap-2 text-[12px] font-medium">
        <span>{strategyName}日对齐</span>
        <span className="rounded border border-[var(--k-border)] bg-[var(--k-surface)] px-1.5 py-0.5 font-mono text-[10px]">
          pick={report.pick}
        </span>
        <span className={cn('text-[11px] font-semibold', VERDICT_CLS[report.verdict])}>
          {verdictLabel}
        </span>
        <span className="ml-auto font-mono text-[10px] font-normal text-[var(--k-muted)]">
          目标腿 {report.targetWeightPct}% · 股 {report.stockWeightPct}% · ETF{' '}
          {report.etfWeightPct}% · 闲置 {report.idlePct}%
        </span>
      </div>
      {mode !== 'harbor' ? (
        <p className="mt-1 text-[10px] text-[var(--k-muted)]">
          对齐针对港湾核心腿；
          {mode === 'homeport'
            ? 'B3 月频再平衡'
            : mode === 'twin_star'
              ? '卫星 1/2 overlay（港湾核心之外）'
              : mode === 'starship'
                ? '卫星 standalone + 闲钱停车'
                : mode === 'starship_robust'
                   ? '卫星 standalone + 闲钱 25% H2 ETF / 75% B3'

                  : mode === 'starship_b'
                    ? '卫星 standalone + 闲钱 100% {国债+黄金+纳指} 逆波动率'

                    : 'B3 月频 + 卫星 0.2 overlay'}
          未接入日对齐（OPT-186）。
        </p>
      ) : null}
      {healthQ.isFetching && !healthQ.data ? (
        <p className="mt-1 text-[11px] text-[var(--k-muted)]">加载持仓…</p>
      ) : null}
      {actionable.length === 0 ? (
        <p className="mt-1.5 text-[11px] text-[var(--k-muted)]">
          相对今日 pick 结构无 block/warn（信息级时点差异仍可能存在）。
        </p>
      ) : (
        <ul className="mt-2 space-y-1.5">
          {actionable.map((r) => (
            <li
              key={r.id}
              className={cn('rounded border px-2 py-1.5 text-[11px]', SEV_CLS[r.severity])}
            >
              <div className="font-semibold">
                {r.severity === 'block' ? '⛔' : '⚠'} {r.title}
              </div>
              <div className="mt-0.5 opacity-90">{r.detail}</div>
            </li>
          ))}
        </ul>
      )}
      {infos.length > 0 ? (
        <details className="mt-2 text-[10px] text-[var(--k-muted)]">
          <summary className="cursor-pointer">结构说明（{infos.length}）</summary>
          <ul className="mt-1 list-disc space-y-0.5 pl-4">
            {infos.map((r) => (
              <li key={r.id}>
                <span className="font-medium text-[var(--k-fg)]">{r.title}</span> — {r.detail}
              </li>
            ))}
          </ul>
        </details>
      ) : null}
    </div>
  );
}
