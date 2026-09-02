'use client';

/**
 * Watchlist · daily align of live book vs today's recipe (core % + sat 4 slots).
 */

import * as React from 'react';
import { useQuery } from '@tanstack/react-query';

import { fetchPortfolioHealth } from '@/lib/queries/portfolioHealth';
import { useTwinStarActionQuery } from '@/lib/queries/backtest';
import {
  detectReplicaGaps,
  type GapSeverity,
  type HoldingSnap,
} from '@/lib/replica-gap';
import { useStrategyMode } from '@/lib/strategy-settings';
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
 * Twin-star uses clip4 core% + sat sleeve; single-track keeps 100% hard switch.
 */
export function PickStrongAlignBanner() {
  const [strategyMode] = useStrategyMode();
  const twinStar = strategyMode !== 'single_track';
  const healthQ = useQuery({
    queryKey: ['portfolio-health'],
    queryFn: ({ signal }) => fetchPortfolioHealth(undefined, signal),
    refetchInterval: 5 * 60_000,
  });
  const twinStarQ = useTwinStarActionQuery(twinStar);

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
    return detectReplicaGaps({
      pick,
      holdings,
      mode: twinStar ? 'twin_star' : 'single_track',
      coreTargetPct: twinStarQ.data?.sat?.coreTargetPct,
    });
  }, [healthQ.data, twinStar, twinStarQ.data?.sat?.coreTargetPct]);

  const actionable = report.reasons.filter((r) => r.severity !== 'info');
  const infos = report.reasons.filter((r) => r.severity === 'info');
  const verdictLabel =
    report.verdict === 'aligned'
      ? '对齐'
      : report.verdict === 'partial'
        ? '部分偏离'
        : twinStar
          ? '偏离目标结构'
          : '偏离硬切';

  return (
    <div
      className={cn(
        'mb-4 rounded-lg border px-4 py-3 text-sm',
        report.verdict === 'aligned'
          ? 'border-emerald-500/35 bg-emerald-500/5'
          : report.verdict === 'partial'
            ? 'border-amber-500/40 bg-amber-500/5'
            : 'border-red-500/40 bg-red-500/5',
      )}
    >
      <div className="flex flex-wrap items-center gap-2 text-[12px] font-medium">
        <span>{twinStar ? '机会双子星日对齐' : '择强日对齐'}</span>
        <span className="rounded border border-[var(--k-border)] bg-[var(--k-surface)] px-1.5 py-0.5 font-mono text-[10px]">
          pick={report.pick}
        </span>
        {twinStar ? (
          <span className="rounded border border-[var(--k-border)] bg-[var(--k-surface)] px-1.5 py-0.5 font-mono text-[10px]">
            核心 {report.coreTargetPct}%
          </span>
        ) : null}
        <span className={cn('text-[11px] font-semibold', VERDICT_CLS[report.verdict])}>
          {verdictLabel}
        </span>
        <span className="ml-auto font-mono text-[10px] font-normal text-[var(--k-muted)]">
          {twinStar ? '核心腿' : '目标腿'} {report.targetWeightPct}% · 股 {report.stockWeightPct}% · ETF{' '}
          {report.etfWeightPct}% · 闲置 {report.idlePct}%
        </span>
      </div>
      {healthQ.isLoading && !healthQ.data ? (
        <p className="mt-1 text-[11px] text-[var(--k-muted)]">加载持仓…</p>
      ) : null}
      {actionable.length === 0 ? (
        <p className="mt-1.5 text-[11px] text-[var(--k-muted)]">
          {twinStar
            ? '相对今日核心% + 卫星套筒无 block/warn（14:30 / body 残差仍可能存在）。'
            : '相对今日 pick 结构无 block/warn（信息级时点差异仍可能存在）。'}
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
