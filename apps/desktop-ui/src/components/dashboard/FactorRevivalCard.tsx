'use client';

import * as React from 'react';
import { ArrowRight, FlaskConical, RefreshCw, Sprout } from 'lucide-react';

import { cn } from '@/lib/utils';
import { useFactorVaultQuery, useSgapDecayQuery } from '@/lib/queries/backtest';

function fmtPct(v: number | null | undefined, digits = 1): string {
  if (v == null || !Number.isFinite(v)) return '—';
  return `${v.toFixed(digits)}%`;
}

function fmtSigned(v: number | null | undefined, digits = 2): string {
  if (v == null || !Number.isFinite(v)) return '—';
  return `${v > 0 ? '+' : ''}${v.toFixed(digits)}`;
}

/**
 * Dashboard · 因子 / S-gap 复活雷达.
 *
 * Generic "active again" surface for every factor in the vault plus the
 * S-gap revival light. Reuses the existing `/factor-vault` + `/sgap-decay`
 * queries (frozen audit inputs); no new scoring, no weight changes.
 */
export function FactorRevivalCard({
  onOpenVault,
  onOpenSgap,
  className,
}: {
  onOpenVault?: () => void;
  onOpenSgap?: () => void;
  className?: string;
}) {
  const vaultQ = useFactorVaultQuery(true);
  const sgapQ = useSgapDecayQuery(true);
  const vault = vaultQ.data?.vault;
  const factors = React.useMemo(() => vault?.factors ?? [], [vault]);
  const summary = vault?.summary;
  const revived = React.useMemo(() => factors.filter((f) => f.status === 'revived'), [factors]);

  /** Latest date/window each factor flipped (or last sat) in the revived state. */
  const revivedAt = React.useMemo(() => {
    const out = new Map<string, string>();
    for (const f of factors) {
      const hit = [...f.history].reverse().find((p) => p.revived);
      if (hit) out.set(f.id, hit.date ?? hit.window);
    }
    return out;
  }, [factors]);

  const s = sgapQ.data?.decay.summary;
  const sgapPos = s?.revival_pos ?? 0;
  const sgapActive = sgapPos > 0;
  const sgapLabel = sgapPos >= 20 ? '半仓观察' : sgapPos >= 10 ? '试探加回' : '保持观察';
  const loading = vaultQ.isPending || sgapQ.isPending;

  return (
    <div
      data-testid="factor-revival-card"
      className={cn(
        'rounded-lg border border-[var(--k-border)] bg-[var(--k-surface-2)]/50 p-2.5 text-xs',
        className,
      )}
    >
      <div className="flex flex-wrap items-center gap-2">
        <Sprout size={13} className="text-emerald-600 dark:text-emerald-400" />
        <span className="font-semibold">因子 / S-gap 复活</span>
        <span
          data-testid="factor-revival-count"
          className={cn(
            'rounded border px-1.5 py-0.5 text-[10px] font-semibold',
            revived.length
              ? 'border-emerald-500/40 bg-emerald-500/10 text-emerald-700 dark:text-emerald-300'
              : 'border-[var(--k-border)] bg-[var(--k-surface)] text-[var(--k-muted)]',
          )}
        >
          {revived.length} 个复活
          {summary ? ` / ${summary.n_factors}` : ''}
        </span>
        <span className="ml-auto flex items-center gap-1 text-[10px] text-[var(--k-muted)]">
          <button
            type="button"
            onClick={() => {
              void vaultQ.refetch();
              void sgapQ.refetch();
            }}
            disabled={loading}
            className="rounded border border-[var(--k-border)] p-0.5 hover:text-[var(--k-fg)] disabled:opacity-50"
            title="刷新因子冷库 / S-gap"
          >
            <RefreshCw size={10} className={loading ? 'animate-spin' : ''} />
          </button>
        </span>
      </div>

      {/* S-gap revival light — always visible, even at 0. */}
      <div
        data-testid="sgap-revival"
        data-active={sgapActive ? 'true' : 'false'}
        className={cn(
          'mt-1.5 flex flex-wrap items-center gap-x-2 gap-y-0.5 rounded border px-2 py-1',
          sgapActive
            ? 'border-emerald-500/40 bg-emerald-500/10 text-emerald-800 dark:text-emerald-200'
            : 'border-[var(--k-border)] bg-[var(--k-surface)] text-[var(--k-muted)]',
        )}
      >
        <FlaskConical size={11} />
        <span className="font-semibold">S-gap 复活灯 {sgapPos}%</span>
        <span className="text-[10px]">（{sgapLabel}）</span>
        <span className="text-[10px] tabular-nums">
          滚动40笔分位 {fmtPct(s?.latest_r40_percentile)} · 均值 {fmtSigned(s?.latest_r40_mean)} ·
          胜率 {fmtPct(s?.latest_r40_win_rate != null ? s.latest_r40_win_rate * 100 : null)}
        </span>
        {onOpenSgap ? (
          <button
            type="button"
            onClick={onOpenSgap}
            className="ml-auto inline-flex items-center gap-0.5 rounded border border-[var(--k-border)] px-1 py-0.5 text-[10px] hover:border-emerald-500/50"
          >
            趋势 <ArrowRight size={9} />
          </button>
        ) : null}
      </div>

      {vaultQ.isError && sgapQ.isError ? (
        <p className="mt-1.5 text-[11px] text-red-600 dark:text-red-400">
          因子冷库 / S-gap 数据不可用（后端未生成报告）
        </p>
      ) : revived.length > 0 ? (
        <div className="mt-1.5 space-y-1">
          {revived.slice(0, 4).map((f) => (
            <div
              key={f.id}
              data-testid="revived-factor"
              className="flex flex-wrap items-center gap-x-2 gap-y-0.5 rounded border border-emerald-500/25 bg-emerald-500/5 px-1.5 py-0.5"
            >
              <span className="font-medium">{f.name}</span>
              <span className="rounded bg-emerald-500/15 px-1 py-0.5 text-[10px] font-semibold text-emerald-700 dark:text-emerald-300">
                {f.badge || '复活'}
              </span>
              <span className="text-[10px] text-[var(--k-muted)]">{f.family}</span>
              <span className="ml-auto text-[10px] tabular-nums">
                分位 {fmtPct(f.percentile)} · 净 {fmtSigned(f.net)} · 建议 {f.position}%
              </span>
              <span className="text-[10px] text-[var(--k-muted)]">
                {revivedAt.get(f.id) ?? '—'}
              </span>
            </div>
          ))}
          {revived.length > 4 ? (
            <div className="text-[10px] text-[var(--k-muted)]">
              另有 {revived.length - 4} 个复活因子
            </div>
          ) : null}
        </div>
      ) : (
        <p className="mt-1.5 text-[10px] text-[var(--k-muted)]">
          暂无复活因子
          {summary ? `（冷库 ${summary.n_cold} · 观察 ${summary.n_watch}）` : ''}
          ；S-gap {sgapActive ? '已重新亮灯' : '保持观察'}。
        </p>
      )}

      <div className="mt-1 flex items-center justify-between gap-2 text-[10px] text-[var(--k-muted)]">
        <span>复活 = 冷库体检信号，仅观察；不改 M30 / 星舰权重。</span>
        {onOpenVault ? (
          <button
            type="button"
            onClick={onOpenVault}
            className="inline-flex items-center gap-0.5 rounded border border-[var(--k-border)] px-1.5 py-0.5 hover:border-emerald-500/50 hover:text-emerald-700 dark:hover:text-emerald-300"
          >
            因子冷库 <ArrowRight size={10} />
          </button>
        ) : null}
      </div>
    </div>
  );
}
