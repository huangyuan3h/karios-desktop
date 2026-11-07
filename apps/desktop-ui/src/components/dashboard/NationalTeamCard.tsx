'use client';

import * as React from 'react';
import { ArrowRight, Landmark, RefreshCw } from 'lucide-react';

import { cn } from '@/lib/utils';
import {
  flowLatest,
  flowSpanWindow,
  flowTotalDaily20,
  fmtSigned,
  gateOnRuns,
} from '@/lib/fund-flow-series';
import { useFundFlowSeriesQuery } from '@/lib/queries/backtest';

/**
 * Dashboard · 国家队 / 资金确认.
 *
 * Context card — deliberately lives on Dashboard, NOT on the Watchlist
 * execution surface. Data wiring is the same `flow-series` query the Backtest
 * 资金流全景 uses (no new endpoint, no strategy input).
 */
export function NationalTeamCard({
  onOpenBacktest,
  className,
}: {
  onOpenBacktest?: () => void;
  className?: string;
}) {
  const win = React.useMemo(() => flowSpanWindow('1y'), []);
  const q = useFundFlowSeriesQuery(win.start, win.end);
  const rows = React.useMemo(() => q.data?.rows ?? [], [q.data]);
  const latest = flowLatest(rows);
  // Direct (cheap) derives — React Compiler can preserve the query memo but
  // not a second manual memo keyed on a freshly-built array.
  const runs = gateOnRuns(rows);
  const lastGateRun = runs.length ? runs[runs.length - 1] : null;
  const inGate = lastGateRun != null && lastGateRun.end === rows.length;

  const spark = (() => {
    const vals = rows
      .slice(-60)
      .map((r) => r.etfShareYi)
      .filter((v): v is number => typeof v === 'number' && Number.isFinite(v));
    if (vals.length < 2) return null;
    const min = Math.min(...vals);
    const max = Math.max(...vals);
    const span = max - min || 1;
    return vals
      .map((v, i) => {
        const x = (i / (vals.length - 1)) * 100;
        const y = 28 - ((v - min) / span) * 24;
        return `${i === 0 ? 'M' : 'L'}${x.toFixed(1)},${y.toFixed(1)}`;
      })
      .join(' ');
  })();

  const verdict = inGate
    ? { text: '国家队撤退 · B 闸 ON', tone: 'bad' as const }
    : latest?.etfShareD20Pct != null && latest.etfShareD20Pct > 0
      ? { text: '国家队净买', tone: 'ok' as const }
      : latest?.etfShareD20Pct != null && latest.etfShareD20Pct < 0
        ? { text: '国家队流出', tone: 'warn' as const }
        : { text: '中性', tone: 'muted' as const };

  const toneCls = {
    bad: 'border-red-500/40 bg-red-500/10 text-red-700 dark:text-red-300',
    ok: 'border-emerald-500/40 bg-emerald-500/10 text-emerald-700 dark:text-emerald-300',
    warn: 'border-amber-500/40 bg-amber-500/10 text-amber-700 dark:text-amber-300',
    muted: 'border-[var(--k-border)] bg-[var(--k-surface-2)] text-[var(--k-muted)]',
  } as const;

  return (
    <div
      data-testid="national-team-card"
      className={cn(
        'rounded-lg border border-[var(--k-border)] bg-[var(--k-surface-2)]/50 p-2.5 text-xs',
        className,
      )}
    >
      <div className="flex flex-wrap items-center gap-2">
        <Landmark size={13} className="text-sky-600 dark:text-sky-400" />
        <span className="font-semibold">国家队 · 宽基 ETF 份额</span>
        <span className="rounded border border-sky-500/30 bg-sky-500/10 px-1.5 py-0.5 text-[10px] font-medium text-sky-700 dark:text-sky-300">
          Dashboard 上下文
        </span>
        <span className="ml-auto flex items-center gap-1 text-[10px] text-[var(--k-muted)]">
          {latest?.date ?? '—'}
          <button
            type="button"
            onClick={() => void q.refetch()}
            disabled={q.isFetching}
            className="rounded border border-[var(--k-border)] p-0.5 text-[var(--k-muted)] hover:text-[var(--k-fg)] disabled:opacity-50"
            title="刷新国家队数据"
          >
            <RefreshCw size={10} className={q.isFetching ? 'animate-spin' : ''} />
          </button>
        </span>
      </div>

      {q.isError ? (
        <p className="mt-1.5 text-[11px] text-red-600 dark:text-red-400">
          资金流数据不可用（后端 flow-series 未就绪）
        </p>
      ) : (
        <>
          <div className="mt-1.5 flex flex-wrap items-center gap-2">
            <span className={cn('rounded border px-1.5 py-0.5 text-[11px] font-semibold', toneCls[verdict.tone])}>
              {verdict.text}
            </span>
            <span className="tabular-nums">
              份额 {latest?.etfShareYi != null ? `${latest.etfShareYi} 亿份` : '—'}
            </span>
            <span className="tabular-nums text-[var(--k-muted)]">
              Δ20 {fmtSigned(latest?.etfShareD20Pct, 1, '%')}
            </span>
            <span className="tabular-nums text-[var(--k-muted)]">
              国家队20日 {fmtSigned(latest?.natD20Yi, 0, ' 亿')}
            </span>
          </div>
          <div className="mt-1 flex flex-wrap items-center gap-2 text-[10px] text-[var(--k-muted)]">
            <span>三路20日 {fmtSigned(latest ? flowTotalDaily20(latest) : null, 0, ' 亿')}</span>
            <span>两融 {latest?.marginTrillion != null ? `${latest.marginTrillion} 万亿` : '—'}</span>
            <span>北向20日 {fmtSigned(latest?.northD20Yi, 0, ' 亿')}</span>
          </div>
          {spark ? (
            <svg
              viewBox="0 0 100 30"
              className="mt-1 h-[30px] w-full"
              role="img"
              aria-label="宽基 ETF 份额 60 日走势"
            >
              <path d={spark} fill="none" stroke="#0ea5e9" strokeWidth="1.4" />
            </svg>
          ) : null}
          <div className="mt-1 flex items-center justify-between gap-2 text-[10px] text-[var(--k-muted)]">
            <span>只做背景，不进入执行/权重；B 闸仅展示。</span>
            {onOpenBacktest ? (
              <button
                type="button"
                onClick={onOpenBacktest}
                className="inline-flex items-center gap-0.5 rounded border border-[var(--k-border)] px-1.5 py-0.5 text-[10px] hover:border-sky-500/50 hover:text-sky-700 dark:hover:text-sky-300"
              >
                看全图（回测 → 资金流） <ArrowRight size={10} />
              </button>
            ) : null}
          </div>
        </>
      )}
    </div>
  );
}
