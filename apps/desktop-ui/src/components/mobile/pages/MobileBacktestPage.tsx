'use client';

import * as React from 'react';

import {
  useBacktestRunQuery,
  GATE_LEVELS,
  type BacktestParams,
  type BacktestSummary,
} from '@/lib/queries/backtest';
import { MobileButton, MobileCard, MobileField, MobileSection, StatusPill } from '../primitives';

/** 回测 (mobile) — minimal params + key metrics. §5.2 高频. */

function defaultParams(): BacktestParams {
  const end = new Date();
  const start = new Date();
  start.setFullYear(end.getFullYear() - 1);
  const iso = (d: Date) => d.toISOString().slice(0, 10);
  return {
    start: iso(start),
    end: iso(end),
    scoreThreshold: 70,
    maxHoldDays: 20,
    stopLossPct: 6,
    gates: 'full',
    trailingStopPct: 5,
    positionPct: 5,
    maxPositions: 10,
    rsRankMin: 50,
    divergingScale: 0.5,
    targetPnlPct: 8,
    scoreFloor: 60,
    panicCooldownDays: 30,
    slippagePct: 0.3,
    excludeBoards: '',
  };
}

function Metric({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-[var(--m-radius-sm)] bg-[var(--k-surface-2)] px-2 py-1.5">
      <div className="text-[var(--m-text-xs)] text-[var(--k-muted)]">{label}</div>
      <div className="font-mono text-[var(--m-text-base)] tabular-nums">{value}</div>
    </div>
  );
}

export function MobileBacktestPage() {
  const [params, setParams] = React.useState<BacktestParams>(defaultParams);
  const [submitted, setSubmitted] = React.useState<BacktestParams | null>(null);
  const [attempt, setAttempt] = React.useState(0);

  const run = useBacktestRunQuery(submitted ?? defaultParams(), attempt);

  const set = <K extends keyof BacktestParams>(k: K, v: BacktestParams[K]) => {
    setParams((p) => ({ ...p, [k]: v }));
  };

  const summary: BacktestSummary | null = run.data?.summary ?? null;

  return (
    <div className="space-y-4">
      <MobileSection title="参数">
        <MobileCard className="space-y-2.5 p-3">
          <MobileField label="起止日期（YYYY-MM-DD）">
            <div className="flex gap-2">
              <input
                value={params.start}
                onChange={(e) => set('start', e.target.value)}
                className="h-10 min-w-0 flex-1 rounded-[var(--m-radius-md)] border border-[var(--k-border)] bg-[var(--k-surface-2)] px-2 font-mono text-[var(--m-text-sm)] outline-none focus:border-[var(--k-accent)]"
              />
              <input
                value={params.end}
                onChange={(e) => set('end', e.target.value)}
                className="h-10 min-w-0 flex-1 rounded-[var(--m-radius-md)] border border-[var(--k-border)] bg-[var(--k-surface-2)] px-2 font-mono text-[var(--m-text-sm)] outline-none focus:border-[var(--k-accent)]"
              />
            </div>
          </MobileField>
          <div className="grid grid-cols-2 gap-2">
            <MobileField label="评分阈值">
              <input
                type="number"
                value={params.scoreThreshold}
                onChange={(e) => set('scoreThreshold', Number(e.target.value))}
                className="h-10 w-full rounded-[var(--m-radius-md)] border border-[var(--k-border)] bg-[var(--k-surface-2)] px-2 font-mono text-[var(--m-text-sm)] outline-none focus:border-[var(--k-accent)]"
              />
            </MobileField>
            <MobileField label="最长持有天数">
              <input
                type="number"
                value={params.maxHoldDays}
                onChange={(e) => set('maxHoldDays', Number(e.target.value))}
                className="h-10 w-full rounded-[var(--m-radius-md)] border border-[var(--k-border)] bg-[var(--k-surface-2)] px-2 font-mono text-[var(--m-text-sm)] outline-none focus:border-[var(--k-accent)]"
              />
            </MobileField>
          </div>
          <MobileField label="止损 %">
            <input
              type="number"
              value={params.stopLossPct}
              onChange={(e) => set('stopLossPct', Number(e.target.value))}
              className="h-10 w-full rounded-[var(--m-radius-md)] border border-[var(--k-border)] bg-[var(--k-surface-2)] px-2 font-mono text-[var(--m-text-sm)] outline-none focus:border-[var(--k-accent)]"
            />
          </MobileField>
          <div className="flex flex-wrap gap-1.5">
            {GATE_LEVELS.map((g) => (
              <button
                key={g.value}
                type="button"
                onClick={() => set('gates', g.value)}
                className={`rounded-[var(--m-radius-pill)] px-2.5 py-1 text-[var(--m-text-xs)] font-medium ${
                  params.gates === g.value
                    ? 'bg-[var(--k-accent)] text-white'
                    : 'border border-[var(--k-border)] bg-[var(--k-surface-2)] text-[var(--k-muted)]'
                }`}
              >
                {g.label}
              </button>
            ))}
          </div>
          <MobileButton block onClick={() => { setSubmitted({ ...params }); setAttempt((a) => a + 1); }}>
            {run.isFetching ? '回测中…' : '运行回测'}
          </MobileButton>
        </MobileCard>
      </MobileSection>

      <MobileSection title="结果">
        {run.isFetching && !summary ? (
          <div className="space-y-2">
            <div className="m-shimmer h-20" />
            <div className="m-shimmer h-16" />
          </div>
        ) : summary ? (
          <div className="space-y-2">
            <MobileCard className="p-3">
              <div className="flex flex-wrap gap-1.5">
                <StatusPill tone={summary.total_net_pnl_pct >= 0 ? 'up' : 'down'}>
                  累计 {summary.total_net_pnl_pct >= 0 ? '+' : ''}
                  {summary.total_net_pnl_pct.toFixed(2)}%
                </StatusPill>
                <StatusPill tone="neutral">年化 {summary.annual_net_pnl_pct?.toFixed(2) ?? '—'}%</StatusPill>
                <StatusPill tone="neutral">超额 {summary.excess_vs_best_benchmark_pct?.toFixed(2) ?? '—'}%</StatusPill>
              </div>
            </MobileCard>
            <MobileCard className="grid grid-cols-3 gap-1.5 p-3">
              <Metric label="已平仓" value={String(summary.closed)} />
              <Metric label="胜率" value={summary.win_rate != null ? `${summary.win_rate.toFixed(1)}%` : '—'} />
              <Metric label="最大回撤" value={`${summary.max_drawdown_pct.toFixed(2)}%`} />
              <Metric label="均净盈亏" value={summary.avg_net_pnl_pct != null ? `${summary.avg_net_pnl_pct.toFixed(2)}%` : '—'} />
              <Metric label="夏普" value={summary.sharpe != null ? summary.sharpe.toFixed(2) : '—'} />
              <Metric label="基准" value={summary.best_benchmark} />
            </MobileCard>
          </div>
        ) : (
          <MobileCard className="px-3 py-8 text-center text-[var(--m-text-sm)] text-[var(--k-muted)]">
            设置参数后运行回测
          </MobileCard>
        )}
      </MobileSection>
    </div>
  );
}
