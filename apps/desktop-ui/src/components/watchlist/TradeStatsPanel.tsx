'use client';

import * as React from 'react';

import { useUserTradesListQuery, useUserTradesStatsQuery } from '@/lib/queries/userTrades';
import type { TradeBucketStats } from '@karios/shared';

function fmtPct(v: number | null | undefined, digits = 2): string {
  if (v == null || !Number.isFinite(v)) return '—';
  return `${v >= 0 ? '+' : ''}${v.toFixed(digits)}%`;
}

function fmtNum(v: number | null | undefined, digits = 2): string {
  if (v == null || !Number.isFinite(v)) return '—';
  return v.toFixed(digits);
}

function StatCard({
  label,
  value,
  tone,
  hint,
}: {
  label: string;
  value: string;
  tone?: 'green' | 'red' | 'neutral';
  hint?: string;
}) {
  const toneClass =
    tone === 'green'
      ? 'text-emerald-600'
      : tone === 'red'
        ? 'text-red-600'
        : 'text-[var(--k-text)]';
  return (
    <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface-2)] px-3 py-2">
      <div className="text-[11px] text-[var(--k-muted)]" title={hint}>
        {label}
      </div>
      <div className={`mt-0.5 font-mono text-lg font-semibold ${toneClass}`}>{value}</div>
    </div>
  );
}

function expectancyTone(v: number | null | undefined): 'green' | 'red' | 'neutral' {
  if (v == null || !Number.isFinite(v)) return 'neutral';
  return v >= 0 ? 'green' : 'red';
}

function SourceRow({ source, s }: { source: string; s: TradeBucketStats }) {
  return (
    <div className="flex items-center justify-between gap-3 text-xs">
      <span className="font-medium text-[var(--k-muted)]">{source}</span>
      <span className="text-[var(--k-muted)]">
        {s.count} 笔 · 胜率 {s.winRate != null ? `${(s.winRate * 100).toFixed(0)}%` : '—'}
      </span>
      <span
        className={`font-mono font-medium ${
          (s.netExpectancyPct ?? 0) >= 0 ? 'text-emerald-600' : 'text-red-600'
        }`}
      >
        {fmtPct(s.netExpectancyPct)}/笔
      </span>
    </div>
  );
}

export function TradeStatsPanel() {
  const statsQuery = useUserTradesStatsQuery();
  const listQuery = useUserTradesListQuery(8);
  const stats = statsQuery.data ?? null;
  const trades = listQuery.data ?? [];

  if (stats == null || stats.total === 0) {
    return (
      <section className="mb-4 min-w-0 rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] p-4">
        <div className="mb-2 text-sm font-medium">交易期望值看板</div>
        <div className="text-xs text-[var(--k-muted)]">
          还没有卖出记录。持仓标的点「卖出」输入卖出价格后，这里会显示真实交易的胜率与期望值。
        </div>
      </section>
    );
  }

  const sources = Object.entries(stats.bySource ?? {}).filter(([, s]) => s.count > 0);

  return (
    <section className="mb-4 min-w-0 rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] p-4">
      <div className="mb-3 flex items-center justify-between">
        <div className="text-sm font-medium">交易期望值看板</div>
        <div className="text-[11px] text-[var(--k-muted)]">
          {stats.total} 笔平仓 · 平均持有 {fmtNum(stats.avgHoldingDays, 0)} 天
        </div>
      </div>
      <div className="grid grid-cols-2 gap-2 md:grid-cols-5">
        <StatCard
          label="胜率"
          value={stats.winRate != null ? `${(stats.winRate * 100).toFixed(0)}%` : '—'}
          hint="pnlPct > 0 的平仓占比"
        />
        <StatCard
          label="平均盈利"
          value={fmtPct(stats.avgWinPct)}
          tone="green"
          hint="盈利平仓的平均收益"
        />
        <StatCard
          label="平均亏损"
          value={fmtPct(stats.avgLossPct)}
          tone="red"
          hint="亏损平仓的平均亏损（正值显示）"
        />
        <StatCard
          label="盈亏比"
          value={fmtNum(stats.profitFactor)}
          hint="总盈利 / 总亏损，>1 说明赚的比亏的多"
        />
        <StatCard
          label="每笔期望值（净）"
          value={fmtPct(stats.netExpectancyPct)}
          tone={expectancyTone(stats.netExpectancyPct)}
          hint={`胜率×平均盈利 − 败率×平均亏损 − ${stats.roundTripCostPct}% 交易成本。>0 才说明系统长期可盈利`}
        />
      </div>
      <div className="mt-1 text-[11px] text-[var(--k-muted)]">
        净期望值 = 胜率×平均盈利 − 败率×平均亏损 − {stats.roundTripCostPct}%
        往返成本（毛期望值 {fmtPct(stats.expectancyPct)}）。样本不足 50
        笔时仅作趋势参考，不要据此调参数。
      </div>
      {sources.length > 0 ? (
        <div className="mt-3 space-y-1.5 border-t border-[var(--k-border)] pt-3">
          <div className="text-[11px] font-medium text-[var(--k-muted)]">按来源（TIP-011 归因口径）</div>
          {sources.map(([source, s]) => (
            <SourceRow key={source} source={source} s={s} />
          ))}
        </div>
      ) : null}
      {trades.length > 0 ? (
        <div className="mt-3 border-t border-[var(--k-border)] pt-3">
          <div className="mb-1.5 text-[11px] font-medium text-[var(--k-muted)]">最近卖出</div>
          <div className="space-y-1">
            {trades
              .filter((t) => t.side === 'SELL')
              .slice(0, 6)
              .map((t) => (
                <div key={t.id} className="flex items-center justify-between gap-3 text-xs">
                  <span className="font-mono">{t.symbol}</span>
                  <span className="text-[var(--k-muted)]">
                    {t.tradeDate} · 卖 {fmtNum(t.price)} · {t.holdingDays ?? 0} 天
                  </span>
                  <span
                    className={`font-mono font-medium ${(t.pnlPct ?? 0) >= 0 ? 'text-emerald-600' : 'text-red-600'}`}
                  >
                    {fmtPct(t.pnlPct)}
                  </span>
                </div>
              ))}
          </div>
        </div>
      ) : null}
    </section>
  );
}
