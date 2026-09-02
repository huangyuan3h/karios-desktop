'use client';

import * as React from 'react';

import { cn } from '@/lib/utils';
import type { SatBlotterRow } from '@/lib/queries/backtest';

const KIND_LABEL: Record<string, string> = {
  fill: '到期成交',
  skip_t1: '涨停跳过',
  open: '持仓中',
};

function tone(v: number | null | undefined): string {
  if (v == null) return 'text-[var(--k-muted)]';
  return v >= 0 ? 'text-emerald-700 dark:text-emerald-300' : 'text-red-700 dark:text-red-400';
}

export function SatBlotterCard({
  rows,
}: {
  rows: SatBlotterRow[];
}) {
  const [kind, setKind] = React.useState<'all' | 'fill' | 'skip_t1' | 'open'>('all');
  const [showAll, setShowAll] = React.useState(false);
  const filtered = rows.filter((r) => (kind === 'all' ? true : r.kind === kind));
  const visible = showAll ? filtered : filtered.slice(-40);
  const skipN = rows.filter((r) => r.kind === 'skip_t1').length;
  const fillN = rows.filter((r) => r.kind === 'fill').length;
  const openN = rows.filter((r) => r.kind === 'open').length;

  if (!rows.length) {
    return (
      <p className="text-[10px] text-[var(--k-muted)]">本窗无卫星成交 / 涨停跳过记录</p>
    );
  }

  return (
    <div className="flex flex-col gap-1.5">
      <div className="flex flex-wrap items-center gap-1.5 text-[10px]">
        <span className="font-medium text-[var(--k-fg)]">卫星 blotter</span>
        {(['all', 'fill', 'skip_t1', 'open'] as const).map((k) => (
          <button
            key={k}
            type="button"
            onClick={() => setKind(k)}
            className={cn(
              'rounded border px-1.5 py-0.5',
              kind === k
                ? 'border-sky-500/50 bg-sky-500/10 text-sky-800 dark:text-sky-200'
                : 'border-[var(--k-border)] text-[var(--k-muted)]',
            )}
          >
            {k === 'all' ? `全部 ${rows.length}` : `${KIND_LABEL[k]} ${k === 'fill' ? fillN : k === 'skip_t1' ? skipN : openN}`}
          </button>
        ))}
        <span className="ml-auto text-[var(--k-muted)]">
          点开一笔看振幅名次 / 是否 skip_t1 / body 出日 / 卫星腿贡献 pt
        </span>
      </div>
      <div className="max-h-[220px] overflow-auto rounded border border-[var(--k-border)]">
        <table className="w-full text-left text-[11px] tabular-nums">
          <thead className="sticky top-0 bg-[var(--k-surface)] text-[10px] text-[var(--k-muted)]">
            <tr>
              <th className="py-1 pl-2 pr-2">日</th>
              <th className="py-1 pr-2">代码</th>
              <th className="py-1 pr-2">类型</th>
              <th className="py-1 pr-2">振幅名次</th>
              <th className="py-1 pr-2">skip_t1</th>
              <th className="py-1 pr-2">入/出</th>
              <th className="py-1 pr-2">body出日</th>
              <th className="py-1 pr-2">盈亏%</th>
              <th className="py-1 pr-2">贡献pt</th>
            </tr>
          </thead>
          <tbody>
            {visible.map((r, i) => (
              <tr key={`${r.kind}-${r.ts}-${r.date}-${i}`} className="border-t border-[var(--k-border)]/50">
                <td className="py-1 pl-2 pr-2 font-mono">{r.date}</td>
                <td className="py-1 pr-2 font-mono">{r.ts}</td>
                <td className="py-1 pr-2">{KIND_LABEL[r.kind] ?? r.kind}</td>
                <td className="py-1 pr-2">{r.ampRank ?? '—'}</td>
                <td className="py-1 pr-2">{r.skipT1 ? '是' : '—'}</td>
                <td className="py-1 pr-2 text-[10px] text-[var(--k-muted)]">
                  {r.entryDate ?? '—'} → {r.exitDate ?? (r.kind === 'open' ? '持仓' : '—')}
                </td>
                <td className="py-1 pr-2">{r.exitDue ?? '—'}</td>
                <td className={cn('py-1 pr-2', tone(r.pnlPct))}>
                  {r.pnlPct != null ? `${r.pnlPct >= 0 ? '+' : ''}${r.pnlPct.toFixed(1)}` : '—'}
                </td>
                <td className={cn('py-1 pr-2 font-medium', tone(r.contribPct))}>
                  {r.contribPct != null ? `${r.contribPct >= 0 ? '+' : ''}${r.contribPct.toFixed(2)}` : '—'}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {filtered.length > 40 ? (
        <button
          type="button"
          onClick={() => setShowAll((v) => !v)}
          className="self-end rounded border border-[var(--k-border)] px-2 py-0.5 text-[10px] text-[var(--k-muted)]"
        >
          {showAll ? '收起' : `全部 ${filtered.length} 笔`}
        </button>
      ) : null}
    </div>
  );
}
