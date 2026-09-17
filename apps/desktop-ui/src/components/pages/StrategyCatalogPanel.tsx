'use client';

import * as React from 'react';

import type { StrategyCatalogEntry, StrategyCatalogKey } from '@karios/shared';

import { useStrategyCatalogQuery } from '@/lib/queries/backtest';
import { cn } from '@/lib/utils';

const WINDOW_ROWS: {
  key: 'OOS2' | 'train' | 'valid' | 'long';
  label: string;
  role: string;
}[] = [
  { key: 'OOS2', label: 'OOS2', role: '第 1 段' },
  { key: 'train', label: 'train', role: '第 2 段' },
  { key: 'valid', label: 'valid', role: '第 3 段' },
  { key: 'long', label: 'long', role: '全周期 5 年' },
];

function fmtPct(v: number): string {
  return `${v > 0 ? '+' : ''}${v.toFixed(1)}%`;
}

function statusTone(status: StrategyCatalogEntry['status']): string {
  if (status === 'live') return 'bg-emerald-500/10 text-emerald-700 dark:text-emerald-300';
  if (status === 'rejected') return 'bg-red-500/10 text-red-700 dark:text-red-300';
  if (status === 'aggressive_pending') return 'bg-amber-500/10 text-amber-700 dark:text-amber-300';
  if (status === 'parallel_candidate')
    return 'bg-violet-500/10 text-violet-700 dark:text-violet-300';
  return 'bg-sky-500/10 text-sky-700 dark:text-sky-300';
}

function roleTone(role: StrategyCatalogEntry['role']): string {
  if (role === 'offense') return 'bg-red-500/10 text-red-700 dark:text-red-300';
  if (role === 'balanced') return 'bg-violet-500/10 text-violet-700 dark:text-violet-300';
  if (role === 'defense') return 'bg-sky-500/10 text-sky-700 dark:text-sky-300';
  return 'bg-emerald-500/10 text-emerald-700 dark:text-emerald-300';
}

export function StrategyCatalogPanel({
  selectedKey,
  onSelect,
}: {
  selectedKey?: string;
  onSelect?: (key: StrategyCatalogKey) => void;
} = {}) {
  const q = useStrategyCatalogQuery();
  const strategies = q.data?.strategies ?? [];
  const [innerKey, setInnerKey] = React.useState<string | null>(null);
  const key = selectedKey ?? innerKey;
  const selected = strategies.find((s) => s.key === key) ?? strategies[0];

  if (q.isLoading) {
    return (
      <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] p-4 text-xs text-[var(--k-muted)]">
        加载策略总览…
      </div>
    );
  }
  if (q.isError || !selected) {
    return (
      <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] p-4 text-xs">
        <p className="text-red-700">{q.isError ? String(q.error) : '无策略目录数据'}</p>
      </div>
    );
  }

  return (
    <div className="flex flex-col gap-3">
      <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] p-3">
        <div className="flex items-center gap-2 text-[12px] font-medium">
          策略族总览
          <span className="text-[10px] font-normal text-[var(--k-muted)]">
            2026-09-14 clean 口径（母港为 M30 防守档，2026-09-16）· 参考数字，不进 Live（Live 恒为港湾）
          </span>
        </div>
        <div className="mt-2 flex flex-wrap gap-1">
          {strategies.map((s) => (
            <button
              key={s.key}
              type="button"
              onClick={() => {
                setInnerKey(s.key);
                onSelect?.(s.key);
              }}
              className={cn(
                'rounded border px-2 py-1 text-[11px]',
                s.key === selected.key
                  ? 'border-sky-500/50 bg-sky-500/10 text-sky-800 dark:text-sky-200'
                  : 'border-[var(--k-border)] text-[var(--k-muted)]',
              )}
            >
              {s.name}
              <span className={cn('ml-1.5 rounded px-1 py-0.5 text-[9px]', roleTone(s.role))}>
                {s.roleLabel}
              </span>
              <span
                className={cn(
                  'ml-1.5 rounded px-1 py-0.5 text-[9px]',
                  statusTone(s.status),
                )}
              >
                {s.statusLabel}
              </span>
            </button>
          ))}
        </div>
      </div>

      <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] p-3">
        <div className="flex flex-wrap items-baseline gap-2">
          <span className="text-sm font-semibold">{selected.name}</span>
          <span className={cn('rounded px-1.5 py-0.5 text-[10px]', roleTone(selected.role))}>
            {selected.roleLabel}
          </span>
          <span className={cn('rounded px-1.5 py-0.5 text-[10px]', statusTone(selected.status))}>
            {selected.statusLabel}
          </span>
          <span className="text-[10px] text-[var(--k-muted)]">
            {selected.tag} · 更新 {selected.updated}
          </span>
        </div>
        <p className="mt-1 text-xs text-[var(--k-muted)]">{selected.structure}</p>

        <div className="mt-3 overflow-hidden rounded border border-[var(--k-border)]">
          <table className="w-full text-left text-xs tabular-nums">
            <thead className="bg-[var(--k-surface)] text-[10px] text-[var(--k-muted)]">
              <tr>
                <th className="py-1 pr-2 pl-2">窗口</th>
                <th className="py-1 pr-2">总收益</th>
                <th className="py-1 pr-2">年化</th>
                <th className="py-1 pr-2">最大回撤</th>
                <th className="py-1 pr-2">性价比</th>
              </tr>
            </thead>
            <tbody>
              {WINDOW_ROWS.map(({ key: wk, label, role }) => {
                const w = selected.windows[wk];
                return (
                  <tr key={wk} className="border-t border-[var(--k-border)]">
                    <td className="py-1 pr-2 pl-2">
                      <span className="font-medium">{label}</span>
                      <span className="ml-1 text-[9px] text-[var(--k-muted)]">{role}</span>
                    </td>
                    <td
                      className={cn(
                        'py-1 pr-2',
                        w.total >= 0 ? 'text-emerald-700 dark:text-emerald-300' : 'text-red-700',
                      )}
                    >
                      {fmtPct(w.total)}
                    </td>
                    <td className="py-1 pr-2">{fmtPct(w.cagr)}</td>
                    <td className="py-1 pr-2 text-red-700 dark:text-red-300">{fmtPct(w.mdd)}</td>
                    <td className="py-1 pr-2">{w.sharpe.toFixed(2)}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>

        <div className="mt-3 grid gap-3 md:grid-cols-2">
          <div className="rounded border border-emerald-500/30 bg-emerald-500/5 p-2">
            <div className="text-[11px] font-medium text-emerald-700 dark:text-emerald-300">优点</div>
            <ul className="mt-1 list-disc space-y-0.5 pl-4 text-xs text-[var(--k-muted)]">
              {selected.pros.map((p) => (
                <li key={p}>{p}</li>
              ))}
            </ul>
          </div>
          <div className="rounded border border-red-500/30 bg-red-500/5 p-2">
            <div className="text-[11px] font-medium text-red-700 dark:text-red-300">注意</div>
            <ul className="mt-1 list-disc space-y-0.5 pl-4 text-xs text-[var(--k-muted)]">
              {selected.cons.map((c) => (
                <li key={c}>{c}</li>
              ))}
            </ul>
          </div>
        </div>

        {selected.regime ? (
          <div className="mt-3 rounded border border-[var(--k-border)] bg-[var(--k-surface)] p-2">
            <div className="text-[11px] font-medium">什么行情下好用（只是记录，不是买卖开关）</div>
            <div className="mt-1 grid gap-2 md:grid-cols-2">
              <div>
                <div className="text-[10px] font-medium text-emerald-700 dark:text-emerald-300">
                  适合
                </div>
                <ul className="mt-0.5 list-disc space-y-0.5 pl-4 text-[11px] text-[var(--k-muted)]">
                  {selected.regime.fit.map((x) => (
                    <li key={x}>{x}</li>
                  ))}
                </ul>
              </div>
              <div>
                <div className="text-[10px] font-medium text-red-700 dark:text-red-300">
                  不适合 / 主要风险
                </div>
                <ul className="mt-0.5 list-disc space-y-0.5 pl-4 text-[11px] text-[var(--k-muted)]">
                  {selected.regime.unfit.map((x) => (
                    <li key={x}>{x}</li>
                  ))}
                </ul>
              </div>
            </div>
            {selected.regime.evidence.length ? (
              <table className="mt-2 w-full text-left text-[10px] tabular-nums">
                <tbody>
                  {selected.regime.evidence.map((e) => (
                    <tr key={`${e.label}-${e.value}`} className="border-t border-[var(--k-border)]/50">
                      <td className="w-[88px] py-0.5 pr-2 text-[var(--k-muted)]">{e.label}</td>
                      <td className="py-0.5">{e.value}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            ) : null}
            {selected.regime.note ? (
              <p className="mt-1 text-[10px] text-[var(--k-muted)]">{selected.regime.note}</p>
            ) : null}
          </div>
        ) : null}

        <p className="mt-3 text-[10px] text-[var(--k-muted)]">
          真值：{selected.doc}
          {selected.timelineStrategy
            ? ` · 可在「对比」页将 Timeline 切到「${selected.name}」查看逐日曲线`
            : ' · 已退役，无 Timeline 曲线'}
        </p>
      </div>
    </div>
  );
}
