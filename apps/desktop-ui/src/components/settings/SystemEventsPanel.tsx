'use client';

import * as React from 'react';

import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';

import { fetchSystemEvents, resolveSystemEvent, type SystemEvent } from '@/lib/queries/systemEvents';
import { cn } from '@/lib/utils';

function fmtTime(iso: string): string {
  return new Date(iso).toLocaleString('zh-CN', { month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false });
}

export function SystemEventsPanel() {
  const qc = useQueryClient();
  const [filter, setFilter] = React.useState<'all' | 'high' | 'low'>('all');
  const q = useQuery({
    queryKey: ['system-events', 100],
    queryFn: () => fetchSystemEvents(100),
    refetchInterval: 60_000,
  });

  const m = useMutation({
    mutationFn: (id: number) => resolveSystemEvent(id),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['system-events'] }),
  });

  const events = (q.data ?? []).filter((e) => (filter === 'all' ? true : e.severity === filter));

  return (
    <div className="rounded-xl border border-[var(--k-border)] bg-[var(--k-card)] p-4">
      <div className="mb-3 flex items-center justify-between gap-2">
        <div>
          <h3 className="text-sm font-semibold">系统收件箱</h3>
          <p className="text-[11px] text-[var(--k-muted)]">高级别推 Bark/站内，低级别仅落表 · 每周集中修复</p>
        </div>
        <div className="flex items-center gap-1">
          {(['all', 'high', 'low'] as const).map((v) => (
            <button
              key={v}
              type="button"
              onClick={() => setFilter(v)}
              className={cn('rounded-full px-2.5 py-1 text-xs', filter === v ? 'bg-zinc-900 text-white dark:bg-white dark:text-zinc-900' : 'border border-[var(--k-border)]')}
            >
              {v === 'all' ? '全部' : v === 'high' ? '高' : '低'}
            </button>
          ))}
          <button type="button" onClick={() => void q.refetch()} className="ml-1 rounded border px-2 py-1 text-xs">
            刷新
          </button>
        </div>
      </div>

      {q.isLoading ? (
        <div className="py-8 text-center text-sm text-[var(--k-muted)]">加载中…</div>
      ) : events.length === 0 ? (
        <div className="py-8 text-center text-sm text-[var(--k-muted)]">暂无未处理事件 — 系统干净 ✓</div>
      ) : (
        <div className="overflow-x-auto">
          <table className="w-full text-left text-xs">
            <thead className="text-[11px] text-[var(--k-muted)]">
              <tr>
                <th className="px-2 py-1.5">级别</th>
                <th className="px-2 py-1.5">时间</th>
                <th className="px-2 py-1.5">标题</th>
                <th className="px-2 py-1.5">详情</th>
                <th className="px-2 py-1.5" />
              </tr>
            </thead>
            <tbody>
              {events.map((ev: SystemEvent) => (
                <tr key={ev.id} className="border-t border-[var(--k-border)]">
                  <td className="px-2 py-2">
                    <span className={cn('rounded px-1.5 py-0.5 text-[11px]', ev.severity === 'high' ? 'bg-red-500/15 text-red-600' : 'bg-zinc-500/15 text-zinc-500')}>
                      {ev.severity}
                    </span>
                  </td>
                  <td className="whitespace-nowrap px-2 py-2 text-[11px] text-[var(--k-muted)]">{fmtTime(ev.createdAt)}</td>
                  <td className="max-w-[220px] truncate px-2 py-2 font-medium">{ev.title}</td>
                  <td className="max-w-[360px] truncate px-2 py-2 text-[var(--k-muted)]" title={ev.detail}>
                    {ev.detail || JSON.stringify(ev.payload).slice(0, 80)}
                  </td>
                  <td className="px-2 py-2">
                    <button
                      type="button"
                      disabled={m.isPending}
                      onClick={() => m.mutate(ev.id)}
                      className="rounded border px-2 py-1 text-[11px] hover:bg-zinc-500/10 disabled:opacity-50"
                    >
                      已修复
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
