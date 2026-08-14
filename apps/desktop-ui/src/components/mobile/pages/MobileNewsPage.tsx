'use client';

import * as React from 'react';

import { apiPatchJson } from '@/lib/api/client';
import { useNewsItemsQuery, invalidateNewsPageQueries } from '@/lib/queries/news';
import { useQueryClient } from '@tanstack/react-query';
import { MobileCard, MobileSection, StatusPill } from '../primitives';

/** News (mobile) — card flow, star important, open links. §5.2 中频. */
function fmtTime(iso: string | null): string {
  if (!iso) return '';
  const d = new Date(iso);
  const now = Date.now();
  const diff = now - d.getTime();
  if (diff < 60_000) return '刚刚';
  if (diff < 3_600_000) return `${Math.floor(diff / 60_000)} 分钟前`;
  if (diff < 86_400_000) return `${Math.floor(diff / 3_600_000)} 小时前`;
  return d.toLocaleDateString('zh-CN', { month: 'numeric', day: 'numeric' });
}

export function MobileNewsPage() {
  const qc = useQueryClient();
  const news = useNewsItemsQuery(24, 50);
  const [filterMode, setFilterMode] = React.useState<'all' | 'important'>('all');
  const [busyId, setBusyId] = React.useState<string | null>(null);

  const all = news.data?.items ?? [];
  const items = filterMode === 'important' ? all.filter((n) => n.isImportant) : all;

  const toggleImportant = async (id: string, important: boolean) => {
    setBusyId(id);
    try {
      await apiPatchJson(`/api/news/items/${id}/important`, { important });
      await invalidateNewsPageQueries(qc);
    } finally {
      setBusyId(null);
    }
  };

  return (
    <div className="space-y-4">
      <MobileSection
        title={filterMode === 'important' ? `重要新闻（${items.length}）` : `最新新闻（${items.length}）`}
        action={
          <div className="flex gap-2">
            <button
              type="button"
              onClick={() => setFilterMode('all')}
              className={filterMode === 'all' ? 'font-semibold text-[var(--k-accent)]' : 'text-[var(--k-muted)]'}
            >
              全部
            </button>
            <button
              type="button"
              onClick={() => setFilterMode('important')}
              className={filterMode === 'important' ? 'font-semibold text-[var(--k-accent)]' : 'text-[var(--k-muted)]'}
            >
              ⭐ 重要
            </button>
          </div>
        }
      >
        {items.length ? (
          <div className="space-y-2">
            {items.map((n) => (
              <MobileCard key={n.id} className="p-3">
                <div className="flex items-start justify-between gap-2">
                  <button
                    type="button"
                    onClick={() => void toggleImportant(n.id, !n.isImportant)}
                    disabled={busyId === n.id}
                    className="shrink-0 text-[var(--m-text-base)] disabled:opacity-50"
                    aria-label={n.isImportant ? '取消重要' : '标记重要'}
                  >
                    {n.isImportant ? '⭐' : '☆'}
                  </button>
                  <a
                    href={n.link || undefined}
                    target="_blank"
                    rel="noreferrer"
                    className="min-w-0 flex-1 text-[var(--m-text-base)] font-medium text-[var(--k-text)]"
                  >
                    <span className="line-clamp-2">{n.title}</span>
                  </a>
                  <span className="shrink-0 text-[var(--m-text-xs)] text-[var(--k-muted)]">{fmtTime(n.publishedAt)}</span>
                </div>
                {n.aiSummary ? (
                  <div className="mt-1.5 line-clamp-2 text-[var(--m-text-sm)] text-[var(--k-muted)]">{n.aiSummary}</div>
                ) : n.summary ? (
                  <div className="mt-1.5 line-clamp-2 text-[var(--m-text-sm)] text-[var(--k-muted)]">{n.summary}</div>
                ) : null}
                <div className="mt-2 flex flex-wrap gap-1.5">
                  {n.eventType ? <StatusPill tone="neutral">{n.eventType}</StatusPill> : null}
                  {n.actionability ? <StatusPill tone={n.actionability === 'actionable' ? 'open' : 'neutral'}>{n.actionability}</StatusPill> : null}
                  {n.importance != null && n.importance >= 1 ? (
                    <StatusPill tone="warn">I{n.importance}</StatusPill>
                  ) : null}
                </div>
              </MobileCard>
            ))}
          </div>
        ) : (
          <MobileCard className="px-3 py-8 text-center text-[var(--m-text-sm)] text-[var(--k-muted)]">
            {news.isLoading ? '加载中…' : '暂无新闻'}
          </MobileCard>
        )}
      </MobileSection>
    </div>
  );
}
