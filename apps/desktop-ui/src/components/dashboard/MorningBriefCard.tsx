'use client';

import * as React from 'react';

import { Button } from '@/components/ui/button';
import { useMorningBriefQuery, type BriefCategory, type BriefItem } from '@/lib/queries/news';

const CATEGORY_META: Record<BriefCategory, { label: string; color: string; dot: string }> = {
  watchlist: {
    label: '持仓相关',
    color: 'text-emerald-700',
    dot: 'bg-emerald-500',
  },
  risk: {
    label: '风险提醒',
    color: 'text-red-700',
    dot: 'bg-red-500',
  },
  macro: {
    label: '板块/宏观',
    color: 'text-blue-700',
    dot: 'bg-blue-500',
  },
  sector: {
    label: '板块动态',
    color: 'text-amber-700',
    dot: 'bg-amber-500',
  },
};

const CATEGORY_ORDER: BriefCategory[] = ['watchlist', 'risk', 'sector', 'macro'];

function groupByCategory(items: BriefItem[]): Map<BriefCategory, BriefItem[]> {
  const grouped = new Map<BriefCategory, BriefItem[]>();
  for (const cat of CATEGORY_ORDER) {
    grouped.set(cat, []);
  }
  for (const item of items) {
    const cat = item.category || 'macro';
    const list = grouped.get(cat) || [];
    list.push(item);
    grouped.set(cat, list);
  }
  return grouped;
}

function BriefItemRow({ item }: { item: BriefItem }) {
  const meta = CATEGORY_META[item.category] || CATEGORY_META.macro;
  return (
    <div className="flex items-start gap-2 py-1">
      <span className={`mt-1.5 h-1.5 w-1.5 shrink-0 rounded-full ${meta.dot}`} />
      <div className="min-w-0 flex-1">
        <div className="text-sm leading-snug">{item.title}</div>
        {item.aiSummary ? (
          <div className="mt-0.5 text-xs text-[var(--k-muted)] line-clamp-1">{item.aiSummary}</div>
        ) : null}
        <div className="mt-0.5 flex items-center gap-2 text-[10px] text-[var(--k-muted)]">
          {item.tickers?.length ? (
            <span className="rounded bg-amber-500/10 px-1 py-0.5 font-mono text-amber-700">
              {item.tickers.slice(0, 3).join(', ')}
            </span>
          ) : null}
          {item.importance != null && item.importance >= 3 ? (
            <span className="rounded bg-red-500/10 px-1 py-0.5 text-red-700">
              I{item.importance}
            </span>
          ) : null}
          {item.relevanceScore != null && item.relevanceScore >= 50 ? (
            <span className="rounded bg-blue-500/10 px-1 py-0.5 text-blue-700">
              R{item.relevanceScore}
            </span>
          ) : null}
        </div>
      </div>
    </div>
  );
}

export function MorningBriefCard(props: {
  onNavigate?: (pageId: string) => void;
}) {
  const { onNavigate } = props;
  const briefQ = useMorningBriefQuery();
  const brief = briefQ.data?.brief;

  if (briefQ.isPending) {
    return (
      <div className="text-sm text-[var(--k-muted)]">
        Loading brief…
      </div>
    );
  }

  if (!brief) {
    return (
      <div className="text-sm text-[var(--k-muted)]">
        No brief yet. Briefs are generated at 08:30 and 12:30 on weekdays.
      </div>
    );
  }

  const grouped = groupByCategory(brief.items);
  const nonEmpty = CATEGORY_ORDER.filter((cat) => (grouped.get(cat) || []).length > 0);

  const briefTime = brief.createdAt
    ? new Date(brief.createdAt).toLocaleTimeString('zh-CN', {
        hour: '2-digit',
        minute: '2-digit',
        hour12: false,
      })
    : '';

  return (
    <div>
      <div className="mb-3 flex items-center justify-between text-xs text-[var(--k-muted)]">
        <span>
          {brief.briefType === 'morning' ? '早盘' : '午间'} · {brief.briefDate}
        </span>
        {briefTime ? <span className="font-mono">{briefTime}</span> : null}
      </div>

      <div className="space-y-3">
        {nonEmpty.map((cat) => {
          const meta = CATEGORY_META[cat];
          const items = grouped.get(cat) || [];
          return (
            <div key={cat}>
              <div className={`mb-1 flex items-center gap-1.5 text-xs font-medium ${meta.color}`}>
                <span className={`h-1.5 w-1.5 rounded-full ${meta.dot}`} />
                {meta.label}
                <span className="text-[var(--k-muted)]">({items.length})</span>
              </div>
              <div className="space-y-0.5 pl-3">
                {items.map((item) => (
                  <BriefItemRow key={item.id} item={item} />
                ))}
              </div>
            </div>
          );
        })}
      </div>

      <div className="mt-3 flex items-center gap-2">
        <Button size="sm" variant="secondary" onClick={() => onNavigate?.('news')}>
          Open News
        </Button>
      </div>
    </div>
  );
}
