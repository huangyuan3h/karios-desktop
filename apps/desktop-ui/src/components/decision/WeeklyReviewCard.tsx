'use client';

import React from 'react';

import { CalendarRange, Copy, RefreshCw } from 'lucide-react';

import { MarkdownMessage } from '@/components/chat/MarkdownMessage';
import { Button } from '@/components/ui/button';
import { useWeeklyReviewQuery } from '@/lib/queries/weekly-review';

export function WeeklyReviewCard() {
  const q = useWeeklyReviewQuery(true);
  const [copied, setCopied] = React.useState(false);

  const copyReport = () => {
    if (!q.data?.markdown) return;
    void navigator.clipboard
      .writeText(q.data.markdown)
      .then(() => {
        setCopied(true);
        setTimeout(() => setCopied(false), 1500);
      })
      .catch((err) => {
        console.warn('copy weekly review failed:', err);
      });
  };

  return (
    <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] p-3">
      <div className="mb-2 flex items-center justify-between gap-2">
        <div className="flex items-center gap-2 text-[12px] font-medium">
          <CalendarRange className="size-3.5" />
          周度复盘（L3-P4）
          {q.data && (
            <span className="text-[10px] font-normal text-[var(--k-muted)]">
              {q.data.week.start} ~ {q.data.week.end}
            </span>
          )}
          {q.isFetching && <span className="text-[10px] text-[var(--k-muted)]">聚合中…</span>}
        </div>
        <div className="flex gap-1.5">
          <Button
            variant="ghost"
            size="sm"
            onClick={() => void q.refetch()}
            disabled={q.isFetching}
            title="重新聚合"
          >
            <RefreshCw size={12} />
          </Button>
          <Button variant="outline" size="sm" onClick={copyReport} disabled={!q.data?.markdown}>
            <Copy size={12} className="mr-1" />
            {copied ? '已复制' : '复制报告'}
          </Button>
        </div>
      </div>

      {q.isError ? (
        <p className="text-xs text-red-700">{String(q.error)}</p>
      ) : q.data ? (
        <div className="max-h-[28rem] overflow-auto pr-1">
          <MarkdownMessage content={q.data.markdown} className="prose-sm" />
        </div>
      ) : (
        <p className="text-xs text-[var(--k-muted)]">加载中…</p>
      )}
    </div>
  );
}
