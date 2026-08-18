'use client';

import React from 'react';

import { CalendarRange, Copy, RefreshCw, Sparkles } from 'lucide-react';

import { MarkdownMessage } from '@/components/chat/MarkdownMessage';
import { Button } from '@/components/ui/button';
import { useWeeklyReviewQuery } from '@/lib/queries/weekly-review';
import { generateWeeklyPlan, useWeeklyPlanQuery } from '@/lib/queries/weekly-plan';
import { cn } from '@/lib/utils';

export function WeeklyReviewCard() {
  const q = useWeeklyReviewQuery(true);
  const planQ = useWeeklyPlanQuery(true);
  const [copied, setCopied] = React.useState(false);
  const [planCopied, setPlanCopied] = React.useState(false);
  const [planBusy, setPlanBusy] = React.useState(false);
  const [planError, setPlanError] = React.useState<string | null>(null);
  const [plan, setPlan] = React.useState<string | null>(null);

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

  const storedPlan = plan ?? planQ.data?.plan?.markdown ?? null;
  const generatePlan = async () => {
    setPlanBusy(true);
    setPlanError(null);
    try {
      const out = await generateWeeklyPlan();
      if (!out.ok || !out.plan) {
        setPlanError(out.error ?? '生成失败');
        return;
      }
      setPlan(out.plan);
      void planQ.refetch();
    } catch (e) {
      setPlanError(e instanceof Error ? e.message : String(e));
    } finally {
      setPlanBusy(false);
    }
  };

  const copyPlan = () => {
    if (!storedPlan) return;
    void navigator.clipboard
      .writeText(storedPlan)
      .then(() => {
        setPlanCopied(true);
        setTimeout(() => setPlanCopied(false), 1500);
      })
      .catch((err) => {
        console.warn('copy plan failed:', err);
      });
  };

  return (
    <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] p-3">
      <div className="mb-2 flex items-center justify-between gap-2">
        <div className="flex items-center gap-2 text-[12px] font-medium">
          <CalendarRange className="size-3.5" />
          周度复盘（L3-P4 · 周一 07:40 自动生成）
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
        <div className="max-h-[24rem] overflow-auto pr-1">
          <MarkdownMessage content={q.data.markdown} className="prose-sm" />
        </div>
      ) : (
        <p className="text-xs text-[var(--k-muted)]">加载中…</p>
      )}

      <div className="mt-3 border-t border-[var(--k-border)] pt-2.5">
        <div className="mb-1.5 flex items-center gap-2">
          <span className="flex items-center gap-1 text-[12px] font-medium">
            <Sparkles className="size-3.5 text-[var(--k-accent)]" />
            下周行动计划（决策 Agent 自动产出 · 你只确认）
          </span>
          <span className="ml-auto flex gap-1.5">
            {storedPlan && (
              <Button variant="outline" size="sm" onClick={copyPlan}>
                <Copy size={12} className="mr-1" />
                {planCopied ? '已复制' : '复制计划'}
              </Button>
            )}
            <Button size="sm" onClick={() => void generatePlan()} disabled={planBusy}>
              {planBusy ? '生成中（约 1 分钟）…' : storedPlan ? '重新生成' : '生成计划'}
            </Button>
          </span>
        </div>
        {planError && <p className="mb-1 text-[11px] text-red-600">{planError}</p>}
        {planBusy ? (
          <div className="rounded bg-[var(--k-surface-1)]/60 p-3 text-[11px] text-[var(--k-muted)]">
            决策 Agent 正在读周报 + 实时体检 + 对账 + 滚动 OOS……（预取上下文，纯文本生成）
          </div>
        ) : storedPlan ? (
          <div
            className={cn(
              'max-h-[24rem] overflow-auto rounded bg-[var(--k-surface-1)]/60 p-2 pr-1',
            )}
          >
            <MarkdownMessage content={storedPlan} className="prose-sm" />
          </div>
        ) : (
          <p className="text-[11px] text-[var(--k-muted)]">
            暂无计划（周一 07:40 周报自动生成后可一键产出；也可以现在生成）
          </p>
        )}
      </div>
    </div>
  );
}
