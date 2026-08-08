'use client';

import React from 'react';

import { RefreshCw, Database } from 'lucide-react';

import { useQueryClient } from '@tanstack/react-query';

import { Button } from '@/components/ui/button';
import { cn } from '@/lib/utils';

import {
  invalidateResearchQueries,
  triggerResearchSync,
  useResearchReportsQuery,
  useResearchStatsQuery,
  type ResearchReport,
} from '@/lib/queries/research';

const RATING_STYLE: Record<string, string> = {
  买入: 'border-emerald-600/40 bg-emerald-500/10 text-emerald-700',
  增持: 'border-sky-600/40 bg-sky-500/10 text-sky-700',
  中性: 'border-zinc-500/40 bg-zinc-500/10 text-zinc-600',
  减持: 'border-red-600/40 bg-red-500/10 text-red-700',
  卖出: 'border-red-700/40 bg-red-600/10 text-red-800',
};

function ratingBadge(rating: string | null): string {
  const key = String(rating || '').trim();
  return RATING_STYLE[key] ?? 'border-zinc-400/40 bg-zinc-400/10 text-zinc-500';
}

function scoreTone(score: number | null): string {
  if (score == null) return 'text-[var(--k-muted)]';
  if (score >= 80) return 'text-emerald-700';
  if (score >= 70) return 'text-sky-700';
  return 'text-[var(--k-muted)]';
}

function ReportCard({ report }: { report: ResearchReport }) {
  return (
    <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] p-3">
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0">
          <div className="flex flex-wrap items-center gap-x-2 gap-y-1">
            <span className="font-mono text-sm font-semibold">{report.stockCode}</span>
            <span className="text-sm">{report.stockName}</span>
            <span
              className={cn(
                'rounded border px-1.5 py-0.5 text-[11px] font-medium',
                ratingBadge(report.rating),
              )}
            >
              {report.rating || '—'}
            </span>
            <span className="text-xs text-[var(--k-muted)]">{report.orgName}</span>
          </div>
          <div className="mt-1 line-clamp-2 text-[13px] text-[var(--k-text)]" title={report.title}>
            {report.title}
          </div>
          <div className="mt-1.5 flex flex-wrap items-center gap-x-3 gap-y-1 text-xs text-[var(--k-muted)]">
            {report.industryName ? <span>{report.industryName}</span> : null}
            {report.targetPrice != null ? (
              <span>
                目标价 <span className="font-mono">{report.targetPrice.toFixed(2)}</span>
              </span>
            ) : null}
            {report.epsThisYear != null ? (
              <span>
                EPS <span className="font-mono">{report.epsThisYear.toFixed(2)}</span>
              </span>
            ) : null}
            {report.peThisYear != null ? (
              <span>
                PE <span className="font-mono">{report.peThisYear.toFixed(1)}</span>
              </span>
            ) : null}
            <span>{report.publishDate}</span>
          </div>
        </div>
        <div className="shrink-0 text-right">
          <div className={cn('font-mono text-lg font-semibold', scoreTone(report.alphaScore))}>
            {report.alphaScore != null ? report.alphaScore.toFixed(0) : '—'}
          </div>
          <div className="text-[10px] text-[var(--k-muted)]">alpha</div>
        </div>
      </div>
    </div>
  );
}

export function ResearchPage() {
  const queryClient = useQueryClient();
  const [days, setDays] = React.useState(7);
  const [showHighScore, setShowHighScore] = React.useState(true);
  const [busy, setBusy] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);

  const reportsQuery = useResearchReportsQuery(days, 100);
  const statsQuery = useResearchStatsQuery();

  async function syncNow() {
    setBusy(true);
    setError(null);
    try {
      await triggerResearchSync(3);
      await invalidateResearchQueries(queryClient);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }

  const reports = reportsQuery.data?.reports ?? [];
  const filtered = showHighScore
    ? reports.filter((r) => r.alphaScore != null && r.alphaScore >= 70)
    : reports;
  const stats = statsQuery.data?.stats;

  return (
    <div className="mx-auto w-full max-w-4xl px-6 pb-6 pt-3">
      <div className="mb-6 flex flex-wrap items-start justify-between gap-3">
        <div>
          <div className="text-lg font-semibold">Research · 研报 α</div>
          <div className="mt-1 text-sm text-[var(--k-muted)]">
            东方财富个股研报（评级 / 目标价 / EPS），alpha 评分 ≥70 可进 Watchlist 监控池。
          </div>
        </div>
        <div className="flex items-center gap-2">
          <Button
            variant="ghost"
            size="sm"
            onClick={syncNow}
            disabled={busy}
            title="手动触发东财研报同步（最近 3 天）"
          >
            <RefreshCw className={cn('mr-1 h-3.5 w-3.5', busy && 'animate-spin')} />
            {busy ? '同步中…' : '同步'}
          </Button>
          <Button
            variant="ghost"
            size="sm"
            onClick={() => setShowHighScore((v) => !v)}
            title={showHighScore ? '显示全部研报' : '仅显示 alpha ≥70（可进池）'}
          >
            <Database className="mr-1 h-3.5 w-3.5" />
            {showHighScore ? '高分筛选开' : '高分筛选关'}
          </Button>
        </div>
      </div>

      {stats ? (
        <div className="mb-4 flex flex-wrap gap-2 text-xs text-[var(--k-muted)]">
          <span className="rounded border border-[var(--k-border)] bg-[var(--k-surface)] px-2 py-1">
            累计 {stats.total} 份
          </span>
          <span className="rounded border border-[var(--k-border)] bg-[var(--k-surface)] px-2 py-1">
            近 24h {stats.last24h} 份
          </span>
          <span className="rounded border border-[var(--k-border)] bg-[var(--k-surface)] px-2 py-1">
            近 7 天 {stats.last7d} 份 · {stats.stocks7d} 只
          </span>
        </div>
      ) : null}

      {error ? (
        <div className="mb-4 rounded border border-red-500/40 bg-red-500/10 px-3 py-2 text-sm text-red-700">
          {error}
        </div>
      ) : null}

      {reportsQuery.isLoading ? (
        <div className="py-8 text-center text-sm text-[var(--k-muted)]">加载研报…</div>
      ) : reportsQuery.isError ? (
        <div className="py-8 text-center text-sm text-red-600">研报加载失败</div>
      ) : filtered.length === 0 ? (
        <div className="py-8 text-center text-sm text-[var(--k-muted)]">
          {showHighScore ? '暂无 alpha ≥70 的研报信号（24 小时自动同步）' : '暂无研报'}
        </div>
      ) : (
        <div className="flex flex-col gap-2">
          {filtered.map((r) => (
            <ReportCard key={r.id} report={r} />
          ))}
        </div>
      )}
    </div>
  );
}
