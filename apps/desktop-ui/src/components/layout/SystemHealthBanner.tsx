'use client';

import * as React from 'react';

import { AlertTriangle, CheckCircle2, RefreshCw, XCircle } from 'lucide-react';

import { useQuery } from '@tanstack/react-query';

import { fetchSystemHealth, type DataSourceStatus } from '@/lib/queries/systemHealth';
import { cn } from '@/lib/utils';

function fmtAge(min: number | null): string {
  if (min == null) return '无记录';
  if (min < 60) return `${min} 分钟前`;
  if (min < 24 * 60) return `${Math.round(min / 60)} 小时前`;
  return `${Math.round(min / 60 / 24)} 天前`;
}

function fmtTime(iso: string | null): string {
  if (!iso) return '—';
  return new Date(iso).toLocaleString('zh-CN', {
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
  });
}

/**
 * Global self-check banner: any page shows backend/AI service outages,
 * stale data sources and recent sync-job failures. Renders nothing when
 * everything is healthy. Click to expand details.
 */
export function SystemHealthBanner() {
  const q = useQuery({
    queryKey: ['system-health'],
    queryFn: () => fetchSystemHealth(),
    refetchInterval: 5 * 60 * 1000,
    staleTime: 4 * 60 * 1000,
    retry: 1,
  });
  const [open, setOpen] = React.useState(false);

  const report = q.data;
  const total = (report?.errorCount ?? 0) + (report?.warnCount ?? 0);
  if (!report || total === 0) return null;

  const hasError = (report.errorCount ?? 0) > 0;
  const stale = (report.datasources ?? []).filter((s) => s.stale);
  const failures = report.failures ?? [];

  return (
    <div
      className={cn(
        'border-b px-4 py-2',
        hasError ? 'border-red-500/40 bg-red-500/10' : 'border-amber-500/40 bg-amber-500/10',
      )}
    >
      <button
        type="button"
        onClick={() => setOpen((v) => !v)}
        className="flex w-full items-center gap-2 text-left"
      >
        {hasError ? (
          <XCircle size={14} className="shrink-0 text-red-600 dark:text-red-400" />
        ) : (
          <AlertTriangle size={14} className="shrink-0 text-amber-600 dark:text-amber-400" />
        )}
        <span
          className={cn(
            'text-[12px] font-medium',
            hasError ? 'text-red-800 dark:text-red-200' : 'text-amber-800 dark:text-amber-200',
          )}
        >
          系统自检：{report.errorCount} 项异常 · {report.warnCount} 项告警
        </span>
        <span className="text-[11px] text-[var(--k-muted)]">{open ? '收起' : '查看明细'}</span>
        <RefreshCw
          size={12}
          className={cn('ml-auto text-[var(--k-muted)]', q.isFetching && 'animate-spin')}
          onClick={(e) => {
            e.stopPropagation();
            void q.refetch();
          }}
        />
      </button>

      {open && (
        <div className="mt-2 space-y-1.5 border-t border-red-500/20 pt-2 text-[11.5px]">
          {!report.dataSyncOnline && (
            <div className="text-red-700 dark:text-red-300">
              ✗ data-sync-service（后端）不可达 —— 数据与健康检查均不可用
            </div>
          )}
          {!report.aiOnline && (
            <div className="text-red-700 dark:text-red-300">
              ✗ ai-service（决策 Agent）不可达 —— 决策问答不可用
            </div>
          )}
          {stale.map((s: DataSourceStatus) => (
            <div key={s.source} className="flex items-center justify-between gap-2 text-amber-700 dark:text-amber-300">
              <span>△ {s.label} 数据陈旧（{fmtAge(s.ageMinutes)} ≥ 阈值 {Math.round(s.thresholdMinutes / 60)}h）</span>
              <span className="text-[10px] text-[var(--k-muted)]">最近同步 {fmtTime(s.lastSyncedAt)}</span>
            </div>
          ))}
          {failures.map((f) => (
            <div key={f.jobType} className="flex items-center justify-between gap-2 text-amber-700 dark:text-amber-300">
              <span className="min-w-0 truncate">
                △ 同步失败 {f.jobType} ×{f.failures24h ?? 1}（{fmtTime(f.syncedAt)}）
              </span>
              {f.errorMessage && (
                <span className="truncate text-[10px] text-[var(--k-muted)]">{f.errorMessage.slice(0, 80)}</span>
              )}
            </div>
          ))}
          <div className="flex items-center gap-1 text-[10.5px] text-[var(--k-muted)]">
            <CheckCircle2 size={11} />
            每 5 分钟自动检查 · 点刷新立即重检
          </div>
        </div>
      )}
    </div>
  );
}
