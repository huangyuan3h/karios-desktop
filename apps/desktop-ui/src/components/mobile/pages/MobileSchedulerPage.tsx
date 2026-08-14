'use client';

import * as React from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { SCHEDULER_JOB_CATALOG, type SchedulerJobStatus, type SchedulerJobMeta } from '@karios/shared';

import { useSchedulerJobsQuery, triggerSchedulerAction } from '@/lib/queries/scheduler';
import { MobileButton, MobileCard, MobileSection, StatusPill } from '../primitives';

/** 任务调度 (mobile) — job cards + manual trigger. §5.2 低频. */

function classifyJob(meta: SchedulerJobMeta, st: SchedulerJobStatus | undefined): { tone: 'open' | 'closed' | 'warn' | 'danger' | 'neutral'; label: string } {
  if (!st || !st.todayRun) {
    if (st?.lastSuccess) return { tone: 'neutral', label: '今日未运行' };
    if (!meta.tracked) return { tone: 'neutral', label: '未跟踪' };
    return { tone: 'closed', label: '未运行' };
  }
  if (st.todayRun.success) return { tone: 'open', label: '今日成功' };
  return { tone: 'danger', label: '今日失败' };
}

export function MobileSchedulerPage() {
  const qc = useQueryClient();
  const jobs = useSchedulerJobsQuery();
  const [running, setRunning] = React.useState<string | null>(null);
  const [msg, setMsg] = React.useState<{ job: string; text: string; ok: boolean } | null>(null);

  const metas = [...SCHEDULER_JOB_CATALOG].sort((a, b) => a.sortOrder - b.sortOrder);

  const trigger = async (meta: SchedulerJobMeta) => {
    if (!meta.action || running) return;
    setRunning(meta.jobType);
    setMsg(null);
    try {
      const body =
        meta.action.endpoint === '/alpha-radar/run-pipeline'
          ? { force: true }
          : meta.action.endpoint === '/watchlist/automation/run'
            ? { force: true }
            : undefined;
      const res = await triggerSchedulerAction(meta.action.endpoint, meta.action.method, body);
      if (res.error) {
        setMsg({ job: meta.jobType, text: res.error, ok: false });
      } else if (res.skipped) {
        setMsg({ job: meta.jobType, text: String(res.skipReason ?? res.message ?? '已跳过'), ok: false });
      } else if (res.updatedDailyRows != null) {
        setMsg({ job: meta.jobType, text: `完成 · 更新 ${res.updatedDailyRows} 行`, ok: true });
      } else {
        setMsg({ job: meta.jobType, text: String(res.message ?? '完成'), ok: true });
      }
      await qc.refetchQueries({ queryKey: ['scheduler', 'jobs'] });
    } catch (e) {
      setMsg({ job: meta.jobType, text: e instanceof Error ? e.message : String(e), ok: false });
    } finally {
      setRunning(null);
    }
  };

  return (
    <div className="space-y-4">
      <MobileSection
        title={`任务调度（${metas.length}）`}
        action={
          <button type="button" onClick={() => void qc.refetchQueries({ queryKey: ['scheduler', 'jobs'] })} className="text-[var(--m-text-sm)] text-[var(--k-accent)]">
            刷新
          </button>
        }
      >
        {msg ? (
          <MobileCard
            className={`px-3 py-2 text-[var(--m-text-sm)] ${msg.ok ? 'text-[var(--k-down)]' : 'text-[var(--k-danger)]'}`}
          >
            {msg.job} · {msg.text}
          </MobileCard>
        ) : null}

        {metas.length ? (
          <div className="space-y-2">
            {metas.map((meta) => {
              const st = jobs.data?.jobs[meta.jobType];
              const cls = classifyJob(meta, st);
              return (
                <MobileCard key={meta.jobType} className="p-3">
                  <div className="flex items-center justify-between gap-2">
                    <div className="min-w-0">
                      <div className="truncate text-[var(--m-text-base)] font-medium">{meta.titleCn}</div>
                      <div className="mt-0.5 truncate font-mono text-[var(--m-text-xs)] text-[var(--k-muted)]">
                        {meta.jobType}
                      </div>
                    </div>
                    <StatusPill tone={cls.tone}>{cls.label}</StatusPill>
                  </div>
                  <div className="mt-1.5 line-clamp-2 text-[var(--m-text-xs)] text-[var(--k-muted)]">
                    {meta.scheduleCn} · {meta.descriptionCn}
                  </div>
                  <div className="mt-2 flex items-center justify-between gap-2">
                    <div className="text-[var(--m-text-xs)] text-[var(--k-muted)]">
                      上次成功 {st?.lastSuccess ? new Date(st.lastSuccess.sync_at).toLocaleString('zh-CN') : '—'}
                    </div>
                    {meta.action ? (
                      <MobileButton size="sm" variant="ghost" onClick={() => void trigger(meta)} disabled={running != null}>
                        {running === meta.jobType ? '执行中…' : meta.action.label}
                      </MobileButton>
                    ) : null}
                  </div>
                  {st?.todayRun?.error_message ? (
                    <div className="mt-1.5 text-[var(--m-text-xs)] text-[var(--k-danger)]">{st.todayRun.error_message}</div>
                  ) : null}
                </MobileCard>
              );
            })}
          </div>
        ) : (
          <MobileCard className="px-3 py-8 text-center text-[var(--m-text-sm)] text-[var(--k-muted)]">暂无任务</MobileCard>
        )}
      </MobileSection>
    </div>
  );
}
