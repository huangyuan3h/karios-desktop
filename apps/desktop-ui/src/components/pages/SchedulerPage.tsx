'use client';

import * as React from 'react';
import {
  AlertTriangle,
  CheckCircle2,
  ChevronDown,
  ChevronRight,
  CircleDashed,
  Clock,
  Loader2,
  PlayCircle,
  RefreshCw,
  Sparkles,
} from 'lucide-react';
import {
  type HkIndustryCoverage,
  type SchedulerJobAction,
  type SchedulerJobMeta,
  type SchedulerJobStatus,
  type SchedulerJobsResponse,
  type SyncJobRecord,
  SCHEDULER_GROUP_META,
  SCHEDULER_JOB_CATALOG,
  groupSchedulerJobs,
} from '@karios/shared';

import { Button } from '@/components/ui/button';
import { Progress } from '@/components/ui/progress';
import {
  SCHEDULER_POLL_MS,
  triggerSchedulerAction,
  useSchedulerJobsQuery,
} from '@/lib/queries/scheduler';
import { cn } from '@/lib/utils';

type JobRunState = 'ok' | 'failed' | 'idle' | 'never' | 'untracked';

const STATUS_META: Record<
  JobRunState,
  { label: string; pill: string; dot: string; text: string; icon: React.ComponentType<{ className?: string }> }
> = {
  ok: {
    label: 'OK',
    pill: 'border-emerald-500/30 bg-emerald-500/10 text-emerald-700',
    dot: 'bg-emerald-500',
    text: 'text-emerald-700',
    icon: CheckCircle2,
  },
  failed: {
    label: '失败',
    pill: 'border-red-500/30 bg-red-500/10 text-red-700',
    dot: 'bg-red-500',
    text: 'text-red-700',
    icon: AlertTriangle,
  },
  idle: {
    label: '未运行',
    pill: 'border-[var(--k-border)] bg-[var(--k-surface-2)] text-[var(--k-muted)]',
    dot: 'bg-[var(--k-muted)]',
    text: 'text-[var(--k-muted)]',
    icon: CircleDashed,
  },
  never: {
    label: '从未运行',
    pill: 'border-[var(--k-border)] bg-[var(--k-surface-2)] text-[var(--k-muted)]',
    dot: 'bg-[var(--k-border)]',
    text: 'text-[var(--k-muted)]',
    icon: Clock,
  },
  untracked: {
    label: '未记录',
    pill: 'border-[var(--k-border)] bg-[var(--k-surface-2)] text-[var(--k-muted)]',
    dot: 'bg-[var(--k-border)]',
    text: 'text-[var(--k-muted)]',
    icon: CircleDashed,
  },
};

function classifyJob(
  tracked: boolean,
  todayRun: SyncJobRecord | null | undefined,
  lastSuccess: SyncJobRecord | null | undefined,
): JobRunState {
  if (!tracked) return 'untracked';
  if (todayRun) return todayRun.success ? 'ok' : 'failed';
  if (lastSuccess) return 'idle';
  return 'never';
}

function fmtWhen(iso: string | null | undefined): string {
  if (!iso) return '—';
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return String(iso);
  return d.toLocaleString('zh-CN', { hour12: false });
}

function fmtRelative(iso: string | null | undefined): string {
  if (!iso) return '—';
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return String(iso);
  const now = Date.now();
  const diff = Math.max(0, now - d.getTime());
  const min = Math.floor(diff / 60_000);
  if (min < 1) return '刚刚';
  if (min < 60) return `${min} 分钟前`;
  const hr = Math.floor(min / 60);
  if (hr < 24) return `${hr} 小时前`;
  const day = Math.floor(hr / 24);
  if (day < 30) return `${day} 天前`;
  return d.toLocaleDateString('zh-CN');
}

function statusCounts(jobsResponse: SchedulerJobsResponse | undefined): {
  total: number;
  ok: number;
  failed: number;
  never: number;
  elapsedToday: number;
} {
  const total = groupSchedulerJobs().reduce((sum, g) => sum + g.jobs.length, 0);
  if (!jobsResponse) {
    return { total, ok: 0, failed: 0, never: 0, elapsedToday: 0 };
  }
  let ok = 0;
  let failed = 0;
  let never = 0;
  let elapsedToday = 0;
  for (const meta of SCHEDULER_JOB_CATALOG) {
    const status = jobsResponse.jobs[meta.jobType];
    if (meta.tracked && status?.todayRun) {
      if (status.todayRun.success) ok += 1;
      else failed += 1;
      elapsedToday += 1;
    } else if (meta.tracked) {
      never += 1;
    }
  }
  return { total, ok, failed, never, elapsedToday };
}

function StatTile({
  label,
  value,
  hint,
  tone,
  icon: Icon,
}: {
  label: string;
  value: number | string;
  hint?: string;
  tone: 'ok' | 'failed' | 'idle' | 'neutral';
  icon: React.ComponentType<{ className?: string }>;
}) {
  const toneClass = {
    ok: 'text-emerald-700 bg-emerald-500/10',
    failed: 'text-red-700 bg-red-500/10',
    idle: 'text-[var(--k-muted)] bg-[var(--k-surface-2)]',
    neutral: 'text-[var(--k-fg)] bg-[var(--k-surface-2)]',
  }[tone];
  return (
    <div className="flex items-center gap-3 rounded-2xl border border-[var(--k-border)] bg-[var(--k-surface)] p-4 shadow-sm">
      <div className={cn('grid h-10 w-10 shrink-0 place-items-center rounded-xl', toneClass)}>
        <Icon className="h-5 w-5" />
      </div>
      <div className="min-w-0">
        <div className="text-xs text-[var(--k-muted)]">{label}</div>
        <div className="mt-0.5 text-2xl font-semibold tabular-nums tracking-tight">{value}</div>
        {hint ? <div className="text-[11px] text-[var(--k-muted)]">{hint}</div> : null}
      </div>
    </div>
  );
}

function StatusPill({ state }: { state: JobRunState }) {
  const meta = STATUS_META[state];
  const Icon = meta.icon;
  return (
    <span
      className={cn(
        'inline-flex items-center gap-1.5 rounded-full border px-2.5 py-0.5 text-xs font-medium',
        meta.pill,
      )}
    >
      <Icon className="h-3.5 w-3.5" />
      {meta.label}
    </span>
  );
}

function HkCoverageCard({ coverage }: { coverage: HkIndustryCoverage | null }) {
  if (!coverage) {
    return (
      <div className="rounded-xl border border-[var(--k-border)] bg-[var(--k-surface-2)] p-3 text-xs text-[var(--k-muted)]">
        港股行业覆盖率数据不可用
      </div>
    );
  }
  const pct = Number(coverage.coveragePct ?? 0);
  return (
    <div className="rounded-xl border border-[var(--k-border)] bg-[var(--k-surface-2)] p-3">
      <div className="flex items-center justify-between text-xs">
        <span className="text-[var(--k-muted)]">港股行业覆盖</span>
        <span className="font-mono tabular-nums">
          {coverage.mappedHk}/{coverage.totalHk}
        </span>
      </div>
      <div className="mt-2">
        <Progress value={pct} />
      </div>
      <div className="mt-1.5 flex items-center justify-between text-[11px] text-[var(--k-muted)]">
        <span>{pct.toFixed(1)}% 已映射</span>
        <span>缺失 {coverage.missingHk.toLocaleString('zh-CN')}</span>
      </div>
    </div>
  );
}

function JobCard({
  meta,
  status,
  onTrigger,
  triggering,
  extra,
}: {
  meta: SchedulerJobMeta;
  status: SchedulerJobStatus | null | undefined;
  onTrigger: (meta: SchedulerJobMeta) => void;
  triggering: boolean;
  extra?: React.ReactNode;
}) {
  const lastSuccess = status?.lastSuccess ?? null;
  const todayRun = status?.todayRun ?? null;
  const state = classifyJob(meta.tracked, todayRun, lastSuccess);

  return (
    <article
      className={cn(
        'flex flex-col rounded-2xl border p-4 shadow-sm transition-colors',
        'border-[var(--k-border)] bg-[var(--k-surface)]',
        state === 'failed' && 'border-red-500/40 bg-red-500/[0.03]',
        state === 'ok' && 'border-emerald-500/25',
      )}
    >
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0">
          <div className="flex items-center gap-2">
            <h3 className="truncate text-sm font-semibold tracking-tight text-[var(--k-fg)]">
              {meta.titleCn}
            </h3>
            <span className="shrink-0 rounded bg-[var(--k-surface-2)] px-1.5 py-0.5 font-mono text-[10px] text-[var(--k-muted)]">
              {meta.jobType}
            </span>
          </div>
          <div className="mt-1 flex items-center gap-1.5 text-[11px] text-[var(--k-muted)]">
            <Clock className="h-3 w-3" />
            <span>{meta.scheduleCn}</span>
          </div>
        </div>
        <StatusPill state={state} />
      </div>

      <p className="mt-2.5 line-clamp-2 text-xs leading-relaxed text-[var(--k-muted)]">
        {meta.descriptionCn}
      </p>

      <div className="mt-3 grid grid-cols-2 gap-2">
        <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface-2)] px-2.5 py-1.5">
          <div className="text-[10px] uppercase tracking-wide text-[var(--k-muted)]">今日运行</div>
          <div className="mt-0.5 font-mono text-[11px] text-[var(--k-fg)]">
            {todayRun ? fmtWhen(todayRun.sync_at) : state === 'untracked' ? '未记录' : '—'}
          </div>
          {todayRun?.error_message ? (
            <div className="mt-1 line-clamp-2 text-[10px] text-red-700" title={todayRun.error_message}>
              {todayRun.error_message}
            </div>
          ) : null}
        </div>
        <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface-2)] px-2.5 py-1.5">
          <div className="text-[10px] uppercase tracking-wide text-[var(--k-muted)]">上次成功</div>
          <div className="mt-0.5 font-mono text-[11px] text-[var(--k-fg)]">
            {lastSuccess ? fmtRelative(lastSuccess.sync_at) : '—'}
          </div>
          {lastSuccess ? (
            <div className="mt-0.5 text-[10px] text-[var(--k-muted)]">
              {fmtWhen(lastSuccess.sync_at)}
            </div>
          ) : null}
        </div>
      </div>

      {extra ? <div className="mt-3">{extra}</div> : null}

      {meta.action ? (
        <div className="mt-3 flex items-center justify-end">
          <Button
            size="sm"
            variant="secondary"
            disabled={triggering}
            onClick={() => onTrigger(meta)}
          >
            {triggering ? (
              <Loader2 className="h-3.5 w-3.5 animate-spin" />
            ) : (
              <PlayCircle className="h-3.5 w-3.5" />
            )}
            {meta.action.label}
          </Button>
        </div>
      ) : null}
    </article>
  );
}

function GroupSection({
  group,
  jobs,
  statusByType,
  onTrigger,
  triggeringJobType,
  hkCoverage,
  alphaRadarExtra,
  watchlistRun,
}: {
  group: SchedulerJobMeta['group'];
  jobs: SchedulerJobMeta[];
  statusByType: SchedulerJobsResponse['jobs'];
  onTrigger: (meta: SchedulerJobMeta) => void;
  triggeringJobType: string | null;
  hkCoverage: HkIndustryCoverage | null;
  alphaRadarExtra: SchedulerJobsResponse['alphaRadar'];
  watchlistRun: SchedulerJobsResponse['watchlistAutomation'];
}) {
  const [collapsed, setCollapsed] = React.useState(false);
  const groupLabel = SCHEDULER_GROUP_META[group];
  const okCount = jobs.filter((j) => {
    const s = statusByType[j.jobType];
    return classifyJob(j.tracked, s?.todayRun ?? null, s?.lastSuccess ?? null) === 'ok';
  }).length;
  const failedCount = jobs.filter((j) => {
    const s = statusByType[j.jobType];
    return classifyJob(j.tracked, s?.todayRun ?? null, s?.lastSuccess ?? null) === 'failed';
  }).length;

  return (
    <section className="rounded-2xl border border-[var(--k-border)] bg-[var(--k-surface)] p-4 shadow-sm">
      <button
        type="button"
        onClick={() => setCollapsed((c) => !c)}
        className="flex w-full items-center justify-between gap-2"
      >
        <div className="flex items-center gap-2">
          {collapsed ? (
            <ChevronRight className="h-4 w-4 text-[var(--k-muted)]" />
          ) : (
            <ChevronDown className="h-4 w-4 text-[var(--k-muted)]" />
          )}
          <h2 className="text-sm font-semibold tracking-tight">{groupLabel.titleCn}</h2>
          <span className="rounded-md bg-[var(--k-surface-2)] px-1.5 py-0.5 text-[11px] text-[var(--k-muted)]">
            {jobs.length}
          </span>
        </div>
        <div className="flex items-center gap-2">
          {okCount > 0 ? (
            <span className="inline-flex items-center gap-1 text-[11px] text-emerald-700">
              <span className="h-1.5 w-1.5 rounded-full bg-emerald-500" />
              {okCount}
            </span>
          ) : null}
          {failedCount > 0 ? (
            <span className="inline-flex items-center gap-1 text-[11px] text-red-700">
              <span className="h-1.5 w-1.5 rounded-full bg-red-500" />
              {failedCount}
            </span>
          ) : null}
          <span className="text-[11px] text-[var(--k-muted)]">{groupLabel.descriptionCn}</span>
        </div>
      </button>

      {!collapsed ? (
        <div className="mt-4 grid gap-3 md:grid-cols-2 xl:grid-cols-3">
          {jobs.map((meta) => (
            <JobCard
              key={meta.jobType}
              meta={meta}
              status={statusByType[meta.jobType] ?? null}
              onTrigger={onTrigger}
              triggering={triggeringJobType === meta.jobType}
              extra={
                meta.jobType === 'hk_industry_sync' ? (
                  <HkCoverageCard coverage={hkCoverage} />
                ) : meta.jobType === 'alpha_radar_pipeline' && alphaRadarExtra ? (
                  <AlphaRadarExtra alphaRadar={alphaRadarExtra} />
                ) : meta.jobType === 'watchlist_automation' && watchlistRun ? (
                  <WatchlistAutomationExtra run={watchlistRun} />
                ) : null
              }
            />
          ))}
        </div>
      ) : null}
    </section>
  );
}

function AlphaRadarExtra({ alphaRadar }: { alphaRadar: NonNullable<SchedulerJobsResponse['alphaRadar']> }) {
  const backlog = alphaRadar.rawBacklogCount ?? 0;
  const trends = alphaRadar.currentTrendCount ?? alphaRadar.lastTrendCount ?? 0;
  const accumulated = alphaRadar.accumulatedTrendCount ?? 0;
  const inCooldown = Boolean(alphaRadar.withinCooldown);
  const cooldownH = alphaRadar.cooldownHours ?? 12;
  return (
    <div className="rounded-xl border border-[var(--k-border)] bg-[var(--k-surface-2)] p-3">
      <div className="flex items-center justify-between text-xs">
        <span className="text-[var(--k-muted)]">情报状态</span>
        <span
          className={cn(
            'inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-[10px] font-medium',
            inCooldown
              ? 'border border-amber-500/30 bg-amber-500/10 text-amber-700'
              : 'border border-emerald-500/30 bg-emerald-500/10 text-emerald-700',
          )}
        >
          {inCooldown ? `冷却中 (${cooldownH}h)` : '可运行'}
        </span>
      </div>
      <div className="mt-2 grid grid-cols-3 gap-2 text-center">
        <div>
          <div className="text-base font-semibold tabular-nums">{backlog}</div>
          <div className="text-[10px] text-[var(--k-muted)]">待处理</div>
        </div>
        <div>
          <div className="text-base font-semibold tabular-nums">{trends}</div>
          <div className="text-[10px] text-[var(--k-muted)]">本批趋势</div>
        </div>
        <div>
          <div className="text-base font-semibold tabular-nums">{accumulated}</div>
          <div className="text-[10px] text-[var(--k-muted)]">累计趋势</div>
        </div>
      </div>
      {alphaRadar.lastIngestStats ? (
        <div className="mt-2 text-[10px] text-[var(--k-muted)]">
          上次抓取: stored={alphaRadar.lastIngestStats.stored ?? 0} ·
          filtered={alphaRadar.lastIngestStats.filteredOut ?? 0}
        </div>
      ) : null}
    </div>
  );
}

function WatchlistAutomationExtra({ run }: { run: NonNullable<SchedulerJobsResponse['watchlistAutomation']> }) {
  const created = run.createdAt ? fmtWhen(run.createdAt) : '—';
  const removed = Array.isArray(run.removeItems) ? run.removeItems.length : 0;
  const added = Array.isArray(run.alphaAdd) ? run.alphaAdd.length : 0;
  const skipped = run.skipped;
  return (
    <div className="rounded-xl border border-[var(--k-border)] bg-[var(--k-surface-2)] p-3">
      <div className="flex items-center justify-between text-xs">
        <span className="text-[var(--k-muted)]">最近一次运行</span>
        <span className="font-mono text-[11px]">{created}</span>
      </div>
      {skipped ? (
        <div className="mt-1 text-[11px] text-amber-700">已跳过: {run.skipReason ?? '—'}</div>
      ) : (
        <div className="mt-1 grid grid-cols-3 gap-2 text-center">
          <div>
            <div className="text-base font-semibold tabular-nums">{removed}</div>
            <div className="text-[10px] text-[var(--k-muted)]">剔除</div>
          </div>
          <div>
            <div className="text-base font-semibold tabular-nums">{added}</div>
            <div className="text-[10px] text-[var(--k-muted)]">Alpha 添加</div>
          </div>
        </div>
      )}
    </div>
  );
}

function SchedulerBanner({
  tone,
  text,
  action,
}: {
  tone: 'info' | 'error' | 'success';
  text: string;
  action?: React.ReactNode;
}) {
  const toneClass = {
    info: 'border-sky-500/30 bg-sky-500/10 text-sky-700',
    error: 'border-red-500/30 bg-red-500/10 text-red-700',
    success: 'border-emerald-500/30 bg-emerald-500/10 text-emerald-700',
  }[tone];
  return (
    <div className={cn('flex items-center justify-between gap-3 rounded-xl border px-3 py-2 text-sm', toneClass)}>
      <div className="min-w-0 flex-1">{text}</div>
      {action ? <div className="shrink-0">{action}</div> : null}
    </div>
  );
}

function friendlyCloseMessage(message: string | null | undefined): string | null {
  if (!message) return null;
  const m = String(message).toLowerCase();
  if (m.includes('too early') && m.includes('17:05')) {
    return '今日尚未收盘，收盘同步将在 17:05 后可用。';
  }
  if (m.includes('not a trading day')) {
    if (m.includes('already up to date')) return '今天不是交易日，数据已是最新，已跳过。';
    return '今天不是交易日，已跳过收盘同步。';
  }
  if (m.includes('non-trading day catchup')) return '非交易日补同步完成（已同步至最近一个交易日）。';
  if (m.includes('already synced today') || m.includes('already up to date')) return '今天已同步，无需重复操作。';
  if (m.includes('no trading dates in range')) return '交易日区间为空，请先同步交易日历。';
  return message;
}

export function SchedulerPage() {
  const queryClient = useSchedulerJobsQuery();
  const jobsData = queryClient.data;
  const isLoading = queryClient.isLoading;
  const isFetching = queryClient.isFetching;

  const [triggeringJobType, setTriggeringJobType] = React.useState<string | null>(null);
  const [feedback, setFeedback] = React.useState<{
    tone: 'info' | 'error' | 'success';
    text: string;
  } | null>(null);

  const groups = React.useMemo(() => groupSchedulerJobs(), []);
  const counts = React.useMemo(() => statusCounts(jobsData), [jobsData]);

  const triggerJob = React.useCallback(
    async (meta: SchedulerJobMeta) => {
      if (!meta.action) return;
      const action: SchedulerJobAction = meta.action;
      setTriggeringJobType(meta.jobType);
      setFeedback(null);
      try {
        const body = action.endpoint.startsWith('/sync/close')
          ? undefined
          : action.endpoint.includes('/alpha-radar/run-pipeline')
            ? { force: true }
            : undefined;
        const result = await triggerSchedulerAction(action.endpoint, action.method, body);
        if (result.ok === false) {
          setFeedback({
            tone: 'error',
            text: `「${meta.titleCn}」执行失败: ${result.error ?? '未知错误'}`,
          });
        } else if (result.skipped) {
          setFeedback({
            tone: 'info',
            text: `「${meta.titleCn}」已跳过：${friendlyCloseMessage(result.message) ?? result.message ?? '条件不满足'}`,
          });
        } else if (result.partial) {
          setFeedback({
            tone: 'info',
            text: '已同步到昨日，今日收盘后可再同步。',
          });
        } else if (action.endpoint.includes('/alpha-radar/run-pipeline')) {
          const stored = result.ingestStats?.stored ?? 0;
          const trends = result.trendCount ?? 0;
          setFeedback({
            tone: 'success',
            text: `Alpha Radar 主流程完成：stored=${stored} → ${trends} 张趋势卡片`,
          });
        } else if (typeof result.updated_daily_rows === 'number') {
          setFeedback({
            tone: 'success',
            text: `「${meta.titleCn}」完成：daily=${result.updated_daily_rows}, adj=${result.updated_adj_factor_rows ?? 0}`,
          });
        } else if (typeof result.updated === 'number') {
          setFeedback({
            tone: 'success',
            text: `「${meta.titleCn}」完成：updated=${result.updated}`,
          });
        } else {
          setFeedback({ tone: 'success', text: `「${meta.titleCn}」已触发` });
        }
      } catch (e) {
        setFeedback({
          tone: 'error',
          text: `「${meta.titleCn}」执行失败: ${e instanceof Error ? e.message : String(e)}`,
        });
      } finally {
        setTriggeringJobType(null);
        await queryClient.refetch();
      }
    },
    [queryClient],
  );

  const handleRefresh = React.useCallback(() => {
    void queryClient.refetch();
  }, [queryClient]);

  const statusByType = jobsData?.jobs ?? {};
  const hkCoverage = jobsData?.hkIndustryCoverage ?? null;
  const alphaRadar = jobsData?.alphaRadar ?? null;
  const watchlistRun = jobsData?.watchlistAutomation ?? null;

  const needTradeCal = Boolean(
    feedback?.tone === 'error' && /trade calendar missing/i.test(feedback.text),
  );

  const lastSyncAt = (() => {
    let latest: string | null = null;
    for (const meta of SCHEDULER_JOB_CATALOG) {
      const s = statusByType[meta.jobType];
      const at = s?.todayRun?.sync_at ?? s?.lastSuccess?.sync_at ?? null;
      if (at && (!latest || new Date(at) > new Date(latest))) latest = at;
    }
    return latest;
  })();

  return (
    <div className="mx-auto w-full max-w-7xl p-6">
      <div className="mb-6 flex items-start justify-between gap-3">
        <div>
          <div className="flex items-center gap-2">
            <Sparkles className="h-5 w-5 text-[var(--k-primary)]" />
            <h1 className="text-xl font-semibold tracking-tight">同步调度面板</h1>
            <span className="rounded-md bg-[var(--k-surface-2)] px-2 py-0.5 text-[11px] text-[var(--k-muted)]">
              共 {counts.total} 个任务
            </span>
          </div>
          <p className="mt-1 text-sm text-[var(--k-muted)]">
            管理所有自动同步脚本；调度器运行在 backend 进程内，如果服务停止或机器休眠，任务将不会执行。
          </p>
        </div>
        <div className="flex items-center gap-2">
          <span className="text-[11px] text-[var(--k-muted)]">
            {lastSyncAt ? `最近更新 ${fmtRelative(lastSyncAt)}` : '暂无记录'}
          </span>
          <Button
            variant="secondary"
            size="sm"
            onClick={handleRefresh}
            disabled={isFetching}
          >
            <RefreshCw className={cn('h-3.5 w-3.5', isFetching && 'animate-spin')} />
            刷新
          </Button>
        </div>
      </div>

      <div className="mb-4 grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
        <StatTile
          label="今日已运行"
          value={counts.elapsedToday}
          hint={`共 ${counts.total} 个任务`}
          tone="neutral"
          icon={Loader2}
        />
        <StatTile label="今日成功" value={counts.ok} tone="ok" icon={CheckCircle2} />
        <StatTile label="今日失败" value={counts.failed} tone="failed" icon={AlertTriangle} />
        <StatTile
          label="今日未运行"
          value={counts.never}
          tone="idle"
          icon={CircleDashed}
        />
      </div>

      <div className="mb-4 space-y-2">
        {feedback ? (
          <SchedulerBanner
            tone={feedback.tone}
            text={feedback.text}
            action={
              needTradeCal ? (
                <Button
                  size="sm"
                  variant="secondary"
                  disabled={triggeringJobType !== null}
                  onClick={async () => {
                    setTriggeringJobType('trade-cal');
                    setFeedback(null);
                    try {
                      await triggerSchedulerAction('/sync/trade-cal', 'POST');
                      setFeedback({ tone: 'success', text: '交易日历已同步，请重试收盘同步' });
                    } catch (e) {
                      setFeedback({
                        tone: 'error',
                        text: `交易日历同步失败: ${e instanceof Error ? e.message : String(e)}`,
                      });
                    } finally {
                      setTriggeringJobType(null);
                      await queryClient.refetch();
                    }
                  }}
                >
                  同步交易日历并重试
                </Button>
              ) : null
            }
          />
        ) : null}
      </div>

      {isLoading && !jobsData ? (
        <div className="rounded-2xl border border-[var(--k-border)] bg-[var(--k-surface)] p-6 text-sm text-[var(--k-muted)]">
          <Loader2 className="mr-2 inline h-4 w-4 animate-spin" />
          正在加载任务状态…
        </div>
      ) : (
        <div className="space-y-4">
          {groups.map((g) => (
            <GroupSection
              key={g.group}
              group={g.group}
              jobs={g.jobs}
              statusByType={statusByType}
              onTrigger={(meta) => void triggerJob(meta)}
              triggeringJobType={triggeringJobType}
              hkCoverage={hkCoverage}
              alphaRadarExtra={alphaRadar}
              watchlistRun={watchlistRun}
            />
          ))}
        </div>
      )}

      <p className="mt-4 text-[11px] text-[var(--k-muted)]">
        自动刷新间隔 {Math.round(SCHEDULER_POLL_MS / 1000)} 秒 ·
        调度器运行在 data-sync-service 进程内；非交易日的 `stock_close_sync` 会被自动跳过。
      </p>
    </div>
  );
}
