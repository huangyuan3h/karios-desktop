'use client';

import React from 'react';

import { BarChart3 } from 'lucide-react';

import { useQuery } from '@tanstack/react-query';

import { DATA_SYNC_BASE_URL } from '@/lib/endpoints';
import { cn } from '@/lib/utils';

export type DecisionAnalysis = {
  firedDays: number;
  firedBySource: Record<string, number>;
  firedTotal: number;
  paper: {
    total: number;
    open: number;
    closed: number;
    wins: number;
    losses: number;
    winRate: number | null;
    avgPnlPct: number | null;
  };
  sessions: Array<{
    id: number;
    title: string | null;
    lastActiveAt: string;
    messageCount: number;
    auditRounds: number;
  }>;
};

const SOURCE_LABEL: Record<string, string> = {
  ALPHA: 'Alpha Radar',
  TV: 'TradingView',
  MANUAL: '手动',
  RESEARCH: '研报',
  UNKNOWN: '未知',
};

function StatCard({ label, value, sub }: { label: string; value: string; sub?: string }) {
  return (
    <div className="rounded-md border border-[var(--k-border)] px-2.5 py-2">
      <div className="text-[10px] text-[var(--k-muted)]">{label}</div>
      <div className="mt-0.5 text-lg font-semibold tabular-nums leading-none">{value}</div>
      {sub && <div className="mt-1 text-[10px] text-[var(--k-muted)]">{sub}</div>}
    </div>
  );
}

export type DecisionAction = {
  id: number;
  symbol: string;
  action: string;
  rationale: string | null;
  confidence: number | null;
  status: 'proposed' | 'executed' | 'not_executed';
  source: string;
  snapshotDate: string | null;
  matchedChangeId: string | null;
  outcome: { pct1?: number | null; pct3?: number | null; pct5?: number | null } | null;
  createdAt: string;
};

const ACTION_STYLE: Record<string, string> = {
  BUY: 'border-emerald-600/40 bg-emerald-500/10 text-emerald-700 dark:text-emerald-300',
  ADD: 'border-sky-600/40 bg-sky-500/10 text-sky-700 dark:text-sky-300',
  HOLD: 'border-zinc-500/40 bg-zinc-500/10 text-zinc-600 dark:text-zinc-400',
  EXIT: 'border-red-600/40 bg-red-500/10 text-red-700 dark:text-red-300',
};

const STATUS_LABEL: Record<string, { label: string; cls: string }> = {
  proposed: { label: '已建议', cls: 'border-zinc-500/40 bg-zinc-500/10 text-zinc-600 dark:text-zinc-400' },
  executed: { label: '已执行', cls: 'border-emerald-600/40 bg-emerald-500/10 text-emerald-700 dark:text-emerald-300' },
  not_executed: { label: '未执行', cls: 'border-red-600/40 bg-red-500/10 text-red-700 dark:text-red-300' },
};

function ActionRow({ a }: { a: DecisionAction }) {
  const status = STATUS_LABEL[a.status] ?? STATUS_LABEL.proposed;
  const pct = (v: number | null | undefined) => (v == null ? '—' : `${v >= 0 ? '+' : ''}${v}%`);
  return (
    <div className="rounded-md border border-[var(--k-border)] px-2 py-1.5">
      <div className="flex items-center gap-2">
        <span className="min-w-0 truncate font-mono text-[11px] font-semibold">{a.symbol}</span>
        <span className={cn('rounded border px-1 py-0.5 text-[10px] font-semibold', ACTION_STYLE[a.action] ?? '')}>
          {a.action}
        </span>
        <span className={cn('rounded border px-1 py-0.5 text-[10px]', status.cls)}>{status.label}</span>
        {a.confidence != null && (
          <span className="ml-auto text-[10px] tabular-nums text-[var(--k-muted)]">
            conf {a.confidence.toFixed(2)}
          </span>
        )}
      </div>
      {a.rationale && (
        <p className="mt-1 line-clamp-2 text-[10px] leading-4 text-[var(--k-muted)]">{a.rationale}</p>
      )}
      {a.outcome && (
        <div className="mt-1 flex gap-2 text-[10px] tabular-nums">
          <span className="text-[var(--k-muted)]">
            +1d <span className={cn(pct(a.outcome.pct1).startsWith('+') ? 'text-emerald-600 dark:text-emerald-400' : 'text-red-500')}>{pct(a.outcome.pct1)}</span>
          </span>
          <span className="text-[var(--k-muted)]">
            +3d <span className={cn(pct(a.outcome.pct3).startsWith('+') ? 'text-emerald-600 dark:text-emerald-400' : 'text-red-500')}>{pct(a.outcome.pct3)}</span>
          </span>
          <span className="text-[var(--k-muted)]">
            +5d <span className={cn(pct(a.outcome.pct5).startsWith('+') ? 'text-emerald-600 dark:text-emerald-400' : 'text-red-500')}>{pct(a.outcome.pct5)}</span>
          </span>
          <span className="ml-auto text-[var(--k-muted)]">{a.createdAt.slice(5, 16)}</span>
        </div>
      )}
    </div>
  );
}

export function AnalysisView() {
  const analysisQuery = useQuery({
    queryKey: ['decision', 'analysis'],
    queryFn: async (): Promise<DecisionAnalysis> => {
      const resp = await fetch(`${DATA_SYNC_BASE_URL}/api/decision/analysis`);
      const data = (await resp.json()) as { ok: boolean } & DecisionAnalysis;
      return data;
    },
  });

  const actionsQuery = useQuery({
    queryKey: ['decision', 'actions'],
    queryFn: async (): Promise<DecisionAction[]> => {
      const resp = await fetch(`${DATA_SYNC_BASE_URL}/api/decision/actions?days=30`);
      const data = (await resp.json()) as { ok: boolean; actions: DecisionAction[] };
      return data?.actions ?? [];
    },
  });

  const a = analysisQuery.data;
  if (!a) {
    return (
      <div className="flex h-full flex-col gap-3 overflow-y-auto border-l border-[var(--k-border)] bg-[var(--k-surface)] p-3">
        <div className="flex items-center gap-1.5 text-xs font-semibold">
          <BarChart3 size={13} className="text-[var(--k-accent)]" />
          分析
        </div>
        <p className="text-xs text-[var(--k-muted)]">
          {analysisQuery.isLoading ? '加载中…' : '暂不可用'}
        </p>
      </div>
    );
  }

  const maxSource = Math.max(1, ...Object.values(a.firedBySource));
  const winRate = a.paper.winRate != null ? `${(a.paper.winRate * 100).toFixed(1)}%` : '—';

  return (
    <div className="flex h-full flex-col gap-3 overflow-y-auto border-l border-[var(--k-border)] bg-[var(--k-surface)] p-3">
      <div className="flex items-center gap-1.5 text-xs font-semibold">
        <BarChart3 size={13} className="text-[var(--k-accent)]" />
        分析
        <span className="ml-auto text-[10px] font-normal text-[var(--k-muted)]">
          近 {a.firedDays} 天
        </span>
      </div>

      {/* 开火归因 */}
      <div>
        <div className="mb-1.5 text-[11px] font-medium text-[var(--k-muted)]">
          开火归因（action BUY）
        </div>
        <div className="flex flex-col gap-1.5">
          {Object.entries(a.firedBySource)
            .sort((x, y) => y[1] - x[1])
            .map(([source, count]) => (
              <div key={source} className="flex items-center gap-2">
                <span className="w-20 shrink-0 truncate text-[11px]">
                  {SOURCE_LABEL[source] ?? source}
                </span>
                <div className="h-1.5 flex-1 overflow-hidden rounded-full bg-[var(--k-surface-2)]">
                  <div
                    className="h-full rounded-full bg-[var(--k-accent)]"
                    style={{ width: `${(count / maxSource) * 100}%` }}
                  />
                </div>
                <span className="w-8 shrink-0 text-right text-[11px] tabular-nums">{count}</span>
              </div>
            ))}
          {!Object.keys(a.firedBySource).length && (
            <p className="text-[10px] text-[var(--k-muted)]">暂无开火记录</p>
          )}
        </div>
      </div>

      {/* 模拟盘 */}
      <div className="grid grid-cols-2 gap-1.5">
        <StatCard label="总交易" value={String(a.paper.total)} />
        <StatCard label="胜率" value={winRate} sub={`${a.paper.wins} 胜 / ${a.paper.losses} 负`} />
        <StatCard label="平均盈亏" value={a.paper.avgPnlPct != null ? `${a.paper.avgPnlPct}%` : '—'} />
        <StatCard label="持仓中" value={String(a.paper.open)} sub={`已平仓 ${a.paper.closed}`} />
      </div>

      {/* 注入审计 */}
      <div>
        <div className="mb-1.5 text-[11px] font-medium text-[var(--k-muted)]">
          每轮注入审计（按会话）
        </div>
        <div className="flex flex-col gap-1">
          {a.sessions.map((s) => (
            <div
              key={s.id}
              className="flex items-center gap-2 rounded-md border border-[var(--k-border)] px-2 py-1.5"
            >
              <span className="min-w-0 flex-1 truncate text-[11px]">
                {s.title?.trim() || `Session ${s.id}`}
              </span>
              <span
                className={cn(
                  'rounded border px-1 py-0.5 text-[10px] tabular-nums',
                  s.auditRounds > 0
                    ? 'border-emerald-600/40 bg-emerald-500/10 text-emerald-700 dark:text-emerald-300'
                    : 'border-zinc-500/30 text-[var(--k-muted)]',
                )}
              >
                {s.auditRounds} 轮审计
              </span>
              <span className="text-[10px] tabular-nums text-[var(--k-muted)]">
                {s.messageCount} 条
              </span>
            </div>
          ))}
          {!a.sessions.length && (
            <p className="text-[10px] text-[var(--k-muted)]">暂无会话</p>
          )}
        </div>
      </div>

      {/* 建议追踪 */}
      <div>
        <div className="mb-1.5 flex items-baseline justify-between">
          <span className="text-[11px] font-medium text-[var(--k-muted)]">
            建议追踪（简报 → 执行 → 效果）
          </span>
          <span className="text-[10px] text-[var(--k-muted)]">
            {actionsQuery.data?.filter((a) => a.status === 'executed').length ?? 0}/
            {actionsQuery.data?.length ?? 0} 已执行
          </span>
        </div>
        <div className="flex flex-col gap-1.5">
          {(actionsQuery.data ?? []).slice(0, 15).map((a) => (
            <ActionRow key={a.id} a={a} />
          ))}
          {!actionsQuery.data?.length && (
            <p className="text-[10px] leading-4 text-[var(--k-muted)]">
              暂无建议记录。使用「引用当前数据」生成简报后，18:30 作业会提取建议并跟踪执行与效果。
            </p>
          )}
        </div>
      </div>

      <p className="text-[10px] leading-4 text-[var(--k-muted)]">
        每轮 context_snapshot 落库（块清单 + token），可在会话中追溯「本轮基于什么数据」；
        胜率口径与 TIP-011 开火归因一致。
      </p>
    </div>
  );
}
