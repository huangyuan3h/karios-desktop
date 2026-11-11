'use client';

import * as React from 'react';
import { AlertTriangle, Info } from 'lucide-react';

import {
  useFactorSystemQuery,
  type FactorSystemFocusItem,
  type FactorSystemGate,
  type FactorSystemPolicy,
  type FactorSystemStatus,
} from '@/lib/queries/agent';
import { cn } from '@/lib/utils';

/**
 * 因子库 (2026-11-11) — read-only view of the advice-only factor system.
 *
 * The system of record is `GET /v1/agent/factor-system` (frozen factor vault
 * -> wait-in-M30 / advise-follow / advise-exit policy). This page never
 * places an order and never writes a setting or weight; `suggestedLiveWeight`
 * and revived lights are rendered as copy, never as actions.
 */

const STATUS_LABEL: Record<string, string> = {
  cold: '冷库',
  watch: '观察',
  revived: '复活',
};

const ROLE_LABEL: Record<string, string> = {
  reference: '参考灯',
  satellite: '卫星',
  research: '研究',
};

const ACTION_COPY: Record<string, { label: string; detail: string }> = {
  wait_in_baseline: {
    label: '母港 M30 等待',
    detail: '卫星因子未复活或未完成确认，按基线等待，不自动跟星舰。',
  },
  advise_follow: {
    label: '建议跟随（仅建议）',
    detail: '规则建议权重不等于实盘仓位；实盘权重仍受三道前置限制。',
  },
  advise_exit: {
    label: '建议退出 → 回母港 M30',
    detail: '跟随中的卫星因子出现衰减；退出该因子/袖套后回基线等待。',
  },
};

const GATE_LABEL: Record<string, string> = {
  paper20: 'paper-20 同配方',
  holdoutRecovery: 'holdout 收复',
  filteredValidPositive: '过滤版 valid 转正',
};

function badgeCls(status: FactorSystemStatus): string {
  if (status === 'revived')
    return 'border-emerald-500/40 bg-emerald-500/10 text-emerald-700 dark:text-emerald-300';
  if (status === 'watch')
    return 'border-amber-500/40 bg-amber-500/10 text-amber-700 dark:text-amber-300';
  return 'border-slate-500/40 bg-slate-500/10 text-slate-600 dark:text-slate-300';
}

function fmtPct(v: number | null | undefined, digits = 1): string {
  if (v == null || !Number.isFinite(v)) return '—';
  return `${v.toFixed(digits)}%`;
}

function fmtDay(v: string | null | undefined): string {
  return v ? String(v).slice(0, 10) : '—';
}

function AdviceBanner() {
  return (
    <div
      className="rounded-lg border border-sky-500/40 bg-sky-500/10 px-3 py-2 text-[12px] text-sky-900 dark:text-sky-200"
      data-testid="factor-system-advice-banner"
    >
      <span className="font-semibold">仅建议 advice only</span>
      <span className="ml-2">
        本页只把因子库翻译成政策建议：不下单、不改仓位/参数/设置，也不会自动跟星舰。
      </span>
    </div>
  );
}

function Metric({ label, value }: { label: string; value: number }) {
  return (
    <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] px-3 py-2">
      <div className="text-[10px] text-[var(--k-muted)]">{label}</div>
      <div className="text-lg font-semibold tabular-nums">{value}</div>
    </div>
  );
}

function gateDetail(key: string, gate: FactorSystemGate): string {
  if (key === 'paper20') return `${gate.completed ?? '—'}/${gate.required ?? '—'} 笔`;
  if (key === 'holdoutRecovery')
    return `当前 ${fmtPct(gate.currentPct)} · 目标 ${fmtPct(gate.recoverToPct)}`;
  return `当前 ${fmtPct(gate.currentPct)} · 阈值 ${fmtPct(gate.thresholdPct)}`;
}

function Preconditions({ pre }: { pre: FactorSystemPolicy['preconditions'] }) {
  const keys = ['paper20', 'holdoutRecovery', 'filteredValidPositive'] as const;
  return (
    <div className="mt-2" data-testid="factor-system-preconditions">
      <div className="text-[10px] font-semibold text-[var(--k-muted)]">
        实盘前置（任一未过，建议实盘权重保持 0%）
      </div>
      <ul className="mt-1 space-y-0.5">
        {keys.map((key) => {
          const gate = pre?.[key];
          if (!gate) return null;
          return (
            <li
              key={key}
              className="flex flex-wrap items-baseline gap-2 text-[11px]"
              data-testid={`factor-system-gate-${key}`}
            >
              <span
                className={cn(
                  'rounded border px-1.5 py-0.5 text-[10px]',
                  gate.pass
                    ? 'border-emerald-500/40 bg-emerald-500/10 text-emerald-700 dark:text-emerald-300'
                    : 'border-red-500/40 bg-red-500/10 text-red-700 dark:text-red-400',
                )}
              >
                {gate.pass ? '通过' : '未过'}
              </span>
              <span className="font-medium">{GATE_LABEL[key]}</span>
              <span className="font-mono tabular-nums text-[var(--k-muted)]">
                {gateDetail(key, gate)}
              </span>
              {gate.source ? (
                <span className="text-[10px] text-[var(--k-muted)]">{gate.source}</span>
              ) : null}
              {gate.stale ? (
                <span className="text-[10px] text-amber-700 dark:text-amber-300">（口径陈旧）</span>
              ) : null}
            </li>
          );
        })}
      </ul>
    </div>
  );
}

function PolicyCard({ policy }: { policy: FactorSystemPolicy }) {
  const action = ACTION_COPY[policy.action] ?? ACTION_COPY.wait_in_baseline;
  return (
    <div
      className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] p-3"
      data-testid="factor-system-policy"
    >
      <div className="flex flex-wrap items-center gap-2">
        <span className="text-[12px] font-semibold">当前政策</span>
        <span
          className="rounded border border-sky-500/40 bg-sky-500/10 px-1.5 py-0.5 text-[10px] text-sky-800 dark:text-sky-200"
          data-testid="factor-system-action"
        >
          {action.label}
        </span>
        <span className="text-[10px] text-[var(--k-muted)]">
          基线 {policy.baselineLabel ?? '母港 M30'}
          {policy.baseline ? ` (${policy.baseline})` : ''}
        </span>
      </div>

      <div className="mt-2 grid grid-cols-1 gap-2 text-[11px] sm:grid-cols-3">
        <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface-2)]/50 px-2 py-1.5">
          <div className="text-[10px] text-[var(--k-muted)]">规则建议权重</div>
          <div className="font-mono tabular-nums" data-testid="factor-system-weight">
            {policy.suggestedWeight ?? 0}%（上限 {policy.maxLiveWeight ?? 20}%）
          </div>
        </div>
        <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface-2)]/50 px-2 py-1.5">
          <div className="text-[10px] text-[var(--k-muted)]">建议实盘权重（只读）</div>
          <div className="font-mono tabular-nums" data-testid="factor-system-live-weight">
            {policy.suggestedLiveWeight ?? 0}%
          </div>
        </div>
        <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface-2)]/50 px-2 py-1.5">
          <div className="text-[10px] text-[var(--k-muted)]">实盘闸</div>
          <div
            className={cn(
              'tabular-nums',
              policy.liveBlocked
                ? 'text-amber-700 dark:text-amber-300'
                : 'text-emerald-700 dark:text-emerald-300',
            )}
            data-testid="factor-system-live-blocked"
          >
            {policy.liveBlocked ? '受阻 · 不跟' : '已满足（仍需人工确认）'}
          </div>
        </div>
      </div>

      {policy.liveBlockedReason ? (
        <p
          className="mt-2 text-[11px] text-amber-800 dark:text-amber-200"
          data-testid="factor-system-live-reason"
        >
          {policy.liveBlockedReason}
        </p>
      ) : null}
      {policy.reason ? (
        <p className="mt-1 text-[11px] text-[var(--k-muted)]" data-testid="factor-system-reason">
          {policy.reason}
        </p>
      ) : null}

      <Preconditions pre={policy.preconditions} />
    </div>
  );
}

function FocusRow({ item }: { item: FactorSystemFocusItem }) {
  return (
    <div className="flex flex-col gap-1 px-3 py-2" data-testid={`factor-system-row-${item.id}`}>
      <div className="flex flex-wrap items-center gap-2 text-[12px]">
        <span className="font-medium">{item.name}</span>
        <span className="text-[10px] text-[var(--k-muted)]">
          {item.id}
          {item.family ? ` · ${item.family}` : ''}
        </span>
        <span className={cn('rounded border px-1.5 py-0.5 text-[10px]', badgeCls(item.status))}>
          {item.badge ?? STATUS_LABEL[item.status] ?? item.status}
        </span>
        <span className="rounded border border-[var(--k-border)] px-1.5 py-0.5 text-[10px] text-[var(--k-muted)]">
          {ROLE_LABEL[item.role] ?? item.role}
        </span>
        <span className="ml-auto font-mono text-[11px] tabular-nums text-[var(--k-muted)]">
          仓位 {item.position ?? 0}% · 分位 {fmtPct(item.percentile)}
        </span>
      </div>
      <div className="flex flex-wrap items-center gap-2 text-[10px] text-[var(--k-muted)]">
        <span data-testid={`factor-system-action-${item.id}`}>
          {item.actionable ? '可跟随（仅建议，不下单）' : '仅观察 · 非下单指令'}
        </span>
        {item.note ? <span>{item.note}</span> : null}
      </div>
    </div>
  );
}

function UnavailableCard({ error, reason }: { error?: string | null; reason?: string | null }) {
  return (
    <div
      className="rounded-lg border border-red-500/40 bg-red-500/10 px-3 py-2"
      data-testid="factor-system-unavailable"
    >
      <div className="flex items-center gap-1.5 text-[12px] font-semibold text-red-800 dark:text-red-300">
        <AlertTriangle className="h-3.5 w-3.5" />
        因子库不可用，按母港 M30 基线等待（fail-open）
      </div>
      {error ? (
        <p className="mt-1 text-[11px] text-red-800/80 dark:text-red-300/80">{error}</p>
      ) : null}
      {reason ? <p className="mt-0.5 text-[11px] text-[var(--k-muted)]">{reason}</p> : null}
      <p className="mt-0.5 text-[10px] text-[var(--k-muted)]">
        缺失或损坏的因子库不会触发任何交易或设置变更。
      </p>
    </div>
  );
}

export function FactorSystemPage() {
  const q = useFactorSystemQuery(true);
  const data = q.data;

  if (q.isLoading && !data) {
    return (
      <div className="flex flex-col gap-4 p-4" data-testid="factor-system-page">
        <AdviceBanner />
        <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] p-3 text-xs text-[var(--k-muted)]">
          因子库加载中…
        </div>
      </div>
    );
  }

  const requestError = q.isError ? String(q.error ?? '请求失败') : null;
  const unavailable = !data || data.available === false;
  const errorText = requestError ?? data?.error ?? null;

  return (
    <div className="flex flex-col gap-4 p-4" data-testid="factor-system-page">
      <AdviceBanner />

      {unavailable ? <UnavailableCard error={errorText} reason={data?.policy?.reason} /> : null}

      {data?.freshness?.stale ? (
        <div
          className="rounded-lg border border-amber-500/40 bg-amber-500/10 px-3 py-2 text-[12px] text-amber-900 dark:text-amber-200"
          data-testid="factor-system-stale"
        >
          <div className="flex items-center gap-1.5 font-semibold">
            <AlertTriangle className="h-3.5 w-3.5" />
            数据陈旧 · 灯不会自行变化
          </div>
          <p className="mt-0.5 text-[11px]">
            {data.freshness.reason ?? '因子库或交易流水为旧快照'}
          </p>
          <p className="mt-0.5 text-[10px] text-amber-800/80 dark:text-amber-300/80">
            因子库生成 {fmtDay(data.freshness.sourceGeneratedAt)} · 历史{' '}
            {fmtDay(data.freshness.historyLatest)} · 交易流水止于{' '}
            {fmtDay(data.freshness.tradeBlotterLastEntry)} · 距今 {data.freshness.ageDays ?? '—'} 天
          </p>
        </div>
      ) : null}

      {data?.summary ? (
        <div className="grid grid-cols-2 gap-2 sm:grid-cols-4" data-testid="factor-system-summary">
          <Metric label="因子总数" value={data.summary.nFactors ?? 0} />
          <Metric label="冷库" value={data.summary.nCold ?? 0} />
          <Metric label="观察" value={data.summary.nWatch ?? 0} />
          <Metric label="复活" value={data.summary.nRevived ?? 0} />
        </div>
      ) : null}

      {data?.policy ? <PolicyCard policy={data.policy} /> : null}

      {data?.focus?.length ? (
        <div
          className="overflow-hidden rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)]"
          data-testid="factor-system-focus"
        >
          <div className="flex items-center gap-2 border-b border-[var(--k-border)] px-3 py-2 text-[12px] font-semibold">
            关注 focus
            <span className="text-[10px] font-normal text-[var(--k-muted)]">
              （{data.focus.length} 项 · 复活灯不等于下单）
            </span>
          </div>
          <div className="divide-y divide-[var(--k-border)]/60">
            {data.focus.map((item) => (
              <FocusRow key={item.id} item={item} />
            ))}
          </div>
        </div>
      ) : null}

      {data?.recentTransitions?.length ? (
        <div
          className="overflow-auto rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)]"
          data-testid="factor-system-transitions"
        >
          <div className="border-b border-[var(--k-border)] px-3 py-2 text-[12px] font-semibold">
            最近转换
          </div>
          <table className="w-full min-w-[520px] text-left text-xs tabular-nums">
            <thead>
              <tr className="text-[10px] text-[var(--k-muted)]">
                <th className="px-3 py-1.5">日期</th>
                <th className="px-3 py-1.5">因子</th>
                <th className="px-3 py-1.5">变化</th>
                <th className="px-3 py-1.5">类型</th>
              </tr>
            </thead>
            <tbody>
              {data.recentTransitions.map((t, i) => (
                <tr
                  key={`${t.date ?? ''}-${t.factorId ?? ''}-${i}`}
                  className="border-t border-[var(--k-border)]/60"
                >
                  <td className="px-3 py-1.5 text-[var(--k-muted)]">{fmtDay(t.date)}</td>
                  <td className="px-3 py-1.5">
                    {t.name ?? t.factorId ?? '—'}
                    {t.factorId ? (
                      <span className="ml-1 text-[10px] text-[var(--k-muted)]">{t.factorId}</span>
                    ) : null}
                  </td>
                  <td className="px-3 py-1.5 font-mono">
                    {t.from ?? '—'} → {t.to ?? '—'}
                  </td>
                  <td className="px-3 py-1.5 text-[var(--k-muted)]">
                    {t.eventType ?? t.kind ?? '—'}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : null}

      {!unavailable ? (
        <p className="flex items-center gap-1 text-[10px] text-[var(--k-muted)]">
          <Info className="h-3 w-3" />
          数据源 GET /v1/agent/factor-system · 生成于 {fmtDay(data?.generatedAt)} ·
          只读页面，不复权、不改参、不下单
        </p>
      ) : null}
    </div>
  );
}
