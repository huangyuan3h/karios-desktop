'use client';

import * as React from 'react';
import { AlertTriangle, Info } from 'lucide-react';

import {
  useFactorSystemQuery,
  type FactorBrick,
  type FactorSystemFocusItem,
  type FactorSystemGate,
  type FactorSystemPolicy,
  type FactorSystemStatus,
} from '@/lib/queries/agent';
import { FactorHistoryChart, type FactorHistoryPoint } from '@/components/pages/FactorHistoryChart';
import { SgapDecayPanel } from '@/components/pages/SgapDecayPanel';
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

const KIND_LABEL: Record<string, string> = {
  brick: '原子砖块',
  sleeve: '组合',
  reference: '参考',
};

const BRICK_FILTERS = [
  { key: 'all', label: '全部' },
  { key: 'brick', label: '原子砖块' },
  { key: 'sleeve', label: '组合' },
  { key: 'reference', label: '参考' },
] as const;

type BrickKindFilter = (typeof BRICK_FILTERS)[number]['key'];
type BrickStatusFilter = 'all' | 'cold' | 'watch' | 'revived';
type BrickSortKey =
  | 'default'
  | 'name'
  | 'family'
  | 'status'
  | 'net'
  | 'percentile'
  | 'winRate'
  | 'trades'
  | 'onceRevived';

const BRICK_WINDOW_ORDER = ['OOS2', 'train', 'valid', 'holdout'] as const;

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

function fmtNum(v: number | null | undefined, digits = 2): string {
  if (v == null || !Number.isFinite(v)) return '—';
  return v.toFixed(digits).replace(/\.?0+$/, '');
}

function fmtWinRate(v: number | null | undefined): string {
  if (v == null || !Number.isFinite(v)) return '—';
  return `${(v * 100).toFixed(1)}%`;
}

function brickWindowNet(brick: FactorBrick, window: (typeof BRICK_WINDOW_ORDER)[number]) {
  return brick.windows?.[window]?.net ?? null;
}

/** Watch with a positive holdout first, cold last, references never jump the queue. */
function brickTier(brick: FactorBrick): number {
  if (brick.kind === 'reference') return 3;
  if (brick.status === 'cold') return 2;
  const net = brick.recentPerformance?.net;
  if (brick.status === 'watch' && net != null && net > 0) return 0;
  return 1;
}

function defaultBrickCompare(a: FactorBrick, b: FactorBrick): number {
  const tier = brickTier(a) - brickTier(b);
  if (tier !== 0) return tier;
  const an = a.recentPerformance?.net;
  const bn = b.recentPerformance?.net;
  if (an == null && bn == null) return a.name.localeCompare(b.name);
  if (an == null) return 1;
  if (bn == null) return -1;
  if (bn !== an) return bn - an;
  return a.name.localeCompare(b.name);
}

function brickSortValue(brick: FactorBrick, key: BrickSortKey): string | number | null {
  switch (key) {
    case 'name':
      return brick.name;
    case 'family':
      return brick.family ?? '';
    case 'status':
      return brick.status;
    case 'net':
      return brick.recentPerformance?.net ?? null;
    case 'percentile':
      return brick.recentPerformance?.percentile ?? null;
    case 'winRate':
      return brick.recentPerformance?.winRate ?? null;
    case 'trades':
      return brick.recentPerformance?.trades ?? null;
    case 'onceRevived':
      return brick.onceRevived ? 1 : 0;
    default:
      return null;
  }
}

function compareBricks(
  a: FactorBrick,
  b: FactorBrick,
  sort: { key: BrickSortKey; dir: 'asc' | 'desc' },
): number {
  if (sort.key === 'default') return defaultBrickCompare(a, b);
  const av = brickSortValue(a, sort.key);
  const bv = brickSortValue(b, sort.key);
  if (av == null && bv == null) return a.name.localeCompare(b.name);
  if (av == null) return 1;
  if (bv == null) return -1;
  const dir = sort.dir === 'asc' ? 1 : -1;
  if (typeof av === 'number' && typeof bv === 'number') {
    if (av === bv) return a.name.localeCompare(b.name);
    return (av - bv) * dir;
  }
  return String(av).localeCompare(String(bv)) * dir;
}

function brickWindowLine(brick: FactorBrick): string {
  const nets = BRICK_WINDOW_ORDER.map(
    (window) => `${window} ${fmtNum(brickWindowNet(brick, window))}`,
  );
  const hasWindow = BRICK_WINDOW_ORDER.some((window) => brickWindowNet(brick, window) != null);
  if (hasWindow) return nets.join(' · ');
  if (brick.spark?.length) {
    return `spark ${brick.spark.map((v) => fmtNum(v, 1)).join('/')}`;
  }
  return '四窗数据缺失';
}

/** Expanded brick row: frozen four-window time axis + read-only metadata. */
function BrickDetail({ brick }: { brick: FactorBrick }) {
  const points: FactorHistoryPoint[] = BRICK_WINDOW_ORDER.map((window) => ({
    window,
    percentile: brick.windows?.[window]?.percentile ?? null,
    net: brick.windows?.[window]?.net ?? null,
  }));
  return (
    <div
      className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface-2)]/50 p-3"
      data-testid={`factor-brick-detail-${brick.id}`}
    >
      <div className="mb-1 flex flex-wrap items-baseline gap-2 text-[12px]">
        <span className="font-medium">{brick.name} · 时间轴</span>
        <span className="text-[10px] font-normal text-[var(--k-muted)]">
          分位紫 / 净收益绿 · 50 降档 / 75 加仓线 · OOS2/train/valid/holdout 冻结审计
        </span>
        <span className="ml-auto text-[10px] text-[var(--k-muted)]">
          独立来源 {brick.source ?? '—'}
        </span>
      </div>
      <FactorHistoryChart points={points} testId={`factor-brick-chart-${brick.id}`} />
      <div className="mt-1 flex flex-wrap items-baseline gap-x-3 gap-y-1 text-[10px] text-[var(--k-muted)]">
        <span data-testid={`factor-brick-position-${brick.id}`}>
          建议仓位 <span className="font-mono text-[var(--k-fg)]">{brick.position ?? 0}%</span>（
          {brick.confirmations ?? 0} 次确认）
        </span>
        <span data-testid={`factor-brick-days-${brick.id}`}>
          {brick.daysSinceChange ?? '—'} 天未变档
        </span>
        <span>
          {brick.family ?? '—'} · {brick.source ?? '—'}
        </span>
        <span data-testid={`factor-brick-doc-${brick.id}`}>文档 {brick.doc ?? '—'}</span>
      </div>
      <p className="mt-1 text-[10px] text-[var(--k-muted)]">
        H2k-K2：滚动 40 笔分位 vs 同期随机 + 扣费后净；≥75 且净&gt;0 才复活，&lt;50 降档。仅建议，
        不改设置、不下单。
      </p>
    </div>
  );
}

function FactorBricks({ bricks }: { bricks: FactorBrick[] }) {
  const [kindFilter, setKindFilter] = React.useState<BrickKindFilter>('brick');
  const [statusFilter, setStatusFilter] = React.useState<BrickStatusFilter>('all');
  const [onceRevivedOnly, setOnceRevivedOnly] = React.useState(false);
  const [openId, setOpenId] = React.useState<string | null>(null);
  const [sort, setSort] = React.useState<{ key: BrickSortKey; dir: 'asc' | 'desc' }>({
    key: 'default',
    dir: 'desc',
  });

  const counts = React.useMemo(() => {
    const out: Record<BrickKindFilter, number> = {
      all: bricks.length,
      brick: 0,
      sleeve: 0,
      reference: 0,
    };
    for (const brick of bricks) {
      if (brick.kind === 'brick' || brick.kind === 'sleeve' || brick.kind === 'reference') {
        out[brick.kind] += 1;
      }
    }
    return out;
  }, [bricks]);

  const rows = React.useMemo(() => {
    const filtered = bricks.filter((brick) => {
      if (kindFilter !== 'all' && brick.kind !== kindFilter) return false;
      if (statusFilter !== 'all' && brick.status !== statusFilter) return false;
      if (onceRevivedOnly && !brick.onceRevived) return false;
      return true;
    });
    return [...filtered].sort((a, b) => compareBricks(a, b, sort));
  }, [bricks, kindFilter, statusFilter, onceRevivedOnly, sort]);

  const holdoutAsOf = React.useMemo(() => {
    const dates = bricks
      .map((brick) => brick.recentPerformance?.asOf)
      .filter((value): value is string => Boolean(value))
      .sort();
    return dates.length ? dates[dates.length - 1] : null;
  }, [bricks]);

  const onSort = (key: BrickSortKey) => {
    setSort((prev) => {
      if (prev.key === key) return { key, dir: prev.dir === 'desc' ? 'asc' : 'desc' };
      const textKey = key === 'name' || key === 'family' || key === 'status';
      return { key, dir: textKey ? 'asc' : 'desc' };
    });
  };

  const sortHeader = (label: string, key: BrickSortKey) => {
    const active = sort.key === key;
    return (
      <th
        scope="col"
        className="cursor-pointer select-none px-3 py-1.5 hover:text-[var(--k-fg)]"
        aria-sort={active ? (sort.dir === 'asc' ? 'ascending' : 'descending') : 'none'}
        tabIndex={0}
        onClick={() => onSort(key)}
        onKeyDown={(event) => {
          if (event.key === 'Enter' || event.key === ' ') {
            event.preventDefault();
            onSort(key);
          }
        }}
      >
        {label}
        {active ? (sort.dir === 'asc' ? ' ▲' : ' ▼') : ''}
      </th>
    );
  };

  const chipCls = (active: boolean) =>
    cn(
      'cursor-pointer rounded border px-1.5 py-0.5',
      active
        ? 'border-sky-500/40 bg-sky-500/10 text-sky-800 dark:text-sky-200'
        : 'border-[var(--k-border)] bg-[var(--k-surface-2)]/40 text-[var(--k-muted)]',
    );

  return (
    <div
      className="overflow-hidden rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)]"
      data-testid="factor-bricks"
    >
      <div className="flex flex-wrap items-center gap-2 border-b border-[var(--k-border)] px-3 py-2">
        <span className="text-[12px] font-semibold">砖块 bricks</span>
        <span className="text-[10px] font-normal text-[var(--k-muted)]">
          （{bricks.length} 块 · 原子小砖块含最近表现 · 只展示，不下单 · 点击行展开时间轴）
        </span>
        <span
          className="ml-auto text-[10px] text-[var(--k-muted)]"
          data-testid="factor-bricks-asof"
        >
          最近表现截至 {holdoutAsOf ?? '—'}（滚动实盘窗口；下方四窗为冻结审计）
        </span>
      </div>

      <div
        className="flex flex-wrap items-center gap-x-3 gap-y-1.5 border-b border-[var(--k-border)] px-3 py-2 text-[10px]"
        data-testid="factor-bricks-filters"
      >
        <span className="text-[var(--k-muted)]">类型</span>
        {BRICK_FILTERS.map((filter) => (
          <label key={filter.key} className={chipCls(kindFilter === filter.key)}>
            <input
              type="radio"
              name="factor-brick-kind"
              value={filter.key}
              checked={kindFilter === filter.key}
              onChange={() => setKindFilter(filter.key)}
              aria-label={`类型 ${filter.label}`}
              className="sr-only"
            />
            {filter.label}
            <span className="ml-1 tabular-nums text-[var(--k-muted)]">{counts[filter.key]}</span>
          </label>
        ))}

        <span className="ml-2 text-[var(--k-muted)]">判定</span>
        {(
          [
            ['all', '全部'],
            ['cold', '冷库'],
            ['watch', '观察'],
            ['revived', '复活'],
          ] as const
        ).map(([key, label]) => (
          <label key={key} className={chipCls(statusFilter === key)}>
            <input
              type="radio"
              name="factor-brick-status"
              value={key}
              checked={statusFilter === key}
              onChange={() => setStatusFilter(key)}
              aria-label={`判定 ${label}`}
              className="sr-only"
            />
            {label}
          </label>
        ))}

        <label className={cn('ml-2', chipCls(onceRevivedOnly))}>
          <input
            type="checkbox"
            checked={onceRevivedOnly}
            onChange={(event) => setOnceRevivedOnly(event.target.checked)}
            aria-label="曾经复活"
            className="sr-only"
          />
          曾经复活
        </label>
      </div>

      {bricks.length === 0 ? (
        <div
          className="px-3 py-4 text-[11px] text-[var(--k-muted)]"
          data-testid="factor-bricks-empty"
        >
          暂无砖块数据（因子库缺失或为空）；按母港 M30 基线等待，不做任何跟随。
        </div>
      ) : rows.length === 0 ? (
        <div
          className="px-3 py-4 text-[11px] text-[var(--k-muted)]"
          data-testid="factor-bricks-empty"
        >
          当前筛选没有砖块；切回「全部」或清除筛选。
        </div>
      ) : (
        <div className="overflow-auto">
          <table
            className="w-full min-w-[760px] text-left text-xs tabular-nums"
            data-testid="factor-bricks-table"
          >
            <thead>
              <tr className="text-[10px] text-[var(--k-muted)]">
                {sortHeader('砖块', 'name')}
                {sortHeader('家族', 'family')}
                {sortHeader('判定', 'status')}
                {sortHeader('最近表现', 'net')}
                {sortHeader('分位', 'percentile')}
                {sortHeader('胜率', 'winRate')}
                {sortHeader('笔数', 'trades')}
                {sortHeader('曾经复活', 'onceRevived')}
              </tr>
            </thead>
            <tbody>
              {rows.map((brick) => {
                const recent = brick.recentPerformance;
                const open = openId === brick.id;
                return (
                  <React.Fragment key={brick.id}>
                    <tr
                      className="cursor-pointer border-t border-[var(--k-border)]/60 hover:bg-[var(--k-surface-2)]/60"
                      data-testid={`factor-brick-row-${brick.id}`}
                      aria-expanded={open}
                      tabIndex={0}
                      onClick={() => setOpenId((prev) => (prev === brick.id ? null : brick.id))}
                      onKeyDown={(event) => {
                        if (event.key === 'Enter' || event.key === ' ') {
                          event.preventDefault();
                          setOpenId((prev) => (prev === brick.id ? null : brick.id));
                        }
                      }}
                    >
                      <td className="px-3 py-1.5 align-top">
                        <div className="flex flex-wrap items-center gap-1.5">
                          <span className="font-medium">{brick.name}</span>
                          <span className="text-[10px] text-[var(--k-muted)]">{brick.id}</span>
                          <span className="rounded border border-[var(--k-border)] px-1 text-[10px] text-[var(--k-muted)]">
                            {KIND_LABEL[brick.kind] ?? brick.kind}
                          </span>
                        </div>
                        <div className="mt-0.5 text-[10px] text-[var(--k-muted)]">
                          {brickWindowLine(brick)}
                        </div>
                        {brick.note ? (
                          <div className="mt-0.5 text-[10px] text-[var(--k-muted)]">
                            {brick.note}
                          </div>
                        ) : null}
                      </td>
                      <td className="px-3 py-1.5 align-top text-[11px] text-[var(--k-muted)]">
                        <div>{brick.family ?? '—'}</div>
                        {brick.source ? (
                          <div
                            className="text-[10px]"
                            data-testid={`factor-brick-source-${brick.id}`}
                          >
                            {brick.source}
                          </div>
                        ) : null}
                      </td>
                      <td className="px-3 py-1.5 align-top">
                        <span
                          className={cn(
                            'rounded border px-1.5 py-0.5 text-[10px]',
                            badgeCls(brick.status),
                          )}
                        >
                          {brick.badge ?? STATUS_LABEL[brick.status] ?? brick.status}
                        </span>
                        <div className="mt-0.5 text-[10px] text-[var(--k-muted)]">
                          仓位 {brick.position ?? 0}% · 确认 {brick.confirmations ?? 0}
                          {brick.daysSinceChange != null
                            ? ` · ${brick.daysSinceChange} 天未变档`
                            : ''}
                        </div>
                      </td>
                      <td className="px-3 py-1.5 align-top">
                        <div className="font-mono">
                          {recent?.net == null ? '—' : `${fmtNum(recent.net)}${brick.unit ?? ''}`}
                        </div>
                        <div className="text-[10px] text-[var(--k-muted)]">
                          截至 {fmtDay(recent?.asOf)}
                          {recent?.source === 'live' ? ' · 实盘重演' : ' · 冻结审计'}
                        </div>
                      </td>
                      <td className="px-3 py-1.5 align-top font-mono">
                        {fmtPct(recent?.percentile)}
                      </td>
                      <td className="px-3 py-1.5 align-top font-mono">
                        {fmtWinRate(recent?.winRate)}
                      </td>
                      <td className="px-3 py-1.5 align-top font-mono">{recent?.trades ?? '—'}</td>
                      <td className="px-3 py-1.5 align-top">
                        {brick.onceRevived ? (
                          <span
                            className="rounded border border-amber-500/40 bg-amber-500/10 px-1.5 py-0.5 text-[10px] text-amber-700 dark:text-amber-300"
                            data-testid={`factor-brick-revived-${brick.id}`}
                          >
                            曾经复活
                            {brick.onceRevivedWindows.length
                              ? ` · ${brick.onceRevivedWindows.join('/')}`
                              : ''}
                          </span>
                        ) : (
                          <span className="text-[10px] text-[var(--k-muted)]">—</span>
                        )}
                      </td>
                    </tr>
                    {open ? (
                      <tr className="border-t border-[var(--k-border)]/60">
                        <td colSpan={8} className="px-3 py-2">
                          <BrickDetail brick={brick} />
                        </td>
                      </tr>
                    ) : null}
                  </React.Fragment>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
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

      {data ? <FactorBricks bricks={data.bricks ?? []} /> : null}

      <section className="flex flex-col gap-2" data-testid="factor-system-sgap">
        <div className="flex flex-wrap items-baseline gap-2">
          <span className="text-[12px] font-semibold">S-gap 失效趋势</span>
          <span className="text-[10px] text-[var(--k-muted)]">
            （滚动 40 笔衰减审计 · 只读，与上方砖块同源；不改设置、不下单）
          </span>
        </div>
        <SgapDecayPanel />
      </section>

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
          只读页面，不改设置、不改止损、不下单
        </p>
      ) : null}
    </div>
  );
}
