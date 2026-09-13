'use client';

import * as React from 'react';
import {
  Bar,
  Line,
  LineChart,
  ReferenceArea,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip as RechartsTooltip,
  XAxis,
  YAxis,
  CartesianGrid,
} from 'recharts';

import { cn } from '@/lib/utils';
import {
  flowLatest,
  flowSpanWindow,
  flowTickDates,
  fmtSigned,
  flowTotalDaily20,
  gateOnRuns,
  type FlowSpan,
} from '@/lib/fund-flow-series';
import { useFundFlowSeriesQuery, type FundFlowRow } from '@/lib/queries/backtest';

const FF_COLORS = {
  etf: '#0ea5e9',
  margin: '#a855f7',
  north: '#5eead4',
  northCum: '#14b8a6',
  sm: '#f97316',
  total: '#e4e4e7',
  grid: '#94a3b8',
  axis: '#71717a',
} as const;

type Span = FlowSpan;
const SPANS: Array<{ id: Span; label: string }> = [
  { id: '6m', label: '6M' },
  { id: '1y', label: '1Y' },
  { id: '3y', label: '3Y' },
];

type FieldSpec = {
  key: string;
  label: string;
  color: string;
  digits: number;
  unit: string;
  /** Render boolean-ish fields as state labels. */
  flag?: 'gate';
};

type MiniDatum = Record<string, string | number | boolean | null>;

function MiniTooltip({
  active,
  payload,
  label,
  fields,
}: {
  active?: boolean;
  payload?: Array<{ payload?: MiniDatum }>;
  label?: string | number;
  fields: FieldSpec[];
}) {
  if (!active || !payload?.length) return null;
  const d = payload[0]?.payload;
  if (!d) return null;
  return (
    <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] px-3 py-2 text-[11px] shadow-sm">
      <div className="mb-1 font-medium text-[var(--k-fg)]">{label}</div>
      <div className="space-y-1">
        {fields.map((f) => {
          const raw = d[f.key];
          let text: string;
          if (f.flag === 'gate') text = raw ? '国家队撤退 (B 闸 ON)' : '正常';
          else if (raw == null || !Number.isFinite(Number(raw))) text = '—';
          else text = `${Number(raw) > 0 ? '+' : ''}${Number(raw).toFixed(f.digits)}${f.unit}`;
          return (
            <div key={f.key} className="flex items-center justify-between gap-4">
              <span className="flex items-center gap-1.5 text-[var(--k-muted)]">
                <span
                  className="inline-block h-2 w-2 rounded-full"
                  style={{ background: f.flag === 'gate' ? '#ef4444' : f.color }}
                />
                {f.label}
              </span>
              <span
                className={cn(
                  'font-mono',
                  f.flag === 'gate' && raw
                    ? 'text-red-600 dark:text-red-400'
                    : 'text-[var(--k-text)]',
                )}
              >
                {text}
              </span>
            </div>
          );
        })}
      </div>
    </div>
  );
}

function MiniChart({
  rows,
  tickDates,
  children,
  testId,
  syncId,
}: {
  rows: Array<FundFlowRow & { totalD20Yi?: number | null }>;
  tickDates: string[];
  children: React.ReactNode;
  testId: string;
  syncId: string;
}) {
  if (!rows.length) return null;
  return (
    <div className="h-[120px] w-full" data-testid={testId}>
      <ResponsiveContainer width="100%" height="100%">
        <LineChart
          data={rows as unknown as MiniDatum[]}
          syncId={syncId}
          margin={{ top: 4, right: 8, bottom: 0, left: 0 }}
        >
          <CartesianGrid stroke={FF_COLORS.grid} strokeOpacity={0.2} vertical={false} />
          <XAxis
            dataKey="date"
            ticks={tickDates}
            tick={{ fontSize: 9, fill: FF_COLORS.axis }}
            stroke={FF_COLORS.grid}
            strokeOpacity={0.4}
            tickLine={false}
            axisLine={{ stroke: FF_COLORS.grid, strokeOpacity: 0.4 }}
          />
          {children}
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}

function SectionHeader({
  title,
  chips,
}: {
  title: string;
  chips: Array<{ text: string; color?: string; danger?: boolean }>;
}) {
  return (
    <div className="mb-0.5 flex flex-wrap items-center gap-2 text-[9px] text-[var(--k-muted)]">
      <span className="font-medium text-[var(--k-fg)]">{title}</span>
      {chips.map((c) => (
        <span
          key={c.text}
          className={cn(
            'flex items-center gap-1',
            c.danger && 'rounded bg-red-500/15 px-1 text-red-700 dark:text-red-300',
          )}
        >
          {c.color ? (
            <span className="inline-block h-0.5 w-3" style={{ background: c.color }} />
          ) : null}
          {c.text}
        </span>
      ))}
    </div>
  );
}

/** TIP-017 资金流全景: 三路叠加(20日累计·亿元) / 国家队(宽基份额+B闸红带) / 两融 / 北向 / 散户 — 显示层.
 *
 * 默认窗口与港湾 Timeline 同步（props start/end），十字线跨组件联动
 * （recharts 按 index 同步 → 同窗口才对齐）；切换 6M/1Y/3Y 后用自身窗口.
 */
export function FundFlowPanel({
  className,
  start: syncStart,
  end: syncEnd,
}: {
  className?: string;
  /** Timeline 窗口（BacktestPage 传入）— 同步模式下十字线与 NAV 图联动. */
  start?: string;
  end?: string;
}) {
  type Mode = 'sync' | FlowSpan;
  const [mode, setMode] = React.useState<Mode>(syncStart ? 'sync' : '1y');
  const win = React.useMemo(() => {
    if (mode === 'sync' && syncStart && syncEnd) return { start: syncStart, end: syncEnd };
    return flowSpanWindow(mode === 'sync' ? '1y' : mode);
  }, [mode, syncStart, syncEnd]);
  const q = useFundFlowSeriesQuery(win.start, win.end);
  const rawRows = q.data?.rows ?? [];
  const rows = React.useMemo(
    () => rawRows.map((r) => ({ ...r, totalD20Yi: flowTotalDaily20(r) })),
    [rawRows],
  );
  const tickDates = React.useMemo(() => flowTickDates(rows), [rows]);
  const runs = React.useMemo(() => gateOnRuns(rows), [rows]);
  const last = flowLatest(rows);
  // recharts sync works by data index — same window → link with the NAV chart.
  const syncId = mode === 'sync' ? 'harbor-nav' : 'fund-flow';

  if (q.isLoading) {
    return (
      <div
        className={cn(
          'rounded border border-purple-500/25 bg-purple-500/5 px-2 py-1.5 text-[10px] text-[var(--k-muted)]',
          className,
        )}
      >
        资金流全景加载中…
      </div>
    );
  }
  if (q.isError || !rows.length) {
    return (
      <div
        className={cn(
          'rounded border border-purple-500/25 bg-purple-500/5 px-2 py-1.5 text-[10px] text-[var(--k-muted)]',
          className,
        )}
      >
        资金流数据不可用（需后端 ≥ 0043 迁移 + risk_state_sync 回填）
      </div>
    );
  }

  return (
    <div
      className={cn('rounded border border-purple-500/25 bg-purple-500/5 px-2 py-1.5', className)}
      data-testid="fund-flow-panel"
    >
      <div className="mb-1 flex flex-wrap items-center gap-2 text-[10px] text-[var(--k-muted)]">
        <span className="font-medium text-[var(--k-fg)]">资金流全景</span>
        <span>
          国家队{' '}
          {last?.etfShareYi != null
            ? `${last.etfShareYi} 亿份 · Δ20 ${fmtSigned(last.etfShareD20Pct, 1, '%')}`
            : '—'}
        </span>
        {last?.gateOn ? (
          <span className="rounded bg-red-500/15 px-1 text-red-700 dark:text-red-300">
            国家队撤退 (B 闸)
          </span>
        ) : null}
        <span>两融 {last?.marginTrillion != null ? `${last.marginTrillion} 万亿` : '—'}</span>
        <span>北向20日 {last?.northD20Yi != null ? fmtSigned(last.northD20Yi, 0, '亿') : '—'}</span>
        <span>三路20日 {last ? fmtSigned(flowTotalDaily20(last), 0, '亿') : '—'}</span>
        <span className="ml-auto flex items-center gap-1">
          {syncStart ? (
            <button
              type="button"
              onClick={() => setMode('sync')}
              className={cn(
                'rounded border px-1.5 py-0.5 text-[9px] transition-colors',
                mode === 'sync'
                  ? 'border-purple-500/50 bg-purple-500/15 text-purple-700 dark:text-purple-300'
                  : 'border-[var(--k-border)] text-[var(--k-muted)] hover:text-[var(--k-fg)]',
              )}
              title="与上方 Timeline 窗口同步，十字线联动"
            >
              同步
            </button>
          ) : null}
          {SPANS.map((s) => (
            <button
              key={s.id}
              type="button"
              onClick={() => setMode(s.id)}
              className={cn(
                'rounded border px-1.5 py-0.5 text-[9px] transition-colors',
                mode === s.id
                  ? 'border-purple-500/50 bg-purple-500/15 text-purple-700 dark:text-purple-300'
                  : 'border-[var(--k-border)] text-[var(--k-muted)] hover:text-[var(--k-fg)]',
              )}
            >
              {s.label}
            </button>
          ))}
        </span>
      </div>

      <SectionHeader
        title="三路资金叠加（20 日累计净流入 · 亿元 1:1:1）"
        chips={[
          { text: '国家队 20日', color: FF_COLORS.etf },
          { text: '两融 20日', color: FF_COLORS.margin },
          { text: '北向 20日', color: FF_COLORS.northCum },
          { text: '三路合计(虚线)', color: FF_COLORS.total },
        ]}
      />
      <MiniChart rows={rows} tickDates={tickDates} testId="ff-total" syncId={syncId}>
        <YAxis
          domain={['auto', 'auto']}
          tick={{ fontSize: 9, fill: FF_COLORS.axis }}
          width={36}
          tickLine={false}
          axisLine={false}
        />
        <ReferenceLine y={0} stroke={FF_COLORS.axis} strokeOpacity={0.4} strokeDasharray="2 3" />
        <RechartsTooltip
          content={
            <MiniTooltip
              fields={[
                {
                  key: 'natD20Yi',
                  label: '国家队 20日',
                  color: FF_COLORS.etf,
                  digits: 0,
                  unit: ' 亿',
                },
                {
                  key: 'marginD20Yi',
                  label: '两融 20日',
                  color: FF_COLORS.margin,
                  digits: 0,
                  unit: ' 亿',
                },
                {
                  key: 'northD20Yi',
                  label: '北向 20日',
                  color: FF_COLORS.northCum,
                  digits: 0,
                  unit: ' 亿',
                },
                {
                  key: 'totalD20Yi',
                  label: '合计 20日',
                  color: FF_COLORS.total,
                  digits: 0,
                  unit: ' 亿',
                },
              ]}
            />
          }
          cursor={{ stroke: FF_COLORS.axis, strokeDasharray: '4 4' }}
        />
        <Line
          type="linear"
          dataKey="natD20Yi"
          stroke={FF_COLORS.etf}
          strokeWidth={1.25}
          dot={false}
          isAnimationActive={false}
        />
        <Line
          type="linear"
          dataKey="marginD20Yi"
          stroke={FF_COLORS.margin}
          strokeWidth={1.25}
          dot={false}
          isAnimationActive={false}
        />
        <Line
          type="linear"
          dataKey="northD20Yi"
          stroke={FF_COLORS.northCum}
          strokeWidth={1.25}
          dot={false}
          isAnimationActive={false}
        />
        <Line
          type="linear"
          dataKey="totalD20Yi"
          stroke={FF_COLORS.total}
          strokeWidth={2}
          strokeDasharray="4 3"
          dot={false}
          isAnimationActive={false}
        />
      </MiniChart>

      <SectionHeader
        title="国家队 · 宽基 ETF 份额（亿份，4 码）"
        chips={[
          { text: `20日Δ ${fmtSigned(last?.etfShareD20Pct, 1, '%')}`, color: FF_COLORS.etf },
          { text: '红带 = 撤退态（指数<MA200 且份额净缩）', color: '#ef4444' },
        ]}
      />
      <MiniChart rows={rows} tickDates={tickDates} testId="ff-etf" syncId={syncId}>
        <YAxis
          domain={['auto', 'auto']}
          tick={{ fontSize: 9, fill: FF_COLORS.axis }}
          width={36}
          tickLine={false}
          axisLine={false}
        />
        <RechartsTooltip
          content={
            <MiniTooltip
              fields={[
                {
                  key: 'etfShareYi',
                  label: '份额合计',
                  color: FF_COLORS.etf,
                  digits: 1,
                  unit: ' 亿份',
                },
                {
                  key: 'etfShareD20Pct',
                  label: '20日Δ',
                  color: FF_COLORS.etf,
                  digits: 2,
                  unit: '%',
                },
                {
                  key: 'gateOn',
                  label: 'B 闸',
                  color: '#ef4444',
                  digits: 0,
                  unit: '',
                  flag: 'gate',
                },
              ]}
            />
          }
          cursor={{ stroke: FF_COLORS.axis, strokeDasharray: '4 4' }}
        />
        {runs.map((run) => (
          <ReferenceArea
            key={`gate-${run.start}-${run.end}`}
            x1={rows[run.start]?.date}
            x2={rows[Math.max(0, run.end - 1)]?.date}
            fill="#ef4444"
            fillOpacity={0.13}
            stroke="none"
            ifOverflow="visible"
          />
        ))}
        <Line
          type="linear"
          dataKey="etfShareYi"
          stroke={FF_COLORS.etf}
          strokeWidth={1.75}
          dot={false}
          isAnimationActive={false}
        />
      </MiniChart>

      <SectionHeader
        title="两融余额（万亿元，全市场）"
        chips={[
          { text: `20日Δ ${fmtSigned(last?.marginD20Pct, 1, '%')}`, color: FF_COLORS.margin },
        ]}
      />
      <MiniChart rows={rows} tickDates={tickDates} testId="ff-margin" syncId={syncId}>
        <YAxis
          domain={['auto', 'auto']}
          tick={{ fontSize: 9, fill: FF_COLORS.axis }}
          width={36}
          tickLine={false}
          axisLine={false}
          tickFormatter={(v: number) => v.toFixed(2)}
        />
        <RechartsTooltip
          content={
            <MiniTooltip
              fields={[
                {
                  key: 'marginTrillion',
                  label: '两融余额',
                  color: FF_COLORS.margin,
                  digits: 3,
                  unit: ' 万亿',
                },
                {
                  key: 'marginD20Pct',
                  label: '20日Δ',
                  color: FF_COLORS.margin,
                  digits: 2,
                  unit: '%',
                },
              ]}
            />
          }
          cursor={{ stroke: FF_COLORS.axis, strokeDasharray: '4 4' }}
        />
        <Line
          type="linear"
          dataKey="marginTrillion"
          stroke={FF_COLORS.margin}
          strokeWidth={1.75}
          dot={false}
          isAnimationActive={false}
        />
      </MiniChart>

      <SectionHeader
        title="北向资金（亿元）"
        chips={[
          { text: '日线净买', color: FF_COLORS.north },
          { text: `20日累计 ${fmtSigned(last?.northD20Yi, 0, '亿')}`, color: FF_COLORS.northCum },
        ]}
      />
      <MiniChart rows={rows} tickDates={tickDates} testId="ff-north" syncId={syncId}>
        <YAxis
          yAxisId="daily"
          domain={['auto', 'auto']}
          tick={{ fontSize: 9, fill: FF_COLORS.axis }}
          width={36}
          tickLine={false}
          axisLine={false}
        />
        <YAxis
          yAxisId="cum"
          orientation="right"
          domain={['auto', 'auto']}
          tick={{ fontSize: 9, fill: FF_COLORS.axis }}
          width={36}
          tickLine={false}
          axisLine={false}
        />
        <ReferenceLine
          yAxisId="daily"
          y={0}
          stroke={FF_COLORS.axis}
          strokeOpacity={0.4}
          strokeDasharray="2 3"
        />
        <RechartsTooltip
          content={
            <MiniTooltip
              fields={[
                {
                  key: 'northDailyYi',
                  label: '当日净买',
                  color: FF_COLORS.north,
                  digits: 1,
                  unit: ' 亿',
                },
                {
                  key: 'northD20Yi',
                  label: '20日累计',
                  color: FF_COLORS.northCum,
                  digits: 0,
                  unit: ' 亿',
                },
              ]}
            />
          }
          cursor={{ stroke: FF_COLORS.axis, strokeDasharray: '4 4' }}
        />
        <Bar
          yAxisId="daily"
          dataKey="northDailyYi"
          fill={FF_COLORS.north}
          fillOpacity={0.55}
          isAnimationActive={false}
        />
        <Line
          yAxisId="cum"
          type="linear"
          dataKey="northD20Yi"
          stroke={FF_COLORS.northCum}
          strokeWidth={1.5}
          dot={false}
          isAnimationActive={false}
        />
      </MiniChart>

      <SectionHeader
        title="散户 · 小单净买占比（%，2023+）"
        chips={[{ text: `最新 ${fmtSigned(last?.smNetPct, 2, '%')}`, color: FF_COLORS.sm }]}
      />
      <MiniChart rows={rows} tickDates={tickDates} testId="ff-sm" syncId={syncId}>
        <YAxis
          domain={['auto', 'auto']}
          tick={{ fontSize: 9, fill: FF_COLORS.axis }}
          width={36}
          tickLine={false}
          axisLine={false}
        />
        <ReferenceLine y={0} stroke={FF_COLORS.axis} strokeOpacity={0.4} strokeDasharray="2 3" />
        <RechartsTooltip
          content={
            <MiniTooltip
              fields={[
                {
                  key: 'smNetPct',
                  label: '小单净买占比',
                  color: FF_COLORS.sm,
                  digits: 2,
                  unit: '%',
                },
              ]}
            />
          }
          cursor={{ stroke: FF_COLORS.axis, strokeDasharray: '4 4' }}
        />
        <Line
          type="linear"
          dataKey="smNetPct"
          stroke={FF_COLORS.sm}
          strokeWidth={1.5}
          dot={false}
          isAnimationActive={false}
        />
      </MiniChart>

      <div className="mt-0.5 text-[10px] text-[var(--k-muted)]">
        显示层 · 不影响交易 · 同步模式下十字线与上方 NAV 图联动（按 index 同步 → 同窗口才对齐）·
        三路 20 日累计 1:1:1（亿元）：国家队 = 宽基份额Δ×净值、两融 = Δrzye、北向 = 日净买 · 份额
        2021-22 起完整 · 两融 2021+ · 北向 2021+ · 小单 2023+（2025-09~2026-07 缺口已回填）
      </div>
    </div>
  );
}
