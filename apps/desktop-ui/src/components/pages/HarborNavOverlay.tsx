'use client';

import * as React from 'react';
import {
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
  BLOCK_SENTIMENTS,
  buildHarborNavPoints,
  circuitRuns,
  extremeSentimentRuns,
  hasFlowLayer,
  hasSimCurve,
  type HarborNavPoint,
} from '@/lib/harbor-nav-series';
import type { TimelineRow } from '@/lib/queries/backtest';

const COLORS = {
  harbor: '#059669',
  bench: '#a1a1aa',
  grid: '#94a3b8',
  axis: '#71717a',
} as const;

const SERIES_COLORS: Record<string, string> = {
  港湾: COLORS.harbor,
  基准: COLORS.bench,
};

type ChartDatum = {
  date: string;
  港湾: number | null;
  基准: number | null;
  cnCircuit: boolean;
  hkCircuit: boolean;
  sentiment: string | null;
  hold: string;
};

/** TIP-017 flow strip (20-session horizon; display only). */
type FlowDatum = {
  date: string;
  'ETF份额Δ%': number | null;
  '两融Δ%': number | null;
  北向20日累计: number | null;
  '小单净买占比%': number | null;
};

const FLOW_COLORS = {
  etf: '#0ea5e9',
  margin: '#a855f7',
  north: '#14b8a6',
  sm: '#f97316',
} as const;

const FLOW_FIELDS: Array<{
  key: keyof Omit<FlowDatum, 'date'>;
  label: string;
  color: string;
  unit: string;
}> = [
  { key: 'ETF份额Δ%', label: 'ETF份额 20日Δ', color: FLOW_COLORS.etf, unit: '%' },
  { key: '两融Δ%', label: '两融余额 20日Δ', color: FLOW_COLORS.margin, unit: '%' },
  { key: '北向20日累计', label: '北向 20日累计', color: FLOW_COLORS.north, unit: '亿' },
  { key: '小单净买占比%', label: '小单净买占比', color: FLOW_COLORS.sm, unit: '%' },
];

function fmtPct(v: number | null | undefined): string {
  if (v == null || !Number.isFinite(v)) return '—';
  return `${v > 0 ? '+' : ''}${v.toFixed(1)}%`;
}

function yDomain(points: HarborNavPoint[]): [number, number] {
  const vals: number[] = [];
  for (const p of points) {
    vals.push(p.harborPct);
    vals.push(p.basePct);
    if (p.harborSimPct != null) vals.push(p.harborSimPct);
  }
  if (!vals.length) return [0, 1];
  const min = Math.min(...vals);
  const max = Math.max(...vals);
  const pad = Math.max(1, (max - min) * 0.08);
  return [min - pad, max + pad];
}

function OverlayTooltip({
  active,
  payload,
  label,
}: {
  active?: boolean;
  payload?: Array<{ dataKey?: string | number; value?: number | null; payload?: ChartDatum }>;
  label?: string | number;
}) {
  if (!active || !payload?.length) return null;
  const datum = payload[0]?.payload;
  const rows = payload.filter((p) => p.dataKey != null);
  const sentLabel =
    datum?.sentiment != null && BLOCK_SENTIMENTS.has(datum.sentiment)
      ? datum.sentiment === 'extreme_caution'
        ? '极端谨慎'
        : datum.sentiment
      : null;
  return (
    <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] px-3 py-2 text-[11px] shadow-sm">
      <div className="mb-1 flex flex-wrap items-center gap-2 font-medium text-[var(--k-fg)]">
        {label}
        {datum?.cnCircuit ? (
          <span className="rounded bg-red-500/15 px-1 text-[9px] text-red-700 dark:text-red-300">
            CN 熔断
          </span>
        ) : null}
        {datum?.hkCircuit ? (
          <span className="rounded bg-red-500/15 px-1 text-[9px] text-red-700 dark:text-red-300">
            HK 熔断
          </span>
        ) : null}
        {sentLabel ? (
          <span className="rounded bg-amber-500/15 px-1 text-[9px] text-amber-700 dark:text-amber-300">
            {sentLabel}
          </span>
        ) : null}
      </div>
      {datum?.hold ? (
        <div className="mb-1 text-[10px] text-[var(--k-muted)]">{datum.hold}</div>
      ) : null}
      <div className="space-y-1">
        {rows.map((p) => (
          <div key={String(p.dataKey)} className="flex items-center justify-between gap-4">
            <span className="flex items-center gap-1.5 text-[var(--k-muted)]">
              <span
                className="inline-block h-2 w-2 rounded-full"
                style={{
                  background:
                    p.dataKey === '基准' ? 'transparent' : SERIES_COLORS[String(p.dataKey)],
                  border: p.dataKey === '基准' ? '1.5px solid #a1a1aa' : 'none',
                }}
              />
              {String(p.dataKey)}
            </span>
            <span className="font-mono text-[var(--k-text)]">{fmtPct(p.value)}</span>
          </div>
        ))}
      </div>
    </div>
  );
}

function fmtFlowVal(v: number | null | undefined, unit: string): string {
  if (v == null || !Number.isFinite(v)) return '—';
  return `${v > 0 ? '+' : ''}${v.toFixed(unit === '%' ? 2 : 1)}${unit}`;
}

function FlowTooltip({
  active,
  payload,
  label,
}: {
  active?: boolean;
  payload?: Array<{ payload?: FlowDatum }>;
  label?: string | number;
}) {
  if (!active || !payload?.length) return null;
  const datum = payload[0]?.payload;
  if (!datum) return null;
  return (
    <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] px-3 py-2 text-[11px] shadow-sm">
      <div className="mb-1 font-medium text-[var(--k-fg)]">{label} · 资金流（20 日口径）</div>
      <div className="space-y-1">
        {FLOW_FIELDS.map((f) => (
          <div key={f.key} className="flex items-center justify-between gap-4">
            <span className="flex items-center gap-1.5 text-[var(--k-muted)]">
              <span className="inline-block h-2 w-2 rounded-full" style={{ background: f.color }} />
              {f.label}
            </span>
            <span className="font-mono text-[var(--k-text)]">
              {fmtFlowVal(datum[f.key], f.unit)}
            </span>
          </div>
        ))}
      </div>
    </div>
  );
}

export function HarborNavOverlay({ rows }: { rows: TimelineRow[] }) {
  const [showFlow, setShowFlow] = React.useState(false);
  const points = React.useMemo(() => buildHarborNavPoints(rows), [rows]);
  const circuits = React.useMemo(() => circuitRuns(points), [points]);
  const extremes = React.useMemo(() => extremeSentimentRuns(points), [points]);
  const n = points.length;
  const sim = React.useMemo(() => hasSimCurve(points), [points]);
  const domain = React.useMemo(() => yDomain(points), [points]);
  const data = React.useMemo<ChartDatum[]>(
    () =>
      points.map((p) => ({
        date: p.date,
        港湾: sim ? p.harborSimPct : p.harborPct,
        基准: sim ? p.harborPct : null,
        cnCircuit: p.cnCircuit,
        hkCircuit: p.hkCircuit,
        sentiment: p.sentiment,
        hold: p.hold,
      })),
    [points, sim],
  );
  const tickDates = React.useMemo(() => {
    if (n <= 1) return points.map((p) => p.date);
    const step = Math.max(1, Math.ceil(n / 7));
    return points.filter((_, i) => i % step === 0).map((p) => p.date);
  }, [points, n]);
  const flowAvailable = React.useMemo(() => hasFlowLayer(points), [points]);
  const flowData = React.useMemo<FlowDatum[]>(
    () =>
      points.map((p) => ({
        date: p.date,
        'ETF份额Δ%': p.flow?.etfShareD20Pct ?? null,
        '两融Δ%': p.flow?.marginD20Pct ?? null,
        北向20日累计: p.flow?.northD20 ?? null,
        '小单净买占比%': p.flow?.smNetPct ?? null,
      })),
    [points],
  );
  const flowShow = showFlow && flowAvailable;
  const last = points[n - 1];
  const harborMain = sim ? (last?.harborSimPct ?? null) : (last?.harborPct ?? null);

  if (!n) return null;

  return (
    <div className="rounded border border-sky-500/25 bg-sky-500/5 px-2 py-1.5">
      <div className="mb-1 flex flex-wrap items-center gap-2 text-[10px] text-[var(--k-muted)]">
        <span className="font-medium text-[var(--k-fg)]">NAV 叠加</span>
        <span className="flex items-center gap-1">
          <span className="inline-block h-0.5 w-3 bg-emerald-600" />
          港湾 {fmtPct(harborMain)}
        </span>
        {sim ? (
          <span className="flex items-center gap-1">
            <span className="inline-block h-0 w-3 border-t border-dashed border-zinc-500" />
            基准 {last?.harborPct != null ? fmtPct(last.harborPct) : '—'}
          </span>
        ) : null}
        <span className="ml-auto flex items-center gap-2">
          {flowAvailable ? (
            <button
              type="button"
              onClick={() => setShowFlow((v) => !v)}
              className={cn(
                'rounded border px-1.5 py-0.5 text-[9px] transition-colors',
                showFlow
                  ? 'border-emerald-500/50 bg-emerald-500/15 text-emerald-700 dark:text-emerald-300'
                  : 'border-[var(--k-border)] text-[var(--k-muted)] hover:text-[var(--k-fg)]',
              )}
              data-testid="harbor-flow-toggle"
            >
              资金流
            </button>
          ) : null}
        </span>
      </div>
      <div className="h-[300px] w-full" data-testid="harbor-nav-chart">
        {/* OPT-152: recharts for hover crosshair + tooltip. */}
        <ResponsiveContainer width="100%" height="100%">
          <LineChart
            data={data}
            syncId="harbor-nav"
            margin={{ top: 8, right: 8, bottom: 0, left: 0 }}
          >
            <CartesianGrid stroke={COLORS.grid} strokeOpacity={0.25} vertical={false} />
            <XAxis
              dataKey="date"
              ticks={tickDates}
              tick={{ fontSize: 9, fill: COLORS.axis }}
              stroke={COLORS.grid}
              strokeOpacity={0.5}
              tickLine={false}
              axisLine={{ stroke: COLORS.grid, strokeOpacity: 0.5 }}
            />
            <YAxis
              domain={domain}
              tick={{ fontSize: 9, fill: COLORS.axis }}
              tickFormatter={(v: number) => `${Math.round(v)}%`}
              width={38}
              tickLine={false}
              axisLine={false}
            />
            <RechartsTooltip
              content={<OverlayTooltip />}
              cursor={{ stroke: COLORS.axis, strokeDasharray: '4 4' }}
            />
            {/* TIP-016 posture bands: sentiment (amber, behind) + circuit (red). */}
            {extremes.map((run) => (
              <ReferenceArea
                key={`sent-${run.start}-${run.end}`}
                x1={points[run.start]?.date}
                x2={points[Math.max(0, run.end - 1)]?.date}
                y1={domain[0]}
                y2={domain[1]}
                fill="#f59e0b"
                fillOpacity={0.09}
                stroke="none"
                ifOverflow="visible"
              />
            ))}
            {circuits.map((run) => (
              <ReferenceArea
                key={`circ-${run.start}-${run.end}`}
                x1={points[run.start]?.date}
                x2={points[Math.max(0, run.end - 1)]?.date}
                y1={domain[0]}
                y2={domain[1]}
                fill="#ef4444"
                fillOpacity={0.13}
                stroke="none"
                ifOverflow="visible"
              />
            ))}
            {sim ? (
              <Line
                type="linear"
                dataKey="基准"
                stroke={COLORS.bench}
                strokeWidth={1}
                strokeDasharray="3 4"
                dot={false}
                isAnimationActive={false}
              />
            ) : null}
            <Line
              type="linear"
              dataKey="港湾"
              stroke={COLORS.harbor}
              strokeWidth={2.25}
              dot={false}
              isAnimationActive={false}
            />
          </LineChart>
        </ResponsiveContainer>
      </div>
      {flowShow ? (
        <div className="mt-1" data-testid="harbor-flow-chart">
          <div className="mb-0.5 flex flex-wrap items-center gap-2 text-[9px] text-[var(--k-muted)]">
            <span className="font-medium text-[var(--k-fg)]">
              资金流（20 日口径 · 显示层，不影响交易）
            </span>
            <span className="flex items-center gap-1">
              <span className="inline-block h-0.5 w-3" style={{ background: FLOW_COLORS.etf }} />
              ETF份额Δ%
            </span>
            <span className="flex items-center gap-1">
              <span className="inline-block h-0.5 w-3" style={{ background: FLOW_COLORS.margin }} />
              两融Δ%
            </span>
            <span className="flex items-center gap-1">
              <span className="inline-block h-0.5 w-3" style={{ background: FLOW_COLORS.north }} />
              北向20日累计
            </span>
            <span className="flex items-center gap-1">
              <span className="inline-block h-0.5 w-3" style={{ background: FLOW_COLORS.sm }} />
              小单净买占比%
            </span>
          </div>
          <div className="h-[110px] w-full">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart
                data={flowData}
                syncId="harbor-nav"
                margin={{ top: 4, right: 8, bottom: 0, left: 0 }}
              >
                <CartesianGrid stroke={COLORS.grid} strokeOpacity={0.2} vertical={false} />
                <XAxis
                  dataKey="date"
                  ticks={tickDates}
                  tick={{ fontSize: 9, fill: COLORS.axis }}
                  stroke={COLORS.grid}
                  strokeOpacity={0.4}
                  tickLine={false}
                  axisLine={{ stroke: COLORS.grid, strokeOpacity: 0.4 }}
                />
                <YAxis
                  yAxisId="pct"
                  domain={['auto', 'auto']}
                  tick={{ fontSize: 9, fill: COLORS.axis }}
                  width={34}
                  tickLine={false}
                  axisLine={false}
                />
                <YAxis
                  yAxisId="cny"
                  orientation="right"
                  domain={['auto', 'auto']}
                  tick={{ fontSize: 9, fill: COLORS.axis }}
                  width={40}
                  tickLine={false}
                  axisLine={false}
                />
                <RechartsTooltip
                  content={<FlowTooltip />}
                  cursor={{ stroke: COLORS.axis, strokeDasharray: '4 4' }}
                />
                <ReferenceLine
                  yAxisId="pct"
                  y={0}
                  stroke={COLORS.axis}
                  strokeOpacity={0.4}
                  strokeDasharray="2 3"
                />
                <Line
                  yAxisId="pct"
                  type="linear"
                  dataKey="ETF份额Δ%"
                  stroke={FLOW_COLORS.etf}
                  strokeWidth={1.5}
                  dot={false}
                  isAnimationActive={false}
                />
                <Line
                  yAxisId="pct"
                  type="linear"
                  dataKey="两融Δ%"
                  stroke={FLOW_COLORS.margin}
                  strokeWidth={1.5}
                  dot={false}
                  isAnimationActive={false}
                />
                <Line
                  yAxisId="cny"
                  type="linear"
                  dataKey="北向20日累计"
                  stroke={FLOW_COLORS.north}
                  strokeWidth={1.5}
                  dot={false}
                  isAnimationActive={false}
                />
                <Line
                  yAxisId="pct"
                  type="linear"
                  dataKey="小单净买占比%"
                  stroke={FLOW_COLORS.sm}
                  strokeWidth={1.5}
                  dot={false}
                  isAnimationActive={false}
                />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </div>
      ) : null}
      <div className={cn('text-[10px] text-[var(--k-muted)]')}>
        {sim
          ? '主曲线 = 实盘口径港湾（S-3 核心 + 闲置现金 ETF 停车场）· 灰虚线 = 冻结 Timeline 基准 · 红底 = 线熔断（30 天已实现 ≤ -25%）· 琥珀底 = 极端谨慎'
          : '港湾曲线 = 窗口内累计% · 灰虚线仅在实盘口径数据可用时显示'}
      </div>
    </div>
  );
}
