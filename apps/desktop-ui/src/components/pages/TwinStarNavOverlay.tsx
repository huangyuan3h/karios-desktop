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
  buildTwinStarNavPoints,
  circuitRuns,
  extremeSentimentRuns,
  hasFlowLayer,
  hasSimCurve,
  satActiveRuns,
  satOccupancyLine,
  type TwinStarNavPoint,
} from '@/lib/twin-star-nav-series';
import type { TimelineRow } from '@/lib/queries/backtest';

const COLORS = {
  twin: '#059669',
  core: '#d97706',
  sat: '#0284c7',
  bench: '#a1a1aa',
  grid: '#94a3b8',
  axis: '#71717a',
} as const;

const SERIES_COLORS: Record<string, string> = {
  双子星: COLORS.twin,
  核心: COLORS.core,
  卫星: COLORS.sat,
  基准: COLORS.bench,
};

type ChartDatum = {
  date: string;
  双子星: number | null;
  核心: number | null;
  卫星: number | null;
  基准: number | null;
  satActive: boolean;
  satSlots: number;
  cnCircuit: boolean;
  hkCircuit: boolean;
  sentiment: string | null;
};

/** TIP-017 flow strip (20-session horizon; display only). */
type FlowDatum = {
  date: string;
  'ETF份额Δ%': number | null;
  '两融Δ%': number | null;
  '北向20日累计': number | null;
  '小单净买占比%': number | null;
};

const FLOW_COLORS = {
  etf: '#0ea5e9',
  margin: '#a855f7',
  north: '#14b8a6',
  sm: '#f97316',
} as const;

const FLOW_FIELDS: Array<{ key: keyof Omit<FlowDatum, 'date'>; label: string; color: string; unit: string }> = [
  { key: 'ETF份额Δ%', label: 'ETF份额 20日Δ', color: FLOW_COLORS.etf, unit: '%' },
  { key: '两融Δ%', label: '两融余额 20日Δ', color: FLOW_COLORS.margin, unit: '%' },
  { key: '北向20日累计', label: '北向 20日累计', color: FLOW_COLORS.north, unit: '亿' },
  { key: '小单净买占比%', label: '小单净买占比', color: FLOW_COLORS.sm, unit: '%' },
];

function fmtPct(v: number | null | undefined): string {
  if (v == null || !Number.isFinite(v)) return '—';
  return `${v > 0 ? '+' : ''}${v.toFixed(1)}%`;
}

function yDomain(points: TwinStarNavPoint[]): [number, number] {
  const vals: number[] = [];
  for (const p of points) {
    vals.push(p.twinPct);
    if (p.twinSimPct != null) vals.push(p.twinSimPct);
    if (p.corePct != null) vals.push(p.corePct);
    if (p.coreSimPct != null) vals.push(p.coreSimPct);
    if (p.satPct != null) vals.push(p.satPct);
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
        {datum?.satActive ? (
          <span className="rounded bg-sky-500/15 px-1 text-[9px] text-sky-700 dark:text-sky-300">
            卫星占用 {datum.satSlots} 槽
          </span>
        ) : null}
      </div>
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
            <span className="font-mono text-[var(--k-text)]">{fmtFlowVal(datum[f.key], f.unit)}</span>
          </div>
        ))}
      </div>
    </div>
  );
}

export function TwinStarNavOverlay({ rows }: { rows: TimelineRow[] }) {
  const [showFlow, setShowFlow] = React.useState(false);
  const points = React.useMemo(() => buildTwinStarNavPoints(rows), [rows]);
  const runs = React.useMemo(() => satActiveRuns(points), [points]);
  const circuits = React.useMemo(() => circuitRuns(points), [points]);
  const extremes = React.useMemo(() => extremeSentimentRuns(points), [points]);
  const n = points.length;
  const sim = React.useMemo(() => hasSimCurve(points), [points]);
  const domain = React.useMemo(() => yDomain(points), [points]);
  const data = React.useMemo<ChartDatum[]>(
    () =>
      points.map((p) => ({
        date: p.date,
        双子星: sim ? p.twinSimPct : p.twinPct,
        核心: sim ? p.coreSimPct ?? p.corePct : p.corePct,
        卫星: p.satPct,
        基准: sim ? p.twinPct : null,
        satActive: p.satActive,
        satSlots: p.satSlots,
        cnCircuit: p.cnCircuit,
        hkCircuit: p.hkCircuit,
        sentiment: p.sentiment,
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
        '北向20日累计': p.flow?.northD20 ?? null,
        '小单净买占比%': p.flow?.smNetPct ?? null,
      })),
    [points],
  );
  const flowShow = showFlow && flowAvailable;
  const last = points[n - 1];
  const twinMain = sim ? last?.twinSimPct ?? null : last?.twinPct ?? null;
  const coreMain = sim ? last?.coreSimPct ?? last?.corePct ?? null : last?.corePct ?? null;

  if (!n) return null;

  return (
    <div className="rounded border border-sky-500/25 bg-sky-500/5 px-2 py-1.5">
      <div className="mb-1 flex flex-wrap items-center gap-2 text-[10px] text-[var(--k-muted)]">
        <span className="font-medium text-[var(--k-fg)]">NAV 叠加</span>
        <span className="flex items-center gap-1">
          <span className="inline-block h-0.5 w-3 bg-emerald-600" />
          双子星 {fmtPct(twinMain)}
        </span>
        <span className="flex items-center gap-1">
          <span className="inline-block h-0.5 w-3 bg-amber-600" />
          核心 {coreMain != null ? fmtPct(coreMain) : '需刷新'}
        </span>
        <span className="flex items-center gap-1">
          <span className="inline-block h-0.5 w-3 bg-sky-600" />
          卫星 {last?.satPct != null ? fmtPct(last.satPct) : '—'}
        </span>
        {sim ? (
          <span className="flex items-center gap-1">
            <span className="inline-block h-0 w-3 border-t border-dashed border-zinc-500" />
            基准 {last?.twinPct != null ? fmtPct(last.twinPct) : '—'}
          </span>
        ) : null}
        <span className="ml-auto flex items-center gap-2">
          {satOccupancyLine(points)}
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
              data-testid="twin-flow-toggle"
            >
              资金流
            </button>
          ) : null}
        </span>
      </div>
      <div className="h-[300px] w-full" data-testid="twin-nav-chart">
        {/* OPT-152: recharts for hover crosshair + tooltip; bands = satActive runs. */}
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={data} syncId="twin-star-nav" margin={{ top: 8, right: 8, bottom: 0, left: 0 }}>
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
            {runs.map((run) => (
              <ReferenceArea
                key={`${run.start}-${run.end}`}
                x1={points[run.start]?.date}
                x2={points[Math.max(0, run.end - 1)]?.date}
                y1={domain[0]}
                y2={domain[1]}
                fill="#38bdf8"
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
              dataKey="核心"
              stroke={COLORS.core}
              strokeWidth={2}
              strokeDasharray="6 4"
              dot={false}
              isAnimationActive={false}
            />
            <Line
              type="linear"
              dataKey="卫星"
              stroke={COLORS.sat}
              strokeWidth={1.5}
              dot={false}
              isAnimationActive={false}
            />
            <Line
              type="linear"
              dataKey="双子星"
              stroke={COLORS.twin}
              strokeWidth={2.25}
              dot={false}
              isAnimationActive={false}
            />
          </LineChart>
        </ResponsiveContainer>
      </div>
      {flowShow ? (
        <div className="mt-1" data-testid="twin-flow-chart">
          <div className="mb-0.5 flex flex-wrap items-center gap-2 text-[9px] text-[var(--k-muted)]">
            <span className="font-medium text-[var(--k-fg)]">资金流（20 日口径 · 显示层，不影响交易）</span>
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
              <LineChart data={flowData} syncId="twin-star-nav" margin={{ top: 4, right: 8, bottom: 0, left: 0 }}>
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
          ? '主曲线 = 实盘口径（STOCK 日按 S-3 实际仓位与成本，ETF/REPO 按择强硬切；卫星占用日 50/50 切仓）· 灰虚线 = 100% 押注基准对照 · 天蓝底 = 卫星占用 · 红底 = 线熔断（30 天已实现 ≤ -25%）· 琥珀底 = 极端谨慎'
          : '核心虚线 / 卫星细线 / 双子星粗线 · 三条都是窗口内累计% · 天蓝底 = satActive'}
      </div>
    </div>
  );
}
