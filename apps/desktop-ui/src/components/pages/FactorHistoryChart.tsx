'use client';

import * as React from 'react';
import {
  CartesianGrid,
  Line,
  LineChart,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip as RechartsTooltip,
  XAxis,
  YAxis,
} from 'recharts';

const C = {
  grid: '#94a3b8',
  axis: '#71717a',
  pct: '#8b5cf6',
  net: '#059669',
} as const;

/** One frozen window point (no invented values — nulls break the line). */
export type FactorHistoryPoint = {
  window: string;
  percentile: number | null;
  net: number | null;
};

function fmtSigned(v: number | null | undefined, digits = 2): string {
  if (v == null || !Number.isFinite(v)) return '—';
  return `${v > 0 ? '+' : ''}${v.toFixed(digits)}`;
}

function MiniTip({
  active,
  payload,
  label,
}: {
  active?: boolean;
  payload?: Array<{ name?: string; value?: number | string | null }>;
  label?: string | number;
}) {
  if (!active || !payload?.length) return null;
  return (
    <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] px-3 py-2 text-[11px] shadow-sm">
      <div className="mb-1 font-medium text-[var(--k-fg)]">{label}</div>
      <div className="space-y-0.5">
        {payload.map((p, i) => (
          <div key={i} className="flex items-center justify-between gap-4 tabular-nums">
            <span className="text-[var(--k-muted)]">{p.name}</span>
            <span className="font-mono">
              {typeof p.value === 'number' ? fmtSigned(p.value) : String(p.value ?? '—')}
            </span>
          </div>
        ))}
      </div>
    </div>
  );
}

/**
 * Frozen-window time axis shared by the factor library brick expand row.
 *
 * Purple = random percentile (50 降档 / 75 加仓 reference lines), green = net
 * return (0 line on the right axis). Read-only: the chart never writes a
 * setting and never places an order.
 */
export function FactorHistoryChart({
  points,
  testId,
}: {
  points: FactorHistoryPoint[];
  testId?: string;
}) {
  const data = React.useMemo(
    () => points.map((p) => ({ window: p.window, pct: p.percentile, net: p.net })),
    [points],
  );

  return (
    <div className="h-[180px] w-full" data-testid={testId}>
      <ResponsiveContainer width="100%" height="100%">
        <LineChart data={data} margin={{ top: 8, right: 8, bottom: 0, left: 0 }}>
          <CartesianGrid stroke={C.grid} strokeOpacity={0.25} vertical={false} />
          <XAxis dataKey="window" tick={{ fontSize: 10, fill: C.axis }} tickLine={false} />
          <YAxis
            yAxisId="pct"
            domain={[0, 100]}
            tick={{ fontSize: 9, fill: C.axis }}
            width={36}
            tickLine={false}
            axisLine={false}
          />
          <YAxis
            yAxisId="net"
            orientation="right"
            tick={{ fontSize: 9, fill: C.axis }}
            width={52}
            tickLine={false}
            axisLine={false}
          />
          <RechartsTooltip content={<MiniTip />} />
          <ReferenceLine
            yAxisId="pct"
            y={50}
            stroke={C.axis}
            strokeOpacity={0.6}
            strokeDasharray="2 3"
          />
          <ReferenceLine
            yAxisId="pct"
            y={75}
            stroke={C.pct}
            strokeOpacity={0.7}
            strokeDasharray="4 3"
          />
          <ReferenceLine
            yAxisId="net"
            y={0}
            stroke={C.axis}
            strokeOpacity={0.5}
            strokeDasharray="2 3"
          />
          <Line
            yAxisId="pct"
            type="linear"
            dataKey="pct"
            name="分位%"
            stroke={C.pct}
            strokeWidth={1.75}
            dot
            isAnimationActive={false}
            connectNulls
          />
          <Line
            yAxisId="net"
            type="linear"
            dataKey="net"
            name="净"
            stroke={C.net}
            strokeWidth={1.5}
            dot
            isAnimationActive={false}
            connectNulls
          />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}
