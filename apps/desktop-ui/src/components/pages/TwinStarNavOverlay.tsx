'use client';

import * as React from 'react';

import { cn } from '@/lib/utils';
import {
  buildTwinStarNavPoints,
  satActiveRuns,
  satOccupancyLine,
  type TwinStarNavPoint,
} from '@/lib/twin-star-nav-series';
import type { TimelineRow } from '@/lib/queries/backtest';

const W = 1000;
const H = 220;
const PAD_X = 8;
const PAD_Y = 12;

function polyline(xs: number[], ys: (number | null)[], minY: number, maxY: number): string {
  const span = Math.max(1e-6, maxY - minY);
  const n = xs.length;
  const parts: string[] = [];
  for (let i = 0; i < n; i += 1) {
    const yv = ys[i];
    if (yv == null || !Number.isFinite(yv)) continue;
    const x = n === 1 ? PAD_X + (W - PAD_X * 2) / 2 : PAD_X + (xs[i] / (n - 1)) * (W - PAD_X * 2);
    const y = PAD_Y + (1 - (yv - minY) / span) * (H - PAD_Y * 2);
    parts.push(`${parts.length ? 'L' : 'M'}${x.toFixed(1)},${y.toFixed(1)}`);
  }
  return parts.join(' ');
}

function yDomain(points: TwinStarNavPoint[]): { min: number; max: number } {
  const vals: number[] = [];
  for (const p of points) {
    vals.push(p.twinPct);
    if (p.corePct != null) vals.push(p.corePct);
    if (p.satPct != null) vals.push(p.satPct);
  }
  if (!vals.length) return { min: 0, max: 1 };
  const min = Math.min(...vals);
  const max = Math.max(...vals);
  const pad = Math.max(1, (max - min) * 0.08);
  return { min: min - pad, max: max + pad };
}

export function TwinStarNavOverlay({ rows }: { rows: TimelineRow[] }) {
  const points = React.useMemo(() => buildTwinStarNavPoints(rows), [rows]);
  const runs = React.useMemo(() => satActiveRuns(points), [points]);
  const domain = React.useMemo(() => yDomain(points), [points]);
  const n = points.length;
  const xs = points.map((_, i) => i);
  const hasCore = points.some((p) => p.corePct != null);
  const twinPath = polyline(xs, points.map((p) => p.twinPct), domain.min, domain.max);
  const corePath = hasCore
    ? polyline(xs, points.map((p) => p.corePct), domain.min, domain.max)
    : '';
  const satPath = polyline(xs, points.map((p) => p.satPct), domain.min, domain.max);
  const last = points[n - 1];

  if (!n) return null;

  return (
    <div className="rounded border border-sky-500/25 bg-sky-500/5 px-2 py-1.5">
      <div className="mb-1 flex flex-wrap items-center gap-2 text-[10px] text-[var(--k-muted)]">
        <span className="font-medium text-[var(--k-fg)]">NAV 叠加</span>
        <span className="flex items-center gap-1">
          <span className="inline-block h-0.5 w-3 bg-emerald-600" />
          双子星 {last?.twinPct.toFixed(1)}%
        </span>
        <span className="flex items-center gap-1">
          <span className="inline-block h-0.5 w-3 bg-amber-600" />
          核心 {hasCore && last?.corePct != null ? `${last.corePct.toFixed(1)}%` : '需刷新'}
        </span>
        <span className="flex items-center gap-1">
          <span className="inline-block h-0.5 w-3 bg-sky-600" />
          卫星 {last?.satPct != null ? `${last.satPct.toFixed(1)}%` : '—'}
        </span>
        <span className="ml-auto">{satOccupancyLine(points)}</span>
      </div>
      <svg
        viewBox={`0 0 ${W} ${H}`}
        className="h-[120px] w-full"
        role="img"
        aria-label="twin core sat NAV overlay"
      >
        {n > 1
          ? runs.map((run) => {
              const inner = W - PAD_X * 2;
              const x0 = PAD_X + (run.start / (n - 1)) * inner;
              const x1 = PAD_X + ((run.end - 1) / (n - 1)) * inner;
              return (
                <rect
                  key={`${run.start}-${run.end}`}
                  x={x0}
                  y={PAD_Y}
                  width={Math.max(2, x1 - x0)}
                  height={H - PAD_Y * 2}
                  className="fill-sky-500/15"
                />
              );
            })
          : null}
        {corePath ? (
          <path d={corePath} fill="none" stroke="#d97706" strokeWidth="2" strokeDasharray="6 4" />
        ) : null}
        {satPath ? <path d={satPath} fill="none" stroke="#0284c7" strokeWidth="1.5" /> : null}
        {twinPath ? <path d={twinPath} fill="none" stroke="#059669" strokeWidth="2.25" /> : null}
      </svg>
      <div className={cn('text-[10px] text-[var(--k-muted)]')}>
        天蓝底 = satActive（过夜或到期日）· 核心虚线 / 卫星细线 / 双子星粗线 · 三条都是窗口内累计%
      </div>
    </div>
  );
}
