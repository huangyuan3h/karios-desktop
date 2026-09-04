import type { TimelineRow } from '@/lib/queries/backtest';

export type TwinStarNavPoint = {
  date: string;
  twinPct: number;
  corePct: number | null;
  satPct: number | null;
  satActive: boolean;
  satSlots: number;
};

export type SatActiveRun = {
  start: number;
  end: number;
};

export function buildTwinStarNavPoints(rows: TimelineRow[]): TwinStarNavPoint[] {
  return (rows ?? []).map((r) => {
    const corePct =
      r.coreNavReturnPct != null
        ? r.coreNavReturnPct
        : r.coreNav != null
          ? Math.round((r.coreNav - 1) * 1000) / 10
          : null;
    return {
      date: r.date,
      twinPct: r.navSingleReturnPct,
      corePct,
      satPct: r.satNavReturnPct ?? null,
      satActive: Boolean(r.satActive),
      satSlots: r.satSlots ?? r.satPositions ?? 0,
    };
  });
}

/** Inclusive start, exclusive end index runs where satActive is true. */
export function satActiveRuns(points: TwinStarNavPoint[]): SatActiveRun[] {
  const runs: SatActiveRun[] = [];
  let start = -1;
  for (let i = 0; i < points.length; i += 1) {
    if (points[i].satActive) {
      if (start < 0) start = i;
    } else if (start >= 0) {
      runs.push({ start, end: i });
      start = -1;
    }
  }
  if (start >= 0) runs.push({ start, end: points.length });
  return runs;
}

export function satOccupancyLine(points: TwinStarNavPoint[]): string {
  const n = points.length;
  const active = points.filter((p) => p.satActive);
  const slots = active.reduce((s, p) => s + p.satSlots, 0);
  const avg = active.length ? (slots / active.length).toFixed(1) : '0';
  return `开闸占用 ${active.length}/${n} 日 · 均 ${avg} 槽`;
}
