import type { TimelineRow } from '@/lib/queries/backtest';

export type TwinStarNavPoint = {
  date: string;
  twinPct: number;
  corePct: number | null;
  satPct: number | null;
  /** OPT-152 实盘口径 product curve (null when the backend ran without sim NAVs). */
  twinSimPct: number | null;
  coreSimPct: number | null;
  satActive: boolean;
  satSlots: number;
  /** TIP-016 posture annotation: line circuit + CN sentiment risk mode. */
  cnCircuit: boolean;
  hkCircuit: boolean;
  sentiment: string | null;
  /** TIP-017 flow layer (display only). */
  flow: {
    etfShareD20Pct: number | null;
    marginD20Pct: number | null;
    northD20: number | null;
    smNetPct: number | null;
  } | null;
};

export type SatActiveRun = {
  start: number;
  end: number;
};

/** Sentiment modes the strategy treats as entry-blocking (engine truth). */
export const BLOCK_SENTIMENTS: ReadonlySet<string> = new Set([
  'extreme_caution',
  'no_new_positions',
]);

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
      twinSimPct: r.navSimMultiReturnPct ?? null,
      coreSimPct: r.navSimReturnPct ?? null,
      satActive: Boolean(r.satActive),
      satSlots: r.satSlots ?? r.satPositions ?? 0,
      cnCircuit: Boolean(r.cnCircuit),
      hkCircuit: Boolean(r.hkCircuit),
      sentiment: r.sentiment ?? null,
      flow: r.flow ?? null,
    };
  });
}

/** True when the rows carry the OPT-152 product-structured curve. */
export function hasSimCurve(points: TwinStarNavPoint[]): boolean {
  return points.some((p) => p.twinSimPct != null);
}

/** True when any row carries the TIP-017 flow layer. */
export function hasFlowLayer(points: TwinStarNavPoint[]): boolean {
  return points.some((p) => p.flow != null);
}

/** Inclusive start, exclusive end index runs where the predicate holds. */
export function flagRuns(
  points: TwinStarNavPoint[],
  pick: (p: TwinStarNavPoint) => boolean,
): SatActiveRun[] {
  const runs: SatActiveRun[] = [];
  let start = -1;
  for (let i = 0; i < points.length; i += 1) {
    if (pick(points[i])) {
      if (start < 0) start = i;
    } else if (start >= 0) {
      runs.push({ start, end: i });
      start = -1;
    }
  }
  if (start >= 0) runs.push({ start, end: points.length });
  return runs;
}

/** Inclusive start, exclusive end index runs where satActive is true. */
export function satActiveRuns(points: TwinStarNavPoint[]): SatActiveRun[] {
  return flagRuns(points, (p) => p.satActive);
}

/** Runs where either line's realized circuit is ON. */
export function circuitRuns(points: TwinStarNavPoint[]): SatActiveRun[] {
  return flagRuns(points, (p) => p.cnCircuit || p.hkCircuit);
}

/** Runs where CN sentiment is in a blocking risk mode. */
export function extremeSentimentRuns(points: TwinStarNavPoint[]): SatActiveRun[] {
  return flagRuns(points, (p) => p.sentiment != null && BLOCK_SENTIMENTS.has(p.sentiment));
}

export function satOccupancyLine(points: TwinStarNavPoint[]): string {
  const n = points.length;
  const active = points.filter((p) => p.satActive);
  const slots = active.reduce((s, p) => s + p.satSlots, 0);
  const avg = active.length ? (slots / active.length).toFixed(1) : '0';
  return `开闸占用 ${active.length}/${n} 日 · 均 ${avg} 槽`;
}
