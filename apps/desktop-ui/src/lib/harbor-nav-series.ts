import { harborHoldLine } from '@/lib/harbor-segments';
import type { TimelineRow } from '@/lib/queries/backtest';

export type HarborNavPoint = {
  date: string;
  /** Frozen timeline NAV (S-3 core + idle-cash ETF parking). */
  harborPct: number;
  /** OPT-152 实盘口径 product curve (null when the backend ran without sim NAVs). */
  harborSimPct: number | null;
  basePct: number;
  /** TIP-016 posture annotation: line circuit + CN sentiment risk mode. */
  cnCircuit: boolean;
  hkCircuit: boolean;
  sentiment: string | null;
  /** What the portfolio held that day (stock basket / parking ETF). */
  hold: string;
  /** TIP-017 flow layer (display only). */
  flow: {
    etfShareD20Pct: number | null;
    marginD20Pct: number | null;
    northD20: number | null;
    smNetPct: number | null;
  } | null;
};

export type HarborNavRun = {
  start: number;
  end: number;
};

/** Sentiment modes the strategy treats as entry-blocking (engine truth). */
export const BLOCK_SENTIMENTS: ReadonlySet<string> = new Set([
  'extreme_caution',
  'no_new_positions',
]);

export function buildHarborNavPoints(rows: TimelineRow[]): HarborNavPoint[] {
  return (rows ?? []).map((r) => ({
    date: r.date,
    harborPct: r.navSingleReturnPct,
    harborSimPct: r.navSimReturnPct ?? null,
    basePct: r.navBaseReturnPct,
    cnCircuit: Boolean(r.cnCircuit),
    hkCircuit: Boolean(r.hkCircuit),
    sentiment: r.sentiment ?? null,
    hold: harborHoldLine(r),
    flow: r.flow ?? null,
  }));
}

/** True when the rows carry the OPT-152 product-structured curve. */
export function hasSimCurve(points: HarborNavPoint[]): boolean {
  return points.some((p) => p.harborSimPct != null);
}

/** True when any row carries the TIP-017 flow layer. */
export function hasFlowLayer(points: HarborNavPoint[]): boolean {
  return points.some((p) => p.flow != null);
}

/** Inclusive start, exclusive end index runs where the predicate holds. */
export function flagRuns(
  points: HarborNavPoint[],
  pick: (p: HarborNavPoint) => boolean,
): HarborNavRun[] {
  const runs: HarborNavRun[] = [];
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

/** Runs where either line's realized circuit is ON. */
export function circuitRuns(points: HarborNavPoint[]): HarborNavRun[] {
  return flagRuns(points, (p) => p.cnCircuit || p.hkCircuit);
}

/** Runs where CN sentiment is in a blocking risk mode. */
export function extremeSentimentRuns(points: HarborNavPoint[]): HarborNavRun[] {
  return flagRuns(points, (p) => p.sentiment != null && BLOCK_SENTIMENTS.has(p.sentiment));
}
