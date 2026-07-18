import type { ExecutionAction, ExecutionActionCard, ExecutionGate, ExecutionGateMode } from '@karios/shared';

export const CHANDELIER_ARM_PNL_PCT = 10;
export const CHANDELIER_ATR_MULT = 2;
export const BUY_SCORE_MIN = 80;

export type TrendOkLike = {
  symbol?: string;
  score?: number | null;
  buyAction?: string | null;
  stopLossPrice?: number | null;
  stopLossParts?: Record<string, unknown> | null;
};

export type PositionLike = {
  symbol: string;
  costPrice?: number | null;
  maxPrice?: number | null;
  positionPct?: number | null;
};

function num(v: unknown): number | null {
  if (typeof v === 'number' && Number.isFinite(v)) return v;
  if (typeof v === 'string' && v.trim()) {
    const n = Number(v);
    return Number.isFinite(n) ? n : null;
  }
  return null;
}

export function isHeldPosition(pos: PositionLike): boolean {
  const pct = num(pos.positionPct);
  if (pct != null && pct > 0) return true;
  const cost = num(pos.costPrice);
  return cost != null && cost > 0;
}

export function computePnLPct(cost: number | null, current: number | null): number | null {
  if (cost == null || cost <= 0 || current == null || !Number.isFinite(current)) return null;
  return ((current - cost) / cost) * 100;
}

export function deriveTriggerAndTrail(opts: {
  hardStop: number | null;
  costPrice: number | null;
  maxPrice: number | null;
  current: number | null;
  atr14: number | null;
}): {
  trailArmed: boolean;
  peak: number | null;
  hardStop: number | null;
  trailStop: number | null;
  trigger: number | null;
  distPct: number | null;
} {
  const { hardStop, costPrice, maxPrice, current, atr14 } = opts;
  const pnl = computePnLPct(costPrice, current);
  const peak = maxPrice != null && Number.isFinite(maxPrice) ? maxPrice : null;
  let trailArmed = false;
  let trailStop: number | null = null;

  if (
    pnl != null &&
    pnl >= CHANDELIER_ARM_PNL_PCT &&
    peak != null &&
    atr14 != null &&
    atr14 > 0
  ) {
    trailArmed = true;
    trailStop = peak - CHANDELIER_ATR_MULT * atr14;
  }

  let trigger: number | null = null;
  if (hardStop != null && trailStop != null) trigger = Math.max(hardStop, trailStop);
  else if (hardStop != null) trigger = hardStop;
  else if (trailStop != null) trigger = trailStop;

  let distPct: number | null = null;
  if (trigger != null && current != null && current > 0) {
    distPct = ((current - trigger) / current) * 100;
  }

  return { trailArmed, peak, hardStop, trailStop, trigger, distPct };
}

function atrFromParts(parts: Record<string, unknown> | null | undefined): number | null {
  if (!parts) return null;
  return num(parts.atr14);
}

function gateMode(gate: ExecutionGate | null | undefined): ExecutionGateMode | null {
  if (!gate?.mode) return null;
  return gate.mode;
}

/**
 * Derive a single Action Card for a watchlist symbol.
 */
export function deriveActionCard(opts: {
  symbol: string;
  gate: ExecutionGate | null | undefined;
  trendok: TrendOkLike | null | undefined;
  position: PositionLike;
  currentPrice: number | null;
}): ExecutionActionCard {
  const { symbol, gate, trendok, position, currentPrice } = opts;
  const held = isHeldPosition(position);
  const parts = (trendok?.stopLossParts ?? null) as Record<string, unknown> | null;
  const hardStop = num(trendok?.stopLossPrice);
  const atr14 = atrFromParts(parts);
  const cost = num(position.costPrice);
  const maxPrice = num(position.maxPrice);
  const trail = deriveTriggerAndTrail({
    hardStop,
    costPrice: cost,
    maxPrice,
    current: currentPrice,
    atr14,
  });

  const exitNow = Boolean(parts?.exit_now);
  const warnHalf = Boolean(parts?.warn_reduce_half);
  const priceAtOrBelowTrigger =
    trail.trigger != null &&
    currentPrice != null &&
    Number.isFinite(currentPrice) &&
    currentPrice <= trail.trigger;

  const mode = gateMode(gate);
  const allowAttack = mode === 'ATTACK';
  const buyAction = String(trendok?.buyAction || '').toLowerCase();
  const score = num(trendok?.score);
  const scoreOk = score != null && score >= BUY_SCORE_MIN;
  const wantsBuy = buyAction === 'buy' && scoreOk;

  let action: ExecutionAction = 'WATCH';
  let why = 'WATCH';

  if (held && (exitNow || priceAtOrBelowTrigger)) {
    action = 'EXIT';
    why = exitNow ? 'EXIT_NOW' : 'TRIGGER_HIT';
  } else if (held && warnHalf) {
    action = 'TRIM';
    why = 'WARN_REDUCE_HALF';
  } else if (held && allowAttack && wantsBuy) {
    action = 'ADD';
    why = 'ATTACK_BUY';
  } else if (held) {
    action = 'HOLD';
    why = allowAttack ? 'HOLD' : 'GATE_BLOCK_NEW';
  } else if (allowAttack && wantsBuy) {
    action = 'BUY';
    why = 'ATTACK_BUY';
  } else if (!allowAttack && wantsBuy) {
    action = 'WATCH';
    why = 'GATE_BLOCK_NEW';
  } else {
    action = 'WATCH';
    why = 'WATCH';
  }

  return {
    symbol,
    action,
    trailArmed: trail.trailArmed,
    peak: trail.peak,
    hardStop: trail.hardStop,
    trailStop: trail.trailStop,
    trigger: trail.trigger,
    distPct: trail.distPct,
    why,
  };
}

export function parseExecutionGate(raw: unknown): ExecutionGate | null {
  if (!raw || typeof raw !== 'object') return null;
  const o = raw as Record<string, unknown>;
  const mode = String(o.mode || '');
  if (mode !== 'ATTACK' && mode !== 'HOLD_ONLY' && mode !== 'DEFEND') return null;
  const regime = String(o.marketRegime || '');
  if (regime !== 'Strong' && regime !== 'Diverging' && regime !== 'Weak') return null;
  const reasons = Array.isArray(o.reasons) ? o.reasons.map((x) => String(x)) : [];
  return {
    mode,
    allowNewEntries: Boolean(o.allowNewEntries),
    marketRegime: regime,
    indexLight: String(o.indexLight || '—'),
    srvLevel: o.srvLevel == null ? null : String(o.srvLevel),
    srvOverlapCount: num(o.srvOverlapCount),
    downCount: num(o.downCount),
    riskMode: o.riskMode == null ? null : String(o.riskMode),
    reasons,
    positionRangeHint: o.positionRangeHint == null ? undefined : String(o.positionRangeHint),
    satelliteNote: o.satelliteNote == null ? undefined : String(o.satelliteNote),
  };
}
