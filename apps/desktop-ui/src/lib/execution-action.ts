import type {
  ExecutionAction,
  ExecutionActionCard,
  ExecutionGate,
  ExecutionGateMode,
  MainlineTag,
} from '@karios/shared';

import type { MainlineAllowSet } from '@/lib/hot-industry-picks';
import { isIntradaySurge } from '@/lib/watchlist-metrics';

export const CHANDELIER_ARM_PNL_PCT = 10;
export const CHANDELIER_ATR_MULT = 2;
export const BUY_SCORE_MIN = 80;

/** Defense sectors blocked from BUY/ADD (East Money industry substring match). */
export const DEFENSE_SECTOR_KEYWORDS = [
  '银行',
  '电力',
  '公用事业',
  '中药',
  '煤炭',
  '高速公路',
] as const;

export type TrendOkLike = {
  symbol?: string;
  score?: number | null;
  buyAction?: string | null;
  stopLossPrice?: number | null;
  stopLossParts?: Record<string, unknown> | null;
  values?: {
    emIndustry?: unknown;
    industry?: unknown;
    industryFlowReasons?: unknown;
    [key: string]: unknown;
  } | null;
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

export function resolveIndustryName(trendok: TrendOkLike | null | undefined): string | null {
  const values = trendok?.values;
  if (!values) return null;
  const em = values.emIndustry;
  if (typeof em === 'string' && em.trim()) return em.trim();
  const ind = values.industry;
  if (typeof ind === 'string' && ind.trim()) return ind.trim();
  return null;
}

export function isDefenseSector(industryName: string | null | undefined): boolean {
  const name = String(industryName || '').trim();
  if (!name) return false;
  return DEFENSE_SECTOR_KEYWORDS.some((kw) => name.includes(kw));
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

type NewEntryGateResult =
  | { ok: true; tag: MainlineTag | null; why: string }
  | { ok: false; tag: null; why: string };

/**
 * Hard gates for BUY/ADD: defense sector, intraday surge (>6%), mainline bind.
 * Fail-closed when industry missing or mainlineAllow not ready.
 * Surge with null/undefined intradayChgPct does not block (fail-open).
 */
export function evaluateNewEntryGates(opts: {
  industryName: string | null;
  mainlineAllow: MainlineAllowSet | null | undefined;
  intradayChgPct?: number | null;
}): NewEntryGateResult {
  const { industryName, mainlineAllow, intradayChgPct = null } = opts;
  if (!industryName) {
    return { ok: false, tag: null, why: 'MISSING_INDUSTRY' };
  }
  if (isDefenseSector(industryName)) {
    return { ok: false, tag: null, why: 'DEFENSE_SECTOR_BLOCK' };
  }
  if (isIntradaySurge(intradayChgPct)) {
    return { ok: false, tag: null, why: 'INTRADAY_SURGE_BLOCK' };
  }
  if (!mainlineAllow || !mainlineAllow.ready) {
    return { ok: false, tag: null, why: 'MAINLINE_DATA_UNAVAILABLE' };
  }
  if (!mainlineAllow.names.has(industryName)) {
    return { ok: false, tag: null, why: 'NOT_MAINLINE' };
  }
  const tag = mainlineAllow.byName.get(industryName) ?? null;
  const why =
    tag === 'MOMENTUM'
      ? 'MAINLINE_MOMENTUM'
      : tag === '5D_TOP3'
        ? 'MAINLINE_5D_TOP3'
        : 'MAINLINE_OK';
  return { ok: true, tag, why };
}

type HeldTrimGateResult = { trim: true; why: string } | { trim: false; why: null };

/**
 * Held-position trim gates (after EXIT / warn_reduce_half).
 * DEFEND forces TRIM; mainline fade TRIM only when allow-set is ready.
 */
export function evaluateHeldTrimGates(opts: {
  mode: ExecutionGateMode | null;
  industryName: string | null;
  mainlineAllow: MainlineAllowSet | null | undefined;
}): HeldTrimGateResult {
  const { mode, industryName, mainlineAllow } = opts;
  if (mode === 'DEFEND') {
    return { trim: true, why: 'GATE_DEFEND' };
  }
  if (mainlineAllow?.ready) {
    if (!industryName) {
      return { trim: true, why: 'MISSING_INDUSTRY' };
    }
    if (!mainlineAllow.names.has(industryName)) {
      return { trim: true, why: 'MAINLINE_FADE' };
    }
  }
  return { trim: false, why: null };
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
  mainlineAllow?: MainlineAllowSet | null;
  intradayChgPct?: number | null;
}): ExecutionActionCard {
  const {
    symbol,
    gate,
    trendok,
    position,
    currentPrice,
    mainlineAllow = null,
    intradayChgPct = null,
  } = opts;
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

  const industryName = resolveIndustryName(trendok);
  const entryGate = evaluateNewEntryGates({ industryName, mainlineAllow, intradayChgPct });
  // Mainline column independent of surge block (surge is an entry veto, not "off mainline")
  const mainlineOk = Boolean(
    industryName &&
      !isDefenseSector(industryName) &&
      mainlineAllow?.ready &&
      mainlineAllow.names.has(industryName),
  );
  const mainlineTag = mainlineOk
    ? (mainlineAllow!.byName.get(industryName!) ?? null)
    : null;
  const heldTrim = evaluateHeldTrimGates({ mode, industryName, mainlineAllow });

  let action: ExecutionAction = 'WATCH';
  let why = 'WATCH';

  if (held && (exitNow || priceAtOrBelowTrigger)) {
    action = 'EXIT';
    why = exitNow ? 'EXIT_NOW' : 'TRIGGER_HIT';
  } else if (held && warnHalf) {
    action = 'TRIM';
    why = 'WARN_REDUCE_HALF';
  } else if (held && heldTrim.trim) {
    action = 'TRIM';
    why = heldTrim.why;
  } else if (held && allowAttack && wantsBuy) {
    if (!entryGate.ok) {
      action = 'HOLD';
      why = entryGate.why;
    } else {
      action = 'ADD';
      why = entryGate.why;
    }
  } else if (held) {
    action = 'HOLD';
    why = allowAttack ? 'HOLD' : 'GATE_BLOCK_NEW';
  } else if (allowAttack && wantsBuy) {
    if (!entryGate.ok) {
      action = 'WATCH';
      why = entryGate.why;
    } else {
      action = 'BUY';
      why = entryGate.why;
    }
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
    mainlineOk,
    mainlineTag,
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
