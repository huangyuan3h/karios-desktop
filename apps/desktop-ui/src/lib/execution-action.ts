import type {
  ExecutionAction,
  ExecutionActionCard,
  ExecutionGate,
  ExecutionGateMode,
  MainlineTag,
} from '@karios/shared';

import type { MainlineAllowSet } from '@/lib/hot-industry-picks';
import { getShanghaiMinutes } from '@/lib/market-hours';
import { isGapUpWeakMarket, isIntradaySurge } from '@/lib/watchlist-metrics';

export const CHANDELIER_ARM_PNL_PCT = 10;
export const CHANDELIER_ATR_MULT = 2;
export const BUY_SCORE_MIN = 80;
/** TIP-007: Score floor for B_momentum intraday surge allow (6%→9%). */
export const MOMENTUM_SURGE_SCORE_MIN = 85;
/** TIP-007: Max intraday % when momentum surge allow applies. */
export const MOMENTUM_SURGE_ALLOW_MAX_PCT = 9;
/** Flat names below this score with TrendOK=no are marked PURGE. */
export const PURGE_SCORE_MAX = 30;
/** Max single-name weight inside the satellite sleeve; blocks ADD at or above. */
export const POSITION_SIZE_CAP_PCT = 15;
/** Max sum of positionPct in one East Money industry; blocks BUY/ADD at or above. */
export const SECTOR_CONCENTRATION_CAP_PCT = 30;
/** Default per-fire clip for BUY/ADD size suggestion (pct points). */
export const DEFAULT_FIRE_CLIP_PCT = 5;

/** V6.2: Weak/DEFEND buy window opens at 14:30 Shanghai. */
export const TIME_LOCK_CUTOFF_MINUTES = 14 * 60 + 30;
/** V6.2: After 14:50 Shanghai, new entries locked again. */
export const TIME_LOCK_CLOSE_MINUTES = 14 * 60 + 50;

/** V6.2 defensive sleeve: sectors allowed as hedge under DEFEND. */
export const DEFENSIVE_SECTOR_WHITELIST = [
  '石油石化',
  '公用事业',
  '煤炭',
  '银行',
  '有色金属',
] as const;
/** Independent defensive sleeve total cap (pct points). */
export const DEFENSIVE_SLEEVE_MAX_CAP_PCT = 10;
/** Per-name cap inside defensive sleeve. */
export const DEFENSIVE_SINGLE_MAX_CAP_PCT = 5;
/** Score floor for defensive sleeve probe buys (below growth BUY_SCORE_MIN). */
export const DEFENSIVE_BUY_SCORE_MIN = 70;
/** Defensive hard-stop max loss vs current (3.5%). */
export const DEFENSIVE_MAX_LOSS_PCT = 0.035;

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
  scoreParts?: Record<string, unknown> | null;
  trendOk?: boolean | null;
  buyAction?: string | null;
  buyMode?: string | null;
  buyZoneHigh?: number | null;
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
  /** Shanghai calendar YYYY-MM-DD when the live position was opened. */
  entryDate?: string | null;
};

/** Catalyst hint for PURGE exemption (from Alpha Radar Top Catalyst Stocks). */
export type CatalystPurgeHint = {
  maxGrade?: string | null;
  catalystScore?: number | null;
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

/** True when positionPct is a finite number at or above the single-name cap. */
export function isAtOrOverPositionSizeCap(
  positionPct: number | null | undefined,
  capPct: number = POSITION_SIZE_CAP_PCT,
): boolean {
  return typeof positionPct === 'number' && Number.isFinite(positionPct) && positionPct >= capPct;
}

/**
 * Sum positionPct by East Money industry for held names with a finite positive size.
 * Positions without positionPct are excluded (fail-open for incomplete books).
 */
export function buildSectorExposureByIndustry(
  rows: Array<{ industryName: string | null; position: PositionLike }>,
): Map<string, number> {
  const out = new Map<string, number>();
  for (const row of rows) {
    const industry = row.industryName?.trim();
    if (!industry) continue;
    const pct = num(row.position.positionPct);
    if (pct == null || pct <= 0) continue;
    out.set(industry, (out.get(industry) ?? 0) + pct);
  }
  return out;
}

/** Build exposure map from watchlist items + TrendOK industry fields. */
export function buildSectorExposureFromWatchlist(
  items: PositionLike[],
  trend: Record<string, TrendOkLike | null | undefined>,
): Map<string, number> {
  return buildSectorExposureByIndustry(
    items.map((position) => ({
      industryName: resolveIndustryName(trend[position.symbol] ?? null),
      position,
    })),
  );
}

export function isSectorConcentrationBlocked(
  industryName: string | null | undefined,
  exposureByIndustry: Map<string, number> | null | undefined,
  capPct: number = SECTOR_CONCENTRATION_CAP_PCT,
): boolean {
  if (!exposureByIndustry) return false;
  const industry = String(industryName || '').trim();
  if (!industry) return false;
  const sum = exposureByIndustry.get(industry);
  return typeof sum === 'number' && Number.isFinite(sum) && sum >= capPct;
}

/**
 * Parse Gate.positionRangeHint upper bound.
 * "50%-60%" → 60, "30%" → 30, "0%-10%" → 10, "—" / unparseable → null.
 */
export function parsePositionRangeHintMaxPct(hint: string | null | undefined): number | null {
  const raw = String(hint ?? '').trim();
  if (!raw || raw === '—' || raw === '-') return null;
  const range = raw.match(/(-?\d+(?:\.\d+)?)\s*%?\s*[-–—~]\s*(-?\d+(?:\.\d+)?)\s*%?/);
  if (range) {
    const a = Number(range[1]);
    const b = Number(range[2]);
    if (Number.isFinite(a) && Number.isFinite(b)) return Math.max(a, b);
  }
  const single = raw.match(/(-?\d+(?:\.\d+)?)\s*%?/);
  if (single) {
    const n = Number(single[1]);
    if (Number.isFinite(n)) return n;
  }
  return null;
}

/** Sum finite positive positionPct across the satellite sleeve watchlist. */
export function buildSleeveExposurePct(positions: PositionLike[]): number {
  let sum = 0;
  for (const pos of positions) {
    const pct = num(pos.positionPct);
    if (pct != null && pct > 0) sum += pct;
  }
  return sum;
}

export function isSleeveCapBlocked(
  sleeveExposurePct: number | null | undefined,
  positionRangeHint: string | null | undefined,
): boolean {
  if (typeof sleeveExposurePct !== 'number' || !Number.isFinite(sleeveExposurePct)) return false;
  const maxPct = parsePositionRangeHintMaxPct(positionRangeHint);
  if (maxPct == null) return false;
  return sleeveExposurePct >= maxPct;
}

export type FireSizeSuggestion = {
  addPct: number;
  note: 'clip' | 'single' | 'sector' | 'sleeve';
};

/**
 * Suggested sleeve-weight add for BUY/ADD after single / sector / sleeve headroom.
 * Caps at DEFAULT_FIRE_CLIP_PCT unless room is smaller. Null if room < 0.1.
 */
export function suggestFireSizePct(opts: {
  positionPct?: number | null;
  industryName?: string | null;
  sectorExposureByIndustry?: Map<string, number> | null;
  sleeveExposurePct?: number | null;
  positionRangeHint?: string | null;
  clipPct?: number;
}): FireSizeSuggestion | null {
  const clip =
    typeof opts.clipPct === 'number' && Number.isFinite(opts.clipPct) && opts.clipPct > 0
      ? opts.clipPct
      : DEFAULT_FIRE_CLIP_PCT;
  const current = num(opts.positionPct);
  const currentPct = current != null && current > 0 ? current : 0;
  const roomSingle = POSITION_SIZE_CAP_PCT - currentPct;

  const industry = String(opts.industryName || '').trim();
  let roomSector = Number.POSITIVE_INFINITY;
  if (industry && opts.sectorExposureByIndustry) {
    const sum = opts.sectorExposureByIndustry.get(industry);
    const sectorSum = typeof sum === 'number' && Number.isFinite(sum) ? sum : 0;
    roomSector = SECTOR_CONCENTRATION_CAP_PCT - sectorSum;
  }

  const sleeveMax = parsePositionRangeHintMaxPct(opts.positionRangeHint);
  let roomSleeve = Number.POSITIVE_INFINITY;
  if (sleeveMax != null) {
    const sleeve =
      typeof opts.sleeveExposurePct === 'number' && Number.isFinite(opts.sleeveExposurePct)
        ? opts.sleeveExposurePct
        : 0;
    roomSleeve = sleeveMax - sleeve;
  }

  const room = Math.min(clip, roomSingle, roomSector, roomSleeve);
  if (!Number.isFinite(room) || room < 0.1) return null;
  const addPct = Math.round(room * 10) / 10;

  const eps = 1e-6;
  let note: FireSizeSuggestion['note'] = 'clip';
  if (Math.abs(room - roomSleeve) < eps) note = 'sleeve';
  else if (Math.abs(room - roomSector) < eps) note = 'sector';
  else if (Math.abs(room - roomSingle) < eps) note = 'single';

  return { addPct, note };
}

/** Held names with no finite positive positionPct (caps fail-open for these). */
export function countHeldMissingPositionPct(positions: PositionLike[]): number {
  let n = 0;
  for (const pos of positions) {
    if (!isHeldPosition(pos)) continue;
    const pct = num(pos.positionPct);
    if (pct == null || pct <= 0) n += 1;
  }
  return n;
}

/** True when held but positionPct is missing / non-positive. */
export function isHeldMissingPositionPct(pos: PositionLike): boolean {
  if (!isHeldPosition(pos)) return false;
  const pct = num(pos.positionPct);
  return pct == null || pct <= 0;
}

/** Display: "Sleeve 45.0% / 60%" or "Sleeve 0.0% / —". */
export function formatSleeveBudgetLabel(
  sleeveExposurePct: number,
  positionRangeHint: string | null | undefined,
): string {
  const sum = Number.isFinite(sleeveExposurePct) ? sleeveExposurePct : 0;
  const maxPct = parsePositionRangeHintMaxPct(positionRangeHint);
  const maxLabel = maxPct == null ? '—' : `${maxPct}%`;
  return `Sleeve ${sum.toFixed(1)}% / ${maxLabel}`;
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
  // Mirror BE watchlist_automation.is_defense_sector: do not treat 电力设备 as utilities.
  for (const kw of DEFENSE_SECTOR_KEYWORDS) {
    if (kw === '电力') {
      if (name === '电力' || (name.includes('电力') && !name.includes('电力设备'))) return true;
      continue;
    }
    if (name.includes(kw)) return true;
  }
  return false;
}

/** V6.2: True when industry matches defensive sleeve whitelist. */
export function isDefensiveSectorWhitelist(industryName: string | null | undefined): boolean {
  const name = String(industryName || '').trim();
  if (!name) return false;
  for (const kw of DEFENSIVE_SECTOR_WHITELIST) {
    if (name === kw || name.includes(kw)) return true;
  }
  return false;
}

function hasIndustryFlow5dTop3(reasons: unknown): boolean {
  if (!Array.isArray(reasons)) return false;
  return reasons.some((r) => String(r) === 'industry_flow_5d_top3');
}

/** Industry is in 5D net-inflow Top3 via mainline tag or TrendOK flow reasons. */
export function isIndustryIn5dTop3(opts: {
  industryName: string | null | undefined;
  mainlineAllow?: MainlineAllowSet | null;
  industryFlowReasons?: unknown;
}): boolean {
  const industry = String(opts.industryName || '').trim();
  if (!industry) return false;
  if (opts.mainlineAllow?.byName.get(industry) === '5D_TOP3') return true;
  return hasIndustryFlow5dTop3(opts.industryFlowReasons);
}

/**
 * V6.2 TimeLock: under DEFEND or Weak, only allow BUY/ADD in 14:30–14:50 Shanghai.
 * Exempt when ATTACK + Strong.
 */
export function checkExecutionTimeLock(opts: {
  now?: Date | null;
  gateMode?: ExecutionGateMode | null;
  marketRegime?: string | null;
}): { ok: true; why: 'OK' } | { ok: false; why: string } {
  const mode = opts.gateMode ?? null;
  const regime = String(opts.marketRegime || '');
  if (mode === 'ATTACK' && regime === 'Strong') {
    return { ok: true, why: 'OK' };
  }
  if (mode !== 'DEFEND' && regime !== 'Weak') {
    return { ok: true, why: 'OK' };
  }
  const minutes = getShanghaiMinutes(opts.now ?? new Date());
  if (minutes < TIME_LOCK_CUTOFF_MINUTES) {
    return { ok: false, why: 'TIME_LOCK_WEAK_REGIME' };
  }
  if (minutes > TIME_LOCK_CLOSE_MINUTES) {
    return { ok: false, why: 'MARKET_CLOSING_LOCK' };
  }
  return { ok: true, why: 'OK' };
}

/**
 * V6.2 defensive sleeve eligibility (beta deferred).
 * Does not check sleeve capacity — caller surfaces SLEEVE_CAP_BLOCK separately.
 */
export function isDefensiveArbitrageEligible(opts: {
  gateMode?: ExecutionGateMode | null;
  industryName?: string | null;
  mainlineAllow?: MainlineAllowSet | null;
  industryFlowReasons?: unknown;
  score?: number | null;
  trendOk?: boolean | null;
}): boolean {
  if (opts.gateMode !== 'DEFEND') return false;
  if (!isDefensiveSectorWhitelist(opts.industryName)) return false;
  if (opts.trendOk !== true) return false;
  const score = num(opts.score);
  if (score == null || score < DEFENSIVE_BUY_SCORE_MIN) return false;
  return isIndustryIn5dTop3({
    industryName: opts.industryName,
    mainlineAllow: opts.mainlineAllow,
    industryFlowReasons: opts.industryFlowReasons,
  });
}

/** Sum positionPct for whitelist defensive industries only. */
export function buildDefensiveSleeveExposurePct(
  items: PositionLike[],
  trend: Record<string, TrendOkLike | null | undefined>,
): number {
  let sum = 0;
  for (const pos of items) {
    const pct = num(pos.positionPct);
    if (pct == null || pct <= 0) continue;
    const industry = resolveIndustryName(trend[pos.symbol] ?? null);
    if (!isDefensiveSectorWhitelist(industry)) continue;
    sum += pct;
  }
  return sum;
}

/**
 * Tighter defensive hard stop: max(EMA10, current×0.965).
 * Higher price = tighter for long book. Missing ema10 → loss floor only.
 */
export function resolveDefensiveHardStop(opts: {
  ema10?: number | null;
  currentPrice?: number | null;
}): number | null {
  const current = num(opts.currentPrice);
  const ema10 = num(opts.ema10);
  const lossFloor =
    current != null && current > 0 ? current * (1 - DEFENSIVE_MAX_LOSS_PCT) : null;
  if (ema10 != null && lossFloor != null) return Math.max(ema10, lossFloor);
  if (ema10 != null) return ema10;
  return lossFloor;
}

export function ema10FromTrendok(trendok: TrendOkLike | null | undefined): number | null {
  return num(trendok?.values?.ema10);
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
  /** Effective exit stop = max(hardStop, trailStop). */
  exitStop: number | null;
  /** @deprecated Alias of exitStop for older callers/tests. */
  trigger: number | null;
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

  let exitStop: number | null = null;
  if (hardStop != null && trailStop != null) exitStop = Math.max(hardStop, trailStop);
  else if (hardStop != null) exitStop = hardStop;
  else if (trailStop != null) exitStop = trailStop;

  return { trailArmed, peak, hardStop, trailStop, exitStop, trigger: exitStop };
}

/** Flat + Score<30 + TrendOK=no → physical-GC candidate (Action=PURGE). */
export function isPurgeCandidate(opts: {
  held: boolean;
  score: number | null;
  trendOk: boolean | null | undefined;
}): boolean {
  const { held, score, trendOk } = opts;
  return !held && score != null && score < PURGE_SCORE_MAX && trendOk === false;
}

/**
 * Alpha Radar S-grade catalyst exemption from instant PURGE.
 * IF Max Grade == S → keep as WATCH_SILENT (ignore catalystScore / TrendOK score).
 */
export function isAlphaSPurgeExempt(opts: {
  maxGrade?: string | null;
  catalystScore?: number | null;
}): boolean {
  const grade = String(opts.maxGrade || '')
    .trim()
    .toUpperCase();
  return grade === 'S';
}

/** A-share T+1: same Shanghai calendar day as entryDate cannot sell. */
export function isLockedT1(
  entryDate: string | null | undefined,
  todaySh: string | null | undefined,
): boolean {
  const d = String(entryDate || '').trim();
  const today = String(todaySh || '').trim();
  return Boolean(d && today && d === today);
}

/** Held position with no entryDate — fail-closed for sells (cannot prove T+1 unlock). */
export function isMissingEntryDate(entryDate: string | null | undefined): boolean {
  return !String(entryDate || '').trim();
}

/** True when Entry_Trigger (buyZoneHigh) is at or below HardStop — buy would instant-stop. */
export function isEntryAtOrBelowHardStop(
  entryTrigger: number | null | undefined,
  hardStop: number | null | undefined,
): boolean {
  return (
    typeof entryTrigger === 'number' &&
    Number.isFinite(entryTrigger) &&
    typeof hardStop === 'number' &&
    Number.isFinite(hardStop) &&
    entryTrigger <= hardStop
  );
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
 * Score used for TIP-007 surge-allow gate.
 * Anti-Spike keeps −20 on the displayed score; eligibility restores only
 * `penalty_intraday_spike` so a perfect post-spike 80 can still clear Score≥85.
 */
export function scoreForMomentumSurgeGate(
  score: number | null | undefined,
  scoreParts?: Record<string, unknown> | null,
): number | null {
  const base = num(score);
  if (base == null) return null;
  const raw = scoreParts?.penalty_intraday_spike;
  const spike = typeof raw === 'number' && Number.isFinite(raw) ? raw : 0;
  // scoreParts store penalties as negatives (e.g. -20).
  return spike < 0 ? base - spike : base;
}

/**
 * TIP-007: momentum/score/gate inputs for the 6–9% surge exception.
 * Mainline membership is enforced later inside evaluateNewEntryGates (not here).
 */
export function isMomentumSurgeEligible(opts: {
  gateMode?: ExecutionGateMode | null;
  buyMode?: string | null;
  trendOk?: boolean | null;
  score?: number | null;
  scoreParts?: Record<string, unknown> | null;
}): boolean {
  if (opts.gateMode !== 'ATTACK') return false;
  if (String(opts.buyMode || '').trim() !== 'B_momentum') return false;
  if (opts.trendOk !== true) return false;
  const score = scoreForMomentumSurgeGate(opts.score, opts.scoreParts);
  return score != null && score >= MOMENTUM_SURGE_SCORE_MIN;
}

/**
 * Hard gates for BUY/ADD: defense, surge, gap-up weak, sector concentration, mainline.
 * Fail-closed when industry missing or mainlineAllow not ready.
 * Surge/gap/concentration with null inputs do not block (fail-open).
 */
export function evaluateNewEntryGates(opts: {
  industryName: string | null;
  mainlineAllow: MainlineAllowSet | null | undefined;
  intradayChgPct?: number | null;
  gapUp?: boolean | null;
  marketRegime?: string | null;
  sectorExposureByIndustry?: Map<string, number> | null;
  sleeveExposurePct?: number | null;
  positionRangeHint?: string | null;
  /** When all sectors show net outflow on the day. */
  sectorOutflowBlock?: boolean;
  gateMode?: ExecutionGateMode | null;
  buyMode?: string | null;
  trendOk?: boolean | null;
  score?: number | null;
  scoreParts?: Record<string, unknown> | null;
  /** Clock for TimeLock; defaults to now. */
  now?: Date | null;
}): NewEntryGateResult {
  const {
    industryName,
    mainlineAllow,
    intradayChgPct = null,
    gapUp = null,
    marketRegime = null,
    sectorExposureByIndustry = null,
    sleeveExposurePct = null,
    positionRangeHint = null,
    sectorOutflowBlock = false,
    gateMode: mode = null,
    buyMode = null,
    trendOk = null,
    score = null,
    scoreParts = null,
    now = null,
  } = opts;
  if (!industryName) {
    return { ok: false, tag: null, why: 'MISSING_INDUSTRY' };
  }
  if (isDefenseSector(industryName)) {
    return { ok: false, tag: null, why: 'DEFENSE_SECTOR_BLOCK' };
  }

  const timeLock = checkExecutionTimeLock({
    now,
    gateMode: mode,
    marketRegime,
  });
  if (!timeLock.ok) {
    return { ok: false, tag: null, why: timeLock.why };
  }

  let momentumSurgeAllow = false;
  if (isIntradaySurge(intradayChgPct)) {
    const eligible = isMomentumSurgeEligible({
      gateMode: mode,
      buyMode,
      trendOk,
      score,
      scoreParts,
    });
    const pct = typeof intradayChgPct === 'number' ? intradayChgPct : null;
    if (
      !eligible ||
      pct == null ||
      !Number.isFinite(pct) ||
      pct > MOMENTUM_SURGE_ALLOW_MAX_PCT
    ) {
      return { ok: false, tag: null, why: 'INTRADAY_SURGE_BLOCK' };
    }
    momentumSurgeAllow = true;
  }

  if (isGapUpWeakMarket(gapUp, marketRegime)) {
    return { ok: false, tag: null, why: 'GAP_UP_WEAK_BLOCK' };
  }
  if (isSectorConcentrationBlocked(industryName, sectorExposureByIndustry)) {
    return { ok: false, tag: null, why: 'SECTOR_CONC_BLOCK' };
  }
  if (isSleeveCapBlocked(sleeveExposurePct, positionRangeHint)) {
    return { ok: false, tag: null, why: 'SLEEVE_CAP_BLOCK' };
  }
  if (!mainlineAllow || !mainlineAllow.ready) {
    return { ok: false, tag: null, why: 'MAINLINE_DATA_UNAVAILABLE' };
  }
  if (!mainlineAllow.names.has(industryName)) {
    return {
      ok: false,
      tag: null,
      why: sectorOutflowBlock ? 'SECTOR_OUTFLOW_BLOCK' : 'NOT_MAINLINE',
    };
  }
  const tag = mainlineAllow.byName.get(industryName) ?? null;
  if (momentumSurgeAllow) {
    return { ok: true, tag, why: 'MOMENTUM_SURGE_ALLOW' };
  }
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
 * DEFEND forces TRIM unless exemptDefendTrim (defensive sleeve holdings).
 * Mainline fade TRIM only when allow-set is ready.
 */
export function evaluateHeldTrimGates(opts: {
  mode: ExecutionGateMode | null;
  industryName: string | null;
  mainlineAllow: MainlineAllowSet | null | undefined;
  sectorOutflowBlock?: boolean;
  /** V6.2: skip GATE_DEFEND for defensive-sleeve whitelist holdings. */
  exemptDefendTrim?: boolean;
}): HeldTrimGateResult {
  const {
    mode,
    industryName,
    mainlineAllow,
    sectorOutflowBlock = false,
    exemptDefendTrim = false,
  } = opts;
  if (mode === 'DEFEND' && !exemptDefendTrim) {
    return { trim: true, why: 'GATE_DEFEND' };
  }
  if (mainlineAllow?.ready) {
    if (!industryName) {
      return { trim: true, why: 'MISSING_INDUSTRY' };
    }
    if (!mainlineAllow.names.has(industryName)) {
      return {
        trim: true,
        why: sectorOutflowBlock ? 'SECTOR_OUTFLOW_BLOCK' : 'MAINLINE_FADE',
      };
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
  gapUp?: boolean | null;
  marketRegime?: string | null;
  sectorExposureByIndustry?: Map<string, number> | null;
  sleeveExposurePct?: number | null;
  /** V6.2: sum of whitelist defensive positionPct (independent sleeve). */
  defensiveSleeveExposurePct?: number | null;
  sectorOutflowBlock?: boolean;
  /** Alpha Radar catalyst hint; when present, may exempt PURGE. */
  catalyst?: CatalystPurgeHint | null;
  /** Shanghai YYYY-MM-DD for T+1 lock (entryDate === todaySh). */
  todaySh?: string | null;
  /** Clock for TimeLock; defaults to now. */
  now?: Date | null;
}): ExecutionActionCard {
  const {
    symbol,
    gate,
    trendok,
    position,
    currentPrice,
    mainlineAllow = null,
    intradayChgPct = null,
    gapUp = null,
    marketRegime = null,
    sectorExposureByIndustry = null,
    sleeveExposurePct = null,
    defensiveSleeveExposurePct = null,
    sectorOutflowBlock = false,
    catalyst = null,
    todaySh = null,
    now = null,
  } = opts;
  const held = isHeldPosition(position);
  const parts = (trendok?.stopLossParts ?? null) as Record<string, unknown> | null;
  const mode = gateMode(gate);
  const regime = marketRegime ?? gate?.marketRegime ?? null;
  const industryName = resolveIndustryName(trendok);
  const useDefensiveStop =
    mode === 'DEFEND' && isDefensiveSectorWhitelist(industryName);
  const trendHardStop = num(trendok?.stopLossPrice);
  const defensiveStop = useDefensiveStop
    ? resolveDefensiveHardStop({
        ema10: ema10FromTrendok(trendok),
        currentPrice,
      })
    : null;
  // Prefer tighter (higher) stop when both exist.
  let hardStop = trendHardStop;
  if (defensiveStop != null) {
    hardStop =
      hardStop != null ? Math.max(hardStop, defensiveStop) : defensiveStop;
  }
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

  const exitStop = held ? trail.exitStop : null;
  const entryTrigger = held ? null : num(trendok?.buyZoneHigh);
  let distPct: number | null = null;
  if (held) {
    if (exitStop != null && currentPrice != null && currentPrice > 0) {
      distPct = ((currentPrice - exitStop) / currentPrice) * 100;
    }
  } else if (entryTrigger != null && currentPrice != null && currentPrice > 0) {
    distPct = ((entryTrigger - currentPrice) / currentPrice) * 100;
  }
  // Compat: trigger = role-relevant level for journal / cond-order
  const trigger = held ? exitStop : entryTrigger;

  const exitNow = Boolean(parts?.exit_now);
  const warnHalf = Boolean(parts?.warn_reduce_half);
  const priceAtOrBelowTrigger =
    exitStop != null &&
    currentPrice != null &&
    Number.isFinite(currentPrice) &&
    currentPrice <= exitStop;

  const allowAttack = mode === 'ATTACK';
  const buyAction = String(trendok?.buyAction || '').toLowerCase();
  const score = num(trendok?.score);
  const scoreOk = score != null && score >= BUY_SCORE_MIN;
  const wantsBuy = buyAction === 'buy' && scoreOk;
  const trendOkFlag =
    typeof trendok?.trendOk === 'boolean' ? trendok.trendOk : null;

  const entryGate = evaluateNewEntryGates({
    industryName,
    mainlineAllow,
    intradayChgPct,
    gapUp,
    marketRegime: regime,
    sectorExposureByIndustry,
    sleeveExposurePct,
    positionRangeHint: gate?.positionRangeHint ?? null,
    sectorOutflowBlock,
    gateMode: mode,
    buyMode: trendok?.buyMode ?? null,
    trendOk: trendOkFlag,
    score,
    scoreParts: (trendok?.scoreParts as Record<string, unknown> | null | undefined) ?? null,
    now,
  });
  // Mainline column independent of chase / concentration vetoes
  const mainlineOk = Boolean(
    industryName &&
      !isDefenseSector(industryName) &&
      mainlineAllow?.ready &&
      mainlineAllow.names.has(industryName),
  );
  const mainlineTag = mainlineOk
    ? (mainlineAllow!.byName.get(industryName!) ?? null)
    : null;
  const defensiveHolding = isDefensiveSectorWhitelist(industryName);
  const heldTrim = evaluateHeldTrimGates({
    mode,
    industryName,
    mainlineAllow,
    sectorOutflowBlock,
    exemptDefendTrim: defensiveHolding,
  });

  const defensiveUsed =
    typeof defensiveSleeveExposurePct === 'number' &&
    Number.isFinite(defensiveSleeveExposurePct)
      ? defensiveSleeveExposurePct
      : 0;
  const defensiveEligible =
    !held &&
    isDefensiveArbitrageEligible({
      gateMode: mode,
      industryName,
      mainlineAllow,
      industryFlowReasons: trendok?.values?.industryFlowReasons,
      score,
      trendOk: trendOkFlag,
    });

  let action: ExecutionAction = 'WATCH';
  let why = 'WATCH';

  const lockedT1 = held && isLockedT1(position.entryDate, todaySh);
  const missingEntryDate = held && isMissingEntryDate(position.entryDate);
  const entryBelowStop =
    !held && isEntryAtOrBelowHardStop(entryTrigger, hardStop);

  const blockSellWhy = missingEntryDate
    ? 'ENTRY_DATE_MISSING'
    : lockedT1
      ? 'T1_LOCK'
      : null;

  if (held && (exitNow || priceAtOrBelowTrigger)) {
    if (blockSellWhy) {
      action = 'HOLD';
      why = blockSellWhy;
    } else {
      action = 'EXIT';
      why = exitNow ? 'EXIT_NOW' : 'TRIGGER_HIT';
    }
  } else if (held && warnHalf) {
    if (blockSellWhy) {
      action = 'HOLD';
      why = blockSellWhy;
    } else {
      action = 'TRIM';
      why = 'WARN_REDUCE_HALF';
    }
  } else if (held && heldTrim.trim) {
    if (blockSellWhy) {
      action = 'HOLD';
      why = blockSellWhy;
    } else {
      action = 'TRIM';
      why = heldTrim.why;
    }
  } else if (held && allowAttack && wantsBuy) {
    if (!entryGate.ok) {
      action = 'HOLD';
      why = entryGate.why;
    } else if (isAtOrOverPositionSizeCap(position.positionPct)) {
      action = 'HOLD';
      why = 'SIZE_CAP_BLOCK';
    } else {
      action = 'ADD';
      why = entryGate.why;
    }
  } else if (held) {
    action = 'HOLD';
    why = allowAttack ? 'HOLD' : 'GATE_BLOCK_NEW';
  } else if (
    isPurgeCandidate({ held: false, score, trendOk: trendOkFlag })
  ) {
    if (isAlphaSPurgeExempt(catalyst ?? {})) {
      action = 'WATCH_SILENT';
      why = 'ALPHA_S_WATCH';
    } else {
      action = 'PURGE';
      why = 'PURGE_GC';
    }
  } else if (defensiveEligible) {
    if (defensiveUsed >= DEFENSIVE_SLEEVE_MAX_CAP_PCT) {
      action = 'WATCH';
      why = 'SLEEVE_CAP_BLOCK';
    } else {
      const timeLock = checkExecutionTimeLock({
        now,
        gateMode: mode,
        marketRegime: regime,
      });
      if (!timeLock.ok) {
        action = 'WATCH';
        why = timeLock.why;
      } else if (entryBelowStop) {
        action = 'WATCH';
        why = 'ENTRY_BELOW_STOP';
      } else {
        action = 'BUY';
        why = 'DEFENSIVE_SLEEVE_ALLOW';
      }
    }
  } else if (allowAttack && wantsBuy) {
    if (entryBelowStop) {
      action = 'WATCH';
      why = 'ENTRY_BELOW_STOP';
    } else if (!entryGate.ok) {
      action = 'WATCH';
      why = entryGate.why;
    } else {
      action = 'BUY';
      why = entryGate.why;
    }
  } else if (!allowAttack && wantsBuy) {
    const timeLock = checkExecutionTimeLock({
      now,
      gateMode: mode,
      marketRegime: regime,
    });
    if (!timeLock.ok) {
      action = 'WATCH';
      why = timeLock.why;
    } else {
      action = 'WATCH';
      why = entryBelowStop ? 'ENTRY_BELOW_STOP' : 'GATE_BLOCK_NEW';
    }
  } else {
    action = 'WATCH';
    why =
      !mainlineOk && sectorOutflowBlock ? 'SECTOR_OUTFLOW_BLOCK' : 'WATCH';
  }

  let suggestAddPct: number | null = null;
  let suggestSizeNote: string | null = null;
  if (action === 'BUY' && why === 'DEFENSIVE_SLEEVE_ALLOW') {
    const roomSleeve = DEFENSIVE_SLEEVE_MAX_CAP_PCT - defensiveUsed;
    const room = Math.min(DEFENSIVE_SINGLE_MAX_CAP_PCT, roomSleeve);
    if (Number.isFinite(room) && room >= 0.1) {
      suggestAddPct = Math.round(room * 10) / 10;
      suggestSizeNote = roomSleeve <= DEFENSIVE_SINGLE_MAX_CAP_PCT + 1e-6 ? 'sleeve' : 'clip';
    }
  } else if (action === 'BUY' || action === 'ADD') {
    const size = suggestFireSizePct({
      positionPct: position.positionPct,
      industryName,
      sectorExposureByIndustry,
      sleeveExposurePct,
      positionRangeHint: gate?.positionRangeHint ?? null,
    });
    if (size) {
      suggestAddPct = size.addPct;
      suggestSizeNote = size.note;
    }
  }

  return {
    symbol,
    action,
    trailArmed: trail.trailArmed,
    peak: trail.peak,
    hardStop: trail.hardStop,
    trailStop: trail.trailStop,
    trigger,
    entryTrigger,
    exitStop,
    distPct,
    why,
    mainlineOk,
    mainlineTag,
    suggestAddPct,
    suggestSizeNote,
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
