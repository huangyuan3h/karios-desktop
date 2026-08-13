import type {
  ExecutionAction,
  ExecutionActionCard,
  ExecutionGate,
  ExecutionGateMode,
  ExecutionSource,
  MainlineTag,
} from '@karios/shared';

import type { MainlineAllowSet } from '@/lib/hot-industry-picks';
import { getShanghaiMinutes } from '@/lib/market-hours';
import { isEtfWatchlistSymbol } from '@/lib/symbols';
import { isGapUpWeakMarket, isIntradaySurge } from '@/lib/watchlist-metrics';

export const CHANDELIER_ARM_PNL_PCT = 10;
export const CHANDELIER_ATR_MULT = 2;
/** OPT-099: S-3 backtest-caliber exit lines (mirror of the engine / paper constants). */
export const S3_STOP_LOSS_PCT = 0.05; // cost drawdown floor (CN + HK), backtest stop_loss_pct=-5
export const S3_TRAILING_STOP_PCT = 0.08; // CN peak drawdown, backtest trailing_stop_pct=-8
export const S3_TRAILING_STOP_PCT_HK = 0.12; // HK peak drawdown (HK parallel line trailing -12)
export const S3_MAX_HOLD_DAYS = 60; // max_hold_days, engine / paper / health share
/** ETF fallback stop: max drawdown from entry cost (no trendok stop available). */
export const ETF_FALLBACK_MAX_LOSS_PCT = 0.05;
/** ETF fallback stop: max drawdown from peak once position is in profit (trail). */
export const ETF_FALLBACK_TRAIL_PCT = 0.07;
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
/** V6.3: hard cap for WEAK_ATTACK pioneer sleeve (pct points). */
export const WEAK_ATTACK_SINGLE_MAX_CAP_PCT = 5;

/** V7.0-02: max per-fire risk budget in % of account equity (risk-parity sizing). */
export const RISK_BUDGET_PCT = 0.5;
/** V7.0-02: suggested sizes below this (pct points) are rejected as too small to matter. */
export const RISK_MIN_SIZE_PCT = 2.5;
/** V7.0-02: ATR fallback stop distance = 2 × ATR% when no hard stop is known. */
export const RISK_FALLBACK_ATR_MULT = 2;

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
  /** V6.3: ok | no | recovering */
  trendStatus?: string | null;
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

export type SleeveExposureByMarket = {
  /** A-share single names (CN: prefix) in pct points. */
  cn: number;
  /** HK names (HK: prefix) in pct points. */
  hk: number;
  /** ETF baskets (ETF: prefix) in pct points; governed by the CN gate. */
  etf: number;
  /** Total across all markets. */
  total: number;
};

/** HK-index ETFs are HK exposure (e.g. 华夏恒生科技ETF tracks the HSTECH index) — count under the HK sleeve. */
const HK_INDEX_ETF_OVERRIDES: ReadonlySet<string> = new Set(['ETF:513180']);

/** Market bucket of a symbol for sleeve accounting (ETF trades under the CN gate). */
export function marketOfSymbol(symbol: string): 'cn' | 'hk' | 'etf' {
  if (HK_INDEX_ETF_OVERRIDES.has(String(symbol || '').trim().toUpperCase())) return 'hk';
  if (isEtfWatchlistSymbol(symbol)) return 'etf';
  if (String(symbol || '').toUpperCase().startsWith('HK:')) return 'hk';
  return 'cn';
}

/**
 * Per-market sleeve exposure: A-share / HK / ETF buckets sum positionPct
 * independently, so a CN ADD is not blocked (or reported) by HK exposure and
 * vice versa. ETFs are not single names — they count toward the CN sleeve.
 */
export function buildSleeveExposureByMarket(positions: PositionLike[]): SleeveExposureByMarket {
  const acc = { cn: 0, hk: 0, etf: 0, total: 0 };
  for (const pos of positions) {
    const pct = num(pos.positionPct);
    if (pct == null || pct <= 0) continue;
    acc[marketOfSymbol(pos.symbol)] += pct;
    acc.total += pct;
  }
  return acc;
}

/** Sleeve exposure that applies to a symbol's own market (ETF → CN sleeve). */
export function sleeveExposureForSymbol(
  byMarket: SleeveExposureByMarket,
  symbol: string,
): number {
  const m = marketOfSymbol(symbol);
  return m === 'hk' ? byMarket.hk : byMarket.cn + byMarket.etf;
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

/**
 * V7.0-01 / L3-P5: semantic factor-cluster cap (default 30%).
 * When the symbol's cluster is already at/over the cap, new BUY/ADD entries
 * in that cluster are blocked (existing positions are never force-sold).
 */
export function isCorrelationClusterBlocked(
  clusterExposurePct: number | null | undefined,
  capPct: number = CORRELATION_CLUSTER_CAP_PCT,
): boolean {
  if (typeof clusterExposurePct !== 'number' || !Number.isFinite(clusterExposurePct)) {
    return false;
  }
  return clusterExposurePct >= capPct;
}

/** V7.0-01: cluster exposure cap % (30) — mirrors backend CLUSTER_CAP_PCT. */
export const CORRELATION_CLUSTER_CAP_PCT = 30;

export type FireSizeSuggestion = {
  addPct: number;
  /** Binding constraint: clip | single | sector | sleeve | risk (V7.0-02) | correlation (V7.0-01). */
  note: 'clip' | 'single' | 'sector' | 'sleeve' | 'risk' | 'correlation';
  /** Stop distance (%) used by risk-parity sizing; null when sizing degraded to clip-only. */
  stopDistancePct: number | null;
};

/**
 * Suggested sleeve-weight add for BUY/ADD after single / sector / sleeve headroom.
 * V7.0-02: also binds to risk-parity size = RISK_BUDGET_PCT / stop-distance%,
 * where stop-distance% prefers the actual hard stop and falls back to 2 × ATR%.
 * Caps at DEFAULT_FIRE_CLIP_PCT unless room is smaller. Null if room < 0.1
 * or the risk-parity size falls below RISK_MIN_SIZE_PCT.
 */
export function suggestFireSizePct(opts: {
  positionPct?: number | null;
  industryName?: string | null;
  sectorExposureByIndustry?: Map<string, number> | null;
  sleeveExposurePct?: number | null;
  positionRangeHint?: string | null;
  clipPct?: number;
  /** V7.0-02: actual stop distance % (e.g. 10 = 10%); preferred over ATR fallback. */
  stopDistancePct?: number | null;
  /** V7.0-02: ATR14 fallback — used when stopDistancePct is unavailable. */
  atr14?: number | null;
  /** V7.0-02: reference price (current or entryTrigger) for ATR fallback. */
  referencePrice?: number | null;
  /** ETF (basket/index proxy): exempt from the 15% single-name cap. */
  isEtf?: boolean;
  /** V7.0-01 (L3-P5): remaining headroom in the symbol's semantic factor
   *  cluster (30% - current cluster exposure). Entering the min chain
   *  shrinks Suggest% as the cluster fills up. */
  roomCorrelation?: number | null;
  /** S-3 candidate (user-approved 2026-08-09): exempt from the 30% sector /
   *  correlation-cluster caps — the S-3 mainline strategy is concentrated by
   *  design (industry-cap experiments crashed the valid window). Other caps
   *  (clip / single / sleeve / risk) still bind. */
  isS3Candidate?: boolean;
}): FireSizeSuggestion | null {
  const clip =
    typeof opts.clipPct === 'number' && Number.isFinite(opts.clipPct) && opts.clipPct > 0
      ? opts.clipPct
      : DEFAULT_FIRE_CLIP_PCT;
  const current = num(opts.positionPct);
  const currentPct = current != null && current > 0 ? current : 0;
  // ETFs are index/sector baskets, not single names — no single-name cap.
  const roomSingle = opts.isEtf ? Number.POSITIVE_INFINITY : POSITION_SIZE_CAP_PCT - currentPct;

  const industry = String(opts.industryName || '').trim();
  let roomSector = Number.POSITIVE_INFINITY;
  if (!opts.isS3Candidate && industry && opts.sectorExposureByIndustry) {
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

  let riskCap = Number.POSITIVE_INFINITY;
  let stopDistancePct: number | null = null;
  const stopDist = num(opts.stopDistancePct);
  if (stopDist != null && stopDist > 0) {
    stopDistancePct = stopDist;
  } else {
    const atr = num(opts.atr14);
    const ref = num(opts.referencePrice);
    if (atr != null && atr > 0 && ref != null && ref > 0) {
      stopDistancePct = ((RISK_FALLBACK_ATR_MULT * atr) / ref) * 100;
    }
  }
  if (stopDistancePct != null) {
    riskCap = RISK_BUDGET_PCT / (stopDistancePct / 100);
    if (riskCap < RISK_MIN_SIZE_PCT) return null;
  }

  let roomCorrelation = Number.POSITIVE_INFINITY;
  if (!opts.isS3Candidate) {
    const corrRoom = num(opts.roomCorrelation);
    if (corrRoom != null && Number.isFinite(corrRoom)) {
      roomCorrelation = corrRoom;
    }
  }

  const room = Math.min(clip, roomSingle, roomSector, roomSleeve, riskCap, roomCorrelation);
  if (!Number.isFinite(room) || room < 0.1) return null;
  const addPct = Math.round(room * 10) / 10;

  const eps = 1e-6;
  let note: FireSizeSuggestion['note'] = 'clip';
  if (Math.abs(room - roomSleeve) < eps) note = 'sleeve';
  else if (Math.abs(room - roomSector) < eps) note = 'sector';
  else if (Math.abs(room - roomSingle) < eps) note = 'single';
  else if (
    Number.isFinite(riskCap) &&
    riskCap < clip &&
    Math.abs(room - riskCap) < eps
  ) {
    note = 'risk';
  } else if (
    Number.isFinite(roomCorrelation) &&
    roomCorrelation < clip &&
    Math.abs(room - roomCorrelation) < eps
  ) {
    note = 'correlation';
  }

  return { addPct, note, stopDistancePct };
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

/** Display: "卫星仓 17.0%（上限 10%）" or "卫星仓 0.0%（上限 —）". */
export function formatSleeveBudgetLabel(
  sleeveExposurePct: number,
  positionRangeHint: string | null | undefined,
): string {
  const sum = Number.isFinite(sleeveExposurePct) ? sleeveExposurePct : 0;
  const maxPct = parsePositionRangeHintMaxPct(positionRangeHint);
  const maxLabel = maxPct == null ? '—' : `${maxPct}%`;
  return `卫星仓 ${sum.toFixed(1)}%（上限 ${maxLabel}）`;
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
 * V6.3 WEAK_ATTACK: already past 14:30 gate unlock; still respect >14:50 closing lock.
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
  const minutes = getShanghaiMinutes(opts.now ?? new Date());
  if (mode === 'WEAK_ATTACK') {
    if (minutes > TIME_LOCK_CLOSE_MINUTES) {
      return { ok: false, why: 'MARKET_CLOSING_LOCK' };
    }
    return { ok: true, why: 'OK' };
  }
  if (mode !== 'DEFEND' && regime !== 'Weak') {
    return { ok: true, why: 'OK' };
  }
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

/** OPT-099: S-3 fixed -5% stop line from entry cost (backtest stop_loss_pct). */
function s3FixedHardStop(costPrice: number | null): number | null {
  if (costPrice == null || !Number.isFinite(costPrice) || costPrice <= 0) {
    return null;
  }
  return costPrice * (1 - S3_STOP_LOSS_PCT);
}

/** OPT-099: S-3 fixed trailing line = peak × (1-8% CN / 1-12% HK), armed from entry. */
function s3FixedTrail(opts: {
  hardStop: number | null;
  costPrice: number | null;
  maxPrice: number | null;
  current: number | null;
  isHk: boolean;
}): {
  trailArmed: boolean;
  peak: number | null;
  hardStop: number | null;
  trailStop: number | null;
  exitStop: number | null;
  trigger: number | null;
} {
  const { hardStop, maxPrice, isHk } = opts;
  const peak = maxPrice != null && Number.isFinite(maxPrice) ? maxPrice : null;
  const trailPct = isHk ? S3_TRAILING_STOP_PCT_HK : S3_TRAILING_STOP_PCT;
  const trailStop = peak != null ? peak * (1 - trailPct) : null;
  let exitStop: number | null = null;
  if (hardStop != null && trailStop != null) exitStop = Math.max(hardStop, trailStop);
  else if (hardStop != null) exitStop = hardStop;
  else if (trailStop != null) exitStop = trailStop;
  return {
    trailArmed: trailStop != null,
    peak,
    hardStop,
    trailStop,
    exitStop,
    trigger: exitStop,
  };
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
 * ETF price-drawdown stop used when trendok has no stopLossPrice (data-starved /
 * fallback mode, RuleType=ETF_FALLBACK). max(entry -5%, peak -7% when armed),
 * never above current. Mirrors the backend hard-stop shape from position inputs.
 */
export function deriveEtfFallbackStop(opts: {
  costPrice: number | null;
  maxPrice: number | null;
  current: number | null;
}): number | null {
  const cost = num(opts.costPrice);
  const current = num(opts.current);
  if (cost == null || current == null || current <= 0) return null;
  const peak =
    opts.maxPrice != null && Number.isFinite(opts.maxPrice) ? opts.maxPrice : null;
  const pnlPct = cost > 0 ? ((current - cost) / cost) * 100 : 0;
  let stop = Math.max(
    current * (1 - ETF_FALLBACK_MAX_LOSS_PCT),
    cost * (1 - ETF_FALLBACK_MAX_LOSS_PCT),
  );
  if (peak != null && pnlPct >= CHANDELIER_ARM_PNL_PCT) {
    stop = Math.max(stop, peak * (1 - ETF_FALLBACK_TRAIL_PCT));
  }
  return Math.min(stop, current);
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

/** Calendar days between two YYYY-MM-DD dates (0 when either is missing). */
export function daysBetweenDates(
  from: string | null | undefined,
  to: string | null | undefined,
): number {
  const a = String(from || '').trim().slice(0, 10);
  const b = String(to || '').trim().slice(0, 10);
  const ta = Date.parse(`${a}T00:00:00Z`);
  const tb = Date.parse(`${b}T00:00:00Z`);
  if (!Number.isFinite(ta) || !Number.isFinite(tb)) return 0;
  return Math.round((tb - ta) / 86400000);
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
  /** ETF: bypass industry/mainline gates (no-sector direct). */
  isEtf?: boolean;
  /** V7.0-01 / L3-P5: semantic factor-cluster exposure % of the symbol's
   *  cluster. >= 30% blocks new BUY/ADD (existing positions untouched). */
  clusterExposurePct?: number | null;
  /** S-3 candidate (user-approved 2026-08-09): exempt from the 30% sector /
   *  correlation-cluster entry blocks (mainline concentration is validated). */
  isS3Candidate?: boolean;
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
    isEtf = false,
    clusterExposurePct = null,
  } = opts;
  if (!isEtf) {
    if (!industryName) {
      return { ok: false, tag: null, why: 'MISSING_INDUSTRY' };
    }
    if (isDefenseSector(industryName)) {
      return { ok: false, tag: null, why: 'DEFENSE_SECTOR_BLOCK' };
    }
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
  if (!isEtf && !opts.isS3Candidate && isSectorConcentrationBlocked(industryName, sectorExposureByIndustry)) {
    return { ok: false, tag: null, why: 'SECTOR_CONC_BLOCK' };
  }
  if (isSleeveCapBlocked(sleeveExposurePct, positionRangeHint)) {
    return { ok: false, tag: null, why: 'SLEEVE_CAP_BLOCK' };
  }
  // V7.0-01: cluster cap blocks NEW entries only — existing positions are
  // never force-sold, so held symbols (ADD) skip the correlation check.
  // S-3 candidates (user-approved 2026-08-09) also skip it: mainline
  // concentration is a validated feature, not an accident.
  if (!opts.isS3Candidate && isCorrelationClusterBlocked(clusterExposurePct)) {
    return { ok: false, tag: null, why: 'CORRELATION_CAP_BLOCK' };
  }
  if (isEtf) {
    return { ok: true, tag: null, why: momentumSurgeAllow ? 'MOMENTUM_SURGE_ALLOW' : 'ETF_DIRECT' };
  }
  if (industryName == null) {
    return { ok: false, tag: null, why: 'MISSING_INDUSTRY' };
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
  /** ETF: skip mainline-fade/industry trims (no-sector, buy-and-hold). */
  skipMainlineFade?: boolean;
}): HeldTrimGateResult {
  const {
    mode,
    industryName,
    mainlineAllow,
    sectorOutflowBlock = false,
    exemptDefendTrim = false,
    skipMainlineFade = false,
  } = opts;
  if (mode === 'DEFEND' && !exemptDefendTrim) {
    return { trim: true, why: 'GATE_DEFEND' };
  }
  if (!skipMainlineFade && mainlineAllow?.ready) {
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
  /** TIP-011: provenance of the signal; null = unknown/pre-TIP-011. */
  source?: ExecutionSource | null;
  /** V7.0-01 / L3-P5: semantic factor-cluster exposure % for this symbol's
   *  cluster (from GET /api/backtest/correlation-status). Passed into the
   *  entry gates (>=30% blocks new BUY) and Suggest% headroom. */
  clusterExposurePct?: number | null;
  /** S-3 candidate (user-approved 2026-08-09): exempt from the 30% sector /
   *  correlation-cluster caps in the entry gates and Suggest% sizing. */
  isS3Candidate?: boolean;
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
    source = null,
    clusterExposurePct = null,
    isS3Candidate = false,
  } = opts;
  const held = isHeldPosition(position);
  const isEtf = isEtfWatchlistSymbol(symbol);
  const parts = (trendok?.stopLossParts ?? null) as Record<string, unknown> | null;
  const mode = gateMode(gate);
  const regime = marketRegime ?? gate?.marketRegime ?? null;
  const industryName = resolveIndustryName(trendok);
  const cost = num(position.costPrice);
  const maxPrice = num(position.maxPrice);
  const isHk = marketOfSymbol(symbol) === 'hk';
  const useDefensiveStop =
    mode === 'DEFEND' && isDefensiveSectorWhitelist(industryName);
  // OPT-099 (2026-08-13): held positions exit on the S-3 BACKTEST caliber —
  // fixed -5% stop from cost, NOT the trendok adaptive stop (vol bins 6-10%
  // + ATR buffer) which was never the backtested rule. The engine, live
  // paper, and the health card all use STOP_LOSS_PCT=-5.0; this makes the
  // watchlist/copy action derive from the exact same line. Flat rows keep
  // the trendok stop as an entry reference; ETFs keep their fallback shape.
  const trendHardStop =
    held && !isEtf
      ? s3FixedHardStop(cost)
      : num(trendok?.stopLossPrice);
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
  // ETF fallback (RuleType=ETF_FALLBACK): trendok data-starved (no stop) →
  // price-drawdown stop derived from entry cost / peak. Keeps the position
  // protected with a relative stop instead of 0/none hard-exit behaviour.
  let etfFallback = false;
  if (isEtf && held && hardStop == null) {
    const fb = deriveEtfFallbackStop({
      costPrice: cost,
      maxPrice,
      current: currentPrice,
    });
    if (fb != null) {
      hardStop = fb;
      etfFallback = true;
    }
  }
  // OPT-099: held stocks use the S-3 fixed trailing line (peak × (1-8%) CN /
  // (1-12%) HK), armed from entry — exactly the engine's trailing_stop_pct.
  // The ATR chandelier (peak − 2×ATR14, armed only at +10% PnL) stays for
  // ETFs and as the flat-row reference.
  const trail =
    held && !isEtf
      ? s3FixedTrail({
          hardStop,
          costPrice: cost,
          maxPrice,
          current: currentPrice,
          isHk,
        })
      : deriveTriggerAndTrail({
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

  // V7.0-02: stop distance % for risk-parity sizing.
  // Held (ADD) → (current − exitStop) / current; flat (BUY) → (ref − hardStop) / ref.
  const sizeRefPrice = held ? currentPrice : (entryTrigger ?? currentPrice);
  const sizeStopLevel = held ? exitStop : hardStop;
  let sizeStopDistancePct: number | null = null;
  if (sizeStopLevel != null && sizeRefPrice != null && sizeRefPrice > 0) {
    const sizeDist = ((sizeRefPrice - sizeStopLevel) / sizeRefPrice) * 100;
    if (Number.isFinite(sizeDist) && sizeDist > 0) sizeStopDistancePct = sizeDist;
  }
  // Compat: trigger = role-relevant level for journal / cond-order
  const trigger = held ? exitStop : entryTrigger;

  // OPT-097: structure signals (exit_now / warn_reduce_half) are no longer
  // exits for held positions (backtested: they truncate the trend leg).
  // Float tolerance: trailStop = peak - 2*ATR14 can land a hair below current
  // (e.g. 3.5999999999999996 vs 3.6); a touch within 1e-9 counts as hit.
  const PRICE_EPS = 1e-9;
  // Price-based triggers, ETF semantics: trail-stop touch → TRIM; only a
  // hard-stop (or real stop) breach → EXIT. Fallback stops are estimates, so
  // they never count as a hard-stop breach.
  const hardStopHit =
    held &&
    !etfFallback &&
    hardStop != null &&
    currentPrice != null &&
    Number.isFinite(currentPrice) &&
    currentPrice <= hardStop + PRICE_EPS;
  // ETF rule isolation: trend-structure exit_now is a TRIM-level warning (the
  // backend already downgrades it), but defensively downgrade any stray
  // exit_now from stale data here too — only a hard-stop price breach exits.
  const priceAtOrBelowTrigger =
    exitStop != null &&
    currentPrice != null &&
    Number.isFinite(currentPrice) &&
    currentPrice <= exitStop + PRICE_EPS;

  const allowAttack = mode === 'ATTACK' || mode === 'WEAK_ATTACK';
  const buyAction = String(trendok?.buyAction || '').toLowerCase();
  const score = num(trendok?.score);
  const scoreOk = score != null && score >= BUY_SCORE_MIN;
  const wantsBuy = buyAction === 'buy' && scoreOk;
  const trendOkFlag =
    typeof trendok?.trendOk === 'boolean' ? trendok.trendOk : null;
  const isRecovering =
    String(trendok?.trendStatus || '')
      .trim()
      .toLowerCase() === 'recovering';

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
    isEtf,
    clusterExposurePct,
    isS3Candidate,
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
    skipMainlineFade: isEtf,
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
  // OPT-099: max_hold_days=60 (engine / paper / health) — held beyond the
  // calendar-day cap exits regardless of price (backtested exit rule).
  const heldMaxHold =
    held && !missingEntryDate && daysBetweenDates(position.entryDate, todaySh) >= S3_MAX_HOLD_DAYS;

  const blockSellWhy = missingEntryDate
    ? 'ENTRY_DATE_MISSING'
    : lockedT1
      ? 'T1_LOCK'
      : null;

  // 2026-08-12 (OPT-097): S-3-only exits for held positions. The trendok
  // structure signals (exit_now / warn_reduce_half) and the sector-flow
  // trims (heldTrim) were NEVER backtested — per-trade counterfactuals
  // across all windows/markets show they truncate the trend leg (close<EMA20
  // exit: -511pt long-window vs holding to S-3 stop/trail rules). Held
  // positions now exit ONLY on the S-3 price/time rules (stop/trail lines
  // via priceAtOrBelowTrigger). Structure signals may still surface for
  // non-held watchlist rows (observation only).
  if (held && priceAtOrBelowTrigger) {
    if (blockSellWhy) {
      action = 'HOLD';
      why = blockSellWhy;
    } else if (isEtf && !hardStopHit) {
      // ETF: trail/fallback touched without a real hard-stop breach →
      // smooth TRIM (half), never a forced full exit.
      action = 'TRIM';
      why = etfFallback ? 'ETF_FALLBACK_TRIM' : 'TRAIL_STOP_TRIM';
    } else {
      action = 'EXIT';
      why = isEtf ? 'HARD_STOP_HIT' : 'TRIGGER_HIT';
    }
  } else if (held && heldMaxHold) {
    // OPT-099: backtest max_hold_days=60 — time-based exit, same rule the
    // engine / live paper / health card enforce (T+1 lock still fail-closed).
    if (blockSellWhy) {
      action = 'HOLD';
      why = blockSellWhy;
    } else {
      action = 'EXIT';
      why = 'MAX_HOLD';
    }
  } else if (held && allowAttack && wantsBuy) {
    if (!entryGate.ok) {
      action = 'HOLD';
      why = entryGate.why;
    } else if (!isEtf && isAtOrOverPositionSizeCap(position.positionPct)) {
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
    if (isRecovering) {
      action = 'WATCH';
      why = 'TREND_RECOVERING';
    } else if (isAlphaSPurgeExempt(catalyst ?? {})) {
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
  } else if (isRecovering) {
    action = 'WATCH';
    why = 'TREND_RECOVERING';
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
      stopDistancePct: sizeStopDistancePct,
      atr14,
      referencePrice: sizeRefPrice,
      isEtf,
      roomCorrelation:
        typeof clusterExposurePct === 'number' && Number.isFinite(clusterExposurePct)
          ? CORRELATION_CLUSTER_CAP_PCT - clusterExposurePct
          : null,
      isS3Candidate,
    });
    if (size) {
      suggestAddPct = size.addPct;
      suggestSizeNote = size.note;
    }
    // V6.3: WEAK_ATTACK pioneer sleeve hard-caps single-name Suggest% at 5.
    if (mode === 'WEAK_ATTACK' && suggestAddPct != null) {
      const capped = Math.min(suggestAddPct, WEAK_ATTACK_SINGLE_MAX_CAP_PCT);
      if (capped < suggestAddPct - 1e-9) {
        suggestSizeNote = 'overflow';
      }
      suggestAddPct = Math.round(capped * 10) / 10;
    }
  }

  return {
    symbol,
    action,
    source,
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
    /** V7.0-02: stop distance % that drove risk-parity sizing (display/diagnostic). */
    sizeStopDistancePct,
    /** ETF data-starved fallback mode (price-drawdown stop in use). */
    ruleType: etfFallback ? 'ETF_FALLBACK' : null,
  };
}

export function parseExecutionGate(raw: unknown): ExecutionGate | null {
  if (!raw || typeof raw !== 'object') return null;
  const o = raw as Record<string, unknown>;
  const mode = String(o.mode || '');
  if (
    mode !== 'ATTACK' &&
    mode !== 'WEAK_ATTACK' &&
    mode !== 'HOLD_ONLY' &&
    mode !== 'DEFEND'
  ) {
    return null;
  }
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
    upCount: num(o.upCount),
    riskMode: o.riskMode == null ? null : String(o.riskMode),
    reasons,
    positionRangeHint: o.positionRangeHint == null ? undefined : String(o.positionRangeHint),
    satelliteNote: o.satelliteNote == null ? undefined : String(o.satelliteNote),
    overflowSector: o.overflowSector == null ? null : String(o.overflowSector),
    overflowInflowYi: num(o.overflowInflowYi),
    cnGate: parseGateSubset(o.cnGate),
    hkGate: parseGateSubset(o.hkGate),
  };
}

function parseGateSubset(raw: unknown): ExecutionGate['hkGate'] {
  if (!raw || typeof raw !== 'object') return null;
  const o = raw as Record<string, unknown>;
  const mode = String(o.mode || '');
  if (
    mode !== 'ATTACK' &&
    mode !== 'WEAK_ATTACK' &&
    mode !== 'HOLD_ONLY' &&
    mode !== 'DEFEND'
  ) {
    return null;
  }
  const regime = String(o.marketRegime || '');
  if (regime !== 'Strong' && regime !== 'Diverging' && regime !== 'Weak') return null;
  const reasons = Array.isArray(o.reasons) ? o.reasons.map((x) => String(x)) : [];
  return {
    mode,
    allowNewEntries: Boolean(o.allowNewEntries),
    marketRegime: regime,
    indexLight: String(o.indexLight || '—'),
    riskMode: o.riskMode == null ? null : String(o.riskMode),
    reasons,
    positionRangeHint: o.positionRangeHint == null ? undefined : String(o.positionRangeHint),
    satelliteNote: o.satelliteNote == null ? undefined : String(o.satelliteNote),
  };
}
