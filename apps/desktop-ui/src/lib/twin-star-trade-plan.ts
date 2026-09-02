/**
 * Opportunity Twin-Star live trade plan: names + % of total NAV.
 *
 * Two books must not be mixed:
 *   recipe sat book = engine replay (openPositions) — paper state, not the broker
 *   live stocks/ETFs = what the user actually recorded with a position
 *
 * Slot fill uses the live satellite book. A full recipe replay does not block
 * buys when the user never took those names. Core STOCK with 0 executable
 * names must not dump ETFs into an empty basket.
 *
 * Sizing matches the frozen engine (state_bucket_track):
 *   satellite slot = 25% of the sat sleeve → satTargetPct * 0.25 of NAV
 *   (4 names × 12.5% NAV fill the 50% sat sleeve)
 *   core STOCK name = S-3 suggestedSizePct of the core sleeve
 */

import { isCnWatchlistSymbol, isEtfWatchlistSymbol, toTsCodeFromSymbol, tsCodeToWatchlistSymbol } from '@/lib/symbols';
import type { TwinStarSatCandidate, TwinStarSatHolding } from '@/lib/queries/backtest';
import type { PortfolioCandidate } from '@/lib/queries/portfolioHealth';

export const SAT_MAX_POS = 4;
export const SAT_SLOT_OF_SLEEVE = 0.25;
/** Broker protective stop from cost. Not part of the S-gap backtest (body=3 only). */
export const SAT_PROTECT_STOP_PCT = 0.05;
const ISO_DAY = /^\d{4}-\d{2}-\d{2}$/;
/** Frozen live mapping of opportunity twin-star v3.1 clip4 — lockstep with state_bucket_track.py. */
export const TWIN_STAR_LIVE_RECIPE = {
  core: 'pick_strong mom_compare trail8',
  sat: 'S-gap',
  gate: 'R-wide breadth>0.5',
  pool: 'strict skip_t1_limit',
  bucketQ: 3,
  maxPos: SAT_MAX_POS,
  slotOfSleeve: SAT_SLOT_OF_SLEEVE,
  body: 3,
} as const;

export function twinStarRecipeLine(slotNavPct: number): string {
  return `口径核 · 核心择强 mom_compare · 卫星 ${TWIN_STAR_LIVE_RECIPE.sat} ${TWIN_STAR_LIVE_RECIPE.pool} · ${TWIN_STAR_LIVE_RECIPE.gate} · 每槽套筒${TWIN_STAR_LIVE_RECIPE.slotOfSleeve * 100}%=总资产${slotNavPct}% · body${TWIN_STAR_LIVE_RECIPE.body} · 空篮留最强ETF`;
}

export function roundNavPct(n: number): number {
  return Math.round(n * 10) / 10;
}

/** Prefer the Chinese name; fall back to ts_code. Never return a duplicate. */
export function satCandidateLabel(c: { ts: string; name?: string | null }): string {
  const n = c.name?.trim();
  return n && n !== c.ts ? n : c.ts;
}

function parseIsoUtc(iso: string): Date | null {
  if (!ISO_DAY.test(iso)) return null;
  const [y, m, d] = iso.split('-').map(Number);
  return new Date(Date.UTC(y, (m ?? 1) - 1, d ?? 1));
}

function isoUtc(d: Date): string {
  return d.toISOString().slice(0, 10);
}

function isWeekdayUtc(d: Date): boolean {
  const day = d.getUTCDay();
  return day !== 0 && day !== 6;
}

/** Mon–Fri count, inclusive. Live proxy for CN sessions (holidays may be ±1). */
export function countWeekdaysInclusive(fromIso: string, toIso: string): number {
  const start = parseIsoUtc(fromIso);
  const end = parseIsoUtc(toIso);
  if (!start || !end || end < start) return 0;
  let n = 0;
  const cur = new Date(start);
  while (cur <= end) {
    if (isWeekdayUtc(cur)) n += 1;
    cur.setUTCDate(cur.getUTCDate() + 1);
  }
  return n;
}

/** The n-th weekday including `fromIso` (n=1 → first weekday on/after start). */
export function nthWeekdayInclusive(fromIso: string, n: number): string | null {
  const start = parseIsoUtc(fromIso);
  if (!start || n < 1) return null;
  let seen = 0;
  const cur = new Date(start);
  for (let i = 0; i < 40; i += 1) {
    if (isWeekdayUtc(cur)) {
      seen += 1;
      if (seen >= n) return isoUtc(cur);
    }
    cur.setUTCDate(cur.getUTCDate() + 1);
  }
  return null;
}

export type SatBodyProgress = {
  heldDays: number | null;
  daysLeft: number | null;
  exitDue: string | null;
  due: boolean;
  missingEntry: boolean;
};

export function satBodyProgress(entryDate: string | null | undefined, asOf: string | null | undefined): SatBodyProgress {
  const empty: SatBodyProgress = { heldDays: null, daysLeft: null, exitDue: null, due: false, missingEntry: true };
  if (!entryDate || !asOf || !ISO_DAY.test(entryDate) || !ISO_DAY.test(asOf)) return empty;
  const heldDays = countWeekdaysInclusive(entryDate, asOf);
  const exitDue = nthWeekdayInclusive(entryDate, TWIN_STAR_LIVE_RECIPE.body);
  const daysLeft = Math.max(0, TWIN_STAR_LIVE_RECIPE.body - heldDays);
  return { heldDays, daysLeft, exitDue, due: heldDays >= TWIN_STAR_LIVE_RECIPE.body, missingEntry: false };
}

export function satProtectStop(cost: number | null | undefined): number | null {
  if (cost == null || !Number.isFinite(cost) || cost <= 0) return null;
  return Math.round(cost * (1 - SAT_PROTECT_STOP_PCT) * 1000) / 1000;
}

/** CN A-share holdings belong to the S-gap satellite sleeve, not the S-3 stock basket.
 *  When the core pick is an ETF (OIL/GOLD/…), every live CN stock is satellite.
 *  When pick=STOCK, only names in the sat recipe/candidate set are satellite. */
export function isLiveSatelliteStock(
  symbol: string,
  ctx: { pickKey: string | null; satNameTs: Set<string> },
): boolean {
  if (!isCnWatchlistSymbol(symbol)) return false;
  if (ctx.pickKey !== 'STOCK') return true;
  const ts = toTsCodeFromSymbol(symbol);
  return Boolean(ts && ctx.satNameTs.has(ts));
}

export type TwinStarTradeSide = 'BUY' | 'HOLD' | 'SELL';
export type TwinStarTradeSleeve = 'core' | 'sat';
export type TwinStarAssetKind = 'stock' | 'etf';

export type TwinStarTradeRow = {
  side: TwinStarTradeSide;
  sleeve: TwinStarTradeSleeve;
  kind: TwinStarAssetKind;
  symbol: string;
  name?: string | null;
  navPct: number;
  reason: string;
  swapFrom?: string | null;
  limitLocked?: boolean;
  /** sat-fund = trim ETF to pay for today's satellite stock buys. */
  purpose?: 'sat-fill' | 'sat-exit' | 'sat-fund' | 'core-buy' | 'core-rotate' | 'hold';
  costPrice?: number | null;
  lastClose?: number | null;
  pnlPct?: number | null;
  heldDays?: number | null;
  daysLeft?: number | null;
  exitDue?: string | null;
  protectStop?: number | null;
  stopBreached?: boolean;
  missingEntry?: boolean;
  missingCost?: boolean;
};

export function satConditionalLine(r: TwinStarTradeRow): string {
  const code = toTsCodeFromSymbol(r.symbol) ?? r.symbol;
  const name = r.name && r.name !== code && r.name !== r.symbol ? r.name : '';
  const stop = r.protectStop != null ? String(r.protectStop) : '—';
  const due = r.exitDue ?? '—';
  const act = r.side === 'SELL' ? '需卖' : '持有';
  return [name, code, `止损${stop}`, `到期${due}`, act].filter(Boolean).join(' ');
}

export type TwinStarEtfHolding = {
  symbol: string;
  key: string;
  name?: string | null;
  positionPct?: number | null;
};

export type TwinStarLiveStock = {
  symbol: string;
  name?: string | null;
  positionPct?: number | null;
  costPrice?: number | null;
  entryDate?: string | null;
  lastClose?: number | null;
  pnlPct?: number | null;
};

export type TwinStarTradePlanInput = {
  coreTargetPct: number;
  satTargetPct: number;
  gateOpen: boolean;
  afterSatWindow: boolean;
  /** Engine-replayed satellite book (not broker fills). */
  satHoldings: TwinStarSatHolding[];
  satExitsDue: TwinStarSatHolding[];
  satCandidates: TwinStarSatCandidate[];
  satBlocked?: TwinStarSatCandidate[];
  satAlternates?: TwinStarSatCandidate[];
  pickKey: string | null;
  pickSymbol: string | null;
  pickName: string | null;
  cnCandidates: PortfolioCandidate[];
  hkCandidates: PortfolioCandidate[];
  cnAllowBuys: boolean;
  hkAllowBuys: boolean;
  /** S-3 size as % of the STOCK sleeve (typically 10). */
  suggestedSizePct: number | null;
  etfHoldings: TwinStarEtfHolding[];
  /** User-recorded CN stock positions (satellite occupancy). */
  liveStockHoldings?: TwinStarLiveStock[];
  /** Portfolio-health trade date (Shanghai session) for body=3 countdown. */
  asOfDate?: string | null;
  /** Strongest ETF when pick=STOCK cannot execute (pick-strong runner-up). */
  coreParkEtfKey?: string | null;
  /** mom60 by sleeve key, used to cut the weakest ETF first. */
  etfMomByKey?: Record<string, number | null>;
};

export type TwinStarRecipeName = {
  ts: string;
  daysLeft: number | null;
};

export type TwinStarTradePlan = {
  coreTargetPct: number;
  satTargetPct: number;
  satSlotNavPct: number;
  /** Live satellite names the user actually holds. */
  satHeld: number;
  recipeSatHeld: number;
  satFreeSlots: number;
  /** Watchlist symbols currently counted as satellite (CN:300413 …). */
  satHeldSymbols: string[];
  coreBuyable: boolean;
  satHeadline: string;
  coreHeadline: string;
  etfHeadline: string;
  bookNote: string;
  etfTotalPct: number;
  etfSparePct: number;
  /** Sum of today's stock BUY rows as % of NAV. */
  stockBuyNavPct: number;
  /** ETF trim today to fund those stock buys (≤ spare). */
  etfTrimPct: number;
  recipeNames: TwinStarRecipeName[];
  buys: TwinStarTradeRow[];
  holds: TwinStarTradeRow[];
  sells: TwinStarTradeRow[];
};

export function etfSleeveKey(symbol: string): string {
  const s = symbol.toUpperCase();
  if (s.includes('518880') || s.includes('518800')) return 'GOLD';
  if (s.includes('513350') || s.includes('159518') || s.includes('561570')) return 'OIL';
  if (s.includes('513110') || s.includes('513100') || s.includes('513500')) return 'NASDAQ';
  if (s.includes('511260') || s.includes('511010')) return 'BOND10';
  return 'OTHER';
}

export function rowAssetKind(symbol: string): TwinStarAssetKind {
  return isEtfWatchlistSymbol(symbol) ? 'etf' : 'stock';
}

function satSymbol(ts: string): string {
  return tsCodeToWatchlistSymbol(ts);
}

function candidateSymbol(c: PortfolioCandidate): string {
  return (c.symbol ?? c.ts_code ?? '').trim();
}

function liveTsCode(symbol: string): string | null {
  return toTsCodeFromSymbol(symbol);
}

function etfMom(h: TwinStarEtfHolding, momByKey?: Record<string, number | null>): number {
  const v = momByKey?.[h.key];
  return typeof v === 'number' && Number.isFinite(v) ? v : Number.NEGATIVE_INFINITY;
}

function openPct(raw: number | null | undefined): number {
  const n = Number(raw);
  return Number.isFinite(n) && n > 0 ? n : 0;
}

function openEtfHoldings(holdings: TwinStarEtfHolding[] | undefined): TwinStarEtfHolding[] {
  return (holdings ?? []).filter((h) => openPct(h.positionPct) > 0);
}

/** Cut weakest ETFs first; keep the pick-strong runner-up as the core park. */
export function allocateSatFundTrims(input: {
  holdings: TwinStarEtfHolding[];
  trimTotal: number;
  parkKey: string | null;
  momByKey?: Record<string, number | null>;
}): Array<{ holding: TwinStarEtfHolding; cut: number }> {
  const { holdings, trimTotal, parkKey, momByKey } = input;
  if (trimTotal <= 0) return [];
  const usable = holdings.filter((h) => h.key !== 'OTHER' && (Number(h.positionPct) || 0) > 0);
  const ordered = [...usable].sort((a, b) => {
    const aPark = parkKey != null && a.key === parkKey ? 1 : 0;
    const bPark = parkKey != null && b.key === parkKey ? 1 : 0;
    if (aPark !== bPark) return aPark - bPark;
    return etfMom(a, momByKey) - etfMom(b, momByKey);
  });
  let left = trimTotal;
  const out: Array<{ holding: TwinStarEtfHolding; cut: number }> = [];
  for (const h of ordered) {
    if (left <= 0) break;
    const pos = Number(h.positionPct) || 0;
    const cut = roundNavPct(Math.min(pos, left));
    if (cut <= 0) continue;
    out.push({ holding: h, cut });
    left = roundNavPct(left - cut);
  }
  return out;
}

export function satConclusionLine(plan: TwinStarTradePlan, gateOpen: boolean): string {
  if (!gateOpen) return 'R-wide 关闸（不开仓）';
  return plan.satHeadline;
}

function pushRow(rows: TwinStarTradeRow[], row: Omit<TwinStarTradeRow, 'kind'>): void {
  rows.push({ ...row, kind: rowAssetKind(row.symbol) });
}

export function buildTwinStarTradePlan(input: TwinStarTradePlanInput): TwinStarTradePlan {
  const coreTargetPct = Number.isFinite(input.coreTargetPct) ? input.coreTargetPct : 100;
  const satTargetPct = Number.isFinite(input.satTargetPct) ? input.satTargetPct : Math.max(0, 100 - coreTargetPct);
  const satSlotNavPct = roundNavPct(satTargetPct * SAT_SLOT_OF_SLEEVE);
  const recipe = input.satHoldings ?? [];
  const recipeTs = new Set(recipe.map((h) => h.ts));
  const candidatePool = [
    ...(input.satCandidates ?? []),
    ...(input.satBlocked ?? []),
    ...(input.satAlternates ?? []),
  ];
  const satNameTs = new Set([...recipeTs, ...candidatePool.map((c) => c.ts).filter(Boolean)]);

  const liveStocks = (input.liveStockHoldings ?? []).filter((h) => isCnWatchlistSymbol(h.symbol));
  const liveSat = liveStocks.filter((h) =>
    isLiveSatelliteStock(h.symbol, { pickKey: input.pickKey, satNameTs }),
  );
  const liveHeldTs = new Set(liveSat.map((h) => liveTsCode(h.symbol)).filter((ts): ts is string => Boolean(ts)));

  const asOf = input.asOfDate ?? null;
  const recipeExitTs = new Set((input.satExitsDue ?? []).filter((h) => liveHeldTs.has(h.ts)).map((h) => h.ts));
  const satMeta = new Map<string, {
    holding: TwinStarLiveStock;
    body: SatBodyProgress;
    protectStop: number | null;
    lastClose: number | null;
    pnlPct: number | null;
    stopBreached: boolean;
    recipe: TwinStarSatHolding | undefined;
  }>();
  const exitTs = new Set<string>();
  for (const h of liveSat) {
    const ts = liveTsCode(h.symbol);
    if (!ts) continue;
    const recipeRow = recipe.find((r) => r.ts === ts);
    const body = satBodyProgress(h.entryDate ?? recipeRow?.entryDate, asOf);
    const protectStop = satProtectStop(h.costPrice ?? recipeRow?.entryPrice);
    const lastClose = h.lastClose ?? recipeRow?.close ?? null;
    const stopBreached = lastClose != null && protectStop != null && lastClose <= protectStop;
    satMeta.set(ts, {
      holding: h,
      body,
      protectStop,
      lastClose,
      pnlPct: h.pnlPct ?? recipeRow?.pnlPct ?? null,
      stopBreached,
      recipe: recipeRow,
    });
    if (body.due || stopBreached || recipeExitTs.has(ts)) exitTs.add(ts);
  }

  const satHeld = liveSat.length;
  const recipeSatHeld = recipe.length;
  const exiting = exitTs.size;
  const satFreeSlots = Math.max(0, SAT_MAX_POS - (satHeld - exiting));

  const liveEtfs = openEtfHoldings(input.etfHoldings);
  const etfTotalPct = roundNavPct(liveEtfs.reduce((s, h) => s + openPct(h.positionPct), 0));
  const etfSparePct =
    satTargetPct > 0 ? roundNavPct(Math.max(0, etfTotalPct - coreTargetPct)) : 0;

  const buys: TwinStarTradeRow[] = [];
  const holds: TwinStarTradeRow[] = [];
  const sells: TwinStarTradeRow[] = [];

  let satHeadline: string;
  if (satTargetPct <= 0) {
    satHeadline = '卫星未占用（核心 100%）· 不买卫星';
  } else if (!input.gateOpen) {
    satHeadline = 'R-wide 关闸（不开仓）';
  } else if (satFreeSlots <= 0) {
    satHeadline = `R-wide 开闸 · 你卫星仓满 ${satHeld}/${SAT_MAX_POS} · 今日不买新票`;
  } else if (!input.afterSatWindow) {
    satHeadline = `R-wide 开闸 · 你卫星仓 ${satHeld}/${SAT_MAX_POS} · 候选 14:30 后公布（当日近似）`;
  } else {
    // Strict live fill = primary only. Alternates are worse-amp names outside
    // the low-vol 1/3 bucket (rejected replace/expand). Blocked names stay
    // skipped — do not pair them by index onto the primary list as 涨停换.
    const seen = new Set<string>();
    const fill = (input.satCandidates ?? []).filter((c) => {
      if (!c.ts || liveHeldTs.has(c.ts) || c.limitLocked) return false;
      if (seen.has(c.ts)) return false;
      seen.add(c.ts);
      return true;
    }).slice(0, satFreeSlots);
    const blockedN = (input.satBlocked ?? []).filter((c) => c.ts && !liveHeldTs.has(c.ts)).length;
    if (fill.length === 0) {
      satHeadline =
        blockedN > 0
          ? `R-wide 开闸 · 你卫星仓 ${satHeld}/${SAT_MAX_POS} · 今日无填槽候选（涨停跳过 ${blockedN} 只，strict 不补）`
          : `R-wide 开闸 · 你卫星仓 ${satHeld}/${SAT_MAX_POS} · 今日无填槽候选`;
    } else {
      const names = fill.slice(0, 3).map((c) => satCandidateLabel(c)).join(', ');
      const replayNote =
        recipeSatHeld >= SAT_MAX_POS && satHeld === 0 ? '（引擎模拟已满，你未跟，按空仓填）' : '';
      satHeadline = `R-wide 开闸 → 买入 ${names} · 每只总资产 ${satSlotNavPct}%${replayNote}`;
      for (const c of fill) {
        const label = satCandidateLabel(c);
        pushRow(buys, {
          side: 'BUY',
          sleeve: 'sat',
          symbol: satSymbol(c.ts),
          name: label !== c.ts ? label : null,
          navPct: satSlotNavPct,
          reason: `卫星空槽 · 当日行情 · 每只总资产 ${satSlotNavPct}%`,
          purpose: 'sat-fill',
        });
      }
    }
  }

  for (const h of liveSat) {
    const ts = liveTsCode(h.symbol);
    if (!ts) continue;
    const meta = satMeta.get(ts);
    const body = meta?.body ?? satBodyProgress(h.entryDate, asOf);
    const protectStop = meta?.protectStop ?? satProtectStop(h.costPrice);
    const overlay = {
      costPrice: h.costPrice ?? meta?.recipe?.entryPrice ?? null,
      lastClose: meta?.lastClose ?? null,
      pnlPct: meta?.pnlPct ?? null,
      heldDays: body.heldDays,
      daysLeft: body.daysLeft,
      exitDue: body.exitDue ?? meta?.recipe?.exitDue ?? null,
      protectStop,
      stopBreached: Boolean(meta?.stopBreached),
      missingEntry: body.missingEntry,
      missingCost: protectStop == null,
    };
    if (exitTs.has(ts)) {
      const stopOnly = Boolean(meta?.stopBreached) && !body.due && !recipeExitTs.has(ts);
      pushRow(sells, {
        side: 'SELL',
        sleeve: 'sat',
        symbol: h.symbol,
        name: h.name ?? ts,
        navPct: h.positionPct ?? satSlotNavPct,
        reason: stopOnly
          ? `保护止损已破 ${protectStop}（成本−${SAT_PROTECT_STOP_PCT * 100}%）`
          : `卫星到期卖 · ${overlay.exitDue ?? '今日'} · body3 收盘`,
        purpose: 'sat-exit',
        ...overlay,
      });
      continue;
    }
    let reason: string;
    if (body.missingEntry) {
      reason = '补录入场日才能算 body3 到期';
    } else if (body.daysLeft === 1) {
      reason = '明日收盘卖（body3）';
    } else {
      reason = `${body.daysLeft} 个交易日后收盘卖（body3）`;
    }
    pushRow(holds, {
      side: 'HOLD',
      sleeve: 'sat',
      symbol: h.symbol,
      name: h.name ?? ts,
      navPct: h.positionPct ?? satSlotNavPct,
      reason,
      purpose: 'hold',
      ...overlay,
    });
  }

  const satStockSells = sells.filter((r) => r.sleeve === 'sat' && r.kind === 'stock');
  if (satStockSells.length > 0) {
    const names = satStockSells
      .slice(0, 3)
      .map((r) => r.name ?? r.symbol)
      .join('、');
    satHeadline = `${satHeadline} · 卖 ${names}`;
  }

  const pickKey = input.pickKey;
  const sleeveSize = input.suggestedSizePct != null && input.suggestedSizePct > 0 ? input.suggestedSizePct : 10;
  const coreStockNavPct = roundNavPct(sleeveSize * (coreTargetPct / 100));

  const coreStocks: PortfolioCandidate[] = [];
  if (pickKey === 'STOCK') {
    if (input.cnAllowBuys) coreStocks.push(...input.cnCandidates);
    if (input.hkAllowBuys) coreStocks.push(...input.hkCandidates);
  }
  const maxCoreNames = Math.max(1, Math.floor(100 / sleeveSize));
  const coreBuys = coreStocks
    .map((c) => ({ c, symbol: candidateSymbol(c) }))
    .filter((x) => x.symbol)
    .slice(0, maxCoreNames);
  const coreBuyable = pickKey === 'STOCK' ? coreBuys.length > 0 : pickKey != null && pickKey !== 'REPO';

  let coreHeadline: string;
  if (pickKey === 'STOCK') {
    if (coreBuys.length === 0) {
      coreHeadline = `核心 ${coreTargetPct}% pick=STOCK · 今日 0 只可执行，不要为 STOCK 清空 ETF 后空仓`;
    } else {
      coreHeadline = `核心 ${coreTargetPct}% 股票篮 · 每只总资产 ${coreStockNavPct}%（篮内 ${sleeveSize}%）`;
      for (const { c, symbol } of coreBuys) {
        pushRow(buys, {
          side: 'BUY',
          sleeve: 'core',
          symbol,
          name: c.name ?? symbol,
          navPct: coreStockNavPct,
          reason: `核心股票篮 · score ${c.score ?? '—'}`,
        });
      }
    }
  } else if (pickKey === 'REPO' || pickKey == null) {
    coreHeadline = `核心 ${coreTargetPct}% → 逆回购 / 观望`;
  } else {
    const etfSym = input.pickSymbol ?? pickKey;
    const held = liveEtfs.some((h) => etfSleeveKey(h.symbol) === pickKey);
    coreHeadline = `核心 ${coreTargetPct}% → ${input.pickName ?? pickKey}（${etfSym}）`;
    if (held) {
      const heldRow = liveEtfs.find((h) => etfSleeveKey(h.symbol) === pickKey);
      pushRow(holds, {
        side: 'HOLD',
        sleeve: 'core',
        symbol: heldRow?.symbol ?? etfSym,
        name: input.pickName,
        navPct: heldRow?.positionPct ?? coreTargetPct,
        reason: `今日 pick · 目标占总资产 ${coreTargetPct}%`,
      });
    } else {
      pushRow(buys, {
        side: 'BUY',
        sleeve: 'core',
        symbol: etfSym,
        name: input.pickName,
        navPct: coreTargetPct,
        reason: `核心腿买入 ETF · 占总资产 ${coreTargetPct}%`,
      });
    }
  }

  if (coreBuyable && pickKey) {
    for (const h of liveEtfs) {
      if (h.key === 'OTHER') continue;
      if (pickKey === 'STOCK' || h.key !== pickKey) {
        pushRow(sells, {
          side: 'SELL',
          sleeve: 'core',
          symbol: h.symbol,
          name: h.name ?? h.symbol,
          navPct: openPct(h.positionPct),
          reason:
            pickKey === 'STOCK'
              ? `非今日 pick，资金调向股票篮（有可执行候选）`
              : `非今日 pick（${pickKey}）`,
        });
      }
    }
  } else if (pickKey === 'STOCK' && !coreBuyable) {
    const stockBuysNav = roundNavPct(
      buys.filter((r) => r.kind === 'stock' && r.side === 'BUY').reduce((s, r) => s + r.navPct, 0),
    );
    const trimTotal = roundNavPct(Math.min(etfSparePct, stockBuysNav));
    const parkKey = input.coreParkEtfKey ?? null;
    const trims = allocateSatFundTrims({
      holdings: liveEtfs,
      trimTotal,
      parkKey,
      momByKey: input.etfMomByKey,
    });
    for (const { holding: h, cut } of trims) {
      const keptPark = parkKey != null && h.key === parkKey;
      pushRow(sells, {
        side: 'SELL',
        sleeve: 'sat',
        symbol: h.symbol,
        name: h.name ?? h.symbol,
        navPct: cut,
        reason: keptPark
          ? `弱 ETF 不够腾 ${trimTotal}% · 从核心停泊 ${h.key} 再减总资产 ${cut}%`
          : `先砍弱 ETF ${h.key} · 减仓总资产 ${cut}% 买卫星 · 核心 ${coreTargetPct}% 留在 ${parkKey ?? '最强ETF'}`,
        purpose: 'sat-fund',
      });
    }
  }

  const stockBuyNavPct = roundNavPct(
    buys.filter((r) => r.kind === 'stock' && r.side === 'BUY').reduce((s, r) => s + r.navPct, 0),
  );
  const etfTrimPct = roundNavPct(
    sells.filter((r) => r.purpose === 'sat-fund').reduce((s, r) => s + r.navPct, 0),
  );
  const etfHeadline =
    liveEtfs.length === 0
      ? `ETF 未录入 · 核心目标 ${coreTargetPct}%`
      : etfTrimPct > 0
        ? `ETF 合计 ${etfTotalPct}% · 先砍弱 ETF 腾 ${etfTrimPct}% 买卫星 · 核心 ${coreTargetPct}% 留最强`
        : etfSparePct > 0
          ? `ETF 合计 ${etfTotalPct}% · 核心只需 ${coreTargetPct}% · 多出约 ${etfSparePct}% 可腾给卫星股票`
          : `ETF 合计 ${etfTotalPct}% · 核心目标 ${coreTargetPct}%`;

  let bookNote: string;
  const liveLine = `你卫星仓 ${satHeld}/${SAT_MAX_POS}`;
  if (recipeSatHeld === 0) {
    bookNote = `${liveLine} · 引擎模拟仓空`;
  } else if (recipeSatHeld > SAT_MAX_POS) {
    bookNote = `${liveLine} · 引擎模拟 ${recipeSatHeld} 只为对照（按最多 ${SAT_MAX_POS} 只计），不是券商仓`;
  } else if (satHeld === 0) {
    bookNote = `${liveLine}（ETF 不算卫星仓）· 引擎模拟 ${recipeSatHeld}/${SAT_MAX_POS} 是对照，不是券商仓`;
  } else {
    bookNote = `${liveLine} · 引擎模拟 ${recipeSatHeld}/${SAT_MAX_POS}（对照）`;
  }

  return {
    coreTargetPct,
    satTargetPct,
    satSlotNavPct,
    satHeld,
    recipeSatHeld,
    satFreeSlots,
    satHeldSymbols: liveSat.map((h) => h.symbol),
    coreBuyable,
    satHeadline,
    coreHeadline,
    etfHeadline,
    bookNote,
    etfTotalPct,
    etfSparePct,
    stockBuyNavPct,
    etfTrimPct,
    recipeNames: recipe.map((h) => ({ ts: h.ts, daysLeft: h.daysLeft ?? null })),
    buys,
    holds,
    sells,
  };
}
