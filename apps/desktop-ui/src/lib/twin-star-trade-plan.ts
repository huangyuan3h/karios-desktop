/**
 * Opportunity Twin-Star live trade plan: names + % of total NAV.
 *
 * Sizing matches the frozen engine (state_bucket_track):
 *   satellite slot = 10% of the sat sleeve → satTargetPct * 0.10 of NAV
 *   core STOCK name = S-3 suggestedSizePct of the core sleeve
 *     → suggestedSizePct * (coreTargetPct / 100) of NAV
 *
 * New satellite buys only fill free slots (max 15). A full book is HOLD, not
 * "buy today's gap candidates". Core STOCK with 0 executable names must not
 * dump ETFs into an empty basket.
 */

import { tsCodeToWatchlistSymbol } from '@/lib/symbols';
import type { TwinStarSatCandidate, TwinStarSatHolding } from '@/lib/queries/backtest';
import type { PortfolioCandidate } from '@/lib/queries/portfolioHealth';

export const SAT_MAX_POS = 15;
export const SAT_SLOT_OF_SLEEVE = 0.1;

export function roundNavPct(n: number): number {
  return Math.round(n * 10) / 10;
}

export type TwinStarTradeSide = 'BUY' | 'HOLD' | 'SELL';
export type TwinStarTradeSleeve = 'core' | 'sat';

export type TwinStarTradeRow = {
  side: TwinStarTradeSide;
  sleeve: TwinStarTradeSleeve;
  symbol: string;
  name?: string | null;
  navPct: number;
  reason: string;
  swapFrom?: string | null;
  limitLocked?: boolean;
};

export type TwinStarEtfHolding = {
  symbol: string;
  key: string;
  positionPct?: number | null;
};

export type TwinStarTradePlanInput = {
  coreTargetPct: number;
  satTargetPct: number;
  gateOpen: boolean;
  afterSatWindow: boolean;
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
  /** Dashboard execution gate: DEFEND / !allowNewEntries blocks core STOCK buys. */
  s3GateBlocksNew: boolean;
  etfHoldings: TwinStarEtfHolding[];
};

export type TwinStarTradePlan = {
  coreTargetPct: number;
  satTargetPct: number;
  satSlotNavPct: number;
  satHeld: number;
  satFreeSlots: number;
  coreBuyable: boolean;
  satHeadline: string;
  coreHeadline: string;
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

function satSymbol(ts: string): string {
  return tsCodeToWatchlistSymbol(ts);
}

function candidateSymbol(c: PortfolioCandidate): string {
  return (c.symbol ?? c.ts_code ?? '').trim();
}

export function satConclusionLine(plan: TwinStarTradePlan, gateOpen: boolean): string {
  if (!gateOpen) return 'R-wide 关闸（不开仓）';
  return plan.satHeadline;
}

export function buildTwinStarTradePlan(input: TwinStarTradePlanInput): TwinStarTradePlan {
  const coreTargetPct = Number.isFinite(input.coreTargetPct) ? input.coreTargetPct : 100;
  const satTargetPct = Number.isFinite(input.satTargetPct) ? input.satTargetPct : Math.max(0, 100 - coreTargetPct);
  const satSlotNavPct = roundNavPct(satTargetPct * SAT_SLOT_OF_SLEEVE);
  const holdings = input.satHoldings ?? [];
  const exitsDue = input.satExitsDue ?? [];
  const heldSet = new Set(holdings.map((h) => h.ts));
  const exitSet = new Set(exitsDue.map((h) => h.ts));
  const satHeld = holdings.length;
  const exiting = exitsDue.length;
  const satFreeSlots = Math.max(0, SAT_MAX_POS - (satHeld - exiting));

  const buys: TwinStarTradeRow[] = [];
  const holds: TwinStarTradeRow[] = [];
  const sells: TwinStarTradeRow[] = [];

  let satHeadline: string;
  if (satTargetPct <= 0) {
    satHeadline = '卫星未占用（核心 100%）· 不买卫星';
  } else if (!input.gateOpen) {
    satHeadline = 'R-wide 关闸（不开仓）';
  } else if (satFreeSlots <= 0) {
    satHeadline = `R-wide 开闸 · 持仓簿满 ${satHeld}/${SAT_MAX_POS} · 今日不买新票`;
  } else if (!input.afterSatWindow) {
    satHeadline = `R-wide 开闸 · 空槽 ${satFreeSlots} · 14:20 拉当日行情后公布`;
  } else {
    const fillable = [
      ...(input.satCandidates ?? []).filter((c) => c.ts && !heldSet.has(c.ts) && !c.limitLocked),
      ...(input.satAlternates ?? []).filter((c) => c.ts && !heldSet.has(c.ts) && !c.limitLocked),
    ];
    const seen = new Set<string>();
    const fill = fillable.filter((c) => {
      if (seen.has(c.ts)) return false;
      seen.add(c.ts);
      return true;
    }).slice(0, satFreeSlots);
    const blocked = (input.satBlocked ?? []).filter((c) => c.ts && !heldSet.has(c.ts));
    if (fill.length === 0) {
      satHeadline = `R-wide 开闸 · 空槽 ${satFreeSlots} · 今日无填槽候选`;
    } else {
      const names = fill.slice(0, 3).map((c) => c.ts).join(', ');
      satHeadline = `R-wide 开闸 → 买入 ${names} · 每只总资产 ${satSlotNavPct}%`;
      for (let i = 0; i < fill.length; i++) {
        const c = fill[i]!;
        const skipped = blocked[i];
        buys.push({
          side: 'BUY',
          sleeve: 'sat',
          symbol: satSymbol(c.ts),
          name: c.ts,
          navPct: satSlotNavPct,
          reason: skipped
            ? `涨停 ${skipped.ts} → 换 ${c.ts}`
            : `卫星空槽 · 当日行情 · 总资产 ${satSlotNavPct}%`,
          swapFrom: skipped ? skipped.ts : null,
        });
      }
    }
  }

  for (const h of holdings) {
    if (exitSet.has(h.ts)) continue;
    holds.push({
      side: 'HOLD',
      sleeve: 'sat',
      symbol: satSymbol(h.ts),
      name: h.ts,
      navPct: satSlotNavPct,
      reason:
        h.daysLeft != null
          ? `卫星应持 · 剩余 ${h.daysLeft} 天`
          : '卫星应持（持仓簿）',
    });
  }
  for (const h of exitsDue) {
    sells.push({
      side: 'SELL',
      sleeve: 'sat',
      symbol: satSymbol(h.ts),
      name: h.ts,
      navPct: satSlotNavPct,
      reason: h.exitDue ? `卫星到期卖 · ${h.exitDue}` : '卫星到期卖',
    });
  }

  const pickKey = input.pickKey;
  const sleeveSize = input.suggestedSizePct != null && input.suggestedSizePct > 0 ? input.suggestedSizePct : 10;
  const coreStockNavPct = roundNavPct(sleeveSize * (coreTargetPct / 100));

  const coreStocks: PortfolioCandidate[] = [];
  if (pickKey === 'STOCK' && !input.s3GateBlocksNew) {
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
    if (input.s3GateBlocksNew) {
      coreHeadline = `核心 ${coreTargetPct}% pick=STOCK · S-3 闸门关闭，今日不开股票`;
    } else if (coreBuys.length === 0) {
      coreHeadline = `核心 ${coreTargetPct}% pick=STOCK · 今日 0 只可执行，不要为 STOCK 清空 ETF 后空仓`;
    } else {
      coreHeadline = `核心 ${coreTargetPct}% 股票篮 · 每只总资产 ${coreStockNavPct}%（篮内 ${sleeveSize}%）`;
      for (const { c, symbol } of coreBuys) {
        buys.push({
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
    const held = input.etfHoldings.some((h) => etfSleeveKey(h.symbol) === pickKey);
    coreHeadline = `核心 ${coreTargetPct}% → ${input.pickName ?? pickKey}（${etfSym}）`;
    if (held) {
      holds.unshift({
        side: 'HOLD',
        sleeve: 'core',
        symbol: etfSym,
        name: input.pickName,
        navPct: coreTargetPct,
        reason: `今日 pick · 目标占总资产 ${coreTargetPct}%`,
      });
    } else {
      buys.unshift({
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
    for (const h of input.etfHoldings) {
      if (h.key === 'OTHER') continue;
      if (pickKey === 'STOCK' || h.key !== pickKey) {
        sells.push({
          side: 'SELL',
          sleeve: 'core',
          symbol: h.symbol,
          name: h.symbol,
          navPct: h.positionPct ?? 0,
          reason:
            pickKey === 'STOCK'
              ? `非今日 pick，资金调向股票篮（有可执行候选）`
              : `非今日 pick（${pickKey}）`,
        });
      }
    }
  }

  return {
    coreTargetPct,
    satTargetPct,
    satSlotNavPct,
    satHeld,
    satFreeSlots,
    coreBuyable,
    satHeadline,
    coreHeadline,
    buys,
    holds,
    sells,
  };
}
