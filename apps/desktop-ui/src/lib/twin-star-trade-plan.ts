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
 *   satellite slot = 10% of the sat sleeve → satTargetPct * 0.10 of NAV
 *   core STOCK name = S-3 suggestedSizePct of the core sleeve
 */

import { isCnWatchlistSymbol, isEtfWatchlistSymbol, toTsCodeFromSymbol, tsCodeToWatchlistSymbol } from '@/lib/symbols';
import type { TwinStarSatCandidate, TwinStarSatHolding } from '@/lib/queries/backtest';
import type { PortfolioCandidate } from '@/lib/queries/portfolioHealth';

export const SAT_MAX_POS = 15;
export const SAT_SLOT_OF_SLEEVE = 0.1;

export function roundNavPct(n: number): number {
  return Math.round(n * 10) / 10;
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
};

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
  coreBuyable: boolean;
  satHeadline: string;
  coreHeadline: string;
  etfHeadline: string;
  bookNote: string;
  etfTotalPct: number;
  etfSparePct: number;
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
  const liveSat = liveStocks.filter((h) => {
    const ts = liveTsCode(h.symbol);
    return Boolean(ts && satNameTs.has(ts));
  });
  const liveHeldTs = new Set(liveSat.map((h) => liveTsCode(h.symbol)).filter((ts): ts is string => Boolean(ts)));

  const recipeExits = input.satExitsDue ?? [];
  const liveExits = recipeExits.filter((h) => liveHeldTs.has(h.ts));
  const exitSet = new Set(liveExits.map((h) => h.ts));

  const satHeld = liveSat.length;
  const recipeSatHeld = recipe.length;
  const exiting = liveExits.length;
  const satFreeSlots = Math.max(0, SAT_MAX_POS - (satHeld - exiting));

  const etfTotalPct = roundNavPct(
    (input.etfHoldings ?? []).reduce((s, h) => s + (Number(h.positionPct) || 0), 0),
  );
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
    satHeadline = `R-wide 开闸 · 你卫星仓 ${satHeld}/${SAT_MAX_POS} · 14:20 拉当日行情后公布`;
  } else {
    const fillable = [
      ...(input.satCandidates ?? []).filter((c) => c.ts && !liveHeldTs.has(c.ts) && !c.limitLocked),
      ...(input.satAlternates ?? []).filter((c) => c.ts && !liveHeldTs.has(c.ts) && !c.limitLocked),
    ];
    const seen = new Set<string>();
    const fill = fillable.filter((c) => {
      if (seen.has(c.ts)) return false;
      seen.add(c.ts);
      return true;
    }).slice(0, satFreeSlots);
    const blocked = (input.satBlocked ?? []).filter((c) => c.ts && !liveHeldTs.has(c.ts));
    if (fill.length === 0) {
      satHeadline = `R-wide 开闸 · 你卫星仓 ${satHeld}/${SAT_MAX_POS} · 今日无填槽候选`;
    } else {
      const names = fill.slice(0, 3).map((c) => c.ts).join(', ');
      const replayNote =
        recipeSatHeld >= SAT_MAX_POS && satHeld === 0 ? '（策略回放已满，你未跟，按空仓填）' : '';
      satHeadline = `R-wide 开闸 → 买入 ${names} · 每只总资产 ${satSlotNavPct}%${replayNote}`;
      for (let i = 0; i < fill.length; i++) {
        const c = fill[i]!;
        const skipped = blocked[i];
        pushRow(buys, {
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

  for (const h of liveSat) {
    const ts = liveTsCode(h.symbol);
    if (!ts || exitSet.has(ts)) continue;
    const recipeRow = recipe.find((r) => r.ts === ts);
    pushRow(holds, {
      side: 'HOLD',
      sleeve: 'sat',
      symbol: h.symbol,
      name: h.name ?? ts,
      navPct: h.positionPct ?? satSlotNavPct,
      reason:
        recipeRow?.daysLeft != null
          ? `卫星应持 · 剩余 ${recipeRow.daysLeft} 天`
          : '卫星应持（已录入仓位）',
    });
  }
  for (const h of liveExits) {
    pushRow(sells, {
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
    const held = input.etfHoldings.some((h) => etfSleeveKey(h.symbol) === pickKey);
    coreHeadline = `核心 ${coreTargetPct}% → ${input.pickName ?? pickKey}（${etfSym}）`;
    if (held) {
      const heldRow = input.etfHoldings.find((h) => etfSleeveKey(h.symbol) === pickKey);
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
    for (const h of input.etfHoldings) {
      if (h.key === 'OTHER') continue;
      if (pickKey === 'STOCK' || h.key !== pickKey) {
        pushRow(sells, {
          side: 'SELL',
          sleeve: 'core',
          symbol: h.symbol,
          name: h.name ?? h.symbol,
          navPct: h.positionPct ?? 0,
          reason:
            pickKey === 'STOCK'
              ? `非今日 pick，资金调向股票篮（有可执行候选）`
              : `非今日 pick（${pickKey}）`,
        });
      }
    }
  } else if (pickKey === 'STOCK' && !coreBuyable) {
    for (const h of input.etfHoldings) {
      if (h.key === 'OTHER') continue;
      const already = holds.some((r) => r.symbol === h.symbol) || sells.some((r) => r.symbol === h.symbol);
      if (already) continue;
      pushRow(holds, {
        side: 'HOLD',
        sleeve: 'core',
        symbol: h.symbol,
        name: h.name ?? h.symbol,
        navPct: h.positionPct ?? 0,
        reason: '核心 STOCK 今日 0 只可买 · 暂留，勿清仓',
      });
    }
  }

  const etfHeadline =
    input.etfHoldings.length === 0
      ? `ETF 未录入 · 核心目标 ${coreTargetPct}%`
      : etfSparePct > 0
        ? `ETF 合计 ${etfTotalPct}% · 核心只需 ${coreTargetPct}% · 多出约 ${etfSparePct}% 可腾给卫星股票`
        : `ETF 合计 ${etfTotalPct}% · 核心目标 ${coreTargetPct}%`;

  let bookNote: string;
  if (recipeSatHeld === 0 && satHeld === 0) {
    bookNote = `你卫星仓 ${satHeld}/${SAT_MAX_POS} · 策略回放仓空`;
  } else if (satHeld === 0 && recipeSatHeld > 0) {
    bookNote = `你卫星仓 0 只（ETF 不算卫星仓）· 策略回放 ${recipeSatHeld}/${SAT_MAX_POS} 是模拟仓，不是券商持仓`;
  } else {
    bookNote = `你卫星仓 ${satHeld}/${SAT_MAX_POS} · 策略回放 ${recipeSatHeld}/${SAT_MAX_POS}`;
  }

  return {
    coreTargetPct,
    satTargetPct,
    satSlotNavPct,
    satHeld,
    recipeSatHeld,
    satFreeSlots,
    coreBuyable,
    satHeadline,
    coreHeadline,
    etfHeadline,
    bookNote,
    etfTotalPct,
    etfSparePct,
    recipeNames: recipe.map((h) => ({ ts: h.ts, daysLeft: h.daysLeft ?? null })),
    buys,
    holds,
    sells,
  };
}
