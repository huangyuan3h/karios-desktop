'use client';

/**
 * 母港 M30 follow — target/action/cadence math (Watchlist homeport view).
 *
 * M30 definition (frozen backtest, `service/homeport.py` + `strategy-recipes.md`
 * §2 + `homeport-weight-tune-2026-09-16.md`):
 * - 0.7 × Harbor + 0.3 × B3, monthly reset to 70/30 (5bp/side on the trade).
 * - Harbor = S-3 stock core (10% per leg, max 10 legs) + idle cash parked in the
 *   mom60+MA200 argmax ETF (H2, 2pt hysteresis; REPO when no candidate above
 *   MA200). Trail 8% → REPO.
 * - B3 = 5-ETF inverse-vol (510300/510500/518880/513100/511260, 60d, monthly,
 *   first trading day of month, 5bp/side). `b3-state` `universe[].targetPct` is
 *   % *within the B3 leg* (sums 100); scale by 0.3 for % of total.
 *
 * UI follow cadence (this file): weekly default (configurable), first trading
 * day of the ISO week. The backtest stays monthly — the weekly cadence only
 * controls when the panel raises "needs rebalancing" todos, never the engine.
 *
 * Frontend-only: consumes existing APIs (portfolio-health, b3-state, account
 * 总资金). No schema change, no backend change.
 */

import * as React from 'react';

import { holdingMatchesPick } from '@/lib/replica-gap';
import { lotRuleFor, roundToLot } from '@/lib/lot-sizing';
import { PARKING_META, type ParkingKey } from '@/lib/parking-universe';
import { loadJson, saveJson } from '@/lib/storage';

/** Harbor leg weight of total assets (M30). */
export const M30_HARBOR_WEIGHT = 0.7;
/** B3 leg weight of total assets (M30). */
export const M30_B3_WEIGHT = 0.3;
/** Each S-3 stock leg = 10% of the Harbor leg → 7% of total. */
export const M30_STOCK_LEG_PCT_TOTAL = 7;
/** S-3 max legs (engine `max_positions=10`). */
export const M30_S3_MAX_LEGS = 10;
/**
 * Off-cycle rebalance trigger: Harbor/B3 leg split deviates ≥ 5pt of total
 * from 70/30 (absolute).
 */
export const M30_DRIFT_LEG_PT = 5;
/** Off-cycle rebalance trigger: any single target line deviates ≥ 5pt of total. */
export const M30_DRIFT_LINE_PT = 5;
/**
 * Minimum trade size: suppress actions below 0.5% of 总资金 (dust) or below
 * 1 lot (shares round to 0) so the panel never suggests untradable tickets.
 */
export const M30_MIN_TRADE_PCT = 0.5;

export type FollowCadence = 'weekly' | 'monthly' | 'daily';

const CADENCE_KEY = 'karios.m30FollowCadence.v1';
const CADENCE_EVENT = 'karios:m30-follow-cadence';

export const FOLLOW_CADENCE_LABELS: Record<FollowCadence, string> = {
  weekly: '每周',
  monthly: '每月',
  daily: '每日',
};

function isFollowCadence(v: unknown): v is FollowCadence {
  return v === 'weekly' || v === 'monthly' || v === 'daily';
}

export function getFollowCadence(): FollowCadence {
  const v = loadJson<unknown>(CADENCE_KEY, null);
  if (isFollowCadence(v)) return v;
  saveJson(CADENCE_KEY, 'weekly' satisfies FollowCadence);
  return 'weekly';
}

export function setFollowCadence(c: FollowCadence): void {
  saveJson(CADENCE_KEY, c);
  if (typeof window !== 'undefined') window.dispatchEvent(new Event(CADENCE_EVENT));
}

export function useFollowCadence(): FollowCadence {
  const [c, setC] = React.useState<FollowCadence>(() => getFollowCadence());
  React.useEffect(() => {
    const sync = () => setC(getFollowCadence());
    window.addEventListener(CADENCE_EVENT, sync);
    window.addEventListener('storage', sync);
    return () => {
      window.removeEventListener(CADENCE_EVENT, sync);
      window.removeEventListener('storage', sync);
    };
  }, []);
  return c;
}

export type B3Entry = {
  symbol: string;
  name: string;
  /** % within the B3 leg (backend `targetPct`, sums ~100). */
  targetPct: number;
  /** Tradable raw close (backend `px`), null when unavailable. */
  px?: number | null;
};

export type M30Holding = {
  symbol: string;
  positionPct?: number | null;
  name?: string | null;
  /** S-3 per-holding signal from portfolio-health (`EXIT` = hard exit). */
  action?: string | null;
  lastClose?: number | null;
};

export type M30TargetLine = {
  id: string;
  leg: 'harbor-stock' | 'harbor-park' | 'harbor-cash' | 'b3';
  /** Watchlist symbol shape (`ETF:513350` / `CN:600519` / `CASH`). */
  symbol: string;
  /** Engine ts_code when known (`513350.SH`), else ''. */
  ts: string;
  name: string;
  /** Tradable price used for lot math (raw close), null = shares unavailable. */
  price: number | null;
  targetPct: number;
  currentPct: number;
  deltaPct: number;
  targetAmt: number | null;
  currentAmt: number | null;
  deltaAmt: number | null;
  /** Target-size lots (100-share/board lots via `lot-sizing`). */
  targetShares: number | null;
  /** Rebalance-size lots for |delta| (null when price/capital missing). */
  deltaShares: number | null;
  side: 'BUY' | 'SELL' | 'HOLD';
  /** True when the delta was suppressed by the min-trade rule. */
  suppressed: boolean;
};

export type M30Action = {
  side: 'BUY' | 'SELL';
  symbol: string;
  name: string;
  /** % of total assets. */
  pct: number;
  amt: number | null;
  shares: number | null;
  reason: string;
  leg: M30TargetLine['leg'];
};

function round2(v: number): number {
  return Math.round(v * 100) / 100;
}

function bareCode(sym: string): string {
  const s = String(sym || '').toUpperCase();
  const afterColon = s.includes(':') ? s.split(':')[1]! : s;
  return afterColon.split('.')[0]!;
}

function etfCode(ts: string): string {
  return String(ts || '').split('.')[0]!.toUpperCase();
}

/**
 * Harbor parking ETF symbol for a pick key (`ETF:<code>`), or null when the
 * Harbor leg should stay in cash (pick=REPO / STOCK / unknown).
 *
 * Prefers the backend-provided `pickSymbol` when it matches the pick key;
 * otherwise derives from the canonical parking universe.
 */
export function parkingSymbolForPick(
  pickKey: string | null | undefined,
  pickSymbol?: string | null,
): string | null {
  const key = String(pickKey || '').toUpperCase();
  if (!key || key === 'REPO' || key === 'STOCK') return null;
  if (pickSymbol && holdingMatchesPick(pickSymbol, key)) {
    const s = String(pickSymbol).toUpperCase();
    return s.startsWith('ETF:') ? s : `ETF:${bareCode(s)}`;
  }
  const meta = PARKING_META[key as ParkingKey];
  if (!meta) return null;
  return `ETF:${meta.codes[0]}`;
}

export function parkingNameForPick(pickKey: string | null | undefined): string {
  const key = String(pickKey || '').toUpperCase();
  const meta = PARKING_META[key as ParkingKey];
  return meta ? meta.short : key || '现金';
}

/** Scale B3-leg weights (% within leg) to % of total (×0.3). Sums to 30. */
export function b3TotalPct(withinLegPct: number): number {
  return round2(Number(withinLegPct) * M30_B3_WEIGHT);
}

function amtOf(capital: number | null, pct: number): number | null {
  if (capital == null || !Number.isFinite(capital) || capital <= 0) return null;
  return Math.round((capital * pct) / 100);
}

function sharesFor(symbol: string, price: number | null, capital: number | null, pct: number): number | null {
  if (price == null || !Number.isFinite(price) || price <= 0) return null;
  if (capital == null || !Number.isFinite(capital) || capital <= 0) return null;
  if (!Number.isFinite(pct) || pct <= 0) return null;
  const rule = lotRuleFor(symbol);
  return roundToLot(capital * (pct / 100) / price, rule);
}

function isStockSymbol(symbol: string): boolean {
  const s = String(symbol || '').toUpperCase();
  return s.startsWith('CN:') || s.startsWith('HK:');
}

function currentForB3(symbol: string, holdings: M30Holding[]): number {
  const want = bareCode(symbol);
  let sum = 0;
  for (const h of holdings) {
    if (bareCode(h.symbol) === want && String(h.symbol).toUpperCase().startsWith('ETF:')) {
      sum += Number(h.positionPct) || 0;
    }
  }
  return round2(sum);
}

function priceForSymbol(
  symbol: string,
  opts: { b3: B3Entry[]; parkPrice?: number | null; stockCloses: Map<string, number> },
): number | null {
  const s = String(symbol || '').toUpperCase();
  if (s === 'CASH') return null;
  if (s.startsWith('ETF:')) {
    const want = bareCode(s);
    for (const u of opts.b3) {
      if (etfCode(u.symbol) === want && u.px != null && Number.isFinite(u.px) && u.px > 0) {
        return u.px;
      }
    }
    if (opts.parkPrice != null && Number.isFinite(opts.parkPrice) && opts.parkPrice > 0) {
      return opts.parkPrice;
    }
    return null;
  }
  return opts.stockCloses.get(s) ?? opts.stockCloses.get(bareCode(s)) ?? null;
}

function decideSide(deltaPct: number, suppressed: boolean): 'BUY' | 'SELL' | 'HOLD' {
  if (suppressed || Math.abs(deltaPct) < M30_MIN_TRADE_PCT) return 'HOLD';
  if (deltaPct > 0) return 'BUY';
  if (deltaPct < 0) return 'SELL';
  return 'HOLD';
}

export type BuildM30Input = {
  capital: number | null;
  pickKey: string | null | undefined;
  /** Backend `multiAssetSleeve.pick.symbol` (e.g. `ETF:513350`), if any. */
  pickSymbol?: string | null;
  /** Backend `multiAssetSleeve.pick.close` (mark for the parking ETF). */
  pickPrice?: number | null;
  b3: B3Entry[];
  holdings: M30Holding[];
};

export type M30FollowReport = {
  pickKey: string;
  parkSymbol: string | null;
  targets: M30TargetLine[];
  /** Sells first, then buys (netting cash); dust-suppressed lines excluded. */
  actions: M30Action[];
  /** Holdings in no M30 target line (e.g. stale parking ETF / off-book names). */
  nonTargets: M30Holding[];
  harborTargetPct: number;
  b3TargetPct: number;
  harborCurrentPct: number;
  b3CurrentPct: number;
  cashCurrentPct: number;
  cashTargetPct: number;
  /** |harborCurrent − 70| (pt of total). */
  legDriftPt: number;
  /** Max |line delta| across target lines (pt of total). */
  maxLineDriftPt: number;
  stockLegCount: number;
};

/**
 * Build M30 target lines from live inputs.
 *
 * Harbor leg (70% of total):
 * - One 7%-of-total line per S-3 HOLD stock leg (10% of the 70% Harbor leg,
 *   engine `position_pct=0.10`, capped at 10 legs). EXIT legs target 0 (sell).
 * - Remainder parks in the harbor pick ETF (`parkingSymbolForPick`), or cash
 *   when pick=REPO/STOCK. When pick=STOCK the whole 70% is the stock basket
 *   (parking 0).
 * B3 leg (30%): one line per `b3-state` universe entry at 0.3 × targetPct.
 */
export function buildM30Targets(input: BuildM30Input): M30FollowReport {
  const pickKey = String(input.pickKey || 'REPO').toUpperCase();
  const holdings = (input.holdings || []).filter((h) => (Number(h.positionPct) || 0) > 0);
  const b3 = Array.isArray(input.b3) ? input.b3 : [];
  const capital = input.capital;
  const parkSymbol = parkingSymbolForPick(pickKey, input.pickSymbol ?? null);

  const stockCloses = new Map<string, number>();
  for (const h of holdings) {
    if (isStockSymbol(h.symbol) && typeof h.lastClose === 'number' && h.lastClose > 0) {
      stockCloses.set(String(h.symbol).toUpperCase(), h.lastClose);
    }
  }

  const holdStocks = holdings.filter(
    (h) => isStockSymbol(h.symbol) && String(h.action || '').toUpperCase() !== 'EXIT',
  );
  const exitStocks = holdings.filter(
    (h) => isStockSymbol(h.symbol) && String(h.action || '').toUpperCase() === 'EXIT',
  );
  const stockLegCount = Math.min(M30_S3_MAX_LEGS, holdStocks.length);
  const isStockPick = pickKey === 'STOCK';
  const harborStockTargetTotal = isStockPick ? 70 : round2(stockLegCount * M30_STOCK_LEG_PCT_TOTAL);
  const harborParkTargetTotal = isStockPick ? 0 : round2(70 - harborStockTargetTotal);

  const targets: M30TargetLine[] = [];
  const pushLine = (line: Omit<M30TargetLine, 'targetAmt' | 'currentAmt' | 'deltaAmt' | 'targetShares' | 'deltaShares' | 'side' | 'suppressed'> & { price: number | null }): M30TargetLine => {
    const targetAmt = amtOf(capital, line.targetPct);
    const currentAmt = amtOf(capital, line.currentPct);
    const deltaAmt = targetAmt != null && currentAmt != null ? targetAmt - currentAmt : null;
    const targetShares = sharesFor(line.symbol, line.price, capital, line.targetPct);
    const deltaShares = sharesFor(line.symbol, line.price, capital, Math.abs(line.deltaPct));
    // Dust rule: <0.5% of total, or a delta that cannot fill 1 lot.
    const lotBlocked = deltaShares === 0 && line.price != null && capital != null;
    const suppressed = Math.abs(line.deltaPct) < M30_MIN_TRADE_PCT || lotBlocked;
    const full: M30TargetLine = {
      ...line,
      targetPct: round2(line.targetPct),
      currentPct: round2(line.currentPct),
      deltaPct: round2(line.deltaPct),
      targetAmt,
      currentAmt,
      deltaAmt,
      targetShares,
      deltaShares,
      side: decideSide(line.deltaPct, suppressed),
      suppressed,
    };
    targets.push(full);
    return full;
  };

  // Harbor stock legs (HOLD → 7% each; EXIT → 0 sell).
  for (const h of holdStocks.slice(0, M30_S3_MAX_LEGS)) {
    const sym = String(h.symbol).toUpperCase();
    const cur = Number(h.positionPct) || 0;
    const price = priceForSymbol(sym, { b3, stockCloses });
    pushLine({
      id: `harbor-stock-${sym}`,
      leg: 'harbor-stock',
      symbol: sym,
      ts: '',
      name: h.name || sym,
      price,
      targetPct: M30_STOCK_LEG_PCT_TOTAL,
      currentPct: cur,
      deltaPct: round2(M30_STOCK_LEG_PCT_TOTAL - cur),
    });
  }
  for (const h of exitStocks) {
    const sym = String(h.symbol).toUpperCase();
    const cur = Number(h.positionPct) || 0;
    const price = priceForSymbol(sym, { b3, stockCloses });
    pushLine({
      id: `harbor-exit-${sym}`,
      leg: 'harbor-stock',
      symbol: sym,
      ts: '',
      name: h.name || sym,
      price,
      targetPct: 0,
      currentPct: cur,
      deltaPct: round2(0 - cur),
    });
  }

  // Harbor parking leg (ETF) or cash when REPO/STOCK.
  if (parkSymbol) {
    let parkCurrent = 0;
    for (const h of holdings) {
      if (holdingMatchesPick(h.symbol, pickKey)) parkCurrent += Number(h.positionPct) || 0;
    }
    parkCurrent = round2(parkCurrent);
    const price = priceForSymbol(parkSymbol, { b3, parkPrice: input.pickPrice ?? null, stockCloses });
    pushLine({
      id: `harbor-park-${parkSymbol}`,
      leg: 'harbor-park',
      symbol: parkSymbol,
      ts: '',
      name: parkingNameForPick(pickKey),
      price,
      targetPct: harborParkTargetTotal,
      currentPct: parkCurrent,
      deltaPct: round2(harborParkTargetTotal - parkCurrent),
    });
  }

  // B3 leg (5 ETFs at 0.3 × within-leg weight).
  for (const u of b3) {
    const sym = `ETF:${etfCode(u.symbol)}`;
    const targetPct = b3TotalPct(Number(u.targetPct) || 0);
    const current = currentForB3(sym, holdings);
    const price =
      u.px != null && Number.isFinite(u.px) && (u.px as number) > 0 ? (u.px as number) : null;
    pushLine({
      id: `b3-${etfCode(u.symbol)}`,
      leg: 'b3',
      symbol: sym,
      ts: u.symbol,
      name: u.name || sym,
      price,
      targetPct,
      currentPct: current,
      deltaPct: round2(targetPct - current),
    });
  }

  // Leg-split currents (% of total).
  let harborCurrentPct = 0;
  let b3CurrentPct = 0;
  for (const t of targets) {
    if (t.leg === 'harbor-stock' || t.leg === 'harbor-park') harborCurrentPct += t.currentPct;
    if (t.leg === 'b3') b3CurrentPct += t.currentPct;
  }
  harborCurrentPct = round2(harborCurrentPct);
  b3CurrentPct = round2(b3CurrentPct);
  const totalDeployed = holdings.reduce((s, h) => s + (Number(h.positionPct) || 0), 0);
  const cashCurrentPct = round2(Math.max(0, 100 - Math.min(100, totalDeployed)));
  const cashTargetPct = parkSymbol ? 0 : round2(harborParkTargetTotal);

  // Non-target holdings: not in any target line (B3 ETFs are targets in the
  // M30 view — they must never be flagged as "非今日停车 ETF" here).
  const targetSyms = new Set(targets.map((t) => t.symbol.toUpperCase()));
  const targetBare = new Set(targets.map((t) => bareCode(t.symbol)));
  const nonTargets = holdings.filter((h) => {
    const s = String(h.symbol).toUpperCase();
    if (targetSyms.has(s)) return false;
    if (s.startsWith('ETF:') && targetBare.has(bareCode(s))) return false;
    // Anything matching the harbor pick is covered by the park line.
    if (parkSymbol && holdingMatchesPick(h.symbol, pickKey)) return false;
    // S-3 stocks are always covered (HOLD 7% / EXIT 0 lines above).
    if (isStockSymbol(s)) return false;
    return true;
  });

  // Actions: sells first (exits + trims), then buys; dust suppressed already.
  const actions: M30Action[] = [];
  for (const t of targets) {
    if (t.side === 'HOLD') continue;
    actions.push({
      side: t.side,
      symbol: t.symbol,
      name: t.name,
      pct: round2(Math.abs(t.deltaPct)),
      amt:
        t.deltaAmt != null
          ? Math.abs(t.deltaAmt)
          : amtOf(capital, Math.abs(t.deltaPct)),
      shares: t.deltaShares,
      reason:
        t.leg === 'b3'
          ? 'B3 月频目标'
          : t.leg === 'harbor-park'
            ? `港湾停车 ${pickKey}`
            : String(t.id).startsWith('harbor-exit-')
              ? 'S-3 硬退出'
              : 'S-3 核心腿',
      leg: t.leg,
    });
  }
  // Non-target holdings (stale parking ETF / off-book names): suggest a full
  // exit so the book converges back to the M30 lines. Same dust rule applies.
  for (const h of nonTargets) {
    const cur = Number(h.positionPct) || 0;
    if (cur < M30_MIN_TRADE_PCT) continue;
    const sym = String(h.symbol).toUpperCase();
    const price = priceForSymbol(sym, { b3, stockCloses });
    actions.push({
      side: 'SELL',
      symbol: sym,
      name: h.name || sym,
      pct: round2(cur),
      amt: amtOf(capital, cur),
      shares: sharesFor(sym, price, capital, cur),
      reason: '非M30目标',
      leg: sym.startsWith('ETF:') ? 'b3' : 'harbor-stock',
    });
  }
  actions.sort((a, b) => {
    if (a.side !== b.side) return a.side === 'SELL' ? -1 : 1;
    return b.pct - a.pct;
  });

  const legDriftPt = round2(Math.abs(harborCurrentPct - 70));
  let maxLineDriftPt = 0;
  for (const t of targets) maxLineDriftPt = Math.max(maxLineDriftPt, Math.abs(t.deltaPct));
  maxLineDriftPt = round2(maxLineDriftPt);

  return {
    pickKey,
    parkSymbol,
    targets,
    actions,
    nonTargets,
    harborTargetPct: 70,
    b3TargetPct: 30,
    harborCurrentPct,
    b3CurrentPct,
    cashCurrentPct,
    cashTargetPct,
    legDriftPt,
    maxLineDriftPt,
    stockLegCount,
  };
}

// ---------------------------------------------------------------- cadence ---

function parseIso(iso: string): { y: number; m: number; d: number } | null {
  const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(String(iso || ''));
  if (!m) return null;
  return { y: Number(m[1]), m: Number(m[2]), d: Number(m[3]) };
}

function toIso(y: number, m: number, d: number): string {
  const mm = String(m).padStart(2, '0');
  const dd = String(d).padStart(2, '0');
  return `${y}-${mm}-${dd}`;
}

function addDaysIso(iso: string, n: number): string {
  const p = parseIso(iso);
  if (!p) return iso;
  const dt = new Date(Date.UTC(p.y, p.m - 1, p.d, 12, 0, 0));
  dt.setUTCDate(dt.getUTCDate() + n);
  return toIso(dt.getUTCFullYear(), dt.getUTCMonth() + 1, dt.getUTCDate());
}

/**
 * Shanghai weekday (Mon=1..Sun=7) for an ISO date.
 * Uses noon UTC+8 so the calendar day never shifts under UTC.
 */
export function weekdayShanghai(iso: string): number {
  const p = parseIso(iso);
  if (!p) return 0;
  const dt = new Date(Date.UTC(p.y, p.m - 1, p.d, 4, 0, 0)); // 12:00 Shanghai
  const wd = dt.getUTCDay(); // Sun=0..Sat=6
  return wd === 0 ? 7 : wd;
}

export function isWeekdayIso(iso: string): boolean {
  const wd = weekdayShanghai(iso);
  return wd >= 1 && wd <= 5;
}

function mondayOfWeekIso(iso: string): string {
  const wd = weekdayShanghai(iso);
  if (wd === 0) return iso;
  return addDaysIso(iso, -(wd - 1));
}

function firstWeekdayOfMonthIso(y: number, m: number): string {
  let iso = toIso(y, m, 1);
  for (let i = 0; i < 8; i += 1) {
    if (isWeekdayIso(iso)) return iso;
    iso = addDaysIso(iso, 1);
  }
  return toIso(y, m, 1);
}

function prevMonth(y: number, m: number): { y: number; m: number } {
  return m === 1 ? { y: y - 1, m: 12 } : { y, m: m - 1 };
}

function nextMonth(y: number, m: number): { y: number; m: number } {
  return m === 12 ? { y: y + 1, m: 1 } : { y, m: m + 1 };
}

export type CadenceStatus = {
  last: string;
  next: string;
  isRebalanceDay: boolean;
};

/**
 * Rebalance-day rule per cadence (weekend-aware; holidays NOT excluded —
 * there is no frontend trading-holiday calendar, so a Monday holiday still
 * counts as the rebalance day and the user adjusts manually).
 * - weekly: Monday (first trading day of the ISO week).
 * - monthly: first weekday (Mon–Fri) of the month (backtest B3 anchor shape).
 * - daily: every weekday.
 */
export function cadenceStatus(cadence: FollowCadence, todayIso: string): CadenceStatus {
  const p = parseIso(todayIso);
  if (!p) return { last: todayIso, next: todayIso, isRebalanceDay: false };
  if (cadence === 'daily') {
    if (isWeekdayIso(todayIso)) {
      let nxt = addDaysIso(todayIso, 1);
      while (!isWeekdayIso(nxt)) nxt = addDaysIso(nxt, 1);
      return { last: todayIso, next: nxt, isRebalanceDay: true };
    }
    let last = addDaysIso(todayIso, -1);
    while (!isWeekdayIso(last)) last = addDaysIso(last, -1);
    let nxt = addDaysIso(todayIso, 1);
    while (!isWeekdayIso(nxt)) nxt = addDaysIso(nxt, 1);
    return { last, next: nxt, isRebalanceDay: false };
  }
  if (cadence === 'monthly') {
    const first = firstWeekdayOfMonthIso(p.y, p.m);
    if (todayIso >= first) {
      const n = nextMonth(p.y, p.m);
      return { last: first, next: firstWeekdayOfMonthIso(n.y, n.m), isRebalanceDay: todayIso === first };
    }
    const pr = prevMonth(p.y, p.m);
    return { last: firstWeekdayOfMonthIso(pr.y, pr.m), next: first, isRebalanceDay: false };
  }
  // weekly default
  const monday = mondayOfWeekIso(todayIso);
  if (isWeekdayIso(todayIso) && todayIso === monday) {
    return { last: todayIso, next: addDaysIso(todayIso, 7), isRebalanceDay: true };
  }
  if (!isWeekdayIso(todayIso)) {
    return { last: monday, next: addDaysIso(monday, 7), isRebalanceDay: false };
  }
  return { last: monday, next: addDaysIso(monday, 7), isRebalanceDay: false };
}

export type RebalanceGate = {
  needsRebalance: boolean;
  reason: 'rebalance-day' | 'leg-drift' | 'line-drift' | 'on-track';
};

/**
 * Only raise "needs rebalancing" todos on the rebalance day, OR off-cycle
 * when drift exceeds the documented threshold (leg ≥5pt or any line ≥5pt).
 * Hard risk items (S-3 EXIT / circuit) bypass this gate in the UI layer.
 */
export function rebalanceGate(opts: {
  isRebalanceDay: boolean;
  legDriftPt: number;
  maxLineDriftPt: number;
}): RebalanceGate {
  if (opts.isRebalanceDay) return { needsRebalance: true, reason: 'rebalance-day' };
  if (opts.legDriftPt >= M30_DRIFT_LEG_PT) return { needsRebalance: true, reason: 'leg-drift' };
  if (opts.maxLineDriftPt >= M30_DRIFT_LINE_PT) return { needsRebalance: true, reason: 'line-drift' };
  return { needsRebalance: false, reason: 'on-track' };
}
