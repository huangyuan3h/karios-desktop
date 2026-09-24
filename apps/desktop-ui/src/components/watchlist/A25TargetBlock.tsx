'use client';

import * as React from 'react';
import { useQueryClient } from '@tanstack/react-query';
import type { SatelliteLivePanel } from '@karios/shared';

import { Button } from '@/components/ui/button';
import { useAccountSettings } from '@/lib/account-settings';
import { slotLotShares } from '@/lib/lot-sizing';
import { getShanghaiTodayIso } from '@/lib/market-hours';
import type { TimelineOpenPosition, TimelineParkedHeld } from '@/lib/queries/backtest';
import {
  useB3StateQuery,
  useSatelliteExitDueQuery,
  useStarshipBStateQuery,
} from '@/lib/queries/backtest';
import {
  invalidateUserTradesQueries,
  recordUserTrade,
  useUserTradesListQuery,
} from '@/lib/queries/userTrades';
import { cn } from '@/lib/utils';
import {
  loadWatchlist,
  saveWatchlist,
  upsertWatchlistOpenTrade,
} from '@/lib/watchlist-storage';

/** H2-a25 recipe constants (mirror the canonical research/display spec). */
const SLOT_PCT = 25;
const SLEEVE_OF_IDLE = 0.25;
const DEFAULT_CAPACITY = 4;
/** Weight drift (pct points) worth acting on — smaller wobble is tolerated. */
const DRIFT_PCT = 3;
/** Hide parking dust trades below this target delta (pct points of total assets). */
const MIN_PARK_TRADE_PCT = 0.1;

function cny(v: number): string {
  return `¥${Math.round(v).toLocaleString('zh-CN')}`;
}

/** 2dp so a computed weight never lands as 1.4625000000000001. */
function round2(v: number): number {
  return Math.round(v * 100) / 100;
}

function code(ts: string): string {
  return String(ts).split('.')[0];
}

/** Bare 6-digit code from a registry symbol ("ETF:513350" / "CN:603019"). */
function bareCode(sym: string): string {
  return sym.includes(':') ? String(sym.split(':')[1]).split('.')[0]! : code(sym);
}

/**
 * Exchange suffix when the registry symbol carries no ts_code (e.g. a row
 * recorded before the ts was stored). Heuristic only — display/link use.
 */
function guessTs(sym: string): string {
  const c = bareCode(sym);
  if (!/^\d{6}$/.test(c)) return c;
  return /^[569]/.test(c) ? `${c}.SH` : `${c}.SZ`;
}

function pctOf(v: number): string {
  return `${v.toFixed(2).replace(/\.?0+$/, '')}%`;
}

/** Xueqiu page for a CN ts_code ("513350.SH" -> https://xueqiu.com/S/SH513350). */
function xueqiuUrl(ts: string): string {
  const [c, ex] = String(ts).split('.');
  return `https://xueqiu.com/S/${ex === 'SZ' ? 'SZ' : 'SH'}${c}`;
}

type Side = 'BUY' | 'SELL' | 'HOLD';

type Row = {
  key: string;
  /** Watchlist / journal symbol ("ETF:513350" / "CN:603019"). */
  symbol: string;
  ts: string;
  name: string;
  px: number | null | undefined;
  /** Target/held share of total assets, 0-100. */
  pct: number;
  leg: 'h2' | 'b3' | 'satellite';
  side: Side;
  /** Satellite-only context ("+3.5% · amp 3.0%"). */
  note?: string;
  cost?: number | null;
  pnl?: number | null;
  exitDue?: string | null;
  /** Strategy target weight; null = not part of the a25 book (off-strategy). */
  target?: number | null;
  /** Held, but absent from the engine's book (panel-signal fill). */
  offBook?: boolean;
  /** Rebalance to reach the target (drift > 1pt). */
  rebalance?: { side: 'BUY' | 'SELL'; pct: number } | null;
};

type BookLeg = {
  /** Watchlist symbol ("CN:603019"). */
  symbol: string;
  ts: string;
  entryDate?: string | null;
  cost: number | null;
  px: number | null;
  pnl: number | null;
  exitDue: string | null;
  /** Recorded size (% of total assets). */
  pct: number;
};

/**
 * The a25 order surface — ONE flat list for every leg, at the same position.
 *
 * Two modes, switched at midnight (user rule: yesterday's orders clear before
 * the next open):
 *
 * - **今日下单** — while the live 14:30 panel is today's: the satellite exits
 *   due today (卖出) + the panel's fillable names, capped by the free slots.
 * - **当前持有** — otherwise: the frozen book (satellite legs + parked sleeve +
 *   B3 weights) with cost / last mark / P&L / exit due.
 *
 * The book is read from the frozen timeline (`openPositions` / `parkedHeld`),
 * NOT the panel's held set: the panel's replay calendar ends at the panel day,
 * so in-flight legs used to collapse to "due today" (2026-09-23 incident).
 *
 * The strategy's internals (satellite / sleeve / B3) are deliberately not
 * surfaced: the user sees instruments + amounts + one button each.
 * Display/accounting aid only — Live stays 港湾.
 */
export function A25TargetBlock({
  parkedHeld,
  sleevePick = null,
  panel,
  openPositions = [],
  satCapacity = null,
  holdingLastClose = null,
  parkMode = 'a25',
}: {
  parkedHeld?: TimelineParkedHeld | null;
  /** Fallback parking ETF from portfolio-health when the timeline is unavailable. */
  sleevePick?: { key?: string; ts?: string; name?: string; close?: number } | null;
  panel?: SatelliteLivePanel | null;
  openPositions?: TimelineOpenPosition[];
  /** Satellite slot capacity from the timeline (never re-derive). */
  satCapacity?: number | null;
  /**
   * Last close per watchlist symbol ("CN:603019") from portfolio-health. A leg
   * the user entered on a live-panel signal has no engine `openPositions` row
   * and no panel px (not in today's gap list), so without this its sell row had
   * no price → "不足 1 手" and a dead button (2026-09-24 中科曙光).
   */
  holdingLastClose?: Record<string, number> | null;
  /** 星舰 B: park leg is the 3-leg {国债,黄金,纳指} inverse-vol blend (no H2/B3). */
  parkMode?: 'a25' | 'starship_b';
}) {
  const queryClient = useQueryClient();
  const { capital } = useAccountSettings();
  const isB = parkMode === 'starship_b';
  const b3Q = useB3StateQuery(!isB);
  const starBQ = useStarshipBStateQuery(isB);
  const tradesQ = useUserTradesListQuery(50);
  const [busy, setBusy] = React.useState<string | null>(null);
  const [err, setErr] = React.useState<string | null>(null);
  const [justSynced, setJustSynced] = React.useState<Set<string>>(() => new Set());

  const today = getShanghaiTodayIso();
  const live = panel && panel.decisionAvailable ? panel : null;
  const panelDay = live?.tradeDate ?? null;
  /** The panel is today's → its fills are today's orders. */
  const fresh = panelDay != null && panelDay === today;
  const capacity = satCapacity && satCapacity > 0 ? satCapacity : DEFAULT_CAPACITY;

  // Journaled on the given day (survives reloads).
  const recordedOn = React.useCallback(
    (symbol: string, day: string) =>
       (tradesQ.data ?? []).some(
         (t) =>
           t.strategyMode === 'starship_robust' &&
           t.symbol === symbol &&
           t.tradeDate === day,
       ),

    [tradesQ.data],
  );

  // Held in the watchlist — a row is only "done" when BOTH are written, so a
  // journal-only row (e.g. recorded before the watchlist write shipped) stays
  // clickable and self-heals on the next click.
  const watchlist = React.useMemo(
    () => loadWatchlist(),
    // Re-read after a save (justSynced) or a journal refresh — localStorage is
    // not reactive.
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [justSynced, tradesQ.data],
  );
  const watchlistSymbols = React.useMemo(
    () => new Set(watchlist.map((w) => w.symbol)),
    [watchlist],
  );
  // The registry also mirrors the strategy's own legs (source='satellite', no
  // size/cost) — that is NOT a user position. A leg only counts as held when a
  // size was recorded (or the journal has an entry), otherwise a mirrored leg
  // would offer a phantom sell (2026-09-23: 4 mirrored legs showed 卖出).
  const holdsMap = React.useMemo(() => {
    const m = new Map<string, number>();
    for (const w of watchlist) {
      const p = w.positionPct;
      if (typeof p === 'number' && p > 0) m.set(w.symbol, p);
    }
    return m;
  }, [watchlist]);
  const nameMap = React.useMemo(() => {
    const m = new Map<string, string>();
    for (const w of watchlist) if (w.name) m.set(w.symbol, w.name);
    return m;
  }, [watchlist]);
  const costMap = React.useMemo(() => {
    const m = new Map<string, number>();
    for (const w of watchlist) {
      if (typeof w.costPrice === 'number') m.set(w.symbol, w.costPrice);
    }
    return m;
  }, [watchlist]);
  const entryMap = React.useMemo(() => {
    const m = new Map<string, string>();
    for (const w of watchlist) if (w.entryDate) m.set(w.symbol, w.entryDate);
    return m;
  }, [watchlist]);
  const userHolds = React.useCallback(
    (symbol: string) =>
      holdsMap.has(symbol) || (tradesQ.data ?? []).some((t) => t.symbol === symbol),
    [holdsMap, tradesQ.data],
  );

  // 星舰 B park leg = the 3-leg state; H2-a25 park = B3 (5-leg).
  const b3 = isB ? (starBQ.data?.ok ? starBQ.data : null) : (b3Q.data?.ok ? b3Q.data : null);

  // Parking ETF: prefer the timeline (fresh price); fall back to the health
  // sleeve pick so the row survives a timeline hiccup ("状态不可用").
  const etf = React.useMemo(
    () =>
      parkedHeld?.ts
        ? { ts: parkedHeld.ts, name: parkedHeld.name, px: parkedHeld.price }
        : sleevePick?.ts
          ? {
              ts: sleevePick.ts,
              name: sleevePick.name ?? sleevePick.key ?? sleevePick.ts,
              px: sleevePick.close ?? null,
            }
          : null,
    [parkedHeld, sleevePick],
  );

  // Price/ts lookups for every recorded position (the card only holds the
  // strategy's own data, so an off-strategy holding can still resolve its
  // exchange + last mark).
  const rankedPx = React.useMemo(() => {
    const m = new Map<string, number>();
    for (const e of live?.ranked ?? []) if (e.px1430) m.set(code(e.ts), e.px1430);
    return m;
  }, [live]);
  const tsMap = React.useMemo(() => {
    const m = new Map<string, string>();
    if (etf) m.set(`ETF:${code(etf.ts)}`, etf.ts);
    for (const u of b3?.universe ?? []) m.set(`ETF:${code(u.symbol)}`, u.symbol);
    for (const l of live?.heldLegs ?? []) m.set(`CN:${code(l.ts)}`, l.ts);
    for (const e of live?.ranked ?? []) m.set(`CN:${code(e.ts)}`, e.ts);
    for (const p of openPositions) m.set(`CN:${code(p.ts)}`, p.ts);
    return m;
  }, [etf, b3, live, openPositions]);

  // The book = the USER's own satellite legs. The engine's frozen book is only
  // a reference: its replay picks (close basis) differ from the live 14:30
  // panel, so listing its legs as today's orders told the user to sell names
  // they never bought (2026-09-23). Sell times come from the frozen rule
  // (entry + BODY open sessions) via the calendar endpoint.
  const myEntries = React.useMemo(() => {
    const out: string[] = [];
    for (const sym of holdsMap.keys()) {
      if (!sym.startsWith('CN:')) continue;
      const e = entryMap.get(sym);
      if (e) out.push(e);
    }
    return out;
  }, [holdsMap, entryMap]);
  const dueQ = useSatelliteExitDueQuery(myEntries);
  // Last mark for a CN leg: the panel's 14:30 print, else the engine's close.
  const enginePx = React.useMemo(() => {
    const m = new Map<string, number>();
    for (const p of openPositions) if (p.close != null) m.set(code(p.ts), p.close);
    return m;
  }, [openPositions]);

  const book: BookLeg[] = React.useMemo(() => {
    const out: BookLeg[] = [];
    for (const [sym, pct] of holdsMap) {
      if (!sym.startsWith('CN:')) continue;
      const ts = tsMap.get(sym) ?? guessTs(sym);
      const entryDate = entryMap.get(sym) ?? null;
      const px =
        rankedPx.get(code(ts)) ??
        enginePx.get(code(ts)) ??
        holdingLastClose?.[sym] ??
        null;
      const cost = costMap.get(sym) ?? null;
      out.push({
        symbol: sym,
        ts,
        entryDate,
        cost,
        px,
        pnl: px != null && cost ? Math.round((px / cost - 1) * 10000) / 100 : null,
        exitDue: entryDate ? (dueQ.data?.exitDue?.[entryDate] ?? null) : null,
        pct,
      });
    }
    return out;
  }, [holdsMap, entryMap, tsMap, costMap, rankedPx, enginePx, holdingLastClose, dueQ.data]);

  // Due today (or overdue) = sell now; the rest still occupy a slot.
  const dueToday = React.useMemo(
    () => book.filter((b) => b.exitDue != null && b.exitDue <= today),
    [book, today],
  );
  const stillHeld = React.useMemo(
    () => book.filter((b) => b.exitDue == null || b.exitDue > today),
    [book, today],
  );
  const freeSlots = Math.max(0, capacity - stillHeld.length);

  // New fills: the panel's fillable set, capped by the slots the exits free.
  const buys = React.useMemo(
    () => (fresh ? (live?.ranked ?? []).filter((e) => e.wouldFill).slice(0, freeSlots) : []),
    [fresh, live, freeSlots],
  );

  // a25 parks the book's OWN idle cash: the unfilled satellite slots (plus
  // realized PnL). It is NOT the engine's `parkedHeld.weight` — that is the
  // frozen book's cash (4/4 slots filled + a year of accumulated profits =
  // 31.24%), which would tell a live book holding one leg to hold a tiny
  // sleeve/B3 and to sell what it has. Size off the user's own satellite weight.
  const satUsedPct = React.useMemo(() => {
    let sum = 0;
    for (const [sym, p] of holdsMap) if (sym.startsWith('CN:')) sum += p;
    return sum;
  }, [holdsMap]);
  const idle = Math.max(0, 1 - satUsedPct / 100);
  // 星舰 B parks 100% of idle in the 3-leg blend; H2-a25 splits 25% H2 / 75% B3.
  const sleevePct = isB ? 0 : round2(idle * SLEEVE_OF_IDLE * 100);
  const b3TotalPct = round2(idle * (isB ? 1 : 1 - SLEEVE_OF_IDLE) * 100);

  // --- 今日下单: exits (sell) + fills (buy), one flat list -------------------
  const orders: Row[] = React.useMemo(() => {
    const out: Row[] = [];
    for (const b of dueToday) {
      const sym = `CN:${code(b.ts)}`;
      out.push({
        key: `S-${b.ts}`,
        symbol: sym,
        ts: b.ts,
        name: nameMap.get(sym) ?? code(b.ts),
        px: b.px,
        // Sell the user's recorded size, not the nominal slot.
        pct: holdsMap.get(sym) ?? SLOT_PCT,
        leg: 'satellite',
        side: 'SELL',
        cost: b.cost,
        pnl: b.pnl,
        exitDue: b.exitDue,
      });
    }
    for (const e of buys) {
      out.push({
        key: `B-${e.ts}`,
        symbol: `CN:${code(e.ts)}`,
        ts: e.ts,
        name: e.name ?? code(e.ts),
        px: e.px1430,
        pct: SLOT_PCT,
        leg: 'satellite',
        side: 'BUY',
        note: `+${e.gapPct ?? '—'}% · amp ${e.amp1430Pct ?? '—'}%`,
      });
    }
    // Sleeve + B3: order the DELTA to target (target from the book's own idle),
    // not just the first funding. A held-but-overweight parking leg must be
    // trimmed when its sibling is funded, or the list over-allocates (2026-09-24:
    // OIL 25% vs target 6.25% + B3 18.75% would have totalled 118.75%). A zeroed
    // row or an automation-mirrored leg (no size) is not a holding, so it still
    // gets funded.
    const parking: Array<{
      ts: string;
      symbol: string;
      name: string;
      px: number | null | undefined;
      target: number;
      leg: 'h2' | 'b3';
    }> = [];
    if (etf) {
      parking.push({
        ts: etf.ts,
        symbol: `ETF:${code(etf.ts)}`,
        name: etf.name,
        px: etf.px,
        target: sleevePct,
        leg: 'h2',
      });
    }
    for (const u of b3?.universe ?? []) {
      parking.push({
        ts: u.symbol,
        symbol: `ETF:${code(u.symbol)}`,
        name: u.name,
        px: u.px,
        target: round2(b3TotalPct * (u.targetPct / 100)),
        leg: 'b3',
      });
    }
    for (const p of parking) {
      const delta = round2(p.target - (holdsMap.get(p.symbol) ?? 0));
      if (Math.abs(delta) < MIN_PARK_TRADE_PCT) continue;
      out.push({
        key: `${delta > 0 ? 'B' : 'S'}-${p.ts}`,
        symbol: p.symbol,
        ts: p.ts,
        name: p.name,
        px: p.px,
        pct: Math.abs(delta),
        leg: p.leg,
        side: delta > 0 ? 'BUY' : 'SELL',
      });
    }
    return out;
  }, [
    dueToday,
    buys,
    etf,
    b3,
    sleevePct,
    b3TotalPct,
    nameMap,
    holdsMap,
  ]);

  // --- 当前持有: only positions the user actually holds --------------------
  // The registry mirrors the strategy's legs (source='satellite', no size), so
  // iterating the book would list legs the user never bought. The view is the
  // user's RECORDED positions, enriched with the strategy's exit dues.
  const holdings: Row[] = React.useMemo(() => {
    const priceOf = (sym: string, ts: string): number | null => {
      if (sym.startsWith('ETF:')) {
        if (etf && code(etf.ts) === code(ts)) return etf.px ?? null;
        const u = (b3?.universe ?? []).find((x) => code(x.symbol) === code(ts));
        if (u?.px != null) return u.px;
      }
      const b = book.find((x) => code(x.ts) === code(ts));
      if (b?.px != null) return b.px;
      const r = rankedPx.get(code(ts));
      return r != null ? r : (holdingLastClose?.[sym] ?? null);
    };
    const out: Row[] = [];
    const seen = new Set<string>();
    // 1) held satellite legs (frozen book ∩ the user's recorded positions)
    for (const b of book) {
      const sym = `CN:${code(b.ts)}`;
      if (!userHolds(sym)) continue;
      seen.add(sym);
      const due = b.exitDue != null && b.exitDue <= today;
      out.push({
        key: `H-${b.ts}`,
        symbol: sym,
        ts: b.ts,
        name: nameMap.get(sym) ?? code(b.ts),
        px: b.px,
        pct: holdsMap.get(sym) ?? SLOT_PCT,
        leg: 'satellite',
        side: due ? 'SELL' : 'HOLD',
        cost: b.cost ?? costMap.get(sym) ?? null,
        pnl: b.pnl,
        exitDue: b.exitDue,
        target: SLOT_PCT,
      });
    }
    // 2) the rest of the user's recorded positions (sleeve / B3 / off-strategy)
    for (const [sym, pct] of holdsMap) {
      if (seen.has(sym)) continue;
      const ts = tsMap.get(sym) ?? guessTs(sym);
      const px = priceOf(sym, ts);
      const cost = costMap.get(sym) ?? null;
      // Strategy target for this symbol — only when the engine's idle weight is
      // known. Without it the target would read 0% and every row would look
      // massively over-weight (a transient timeline failure showed 目标 0%).
      // `idle` comes from the user's own satellite weight, so a 0 target is a
      // real state (all 4 slots filled → liquidate the parking), not a data
      // failure. A missing sleeve/B3 pick leaves the target null instead.
      let target: number | null = null;
      if (sym.startsWith('ETF:')) {
        if (etf && code(etf.ts) === code(ts)) target = sleevePct;
        else {
          const u = (b3?.universe ?? []).find((x) => code(x.symbol) === code(ts));
          if (u) target = round2(b3TotalPct * (u.targetPct / 100));
        }
      }
      const drift = target != null ? pct - target : 0;
      const entry = entryMap.get(sym);
      const offBookDue =
        !sym.startsWith('ETF:') && entry ? (dueQ.data?.exitDue?.[entry] ?? null) : null;
      out.push({
        key: `H-${sym}`,
        symbol: sym,
        ts,
        name: nameMap.get(sym) ?? bareCode(sym),
        px,
        pct,
        leg: sym.startsWith('ETF:')
          ? b3?.universe?.some((u) => code(u.symbol) === code(ts))
            ? 'b3'
            : 'h2'
          : 'satellite',
        side: 'HOLD',
        cost,
        pnl: px != null && cost ? Math.round((px / cost - 1) * 10000) / 100 : null,
        exitDue: offBookDue,
        offBook: !sym.startsWith('ETF:') && offBookDue != null,
        target,
        // A 0 target = fully off-strategy (idle 0 when every slot is filled), so
        // always offer the exit even for a weight under the drift threshold.
        rebalance:
          target != null && (Math.abs(drift) > DRIFT_PCT || (target === 0 && pct > 0))
            ? { side: drift > 0 ? 'SELL' : 'BUY', pct: Math.abs(drift) }
            : null,
      });
    }
    return out;
  }, [
    book,
    today,
    userHolds,
    holdsMap,
    nameMap,
    costMap,
    tsMap,
    b3,
    etf,
    rankedPx,
    sleevePct,
    b3TotalPct,
    entryMap,
    holdingLastClose,
    dueQ.data,
  ]);

  const rows = fresh ? orders : holdings;
  const sellsToday = fresh ? dueToday.length : 0;

  // Adjustment prompts for the 持有 view (only the user's recorded rows).
  const adjustBits: string[] = [];
  if (!fresh) {
    if (book.length > 0 && stillHeld.length < book.length) {
      adjustBits.push(`卫星腿 策略 ${book.length} 只 / 你持 ${stillHeld.length} 只`);
    }
    const off = holdings.filter((o) => o.rebalance != null).length;
    if (off > 0) adjustBits.push(`${off} 项权重偏离目标`);
  }

  /** The trade a row stands for: its own side, or the rebalance delta. */
  function tradeOf(o: Row): { side: 'BUY' | 'SELL'; pct: number } | null {
    if (o.side === 'BUY' || o.side === 'SELL') return { side: o.side, pct: o.pct };
    return o.rebalance ?? null;
  }

  async function record(o: Row) {
    const trade = tradeOf(o);
    if (!trade || !(o.px && o.px > 0) || trade.pct <= 0 || busy) return;
    setErr(null);
    setBusy(o.key);
    // Fills belong to the panel day; sells/rebalances are today's actions.
    const day = o.side === 'BUY' ? (panelDay ?? today) : today;
    try {
      const next = upsertWatchlistOpenTrade(loadWatchlist(), {
        symbol: o.symbol,
        name: o.name,
        side: trade.side,
        price: o.px,
        positionPct: round2(trade.pct),
        entryDate: today,
      });
      await saveWatchlist(next);
      setJustSynced((prev) => new Set(prev).add(o.symbol));
      // Journal only once per day — re-clicking a journal-only row just syncs
      // the watchlist instead of duplicating the ledger entry.
      if (!recordedOn(o.symbol, day)) {
        await recordUserTrade({
          symbol: o.symbol,
          side: trade.side,
          price: o.px,
          positionPct: round2(trade.pct),
          source: 'RESEARCH',
           market: 'CN',
           leg: o.leg,
           strategyMode: 'starship_robust',
           tradeDate: day,

        });
        void invalidateUserTradesQueries(queryClient).catch(() => {});
      }
    } catch (e) {
      setErr(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(null);
    }
  }

  if (!capital) {
    return (
      <div
        data-testid="a25-target-block"
        className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)]/45 px-3 py-2 text-xs text-[var(--k-muted)]"
      >
         {isB ? '星舰 B' : '稳健星舰 H2-a25'} · 今日下单：在上方「账户」填总资金后列出标的与份数

      </div>
    );
  }

  return (
    <div
      data-testid="a25-target-block"
      className="space-y-1 rounded-xl border border-sky-500/25 bg-[var(--k-surface)]/55 p-2 shadow-sm backdrop-blur-md"
    >
      <div className="flex flex-wrap items-baseline gap-2 px-1 text-xs">
         <span className="font-semibold">{isB ? '星舰 B' : 'H2-a25'} · {fresh ? '今日下单' : '当前持有'}</span>
         <span className="text-[10px] text-[var(--k-muted)]">人工账本目标</span>

        <span className="text-[var(--k-muted)]">
          {fresh
            ? `${panelDay} 14:30 · 总资金 ${cny(capital)}`
            : `${today} · 总资金 ${cny(capital)}`}
        </span>
        <span className="ml-auto tabular-nums text-[var(--k-muted)]">
          {rows.length
            ? fresh
              ? `共 ${rows.length} 笔${sellsToday ? `（含卖出 ${sellsToday}）` : ''}`
              : `持仓 ${rows.length} 项`
            : fresh
              ? '今日无下单'
              : '当前无持仓'}
        </span>
      </div>
      {err ? (
        <div className="px-1 text-xs text-red-600 dark:text-red-400">记录失败：{err}</div>
      ) : null}
      {rows.map((o) => {
        const trade = tradeOf(o);
        const lot =
          o.px && o.px > 0
            ? slotLotShares({
                symbol: o.symbol,
                price: o.px,
                capital,
                slotPct: trade?.pct ?? o.pct,
              })
            : null;
        const day = o.side === 'BUY' ? (panelDay ?? today) : today;
        const done = recordedOn(o.symbol, day) && watchlistSymbols.has(o.symbol);
        const unit = lot?.rule.unit ?? '股';
        const value = lot && o.px ? lot.shares * o.px : 0;
        const pnl = o.pnl;
        return (
          <div
            key={o.key}
            data-testid="a25-order-row"
            className={cn(
              'flex flex-wrap items-center gap-x-3 gap-y-0.5 rounded-lg px-3 py-2 text-xs',
              'border border-[var(--k-border)]/60 bg-[var(--k-bg)]/40',
              'transition-colors hover:border-sky-500/40 hover:bg-[var(--k-bg)]/70',
              done && 'opacity-60',
            )}
          >
            <a
              href={xueqiuUrl(o.ts)}
              target="_blank"
              rel="noopener noreferrer"
              title="在雪球查看"
              className="flex min-w-0 items-baseline gap-1.5 hover:underline"
            >
              <span className="font-mono font-semibold">{code(o.ts)}</span>
              <span className="max-w-28 truncate text-[var(--k-muted)]">{o.name}</span>
            </a>
            {o.note ? <span className="tabular-nums text-[var(--k-muted)]">{o.note}</span> : null}
            <span className="tabular-nums text-[var(--k-muted)]">¥{o.px ?? '—'}</span>
            <span className="w-12 shrink-0 text-right font-medium tabular-nums text-sky-700 dark:text-sky-300">
              {pctOf(o.pct)}
            </span>
            {o.cost != null || pnl != null || o.exitDue ? (
              <>
                {o.cost != null ? (
                  <span className="tabular-nums text-[var(--k-muted)]">成本 ¥{o.cost}</span>
                ) : null}
                {pnl != null ? (
                  <span
                    className={cn(
                      'tabular-nums font-medium',
                      pnl >= 0
                        ? 'text-emerald-700 dark:text-emerald-300'
                        : 'text-red-600 dark:text-red-400',
                    )}
                  >
                    {pnl >= 0 ? '+' : ''}
                    {pnl}%
                  </span>
                ) : null}
                {o.exitDue ? (
                  <span className="tabular-nums text-[var(--k-muted)]">
                    到期 {o.exitDue} <span className="text-sky-700 dark:text-sky-300">14:30</span> 卖出
                    {o.offBook ? (
                      <span className="text-amber-700 dark:text-amber-300">（面板信号）</span>
                    ) : null}
                  </span>
                ) : null}
              </>
            ) : null}
            {o.side !== 'BUY' && o.leg === 'satellite' && !o.exitDue ? (
              <span className="tabular-nums text-amber-700 dark:text-amber-300">
                策略外 · 无策略到期
              </span>
            ) : null}
            {o.side !== 'BUY' && o.rebalance != null && o.target != null ? (
              <span className="tabular-nums font-medium text-amber-700 dark:text-amber-300">
                目标 {pctOf(o.target)} · {o.pct > o.target ? '偏多' : '偏少'}{' '}
                {Math.abs(o.pct - o.target).toFixed(1)}pt
              </span>
            ) : null}
            {o.side !== 'HOLD' || o.rebalance ? (
              <span className="tabular-nums">
                {lot && lot.shares > 0
                  ? `${trade?.side === 'SELL' ? '卖' : '买'} ${lot.shares.toLocaleString('zh-CN')} ${unit} ≈ ${cny(value)}`
                  : '不足 1 手'}
              </span>
            ) : null}
            {o.side === 'HOLD' && o.rebalance ? (
              <Button
                type="button"
                size="sm"
                variant={done ? 'ghost' : 'outline'}
                disabled={done || busy === o.key}
                onClick={() => void record(o)}
                title="按目标调整仓位"
                className={cn(
                  'ml-auto h-6 rounded-full px-3 text-[11px]',
                  !done &&
                    o.rebalance.side === 'SELL' &&
                    'border-red-500/40 text-red-600 hover:bg-red-500/10 dark:text-red-300',
                  !done &&
                    o.rebalance.side === 'BUY' &&
                    'border-emerald-600/40 text-emerald-700 hover:bg-emerald-500/10 dark:text-emerald-300',
                )}
              >
                {done
                  ? '已记录'
                  : busy === o.key
                    ? '…'
                    : `${o.rebalance.side === 'SELL' ? '减仓' : '加仓'} ${pctOf(o.rebalance.pct)}`}
              </Button>
            ) : o.side === 'HOLD' ? (
              <span
                className={cn(
                  'ml-auto rounded-full px-3 py-0.5 text-[11px]',
                  userHolds(o.symbol)
                    ? 'text-[var(--k-muted)]'
                    : 'border border-amber-500/40 text-amber-700 dark:text-amber-300',
                )}
              >
                {userHolds(o.symbol) ? '已持有' : '未持有'}
              </span>
            ) : o.side === 'SELL' && !userHolds(o.symbol) ? (
              // Strategy holds it, the user does not — never journal a phantom sell.
              <span className="ml-auto rounded-full border border-[var(--k-border)] px-3 py-0.5 text-[11px] text-[var(--k-muted)]">
                未持有
              </span>
            ) : (
              <Button
                type="button"
                size="sm"
                variant={done ? 'ghost' : 'outline'}
                disabled={done || busy === o.key}
                onClick={() => void record(o)}
                className={cn(
                  'ml-auto h-6 rounded-full px-3 text-[11px]',
                  !done &&
                    o.side === 'SELL' &&
                    'border-red-500/40 text-red-600 hover:bg-red-500/10 dark:text-red-300',
                  !done &&
                    o.side === 'BUY' &&
                    'border-emerald-600/40 text-emerald-700 hover:bg-emerald-500/10 dark:text-emerald-300',
                )}
              >
                {done ? '已记录' : busy === o.key ? '…' : o.side === 'SELL' ? '卖出' : '买入'}
              </Button>
            )}
          </div>
        );
      })}
      <div className="space-y-0.5 px-1 text-[11px] text-[var(--k-muted)]">
        <div>
          {fresh
            ? '点「买入 / 卖出」记入自选 + 交易日志；份数按最新收盘价（以成交价为准）。'
            : '昨日下单已结转为持仓；到期日当天 14:30 卖出。'}
        </div>
        {adjustBits.length > 0 ? (
          <div className="text-amber-700 dark:text-amber-300">
            ⚠ 需调整：{adjustBits.join('；')}
          </div>
        ) : null}
      </div>
    </div>
  );
}
