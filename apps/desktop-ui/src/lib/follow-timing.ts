'use client';

/**
 * Execution-follow UX helpers (display only).
 *
 * Two problems this file encodes, for every strategy mode:
 *
 * 1. Signals are computed on the close, but the user cannot trade the close.
 *    Every NextAction row therefore carries an *achievable* clock:
 *    明日开盘跟 / 盘中可追 / 收盘待确认 → 次日开盘卖.
 * 2. A trail/must-act sell must outrank a target-weight rebuy on the same key.
 *    We collect the trail-exit symbols the backend already exposes (sticky
 *    `trailExit` / `pendingOpenSell` / `SELL_TO_REPO`) and remember them for a
 *    short display-only window, so a same-key ROTATE is annotated as a
 *    potential whip-saw instead of reading like a clean buy.
 *
 * Nothing here changes weights, TRAIL_PCT, cooldown (k=0), or order routing.
 */

import * as React from 'react';

import { getShanghaiTodayIso, isWeekdayShanghai } from '@/lib/market-hours';
import { loadJson, saveJson } from '@/lib/storage';

export type ExecutionTimingKind =
  /** Close-basis BUY (M30 / S-3 / B3 monthly) → next open. */
  | 'close-buy'
  /** Close-basis SELL already confirmed → next open. */
  | 'close-sell-confirmed'
  /** Intraday 14:30 trail print, close still decides → next open sell. */
  | 'close-sell-pending'
  /** 14:30 live panel fill — tradable before the close. */
  | 'intraday-buy'
  /** Satellite/fixed due date — sell at 14:30 today. */
  | 'exit-due'
  /** No action. */
  | 'hold';

export type ExecutionTiming = {
  label: string;
  detail: string;
  tone: 'buy' | 'sell' | 'warn' | 'muted';
};

/** Next-open wording (weekends/holidays read as "下一交易日"). */
export function nextOpenWord(now: Date = new Date()): string {
  return isWeekdayShanghai(now) ? '明日' : '下一交易日';
}

/**
 * The achievable execution clock for one action. Pure + injectable clock so
 * copy is testable without freezing the whole test runner.
 */
export function executionTiming(
  kind: ExecutionTimingKind,
  now: Date = new Date(),
): ExecutionTiming {
  const next = nextOpenWord(now);
  const weekday = isWeekdayShanghai(now);
  switch (kind) {
    case 'close-buy':
      return {
        label: `${next}开盘跟`,
        detail: '收盘后信号 → 次日开盘执行；不追收盘价',
        tone: 'buy',
      };
    case 'close-sell-confirmed':
      return {
        label: `${next}开盘卖`,
        detail: '信号已在收盘确认 → 次日开盘执行（不盘中挂单）',
        tone: 'sell',
      };
    case 'close-sell-pending':
      return {
        label: '收盘待确认 → 次日开盘卖',
        detail: '14:30 现价触线只做预警；收盘仍触发才卖，次日开盘执行',
        tone: 'warn',
      };
    case 'intraday-buy':
      return {
        label: weekday ? '盘中可追' : '下一交易日盘中可追',
        detail: weekday
          ? '14:30 现场面板信号 → 收盘前可按现价执行'
          : '下一交易日 14:30 现场面板确认后再执行',
        tone: 'buy',
      };
    case 'exit-due':
      return {
        label: weekday ? '今日 14:30 卖出' : '下一交易日 14:30 卖出',
        detail: '到期日 14:30 现场执行（不提前、不拖到收盘）',
        tone: 'sell',
      };
    case 'hold':
    default:
      return { label: '无需操作', detail: '未达阈值，跟踪中', tone: 'muted' };
  }
}

/** Copy shown on a BUY that targets a leg we just stopped out of. */
export const WHIP_ANNOTATION = '刚止损，回买=鞭打，请确认';

/** Copy after Yuan accepts `hold_same_key_rebuy` (still a confirmation, not a block). */
export const WHIP_OVERLAY_COPY = '建议暂缓 · 需确认';

/** Badge copy: accepted overlay downgrades the warning, never the engine. */
export function whipCopy(overlayActive: boolean): string {
  return overlayActive ? WHIP_OVERLAY_COPY : WHIP_ANNOTATION;
}

/** Canonical key for cross-mode symbol comparison (ETF:513350.SH → 513350). */
export function followSymbolKey(symbol: string | null | undefined): string {
  const s = String(symbol || '').trim().toUpperCase();
  if (!s) return '';
  return (s.includes(':') ? s.split(':')[1]! : s).split('.')[0]!;
}

type SleeveLike = {
  action?: string | null;
  symbol?: string | null;
  pendingOpenSell?: boolean | null;
  trailExit?: boolean | null;
};

type HoldingLike = {
  symbol?: string | null;
  trailExit?: boolean | null;
  pendingOpenSell?: boolean | null;
};

/**
 * Trail/must-act exit keys the backend is currently signalling. Uses the
 * sticky per-holding `trailExit` / `pendingOpenSell` flags plus the sleeve's
 * `SELL_TO_REPO` action — no client-side re-derivation of the trail math.
 */
export function collectTrailExitSymbols(
  sleeve: SleeveLike | null | undefined,
  holdings: HoldingLike[] | null | undefined,
): string[] {
  const out = new Set<string>();
  const add = (s: string | null | undefined) => {
    const key = followSymbolKey(s);
    if (key) out.add(key);
  };
  if (sleeve) {
    if (String(sleeve.action || '').toUpperCase() === 'SELL_TO_REPO') add(sleeve.symbol);
    if (sleeve.pendingOpenSell || sleeve.trailExit) add(sleeve.symbol);
  }
  for (const h of holdings ?? []) {
    if (h.pendingOpenSell || h.trailExit) add(h.symbol);
  }
  return [...out];
}

/**
 * True when `symbol` is a BUY target on a key that was just stopped out.
 * The caller decides whether to suppress (same-day must-act) or annotate
 * (post-trail ROTATE); this helper only answers the question.
 */
export function isWhipRebuy(
  side: 'BUY' | 'SELL' | 'HOLD' | null | undefined,
  symbol: string | null | undefined,
  trailExitSymbols: Iterable<string> | null | undefined,
): boolean {
  if (String(side || '').toUpperCase() !== 'BUY') return false;
  const key = followSymbolKey(symbol);
  if (!key || !trailExitSymbols) return false;
  for (const s of trailExitSymbols) {
    if (followSymbolKey(s) === key) return true;
  }
  return false;
}

// ------------------------------------------------------------ trail memory ---

const TRAIL_MEMORY_KEY = 'karios.followTrailMemory.v1';
const TRAIL_MEMORY_MAX_DAYS = 30;

export type TrailMemory = Record<string, string>;

export function readTrailMemory(): TrailMemory {
  const raw = loadJson<unknown>(TRAIL_MEMORY_KEY, {});
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return {};
  const out: TrailMemory = {};
  for (const [k, v] of Object.entries(raw as Record<string, unknown>)) {
    const key = followSymbolKey(k);
    if (key && typeof v === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(v)) out[key] = v;
  }
  return out;
}

function isoDaysAgo(days: number, now: Date = new Date()): string {
  return new Date(now.getTime() - days * 86_400_000).toISOString().slice(0, 10);
}

/**
 * Remember trail-exit keys we have observed, so the *next day's* target-weight
 * ROTATE back into the same ETF can be annotated. Display only — it never
 * blocks the trade (cooldown research was REJECTED; k stays 0).
 */
export function rememberTrailExits(
  symbols: string[],
  day: string = getShanghaiTodayIso(),
): TrailMemory {
  if (!symbols.length) return readTrailMemory();
  const floor = isoDaysAgo(TRAIL_MEMORY_MAX_DAYS);
  const next: TrailMemory = {};
  for (const [k, v] of Object.entries(readTrailMemory())) {
    if (v >= floor) next[k] = v;
  }
  for (const s of symbols) {
    const key = followSymbolKey(s);
    if (key) next[key] = day;
  }
  saveJson(TRAIL_MEMORY_KEY, next);
  return next;
}

/** Keys stopped out within `days`, mapped to their (latest) signal date. */
export function recentTrailExits(
  days = 10,
  now: Date = new Date(),
): Map<string, string> {
  const floor = isoDaysAgo(days, now);
  const out = new Map<string, string>();
  for (const [k, v] of Object.entries(readTrailMemory())) {
    if (v >= floor) out.set(k, v);
  }
  return out;
}

/**
 * Observe the current trail-exit keys and return the recent (display-only)
 * set for whip annotations. Cross-day is the point: once a stop-out is
 * executed the holding disappears, but the memory keeps the key around for a
 * short window so the next-day ROTATE does not look like a clean buy.
 */
export function useRecentTrailExits(
  observed: string[],
  days = 10,
): Map<string, string> {
  const key = React.useMemo(
    () => [...new Set(observed.map(followSymbolKey).filter(Boolean))].sort().join(','),
    [observed],
  );
  const [memory, setMemory] = React.useState<TrailMemory>(() => readTrailMemory());
  React.useEffect(() => {
    if (!key) return;
    setMemory(rememberTrailExits(key.split(',')));
  }, [key]);
  return React.useMemo(() => {
    const floor = isoDaysAgo(days);
    const out = new Map<string, string>();
    for (const [k, v] of Object.entries(memory)) {
      if (v >= floor) out.set(k, v);
    }
    return out;
  }, [memory, days]);
}

/** Union helper: observed trail keys + remembered keys (canonical keys). */
export function mergeTrailExitKeys(
  observed: Iterable<string>,
  remembered: Iterable<string>,
): Set<string> {
  const out = new Set<string>();
  for (const s of observed) {
    const key = followSymbolKey(s);
    if (key) out.add(key);
  }
  for (const s of remembered) {
    const key = followSymbolKey(s);
    if (key) out.add(key);
  }
  return out;
}
