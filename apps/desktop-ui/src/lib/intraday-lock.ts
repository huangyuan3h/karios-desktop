/**
 * Intraday action-price locking (2026-08-12 · OPT-098).
 *
 * The user trades at 14:00 and needs a STABLE, correct action signal between
 * 12:00 and 14:30 — the realtime quote makes the action flip-flop whenever
 * the price touches a stop/trail line intraday.
 *
 * Locking rules (Asia/Shanghai, trading time only):
 *  - 12:00–13:00 (lunch break): use the morning close (trendClose) — already
 *    stable, no quote moves anyway.
 *  - 13:00–14:00: realtime as usual (intraday alerts still live).
 *  - 14:00–15:00 (close): use a ONE-TIME 14:00 snapshot price (first quote
 *    seen in the window is frozen per symbol per day). The user trades
 *    mostly AFTER 14:00, so the action freezes at the 2pm snapshot and
 *    cannot flip while they act.
 *  - Otherwise: realtime price as usual.
 */

export type IntradayLockInput = {
  symbol: string;
  /** Realtime resolved price (resolveWatchlistCurrentPrice result). */
  realtimePrice: number | null;
  /** Morning/most-recent close from trendok. */
  trendClose: number | null;
  /** Shanghai wall-clock now. */
  now: Date;
  /** Whether we are inside trading hours (polling is active). */
  tradingTime: boolean;
};

const SNAPSHOT_CACHE = new Map<string, { day: string; price: number }>();

function shanghaiMinutes(now: Date): { day: string; minutes: number } {
  const parts = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Asia/Shanghai',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
  }).formatToParts(now);
  const get = (t: string) => parts.find((p) => p.type === t)?.value ?? '0';
  const day = `${get('year')}-${get('month')}-${get('day')}`;
  const minutes = Number(get('hour')) * 60 + Number(get('minute'));
  return { day, minutes };
}

const LUNCH_START = 12 * 60; // 12:00
const LUNCH_END = 13 * 60; // 13:00
const LOCK_START = 14 * 60; // 14:00 — snapshot freeze begins (user trades after 2pm)
const LOCK_END = 15 * 60; // 15:00 close

/** Stable price used ONLY for action derivation (not display/PnL). */
export function resolveStableActionPrice(input: IntradayLockInput): number | null {
  const { symbol, realtimePrice, trendClose, now, tradingTime } = input;
  if (!tradingTime) {
    return realtimePrice;
  }
  const { day, minutes } = shanghaiMinutes(now);

  // Lunch break: morning close is the stable reference.
  if (minutes >= LUNCH_START && minutes < LUNCH_END) {
    return trendClose ?? realtimePrice;
  }

  // 14:00–15:00: frozen 14:00 snapshot (first quote in the window).
  if (minutes >= LOCK_START && minutes < LOCK_END) {
    const cached = SNAPSHOT_CACHE.get(symbol);
    if (cached && cached.day === day) {
      return cached.price;
    }
    if (realtimePrice != null && Number.isFinite(realtimePrice)) {
      SNAPSHOT_CACHE.set(symbol, { day, price: realtimePrice });
      return realtimePrice;
    }
    return trendClose ?? realtimePrice;
  }

  // Before 12:00 / 13:00–14:00 / after 15:00: realtime as usual.
  return realtimePrice;
}

/** Test-only: clear the frozen snapshot cache. */
export function clearIntradaySnapshotCache(): void {
  SNAPSHOT_CACHE.clear();
}
