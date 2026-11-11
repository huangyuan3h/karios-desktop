import { isShanghaiSyncWindow, isShanghaiTradingTime } from '@/lib/market-hours';

export const DASHBOARD_POLL_MS = 60_000;
export const WATCHLIST_POLL_MS = 10 * 60_000;
export const MACRO_POLL_MS = 45_000;
export const SCREENER_STALE_MS = 5 * 60_000;

/** Heavy SSE sync cadence: ≤90s in trading hours per dashboard auto-sync spec. */
export const DASHBOARD_AUTO_SYNC_TRADING_MS = 90_000;
/** After-hours quote window (until 20:00 Shanghai weekday): 3 min, no spam. */
export const DASHBOARD_AUTO_SYNC_AFTER_HOURS_MS = 3 * 60_000;

export function dashboardRefetchIntervalMs(): number | false {
  return isShanghaiTradingTime() ? DASHBOARD_POLL_MS : false;
}

/** Auto-sync timer period: 90s trading, 3min after-hours quote window, false off-hours. */
export function dashboardAutoSyncIntervalMs(): number | false {
  if (isShanghaiTradingTime()) return DASHBOARD_AUTO_SYNC_TRADING_MS;
  if (isShanghaiSyncWindow()) return DASHBOARD_AUTO_SYNC_AFTER_HOURS_MS;
  return false;
}
