import { isShanghaiTradingTime } from '@/lib/market-hours';

export const DASHBOARD_POLL_MS = 60_000;
export const WATCHLIST_POLL_MS = 10 * 60_000;
export const MACRO_POLL_MS = 45_000;

export function dashboardRefetchIntervalMs(): number | false {
  return isShanghaiTradingTime() ? DASHBOARD_POLL_MS : false;
}
