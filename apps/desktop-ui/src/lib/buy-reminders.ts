'use client';

/**
 * Local buy reminders set from the S-3 "下午 2 点买入清单" (PortfolioHealthCard).
 * These are personal sticky notes (target price + note) stored in
 * localStorage only — there is no backend price-alert machinery yet.
 */

export interface BuyReminder {
  symbol: string;
  name: string | null;
  targetPrice: number | null;
  note: string;
  createdAt: string;
}

const STORAGE_KEY = 'karios_buy_reminders';
export const BUY_REMINDERS_UPDATED_EVENT = 'karios-buy-reminders-updated';

export function loadBuyReminders(): BuyReminder[] {
  if (typeof window === 'undefined') return [];
  try {
    const raw = window.localStorage.getItem(STORAGE_KEY);
    if (!raw) return [];
    const arr = Array.isArray(JSON.parse(raw)) ? (JSON.parse(raw) as unknown[]) : [];
    return arr
      .filter((x): x is BuyReminder => Boolean(x && typeof x === 'object'))
      .map((x) => {
        const r = x as Partial<BuyReminder>;
        return {
          symbol: String(r.symbol ?? '').trim(),
          name: typeof r.name === 'string' ? r.name : null,
          targetPrice:
            typeof r.targetPrice === 'number' && Number.isFinite(r.targetPrice)
              ? r.targetPrice
              : null,
          note: typeof r.note === 'string' ? r.note : '',
          createdAt: String(r.createdAt ?? new Date().toISOString()),
        };
      })
      .filter((r) => Boolean(r.symbol));
  } catch {
    return [];
  }
}

function persistBuyReminders(items: BuyReminder[]): void {
  if (typeof window === 'undefined') return;
  window.localStorage.setItem(STORAGE_KEY, JSON.stringify(items));
  window.dispatchEvent(new CustomEvent(BUY_REMINDERS_UPDATED_EVENT));
}

export function addBuyReminder(reminder: BuyReminder): BuyReminder[] {
  const next = [reminder, ...loadBuyReminders().filter((r) => r.symbol !== reminder.symbol)];
  persistBuyReminders(next);
  return next;
}

export function removeBuyReminder(symbol: string): BuyReminder[] {
  const next = loadBuyReminders().filter((r) => r.symbol !== symbol);
  persistBuyReminders(next);
  return next;
}

export function isWatchlistReminded(symbol: string): boolean {
  return loadBuyReminders().some((r) => r.symbol === symbol);
}
