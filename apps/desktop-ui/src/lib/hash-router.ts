/**
 * Hash router for the desktop UI.
 *
 * Every key page has a stable, shareable URL so reviews/archives can link
 * straight to a page or stock and external AI assistants can deep-link:
 *
 *   #/dashboard            #/watchlist            #/news
 *   #/market               #/industry-flow        #/alpha
 *   #/decision             #/backtest             #/scheduler
 *   #/screener             #/settings             #/broker
 *   #/index                #/stock/HK%3A00700     #/journal
 *   #/journal/write        #/journal/review       #/journal/write/<id>
 */

export interface ParsedRoute {
  page: string;
  symbol: string | null;
  journalMode: 'read' | 'write' | 'review' | null;
  journalId: string | null;
}

export const PAGE_IDS = [
  'dashboard',
  'index',
  'news',
  'market',
  'industryFlow',
  'alpha',
  'decision',
  'backtest',
  'watchlist',
  'broker',
  'journal',
  'screener',
  'scheduler',
  'settings',
  'stock',
] as const;

const SLUG_TO_PAGE: Record<string, string> = {
  'industry-flow': 'industryFlow',
  dashboard: 'dashboard',
  index: 'index',
  news: 'news',
  market: 'market',
  alpha: 'alpha',
  decision: 'decision',
  backtest: 'backtest',
  watchlist: 'watchlist',
  broker: 'broker',
  journal: 'journal',
  screener: 'screener',
  scheduler: 'scheduler',
  settings: 'settings',
  stock: 'stock',
};

/** Parse `window.location.hash` (e.g. `#/stock/HK:00700`) into a route. */
export function parseHash(hash: string): ParsedRoute {
  const raw = (hash || '').trim().replace(/^#\/?/, '');
  const parts = raw.split('/').map((p) => {
    try {
      return decodeURIComponent(p);
    } catch {
      return p;
    }
  });
  const page = SLUG_TO_PAGE[parts[0] ?? ''] ?? 'dashboard';
  if (page === 'stock' && parts[1]) {
    return { page, symbol: parts[1], journalMode: null, journalId: null };
  }
  if (page === 'journal') {
    const mode = parts[1];
    if (mode === 'write' || mode === 'review') {
      return { page, symbol: null, journalMode: mode, journalId: parts[2] ?? null };
    }
    return { page, symbol: null, journalMode: 'read', journalId: null };
  }
  return { page, symbol: null, journalMode: null, journalId: null };
}

/** Build the hash string for a route (no leading `#/` normalization issues). */
export function buildHash(route: ParsedRoute): string {
  switch (route.page) {
    case 'stock':
      return route.symbol ? `#/stock/${encodeURIComponent(route.symbol)}` : '#/stock';
    case 'journal': {
      if (route.journalMode === 'write') {
        return route.journalId
          ? `#/journal/write/${encodeURIComponent(route.journalId)}`
          : '#/journal/write';
      }
      if (route.journalMode === 'review') return '#/journal/review';
      return '#/journal';
    }
    default:
      return `#/${route.page}`;
  }
}

/** Shareable href for a page (for markdown/links). */
export function pageHref(page: string, symbol?: string | null): string {
  return buildHash({ page, symbol: symbol ?? null, journalMode: null, journalId: null });
}

/** Current hash from `window.location` (safe for SSR-less client code). */
export function currentHash(): string {
  return typeof window === 'undefined' ? '' : window.location.hash;
}
