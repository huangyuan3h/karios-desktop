import type {
  ExecutionDecisionChange,
  ExecutionGate,
  ExecutionJournalCard,
  ExecutionSnapshotIngestRequest,
  ExecutionSnapshotIngestResponse,
  ExecutionSnapshotSource,
} from '@karios/shared';
import type { QueryClient } from '@tanstack/react-query';

import { apiGetJson, apiPostJson } from '@/lib/api/client';
import type { TrendOkResult } from '@/lib/api/types';
import {
  buildCatalystPurgeMap,
  DEFAULT_CATALYST_MAX_AGE_DAYS,
  fetchCatalystStocks,
} from '@/lib/alpha-radar-catalyst';
import { DATA_SYNC_BASE_URL } from '@/lib/endpoints';
import {
  buildSectorExposureFromWatchlist,
  buildSleeveExposurePct,
  deriveActionCard,
  resolveIndustryName,
  type CatalystPurgeHint,
} from '@/lib/execution-action';
import type { MainlineAllowSet } from '@/lib/hot-industry-picks';
import { getShanghaiTodayIso, isShanghaiTradingTime } from '@/lib/market-hours';
import { buildWatchlistRowMetrics } from '@/lib/watchlist-metrics';
import type { WatchlistItem } from '@/lib/watchlist-storage';

const QUEUE_KEY = 'karios.executionJournal.queue.v1';

/** Fields that qualify a symbol for Latest Actions (delta logging). */
export const LATEST_ACTIONS_DELTA_FIELDS = new Set([
  'action',
  'trigger',
  'hardStop',
  'trailStop',
]);

/**
 * Symbols whose Action / Trigger / HardStop / TrailStop changed.
 * Silent WATCH→WATCH with unchanged stops are excluded.
 */
export function symbolsWithLatestActionDeltas(
  changes: Array<Pick<ExecutionDecisionChange, 'scope' | 'symbol' | 'field'>>,
): Set<string> {
  const out = new Set<string>();
  for (const c of changes) {
    if (c.scope !== 'symbol') continue;
    if (!LATEST_ACTIONS_DELTA_FIELDS.has(String(c.field || ''))) continue;
    const sym = String(c.symbol || '').trim();
    if (sym) out.add(sym);
  }
  return out;
}

export function filterLatestActionCards<T extends { symbol?: string | null }>(
  cards: T[],
  changes: Array<Pick<ExecutionDecisionChange, 'scope' | 'symbol' | 'field'>>,
): T[] {
  const delta = symbolsWithLatestActionDeltas(changes);
  if (!delta.size) return [];
  return cards.filter((c) => delta.has(String(c.symbol || '').trim()));
}

export type BuildExecutionSnapshotInput = {
  items: WatchlistItem[];
  trend: Record<string, TrendOkResult | undefined>;
  quotes: Record<
    string,
    | {
        price?: number | null;
        preClose?: number | null;
        pctChg?: number | null;
        tradeTime?: string | null;
        amount?: number | null;
        volume?: number | null;
      }
    | undefined
  >;
  gate: ExecutionGate | null;
  mainlineAllow: MainlineAllowSet | null;
  tradingTime?: boolean;
  todaySh?: string;
  sectorOutflowBlock?: boolean;
  catalystBySymbol?: Map<string, CatalystPurgeHint> | null;
  source: ExecutionSnapshotSource;
  meta?: Record<string, unknown>;
};

export function buildExecutionSnapshotPayload(
  input: BuildExecutionSnapshotInput,
): ExecutionSnapshotIngestRequest | null {
  const { items, trend, quotes, gate, mainlineAllow, source, meta } = input;
  if (!gate) return null;
  const tradingTime = input.tradingTime ?? isShanghaiTradingTime();
  const todaySh = input.todaySh ?? getShanghaiTodayIso();
  const sectorOutflowBlock = input.sectorOutflowBlock === true;
  const catalystBySymbol = input.catalystBySymbol ?? null;
  const sectorExposureByIndustry = buildSectorExposureFromWatchlist(items, trend);
  const sleeveExposurePct = buildSleeveExposurePct(items);
  const cards: ExecutionJournalCard[] = [];
  for (const it of items) {
    const t = trend[it.symbol];
    const q = quotes[it.symbol];
    const rowMetrics = buildWatchlistRowMetrics({
      symbol: it.symbol,
      trend: t,
      quote: q
        ? {
            price: q.price ?? null,
            tradeTime: q.tradeTime ?? null,
            amount: q.amount ?? null,
            volume: q.volume ?? null,
            preClose: q.preClose ?? null,
            pctChg: q.pctChg ?? null,
          }
        : null,
      tradingTime,
      todaySh,
    });
    const card = deriveActionCard({
      symbol: it.symbol,
      gate,
      trendok: t ?? null,
      position: it,
      currentPrice: rowMetrics.current,
      mainlineAllow,
      intradayChgPct: rowMetrics.intradayChgPct,
      gapUp: typeof t?.gapUp === 'boolean' ? t.gapUp : null,
      marketRegime: t?.marketRegime ?? null,
      sectorExposureByIndustry,
      sleeveExposurePct,
      sectorOutflowBlock,
      catalyst: catalystBySymbol?.get(it.symbol) ?? null,
      todaySh,
    });
    cards.push({
      ...card,
      positionPct: typeof it.positionPct === 'number' ? it.positionPct : null,
      costPrice: typeof it.costPrice === 'number' ? it.costPrice : null,
      currentPrice: rowMetrics.current,
      industry: resolveIndustryName(t ?? null),
    });
  }
  return {
    source,
    tradeDate: todaySh,
    gate,
    cards,
    meta: {
      watchlistCount: items.length,
      tradingTime,
      ...meta,
    },
  };
}

function readQueue(): ExecutionSnapshotIngestRequest[] {
  if (typeof window === 'undefined') return [];
  try {
    const raw = window.localStorage.getItem(QUEUE_KEY);
    if (!raw) return [];
    const parsed = JSON.parse(raw) as unknown;
    return Array.isArray(parsed) ? (parsed as ExecutionSnapshotIngestRequest[]) : [];
  } catch {
    return [];
  }
}

function writeQueue(items: ExecutionSnapshotIngestRequest[]) {
  if (typeof window === 'undefined') return;
  try {
    if (!items.length) {
      window.localStorage.removeItem(QUEUE_KEY);
      return;
    }
    window.localStorage.setItem(QUEUE_KEY, JSON.stringify(items.slice(-20)));
  } catch {
    // ignore quota
  }
}

export async function flushExecutionSnapshotQueue(): Promise<void> {
  const q = readQueue();
  if (!q.length) return;
  const remaining: ExecutionSnapshotIngestRequest[] = [];
  for (const item of q) {
    try {
      await apiPostJson<ExecutionSnapshotIngestResponse>('/execution/snapshots', item);
    } catch {
      remaining.push(item);
    }
  }
  writeQueue(remaining);
}

export async function pushExecutionSnapshot(
  payload: ExecutionSnapshotIngestRequest,
): Promise<ExecutionSnapshotIngestResponse | null> {
  try {
    await flushExecutionSnapshotQueue();
    return await apiPostJson<ExecutionSnapshotIngestResponse>('/execution/snapshots', payload);
  } catch {
    const q = readQueue();
    q.push(payload);
    writeQueue(q);
    return null;
  }
}

export async function fetchExecutionJournalMarkdown(opts?: {
  tradeDate?: string;
  days?: number;
}): Promise<string> {
  const params = new URLSearchParams();
  if (opts?.tradeDate) params.set('trade_date', opts.tradeDate);
  if (opts?.days != null) params.set('days', String(opts.days));
  const qs = params.toString();
  const path = qs ? `/execution/journal.md?${qs}` : '/execution/journal.md';
  // apiGetJson expects JSON; use fetch text via api client base
  const { DATA_SYNC_BASE_URL } = await import('@/lib/endpoints');
  const res = await fetch(`${DATA_SYNC_BASE_URL}${path}`, { cache: 'no-store' });
  if (!res.ok) {
    const txt = await res.text().catch(() => '');
    throw new Error(`${res.status} ${res.statusText}${txt ? `: ${txt}` : ''}`);
  }
  return res.text();
}

export async function captureAndPushExecutionSnapshot(
  queryClient: QueryClient,
  opts: {
    items: WatchlistItem[];
    gate: ExecutionGate | null;
    mainlineAllow: MainlineAllowSet | null;
    sectorOutflowBlock?: boolean;
    source: ExecutionSnapshotSource;
    trend?: Record<string, TrendOkResult | undefined>;
    quotes?: BuildExecutionSnapshotInput['quotes'];
  },
): Promise<ExecutionSnapshotIngestResponse | null> {
  const symbols = opts.items.map((i) => i.symbol);
  let trend = opts.trend;
  let quotes = opts.quotes;
  if (!trend || !quotes) {
    const { refetchWatchlistMarket } = await import('@/lib/queries/watchlist');
    const snap = await refetchWatchlistMarket(queryClient, symbols, { forceMarket: false });
    trend = snap.trend as Record<string, TrendOkResult | undefined>;
    quotes = snap.quotes;
  }
  const catalystBySymbol = await fetchCatalystStocks(
    DATA_SYNC_BASE_URL,
    50,
    DEFAULT_CATALYST_MAX_AGE_DAYS,
  )
    .then((resp) => buildCatalystPurgeMap(resp))
    .catch(() => null);
  const payload = buildExecutionSnapshotPayload({
    items: opts.items,
    trend: trend ?? {},
    quotes: quotes ?? {},
    gate: opts.gate,
    mainlineAllow: opts.mainlineAllow,
    sectorOutflowBlock: opts.sectorOutflowBlock === true,
    catalystBySymbol,
    source: opts.source,
  });
  if (!payload) return null;
  return pushExecutionSnapshot(payload);
}
