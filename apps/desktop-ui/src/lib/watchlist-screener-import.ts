import { apiGetJson } from '@/lib/api/client';
import type { TrendOkResult } from '@/lib/api/types';
import { isShanghaiQuoteWindow } from '@/lib/market-hours';
import { fetchTrendOkMap, normalizeScreenerSymbol } from '@/lib/screenerExport';
import { fetchScreenerSnapshotsMap } from '@/lib/queries/screener';
import {
  loadWatchlist,
  saveWatchlist,
  type WatchlistItem,
} from '@/lib/watchlist-storage';

export type { TrendOkResult };

/** TIP-002: Screener → Watchlist funnel counts (same-day observability). */
export type ScreenerFunnel = {
  tvHit: number;
  passPullback: number;
  passTrendOk: number;
  addedNew: number;
  droppedByPullback: number;
};

export type ScreenerImportDebugState = {
  updatedAt: string | null;
  scanned: number;
  trendOkCount: number;
  rows: TrendOkResult[];
  funnel: ScreenerFunnel;
};

export type ScreenerImportResult = {
  message: string;
  addedCount: number;
  debug: ScreenerImportDebugState;
};

type TvScreener = {
  id: string;
  name: string;
  enabled: boolean;
};

type TvSnapshotDetail = {
  rows: Record<string, string>[];
};

export type ScreenerImportOptions = {
  existingItems?: WatchlistItem[];
  silent?: boolean;
  onStage?: (label: string, cur?: number, total?: number) => void;
};

export function emptyScreenerFunnel(): ScreenerFunnel {
  return {
    tvHit: 0,
    passPullback: 0,
    passTrendOk: 0,
    addedNew: 0,
    droppedByPullback: 0,
  };
}

export function formatScreenerFunnel(funnel: ScreenerFunnel | null | undefined): string {
  if (!funnel) return '';
  return `TV ${funnel.tvHit} → pullback ${funnel.passPullback} → TrendOK ${funnel.passTrendOk} → +${funnel.addedNew}`;
}

function makeDebug(partial: {
  scanned: number;
  trendOkCount: number;
  rows: TrendOkResult[];
  funnel: ScreenerFunnel;
}): ScreenerImportDebugState {
  return {
    updatedAt: new Date().toISOString(),
    scanned: partial.scanned,
    trendOkCount: partial.trendOkCount,
    rows: partial.rows,
    funnel: partial.funnel,
  };
}

function parseScreenerNumber(raw: unknown): number | null {
  const s = String(raw ?? '').trim();
  if (!s) return null;
  const m = s.match(/-?\d+(?:,\d{3})*(?:\.\d+)?/);
  if (!m) return null;
  const n = Number(m[0].replaceAll(',', ''));
  return Number.isFinite(n) ? n : null;
}

function pickFirstRowValue(row: Record<string, string>, keys: string[]): string {
  for (const k of keys) {
    const v = row[k];
    if (typeof v === 'string' && v.trim()) return v;
  }
  return '';
}

function getRetracementRatioFromScreenerRow(row: Record<string, string>): number | null {
  const priceRaw = pickFirstRowValue(row, ['Price', 'Last', 'Close']);
  const high52wRaw = pickFirstRowValue(row, [
    'High 52W',
    'High | Interval52Weeks',
    '52 Week High',
    'High 52 week',
    'High 52Wk',
  ]);
  const price = parseScreenerNumber(priceRaw);
  const high52w = parseScreenerNumber(high52wRaw);
  if (price == null || high52w == null || high52w <= 0) return null;
  return (price - high52w) / high52w;
}

export async function importFromScreener(options: ScreenerImportOptions = {}): Promise<ScreenerImportResult> {
  const existingItems = options.existingItems ?? loadWatchlist();
  const onStage = options.onStage;
  const setStep = (label: string, cur?: number, total?: number) => {
    onStage?.(label, cur, total);
  };

  setStep('Loading enabled screeners');
  const s = await apiGetJson<{ items: TvScreener[] }>('/integrations/tradingview/screeners');
  const enabled = (s.items || []).filter((x) => x && x.enabled);
  if (!enabled.length) {
    const debug = makeDebug({
      scanned: 0,
      trendOkCount: 0,
      rows: [],
      funnel: emptyScreenerFunnel(),
    });
    return { message: 'No enabled screeners.', addedCount: 0, debug };
  }

  setStep('Loading latest snapshots (DB)', 0, enabled.length);
  const snapshotMap = await fetchScreenerSnapshotsMap(enabled.map((sc) => sc.id));
  const snapshotDetails: TvSnapshotDetail[] = [];
  for (const sc of enabled) {
    const detail = snapshotMap[sc.id];
    if (detail?.rows) snapshotDetails.push({ rows: detail.rows });
  }

  const candidates: string[] = [];
  const retracementBySymbol = new Map<string, number[]>();
  for (const snap of snapshotDetails) {
    if (!snap) continue;
    for (const r of snap.rows || []) {
      const raw = String(r['Ticker'] || r['Symbol'] || '').trim();
      const sym = normalizeScreenerSymbol(raw);
      if (!sym) continue;
      candidates.push(sym);
      const ratio = getRetracementRatioFromScreenerRow(r);
      if (ratio == null) continue;
      const arr = retracementBySymbol.get(sym) ?? [];
      arr.push(ratio);
      retracementBySymbol.set(sym, arr);
    }
  }

  const uniq = Array.from(new Set(candidates)).slice(0, 2000);
  if (!uniq.length) {
    const debug = makeDebug({
      scanned: 0,
      trendOkCount: 0,
      rows: [],
      funnel: emptyScreenerFunnel(),
    });
    return { message: 'No symbols found in latest screener snapshots.', addedCount: 0, debug };
  }

  const minPullback = -0.15;
  const maxPullback = -0.05;
  const filtered = uniq.filter((sym) => {
    const rs = retracementBySymbol.get(sym) ?? [];
    return rs.some((x) => x >= minPullback && x <= maxPullback);
  });
  const droppedByPullback = uniq.length - filtered.length;
  if (!filtered.length) {
    const funnel: ScreenerFunnel = {
      tvHit: uniq.length,
      passPullback: 0,
      passTrendOk: 0,
      addedNew: 0,
      droppedByPullback,
    };
    const debug = makeDebug({
      scanned: uniq.length,
      trendOkCount: 0,
      rows: [],
      funnel,
    });
    return {
      message: `Screener scanned ${uniq.length} symbols, but 0 passed pullback ratio filter ((Current-52WHigh)/52WHigh in [-0.15, -0.05]). Funnel: ${formatScreenerFunnel(funnel)}.`,
      addedCount: 0,
      debug,
    };
  }

  setStep('TrendOK check', 0, filtered.length);
  const trendMap = await fetchTrendOkMap(filtered, {
    realtime: isShanghaiQuoteWindow(),
  });
  const okSymsCached: string[] = [];
  const debugBySym: Record<string, TrendOkResult> = {};
  for (const sym of filtered) {
    const rr = trendMap.get(sym);
    if (!rr?.symbol) continue;
    debugBySym[rr.symbol] = rr;
    if (rr.trendOk === true) okSymsCached.push(rr.symbol);
  }
  setStep('TrendOK check', filtered.length, filtered.length);
  const okUniq = Array.from(new Set(okSymsCached));

  const existing = new Set(existingItems.map((x) => x.symbol));
  const now = new Date().toISOString();
  const added: WatchlistItem[] = okUniq
    .filter((sym) => !existing.has(sym))
    .map((sym) => ({
      symbol: sym,
      name: null,
      addedAt: now,
      color: '#ffffff',
      source: 'screener' as const,
    }));

  const funnel: ScreenerFunnel = {
    tvHit: uniq.length,
    passPullback: filtered.length,
    passTrendOk: okUniq.length,
    addedNew: added.length,
    droppedByPullback,
  };

  const debug = makeDebug({
    scanned: filtered.length,
    trendOkCount: okUniq.length,
    rows: filtered.map(
      (sym) =>
        debugBySym[sym] ??
        ({
          symbol: sym,
          trendOk: null,
          score: null,
          missingData: ['no_result'],
        } satisfies TrendOkResult),
    ),
    funnel,
  });

  if (!added.length) {
    return {
      message: `Screener scanned ${uniq.length} symbols; pullback-filter kept ${filtered.length} (dropped ${droppedByPullback}); TrendOK ✅: ${okUniq.length}; nothing new to add. Funnel: ${formatScreenerFunnel(funnel)}.`,
      addedCount: 0,
      debug,
    };
  }

  await saveWatchlist([...added, ...existingItems]);
  return {
    message: `Added ${added.length} TrendOK ✅ stocks from screener (scanned ${uniq.length}; pullback-filter kept ${filtered.length}, dropped ${droppedByPullback}). Funnel: ${formatScreenerFunnel(funnel)}.`,
    addedCount: added.length,
    debug,
  };
}
