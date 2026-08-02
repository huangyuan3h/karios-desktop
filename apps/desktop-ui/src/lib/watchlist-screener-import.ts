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

/** TIP-002/003: Screener → Watchlist funnel counts (same-day observability). */
export type ScreenerFunnel = {
  tvHit: number;
  passPullback: number;
  passTrendOk: number;
  addedNew: number;
  droppedByPullback: number;
  fallbackUsed: boolean;
  fallbackHit: number;
  fallbackTrendOk: number;
  fallbackAdded: number;
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

type FallbackUniverseResponse = {
  industries?: string[];
  symbols?: string[];
  namesBySymbol?: Record<string, string>;
  truncated?: boolean;
  count?: number;
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
    fallbackUsed: false,
    fallbackHit: 0,
    fallbackTrendOk: 0,
    fallbackAdded: 0,
  };
}

export function formatScreenerFunnel(funnel: ScreenerFunnel | null | undefined): string {
  if (!funnel) return '';
  const primary = `TV ${funnel.tvHit} → pullback ${funnel.passPullback} → TrendOK ${funnel.passTrendOk} → +${funnel.addedNew}`;
  if (!funnel.fallbackUsed) return primary;
  return `${primary} | fb ${funnel.fallbackHit}→OK ${funnel.fallbackTrendOk}→+${funnel.fallbackAdded}`;
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

async function importFallbackTrendOk(options: {
  existing: Set<string>;
  now: string;
  setStep: (label: string, cur?: number, total?: number) => void;
}): Promise<{
  added: WatchlistItem[];
  fallbackHit: number;
  fallbackTrendOk: number;
  rows: TrendOkResult[];
}> {
  const { existing, now, setStep } = options;
  setStep('Fallback universe (5D Top5)…');
  let universe: FallbackUniverseResponse;
  try {
    universe = await apiGetJson<FallbackUniverseResponse>(
      '/watchlist/automation/fallback-universe?maxTotal=80',
    );
  } catch {
    return { added: [], fallbackHit: 0, fallbackTrendOk: 0, rows: [] };
  }
  const symbols = Array.from(
    new Set((universe.symbols || []).map((s) => String(s || '').trim()).filter(Boolean)),
  );
  const namesBySymbol = universe.namesBySymbol || {};
  if (!symbols.length) {
    return { added: [], fallbackHit: 0, fallbackTrendOk: 0, rows: [] };
  }

  setStep('Fallback TrendOK check', 0, symbols.length);
  const trendMap = await fetchTrendOkMap(symbols, {
    realtime: isShanghaiQuoteWindow(),
  });
  const okSyms: string[] = [];
  const rows: TrendOkResult[] = [];
  for (const sym of symbols) {
    const rr = trendMap.get(sym);
    if (rr?.symbol) {
      rows.push(rr);
      if (rr.trendOk === true) okSyms.push(rr.symbol);
    } else {
      rows.push({
        symbol: sym,
        trendOk: null,
        score: null,
        missingData: ['no_result'],
      } satisfies TrendOkResult);
    }
  }
  setStep('Fallback TrendOK check', symbols.length, symbols.length);
  const okUniq = Array.from(new Set(okSyms));
  const added: WatchlistItem[] = okUniq
    .filter((sym) => !existing.has(sym))
    .map((sym) => ({
      symbol: sym,
      name: namesBySymbol[sym] ?? null,
      addedAt: now,
      color: '#fef9c3',
      source: 'screener_fallback' as const,
    }));
  return {
    added,
    fallbackHit: symbols.length,
    fallbackTrendOk: okUniq.length,
    rows,
  };
}

export async function importFromScreener(options: ScreenerImportOptions = {}): Promise<ScreenerImportResult> {
  const existingItems = options.existingItems ?? loadWatchlist();
  const onStage = options.onStage;
  const setStep = (label: string, cur?: number, total?: number) => {
    onStage?.(label, cur, total);
  };

  const existing = new Set(existingItems.map((x) => x.symbol));
  const now = new Date().toISOString();

  setStep('Loading enabled screeners');
  let enabled: TvScreener[] = [];
  try {
    const s = await apiGetJson<{ items: TvScreener[] }>('/integrations/tradingview/screeners');
    enabled = (s.items || []).filter((x) => x && x.enabled);
  } catch {
    enabled = [];
  }

  let uniq: string[] = [];
  let filtered: string[] = [];
  let droppedByPullback = 0;
  let okUniq: string[] = [];
  const debugBySym: Record<string, TrendOkResult> = {};
  let primaryAdded: WatchlistItem[] = [];
  let primaryRows: TrendOkResult[] = [];

  if (enabled.length) {
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

    uniq = Array.from(new Set(candidates)).slice(0, 2000);
    const minPullback = -0.15;
    const maxPullback = -0.05;
    filtered = uniq.filter((sym) => {
      const rs = retracementBySymbol.get(sym) ?? [];
      return rs.some((x) => x >= minPullback && x <= maxPullback);
    });
    droppedByPullback = uniq.length - filtered.length;

    if (filtered.length) {
      setStep('TrendOK check', 0, filtered.length);
      const trendMap = await fetchTrendOkMap(filtered, {
        realtime: isShanghaiQuoteWindow(),
      });
      const okSymsCached: string[] = [];
      for (const sym of filtered) {
        const rr = trendMap.get(sym);
        if (!rr?.symbol) continue;
        debugBySym[rr.symbol] = rr;
        if (rr.trendOk === true) okSymsCached.push(rr.symbol);
      }
      setStep('TrendOK check', filtered.length, filtered.length);
      okUniq = Array.from(new Set(okSymsCached));
      primaryAdded = okUniq
        .filter((sym) => !existing.has(sym))
        .map((sym) => ({
          symbol: sym,
          name: null,
          addedAt: now,
          color: '#ffffff',
          source: 'screener' as const,
        }));
      for (const item of primaryAdded) existing.add(item.symbol);
      primaryRows = filtered.map(
        (sym) =>
          debugBySym[sym] ??
          ({
            symbol: sym,
            trendOk: null,
            score: null,
            missingData: ['no_result'],
          } satisfies TrendOkResult),
      );
    }
  }

  const funnel: ScreenerFunnel = {
    ...emptyScreenerFunnel(),
    tvHit: uniq.length,
    passPullback: filtered.length,
    passTrendOk: okUniq.length,
    addedNew: primaryAdded.length,
    droppedByPullback,
  };

  let fallbackAdded: WatchlistItem[] = [];
  let fallbackRows: TrendOkResult[] = [];
  const needFallback = funnel.tvHit === 0 || funnel.passPullback === 0;
  if (needFallback) {
    const fb = await importFallbackTrendOk({ existing, now, setStep });
    funnel.fallbackUsed = true;
    funnel.fallbackHit = fb.fallbackHit;
    funnel.fallbackTrendOk = fb.fallbackTrendOk;
    funnel.fallbackAdded = fb.added.length;
    funnel.addedNew += fb.added.length;
    fallbackAdded = fb.added;
    fallbackRows = fb.rows;
  }

  const allAdded = [...primaryAdded, ...fallbackAdded];
  const debugRows = primaryRows.length ? primaryRows : fallbackRows;
  const debug = makeDebug({
    scanned: funnel.tvHit || funnel.fallbackHit,
    trendOkCount: funnel.passTrendOk || funnel.fallbackTrendOk,
    rows: debugRows,
    funnel,
  });

  if (!allAdded.length) {
    const reason =
      needFallback && funnel.fallbackUsed
        ? `Primary empty (tvHit=${funnel.tvHit}, pullback=${funnel.passPullback}); fallback Hit=${funnel.fallbackHit} TrendOK=${funnel.fallbackTrendOk}; nothing new to add.`
        : enabled.length
          ? `Screener scanned ${uniq.length}; pullback kept ${filtered.length}; TrendOK ✅: ${okUniq.length}; nothing new to add.`
          : 'No enabled screeners and fallback added nothing.';
    return {
      message: `${reason} Funnel: ${formatScreenerFunnel(funnel)}.`,
      addedCount: 0,
      debug,
    };
  }

  await saveWatchlist([...allAdded, ...existingItems]);
  const primaryMsg = primaryAdded.length
    ? `Added ${primaryAdded.length} from screener`
    : 'Primary screener empty';
  const fbMsg = fallbackAdded.length
    ? `; fallback +${fallbackAdded.length} (TrendOK, no pullback)`
    : funnel.fallbackUsed
      ? '; fallback used but 0 new'
      : '';
  return {
    message: `${primaryMsg}${fbMsg}. Funnel: ${formatScreenerFunnel(funnel)}.`,
    addedCount: allAdded.length,
    debug,
  };
}
