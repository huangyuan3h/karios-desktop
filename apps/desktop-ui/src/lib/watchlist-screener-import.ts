import { DATA_SYNC_BASE_URL } from '@/lib/endpoints';
import { normalizeScreenerSymbol } from '@/lib/screenerExport';
import {
  loadWatchlist,
  saveWatchlist,
  type WatchlistItem,
} from '@/lib/watchlist-storage';

export type TrendOkResult = {
  symbol: string;
  trendOk?: boolean | null;
  score?: number | null;
  missingData?: string[];
};

export type ScreenerImportDebugState = {
  updatedAt: string | null;
  scanned: number;
  trendOkCount: number;
  rows: TrendOkResult[];
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

type TvSnapshotSummary = {
  id: string;
};

type TvSnapshotDetail = {
  rows: Record<string, string>[];
};

export type ScreenerImportOptions = {
  existingItems?: WatchlistItem[];
  silent?: boolean;
  onStage?: (label: string, cur?: number, total?: number) => void;
};

function chunk<T>(arr: T[], n: number): T[][] {
  const out: T[][] = [];
  for (let i = 0; i < arr.length; i += n) out.push(arr.slice(i, i + n));
  return out;
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

function getShanghaiTimeParts(): { weekday: string; hour: number; minute: number } {
  const parts = new Intl.DateTimeFormat('en-US', {
    timeZone: 'Asia/Shanghai',
    weekday: 'short',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
  }).formatToParts(new Date());
  const map = new Map(parts.map((p) => [p.type, p.value]));
  return {
    weekday: map.get('weekday') ?? '',
    hour: Number(map.get('hour') ?? 0),
    minute: Number(map.get('minute') ?? 0),
  };
}

function isShanghaiTradingTime(): boolean {
  const { weekday, hour, minute } = getShanghaiTimeParts();
  if (!['Mon', 'Tue', 'Wed', 'Thu', 'Fri'].includes(weekday)) return false;
  const minutes = hour * 60 + minute;
  const inMorning = minutes >= 9 * 60 + 30 && minutes <= 11 * 60 + 30;
  const inAfternoon = minutes >= 13 * 60 && minutes <= 15 * 60;
  const inLunch = minutes > 11 * 60 + 30 && minutes < 13 * 60;
  return inMorning || inAfternoon || inLunch;
}

function isShanghaiQuoteWindow(): boolean {
  const { weekday, hour, minute } = getShanghaiTimeParts();
  if (!['Mon', 'Tue', 'Wed', 'Thu', 'Fri'].includes(weekday)) return false;
  const minutes = hour * 60 + minute;
  if (isShanghaiTradingTime()) return true;
  return minutes > 15 * 60 && minutes <= 20 * 60;
}

async function apiGetJson<T>(path: string): Promise<T> {
  const res = await fetch(`${DATA_SYNC_BASE_URL}${path}`, { cache: 'no-store' });
  const txt = await res.text().catch(() => '');
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}${txt ? `: ${txt}` : ''}`);
  return txt ? (JSON.parse(txt) as T) : ({} as T);
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
    const debug: ScreenerImportDebugState = {
      updatedAt: new Date().toISOString(),
      scanned: 0,
      trendOkCount: 0,
      rows: [],
    };
    return { message: 'No enabled screeners.', addedCount: 0, debug };
  }

  setStep('Loading latest snapshots (DB)', 0, enabled.length);
  const snapshotDetails: TvSnapshotDetail[] = [];
  for (let i = 0; i < enabled.length; i++) {
    const sc = enabled[i]!;
    setStep('Loading latest snapshots (DB)', i + 1, enabled.length);
    try {
      let snapId: string | null = null;
      const list = await apiGetJson<{ items: TvSnapshotSummary[] }>(
        `/integrations/tradingview/screeners/${encodeURIComponent(sc.id)}/snapshots?limit=1`,
      );
      const latest = list.items?.[0];
      if (latest?.id) snapId = String(latest.id);
      if (!snapId) continue;
      const d = await apiGetJson<TvSnapshotDetail>(
        `/integrations/tradingview/snapshots/${encodeURIComponent(snapId)}`,
      );
      snapshotDetails.push(d);
    } catch {
      // ignore per-screener
    }
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
    const debug: ScreenerImportDebugState = {
      updatedAt: new Date().toISOString(),
      scanned: 0,
      trendOkCount: 0,
      rows: [],
    };
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
    const debug: ScreenerImportDebugState = {
      updatedAt: new Date().toISOString(),
      scanned: uniq.length,
      trendOkCount: 0,
      rows: [],
    };
    return {
      message: `Screener scanned ${uniq.length} symbols, but 0 passed pullback ratio filter ((Current-52WHigh)/52WHigh in [-0.15, -0.05]).`,
      addedCount: 0,
      debug,
    };
  }

  setStep('TrendOK check', 0, filtered.length);
  const okSymsCached: string[] = [];
  const debugBySym: Record<string, TrendOkResult> = {};
  for (const part of chunk(filtered, 200)) {
    const sp = new URLSearchParams();
    sp.set('refresh', 'true');
    sp.set('realtime', isShanghaiQuoteWindow() ? 'true' : 'false');
    for (const s2 of part) sp.append('symbols', s2);
    const rows = await apiGetJson<TrendOkResult[]>(`/market/stocks/trendok?${sp.toString()}`);
    for (const rr of Array.isArray(rows) ? rows : []) {
      if (!rr || !rr.symbol) continue;
      debugBySym[rr.symbol] = rr;
      if (rr.trendOk === true) okSymsCached.push(rr.symbol);
    }
    setStep('TrendOK check', Math.min(filtered.length, (okSymsCached.length || part.length)), filtered.length);
  }
  const okUniq = Array.from(new Set(okSymsCached));

  const debug: ScreenerImportDebugState = {
    updatedAt: new Date().toISOString(),
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
  };

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

  if (!added.length) {
    return {
      message: `Screener scanned ${uniq.length} symbols; pullback-filter kept ${filtered.length} (dropped ${droppedByPullback}); TrendOK ✅: ${okUniq.length}; nothing new to add.`,
      addedCount: 0,
      debug,
    };
  }

  saveWatchlist([...added, ...existingItems]);
  return {
    message: `Added ${added.length} TrendOK ✅ stocks from screener (scanned ${uniq.length}; pullback-filter kept ${filtered.length}, dropped ${droppedByPullback}).`,
    addedCount: added.length,
    debug,
  };
}
