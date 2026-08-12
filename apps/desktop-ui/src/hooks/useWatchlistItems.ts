'use client';

import * as React from 'react';

import { apiGetJson } from '@/lib/api/client';
import { getShanghaiTodayIso } from '@/lib/market-hours';
import {
  applyZeroPositionCleanup,
  ensureWatchlistHydrated,
  loadWatchlist,
  saveWatchlist,
  WATCHLIST_UPDATED_EVENT,
  type WatchlistItem,
} from '@/lib/watchlist-storage';

type MarketStockBasicRow = {
  symbol: string;
  market: string;
  ticker: string;
  name: string;
  currency: string;
};

export function normalizeSymbolInput(input: string): { symbol: string } | { error: string } {
  const raw = (input || '').trim().toUpperCase();
  if (!raw) return { error: 'Empty input' };

  if (/^(CN|HK|ETF):[0-9A-Z.\-]{1,16}$/.test(raw)) {
    return { symbol: raw };
  }

  // 5xxxxx / 1xxxxx / 9xxxxx are CN ETFs; 0xxxxx/3xxxxx are CN stocks;
  // 6xxxxx is ambiguous (CN SH stocks + some ETFs) — default to CN stocks
  // because ETFs are rarer; users can prefix ETF: explicitly.
  if (/^\d{6}$/.test(raw)) {
    if (raw.startsWith('5') || raw.startsWith('1') || raw.startsWith('9')) {
      return { symbol: `ETF:${raw}` };
    }
    return { symbol: `CN:${raw}` };
  }

  if (/^\d{4,5}$/.test(raw)) {
    return { symbol: `HK:${raw.padStart(4, '0')}` };
  }

  return {
    error:
      'Unsupported code format. Use 6-digit CN ticker, 4-5 digit HK ticker, 6-digit ETF ticker, or CN:/HK:/ETF: prefixed symbol.',
  };
}

export function useWatchlistItems() {
  const [items, setItems] = React.useState<WatchlistItem[]>([]);
  const [watchlistHydrating, setWatchlistHydrating] = React.useState(true);
  const [code, setCode] = React.useState('');
  const [error, setError] = React.useState<string | null>(null);
  const [costPriceDrafts, setCostPriceDrafts] = React.useState<Record<string, string>>({});
  const [positionPctDrafts, setPositionPctDrafts] = React.useState<Record<string, string>>({});

  const persist = React.useCallback((next: WatchlistItem[]) => {
    setItems(next);
    void saveWatchlist(next);
  }, []);

  // Resolve a batch of symbols via /market/stocks/resolve and update items.
  // Returns the resolved rows so callers can avoid an extra round-trip.
  const resolveSymbols = React.useCallback(
    async (symbolsToResolve: string[]): Promise<MarketStockBasicRow[]> => {
      const uniq = Array.from(new Set(symbolsToResolve.filter(Boolean)));
      if (!uniq.length) return [];
      const sp = new URLSearchParams();
      for (const s of uniq) sp.append('symbols', s);
      try {
        const rows = await apiGetJson<MarketStockBasicRow[]>(
          `/market/stocks/resolve?${sp.toString()}`,
        );
        return Array.isArray(rows) ? rows : [];
      } catch (e) {
        console.warn('Watchlist name resolve failed:', e);
        return [];
      }
    },
    [],
  );

  const applyResolvedNames = React.useCallback(
    (rows: MarketStockBasicRow[]) => {
      if (!rows.length) return;
      const bySym = new Map<string, MarketStockBasicRow>();
      for (const r of rows) bySym.set(r.symbol, r);
      setItems((prev) => {
        const next = prev.map((it) => {
          if (it.name || it.nameStatus === 'resolved' || it.nameStatus === 'not_found') {
            return it;
          }
          const hit = bySym.get(it.symbol);
          if (hit) return { ...it, name: hit.name, nameStatus: 'resolved' as const };
          return { ...it, nameStatus: 'not_found' as const };
        });
        // Avoid writing identical arrays to localStorage.
        const changed = next.some((x, i) => x !== prev[i]);
        if (changed) void saveWatchlist(next);
        return next;
      });
    },
    [],
  );

  React.useEffect(() => {
    function onExternalUpdate() {
      setItems(loadWatchlist());
    }
    window.addEventListener(WATCHLIST_UPDATED_EVENT, onExternalUpdate);
    return () => window.removeEventListener(WATCHLIST_UPDATED_EVENT, onExternalUpdate);
  }, []);

  React.useEffect(() => {
    let cancelled = false;
    void ensureWatchlistHydrated()
      .then(() => {
        if (!cancelled) setItems(loadWatchlist());
      })
      .finally(() => {
        if (!cancelled) setWatchlistHydrating(false);
      });
    return () => {
      cancelled = true;
    };
  }, []);

  React.useEffect(() => {
    const missing = items
      .filter((x) => !x.name && x.nameStatus !== 'not_found' && x.nameStatus !== 'resolved')
      .map((x) => x.symbol);
    if (!missing.length) return;
    let cancelled = false;
    void resolveSymbols(missing).then((rows) => {
      if (cancelled) return;
      applyResolvedNames(rows);
    });
    return () => {
      cancelled = true;
    };
  }, [items, resolveSymbols, applyResolvedNames]);

  function _addAndResolve(symRaw: string) {
    setError(null);
    const parsed = normalizeSymbolInput(symRaw);
    if ('error' in parsed) {
      setError(parsed.error);
      return;
    }
    const sym = parsed.symbol;
    if (items.some((x) => x.symbol === sym)) return;
    const next: WatchlistItem[] = [
      {
        symbol: sym,
        name: null,
        addedAt: new Date().toISOString(),
        color: '#ffffff',
      },
      ...items,
    ];
    persist(next);
    // Resolve the just-added symbol immediately so the Name column populates
    // without waiting for the items useEffect to re-fire.
    void resolveSymbols([sym])
      .then(applyResolvedNames)
      .catch((err) => console.warn('resolve symbol names failed:', err));
  }

  function addSymbolToWatchlist(symRaw: string) {
    _addAndResolve(symRaw);
  }

  function onAdd() {
    _addAndResolve(code);
    setCode('');
  }

  function onRemove(sym: string) {
    persist(items.filter((x) => x.symbol !== sym));
  }

  function setItemColor(symbol: string, color: string) {
    const next = items.map((it) => (it.symbol === symbol ? { ...it, color } : it));
    persist(next);
  }

  function setItemPositionPct(symbol: string, value: string) {
    const raw = value.trim();
    const num = raw === '' ? null : Number(raw);
    const nextVal =
      typeof num === 'number' && Number.isFinite(num) ? Math.max(0, Math.min(100, num)) : null;
    setPositionPctDrafts((prev) => {
      if (prev[symbol] == null) return prev;
      const next = { ...prev };
      delete next[symbol];
      return next;
    });
    const todaySh = getShanghaiTodayIso();
    const next = items.map((it) => {
      if (it.symbol !== symbol) return it;
      const prevPct =
        typeof it.positionPct === 'number' && Number.isFinite(it.positionPct) ? it.positionPct : 0;
      const opening = prevPct <= 0 && nextVal != null && nextVal > 0;
      const clearing = nextVal == null || nextVal <= 0;
      if (clearing) {
        return applyZeroPositionCleanup({ ...it, positionPct: nextVal });
      }
      let entryDate = it.entryDate ?? null;
      if (opening && !entryDate) entryDate = todaySh;
      return { ...it, positionPct: nextVal, entryDate };
    });
    persist(next);
  }

  function setItemPositionPctDraft(symbol: string, value: string) {
    setPositionPctDrafts((prev) => ({ ...prev, [symbol]: value }));
  }

  function commitItemPositionPctDraft(symbol: string) {
    const raw = positionPctDrafts[symbol];
    setPositionPctDrafts((prev) => {
      const next = { ...prev };
      delete next[symbol];
      return next;
    });
    if (raw == null) return;
    setItemPositionPct(symbol, raw);
  }

  function setItemCostPriceValue(symbol: string, value: number | null) {
    const nextVal =
      typeof value === 'number' && Number.isFinite(value) ? Math.round(value * 1000) / 1000 : null;
    const next = items.map((it) =>
      it.symbol === symbol ? { ...it, costPrice: nextVal, maxPrice: nextVal ?? it.maxPrice } : it,
    );
    persist(next);
  }

  function setItemCostPriceDraft(symbol: string, value: string) {
    setCostPriceDrafts((prev) => ({ ...prev, [symbol]: value }));
  }

  function commitItemCostPriceDraft(symbol: string) {
    const raw = costPriceDrafts[symbol];
    setCostPriceDrafts((prev) => {
      const next = { ...prev };
      delete next[symbol];
      return next;
    });
    if (raw == null) return;
    const trimmed = raw.trim();
    if (!trimmed) {
      setItemCostPriceValue(symbol, null);
      return;
    }
    const num = Number(trimmed);
    if (Number.isFinite(num)) {
      setItemCostPriceValue(symbol, num);
    }
  }

  return {
    items,
    setItems,
    persist,
    watchlistHydrating,
    error,
    setError,
    code,
    setCode,
    costPriceDrafts,
    positionPctDrafts,
    onAdd,
    onRemove,
    addSymbolToWatchlist,
    setItemColor,
    setItemPositionPct,
    setItemPositionPctDraft,
    commitItemPositionPctDraft,
    setItemCostPriceDraft,
    setItemCostPriceValue,
    commitItemCostPriceDraft,
  };
}

export type UseWatchlistItemsReturn = ReturnType<typeof useWatchlistItems>;
