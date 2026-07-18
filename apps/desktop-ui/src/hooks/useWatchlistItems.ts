'use client';

import * as React from 'react';

import { apiGetJson } from '@/lib/api/client';
import { getShanghaiTodayIso } from '@/lib/market-hours';
import {
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

  if (/^(CN|HK):[0-9A-Z.\-]{1,16}$/.test(raw)) {
    return { symbol: raw };
  }

  if (/^\d{6}$/.test(raw)) {
    return { symbol: `CN:${raw}` };
  }

  if (/^\d{4,5}$/.test(raw)) {
    return { symbol: `HK:${raw.padStart(4, '0')}` };
  }

  return {
    error:
      'Unsupported code format. Use 6-digit CN ticker, 4-5 digit HK ticker, or CN:/HK: prefixed symbol.',
  };
}

export function useWatchlistItems() {
  const [items, setItems] = React.useState<WatchlistItem[]>([]);
  const [watchlistHydrating, setWatchlistHydrating] = React.useState(true);
  const [code, setCode] = React.useState('');
  const [error, setError] = React.useState<string | null>(null);
  const [costPriceDrafts, setCostPriceDrafts] = React.useState<Record<string, string>>({});

  const persist = React.useCallback((next: WatchlistItem[]) => {
    setItems(next);
    void saveWatchlist(next);
  }, []);

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
    let cancelled = false;
    async function resolveMissingNames() {
      const missing = items
        .filter((x) => !x.name && x.nameStatus !== 'not_found')
        .map((x) => x.symbol);
      if (!missing.length) return;

      try {
        const sp = new URLSearchParams();
        for (const s of missing) sp.append('symbols', s);
        const rows = await apiGetJson<MarketStockBasicRow[]>(
          `/market/stocks/resolve?${sp.toString()}`,
        );
        if (cancelled) return;
        const bySym = new Map<string, MarketStockBasicRow>();
        for (const r of Array.isArray(rows) ? rows : []) bySym.set(r.symbol, r);

        const next = items.map((it) => {
          if (it.name || it.nameStatus === 'resolved') return it;
          const hit = bySym.get(it.symbol);
          if (hit) return { ...it, name: hit.name, nameStatus: 'resolved' as const };
          if (missing.includes(it.symbol)) return { ...it, nameStatus: 'not_found' as const };
          return it;
        });
        persist(next);
      } catch (e) {
        if (!cancelled) console.warn('Watchlist name resolve failed:', e);
      }
    }
    void resolveMissingNames();
    return () => {
      cancelled = true;
    };
  }, [items]);

  function addSymbolToWatchlist(symRaw: string) {
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
  }

  function onAdd() {
    setError(null);
    const parsed = normalizeSymbolInput(code);
    if ('error' in parsed) {
      setError(parsed.error);
      return;
    }
    const sym = parsed.symbol;
    if (items.some((x) => x.symbol === sym)) {
      setError('Already in watchlist.');
      return;
    }
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
    const todaySh = getShanghaiTodayIso();
    const next = items.map((it) => {
      if (it.symbol !== symbol) return it;
      const prevPct =
        typeof it.positionPct === 'number' && Number.isFinite(it.positionPct) ? it.positionPct : 0;
      const opening = prevPct <= 0 && nextVal != null && nextVal > 0;
      const clearing = nextVal == null || nextVal <= 0;
      let entryDate = it.entryDate ?? null;
      if (clearing) entryDate = null;
      else if (opening && !entryDate) entryDate = todaySh;
      return { ...it, positionPct: nextVal, entryDate };
    });
    persist(next);
  }

  function setItemCostPriceValue(symbol: string, value: number | null) {
    const nextVal =
      typeof value === 'number' && Number.isFinite(value) ? Math.round(value * 100) / 100 : null;
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
    onAdd,
    onRemove,
    addSymbolToWatchlist,
    setItemColor,
    setItemPositionPct,
    setItemCostPriceDraft,
    setItemCostPriceValue,
    commitItemCostPriceDraft,
  };
}

export type UseWatchlistItemsReturn = ReturnType<typeof useWatchlistItems>;
