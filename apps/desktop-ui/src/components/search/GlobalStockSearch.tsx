'use client';

import * as React from 'react';
import { Search } from 'lucide-react';

import { QUANT_BASE_URL } from '@/lib/endpoints';

type MarketStockRow = {
  symbol: string;
  market: string;
  ticker: string;
  name: string;
  currency: string;
  price: string | null;
  changePct: string | null;
};

type MarketStocksResponse = {
  items: MarketStockRow[];
};

async function apiGetJson<T>(path: string): Promise<T> {
  const res = await fetch(`${QUANT_BASE_URL}${path}`, { cache: 'no-store' });
  const txt = await res.text().catch(() => '');
  if (!res.ok) {
    try {
      const j = JSON.parse(txt) as { detail?: string; error?: string };
      const msg = (j && (j.detail || j.error)) || '';
      if (msg) throw new Error(msg);
    } catch {
      // ignore
    }
    throw new Error(`${res.status} ${res.statusText}${txt ? `: ${txt}` : ''}`);
  }
  return (txt ? (JSON.parse(txt) as T) : ({} as T));
}

function useDebouncedValue<T>(value: T, delayMs: number) {
  const [debounced, setDebounced] = React.useState(value);
  React.useEffect(() => {
    const t = window.setTimeout(() => setDebounced(value), delayMs);
    return () => window.clearTimeout(t);
  }, [value, delayMs]);
  return debounced;
}

export function GlobalStockSearch({
  onSelectSymbol,
}: {
  onSelectSymbol: (symbol: string) => void;
}) {
  const [query, setQuery] = React.useState('');
  const debounced = useDebouncedValue(query, 160);
  const [open, setOpen] = React.useState(false);
  const [loading, setLoading] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);
  const [items, setItems] = React.useState<MarketStockRow[]>([]);
  const [activeIdx, setActiveIdx] = React.useState(0);
  const rootRef = React.useRef<HTMLDivElement | null>(null);

  React.useEffect(() => {
    let cancelled = false;
    async function run() {
      const q = debounced.trim();
      setError(null);
      if (!q) {
        setItems([]);
        setOpen(false);
        return;
      }
      setLoading(true);
      setOpen(true);
      try {
        const data = await apiGetJson<MarketStocksResponse>(
          `/market/stocks?limit=8&offset=0&q=${encodeURIComponent(q)}`,
        );
        if (cancelled) return;
        setItems(Array.isArray(data.items) ? data.items : []);
        setActiveIdx(0);
      } catch (e) {
        if (cancelled) return;
        setItems([]);
        setError(e instanceof Error ? e.message : String(e));
      } finally {
        if (!cancelled) setLoading(false);
      }
    }
    void run();
    return () => {
      cancelled = true;
    };
  }, [debounced]);

  React.useEffect(() => {
    function onDocDown(e: MouseEvent) {
      const el = rootRef.current;
      if (!el) return;
      if (!el.contains(e.target as Node)) setOpen(false);
    }
    document.addEventListener('mousedown', onDocDown);
    return () => document.removeEventListener('mousedown', onDocDown);
  }, []);

  function select(it: MarketStockRow) {
    onSelectSymbol(it.symbol);
    setQuery('');
    setOpen(false);
  }

  return (
    <div ref={rootRef} className="relative hidden md:block">
      <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-[var(--k-muted)]" />
      <input
        className="h-9 w-[360px] rounded-full border border-[var(--k-border)] bg-[var(--k-surface)] pl-9 pr-3 text-sm outline-none focus:ring-2 focus:ring-[var(--k-ring)]"
        placeholder="Search stocks (CN/HK)…"
        value={query}
        onChange={(e) => setQuery(e.target.value)}
        onFocus={() => {
          if (query.trim()) setOpen(true);
        }}
        onKeyDown={(e) => {
          if (!open) return;
          if (e.key === 'Escape') {
            e.preventDefault();
            setOpen(false);
            return;
          }
          if (e.key === 'ArrowDown') {
            e.preventDefault();
            setActiveIdx((v) => Math.min(items.length - 1, v + 1));
            return;
          }
          if (e.key === 'ArrowUp') {
            e.preventDefault();
            setActiveIdx((v) => Math.max(0, v - 1));
            return;
          }
          if (e.key === 'Enter') {
            e.preventDefault();
            const it = items[activeIdx];
            if (it) select(it);
          }
        }}
      />

      {open ? (
        <div className="absolute left-0 right-0 top-[44px] z-50 overflow-hidden rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] shadow-lg">
          {loading ? (
            <div className="px-3 py-2 text-sm text-[var(--k-muted)]">Searching…</div>
          ) : error ? (
            <div className="px-3 py-2 text-sm text-red-600">{error}</div>
          ) : items.length === 0 ? (
            <div className="px-3 py-2 text-sm text-[var(--k-muted)]">
              No results. Try syncing Market first.
            </div>
          ) : (
            <div className="divide-y divide-[var(--k-border)]">
              {items.map((it, idx) => (
                <button
                  key={it.symbol}
                  type="button"
                  className={
                    'flex w-full items-center gap-3 px-3 py-2 text-left text-sm ' +
                    (idx === activeIdx ? 'bg-[var(--k-surface-2)]' : 'hover:bg-[var(--k-surface-2)]')
                  }
                  onMouseEnter={() => setActiveIdx(idx)}
                  onClick={() => select(it)}
                >
                  <div className="w-10 rounded-md bg-[var(--k-surface-2)] px-2 py-1 text-center text-xs text-[var(--k-muted)]">
                    {it.market}
                  </div>
                  <div className="min-w-0 flex-1">
                    <div className="flex items-center gap-2">
                      <div className="font-mono text-xs">{it.ticker}</div>
                      <div className="truncate">{it.name}</div>
                    </div>
                    <div className="mt-0.5 font-mono text-[11px] text-[var(--k-muted)]">{it.symbol}</div>
                  </div>
                  <div className="text-right font-mono text-xs text-[var(--k-muted)]">
                    {it.price ?? '—'}
                  </div>
                </button>
              ))}
            </div>
          )}
        </div>
      ) : null}
    </div>
  );
}


