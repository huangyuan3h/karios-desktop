'use client';

import * as React from 'react';
import { RefreshCw, Search } from 'lucide-react';

import { Button } from '@/components/ui/button';
import { QUANT_BASE_URL } from '@/lib/endpoints';
import { useChatStore } from '@/lib/chat/store';

type MarketStatus = {
  stocks: number;
  lastSyncAt: string | null;
};

type MarketStockRow = {
  symbol: string;
  market: string;
  ticker: string;
  name: string;
  currency: string;
  price: string | null;
  changePct: string | null;
  volume: string | null;
  turnover: string | null;
  marketCap: string | null;
  updatedAt: string;
};

type MarketStocksResponse = {
  items: MarketStockRow[];
  total: number;
  offset: number;
  limit: number;
};

async function apiGetJson<T>(path: string): Promise<T> {
  const res = await fetch(`${QUANT_BASE_URL}${path}`, { cache: 'no-store' });
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  return (await res.json()) as T;
}

async function apiPostJson<T>(path: string): Promise<T> {
  const res = await fetch(`${QUANT_BASE_URL}${path}`, { method: 'POST' });
  const txt = await res.text().catch(() => '');
  if (!res.ok) {
    try {
      const j = JSON.parse(txt) as { error?: string; detail?: string };
      const msg = (j && (j.error || j.detail)) || '';
      if (msg) throw new Error(msg);
    } catch {
      // ignore
    }
    throw new Error(`${res.status} ${res.statusText}${txt ? `: ${txt}` : ''}`);
  }
  return (txt ? (JSON.parse(txt) as T) : ({} as T));
}

export function MarketPage({
  onOpenStock,
}: {
  onOpenStock: (symbol: string) => void;
}) {
  const { addReference } = useChatStore();
  const [status, setStatus] = React.useState<MarketStatus | null>(null);
  const [data, setData] = React.useState<MarketStocksResponse | null>(null);
  const [error, setError] = React.useState<string | null>(null);
  const [busy, setBusy] = React.useState(false);
  const [q, setQ] = React.useState('');
  const [market, setMarket] = React.useState<'ALL' | 'CN' | 'HK'>('ALL');
  const [offset, setOffset] = React.useState(0);
  const limit = 50;

  const refresh = React.useCallback(async () => {
    setError(null);
    try {
      const [st, list] = await Promise.all([
        apiGetJson<MarketStatus>('/market/status'),
        apiGetJson<MarketStocksResponse>(
          `/market/stocks?limit=${limit}&offset=${offset}` +
            `${market !== 'ALL' ? `&market=${market}` : ''}` +
            `${q.trim() ? `&q=${encodeURIComponent(q.trim())}` : ''}`,
        ),
      ]);
      setStatus(st);
      setData(list);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  }, [limit, offset, q, market]);

  React.useEffect(() => {
    void refresh();
  }, [refresh]);

  async function sync() {
    setBusy(true);
    setError(null);
    try {
      await apiPostJson('/market/sync');
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }

  const total = data?.total ?? 0;
  const page = Math.floor(offset / limit) + 1;
  const pages = Math.max(1, Math.ceil(total / limit));

  return (
    <div className="mx-auto w-full max-w-6xl p-6">
      <div className="mb-6 flex items-start justify-between gap-3">
        <div>
          <div className="text-lg font-semibold">Market</div>
          <div className="mt-1 text-sm text-[var(--k-muted)]">CN + HK stock universe.</div>
          <div className="mt-1 text-xs text-[var(--k-muted)]">
            Total: {status?.stocks ?? '—'}
            {status?.lastSyncAt ? ` • Last sync: ${new Date(status.lastSyncAt).toLocaleString()}` : ''}
          </div>
        </div>
        <div className="flex items-center gap-2">
          <Button variant="secondary" size="sm" onClick={() => void refresh()} disabled={busy}>
            Refresh
          </Button>
          <Button size="sm" onClick={() => void sync()} disabled={busy} className="gap-2">
            <RefreshCw className="h-4 w-4" />
            Sync
          </Button>
        </div>
      </div>

      {error ? (
        <div className="mb-4 rounded-lg border border-red-500/30 bg-red-500/10 px-3 py-2 text-sm text-red-600">
          {error}
        </div>
      ) : null}

      <div className="mb-4 flex flex-wrap items-center gap-2">
        <div className="relative w-full max-w-md">
          <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-[var(--k-muted)]" />
          <input
            className="h-9 w-full rounded-full border border-[var(--k-border)] bg-[var(--k-surface)] pl-9 pr-3 text-sm outline-none focus:ring-2 focus:ring-[var(--k-ring)]"
            placeholder="Search by ticker / name..."
            value={q}
            onChange={(e) => {
              setQ(e.target.value);
              setOffset(0);
            }}
          />
        </div>
        <div className="flex items-center gap-2 text-sm">
          <Button
            variant={market === 'ALL' ? 'secondary' : 'ghost'}
            size="sm"
            onClick={() => {
              setMarket('ALL');
              setOffset(0);
            }}
          >
            All
          </Button>
          <Button
            variant={market === 'CN' ? 'secondary' : 'ghost'}
            size="sm"
            onClick={() => {
              setMarket('CN');
              setOffset(0);
            }}
          >
            CN
          </Button>
          <Button
            variant={market === 'HK' ? 'secondary' : 'ghost'}
            size="sm"
            onClick={() => {
              setMarket('HK');
              setOffset(0);
            }}
          >
            HK
          </Button>
        </div>
        <div className="flex-1" />
        <div className="text-xs text-[var(--k-muted)]">
          Page {page}/{pages}
        </div>
        <Button
          variant="secondary"
          size="sm"
          disabled={offset === 0}
          onClick={() => setOffset((v) => Math.max(0, v - limit))}
        >
          Prev
        </Button>
        <Button
          variant="secondary"
          size="sm"
          disabled={offset + limit >= total}
          onClick={() => setOffset((v) => v + limit)}
        >
          Next
        </Button>
      </div>

      <div className="overflow-hidden rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)]">
        <div className="grid grid-cols-12 gap-2 bg-[var(--k-surface-2)] px-3 py-2 text-xs text-[var(--k-muted)]">
          <div className="col-span-1">Mkt</div>
          <div className="col-span-2">Ticker</div>
          <div className="col-span-4">Name</div>
          <div className="col-span-2">Price</div>
          <div className="col-span-2">Change%</div>
          <div className="col-span-1 text-right">Action</div>
        </div>
        <div className="divide-y divide-[var(--k-border)]">
          {(data?.items ?? []).map((it) => (
            <div
              key={it.symbol}
              className="grid cursor-pointer grid-cols-12 gap-2 px-3 py-2 hover:bg-[var(--k-surface-2)]"
              onClick={() => onOpenStock(it.symbol)}
            >
              <div className="col-span-1 pt-2 text-xs text-[var(--k-muted)]">{it.market}</div>
              <div className="col-span-2 pt-2 font-mono text-xs">{it.ticker}</div>
              <div className="col-span-4 truncate pt-2 text-sm">{it.name}</div>
              <div className="col-span-2 pt-2 font-mono text-xs">{it.price ?? '—'}</div>
              <div className="col-span-2 pt-2 font-mono text-xs">{it.changePct ?? '—'}</div>
              <div
                className="col-span-1 flex justify-end"
                onClick={(e) => {
                  e.stopPropagation();
                  addReference({
                    kind: 'stock',
                    refId: it.symbol,
                    symbol: it.symbol,
                    market: it.market,
                    ticker: it.ticker,
                    name: it.name,
                    barsDays: 60,
                    chipsDays: 30,
                    fundFlowDays: 30,
                    capturedAt: new Date().toISOString(),
                  });
                }}
              >
                <Button variant="secondary" size="sm" className="h-8 px-2">
                  Ref
                </Button>
              </div>
            </div>
          ))}
          {(data?.items?.length ?? 0) === 0 ? (
            <div className="px-3 py-10 text-center text-sm text-[var(--k-muted)]">
              No data. Click Sync to fetch the stock universe.
            </div>
          ) : null}
        </div>
      </div>
    </div>
  );
}


