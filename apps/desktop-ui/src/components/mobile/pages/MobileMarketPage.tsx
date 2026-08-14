'use client';

import * as React from 'react';

import { apiGetJson, apiPostJson } from '@/lib/api/client';
import { useWatchlistItems } from '@/hooks/useWatchlistItems';
import { MobileButton, MobileCard, MobileField, MobileSection } from '../primitives';

/** Market (mobile) — search + paginated stock list, add to watchlist. §5.2 高频. */
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

const PAGE_LIMIT = 20;

export function MobileMarketPage() {
  const { addSymbolToWatchlist } = useWatchlistItems();
  const [q, setQ] = React.useState('');
  const [offset, setOffset] = React.useState(0);
  const [rows, setRows] = React.useState<MarketStockRow[]>([]);
  const [total, setTotal] = React.useState(0);
  const [loading, setLoading] = React.useState(false);
  const [syncing, setSyncing] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);
  const [added, setAdded] = React.useState<string | null>(null);

  const refresh = React.useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const params = new URLSearchParams({ limit: String(PAGE_LIMIT), offset: String(offset), market: 'CN' });
      if (q.trim()) params.set('q', q.trim());
      const res = await apiGetJson<{ items: MarketStockRow[]; total: number }>(
        `/market/stocks?${params.toString()}`,
      );
      setRows(res.items ?? []);
      setTotal(res.total ?? 0);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }, [q, offset]);

  React.useEffect(() => {
    void refresh();
  }, [refresh]);

  const sync = async () => {
    setSyncing(true);
    try {
      await apiPostJson('/market/sync', {});
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setSyncing(false);
    }
  };

  const pages = Math.max(1, Math.ceil(total / PAGE_LIMIT));
  const page = Math.floor(offset / PAGE_LIMIT) + 1;

  return (
    <div className="space-y-4">
      <MobileSection
        title={`行情列表（${total}）`}
        action={
          <button type="button" onClick={() => void refresh()} className="text-[var(--m-text-sm)] text-[var(--k-accent)]">
            刷新
          </button>
        }
      >
        <MobileCard className="space-y-2 p-3">
          <MobileField label="搜索（代码 / 名称）">
            <div className="flex gap-2">
              <input
                value={q}
                onChange={(e) => {
                  setQ(e.target.value);
                  setOffset(0);
                }}
                placeholder="如 白酒 / 600519"
                className="h-[var(--m-tap)] min-w-0 flex-1 rounded-[var(--m-radius-md)] border border-[var(--k-border)] bg-[var(--k-surface-2)] px-3 text-[var(--m-text-base)] outline-none focus:border-[var(--k-accent)]"
              />
              <MobileButton variant="ghost" onClick={() => void sync()} disabled={syncing}>
                {syncing ? '同步中…' : '同步'}
              </MobileButton>
            </div>
          </MobileField>
        </MobileCard>

        {error ? (
          <MobileCard className="px-3 py-4 text-[var(--m-text-sm)] text-[var(--k-danger)]">{error}</MobileCard>
        ) : null}
        {added ? (
          <MobileCard className="px-3 py-2 text-[var(--m-text-sm)] text-[var(--k-down)]">{added}</MobileCard>
        ) : null}

        {rows.length ? (
          <MobileCard>
            {rows.map((r, idx) => {
              const price = r.price != null ? Number(r.price) : null;
              const pct = r.changePct != null ? Number(r.changePct) : null;
              return (
                <div
                  key={r.symbol}
                  className={
                    idx === 0
                      ? 'flex items-center justify-between gap-2 px-3 py-2.5'
                      : 'flex items-center justify-between gap-2 border-t border-[var(--k-border)] px-3 py-2.5'
                  }
                >
                  <div className="min-w-0 flex-1">
                    <div className="truncate text-[var(--m-text-base)] font-medium">{r.name}</div>
                    <div className="truncate font-mono text-[var(--m-text-xs)] text-[var(--k-muted)]">
                      {r.ticker} · {r.market}
                    </div>
                  </div>
                  <div className="shrink-0 text-right">
                    <div className="font-mono text-[var(--m-text-base)] tabular-nums">
                      {price != null ? price.toFixed(2) : '—'}
                    </div>
                    {pct != null ? (
                      <div
                        className="text-[var(--m-text-sm)] font-medium"
                        style={{
                          color: pct > 0 ? 'var(--k-up)' : pct < 0 ? 'var(--k-down)' : 'var(--k-muted)',
                        }}
                      >
                        {pct > 0 ? '▲' : pct < 0 ? '▼' : ''}
                        {Math.abs(pct).toFixed(2)}%
                      </div>
                    ) : null}
                  </div>
                  <button
                    type="button"
                    onClick={() => {
                      addSymbolToWatchlist(r.symbol);
                      setAdded(`${r.name} 已加入自选`);
                      setTimeout(() => setAdded(null), 2000);
                    }}
                    className="shrink-0 rounded-md border border-[var(--k-accent)] px-2 py-1 text-[var(--m-text-sm)] text-[var(--k-accent)] active:bg-[var(--k-surface-2)]"
                  >
                    +自选
                  </button>
                </div>
              );
            })}
          </MobileCard>
        ) : loading ? (
          <div className="space-y-2">
            <div className="m-shimmer h-11" />
            <div className="m-shimmer h-11" />
            <div className="m-shimmer h-11" />
          </div>
        ) : (
          <MobileCard className="px-3 py-8 text-center text-[var(--m-text-sm)] text-[var(--k-muted)]">
            无数据，点「同步」拉取行情
          </MobileCard>
        )}

        <div className="flex items-center justify-between">
          <MobileButton variant="ghost" size="sm" disabled={offset === 0} onClick={() => setOffset((v) => Math.max(0, v - PAGE_LIMIT))}>
            上一页
          </MobileButton>
          <span className="text-[var(--m-text-sm)] text-[var(--k-muted)]">
            {page} / {pages}
          </span>
          <MobileButton variant="ghost" size="sm" disabled={offset + PAGE_LIMIT >= total} onClick={() => setOffset((v) => v + PAGE_LIMIT)}>
            下一页
          </MobileButton>
        </div>
      </MobileSection>
    </div>
  );
}
