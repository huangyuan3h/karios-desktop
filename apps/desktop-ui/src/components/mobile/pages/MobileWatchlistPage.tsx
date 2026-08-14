'use client';

import * as React from 'react';

import { useWatchlistItems } from '@/hooks/useWatchlistItems';
import { useWatchlistMarketQuery } from '@/lib/queries/watchlist';
import { MobileButton, MobileCard, MobileField, MobileSection, PctText } from '../primitives';

/** Watchlist (mobile) — one row per stock, add/remove, refresh. §5.2 高频. */
export function MobileWatchlistPage() {
  const {
    items,
    watchlistHydrating,
    onRemove,
    code,
    setCode,
    error,
    addSymbolToWatchlist,
  } = useWatchlistItems();
  const symbols = items.map((i) => i.symbol);
  const market = useWatchlistMarketQuery(symbols);
  const [addedMsg, setAddedMsg] = React.useState<string | null>(null);

  const addByCode = () => {
    const parsed = code.trim().toUpperCase();
    if (!parsed) return;
    addSymbolToWatchlist(parsed);
    setCode('');
    setAddedMsg(`${parsed} 已添加`);
    setTimeout(() => setAddedMsg(null), 2000);
  };

  const trend = market.data?.trend ?? {};
  const quotes = market.data?.quotes ?? {};

  return (
    <div className="space-y-4">
      <MobileSection
        title={`自选股（${items.length}）`}
        action={
          <button
            type="button"
            onClick={() => void market.refetch()}
            className="text-[var(--m-text-sm)] text-[var(--k-accent)]"
          >
            刷新
          </button>
        }
      >
        {watchlistHydrating ? (
          <div className="space-y-2">
            <div className="m-shimmer h-11" />
            <div className="m-shimmer h-11" />
          </div>
        ) : items.length ? (
          <MobileCard>
            {items.map((it, idx) => {
              const q = quotes[it.symbol];
              const t = trend[it.symbol];
              return (
                <div
                  key={it.symbol}
                  className={
                    idx === 0
                      ? 'flex items-center justify-between gap-2 px-3 py-2.5'
                      : 'flex items-center justify-between gap-2 border-t border-[var(--k-border)] px-3 py-2.5'
                  }
                >
                  <div className="min-w-0 flex-1">
                    <div className="truncate text-[var(--m-text-base)] font-medium">
                      {it.name ?? it.symbol}
                    </div>
                    <div className="truncate font-mono text-[var(--m-text-xs)] text-[var(--k-muted)]">
                      {it.symbol}
                      {t?.trendStatus ? ` · ${t.trendStatus}` : ''}
                    </div>
                  </div>
                  <div className="shrink-0 text-right">
                    <div className="font-mono text-[var(--m-text-base)] tabular-nums">
                      {q?.price?.toFixed(2) ?? '—'}
                    </div>
                    {q?.pctChg != null ? <PctText value={q.pctChg} /> : null}
                  </div>
                  <div className="shrink-0 text-right">
                    {t?.score != null ? (
                      <div className="font-mono text-[var(--m-text-sm)] tabular-nums">
                        score {t.score}
                      </div>
                    ) : null}
                    {t?.buyAction ? (
                      <div className="text-[var(--m-text-xs)] text-[var(--k-accent)]">{t.buyAction}</div>
                    ) : null}
                  </div>
                  <button
                    type="button"
                    onClick={() => onRemove(it.symbol)}
                    className="shrink-0 rounded-md px-2 py-1 text-[var(--m-text-sm)] text-[var(--k-muted)] active:bg-[var(--k-surface-2)]"
                    aria-label={`删除 ${it.symbol}`}
                  >
                    删
                  </button>
                </div>
              );
            })}
          </MobileCard>
        ) : (
          <MobileCard className="px-3 py-8 text-center text-[var(--m-text-sm)] text-[var(--k-muted)]">
            暂无自选股，输入代码添加
          </MobileCard>
        )}
      </MobileSection>

      <MobileSection title="添加自选">
        <MobileCard className="space-y-2 p-3">
          <MobileField label="股票代码（6 位 A 股 / 4-5 位港股 / ETF）">
            <input
              value={code}
              onChange={(e) => setCode(e.target.value)}
              onKeyDown={(e) => e.key === 'Enter' && addByCode()}
              placeholder="如 600519 / 00700"
              className="h-[var(--m-tap)] w-full rounded-[var(--m-radius-md)] border border-[var(--k-border)] bg-[var(--k-surface-2)] px-3 text-[var(--m-text-base)] outline-none focus:border-[var(--k-accent)]"
            />
          </MobileField>
          <MobileButton block onClick={addByCode}>
            添加
          </MobileButton>
          {error ? <div className="text-[var(--m-text-sm)] text-[var(--k-danger)]">{error}</div> : null}
          {addedMsg ? <div className="text-[var(--m-text-sm)] text-[var(--k-down)]">{addedMsg}</div> : null}
        </MobileCard>
      </MobileSection>
    </div>
  );
}
