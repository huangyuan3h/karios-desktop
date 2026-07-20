'use client';

import * as React from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { ArrowLeft } from 'lucide-react';

import { Button } from '@/components/ui/button';
import { StockChart } from '@/components/stock/StockChart';
import { useChatStore } from '@/lib/chat/store';
import type { OHLCV } from '@/lib/indicators';
import {
  getLastDetailSyncMs,
  refetchStockDetail,
  useStockDetailQuery,
} from '@/lib/queries/stock';

export function StockPage({
  symbol,
  onBack,
}: {
  symbol: string;
  onBack: () => void;
}) {
  const queryClient = useQueryClient();
  const { addReference } = useChatStore();
  const { data, error: queryError, isFetching, refetch } = useStockDetailQuery(symbol);
  const [lastSyncMs, setLastSyncMs] = React.useState<number>(() => getLastDetailSyncMs(symbol));

  React.useEffect(() => {
    setLastSyncMs(getLastDetailSyncMs(symbol));
  }, [symbol, data]);

  const bars = data?.bars ?? null;
  const chips = data?.chips ?? null;
  const fundFlow = data?.fundFlow ?? null;
  const error =
    queryError instanceof Error ? queryError.message : queryError ? String(queryError) : null;
  const busy = isFetching;

  const chartData: OHLCV[] = React.useMemo(() => {
    const barRows = bars?.bars ?? [];
    return barRows
      .map((b) => {
        const open = Number(b.open);
        const high = Number(b.high);
        const low = Number(b.low);
        const close = Number(b.close);
        const volume = Number(String(b.volume).replaceAll(',', ''));
        if (!b.date || !Number.isFinite(open) || !Number.isFinite(high) || !Number.isFinite(low) || !Number.isFinite(close)) {
          return null;
        }
        return {
          time: b.date,
          open,
          high,
          low,
          close,
          volume: Number.isFinite(volume) ? volume : 0,
        };
      })
      .filter(Boolean) as OHLCV[];
  }, [bars]);

  async function onRefresh() {
    await refetch();
  }

  async function onSyncDetail() {
    await refetchStockDetail(queryClient, symbol, { force: true, quote: true });
    setLastSyncMs(getLastDetailSyncMs(symbol));
  }

  return (
    <div className="mx-auto w-full max-w-6xl p-6">
      <div className="mb-4 flex items-start justify-between gap-3">
        <div className="min-w-0">
          <div className="flex items-center gap-2">
            <Button variant="secondary" size="sm" onClick={onBack} className="gap-2">
              <ArrowLeft className="h-4 w-4" />
              Back
            </Button>
            <div className="text-lg font-semibold">{bars ? `${bars.ticker} ${bars.name}` : symbol}</div>
          </div>
          <div className="mt-1 text-sm text-[var(--k-muted)]">
            {bars ? `${bars.market} • ${bars.currency}` : 'Loading...'}
          </div>
        </div>
        <div className="flex items-center gap-2">
          <Button variant="secondary" size="sm" onClick={() => void onRefresh()} disabled={busy}>
            Refresh
          </Button>
          <Button size="sm" onClick={() => void onSyncDetail()} disabled={busy}>
            Sync detail
          </Button>
          <Button
            size="sm"
            disabled={!bars}
            onClick={() => {
              if (!bars) return;
              addReference({
                kind: 'stock',
                refId: bars.symbol,
                symbol: bars.symbol,
                market: bars.market,
                ticker: bars.ticker,
                name: bars.name,
                barsDays: 60,
                chipsDays: 30,
                fundFlowDays: 30,
                capturedAt: new Date().toISOString(),
              });
            }}
          >
            Reference to chat
          </Button>
        </div>
      </div>
      {lastSyncMs ? (
        <div className="mb-3 text-xs text-[var(--k-muted)]">
          Last detail sync: {new Date(lastSyncMs).toLocaleString()}
        </div>
      ) : null}

      {error ? (
        <div className="mb-4 rounded-lg border border-red-500/30 bg-red-500/10 px-3 py-2 text-sm text-red-600">
          {error}
        </div>
      ) : null}

      <section className="rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] p-4">
        <div className="flex items-center justify-between">
          <div className="font-medium">Candles / Volume / MACD / KDJ</div>
          <div className="text-xs text-[var(--k-muted)]">{bars?.bars?.length ?? 0} bars</div>
        </div>
        <div className="mt-3">
          {chartData.length > 0 ? (
            <StockChart data={chartData} />
          ) : (
            <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-bg)] px-3 py-10 text-center text-sm text-[var(--k-muted)]">
              No bars yet. Try Refresh.
            </div>
          )}
        </div>
      </section>

      <section className="mt-4 rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] p-4">
        <div className="flex items-center justify-between">
          <div className="font-medium">Chip distribution (筹码分布)</div>
          <div className="text-xs text-[var(--k-muted)]">
            {chips?.items?.length ? `${chips.items.length} rows` : '—'}
          </div>
        </div>
        {chips?.items?.length ? (
          <>
            <div className="mt-2 text-sm text-[var(--k-muted)]">
              Latest: profitRatio={chips.items[chips.items.length - 1]?.profitRatio} • avgCost=
              {chips.items[chips.items.length - 1]?.avgCost} • 70%[{chips.items[chips.items.length - 1]?.cost70Low},{' '}
              {chips.items[chips.items.length - 1]?.cost70High}] • 90%[{chips.items[chips.items.length - 1]?.cost90Low},{' '}
              {chips.items[chips.items.length - 1]?.cost90High}]
            </div>
            <div className="mt-3 overflow-hidden rounded-lg border border-[var(--k-border)]">
              <div className="max-h-[320px] overflow-auto">
                <table className="w-full border-collapse text-sm">
                  <thead className="sticky top-0 bg-[var(--k-surface-2)]">
                    <tr className="text-left text-xs text-[var(--k-muted)]">
                      {['Date', 'Profit', 'Avg', '70% Low', '70% High', '90% Low', '90% High'].map((h) => (
                        <th key={h} className="whitespace-nowrap px-3 py-2">
                          {h}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {chips.items.map((it) => (
                      <tr key={it.date} className="border-t border-[var(--k-border)]">
                        <td className="px-3 py-2 font-mono text-xs">{it.date}</td>
                        <td className="px-3 py-2 font-mono text-xs">{it.profitRatio}</td>
                        <td className="px-3 py-2 font-mono text-xs">{it.avgCost}</td>
                        <td className="px-3 py-2 font-mono text-xs">{it.cost70Low}</td>
                        <td className="px-3 py-2 font-mono text-xs">{it.cost70High}</td>
                        <td className="px-3 py-2 font-mono text-xs">{it.cost90Low}</td>
                        <td className="px-3 py-2 font-mono text-xs">{it.cost90High}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          </>
        ) : (
          <div className="mt-2 text-sm text-[var(--k-muted)]">
            Not available yet for this market (v0 supports CN A-shares only), or data source failed.
          </div>
        )}
      </section>

      <section className="mt-4 rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] p-4">
        <div className="flex items-center justify-between">
          <div className="font-medium">Fund flow distribution (资金成交分布)</div>
          <div className="text-xs text-[var(--k-muted)]">
            {fundFlow?.items?.length ? `${fundFlow.items.length} rows` : '—'}
          </div>
        </div>
        {fundFlow?.items?.length ? (
          <>
            <div className="mt-2 text-sm text-[var(--k-muted)]">
              Latest: main={fundFlow.items[fundFlow.items.length - 1]?.mainNetAmount} (
              {fundFlow.items[fundFlow.items.length - 1]?.mainNetRatio}
              %) • super={fundFlow.items[fundFlow.items.length - 1]?.superNetAmount} • large=
              {fundFlow.items[fundFlow.items.length - 1]?.largeNetAmount} • medium=
              {fundFlow.items[fundFlow.items.length - 1]?.mediumNetAmount} • small=
              {fundFlow.items[fundFlow.items.length - 1]?.smallNetAmount}
            </div>
            <div className="mt-3 overflow-hidden rounded-lg border border-[var(--k-border)]">
              <div className="max-h-[320px] overflow-auto">
                <table className="w-full border-collapse text-sm">
                  <thead className="sticky top-0 bg-[var(--k-surface-2)]">
                    <tr className="text-left text-xs text-[var(--k-muted)]">
                      {['Date', 'Main', 'Super', 'Large', 'Medium', 'Small'].map((h) => (
                        <th key={h} className="whitespace-nowrap px-3 py-2">
                          {h}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {fundFlow.items.map((it) => (
                      <tr key={it.date} className="border-t border-[var(--k-border)]">
                        <td className="px-3 py-2 font-mono text-xs">{it.date}</td>
                        <td className="px-3 py-2 font-mono text-xs">
                          {it.mainNetAmount} ({it.mainNetRatio}%)
                        </td>
                        <td className="px-3 py-2 font-mono text-xs">
                          {it.superNetAmount} ({it.superNetRatio}%)
                        </td>
                        <td className="px-3 py-2 font-mono text-xs">
                          {it.largeNetAmount} ({it.largeNetRatio}%)
                        </td>
                        <td className="px-3 py-2 font-mono text-xs">
                          {it.mediumNetAmount} ({it.mediumNetRatio}%)
                        </td>
                        <td className="px-3 py-2 font-mono text-xs">
                          {it.smallNetAmount} ({it.smallNetRatio}%)
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          </>
        ) : (
          <div className="mt-2 text-sm text-[var(--k-muted)]">
            Not available yet for this market (v0 supports CN A-shares only), or data source failed.
          </div>
        )}
      </section>
    </div>
  );
}
