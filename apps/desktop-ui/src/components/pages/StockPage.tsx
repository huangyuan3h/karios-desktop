'use client';

import * as React from 'react';
import { ArrowLeft } from 'lucide-react';

import { Button } from '@/components/ui/button';
import { StockChart } from '@/components/stock/StockChart';
import { QUANT_BASE_URL } from '@/lib/endpoints';
import { useChatStore } from '@/lib/chat/store';
import type { OHLCV } from '@/lib/indicators';

type BarsResp = {
  symbol: string;
  market: string;
  ticker: string;
  name: string;
  currency: string;
  bars: Array<{
    date: string;
    open: string;
    high: string;
    low: string;
    close: string;
    volume: string;
    amount: string;
  }>;
};

type ChipsResp = {
  symbol: string;
  market: string;
  ticker: string;
  name: string;
  currency: string;
  items: Array<{
    date: string;
    profitRatio: string;
    avgCost: string;
    cost90Low: string;
    cost90High: string;
    cost90Conc: string;
    cost70Low: string;
    cost70High: string;
    cost70Conc: string;
  }>;
};

type FundFlowResp = {
  symbol: string;
  market: string;
  ticker: string;
  name: string;
  currency: string;
  items: Array<{
    date: string;
    close: string;
    changePct: string;
    mainNetAmount: string;
    mainNetRatio: string;
    superNetAmount: string;
    superNetRatio: string;
    largeNetAmount: string;
    largeNetRatio: string;
    mediumNetAmount: string;
    mediumNetRatio: string;
    smallNetAmount: string;
    smallNetRatio: string;
  }>;
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

export function StockPage({
  symbol,
  onBack,
}: {
  symbol: string;
  onBack: () => void;
}) {
  const { addReference } = useChatStore();
  const [data, setData] = React.useState<BarsResp | null>(null);
  const [chips, setChips] = React.useState<ChipsResp | null>(null);
  const [fundFlow, setFundFlow] = React.useState<FundFlowResp | null>(null);
  const [error, setError] = React.useState<string | null>(null);
  const [busy, setBusy] = React.useState(false);
  const chartData: OHLCV[] = React.useMemo(() => {
    const bars = data?.bars ?? [];
    return bars
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
  }, [data]);

  const refresh = React.useCallback(async () => {
    setError(null);
    setBusy(true);
    try {
      const [d, c] = await Promise.all([
        apiGetJson<BarsResp>(`/market/stocks/${encodeURIComponent(symbol)}/bars?days=60`),
        apiGetJson<ChipsResp>(`/market/stocks/${encodeURIComponent(symbol)}/chips?days=60`).catch(
          () => null,
        ),
      ]);
      const ff = await apiGetJson<FundFlowResp>(
        `/market/stocks/${encodeURIComponent(symbol)}/fund-flow?days=60`,
      ).catch(() => null);
      setData(d);
      setChips(c);
      setFundFlow(ff);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }, [symbol]);

  React.useEffect(() => {
    void refresh();
  }, [refresh]);

  return (
    <div className="mx-auto w-full max-w-6xl p-6">
      <div className="mb-4 flex items-start justify-between gap-3">
        <div className="min-w-0">
          <div className="flex items-center gap-2">
            <Button variant="secondary" size="sm" onClick={onBack} className="gap-2">
              <ArrowLeft className="h-4 w-4" />
              Back
            </Button>
            <div className="text-lg font-semibold">{data ? `${data.ticker} ${data.name}` : symbol}</div>
          </div>
          <div className="mt-1 text-sm text-[var(--k-muted)]">
            {data ? `${data.market} • ${data.currency}` : 'Loading...'}
          </div>
        </div>
        <div className="flex items-center gap-2">
          <Button variant="secondary" size="sm" onClick={() => void refresh()} disabled={busy}>
            Refresh
          </Button>
          <Button
            size="sm"
            disabled={!data}
            onClick={() => {
              if (!data) return;
              addReference({
                kind: 'stock',
                refId: data.symbol,
                symbol: data.symbol,
                market: data.market,
                ticker: data.ticker,
                name: data.name,
                days: 60,
                capturedAt: new Date().toISOString(),
              });
            }}
          >
            Reference to chat
          </Button>
        </div>
      </div>

      {error ? (
        <div className="mb-4 rounded-lg border border-red-500/30 bg-red-500/10 px-3 py-2 text-sm text-red-600">
          {error}
        </div>
      ) : null}

      <section className="rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] p-4">
        <div className="flex items-center justify-between">
          <div className="font-medium">Candles / Volume / MACD / KDJ</div>
          <div className="text-xs text-[var(--k-muted)]">{data?.bars?.length ?? 0} bars</div>
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


