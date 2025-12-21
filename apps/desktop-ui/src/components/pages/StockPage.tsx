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
      const d = await apiGetJson<BarsResp>(
        `/market/stocks/${encodeURIComponent(symbol)}/bars?days=60`,
      );
      setData(d);
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
        <div className="font-medium">Chip distribution / Fund flow</div>
        <div className="mt-2 text-sm text-[var(--k-muted)]">
          Coming soon. We will add AkShare/Eastmoney-based data adapters here (cyq/fund flow).
        </div>
      </section>
    </div>
  );
}


