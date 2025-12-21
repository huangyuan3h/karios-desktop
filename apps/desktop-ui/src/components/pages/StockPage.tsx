'use client';

import * as React from 'react';
import { ArrowLeft } from 'lucide-react';

import { Button } from '@/components/ui/button';
import { QUANT_BASE_URL } from '@/lib/endpoints';
import { useChatStore } from '@/lib/chat/store';

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
        <div className="font-medium">Price & Volume (last {data?.bars?.length ?? 0} days)</div>
        <div className="mt-3 overflow-hidden rounded-lg border border-[var(--k-border)]">
          <div className="max-h-[520px] overflow-auto">
            <table className="w-full border-collapse text-sm">
              <thead className="sticky top-0 bg-[var(--k-surface-2)]">
                <tr className="text-left text-xs text-[var(--k-muted)]">
                  {['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Amount'].map((h) => (
                    <th key={h} className="whitespace-nowrap px-3 py-2">
                      {h}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {(data?.bars ?? []).map((b) => (
                  <tr key={b.date} className="border-t border-[var(--k-border)]">
                    <td className="px-3 py-2 font-mono text-xs">{b.date}</td>
                    <td className="px-3 py-2 font-mono text-xs">{b.open}</td>
                    <td className="px-3 py-2 font-mono text-xs">{b.high}</td>
                    <td className="px-3 py-2 font-mono text-xs">{b.low}</td>
                    <td className="px-3 py-2 font-mono text-xs">{b.close}</td>
                    <td className="px-3 py-2 font-mono text-xs">{b.volume}</td>
                    <td className="px-3 py-2 font-mono text-xs">{b.amount}</td>
                  </tr>
                ))}
                {(data?.bars?.length ?? 0) === 0 ? (
                  <tr>
                    <td className="px-3 py-8 text-center text-sm text-[var(--k-muted)]" colSpan={7}>
                      No bars yet. Try Refresh.
                    </td>
                  </tr>
                ) : null}
              </tbody>
            </table>
          </div>
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


