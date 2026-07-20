'use client';

import * as React from 'react';

import { DATA_SYNC_BASE_URL } from '@/lib/endpoints';
import { StockChart } from '@/components/stock/StockChart';
import type { OHLCV } from '@/lib/indicators';

type IndexDetailProps = {
  type: 'cn' | 'macro';
  code: string;
  name: string;
  onBack: () => void;
};

type HistoryRow = {
  trade_date: string;
  open: number | null;
  high: number | null;
  low: number | null;
  close: number | null;
  vol: number | null;
  pct_chg?: number | null;
};

function rowToOHLCV(rows: HistoryRow[]): OHLCV[] {
  const sorted = rows
    .filter((r) => r.close != null && Number.isFinite(r.close))
    .sort((a, b) => a.trade_date.localeCompare(b.trade_date));
  return sorted.map((r) => ({
    time: r.trade_date,
    open: Number(r.open ?? r.close ?? 0),
    high: Number(r.high ?? r.close ?? 0),
    low: Number(r.low ?? r.close ?? 0),
    close: Number(r.close ?? 0),
    volume: Number(r.vol ?? 0),
  }));
}

async function fetchHistory(type: 'cn' | 'macro', code: string): Promise<HistoryRow[]> {
  const url =
    type === 'cn'
      ? `${DATA_SYNC_BASE_URL}/index/signals/history?ts_code=${encodeURIComponent(code)}&limit=500`
      : `${DATA_SYNC_BASE_URL}/macro/history?series_id=${encodeURIComponent(code)}&limit=500`;
  const res = await fetch(url, { cache: 'no-store' });
  if (!res.ok) throw new Error(`Failed to fetch history: ${res.status}`);
  const data = await res.json();
  return Array.isArray(data?.data) ? data.data : [];
}

export function IndexDetailPage({ type, code, name, onBack }: IndexDetailProps) {
  const [data, setData] = React.useState<OHLCV[]>([]);
  const [loading, setLoading] = React.useState(true);
  const [error, setError] = React.useState<string | null>(null);

  React.useEffect(() => {
    setLoading(true);
    setError(null);
    fetchHistory(type, code)
      .then((rows) => {
        setData(rowToOHLCV(rows));
      })
      .catch((e) => setError(e instanceof Error ? e.message : String(e)))
      .finally(() => setLoading(false));
  }, [type, code]);

  return (
    <div className="mx-auto max-w-5xl space-y-4 p-4">
      <div className="flex items-center gap-3">
        <button
          type="button"
          className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface-2)] px-3 py-1.5 text-xs font-medium shadow-sm hover:bg-[var(--k-surface)]"
          onClick={onBack}
        >
          Back
        </button>
        <h2 className="text-lg font-semibold text-[var(--k-fg)]">{name}</h2>
        <span className="rounded-md bg-[var(--k-surface-2)] px-2 py-0.5 text-xs text-[var(--k-muted)]">
          {code}
        </span>
      </div>

      {loading ? (
        <div className="text-sm text-[var(--k-muted)]">Loading history…</div>
      ) : error ? (
        <div className="rounded-xl border border-red-500/30 bg-red-500/10 px-4 py-3 text-sm text-red-800 dark:text-red-200">
          {error}
        </div>
      ) : data.length === 0 ? (
        <div className="text-sm text-[var(--k-muted)]">No historical data available.</div>
      ) : (
        <div className="space-y-2">
          <div className="text-xs text-[var(--k-muted)]">{data.length} trading days</div>
          <StockChart data={data} />
        </div>
      )}
    </div>
  );
}