'use client';

import * as React from 'react';

import { RefreshCw, Sparkles } from 'lucide-react';

import { Button } from '@/components/ui/button';
import { QUANT_BASE_URL } from '@/lib/endpoints';
import { useChatStore } from '@/lib/chat/store';

type LeaderSeriesPoint = { date: string; close: number };

type LeaderPick = {
  id: string;
  date: string;
  symbol: string;
  market: string;
  ticker: string;
  name: string;
  entryPrice?: number | null;
  score?: number | null;
  reason: string;
  sourceSignals?: Record<string, unknown>;
  riskPoints?: string[];
  createdAt: string;
  nowClose?: number | null;
  pctSinceEntry?: number | null;
  series?: LeaderSeriesPoint[];
};

type LeaderListResponse = {
  days: number;
  dates: string[];
  leaders: LeaderPick[];
};

type LeaderDailyResponse = {
  date: string;
  leaders: LeaderPick[];
  debug?: unknown;
};

function fmtPct(v: number | null | undefined) {
  if (!Number.isFinite(v as number)) return '—';
  return `${((v as number) * 100).toFixed(2)}%`;
}

function fmtPrice(v: number | null | undefined) {
  if (!Number.isFinite(v as number)) return '—';
  return (v as number).toFixed(2);
}

function CloseSparkline({ series }: { series: LeaderSeriesPoint[] }) {
  const vals = series.map((p) => (Number.isFinite(p.close) ? p.close : 0));
  const min = Math.min(...vals, 0);
  const max = Math.max(...vals, 1);
  const w = 120;
  const h = 24;
  const pad = 2;
  const span = Math.max(1e-6, max - min);
  const pts = vals
    .map((v, i) => {
      const x = pad + (i / Math.max(1, vals.length - 1)) * (w - pad * 2);
      const y = pad + (1 - (v - min) / span) * (h - pad * 2);
      return `${x.toFixed(1)},${y.toFixed(1)}`;
    })
    .join(' ');
  return (
    <svg width={w} height={h} viewBox={`0 0 ${w} ${h}`} className="block">
      <polyline fill="none" stroke="currentColor" strokeWidth="1.5" points={pts} opacity="0.85" />
    </svg>
  );
}

async function apiGetJson<T>(path: string): Promise<T> {
  const res = await fetch(`${QUANT_BASE_URL}${path}`, { cache: 'no-store' });
  const txt = await res.text().catch(() => '');
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}${txt ? `: ${txt}` : ''}`);
  return txt ? (JSON.parse(txt) as T) : ({} as T);
}

async function apiPostJson<T>(path: string, body: unknown): Promise<T> {
  const res = await fetch(`${QUANT_BASE_URL}${path}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  });
  const txt = await res.text().catch(() => '');
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}${txt ? `: ${txt}` : ''}`);
  return txt ? (JSON.parse(txt) as T) : ({} as T);
}

export function LeaderStocksPage() {
  const { addReference } = useChatStore();
  const [data, setData] = React.useState<LeaderListResponse | null>(null);
  const [busy, setBusy] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);
  const [debugOpen, setDebugOpen] = React.useState(false);
  const [lastDebug, setLastDebug] = React.useState<unknown>(null);

  const refresh = React.useCallback(async () => {
    setError(null);
    try {
      const r = await apiGetJson<LeaderListResponse>('/leader?days=10');
      setData(r);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  }, []);

  React.useEffect(() => {
    void refresh();
  }, [refresh]);

  async function onGenerateToday() {
    setBusy(true);
    setError(null);
    try {
      const r = await apiPostJson<LeaderDailyResponse>('/leader/daily', { force: true });
      setLastDebug(r.debug ?? null);
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }

  const leaders = data?.leaders ?? [];
  const byDate = React.useMemo(() => {
    const m = new Map<string, LeaderPick[]>();
    for (const it of leaders) {
      const arr = m.get(it.date) ?? [];
      arr.push(it);
      m.set(it.date, arr);
    }
    // newest first
    return Array.from(m.entries()).sort((a, b) => b[0].localeCompare(a[0]));
  }, [leaders]);

  return (
    <div className="mx-auto w-full max-w-6xl p-6">
      <div className="mb-4 flex items-start justify-between gap-3">
        <div>
          <div className="text-lg font-semibold">Leader Stocks (龙头股)</div>
          <div className="mt-1 text-sm text-[var(--k-muted)]">
            Maintain up to 2 leaders per trading day, keep last 10 trading days.
          </div>
        </div>
        <div className="flex items-center gap-2">
          <Button variant="secondary" size="sm" disabled={busy} onClick={() => void refresh()} className="gap-2">
            <RefreshCw className="h-4 w-4" />
            Refresh
          </Button>
          <Button size="sm" disabled={busy} onClick={() => void onGenerateToday()} className="gap-2">
            {busy ? <RefreshCw className="h-4 w-4 animate-spin" /> : <Sparkles className="h-4 w-4" />}
            {busy ? 'Generating…' : 'Generate today'}
          </Button>
          <Button
            size="sm"
            variant="secondary"
            disabled={!leaders.length}
            onClick={() => {
              addReference({
                kind: 'leaderStocks',
                refId: `leaderStocks:10:${Date.now()}`,
                days: 10,
                createdAt: new Date().toISOString(),
              });
            }}
          >
            Reference
          </Button>
        </div>
      </div>

      {error ? (
        <div className="mb-4 rounded-lg border border-red-500/30 bg-red-500/10 px-3 py-2 text-sm text-red-600">
          {error}
        </div>
      ) : null}

      <section className="rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] p-4">
        <div className="mb-2 flex items-center justify-between gap-2">
          <div className="text-sm font-medium">Leaders (last {data?.days ?? 10} trading days)</div>
          <Button
            size="sm"
            variant="secondary"
            onClick={() => setDebugOpen((v) => !v)}
            className="h-8 px-3 text-xs"
          >
            {debugOpen ? 'Hide debug' : 'Show debug'}
          </Button>
        </div>

        {debugOpen && lastDebug ? (
          <div className="mb-4 overflow-auto rounded-lg border border-[var(--k-border)] bg-[var(--k-surface-2)] p-3">
            <div className="mb-2 text-xs font-medium">Last generate debug</div>
            <pre className="whitespace-pre-wrap break-words text-xs text-[var(--k-muted)]">
              {JSON.stringify(lastDebug, null, 2)}
            </pre>
          </div>
        ) : null}

        {byDate.length ? (
          <div className="space-y-4">
            {byDate.map(([date, rows]) => (
              <div key={date} className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface-2)] p-3">
                <div className="mb-2 text-sm font-medium">{date}</div>
                <div className="overflow-x-auto">
                  <table className="w-full border-collapse text-sm">
                    <thead className="bg-[var(--k-surface)]">
                      <tr>
                        <th className="whitespace-nowrap border-b border-[var(--k-border)] px-2 py-2 text-left">
                          Symbol
                        </th>
                        <th className="whitespace-nowrap border-b border-[var(--k-border)] px-2 py-2 text-left">
                          Name
                        </th>
                        <th className="whitespace-nowrap border-b border-[var(--k-border)] px-2 py-2 text-right">
                          Score
                        </th>
                        <th className="whitespace-nowrap border-b border-[var(--k-border)] px-2 py-2 text-right">
                          Entry close
                        </th>
                        <th className="whitespace-nowrap border-b border-[var(--k-border)] px-2 py-2 text-right">
                          Now
                        </th>
                        <th className="whitespace-nowrap border-b border-[var(--k-border)] px-2 py-2 text-right">
                          Pct
                        </th>
                        <th className="whitespace-nowrap border-b border-[var(--k-border)] px-2 py-2 text-left">
                          Why
                        </th>
                        <th className="whitespace-nowrap border-b border-[var(--k-border)] px-2 py-2 text-left">
                          Trend
                        </th>
                      </tr>
                    </thead>
                    <tbody>
                      {rows.map((r) => (
                        <React.Fragment key={r.id}>
                          <tr className="bg-[var(--k-surface)]">
                            <td className="border-b border-[var(--k-border)] px-2 py-2 font-mono">
                              {r.ticker || r.symbol}
                            </td>
                            <td className="border-b border-[var(--k-border)] px-2 py-2">{r.name}</td>
                            <td className="border-b border-[var(--k-border)] px-2 py-2 text-right">
                              {Number.isFinite(r.score as number) ? Math.round(r.score as number) : '—'}
                            </td>
                            <td className="border-b border-[var(--k-border)] px-2 py-2 text-right">
                              {fmtPrice(r.entryPrice)}
                            </td>
                            <td className="border-b border-[var(--k-border)] px-2 py-2 text-right">
                              {fmtPrice(r.nowClose)}
                            </td>
                            <td className="border-b border-[var(--k-border)] px-2 py-2 text-right">
                              {fmtPct(r.pctSinceEntry)}
                            </td>
                            <td className="border-b border-[var(--k-border)] px-2 py-2 text-[var(--k-muted)]">
                              {r.reason}
                            </td>
                            <td className="border-b border-[var(--k-border)] px-2 py-2">
                              {r.series?.length ? <CloseSparkline series={r.series} /> : null}
                            </td>
                          </tr>
                          <tr>
                            <td colSpan={8} className="border-b border-[var(--k-border)] px-2 py-2">
                              <details>
                                <summary className="cursor-pointer text-xs text-[var(--k-muted)]">
                                  Details
                                </summary>
                                <div className="mt-2 grid gap-2 text-xs text-[var(--k-muted)] md:grid-cols-2">
                                  <div>
                                    <div className="font-medium text-[var(--k-text)]">Source signals</div>
                                    <pre className="mt-1 whitespace-pre-wrap break-words">
                                      {JSON.stringify(r.sourceSignals ?? {}, null, 2)}
                                    </pre>
                                  </div>
                                  <div>
                                    <div className="font-medium text-[var(--k-text)]">Risk points</div>
                                    <ul className="mt-1 list-disc pl-4">
                                      {(r.riskPoints ?? []).map((x, idx) => (
                                        <li key={idx}>{x}</li>
                                      ))}
                                    </ul>
                                  </div>
                                </div>
                              </details>
                            </td>
                          </tr>
                        </React.Fragment>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            ))}
          </div>
        ) : (
          <div className="text-sm text-[var(--k-muted)]">
            No leaders yet. Click “Generate today” after syncing screener + industry flow.
          </div>
        )}
      </section>
    </div>
  );
}


