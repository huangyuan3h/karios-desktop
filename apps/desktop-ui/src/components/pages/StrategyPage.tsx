'use client';

import * as React from 'react';
import { RefreshCw, Sparkles } from 'lucide-react';

import { Button } from '@/components/ui/button';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import { Textarea } from '@/components/ui/textarea';
import { QUANT_BASE_URL } from '@/lib/endpoints';
import { useChatStore } from '@/lib/chat/store';

type BrokerAccount = {
  id: string;
  broker: string;
  title: string;
  accountMasked: string | null;
  updatedAt: string;
};

type StrategyOrder = {
  kind: string;
  side: string;
  trigger: string;
  qty: string;
  timeInForce?: string | null;
  notes?: string | null;
};

type StrategyRecommendation = {
  symbol: string;
  ticker: string;
  name: string;
  thesis: string;
  levels: {
    support: string[];
    resistance: string[];
    invalidations: string[];
  };
  orders: StrategyOrder[];
  positionSizing: string;
  riskNotes: string[];
};

type StrategyCandidate = {
  symbol: string;
  market: string;
  ticker: string;
  name: string;
  score: number;
  rank: number;
  why: string;
};

type StrategyReport = {
  id: string;
  date: string;
  accountId: string;
  accountTitle: string;
  createdAt: string;
  model: string;
  candidates: StrategyCandidate[];
  leader: { symbol: string; reason: string };
  recommendations: StrategyRecommendation[];
  riskNotes: string[];
  inputSnapshot?: unknown;
  raw?: unknown;
};

type StrategyPrompt = {
  accountId: string;
  prompt: string;
  updatedAt: string | null;
};

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

async function apiPutJson<T>(path: string, body: unknown): Promise<T> {
  const res = await fetch(`${QUANT_BASE_URL}${path}`, {
    method: 'PUT',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  });
  const txt = await res.text().catch(() => '');
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}${txt ? `: ${txt}` : ''}`);
  return txt ? (JSON.parse(txt) as T) : ({} as T);
}

export function StrategyPage() {
  const { addReference } = useChatStore();
  const [accounts, setAccounts] = React.useState<BrokerAccount[]>([]);
  const [accountId, setAccountId] = React.useState<string>('');
  const [prompt, setPrompt] = React.useState<string>('');
  const [report, setReport] = React.useState<StrategyReport | null>(null);
  const [busy, setBusy] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);
  const [debugOpen, setDebugOpen] = React.useState(false);

  const refresh = React.useCallback(async () => {
    setError(null);
    try {
      const acc = await apiGetJson<BrokerAccount[]>('/broker/accounts?broker=pingan');
      setAccounts(acc);
      const effectiveAccountId = accountId || acc[0]?.id || '';
      if (!accountId && effectiveAccountId) setAccountId(effectiveAccountId);
      if (effectiveAccountId) {
        const p = await apiGetJson<StrategyPrompt>(
          `/strategy/accounts/${encodeURIComponent(effectiveAccountId)}/prompt`,
        );
        setPrompt(p.prompt || '');
        try {
          const r = await apiGetJson<StrategyReport>(
            `/strategy/accounts/${encodeURIComponent(effectiveAccountId)}/daily`,
          );
          setReport(r);
        } catch {
          setReport(null);
        }
      } else {
        setPrompt('');
        setReport(null);
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  }, [accountId]);

  React.useEffect(() => {
    void refresh();
  }, [refresh]);

  async function onSavePrompt() {
    if (!accountId) return;
    setBusy(true);
    setError(null);
    try {
      await apiPutJson<StrategyPrompt>(`/strategy/accounts/${encodeURIComponent(accountId)}/prompt`, {
        prompt,
      });
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }

  async function onGenerateToday() {
    if (!accountId) return;
    setBusy(true);
    setError(null);
    try {
      const r = await apiPostJson<StrategyReport>(
        `/strategy/accounts/${encodeURIComponent(accountId)}/daily`,
        { force: true },
      );
      setReport(r);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }

  async function onLoadCached() {
    if (!accountId) return;
    setBusy(true);
    setError(null);
    try {
      const r = await apiPostJson<StrategyReport>(
        `/strategy/accounts/${encodeURIComponent(accountId)}/daily`,
        { force: false },
      );
      setReport(r);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="mx-auto w-full max-w-5xl p-6">
      <div className="mb-6 flex items-start justify-between gap-3">
        <div>
          <div className="text-lg font-semibold">Strategy (Swing 1-10D)</div>
          <div className="mt-1 text-sm text-[var(--k-muted)]">
            Generate a daily action guide from TradingView + account state + stock context.
          </div>
        </div>
        <div className="flex items-center gap-2">
          <Select value={accountId} onValueChange={(v) => setAccountId(v)}>
            <SelectTrigger className="h-9 w-[240px]">
              <SelectValue placeholder="Select account" />
            </SelectTrigger>
            <SelectContent>
              {accounts.map((a) => (
                <SelectItem key={a.id} value={a.id}>
                  {a.title}
                  {a.accountMasked ? ` (${a.accountMasked})` : ''}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
          <Button variant="secondary" size="sm" onClick={() => void refresh()} className="gap-2">
            <RefreshCw className="h-4 w-4" />
            Refresh
          </Button>
        </div>
      </div>

      {error ? (
        <div className="mb-4 rounded-lg border border-red-500/30 bg-red-500/10 px-3 py-2 text-sm text-red-600">
          {error}
        </div>
      ) : null}

      <section className="mb-4 rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] p-4">
        <div className="mb-3 flex items-center justify-between gap-2">
          <div className="text-sm font-medium">Account strategy prompt</div>
          <div className="flex items-center gap-2">
            <Button size="sm" variant="secondary" disabled={!accountId || busy} onClick={() => void onSavePrompt()}>
              Save
            </Button>
            <Button
              size="sm"
              variant="secondary"
              disabled={!accountId || busy}
              onClick={() => void onLoadCached()}
            >
              Use cached
            </Button>
            <Button size="sm" disabled={!accountId || busy} onClick={() => void onGenerateToday()} className="gap-2">
              <Sparkles className="h-4 w-4" />
              Generate today
            </Button>
          </div>
        </div>
        <Textarea
          value={prompt}
          onChange={(e) => setPrompt(e.target.value)}
          placeholder="Describe account constraints, preferences, forbidden assets, position sizing rules..."
          className="min-h-[120px]"
        />
        <div className="mt-2 text-xs text-[var(--k-muted)]">
          Tip: include constraints like max positions, max per-position %, no margin, CN/HK only, etc.
        </div>
      </section>

      {report ? (
        <section className="rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] p-4">
          <div className="flex flex-wrap items-center justify-between gap-3">
            <div className="min-w-0">
              <div className="font-medium">Daily report</div>
              <div className="mt-1 text-xs text-[var(--k-muted)]">
                Date: {report.date} • model: {report.model} • created:{' '}
                {new Date(report.createdAt).toLocaleString()}
              </div>
            </div>
            <Button
              size="sm"
              disabled={!accountId}
              onClick={() => {
                addReference({
                  kind: 'strategyReport',
                  refId: report.id,
                  reportId: report.id,
                  accountId: report.accountId,
                  accountTitle: report.accountTitle,
                  date: report.date,
                  createdAt: report.createdAt,
                });
              }}
            >
              Reference report to chat
            </Button>
          </div>

          <div className="mt-4 rounded-lg border border-[var(--k-border)] bg-[var(--k-surface-2)] p-3">
            <div className="mb-2 text-sm font-medium">Top candidates (≤ 5)</div>
            {report.candidates.length ? (
              <div className="overflow-auto rounded border border-[var(--k-border)]">
                <table className="w-full border-collapse text-xs">
                  <thead className="bg-[var(--k-surface)] text-[var(--k-muted)]">
                    <tr className="text-left">
                      <th className="px-2 py-1">Rank</th>
                      <th className="px-2 py-1">Ticker</th>
                      <th className="px-2 py-1">Name</th>
                      <th className="px-2 py-1">Score</th>
                      <th className="px-2 py-1">Why</th>
                    </tr>
                  </thead>
                  <tbody>
                    {report.candidates.map((c) => (
                      <tr key={c.symbol} className="border-t border-[var(--k-border)]">
                        <td className="px-2 py-1 font-mono">{c.rank}</td>
                        <td className="px-2 py-1 font-mono">{c.ticker}</td>
                        <td className="px-2 py-1">{c.name}</td>
                        <td className="px-2 py-1 font-mono">{Math.round(c.score)}</td>
                        <td className="px-2 py-1">{c.why}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : (
              <div className="text-xs text-[var(--k-muted)]">
                No candidates returned. Ensure TradingView snapshots exist (Screener → Sync / History), or import holdings.
              </div>
            )}
          </div>

          <div className="mt-4 grid gap-4 md:grid-cols-2">
            <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface-2)] p-3">
              <div className="mb-2 text-sm font-medium">Leader</div>
              <div className="text-xs text-[var(--k-muted)]">
                {report.leader.symbol ? `${report.leader.symbol}` : 'N/A'}
              </div>
              <div className="mt-2 text-xs">{report.leader.reason || '—'}</div>
            </div>
            <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface-2)] p-3">
              <div className="mb-2 text-sm font-medium">Account-level risk notes</div>
              {report.riskNotes.length ? (
                <ul className="list-inside list-disc text-xs">
                  {report.riskNotes.map((x, i) => (
                    <li key={i}>{x}</li>
                  ))}
                </ul>
              ) : (
                <div className="text-xs text-[var(--k-muted)]">None</div>
              )}
            </div>
          </div>

          <div className="mt-4 rounded-lg border border-[var(--k-border)] bg-[var(--k-surface-2)] p-3">
            <div className="mb-2 text-sm font-medium">Recommendations (≤ 3)</div>
            {report.recommendations.length ? (
              <div className="grid gap-3">
                {report.recommendations.map((r) => (
                  <div key={r.symbol} className="rounded-md border border-[var(--k-border)] bg-[var(--k-surface)] p-3">
                    <div className="flex flex-wrap items-center justify-between gap-2">
                      <div className="text-sm font-medium">
                        {r.ticker} {r.name}
                      </div>
                      <div className="text-xs text-[var(--k-muted)]">{r.symbol}</div>
                    </div>
                    <div className="mt-2 text-xs">
                      <div className="font-medium">Thesis</div>
                      <div className="mt-1 text-[var(--k-muted)]">{r.thesis}</div>
                    </div>
                    <div className="mt-3 grid gap-3 md:grid-cols-2">
                      <div className="text-xs">
                        <div className="font-medium">Levels</div>
                        <div className="mt-1 text-[var(--k-muted)]">
                          <div>Support: {r.levels.support.join(' / ') || '—'}</div>
                          <div>Resistance: {r.levels.resistance.join(' / ') || '—'}</div>
                          <div>Invalidations: {r.levels.invalidations.join(' / ') || '—'}</div>
                        </div>
                      </div>
                      <div className="text-xs">
                        <div className="font-medium">Position sizing</div>
                        <div className="mt-1 text-[var(--k-muted)]">{r.positionSizing || '—'}</div>
                      </div>
                    </div>
                    <div className="mt-3 text-xs">
                      <div className="font-medium">Orders</div>
                      {r.orders.length ? (
                        <div className="mt-1 overflow-auto rounded border border-[var(--k-border)]">
                          <table className="w-full border-collapse text-xs">
                            <thead className="bg-[var(--k-surface-2)] text-[var(--k-muted)]">
                              <tr className="text-left">
                                <th className="px-2 py-1">Kind</th>
                                <th className="px-2 py-1">Side</th>
                                <th className="px-2 py-1">Trigger</th>
                                <th className="px-2 py-1">Qty</th>
                                <th className="px-2 py-1">TIF</th>
                              </tr>
                            </thead>
                            <tbody>
                              {r.orders.map((o, idx) => (
                                <tr key={idx} className="border-t border-[var(--k-border)]">
                                  <td className="px-2 py-1 font-mono">{o.kind}</td>
                                  <td className="px-2 py-1 font-mono">{o.side}</td>
                                  <td className="px-2 py-1 font-mono">{o.trigger}</td>
                                  <td className="px-2 py-1 font-mono">{o.qty}</td>
                                  <td className="px-2 py-1 font-mono">{o.timeInForce || ''}</td>
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                      ) : (
                        <div className="mt-1 text-[var(--k-muted)]">No orders returned.</div>
                      )}
                    </div>
                    {r.riskNotes?.length ? (
                      <div className="mt-3 text-xs">
                        <div className="font-medium">Risk notes</div>
                        <ul className="mt-1 list-inside list-disc text-[var(--k-muted)]">
                          {r.riskNotes.map((x, i) => (
                            <li key={i}>{x}</li>
                          ))}
                        </ul>
                      </div>
                    ) : null}
                  </div>
                ))}
              </div>
            ) : (
              <div className="text-xs text-[var(--k-muted)]">No recommendations returned.</div>
            )}
          </div>

          <div className="mt-4">
            <Button
              variant="secondary"
              size="sm"
              onClick={() => setDebugOpen((v) => !v)}
              className="h-8 px-3 text-xs"
            >
              {debugOpen ? 'Hide debug' : 'Show debug'}
            </Button>
            {debugOpen ? (
              <div className="mt-2 overflow-auto rounded-lg border border-[var(--k-border)] bg-[var(--k-surface-2)] p-3">
                <div className="mb-2 text-xs font-medium">Raw report JSON</div>
                <pre className="whitespace-pre-wrap break-words text-xs text-[var(--k-muted)]">
                  {JSON.stringify(report.raw ?? report, null, 2)}
                </pre>
              </div>
            ) : null}
          </div>
        </section>
      ) : (
        <div className="text-sm text-[var(--k-muted)]">No report yet. Click “Generate today”.</div>
      )}
    </div>
  );
}


