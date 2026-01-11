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
import { QUANT_BASE_URL } from '@/lib/endpoints';
import { useChatStore } from '@/lib/chat/store';
import type { ChatReference } from '@/lib/chat/types';

type BrokerAccount = { id: string; title: string; broker: string };

type RankItem = {
  symbol: string;
  market: string;
  ticker: string;
  name: string;
  sector?: string | null;
  score: number;
  probBand?: string | null;
  probProfit2d?: number | null; // 0-100
  ev2dPct?: number | null;
  dd2dPct?: number | null;
  confidence?: string | null;
  buyPrice?: number | null;
  buyPriceSrc?: string | null;
  rawScore?: number | null;
  whyBullets?: string[];
  signals?: string[];
  breakdown?: Record<string, number>;
};

type RankSnapshot = {
  id: string;
  asOfTs?: string | null;
  asOfDate: string;
  accountId: string;
  createdAt: string;
  universeVersion: string;
  riskMode?: string | null;
  objective?: string | null;
  horizon?: string | null;
  items: RankItem[];
  debug?: unknown;
};

type MorningRadarTheme = {
  kind: string;
  name: string;
  score: number;
  todayStrength?: number;
  volSurge?: number;
  limitupCount?: number;
  followersCount?: number;
  topTickers?: Array<{
    symbol: string;
    ticker: string;
    name: string;
    chgPct?: number;
    volRatio?: number;
  }>;
};

type MorningRadarResponse = {
  asOfTs: string;
  tradeDate: string;
  accountId: string;
  universeVersion: string;
  themes: MorningRadarTheme[];
  debug?: unknown;
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

function fmtDateTime(x: string | null | undefined) {
  if (!x) return '—';
  const d = new Date(x);
  return Number.isNaN(d.getTime()) ? x : d.toLocaleString();
}

function fmtPctOrDash(x: number | null | undefined, digits = 0) {
  if (typeof x !== 'number' || Number.isNaN(x)) return '—';
  return `${x.toFixed(digits)}%`;
}

export function RankPage({ onOpenStock }: { onOpenStock?: (symbol: string) => void } = {}) {
  const { addReference } = useChatStore();
  const [accounts, setAccounts] = React.useState<BrokerAccount[]>([]);
  const [accountId, setAccountId] = React.useState<string>('');
  const [tab, setTab] = React.useState<'top2d' | 'morning'>('top2d');
  const [dataNext2d, setDataNext2d] = React.useState<RankSnapshot | null>(null);
  const [dataMorning, setDataMorning] = React.useState<MorningRadarResponse | null>(null);
  const [busy, setBusy] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);

  React.useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const accs = await apiGetJson<BrokerAccount[]>('/broker/accounts?broker=pingan');
        if (cancelled) return;
        const list = Array.isArray(accs) ? accs : [];
        setAccounts(list);
        if (!accountId && list.length) setAccountId(String(list[0].id));
      } catch {
        // ignore
      }
    })();
    return () => {
      cancelled = true;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const refresh = React.useCallback(
    async (force?: boolean) => {
      if (!accountId) return;
      setError(null);
      try {
        if (tab === 'morning') {
          const r = await apiPostJson<MorningRadarResponse>('/rank/cn/morning/generate', {
            accountId,
            universeVersion: 'v0',
            topK: 3,
            perTheme: 3,
            asOfTs: new Date().toISOString(),
          });
          setDataMorning(r);
        } else {
          if (force) {
            const r = await apiPostJson<RankSnapshot>('/rank/cn/next2d/generate', {
              accountId,
              force: true,
              limit: 30,
              includeHoldings: true,
              universeVersion: 'v0',
            });
            setDataNext2d(r);
          } else {
            const r = await apiGetJson<RankSnapshot>(
              `/rank/cn/next2d?accountId=${encodeURIComponent(accountId)}&limit=30&universeVersion=v0`,
            );
            setDataNext2d(r);
          }
        }
      } catch (e) {
        setError(e instanceof Error ? e.message : String(e));
      }
    },
    [accountId, tab],
  );

  React.useEffect(() => {
    void refresh(false);
  }, [refresh]);

  async function onGenerate() {
    setBusy(true);
    try {
      await refresh(true);
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="mx-auto w-full max-w-6xl p-6">
      <div className="mb-4 flex items-start justify-between gap-3">
        <div>
          <div className="text-lg font-semibold">Quant</div>
          <div className="mt-1 text-sm text-[var(--k-muted)]">
            {tab === 'morning'
              ? 'Morning radar (09:00-10:00): identify strong themes and representative stocks for manual verification.'
              : 'Top picks (2D): score is a calibrated profit-likelihood decision score for buying now and holding ~2 trading days.'}
          </div>
        </div>
        <div className="flex flex-wrap items-center gap-2">
          <div className="flex items-center gap-1 rounded-md border border-[var(--k-border)] bg-[var(--k-surface)] p-1 text-xs">
            <button
              type="button"
              className={`rounded px-2 py-1 ${tab === 'top2d' ? 'bg-[var(--k-surface-2)]' : ''}`}
              onClick={() => setTab('top2d')}
            >
              Top Picks (2D)
            </button>
            <button
              type="button"
              className={`rounded px-2 py-1 ${tab === 'morning' ? 'bg-[var(--k-surface-2)]' : ''}`}
              onClick={() => setTab('morning')}
            >
              Morning Radar
            </button>
          </div>
          <Button
            variant="secondary"
            size="sm"
            className="gap-2"
            disabled={!accountId || busy}
            onClick={() => void refresh(false)}
          >
            <RefreshCw className="h-4 w-4" />
            Refresh
          </Button>
          <Button
            size="sm"
            className="gap-2"
            disabled={!accountId || busy}
            onClick={() => void onGenerate()}
          >
            {busy ? (
              <RefreshCw className="h-4 w-4 animate-spin" />
            ) : (
              <Sparkles className="h-4 w-4" />
            )}
            {busy ? 'Generating…' : 'Generate'}
          </Button>
          {tab === 'top2d' ? (
            <Button
              size="sm"
              variant="secondary"
              disabled={!accountId}
              onClick={() => {
                addReference({
                  kind: 'rankList',
                  refId: `rankList:${accountId}:${Date.now()}`,
                  accountId,
                  asOfDate: String(dataNext2d?.asOfDate ?? ''),
                  limit: 30,
                  createdAt: new Date().toISOString(),
                } satisfies ChatReference);
              }}
            >
              Reference
            </Button>
          ) : null}
        </div>
      </div>

      <div className="mb-4 flex flex-wrap items-center gap-3">
        <div className="w-[280px]">
          <Select value={accountId} onValueChange={setAccountId}>
            <SelectTrigger className="h-9">
              <SelectValue placeholder="Select account" />
            </SelectTrigger>
            <SelectContent>
              {accounts.map((a) => (
                <SelectItem key={a.id} value={a.id}>
                  {a.title || a.id}
                </SelectItem>
              ))}
              {!accounts.length ? (
                <SelectItem value="__none__" disabled>
                  No accounts
                </SelectItem>
              ) : null}
            </SelectContent>
          </Select>
        </div>
        <div className="text-xs text-[var(--k-muted)]">
          {tab === 'morning' ? (
            <>
              tradeDate: {dataMorning?.tradeDate ?? '—'} • asOfTs:{' '}
              {fmtDateTime(dataMorning?.asOfTs)} • createdAt: {fmtDateTime(dataMorning?.asOfTs)} •
              universe: {dataMorning?.universeVersion ?? 'v0'}
            </>
          ) : (
            <>
              asOfDate: {dataNext2d?.asOfDate ?? '—'} • asOfTs:{' '}
              {fmtDateTime(dataNext2d?.asOfTs ?? '')} • createdAt:{' '}
              {fmtDateTime(dataNext2d?.createdAt)} • riskMode: {dataNext2d?.riskMode ?? '—'} •
              objective: {dataNext2d?.objective ?? '—'}
            </>
          )}
        </div>
      </div>

      {error ? (
        <div className="mb-4 rounded-lg border border-red-500/30 bg-red-500/10 px-3 py-2 text-sm text-red-600">
          {error}
        </div>
      ) : null}

      {tab === 'top2d' ? (
        <div className="space-y-4">
          {(() => {
            const items = dataNext2d?.items ?? [];
            const top1 = items[0];
            const top3 = items.slice(0, 3);
            return (
              <>
                <section className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] p-4">
                  <div className="text-sm font-medium">Top pick now (2D)</div>
                  <div className="mt-1 text-xs text-[var(--k-muted)]">
                    Score is a decision score prioritizing profit probability over ~2 trading days
                    (buy now). Prob/EV/DD are calibrated from historical outcomes (best-effort).
                  </div>
                  {top1 ? (
                    <div className="mt-3 grid gap-3 md:grid-cols-3">
                      <div className="md:col-span-2">
                        <div className="flex flex-wrap items-center gap-2">
                          <button
                            type="button"
                            className="font-mono text-[var(--k-accent)] hover:underline"
                            onClick={() => onOpenStock?.(top1.symbol)}
                          >
                            {top1.ticker}
                          </button>
                          <div className="text-sm">{top1.name}</div>
                          <div className="text-xs text-[var(--k-muted)]">
                            {top1.sector ? `· ${top1.sector}` : ''}
                          </div>
                        </div>
                        <div className="mt-2 text-xs text-[var(--k-muted)]">
                          {(top1.whyBullets ?? []).slice(0, 4).join(' · ') ||
                            (typeof top1.probProfit2d === 'number'
                              ? ''
                              : 'Calibration not ready yet (need ~2 trading days of outcomes). Using baseline rawScore.')}
                        </div>
                      </div>
                      <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface-2)] p-3 text-xs">
                        <div className="flex items-center justify-between">
                          <div className="text-[var(--k-muted)]">Score</div>
                          <div className="font-mono text-[var(--k-text)]">
                            {Math.round(Number(top1.score ?? 0))}
                          </div>
                        </div>
                        <div className="mt-2 flex items-center justify-between">
                          <div className="text-[var(--k-muted)]">ProbProfit2D</div>
                          <div className="font-mono">
                            {typeof top1.probProfit2d === 'number'
                              ? `${Math.round(top1.probProfit2d)}%`
                              : '—'}
                          </div>
                        </div>
                        <div className="mt-2 flex items-center justify-between">
                          <div className="text-[var(--k-muted)]">EV2D</div>
                          <div className="font-mono">{fmtPctOrDash(top1.ev2dPct, 2)}</div>
                        </div>
                        <div className="mt-2 flex items-center justify-between">
                          <div className="text-[var(--k-muted)]">DD2D</div>
                          <div className="font-mono">{fmtPctOrDash(top1.dd2dPct, 2)}</div>
                        </div>
                        <div className="mt-2 flex items-center justify-between">
                          <div className="text-[var(--k-muted)]">Confidence</div>
                          <div className="font-mono">{String(top1.confidence ?? '—')}</div>
                        </div>
                        <div className="mt-2 flex items-center justify-between">
                          <div className="text-[var(--k-muted)]">Buy price</div>
                          <div className="font-mono">
                            {top1.buyPrice
                              ? `${top1.buyPrice.toFixed(2)} (${top1.buyPriceSrc ?? '—'})`
                              : '—'}
                          </div>
                        </div>
                      </div>
                    </div>
                  ) : (
                    <div className="mt-3 text-sm text-[var(--k-muted)]">
                      No snapshot yet. Click Generate.
                    </div>
                  )}
                </section>

                <section className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)]">
                  <div className="border-b border-[var(--k-border)] px-4 py-3 text-sm font-medium">
                    Top 3 (backup)
                  </div>
                  <div className="divide-y divide-[var(--k-border)]">
                    {top3.map((r, idx) => (
                      <div key={r.symbol} className="px-4 py-3 text-sm">
                        <div className="flex flex-wrap items-center justify-between gap-2">
                          <div className="flex items-center gap-2">
                            <div className="w-6 font-mono text-xs text-[var(--k-muted)]">
                              {idx + 1}
                            </div>
                            <button
                              type="button"
                              className="font-mono text-[var(--k-accent)] hover:underline"
                              onClick={() => onOpenStock?.(r.symbol)}
                            >
                              {r.ticker}
                            </button>
                            <div>{r.name}</div>
                          </div>
                          <div className="flex items-center gap-3 text-xs">
                            <div className="font-mono">
                              Score {Math.round(Number(r.score ?? 0))}
                            </div>
                            <div className="font-mono text-[var(--k-muted)]">
                              Prob {Math.round(Number(r.probProfit2d ?? 0))}%
                            </div>
                            <div className="font-mono text-[var(--k-muted)]">
                              Conf {String(r.confidence ?? '—')}
                            </div>
                          </div>
                        </div>
                        {(r.whyBullets ?? []).length ? (
                          <div className="mt-1 text-xs text-[var(--k-muted)]">
                            {(r.whyBullets ?? []).slice(0, 3).join(' · ')}
                          </div>
                        ) : null}
                      </div>
                    ))}
                    {!top3.length ? (
                      <div className="px-4 py-3 text-sm text-[var(--k-muted)]">
                        No snapshot yet. Click Generate (or run Dashboard Sync all first).
                      </div>
                    ) : null}
                  </div>
                </section>

                <section className="overflow-auto rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)]">
                  <details>
                    <summary className="cursor-pointer px-4 py-3 text-sm text-[var(--k-muted)]">
                      Full list (details)
                    </summary>
                    <table className="w-full border-collapse text-sm">
                      <thead className="bg-[var(--k-surface-2)] text-[var(--k-muted)]">
                        <tr className="text-left">
                          <th className="px-2 py-2">#</th>
                          <th className="px-2 py-2">Ticker</th>
                          <th className="px-2 py-2">Name</th>
                          <th className="px-2 py-2 text-right">Score</th>
                          <th className="px-2 py-2 text-right">Prob%</th>
                          <th className="px-2 py-2">Why</th>
                        </tr>
                      </thead>
                      <tbody>
                        {items.map((r, idx) => (
                          <tr key={r.symbol} className="border-t border-[var(--k-border)]">
                            <td className="px-2 py-2 font-mono text-[var(--k-muted)]">{idx + 1}</td>
                            <td className="px-2 py-2 font-mono">
                              <button
                                type="button"
                                className="text-[var(--k-accent)] hover:underline"
                                onClick={() => onOpenStock?.(r.symbol)}
                              >
                                {r.ticker}
                              </button>
                            </td>
                            <td className="px-2 py-2">{r.name}</td>
                            <td className="px-2 py-2 text-right font-mono">
                              {Math.round(Number(r.score ?? 0))}
                            </td>
                            <td className="px-2 py-2 text-right font-mono">
                              {Math.round(Number(r.probProfit2d ?? 0))}
                            </td>
                            <td className="px-2 py-2 text-xs text-[var(--k-muted)]">
                              {(r.whyBullets ?? []).slice(0, 2).join(' · ')}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </details>
                </section>
              </>
            );
          })()}
        </div>
      ) : (
        <div className="overflow-auto rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)]">
          <table className="w-full border-collapse text-sm">
            <thead className="bg-[var(--k-surface-2)] text-[var(--k-muted)]">
              <tr className="text-left">
                <th className="px-2 py-2">#</th>
                <th className="px-2 py-2">Theme</th>
                <th className="px-2 py-2 text-right">Score</th>
                <th className="px-2 py-2 text-right">Strength</th>
                <th className="px-2 py-2 text-right">LU</th>
                <th className="px-2 py-2">Top stocks</th>
              </tr>
            </thead>
            <tbody>
              {(dataMorning?.themes ?? []).map((t, idx) => (
                <tr key={`${t.kind}:${t.name}`} className="border-t border-[var(--k-border)]">
                  <td className="px-2 py-2 font-mono text-[var(--k-muted)]">{idx + 1}</td>
                  <td className="px-2 py-2">
                    <span className="font-mono text-[var(--k-muted)]">{t.kind}</span> {t.name}
                  </td>
                  <td className="px-2 py-2 text-right font-mono">
                    {Math.round(Number(t.score ?? 0))}
                  </td>
                  <td className="px-2 py-2 text-right font-mono">
                    {Number(t.todayStrength ?? 0).toFixed(1)}%
                  </td>
                  <td className="px-2 py-2 text-right font-mono">{Number(t.limitupCount ?? 0)}</td>
                  <td className="px-2 py-2 text-xs text-[var(--k-muted)]">
                    {(t.topTickers ?? []).slice(0, 3).map((x) => (
                      <button
                        key={x.symbol}
                        type="button"
                        className="mr-2 font-mono text-[var(--k-accent)] hover:underline"
                        onClick={() => onOpenStock?.(x.symbol)}
                        title={`${x.symbol}${typeof x.chgPct === 'number' ? ` · ${x.chgPct}%` : ''}`}
                      >
                        {x.ticker}
                      </button>
                    ))}
                    {!(t.topTickers ?? []).length ? '—' : null}
                  </td>
                </tr>
              ))}
              {!(dataMorning?.themes ?? []).length ? (
                <tr>
                  <td className="px-2 py-3 text-sm text-[var(--k-muted)]" colSpan={6}>
                    No radar yet. Click Generate.
                  </td>
                </tr>
              ) : null}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
