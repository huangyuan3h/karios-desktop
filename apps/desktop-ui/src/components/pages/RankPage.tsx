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
  probBand: string;
  signals?: string[];
  breakdown?: Record<string, number>;
};

type RankSnapshot = {
  id: string;
  asOfDate: string;
  accountId: string;
  createdAt: string;
  universeVersion: string;
  riskMode?: string | null;
  items: RankItem[];
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

export function RankPage({ onOpenStock }: { onOpenStock?: (symbol: string) => void } = {}) {
  const { addReference } = useChatStore();
  const [accounts, setAccounts] = React.useState<BrokerAccount[]>([]);
  const [accountId, setAccountId] = React.useState<string>('');
  const [data, setData] = React.useState<RankSnapshot | null>(null);
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
        if (force) {
          const r = await apiPostJson<RankSnapshot>('/rank/cn/next2d/generate', {
            accountId,
            force: true,
            limit: 30,
            includeHoldings: true,
            universeVersion: 'v0',
          });
          setData(r);
        } else {
          const r = await apiGetJson<RankSnapshot>(
            `/rank/cn/next2d?accountId=${encodeURIComponent(accountId)}&limit=30&universeVersion=v0`,
          );
          setData(r);
        }
      } catch (e) {
        setError(e instanceof Error ? e.message : String(e));
      }
    },
    [accountId],
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
          <div className="text-lg font-semibold">CN Rank (next 1-2D)</div>
          <div className="mt-1 text-sm text-[var(--k-muted)]">
            Rule+factor scoring from cached market data (no auto-sync). Use Dashboard Sync all or Generate to refresh.
          </div>
        </div>
        <div className="flex flex-wrap items-center gap-2">
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
          <Button size="sm" className="gap-2" disabled={!accountId || busy} onClick={() => void onGenerate()}>
            {busy ? <RefreshCw className="h-4 w-4 animate-spin" /> : <Sparkles className="h-4 w-4" />}
            {busy ? 'Generating…' : 'Generate'}
          </Button>
          <Button
            size="sm"
            variant="secondary"
            disabled={!accountId}
            onClick={() => {
              addReference({
                kind: 'rankList',
                refId: `rankList:${accountId}:${Date.now()}`,
                accountId,
                asOfDate: String(data?.asOfDate ?? ''),
                limit: 30,
                createdAt: new Date().toISOString(),
              } satisfies ChatReference);
            }}
          >
            Reference
          </Button>
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
          asOfDate: {data?.asOfDate ?? '—'} • createdAt: {fmtDateTime(data?.createdAt)} • riskMode:{' '}
          {data?.riskMode ?? '—'}
        </div>
      </div>

      {error ? (
        <div className="mb-4 rounded-lg border border-red-500/30 bg-red-500/10 px-3 py-2 text-sm text-red-600">
          {error}
        </div>
      ) : null}

      <div className="overflow-auto rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)]">
        <table className="w-full border-collapse text-sm">
          <thead className="bg-[var(--k-surface-2)] text-[var(--k-muted)]">
            <tr className="text-left">
              <th className="px-2 py-2">#</th>
              <th className="px-2 py-2">Ticker</th>
              <th className="px-2 py-2">Name</th>
              <th className="px-2 py-2 text-right">Score</th>
              <th className="px-2 py-2">Prob</th>
              <th className="px-2 py-2">Signals</th>
            </tr>
          </thead>
          <tbody>
            {(data?.items ?? []).map((r, idx) => (
              <React.Fragment key={r.symbol}>
                <tr className="border-t border-[var(--k-border)]">
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
                  <td className="px-2 py-2 text-right font-mono">{Math.round(Number(r.score ?? 0))}</td>
                  <td className="px-2 py-2">{String(r.probBand ?? '')}</td>
                  <td className="px-2 py-2 text-xs text-[var(--k-muted)]">
                    {(Array.isArray(r.signals) ? r.signals : []).slice(0, 4).join(' · ')}
                  </td>
                </tr>
                <tr className="border-t border-[var(--k-border)] bg-[var(--k-surface)]">
                  <td className="px-2 py-2" colSpan={6}>
                    <details>
                      <summary className="cursor-pointer text-xs text-[var(--k-muted)]">Details</summary>
                      <div className="mt-2 grid gap-2 text-xs text-[var(--k-muted)] md:grid-cols-3">
                        <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface-2)] p-3">
                          <div className="text-xs font-medium text-[var(--k-text)]">Breakdown</div>
                          <pre className="mt-2 whitespace-pre-wrap break-words text-xs">
                            {JSON.stringify(r.breakdown ?? {}, null, 2)}
                          </pre>
                        </div>
                        <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface-2)] p-3">
                          <div className="text-xs font-medium text-[var(--k-text)]">Meta</div>
                          <div className="mt-2 space-y-1">
                            <div>symbol: {r.symbol}</div>
                            <div>sector: {String(r.sector ?? '—')}</div>
                          </div>
                        </div>
                      </div>
                    </details>
                  </td>
                </tr>
              </React.Fragment>
            ))}
            {!(data?.items ?? []).length ? (
              <tr>
                <td className="px-2 py-3 text-sm text-[var(--k-muted)]" colSpan={6}>
                  No snapshot yet. Click Generate (or run Dashboard Sync all first).
                </td>
              </tr>
            ) : null}
          </tbody>
        </table>
      </div>
    </div>
  );
}


