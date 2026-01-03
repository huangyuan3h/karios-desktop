/* eslint-disable @typescript-eslint/no-explicit-any */
'use client';

import * as React from 'react';
import { RefreshCw } from 'lucide-react';

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

type DashboardSummary = any;
type DashboardSyncResp = any;

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

function loadCardOrder(): string[] | null {
  try {
    const raw = window.localStorage.getItem('karios.dashboard.cardOrder.v0');
    if (!raw) return null;
    const arr = JSON.parse(raw) as unknown;
    return Array.isArray(arr) ? arr.filter((x) => typeof x === 'string') : null;
  } catch {
    return null;
  }
}

function saveCardOrder(ids: string[]) {
  try {
    window.localStorage.setItem('karios.dashboard.cardOrder.v0', JSON.stringify(ids));
  } catch {
    // ignore
  }
}

function fmtDateTime(x: string | null | undefined) {
  if (!x) return '—';
  const d = new Date(x);
  return Number.isNaN(d.getTime()) ? x : d.toLocaleString();
}

function parseNum(x: unknown): number | null {
  const s = String(x ?? '').trim();
  if (!s) return null;
  const n = Number(s.replaceAll(',', ''));
  return Number.isFinite(n) ? n : null;
}

function fmtAmountCn(x: unknown): string {
  const n = parseNum(x);
  if (n == null) return '—';
  const abs = Math.abs(n);
  if (abs >= 1e8) return `${(n / 1e8).toFixed(2)}亿`;
  if (abs >= 1e4) return `${(n / 1e4).toFixed(1)}万`;
  return `${n.toFixed(0)}`;
}

export function DashboardPage({
  onNavigate,
  onOpenStock,
}: {
  onNavigate?: (pageId: string) => void;
  onOpenStock?: (symbol: string) => void;
}) {
  const { addReference } = useChatStore();
  const [summary, setSummary] = React.useState<DashboardSummary | null>(null);
  const [syncResp, setSyncResp] = React.useState<DashboardSyncResp | null>(null);
  const [busy, setBusy] = React.useState(false);
  const [sentimentBusy, setSentimentBusy] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);
  const [accountId, setAccountId] = React.useState<string>('');
  const [editLayout, setEditLayout] = React.useState(false);

  const defaultCards = React.useMemo(
    () => [
      { id: 'account', title: 'Account overview' },
      { id: 'sentiment', title: 'Market sentiment' },
      { id: 'industry', title: 'Industry fund flow' },
      { id: 'leaders', title: 'Leaders' },
      { id: 'screeners', title: 'Screener sync' },
      { id: 'market', title: 'Market status' },
    ],
    [],
  );

  const [cardOrder, setCardOrder] = React.useState<string[]>(() => []);
  React.useEffect(() => {
    const loaded = loadCardOrder();
    const ids = defaultCards.map((c) => c.id);
    const next = loaded
      ? [...loaded.filter((x) => ids.includes(x)), ...ids.filter((x) => !loaded.includes(x))]
      : ids;
    setCardOrder(next);
  }, [defaultCards]);

  const refresh = React.useCallback(
    async (nextAccountId?: string) => {
      setError(null);
      try {
        const q = nextAccountId
          ? `?accountId=${encodeURIComponent(nextAccountId)}`
          : accountId
            ? `?accountId=${encodeURIComponent(accountId)}`
            : '';
        const s = await apiGetJson<DashboardSummary>(`/dashboard/summary${q}`);
        setSummary(s);
        const sel = String(s?.selectedAccountId ?? '');
        if (sel && sel !== accountId) setAccountId(sel);
      } catch (e) {
        setError(e instanceof Error ? e.message : String(e));
      }
    },
    [accountId],
  );

  React.useEffect(() => {
    void refresh();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  async function onSyncAll() {
    setBusy(true);
    setError(null);
    try {
      const r = await apiPostJson<DashboardSyncResp>('/dashboard/sync', { force: true });
      setSyncResp(r);
      await refresh(accountId);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }

  async function onSyncSentiment() {
    setSentimentBusy(true);
    setError(null);
    try {
      await apiPostJson('/market/cn/sentiment/sync', { force: true });
      await refresh(accountId);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setSentimentBusy(false);
    }
  }

  const cardsById = React.useMemo(
    () => Object.fromEntries(defaultCards.map((c) => [c.id, c])),
    [defaultCards],
  );
  const orderedCards = cardOrder.map((id) => cardsById[id]).filter(Boolean);

  function moveCard(id: string, dir: -1 | 1) {
    const idx = cardOrder.indexOf(id);
    if (idx < 0) return;
    const j = idx + dir;
    if (j < 0 || j >= cardOrder.length) return;
    const next = [...cardOrder];
    const tmp = next[idx];
    next[idx] = next[j];
    next[j] = tmp;
    setCardOrder(next);
    saveCardOrder(next);
  }

  return (
    <div className="mx-auto w-full max-w-6xl p-6">
      <div className="mb-4 flex items-start justify-between gap-3">
        <div>
          <div className="text-lg font-semibold">Dashboard</div>
          <div className="mt-1 text-sm text-[var(--k-muted)]">
            One-click sync + monitor key signals (account / flow / leaders / holdings).
          </div>
        </div>
        <div className="flex flex-wrap items-center gap-2">
          <Button
            variant="secondary"
            size="sm"
            className="gap-2"
            disabled={busy}
            onClick={() => void refresh()}
          >
            <RefreshCw className="h-4 w-4" />
            Refresh
          </Button>
          <Button size="sm" className="gap-2" disabled={busy} onClick={() => void onSyncAll()}>
            {busy ? (
              <RefreshCw className="h-4 w-4 animate-spin" />
            ) : (
              <RefreshCw className="h-4 w-4" />
            )}
            {busy ? 'Syncing…' : 'Sync all (force)'}
          </Button>
          <Button size="sm" variant="secondary" onClick={() => setEditLayout((v) => !v)}>
            {editLayout ? 'Done' : 'Edit layout'}
          </Button>
        </div>
      </div>

      <div className="mb-4 flex flex-wrap items-center gap-2 text-xs text-[var(--k-muted)]">
        <Button
          size="sm"
          variant="secondary"
          className="h-8 px-3 text-xs"
          onClick={() => onNavigate?.('broker')}
        >
          Broker
        </Button>
        <Button
          size="sm"
          variant="secondary"
          className="h-8 px-3 text-xs"
          onClick={() => onNavigate?.('market')}
        >
          Market
        </Button>
        <Button
          size="sm"
          variant="secondary"
          className="h-8 px-3 text-xs"
          onClick={() => onNavigate?.('industryFlow')}
        >
          Industry Flow
        </Button>
        <Button
          size="sm"
          variant="secondary"
          className="h-8 px-3 text-xs"
          onClick={() => onNavigate?.('leaders')}
        >
          Leaders
        </Button>
        <Button
          size="sm"
          variant="secondary"
          className="h-8 px-3 text-xs"
          onClick={() => onNavigate?.('strategy')}
        >
          Strategy
        </Button>
        <Button
          size="sm"
          variant="secondary"
          className="h-8 px-3 text-xs"
          onClick={() => onNavigate?.('screener')}
        >
          Screener
        </Button>
        <div className="ml-auto">
          asOfDate: <span className="font-mono">{summary?.asOfDate ?? '—'}</span>
        </div>
      </div>

      {error ? (
        <div className="mb-4 rounded-lg border border-red-500/30 bg-red-500/10 px-3 py-2 text-sm text-red-600">
          {error}
        </div>
      ) : null}

      {syncResp ? (
        <div className="mb-4 rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] p-4">
          <div className="mb-2 text-sm font-medium">Last sync result</div>
          <div className="text-xs text-[var(--k-muted)]">
            started: {fmtDateTime(syncResp.startedAt)} • finished:{' '}
            {fmtDateTime(syncResp.finishedAt)} • ok: {String(Boolean(syncResp.ok))}
          </div>
          <div className="mt-3 overflow-auto rounded-lg border border-[var(--k-border)]">
            <table className="w-full border-collapse text-xs">
              <thead className="bg-[var(--k-surface-2)] text-[var(--k-muted)]">
                <tr className="text-left">
                  <th className="px-3 py-2">Step</th>
                  <th className="px-3 py-2">OK</th>
                  <th className="px-3 py-2">Duration</th>
                  <th className="px-3 py-2">Message</th>
                </tr>
              </thead>
              <tbody>
                {(syncResp.steps ?? []).map((s: any) => (
                  <tr key={String(s.name)} className="border-t border-[var(--k-border)]">
                    <td className="px-3 py-2 font-mono">{String(s.name)}</td>
                    <td className="px-3 py-2">{String(Boolean(s.ok))}</td>
                    <td className="px-3 py-2 font-mono">{String(s.durationMs ?? 0)}ms</td>
                    <td className="px-3 py-2 text-[var(--k-muted)]">{String(s.message ?? '')}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {syncResp.screener?.failed?.length || syncResp.screener?.missing?.length ? (
            <div className="mt-3 text-xs text-red-600">
              Screener issues: failed={syncResp.screener?.failed?.length ?? 0} missing=
              {syncResp.screener?.missing?.length ?? 0}
            </div>
          ) : null}
        </div>
      ) : null}

      {(() => {
        const weightOf = (id: string) => {
          if (id === 'sentiment') return 3;
          if (id === 'industry') return 6;
          if (id === 'account') return 4;
          if (id === 'leaders') return 3;
          if (id === 'screeners') return 2;
          if (id === 'market') return 2;
          return 2;
        };
        const left: any[] = [];
        const right: any[] = [];
        let wl = 0;
        let wr = 0;
        for (const c of orderedCards) {
          const id = String(c.id);
          const w = weightOf(id);
          if (wl <= wr) {
            left.push(c);
            wl += w;
          } else {
            right.push(c);
            wr += w;
          }
        }

        const renderCard = (c: any) => {
          const id = String(c.id);
          return (
            <section
              key={id}
              className="rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] p-4"
            >
              <div className="mb-3 flex items-center justify-between gap-2">
                <div className="text-sm font-medium">{c.title}</div>
                {editLayout ? (
                  <div className="flex items-center gap-1">
                    <Button
                      size="sm"
                      variant="secondary"
                      className="h-7 px-2 text-xs"
                      onClick={() => moveCard(id, -1)}
                    >
                      ↑
                    </Button>
                    <Button
                      size="sm"
                      variant="secondary"
                      className="h-7 px-2 text-xs"
                      onClick={() => moveCard(id, 1)}
                    >
                      ↓
                    </Button>
                  </div>
                ) : null}
              </div>

              {id === 'account' ? (
                <div className="text-sm">
                  <div className="mb-2 flex flex-wrap items-center justify-between gap-2">
                    <div className="text-xs text-[var(--k-muted)]">
                      updatedAt: {fmtDateTime(summary?.accountState?.updatedAt)}
                    </div>
                    <Select
                      value={accountId}
                      onValueChange={(v) => {
                        setAccountId(v);
                        void refresh(v);
                      }}
                    >
                      <SelectTrigger className="h-8 w-[240px]">
                        <SelectValue placeholder="Select account" />
                      </SelectTrigger>
                      <SelectContent>
                        {(summary?.accounts ?? []).map((a: any) => (
                          <SelectItem key={a.id} value={a.id}>
                            {a.title}
                            {a.accountMasked ? ` (${a.accountMasked})` : ''}
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  </div>

                  <div className="mt-2 grid grid-cols-2 gap-2 text-sm">
                    <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface-2)] p-3">
                      <div className="text-xs text-[var(--k-muted)]">Total assets</div>
                      <div className="mt-1 font-mono">
                        {fmtAmountCn(summary?.accountState?.totalAssets)}
                      </div>
                    </div>
                    <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface-2)] p-3">
                      <div className="text-xs text-[var(--k-muted)]">Cash available</div>
                      <div className="mt-1 font-mono">
                        {fmtAmountCn(summary?.accountState?.cashAvailable)}
                      </div>
                    </div>
                  </div>
                  <div className="mt-3">
                    <div className="mb-2 text-xs text-[var(--k-muted)]">Holdings</div>
                    <div className="overflow-auto rounded-lg border border-[var(--k-border)]">
                      <table className="w-full border-collapse text-xs">
                        <thead className="bg-[var(--k-surface-2)] text-[var(--k-muted)]">
                          <tr className="text-left">
                            <th className="px-2 py-2">Ticker</th>
                            <th className="px-2 py-2">Name</th>
                            <th className="px-2 py-2 text-right">Price</th>
                            <th className="px-2 py-2 text-right">Weight</th>
                            <th className="px-2 py-2 text-right">PnL</th>
                          </tr>
                        </thead>
                        <tbody>
                          {(summary?.holdings ?? []).slice(0, 12).map((h: any, idx: number) => {
                            const ticker = String(h.ticker ?? '').trim();
                            const sym = String(h.symbol ?? '').trim();
                            const inferredSymbol = sym
                              ? sym
                              : `${ticker.length === 4 || ticker.length === 5 ? 'HK' : 'CN'}:${ticker}`;
                            const weight = Number.isFinite(h.weightPct)
                              ? `${Number(h.weightPct).toFixed(1)}%`
                              : '—';
                            return (
                              <tr key={idx} className="border-t border-[var(--k-border)]">
                                <td className="px-2 py-2 font-mono">
                                  <button
                                    type="button"
                                    className="text-[var(--k-accent)] hover:underline"
                                    onClick={() => onOpenStock?.(inferredSymbol)}
                                    title="Open stock detail"
                                  >
                                    {ticker}
                                  </button>
                                </td>
                                <td className="px-2 py-2">{String(h.name ?? '')}</td>
                                <td className="px-2 py-2 text-right font-mono">
                                  {Number.isFinite(h.price) ? Number(h.price).toFixed(2) : '—'}
                                </td>
                                <td className="px-2 py-2 text-right font-mono">{weight}</td>
                                <td className="px-2 py-2 text-right font-mono">
                                  {fmtAmountCn(h.pnlAmount)}
                                </td>
                              </tr>
                            );
                          })}
                          {!(summary?.holdings ?? []).length ? (
                            <tr>
                              <td className="px-2 py-3 text-sm text-[var(--k-muted)]" colSpan={5}>
                                No holdings cached yet. Upload broker screenshots in Broker tab.
                              </td>
                            </tr>
                          ) : null}
                        </tbody>
                      </table>
                    </div>
                  </div>
                  <div className="mt-3 flex items-center gap-2">
                    <Button size="sm" variant="secondary" onClick={() => onNavigate?.('broker')}>
                      Open Broker
                    </Button>
                    <Button
                      size="sm"
                      variant="secondary"
                      disabled={!summary?.selectedAccountId}
                      onClick={() => {
                        if (!summary?.selectedAccountId) return;
                        const acc = (summary?.accounts ?? []).find(
                          (a: any) => a.id === summary.selectedAccountId,
                        );
                        addReference({
                          kind: 'brokerState',
                          refId: summary.selectedAccountId,
                          broker: 'pingan',
                          accountId: summary.selectedAccountId,
                          accountTitle: String(acc?.title ?? 'Account'),
                          capturedAt: new Date().toISOString(),
                        } as any);
                      }}
                    >
                      Reference
                    </Button>
                  </div>
                </div>
              ) : id === 'sentiment' ? (
                <div>
                  {(() => {
                    const ms = summary?.marketSentiment ?? {};
                    const items: any[] = Array.isArray(ms.items) ? ms.items : [];
                    const latest = items.length ? items[items.length - 1] : null;
                    const risk = String(latest?.riskMode ?? '—');
                    const premium = Number.isFinite(latest?.yesterdayLimitUpPremium)
                      ? `${Number(latest.yesterdayLimitUpPremium).toFixed(2)}%`
                      : '—';
                    const failed = Number.isFinite(latest?.failedLimitUpRate)
                      ? `${Number(latest.failedLimitUpRate).toFixed(1)}%`
                      : '—';
                    const ratio = Number.isFinite(latest?.upDownRatio)
                      ? Number(latest.upDownRatio).toFixed(2)
                      : '—';
                    const up = Number(latest?.upCount ?? 0);
                    const down = Number(latest?.downCount ?? 0);
                    const flat = Number(latest?.flatCount ?? 0);
                    const badge =
                      risk === 'no_new_positions'
                        ? 'border-red-500/30 bg-red-500/10 text-red-600'
                        : risk === 'caution'
                          ? 'border-yellow-500/30 bg-yellow-500/10 text-yellow-700'
                          : 'border-[var(--k-border)] bg-[var(--k-surface-2)] text-[var(--k-muted)]';
                    return (
                      <>
                        <div className="mb-2 flex flex-wrap items-center gap-2">
                          <div className={`rounded-md border px-2 py-1 text-xs ${badge}`}>
                            risk: {risk}
                          </div>
                          {Array.isArray(latest?.rules) && latest.rules.length ? (
                            <div className="text-xs text-[var(--k-muted)]">
                              {latest.rules
                                .slice(0, 2)
                                .map((x: any) => String(x))
                                .join(' • ')}
                            </div>
                          ) : null}
                        </div>

                        <div className="grid grid-cols-2 gap-2 text-sm">
                          <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface-2)] p-3">
                            <div className="text-xs text-[var(--k-muted)]">Up/Down/Flat</div>
                            <div className="mt-1 font-mono">
                              {up}/{down}/{flat}
                            </div>
                            <div className="mt-1 text-xs text-[var(--k-muted)]">ratio: {ratio}</div>
                          </div>
                          <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface-2)] p-3">
                            <div className="text-xs text-[var(--k-muted)]">Sentiment</div>
                            <div className="mt-1 text-xs text-[var(--k-muted)]">
                              yesterday limit-up premium
                            </div>
                            <div className="mt-0.5 font-mono">{premium}</div>
                            <div className="mt-1 text-xs text-[var(--k-muted)]">
                              failed limit-up rate
                            </div>
                            <div className="mt-0.5 font-mono">{failed}</div>
                          </div>
                        </div>

                        <div className="mt-3">
                          <div className="mb-2 text-xs text-[var(--k-muted)]">Last 5 days</div>
                          <div className="overflow-auto rounded-lg border border-[var(--k-border)]">
                            <table className="w-full border-collapse text-xs">
                              <thead className="bg-[var(--k-surface-2)] text-[var(--k-muted)]">
                                <tr className="text-left">
                                  <th className="px-2 py-2 font-mono">date</th>
                                  <th className="px-2 py-2 text-right">ratio</th>
                                  <th className="px-2 py-2 text-right">premium%</th>
                                  <th className="px-2 py-2 text-right">failed%</th>
                                  <th className="px-2 py-2">risk</th>
                                </tr>
                              </thead>
                              <tbody>
                                {(items || []).slice(-5).map((it: any, idx: number) => (
                                  <tr key={idx} className="border-t border-[var(--k-border)]">
                                    <td className="px-2 py-2 font-mono">{String(it.date ?? '')}</td>
                                    <td className="px-2 py-2 text-right font-mono">
                                      {Number.isFinite(it.upDownRatio)
                                        ? Number(it.upDownRatio).toFixed(2)
                                        : '—'}
                                    </td>
                                    <td className="px-2 py-2 text-right font-mono">
                                      {Number.isFinite(it.yesterdayLimitUpPremium)
                                        ? `${Number(it.yesterdayLimitUpPremium).toFixed(2)}%`
                                        : '—'}
                                    </td>
                                    <td className="px-2 py-2 text-right font-mono">
                                      {Number.isFinite(it.failedLimitUpRate)
                                        ? `${Number(it.failedLimitUpRate).toFixed(1)}%`
                                        : '—'}
                                    </td>
                                    <td className="px-2 py-2">{String(it.riskMode ?? '')}</td>
                                  </tr>
                                ))}
                                {!items.length ? (
                                  <tr>
                                    <td
                                      className="px-2 py-3 text-sm text-[var(--k-muted)]"
                                      colSpan={5}
                                    >
                                      No sentiment cached yet. Click “Sync all (force)”.
                                    </td>
                                  </tr>
                                ) : null}
                              </tbody>
                            </table>
                          </div>
                        </div>

                        <div className="mt-3 flex items-center gap-2">
                          <Button
                            size="sm"
                            variant="secondary"
                            disabled={sentimentBusy}
                            onClick={() => void onSyncSentiment()}
                          >
                            {sentimentBusy ? (
                              <RefreshCw className="mr-2 h-4 w-4 animate-spin" />
                            ) : (
                              <RefreshCw className="mr-2 h-4 w-4" />
                            )}
                            Sync sentiment
                          </Button>
                          <Button
                            size="sm"
                            variant="secondary"
                            onClick={() => {
                              const asOfDate = String(ms.asOfDate ?? summary?.asOfDate ?? '');
                              addReference({
                                kind: 'marketSentiment',
                                refId: `${asOfDate}:5`,
                                asOfDate,
                                days: 5,
                                title: 'CN market sentiment (breadth & limit-up)',
                                createdAt: new Date().toISOString(),
                              } as any);
                            }}
                          >
                            Reference
                          </Button>
                        </div>
                      </>
                    );
                  })()}
                </div>
              ) : id === 'industry' ? (
                <div>
                  <div className="mb-2 text-xs text-[var(--k-muted)]">
                    Top5×Date hotspots (names only)
                  </div>
                  {(() => {
                    const datesAll: string[] = Array.isArray(summary?.industryFundFlow?.dates)
                      ? summary.industryFundFlow.dates
                      : [];
                    const rawShownDates = datesAll.slice(-5);
                    const topByDateArr: any[] = Array.isArray(summary?.industryFundFlow?.topByDate)
                      ? summary.industryFundFlow.topByDate
                      : [];
                    const map: Record<string, string[]> = {};
                    for (const it of topByDateArr) {
                      const d = String(it?.date ?? '');
                      const top = Array.isArray(it?.top)
                        ? it.top.map((x: any) => String(x ?? ''))
                        : [];
                      if (d) map[d] = top;
                    }
                    const dedupedDates: string[] = [];
                    let prevSig = '';
                    let collapsed = 0;
                    for (const d of rawShownDates) {
                      const sig = (map[d] || []).slice(0, 5).join('|');
                      if (sig && sig === prevSig) {
                        collapsed += 1;
                        continue;
                      }
                      dedupedDates.push(d);
                      prevSig = sig;
                    }

                    return (
                      <>
                        {collapsed ? (
                          <div className="mb-2 text-xs text-[var(--k-muted)]">
                            collapsed {collapsed} duplicate non-trading snapshot
                            {collapsed > 1 ? 's' : ''}
                          </div>
                        ) : null}
                        <div className="overflow-auto rounded-lg border border-[var(--k-border)]">
                          <table className="w-full border-collapse text-xs">
                            <thead className="bg-[var(--k-surface-2)] text-[var(--k-muted)]">
                              <tr className="text-left">
                                <th className="px-2 py-2">#</th>
                                {dedupedDates.map((d: string) => (
                                  <th key={d} className="px-2 py-2 font-mono">
                                    {String(d).slice(5)}
                                  </th>
                                ))}
                              </tr>
                            </thead>
                            <tbody>
                              {Array.from({ length: 5 }).map((_, i) => (
                                <tr key={i} className="border-t border-[var(--k-border)]">
                                  <td className="px-2 py-2 font-mono">{i + 1}</td>
                                  {dedupedDates.map((d: string, j: number) => (
                                    <td key={j} className="px-2 py-2">
                                      {String((map[d] || [])[i] ?? '')}
                                    </td>
                                  ))}
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                        {(() => {
                          const flow5d: any = (summary?.industryFundFlow as any)?.flow5d ?? null;
                          const flowDates: string[] = Array.isArray(flow5d?.dates)
                            ? flow5d.dates
                            : [];
                          const cols: string[] = flowDates.length
                            ? flowDates.slice(-5)
                            : dedupedDates;
                          const topRows: any[] = Array.isArray(flow5d?.top) ? flow5d.top : [];
                          if (!topRows.length || !cols.length) return null;
                          const colDates = cols;
                          return (
                            <div className="mt-4">
                              <div className="mb-2 text-xs text-[var(--k-muted)]">
                                5D net inflow (Top by 5D sum)
                              </div>
                              <div className="overflow-auto rounded-lg border border-[var(--k-border)]">
                                <table className="w-full border-collapse text-xs">
                                  <thead className="bg-[var(--k-surface-2)] text-[var(--k-muted)]">
                                    <tr className="text-left">
                                      <th className="px-2 py-2">Industry</th>
                                      <th className="px-2 py-2 text-right">Sum(5D)</th>
                                      {colDates.map((d: string) => (
                                        <th key={d} className="px-2 py-2 text-right font-mono">
                                          {String(d).slice(5)}
                                        </th>
                                      ))}
                                    </tr>
                                  </thead>
                                  <tbody>
                                    {topRows.slice(0, 10).map((r: any, idx: number) => {
                                      const seriesArr: any[] = Array.isArray(r?.series)
                                        ? r.series
                                        : [];
                                      const map: Record<string, number> = {};
                                      for (const p of seriesArr) {
                                        const dd = String(p?.date ?? '');
                                        const nv = Number(p?.netInflow ?? 0);
                                        if (dd) map[dd] = Number.isFinite(nv) ? nv : 0;
                                      }
                                      return (
                                        <tr
                                          key={`${String(r?.industryCode ?? idx)}`}
                                          className="border-t border-[var(--k-border)]"
                                        >
                                          <td className="px-2 py-2">
                                            {String(r?.industryName ?? '')}
                                          </td>
                                          <td className="px-2 py-2 text-right font-mono">
                                            {fmtAmountCn(r?.sum5d)}
                                          </td>
                                          {colDates.map((d: string) => (
                                            <td key={d} className="px-2 py-2 text-right font-mono">
                                              {fmtAmountCn(map[d] ?? 0)}
                                            </td>
                                          ))}
                                        </tr>
                                      );
                                    })}
                                  </tbody>
                                </table>
                              </div>
                            </div>
                          );
                        })()}
                        <div className="mt-3 flex items-center gap-2">
                          <Button
                            size="sm"
                            variant="secondary"
                            onClick={() => onNavigate?.('industryFlow')}
                          >
                            Open Industry Flow
                          </Button>
                          <Button
                            size="sm"
                            variant="secondary"
                            onClick={() => {
                              const asOfDate = String(
                                summary?.industryFundFlow?.asOfDate ?? summary?.asOfDate ?? '',
                              );
                              addReference({
                                kind: 'industryFundFlow',
                                refId: `${asOfDate}:5:10`,
                                asOfDate,
                                days: 5,
                                topN: 10,
                                view: 'dailyTopByDate',
                                title: 'CN industry fund flow (Top by date)',
                                createdAt: new Date().toISOString(),
                              } as any);
                            }}
                          >
                            Reference
                          </Button>
                        </div>
                      </>
                    );
                  })()}
                </div>
              ) : id === 'leaders' ? (
                <div>
                  <div className="mb-2 text-xs text-[var(--k-muted)]">
                    latestDate: {summary?.leaders?.latestDate ?? '—'}
                  </div>
                  <div className="space-y-2">
                    {(summary?.leaders?.latest ?? []).slice(0, 2).map((r: any) => (
                      <div
                        key={String(r.symbol)}
                        className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface-2)] p-3"
                      >
                        <div className="flex items-center justify-between gap-2">
                          <button
                            type="button"
                            className="font-mono text-sm text-[var(--k-accent)] hover:underline"
                            onClick={() => onOpenStock?.(String(r.symbol))}
                          >
                            {String(r.ticker ?? r.symbol)}
                          </button>
                          <div className="text-xs text-[var(--k-muted)]">
                            score: {String(r.score ?? '—')}
                          </div>
                        </div>
                        <div className="mt-1 text-xs text-[var(--k-muted)]">
                          {String(r.name ?? '')}
                        </div>
                        {Array.isArray(r.whyBullets) && r.whyBullets.length ? (
                          <ul className="mt-2 list-disc pl-4 text-xs text-[var(--k-muted)]">
                            {r.whyBullets.slice(0, 2).map((x: any, idx: number) => (
                              <li key={idx}>{String(x)}</li>
                            ))}
                          </ul>
                        ) : (
                          <div className="mt-2 text-xs text-[var(--k-muted)]">
                            {String(r.reason ?? '')}
                          </div>
                        )}
                        <div className="mt-2 text-xs text-[var(--k-muted)]">
                          buy={String(r.buyZone?.low ?? '—')}-{String(r.buyZone?.high ?? '—')} •
                          target=
                          {String(r.targetPrice?.primary ?? '—')} • dur=
                          {String(r.expectedDurationDays ?? '—')}d • p=
                          {Number.isFinite(Number(r.probability))
                            ? `${Math.max(1, Math.min(5, Math.round(Number(r.probability)))) * 20}%`
                            : '—'}
                        </div>
                        <div className="mt-2 text-xs text-[var(--k-muted)]">
                          close={String(r.current?.close ?? '—')} vol=
                          {String(r.current?.volume ?? '—')}
                        </div>
                      </div>
                    ))}
                    {!(summary?.leaders?.latest ?? []).length ? (
                      <div className="text-sm text-[var(--k-muted)]">
                        No leaders yet. Generate in Leaders tab.
                      </div>
                    ) : null}
                  </div>
                  <div className="mt-3 flex items-center gap-2">
                    <Button size="sm" variant="secondary" onClick={() => onNavigate?.('leaders')}>
                      Open Leaders
                    </Button>
                    <Button
                      size="sm"
                      variant="secondary"
                      onClick={() => {
                        addReference({
                          kind: 'leaderStocks',
                          refId: `leaderStocks:10:${Date.now()}`,
                          days: 10,
                          createdAt: new Date().toISOString(),
                        } as any);
                      }}
                    >
                      Reference
                    </Button>
                  </div>
                </div>
              ) : id === 'screeners' ? (
                <div>
                  <div className="mb-2 text-xs text-[var(--k-muted)]">
                    Enabled screeners (no content). Missing/rowCount=0 will be highlighted.
                  </div>
                  <div className="overflow-auto rounded-lg border border-[var(--k-border)]">
                    <table className="w-full border-collapse text-xs">
                      <thead className="bg-[var(--k-surface-2)] text-[var(--k-muted)]">
                        <tr className="text-left">
                          <th className="px-2 py-2">Name</th>
                          <th className="px-2 py-2">capturedAt</th>
                          <th className="px-2 py-2 text-right">rows</th>
                          <th className="px-2 py-2 text-right">filters</th>
                        </tr>
                      </thead>
                      <tbody>
                        {(summary?.screeners ?? []).map((s: any) => {
                          const bad = !s.capturedAt || Number(s.rowCount ?? 0) <= 0;
                          return (
                            <tr key={String(s.id)} className="border-t border-[var(--k-border)]">
                              <td className="px-2 py-2">{String(s.name ?? s.id)}</td>
                              <td className={`px-2 py-2 font-mono ${bad ? 'text-red-600' : ''}`}>
                                {String(s.capturedAt ?? '—')}
                              </td>
                              <td
                                className={`px-2 py-2 text-right font-mono ${bad ? 'text-red-600' : ''}`}
                              >
                                {String(s.rowCount ?? 0)}
                              </td>
                              <td className="px-2 py-2 text-right font-mono">
                                {String(s.filtersCount ?? 0)}
                              </td>
                            </tr>
                          );
                        })}
                        {!(summary?.screeners ?? []).length ? (
                          <tr>
                            <td className="px-2 py-3 text-sm text-[var(--k-muted)]" colSpan={4}>
                              No enabled screeners.
                            </td>
                          </tr>
                        ) : null}
                      </tbody>
                    </table>
                  </div>
                  <div className="mt-3 flex items-center gap-2">
                    <Button size="sm" variant="secondary" onClick={() => onNavigate?.('screener')}>
                      Open Screener
                    </Button>
                  </div>
                </div>
              ) : (
                <div>
                  <div className="text-sm text-[var(--k-muted)]">
                    stocks: {String(summary?.marketStatus?.stocks ?? '—')}
                  </div>
                  <div className="mt-1 text-xs text-[var(--k-muted)]">
                    lastSyncAt: {fmtDateTime(summary?.marketStatus?.lastSyncAt)}
                  </div>
                  <div className="mt-3 flex items-center gap-2">
                    <Button size="sm" variant="secondary" onClick={() => onNavigate?.('market')}>
                      Open Market
                    </Button>
                  </div>
                </div>
              )}
            </section>
          );
        };

        return (
          <>
            <div className="space-y-4 lg:hidden">{orderedCards.map(renderCard)}</div>
            <div className="hidden lg:grid lg:grid-cols-2 lg:gap-4">
              <div className="space-y-4">{left.map(renderCard)}</div>
              <div className="space-y-4">{right.map(renderCard)}</div>
            </div>
          </>
        );
      })()}

      {editLayout ? (
        <div className="mt-4 text-xs text-[var(--k-muted)]">
          Layout config is saved locally. Drag-and-drop UI can be added later; for now use ↑/↓.
        </div>
      ) : null}
    </div>
  );
}
