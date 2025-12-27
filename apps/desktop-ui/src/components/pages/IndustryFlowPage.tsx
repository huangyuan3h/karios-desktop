'use client';

import * as React from 'react';
import { RefreshCw } from 'lucide-react';

import { Button } from '@/components/ui/button';
import { QUANT_BASE_URL } from '@/lib/endpoints';
import { useChatStore } from '@/lib/chat/store';

type IndustryFundFlowPoint = {
  date: string;
  netInflow: number;
};

type IndustryFundFlowRow = {
  industryCode: string;
  industryName: string;
  netInflow: number;
  sum10d: number;
  series10d: IndustryFundFlowPoint[];
};

type IndustryFundFlowResp = {
  asOfDate: string;
  days: number;
  topN: number;
  top: IndustryFundFlowRow[];
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

function fmtCny(x: number): string {
  const v = Number.isFinite(x) ? x : 0;
  const abs = Math.abs(v);
  if (abs >= 1e8) return `${(v / 1e8).toFixed(2)}亿`;
  if (abs >= 1e4) return `${(v / 1e4).toFixed(1)}万`;
  return `${v.toFixed(0)}`;
}

function sumLastN(series: IndustryFundFlowPoint[], n: number): number {
  const xs = Array.isArray(series) ? series : [];
  const tail = xs.slice(-Math.max(1, n));
  let s = 0;
  for (const p of tail) s += Number.isFinite(p.netInflow) ? p.netInflow : 0;
  return s;
}

function Sparkline({ series }: { series: IndustryFundFlowPoint[] }) {
  const vals = series.map((p) => (Number.isFinite(p.netInflow) ? p.netInflow : 0));
  const maxAbs = Math.max(1, ...vals.map((v) => Math.abs(v)));
  return (
    <div className="flex h-6 items-end gap-[2px]">
      {series.map((p) => {
        const v = Number.isFinite(p.netInflow) ? p.netInflow : 0;
        const h = Math.max(1, Math.round((Math.abs(v) / maxAbs) * 24));
        const cls = v >= 0 ? 'bg-red-500/70' : 'bg-emerald-500/70';
        return (
          <div
            key={p.date}
            title={`${p.date}: ${fmtCny(v)}`}
            className={`w-[6px] rounded-sm ${cls}`}
            style={{ height: `${h}px` }}
          />
        );
      })}
    </div>
  );
}

function MiniTable({
  title,
  rows,
  valueLabel,
  onReference,
}: {
  title: string;
  rows: Array<{ industryCode: string; industryName: string; value: number; series10d?: IndustryFundFlowPoint[] }>;
  valueLabel: string;
  onReference: () => void;
}) {
  return (
    <div className="rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] p-4">
      <div className="mb-3 flex items-center justify-between gap-2">
        <div className="text-sm font-medium">{title}</div>
        <Button size="sm" variant="secondary" className="h-8 px-3 text-xs" onClick={onReference}>
          Reference
        </Button>
      </div>
      <div className="overflow-auto rounded-lg border border-[var(--k-border)]">
        <table className="w-full border-collapse text-xs">
          <thead className="bg-[var(--k-surface-2)] text-[var(--k-muted)]">
            <tr className="text-left">
              <th className="px-2 py-1">#</th>
              <th className="px-2 py-1">Industry</th>
              <th className="px-2 py-1">{valueLabel}</th>
              <th className="px-2 py-1">Trend</th>
            </tr>
          </thead>
          <tbody>
            {rows.length ? (
              rows.map((r, idx) => (
                <tr key={r.industryCode} className="border-t border-[var(--k-border)]">
                  <td className="px-2 py-1 font-mono">{idx + 1}</td>
                  <td className="px-2 py-1">{r.industryName}</td>
                  <td className="px-2 py-1 font-mono">{fmtCny(r.value)}</td>
                  <td className="px-2 py-1">
                    <Sparkline series={r.series10d ?? []} />
                  </td>
                </tr>
              ))
            ) : (
              <tr>
                <td colSpan={4} className="px-2 py-6 text-center text-[var(--k-muted)]">
                  No data
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}

export function IndustryFlowPage() {
  const { addReference } = useChatStore();
  const [resp, setResp] = React.useState<IndustryFundFlowResp | null>(null);
  const [busy, setBusy] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);
  const [topN, setTopN] = React.useState(30);
  const [detailsOpen, setDetailsOpen] = React.useState(false);

  const refresh = React.useCallback(async () => {
    setError(null);
    try {
      const r = await apiGetJson<IndustryFundFlowResp>(
        `/market/cn/industry-fund-flow?days=10&topN=${encodeURIComponent(String(topN))}`,
      );
      setResp(r);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
      setResp(null);
    }
  }, [topN]);

  React.useEffect(() => {
    void refresh();
  }, [refresh]);

  async function onSync(force: boolean) {
    setBusy(true);
    setError(null);
    try {
      await apiPostJson('/market/cn/industry-fund-flow/sync', { days: 10, topN: 10, force });
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="mx-auto w-full max-w-5xl p-6">
      <div className="mb-4 flex flex-wrap items-start justify-between gap-3">
        <div>
          <div className="text-lg font-semibold">CN Industry Fund Flow (10D)</div>
          <div className="mt-1 text-sm text-[var(--k-muted)]">
            EOD net inflow by industry. Cached in SQLite and reusable by Strategy.
          </div>
        </div>
        <div className="flex items-center gap-2">
          <Button variant="secondary" size="sm" disabled={busy} onClick={() => void refresh()} className="gap-2">
            <RefreshCw className="h-4 w-4" />
            Refresh
          </Button>
          <Button variant="secondary" size="sm" disabled={busy} onClick={() => void onSync(false)}>
            Sync latest
          </Button>
          <Button size="sm" disabled={busy} onClick={() => void onSync(true)}>
            Force sync
          </Button>
          {resp?.top?.length ? (
            <Button
              size="sm"
              variant="secondary"
              disabled={busy}
              onClick={() => setDetailsOpen((v) => !v)}
            >
              {detailsOpen ? 'Hide details' : 'Show details'}
            </Button>
          ) : null}
        </div>
      </div>

      {error ? (
        <div className="mb-4 rounded-lg border border-red-500/30 bg-red-500/10 px-3 py-2 text-sm text-red-600">
          {error}
        </div>
      ) : null}

      <div className="mb-3 flex items-center justify-between">
        <div className="text-xs text-[var(--k-muted)]">
          As of: {resp?.asOfDate ?? '—'} • days: {resp?.days ?? 10}
        </div>
        <div className="flex items-center gap-2 text-xs text-[var(--k-muted)]">
          <span>Top:</span>
          <select
            className="h-8 rounded-md border border-[var(--k-border)] bg-[var(--k-surface)] px-2 text-xs"
            value={topN}
            onChange={(e) => setTopN(Number(e.target.value))}
            disabled={busy}
          >
            {[10, 20, 30, 50, 100].map((n) => (
              <option key={n} value={n}>
                {n}
              </option>
            ))}
          </select>
        </div>
      </div>

      {resp?.top?.length ? (
        <div className="grid gap-4 md:grid-cols-2">
          {(() => {
            const rows = resp.top.slice(0, 100);
            const asOfDate = resp.asOfDate || new Date().toISOString().slice(0, 10);
            const baseDays = resp.days || 10;
            const top = 10;

            const in1d = rows
              .filter((r) => r.netInflow > 0)
              .sort((a, b) => b.netInflow - a.netInflow)
              .slice(0, top)
              .map((r) => ({ ...r, value: r.netInflow }));
            const out1d = rows
              .filter((r) => r.netInflow < 0)
              .sort((a, b) => a.netInflow - b.netInflow)
              .slice(0, top)
              .map((r) => ({ ...r, value: r.netInflow }));

            const in5d = rows
              .map((r) => ({ ...r, value: sumLastN(r.series10d ?? [], 5) }))
              .sort((a, b) => b.value - a.value)
              .slice(0, top);
            const in10d = rows
              .map((r) => ({ ...r, value: r.sum10d }))
              .sort((a, b) => b.value - a.value)
              .slice(0, top);

            return (
              <>
                <MiniTable
                  title="Top inflow (1D)"
                  valueLabel="Net inflow"
                  rows={in1d}
                  onReference={() =>
                    addReference({
                      kind: 'industryFundFlow',
                      refId: `${asOfDate}:${baseDays}:in1d:${top}`,
                      asOfDate,
                      days: baseDays,
                      topN: top,
                      metric: 'netInflow',
                      windowDays: 1,
                      direction: 'in',
                      title: 'Top inflow (1D)',
                      createdAt: new Date().toISOString(),
                    })
                  }
                />
                <MiniTable
                  title="Top outflow (1D)"
                  valueLabel="Net inflow"
                  rows={out1d}
                  onReference={() =>
                    addReference({
                      kind: 'industryFundFlow',
                      refId: `${asOfDate}:${baseDays}:out1d:${top}`,
                      asOfDate,
                      days: baseDays,
                      topN: top,
                      metric: 'netInflow',
                      windowDays: 1,
                      direction: 'out',
                      title: 'Top outflow (1D)',
                      createdAt: new Date().toISOString(),
                    })
                  }
                />
                <MiniTable
                  title="Top inflow (5D sum)"
                  valueLabel="Sum 5D"
                  rows={in5d}
                  onReference={() =>
                    addReference({
                      kind: 'industryFundFlow',
                      refId: `${asOfDate}:${baseDays}:in5d:${top}`,
                      asOfDate,
                      days: baseDays,
                      topN: top,
                      metric: 'sum',
                      windowDays: 5,
                      direction: 'in',
                      title: 'Top inflow (5D sum)',
                      createdAt: new Date().toISOString(),
                    })
                  }
                />
                <MiniTable
                  title="Top inflow (10D sum)"
                  valueLabel="Sum 10D"
                  rows={in10d}
                  onReference={() =>
                    addReference({
                      kind: 'industryFundFlow',
                      refId: `${asOfDate}:${baseDays}:in10d:${top}`,
                      asOfDate,
                      days: baseDays,
                      topN: top,
                      metric: 'sum',
                      windowDays: 10,
                      direction: 'in',
                      title: 'Top inflow (10D sum)',
                      createdAt: new Date().toISOString(),
                    })
                  }
                />
              </>
            );
          })()}
        </div>
      ) : (
        <div className="rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] p-6 text-center text-sm text-[var(--k-muted)]">
          No cached data. Click “Sync latest” after market close.
        </div>
      )}

      {detailsOpen && resp?.top?.length ? (
        <div className="mt-4 overflow-auto rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)]">
          <table className="w-full border-collapse text-xs">
            <thead className="bg-[var(--k-surface)] text-[var(--k-muted)]">
              <tr className="text-left">
                <th className="px-3 py-2">Rank</th>
                <th className="px-3 py-2">Industry</th>
                <th className="px-3 py-2">Net inflow</th>
                <th className="px-3 py-2">Sum 10D</th>
                <th className="px-3 py-2">Trend (10D)</th>
              </tr>
            </thead>
            <tbody>
              {resp.top.map((r, idx) => (
                <tr key={r.industryCode} className="border-t border-[var(--k-border)]">
                  <td className="px-3 py-2 font-mono">{idx + 1}</td>
                  <td className="px-3 py-2">{r.industryName}</td>
                  <td className="px-3 py-2 font-mono">{fmtCny(r.netInflow)}</td>
                  <td className="px-3 py-2 font-mono">{fmtCny(r.sum10d)}</td>
                  <td className="px-3 py-2">
                    <Sparkline series={r.series10d ?? []} />
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : null}
    </div>
  );
}


