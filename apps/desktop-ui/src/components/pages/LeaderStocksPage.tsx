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
  whyBullets?: string[];
  expectedDurationDays?: number | null;
  buyZone?: Record<string, unknown>;
  triggers?: Array<Record<string, unknown>>;
  invalidation?: string | null;
  targetPrice?: Record<string, unknown>;
  probability?: number | null;
  risks?: string[];
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

function fmtPerf(r: LeaderPick) {
  const entry = Number.isFinite(r.entryPrice as number) ? (r.entryPrice as number).toFixed(2) : null;
  const now = Number.isFinite(r.nowClose as number) ? (r.nowClose as number).toFixed(2) : null;
  const pct = Number.isFinite(r.pctSinceEntry as number)
    ? `${(((r.pctSinceEntry as number) || 0) * 100).toFixed(2)}%`
    : null;
  if (entry && now && pct) return `${entry} → ${now} (${pct})`;
  if (now && pct) return `${now} (${pct})`;
  if (now) return now;
  return '—';
}

function fmtBuyZoneText(r: LeaderPick) {
  const bz = r.buyZone ?? {};
  const low = (bz as any).low;
  const high = (bz as any).high;
  const note = (bz as any).note;
  if (low == null || high == null) return '—';
  return `${String(low)} - ${String(high)}${note ? ` (${String(note)})` : ''}`;
}

function fmtTargetText(r: LeaderPick) {
  const tp = r.targetPrice ?? {};
  const primary = (tp as any).primary;
  const stretch = (tp as any).stretch;
  const note = (tp as any).note;
  if (primary == null && stretch == null) return '—';
  const s = stretch != null ? `${String(primary)} / ${String(stretch)}` : String(primary);
  return `${s}${note ? ` (${String(note)})` : ''}`;
}

function fmtProbability(p: number | null | undefined) {
  const n = Number(p);
  if (!Number.isFinite(n) || n <= 0) return '—';
  const clamped = Math.max(1, Math.min(5, Math.round(n)));
  const pct = clamped * 20;
  const label =
    clamped === 1 ? '低' : clamped === 2 ? '偏低' : clamped === 3 ? '中等' : clamped === 4 ? '偏高' : '很高';
  return `${pct}%（${label}）`;
}

function fmtTriggerText(t: Record<string, unknown>) {
  const kind = String((t as any).kind ?? '');
  const label = kind === 'breakout' ? 'Breakout' : kind === 'pullback' ? 'Pullback' : kind || 'Trigger';
  const cond = String((t as any).condition ?? '').trim();
  const val = (t as any).value;
  const tail = val != null && String(val).trim() ? ` @ ${String(val)}` : '';
  return `${label}: ${cond}${tail}`.trim();
}

function fmtPlanLine(r: LeaderPick) {
  const bz = r.buyZone ?? {};
  const tp = r.targetPrice ?? {};
  const bzLow = (bz as any).low;
  const bzHigh = (bz as any).high;
  const tpPrimary = (tp as any).primary;
  const parts: string[] = [];
  if (bzLow != null && bzHigh != null) parts.push(`Buy:${String(bzLow)}-${String(bzHigh)}`);
  if (tpPrimary != null) parts.push(`Target:${String(tpPrimary)}`);
  if (Number.isFinite(r.expectedDurationDays as number)) parts.push(`Dur:${String(r.expectedDurationDays)}d`);
  if (Number.isFinite(r.probability as number)) parts.push(`P:${fmtProbability(r.probability)}`);
  return parts.length ? parts.join(' • ') : '';
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

export function LeaderStocksPage({ onOpenStock }: { onOpenStock?: (symbol: string) => void } = {}) {
  const { addReference } = useChatStore();
  const [data, setData] = React.useState<LeaderListResponse | null>(null);
  const [busy, setBusy] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);
  const [debugOpen, setDebugOpen] = React.useState(false);
  const [lastDebug, setLastDebug] = React.useState<unknown>(null);

  const refresh = React.useCallback(async () => {
    setError(null);
    try {
      // Force refresh so historical leaders' perf uses the latest market data.
      const r = await apiGetJson<LeaderListResponse>('/leader?days=10&force=true');
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
  // Consolidated view: dedupe by symbol, keep the latest record (largest date).
  const consolidated = React.useMemo(() => {
    const m = new Map<string, LeaderPick>();
    for (const it of leaders) {
      const key = String(it.symbol || it.ticker || '').trim();
      if (!key) continue;
      const prev = m.get(key);
      if (!prev || String(it.date || '').localeCompare(String(prev.date || '')) > 0) {
        m.set(key, it);
      }
    }
    return Array.from(m.values()).sort((a, b) => {
      const sa = Number.isFinite(a.score as number) ? (a.score as number) : -1;
      const sb = Number.isFinite(b.score as number) ? (b.score as number) : -1;
      if (sb !== sa) return sb - sa;
      return String(b.date || '').localeCompare(String(a.date || ''));
    });
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
          <div className="text-sm font-medium">
            Leaders (deduped, keep latest per symbol • last {data?.days ?? 10} trading days)
          </div>
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

        {consolidated.length ? (
          <div className="overflow-x-auto">
            <table className="w-full border-collapse text-sm">
              <thead className="bg-[var(--k-surface)]">
                <tr>
                  <th className="whitespace-nowrap border-b border-[var(--k-border)] px-2 py-2 text-left">Symbol</th>
                  <th className="whitespace-nowrap border-b border-[var(--k-border)] px-2 py-2 text-left">Name</th>
                  <th className="whitespace-nowrap border-b border-[var(--k-border)] px-2 py-2 text-right">Score</th>
                  <th className="whitespace-nowrap border-b border-[var(--k-border)] px-2 py-2 text-right">Last date</th>
                  <th className="whitespace-nowrap border-b border-[var(--k-border)] px-2 py-2 text-right">Perf</th>
                  <th className="whitespace-nowrap border-b border-[var(--k-border)] px-2 py-2 text-left">Why</th>
                  <th className="whitespace-nowrap border-b border-[var(--k-border)] px-2 py-2 text-left">Trend</th>
                </tr>
              </thead>
              <tbody>
                {consolidated.map((r) => (
                  <React.Fragment key={r.id}>
                    <tr className="bg-[var(--k-surface)]">
                      <td className="border-b border-[var(--k-border)] px-2 py-2 font-mono">
                        <button
                          type="button"
                          className="text-[var(--k-accent)] hover:underline"
                          onClick={() => onOpenStock?.(r.symbol)}
                          title="Open stock detail"
                        >
                          {r.ticker || r.symbol}
                        </button>
                      </td>
                      <td className="border-b border-[var(--k-border)] px-2 py-2">{r.name}</td>
                      <td className="border-b border-[var(--k-border)] px-2 py-2 text-right">
                        {Number.isFinite(r.score as number) ? Math.round(r.score as number) : '—'}
                      </td>
                      <td className="border-b border-[var(--k-border)] px-2 py-2 text-right font-mono">
                        {String(r.date || '—')}
                      </td>
                      <td className="border-b border-[var(--k-border)] px-2 py-2 text-right font-mono">{fmtPerf(r)}</td>
                      <td className="border-b border-[var(--k-border)] px-2 py-2 text-[var(--k-muted)]">
                        {r.whyBullets?.length ? (
                          <div className="space-y-1">
                            <ul className="list-disc pl-4">
                              {r.whyBullets.slice(0, 3).map((x, idx) => (
                                <li key={idx}>{x}</li>
                              ))}
                            </ul>
                            {fmtPlanLine(r) ? <div className="text-[11px] opacity-80">{fmtPlanLine(r)}</div> : null}
                          </div>
                        ) : (
                          r.reason
                        )}
                      </td>
                      <td className="border-b border-[var(--k-border)] px-2 py-2">
                        {r.series?.length ? <CloseSparkline series={r.series} /> : null}
                      </td>
                    </tr>
                    <tr>
                      <td colSpan={7} className="border-b border-[var(--k-border)] px-2 py-2">
                        <details>
                          <summary className="cursor-pointer text-xs text-[var(--k-muted)]">Details</summary>
                          <div className="mt-2 grid gap-3 text-xs md:grid-cols-3">
                            <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface-2)] p-3">
                              <div className="text-xs font-medium text-[var(--k-text)]">Source</div>
                              <div className="mt-2 space-y-2 text-[var(--k-muted)]">
                                <div>
                                  <div className="text-[11px] uppercase tracking-wide opacity-80">Industries</div>
                                  <div className="mt-1 flex flex-wrap gap-1">
                                    {Array.isArray((r.sourceSignals as any)?.industries) && (r.sourceSignals as any).industries.length ? (
                                      (r.sourceSignals as any).industries.slice(0, 3).map((x: any, idx: number) => (
                                        <span
                                          key={idx}
                                          className="rounded-md border border-[var(--k-border)] bg-[var(--k-surface)] px-2 py-0.5"
                                        >
                                          {String(x)}
                                        </span>
                                      ))
                                    ) : (
                                      <span>—</span>
                                    )}
                                  </div>
                                </div>
                                <div>
                                  <div className="text-[11px] uppercase tracking-wide opacity-80">Screeners</div>
                                  <div className="mt-1">
                                    {Array.isArray((r.sourceSignals as any)?.screeners) && (r.sourceSignals as any).screeners.length ? (
                                      <ul className="list-disc pl-4">
                                        {(r.sourceSignals as any).screeners.slice(0, 3).map((x: any, idx: number) => (
                                          <li key={idx}>{String(x)}</li>
                                        ))}
                                      </ul>
                                    ) : (
                                      <span>—</span>
                                    )}
                                  </div>
                                </div>
                                <div>
                                  <div className="text-[11px] uppercase tracking-wide opacity-80">Notes</div>
                                  <div className="mt-1">
                                    {Array.isArray((r.sourceSignals as any)?.notes) && (r.sourceSignals as any).notes.length ? (
                                      <ul className="list-disc pl-4">
                                        {(r.sourceSignals as any).notes.slice(0, 3).map((x: any, idx: number) => (
                                          <li key={idx}>{String(x)}</li>
                                        ))}
                                      </ul>
                                    ) : (
                                      <span>—</span>
                                    )}
                                  </div>
                                </div>
                              </div>
                            </div>

                            <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface-2)] p-3">
                              <div className="text-xs font-medium text-[var(--k-text)]">Plan</div>
                              <div className="mt-2 space-y-2 text-[var(--k-muted)]">
                                <div className="grid grid-cols-[96px_1fr] gap-2">
                                  <div className="opacity-80">Performance</div>
                                  <div className="font-mono">
                                    {fmtPerf(r)}
                                    {fmtPerf(r) === '—' ? (
                                      <span className="ml-2 text-[11px] opacity-70">(no bars yet; open Stock page to sync)</span>
                                    ) : null}
                                  </div>
                                </div>
                                <div className="grid grid-cols-[96px_1fr] gap-2">
                                  <div className="opacity-80">Duration</div>
                                  <div>{Number.isFinite(r.expectedDurationDays as number) ? `${r.expectedDurationDays} days` : '—'}</div>
                                </div>
                                <div className="grid grid-cols-[96px_1fr] gap-2">
                                  <div className="opacity-80">Buy zone</div>
                                  <div className="font-mono">{fmtBuyZoneText(r)}</div>
                                </div>
                                <div className="grid grid-cols-[96px_1fr] gap-2">
                                  <div className="opacity-80">Targets</div>
                                  <div className="font-mono">{fmtTargetText(r)}</div>
                                </div>
                                <div className="grid grid-cols-[96px_1fr] gap-2">
                                  <div className="opacity-80">Invalidation</div>
                                  <div>{String(r.invalidation ?? '—')}</div>
                                </div>
                                <div className="grid grid-cols-[96px_1fr] gap-2">
                                  <div className="opacity-80">Probability</div>
                                  <div>{fmtProbability(r.probability ?? null)}</div>
                                </div>
                                <div>
                                  <div className="text-[11px] uppercase tracking-wide opacity-80">Triggers</div>
                                  <div className="mt-1">
                                    {Array.isArray(r.triggers) && r.triggers.length ? (
                                      <ul className="list-disc pl-4">
                                        {r.triggers.slice(0, 4).map((t, idx) => (
                                          <li key={idx}>{fmtTriggerText(t as any)}</li>
                                        ))}
                                      </ul>
                                    ) : (
                                      <span>—</span>
                                    )}
                                  </div>
                                </div>
                              </div>
                            </div>

                            <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface-2)] p-3">
                              <div className="text-xs font-medium text-[var(--k-text)]">Risks</div>
                              <div className="mt-2 text-[var(--k-muted)]">
                                {((r.risks?.length ? r.risks : r.riskPoints) ?? []).length ? (
                                  <ul className="list-disc pl-4">
                                    {((r.risks?.length ? r.risks : r.riskPoints) ?? []).slice(0, 6).map((x, idx) => (
                                      <li key={idx}>{x}</li>
                                    ))}
                                  </ul>
                                ) : (
                                  <span>—</span>
                                )}
                              </div>
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
        ) : (
          <div className="text-sm text-[var(--k-muted)]">
            No leaders yet. Click “Generate today” after syncing screener + industry flow.
          </div>
        )}
      </section>
    </div>
  );
}


