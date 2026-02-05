/* eslint-disable @typescript-eslint/no-explicit-any */
'use client';

import * as React from 'react';
import { RefreshCw } from 'lucide-react';

import { Button } from '@/components/ui/button';
import { QUANT_BASE_URL } from '@/lib/endpoints';

type SyncStatusResp = any;
type SyncRunsResp = any;

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

function fmtDuration(ms?: number | null) {
  if (!ms && ms !== 0) return '—';
  const s = Math.round(ms / 1000);
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  const ss = s % 60;
  return `${m}m ${ss}s`;
}

function statusColor(status?: string | null) {
  const s = (status || '').toLowerCase();
  if (s === 'ok') return 'text-emerald-600';
  if (s === 'partial') return 'text-amber-600';
  if (s === 'failed') return 'text-rose-600';
  if (s === 'running' || s === 'queued') return 'text-blue-600';
  return 'text-[var(--k-muted)]';
}

function progressPercent(run?: any | null): number {
  if (!run) return 0;
  const steps = Array.isArray(run.steps) ? run.steps : [];
  if (!steps.length) return 0;
  let total = 0;
  let done = 0;
  let doneSteps = 0;
  let runningSteps = 0;
  for (const s of steps) {
    const totalSymbols = Number(s.totalSymbols ?? 0);
    const ok = Number(s.okCount ?? 0);
    const failed = Number(s.failedCount ?? 0);
    const st = String(s.status || '').toLowerCase();
    if (['ok', 'partial', 'failed'].includes(st)) doneSteps += 1;
    if (st === 'running' || st === 'queued') runningSteps += 1;
    if (totalSymbols > 0) {
      total += totalSymbols;
      done += Math.min(totalSymbols, ok + failed);
    } else {
      total += 1;
      done += ['ok', 'partial', 'failed'].includes(st) ? 1 : 0;
    }
  }
  const totalSteps = steps.length || 1;
  if (!total || done === 0) {
    const stepProgress = (doneSteps + (runningSteps > 0 ? 0.5 : 0)) / totalSteps;
    return Math.min(100, Math.max(0, Math.round(stepProgress * 100)));
  }
  return Math.min(100, Math.round((done / total) * 100));
}

function formatSymbols(list?: string[] | null, limit = 20): string {
  if (!Array.isArray(list) || list.length === 0) return '—';
  const head = list.slice(0, limit);
  const rest = list.length - head.length;
  return rest > 0 ? `${head.join(', ')} … +${rest}` : head.join(', ');
}

export function SyncPage() {
  const [lastRun, setLastRun] = React.useState<any | null>(null);
  const [runs, setRuns] = React.useState<any[]>([]);
  const [total, setTotal] = React.useState(0);
  const [loading, setLoading] = React.useState(false);
  const [statusLoading, setStatusLoading] = React.useState(false);
  const [triggering, setTriggering] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);

  const loadStatus = React.useCallback(async () => {
    setStatusLoading(true);
    try {
      const status = await apiGetJson<SyncStatusResp>('/sync/status');
      setLastRun(status?.lastRun ?? null);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setStatusLoading(false);
    }
  }, []);

  const loadRuns = React.useCallback(async () => {
    setLoading(true);
    try {
      const list = await apiGetJson<SyncRunsResp>('/sync/runs?limit=50&offset=0');
      setRuns(Array.isArray(list?.items) ? list.items : []);
      setTotal(Number(list?.total || 0));
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }, []);

  const load = React.useCallback(async () => {
    setError(null);
    await Promise.all([loadStatus(), loadRuns()]);
  }, [loadRuns, loadStatus]);

  React.useEffect(() => {
    load();
  }, [load]);

  React.useEffect(() => {
    if (!lastRun) return;
    const s = String(lastRun.status || '').toLowerCase();
    if (!['running', 'queued'].includes(s)) return;
    const t = window.setInterval(() => {
      void loadStatus();
    }, 5000);
    return () => window.clearInterval(t);
  }, [lastRun, loadStatus]);

  React.useEffect(() => {
    const s = String(lastRun?.status || '').toLowerCase();
    if (s && !['running', 'queued'].includes(s)) {
      void loadRuns();
    }
  }, [lastRun?.status, loadRuns]);

  async function onTrigger() {
    setTriggering(true);
    setError(null);
    try {
      await apiPostJson('/sync/trigger', { force: true });
      await loadStatus();
      await loadRuns();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setTriggering(false);
    }
  }

  const steps = Array.isArray(lastRun?.steps) ? lastRun.steps : [];
  const progress = progressPercent(lastRun);
  const targetSymbols = Array.isArray(lastRun?.detail?.targetSymbols)
    ? lastRun.detail.targetSymbols
    : [];

  return (
    <div className="space-y-4 p-4">
      <section className="rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] p-4">
        <div className="flex flex-wrap items-center gap-3">
          <div className="text-sm font-semibold">EOD Sync</div>
          <div className="text-xs text-[var(--k-muted)]">Manual trigger + status</div>
          <div className="flex-1" />
          <Button size="sm" onClick={onTrigger} disabled={triggering || loading}>
            {triggering ? (
              <>
                <RefreshCw className="mr-2 h-4 w-4 animate-spin" />
                Triggering
              </>
            ) : (
              'Run EOD Sync'
            )}
          </Button>
          <Button size="sm" variant="secondary" onClick={load} disabled={loading || statusLoading}>
            Refresh
          </Button>
        </div>
        {error ? <div className="mt-2 text-xs text-rose-600">{error}</div> : null}
      </section>

      <section className="rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] p-4">
        <div className="mb-2 text-sm font-medium">Latest status</div>
        {lastRun ? (
          <div className="space-y-3 text-sm">
            <div>
              <div className="mb-1 flex items-center justify-between text-xs text-[var(--k-muted)]">
                <span>Progress</span>
                <span>{progress}%</span>
              </div>
              <div className="h-2 w-full rounded-full bg-[var(--k-border)]">
                <div
                  className="h-2 rounded-full bg-[var(--k-accent)] transition-[width]"
                  style={{ width: `${progress}%` }}
                />
              </div>
            </div>
            <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
              <div>
                <div className="text-xs text-[var(--k-muted)]">Status</div>
                <div className={`font-semibold ${statusColor(lastRun.status)}`}>{lastRun.status}</div>
              </div>
              <div>
                <div className="text-xs text-[var(--k-muted)]">Trade date</div>
                <div>{lastRun.tradeDate || '—'}</div>
              </div>
              <div>
                <div className="text-xs text-[var(--k-muted)]">Started at</div>
                <div>{fmtDateTime(lastRun.startedAt)}</div>
              </div>
              <div>
                <div className="text-xs text-[var(--k-muted)]">Duration</div>
                <div>{fmtDuration(lastRun.durationMs)}</div>
              </div>
            </div>
            <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
              <div>
                <div className="text-xs text-[var(--k-muted)]">Targets</div>
                <div>{lastRun.targetSymbols ?? 0}</div>
              </div>
              <div>
                <div className="text-xs text-[var(--k-muted)]">OK steps</div>
                <div>{lastRun.okSteps ?? 0}</div>
              </div>
              <div>
                <div className="text-xs text-[var(--k-muted)]">Failed steps</div>
                <div>{lastRun.failedSteps ?? 0}</div>
              </div>
              <div>
                <div className="text-xs text-[var(--k-muted)]">Error</div>
                <div className="truncate">{lastRun.error || '—'}</div>
              </div>
            </div>
            {lastRun?.detail?.message ? (
              <div className="text-xs text-[var(--k-muted)]">{lastRun.detail.message}</div>
            ) : null}
            <div className="text-xs text-[var(--k-muted)]">
              {targetSymbols.length ? `Targets: ${formatSymbols(targetSymbols, 30)}` : 'Targets: none'}
            </div>

            {steps.length ? (
              <div className="overflow-auto rounded border border-[var(--k-border)]">
                <table className="w-full border-collapse text-xs">
                  <thead className="bg-[var(--k-surface-2)] text-[var(--k-muted)]">
                    <tr className="text-left">
                      <th className="px-3 py-2">Step</th>
                      <th className="px-3 py-2">Status</th>
                      <th className="px-3 py-2">Started</th>
                      <th className="px-3 py-2">Duration</th>
                      <th className="px-3 py-2">OK</th>
                      <th className="px-3 py-2">Failed</th>
                      <th className="px-3 py-2">Symbols</th>
                      <th className="px-3 py-2">Error</th>
                    </tr>
                  </thead>
                  <tbody>
                    {steps.map((s: any, idx: number) => (
                      <tr key={`${s.step}-${idx}`} className="border-t border-[var(--k-border)]">
                        <td className="px-3 py-2">{s.step}</td>
                        <td className={`px-3 py-2 ${statusColor(s.status)}`}>{s.status}</td>
                        <td className="px-3 py-2">{fmtDateTime(s.startedAt)}</td>
                        <td className="px-3 py-2">{fmtDuration(s.durationMs)}</td>
                        <td className="px-3 py-2">{s.okCount ?? '—'}</td>
                        <td className="px-3 py-2">{s.failedCount ?? '—'}</td>
                        <td className="px-3 py-2">
                          <details>
                            <summary className="cursor-pointer text-[var(--k-muted)]">
                              {s.totalSymbols ?? '—'} total
                            </summary>
                            <div className="mt-2 space-y-1 text-[11px]">
                              <div>
                                <span className="text-[var(--k-muted)]">Pending:</span>{' '}
                                {formatSymbols(s.symbolsPending)}
                              </div>
                              <div>
                                <span className="text-[var(--k-muted)]">OK:</span>{' '}
                                {formatSymbols(s.symbolsOk)}
                              </div>
                              <div>
                                <span className="text-[var(--k-muted)]">Failed:</span>{' '}
                                {formatSymbols(s.symbolsFailed)}
                              </div>
                            </div>
                          </details>
                        </td>
                        <td className="px-3 py-2 text-[var(--k-muted)]">{s.error || '—'}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : (
              <div className="text-xs text-[var(--k-muted)]">No steps recorded yet.</div>
            )}
          </div>
        ) : (
          <div className="text-xs text-[var(--k-muted)]">No runs yet.</div>
        )}
      </section>

      <section className="rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] p-4">
        <div className="mb-2 flex items-center justify-between">
          <div className="text-sm font-medium">Run history</div>
          <div className="text-xs text-[var(--k-muted)]">{total} runs</div>
        </div>
        <div className="overflow-auto rounded border border-[var(--k-border)]">
          <table className="w-full border-collapse text-xs">
            <thead className="bg-[var(--k-surface-2)] text-[var(--k-muted)]">
              <tr className="text-left">
                <th className="px-3 py-2">Started</th>
                <th className="px-3 py-2">Trade date</th>
                <th className="px-3 py-2">Status</th>
                <th className="px-3 py-2">Duration</th>
                <th className="px-3 py-2">Targets</th>
                <th className="px-3 py-2">OK/Fail</th>
                <th className="px-3 py-2">Error</th>
              </tr>
            </thead>
            <tbody>
              {runs.length ? (
                runs.map((r: any) => (
                  <tr key={r.id} className="border-t border-[var(--k-border)]">
                    <td className="px-3 py-2">{fmtDateTime(r.startedAt)}</td>
                    <td className="px-3 py-2">{r.tradeDate || '—'}</td>
                    <td className={`px-3 py-2 ${statusColor(r.status)}`}>{r.status}</td>
                    <td className="px-3 py-2">{fmtDuration(r.durationMs)}</td>
                    <td className="px-3 py-2">{r.targetSymbols ?? 0}</td>
                    <td className="px-3 py-2">
                      {(r.okSteps ?? 0)}/{(r.failedSteps ?? 0)}
                    </td>
                    <td className="px-3 py-2 text-[var(--k-muted)]">{r.error || '—'}</td>
                  </tr>
                ))
              ) : (
                <tr>
                  <td className="px-3 py-4 text-[var(--k-muted)]" colSpan={7}>
                    No runs yet.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </section>
    </div>
  );
}
