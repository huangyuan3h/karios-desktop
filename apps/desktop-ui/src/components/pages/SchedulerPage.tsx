'use client';

import * as React from 'react';

import { Button } from '@/components/ui/button';
import { apiGetJson, apiPostJson } from '@/lib/api/client';
import { formatAutomationSummary, type AutomationRun } from '@/lib/watchlist-automation';

type SyncJobRecord = {
  id: number;
  job_type: string;
  sync_at: string;
  success: boolean;
  last_ts_code: string | null;
  error_message: string | null;
};

type SimpleStatusResp = {
  job_type: string;
  today_run: SyncJobRecord | null;
};

type CloseStatusResp = {
  job_type: string;
  today_run: SyncJobRecord | null;
  last_success: SyncJobRecord | null;
};

type CloseSyncResp =
  | { ok: true; skipped?: boolean; message?: string; updated_daily_rows?: number; updated_adj_factor_rows?: number; trade_dates?: string[] }
  | { ok: false; error: string; last_marker?: string };

type TradeCalResp = { ok: boolean; updated?: number; error?: string };

function fmtWhen(iso: string | null | undefined): string {
  if (!iso) return '—';
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return String(iso);
  return d.toLocaleString();
}

function statusTone(ok: boolean | null | undefined): string {
  if (ok === true) return 'text-emerald-700';
  if (ok === false) return 'text-red-700';
  return 'text-[var(--k-muted)]';
}

function friendlyCloseMessage(message: string | null | undefined): string | null {
  if (!message) return null;
  const m = String(message).toLowerCase();
  if (m.includes('too early') && m.includes('17:05')) {
    return '今日尚未收盘，收盘同步将在 17:05 后可用。';
  }
  if (m.includes('not a trading day')) {
    if (m.includes('already up to date')) {
      return '今天不是交易日，数据已是最新，已跳过。';
    }
    return '今天不是交易日，已跳过收盘同步。';
  }
  if (m.includes('non-trading day catchup')) {
    return '非交易日补同步完成（已同步至最近一个交易日）。';
  }
  if (m.includes('already synced today') || m.includes('already up to date')) {
    return '今天已同步，无需重复操作。';
  }
  if (m.includes('no trading dates in range')) {
    return '交易日区间为空，请先同步交易日历。';
  }
  return message;
}

function StatusCard({
  title,
  schedule,
  status,
  extra,
}: {
  title: string;
  schedule: string;
  status: SimpleStatusResp | CloseStatusResp | null;
  extra?: React.ReactNode;
}) {
  const today = status && 'today_run' in status ? status.today_run : null;
  const ok = today ? Boolean(today.success) : null;
  return (
    <section className="rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] p-4">
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0">
          <div className="font-medium">{title}</div>
          <div className="mt-1 text-xs text-[var(--k-muted)]">{schedule}</div>
        </div>
        <div className={`text-xs font-medium ${statusTone(ok)}`}>
          {ok === true ? 'OK' : ok === false ? 'FAILED' : 'NO RUN'}
        </div>
      </div>

      <div className="mt-3 grid grid-cols-1 gap-2 text-sm">
        <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface-2)] px-3 py-2">
          <div className="text-xs text-[var(--k-muted)]">Today</div>
          {today ? (
            <div className="mt-1 space-y-1">
              <div className="font-mono text-xs">at {fmtWhen(today.sync_at)}</div>
              {today.error_message ? (
                <div className="text-xs text-red-700">{today.error_message}</div>
              ) : null}
            </div>
          ) : (
            <div className="mt-1 text-xs text-[var(--k-muted)]">No record today.</div>
          )}
        </div>

        {'last_success' in (status ?? {}) ? (
          <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface-2)] px-3 py-2">
            <div className="text-xs text-[var(--k-muted)]">Last success</div>
            <div className="mt-1 font-mono text-xs">
              {fmtWhen((status as CloseStatusResp | null)?.last_success?.sync_at)}
            </div>
          </div>
        ) : null}
      </div>

      {extra ? <div className="mt-3">{extra}</div> : null}
    </section>
  );
}

type AlphaRadarStatusResp = {
  ok?: boolean;
  todayRun?: SyncJobRecord | null;
  lastSuccess?: SyncJobRecord | null;
  lastIngestStats?: { fetched?: number; filteredOut?: number; stored?: number } | null;
};

export function SchedulerPage() {
  const [busy, setBusy] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);
  const [msg, setMsg] = React.useState<string | null>(null);
  const [needTradeCal, setNeedTradeCal] = React.useState(false);

  const [closeStatus, setCloseStatus] = React.useState<CloseStatusResp | null>(null);
  const [dailyStatus, setDailyStatus] = React.useState<SimpleStatusResp | null>(null);
  const [adjStatus, setAdjStatus] = React.useState<SimpleStatusResp | null>(null);
  const [basicStatus, setBasicStatus] = React.useState<SimpleStatusResp | null>(null);
  const [alphaRadarStatus, setAlphaRadarStatus] = React.useState<AlphaRadarStatusResp | null>(null);
  const [watchlistAutomation, setWatchlistAutomation] = React.useState<AutomationRun | null>(null);

  const refresh = React.useCallback(async () => {
    setError(null);
    setNeedTradeCal(false);
    setBusy(true);
    try {
      const [c, d, a, b, ar, wl] = await Promise.all([
        apiGetJson<CloseStatusResp>('/close/status'),
        apiGetJson<SimpleStatusResp>('/daily/status'),
        apiGetJson<SimpleStatusResp>('/adj-factor/status'),
        apiGetJson<SimpleStatusResp>('/stock-basic/status'),
        apiGetJson<AlphaRadarStatusResp>('/api/alpha-radar/status'),
        apiGetJson<{ found: boolean } & AutomationRun>('/watchlist/automation/latest'),
      ]);
      setCloseStatus(c);
      setDailyStatus(d);
      setAdjStatus(a);
      setBasicStatus(b);
      setAlphaRadarStatus(ar);
      setWatchlistAutomation(wl.found && wl.runId ? wl : null);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }, []);

  React.useEffect(() => {
    void refresh();
  }, [refresh]);

  async function syncTodayAll() {
    setError(null);
    setMsg('Syncing...');
    setNeedTradeCal(false);
    setBusy(true);
    try {
      // Use force=true to heal "false success" records (e.g. user clicked before close).
      const r = await apiPostJson<CloseSyncResp>('/sync/close?force=true');
      if ('ok' in r && r.ok) {
        if (r.skipped) setMsg(friendlyCloseMessage(r.message) || 'Skipped.');
        else if ('partial' in r && r.partial)
          setMsg('已同步到昨日，今日收盘后可再同步。');
        else if (r.message?.toLowerCase().includes('non-trading day catchup'))
          setMsg(friendlyCloseMessage(r.message) || r.message);
        else
          setMsg(
            `OK: daily=${r.updated_daily_rows ?? 0}, adj_factor=${r.updated_adj_factor_rows ?? 0}${
              r.trade_dates?.length ? `, dates=${r.trade_dates.join(',')}` : ''
            }`,
          );
      } else {
        const err = (r as any).error || 'Sync failed.';
        setError(err);
        if (String(err).toLowerCase().includes('trade calendar missing')) setNeedTradeCal(true);
      }
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }

  async function syncTradeCalAndRetry() {
    setError(null);
    setMsg(null);
    setNeedTradeCal(false);
    setBusy(true);
    try {
      const r = await apiPostJson<TradeCalResp>('/sync/trade-cal');
      if (!r.ok) throw new Error(r.error || 'Trade calendar sync failed.');
      setMsg(`Trade calendar synced: updated=${r.updated ?? 0}`);
      await syncTodayAll();
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
          <div className="text-lg font-semibold">Scheduler</div>
          <div className="mt-1 text-sm text-[var(--k-muted)]">
            Manage sync jobs. This scheduler runs inside the backend process; if the service is stopped or the machine
            sleeps, jobs will not run.
          </div>
        </div>
        <div className="flex items-center gap-2">
          <Button variant="secondary" size="sm" onClick={() => void refresh()} disabled={busy}>
            Refresh status
          </Button>
          <Button size="sm" onClick={() => void syncTodayAll()} disabled={busy}>
            Sync today (close)
          </Button>
        </div>
      </div>

      {error ? (
        <div className="mb-4 rounded-lg border border-red-500/30 bg-red-500/10 px-3 py-2 text-sm text-red-600">
          <div className="flex flex-wrap items-center justify-between gap-2">
            <div className="min-w-0">{error}</div>
            {needTradeCal ? (
              <Button size="sm" variant="secondary" onClick={() => void syncTradeCalAndRetry()} disabled={busy}>
                Sync trade calendar & retry
              </Button>
            ) : null}
          </div>
        </div>
      ) : null}
      {msg ? (
        <div className="mb-4 rounded-lg border border-emerald-500/30 bg-emerald-500/10 px-3 py-2 text-sm text-emerald-700">
          {msg}
        </div>
      ) : null}

      <div className="grid gap-4 md:grid-cols-2">
        <StatusCard title="Close sync" schedule="Daily 17:10 Asia/Shanghai (skips non-trading days)" status={closeStatus} />
        <StatusCard title="Daily full sync" schedule="Fri 17:00 Asia/Shanghai (fallback)" status={dailyStatus} />
        <StatusCard title="Adj factor full sync" schedule="Fri 17:00 Asia/Shanghai (fallback)" status={adjStatus} />
        <StatusCard title="Stock basic sync" schedule="Fri 18:00 Asia/Shanghai" status={basicStatus} />
        <StatusCard
          title="Alpha Radar pipeline"
          schedule="Every 12 hours (RSS + filter + fulltext + batch LLM + A-share map)"
          status={
            alphaRadarStatus?.todayRun
              ? {
                  job_type: 'alpha_radar_pipeline',
                  today_run: alphaRadarStatus.todayRun,
                  last_success: alphaRadarStatus.lastSuccess ?? undefined,
                }
              : alphaRadarStatus?.lastSuccess
                ? {
                    job_type: 'alpha_radar_pipeline',
                    today_run: null,
                    last_success: alphaRadarStatus.lastSuccess,
                  }
                : null
          }
          extra={
            <Button
              size="sm"
              variant="secondary"
              disabled={busy}
              onClick={async () => {
                setError(null);
                setMsg('Alpha Radar pipeline running...');
                setBusy(true);
                try {
                  const r = await apiPostJson<{
                    ok?: boolean;
                    skipped?: boolean;
                    trendCount?: number;
                    ingestStats?: { stored?: number; filteredOut?: number };
                    keptPreviousTrends?: boolean;
                    errors?: Array<{ error?: string }>;
                  }>('/api/alpha-radar/run-pipeline', { force: true });
                  const stored = r.ingestStats?.stored ?? 0;
                  const filtered = r.ingestStats?.filteredOut ?? 0;
                  if (r.skipped) {
                    setMsg(`Skipped (12h cooldown) · ${r.trendCount ?? 0} card(s)`);
                  } else if (r.ok === false) {
                    setMsg(
                      `Pipeline failed · stored ${stored}, filtered ${filtered}` +
                        (r.keptPreviousTrends ? ' · kept previous cards' : ''),
                    );
                    if (r.errors?.length) {
                      setError(r.errors.map((e) => e.error).filter(Boolean).join('\n'));
                    }
                  } else {
                    setMsg(
                      `Pipeline OK · stored ${stored} → ${r.trendCount ?? 0} trend card(s)` +
                        (filtered ? ` · filtered ${filtered}` : ''),
                    );
                  }
                  await refresh();
                } catch (e) {
                  setError(e instanceof Error ? e.message : String(e));
                } finally {
                  setBusy(false);
                }
              }}
            >
              Run pipeline (force)
            </Button>
          }
        />
        <section className="rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] p-4 md:col-span-2">
          <div className="font-medium">Watchlist automation</div>
          <div className="mt-1 text-xs text-[var(--k-muted)]">
            Weekdays 17:30 Asia/Shanghai · remove weak / screener import / Alpha Radar S append
          </div>
          <div className="mt-3 text-sm">
            {watchlistAutomation ? (
              <div className="space-y-1">
                <div>{formatAutomationSummary(watchlistAutomation)}</div>
                <div className="text-xs text-[var(--k-muted)]">
                  Applied: {watchlistAutomation.appliedAt ? fmtWhen(watchlistAutomation.appliedAt) : 'pending'}
                </div>
              </div>
            ) : (
              <div className="text-xs text-[var(--k-muted)]">No automation run recorded yet.</div>
            )}
          </div>
        </section>
      </div>
    </div>
  );
}

