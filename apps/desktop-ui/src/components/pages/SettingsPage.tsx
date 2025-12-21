'use client';

import * as React from 'react';

import { QUANT_BASE_URL } from '@/lib/endpoints';
import { Button } from '@/components/ui/button';
import { Switch } from '@/components/ui/switch';
import { cn } from '@/lib/utils';

type TvScreener = {
  id: string;
  name: string;
  url: string;
  enabled: boolean;
  updatedAt: string;
};

type TvChromeStatus = {
  running: boolean;
  pid: number | null;
  host: string;
  port: number;
  cdpOk: boolean;
  cdpVersion: Record<string, string> | null;
  userDataDir: string;
  profileDirectory: string;
};

async function apiGetJson<T>(path: string): Promise<T> {
  const res = await fetch(`${QUANT_BASE_URL}${path}`, { cache: 'no-store' });
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  return (await res.json()) as T;
}

async function apiSendJson<T>(
  path: string,
  method: 'POST' | 'PUT' | 'DELETE',
  body?: unknown,
): Promise<T> {
  const res = await fetch(`${QUANT_BASE_URL}${path}`, {
    method,
    headers: body ? { 'Content-Type': 'application/json' } : undefined,
    body: body ? JSON.stringify(body) : undefined,
  });
  if (!res.ok) {
    const txt = await res.text().catch(() => '');
    throw new Error(`${res.status} ${res.statusText}${txt ? `: ${txt}` : ''}`);
  }
  return (await res.json()) as T;
}

export function SettingsPage() {
  const [screeners, setScreeners] = React.useState<TvScreener[]>([]);
  const [status, setStatus] = React.useState<TvChromeStatus | null>(null);
  const [error, setError] = React.useState<string | null>(null);
  const [busy, setBusy] = React.useState(false);

  const [newName, setNewName] = React.useState('');
  const [newUrl, setNewUrl] = React.useState('');

  const [editingId, setEditingId] = React.useState<string | null>(null);
  const [editName, setEditName] = React.useState('');
  const [editUrl, setEditUrl] = React.useState('');

  const refresh = React.useCallback(async () => {
    setError(null);
    try {
      const [s, st] = await Promise.all([
        apiGetJson<{ items: TvScreener[] }>('/integrations/tradingview/screeners'),
        apiGetJson<TvChromeStatus>('/integrations/tradingview/status'),
      ]);
      setScreeners(s.items);
      setStatus(st);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  }, []);

  React.useEffect(() => {
    void refresh();
  }, [refresh]);

  async function startChrome() {
    setBusy(true);
    setError(null);
    try {
      const st = await apiSendJson<TvChromeStatus>(
        '/integrations/tradingview/chrome/start',
        'POST',
        {},
      );
      setStatus(st);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }

  async function stopChrome() {
    setBusy(true);
    setError(null);
    try {
      const st = await apiSendJson<TvChromeStatus>('/integrations/tradingview/chrome/stop', 'POST');
      setStatus(st);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }

  async function addScreener() {
    if (!newUrl.trim()) return;
    setBusy(true);
    setError(null);
    try {
      await apiSendJson<{ id: string }>('/integrations/tradingview/screeners', 'POST', {
        name: newName.trim() || 'Untitled',
        url: newUrl.trim(),
        enabled: true,
      });
      setNewName('');
      setNewUrl('');
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }

  async function saveScreener(it: TvScreener, next: Partial<TvScreener>) {
    setBusy(true);
    setError(null);
    try {
      await apiSendJson<{ ok: boolean }>(
        `/integrations/tradingview/screeners/${encodeURIComponent(it.id)}`,
        'PUT',
        {
          name: (next.name ?? it.name).trim() || 'Untitled',
          url: (next.url ?? it.url).trim(),
          enabled: next.enabled ?? it.enabled,
        },
      );
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }

  async function deleteScreener(it: TvScreener) {
    setBusy(true);
    setError(null);
    try {
      await apiSendJson<{ ok: boolean }>(
        `/integrations/tradingview/screeners/${encodeURIComponent(it.id)}`,
        'DELETE',
      );
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="mx-auto w-full max-w-4xl p-6">
      <div className="mb-6">
        <div className="text-lg font-semibold">TradingView Integration</div>
        <div className="mt-1 text-sm text-[var(--k-muted)]">
          Configure screeners and manage a dedicated Chrome instance for CDP (Playwright attach).
        </div>
      </div>

      {error ? (
        <div className="mb-4 rounded-lg border border-red-500/30 bg-red-500/10 px-3 py-2 text-sm text-red-600">
          {error}
        </div>
      ) : null}

      <section className="mb-8 rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] p-4">
        <div className="flex items-center justify-between gap-3">
          <div>
            <div className="font-medium">Dedicated Chrome (CDP)</div>
            <div className="text-sm text-[var(--k-muted)]">
              Start Chrome once, login to TradingView, then sync screeners without re-auth.
            </div>
          </div>
          <div className="flex items-center gap-2">
            <Button variant="secondary" size="sm" onClick={() => void refresh()} disabled={busy}>
              Test Connection
            </Button>
            {status?.running ? (
              <Button variant="secondary" size="sm" onClick={() => void stopChrome()} disabled={busy}>
                Stop
              </Button>
            ) : (
              <Button size="sm" onClick={() => void startChrome()} disabled={busy}>
                Start
              </Button>
            )}
          </div>
        </div>

        <div className="mt-4 grid gap-3 text-sm">
          <div className="grid grid-cols-2 gap-3">
            <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface-2)] px-3 py-2">
              <div className="text-[var(--k-muted)]">Status</div>
              <div className="mt-1 font-medium">
                {status?.running ? 'Running' : 'Stopped'}{' '}
                <span className={cn(status?.cdpOk ? 'text-emerald-600' : 'text-[var(--k-muted)]')}>
                  {status?.cdpOk ? '(CDP OK)' : '(CDP not ready)'}
                </span>
              </div>
            </div>
            <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface-2)] px-3 py-2">
              <div className="text-[var(--k-muted)]">Endpoint</div>
              <div className="mt-1 font-medium">
                {status ? `${status.host}:${status.port}` : '—'}
              </div>
            </div>
          </div>

          <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface-2)] px-3 py-2">
            <div className="text-[var(--k-muted)]">How to use</div>
            <ol className="mt-1 list-decimal pl-5 text-[var(--k-muted)]">
              <li>Click Start to launch a dedicated Chrome profile for TradingView automation.</li>
              <li>In that Chrome window, login to TradingView (Google SSO etc.).</li>
              <li>Come back to Kairos and click Sync on a screener.</li>
            </ol>
          </div>

          {status ? (
            <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface-2)] px-3 py-2">
              <div className="text-[var(--k-muted)]">Profile</div>
              <div className="mt-1 font-mono text-xs text-[var(--k-muted)]">
                userDataDir={status.userDataDir} • profile={status.profileDirectory} • pid=
                {status.pid ?? '—'}
              </div>
            </div>
          ) : null}
        </div>
      </section>

      <section className="rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] p-4">
        <div className="flex items-center justify-between">
          <div>
            <div className="font-medium">Screeners</div>
            <div className="text-sm text-[var(--k-muted)]">
              Manage TradingView screener URLs (targets) persisted in SQLite.
            </div>
          </div>
          <Button variant="secondary" size="sm" onClick={() => void refresh()} disabled={busy}>
            Refresh
          </Button>
        </div>

        <div className="mt-4 grid gap-2">
          <div className="grid grid-cols-12 gap-2">
            <input
              className="col-span-3 h-9 rounded-md border border-[var(--k-border)] bg-[var(--k-surface)] px-3 text-sm outline-none focus:ring-2 focus:ring-[var(--k-ring)]"
              placeholder="Name"
              value={newName}
              onChange={(e) => setNewName(e.target.value)}
            />
            <input
              className="col-span-8 h-9 rounded-md border border-[var(--k-border)] bg-[var(--k-surface)] px-3 text-sm outline-none focus:ring-2 focus:ring-[var(--k-ring)]"
              placeholder="https://www.tradingview.com/screener/..."
              value={newUrl}
              onChange={(e) => setNewUrl(e.target.value)}
            />
            <Button className="col-span-1 h-9" onClick={() => void addScreener()} disabled={busy}>
              Add
            </Button>
          </div>

          <div className="mt-2 overflow-hidden rounded-lg border border-[var(--k-border)]">
            <div className="grid grid-cols-12 gap-2 bg-[var(--k-surface-2)] px-3 py-2 text-xs text-[var(--k-muted)]">
              <div className="col-span-3">Name</div>
              <div className="col-span-7">URL</div>
              <div className="col-span-1 text-center">On</div>
              <div className="col-span-1 text-right">Actions</div>
            </div>
            <div className="divide-y divide-[var(--k-border)]">
              {screeners.map((it) => {
                const editing = editingId === it.id;
                return (
                  <div key={it.id} className="grid grid-cols-12 gap-2 px-3 py-2">
                    <div className="col-span-3">
                      {editing ? (
                        <input
                          className="h-9 w-full rounded-md border border-[var(--k-border)] bg-[var(--k-surface)] px-3 text-sm outline-none focus:ring-2 focus:ring-[var(--k-ring)]"
                          value={editName}
                          onChange={(e) => setEditName(e.target.value)}
                        />
                      ) : (
                        <div className="truncate pt-2 text-sm">{it.name}</div>
                      )}
                    </div>
                    <div className="col-span-7">
                      {editing ? (
                        <input
                          className="h-9 w-full rounded-md border border-[var(--k-border)] bg-[var(--k-surface)] px-3 text-sm outline-none focus:ring-2 focus:ring-[var(--k-ring)]"
                          value={editUrl}
                          onChange={(e) => setEditUrl(e.target.value)}
                        />
                      ) : (
                        <div className="truncate pt-2 font-mono text-xs text-[var(--k-muted)]">
                          {it.url}
                        </div>
                      )}
                    </div>
                    <div className="col-span-1 grid place-items-center">
                      <Switch
                        checked={it.enabled}
                        onCheckedChange={(v) => void saveScreener(it, { enabled: v })}
                        disabled={busy}
                      />
                    </div>
                    <div className="col-span-1 flex items-center justify-end gap-1">
                      {editing ? (
                        <>
                          <Button
                            variant="secondary"
                            size="sm"
                            className="h-8 px-2"
                            onClick={() =>
                              void saveScreener(it, { name: editName, url: editUrl }).then(() =>
                                setEditingId(null),
                              )
                            }
                            disabled={busy}
                          >
                            Save
                          </Button>
                          <Button
                            variant="ghost"
                            size="sm"
                            className="h-8 px-2"
                            onClick={() => setEditingId(null)}
                            disabled={busy}
                          >
                            Cancel
                          </Button>
                        </>
                      ) : (
                        <>
                          <Button
                            variant="secondary"
                            size="sm"
                            className="h-8 px-2"
                            onClick={() => {
                              setEditingId(it.id);
                              setEditName(it.name);
                              setEditUrl(it.url);
                            }}
                            disabled={busy}
                          >
                            Edit
                          </Button>
                          <Button
                            variant="ghost"
                            size="sm"
                            className="h-8 px-2 text-red-600 hover:text-red-600"
                            onClick={() => void deleteScreener(it)}
                            disabled={busy}
                          >
                            Delete
                          </Button>
                        </>
                      )}
                    </div>
                  </div>
                );
              })}
              {screeners.length === 0 ? (
                <div className="px-3 py-6 text-center text-sm text-[var(--k-muted)]">
                  No screeners configured.
                </div>
              ) : null}
            </div>
          </div>
        </div>
      </section>
    </div>
  );
}


