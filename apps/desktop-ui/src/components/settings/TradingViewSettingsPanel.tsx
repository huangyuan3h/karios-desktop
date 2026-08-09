'use client';

import * as React from 'react';

import { apiDeleteJson, apiGetJson, apiPostJson, apiPutJson } from '@/lib/api/client';
import { Button } from '@/components/ui/button';
import { Switch } from '@/components/ui/switch';
import { cn } from '@/lib/utils';

type TvScreener = {
  id: string;
  name: string;
  url: string;
  enabled: boolean;
  updatedAt: string;
  mode: 'api' | 'chrome';
  market?: string | null;
  filterJson?: Record<string, unknown> | null;
  apiColumns?: string[] | null;
};

type TvScreenerTemplate = {
  templateId: string;
  displayName: string;
  market: string;
  description: string;
  nestedFilterValidated: boolean;
  screenTitleSubstr: string;
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
  headless: boolean;
};

type NewScreenerMode = 'template' | 'url' | 'json';

export function TradingViewSettingsPanel() {
  const [screeners, setScreeners] = React.useState<TvScreener[]>([]);
  const [templates, setTemplates] = React.useState<TvScreenerTemplate[]>([]);
  const [status, setStatus] = React.useState<TvChromeStatus | null>(null);
  const [error, setError] = React.useState<string | null>(null);
  const [busy, setBusy] = React.useState(false);

  const [headless, setHeadless] = React.useState(true);
  const [sourceUserDataDir, setSourceUserDataDir] = React.useState(
    '~/Library/Application Support/Google/Chrome',
  );
  const [sourceProfileDir, setSourceProfileDir] = React.useState('Profile 1');
  const [forceBootstrap, setForceBootstrap] = React.useState(false);

  // New screener form state (OPT-057 three-mode UI).
  const [newMode, setNewMode] = React.useState<NewScreenerMode>('template');
  const [newTemplateId, setNewTemplateId] = React.useState<string>('');
  const [newName, setNewName] = React.useState('');
  const [newUrl, setNewUrl] = React.useState('');
  const [newFilterJson, setNewFilterJson] = React.useState('');

  const [editingId, setEditingId] = React.useState<string | null>(null);
  const [editName, setEditName] = React.useState('');
  const [editUrl, setEditUrl] = React.useState('');
  const [editMode, setEditMode] = React.useState<'api' | 'chrome'>('chrome');
  const [editFilterJson, setEditFilterJson] = React.useState('');

  const refresh = React.useCallback(async () => {
    setError(null);
    try {
      const [s, st, ts] = await Promise.all([
        apiGetJson<{ items: TvScreener[] }>('/integrations/tradingview/screeners'),
        apiGetJson<TvChromeStatus>('/integrations/tradingview/status'),
        apiGetJson<{ items: TvScreenerTemplate[] }>(
          '/integrations/tradingview/screener-templates',
        ),
      ]);
      setScreeners(s.items);
      setStatus(st);
      setTemplates(ts.items);
      if (!newTemplateId && ts.items.length > 0) {
        const cn = ts.items.find((t) => t.market === 'cn');
        setNewTemplateId((cn ?? ts.items[0]).templateId);
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  }, [newTemplateId]);

  React.useEffect(() => {
    void refresh();
  }, [refresh]);

  async function startChrome() {
    setBusy(true);
    setError(null);
    try {
      const needsForce = !!status && status.profileDirectory !== sourceProfileDir;
      const st = await apiPostJson<TvChromeStatus>(
        '/integrations/tradingview/chrome/start',
        {
          headless,
          userDataDir: status?.userDataDir ?? '~/.karios/chrome-tv-cdp',
          profileDirectory: sourceProfileDir,
          bootstrapFromChromeUserDataDir: sourceUserDataDir,
          bootstrapFromProfileDirectory: sourceProfileDir,
          forceBootstrap: forceBootstrap || needsForce,
        },
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
      const st = await apiPostJson<TvChromeStatus>('/integrations/tradingview/chrome/stop');
      setStatus(st);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }

  async function addScreener() {
    setBusy(true);
    setError(null);
    try {
      if (newMode === 'template') {
        if (!newTemplateId) {
          setError('Pick a template first.');
          return;
        }
        await apiPostJson<{ id: string }>(
          '/integrations/tradingview/screeners/from-template',
          { templateId: newTemplateId, enabled: true },
        );
      } else if (newMode === 'url') {
        if (!newUrl.trim()) {
          setError('URL is required for legacy mode.');
          return;
        }
        await apiPostJson<{ id: string }>(
          '/integrations/tradingview/screeners',
          {
            name: newName.trim() || 'Untitled',
            url: newUrl.trim(),
            enabled: true,
            mode: 'chrome',
          },
        );
      } else {
        // json
        if (!newFilterJson.trim()) {
          setError('Filter JSON is required.');
          return;
        }
        let parsed: unknown;
        try {
          parsed = JSON.parse(newFilterJson);
        } catch (e) {
          setError(`Filter JSON parse error: ${e instanceof Error ? e.message : String(e)}`);
          return;
        }
        await apiPostJson<{ id: string }>(
          '/integrations/tradingview/screeners',
          {
            name: newName.trim() || 'Untitled',
            url: '',
            enabled: true,
            mode: 'api',
            filterJson: parsed as Record<string, unknown>,
          },
        );
      }
      setNewName('');
      setNewUrl('');
      setNewFilterJson('');
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
      let filterJson = it.filterJson ?? null;
      if (next.mode === 'api' && editFilterJson.trim()) {
        try {
          filterJson = JSON.parse(editFilterJson) as Record<string, unknown>;
        } catch (e) {
          setError(`Filter JSON parse error: ${e instanceof Error ? e.message : String(e)}`);
          return;
        }
      }
      await apiPutJson<{ ok: boolean }>(
        `/integrations/tradingview/screeners/${encodeURIComponent(it.id)}`,
        {
          name: (next.name ?? it.name).trim() || 'Untitled',
          url: (next.url ?? it.url).trim(),
          enabled: next.enabled ?? it.enabled,
          mode: next.mode ?? it.mode,
          filterJson,
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
      await apiDeleteJson<{ ok: boolean }>(
        `/integrations/tradingview/screeners/${encodeURIComponent(it.id)}`,
      );
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }
  return (
    <div className="mx-auto w-full max-w-6xl p-6">
        
          {error ? (
            <div className="mb-4 rounded-lg border border-red-500/30 bg-red-500/10 px-3 py-2 text-sm text-red-600">
              {error}
            </div>
          ) : null}

          <>
            <div className="mb-6">
              <div className="text-lg font-semibold">TradingView Integration</div>
              <div className="mt-1 text-sm text-[var(--k-muted)]">
                Configure screeners and manage a dedicated Chrome instance for CDP (Playwright
                attach).
              </div>
            </div>

            <section className="mb-8 rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] p-4">
              <div className="flex items-center justify-between gap-3">
                <div>
                  <div className="font-medium">Dedicated Chrome (CDP)</div>
                  <div className="text-sm text-[var(--k-muted)]">
                    Start a headless Chrome and reuse your existing Chrome profile login state.
                  </div>
                </div>
                <div className="flex items-center gap-2">
                  <Button
                    variant="secondary"
                    size="sm"
                    onClick={() => void refresh()}
                    disabled={busy}
                  >
                    Test Connection
                  </Button>
                  {status?.running ? (
                    <Button
                      variant="secondary"
                      size="sm"
                      onClick={() => void stopChrome()}
                      disabled={busy}
                    >
                      Stop
                    </Button>
                  ) : (
                    <Button size="sm" onClick={() => void startChrome()} disabled={busy}>
                      Start
                    </Button>
                  )}
                </div>
              </div>

              <div className="mt-4 overflow-hidden rounded-lg border border-[var(--k-border)] bg-[var(--k-surface-2)]">
                <div className="divide-y divide-[var(--k-border)]">
                  <div className="flex items-center justify-between gap-4 px-4 py-3">
                    <div>
                      <div className="text-sm font-medium">Silent mode</div>
                      <div className="mt-0.5 text-xs text-[var(--k-muted)]">
                        Headless Chrome will not open a window. If login is required, temporarily
                        turn it off.
                      </div>
                    </div>
                    <div className="flex items-center gap-3">
                      <div className="text-xs text-[var(--k-muted)]">Headless</div>
                      <Switch checked={headless} onCheckedChange={setHeadless} disabled={busy} />
                    </div>
                  </div>

                  <div className="px-4 py-3">
                    <div className="text-sm font-medium">
                      Bootstrap from existing Chrome profile
                    </div>
                    <div className="mt-2 grid grid-cols-1 gap-2 md:grid-cols-12">
                      <input
                        className="h-9 w-full rounded-md border border-[var(--k-border)] bg-[var(--k-surface)] px-3 text-sm outline-none focus:ring-2 focus:ring-[var(--k-ring)] md:col-span-8"
                        placeholder="~/Library/Application Support/Google/Chrome"
                        value={sourceUserDataDir}
                        onChange={(e) => setSourceUserDataDir(e.target.value)}
                        disabled={busy}
                      />
                      <input
                        className="h-9 w-full rounded-md border border-[var(--k-border)] bg-[var(--k-surface)] px-3 text-sm outline-none focus:ring-2 focus:ring-[var(--k-ring)] md:col-span-4"
                        placeholder="Profile 1"
                        value={sourceProfileDir}
                        onChange={(e) => setSourceProfileDir(e.target.value)}
                        disabled={busy}
                      />
                    </div>
                    <div className="mt-2 text-xs text-[var(--k-muted)]">
                      We copy &quot;Local State&quot; and the selected profile into the dedicated
                      user-data-dir, so CDP works.
                    </div>

                    <div className="mt-3 flex items-center justify-between gap-3">
                      <div className="text-xs text-[var(--k-muted)]">
                        Force bootstrap will recopy the profile and restart Chrome if needed.
                      </div>
                      <div className="flex items-center gap-3">
                        <div className="text-xs text-[var(--k-muted)]">Force</div>
                        <Switch
                          checked={forceBootstrap}
                          onCheckedChange={setForceBootstrap}
                          disabled={busy}
                        />
                      </div>
                    </div>
                  </div>
                </div>
              </div>
              {/* rest of TradingView UI unchanged */}
              <div className="mt-4 grid grid-cols-1 gap-3 md:grid-cols-2">
                <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface-2)] px-4 py-3">
                  <div className="text-xs text-[var(--k-muted)]">Status</div>
                  <div className="mt-1 text-sm font-medium">
                    {status?.running ? 'Running' : 'Stopped'}{' '}
                    <span
                      className={cn(status?.cdpOk ? 'text-emerald-600' : 'text-[var(--k-muted)]')}
                    >
                      {status?.cdpOk ? '(CDP OK)' : '(CDP not ready)'}
                    </span>
                  </div>
                </div>
                <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface-2)] px-4 py-3">
                  <div className="text-xs text-[var(--k-muted)]">Endpoint</div>
                  <div className="mt-1 text-sm font-medium">
                    {status ? `${status.host}:${status.port}` : '—'}
                  </div>
                </div>
              </div>

              <div className="mt-3 rounded-lg border border-[var(--k-border)] bg-[var(--k-surface-2)] px-4 py-3">
                <div className="text-xs text-[var(--k-muted)]">How to use</div>
                <ol className="mt-2 list-decimal pl-5 text-sm text-[var(--k-muted)]">
                  <li>
                    Click Start to launch a dedicated Chrome profile for TradingView automation.
                  </li>
                  <li>In that Chrome window, login to TradingView (Google SSO etc.).</li>
                  <li>Come back to Kairos and click Sync on a screener.</li>
                </ol>
              </div>

              {status ? (
                <div className="mt-3 rounded-lg border border-[var(--k-border)] bg-[var(--k-surface-2)] px-4 py-3">
                  <div className="text-xs text-[var(--k-muted)]">Profile</div>
                  <div className="mt-1 font-mono text-xs text-[var(--k-muted)]">
                    userDataDir={status.userDataDir} • profile={status.profileDirectory} • pid=
                    {status.pid ?? '—'}
                  </div>
                </div>
              ) : null}
            </section>

            <section className="rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] p-4">
              <div className="flex items-center justify-between">
                <div>
                  <div className="font-medium">Screeners</div>
                  <div className="text-sm text-[var(--k-muted)]">
                    Manage TradingView screener URLs (targets) persisted in Postgres via
                    data-sync-service.
                  </div>
                </div>
                <div className="flex items-center gap-2">
                  <Button
                    variant="secondary"
                    size="sm"
                    onClick={() => void refresh()}
                    disabled={busy}
                  >
                    Refresh
                  </Button>
                </div>
              </div>

              <div className="mt-4 grid gap-3">
                <div className="flex flex-wrap items-center gap-2 text-sm">
                  <span className="text-[var(--k-muted)]">Mode:</span>
                  {(['template', 'url', 'json'] as const).map((m) => (
                    <Button
                      key={m}
                      size="sm"
                      variant={newMode === m ? 'default' : 'secondary'}
                      onClick={() => setNewMode(m)}
                      disabled={busy}
                    >
                      {m === 'template' ? 'Template (推荐)' : m === 'url' ? 'Custom URL (legacy)' : 'Filter JSON (advanced)'}
                    </Button>
                  ))}
                </div>

                {newMode === 'template' ? (
                  <div className="grid grid-cols-12 gap-2">
                    <select
                      className="col-span-9 h-9 rounded-md border border-[var(--k-border)] bg-[var(--k-surface)] px-3 text-sm outline-none focus:ring-2 focus:ring-[var(--k-ring)]"
                      value={newTemplateId}
                      onChange={(e) => setNewTemplateId(e.target.value)}
                      disabled={busy || templates.length === 0}
                    >
                      {templates.length === 0 ? (
                        <option value="">Loading templates...</option>
                      ) : (
                        templates.map((t) => (
                          <option key={t.templateId} value={t.templateId}>
                            {t.displayName} ({t.market.toUpperCase()})
                            {t.nestedFilterValidated ? '' : ' ⚠'}
                          </option>
                        ))
                      )}
                    </select>
                    <Button
                      className="col-span-3 h-9"
                      onClick={() => void addScreener()}
                      disabled={busy || !newTemplateId}
                    >
                      Save &amp; Enable
                    </Button>
                    {(() => {
                      const t = templates.find((x) => x.templateId === newTemplateId);
                      return t ? (
                        <div className="col-span-12 text-xs text-[var(--k-muted)]">
                          {t.description}
                          {!t.nestedFilterValidated ? (
                            <span className="ml-2 text-amber-600">
                              ⚠ Filter not yet validated against live API
                            </span>
                          ) : null}
                        </div>
                      ) : null;
                    })()}
                  </div>
                ) : null}

                {newMode === 'url' ? (
                  <div className="grid grid-cols-12 gap-2">
                    <input
                      className="col-span-3 h-9 rounded-md border border-[var(--k-border)] bg-[var(--k-surface)] px-3 text-sm outline-none focus:ring-2 focus:ring-[var(--k-ring)]"
                      placeholder="Name"
                      value={newName}
                      onChange={(e) => setNewName(e.target.value)}
                      disabled={busy}
                    />
                    <input
                      className="col-span-8 h-9 rounded-md border border-[var(--k-border)] bg-[var(--k-surface)] px-3 text-sm outline-none focus:ring-2 focus:ring-[var(--k-ring)]"
                      placeholder="https://www.tradingview.com/screener/..."
                      value={newUrl}
                      onChange={(e) => setNewUrl(e.target.value)}
                      disabled={busy}
                    />
                    <Button
                      className="col-span-1 h-9"
                      onClick={() => void addScreener()}
                      disabled={busy || !newUrl.trim()}
                    >
                      Add
                    </Button>
                  </div>
                ) : null}

                {newMode === 'json' ? (
                  <div className="grid grid-cols-12 gap-2">
                    <input
                      className="col-span-3 h-9 rounded-md border border-[var(--k-border)] bg-[var(--k-surface)] px-3 text-sm outline-none focus:ring-2 focus:ring-[var(--k-ring)]"
                      placeholder="Name"
                      value={newName}
                      onChange={(e) => setNewName(e.target.value)}
                      disabled={busy}
                    />
                    <textarea
                      className="col-span-12 min-h-[120px] rounded-md border border-[var(--k-border)] bg-[var(--k-surface)] px-3 py-2 font-mono text-xs outline-none focus:ring-2 focus:ring-[var(--k-ring)]"
                      placeholder='{"and": [{"left": "market_cap_basic", "operation": "greater", "right": 30000000000}, ...]}'
                      value={newFilterJson}
                      onChange={(e) => setNewFilterJson(e.target.value)}
                      disabled={busy}
                    />
                    <Button
                      className="col-span-12 h-9"
                      onClick={() => void addScreener()}
                      disabled={busy || !newFilterJson.trim()}
                    >
                      Save &amp; Enable
                    </Button>
                  </div>
                ) : null}

                <div className="mt-2 overflow-hidden rounded-lg border border-[var(--k-border)]">
                  <div className="grid grid-cols-12 gap-2 bg-[var(--k-surface-2)] px-3 py-2 text-xs text-[var(--k-muted)]">
                    <div className="col-span-3">Name</div>
                    <div className="col-span-2">Mode</div>
                    <div className="col-span-3">Source</div>
                    <div className="col-span-1 text-center">On</div>
                    <div className="col-span-3 text-right">Actions</div>
                  </div>
                  <div className="divide-y divide-[var(--k-border)]">
                    {screeners.map((it) => {
                      const editing = editingId === it.id;
                      const sourceLabel =
                        it.mode === 'api'
                          ? it.market
                            ? `API · ${it.market.toUpperCase()}`
                            : 'API'
                          : it.url
                            ? `Chrome · ${it.url.length > 32 ? `${it.url.slice(0, 32)}…` : it.url}`
                            : 'Chrome';
                      return (
                        <div key={it.id} className="grid grid-cols-12 gap-2 px-3 py-2">
                          <div className="col-span-3">
                            {editing ? (
                              <input
                                className="h-9 w-full rounded-md border border-[var(--k-border)] bg-[var(--k-surface)] px-3 text-sm outline-none focus:ring-2 focus:ring-[var(--k-ring)]"
                                value={editName}
                                onChange={(e) => setEditName(e.target.value)}
                                disabled={busy}
                              />
                            ) : (
                              <div className="truncate pt-2 text-sm">{it.name}</div>
                            )}
                          </div>
                          <div className="col-span-2 pt-2">
                            {editing ? (
                              <select
                                className="h-9 rounded-md border border-[var(--k-border)] bg-[var(--k-surface)] px-2 text-xs"
                                value={editMode}
                                onChange={(e) => setEditMode(e.target.value as 'api' | 'chrome')}
                                disabled={busy}
                              >
                                <option value="api">api</option>
                                <option value="chrome">chrome</option>
                              </select>
                            ) : (
                              <span
                                className={cn(
                                  'inline-block rounded-full border px-2 py-0.5 font-mono text-xs',
                                  it.mode === 'api'
                                    ? 'border-emerald-500/40 bg-emerald-500/10 text-emerald-700'
                                    : 'border-[var(--k-border)] bg-[var(--k-surface-2)] text-[var(--k-muted)]',
                                )}
                              >
                                {it.mode}
                              </span>
                            )}
                          </div>
                          <div className="col-span-3">
                            {editing ? (
                              editMode === 'chrome' ? (
                                <input
                                  className="h-9 w-full rounded-md border border-[var(--k-border)] bg-[var(--k-surface)] px-3 text-sm outline-none focus:ring-2 focus:ring-[var(--k-ring)]"
                                  placeholder="URL (chrome mode)"
                                  value={editUrl}
                                  onChange={(e) => setEditUrl(e.target.value)}
                                  disabled={busy}
                                />
                              ) : (
                                <textarea
                                  className="min-h-[80px] w-full rounded-md border border-[var(--k-border)] bg-[var(--k-surface)] px-3 py-2 font-mono text-xs"
                                  placeholder="filter_json"
                                  value={editFilterJson}
                                  onChange={(e) => setEditFilterJson(e.target.value)}
                                  disabled={busy}
                                />
                              )
                            ) : (
                              <div className="truncate pt-2 font-mono text-xs text-[var(--k-muted)]">
                                {sourceLabel}
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
                          <div className="col-span-3 flex items-center justify-end gap-1">
                            {editing ? (
                              <>
                                <Button
                                  variant="secondary"
                                  size="sm"
                                  className="h-8 px-2"
                                  onClick={() =>
                                    void saveScreener(it, {
                                      name: editName,
                                      url: editUrl,
                                      mode: editMode,
                                    }).then(() => setEditingId(null))
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
                                    setEditMode(it.mode);
                                    setEditFilterJson(
                                      it.filterJson ? JSON.stringify(it.filterJson, null, 2) : '',
                                    );
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
          </>
            </div>
  );
}
