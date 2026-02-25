'use client';

import * as React from 'react';
import Image from 'next/image';
import { RefreshCw, UploadCloud, X } from 'lucide-react';

import { Button } from '@/components/ui/button';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import { newId } from '@/lib/id';
import { DATA_SYNC_BASE_URL } from '@/lib/endpoints';
import { useChatStore } from '@/lib/chat/store';

type ImportImage = {
  id: string;
  name: string;
  mediaType: string;
  dataUrl: string;
};

type BrokerAccountState = {
  accountId: string;
  broker: string;
  updatedAt: string;
  overview: Record<string, unknown>;
  positions: Array<Record<string, unknown>>;
  conditionalOrders: Array<Record<string, unknown>>;
  trades: Array<Record<string, unknown>>;
  counts: Record<string, number>;
};

type BrokerAccount = {
  id: string;
  broker: string;
  title: string;
  accountMasked: string | null;
  updatedAt: string;
};

function toNum(v: unknown): number | null {
  if (typeof v === 'number' && Number.isFinite(v)) return v;
  if (typeof v !== 'string') return null;
  const s = v.trim().replaceAll(',', '');
  if (!s) return null;
  const n = Number(s);
  return Number.isFinite(n) ? n : null;
}

function formatWan(v: unknown): string {
  const n = toNum(v);
  if (n == null) return v != null ? String(v) : '-';
  return `${(n / 10000).toFixed(2)} 万`;
}

function pickStr(obj: Record<string, unknown>, keys: string[]): string {
  for (const k of keys) {
    const v = obj[k];
    if (v == null) continue;
    const s = String(v).trim();
    if (s) return s;
  }
  return '';
}

async function apiGetJson<T>(path: string): Promise<T> {
  const res = await fetch(`${DATA_SYNC_BASE_URL}${path}`, { cache: 'no-store' });
  const txt = await res.text().catch(() => '');
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}${txt ? `: ${txt}` : ''}`);
  return txt ? (JSON.parse(txt) as T) : ({} as T);
}

async function apiPostJson<T>(path: string, body: unknown): Promise<T> {
  const res = await fetch(`${DATA_SYNC_BASE_URL}${path}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  });
  const txt = await res.text().catch(() => '');
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}${txt ? `: ${txt}` : ''}`);
  return txt ? (JSON.parse(txt) as T) : ({} as T);
}

async function apiPutJson<T>(path: string, body: unknown): Promise<T> {
  const res = await fetch(`${DATA_SYNC_BASE_URL}${path}`, {
    method: 'PUT',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  });
  const txt = await res.text().catch(() => '');
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}${txt ? `: ${txt}` : ''}`);
  return txt ? (JSON.parse(txt) as T) : ({} as T);
}

export function BrokerPage() {
  const { addReference } = useChatStore();
  const [images, setImages] = React.useState<ImportImage[]>([]);
  const [busy, setBusy] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);
  const [state, setState] = React.useState<BrokerAccountState | null>(null);
  const [accounts, setAccounts] = React.useState<BrokerAccount[]>([]);
  const [accountId, setAccountId] = React.useState<string>('');
  const [showNewAccount, setShowNewAccount] = React.useState(false);
  const [newAccountTitle, setNewAccountTitle] = React.useState('');
  const [newAccountMasked, setNewAccountMasked] = React.useState('');
  const [showRenameAccount, setShowRenameAccount] = React.useState(false);
  const [renameAccountTitle, setRenameAccountTitle] = React.useState('');
  const [showAllPositions, setShowAllPositions] = React.useState(false);

  const refresh = React.useCallback(async () => {
    setError(null);
    try {
      const acc = await apiGetJson<BrokerAccount[]>('/broker/accounts?broker=pingan');
      setAccounts(acc);
      const effectiveAccountId = accountId || acc[0]?.id || '';
      if (!accountId && effectiveAccountId) setAccountId(effectiveAccountId);
      if (effectiveAccountId) {
        const st = await apiGetJson<BrokerAccountState>(
          `/broker/pingan/accounts/${encodeURIComponent(effectiveAccountId)}/state`,
        );
        setState(st);
      } else {
        setState(null);
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  }, [accountId]);

  React.useEffect(() => {
    void refresh();
  }, [refresh]);

  React.useEffect(() => {
    if (!showRenameAccount) return;
    const onKeyDown = (e: KeyboardEvent) => {
      if (e.key === 'Escape') setShowRenameAccount(false);
    };
    window.addEventListener('keydown', onKeyDown);
    return () => window.removeEventListener('keydown', onKeyDown);
  }, [showRenameAccount]);

  const addImageFiles = React.useCallback(async (files: File[]) => {
    const next: ImportImage[] = [];
    for (const file of files) {
      if (!file.type.startsWith('image/')) continue;
      if (file.size > 8 * 1024 * 1024) continue; // v0: avoid huge payloads
      const dataUrl = await new Promise<string>((resolve) => {
        const reader = new FileReader();
        reader.onload = () => resolve(String(reader.result ?? ''));
        reader.readAsDataURL(file);
      });
      next.push({
        id: newId(),
        name: file.name || 'screenshot',
        mediaType: file.type || 'image/*',
        dataUrl,
      });
    }
    if (next.length) setImages((prev) => [...prev, ...next]);
  }, []);

  async function onImport() {
    if (!images.length) return;
    setBusy(true);
    setError(null);
    try {
      if (!accountId) throw new Error('Select an account first');
      const st = await apiPostJson<BrokerAccountState>(
        `/broker/pingan/accounts/${encodeURIComponent(accountId)}/sync`,
        {
          capturedAt: new Date().toISOString(),
          images,
        },
      );
      setImages([]);
      setState(st);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }

  async function onCreateAccount() {
    const title = newAccountTitle.trim();
    if (!title) return;
    setBusy(true);
    setError(null);
    try {
      const created = await apiPostJson<BrokerAccount>('/broker/accounts', {
        broker: 'pingan',
        title,
        accountMasked: newAccountMasked.trim() || null,
      });
      setNewAccountTitle('');
      setNewAccountMasked('');
      setShowNewAccount(false);
      setAccountId(created.id);
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }

  async function onRenameAccount() {
    const title = renameAccountTitle.trim();
    if (!accountId) return;
    if (!title) return;
    setBusy(true);
    setError(null);
    try {
      await apiPutJson<{ ok: boolean }>(`/broker/accounts/${encodeURIComponent(accountId)}`, { title });
      setShowRenameAccount(false);
      await refresh();
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
          <div className="text-lg font-semibold">Broker Sync (Ping An)</div>
          <div className="mt-1 text-sm text-[var(--k-muted)]">
            Paste or drop screenshots, extract with AI, and save into Postgres.
          </div>
        </div>
        <div className="flex items-center gap-2">
          <Select value={accountId} onValueChange={(v) => setAccountId(v)}>
            <SelectTrigger className="h-9 w-[220px]">
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
          <Button
            variant="secondary"
            size="sm"
            onClick={() => {
              if (!accountId) return;
              const acct = accounts.find((a) => a.id === accountId);
              setRenameAccountTitle((acct?.title ?? '').trim());
              setShowRenameAccount(true);
            }}
            disabled={busy || !accountId}
            title="Rename account"
          >
            Rename
          </Button>
          <Button
            variant="secondary"
            size="sm"
            onClick={() => setShowNewAccount((v) => !v)}
            disabled={busy}
          >
            New account
          </Button>
          <Button variant="secondary" size="sm" onClick={() => void refresh()} className="gap-2">
            <RefreshCw className="h-4 w-4" />
            Refresh
          </Button>
        </div>
      </div>

      {showRenameAccount ? (
        <div
          className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4"
          role="dialog"
          aria-modal="true"
          aria-label="Rename account"
          onMouseDown={(e) => {
            if (e.target === e.currentTarget) setShowRenameAccount(false);
          }}
        >
          <div className="w-full max-w-md rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] p-4 shadow-xl">
            <div className="mb-2 text-sm font-medium">Rename account</div>
            <div className="text-xs text-[var(--k-muted)]">
              This updates the account title in SQLite and affects all modules.
            </div>
            <div className="mt-3 grid gap-2">
              <input
                className="h-9 rounded-md border border-[var(--k-border)] bg-[var(--k-surface-2)] px-3 text-sm outline-none"
                placeholder="New title"
                value={renameAccountTitle}
                onChange={(e) => setRenameAccountTitle(e.target.value)}
                maxLength={64}
                autoFocus
              />
              <div className="flex justify-end gap-2">
                <Button
                  variant="secondary"
                  size="sm"
                  onClick={() => setShowRenameAccount(false)}
                  disabled={busy}
                >
                  Cancel
                </Button>
                <Button size="sm" onClick={() => void onRenameAccount()} disabled={busy || !renameAccountTitle.trim()}>
                  Save
                </Button>
              </div>
            </div>
          </div>
        </div>
      ) : null}

      {showNewAccount ? (
        <div className="mb-4 rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] p-4">
          <div className="mb-2 text-sm font-medium">Create account</div>
          <div className="grid gap-2 md:grid-cols-3">
            <input
              className="h-9 rounded-md border border-[var(--k-border)] bg-[var(--k-surface-2)] px-3 text-sm outline-none"
              placeholder="Title (e.g. PingAn Main)"
              value={newAccountTitle}
              onChange={(e) => setNewAccountTitle(e.target.value)}
            />
            <input
              className="h-9 rounded-md border border-[var(--k-border)] bg-[var(--k-surface-2)] px-3 text-sm outline-none"
              placeholder="Account masked (optional, e.g. 3260****7775)"
              value={newAccountMasked}
              onChange={(e) => setNewAccountMasked(e.target.value)}
            />
            <div className="flex gap-2">
              <Button
                size="sm"
                onClick={() => void onCreateAccount()}
                disabled={busy || !newAccountTitle.trim()}
              >
                Create
              </Button>
              <Button
                variant="secondary"
                size="sm"
                onClick={() => setShowNewAccount(false)}
                disabled={busy}
              >
                Cancel
              </Button>
            </div>
          </div>
        </div>
      ) : null}

      {error ? (
        <div className="mb-4 rounded-lg border border-red-500/30 bg-red-500/10 px-3 py-2 text-sm text-red-600">
          {error}
        </div>
      ) : null}

      {state ? (
        <section className="mb-4 rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] p-4">
          <div className="flex flex-wrap items-center justify-between gap-3">
            <div className="min-w-0">
              <div className="font-medium">Account state</div>
              <div className="mt-1 text-xs text-[var(--k-muted)]">
                Updated: {new Date(state.updatedAt).toLocaleString()} • positions{' '}
                {Number(state.counts?.positions ?? state.positions.length)}
              </div>
            </div>
            <Button
              size="sm"
              disabled={!accountId}
              onClick={() => {
                const acct = accounts.find((a) => a.id === accountId);
                addReference({
                  kind: 'brokerState',
                  refId: accountId,
                  broker: 'pingan',
                  accountId,
                  accountTitle: acct
                    ? `${acct.title}${acct.accountMasked ? ` (${acct.accountMasked})` : ''}`
                    : 'PingAn',
                  capturedAt: new Date().toISOString(),
                });
              }}
            >
              Reference account to chat
            </Button>
          </div>

          <div className="mt-4 grid gap-4 md:grid-cols-2">
            <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface-2)] p-3">
              <div className="mb-2 text-sm font-medium">Overview</div>
              <div className="grid grid-cols-2 gap-2 text-xs">
                {(() => {
                  const ov = state.overview || {};
                  return [
                    ['totalAssets', 'Total assets', formatWan(ov.totalAssets)],
                    ['securitiesValue', 'Securities', formatWan(ov.securitiesValue)],
                    ['cashAvailable', 'Cash available', formatWan(ov.cashAvailable)],
                    ['withdrawable', 'Withdrawable', formatWan(ov.withdrawable)],
                    ['pnlTotal', 'PnL total', formatWan(ov.pnlTotal)],
                    ['pnlToday', 'PnL today', formatWan(ov.pnlToday)],
                  ].map(([k, label, value]) => (
                    <div key={String(k)} className="flex items-center justify-between gap-2">
                      <div className="text-[var(--k-muted)]">{String(label)}</div>
                      <div className="truncate font-mono">{String(value)}</div>
                    </div>
                  ));
                })()}
              </div>
            </div>

            <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface-2)] p-3">
              <div className="mb-1 flex items-center justify-between gap-2">
                <div className="text-sm font-medium">Positions</div>
                {state.positions.length > 12 ? (
                  <Button
                    variant="ghost"
                    size="sm"
                    className="h-7 px-2 text-xs text-[var(--k-muted)]"
                    onClick={() => setShowAllPositions((v) => !v)}
                  >
                    {showAllPositions ? 'Show less' : `Show all (${state.positions.length})`}
                  </Button>
                ) : null}
              </div>
              <div className="mb-2 text-xs text-[var(--k-muted)]">
                Positions = your current holdings. Upload a holdings screenshot (持仓) to populate.
              </div>
              {state.positions.length ? (
                <div
                  className="overflow-auto rounded border border-[var(--k-border)]"
                  style={{ maxHeight: showAllPositions ? 360 : undefined }}
                >
                  <table className="w-full border-collapse text-xs">
                    <thead className="bg-[var(--k-surface)] text-[var(--k-muted)]">
                      <tr className="text-left">
                        <th className="px-2 py-1">Ticker</th>
                        <th className="px-2 py-1">Name</th>
                        <th className="px-2 py-1">Qty</th>
                        <th className="px-2 py-1">Price</th>
                        <th className="px-2 py-1">PnL%</th>
                      </tr>
                    </thead>
                    <tbody>
                      {(showAllPositions ? state.positions : state.positions.slice(0, 12)).map(
                        (p, idx) => {
                          const ticker = pickStr(p, ['ticker', 'Ticker', 'symbol', 'Symbol']);
                          const name = pickStr(p, ['name', 'Name']);
                          const qty = pickStr(p, [
                            'qtyHeld',
                            'qty',
                            'quantity',
                            '持仓',
                            '持仓/可用',
                          ]);
                          const price = pickStr(p, ['price', '现价', 'last']);
                          const pnlPct = pickStr(p, ['pnlPct', 'pnl%', '盈亏%', 'PnlPct']);
                          return (
                            <tr key={idx} className="border-t border-[var(--k-border)]">
                              <td className="px-2 py-1 font-mono">{ticker}</td>
                              <td className="px-2 py-1">{name}</td>
                              <td className="px-2 py-1 font-mono">{qty}</td>
                              <td className="px-2 py-1 font-mono">{price}</td>
                              <td className="px-2 py-1 font-mono">{pnlPct}</td>
                            </tr>
                          );
                        },
                      )}
                    </tbody>
                  </table>
                </div>
              ) : (
                <div className="text-xs text-[var(--k-muted)]">No positions in current state.</div>
              )}
            </div>
          </div>

        </section>
      ) : null}

      <div className="grid grid-cols-1 gap-4">
        <section className="rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] p-4">
          <div className="mb-3 flex items-center justify-between">
            <div className="font-medium">Import screenshots</div>
            <Button
              size="sm"
              disabled={busy || images.length === 0}
              onClick={() => void onImport()}
            >
              {busy ? 'Analyzing…' : 'Analyze & Save'}
            </Button>
          </div>

          <div
            className="rounded-lg border border-dashed border-[var(--k-border)] bg-[var(--k-surface-2)] p-4"
            onDragOver={(e) => e.preventDefault()}
            onDrop={(e) => {
              e.preventDefault();
              const files = Array.from(e.dataTransfer.files || []);
              void addImageFiles(files);
            }}
            onPaste={(e) => {
              const files = Array.from(e.clipboardData?.files || []);
              if (files.length) void addImageFiles(files);
            }}
          >
            <div className="flex items-center gap-2 text-sm text-[var(--k-muted)]">
              <UploadCloud className="h-4 w-4" />
              Drop images here or paste from clipboard.
            </div>

            {images.length ? (
              <div className="mt-3 grid grid-cols-4 gap-2">
                {images.map((img) => (
                  <div
                    key={img.id}
                    className="group relative overflow-hidden rounded-md border border-[var(--k-border)]"
                  >
                    <Image
                      src={img.dataUrl}
                      alt={img.name}
                      width={220}
                      height={220}
                      className="h-20 w-full object-cover"
                      unoptimized
                    />
                    <button
                      type="button"
                      className="absolute right-1 top-1 hidden rounded bg-black/60 p-1 text-white group-hover:block"
                      onClick={() => setImages((prev) => prev.filter((x) => x.id !== img.id))}
                      aria-label="Remove"
                    >
                      <X className="h-3 w-3" />
                    </button>
                  </div>
                ))}
              </div>
            ) : null}
          </div>
        </section>
      </div>
    </div>
  );
}
