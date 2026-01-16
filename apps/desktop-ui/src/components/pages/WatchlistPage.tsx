'use client';

import * as React from 'react';
import { Eye, Trash2 } from 'lucide-react';

import { Button } from '@/components/ui/button';
import { loadJson, saveJson } from '@/lib/storage';

type WatchlistItem = {
  symbol: string; // e.g. "CN:600000" or "HK:0700"
  note?: string | null;
  addedAt: string; // ISO
};

const STORAGE_KEY = 'karios.watchlist.v1';

function normalizeSymbolInput(input: string): { symbol: string } | { error: string } {
  const raw = (input || '').trim().toUpperCase();
  if (!raw) return { error: 'Empty input' };

  // Accept already-normalized market prefix forms.
  // Examples: "CN:600000", "HK:0700"
  if (/^(CN|HK):[0-9A-Z.\-]{1,16}$/.test(raw)) {
    return { symbol: raw };
  }

  // CN A-share ticker (6 digits)
  if (/^\d{6}$/.test(raw)) {
    return { symbol: `CN:${raw}` };
  }

  // HK ticker (4-5 digits), allow leading zeros
  if (/^\d{4,5}$/.test(raw)) {
    return { symbol: `HK:${raw.padStart(4, '0')}` };
  }

  return { error: 'Unsupported code format. Use 6-digit CN ticker, 4-5 digit HK ticker, or CN:/HK: prefixed symbol.' };
}

export function WatchlistPage({ onOpenStock }: { onOpenStock?: (symbol: string) => void } = {}) {
  const [items, setItems] = React.useState<WatchlistItem[]>([]);
  const [code, setCode] = React.useState('');
  const [note, setNote] = React.useState('');
  const [error, setError] = React.useState<string | null>(null);

  React.useEffect(() => {
    const saved = loadJson<WatchlistItem[]>(STORAGE_KEY, []);
    setItems(Array.isArray(saved) ? saved : []);
  }, []);

  function persist(next: WatchlistItem[]) {
    setItems(next);
    saveJson(STORAGE_KEY, next);
  }

  function onAdd() {
    setError(null);
    const parsed = normalizeSymbolInput(code);
    if ('error' in parsed) {
      setError(parsed.error);
      return;
    }
    const sym = parsed.symbol;
    if (items.some((x) => x.symbol === sym)) {
      setError('Already in watchlist.');
      return;
    }
    const next: WatchlistItem[] = [
      { symbol: sym, note: (note || '').trim() || null, addedAt: new Date().toISOString() },
      ...items,
    ];
    persist(next);
    setCode('');
    setNote('');
  }

  function onRemove(sym: string) {
    persist(items.filter((x) => x.symbol !== sym));
  }

  return (
    <div className="mx-auto w-full max-w-5xl p-6">
      <div className="mb-6">
        <div className="text-lg font-semibold">Watchlist</div>
        <div className="mt-1 text-sm text-[var(--k-muted)]">Manage the stocks you are watching.</div>
      </div>

      <section className="mb-4 rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] p-4">
        <div className="mb-2 text-sm font-medium">Add</div>
        <div className="grid gap-2 md:grid-cols-12">
          <input
            className="h-9 md:col-span-3 rounded-md border border-[var(--k-border)] bg-[var(--k-surface-2)] px-3 text-sm outline-none"
            placeholder="Ticker (e.g. 600000 / 0700 / CN:600000)"
            value={code}
            onChange={(e) => setCode(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === 'Enter') onAdd();
            }}
          />
          <input
            className="h-9 md:col-span-7 rounded-md border border-[var(--k-border)] bg-[var(--k-surface-2)] px-3 text-sm outline-none"
            placeholder="Note (optional)"
            value={note}
            onChange={(e) => setNote(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === 'Enter') onAdd();
            }}
          />
          <div className="md:col-span-2 flex gap-2">
            <Button size="sm" onClick={onAdd} disabled={!code.trim()}>
              Add
            </Button>
            <Button
              size="sm"
              variant="secondary"
              onClick={() => {
                setCode('');
                setNote('');
                setError(null);
              }}
              disabled={!code.trim() && !note.trim() && !error}
            >
              Clear
            </Button>
          </div>
        </div>
        {error ? (
          <div className="mt-2 text-sm text-red-600">{error}</div>
        ) : (
          <div className="mt-2 text-xs text-[var(--k-muted)]">
            Supported inputs: CN 6-digit ticker, HK 4-5 digit ticker, or prefixed symbol (CN:/HK:).
          </div>
        )}
      </section>

      <section className="rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] p-4">
        <div className="mb-2 flex items-center justify-between">
          <div className="text-sm font-medium">List</div>
          <div className="text-xs text-[var(--k-muted)]">{items.length} items</div>
        </div>

        {items.length ? (
          <div className="overflow-auto rounded border border-[var(--k-border)]">
            <table className="w-full border-collapse text-sm">
              <thead className="bg-[var(--k-surface)] text-[var(--k-muted)]">
                <tr className="text-left">
                  <th className="px-3 py-2">Symbol</th>
                  <th className="px-3 py-2">Note</th>
                  <th className="px-3 py-2 w-[120px]">Added</th>
                  <th className="px-3 py-2 w-[90px] text-right"> </th>
                </tr>
              </thead>
              <tbody>
                {items.map((it) => (
                  <tr key={it.symbol} className="border-t border-[var(--k-border)]">
                    <td className="px-3 py-2 font-mono">{it.symbol}</td>
                    <td className="px-3 py-2">{it.note || ''}</td>
                    <td className="px-3 py-2 text-xs text-[var(--k-muted)]">
                      {new Date(it.addedAt).toLocaleDateString()}
                    </td>
                    <td className="px-3 py-2 text-right">
                      <div className="flex justify-end gap-2">
                        <Button
                          variant="secondary"
                          size="icon"
                          className="h-8 w-8"
                          onClick={() => onOpenStock?.(it.symbol)}
                          disabled={!onOpenStock}
                          aria-label="Open"
                          title="Open"
                        >
                          <Eye className="h-4 w-4" />
                        </Button>
                        <Button
                          variant="ghost"
                          size="icon"
                          className="h-8 w-8"
                          onClick={() => onRemove(it.symbol)}
                          aria-label="Remove"
                          title="Remove"
                        >
                          <Trash2 className="h-4 w-4" />
                        </Button>
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <div className="text-sm text-[var(--k-muted)]">No items yet. Add a ticker above.</div>
        )}
      </section>
    </div>
  );
}

