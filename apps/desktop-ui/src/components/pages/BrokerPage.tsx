'use client';

import * as React from 'react';
import Image from 'next/image';
import { RefreshCw, UploadCloud, X } from 'lucide-react';

import { Button } from '@/components/ui/button';
import { newId } from '@/lib/id';
import { QUANT_BASE_URL } from '@/lib/endpoints';

type ImportImage = {
  id: string;
  name: string;
  mediaType: string;
  dataUrl: string;
};

type BrokerSnapshotSummary = {
  id: string;
  broker: string;
  capturedAt: string;
  kind: string;
  createdAt: string;
};

type BrokerSnapshotDetail = BrokerSnapshotSummary & {
  imagePath: string;
  extracted: Record<string, unknown>;
};

async function apiGetJson<T>(path: string): Promise<T> {
  const res = await fetch(`${QUANT_BASE_URL}${path}`, { cache: 'no-store' });
  const txt = await res.text().catch(() => '');
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}${txt ? `: ${txt}` : ''}`);
  return (txt ? (JSON.parse(txt) as T) : ({} as T));
}

async function apiPostJson<T>(path: string, body: unknown): Promise<T> {
  const res = await fetch(`${QUANT_BASE_URL}${path}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  });
  const txt = await res.text().catch(() => '');
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}${txt ? `: ${txt}` : ''}`);
  return (txt ? (JSON.parse(txt) as T) : ({} as T));
}

export function BrokerPage() {
  const [images, setImages] = React.useState<ImportImage[]>([]);
  const [busy, setBusy] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);
  const [snapshots, setSnapshots] = React.useState<BrokerSnapshotSummary[]>([]);
  const [selected, setSelected] = React.useState<BrokerSnapshotDetail | null>(null);

  const refresh = React.useCallback(async () => {
    setError(null);
    try {
      const items = await apiGetJson<BrokerSnapshotSummary[]>('/broker/pingan/snapshots?limit=30');
      setSnapshots(items);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  }, []);

  React.useEffect(() => {
    void refresh();
  }, [refresh]);

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
      await apiPostJson<{ ok: boolean; items: BrokerSnapshotSummary[] }>('/broker/pingan/import', {
        capturedAt: new Date().toISOString(),
        images,
      });
      setImages([]);
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
            Paste or drop screenshots, extract with AI, and save into SQLite.
          </div>
        </div>
        <Button variant="secondary" size="sm" onClick={() => void refresh()} className="gap-2">
          <RefreshCw className="h-4 w-4" />
          Refresh
        </Button>
      </div>

      {error ? (
        <div className="mb-4 rounded-lg border border-red-500/30 bg-red-500/10 px-3 py-2 text-sm text-red-600">
          {error}
        </div>
      ) : null}

      <div className="grid grid-cols-1 gap-4 md:grid-cols-2">
        <section className="rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] p-4">
          <div className="mb-3 flex items-center justify-between">
            <div className="font-medium">Import screenshots</div>
            <Button size="sm" disabled={busy || images.length === 0} onClick={() => void onImport()}>
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
                  <div key={img.id} className="group relative overflow-hidden rounded-md border border-[var(--k-border)]">
                    <Image
                      src={img.dataUrl}
                      alt={img.name}
                      width={220}
                      height={220}
                      className="h-20 w-full object-cover"
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

        <section className="rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] p-4">
          <div className="mb-3 font-medium">Recent imports</div>
          <div className="max-h-[420px] overflow-auto rounded-lg border border-[var(--k-border)]">
            {snapshots.length ? (
              <div className="divide-y divide-[var(--k-border)]">
                {snapshots.map((s) => (
                  <button
                    key={s.id}
                    type="button"
                    className="w-full px-3 py-2 text-left hover:bg-[var(--k-surface-2)]"
                    onClick={() => {
                      void (async () => {
                        const d = await apiGetJson<BrokerSnapshotDetail>(
                          `/broker/pingan/snapshots/${encodeURIComponent(s.id)}`,
                        );
                        setSelected(d);
                      })();
                    }}
                  >
                    <div className="flex items-center justify-between gap-2">
                      <div className="min-w-0">
                        <div className="truncate text-sm font-medium">{s.kind}</div>
                        <div className="truncate text-xs text-[var(--k-muted)]">
                          {new Date(s.capturedAt).toLocaleString()}
                        </div>
                      </div>
                      <div className="text-xs text-[var(--k-muted)]">{s.broker}</div>
                    </div>
                  </button>
                ))}
              </div>
            ) : (
              <div className="px-3 py-6 text-center text-sm text-[var(--k-muted)]">No imports yet.</div>
            )}
          </div>
        </section>
      </div>

      {selected ? (
        <section className="mt-4 rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] p-4">
          <div className="mb-3 flex items-center justify-between gap-2">
            <div className="min-w-0">
              <div className="truncate font-medium">Snapshot detail</div>
              <div className="truncate text-xs text-[var(--k-muted)]">{selected.id}</div>
            </div>
            <Button variant="secondary" size="sm" onClick={() => setSelected(null)}>
              Close
            </Button>
          </div>

          <div className="grid gap-3 md:grid-cols-2">
            <div className="overflow-hidden rounded-lg border border-[var(--k-border)]">
              <Image
                src={`${QUANT_BASE_URL}/broker/pingan/snapshots/${encodeURIComponent(selected.id)}/image`}
                alt="screenshot"
                width={640}
                height={1280}
                className="h-auto w-full object-contain"
              />
            </div>
            <pre className="max-h-[520px] overflow-auto rounded-lg border border-[var(--k-border)] bg-[var(--k-surface-2)] p-3 text-xs">
{JSON.stringify(selected.extracted, null, 2)}
            </pre>
          </div>
        </section>
      ) : null}
    </div>
  );
}


