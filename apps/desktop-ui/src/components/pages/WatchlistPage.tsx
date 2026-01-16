'use client';

import * as React from 'react';
import { Eye, Info, RefreshCw, Trash2 } from 'lucide-react';
import { createPortal } from 'react-dom';

import { Button } from '@/components/ui/button';
import { QUANT_BASE_URL } from '@/lib/endpoints';
import { loadJson, saveJson } from '@/lib/storage';

type WatchlistItem = {
  symbol: string; // e.g. "CN:600000" or "HK:0700"
  name?: string | null;
  nameStatus?: 'resolved' | 'not_found';
  addedAt: string; // ISO
};

const STORAGE_KEY = 'karios.watchlist.v1';

type MarketStockBasicRow = {
  symbol: string;
  market: string;
  ticker: string;
  name: string;
  currency: string;
};

type TrendOkChecks = {
  emaOrder?: boolean | null;
  macdPositive?: boolean | null;
  macdHistExpanding?: boolean | null;
  closeNear20dHigh?: boolean | null;
  rsiInRange?: boolean | null;
  volumeSurge?: boolean | null;
};

type TrendOkValues = {
  close?: number | null;
  ema5?: number | null;
  ema20?: number | null;
  ema60?: number | null;
  macd?: number | null;
  macdSignal?: number | null;
  macdHist?: number | null;
  macdHist4?: number[];
  rsi14?: number | null;
  high20?: number | null;
  avgVol5?: number | null;
  avgVol30?: number | null;
};

type TrendOkResult = {
  symbol: string;
  name?: string | null;
  asOfDate?: string | null;
  trendOk?: boolean | null;
  checks?: TrendOkChecks;
  values?: TrendOkValues;
  missingData?: string[];
};

type TvScreener = {
  id: string;
  name: string;
  url: string;
  enabled: boolean;
  updatedAt: string;
};

type TvSnapshotSummary = {
  id: string;
  screenerId: string;
  capturedAt: string;
  rowCount: number;
};

type TvSnapshotDetail = {
  id: string;
  screenerId: string;
  capturedAt: string;
  rowCount: number;
  screenTitle: string | null;
  filters: string[];
  url: string;
  headers: string[];
  rows: Record<string, string>[];
};

async function apiGetJson<T>(path: string): Promise<T> {
  const res = await fetch(`${QUANT_BASE_URL}${path}`, { cache: 'no-store' });
  const txt = await res.text().catch(() => '');
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}${txt ? `: ${txt}` : ''}`);
  return txt ? (JSON.parse(txt) as T) : ({} as T);
}

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

  return {
    error:
      'Unsupported code format. Use 6-digit CN ticker, 4-5 digit HK ticker, or CN:/HK: prefixed symbol.',
  };
}

function normalizeScreenerSymbol(raw: string): string | null {
  const s = String(raw || '')
    .trim()
    .toUpperCase();
  if (!s) return null;

  // Try the same rules as manual input first.
  const parsed = normalizeSymbolInput(s);
  if (!('error' in parsed)) return parsed.symbol;

  // TradingView forms like "SSE:600000" / "SZSE:000001" / "HKEX:0700"
  const m = s.match(/^[A-Z]+:(\d{4,6})$/);
  if (m) {
    const code = m[1];
    if (/^\d{6}$/.test(code)) return `CN:${code}`;
    if (/^\d{4,5}$/.test(code)) return `HK:${code.padStart(4, '0')}`;
  }
  return null;
}

function chunk<T>(arr: T[], n: number): T[][] {
  const out: T[][] = [];
  for (let i = 0; i < arr.length; i += n) out.push(arr.slice(i, i + n));
  return out;
}

export function WatchlistPage({ onOpenStock }: { onOpenStock?: (symbol: string) => void } = {}) {
  const [items, setItems] = React.useState<WatchlistItem[]>([]);
  const [code, setCode] = React.useState('');
  const [error, setError] = React.useState<string | null>(null);
  const [trend, setTrend] = React.useState<Record<string, TrendOkResult>>({});
  const [syncBusy, setSyncBusy] = React.useState(false);
  const [syncMsg, setSyncMsg] = React.useState<string | null>(null);
  const [tooltip, setTooltip] = React.useState<{
    open: boolean;
    x: number;
    y: number;
    w: number;
    content: React.ReactNode;
  }>({ open: false, x: 0, y: 0, w: 0, content: null });

  React.useEffect(() => {
    const saved = loadJson<WatchlistItem[]>(STORAGE_KEY, []);
    // Backward-compatible migration: drop deprecated fields (e.g. note).
    const arr = Array.isArray(saved) ? saved : [];
    const migrated: WatchlistItem[] = arr
      .filter((x) => x && typeof x === 'object')
      .map((x) => {
        const it = x as Partial<WatchlistItem> & { note?: unknown };
        return {
          symbol: String(it.symbol ?? '').trim(),
          name: it.name ?? null,
          nameStatus:
            it.nameStatus === 'resolved' || it.nameStatus === 'not_found'
              ? it.nameStatus
              : undefined,
          addedAt: String(it.addedAt ?? new Date().toISOString()),
        };
      })
      .filter((x) => Boolean(x.symbol));
    setItems(migrated);
    saveJson(STORAGE_KEY, migrated);
  }, []);

  function persist(next: WatchlistItem[]) {
    setItems(next);
    saveJson(STORAGE_KEY, next);
  }

  React.useEffect(() => {
    let cancelled = false;
    async function resolveMissingNames() {
      const missing = items
        .filter((x) => !x.name && x.nameStatus !== 'not_found')
        .map((x) => x.symbol);
      if (!missing.length) return;

      try {
        const sp = new URLSearchParams();
        for (const s of missing) sp.append('symbols', s);
        const rows = await apiGetJson<MarketStockBasicRow[]>(
          `/market/stocks/resolve?${sp.toString()}`,
        );
        if (cancelled) return;
        const bySym = new Map<string, MarketStockBasicRow>();
        for (const r of Array.isArray(rows) ? rows : []) bySym.set(r.symbol, r);

        const next = items.map((it) => {
          if (it.name || it.nameStatus === 'resolved') return it;
          const hit = bySym.get(it.symbol);
          if (hit) return { ...it, name: hit.name, nameStatus: 'resolved' as const };
          if (missing.includes(it.symbol)) return { ...it, nameStatus: 'not_found' as const };
          return it;
        });
        persist(next);
      } catch (e) {
        // If Market is not synced or service is unavailable, keep silent; user can still manage codes.
        if (!cancelled) console.warn('Watchlist name resolve failed:', e);
      }
    }
    void resolveMissingNames();
    return () => {
      cancelled = true;
    };
  }, [items]);

  React.useEffect(() => {
    let cancelled = false;
    async function loadTrendOk() {
      const syms = items.map((x) => x.symbol).filter(Boolean);
      if (!syms.length) {
        setTrend({});
        return;
      }
      try {
        const sp = new URLSearchParams();
        for (const s of syms) sp.append('symbols', s);
        const rows = await apiGetJson<TrendOkResult[]>(`/market/stocks/trendok?${sp.toString()}`);
        if (cancelled) return;
        const next: Record<string, TrendOkResult> = {};
        for (const r of Array.isArray(rows) ? rows : []) {
          if (r && r.symbol) next[r.symbol] = r;
        }
        setTrend(next);
      } catch (e) {
        if (!cancelled) console.warn('Watchlist trendok load failed:', e);
      }
    }
    void loadTrendOk();
    return () => {
      cancelled = true;
    };
  }, [items]);

  function onAdd() {
    setError(null);
    setSyncMsg(null);
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
      {
        symbol: sym,
        name: null,
        addedAt: new Date().toISOString(),
      },
      ...items,
    ];
    persist(next);
    setCode('');
  }

  function onRemove(sym: string) {
    setSyncMsg(null);
    persist(items.filter((x) => x.symbol !== sym));
  }

  async function onSyncFromScreener() {
    setError(null);
    setSyncMsg(null);
    setSyncBusy(true);
    try {
      const s = await apiGetJson<{ items: TvScreener[] }>('/integrations/tradingview/screeners');
      const enabled = (s.items || []).filter((x) => x && x.enabled);
      if (!enabled.length) {
        setSyncMsg('No enabled screeners.');
        return;
      }

      const snapshotDetails = await Promise.all(
        enabled.map(async (sc) => {
          const list = await apiGetJson<{ items: TvSnapshotSummary[] }>(
            `/integrations/tradingview/screeners/${encodeURIComponent(sc.id)}/snapshots?limit=1`,
          );
          const latest = list.items?.[0];
          if (!latest) return null;
          return await apiGetJson<TvSnapshotDetail>(
            `/integrations/tradingview/snapshots/${encodeURIComponent(latest.id)}`,
          );
        }),
      );

      const candidates: string[] = [];
      for (const snap of snapshotDetails) {
        if (!snap) continue;
        for (const r of snap.rows || []) {
          const raw = String(r['Ticker'] || r['Symbol'] || '').trim();
          const sym = normalizeScreenerSymbol(raw);
          if (sym) candidates.push(sym);
        }
      }

      const uniq = Array.from(new Set(candidates)).slice(0, 2000);
      if (!uniq.length) {
        setSyncMsg('No symbols found in latest screener snapshots.');
        return;
      }

      // Batch TrendOK checks (backend caps at 200; we chunk explicitly).
      const okSyms: string[] = [];
      for (const part of chunk(uniq, 200)) {
        const sp = new URLSearchParams();
        for (const s2 of part) sp.append('symbols', s2);
        const rows = await apiGetJson<TrendOkResult[]>(`/market/stocks/trendok?${sp.toString()}`);
        for (const rr of Array.isArray(rows) ? rows : []) {
          if (rr && rr.symbol && rr.trendOk === true) okSyms.push(rr.symbol);
        }
      }
      const okUniq = Array.from(new Set(okSyms));

      const existing = new Set(items.map((x) => x.symbol));
      const now = new Date().toISOString();
      const added: WatchlistItem[] = okUniq
        .filter((sym) => !existing.has(sym))
        .map((sym) => ({ symbol: sym, name: null, addedAt: now }));

      if (!added.length) {
        setSyncMsg(
          `Screener scanned ${uniq.length} symbols; TrendOK ✅: ${okUniq.length}; nothing new to add.`,
        );
        return;
      }

      persist([...added, ...items]);
      setSyncMsg(`Added ${added.length} TrendOK ✅ stocks from screener (scanned ${uniq.length}).`);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setSyncBusy(false);
    }
  }

  function showTooltip(el: HTMLElement, content: React.ReactNode, width = 360) {
    const r = el.getBoundingClientRect();
    const x = Math.max(8, Math.min(window.innerWidth - width - 8, r.left));
    const y = Math.min(window.innerHeight - 16, r.bottom + 8);
    setTooltip({ open: true, x, y, w: width, content });
  }

  function hideTooltip() {
    setTooltip((prev) => (prev.open ? { ...prev, open: false } : prev));
  }

  function checkLine(label: string, ok: boolean | null | undefined, detail: string) {
    if (ok == null) return { label, state: '—', detail };
    return { label, state: ok ? '✅' : '❌', detail };
  }

  function renderTrendOkCell(sym: string) {
    const t = trend[sym];
    const ok = t?.trendOk ?? null;
    const icon = ok == null ? '—' : ok ? '✅' : '❌';
    const rsiNow =
      typeof t?.values?.rsi14 === 'number' && Number.isFinite(t.values.rsi14)
        ? t.values.rsi14
        : null;
    const h4 =
      Array.isArray(t?.values?.macdHist4) && t?.values?.macdHist4?.length === 4
        ? t.values.macdHist4
        : null;
    const hpos = h4 ? h4.map((x) => Math.max(0, Number(x))) : null;
    const d1 = hpos ? hpos[1] > hpos[0] : null;
    const d2 = hpos ? hpos[2] > hpos[1] : null;
    const d3 = hpos ? hpos[3] > hpos[2] : null;
    const hLastPos = hpos ? hpos[3] > 0 : null;
    const macdHistDetail = h4
      ? `need h_last>0: ${hLastPos ? '✅' : '❌'}; d1 ${d1 ? '✅' : '❌'}; d2 ${
          d2 ? '✅' : '❌'
        }; d3 ${d3 ? '✅' : '❌'} (h: ${h4
          .map((x) => (Number.isFinite(Number(x)) ? Number(x).toFixed(3) : '—'))
          .join(', ')})`
      : 'need last 4 histogram values';
    const lines = [
      checkLine('EMA order', t?.checks?.emaOrder ?? null, 'EMA(5) > EMA(20) > EMA(60)'),
      checkLine('MACD > 0', t?.checks?.macdPositive ?? null, 'macdLine > 0'),
      checkLine(
        'MACD hist',
        t?.checks?.macdHistExpanding ?? null,
        `last 4 days: (hist>0) and >=2 increases (positive-part); ${macdHistDetail}`,
      ),
      checkLine('Near 20D high', t?.checks?.closeNear20dHigh ?? null, 'Close >= 0.95 * High(20)'),
      checkLine(
        'RSI(14)',
        t?.checks?.rsiInRange ?? null,
        `50 <= RSI <= 75${rsiNow == null ? '' : ` (now: ${rsiNow.toFixed(1)})`}`,
      ),
      checkLine('Volume surge', t?.checks?.volumeSurge ?? null, 'AvgVol(5) > 1.2 * AvgVol(30)'),
    ];
    const missing = (t?.missingData ?? []).filter(Boolean);
    const tip = (
      <>
        <div className="mb-2 flex items-center justify-between">
          <div className="font-medium">TrendOK checks</div>
          <div className="font-mono text-[var(--k-muted)]">{sym}</div>
        </div>
        <div className="space-y-1">
          {lines.map((x) => (
            <div key={x.label} className="flex items-start justify-between gap-3">
              <div className="text-[var(--k-muted)]">{x.label}</div>
              <div className="flex-1 text-right">
                <span className="font-mono">{x.state}</span>{' '}
                <span className="text-[var(--k-muted)]">{x.detail}</span>
              </div>
            </div>
          ))}
        </div>
        {missing.length ? (
          <div className="mt-2 text-[var(--k-muted)]">
            Missing: <span className="font-mono">{missing.join(', ')}</span>
          </div>
        ) : null}
      </>
    );
    return (
      <button
        type="button"
        className="inline-flex items-center"
        onMouseEnter={(e) => showTooltip(e.currentTarget, tip, 360)}
        onMouseLeave={hideTooltip}
        onFocus={(e) => showTooltip(e.currentTarget, tip, 360)}
        onBlur={hideTooltip}
        aria-label="TrendOK details"
      >
        <span className="font-mono">{icon}</span>
      </button>
    );
  }

  const headerTip = (
    <>
      <div className="mb-2 font-medium">Definition (CN daily)</div>
      <div className="space-y-1 text-[var(--k-muted)]">
        <div>✅ only when ALL rules are satisfied.</div>
        <div>— when data/indicators are insufficient.</div>
      </div>
      <div className="mt-2 space-y-1">
        <div>1) EMA(5) &gt; EMA(20) &gt; EMA(60)</div>
        <div>2) MACD line &gt; 0</div>
        <div>3) MACD histogram expanding: last 4 days, at least 2 day-over-day increases</div>
        <div>4) Close ≥ 0.95 × High(20)</div>
        <div>5) RSI(14) in [50, 75]</div>
        <div>6) AvgVol(5) &gt; 1.2 × AvgVol(30)</div>
      </div>
    </>
  );

  return (
    <div className="mx-auto w-full max-w-5xl p-6">
      <div className="mb-6 flex items-start justify-between gap-3">
        <div className="min-w-0">
          <div className="text-lg font-semibold">Watchlist</div>
          <div className="mt-1 text-sm text-[var(--k-muted)]">
            Manage the stocks you are watching.
          </div>
          <div className="mt-1 text-xs text-[var(--k-muted)]">
            Names are resolved from Market cache. If names are missing, go to Market and click Sync
            once.
          </div>
          {syncMsg ? <div className="mt-2 text-xs text-[var(--k-muted)]">{syncMsg}</div> : null}
          {error ? <div className="mt-2 text-sm text-red-600">{error}</div> : null}
        </div>
        <Button
          size="sm"
          variant="secondary"
          onClick={() => void onSyncFromScreener()}
          disabled={syncBusy}
          className="gap-2"
        >
          <RefreshCw className="h-4 w-4" />
          Sync from screener
        </Button>
      </div>

      <section className="mb-4 rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] p-4">
        <div className="mb-2 text-sm font-medium">Add</div>
        <div className="grid gap-2 md:grid-cols-12">
          <input
            className="h-9 md:col-span-10 rounded-md border border-[var(--k-border)] bg-[var(--k-surface-2)] px-3 text-sm outline-none"
            placeholder="Ticker (e.g. 600000 / 0700 / CN:600000)"
            value={code}
            onChange={(e) => setCode(e.target.value)}
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
                setError(null);
              }}
              disabled={!code.trim() && !error}
            >
              Clear
            </Button>
          </div>
        </div>
        <div className="mt-2 text-xs text-[var(--k-muted)]">
          Supported inputs: CN 6-digit ticker, HK 4-5 digit ticker, or prefixed symbol (CN:/HK:).
        </div>
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
                  <th className="px-3 py-2">Name</th>
                  <th className="px-3 py-2">
                    <div className="inline-flex items-center gap-2">
                      <span>TrendOK</span>
                      <Button
                        variant="ghost"
                        size="icon"
                        className="h-8 w-8 rounded-full"
                        onMouseEnter={(e) => showTooltip(e.currentTarget, headerTip, 380)}
                        onMouseLeave={hideTooltip}
                        onFocus={(e) => showTooltip(e.currentTarget, headerTip, 380)}
                        onBlur={hideTooltip}
                        aria-label="TrendOK definition"
                      >
                        <Info className="h-4 w-4" />
                      </Button>
                    </div>
                  </th>
                  <th className="px-3 py-2 w-[90px] text-right"> </th>
                </tr>
              </thead>
              <tbody>
                {items.map((it) => (
                  <tr key={it.symbol} className="border-t border-[var(--k-border)]">
                    <td className="px-3 py-2 font-mono">{it.symbol}</td>
                    <td className="px-3 py-2">{it.name || '—'}</td>
                    <td className="px-3 py-2">{renderTrendOkCell(it.symbol)}</td>
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

      {tooltip.open
        ? createPortal(
            <div
              className="fixed z-[9999] rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] p-3 text-xs text-[var(--k-text)] shadow-lg"
              style={{ left: tooltip.x, top: tooltip.y, width: tooltip.w }}
            >
              {tooltip.content}
            </div>,
            document.body,
          )
        : null}
    </div>
  );
}
