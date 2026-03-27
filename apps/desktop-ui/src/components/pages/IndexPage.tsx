'use client';

import * as React from 'react';

import { DATA_SYNC_BASE_URL } from '@/lib/endpoints';

type CnIndexSignal = {
  tsCode?: string;
  name?: string;
  signal?: string;
  positionRange?: string;
  close?: number | null;
  ma5?: number | null;
  ma20?: number | null;
  realtime?: boolean;
  tradeTime?: string | null;
  source?: string | null;
};

type MacroItem = {
  seriesId?: string;
  name?: string;
  category?: string;
  why?: string;
  asOfDate?: string | null;
  close?: number | null;
  pctChg?: number | null;
  ma5?: number | null;
  ma20?: number | null;
  source?: string | null;
  underlyingTsCode?: string | null;
  realtime?: boolean;
  tradeTime?: string | null;
  quotePrice?: number | null;
  quotePctChg?: number | null;
};

type MacroSnapshot = {
  cnIndexSignals?: CnIndexSignal[];
  macro?: MacroItem[];
  warning?: string;
};

const POLL_MS = 45_000;
const FETCH_TIMEOUT_MS = 30_000;

async function fetchSnapshot(): Promise<MacroSnapshot> {
  const ctrl = new AbortController();
  const timer = window.setTimeout(() => ctrl.abort(), FETCH_TIMEOUT_MS);
  try {
    const res = await fetch(`${DATA_SYNC_BASE_URL}/macro/snapshot`, {
      cache: 'no-store',
      signal: ctrl.signal,
    });
    const txt = await res.text().catch(() => '');
    if (!res.ok) throw new Error(`${res.status} ${res.statusText}${txt ? `: ${txt}` : ''}`);
    return (txt ? (JSON.parse(txt) as MacroSnapshot) : {}) as MacroSnapshot;
  } catch (e) {
    if (e instanceof DOMException && e.name === 'AbortError') {
      throw new Error(`Request timed out after ${FETCH_TIMEOUT_MS / 1000}s (check data-sync-service)`);
    }
    throw e;
  } finally {
    window.clearTimeout(timer);
  }
}

function signalBadgeClass(signal: string): string {
  const s = String(signal || 'unknown');
  if (s === 'deep_green')
    return 'border-emerald-600/40 bg-emerald-600/15 text-emerald-800';
  if (s === 'light_green' || s === 'green') return 'border-emerald-500/30 bg-emerald-500/10 text-emerald-700';
  if (s === 'red') return 'border-red-500/30 bg-red-500/10 text-red-600';
  if (s === 'yellow') return 'border-yellow-500/30 bg-yellow-500/10 text-yellow-700';
  return 'border-[var(--k-border)] bg-[var(--k-surface-2)] text-[var(--k-muted)]';
}

function MacroCard({ item }: { item: MacroItem }) {
  const live = Boolean(item.realtime);
  const pct = item.pctChg;
  return (
    <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface-2)] px-3 py-2 text-xs">
      <div className="flex items-start justify-between gap-2">
        <div className="font-medium text-[var(--k-fg)]">{item.name ?? item.seriesId}</div>
        <div className="shrink-0 text-[10px] uppercase text-[var(--k-muted)]">
          {live ? 'live' : 'eod'}
        </div>
      </div>
      {item.why ? <div className="mt-1 text-[11px] leading-snug text-[var(--k-muted)]">{item.why}</div> : null}
      {item.source === 'index_global' && item.underlyingTsCode === 'XIN9' ? (
        <div className="mt-1 text-[10px] text-amber-700/90">Spot index (XIN9) when SGX futures unavailable</div>
      ) : null}
      <div className="mt-2 font-mono text-[var(--k-fg)]">
        {item.close != null && Number.isFinite(item.close) ? Number(item.close).toFixed(2) : '—'}
        {pct != null && Number.isFinite(pct) ? (
          <span className={pct >= 0 ? 'ml-2 text-emerald-600' : 'ml-2 text-red-600'}>
            {pct >= 0 ? '+' : ''}
            {pct.toFixed(2)}%
          </span>
        ) : null}
      </div>
      <div className="mt-1 text-[var(--k-muted)]">
        MA5 {item.ma5 != null && Number.isFinite(item.ma5) ? Number(item.ma5).toFixed(2) : '—'} • MA20{' '}
        {item.ma20 != null && Number.isFinite(item.ma20) ? Number(item.ma20).toFixed(2) : '—'}
      </div>
      <div className="mt-1 text-[10px] text-[var(--k-muted)]">
        {item.asOfDate ? `as of ${item.asOfDate}` : 'no data'}
        {item.tradeTime ? ` • ${item.tradeTime}` : ''}
        {item.underlyingTsCode ? ` • ${item.underlyingTsCode}` : ''}
      </div>
    </div>
  );
}

function Section({
  title,
  subtitle,
  children,
}: {
  title: string;
  subtitle?: string;
  children: React.ReactNode;
}) {
  return (
    <section className="rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] p-4">
      <div className="mb-1 text-sm font-semibold">{title}</div>
      {subtitle ? <div className="mb-3 text-xs text-[var(--k-muted)]">{subtitle}</div> : null}
      {children}
    </section>
  );
}

export function IndexPage() {
  const [data, setData] = React.useState<MacroSnapshot | null>(null);
  const [error, setError] = React.useState<string | null>(null);
  const [pending, setPending] = React.useState(true);

  const load = React.useCallback(async () => {
    try {
      setError(null);
      const snap = await fetchSnapshot();
      setData(snap);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setPending(false);
    }
  }, []);

  React.useEffect(() => {
    void load();
    const t = window.setInterval(() => void load(), POLL_MS);
    return () => window.clearInterval(t);
  }, [load]);

  const cn = Array.isArray(data?.cnIndexSignals) ? data!.cnIndexSignals! : [];
  const macro = Array.isArray(data?.macro) ? data!.macro! : [];

  const byCat = (c: string) => macro.filter((m) => m.category === c);

  return (
    <div className="mx-auto max-w-4xl space-y-4 p-4">
      <div className="flex items-start justify-between gap-3">
        <div>
          <p className="mt-1 text-xs text-[var(--k-muted)]">
            CN index traffic lights + global macro (poll ~every {POLL_MS / 1000}s). Post-close EOD sync; quotes
            merge when Tushare realtime supports the symbol.
          </p>
        </div>
        <button
          type="button"
          className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface-2)] px-3 py-1.5 text-xs"
          onClick={() => {
            setPending(true);
            void load();
          }}
        >
          Refresh
        </button>
      </div>

      {pending ? <div className="text-xs text-[var(--k-muted)]">Updating snapshot…</div> : null}
      {data?.warning ? (
        <div className="rounded-lg border border-amber-500/30 bg-amber-500/10 px-3 py-2 text-xs text-amber-900">{data.warning}</div>
      ) : null}
      {error ? <div className="rounded-lg border border-red-500/30 bg-red-500/10 px-3 py-2 text-sm text-red-700">{error}</div> : null}

      <Section
        title="CN index traffic lights"
        subtitle="Same MA/volume rules as Dashboard. Full-market breadth (deep green) is omitted here for fast load; use Dashboard for that gate."
      >
        {cn.length ? (
          <div className="grid gap-2 md:grid-cols-2">
            {cn.map((it) => {
              const signal = String(it?.signal ?? 'unknown');
              return (
                <div key={String(it?.tsCode ?? it?.name)} className={`rounded-lg border px-3 py-2 text-xs ${signalBadgeClass(signal)}`}>
                  <div className="font-medium">{String(it?.name ?? it?.tsCode ?? '')}</div>
                  <div className="mt-1 font-mono">
                    {signal} • pos {String(it?.positionRange ?? '—')}
                  </div>
                  <div className="mt-1 text-[var(--k-muted)]">
                    close{' '}
                    {Number.isFinite(it?.close) ? Number(it.close).toFixed(2) : '—'} • MA5{' '}
                    {Number.isFinite(it?.ma5) ? Number(it.ma5).toFixed(2) : '—'} • MA20{' '}
                    {Number.isFinite(it?.ma20) ? Number(it.ma20).toFixed(2) : '—'}
                  </div>
                  {it.realtime ? <div className="mt-1 text-[10px] text-[var(--k-muted)]">live {it.tradeTime ?? ''}</div> : null}
                </div>
              );
            })}
          </div>
        ) : (
          <div className="text-xs text-[var(--k-muted)]">No CN index signals (sync index daily / market data).</div>
        )}
      </Section>

      <Section title="US tech linkage" subtitle="Overnight US tech drives CN TMT / AI themes.">
        <div className="grid gap-2 md:grid-cols-2">
          {byCat('us_tech').map((m) => (
            <MacroCard key={m.seriesId ?? m.name} item={m} />
          ))}
        </div>
      </Section>

      <Section title="FX" subtitle="USD/CNH is a key valve for foreign flows.">
        <div className="grid gap-2 md:grid-cols-2">
          {byCat('fx').map((m) => (
            <MacroCard key={m.seriesId ?? m.name} item={m} />
          ))}
        </div>
      </Section>

      <Section title="FTSE China A50" subtitle="A50 futures preferred; XIN9 spot when futures unavailable.">
        <div className="grid gap-2 md:grid-cols-2">
          {byCat('a50').map((m) => (
            <MacroCard key={m.seriesId ?? m.name} item={m} />
          ))}
        </div>
      </Section>

      <Section title="Commodities (proxies)" subtitle="INE crude / SHFE gold & copper (main contracts).">
        <div className="grid gap-2 md:grid-cols-2">
          {byCat('commodity').map((m) => (
            <MacroCard key={m.seriesId ?? m.name} item={m} />
          ))}
        </div>
      </Section>
    </div>
  );
}
