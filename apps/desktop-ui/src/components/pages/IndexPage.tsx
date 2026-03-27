'use client';

import * as React from 'react';

import { DATA_SYNC_BASE_URL } from '@/lib/endpoints';
import { cn } from '@/lib/utils';

type CnIndexSignal = {
  tsCode?: string;
  name?: string;
  signal?: string;
  positionRange?: string;
  close?: number | null;
  /** vs prior trading day close (realtime uses quote pct_chg when available) */
  pctChg?: number | null;
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

function signalSurfaceClass(signal: string): string {
  const s = String(signal || 'unknown');
  if (s === 'deep_green')
    return 'border-emerald-500/35 bg-emerald-500/[0.07] shadow-emerald-900/5';
  if (s === 'light_green' || s === 'green')
    return 'border-emerald-500/30 bg-emerald-500/[0.06] shadow-emerald-900/5';
  if (s === 'red') return 'border-red-500/35 bg-red-500/[0.06] shadow-red-900/5';
  if (s === 'yellow') return 'border-amber-400/40 bg-amber-500/[0.07] shadow-amber-900/5';
  return 'border-[var(--k-border)] bg-[var(--k-surface)] shadow-black/5';
}

function fmtPrice(n: unknown): string {
  return Number.isFinite(n) ? Number(n).toFixed(2) : '—';
}

type IndexCardProps = {
  title: string;
  mode: 'live' | 'eod';
  /** CN: signal • pos */
  metaLine?: string;
  /** Macro: short description */
  description?: string;
  spotHint?: boolean;
  /** Colored border for CN traffic light */
  cnSignalClass?: string;
  close: number | null;
  pctChg?: number | null;
  ma5: number | null;
  ma20: number | null;
  footnote: React.ReactNode;
};

function IndexCard({
  title,
  mode,
  metaLine,
  description,
  spotHint,
  cnSignalClass,
  close,
  pctChg,
  ma5,
  ma20,
  footnote,
}: IndexCardProps) {
  const hasPct = pctChg != null && Number.isFinite(pctChg);
  return (
    <article
      className={cn(
        'flex flex-col rounded-2xl border p-4 shadow-sm',
        cnSignalClass ?? 'border-[var(--k-border)] bg-[var(--k-surface)]',
      )}
    >
      <div className="flex items-start justify-between gap-3">
        <h3 className="text-base font-semibold leading-tight tracking-tight text-[var(--k-fg)]">{title}</h3>
        <span className="shrink-0 rounded-md bg-[var(--k-surface-2)] px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wide text-[var(--k-muted)]">
          {mode}
        </span>
      </div>

      {metaLine ? (
        <p className="mt-2 font-mono text-xs leading-snug text-[var(--k-muted)]">{metaLine}</p>
      ) : null}

      {description ? (
        <p className="mt-2 text-xs leading-relaxed text-[var(--k-muted)]">{description}</p>
      ) : null}

      {spotHint ? (
        <p className="mt-1.5 text-[11px] leading-snug text-amber-800/90 dark:text-amber-200/90">
          Spot index (XIN9) when SGX futures unavailable
        </p>
      ) : null}

      <div className="mt-4 flex flex-wrap items-baseline gap-x-2.5 gap-y-1">
        <span className="text-2xl font-semibold tabular-nums tracking-tight text-[var(--k-fg)]">
          {close != null && Number.isFinite(close) ? close.toFixed(2) : '—'}
        </span>
        {hasPct ? (
          <span
            className={cn(
              'text-lg font-semibold tabular-nums',
              pctChg! >= 0 ? 'text-emerald-600 dark:text-emerald-400' : 'text-red-600 dark:text-red-400',
            )}
          >
            {pctChg! >= 0 ? '+' : ''}
            {pctChg!.toFixed(2)}%
          </span>
        ) : null}
      </div>

      <p className="mt-3 text-xs leading-snug text-[var(--k-muted)]">
        MA5 {fmtPrice(ma5)} <span className="text-[var(--k-border)]">•</span> MA20 {fmtPrice(ma20)}
      </p>

      <div className="mt-3 border-t border-[var(--k-border)]/80 pt-3 text-[11px] leading-snug text-[var(--k-muted)]">
        {footnote}
      </div>
    </article>
  );
}

function macroToCardProps(item: MacroItem): IndexCardProps {
  const live = Boolean(item.realtime);
  const footParts: string[] = [];
  if (item.asOfDate) footParts.push(`as of ${item.asOfDate}`);
  if (item.tradeTime) footParts.push(String(item.tradeTime));
  if (item.underlyingTsCode) footParts.push(String(item.underlyingTsCode));
  const footnote = footParts.length ? footParts.join(' • ') : '—';

  return {
    title: String(item.name ?? item.seriesId ?? ''),
    mode: live ? 'live' : 'eod',
    description: item.why,
    spotHint: item.source === 'index_global' && item.underlyingTsCode === 'XIN9',
    close: Number.isFinite(item.close) ? Number(item.close) : null,
    pctChg: Number.isFinite(item.pctChg) ? Number(item.pctChg) : null,
    ma5: Number.isFinite(item.ma5) ? Number(item.ma5) : null,
    ma20: Number.isFinite(item.ma20) ? Number(item.ma20) : null,
    footnote,
  };
}

function cnToCardProps(it: CnIndexSignal): IndexCardProps {
  const signal = String(it?.signal ?? 'unknown');
  const live = Boolean(it.realtime);
  const footnote = it.realtime && it.tradeTime ? `live ${it.tradeTime}` : '—';
  const pc = it?.pctChg;
  const pctChg = Number.isFinite(pc) ? Number(pc) : null;

  return {
    title: String(it?.name ?? it?.tsCode ?? ''),
    mode: live ? 'live' : 'eod',
    metaLine: `${signal} • pos ${String(it?.positionRange ?? '—')}`,
    cnSignalClass: signalSurfaceClass(signal),
    close: Number.isFinite(it?.close) ? Number(it.close) : null,
    pctChg,
    ma5: Number.isFinite(it?.ma5) ? Number(it.ma5) : null,
    ma20: Number.isFinite(it?.ma20) ? Number(it.ma20) : null,
    footnote,
  };
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

  return (
    <div className="mx-auto max-w-6xl space-y-4 p-4">
      <div className="flex items-start justify-between gap-3">
        <p className="max-w-prose text-xs leading-relaxed text-[var(--k-muted)]">
          CN indices + macro · poll ~{POLL_MS / 1000}s · offshore series are typically prior session EOD.
        </p>
        <button
          type="button"
          className="shrink-0 rounded-xl border border-[var(--k-border)] bg-[var(--k-surface-2)] px-3 py-1.5 text-xs font-medium shadow-sm"
          onClick={() => {
            setPending(true);
            void load();
          }}
        >
          Refresh
        </button>
      </div>

      {pending ? <div className="text-xs text-[var(--k-muted)]">Updating…</div> : null}
      {data?.warning ? (
        <div className="rounded-2xl border border-amber-500/30 bg-amber-500/10 px-4 py-2.5 text-xs text-amber-950 dark:text-amber-100">
          {data.warning}
        </div>
      ) : null}
      {error ? (
        <div className="rounded-2xl border border-red-500/30 bg-red-500/10 px-4 py-2.5 text-sm text-red-800 dark:text-red-200">
          {error}
        </div>
      ) : null}

      <div className="grid grid-cols-1 gap-5 sm:grid-cols-2 xl:grid-cols-3">
        {cn.map((it) => (
          <IndexCard key={String(it?.tsCode ?? it?.name)} {...cnToCardProps(it)} />
        ))}
        {macro.map((m) => (
          <IndexCard key={m.seriesId ?? m.name} {...macroToCardProps(m)} />
        ))}
      </div>

      {!cn.length && !macro.length ? (
        <div className="text-xs text-[var(--k-muted)]">No data — sync index/macro or check Tushare token.</div>
      ) : null}
    </div>
  );
}
