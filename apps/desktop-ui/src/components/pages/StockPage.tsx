'use client';

import * as React from 'react';
import { ArrowLeft } from 'lucide-react';

import { Button } from '@/components/ui/button';
import { StockChart } from '@/components/stock/StockChart';
import { DATA_SYNC_BASE_URL, QUANT_BASE_URL } from '@/lib/endpoints';
import { useChatStore } from '@/lib/chat/store';
import type { OHLCV } from '@/lib/indicators';

type BarsResp = {
  symbol: string;
  market: string;
  ticker: string;
  name: string;
  currency: string;
  bars: Array<{
    date: string;
    open: string;
    high: string;
    low: string;
    close: string;
    volume: string;
    amount: string;
  }>;
};

type ChipsResp = {
  symbol: string;
  market: string;
  ticker: string;
  name: string;
  currency: string;
  items: Array<{
    date: string;
    profitRatio: string;
    avgCost: string;
    cost90Low: string;
    cost90High: string;
    cost90Conc: string;
    cost70Low: string;
    cost70High: string;
    cost70Conc: string;
  }>;
};

type FundFlowResp = {
  symbol: string;
  market: string;
  ticker: string;
  name: string;
  currency: string;
  items: Array<{
    date: string;
    close: string;
    changePct: string;
    mainNetAmount: string;
    mainNetRatio: string;
    superNetAmount: string;
    superNetRatio: string;
    largeNetAmount: string;
    largeNetRatio: string;
    mediumNetAmount: string;
    mediumNetRatio: string;
    smallNetAmount: string;
    smallNetRatio: string;
  }>;
};

type QuoteResp = {
  ok: boolean;
  error?: string;
  items: Array<{
    ts_code: string;
    price: string | null;
    open: string | null;
    high: string | null;
    low: string | null;
    pre_close: string | null;
    change: string | null;
    pct_chg: string | null;
    volume: string | null;
    amount: string | null;
    trade_time: string | null;
  }>;
};

async function apiGetJson<T>(path: string): Promise<T> {
  const res = await fetch(`${QUANT_BASE_URL}${path}`, { cache: 'no-store' });
  const txt = await res.text().catch(() => '');
  if (!res.ok) {
    try {
      const j = JSON.parse(txt) as { detail?: string; error?: string };
      const msg = (j && (j.detail || j.error)) || '';
      if (msg) throw new Error(msg);
    } catch {
      // ignore
    }
    throw new Error(`${res.status} ${res.statusText}${txt ? `: ${txt}` : ''}`);
  }
  return (txt ? (JSON.parse(txt) as T) : ({} as T));
}

async function apiGetJsonFrom<T>(baseUrl: string, path: string): Promise<T> {
  const res = await fetch(`${baseUrl}${path}`, { cache: 'no-store' });
  const txt = await res.text().catch(() => '');
  if (!res.ok) {
    try {
      const j = JSON.parse(txt) as { detail?: string; error?: string };
      const msg = (j && (j.detail || j.error)) || '';
      if (msg) throw new Error(msg);
    } catch {
      // ignore
    }
    throw new Error(`${res.status} ${res.statusText}${txt ? `: ${txt}` : ''}`);
  }
  return (txt ? (JSON.parse(txt) as T) : ({} as T));
}

function normalizeSymbol(symbol: string): string {
  // Normalize symbol format: "主板:000001" -> "CN:000001"
  const s = symbol.trim();
  if (s.startsWith('主板:') || s.startsWith('中小板:') || s.startsWith('创业板:') || s.startsWith('科创板:')) {
    const parts = s.split(':', 2);
    if (parts.length >= 2) {
      return `CN:${parts[1].trim()}`;
    }
  }
  return s;
}

function toTsCodeFromSymbol(symbol: string): string | null {
  // Only handle CN A-shares for now: "CN:000001" -> "000001.SZ/SH"
  // Also handle normalized symbols like "主板:000001" -> "CN:000001" -> "000001.SZ/SH"
  const normalized = normalizeSymbol(symbol);
  const s = normalized.trim();
  if (!s.startsWith('CN:')) return null;
  const ticker = s.slice('CN:'.length).trim();
  if (!/^[0-9]{6}$/.test(ticker)) return null;
  const suffix = ticker.startsWith('6') ? 'SH' : 'SZ';
  return `${ticker}.${suffix}`;
}

function shanghaiTodayIso(): string {
  // "sv-SE" returns YYYY-MM-DD format.
  return new Date().toLocaleDateString('sv-SE', { timeZone: 'Asia/Shanghai' });
}

function getShanghaiTimeParts(): { weekday: string; hour: number; minute: number } {
  const parts = new Intl.DateTimeFormat('en-US', {
    timeZone: 'Asia/Shanghai',
    weekday: 'short',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
  }).formatToParts(new Date());
  const map = new Map(parts.map((p) => [p.type, p.value]));
  return {
    weekday: map.get('weekday') ?? '',
    hour: Number(map.get('hour') ?? 0),
    minute: Number(map.get('minute') ?? 0),
  };
}

function isShanghaiTradingTime(): boolean {
  const { weekday, hour, minute } = getShanghaiTimeParts();
  if (!['Mon', 'Tue', 'Wed', 'Thu', 'Fri'].includes(weekday)) return false;
  const minutes = hour * 60 + minute;
  // CN A-share: 09:30-11:30, 13:00-15:00
  const inMorning = minutes >= 9 * 60 + 30 && minutes <= 11 * 60 + 30;
  const inAfternoon = minutes >= 13 * 60 && minutes <= 15 * 60;
  return inMorning || inAfternoon;
}

function mergeQuoteIntoBars(d: BarsResp, q: QuoteResp['items'][number]): BarsResp {
  const price = q.price ?? '';
  if (!price) return d;
  const date = (q.trade_time && q.trade_time.slice(0, 10)) || shanghaiTodayIso();
  const nextBar = {
    date,
    open: q.open ?? price,
    high: q.high ?? price,
    low: q.low ?? price,
    close: price,
    volume: q.volume ?? '',
    amount: q.amount ?? '',
  };
  const bars = [...(d.bars ?? [])];
  const last = bars[bars.length - 1];
  if (last && last.date === date) {
    bars[bars.length - 1] = nextBar;
  } else {
    bars.push(nextBar);
  }
  return { ...d, bars };
}

function getLastDetailSyncMs(symbol: string): number {
  try {
    const v = window.localStorage.getItem(`karios.market.stockDetailLastSync:${symbol}`);
    const n = Number(v);
    return Number.isFinite(n) ? n : 0;
  } catch {
    return 0;
  }
}

function setLastDetailSyncMs(symbol: string, ms: number) {
  try {
    window.localStorage.setItem(`karios.market.stockDetailLastSync:${symbol}`, String(ms));
  } catch {
    // ignore
  }
}

export function StockPage({
  symbol,
  onBack,
}: {
  symbol: string;
  onBack: () => void;
}) {
  const { addReference } = useChatStore();
  const [data, setData] = React.useState<BarsResp | null>(null);
  const [chips, setChips] = React.useState<ChipsResp | null>(null);
  const [fundFlow, setFundFlow] = React.useState<FundFlowResp | null>(null);
  const [error, setError] = React.useState<string | null>(null);
  const [busy, setBusy] = React.useState(false);
  const [lastSyncMs, setLastSyncMs] = React.useState<number>(0);
  const chartData: OHLCV[] = React.useMemo(() => {
    const bars = data?.bars ?? [];
    return bars
      .map((b) => {
        const open = Number(b.open);
        const high = Number(b.high);
        const low = Number(b.low);
        const close = Number(b.close);
        const volume = Number(String(b.volume).replaceAll(',', ''));
        if (!b.date || !Number.isFinite(open) || !Number.isFinite(high) || !Number.isFinite(low) || !Number.isFinite(close)) {
          return null;
        }
        return {
          time: b.date,
          open,
          high,
          low,
          close,
          volume: Number.isFinite(volume) ? volume : 0,
        };
      })
      .filter(Boolean) as OHLCV[];
  }, [data]);

  const refresh = React.useCallback(async ({ force, quote }: { force?: boolean; quote?: boolean } = {}) => {
    setError(null);
    setBusy(true);
    try {
      // Normalize symbol format: "主板:000001" -> "CN:000001"
      const normalizedSymbol = normalizeSymbol(symbol);
      const [d, c] = await Promise.all([
        apiGetJsonFrom<BarsResp>(
          DATA_SYNC_BASE_URL,
          `/market/stocks/${encodeURIComponent(normalizedSymbol)}/bars?days=60${force ? '&force=true' : ''}`,
        ),
        apiGetJson<ChipsResp>(
          `/market/stocks/${encodeURIComponent(normalizedSymbol)}/chips?days=30${force ? '&force=true' : ''}`,
        ).catch(
          () => null,
        ),
      ]);
      const ff = await apiGetJson<FundFlowResp>(
        `/market/stocks/${encodeURIComponent(normalizedSymbol)}/fund-flow?days=30${force ? '&force=true' : ''}`,
      ).catch(() => null);
      let d2 = d;
      if (quote) {
        const tsCode = toTsCodeFromSymbol(symbol);
        if (tsCode) {
          const qr = await apiGetJsonFrom<QuoteResp>(DATA_SYNC_BASE_URL, `/quote?ts_code=${encodeURIComponent(tsCode)}`).catch(
            () => null,
          );
          const item = qr?.items?.[0];
          if (item) d2 = mergeQuoteIntoBars(d2, item);
        }
      }
      setData(d2);
      setChips(c);
      setFundFlow(ff);
      if (force) {
        const now = Date.now();
        setLastDetailSyncMs(symbol, now);
        setLastSyncMs(now);
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }, [symbol]);

  React.useEffect(() => {
    const prev = getLastDetailSyncMs(symbol);
    setLastSyncMs(prev);
    // Auto-sync at most once every 10 minutes per symbol.
    const age = Date.now() - prev;
    const shouldQuote = isShanghaiTradingTime();
    void refresh({ force: age > 10 * 60 * 1000, quote: shouldQuote });
  }, [refresh, symbol]);

  return (
    <div className="mx-auto w-full max-w-6xl p-6">
      <div className="mb-4 flex items-start justify-between gap-3">
        <div className="min-w-0">
          <div className="flex items-center gap-2">
            <Button variant="secondary" size="sm" onClick={onBack} className="gap-2">
              <ArrowLeft className="h-4 w-4" />
              Back
            </Button>
            <div className="text-lg font-semibold">{data ? `${data.ticker} ${data.name}` : symbol}</div>
          </div>
          <div className="mt-1 text-sm text-[var(--k-muted)]">
            {data ? `${data.market} • ${data.currency}` : 'Loading...'}
          </div>
        </div>
        <div className="flex items-center gap-2">
          <Button variant="secondary" size="sm" onClick={() => void refresh({ quote: true })} disabled={busy}>
            Refresh
          </Button>
          <Button size="sm" onClick={() => void refresh({ force: true, quote: true })} disabled={busy}>
            Sync detail
          </Button>
          <Button
            size="sm"
            disabled={!data}
            onClick={() => {
              if (!data) return;
              addReference({
                kind: 'stock',
                refId: data.symbol,
                symbol: data.symbol,
                market: data.market,
                ticker: data.ticker,
                name: data.name,
                barsDays: 60,
                chipsDays: 30,
                fundFlowDays: 30,
                capturedAt: new Date().toISOString(),
              });
            }}
          >
            Reference to chat
          </Button>
        </div>
      </div>
      {lastSyncMs ? (
        <div className="mb-3 text-xs text-[var(--k-muted)]">
          Last detail sync: {new Date(lastSyncMs).toLocaleString()}
        </div>
      ) : null}

      {error ? (
        <div className="mb-4 rounded-lg border border-red-500/30 bg-red-500/10 px-3 py-2 text-sm text-red-600">
          {error}
        </div>
      ) : null}

      <section className="rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] p-4">
        <div className="flex items-center justify-between">
          <div className="font-medium">Candles / Volume / MACD / KDJ</div>
          <div className="text-xs text-[var(--k-muted)]">{data?.bars?.length ?? 0} bars</div>
        </div>
        <div className="mt-3">
          {chartData.length > 0 ? (
            <StockChart data={chartData} />
          ) : (
            <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-bg)] px-3 py-10 text-center text-sm text-[var(--k-muted)]">
              No bars yet. Try Refresh.
            </div>
          )}
        </div>
      </section>

      <section className="mt-4 rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] p-4">
        <div className="flex items-center justify-between">
          <div className="font-medium">Chip distribution (筹码分布)</div>
          <div className="text-xs text-[var(--k-muted)]">
            {chips?.items?.length ? `${chips.items.length} rows` : '—'}
          </div>
        </div>
        {chips?.items?.length ? (
          <>
            <div className="mt-2 text-sm text-[var(--k-muted)]">
              Latest: profitRatio={chips.items[chips.items.length - 1]?.profitRatio} • avgCost=
              {chips.items[chips.items.length - 1]?.avgCost} • 70%[{chips.items[chips.items.length - 1]?.cost70Low},{' '}
              {chips.items[chips.items.length - 1]?.cost70High}] • 90%[{chips.items[chips.items.length - 1]?.cost90Low},{' '}
              {chips.items[chips.items.length - 1]?.cost90High}]
            </div>
            <div className="mt-3 overflow-hidden rounded-lg border border-[var(--k-border)]">
              <div className="max-h-[320px] overflow-auto">
                <table className="w-full border-collapse text-sm">
                  <thead className="sticky top-0 bg-[var(--k-surface-2)]">
                    <tr className="text-left text-xs text-[var(--k-muted)]">
                      {['Date', 'Profit', 'Avg', '70% Low', '70% High', '90% Low', '90% High'].map((h) => (
                        <th key={h} className="whitespace-nowrap px-3 py-2">
                          {h}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {chips.items.map((it) => (
                      <tr key={it.date} className="border-t border-[var(--k-border)]">
                        <td className="px-3 py-2 font-mono text-xs">{it.date}</td>
                        <td className="px-3 py-2 font-mono text-xs">{it.profitRatio}</td>
                        <td className="px-3 py-2 font-mono text-xs">{it.avgCost}</td>
                        <td className="px-3 py-2 font-mono text-xs">{it.cost70Low}</td>
                        <td className="px-3 py-2 font-mono text-xs">{it.cost70High}</td>
                        <td className="px-3 py-2 font-mono text-xs">{it.cost90Low}</td>
                        <td className="px-3 py-2 font-mono text-xs">{it.cost90High}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          </>
        ) : (
          <div className="mt-2 text-sm text-[var(--k-muted)]">
            Not available yet for this market (v0 supports CN A-shares only), or data source failed.
          </div>
        )}
      </section>

      <section className="mt-4 rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] p-4">
        <div className="flex items-center justify-between">
          <div className="font-medium">Fund flow distribution (资金成交分布)</div>
          <div className="text-xs text-[var(--k-muted)]">
            {fundFlow?.items?.length ? `${fundFlow.items.length} rows` : '—'}
          </div>
        </div>
        {fundFlow?.items?.length ? (
          <>
            <div className="mt-2 text-sm text-[var(--k-muted)]">
              Latest: main={fundFlow.items[fundFlow.items.length - 1]?.mainNetAmount} (
              {fundFlow.items[fundFlow.items.length - 1]?.mainNetRatio}
              %) • super={fundFlow.items[fundFlow.items.length - 1]?.superNetAmount} • large=
              {fundFlow.items[fundFlow.items.length - 1]?.largeNetAmount} • medium=
              {fundFlow.items[fundFlow.items.length - 1]?.mediumNetAmount} • small=
              {fundFlow.items[fundFlow.items.length - 1]?.smallNetAmount}
            </div>
            <div className="mt-3 overflow-hidden rounded-lg border border-[var(--k-border)]">
              <div className="max-h-[320px] overflow-auto">
                <table className="w-full border-collapse text-sm">
                  <thead className="sticky top-0 bg-[var(--k-surface-2)]">
                    <tr className="text-left text-xs text-[var(--k-muted)]">
                      {['Date', 'Main', 'Super', 'Large', 'Medium', 'Small'].map((h) => (
                        <th key={h} className="whitespace-nowrap px-3 py-2">
                          {h}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {fundFlow.items.map((it) => (
                      <tr key={it.date} className="border-t border-[var(--k-border)]">
                        <td className="px-3 py-2 font-mono text-xs">{it.date}</td>
                        <td className="px-3 py-2 font-mono text-xs">
                          {it.mainNetAmount} ({it.mainNetRatio}%)
                        </td>
                        <td className="px-3 py-2 font-mono text-xs">
                          {it.superNetAmount} ({it.superNetRatio}%)
                        </td>
                        <td className="px-3 py-2 font-mono text-xs">
                          {it.largeNetAmount} ({it.largeNetRatio}%)
                        </td>
                        <td className="px-3 py-2 font-mono text-xs">
                          {it.mediumNetAmount} ({it.mediumNetRatio}%)
                        </td>
                        <td className="px-3 py-2 font-mono text-xs">
                          {it.smallNetAmount} ({it.smallNetRatio}%)
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          </>
        ) : (
          <div className="mt-2 text-sm text-[var(--k-muted)]">
            Not available yet for this market (v0 supports CN A-shares only), or data source failed.
          </div>
        )}
      </section>
    </div>
  );
}


