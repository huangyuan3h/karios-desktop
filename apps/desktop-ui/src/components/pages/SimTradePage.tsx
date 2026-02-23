'use client';

import * as React from 'react';
import { Search, ChevronRight, X, Plus } from 'lucide-react';

import { Button } from '@/components/ui/button';
import { SimCandleChart } from '@/components/stock/SimCandleChart';
import { DATA_SYNC_BASE_URL } from '@/lib/endpoints';
import type { OHLCV } from '@/lib/indicators';
import { computeMacd } from '@/lib/indicators';

const FEE_RATE_DEFAULT = 0.0005;
const SLIPPAGE_RATE_DEFAULT = 0.0005;
const INDEX_CODES = [
  { ts_code: '000001.SH', name: '上证指数' },
  { ts_code: '399006.SZ', name: '创业板指' },
];

type MarketStockRow = {
  symbol: string;
  market: string;
  ticker: string;
  name: string;
  price: string | null;
  changePct: string | null;
};

type MarketStocksResponse = {
  items: MarketStockRow[];
  total: number;
  offset: number;
  limit: number;
};

type DailyBar = {
  date: string;
  open: number | null;
  high: number | null;
  low: number | null;
  close: number | null;
  vol: number | null;
  amount: number | null;
  avg_price: number | null;
};

type IndexBar = {
  ts_code: string;
  trade_date: string;
  close: number | null;
  pct_chg: number | null;
};

async function apiGetJson<T>(path: string): Promise<T> {
  const res = await fetch(`${DATA_SYNC_BASE_URL}${path}`, { cache: 'no-store' });
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  return (await res.json()) as T;
}

function roundDownTo100(n: number): number {
  return Math.max(0, Math.floor(n / 100) * 100);
}

const DEFAULT_START_DATE = '2023-02-01';
const DEFAULT_END_DATE = '2026-01-01';
const WAN = 10_000; // 1 万 = 10000 元

export function SimTradePage() {
  const [initialCashWan, setInitialCashWan] = React.useState(100);
  const [startDate, setStartDate] = React.useState(DEFAULT_START_DATE);
  const [endDate, setEndDate] = React.useState(DEFAULT_END_DATE);
  const [feeRate, setFeeRate] = React.useState(FEE_RATE_DEFAULT);
  const [slippageRate, setSlippageRate] = React.useState(SLIPPAGE_RATE_DEFAULT);

  const [tradingDays, setTradingDays] = React.useState<string[]>([]);
  const [currentDate, setCurrentDate] = React.useState<string | null>(null);
  const [cash, setCash] = React.useState(100 * WAN);
  const [positions, setPositions] = React.useState<Map<string, { qty: number; cost: number }>>(new Map());
  const [buyQueue, setBuyQueue] = React.useState<Array<{ symbol: string; qty: number }>>([]);
  const [sellQueue, setSellQueue] = React.useState<string[]>([]);
  const [selectedStocks, setSelectedStocks] = React.useState<string[]>([]);
  const [buyQtyInputs, setBuyQtyInputs] = React.useState<Record<string, string>>({});

  const [stockList, setStockList] = React.useState<MarketStocksResponse | null>(null);
  const [stockQ, setStockQ] = React.useState('');
  const [stockMarket, setStockMarket] = React.useState<'ALL' | 'CN' | 'HK'>('CN');
  const [stockOffset, setStockOffset] = React.useState(0);
  const stockLimit = 50;

  const [indexData, setIndexData] = React.useState<IndexBar[]>([]);
  const [indexCandleData, setIndexCandleData] = React.useState<Record<string, OHLCV[]>>({});
  const [chartDataBySymbol, setChartDataBySymbol] = React.useState<
    Record<string, { ohlcv: OHLCV[]; dif: Array<number | null>; dea: Array<number | null>; hist: Array<number | null> }>
  >({});
  const [error, setError] = React.useState<string | null>(null);

  const started = currentDate !== null;
  const currentIndex = currentDate ? tradingDays.indexOf(currentDate) : -1;
  const canNextDay = started && currentIndex >= 0 && currentIndex < tradingDays.length - 1;
  const nextDayDate = canNextDay ? tradingDays[currentIndex + 1] ?? null : null;

  const totalEquity = React.useMemo(() => {
    let sum = cash;
    positions.forEach(({ qty, cost }) => {
      sum += qty * cost;
    });
    return sum;
  }, [cash, positions]);

  const marketValue = React.useMemo(() => {
    let sum = 0;
    positions.forEach(({ qty, cost }) => {
      sum += qty * cost;
    });
    return sum;
  }, [positions]);

  const yieldPct = React.useMemo(() => {
    const initial = initialCashWan * WAN;
    if (initial <= 0) return 0;
    return ((totalEquity - initial) / initial) * 100;
  }, [initialCashWan, totalEquity]);

  const fetchStockList = React.useCallback(async () => {
    setError(null);
    try {
      const data = await apiGetJson<MarketStocksResponse>(
        `/market/stocks?limit=${stockLimit}&offset=${stockOffset}` +
          `${stockMarket !== 'ALL' ? `&market=${stockMarket}` : ''}` +
          `${stockQ.trim() ? `&q=${encodeURIComponent(stockQ.trim())}` : ''}`,
      );
      setStockList(data);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
      setStockList(null);
    }
  }, [stockLimit, stockOffset, stockMarket, stockQ]);

  const fetchIndexForDate = React.useCallback(async (dateStr: string) => {
    setError(null);
    try {
      const all: IndexBar[] = [];
      for (const { ts_code } of INDEX_CODES) {
        const list = await apiGetJson<Array<{ ts_code?: string; trade_date?: string; close?: number; pct_chg?: number }>>(
          `/index-daily?ts_code=${encodeURIComponent(ts_code)}&start_date=${dateStr}&end_date=${dateStr}&limit=5`,
        );
        for (const x of Array.isArray(list) ? list : []) {
          all.push({
            ts_code: String(x.ts_code ?? ''),
            trade_date: String(x.trade_date ?? ''),
            close: x.close != null ? Number(x.close) : null,
            pct_chg: x.pct_chg != null ? Number(x.pct_chg) : null,
          });
        }
      }
      setIndexData(all);
    } catch {
      setIndexData([]);
    }
  }, []);

  React.useEffect(() => {
    void fetchStockList();
  }, [fetchStockList]);

  React.useEffect(() => {
    if (currentDate) void fetchIndexForDate(currentDate);
  }, [currentDate, fetchIndexForDate]);

  React.useEffect(() => {
    if (!currentDate && !startDate) return;
    const end = currentDate ?? endDate;
    const d = new Date(end);
    d.setDate(d.getDate() - 45);
    const start = d.toISOString().slice(0, 10);
    let cancelled = false;
    const next: Record<string, OHLCV[]> = {};
    Promise.all(
      INDEX_CODES.map(async ({ ts_code, name }) => {
        try {
          const list = await apiGetJson<
            Array<{ trade_date?: string; open?: number; high?: number; low?: number; close?: number; vol?: number }>
          >(
            `/index-daily?ts_code=${encodeURIComponent(ts_code)}&start_date=${start}&end_date=${end}&limit=60`,
          );
          const ohlcv: OHLCV[] = (Array.isArray(list) ? list : [])
            .map((b) => {
              const open = Number(b.open);
              const high = Number(b.high);
              const low = Number(b.low);
              const close = Number(b.close);
              const vol = Number(b.vol ?? 0);
              if (!b.trade_date || !Number.isFinite(close)) return null;
              return {
                time: String(b.trade_date),
                open: Number.isFinite(open) ? open : close,
                high: Number.isFinite(high) ? high : close,
                low: Number.isFinite(low) ? low : close,
                close,
                volume: Number.isFinite(vol) ? vol : 0,
              };
            })
            .filter(Boolean) as OHLCV[];
          if (!cancelled) next[ts_code] = ohlcv;
        } catch {
          if (!cancelled) next[ts_code] = [];
        }
      }),
    ).then(() => {
      if (!cancelled) setIndexCandleData((prev) => ({ ...prev, ...next }));
    });
    return () => {
      cancelled = true;
    };
  }, [currentDate, startDate, endDate]);

  React.useEffect(() => {
    if (selectedStocks.length === 0) {
      setChartDataBySymbol({});
      return;
    }
    let cancelled = false;
    const next: Record<
      string,
      { ohlcv: OHLCV[]; dif: Array<number | null>; dea: Array<number | null>; hist: Array<number | null> }
    > = {};
    Promise.all(
      selectedStocks.map(async (symbol) => {
        try {
          const res = await apiGetJson<{ bars?: Array<{ date?: string; open?: unknown; high?: unknown; low?: unknown; close?: unknown; volume?: unknown }> }>(
            `/market/stocks/${encodeURIComponent(symbol)}/bars?days=20`,
          );
          const raw = res?.bars ?? [];
          const ohlcv: OHLCV[] = raw
            .map((b) => {
              const open = Number(b.open);
              const high = Number(b.high);
              const low = Number(b.low);
              const close = Number(b.close);
              const vol = Number(String(b.volume ?? 0).replaceAll(',', ''));
              if (!b.date || !Number.isFinite(open) || !Number.isFinite(high) || !Number.isFinite(low) || !Number.isFinite(close))
                return null;
              return {
                time: b.date,
                open,
                high,
                low,
                close,
                volume: Number.isFinite(vol) ? vol : 0,
              };
            })
            .filter(Boolean) as OHLCV[];
          if (cancelled) return;
          const { dif, dea, hist } = computeMacd(ohlcv);
          next[symbol] = { ohlcv, dif, dea, hist };
        } catch {
          if (!cancelled)
            next[symbol] = { ohlcv: [], dif: [], dea: [], hist: [] };
        }
      }),
    ).then(() => {
      if (!cancelled)
        setChartDataBySymbol((prev) => ({ ...prev, ...next }));
    });
    return () => {
      cancelled = true;
    };
  }, [selectedStocks.join(',')]);

  async function handleStartSim() {
    if (!startDate || !endDate || startDate > endDate) {
      setError('请设置有效的时间范围');
      return;
    }
    setError(null);
    setCash(initialCashWan * WAN);
    setPositions(new Map());
    setBuyQueue([]);
    setSellQueue([]);
    try {
      const list = await apiGetJson<string[]>(
        `/simtrade/trading-days?start=${encodeURIComponent(startDate)}&end=${encodeURIComponent(endDate)}`,
      );
      const days = Array.isArray(list) ? list : [];
      setTradingDays(days);
      setCurrentDate(days.length > 0 ? days[0] : null);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
      setTradingDays([]);
      setCurrentDate(null);
    }
  }

  const runNextDay = React.useCallback(async () => {
    if (!nextDayDate || !currentDate) return;
    const symbolsToFetch = [
      ...new Set([...sellQueue, ...buyQueue.map((b) => b.symbol)]),
    ];
    setError(null);
    try {
      let newCash = cash;
      let newPositions = new Map(positions);

      if (symbolsToFetch.length > 0) {
        const res = await apiGetJson<{ bars: Record<string, DailyBar[]> }>(
          `/simtrade/daily-bars?symbols=${symbolsToFetch.map((s) => encodeURIComponent(s)).join(',')}&start=${encodeURIComponent(nextDayDate)}&end=${encodeURIComponent(nextDayDate)}`,
        );
        const bars = res?.bars ?? {};

        for (const sym of sellQueue) {
          const dayBars = bars[sym];
          const bar = dayBars?.find((b) => b.date === nextDayDate) ?? dayBars?.[0];
          const avg = bar?.avg_price;
          if (avg != null && avg > 0) {
            const pos = newPositions.get(sym);
            if (pos) {
              const gross = pos.qty * avg;
              const fee = gross * feeRate;
              const slip = gross * slippageRate;
              newCash += gross - fee - slip;
              newPositions.delete(sym);
            }
          }
        }

        for (const { symbol, qty } of buyQueue) {
          const dayBars = bars[symbol];
          const bar = dayBars?.find((b) => b.date === nextDayDate) ?? dayBars?.[0];
          const avg = bar?.avg_price;
          if (avg != null && avg > 0) {
            const qty100 = roundDownTo100(qty);
            if (qty100 > 0) {
              const gross = qty100 * avg;
              const fee = gross * feeRate;
              const slip = gross * slippageRate;
              const cost = gross + fee + slip;
              if (cost <= newCash) {
                newCash -= cost;
                const existing = newPositions.get(symbol);
                if (existing) {
                  const totalQty = existing.qty + qty100;
                  const totalCost = existing.qty * existing.cost + qty100 * avg;
                  newPositions.set(symbol, { qty: totalQty, cost: totalCost / totalQty });
                } else {
                  newPositions.set(symbol, { qty: qty100, cost: avg });
                }
              }
            }
          }
        }
      }

      setCash(newCash);
      setPositions(newPositions);
      setSellQueue([]);
      setBuyQueue([]);
      setBuyQtyInputs({});
      setCurrentDate(nextDayDate);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  }, [nextDayDate, currentDate, sellQueue, buyQueue, feeRate, slippageRate, cash, positions]);


  const addToSelected = (symbol: string) => {
    if (!selectedStocks.includes(symbol)) {
      setSelectedStocks((s) => [...s, symbol]);
    }
  };

  const removeFromSelected = (symbol: string) => {
    setSelectedStocks((s) => s.filter((x) => x !== symbol));
    setBuyQueue((q) => q.filter((x) => x.symbol !== symbol));
    setBuyQtyInputs((p) => {
      const next = { ...p };
      delete next[symbol];
      return next;
    });
  };

  const addToBuyQueue = (symbol: string) => {
    const raw = buyQtyInputs[symbol] ?? '';
    const n = parseInt(raw, 10);
    const qty = Number.isFinite(n) && n > 0 ? roundDownTo100(n) : 0;
    if (qty <= 0) return;
    setBuyQueue((prev) => {
      const rest = prev.filter((x) => x.symbol !== symbol);
      return [...rest, { symbol, qty }];
    });
    setBuyQtyInputs((p) => ({ ...p, [symbol]: '' }));
  };

  const removeFromBuyQueue = (symbol: string) => {
    setBuyQueue((q) => q.filter((x) => x.symbol !== symbol));
  };

  const addToSellQueue = (symbol: string) => {
    if (!sellQueue.includes(symbol)) setSellQueue((s) => [...s, symbol]);
  };

  const removeFromSellQueue = (symbol: string) => {
    setSellQueue((s) => s.filter((x) => x !== symbol));
  };

  const positionSymbols = Array.from(positions.keys());

  return (
    <div className="flex h-full flex-col gap-3 p-4 overflow-hidden">
      {error ? (
        <div className="rounded-lg border border-red-500/30 bg-red-500/10 px-3 py-2 text-sm text-red-600">
          {error}
        </div>
      ) : null}

      <div className="flex flex-wrap items-end gap-4 rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] p-4">
        <div className="flex flex-wrap items-center gap-4">
          <label className="flex flex-col gap-1 text-xs">
            <span className="text-[var(--k-muted)]">初始资金（万）</span>
            <input
              type="number"
              min={0.1}
              step={1}
              className="w-24 rounded border border-[var(--k-border)] bg-[var(--k-bg)] px-2 py-1.5 text-sm"
              value={initialCashWan}
              onChange={(e) => setInitialCashWan(Number(e.target.value) || 0)}
              disabled={started}
            />
          </label>
          <label className="flex flex-col gap-1 text-xs">
            <span className="text-[var(--k-muted)]">开始日期</span>
            <input
              type="date"
              className="w-36 rounded border border-[var(--k-border)] bg-[var(--k-bg)] px-2 py-1.5 text-sm"
              value={startDate}
              onChange={(e) => setStartDate(e.target.value)}
              disabled={started}
            />
          </label>
          <label className="flex flex-col gap-1 text-xs">
            <span className="text-[var(--k-muted)]">结束日期</span>
            <input
              type="date"
              className="w-36 rounded border border-[var(--k-border)] bg-[var(--k-bg)] px-2 py-1.5 text-sm"
              value={endDate}
              onChange={(e) => setEndDate(e.target.value)}
              disabled={started}
            />
          </label>
          <label className="flex flex-col gap-1 text-xs">
            <span className="text-[var(--k-muted)]">手续费</span>
            <input
              type="number"
              step={0.0001}
              min={0}
              max={0.01}
              className="w-20 rounded border border-[var(--k-border)] bg-[var(--k-bg)] px-2 py-1.5 text-sm"
              value={feeRate}
              onChange={(e) => setFeeRate(Number(e.target.value) || 0)}
              disabled={started}
            />
          </label>
          <label className="flex flex-col gap-1 text-xs">
            <span className="text-[var(--k-muted)]">滑点</span>
            <input
              type="number"
              step={0.0001}
              min={0}
              max={0.01}
              className="w-20 rounded border border-[var(--k-border)] bg-[var(--k-bg)] px-2 py-1.5 text-sm"
              value={slippageRate}
              onChange={(e) => setSlippageRate(Number(e.target.value) || 0)}
              disabled={started}
            />
          </label>
          {!started ? (
            <Button size="sm" onClick={handleStartSim}>
              开始模拟
            </Button>
          ) : (
            <div className="flex items-center gap-2 text-sm">
              <span className="text-[var(--k-muted)]">当前日期</span>
              <span className="font-mono font-semibold">{currentDate ?? '—'}</span>
            </div>
          )}
        </div>
        {started && indexData.length > 0 ? (
          <div className="ml-4 flex flex-wrap gap-4 border-l border-[var(--k-border)] pl-4">
            {INDEX_CODES.map(({ ts_code, name }) => {
              const row = indexData.find((r) => r.ts_code === ts_code);
              return (
                <div key={ts_code} className="rounded bg-[var(--k-surface-2)] px-3 py-1.5 text-xs">
                  <span className="text-[var(--k-muted)]">{name}</span>
                  <span className="ml-2 font-mono">{row?.close != null ? row.close.toFixed(2) : '—'}</span>
                  {row?.pct_chg != null ? (
                    <span className={row.pct_chg >= 0 ? 'text-green-600' : 'text-red-600'}>
                      {' '}
                      {row.pct_chg >= 0 ? '+' : ''}
                      {row.pct_chg.toFixed(2)}%
                    </span>
                  ) : null}
                </div>
              );
            })}
          </div>
        ) : null}
      </div>

      <div className="flex min-h-0 flex-1 flex-col overflow-auto">
        <div className="grid min-h-0 flex-1 grid-cols-1 gap-3 lg:grid-cols-12">
        <div className="flex flex-col rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] lg:col-span-4">
          <div className="border-b border-[var(--k-border)] px-3 py-2 text-sm font-medium">股票列表</div>
          {INDEX_CODES.map(({ ts_code, name }) => {
            const idxBars = indexCandleData[ts_code] ?? [];
            if (idxBars.length === 0) return null;
            return (
              <div key={ts_code} className="border-b border-[var(--k-border)] p-2">
                <div className="mb-1 text-xs font-medium text-[var(--k-muted)]">{name} 蜡烛图</div>
                <div className="h-[180px] w-full">
                  <SimCandleChart data={idxBars} height={180} showVolume={true} />
                </div>
              </div>
            );
          })}
          <div className="flex flex-wrap gap-2 p-2">
            <div className="relative flex-1 min-w-[120px]">
              <Search className="absolute left-2 top-1/2 h-4 w-4 -translate-y-1/2 text-[var(--k-muted)]" />
              <input
                className="h-8 w-full rounded border border-[var(--k-border)] bg-[var(--k-bg)] pl-8 pr-2 text-xs"
                placeholder="代码/名称..."
                value={stockQ}
                onChange={(e) => {
                  setStockQ(e.target.value);
                  setStockOffset(0);
                }}
              />
            </div>
            <div className="flex gap-1">
              {(['ALL', 'CN', 'HK'] as const).map((m) => (
                <Button
                  key={m}
                  variant={stockMarket === m ? 'secondary' : 'ghost'}
                  size="sm"
                  className="h-8 text-xs"
                  onClick={() => {
                    setStockMarket(m);
                    setStockOffset(0);
                  }}
                >
                  {m}
                </Button>
              ))}
            </div>
          </div>
          <div className="flex-1 overflow-auto">
            <table className="w-full text-xs">
              <thead className="sticky top-0 bg-[var(--k-surface-2)]">
                <tr>
                  <th className="px-2 py-1.5 text-left font-medium text-[var(--k-muted)]">代码</th>
                  <th className="px-2 py-1.5 text-left font-medium text-[var(--k-muted)]">名称</th>
                  <th className="px-2 py-1.5 text-right font-medium text-[var(--k-muted)]">操作</th>
                </tr>
              </thead>
              <tbody>
                {(stockList?.items ?? []).map((it) => (
                  <tr key={it.symbol} className="border-t border-[var(--k-border)] hover:bg-[var(--k-surface-2)]">
                    <td className="px-2 py-1 font-mono">{it.ticker}</td>
                    <td className="max-w-[80px] truncate px-2 py-1" title={it.name}>
                      {it.name}
                    </td>
                    <td className="px-2 py-1 text-right">
                      <Button
                        variant="secondary"
                        size="sm"
                        className="h-6 px-2 text-xs"
                        onClick={() => addToSelected(it.symbol)}
                        disabled={selectedStocks.includes(it.symbol)}
                      >
                        <Plus className="h-3 w-3" />
                      </Button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <div className="flex items-center justify-between border-t border-[var(--k-border)] px-2 py-1.5">
            <span className="text-xs text-[var(--k-muted)]">
              {stockList?.total ?? 0} 只
              {stockOffset > 0 ? (
                <Button variant="ghost" size="sm" className="h-6 text-xs" onClick={() => setStockOffset((o) => Math.max(0, o - stockLimit))}>
                  Prev
                </Button>
              ) : null}
              {stockOffset + stockLimit < (stockList?.total ?? 0) ? (
                <Button variant="ghost" size="sm" className="h-6 text-xs" onClick={() => setStockOffset((o) => o + stockLimit)}>
                  Next
                </Button>
              ) : null}
            </span>
          </div>
        </div>

        <div className="flex flex-col gap-3 rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] lg:col-span-4">
          <div className="border-b border-[var(--k-border)] px-3 py-2 text-sm font-medium">选中股票 · 过去 20 日 股价 / 成交量 / MACD</div>
          <div className="flex-1 overflow-auto p-2">
            {selectedStocks.length === 0 ? (
              <div className="py-8 text-center text-xs text-[var(--k-muted)]">从左侧表格添加股票，用于查看图表与买入</div>
            ) : (
              <ul className="space-y-4">
                {selectedStocks.map((sym) => {
                  const chart = chartDataBySymbol[sym];
                  const rows = chart?.ohlcv ?? [];
                  const dif = chart?.dif ?? [];
                  const dea = chart?.dea ?? [];
                  const hist = chart?.hist ?? [];
                  return (
                    <li key={sym} className="rounded border border-[var(--k-border)] bg-[var(--k-surface-2)] p-2">
                      <div className="flex items-center justify-between">
                        <span className="font-mono text-sm font-medium">{sym}</span>
                        <Button variant="ghost" size="sm" className="h-6 w-6 p-0" onClick={() => removeFromSelected(sym)}>
                          <X className="h-4 w-4" />
                        </Button>
                      </div>
                      {rows.length === 0 ? (
                        <div className="mt-2 text-xs text-[var(--k-muted)]">加载中…</div>
                      ) : (
                        <>
                          <div className="mt-2 h-[200px] w-full">
                            <SimCandleChart data={rows} height={200} showVolume={true} />
                          </div>
                          <div className="mt-2 overflow-x-auto">
                          <table className="w-full text-[10px]">
                            <thead>
                              <tr className="text-[var(--k-muted)]">
                                <th className="px-1 py-0.5 text-left">日期</th>
                                <th className="px-1 py-0.5 text-right">收盘</th>
                                <th className="px-1 py-0.5 text-right">成交量</th>
                                <th className="px-1 py-0.5 text-right">DIF</th>
                                <th className="px-1 py-0.5 text-right">DEA</th>
                                <th className="px-1 py-0.5 text-right">Hist</th>
                              </tr>
                            </thead>
                            <tbody>
                              {rows.map((r, i) => (
                                <tr key={r.time} className="border-t border-[var(--k-border)]">
                                  <td className="px-1 py-0.5 font-mono">{r.time}</td>
                                  <td className="px-1 py-0.5 text-right font-mono">{r.close.toFixed(2)}</td>
                                  <td className="px-1 py-0.5 text-right font-mono">{(r.volume / 1e4).toFixed(1)}万</td>
                                  <td className="px-1 py-0.5 text-right font-mono">{(dif[i] ?? 0).toFixed(3)}</td>
                                  <td className="px-1 py-0.5 text-right font-mono">{(dea[i] ?? 0).toFixed(3)}</td>
                                  <td className="px-1 py-0.5 text-right font-mono">{(hist[i] ?? 0).toFixed(3)}</td>
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                        </>
                      )}
                    </li>
                  );
                })}
              </ul>
            )}
          </div>
        </div>

        <div className="flex flex-col rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] lg:col-span-4">
          <div className="border-b border-[var(--k-border)] px-3 py-2 text-sm font-medium">买卖操作</div>
          <div className="flex flex-1 flex-col gap-4 p-3">
            <div>
              <div className="mb-2 text-xs font-medium text-[var(--k-muted)]">明日买入（100 的整数倍）</div>
              {selectedStocks.length === 0 ? (
                <p className="text-xs text-[var(--k-muted)]">先从左栏添加股票</p>
              ) : (
                <ul className="space-y-2">
                  {selectedStocks.map((sym) => (
                    <li key={sym} className="flex items-center gap-2">
                      <span className="min-w-[80px] font-mono text-xs">{sym}</span>
                      <input
                        type="number"
                        min={0}
                        step={100}
                        className="w-24 rounded border border-[var(--k-border)] bg-[var(--k-bg)] px-2 py-1 text-xs"
                        placeholder="股数"
                        value={buyQtyInputs[sym] ?? ''}
                        onChange={(e) => setBuyQtyInputs((p) => ({ ...p, [sym]: e.target.value }))}
                      />
                      <Button size="sm" className="h-7 text-xs" onClick={() => addToBuyQueue(sym)}>
                        加入买入
                      </Button>
                    </li>
                  ))}
                </ul>
              )}
              {buyQueue.length > 0 ? (
                <div className="mt-2 text-xs">
                  待买入:{' '}
                  {buyQueue.map(({ symbol, qty }) => (
                    <span key={symbol} className="mr-2 inline-flex items-center gap-1 rounded bg-[var(--k-surface-2)] px-1.5 py-0.5">
                      {symbol} {qty}
                      <button type="button" className="hover:opacity-80" onClick={() => removeFromBuyQueue(symbol)}>
                        <X className="h-3 w-3" />
                      </button>
                    </span>
                  ))}
                </div>
              ) : null}
            </div>
            <div>
              <div className="mb-2 text-xs font-medium text-[var(--k-muted)]">明日卖出</div>
              {positionSymbols.length === 0 ? (
                <p className="text-xs text-[var(--k-muted)]">当前无持仓</p>
              ) : (
                <ul className="space-y-1">
                  {positionSymbols.map((sym) => {
                    const pos = positions.get(sym)!;
                    const inSell = sellQueue.includes(sym);
                    return (
                      <li key={sym} className="flex items-center justify-between text-xs">
                        <span className="font-mono">
                          {sym} × {pos.qty} @ {pos.cost.toFixed(2)}
                        </span>
                        <Button
                          size="sm"
                          variant={inSell ? 'secondary' : 'ghost'}
                          className="h-6 text-xs"
                          onClick={() => (inSell ? removeFromSellQueue(sym) : addToSellQueue(sym))}
                        >
                          {inSell ? '取消卖出' : '卖出'}
                        </Button>
                      </li>
                    );
                  })}
                </ul>
              )}
              {sellQueue.length > 0 ? (
                <div className="mt-2 text-xs text-[var(--k-muted)]">待卖出: {sellQueue.join(', ')}</div>
              ) : null}
            </div>
            <Button className="mt-auto" disabled={!canNextDay} onClick={() => void runNextDay()}>
              <ChevronRight className="mr-1 h-4 w-4" />
              下一天
            </Button>
          </div>
        </div>
        </div>

        <div className="sticky bottom-0 z-10 mt-3 shrink-0 rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] p-4 shadow-[0_-2px 8px rgba(0,0,0,0.06)]">
        <div className="mb-2 text-sm font-medium">持仓与收益</div>
        <div className="flex flex-wrap items-center gap-6">
          <div className="text-xs">
            <span className="text-[var(--k-muted)]">现金 </span>
            <span className="font-mono font-semibold">{cash.toLocaleString('zh-CN', { minimumFractionDigits: 2 })}</span>
          </div>
          <div className="text-xs">
            <span className="text-[var(--k-muted)]">持仓市值 </span>
            <span className="font-mono font-semibold">{marketValue.toLocaleString('zh-CN', { minimumFractionDigits: 2 })}</span>
          </div>
          <div className="text-xs">
            <span className="text-[var(--k-muted)]">总资产 </span>
            <span className="font-mono font-semibold">{totalEquity.toLocaleString('zh-CN', { minimumFractionDigits: 2 })}</span>
          </div>
          <div className="text-xs">
            <span className="text-[var(--k-muted)]">收益率 </span>
            <span className={`font-mono font-semibold ${yieldPct >= 0 ? 'text-green-600' : 'text-red-600'}`}>
              {yieldPct >= 0 ? '+' : ''}
              {yieldPct.toFixed(2)}%
            </span>
          </div>
        </div>
        {positionSymbols.length > 0 ? (
          <div className="mt-3 overflow-x-auto">
            <table className="w-full text-xs">
              <thead>
                <tr className="text-left text-[var(--k-muted)]">
                  <th className="px-2 py-1">标的</th>
                  <th className="px-2 py-1 text-right">数量</th>
                  <th className="px-2 py-1 text-right">成本</th>
                  <th className="px-2 py-1 text-right">市值</th>
                </tr>
              </thead>
              <tbody>
                {positionSymbols.map((sym) => {
                  const pos = positions.get(sym)!;
                  const mv = pos.qty * pos.cost;
                  return (
                    <tr key={sym} className="border-t border-[var(--k-border)]">
                      <td className="px-2 py-1 font-mono">{sym}</td>
                      <td className="px-2 py-1 text-right">{pos.qty}</td>
                      <td className="px-2 py-1 text-right">{pos.cost.toFixed(2)}</td>
                      <td className="px-2 py-1 text-right">{mv.toLocaleString('zh-CN', { minimumFractionDigits: 2 })}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        ) : null}
        </div>
      </div>
    </div>
  );
}
