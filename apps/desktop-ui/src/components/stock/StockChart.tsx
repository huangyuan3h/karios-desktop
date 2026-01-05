'use client';

import * as React from 'react';
import type { IChartApi, LogicalRange, Time } from 'lightweight-charts';
import {
  CandlestickSeries,
  ColorType,
  HistogramSeries,
  LineSeries,
  createChart,
} from 'lightweight-charts';

import type { OHLCV } from '@/lib/indicators';
import { computeKdj, computeMacd } from '@/lib/indicators';

type Props = {
  data: OHLCV[];
};

function clampNumber(v: number, lo: number, hi: number) {
  return Math.min(hi, Math.max(lo, v));
}

function parseTime(t: string): Time {
  // lightweight-charts supports BusinessDay strings as "YYYY-MM-DD"
  return t as Time;
}

export function StockChart({ data }: Props) {
  const priceRef = React.useRef<HTMLDivElement | null>(null);
  const volRef = React.useRef<HTMLDivElement | null>(null);
  const macdRef = React.useRef<HTMLDivElement | null>(null);
  const kdjRef = React.useRef<HTMLDivElement | null>(null);

  React.useEffect(() => {
    const priceEl = priceRef.current;
    const volEl = volRef.current;
    const macdEl = macdRef.current;
    const kdjEl = kdjRef.current;
    if (!priceEl || !volEl || !macdEl || !kdjEl) return;
    if (data.length === 0) return;

    const bg = getComputedStyle(document.documentElement).getPropertyValue('--k-bg').trim() || '#fff';
    const text = getComputedStyle(document.documentElement).getPropertyValue('--k-text').trim() || '#111';
    const border = getComputedStyle(document.documentElement).getPropertyValue('--k-border').trim() || '#e5e7eb';
    const muted = getComputedStyle(document.documentElement).getPropertyValue('--k-muted').trim() || '#6b7280';

    const common = (el: HTMLElement) => {
      return createChart(el, {
        layout: {
          background: { type: ColorType.Solid, color: bg },
          textColor: text,
        },
        rightPriceScale: { borderColor: border },
        timeScale: { borderColor: border, timeVisible: true },
        grid: {
          horzLines: { color: border, style: 0 },
          vertLines: { color: border, style: 0 },
        },
        crosshair: { mode: 1 },
        handleScale: true,
        handleScroll: true,
      });
    };

    const priceChart = common(priceEl);
    const volChart = common(volEl);
    const macdChart = common(macdEl);
    const kdjChart = common(kdjEl);

    const charts: IChartApi[] = [priceChart, volChart, macdChart, kdjChart];

    // Series: candles + volume histogram.
    const candle = priceChart.addSeries(CandlestickSeries, {
      upColor: '#16a34a',
      downColor: '#dc2626',
      borderUpColor: '#16a34a',
      borderDownColor: '#dc2626',
      wickUpColor: '#16a34a',
      wickDownColor: '#dc2626',
    });

    const vol = volChart.addSeries(HistogramSeries, {
      priceFormat: { type: 'volume' },
      priceScaleId: 'right',
      color: muted,
    });

    // MACD: dif/dea lines + histogram.
    const { dif, dea, hist } = computeMacd(data);
    const macdHist = macdChart.addSeries(HistogramSeries, {
      priceScaleId: 'right',
      priceFormat: { type: 'price', precision: 4, minMove: 0.0001 },
    });
    const difLine = macdChart.addSeries(LineSeries, {
      color: '#2563eb',
      lineWidth: 2,
    });
    const deaLine = macdChart.addSeries(LineSeries, {
      color: '#f59e0b',
      lineWidth: 2,
    });

    // KDJ: K/D/J lines.
    const { k, d, j } = computeKdj(data);
    const kLine = kdjChart.addSeries(LineSeries, { color: '#2563eb', lineWidth: 2 });
    const dLine = kdjChart.addSeries(LineSeries, { color: '#f59e0b', lineWidth: 2 });
    const jLine = kdjChart.addSeries(LineSeries, { color: '#ef4444', lineWidth: 2 });

    // Data mapping
    candle.setData(
      data.map((x) => ({
        time: parseTime(x.time),
        open: x.open,
        high: x.high,
        low: x.low,
        close: x.close,
      })),
    );

    vol.setData(
      data.map((x) => ({
        time: parseTime(x.time),
        value: x.volume,
        color: x.close >= x.open ? 'rgba(22,163,74,0.55)' : 'rgba(220,38,38,0.55)',
      })),
    );

    macdHist.setData(
      data.map((x, i) => ({
        time: parseTime(x.time),
        value: hist[i] ?? 0,
        color: (hist[i] ?? 0) >= 0 ? 'rgba(22,163,74,0.55)' : 'rgba(220,38,38,0.55)',
      })),
    );
    difLine.setData(
      data.map((x, i) => ({ time: parseTime(x.time), value: dif[i] ?? 0 })),
    );
    deaLine.setData(
      data.map((x, i) => ({ time: parseTime(x.time), value: dea[i] ?? 0 })),
    );

    kLine.setData(
      data.map((x, i) => ({ time: parseTime(x.time), value: clampNumber(k[i] ?? 50, 0, 100) })),
    );
    dLine.setData(
      data.map((x, i) => ({ time: parseTime(x.time), value: clampNumber(d[i] ?? 50, 0, 100) })),
    );
    jLine.setData(
      data.map((x, i) => ({ time: parseTime(x.time), value: clampNumber(j[i] ?? 50, -20, 120) })),
    );

    priceChart.timeScale().fitContent();
    volChart.timeScale().fitContent();
    macdChart.timeScale().fitContent();
    kdjChart.timeScale().fitContent();

    // Sync visible range from the main (price) chart.
    const onRangeChange = (range: LogicalRange | null) => {
      if (!range) return;
      for (const c of [volChart, macdChart, kdjChart]) {
        c.timeScale().setVisibleLogicalRange(range);
      }
    };
    priceChart.timeScale().subscribeVisibleLogicalRangeChange(onRangeChange);

    // ResizeObserver to keep charts responsive.
    const ro = new ResizeObserver(() => {
      const w = priceEl.clientWidth;
      const h1 = priceEl.clientHeight;
      const h2 = volEl.clientHeight;
      const h3 = macdEl.clientHeight;
      const h4 = kdjEl.clientHeight;
      priceChart.resize(w, h1);
      volChart.resize(w, h2);
      macdChart.resize(w, h3);
      kdjChart.resize(w, h4);
    });
    ro.observe(priceEl);
    ro.observe(volEl);
    ro.observe(macdEl);
    ro.observe(kdjEl);

    return () => {
      ro.disconnect();
      // Unsubscribe and clean up charts.
      priceChart.timeScale().unsubscribeVisibleLogicalRangeChange(onRangeChange);
      for (const c of charts) c.remove();
    };
  }, [data]);

  return (
    <div className="grid gap-2">
      <div className="h-[420px] rounded-xl border border-[var(--k-border)] bg-[var(--k-bg)]">
        <div ref={priceRef} className="h-full w-full" />
      </div>
      <div className="h-[140px] rounded-xl border border-[var(--k-border)] bg-[var(--k-bg)]">
        <div ref={volRef} className="h-full w-full" />
      </div>
      <div className="h-[160px] rounded-xl border border-[var(--k-border)] bg-[var(--k-bg)]">
        <div ref={macdRef} className="h-full w-full" />
      </div>
      <div className="h-[160px] rounded-xl border border-[var(--k-border)] bg-[var(--k-bg)]">
        <div ref={kdjRef} className="h-full w-full" />
      </div>
    </div>
  );
}


