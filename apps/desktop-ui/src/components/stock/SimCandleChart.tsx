'use client';

import * as React from 'react';
import type { IChartApi, Time } from 'lightweight-charts';
import { CandlestickSeries, ColorType, HistogramSeries, createChart } from 'lightweight-charts';

import type { OHLCV } from '@/lib/indicators';

type Props = {
  data: OHLCV[];
  height?: number;
  showVolume?: boolean;
};

function parseTime(t: string): Time {
  return t as Time;
}

export function SimCandleChart({ data, height = 200, showVolume = true }: Props) {
  const priceRef = React.useRef<HTMLDivElement | null>(null);
  const volRef = React.useRef<HTMLDivElement | null>(null);

  React.useEffect(() => {
    const priceEl = priceRef.current;
    const volEl = volRef.current;
    if (!priceEl || !volEl) return;
    if (data.length === 0) return;

    const bg = getComputedStyle(document.documentElement).getPropertyValue('--k-bg').trim() || '#fff';
    const text = getComputedStyle(document.documentElement).getPropertyValue('--k-text').trim() || '#111';
    const border = getComputedStyle(document.documentElement).getPropertyValue('--k-border').trim() || '#e5e7eb';
    const muted = getComputedStyle(document.documentElement).getPropertyValue('--k-muted').trim() || '#6b7280';

    const common = (el: HTMLElement, h: number) =>
      createChart(el, {
        layout: { background: { type: ColorType.Solid, color: bg }, textColor: text },
        rightPriceScale: { borderColor: border },
        timeScale: { borderColor: border, timeVisible: true },
        grid: { horzLines: { color: border, style: 0 }, vertLines: { color: border, style: 0 } },
        crosshair: { mode: 1 },
        handleScale: true,
        handleScroll: true,
      });

    const volHeight = showVolume ? 80 : 0;
    const priceChart = common(priceEl, height - volHeight);
    const candle = priceChart.addSeries(CandlestickSeries, {
      upColor: '#16a34a',
      downColor: '#dc2626',
      borderUpColor: '#16a34a',
      borderDownColor: '#dc2626',
      wickUpColor: '#16a34a',
      wickDownColor: '#dc2626',
    });
    candle.setData(
      data.map((x) => ({
        time: parseTime(x.time),
        open: x.open,
        high: x.high,
        low: x.low,
        close: x.close,
      })),
    );
    priceChart.timeScale().fitContent();

    let volChart: IChartApi | null = null;
    if (showVolume && volHeight > 0) {
      volChart = common(volEl, volHeight);
      const vol = volChart.addSeries(HistogramSeries, {
        priceFormat: { type: 'volume' },
        priceScaleId: 'right',
        color: muted,
      });
      vol.setData(
        data.map((x) => ({
          time: parseTime(x.time),
          value: x.volume,
          color: x.close >= x.open ? 'rgba(22,163,74,0.55)' : 'rgba(220,38,38,0.55)',
        })),
      );
      volChart.timeScale().fitContent();
    }

    const ro = new ResizeObserver(() => {
      const w = priceEl.clientWidth;
      priceChart.resize(w, height - volHeight);
      if (volChart && volEl) volChart.resize(volEl.clientWidth, volHeight);
    });
    ro.observe(priceEl);
    if (volEl) ro.observe(volEl);

    return () => {
      ro.disconnect();
      priceChart.remove();
      if (volChart) volChart.remove();
    };
  }, [data, height, showVolume]);

  const priceH = height - (showVolume ? 80 : 0);
  const volH = showVolume ? 80 : 0;
  return (
    <div className="w-full" style={{ height: `${height}px` }}>
      <div ref={priceRef} style={{ height: `${priceH}px`, width: '100%' }} />
      {showVolume ? <div ref={volRef} style={{ height: `${volH}px`, width: '100%' }} /> : null}
    </div>
  );
}
