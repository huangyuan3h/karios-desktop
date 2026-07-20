import { describe, expect, it } from 'vitest';
import { computeMacd, computeKdj, type OHLCV } from './indicators';

describe('computeMacd', () => {
  it('computes MACD for simple data', () => {
    const data: OHLCV[] = [
      { time: '2024-01-01', open: 100, high: 105, low: 95, close: 102, volume: 1000 },
      { time: '2024-01-02', open: 102, high: 108, low: 100, close: 106, volume: 1200 },
      { time: '2024-01-03', open: 106, high: 110, low: 104, close: 108, volume: 1500 },
    ];
    const result = computeMacd(data);
    expect(result.dif.length).toBe(3);
    expect(result.dea.length).toBe(3);
    expect(result.hist.length).toBe(3);
  });

  it('handles empty data', () => {
    const result = computeMacd([]);
    expect(result.dif).toEqual([]);
    expect(result.dea).toEqual([]);
    expect(result.hist).toEqual([]);
  });

  it('uses custom periods', () => {
    const data: OHLCV[] = [
      { time: '2024-01-01', open: 100, high: 105, low: 95, close: 100, volume: 1000 },
      { time: '2024-01-02', open: 100, high: 105, low: 95, close: 101, volume: 1000 },
    ];
    const result = computeMacd(data, 5, 10, 3);
    expect(result.dif.length).toBe(2);
  });
});

describe('computeKdj', () => {
  it('computes KDJ for simple data', () => {
    const data: OHLCV[] = [
      { time: '2024-01-01', open: 100, high: 105, low: 95, close: 100, volume: 1000 },
      { time: '2024-01-02', open: 100, high: 110, low: 90, close: 105, volume: 1200 },
      { time: '2024-01-03', open: 105, high: 115, low: 100, close: 110, volume: 1500 },
    ];
    const result = computeKdj(data);
    expect(result.k.length).toBe(3);
    expect(result.d.length).toBe(3);
    expect(result.j.length).toBe(3);
  });

  it('handles empty data', () => {
    const result = computeKdj([]);
    expect(result.k).toEqual([]);
    expect(result.d).toEqual([]);
    expect(result.j).toEqual([]);
  });

  it('computes RSV correctly when high equals low', () => {
    const data: OHLCV[] = [
      { time: '2024-01-01', open: 100, high: 100, low: 100, close: 100, volume: 1000 },
    ];
    const result = computeKdj(data);
    expect(result.k[0]).toBe(50);
  });
});
