export type OHLCV = {
  time: string; // YYYY-MM-DD
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
};

function ema(values: number[], period: number): Array<number | null> {
  if (period <= 0) return values.map(() => null);
  const k = 2 / (period + 1);
  const out: Array<number | null> = [];
  let prev: number | null = null;
  for (let i = 0; i < values.length; i++) {
    const v = values[i];
    if (prev === null) {
      prev = v;
      out.push(prev);
      continue;
    }
    prev = v * k + prev * (1 - k);
    out.push(prev);
  }
  return out;
}

/**
 * Smoothed moving average used by KDJ:
 * SMA(X, m, 1) == (m-1)/m * SMA_prev + 1/m * X
 */
function smoothedSma(values: number[], m: number): Array<number | null> {
  if (m <= 0) return values.map(() => null);
  const out: Array<number | null> = [];
  let prev: number | null = null;
  for (let i = 0; i < values.length; i++) {
    const x = values[i];
    if (prev === null) {
      prev = x;
      out.push(prev);
      continue;
    }
    prev = ((m - 1) / m) * prev + (1 / m) * x;
    out.push(prev);
  }
  return out;
}

export function computeMacd(
  data: OHLCV[],
  fast = 12,
  slow = 26,
  signal = 9,
): {
  dif: Array<number | null>;
  dea: Array<number | null>;
  hist: Array<number | null>;
} {
  const closes = data.map((d) => d.close);
  const emaFast = ema(closes, fast);
  const emaSlow = ema(closes, slow);
  const dif = closes.map((_, i) => {
    const a = emaFast[i];
    const b = emaSlow[i];
    if (a === null || b === null) return null;
    return a - b;
  });
  const difVals = dif.map((v) => v ?? 0);
  const dea = ema(difVals, signal);
  const hist = dif.map((v, i) => {
    if (v === null) return null;
    const s = dea[i];
    if (s === null) return null;
    return (v - s) * 2;
  });
  return { dif, dea, hist };
}

export function computeKdj(
  data: OHLCV[],
  n = 9,
  kPeriod = 3,
  dPeriod = 3,
): {
  k: Array<number | null>;
  d: Array<number | null>;
  j: Array<number | null>;
} {
  const rsv: number[] = [];
  for (let i = 0; i < data.length; i++) {
    const start = Math.max(0, i - n + 1);
    let low = Number.POSITIVE_INFINITY;
    let high = Number.NEGATIVE_INFINITY;
    for (let j = start; j <= i; j++) {
      low = Math.min(low, data[j].low);
      high = Math.max(high, data[j].high);
    }
    const close = data[i].close;
    const denom = high - low;
    const v = denom <= 0 ? 50 : ((close - low) / denom) * 100;
    rsv.push(v);
  }

  const k = smoothedSma(rsv, kPeriod);
  const kVals = k.map((v) => v ?? 50);
  const d = smoothedSma(kVals, dPeriod);
  const j = k.map((kv, i) => {
    const dv = d[i];
    if (kv === null || dv === null) return null;
    return 3 * kv - 2 * dv;
  });
  return { k, d, j };
}


