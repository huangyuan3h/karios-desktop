'use client';

import * as React from 'react';

import { loadJson, saveJson } from '@/lib/storage';

/** Account input for integer-lot sizing (OPT-204). Local-only settings: the
 * app never had an account-size field, and positions are persisted as % of
 * total assets — this fills the missing denominator for order conversion. */

const CAPITAL_KEY = 'karios.accountCapital.v1';
const FX_KEY = 'karios.hkdCnyRate.v1';
const EVENT = 'karios:account-settings';

export const DEFAULT_HKD_CNY = 0.92;

export function getAccountCapital(): number | null {
  const raw = loadJson<unknown>(CAPITAL_KEY, null);
  const n = typeof raw === 'number' ? raw : Number(raw);
  return Number.isFinite(n) && n > 0 ? n : null;
}

export function setAccountCapital(value: number | null): void {
  if (value == null || !Number.isFinite(value) || value <= 0) {
    window.localStorage.removeItem(CAPITAL_KEY);
  } else {
    saveJson(CAPITAL_KEY, value);
  }
  window.dispatchEvent(new Event(EVENT));
}

export function getHkdCnyRate(): number {
  const n = Number(loadJson<unknown>(FX_KEY, DEFAULT_HKD_CNY));
  return Number.isFinite(n) && n > 0.1 && n < 10 ? n : DEFAULT_HKD_CNY;
}

export function setHkdCnyRate(value: number): void {
  if (Number.isFinite(value) && value > 0.1 && value < 10) {
    saveJson(FX_KEY, value);
    window.dispatchEvent(new Event(EVENT));
  }
}

export function useAccountSettings(): { capital: number | null; rate: number } {
  const [state, setState] = React.useState(() => ({
    capital: getAccountCapital(),
    rate: getHkdCnyRate(),
  }));
  React.useEffect(() => {
    const sync = () => setState({ capital: getAccountCapital(), rate: getHkdCnyRate() });
    sync();
    window.addEventListener(EVENT, sync);
    window.addEventListener('storage', sync);
    return () => {
      window.removeEventListener(EVENT, sync);
      window.removeEventListener('storage', sync);
    };
  }, []);
  return state;
}
