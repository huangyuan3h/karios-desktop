/**
 * Canonical parking universe (港湾 idle-cash ETF sleeve) — frontend single
 * source (OPT-206).
 *
 * The engine definitions live in
 * `services/data-sync-service/src/data_sync_service/service/harbor.py`
 * (`MULTI_TS` / `NASDAQ_ALIASES` / `NAMES`). Keep this mirror in sync with it,
 * and prefer the API-provided `key` on holdings when present — never re-derive
 * a competing map inside a component.
 */

export type ParkingKey = 'GOLD' | 'OIL' | 'NASDAQ' | 'BOND10';

export type ParkingMeta = {
  /** Short key label used across the watchlist (e.g. 黄金). */
  label: string;
  /** ETF leg label (e.g. 黄金 ETF). */
  short: string;
  /** Canonical tradable ts_code (the engine's MULTI_TS value). */
  ts: string;
  /** Display hint (bare codes). */
  hint: string;
  /** Canonical + sibling codes that map to this key (display-safe aliases). */
  codes: string[];
};

export const PARKING_META: Record<ParkingKey, ParkingMeta> = {
  GOLD: {
    label: '黄金',
    short: '黄金 ETF',
    ts: '518880.SH',
    hint: '518880',
    codes: ['518880', '518800'],
  },
  OIL: {
    label: '原油',
    short: '原油 ETF',
    ts: '513350.SH',
    hint: '513350',
    codes: ['513350', '159518', '561570'],
  },
  NASDAQ: {
    label: '纳指',
    short: '纳指 ETF',
    ts: '513110.SH',
    hint: '513110/513100',
    codes: ['513110', '513100', '513500', '159941'],
  },
  BOND10: {
    label: '国债',
    short: '国债 ETF',
    ts: '511260.SH',
    hint: '511260',
    codes: ['511260', '511010'],
  },
};

export const PARKING_KEYS = Object.keys(PARKING_META) as ParkingKey[];

/** Symbol / ts_code -> parking key (alias-tolerant; null = not a parking ETF). */
export function parkingKeyForSymbol(symbol: string | null | undefined): ParkingKey | null {
  const s = String(symbol ?? '').toUpperCase();
  if (!s) return null;
  for (const key of PARKING_KEYS) {
    if (PARKING_META[key].codes.some((code) => s.includes(code))) return key;
  }
  return null;
}

/** Display codes for a parking key (empty when unknown). */
export function parkingCodesFor(key: string | null | undefined): string[] {
  if (!key) return [];
  return PARKING_META[key as ParkingKey]?.codes ?? [];
}

/** Every matching symbol shape for a parking key (ETF: code + bare code). */
export function parkingSymbolsFor(key: string | null | undefined): string[] {
  return parkingCodesFor(key).flatMap((code) => [`ETF:${code}`, code]);
}
