import { describe, expect, it } from 'vitest';

import {
  isCnWatchlistSymbol,
  isEtfWatchlistSymbol,
  isHkWatchlistSymbol,
  toTsCodeFromSymbol,
} from '@/lib/symbols';

describe('toTsCodeFromSymbol', () => {
  it('converts CN:6xxxxx to SH', () => {
    expect(toTsCodeFromSymbol('CN:600000')).toBe('600000.SH');
    expect(toTsCodeFromSymbol('CN:688981')).toBe('688981.SH');
  });

  it('converts CN:0xxxxx/3xxxxx to SZ', () => {
    expect(toTsCodeFromSymbol('CN:000001')).toBe('000001.SZ');
    expect(toTsCodeFromSymbol('CN:300750')).toBe('300750.SZ');
  });

  it('converts HK:00700 to 00700.HK with zero-padding', () => {
    expect(toTsCodeFromSymbol('HK:00700')).toBe('00700.HK');
    expect(toTsCodeFromSymbol('HK:700')).toBe('00700.HK');
    expect(toTsCodeFromSymbol('HK:5')).toBe('00005.HK');
  });

  it('converts ETF:5xxxxx to SH', () => {
    expect(toTsCodeFromSymbol('ETF:510300')).toBe('510300.SH');
    expect(toTsCodeFromSymbol('ETF:512480')).toBe('512480.SH');
    expect(toTsCodeFromSymbol('ETF:588000')).toBe('588000.SH');
  });

  it('converts ETF:1xxxxx/9xxxxx to SZ/SH', () => {
    expect(toTsCodeFromSymbol('ETF:159819')).toBe('159819.SZ');
    expect(toTsCodeFromSymbol('ETF:159099')).toBe('159099.SZ');
    expect(toTsCodeFromSymbol('ETF:513050')).toBe('513050.SH');
  });

  it('returns null on invalid input', () => {
    expect(toTsCodeFromSymbol('')).toBeNull();
    expect(toTsCodeFromSymbol('INVALID')).toBeNull();
    expect(toTsCodeFromSymbol('CN:ABC')).toBeNull();
    expect(toTsCodeFromSymbol('HK:ABC')).toBeNull();
    expect(toTsCodeFromSymbol('ETF:ABC')).toBeNull();
    expect(toTsCodeFromSymbol('ETF:12345')).toBeNull();
  });
});

describe('market predicates', () => {
  it('isCnWatchlistSymbol matches CN: prefix only', () => {
    expect(isCnWatchlistSymbol('CN:600519')).toBe(true);
    expect(isCnWatchlistSymbol('HK:00700')).toBe(false);
    expect(isCnWatchlistSymbol('ETF:510300')).toBe(false);
  });

  it('isHkWatchlistSymbol matches HK: prefix only', () => {
    expect(isHkWatchlistSymbol('HK:00700')).toBe(true);
    expect(isHkWatchlistSymbol('CN:600519')).toBe(false);
    expect(isHkWatchlistSymbol('ETF:510300')).toBe(false);
  });

  it('isEtfWatchlistSymbol matches ETF: prefix only', () => {
    expect(isEtfWatchlistSymbol('ETF:510300')).toBe(true);
    expect(isEtfWatchlistSymbol('CN:510300')).toBe(false);
    expect(isEtfWatchlistSymbol('HK:510300')).toBe(false);
  });
});