import { describe, expect, it } from 'vitest';

import {
  formatScreenerCell,
  formatScreenerRow,
  isAlreadyFormattedScreenerValue,
  isScreenerTextColumn,
} from './screener-format';

describe('formatScreenerCell', () => {
  describe('passthrough', () => {
    it('returns empty for null / undefined / empty', () => {
      expect(formatScreenerCell('Volume', null)).toBe('');
      expect(formatScreenerCell('Volume', undefined)).toBe('');
      expect(formatScreenerCell('Volume', '')).toBe('');
      expect(formatScreenerCell('Volume', '   ')).toBe('');
    });

    it('passes through text columns unchanged', () => {
      expect(formatScreenerCell('Symbol', '603301')).toBe('603301');
      expect(formatScreenerCell('Ticker', '000333')).toBe('000333');
      expect(formatScreenerCell('Name', 'Midea Group Co. Ltd. Class A')).toBe(
        'Midea Group Co. Ltd. Class A',
      );
      expect(formatScreenerCell('Sector', 'Health technology')).toBe(
        'Health technology',
      );
      expect(formatScreenerCell('Industry', 'Pharmaceuticals: Major')).toBe(
        'Pharmaceuticals: Major',
      );
      expect(formatScreenerCell('Country', 'China')).toBe('China');
      expect(formatScreenerCell('Flags', 'D')).toBe('D');
      expect(formatScreenerCell('Analyst rating', 'Strong buy')).toBe(
        'Strong buy',
      );
    });

    it('passes through already-formatted values', () => {
      expect(formatScreenerCell('Change %', '+3.82%')).toBe('+3.82%');
      expect(formatScreenerCell('Change %', '−0.55%')).toBe('−0.55%');
      expect(formatScreenerCell('Market cap', '165.78 B USD')).toBe(
        '165.78 B USD',
      );
      expect(formatScreenerCell('Market cap', '7 B USD')).toBe('7 B USD');
      expect(formatScreenerCell('Avg Volume 10D', '11.06 M')).toBe('11.06 M');
      expect(formatScreenerCell('High 52W', '104.84 CNY')).toBe('104.84 CNY');
      expect(formatScreenerCell('Price', '44.00 CNY')).toBe('44.00 CNY');
      expect(formatScreenerCell('Div yield % TTM', '1.47%')).toBe('1.47%');
      expect(formatScreenerCell('Perf % 1M', '+7.08%')).toBe('+7.08%');
      expect(
        formatScreenerCell('EPS dil growth TTM YoY', '+24.02%'),
      ).toBe('+24.02%');
    });

    it('passes through non-parseable values', () => {
      expect(formatScreenerCell('Volume', 'N/A')).toBe('N/A');
      expect(formatScreenerCell('Price', '--')).toBe('--');
    });
  });

  describe('change / percentage columns', () => {
    it('formats raw change percent with 2 decimals and sign', () => {
      expect(formatScreenerCell('Change %', '-1.5177065767285085')).toBe(
        '-1.52%',
      );
      expect(formatScreenerCell('Change %', '5.699045480067381')).toBe(
        '+5.70%',
      );
      expect(formatScreenerCell('Change %', '0')).toBe('0.00%');
    });

    it('handles Chg % alias', () => {
      expect(formatScreenerCell('Chg %', '-1.52')).toBe('-1.52%');
    });

    it('formats Perf.Y as percent', () => {
      expect(formatScreenerCell('Perf.Y', '21.66666666666666')).toBe('+21.67%');
      expect(formatScreenerCell('Perf.Y', '-3.5')).toBe('-3.50%');
    });
  });

  describe('big-number columns (Volume, Market Cap)', () => {
    it('formats Volume with B/M/K units', () => {
      expect(formatScreenerCell('Volume', '47510154')).toBe('47.51M');
      expect(formatScreenerCell('Volume', '171635716')).toBe('171.64M');
      expect(formatScreenerCell('Volume', '4143954')).toBe('4.14M');
      expect(formatScreenerCell('Volume', '1234')).toBe('1.23K');
      expect(formatScreenerCell('Volume', '999')).toBe('999');
    });

    it('formats Market Cap with B/M/K units', () => {
      expect(formatScreenerCell('Market Cap', '97126646894.81198')).toBe(
        '97.13B',
      );
      expect(formatScreenerCell('Market Cap', '51705889066.44177')).toBe(
        '51.71B',
      );
      expect(formatScreenerCell('Market Cap', '36862319043.2121')).toBe(
        '36.86B',
      );
      expect(formatScreenerCell('Market cap', '165780000000')).toBe('165.78B');
    });

    it('formats Avg Volume 10D as big number when raw', () => {
      expect(formatScreenerCell('Avg Volume 10D', '11000000')).toBe('11.00M');
    });

    it('handles negative big numbers with sign', () => {
      expect(formatScreenerCell('Volume', '-1234567')).toBe('-1.23M');
      expect(formatScreenerCell('Market Cap', '-50000000')).toBe('-50.00M');
    });
  });

  describe('price-like / technical indicators', () => {
    it('formats P/E to 2 decimals', () => {
      expect(formatScreenerCell('P/E', '15.127443531118324')).toBe('15.13');
      expect(formatScreenerCell('P/E', '108.8300099043566')).toBe('108.83');
    });

    it('formats RSI to 2 decimals', () => {
      expect(formatScreenerCell('RSI', '67.86506094476763')).toBe('67.87');
      expect(formatScreenerCell('RSI (14)', '66.45')).toBe('66.45');
    });

    it('formats MACD to 2 decimals', () => {
      expect(formatScreenerCell('MACD', '2.010955384738665')).toBe('2.01');
      expect(formatScreenerCell('MACD', '-0.5')).toBe('-0.50');
    });

    it('formats SMA / EMA stripping floating-point noise', () => {
      expect(formatScreenerCell('SMA20', '83.06549999999979')).toBe('83.07');
      expect(formatScreenerCell('SMA50', '81.1959999999999')).toBe('81.20');
      expect(formatScreenerCell('EMA50', '81.73253825769747')).toBe('81.73');
      expect(formatScreenerCell('EMA200', '78.69056116651355')).toBe('78.69');
    });

    it('formats raw Price to 2 decimals', () => {
      expect(formatScreenerCell('Price', '87.6')).toBe('87.60');
      expect(formatScreenerCell('Price', '44')).toBe('44.00');
    });
  });

  describe('rel volume / ratios', () => {
    it('keeps Rel Volume as 2 decimals (already compact)', () => {
      expect(formatScreenerCell('Rel Volume', '3.63')).toBe('3.63');
      expect(formatScreenerCell('Rel Volume', '0.84')).toBe('0.84');
      expect(formatScreenerCell('Rel Volume 1W', '0.85')).toBe('0.85');
    });
  });

  describe('unknown numeric columns', () => {
    it('falls back to 2 decimals for unknown headers', () => {
      expect(formatScreenerCell('Some Metric', '3.14159')).toBe('3.14');
    });
  });
});

describe('formatScreenerRow', () => {
  it('formats every value in a row according to its header', () => {
    const headers = [
      'Ticker',
      'Name',
      'Price',
      'Change %',
      'Volume',
      'Market Cap',
      'P/E',
      'RSI',
    ];
    const row: Record<string, string> = {
      Ticker: '000333',
      Name: 'Midea Group Co. Ltd. Class A',
      Price: '87.6',
      'Change %': '-1.5177065767285085',
      Volume: '47510154',
      'Market Cap': '97126646894.81198',
      'P/E': '15.127443531118324',
      RSI: '67.86506094476763',
    };
    expect(formatScreenerRow(headers, row)).toEqual({
      Ticker: '000333',
      Name: 'Midea Group Co. Ltd. Class A',
      Price: '87.60',
      'Change %': '-1.52%',
      Volume: '47.51M',
      'Market Cap': '97.13B',
      'P/E': '15.13',
      RSI: '67.87',
    });
  });

  it('handles already-formatted values alongside raw ones', () => {
    const headers = ['Symbol', 'Change %', 'Market cap', 'Avg Volume 10D'];
    const row: Record<string, string> = {
      Symbol: '601628',
      'Change %': '−0.55%',
      'Market cap': '165.78 B USD',
      'Avg Volume 10D': '11.06 M',
    };
    expect(formatScreenerRow(headers, row)).toEqual({
      Symbol: '601628',
      'Change %': '−0.55%',
      'Market cap': '165.78 B USD',
      'Avg Volume 10D': '11.06 M',
    });
  });
});

describe('isAlreadyFormattedScreenerValue', () => {
  it('detects percent / unit / currency / signed prefixes', () => {
    expect(isAlreadyFormattedScreenerValue('+3.82%')).toBe(true);
    expect(isAlreadyFormattedScreenerValue('−0.55%')).toBe(true);
    expect(isAlreadyFormattedScreenerValue('11.06 M')).toBe(true);
    expect(isAlreadyFormattedScreenerValue('165.78 B USD')).toBe(true);
    expect(isAlreadyFormattedScreenerValue('44.00 CNY')).toBe(true);
    expect(isAlreadyFormattedScreenerValue('+7.08%')).toBe(true);
  });

  it('detects raw numbers as not formatted', () => {
    expect(isAlreadyFormattedScreenerValue('47510154')).toBe(false);
    expect(isAlreadyFormattedScreenerValue('-1.5177065767285085')).toBe(false);
    expect(isAlreadyFormattedScreenerValue('97126646894.81198')).toBe(false);
  });
});

describe('isScreenerTextColumn', () => {
  it('identifies identifier / categorical headers', () => {
    expect(isScreenerTextColumn('Symbol')).toBe(true);
    expect(isScreenerTextColumn('Ticker')).toBe(true);
    expect(isScreenerTextColumn('Name')).toBe(true);
    expect(isScreenerTextColumn('Sector')).toBe(true);
    expect(isScreenerTextColumn('Industry')).toBe(true);
    expect(isScreenerTextColumn('Country')).toBe(true);
    expect(isScreenerTextColumn('Flags')).toBe(true);
    expect(isScreenerTextColumn('Analyst rating')).toBe(true);
    expect(isScreenerTextColumn('Analyst Rating')).toBe(true);
  });

  it('treats numeric headers as non-text', () => {
    expect(isScreenerTextColumn('Change %')).toBe(false);
    expect(isScreenerTextColumn('Volume')).toBe(false);
    expect(isScreenerTextColumn('Market Cap')).toBe(false);
    expect(isScreenerTextColumn('P/E')).toBe(false);
    expect(isScreenerTextColumn('RSI')).toBe(false);
  });
});
