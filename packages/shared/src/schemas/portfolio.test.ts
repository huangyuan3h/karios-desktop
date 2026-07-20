import { describe, expect, it } from 'vitest';
import { PortfolioSnapshotSchema } from './portfolio';

describe('PortfolioSnapshotSchema', () => {
  it('validates a complete portfolio snapshot', () => {
    const portfolio = {
      asOf: '2024-01-01T00:00:00Z',
      baseCurrency: 'USD',
      totalValue: 100000,
      positions: [
        { symbol: 'AAPL', quantity: 100, price: 150, currency: 'USD' },
        { symbol: 'GOOG', quantity: 50 },
      ],
    };
    const result = PortfolioSnapshotSchema.parse(portfolio);
    expect(result.baseCurrency).toBe('USD');
    expect(result.positions.length).toBe(2);
    expect(result.positions[0]?.symbol).toBe('AAPL');
  });

  it('validates portfolio with empty positions', () => {
    const portfolio = {
      asOf: '2024-01-01T00:00:00Z',
      baseCurrency: 'CNY',
      totalValue: 0,
      positions: [],
    };
    const result = PortfolioSnapshotSchema.parse(portfolio);
    expect(result.positions).toEqual([]);
  });

  it('rejects portfolio with missing required fields', () => {
    const portfolio = {
      asOf: '2024-01-01T00:00:00Z',
      positions: [],
    };
    expect(() => PortfolioSnapshotSchema.parse(portfolio)).toThrow();
  });
});
