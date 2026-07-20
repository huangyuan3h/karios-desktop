import { describe, expect, it } from 'vitest';
import { OrderRecipeSchema, OrderSideSchema } from './orderRecipe';

describe('OrderSideSchema', () => {
  it('accepts valid order sides', () => {
    expect(OrderSideSchema.parse('buy')).toBe('buy');
    expect(OrderSideSchema.parse('sell')).toBe('sell');
  });

  it('rejects invalid order sides', () => {
    expect(() => OrderSideSchema.parse('hold')).toThrow();
  });
});

describe('OrderRecipeSchema', () => {
  it('validates a complete order recipe', () => {
    const order = {
      id: 'order-1',
      symbol: 'AAPL',
      side: 'buy',
      quantity: 100,
      price: 150.5,
      currency: 'USD',
      notes: 'Test order',
      createdAt: '2024-01-01T00:00:00Z',
    };
    const result = OrderRecipeSchema.parse(order);
    expect(result.symbol).toBe('AAPL');
    expect(result.side).toBe('buy');
    expect(result.quantity).toBe(100);
  });

  it('validates order without optional fields', () => {
    const order = {
      id: 'order-2',
      symbol: 'GOOG',
      side: 'sell',
      createdAt: '2024-01-01T00:00:00Z',
    };
    const result = OrderRecipeSchema.parse(order);
    expect(result.quantity).toBeUndefined();
    expect(result.price).toBeUndefined();
  });

  it('rejects order with negative quantity', () => {
    const order = {
      id: 'order-3',
      symbol: 'AAPL',
      side: 'buy',
      quantity: -10,
      createdAt: '2024-01-01T00:00:00Z',
    };
    expect(() => OrderRecipeSchema.parse(order)).toThrow();
  });
});
