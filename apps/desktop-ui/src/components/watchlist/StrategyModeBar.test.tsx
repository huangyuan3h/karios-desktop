import { fireEvent, render, screen } from '@testing-library/react';
import { afterEach, describe, expect, it } from 'vitest';

import { StrategyModeBar } from './StrategyModeBar';

afterEach(() => {
  window.localStorage.removeItem('karios.strategyMode.v2');
  window.localStorage.removeItem('karios.strategyMode');
});

describe('StrategyModeBar', () => {
  it('renders the three tiers plus sunset baseline in role order, default selected', () => {
    render(<StrategyModeBar />);
    for (const name of ['星舰', '星港', '母港', '港湾', '双子星']) {
      expect(screen.getByRole('button', { name: new RegExp(name) })).toBeDefined();
    }
    expect(screen.getByRole('button', { name: /星港/ }).getAttribute('aria-pressed')).toBe('true');
    expect(screen.getByRole('button', { name: /进攻/ })).toBeDefined();
    expect(screen.getByRole('button', { name: /防守/ })).toBeDefined();
    expect(screen.getByRole('button', { name: /均衡/ })).toBeDefined();
    const order = screen
      .getAllByRole('button')
      .map((b) => b.textContent ?? '')
      .map((t) => ['星舰', '星港', '母港', '港湾', '双子星'].find((n) => t.startsWith(n)));
    expect(order).toEqual(['星舰', '星港', '母港', '港湾', '双子星']);
  });

  it('switches the view and persists the selection', () => {
    render(<StrategyModeBar />);
    fireEvent.click(screen.getByRole('button', { name: /港湾/ }));
    expect(screen.getByRole('button', { name: /港湾/ }).getAttribute('aria-pressed')).toBe('true');
    expect(window.localStorage.getItem('karios.strategyMode.v2')).toBe('"harbor"');
  });
});
