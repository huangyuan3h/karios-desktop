import { fireEvent, render, screen } from '@testing-library/react';
import { afterEach, describe, expect, it } from 'vitest';

import { StrategyModeBar } from './StrategyModeBar';

afterEach(() => {
  window.localStorage.removeItem('karios.strategyMode.v2');
  window.localStorage.removeItem('karios.strategyMode');
});

describe('StrategyModeBar', () => {
  it('renders 星舰 B as default next to 稳健星舰 H2-a25 / 星舰', () => {
    render(<StrategyModeBar />);
    for (const name of ['星舰 B', '稳健星舰 H2-a25', '星舰', '星港', '母港', '港湾', '双子星']) {
      // anchor at the B label: `^星舰` would also match `星舰 B`.
      const re = name === '星舰' ? /^星舰(?!\s*B)/ : new RegExp(`^${name}`);
      expect(screen.getByRole('button', { name: re })).toBeDefined();
    }
    // 星舰 B is the current default.
    expect(
      screen.getByRole('button', { name: /^星舰 B/ }).getAttribute('aria-pressed'),
    ).toBe('true');
    expect(screen.getByRole('button', { name: /进攻/ })).toBeDefined();
    expect(screen.getByRole('button', { name: /防守/ })).toBeDefined();
    expect(screen.getByRole('button', { name: /均衡/ })).toBeDefined();
    const order = screen
      .getAllByRole('button')
      .map((b) => b.textContent ?? '')
      .map((t) =>
        ['星舰 B', '稳健星舰 H2-a25', '星舰', '星港', '母港', '港湾', '双子星'].find((n) => t.startsWith(n)),
      );
    expect(order).toEqual(['星舰 B', '稳健星舰 H2-a25', '星舰', '星港', '母港', '港湾', '双子星']);
  });

  it('switches the view and persists the selection', () => {
    render(<StrategyModeBar />);
    fireEvent.click(screen.getByRole('button', { name: /港湾/ }));
    expect(screen.getByRole('button', { name: /港湾/ }).getAttribute('aria-pressed')).toBe('true');
    expect(window.localStorage.getItem('karios.strategyMode.v2')).toBe('"harbor"');
  });
});
