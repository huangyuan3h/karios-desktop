import { fireEvent, render, screen } from '@testing-library/react';
import { afterEach, describe, expect, it } from 'vitest';

import { StrategyModeBar } from './StrategyModeBar';

afterEach(() => {
  window.localStorage.removeItem('karios.strategyMode.v2');
  window.localStorage.removeItem('karios.strategyMode');
});

describe('StrategyModeBar', () => {
  it('renders the five strategy views with the default selected', () => {
    render(<StrategyModeBar />);
    for (const name of ['港湾', '母港', '星港', '星舰', '双子星']) {
      expect(screen.getByRole('button', { name: new RegExp(name) })).toBeDefined();
    }
    expect(screen.getByRole('button', { name: /星港/ }).getAttribute('aria-pressed')).toBe('true');
  });

  it('switches the view and persists the selection', () => {
    render(<StrategyModeBar />);
    fireEvent.click(screen.getByRole('button', { name: /港湾/ }));
    expect(screen.getByRole('button', { name: /港湾/ }).getAttribute('aria-pressed')).toBe('true');
    expect(window.localStorage.getItem('karios.strategyMode.v2')).toBe('"harbor"');
  });
});
