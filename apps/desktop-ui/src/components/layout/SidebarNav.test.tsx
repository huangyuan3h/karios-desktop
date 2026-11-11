import { render, screen } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';

import { SidebarNav } from './SidebarNav';

describe('SidebarNav', () => {
  it('lists 因子库 immediately after 回测 in the left nav', () => {
    render(<SidebarNav activeId="backtest" onSelect={vi.fn()} />);
    const labels = screen
      .getAllByRole('button')
      .map((el) => (el.textContent ?? '').trim())
      .filter(Boolean);
    const backtest = labels.indexOf('回测');
    expect(backtest).toBeGreaterThanOrEqual(0);
    expect(labels[backtest + 1]).toBe('因子库');
  });

  it('marks the factor library entry active for the factorSystem page id', () => {
    render(<SidebarNav activeId="factorSystem" onSelect={vi.fn()} />);
    const item = screen.getByRole('button', { name: '因子库' });
    expect(item.className).toContain('bg-[var(--k-surface-2)]');
  });
});
