import { fireEvent, render, screen } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';

import { DashboardHeader } from './DashboardHeader';

describe('DashboardHeader', () => {
  it('renders label and sub with aria-label', () => {
    render(<DashboardHeader helpId="etf.name" />);
    const el = screen.getByLabelText(/ETF 名称/);
    expect(el).toHaveAttribute('title');
    expect(screen.getByText('ETF 名称')).toBeInTheDocument();
  });

  it('renders label-only help without sub', () => {
    render(<DashboardHeader helpId="etf.symbol" />);
    expect(screen.getByText(/代码/)).toBeInTheDocument();
  });

  it('falls back to raw helpId when not in DASHBOARD_HELP', () => {
    render(<DashboardHeader helpId="does_not_exist" />);
    expect(screen.getByText('does_not_exist')).toBeInTheDocument();
  });

  it('accepts an explicit help prop override', () => {
    render(
      <DashboardHeader
        helpId="etf.name"
        help={{ label: '自定义', short: 's' } as never}
      />,
    );
    expect(screen.getByText('自定义')).toBeInTheDocument();
  });

  it('shows tooltip on hover and hides on blur', () => {
    render(<DashboardHeader helpId="etf.name" />);
    const el = screen.getByLabelText(/ETF 名称/);
    expect(screen.queryByRole('tooltip')).not.toBeInTheDocument();
    fireEvent.mouseEnter(el);
    expect(screen.getByRole('tooltip')).toBeInTheDocument();
    fireEvent.blur(el);
    expect(screen.queryByRole('tooltip')).not.toBeInTheDocument();
  });

  it('positions tooltip left-aligned (default) from rect', () => {
    render(<DashboardHeader helpId="etf.mainFlow" width={340} />);
    const el = screen.getByLabelText(/主力净流入/);
    fireEvent.mouseEnter(el);
    const tooltip = screen.getByRole('tooltip');
    expect(tooltip.style.left).toBe('8px');
    expect(tooltip.style.width).toBe('340px');
  });

  it('positions tooltip right-aligned against the rect right edge', () => {
    const elRect = { left: 500, right: 700, bottom: 800, top: 790, width: 200, height: 10 } as DOMRect;
    vi.spyOn(Element.prototype, 'getBoundingClientRect').mockReturnValue(elRect);
    render(<DashboardHeader helpId="etf.mainFlow" align="right" width={340} />);
    const el = screen.getByLabelText(/主力净流入/);
    fireEvent.mouseEnter(el);
    const tooltip = screen.getByRole('tooltip');
    expect(tooltip.style.left).toBe('360px');
    vi.restoreAllMocks();
  });
});
