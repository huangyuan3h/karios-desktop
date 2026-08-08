import { fireEvent, render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';

import { WatchlistInsightsPanel } from './WatchlistInsightsPanel';

describe('WatchlistInsightsPanel', () => {
  it('hides children by default', () => {
    render(
      <WatchlistInsightsPanel>
        <div data-testid="child">期望值看板</div>
      </WatchlistInsightsPanel>,
    );
    expect(screen.getByText('诊断面板')).toBeInTheDocument();
    expect(screen.getByTestId('child').parentElement).toHaveClass('hidden');
  });

  it('shows children after toggling the switch', () => {
    render(
      <WatchlistInsightsPanel>
        <div data-testid="child">Funnel History</div>
      </WatchlistInsightsPanel>,
    );
    fireEvent.click(screen.getByLabelText('Toggle insights panel'));
    expect(screen.getByTestId('child').parentElement).not.toHaveClass('hidden');
    expect(screen.getByText('Funnel History')).toBeVisible();
  });

  it('keeps children mounted across toggles', () => {
    const { container } = render(
      <WatchlistInsightsPanel>
        <div data-testid="child">Import debug table</div>
      </WatchlistInsightsPanel>,
    );
    expect(container.querySelector('[data-testid="child"]')).not.toBeNull();
    fireEvent.click(screen.getByLabelText('Toggle insights panel'));
    fireEvent.click(screen.getByLabelText('Toggle insights panel'));
    expect(container.querySelector('[data-testid="child"]')).not.toBeNull();
  });
});
