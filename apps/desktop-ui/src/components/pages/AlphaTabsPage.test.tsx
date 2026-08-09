import { fireEvent, render, screen } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';

const { useChatStore } = vi.hoisted(() => ({ useChatStore: vi.fn() }));

vi.mock('@/lib/chat/store', () => ({ useChatStore }));

vi.mock('./AlphaIncubatorPage', () => ({
  AlphaIncubatorPage: () => <div data-testid="incubator-page">Incubator content</div>,
}));

vi.mock('./ResearchPage', () => ({
  ResearchPage: () => <div data-testid="research-page">Research content</div>,
}));

import { AlphaTabsPage } from './AlphaTabsPage';

describe('AlphaTabsPage', () => {
  it('renders Alpha Incubator by default and keeps Research mounted', () => {
    useChatStore.mockReturnValue({ addReference: vi.fn() });
    render(<AlphaTabsPage />);
    expect(screen.getByTestId('incubator-page')).toBeInTheDocument();
    expect(screen.getByTestId('research-page')).toBeInTheDocument();
    expect(screen.getByTestId('research-page').parentElement).toHaveClass('hidden');
  });

  it('switches to Research tab on click and back', () => {
    useChatStore.mockReturnValue({ addReference: vi.fn() });
    render(<AlphaTabsPage />);
    fireEvent.click(screen.getByText('Research · 研报 α'));
    expect(screen.getByTestId('research-page').parentElement).not.toHaveClass('hidden');
    expect(screen.getByTestId('incubator-page').parentElement).toHaveClass('hidden');
    fireEvent.click(screen.getByText('Alpha Incubator'));
    expect(screen.getByTestId('incubator-page').parentElement).not.toHaveClass('hidden');
  });

  it('marks the active tab with aria-pressed', () => {
    useChatStore.mockReturnValue({ addReference: vi.fn() });
    render(<AlphaTabsPage />);
    expect(screen.getByText('Alpha Incubator')).toHaveAttribute('aria-pressed', 'true');
    expect(screen.getByText('Research · 研报 α')).toHaveAttribute('aria-pressed', 'false');
  });
});
