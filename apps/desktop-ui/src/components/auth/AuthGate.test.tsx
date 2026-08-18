import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { AuthGate } from './AuthGate';
import { clearGatewayKey, getGatewayKey, UNAUTHORIZED_EVENT } from '@/lib/auth';

describe('AuthGate (Family Hub Phase 0)', () => {
  beforeEach(() => {
    clearGatewayKey();
  });
  afterEach(() => {
    clearGatewayKey();
    vi.unstubAllGlobals();
  });

  it('shows the login page when no key is stored', () => {
    render(
      <AuthGate>
        <div>app content</div>
      </AuthGate>,
    );
    expect(screen.getByText('Karios')).toBeTruthy();
    expect(screen.queryByText('app content')).toBeNull();
  });

  it('renders children when a key is stored', () => {
    localStorage.setItem('karios.gateway-key', 'sekrit');
    render(
      <AuthGate>
        <div>app content</div>
      </AuthGate>,
    );
    expect(screen.getByText('app content')).toBeTruthy();
  });

  it('stores the password and reloads on submit', () => {
    const reload = vi.fn();
    vi.stubGlobal('location', { reload });
    render(
      <AuthGate>
        <div>app content</div>
      </AuthGate>,
    );
    fireEvent.change(screen.getByPlaceholderText('访问密码'), { target: { value: 'sekrit' } });
    fireEvent.click(screen.getByText('进入'));
    expect(getGatewayKey()).toBe('sekrit');
    expect(reload).toHaveBeenCalledTimes(1);
  });

  it('returns to login on 401 event', async () => {
    localStorage.setItem('karios.gateway-key', 'stale');
    render(
      <AuthGate>
        <div>app content</div>
      </AuthGate>,
    );
    await waitFor(() => {
      expect(screen.getByText('app content')).toBeTruthy();
    });
    window.dispatchEvent(new CustomEvent(UNAUTHORIZED_EVENT));
    await waitFor(() => {
      expect(screen.getByPlaceholderText('访问密码')).toBeTruthy();
    });
    expect(screen.queryByText('app content')).toBeNull();
    expect(getGatewayKey()).toBeNull();
  });
});
