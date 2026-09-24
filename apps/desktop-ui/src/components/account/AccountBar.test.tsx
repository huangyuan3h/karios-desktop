import { fireEvent, render, screen } from '@testing-library/react';
import { afterEach, describe, expect, it } from 'vitest';

import { getAccountCapital, setAccountCapital } from '@/lib/account-settings';

import { AccountBar } from './AccountBar';

afterEach(() => {
  setAccountCapital(null);
});

describe('AccountBar', () => {
  it('shows 未设置 and saves the total capital on blur', () => {
    render(<AccountBar />);
    expect(screen.getByText('账户')).toBeDefined();
    expect(screen.getByText('未设置')).toBeDefined();
    const input = screen.getByLabelText('总资金');
    fireEvent.change(input, { target: { value: '1000000' } });
    expect(screen.getByText('未保存')).toBeDefined();
    fireEvent.blur(input);
    expect(getAccountCapital()).toBe(1_000_000);
    expect(screen.getByText('¥1,000,000')).toBeDefined();
  });

  it('shows an already-saved capital', () => {
    setAccountCapital(500000);
    render(<AccountBar />);
    expect(screen.getByText('¥500,000')).toBeDefined();
    expect(screen.queryByText('未设置')).toBeNull();
  });
});
