import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import {
  MobileCard,
  MobileSection,
  StatusPill,
  GateBadge,
  PriceText,
  PctText,
  MobileRow,
  MobileButton,
  MobileSheet,
  MobileField,
  SkeletonBlock,
  EmptyState,
} from './primitives';

describe('mobile primitives', () => {
  it('MobileCard renders children and is tappable', () => {
    const onClick = vi.fn();
    render(<MobileCard onClick={onClick}>hello</MobileCard>);
    fireEvent.click(screen.getByText('hello'));
    expect(onClick).toHaveBeenCalledOnce();
  });

  it('MobileSection shows title and action', () => {
    render(<MobileSection title="持仓" action={<span>全部</span>}>body</MobileSection>);
    expect(screen.getByText('持仓')).toBeTruthy();
    expect(screen.getByText('全部')).toBeTruthy();
    expect(screen.getByText('body')).toBeTruthy();
  });

  it('StatusPill maps tone to inline color', () => {
    const { container } = render(<StatusPill tone="up">涨</StatusPill>);
    expect(container.firstChild).toHaveStyle({ color: 'var(--k-up)' });
  });

  it('GateBadge shows open/closed', () => {
    const { rerender } = render(<GateBadge market="A股" open />);
    expect(screen.getByText('A股')).toBeTruthy();
    rerender(<GateBadge market="港股" open={false} />);
    expect(screen.getByText('港股')).toBeTruthy();
  });

  it('PriceText colors by sign with arrow', () => {
    const { rerender } = render(<PriceText value={1.2} />);
    expect(screen.getByText(/▲/)).toBeTruthy();
    rerender(<PriceText value={-3.4} />);
    expect(screen.getByText(/▼/)).toBeTruthy();
    rerender(<PriceText value={0} />);
    expect(screen.queryByText(/[▲▼]/)).toBeNull();
  });

  it('PctText shows abs percent', () => {
    render(<PctText value={-2.5} />);
    expect(screen.getByText(/2\.50%/)).toBeTruthy();
  });

  it('MobileRow is tappable and renders trailing', () => {
    const onClick = vi.fn();
    render(<MobileRow leading="代码" trailing={<span>右侧</span>} onClick={onClick} />);
    fireEvent.click(screen.getByText('代码'));
    expect(onClick).toHaveBeenCalledOnce();
    expect(screen.getByText('右侧')).toBeTruthy();
  });

  it('MobileButton variants render and block spreads', () => {
    const { container } = render(
      <MobileButton variant="ghost" block>
        添加
      </MobileButton>,
    );
    const btn = container.querySelector('button');
    expect(btn?.className).toContain('w-full');
    expect(btn?.className).toContain('border');
  });

  it('MobileSheet open/close interaction', () => {
    const onClose = vi.fn();
    render(
      <MobileSheet open title="详情" onClose={onClose}>
        <span>内容</span>
      </MobileSheet>,
    );
    expect(screen.getByText('内容')).toBeTruthy();
    fireEvent.click(screen.getByText('关闭'));
    expect(onClose).toHaveBeenCalledOnce();
  });

  it('MobileField wraps label + control', () => {
    render(
      <MobileField label="搜索">
        <input />
      </MobileField>,
    );
    expect(screen.getByText('搜索')).toBeTruthy();
  });

  it('SkeletonBlock renders shimmer element', () => {
    const { container } = render(<SkeletonBlock h={20} w={100} />);
    expect(container.firstChild).toHaveClass('m-shimmer');
  });

  it('EmptyState shows title and hint', () => {
    render(<EmptyState icon="✅" title="无偏差" hint="与回测一致" />);
    expect(screen.getByText('无偏差')).toBeTruthy();
    expect(screen.getByText('与回测一致')).toBeTruthy();
  });
});
