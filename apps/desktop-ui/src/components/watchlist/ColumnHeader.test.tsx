import { renderToString } from 'react-dom/server';
import { describe, expect, it, vi } from 'vitest';

import { ColumnHeader } from './ColumnHeader';

describe('ColumnHeader', () => {
  it('renders a <span> with role=button when showTooltip is wired (so it can be safely nested inside another <button>)', () => {
    const html = renderToString(
      <button type="button" aria-label="outer">
        <ColumnHeader
          columnId="score"
          showTooltip={vi.fn()}
          hideTooltip={vi.fn()}
          width={340}
        />
      </button>,
    );
    expect(html).not.toMatch(/<button[^>]*>[^<]*<button/i);
    expect(html).toMatch(/<span[^>]*role="button"/);
  });

  it('renders a plain <span> (no role=button) when showTooltip is not passed', () => {
    const html = renderToString(<ColumnHeader columnId="score" />);
    expect(html).toContain('评分');
    expect(html).toContain('Score');
    expect(html).not.toMatch(/<button/);
  });

  it('uses whitespace-nowrap on label and sub so bilingual headers stay single-line', () => {
    const html = renderToString(
      <ColumnHeader
        columnId="score"
        showTooltip={vi.fn()}
        hideTooltip={vi.fn()}
        width={340}
      />,
    );
    const nowrapCount = (html.match(/whitespace-nowrap/g) ?? []).length;
    expect(nowrapCount).toBeGreaterThanOrEqual(2);
  });

  it('renders unknown columnId as raw fallback text', () => {
    const html = renderToString(<ColumnHeader columnId="does_not_exist" />);
    expect(html).toMatch(/does_not_exist/);
  });
});
