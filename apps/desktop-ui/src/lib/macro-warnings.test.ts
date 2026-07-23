import { describe, expect, it } from 'vitest';

import { formatMacroWarning, formatMacroWarningPart } from './macro-warnings';

describe('formatMacroWarning', () => {
  it('maps put IV soft fallback code', () => {
    expect(formatMacroWarningPart('put_iv_live_fetch_failed_using_db')).toContain('last DB value');
  });

  it('maps stale offshore codes', () => {
    expect(formatMacroWarning('macro_data_stale: IXIC,SPX')).toBe(
      'Offshore indices may be stale (IXIC,SPX)',
    );
  });

  it('joins multiple parts', () => {
    expect(
      formatMacroWarning('put_iv_fetch_failed; macro_data_stale: HSI'),
    ).toBe('510300 Put IV unavailable · Offshore indices may be stale (HSI)');
  });

  it('passes through already-human messages', () => {
    expect(formatMacroWarning('TU_SHARE_API_KEY is not set; hello')).toBe(
      'TU_SHARE_API_KEY is not set · hello',
    );
  });
});
