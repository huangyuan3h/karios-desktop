import { describe, expect, it } from 'vitest';

import { decisionMessagesQueryKey, decisionSessionsQueryKey } from './decision';

describe('decision query keys', () => {
  it('distinguishes sessions vs per-session messages', () => {
    expect(decisionSessionsQueryKey()).toEqual(['decision', 'sessions']);
    expect(decisionMessagesQueryKey(7)).toEqual(['decision', 'messages', 7]);
  });
});
