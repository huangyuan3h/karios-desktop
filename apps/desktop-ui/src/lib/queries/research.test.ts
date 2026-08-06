import { describe, expect, it } from 'vitest';

import { researchReportsQueryKey, researchStatsQueryKey } from './research';

describe('research query keys', () => {
  it('distinguishes reports vs stats', () => {
    expect(researchReportsQueryKey(7, 50)).toEqual(['research', 'reports', 7, 50]);
    expect(researchStatsQueryKey()).toEqual(['research', 'stats']);
  });
});
