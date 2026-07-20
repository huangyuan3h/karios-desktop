import { describe, expect, it } from 'vitest';

import { chunk } from './chunk';

describe('chunk', () => {
  it('returns empty array for empty input', () => {
    expect(chunk([], 10)).toEqual([]);
  });

  it('splits array into batches', () => {
    expect(chunk([1, 2, 3, 4, 5], 2)).toEqual([[1, 2], [3, 4], [5]]);
  });

  it('returns single batch when size exceeds length', () => {
    expect(chunk([1, 2], 10)).toEqual([[1, 2]]);
  });
});
