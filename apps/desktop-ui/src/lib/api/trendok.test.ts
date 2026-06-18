import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { resetTrendOkInflightForTests, fetchTrendOkMap } from './trendok';

vi.mock('./client', () => ({
  apiGetJson: vi.fn(),
}));

import { apiGetJson } from './client';

beforeEach(() => {
  resetTrendOkInflightForTests();
  vi.mocked(apiGetJson).mockReset();
});

afterEach(() => {
  resetTrendOkInflightForTests();
});

describe('fetchTrendOkMap', () => {
  it('dedupes concurrent requests with same symbols', async () => {
    vi.mocked(apiGetJson).mockImplementation(
      () =>
        new Promise((resolve) => {
          setTimeout(
            () => resolve([{ symbol: 'CN:600519', score: 90 }]),
            20,
          );
        }),
    );

    const [a, b] = await Promise.all([
      fetchTrendOkMap(['CN:600519'], { realtime: true }),
      fetchTrendOkMap(['CN:600519'], { realtime: true }),
    ]);

    expect(a.get('CN:600519')?.score).toBe(90);
    expect(b.get('CN:600519')?.score).toBe(90);
    expect(apiGetJson).toHaveBeenCalledTimes(1);
  });

  it('chunks symbols into batches of 200', async () => {
    const symbols = Array.from({ length: 250 }, (_, i) => `CN:${String(i).padStart(6, '0')}`);
    vi.mocked(apiGetJson).mockResolvedValue([]);

    await fetchTrendOkMap(symbols, { realtime: false });
    expect(apiGetJson).toHaveBeenCalledTimes(2);
  });

  it('normalizes map keys to uppercase', async () => {
    vi.mocked(apiGetJson).mockResolvedValue([{ symbol: 'cn:600519', score: 88 }]);
    const map = await fetchTrendOkMap(['CN:600519'], { realtime: false });
    expect(map.get('CN:600519')?.score).toBe(88);
  });
});
