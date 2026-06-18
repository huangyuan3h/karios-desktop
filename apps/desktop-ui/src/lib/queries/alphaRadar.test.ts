import { describe, expect, it } from 'vitest';

import {
  alphaRadarCatalystQueryKey,
  alphaRadarQueryKey,
  alphaRadarStatusQueryKey,
  alphaRadarTrendsQueryKey,
} from './alphaRadar';

describe('alphaRadar query keys', () => {
  it('uses stable alpha radar keys', () => {
    expect(alphaRadarQueryKey()).toEqual(['alphaRadar']);
    expect(alphaRadarStatusQueryKey()).toEqual(['alphaRadar', 'status']);
    expect(alphaRadarTrendsQueryKey('batch')).toEqual(['alphaRadar', 'trends', 'batch']);
    expect(alphaRadarCatalystQueryKey(7)).toEqual(['alphaRadar', 'catalyst', 7]);
  });
});
