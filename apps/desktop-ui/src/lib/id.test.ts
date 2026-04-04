import { describe, expect, it } from 'vitest';
import { newId } from './id';

describe('newId', () => {
  it('generates a unique id', () => {
    const id1 = newId();
    const id2 = newId();
    expect(id1).toBeDefined();
    expect(id2).toBeDefined();
    expect(id1).not.toBe(id2);
  });

  it('generates a string id', () => {
    const id = newId();
    expect(typeof id).toBe('string');
    expect(id.length).toBeGreaterThan(0);
  });

  it('generates uuid format when crypto is available', () => {
    const id = newId();
    if (typeof crypto !== 'undefined' && 'randomUUID' in crypto) {
      expect(id).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/);
    }
  });
});
