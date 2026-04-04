import { describe, expect, it, beforeEach, afterEach, vi } from 'vitest';
import { loadJson, saveJson } from './storage';

describe('storage', () => {
  const mockLocalStorage = {
    getItem: vi.fn(),
    setItem: vi.fn(),
    clear: vi.fn(),
  };

  beforeEach(() => {
    global.window = { localStorage: mockLocalStorage } as any;
    vi.clearAllMocks();
  });

  afterEach(() => {
    delete (global as any).window;
  });

  describe('loadJson', () => {
    it('returns fallback when window is undefined', () => {
      delete (global as any).window;
      const result = loadJson('test-key', { default: true });
      expect(result).toEqual({ default: true });
    });

    it('returns fallback when key does not exist', () => {
      mockLocalStorage.getItem.mockReturnValue(null);
      const result = loadJson('missing-key', 'fallback');
      expect(result).toBe('fallback');
    });

    it('returns parsed JSON when key exists', () => {
      mockLocalStorage.getItem.mockReturnValue(JSON.stringify({ data: 123 }));
      const result = loadJson('test-key', {});
      expect(result).toEqual({ data: 123 });
    });

    it('returns fallback on parse error', () => {
      mockLocalStorage.getItem.mockReturnValue('invalid-json');
      const result = loadJson('bad-key', { fallback: true });
      expect(result).toEqual({ fallback: true });
    });
  });

  describe('saveJson', () => {
    it('does nothing when window is undefined', () => {
      delete (global as any).window;
      saveJson('test-key', { data: 123 });
      expect(mockLocalStorage.setItem).not.toHaveBeenCalled();
    });

    it('saves JSON to localStorage', () => {
      saveJson('test-key', { value: 42 });
      expect(mockLocalStorage.setItem).toHaveBeenCalledWith(
        'test-key',
        JSON.stringify({ value: 42 }),
      );
    });

    it('handles serialization errors gracefully', () => {
      const circular: any = { a: 1 };
      circular.self = circular;
      saveJson('circular-key', circular);
      expect(mockLocalStorage.setItem).not.toHaveBeenCalled();
    });
  });
});
