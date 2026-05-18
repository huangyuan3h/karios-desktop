import { describe, expect, it } from 'vitest';
import { ArtifactSchema, ArtifactTypeSchema } from './artifact';

describe('ArtifactTypeSchema', () => {
  it('accepts valid artifact types', () => {
    expect(ArtifactTypeSchema.parse('url')).toBe('url');
    expect(ArtifactTypeSchema.parse('text')).toBe('text');
    expect(ArtifactTypeSchema.parse('file')).toBe('file');
    expect(ArtifactTypeSchema.parse('table')).toBe('table');
    expect(ArtifactTypeSchema.parse('note')).toBe('note');
  });

  it('rejects invalid artifact types', () => {
    expect(() => ArtifactTypeSchema.parse('invalid')).toThrow();
  });
});

describe('ArtifactSchema', () => {
  it('validates a complete artifact', () => {
    const artifact = {
      id: 'test-id',
      type: 'url',
      source: 'test-source',
      createdAt: '2024-01-01T00:00:00Z',
      tags: ['tag1', 'tag2'],
      confidence: 0.8,
      payload: { url: 'https://example.com' },
    };
    const result = ArtifactSchema.parse(artifact);
    expect(result.id).toBe('test-id');
    expect(result.type).toBe('url');
    expect(result.confidence).toBe(0.8);
  });

  it('validates artifact without optional fields', () => {
    const artifact = {
      id: 'test-id',
      type: 'text',
      createdAt: '2024-01-01T00:00:00Z',
      payload: 'some text',
    };
    const result = ArtifactSchema.parse(artifact);
    expect(result.tags).toEqual([]);
    expect(result.source).toBeUndefined();
  });

  it('rejects artifact with invalid confidence', () => {
    const artifact = {
      id: 'test-id',
      type: 'url',
      createdAt: '2024-01-01T00:00:00Z',
      confidence: 1.5,
      payload: null,
    };
    expect(() => ArtifactSchema.parse(artifact)).toThrow();
  });
});
