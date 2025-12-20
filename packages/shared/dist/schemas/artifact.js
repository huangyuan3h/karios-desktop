import { z } from 'zod';
export const ArtifactTypeSchema = z.enum(['url', 'text', 'file', 'table', 'note']);
export const ArtifactSchema = z.object({
    id: z.string(),
    type: ArtifactTypeSchema,
    source: z.string().optional(),
    createdAt: z.string(),
    tags: z.array(z.string()).default([]),
    confidence: z.number().min(0).max(1).optional(),
    payload: z.unknown(),
});
