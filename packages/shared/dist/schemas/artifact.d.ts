import { z } from 'zod';
export declare const ArtifactTypeSchema: z.ZodEnum<["url", "text", "file", "table", "note"]>;
export type ArtifactType = z.infer<typeof ArtifactTypeSchema>;
export declare const ArtifactSchema: z.ZodObject<{
    id: z.ZodString;
    type: z.ZodEnum<["url", "text", "file", "table", "note"]>;
    source: z.ZodOptional<z.ZodString>;
    createdAt: z.ZodString;
    tags: z.ZodDefault<z.ZodArray<z.ZodString, "many">>;
    confidence: z.ZodOptional<z.ZodNumber>;
    payload: z.ZodUnknown;
}, "strip", z.ZodTypeAny, {
    type: "url" | "text" | "file" | "table" | "note";
    id: string;
    createdAt: string;
    tags: string[];
    source?: string | undefined;
    confidence?: number | undefined;
    payload?: unknown;
}, {
    type: "url" | "text" | "file" | "table" | "note";
    id: string;
    createdAt: string;
    source?: string | undefined;
    tags?: string[] | undefined;
    confidence?: number | undefined;
    payload?: unknown;
}>;
export type Artifact = z.infer<typeof ArtifactSchema>;
