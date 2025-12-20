import { z } from 'zod';
export declare const PortfolioSnapshotSchema: z.ZodObject<{
    asOf: z.ZodString;
    baseCurrency: z.ZodString;
    totalValue: z.ZodNumber;
    positions: z.ZodArray<z.ZodObject<{
        symbol: z.ZodString;
        quantity: z.ZodNumber;
        price: z.ZodOptional<z.ZodNumber>;
        currency: z.ZodOptional<z.ZodString>;
    }, "strip", z.ZodTypeAny, {
        symbol: string;
        quantity: number;
        price?: number | undefined;
        currency?: string | undefined;
    }, {
        symbol: string;
        quantity: number;
        price?: number | undefined;
        currency?: string | undefined;
    }>, "many">;
}, "strip", z.ZodTypeAny, {
    asOf: string;
    baseCurrency: string;
    totalValue: number;
    positions: {
        symbol: string;
        quantity: number;
        price?: number | undefined;
        currency?: string | undefined;
    }[];
}, {
    asOf: string;
    baseCurrency: string;
    totalValue: number;
    positions: {
        symbol: string;
        quantity: number;
        price?: number | undefined;
        currency?: string | undefined;
    }[];
}>;
export type PortfolioSnapshot = z.infer<typeof PortfolioSnapshotSchema>;
