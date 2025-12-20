import { z } from 'zod';
export declare const OrderSideSchema: z.ZodEnum<["buy", "sell"]>;
export type OrderSide = z.infer<typeof OrderSideSchema>;
export declare const OrderRecipeSchema: z.ZodObject<{
    id: z.ZodString;
    symbol: z.ZodString;
    side: z.ZodEnum<["buy", "sell"]>;
    quantity: z.ZodOptional<z.ZodNumber>;
    price: z.ZodOptional<z.ZodNumber>;
    currency: z.ZodOptional<z.ZodString>;
    notes: z.ZodOptional<z.ZodString>;
    createdAt: z.ZodString;
}, "strip", z.ZodTypeAny, {
    symbol: string;
    id: string;
    createdAt: string;
    side: "buy" | "sell";
    quantity?: number | undefined;
    price?: number | undefined;
    currency?: string | undefined;
    notes?: string | undefined;
}, {
    symbol: string;
    id: string;
    createdAt: string;
    side: "buy" | "sell";
    quantity?: number | undefined;
    price?: number | undefined;
    currency?: string | undefined;
    notes?: string | undefined;
}>;
export type OrderRecipe = z.infer<typeof OrderRecipeSchema>;
