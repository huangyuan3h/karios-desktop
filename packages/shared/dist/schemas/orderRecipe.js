import { z } from 'zod';
export const OrderSideSchema = z.enum(['buy', 'sell']);
export const OrderRecipeSchema = z.object({
    id: z.string(),
    symbol: z.string(),
    side: OrderSideSchema,
    quantity: z.number().positive().optional(),
    price: z.number().positive().optional(),
    currency: z.string().optional(),
    notes: z.string().optional(),
    createdAt: z.string(),
});
