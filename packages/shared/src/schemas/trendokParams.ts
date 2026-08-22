import { z } from 'zod';

export const TrendOKParamsSchema = z.object({
  w_ema: z.number().min(0).max(1).default(0.4),
  w_macd: z.number().min(0).max(1).default(0.2),
  w_break: z.number().min(0).max(1).default(0.1),
  w_rsi: z.number().min(0).max(1).default(0.1),
  w_vol: z.number().min(0).max(1).default(0.2),
  failed_score_cap: z.number().min(0).max(100).default(79),
  macro_lock_down_threshold: z.number().int().min(0).default(3500),
  low_volume_ratio_threshold: z.number().min(0).max(5).default(1.2),
  low_volume_ratio_score_cap: z.number().min(0).max(100).default(79),
  bonus_ema20_slope_5d: z.number().min(0).max(20).default(5),
  intraday_surge_threshold_pct: z.number().min(0).max(20).default(6),
  intraday_surge_penalty: z.number().min(0).max(100).default(20),
  atr_ratio_threshold: z.number().min(0).max(0.2).default(0.05),
  atr_penalty_scale: z.number().min(0).max(10000).default(1000),
  volume_climax_mult: z.number().min(0).max(10).default(3),
  volume_climax_penalty: z.number().min(0).max(100).default(15),
  below_ema20_penalty: z.number().min(0).max(100).default(30),
  vol_break_1: z.number().min(0).max(5).default(1.0),
  vol_break_2: z.number().min(0).max(5).default(1.2),
  vol_break_3: z.number().min(0).max(5).default(2.0),
  vol_break_4: z.number().min(0).max(5).default(3.0),
  flow_5d_top3: z.number().min(-50).max(50).default(10),
  flow_5d_bottom5: z.number().min(-50).max(50).default(-20),
  flow_today_top3: z.number().min(-50).max(50).default(5),
  flow_today_top4_5: z.number().min(-50).max(50).default(3),
  flow_falloff_big_outflow: z.number().min(-50).max(50).default(-15),
  flow_absent_2d_big_outflow: z.number().min(-50).max(50).default(-10),
  flow_large_outflow: z.number().default(-1e8),
  alpha_vol_mult: z.number().min(0).max(10).default(2.5),
  alpha_score_floor: z.number().min(0).max(100).default(60),
});
export type TrendOKParams = z.infer<typeof TrendOKParamsSchema>;

export const DEFAULT_TRENDOK_PARAMS: TrendOKParams = TrendOKParamsSchema.parse({});
