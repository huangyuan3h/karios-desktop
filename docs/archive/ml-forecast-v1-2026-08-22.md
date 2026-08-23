# ML N-day Forecast v1 — 2026-08-22

**Goal** N=20 X=8% time-series TCN predicting 20-day forward >8%.

**Data** PiT 8.26M samples (2021-10→2026-08) 14-dim L=60, split train 2025-08/2026-02 931k OOS2 1.82M valid 710k downsample 20k/10k/10k.

**Model** TCN hidden64 levels4 CPU lr3e-4 cosine early stop 12. Best AUC 0.588 @epoch11.

**Offline**

| | valid (2026-03-01→08-07) | OOS2 (2024-08→2025-08) |
|---|---|---|
| base pos_rate (>8% in 20d) | 0.179 | 0.297 |
| base avg 20d ret | -2.43% | +4.34% |
| AUC / AP / IC | 0.576 / 0.218 / 0.060 | 0.550 / 0.331 / 0.062 |
| prec@5% | 0.256 n454 avg -0.96% (+1.47) | 0.338 n405 avg 4.48% (+0.14) |
| prec thr0.6 | 0.244 n840 avg -0.73% (+1.70) | 0.340 n739 avg 5.42% (+1.08) |
| prec thr0.7 | 0.333 n24 avg +4.0% (+6.4) | 0.40 n40 avg - |

**Read** valid弱市 (-2.4%) top5% 仍 -0.96% 未转正，thr0.65才 +1.49%；OOS2牛市 lift +0.1~1.3%。IC 0.06 vs 原TIP-013 score IC -0.02 有进步，但绝对值仍弱。

**Next** 
- N=10/5 对比（短周期更贴合 S-3 hold5）
- 全量推理 710k → 回测 `pred>0.6 ∧ score65` 三窗 vs S-3 43.1% (>5pt 票决)
- LSTM对照、特征加入 RS/score、量价全量 120k训练
