# 预注册：港湾停车切 H2 验证（H-HARBOR-H2 · 2026-09-16）

> **状态**：用户授权执行（"其他都换，多窗验证"），跑前冻结。
> **边界**：本轮只做验证实验，不翻转任何 Live。PASS 也不自动切 Live
> （港湾 Live 翻转另需 paper + 用户授权）。母港/星港继承港湾结果，
> 本轮不单独跑（它们的停车腿就是港湾停车）。

## 1. 机制（一句话）

S-3 核心冻结，闲置停车换 H2 迟滞套筒（换仓需 2pt mom60 领先；与星舰采用的
同一机器 `service/parking_sleeve.py`，同一口径）。预期：Shedding 噪音换仓税，
在 S-3 高占用的窗口影响小（idle 权重低），在空仓窗口（弱市/valid）影响大。

## 2. 冻结裁决（B11 模式，Live 血统从严）

记 P1_H2 vs P1（冻结港湾基线）Δ，均含成本：

- **K1**：三窗每窗 Δtot ≥ **−1.0**（Live 钱，容忍带收紧；B11 当年是 −0.05，
  但那是"闲置从 0 到有"的纯增量，本次是"有到优"的替换，±1pt 噪音带诚实些）。
- **K2**：三窗均值 Δ > 0 **且** long Δ > 0。
- **K3**：long MDD ≥ P1 −1pt 且 valid MDD ≥ P1 −2pt（风险不劣化）。
- 成本：同机制两边付同样的转移摩擦，不另起 cost 臂（同成本逻辑，见 H-SLEEVE-TUNE §3）。
- PASS → 港湾-H2 候选（进 Live-track：paper + 授权）；任一挂 → REJECT，
  港湾维持 canonical，全产品线不动。
- 死因预判：2024 类急涨轮动年（H2 套筒当年跑输 canonical +1.6 vs +8.1），
  若 S-3 在 OOS2/train 高占用，blend 稀释后可能贴线。

## 3. L 门 tick

L1 宇宙冻结 5 ETF ✓ · L2 无新成交语义（同 prev 收盘决策/当日收益时点；
H2 机只增"跳过换仓"分支）✓ · L3 同一份 eval px（fund-adj CSV 口径，
与冻结 P1 逐数可比）✓ · L5 独立报告文件（不覆盖 B11 冻结 JSON）· L6/L7 惯例。

## 4. 复现

```bash
cd services/data-sync-service
PYTHONPATH=src:scripts python3 scripts/eval_etf_parking_baseline.py --save-report
cp data/backtest_reports/etf_parking_baseline_2026-09-13.json \
   data/backtest_reports/harbor_h2_parking_2026-09-16.json
```
（P1_H2 臂见脚本内 `H-HARBOR-H2` 注释；冻结 verdict 块不动，另起 H2 verdict 行。）
