# 因子库 · 形态（Morphology）

> 覆盖箱体/突破/回调/耗尽等形态识别。原始长档：[`../backtests/factors/`](../backtests/factors/)
> **总基调**：形态多为 **WEAK**（厚尾、win~52%、未达 win>70%/n>50 门槛）；唯一曾"高置信"的
> 勺型耗尽在**正确限价成交语义**下证伪。**形态一律不进 S-3；只作方向判别层**。

---

## 强股勺型耗尽
- **定义**：上升趋势中圆滑回踩后的**放量破位** → 看空/耗尽顶（`ml_forecast.morphology.strong_scoop_exhaustion`）。
- **初版结果**：`ret60>0.40 & 放量` 命中 89.4% / R +13.55（做空回放）。
- **⚠️ 2026-09-05 证伪**：在**正确限价成交语义**下 strict 89.4%/+13.55 → **65.3%/+0.92%**，最差单笔 **−236%**；
  冻结验证疑似用了 naive bar-touch（82% setup 的"止损"本在入场价下方，按 high 碰即记盈利）。
- **判定**：❌ REJECT（成交语义证伪）；**S-3 联动与空袖指引暂停**；`factor_signals` 港股误标待修。
- 源：[`strategy-params.md` §7](../modules/strategy-params.md) · [`scoop-exhaustion-oos-check`](../backtests/factors/scoop-exhaustion-oos-check-2026-09-04.md) · [`hedge-twin-2026-09-05`](../backtests/hedge/hedge-twin-2026-09-05.md)。

## 长调整+突破
- **定义**：长而窄的箱体（n=60/120，宽度 <20 分位）后向上突破。
- **数据/方法**：CN 流动 1500，2022-01~2026-08，forward 10/20/60d，n≥500；基准 CN 10d +0.76% win47%。
- **结果**：60d 窄箱突破 60d **+6.94%**（+3.53% 超额）win51.9%；20d +4.10%（+2.31%）；最强 CN 形态超额，但**厚尾**（中位 0.43–0.73%）、win~52%、10d 弱。
- **判定**：🟡 WEAK（观察层，不入 S-3/sleeve）。源：[`long-consolidation-breakout-study`](../backtests/factors/long-consolidation-breakout-study.md)。

## 上升趋势回调
- **定义**：上升趋势（close>MA20>MA60）中回踩 MA20±2% 的入场。
- **数据/方法**：CN 流动 1500，2021-08~2026-08，4 趋势 × 8 回调 × 3 horizon = 96 网格。
- **结果**：仅 A+P1 60d **+2.01%**（+0.95% 超额）win48%；其余 ±0.2% 噪音；`between20_60`（跌破 MA20-MA60 之间）**−0.57%**（接刀）。
- **判定**：🟡 WEAK（只在 MA20 触及时买、不更深）。源：[`uptrend-pullback-study`](../backtests/factors/uptrend-pullback-study.md)。

## 箱体支撑/阻力
- **定义**：前箱体突破后回踩箱顶=支撑；前箱顶=阻力。
- **数据/方法**：CN 流动 1500，2023-01~2026-08，箱体 n=20/60，突破后 20d 内回踩，forward 10d。
- **结果**：窄箱支撑 60d **+2.21%**（+1.45% 超额）win53.1%；**阻力证伪**（resist_any 仍 +0.87~1.16%）；厚尾中位 0.2–0.6%。
- **判定**：🟡 WEAK（支撑弱 edge / 阻力证伪）。源：[`support-resistance-box-study`](../backtests/factors/support-resistance-box-study.md)。

---

## 形态库方向（备忘）
- 逐个形态用**限价成交语义**独立验证；≥80% 置信度才考虑作方向判别层；**不回写 S-3**。
- 后续形态候选：箱体/双底/杯柄/旗形/三角/突破——均须独立小回放 + 成交语义 + 多窗。
