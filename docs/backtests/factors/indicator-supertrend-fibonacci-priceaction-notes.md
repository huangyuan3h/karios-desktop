# SuperTrend / Fibonacci / Price Action — 对 Karios 回测的指导意义（2026-08-27）

> 起因：TV 常用三件套是否值得进 `S-3/sleeve`。结论：**SuperTrend 可试作 MA200 替代（低优）、Fibonacci 不单测、Price Action 已覆盖**。

## 1. 是什么

### SuperTrend
- **定义**：`ATR(10)×3` 的自适应跟踪止损。`upper = (H+L)/2 + 3×ATR`，`lower = (H+L)/2 - 3×ATR`；收盘站上 `upper` 翻多、跌破 `lower` 翻空，线在 `K` 反侧形成尾随。TV 默认 `10,3`。
- **本质**：`MA + 波动率` 的趋势跟随，与 `Karios MA200 + 移动止损 -8%` 同族；抗噪靠 `ATR`，顺势期胜、震荡期来回打脸。

### Fibonacci（斐波那契回撤）
- **定义**：取一段 `高-低`，按 `0.236/0.382/0.5/0.618/0.786` 画回撤位，`0.618` 最受关注；自 `1930s` 心理位，属 **自证预言**。
- **本质**：对“回踩多深才止跌”打刻度；与 `MA20 ±2%` 的 `回踩` 测试同问法，只是换了把尺子。

### Price Action（PA，裸K）
- **定义**：不看指标、只看 `K 线形态`：`pin_bar/engulfing/inside_bar/box_break` 等；`uptrend_pullback / box / long_consolidation` 均属 PA。
- **本质**：把 `高开低收` 的形状编码为信号；`Karios` 已测 `pin(2026-08-24)` `bollinger/macd/kdj` `box支撑+1.4%` `长调整+3.5%` `回踩MA20+0.95%`，均属 PA 分支。

## 2. 对我们回测有没有用

| 指标 | 与现有系统的重叠 | 已测证据 | 是否进回测 | 怎么用（若做） |
|------|------------------|----------|------------|----------------|
| **SuperTrend** | 与 `MA200` + `trailing -8%` 同功能，区别是 `ATR` 自适应 | `MA200 sleeve 60/200 最优` 已实证 `趋势跟随` 有效；`S-3 trailing -8%` 已是 SuperTrend 思想 | **低优可试** | 作 `sleeve` 的 `MA200` 替代：`SuperTrend(10,3)` 过滤 `金/油/纳指`，与 `60/200` 跑 `多袖网格` 同口径对比 `OOS2/train/valid`；不改 `S-3` 入场 |
| **Fibonacci** | 与 `回踩MA20 ±2% +0.95% 60d` 同属“回调多深” | 回踩已证 `+0.08~0.95%` 弱 edge，换 `0.382/0.618` 难超 `1%` | **不单测** | 若非要，用作 `PA` 的刻度：`趋势 A(close>20>60)` + `回撤 0.382~0.618` 分层，看是否比 `±2%` 更优；预期仍 `<1%`，排在 `SuperTrend` 后 |
| **Price Action** | 已是 `S-3` 外形态层主战场 | `pin 38万1.6%→过滤后1.64%无edge` `box窄支撑+1.4% win53%` `长窄突破+3.5% win52%` `回踩+0.95% win48%` 均不达 `win70%` | **已覆盖，不扩** | 不新增 `engulfing/inside_bar` 大筛，维持 `箱体/长调整/回踩` 三件为 PA 代表；新增必过 `n>500 win>70%` 才升观察层 |

## 3. 为什么这样排

1. **SuperTrend 唯一可能替代**：`sleeve` 的 `MA200` 是 **跨资产择时**最强（`60/200` 胜 `120`），而 `MA` 定速、`SuperTrend` 变速——在 `油 38.9% vol` 上或许少打脸。值得 **一轮网格** 验证，但 `ETF 仅 4 票`，提升天花板约 `+1~2%`，不改 `100%择强` 结论。
2. **Fibonacci 是美学刻度**：`0.618` 无物理因果，实测多为 `±1%` 噪音；`TV` 上好看因 **事后选高低点**，前视位移后失效。与其画 `0.618`，不如守 `MA20` 的 `±2%`（已测）。
3. **PA 已到天花板**：`Karios` 的 `PA` 四连测已把 `CN 1500 2021-26` 的 `10/20/60d` 空间扫完，最强 `+3.5% win52%` 仍不杠杆；再加形态只降 `n` 不升 `win`。

## 4. 记录与下一步

- **不新增必填回测**：三者均不进 `S-3` 与 `sleeve` 主参数。
- **可选**：`SuperTrend(10,3) vs MA200` 一次 `sleeve_nav_sim` 对比（`OOS2/train/valid` 三窗），若 `excess>1%` 则提为 `SuperTrend sleeve` 备选；否则归档。
- **文档链**：`pin_bar_scan` `bollinger/macd/kdj` `support-resistance-box` `long-consolidation` `uptrend-pullback` 已构成 `PA` 证据链，本篇为 **三指标分流结论**。

---
*参考：SuperTrend `ATR10×3` TV 默认；Fib 0.618 源自黄金分割；PA 见 `docs/backtests/*.md` 四篇。*
