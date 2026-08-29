# 回测总结（指向 **择强单轨** · 2026-08-29）

> **何时看**：任何人想「再回测 / 再优化」之前——先读本页 + [`modules/pick-strong-track.md`](../modules/pick-strong-track.md)。
> **一句话**：**产品策略 = 择强单轨**（全市场全资产同权，条件内谁强持谁）。  
> S-3 / 套筒 / 信号池实验是子组件与拒收档案，**不再作为并列终局结论**。

---

## 0. 现行终局：择强单轨

| 项 | 内容 |
|----|------|
| 定义 | 股票篮(S-3 CN+HK) + 金/油/纳/债 + REPO，t-1 `mom60` 且站上 `MA200` 的池内 argmax，100% 硬切 |
| 真值文档 | [`modules/pick-strong-track.md`](../modules/pick-strong-track.md) |
| 过去一年（定案 `mom_compare`） | **+93.6% / DD28.3%**（2025-08-28~2026-08-28） |
| 三窗绝对 NAV（加固基线 A0） | OOS2 **+17.8** / train **+35.7** / valid **+56.5**（dd 18/12/28） |
| 报告 | `pick_strong_track_past_year.json` · `pick_strong_grid_20260828.json` |
| 参数定案 | LB60·MA200·hold1·100% mom（[加固实验](pick-strong-hardening-2026-08-29.md) **维持 A0**；hold5/短 LB/risk-adj/Top2 拒收） |
| 优化范围 | 择强打分已扫一轮；**S-3 冻结**；下一刀优先 Timeline 对齐 |

对照（非定案）：STOCK 优先 +110.8%；UI Timeline 旧口径 +123.9%；纯 CN S-3 引擎 +58.3%。

---

## 1. 历史回测阶段（组件沉淀 · 已结束信号探索）

自 2026-08-09 起的全部回测实验（success + failure 全记录）：

| 轮次 | 数量 | 结果 | 对择强单轨的意义 |
|------|------|------|------------------|
| 防守向攻击 A/B/C/D | 23 项 | 20 拒收 / 3 中性 / 0 采纳 | 股票腿过滤器已够严 |
| 信号池 P1-P26 | 15 项 | **15 全部拒收** | 不再加技术形态信号 |
| 早期实验 | ~10 项 | 全部拒收 | — |
| **固化进 STOCK 腿** | 环境感知 / 仓位 / 数据修复 | 见 strategy-params | 提供择强用的股票篮 |
| **固化进多资产腿** | mom60+MA200 / MIN_HOLD5 | multi_asset_sleeve | 择强 ETF 侧规则 |

**48+ 次失败的共同模式**（仍有效，勿重开）：
1. 绝对量技术形态 → 无增量
2. 防守收紧 → 截断右尾
3. 与 RS 共线 / 闸门重合 → 零增量
4. 单窗好看 = 过拟合

## 2. A 股 S-3（STOCK 腿 · 非终局产品）

> S-3 alpha = RS 转强 + 主线行业 + 环境感知 + 纪律空仓 —— **作为择强单轨的股票候选引擎保留**。

现行 NAV 基线：OOS2 **+47.3%** / train **+34.1%** / valid **+38.7%** n⚠️（`s3-baseline-20260828-nav`）。  
旧 117% / 333% 已封存。

## 3. HK 线（STOCK 腿的一部分）

NAV 重固化：OOS2 **+31.3%** / train **+1.9%** / valid **+60.7%** —— train 弱，**不作独立高置信叙事**；并入择强股票篮即可。

## 4. 下一步（只服务择强单轨）

| 方向 | 状态 |
|------|------|
| Timeline / live 与 `mom_compare` 定案对齐 | **[done] 2026-08-29** API+导出+Watchlist 文案/live pick |
| 择强 LB/MA/hold/cost/risk-adj/Top2 网格 | **[done] 2026-08-29** 维持 A0 |
| C4 paper 对照 | 进行中 |
| 新 S-3 信号 / 扫参 | **冻结** |

## 5. 文档地图

- **策略真值** → `modules/pick-strong-track.md`
- 参数（股票腿）→ `modules/strategy-params.md`
- 实验全记录 → `experiments-*.md`
- 审计 → `audit-verdict-2026-08-29.md`
- 旧「融合单轨」设计 → `designs/fused-single-track-optimization.md`（已加择强单轨指针）
