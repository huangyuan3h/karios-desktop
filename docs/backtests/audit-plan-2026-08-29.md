# 组合回测可信度审计计划（2026-08-29）

> **目标**：在优化任何策略之前，先证明「文档里的结论」能被代码复现、数字不被乐观假设抬高、
> 单策略与组合口径一致。本轮**只审计与复现**，不扫新信号、不调参。
>
> **整体逻辑（用户口径）**：
> 1. **S-3 主线** — gate / score / RS / 环境感知确认「上升期」再买；**CN 与 HK 独立**。
> 2. **第三资产套筒** — S-3 闲置资金轮动黄金 / 原油 / 纳指等（低相关），形成组合。
>
> **质疑点（本轮必须正面回答）**：
> - 代码漏洞 / 过于乐观的执行假设
> - 数据不真实、基线不准
> - 策略 1 单独、策略 2 单独、**1+2 结合**是否都站得住
>
> **前置文档**：[`SUMMARY.md`](./SUMMARY.md) · [`audit-2026-08-22.md`](./audit-2026-08-22.md) ·
> [`archive/2026-08-22-audit-phase0-realism.md`](../archive/2026-08-22-audit-phase0-realism.md) ·
> [`modules/strategy-params.md`](../modules/strategy-params.md) ·
> [`designs/third-asset-sleeve.md`](../designs/third-asset-sleeve.md) ·
> [`gold-oil-nasdaq-balance.md`](./gold-oil-nasdaq-balance.md)

---

## 0. 成功标准（本轮结束时必须交付）

| # | 交付物 | 通过条件 |
|---|--------|----------|
| S1 | **复现矩阵**（本文 §4 逐条打勾） | 文档数字 vs 当前脚本输出：偏差 ≤ 容差（见 §4）或标「不可复现 + 根因」 |
| S2 | **漏洞台账**（延续 audit-2026-08-22 D/E 编号） | 每项：已修 / 仍在 / 新发现；仍在项写清对收益的方向与量级 |
| S3 | **三层可信结论** | ① S-3 CN ② S-3 HK ③ 套筒/多资产 ④ CN+HK+套筒联合 — 各给「可引用 / 仅发现 / 拒信」 |
| S4 | **优化准入清单** | 仅 S3 通过后才允许动策略；列出可动旋钮与禁区 |

**硬纪律**：本轮不改 `strategy-params` 定案参数；不 `--save-baseline` 覆盖正式基线；
`holdout 2026-08-08~` 只读。发现 bug 可修代码，修后重跑复现矩阵并记入台账。

---

## 1. 审计范围（三层）

```text
┌─────────────────────────────────────────────────────────┐
│  Layer C · 联合组合                                      │
│  run_walk_forward_dual (R5 / R5CS) + portfolio_nav_sim  │
│  allocation 闲置 → 套筒；CN/HK 双弱 / 单强 / 双强         │
└───────────────────────┬─────────────────────────────────┘
                        │ 闲置现金
┌───────────────────────┴─────────────────────────────────┐
│  Layer B · 第三资产                                      │
│  third_asset_sleeve / multi_asset_sleeve                │
│  sleeve_nav_sim · commodity_pattern_scan (观察层)         │
└───────────────────────┬─────────────────────────────────┘
                        │ 空仓日 / idle%
┌───────────────────────┴─────────────────────────────────┐
│  Layer A · S-3 主线                                      │
│  CN: gates=full + env 套件 · HK: gates=regime            │
│  run_walk_forward.py · backtest_engine.simulate         │
└─────────────────────────────────────────────────────────┘
```

| 层 | 验什么 | 主入口脚本 / 代码 |
|----|--------|-------------------|
| **A1 CN** | realism 三窗 + 过去一年 vs 文档 | `scripts/run_walk_forward.py` · `walk_forward_baseline.json` |
| **A2 HK** | HK 基线（统一 qfq 口径） | `run_walk_forward.py --market HK` · `walk_forward_hk_baseline.json` |
| **B1 套筒 NAV** | 闲置吃 513100/GC001（或多资产）三窗增量 | `scripts/sleeve_nav_sim.py` · `GET /api/backtest/sleeve-nav` |
| **B2 多资产轮动** | mom60+MA200 金/油/纳 三窗 | `multi_asset_sleeve.py` · `gold-oil-nasdaq-balance.md` 引用数 |
| **C1 联合** | R5 / R5CS 双市场+套筒 | `scripts/run_walk_forward_dual.py` |
| **C2 口径缝** | paper 常量 ↔ BacktestConfig ↔ 文档表 | `paper_s3.py` · `strategy-params.md` §1 |

回测页（`BacktestPage`）只作**展示对照**：Walk-forward 卡 / SleeveNavCard / 基线表；审计以脚本输出为准，UI 数字不一致记为展示 bug。

---

## 2. 已知风险登记（从质疑出发 · 优先验）

### 2.1 代码 / 过于乐观（执行层）

| ID | 问题 | 来源 | 本轮动作 |
|----|------|------|----------|
| E-cash | 曾 10%×20=200% 名义 | audit E1；Phase0 称已 `sum≤1.0` | **复现确认**：当前 `simulate` 是否仍可超 100%；基线 JSON 是否 realism |
| E-liq | 无流动性 / min_avg_amount | audit E2；称 0.7 亿已开 | 确认默认 cfg + walk_forward 注入一致 |
| E-cal | 日历日 vs 交易日 | audit E3；称已修 | 抽 `max_hold` / D2=45 实际交易日数 |
| E-entry | `entry_mode=close` 偏乐观 | audit E4；曾拒 next_open | 跑一次 next_open 对照，披露偏差，不改默认 |
| E-pyr | 金字塔绕 max_positions / 涨停 | audit E6 | 读代码路径 + 小窗用例 |
| E-metric | Sharpe per-close-day 虚高 | audit E7 | 输出旁注 `approx`；引用用 Calmar / 收益+DD |
| E-sleeve | 套筒切换 T+0 / 溢价未建模 | third-asset-sleeve §0 | 核对 `same_day_cut` vs 次日；QDII 溢价是否披露 |

### 2.2 数据不真实 / 基线不准

| ID | 问题 | 来源 | 本轮动作 |
|----|------|------|----------|
| D-score | OOS2/train 分数多为今日配方回填 | audit D3 | 抽样对比「回填分 vs 当时 snapshot」；披露合成区间 |
| D-ind | 行业表无时态 | audit D2 | 披露 mild；gates=full 贡献是否被抬高 |
| D-surv | 长窗幸存者 | audit D4 | 长窗标 `survivor-conditioned`；不引用为实盘期望 |
| D-flow | flow fail-open → OOS2≈regime | audit D6 | 统计 OOS2 有/无 flow 天数占比 |
| D-qfq | CN/HK 复权混源（历史已修） | strategy-params §4 | 抽除权日跳空抽检脚本是否仍绿 |
| D-base | 多份基线并存（117% vs 43%） | SUMMARY / params | **统一引用表**：只允许 realism SHA256 作现行 |

### 2.3 策略结合缝（1 / 2 / 1+2）

| ID | 问题 | 本轮动作 |
|----|------|----------|
| C-idle | 套筒增量依赖「S-3 闲置%」——S-3 改口径后 idle 变，套筒数字漂移 | 用**同一 realism S-3 轨迹**重跑 sleeve_nav；对比设计稿旧数 |
| C-double | CN+HK+套筒是否双重计闲置 / 资金池冲突 | 读 `allocation.py` + R5CS；画资金流一日例 |
| C-corr | 纳指套筒与「上升期买股」同涨同跌尾部 | 联合窗 maxDD vs 单 S-3；披露全球崩盘情景 |
| C-paper | 提示层 vs 自动配置 vs 回测是否同状态机 | `third_asset_sleeve` vs `sleeve_paper_auto` vs `portfolio_nav_sim` 三路对照 |
| C-hk | HK 无套筒 / 双市场权重 R1 等权是否仍成立 | dual 三窗 vs strategy-params §6 表 |

---

## 3. 分阶段计划（建议顺序）

### Phase A — 冻结现场（0.5 天）

1. 记录当前 git HEAD、`walk_forward_baseline.json` SHA256、`walk_forward_hk_baseline.json` SHA256。
2. 导出「文档承诺数字」清单（§4）到本文件附录或 `audit-repro-matrix-YYYYMMDD.md`。
3. 确认工作区无未提交的引擎改动混入复现（有则先 stash / 单独分支）。

### Phase B — Layer A 复现与代码抽检（1–2 天）

1. 跑 CN realism 三窗 + holdout + 过去一年；填复现矩阵。
2. 跑 HK 三窗；统一文档残留旧数（86.9 vs 270）。
3. 对照 `audit-2026-08-22` D/E：grep/`read` 关键路径，更新台账状态（已修是否真的还在）。
4. **压力**：关掉 env 5 件套（neutral_block / auto / D2 / D3 / panic=3）只留 2026-08-09 核芯，看 OOS2/train 是否仍显著为正 → 回答「核心 alpha 是否依赖 valid 偷看」。

### Phase C — Layer B 套筒 / 多资产（1 天）

1. `sleeve_nav_sim.py` 三窗 vs `designs/third-asset-sleeve.md` / 回测页 SleeveNavCard。
2. `multi_asset_sleeve` 固化规则三窗 vs todo / gold-oil 文档。
3. 脉冲观察层（R4/T1b 等）：只验证「分层表可复现」，**不**升格为策略。

### Phase D — Layer C 联合与缝（1–2 天）

1. `run_walk_forward_dual.py` R5 / R5CS 三窗复现。
2. 资金流一日审计（双弱 / 仅 CN / 仅 HK / 双强 × 套筒开/关）。
3. paper 路径：抽样对比「若当日按回测信号」与 paper 闸门是否同码。

### Phase E — 结论与优化准入（0.5 天）

1. 写 `audit-verdict-2026-08-XX.md`（或回写本文件 §5）：三层可信评级 + 不可复现项根因。
2. 输出**优化准入清单**（仅修复类 / 口径对齐类优先；信号类继续冻结）。
3. 用户拍板后再进「优化策略」迭代（另开实验记录，不混进本计划）。

**合计**：约 **4–6 个专注工作日**；可并行 B 与 C 的脚本跑数，代码抽检串行。

---

## 4. 复现矩阵（文档承诺 → 命令 → 容差）

> 跑完在「实得」列填数；**偏差 > 容差** → 标 ❌ 并开漏洞单。  
> 容差默认：收益 ±2pt（同一口径）；笔数 ±5%；夏普仅作参考不票决。

| # | 文档来源 | 承诺数字（摘要） | 复现命令 | 容差 | 实得 | 判定 |
|---|----------|------------------|----------|------|------|------|
| A1 | strategy-params §3 realism | CN OOS2 **43.1** / train **35.6** / valid **43.3** n⚠️ · sha `40ef4cd0…` | `PYTHONPATH=src python3 scripts/run_walk_forward.py --windows OOS2,train,valid,holdout` | ±2pt | | ☐ |
| A2 | strategy-params §1 过去一年 | **66.6%** / DD8.4 / 83 笔 | 同引擎 past_year 脚本或 walk_forward 等价窗 | ±2pt | | ☐ |
| A3 | strategy-params §1b HK | 统一 **qfq 后** OOS2/train/valid（270.2/26.9/60.6 或现行文件） | `… run_walk_forward.py --market HK` | ±2pt | | ☐ |
| A4 | SUMMARY 旧 D3（封存） | 117.2/122.6/142.2 | 仅对照封存 JSON，**禁止当现行** | 读文件一致 | | ☐ |
| B1 | third-asset-sleeve §0b | 套筒三窗增量 **+2.8 / +23.1 / +30.4** | `PYTHONPATH=src python3 scripts/sleeve_nav_sim.py` | ±3pt | | ☐ |
| B2 | todo / multi_asset | mom60 套筒 **+19.3/+17.9/+14.4** · past_year +38.1 | 对应 sleeve 脚本 | ±3pt | | ☐ |
| C1 | third-asset-sleeve OPT-121 | R5CS **+10.8/+17.0/+30.9** | `PYTHONPATH=src python3 scripts/run_walk_forward_dual.py`（R5CS） | ±3pt | | ☐ |
| C2 | strategy-params §6 | R1 等权 dual 表 | dual 脚本 R1 | ±5pt（旧口径需标注） | | ☐ |
| G1 | gold-oil T1b | 油 RSI>80 金-油 win **82.9%** n35 | `commodity_pattern_scan.py` 同窗 | win ±3pt / n±2 | | ☐ |

每条 ❌ 必须写：根因（代码 / 数据 / 文档过期 / 口径漂移）+ 是否影响「策略方向结论」。

---

## 5. 结论模板（Phase E 填写 · 先空着）

### 5.1 可信度评级

| 对象 | 评级 | 一句话 |
|------|------|--------|
| S-3 CN 核芯（score+RS+gates，无 env） | | |
| S-3 CN 现行（含 env 5 件套） | | |
| S-3 HK | | |
| 第三资产套筒（513100 路径） | | |
| 多资产金/油/纳轮动 | | |
| CN+HK+套筒联合 | | |

### 5.2 仍站得住的结论 / 必须降级的结论

- 站得住：
- 降级为「仅发现」：
- 拒信 / 文档作废：

### 5.3 优化准入（通过审计后）

| 优先级 | 动作 | 前提 |
|--------|------|------|
| P0 | 修仍在抬收益的洞 + 文档/基线对齐 | 复现矩阵闭环 |
| P1 | paper↔回测同口径对照（C4） | ≥20 笔或明确不足 |
| P2 | 组合层资金规则澄清（非新信号） | Layer C 缝关闭 |
| 冻结 | 新信号 / TrendOK 扫参 / 调 env | holdout n≥100 前 |

---

## 6. 工作方式

1. **一阶段一结论**：每结束 Phase B/C/D 在本文件或子文件追加「实跑日志」段落（命令、commit、关键输出路径）。
2. **证据链**：结论必须带 `file:line` 或报告 JSON 路径（沿用 audit-2026-08-22 风格）。
3. **不扩 scope**：金油脉冲观察层只做复现，不借机改 `multi_asset_sleeve` 规则。
4. **用户拍板点**：Phase E 评级表 + 优化准入 — 未拍板不进优化实现。

---

## 7. 文档地图（本轮新增/更新）

| 文件 | 角色 |
|------|------|
| **本文件** `audit-plan-2026-08-29.md` | 计划 + 复现矩阵骨架 |
| `audit-repro-matrix-*.md`（可选） | 跑数填表专用，避免本文件过长 |
| `audit-verdict-*.md`（Phase E） | 最终可信结论 |
| `audit-2026-08-22.md` | 上一轮代码审计（本轮对照更新台账，不覆盖） |
| `backtests/README.md` | 索引链到本计划 |

---

*创建：2026-08-29 · 状态：已执行完毕 → 见 [`audit-verdict-2026-08-29.md`](./audit-verdict-2026-08-29.md)。*
