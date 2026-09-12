# 价量公式因子库速筛（Alpha101 / GTJA191 / 跨市场） · 归档于 2026-09-12

## 当时的目标（todo 链接）
- `docs/todo.md` §P0-12 新策略孵化（"不在 S-3 上加 gate；孵化指纹不同的新策略"）。
- 用户追加：把 Alpha101 全集、GTJA191 全集跑一遍，给出"有效/无效"结论与 IC 数据，整理成一眼可查的台账。

## 实际做了什么
- **引擎**：`scripts/alpha101_screen.py`（L0 向量化 RankIC 引擎 + 三窗 + 五分位/换手/成本）→ `alpha101_candidates_diag.py`（S1 正交化）→ `alpha101_candidates_s2.py`（S2 可交易性）；`scripts/gtja191_screen.py`（GTJA191 面板化，168/191 条）；`scripts/hk_factor_screen.py`（HK 复用两套）。
- **Alpha101(1–101)**：L0 `0 PASS/11 CANDIDATE/90 REJECT`；候选全为**价量协方差反向**一族。S1：A16 残差三窗 ICIR `+0.79/+0.98/+0.65` → **SURVIVOR（独立于 size/流动性/反转/动量/波动/行业）**。S2（30bp、非重叠 h=5/10/20）：唯一三窗净为正的 `ls_h20` 净 IR 仅 `+0.01/+0.24/+0.69` → **`S2-REJECT`（成本杀死）**。方向终结。
- **GTJA191**：`0 PASS/16 CANDIDATE/152 REJECT`；候选 = 同一价量相关家族 + 量波动(070/097) + 中期反转(071/025)，净全负；与 Alpha101 逐数吻合（`139=A6 / 105=A3 / 099=A13`）；点名的"抗跌(075/182)/趋势显著性(021)/DMI(172/186)/缺口"新轴**全部 REJECT**。无新轴。
- **跨市场（HK）**：269 条 `0 PASS/15 CANDIDATE/252 REJECT`；同一价量家族复现但**更差**（HK 弱市年 IC≈0、成本 40bp 印花税双边）→ 结论**非 A 股特有**，但 A 股信号最强、HK 最不可交易。
- **台账**：新建 [`docs/backtests/factors/README.md`](../backtests/factors/README.md)——全部因子档一眼判定 + 家族速查。

## 验证 / 数据
- 结果档：`backtests/factors/{alpha101-l0-screen,gtja191-l0-screen,hk-alpha101-gtja191-l0}-2026-09-12.md`；预注册 `designs/{alpha101,gtja191,hk-factor}-screen-prereg-2026-09-12.md`。
- 报告：`data/backtest_reports/{alpha101_screen,gtja191_screen,hk_factor_screen_40bp}_latest.json`（`*_latest` 依 `.gitignore` 不入库，可重跑）。
- 引擎校验：GTJA139=A6、105=A3、099=A13 逐数吻合；ruff 全过；文档链接 0 broken。
- 同批完成（P0-12 其他腿）：X3 投资/应计独立化 **PARK**（+2.2%/年真实、满仓 beta、波动率层无效）；GARP **CLOSE**（=value，质量无增量）。

## 后续影响 / 留给谁
- **价量公式因子（Alpha101/GTJA191/跨市场）此部分关闭**：统计有微弱信号、成本后归零、无新独立轴；不再重开、不组合、不进 paper。
- 保留资产：4 个可复用因子速筛脚本（新因子集 L0/S0/S1/S2 一条龙）。
- 未闭合项：慢价值 V1 / 投资-应计 X3 仍 **INCUBATE**（smart-beta，非引擎）；US/加拿大**未测**（仓库无数据）；这些属 P0-12 后续，不在本部分。
