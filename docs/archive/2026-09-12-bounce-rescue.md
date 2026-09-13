# P0-13 B4 超跌×国家队净买 → 短期反弹（H-BOUNCE） · 归档于 2026-09-12

## 当时的目标（todo 链接）
- `docs/todo.md` → **P0-13 P1① B4 超跌反弹 / 国家队救市**。用户想法：超跌 → 国家队进场救 → 之后几天卖出，能吃到反弹。
- 定位：已固化 `national_team_gate`（国家队**缺席**防守闸）的**镜像**（在场→进攻）。

## 实际做了什么
- 只读诊断 `scripts/diag_bounce_rescue.py`（预注册冻结 `docs/designs/bounce-rescue-prereg-2026-09-12.md`）：沪深300 < MA200（超跌区）且 4 宽基 ETF（510300/510500/510510/159915）20 日份额净增 > 0（国家队在买）→ T+1 开盘买 000300、T+1+N 收盘卖（N=1/3/5/10，主 5）。
- 数据全用库内：`index_daily`（000300 OHLC）+ `cn_etf_share`（2018+）。first-principles 自查：`§一.14` 本地信息**不受限**（例外边界），可开。

## 验证 / 数据
- **REJECT（按冻结线）**：K1 ✅（超跌A ret5 long +0.27，OOS2/valid 正）、覆盖 ✅558；**K2 ❌**——现代"只要破 MA200 国家队必在买"（OOS2 超跌未买 **0 天**、train 整窗无超跌日），A/B 几乎不共存 → **隔离不出国家队增量**；valid 反号 −1.95。
- **生命体征**：深度超跌（ret20≤−5%）& 国家队买 均值+中位双正、随 N 单调（5d **+1.07%** / 10d **+1.91%**）；但落在**已判死的均值回归家族**（`experiments-planned P7/P8`），起手事件（onset，17 次）近抛硬币。
- **核心结论**：**效应来自"超跌"交互，不是国家队本身**（全样"国家队买 vs 不买"几乎无差，N5 +0.18 vs +0.20）。**"国家队救市→反弹"这个特定机制不成立**；真正正的是"**深度超跌本身→反抽**"。

## 后续影响 / 留给谁
- **B4 关闭**；`national_team_gate` 的"进攻镜像"假设证伪（gate 能活靠 dormant 保险+长窗协议，作进攻择时不成立）。
- **复活条件（写死）**：只能走**深度超跌 & 国家队买**的长窗压力测试（仿 gate 协议、只认熊段），且须证明**独立于普通均值回归**；否则按"均值回归换皮"处理，**不重开**。
- 无新数据资产、无新 OPT/TIP、不改 Live/schema。

## 关联档
- 实验：`docs/backtests/event/bounce-rescue-2026-09-12.md`
- 预注册：`docs/designs/bounce-rescue-prereg-2026-09-12.md`
- 设计稿：`docs/designs/oversold-nationalteam-bounce-2026-09-12.md`
- 脚本：`scripts/diag_bounce_rescue.py` · 报告 `data/backtest_reports/bounce_rescue_2026-09-12.json`
