# 因子库 · 风格 / 市况 / 配置 / 指数 beta

> 覆盖风格×市值×市况（C1）、regime 配置（R1）、指数趋势延续、定海 DH（指数 + 波动率目标）。
> 原始长档：[`../backtests/factors/`](../backtests/factors/) · [`../backtests/SUMMARY.md`](../backtests/SUMMARY.md)
> **总基调**：**择时/配置类几乎全灭**；唯一"活着"的是**被动指数 beta + 波动率目标**（定海 DH-2，正年化）。

---

## C1 风格×市值×市况
- **定义**：行业关键词归 6 桶 + 市值三层 × up/choppy/down/跌后震荡。
- **数据**：`stock_eastmoney_industry`（当前快照）+ `stock_dailybasic.total_mv`（2021+），2021–2026。
- **结果**：up 科技/资源领涨、down 金融最抗跌、跌后震荡=医药/科技高 beta 反弹；大盘 5 年 **+243% vs 中盘 +211% vs 小盘 +34%**；用表做轮动 v0 = 打平等权（行业映射幸存者偏差）。
- **判定**：🧱 描述性砖（不作规则）。源：[`style-regime`](../backtests/factors/style-regime-2026-09-11.md)。

## R1 regime 配置腿
- **定义**：趋势腿 T（mom60 十分位）× 慢价值腿 V（价值复合十分位），按 000300 vs MA200 切换。
- **数据/方法**：月度 2021–2026，信号月末 as-of，OPT-156 补洞后。
- **结果**：corr(T,V)=0.391✓；但 R=OFF spread **+1.66%/月**、R=ON **+0.15%/月**——**两态同为正，regime 不翻转 V−T**（只放大）；valid 反号；T 两态全亏。
- **判定**：❌ REJECT / 方向证伪。源：[`regime-allocation-prereg`](../designs/regime-allocation-incubator-prereg-2026-09-11.md) §6。

## 指数趋势延续
- **定义**：000300/000688 MA20/60 趋势态 + 次日开盘回放。
- **结果**：bull 态远期四切全弱于无条件（edge −0.8~−8.8pp）；趋势腿三窗全败且**全输买持**；MA120 三窗零增量。
- **判定**：❌ REJECT / 方向证伪。源：[`index-trend-bigmoney`](../backtests/index/index-trend-bigmoney-2026-09-08.md)。

## 定海 DH
- **DH-1（已证伪）**：top-N 流动性选池 + 波动率目标。**前视**（本月末成交额选本月票）~+20pt/yr；
  修正为上月选池后 top200 **−8.5%/yr**。滚动率目标在无前视数据上**不提升 Sharpe**。→ ❌ WITHDRAWN。
- **DH-2（KEEP，对照腿）**：**真实市值加权指数 + 波动率目标**（指数回补 2005+，无选池无前视、可交易、正年化）。
  中证500 buy&hold **+7.3%/yr**；300/500+vt20% **+4.6%/yr、DD−46%、sr0.33**；沪深300+vt10% +3.4%/yr。
- **与 S-3 结合**：叠刹车/配比/风险平价/闲置资金/趋势门**全 REJECT**（四窗不一致；S-3 自带风控更强）。
  **DH 作方向指引（指数趋势→收紧止损）** 也 REJECT（MA200 不触发=冗余；MA60/20 OOS2 劣化=砍右尾）。
- **判定**：⚠️ DH-1 WITHDRAWN / DH-2 KEEP（对照）。源：[`a1-voltarget-beta`](../backtests/a1-voltarget-beta-2026-09-11.md) · [`dh1-s3-hybrid`](../backtests/dh1-s3-hybrid-2026-09-11.md)。
