# 择强单轨（Pick-Strong Track）

> **产品策略真值**（2026-08-29 起）。此后回测优化、文档结论、回测页 Timeline **只服务本策略**。  
> S-3 / 套筒 / R5c 等是**子组件或历史分层**，不是并列的「主策略」。

---

## 0. 一句话

**所有市场、所有资产同一池：在约定条件下，谁预期收益更高就 100% 持有谁**（最差落到逆回购）。

候选池（同权，无「股票永远优先」）：

| 资产 | 来源 | 强度代理（现行） |
|------|------|------------------|
| **STOCK** | S-3 CN + HK 当日持仓篮（等权日收益 / 均 mom60） | 有仓则入池，强度 = 持仓股 `mom60` 均值（篮本身不施 MA 闸） |
| **GOLD** | `518880.SH` | `mom60` 且收盘 ≥ MA200 |
| **OIL** | `513350.SH` | 同上 |
| **NASDAQ** | `513100` / `513110` | 同上 |
| **BOND10** | `511260.SH` | 同上 |
| **REPO** | GC001 | 无人过线时的兜底 |

规则（定案口径 · `mom_compare` · **2026-08-29 加固 + trail8 吸收**）：

1. 用 **t-1** 收盘算各资产 `mom60`；ETF 须站上 `MA200`（防前视）。  
2. 候选 = {STOCK（若有仓）} ∪ {站上 MA 的 ETF}，取 **`argmax mom60`**，**100%** 硬切；空 → **REPO**。  
3. LOOKBACK=**60** · MA=**200** · **min_hold=1** · **ETF 峰值 −8%（trail8）→ REPO** · 不计强制成本（5–10bp 压力下仍稳）。  
4. 拒收：短/长 lookback、risk-adj、Top2、Nasdaq-first、袖侧 hold5 外推 —— 见 [`backtests/pick-strong-hardening-2026-08-29.md`](../backtests/pick-strong-hardening-2026-08-29.md)。  
5. trail8 绝对 NAV 证据：[`backtests/pick-strong-trail8-and-stock-pool-2026-08-29.md`](../backtests/pick-strong-trail8-and-stock-pool-2026-08-29.md)（valid +82pt / long +75pt；OOS2 持平）。

> **Live / Watchlist**：与定案同规则（`multi_asset_sleeve` + `pick_strong_track`）。  
> STOCK 入池闸（n / mom>0 / 自 MA）仍为实验中，默认「有仓即入池」。

> **机会双子星 v2（2026-09-01）**：可选增强——卫星资金平时跟核心，开闸可买或持仓中才切 50%；**退出日 `satActive` 计入成本**。修正后 past_year/aligned 输给纯核心 → **实盘默认仍是本单轨**；双子星 Settings opt-in（`backtests/state-bucket-algo-2026-08-31.md` §3.0/§7）。

> **不是**「套筒」：套筒只是闲置现金的 ETF 增强。  
> **不是**「纯 S-3」：S-3 只负责生成 STOCK 候选/持仓；最终仓位由择强单轨决定。

---

## 1. 与旧概念的关系

```text
择强单轨（唯一优化目标）
├── STOCK 腿 ← S-3 CN/HK 引擎（gate/score/RS/退出…）提供持仓篮
├── 多资产腿 ← 原「多资产袖 / 套筒」的 mom60+MA200 择强
└── REPO 兜底
```

| 旧名 | 地位 |
|------|------|
| S-3 | **股票腿生成器**（参数冻结，§19） |
| 第三资产套筒 / 多资产袖 | **择强单轨的 ETF 子集规则**（已并入同池） |
| R5c / R5CS | 历史分层资金路由；优化期以单轨 100% 为准 |
| 融合单轨 | 旧设计稿名 → **本文件「择强单轨」** |

---

## 2. 过去一年验证（2026-08-29 实跑 · trail8 吸收后更新）

窗口：`2025-08-28 ~ 2026-08-28`（约 253 个交易日）  
脚本：`scripts/pick_strong_grid.py --batch E` · 报告：`data/backtest_reports/pick_strong_trail8_20260829.json`

| 口径 | 收益 | 最大回撤 | 说明 |
|------|------|----------|------|
| **择强单轨 `mom_compare`+trail8（定案）** | **+190.7%** | **12.6%** | ETF 峰值 −8%→REPO；三窗/长窗见 trail8 文档 |
| 对照：无 trail（旧 A0） | +93.6% | 28.3% | 仅硬切；已降级为对照 |
| 对照：`hard_stock`（有股票仓则锁 STOCK） | +110.8% | 32.0% | 旧 Timeline 偏置；**不作定案** |
| 对照：CN S-3 引擎单独 | +58.3% | 23.0% | 仅股票腿，现金≤100% NAV |

**结论**：定案吸收 trail8 后 past_year / valid / long 同向大幅改善，OOS2 持平。STOCK 入池加闸（n≥2 / mom>0 / MA / CN-only）已拒收 —— 见同日 STOCK 池报告。

---

## 3. 优化纪律（只动择强单轨）

1. **冻结**：S-3 定案参数（`strategy-params.md` §1）——除非审计级 bug。  
2. **可调**：择强打分（lookback / MA / 波动调整 / 是否纳入 HK 篮权重）、切换成本、最少持有、REPO 规则。  
3. **验收**：三窗 walk-forward（OOS2/train/valid）+ holdout 只读；`>5pt` 劣化拒收；`n<100` 标 underpowered。  
4. **工具**：  
   - 网格 / 三窗：`PYTHONPATH=src:scripts python3 scripts/pick_strong_grid.py --batch all`  
   - 单窗核对：`PYTHONPATH=src:scripts python3 scripts/fused_timeline_walk.py --windows past_year --mode mom_compare`  
   - **UI / API**：`GET /api/backtest/timeline` · `mode=mom_compare`（与定案同权；缓存键含 mode，旧 hard_stock 缓存自动失效）  
   - Watchlist live pick：`multi_asset_sleeve.build_multi_asset_sleeve` 同 `mom_compare`（含 STOCK 篮）  
5. **实验记录**：一律写回 `docs/backtests/`；最近加固定案见 `pick-strong-hardening-2026-08-29.md`。

---

## 4. 代码入口

| 用途 | 路径 |
|------|------|
| 定案回放 | `scripts/fused_timeline_walk.py` (`mode=mom_compare`) |
| 多资产 `_pick`（纳指优先变体，袖用） | `service/multi_asset_sleeve.py` |
| Timeline API（待与定案对齐） | `api/backtest_routes.py` `GET /api/backtest/timeline` |
| 过去一年报告 | `data/backtest_reports/pick_strong_track_past_year.json` |

---

*创建 2026-08-29 · 状态：定案命名 + 过去一年已验证 · Timeline UI 对齐 `mom_compare` 为后续工程。*
