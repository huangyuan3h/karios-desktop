# Karios 回测实验记录（Backtest Experiments）

> **何时看**：任何新回测实验前、复盘策略演进时。
> **终局策略**：**[择强单轨](../modules/pick-strong-track.md)** —— 全资产同权，条件内谁强持谁。  
> 本目录记录通往该策略的实验（含拒收）；**新结论必须写清对择强单轨的增量**。

---

## 本目录是什么

| 文档 | 内容 | 状态 |
|------|------|------|
| [`SUMMARY.md`](./SUMMARY.md) | **总览（指向择强单轨）** | ✅ **必读入口** |
| [`../modules/pick-strong-track.md`](../modules/pick-strong-track.md) | **择强单轨策略真值 + 过去一年验证** | ✅ **产品真值** |
| [`audit-plan-2026-08-29.md`](./audit-plan-2026-08-29.md) | 组合可信度审计计划 | ✅ |
| [`audit-verdict-2026-08-29.md`](./audit-verdict-2026-08-29.md) | 审计结论（P0 已修） | ✅ |
| [`pick-strong-hardening-2026-08-29.md`](./pick-strong-hardening-2026-08-29.md) | 择强参数加固网格 · **维持 A0** | ✅ |
| [`audit-2026-08-22.md`](./audit-2026-08-22.md) | 代码层审计（数据/执行/统计） | ✅ |
| [`experiments-tip014.md`](./experiments-tip014.md) | TIP-014 环境感知（STOCK 腿） | ✅ |
| [`experiments-d-pool.md`](./experiments-d-pool.md) | 探索池 D1-D8 | ✅ |
| [`experiments-defensive.md`](./experiments-defensive.md) | 防守向攻击 23 项 | ✅ |
| [`experiments-legacy.md`](./experiments-legacy.md) | 历史实验速查 | ✅ |
| [`experiments-planned.md`](./experiments-planned.md) | 信号池 P1-P26（全拒收） | ✅ |
| [`README.md`](./README.md) | 本索引 | — |

---

## 验证纪律（三窗铁律 · todo §19 · 2026-08-22 审计后）

> 任何参数/机制改动必须过 **三窗 walk-forward + hold-out**：单窗好看 = 过拟合，拒收。

1. **三窗切分（固定）**：
    - `OOS2` = 2024-08-01 ~ 2025-08-01（弱市年 · 资金流 fail-open → 实为 regime 窗）
    - `train` = 2025-08-01 ~ 2026-02-01
    - `valid` = 2026-03-01 ~ 2026-08-07（当前实盘对照窗 · n=55 已复用 4 次，见审计 §3）
2. **hold-out（新增 2026-08-22）**：`2026-08-08 ~ 2027-02-08` 只读不调参，`n≥100` 前不改参；三窗 `>5pt劣化` 判定不含 hold-out，hold-out 只作确认
3. **判定标准**：**三窗 0 劣化 + 单窗收益提升** 且 `hold-out` 不跌。单一窗好看 = 过拟合拒收。
    > 相对固化基线 >5pt 劣化 → 自动判"未通过/拒收"（run_walk_forward 内置）；`n<100` 标 `⚠️ underpowered`。
4. **长窗补充**（2021-08-01 ~ 2026-08-07）：跨周期价格路径检查，非三窗审计；`2021-08~2024-07` 无 sentiment/flow/scores 全 `fail-open`，与 valid 非同分布，拆 `long_price_only` vs `long_full(2024-08~)`。
5. **验收工具**:
    ```bash
    cd services/data-sync-service
    PYTHONPATH=src python3 scripts/run_walk_forward.py            # 三窗 vs 固化基线
    PYTHONPATH=src python3 scripts/run_walk_forward.py --param k=v # 试参数
    PYTHONPATH=src python3 scripts/run_walk_forward.py --windows OOS2,train,valid,holdout,long  # 含 hold-out
    PYTHONPATH=src python3 scripts/run_walk_forward.py --save-baseline  # 需新文件名 + git tag
    ```
6. **纪律**：回测数字不作发布依据；**paper 实绩为准**（C4 对照）；`valid n=55 win81.8% Sharpe11` 仅发现、不可外宣，可信锚点为 `OOS2 n237 / train n123`；`B-T1 TrendOK` 当前 `--param trendok_*` 为 no-op（见审计 §3.3，需修 `recompute_scores_with_params` 注入）。
7. **审计**：改引擎/加参/引用收益前必读 [`audit-2026-08-22.md`](./audit-2026-08-22.md)（数据前视/幸存者 · 执行 200%杠杆/无流动性/calendar 天数 · 统计 129 组合多重检验）。

---

## 基线档案（data/backtest_reports/）

| 文件 | 内容 |
|------|------|
| `pick_strong_track_past_year.json` | **择强单轨**过去一年（定案 mom_compare） |
| `walk_forward_baseline.json` | S-3 股票腿 CN 基线（NAV） |
| `walk_forward_latest.json` | 最近一次三窗结果 |
| `walk_forward_hk_baseline.json` | HK 并行线基线 |
| `walk_forward_dual_latest.json` | CN+HK 双线 |
| `monte_carlo_cn.json` / `monte_carlo_hk.json` | E2 panic 蒙特卡洛 3000 次验证 |
| `tip014_*.json` | TIP-014 各实验原始数据（d1/d3/d6/env_style/industry_profile/long_conf/neutral_diag） |
| `hk_trail_scan_*.json` | HK trailing 扫描 |
| `rolling_oos_latest.json` | 滚动 OOS 监控（scheduler 自动更新） |
| `paper_vs_backtest_latest.json` | C4 paper-vs-backtest 对照 |
| `index_light_backtest_latest.json` / `trend_exit_latest.json` | 红绿灯 / 趋势退出历史 |

## 当前基线数字（2026-08-28/29 · NAV + next_open · 审计 P0 后）

| 窗口 | 收益 | 回撤 | 夏普 | 胜率 | 笔数 | 可信度 |
|------|------|------|------|------|------|--------|
| 择强单轨 past_year | **+93.6%** | **28.3%** | — | — | — | ✅ **产品口径** |
| CN OOS2 | +47.3% | 18.9% | 1.26 | 47.3% | 93 | 股票腿 |
| CN train | +34.1% | 11.6% | 2.22 | 45.1% | 51 | ✅ 可引用 |
| CN valid | +38.7% | 10.7% | 2.40 | 75.0% | 16 | ⚠️ underpowered |
| HK OOS2 | +31.3% | 30.3% | 0.99 | 43.4% | 99 | ⚠ 弱 |
| HK train | +1.9% | 12.8% | 0.28 | 44.7% | 47 | ⚠ 极弱 |
| HK valid | +60.7% | 27.8% | 2.10 | 43.2% | 44 | ⚠ 发现 |
| 套筒增量 | +3.1 / +8.4 / +22.3 | — | — | — | — | ✅ 基线=引擎 NAV |
| R5CS vs R5C | +3.3 / +8.4 / +13.5 | — | — | — | — | ✅ dual 已复现 |

> 参数真值 → [`modules/strategy-params.md`](../modules/strategy-params.md)；审计 → [`audit-verdict-2026-08-29.md`](./audit-verdict-2026-08-29.md)。旧 117%/HK270%/算术43.1% **全部封存**。
