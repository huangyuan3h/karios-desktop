# Karios 回测实验记录（Backtest Experiments）

> **何时看**：任何新回测实验前（先读纪律 + 已走过的路）、改 S-3 参数前（先查历史结论）、
> 复盘策略演进时（成功/失败全记录）。
> **何时不看**：日常运维、代码开发（那些看 todo/optimization-checklist）。

---

## 本目录是什么

**所有 S-3 策略回测实验的唯一记录中心**——成功 ✅ 与失败 ❌ 都在。失败的记录
尤其重要：它说明我们**走过哪些路、为什么被拒收**，防止未来重复踩坑或凭感觉
重开已被三窗证伪的机制。

| 文档 | 内容 | 状态 |
|------|------|------|
| [`experiments-tip014.md`](./experiments-tip014.md) | **TIP-014 环境感知系列**（neutral_block / entry_style auto / E1 / E2 / HK 线 / 情绪回填） | ✅ 主链固化 · 详细 |
| [`experiments-d-pool.md`](./experiments-d-pool.md) | **探索池 D1-D8**（环境仓位 D3 ✅ / 持有期 D2 ✅ / score 确认 D4 ❌ / 行业集中 D5 ❌ / 利润护城河 D6 ❌ / 分钟线 D7 / 港股情绪 D8） | 🔄 滚动 |
| [`experiments-defensive.md`](./experiments-defensive.md) | **防守向攻击 23 项**（A1-A7 / B1-B4 / C1-C5 / D1-D4）——全部拒收/中性的完整论证 | ✅ 2026-08-12 完结 |
| [`experiments-legacy.md`](./experiments-legacy.md) | **历史实验速查**（V6/V7 系列、红绿灯、ATR 止损 OPT-105、熔断 OPT-093、长窗） | ✅ 快照 |
| [`experiments-planned.md`](./experiments-planned.md) | **信号候选计划 + 验证记录**（**已验证 14 项全拒收**：P1-P8 技术形态 + P9/P10 动量/高点 + P11 行业维度 + P12 + P16-ST + P17 组合层 · 待验证 10 项：P13/P14/P15/P18-P26） | 🔄 逐个验证 |
| [`README.md`](./README.md) | 本索引 + 验证纪律 + 报告文件位置 | — |

---

## 验证纪律（三窗铁律 · todo §19）

> 任何参数/机制改动必须过 **三窗 walk-forward**：单窗好看 = 过拟合，拒收。

1. **三窗切分（固定）**：
   - `OOS2` = 2024-08-01 ~ 2025-08-01（真实样本外，无情绪数据 → 环境机制 OOS2/train 不生效）
   - `train` = 2025-08-01 ~ 2026-02-01
   - `valid` = 2026-03-01 ~ 2026-08-07（当前实盘对照窗）
2. **判定标准**：**三窗 0 劣化 + 单窗收益提升**。单一窗好看 = 过拟合拒收。
   > 相对固化基线 >5pt 劣化 → 自动判"未通过/拒收"（run_walk_forward 内置）。
3. **长窗补充**（2021-08-01 ~ 2026-08-07）：跨周期稳健性检查，非三窗审计的一部分。
4. **验收工具**：
   ```bash
   cd services/data-sync-service
   PYTHONPATH=src python3 scripts/run_walk_forward.py            # 三窗 vs 固化基线
   PYTHONPATH=src python3 scripts/run_walk_forward.py --param k=v # 试参数
   PYTHONPATH=src python3 scripts/run_walk_forward.py --save-baseline  # 重固化
   ```
5. **纪律**：回测数字不作发布依据；**paper 实绩为准**（C4 对照）。

---

## 基线档案（data/backtest_reports/）

| 文件 | 内容 |
|------|------|
| `walk_forward_baseline.json` | **正式基线**（S-3 定案口径 · 当前 = D3 后重固化） |
| `walk_forward_latest.json` | 最近一次三窗结果 |
| `walk_forward_hk_baseline.json` | HK 并行线基线 |
| `walk_forward_dual_latest.json` | CN+HK 双线 |
| `monte_carlo_cn.json` / `monte_carlo_hk.json` | E2 panic 蒙特卡洛 3000 次验证 |
| `tip014_*.json` | TIP-014 各实验原始数据（d1/d3/d6/env_style/industry_profile/long_conf/neutral_diag） |
| `hk_trail_scan_*.json` | HK trailing 扫描 |
| `rolling_oos_latest.json` | 滚动 OOS 监控（scheduler 自动更新） |
| `paper_vs_backtest_latest.json` | C4 paper-vs-backtest 对照 |
| `index_light_backtest_latest.json` / `trend_exit_latest.json` | 红绿灯 / 趋势退出历史 |

## 当前基线数字（2026-08-15 · D3 固化后）

| 窗口 | 收益 | 回撤 | 夏普 | 胜率 | 笔数 |
|------|------|------|------|------|------|
| OOS2 | +117.2% | 11.7% | 6.63 | 54.4% | 237 |
| train | +122.6% | 9.3% | 4.40 | 52.0% | 123 |
| valid | +142.2% | 1.8% | 11.05 | 81.8% | 55 |
| 长窗 2021-08~2026-08 | +333.9% | 45.1% | 2.99 | 38.8% | 1069 |

> 参数真值 → [`modules/strategy-params.md`](../modules/strategy-params.md) §1；历史版本记录同文件 §3。
