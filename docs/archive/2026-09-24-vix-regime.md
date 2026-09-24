# B21 VIX 风险 regime 砖（H-VIX-REGIME） · 归档于 2026-09-24

## 当时的目标（todo 链接）

- `docs/todo.md` P0-13 B21（用户 2026-09-24 提出）：VIX / 利率预期能不能当"砖块"，组合进配置层。
- 前置：把 VIX 接进每日宏观同步（`macro_daily` series `VIX`，此前仅一次性回填、陈旧）。

## 实际做了什么

- **数据保鲜**：`service/macro_daily.py` 新增 `sync_vix()`（yfinance `^VIX`，增量 upsert，fail-open），并入 `sync_macro_daily_full`（美股收盘后 Tue–Sat 07:00）；同批新增 `sync_bond_yields()`（CN/US 国债曲线，akshare）。测试 `TestVix` / `TestBondYields`。
- **预注册** `designs/vix-regime-prereg-2026-09-24.md`：冻结 VIX 分位（`vix_prior` 严格早于 CN 日 t，trailing 252d，≥80=HIGH）；K1 条件 edge + K2 闸增量；零网格。
- **只读诊断** `scripts/diag_vix_regime.py`：B3 5 资产 + B3 组合 `fwd20` 按 regime；VIX 闸 B3（HIGH 月频 → 100% 511260）vs 基线 × 四窗。

## 验证 / 数据

- **REJECT（K1/K2 双 FAIL，方向反号）**：long B3 `fwd20` HIGH **+0.82%** > NORMAL +0.58%（分位桶 ≥80 最高，diff +0.24）；闸 B3 long **+43.3 vs 基线 +46.2**（Sharpe 1.76→1.91、MDD −4.7→−4.6），G1/G3/long 挂。
- 结论：**"VIX 高 → risk-off 去风险"证伪**；反向更像"恐慌后反弹"（均值回归家族，B4/B6 已 REJECT）。
- 报告 `data/backtest_reports/vix_regime_2026-09-24.json`。

## 后续影响 / 留给谁

- 档 [`backtests/event/vix-regime-2026-09-24.md`](../backtests/event/vix-regime-2026-09-24.md) + SUMMARY 一行；预注册结果已回填。
- VIX 与收益率曲线现为**每日增量**数据，后续宏观研究即用即新。
- FedWatch / 点阵图（隐含加息概率）**仍缺**——是 §一.14 利率线重开条件 (a)；且需 (b) 美股时段/盘中执行，二者缺一不可。
- 不需要补 OPT/TIP。
