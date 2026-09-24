# H-SGAP-ALT-2 S-gap 层 bar 级量额特征（2026-09-18 · REJECT）

> **一句话**：ALT-1 证「位置类」排序全输 `amp_1430`；本档补 **bar 级量/额**维度——价格相对 VWAP 的偏离、
> 午后参与度、量价重心，共 3 特征 × 双向，走真引擎 A/B。基线复现 **OOS2 +208.9% / train +36.9%**；
> **无一臂在 OOS2 接近基线**（`vwpos:asc` +134.4%、`pm_share:asc` +129.1%，差 −74~−80pt），
> train 小胜但选参窗崩 → **REJECT**。`amp_1430` 仍是这条腿专属、不可替换的排序。
> **关键词**：S-gap 层 VWAP 偏离 午后量占比 量价重心 代理振幅 A/B REJECT

**预注册**：[`docs/designs/sgap-alt2-bar-features-prereg-2026-09-18.md`](../../designs/sgap-alt2-bar-features-prereg-2026-09-18.md)
**脚本**：`scripts/diag_sgap_alt2_engine.py` · **报告**：`data/backtest_reports/sgap_alt2_engine.json`
**前置**：[`sgap-alt-manifestations-2026-09-18.md`](sgap-alt-manifestations-2026-09-18.md)（ALT-1，位置类 REJECT）· [`stable/sgap-habit-satellite-standalone-2026-09-14.md`](../stable/sgap-habit-satellite-standalone-2026-09-14.md)（S-gap 真值）

---

## 1. 口径（预注册冻结）

- 事件池 / 执行 = 冻结 S-gap：gap>3%、R-wide 14:30 闸、skip_t1 + C1 3%、14:30 买 → 第 3 日 14:30 卖、4 槽 × 25%、30bp。
- 新数据通路（只读，不改核心 loader）：`bar_5min` bars `trade_time <= '1430'`、`vol>0`、OHLC 非空聚合。
- 特征（14:30 可得）：
  - `vwap_dev = p1430/VWAP − 1`，`VWAP = Σ((h+l+c)/3·vol)/Σvol`
  - `pm_share = Σvol(trade_time>'1130')/Σvol(≤1430)`
  - `vwpos = Σ(vol·(c−l)/(h−l))/Σvol`（量价重心）
- A/B：特征按日内百分位注入 `ctx['px_hl_1430']`（`hi−lo=pct·px`），跑真引擎；禁用桶/分位均值代理（ALT-1 §3.1）。

## 2. 结果（真引擎 total）

| 臂（14:30 排序，引擎买排序末端） | OOS2 | train | Δ OOS2 / train |
|---|---|---|---|
| **amp_1430（现任）** | **+208.9%**（298 笔 / 胜率 65.8% / +3.14%/笔） | **+36.9%**（124 笔） | — |
| vwpos:asc（量堆 bar 下沿） | +134.4% | +43.4% | −74.6 / +6.5 |
| pm_share:asc（上午放量） | +129.1% | +45.9% | −79.8 / +9.0 |
| vwpos:desc | +9.7% | −18.7% | −199.2 / −55.6 |
| pm_share:desc（午后放量） | −62.2% | −19.4% | −271.1 / −56.3 |
| vwap_dev:asc（低于 VWAP） | −81.4% | +14.2% | −290.3 / −22.7 |
| vwap_dev:desc | −69.8% | −22.4% | −278.7 / −59.3 |

- 基线 OOS2 +208.9% = 直接引擎调用（harness 自检通过）。
- `pm_share:asc` / `vwpos:asc` 在 **train 小胜**（+9.0 / +6.5pt），但 **OOS2 大输**（−80 / −75pt）——
  「train 好看、选参窗崩」的标准过拟合形状，按冻结线 **REJECT**。
- 方向读数：**上午放量 / 量堆在 bar 下沿** 略优于午后放量（与 ALT-1「买强势侧」方向一致：都是选盘口没被砸的），
  但仍远逊 `amp_1430`——再次说明这条腿赚的是「**低振幅 = 未被追高**」本身，而非量价位置。

## 3. 判定与边界

- **REJECT（本方向关闭）**：不再扫 bar 级量额特征。
- **两轮联合**（ALT-1 位置类 + ALT-2 量额类，共 12 臂）：S-gap 层**无第二形态**；`amp_1430` 是唯一强排序。
- **未测边界（诚实）**：分笔/逐笔、盘口挂单、板块/情绪周期维度的排序未覆盖；重开须新预注册 + 新数据源。
- **valid 复用计数不变**（未触碰，L7）。

## 4. 复现

```bash
cd services/data-sync-service
PYTHONPATH=src python3 scripts/diag_sgap_alt2_engine.py --windows OOS2,train --save-report
```
