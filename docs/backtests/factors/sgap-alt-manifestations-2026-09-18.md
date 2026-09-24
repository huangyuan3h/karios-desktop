# H-SGAP-ALT-1 隔夜跳空层的其他表现形式（2026-09-18 · REJECT）

> **一句话**：S-gap 腿的 alpha 长在「隔夜跳空事件 × 14:30 可执行微结构排序」这一层，现任排序
> `amp_1430`（≤14:30 振幅升序 = 买最没被追高的缺口票）是否可替换？把 4 个新的 14:30 零前视
> 「位置」特征（日内位置 / 低点恢复度 / 高点回吐度 × 双向）**注入真引擎 A/B**：
> **无一臂在 OOS2 接近基线**（最好 `range_pos:desc` +92.7% vs 基线 +208.9%），两窗全输 → **REJECT**。
> `amp_1430` 是这条腿专属、不可替换的排序。
> **关键词**：S-gap 层 表现形式 振幅升序 日内位置 微结构排序 代理振幅 A/B REJECT

**预注册**：[`docs/designs/sgap-alt-manifestations-prereg-2026-09-18.md`](../../designs/sgap-alt-manifestations-prereg-2026-09-18.md)
**脚本**：`scripts/diag_sgap_alt_engine.py`（真引擎 A/B）· `scripts/diag_sgap_alt_features.py`（Stage-1 诊断，已判 VOID）
**报告**：`data/backtest_reports/sgap_alt_engine.json` · `data/backtest_reports/sgap_alt_features.json`
**关联**：[`stable/sgap-habit-satellite-standalone-2026-09-14.md`](../stable/sgap-habit-satellite-standalone-2026-09-14.md)（S-gap 真值）· [`sat/sat-rank-hhmm-2026-09-04.md`](../sat/sat-rank-hhmm-2026-09-04.md)（`gap_asc`/`absrunup_asc`）· [`sat/sat-rank-stage-2026-09-11.md`](../sat/sat-rank-stage-2026-09-11.md)（stage 桶内重排）

---

## 1. 口径（预注册冻结）

- 事件池 / 执行 = 冻结 S-gap：`open/pre_close−1 > 3%`、R-wide 14:30 闸、skip_t1 + C1 3%、
  14:30 买 → 第 3 交易日 14:30 卖、4 槽 × 25%、30bp。
- 候选特征（全 14:30 可得）：`range_pos=(p1430−lo)/(hi−lo)` · `low_recovery=(p1430−lo)/lo` ·
  `high_giveback=p1430/hi−1`；每特征**双向**（升/降序）。
- 方法：把特征按**日内百分位**作为代理振幅写进 `ctx['px_hl_1430']`（引擎 amp=(hi−lo)/px，令 hi−lo=pct·px），
  跑引擎 —— 零核心代码改动；基线用原始 hl，复现直接引擎调用。
- 窗口：dev = OOS2 + train；valid 只留给幸存者确认（本档无幸存者，未触碰）。

## 2. 结果（真引擎，total）

| 臂（14:30 排序，引擎买排序末端） | OOS2 | train |
|---|---|---|
| **amp_1430（现任基线）** | **+208.9%**（298 笔 / 胜率 65.8% / +3.14%/笔） | **+36.9%**（124 笔） |
| high_giveback:desc（最贴日内高） | +81.1% | +25.6% |
| range_pos:desc（日内高位） | +92.7% | +11.1% |
| gap_pct:asc（最小缺口） | +16.0% | +4.9% |
| low_recovery:asc | +12.6% | +13.1% |
| high_giveback:asc（最大回吐） | −75.5% | +21.6% |
| low_recovery:desc | −59.9% | −24.7% |
| range_pos:asc（日内低位 = 回踩侧） | −11.0% | +22.9% |

- 基线 OOS2 +208.9% 与冻结档 +212.7%（同口径、数据漂移内）一致 → **harness 自检通过**。
- 无臂在 OOS2 接近基线；两窗超基线的臂 = **0** → **REJECT**。
- 方向读数：买**强势侧**（贴高）> 买**回踩侧**（贴低），但两端都远逊 `amp_1430`——
  说明这条腿赚的不是"位置"，而是"**低振幅 = 未被追高**"本身。

## 3. 方法学记录（重要 · 供后续借鉴）

1. **tercile 桶均值 ≠ 引擎收益**：Stage-1 诊断里 `amp_1430` 顶部 1/3 桶 OOS2 均值 **−1.8%**，
   而引擎 real fills **+3.14%/笔**。原因：引擎只买桶内**最低振幅的 4 只**（极值端），不是整桶等权；
   极值端与桶均值的分布差极大。**任何"桶/分位均值"代理在 4 槽极值选择下都可能反向**——
   本档据此把 Stage-1 判 **VOID**，直接走真引擎 A/B（`L门`：数字不自洽即废，不看 PnL 下结论）。
2. **代理振幅 A/B 手法**：用 `(hi−lo)=pct·px` 改写 `ctx['px_hl_1430']` 即可在不改核心引擎的前提下
   替换 `rank_key='amp_1430'` 的排序——适用于任何"只换排序、不换执行"的筛选实验。

## 4. 判定与边界

- **REJECT（本方向关闭）**：不再扫位置类特征、不换方向定义。
- **未测边界（诚实）**：需要 bar 级量/额的特征（VWAP 偏离、午后来量占比、分时资金）不在 `load_sgap_context`
  的 ctx 内，本档未覆盖；重开 = 新预注册 + 新数据通路，且须先过本档第 3.1 条（禁止用桶均值代理）。
- **valid 复用计数不变**（未触碰，L7）。

## 5. 复现

```bash
cd services/data-sync-service
PYTHONPATH=src python3 scripts/diag_sgap_alt_engine.py --windows OOS2,train --save-report
PYTHONPATH=src python3 scripts/diag_sgap_alt_features.py --save-report   # Stage-1 诊断（已判 VOID）
```
