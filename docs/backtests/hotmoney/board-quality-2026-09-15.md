# H-BOARD-1 封板质量（修正后正式判定 · 2026-09-15 · 三窗 PASS）

> **⚠️ 修正记录（2026-09-15 同日）**：阶段一首跑（[board-quality-phase1](board-quality-phase1-2026-09-15.md)，数字已作废）用 qfq close 判涨停，样本被污染（仅 ~17k/日匹配 20–40 只）。已按 `raw = qfq × adj_latest / adj` 重建，本文为正式结果；**阶段一正格"reopen 1–2 有利"在修正后消失**（见 [exec-probe](exec-probe-2026-09-15.md) REJECT）。
>
> **一句话**：修正后 41,032 个涨停日 × 全天 5min——**封板质量梯度真实且跨三窗稳健**：首封越早（G1 vs G3：OOS2 **+3.69 vs +1.15**、train +3.40 vs +1.45、valid +2.77 vs +1.06）、封板占比越高（G3 vs G1：**+4.39 vs +0.54** / +3.76 vs +0.92 / +2.92 vs +0.71）；剔除买不进的一字后梯度保留，**高质量档净缺口（−30bp）仍 +1.9~2.3%** → **K1/K2/K3 全过 = 质量是真实分界线**。但该缺口归**封板持有者**（游资自己），跟买者（T+1 开盘）拿到的是负漂移（E5b −1.7%）——**分界真实 ≠ 可吃**。
> **关键词**：封板质量 首封时间 封板占比 三窗 PASS 缺口归封板者 不可吃

**预注册**：[`docs/designs/hotmoney-anatomy-prereg-2026-09-15.md`](../../designs/hotmoney-anatomy-prereg-2026-09-15.md)（E3 阶段二）
**数据**：`bar_5min`（source `ext_5min`，本地 vendor 事件集导入 259 万行）· `data/limit/stk_limit.csv` · `daily`（qfq→raw 重建）· 基准 510500
**脚本**：`scripts/import_5min_event_days.py` · `scripts/diag_limitup_board_quality.py --verdict` → `data/backtest_reports/limitup_board_quality_phase2.json`

---

## 1. 口径（预注册）

样本：raw close = 精确涨停价 · 沪深 · 剔 ST/退/BJ · 上市≥60 日 · T 日成交额 ≥0.7 亿 · 全天 5min ≥40 根 → **41,032 个涨停日**（2024-01~2026-09；<40 根剔除 593）。
特征：`first_seal`（首个封死 bar 时间）· `sealed_share` · `reopen` · `tail_sealed`。
分档：**全局三分位**（跑前方法冻结）：first_seal cuts 595/685 分钟（≈09:55/11:25）、sealed_share cuts 0.4375/0.875。
判定：K1 梯度单调（≥2/3 窗）· K2 高质量档 `gap_ex − 30bp > 0`（long 且 ≥2/3）· K3 剔除买不进一字后梯度保留。

## 2. 结果（gap_ex，相对中证500）

| 窗 | n | first_seal G1/G2/G3 | 单调 | sealed_share G1/G2/G3 | 单调 |
|----|---|---------------------|------|----------------------|------|
| OOS2 | 17,187 | **+3.69 / +1.96 / +1.15** | ✅ | **+0.54 / +2.05 / +4.39** | ✅ |
| train | 7,868 | **+3.40 / +2.03 / +1.45** | ✅ | **+0.92 / +2.08 / +3.76** | ✅ |
| valid | 7,896 | **+2.77 / +1.44 / +1.06** | ✅ | **+0.71 / +1.51 / +2.92** | ✅ |
| long* | 39,848 | +3.37 / +1.78 / +1.17 | ✅ | +0.61 / +1.86 / +3.86 | ✅ |

*long 窗此处实为 2024+ 子集（2021–23 全天 5min 待 baostock 补，见 §4）；**剔除买不进一字后（clean）三窗单调全部保持**。

**K2（高质量档、剔一字后 `gap_ex − 30bp`）**：

| 档 | OOS2 | train | valid |
|----|------|-------|-------|
| first_seal G1 | +1.95 | +1.96 | +1.94 |
| sealed_share G3 | +2.57 | +2.25 | +2.08 |

（表中为扣 30bp 前的 `gap_ex`；扣后仍全 > 0。）

## 3. 判定与读数

- **K1 pass · K2 pass · K3 pass → PASS：封板质量是真实分界线**（三窗全过，剔一字不翻号）。
- **质量的定义 = 注意力制造的完成度**：早封（≤09:55）/封死（≥87.5% bar 在板上）的板，次日缺口显著更大；且这不是"涨停"本身（所有样本都是涨停），是**板内路径**的差别。
- **但这口缺口归封板者**：缺口发生在 T 收盘→T+1 开盘之间，只有 T 日收盘时持有（= 把板封住的人）才拿得到；跟买者付掉缺口后拿 d3 —— E5b 实测 **−1.7% 净超额**（[exec-probe](exec-probe-2026-09-15.md)）。
- **与全程序的闭环**：这解释了"头部游资怎么赚钱"——他们的钱 = **自己封板 + 吃自己制造出来的缺口**；散户能观察到的席位榜（E1）只是这个动作的回声，且不含可跟的钱。

## 4. 局限（诚实边界）

1. long 窗 2021–23 全天 5min 未补（baostock 同日被限流，恢复后按 **低并发 + 登录重试** 补跑；脚本 `backfill_5min_event_days.py` 已就绪、修正后事件集 35,364 日/8,410 job）。**三窗判定不依赖 long**，结论不变；补完只影响 long 列。
2. 5min bar 粒度：`first_seal` 精度 ±5 分钟；封死判据（整根 `low ≥ 涨停价`）偏保守。
3. 未建模排队成交（无 level-2）：本文说明"缺口存在且高质量档更大"，不声称"能买到涨停"。

## 5. 复现

```bash
cd services/data-sync-service
PYTHONPATH=src python3 scripts/import_5min_event_days.py          # 2024-26 事件集（修正版）
PYTHONPATH=src python3 scripts/diag_limitup_board_quality.py --verdict --save-report
# 2021-23 长窗（baostock 恢复后）：
PYTHONPATH=src python3 scripts/backfill_5min_event_days.py --workers 2
PYTHONPATH=src python3 scripts/diag_limitup_board_quality.py --start 2021-01-01 --verdict --save-report
```
