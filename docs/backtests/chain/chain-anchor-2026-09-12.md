# H-CHAIN-A：海外 anchor → A 股产业链概念传导（2026-09-12 · REJECT，§一.14 确认）

> **一句话**：12 条产业链配对（苹果←AAPL、英伟达←NVDA、存储←MU…）：anchor 当日涨跌 → A 股概念的 **隔夜 gap corr 0.318**（信息确实传导），但 **日内 open→close corr −0.01**、anchor 20 日动量 → 概念未来 20 日 **high−low −0.11%**（long）。**传导是一次性隔夜跳空、无慢漂移 → 按 `§一.14 宏观信息消费端律` 判死**（产业链信息也是"A 股消费的海外信息"）。
> **关键词**：产业链 anchor 概念板块 隔夜跳空 慢漂移 §一.14 REJECT

**预注册**：[`designs/chain-anchor-prereg-2026-09-12.md`](../../designs/chain-anchor-prereg-2026-09-12.md)
**脚本**：`scripts/diag_chain_anchor.py` → `data/backtest_reports/chain_anchor_2026-09-12.json`
**数据**：`data/chain/board_index.csv`（12 板，同花顺概念指数）+ `anchor_us.csv`（8 美股）；配对见 `meta.csv`。

---

## 1. 反应分解（pooled，13,545 个 board-day）

| 项 | corr |
|----|------|
| `anchor_ret` → **gap**（隔夜兑现） | **+0.318** |
| `anchor_ret` → **intraday**（日内，可交易） | **−0.01** |

**逐配对 gap corr 全正 0.23~0.41**（存储芯片 0.41、光刻机 0.41、苹果 0.39…）；**intraday corr 全部 ≈0（−0.05~+0.02）**。

→ **信息 100% 在 A 股开盘一次性兑现**，盘中没有延续——与 `§一.14` 完全一致。

## 2. 慢漂移（anchor 20 日动量三档 → 概念 fwd20）

| 档 | fwd20 mean% | n |
|----|-------------|---|
| low | +2.41 | 4,440 |
| mid | +2.43 | 4,412 |
| high | +2.30 | 4,453 |

**high−low = −0.11%**（无单调、无增量）。分窗：OOS2 +3.41 / train −0.58 / valid +4.63 / **long −0.32**（强 regime 依赖，long 反号）。

## 3. 判定

| 线 | 结果 | |
|----|------|---|
| K1 corr(anchor_ret, gap)>0.2 | +0.318 | ✅ |
| K2 intraday corr≥0.1 或 mom20 high−low>0 | −0.01 / −0.11 | ❌ |

**→ REJECT（§一.14 gap-consumed）。**

## 4. 读法

- **产业链"推理链"的公开传导 ≠ 可交易 alpha**：anchor 的量价信息在 A 股开盘 gap 里**一步到位**，与本体系日线级执行不兼容。
- 真正可能存在的（**未测、且难**）：KOL 的价值若是*识别二阶/未被映射的供应商*（链上"真受益但市场没反应过来"的那只），那是**个股级横截面 + 供应链映射**问题，需要 PIT 成分 + 主营关联——本体系的**概念指数层**已证"概念整体无慢漂移"，二阶假设先验更弱。
- 分窗 OOS2/valid 正、train/long 负 = 典型 `#4 regime`（大行情年份才有主题延续）。

## 5. 结论 / 处置
- **B3 产业链第一刀（概念层）关闭**：传导=隔夜 gap，`§一.14` 确认，本体系内不重开。
- 深挖需"个股级供应链映射 + PIT 成分"（数据不可得/偏差大）→ **parked**，除非有新的 PIT 供应链数据源。
- 新增只读数据：`data/chain/`（board_index / anchor_us / meta，gitignored）。

## 6. 复现

```bash
cd services/data-sync-service
PYTHONPATH=src python3 scripts/sync_chain.py
PYTHONPATH=src:scripts python3 scripts/diag_chain_anchor.py --save-report
```
