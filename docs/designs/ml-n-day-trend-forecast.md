# ML N日趋势预测 — 全新模型管线设计草稿

> 状态：**设计中·未落地** | 目标：用 PyTorch 时序模型预测 N 天后趋势，`pred > X%` 才进池，替代/补充 TrendOK 主观特征
> 与 S-3 解耦：训练/验证/回测三窗对齐现有 `walk_forward` 窗口，避免与 `strategy-params.md S-3` 混同

## 1. 问题定义

**不是** 1 日择时。`S-3` 已是 `score+RS+行业+熔断` 的 1 日入场，本模型回答：`未来 N 日该票有没有趋势性`。

- 输入：截至 `T` 日收盘的**点时**序列 `X_{t-L+1..t}`（OHLCV/量价/已落库因子）
- 标签：`y = (close_{t+N}/close_t -1)` 或 `max( close_{t+1..t+N}/close_t -1 )` 二选一
  - 推荐先做**回归** `y_reg` + 阈值 `X%` 转信号；次选直接**二分类** `y_cls = y_reg > X`
- 交易：`pred_{t} > X` → 进入次日候选池 → 再过 `S-3` 其余硬闸（熔断/红灯/仓位），**不绕过风控**

默认超参（可配）：`N ∈ {10,20,30}` 主 `20`，`X ∈ {5,8,12}%` 主 `8%`（与 `-5止损/20目标` 对齐），`L=60` 日窗口。

## 2. 数据：本地 Postgres 真值

**源**（只读本地库，零外网）：

| 表 | 用作 |
|---|---|
| `stock_daily` (qfq) | OHLCV/量 |
| `stock_dailybasic` | 市值/换手 |
| `watchlist_scores_daily` | TrendOK 分解（已落库，可当特征） |
| `industry_fund_flow_daily` / `cn_market_sentiment` | 行业/情绪（如有时序） |
| `macro_daily` | HSI/指数 regime |
| `index_dailybasic` | 市场宽度 |

**严禁泄露**：

- 特征只用到 `T` 收盘（含）前；标签用 `T+N`；`adj_factor` 用 `T` 时点值
- 除权/停牌日样本丢弃；`amount` 用 `T` 已知
- 按 `calendar` 交易日对齐，`N` 按交易日计

**样本构造**：每票每日一个样本（`ts × day`），过滤 `avg_amount 0.7亿` 以下（同 S-3 流动性闸），`2021-08-01` 起全市场 `~5200×~1200 ≈ 600万` 候选，实际有 `~250万` 样本。

## 3. 切分：三份 + walk_forward 对齐

**不做随机 shuffle**（时序泄露）。对齐现有三窗，与 `performance-log.md` 可比：

```
OOS2   2024-08-01 ~ 2025-08-01  → TEST  （ never train，跟踪 S-3 43.1%）
train  2025-08-01 ~ 2026-02-01  → TRAIN
valid  2026-03-01 ~ 2026-08-07  → VALID （调阈值 X/N/hyperparam）
holdout 2026-08-08 ~ 至今      → HOLDOUT （n<100 仅观察，满足才动 live）
```

另留 `2021-08 ~ 2024-07` 作**预训练**可选，`2024-08` 起三窗为**硬评估**。滚动时每月向前推 `30日`（同 `rolling_oos_job`）。

划分单位：**按时间切**，同日内所有票同属一 split。

## 4. 特征与标签

**标签**：
- `label_reg = (close_{t+N}/close_t -1)*100%`，`clip [-30, +100]`
- `label_cls = label_reg > X`

**特征（L=60 序列，每日一向量 ~20维）**：
- 量价：`ret1/5/20, log_vol, amount_rank, high_20/m, low_20, atr20, rsi14, ma20_bias, vol_ratio20`
- 趋势：`score, rs_rank, trend_strength`（TrendOK 产物，当特征不用当标签）
- 横截面：`市值分位, rank_in_industry_ret20`
- 市场：`hs300_ret20, is_red_day, breadth`

全部 `z-score` 按 `train` 窗滚动 `252日` 标准化，`valid/test` 用 `train` 统计量（防泄露）。

## 5. 模型

**基线优先**：

1. **TCN**（时序卷积，轻、稳、MPS 友好）— 主
2. **LSTM 1层**（对照）
3. **PatchTST/Transformer**（二期，参数多，易过拟合）

输出头：`回归头 + 分类头` 双任务 `loss = MSE + 0.3*BCE`（消极样本多时稳）。

输入 `[B, L, F]` → `TCN(d=64,k=3,layers=4,dil=[1,2,4,8])` → `pool→MLP 64→1`。

## 6. 训练

- 优化：`AdamW lr1e-3, wd1e-4, cosine`，`batch 1024`，`early stop patience 10` 看 `valid AUC`
- 正负样本：`label_cls` 正样本约 `15-25%`，`pos_weight` 或 `focal`
- 正则：`drop0.2, label smooth, weight decay`，**不做**暴力调参
- 硬件：`Mac MPS` 优先，`batch` 适中，`60×20` 序列单卡可跑；`uv` 管 `torch` `mps` 版
- 复现：`seed 42`，`torch deterministic`，`uv.lock` 锁版

**不过拟合纪律**（与 `todo §19` 同口径）：
- `train` 上 `AUC>0.58` 但 `valid AUC<0.53` → 拒
- 三窗中任一窗 `paper语义` 回测 `vs S-3 diff < +5pt` 或有窗劣化 → 拒（沿用 S-3 `>5pt` 票决）
- `valid` 调参，`OOS2` 只打分不选型

## 7. 评估 → 交易

**离线**：`valid` 上 `AUC, PR-AUC, IC( pred vs label_reg ), 阈值X下的 precision@k, 收益按分位`
**在线（回测语义）**：`pred > X` 当 `S-3` 的 `score65` 替代/并集，跑 `BacktestEngine` 三窗，对比 `S-3` 基线 `dfc6539e (43.1/34.3/43.3%)`：

| 模式 | 含义 |
|---|---|
| `ML-only` | 候选 = `pred>X` |
| `ML∧S-3` | `pred>X && score≥65`（更严） |
| `ML∨S-3` | 并集（更宽） |

**过线才进 `todo`**：任一模式三窗同时 `>+5pt` 且 `valid n≥30` 才固化参数→ `models/ml-forecast/` 版本化。

## 8. 工程

**新目录**（与 `data-sync-service` 并列，不污染）：

```
services/ml-forecast/
  pyproject.toml  (uv, python>=3.11, torch, pandas, psycopg, scikit-learn)
  src/ml_forecast/
    data.py       // PG → Parquet 缓存 → Dataset
    features.py   // 60日窗口构造，点时标准化
    model.py      // TCN/LSTM
    train.py      // 训练循环，early stop，存 ckpt
    evaluate.py   // IC/AUC + 回测对接
    predict.py    // 日终批量推断 → 写 ml_predictions_daily
  scripts/
    build_dataset.py
    run_train.py
    run_backtest.py  // 调 data_sync_service BacktestEngine
  models/         // ckpt 按 valid 日期版本
```

**uv 管理**：
```bash
uv init services/ml-forecast --python 3.11
uv add torch pandas numpy scikit-learn psycopg[binary] python-dotenv
uv add --dev pytest ruff
```

**DB 新增表**（Alembic 在 `data-sync-service`）：
- `ml_predictions_daily (day, ts_code, pred_N, prob_cls)` 日终批量写，供回测/体检卡片读

**调度**：`predict` 每日 `17:50`（`score` 落库后），失败不阻断 `S-3`。

## 9. 里程碑（~2-3 周）

- [ ] **M0 架子**：`services/ml-forecast` + `uv` + `data.py` 能从本地 PG 拉 `2021起` 样本 `10万` 跑通 `Dataset`
- [ ] **M1 数据集**：`features.py` 60窗全量 Parquet 缓存 `~250万` 样本 + `train/valid/test` 切分单测
- [ ] **M2 基线**：`TCN` 在 `train→valid` 跑通 `AUC/IC`，MLP/均值基线对照
- [ ] **M3 回测对接**：`run_backtest.py` 三窗 `ML∧S-3 / ML-only` vs `S-3 43.1%` 出表 + `performance-log.md` 增 `ML` 行
- [ ] **M4 定案**：`valid` 选 `N/X`，`OOS2` 盲测 `>+5pt` 才归档 `docs/archive/ml-forecast-*.md` + `strategy-params §X`

## 10. 风险与取舍

- **数据天花板**：行业资金流仅 `121日` 历史，`OOS2` 无此特征 → 特征集对 `OOS2` 保持可得子集
- **算力**：`250万×60×20` 全量 `TCN` 约 `1-2h/MPS`，先 `2023起` 子集验证再全量
- **过拟合**：`S-3` `17.7%` 年化已是强基线，`ML` 大概率 `±5pt` 内平台期——接受，按 `§19` 不破纪律
- **可解释**：`pred` 需可追溯到 `量价/RS` 因子，`evaluate` 打 `SHAP/IC` 附表
