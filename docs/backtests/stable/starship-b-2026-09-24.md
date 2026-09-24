# 星舰 B（稳定停放腿版）· 2026-09-24 · 研究/展示/人工操作 canonical 备选

> **一句话**：把稳健星舰的闲置现金停放腿从「25% H2 套筒 + 75% B3」换成
> **100% 停 {国债 + 黄金 + 纳指} 逆波动率（3 腿月频）**。长期收益比 H2-a25 少 69pt，
> 但**回撤更浅（−5.5 vs −8.4）**、**Sharpe 更高（3.90 vs 3.50）**、**2022–23 熊市显著更强**，
> 且**停放腿只有 3 只 ETF，实盘好复制**。
> **身份**：研究/展示/人工操作档；Live 自动执行仍 = 港湾。
> **关键词**：星舰B 停放腿 3腿逆波动率 国债 黄金 纳指 回撤 熊市

**脚本**：`scripts/eval_starship_b.py`（只读，复用 `eval_sat_idle_parking` 单源件 + `homeport.starship_b_run`）
**组件真值**：`service/homeport.py:starship_b_run` / `starship_b_nav` / `load_starship_b_closes`；`state_bucket_track.STARSIP_B_PARKING_MODE`

---

## 1. 定义（可重建 spec）

| 项 | 星舰 B | （对照）H2-a25 |
|---|---|---|
| 卫星腿 | **冻结不变**：`HABIT_RECIPE`（4 槽 ×25%，14:30 进出，body=3，`amp_1430`，`gate_1430`） | 同 |
| 闲置现金权重 | `w_t = cashShare(T−1)`（因果） | 同 |
| 停放资产 | **100% `{511260 国债, 518880 黄金, 513100 纳指}` 逆波动率**（60d，月频再平衡，5bp/边） | 25% H2 套筒 + 75% B3（5 资产逆波动率） |
| 组合公式 | `r_t = sat_ret_t + w_t × bRet_t − 5bp×\|Δw\|` | `r_t = sat_ret_t + w_t × (0.25×h2 + 0.75×b3) − 5bp×\|Δw\|` |
| tag | `starship-b-20260924` | `sat-h2-a25-v1-20260924` |

## 2. 结果（总收益% / maxDD% / Sharpe）

| 窗口 | **星舰 B** | H2-a25（现行） | 纯 B3 |
|------|-----------|----------------|-------|
| OOS2 | +231.7 / −4.0 / 6.65 | +238.4 / −5.0 / 6.32 | +236.5 / −5.6 |
| train | +51.5 / −8.2 / 4.01 | +55.9 / −7.5 / 4.16 | +49.6 / −8.3 |
| valid | **+5.4** / −10.0 / 0.66 | +3.2 / −10.8 / 0.46 | +3.0 / −10.1 |
| **long** | +669.6 / **−5.5** / **3.90** | **+738.5** / −8.4 / 3.50 | +654.3 / −6.8 |
| **stress**（22–23） | **+124.0 / −7.4 / 2.95** | +103.5 / −10.1 / 2.48 | +97.2 / −8.7 |
| holdout（描述） | −7.1 / −13.1 | −7.5 / −13.4 | −6.5 / −12.5 |

（`cagr%`：OOS2 248.55 / train 135.95 / valid 12.87 / long 52.69。）

## 3. 判定与取舍

| 维度 | 谁赢 |
|------|------|
| long 收益 | **H2-a25**（+738.5，多 69pt）← 收益第一优先级 |
| long 回撤 / Sharpe | **星舰 B**（−5.5 / 3.90） |
| **stress（熊市）** | **星舰 B**（+124.0 / −7.4 vs +103.5 / −10.1） |
| valid / holdout | 星舰 B 略好 |
| **可复制性** | **星舰 B**（停放腿 3 只 ETF vs H2+B3 的 6 只） |

**结论**：**星舰 B 是"收益略低但更稳、更好复制"的备选**。追求最高长期收益仍用 H2-a25；
要熊市韧性 + 实盘好落地（3 腿 ETF），用星舰 B。

## 4. 复用组件（写文档 + 接线）

| 组件 | 位置 | 说明 |
|------|------|------|
| 停放腿 NAV | `homeport.starship_b_run` / `starship_b_nav` | 3 腿逆波动率月频（复用 B3 数学），因果 |
| 数据加载 | `homeport.load_starship_b_closes` | 复用 B3 复权面板 + DB 尾部 |
| 停放模式 | `state_bucket_track.STARSIP_B_PARKING_MODE` (`"starship_b"`) | `apply_parked_display(parked_mode=...)` 分支 |
| 常量 | `state_bucket_track.STARSIP_B_UNIVERSE` / `STARSIP_B_TAG` | 单一来源 |
| Timeline | `GET /api/backtest/timeline?strategy=starship_b` | mode/strategy/parkingMode=starship_b |
| 展示档 | `strategy-settings.ts` / `settings_routes.py` / `strategy_catalog.py` | 五档 + 星舰B；默认仍 `starship_robust` |
| frontend | `lib/queries/backtest.ts`（TimelineStrategy）、`BacktestPage.tsx`、`StrategyModeBar` | 标签「星舰 B」 |

## 5. 复现

```bash
cd services/data-sync-service
PYTHONPATH=src:scripts python3 scripts/eval_starship_b.py
# 对账：Timeline strategy=starship_b 的 OOS2 fusedPct 应 = +231.7
```

## 6. 边界

- **不给 PASS 裁决**：这是研究/展示档，非冻结 K 门；与 H2-a25 并列。
- 停放腿 3 腿逆波动率是 `homeport.risk_budget_run` 数学的复用（月频，5bp/边），未新增机制。
- 长期收益让渡来自停放腿（B 腿 long +46.7 vs H2 套筒 +100.3）；若未来想两者兼得可试「B 腿 + 少量 H2」，需另起预注册。
