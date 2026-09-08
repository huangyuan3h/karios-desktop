# OPT-148：paper_s3 HK intake settled-cash 账本 · 归档于 2026-09-08

> paper 开得出真券商买不起的 HK 仓（paper 偏乐观）→ T+2 账本落地。

## 改动（`service/paper_s3.py`，只动 HK 路径）

- `_hk_settled_cash(day)`：settled = 1.0 − 已部署（open S3HK sleeves）− 未交收（非 swap、不足 3 个 HK session 的平仓，按 sleeve×close/entry 折算）；swap 平仓排除（引擎口径：swap 不走现金）；HK session 数走 `daily` 去重日期，异常回退 Mon–Fri；任何失败 fail-open 大声记 `settleLedger=fallback`。
- intake 接线：每个 fresh candidate 检查 `settled ≥ sleeve×(1+entry_frac)`，不够记 `settle-lock` 跳过并扣本地账；swap-in bypass（引擎口径）；summary 透出 `settledCash/settledDeployed/settledUnsettled`。
- 附带修：swap 平仓费用 CN 硬编码 → 按市场取（HK 90bps），`_swap_holds_for_candidates(..., market)`。
- 费用模型同步：平安网上最高 0.20%/边 → HK commission 5→20bps（round-trip 60→90bps）+ `entry_cost_frac()`；API 文案 `~0.60%`→`~0.90%`。

## 验收证据

- 单测 7 个全绿：账本（空账/冻结/swap排除/旧单释放/故障回退）+ intake（锁单跳过/放行）+ LIKE 转义回归 + swap 按市场计费。
- live 只读验证 2026-09-08：`settled=-0.045`（已部署 0.95 + 未交收 0.095）→ 当日 HK:02343 那笔 paper intake 在现实里会被 `settle-lock` 拦下——缺口真实存在，账本行为正确。
- 回测侧：90bps 下 HK 现实最终 OOS2 +2.1 / train −3.0 / valid +64.1 / past_year +57.3（见 `docs/backtests/hk/hk-settle-t2-2026-09-08.md §11`）。

## 残留（故意不做）

- HK$100/笔最低收费未建模（需绝对本金；sleeve 是 % 口径）+ 微费 ~1.27bps/边（≈0.2pt）——声明偏乐观残留。
- 金字塔加仓腿走行插入（无独立账本检查），敞口已含在已部署侧；可接受近似。
