# P0-13 深层次信号（主力脚印）· 三源全跑  · 归档于 2026-09-12

## 当时的目标（todo 链接）
- `docs/todo.md` → **P0-13 B2 深层次信号**。用户框架：信息链**尾**（散户关注/情绪）为负、链**首**（主力脚印）为正；想验证"深层次正向"。
- 先测个股层**主力脚印**三源（长历史、可回填）：龙虎榜机构席位 → 股东户数/筹码 → 大宗交易。

## 实际做了什么
- `sync_lhb_inst.py`（akshare `stock_lhb_jgmmtj_em`）→ `data/lhb/lhb_inst.csv`（52,475 行，2021+）。
- `sync_gdhs.py`（tushare `stk_holdernumber`，含 ann_date）→ `data/gdhs/gdhs.csv`（253,503 行，2021+）。
- `sync_block_trade.py`（tushare `block_trade`）→ `data/block/block_trade.csv`（306,113 行，2021+，13.9 万含机构专用）。
- 三个只读前瞻诊断（`diag_lhb_inst` / `diag_gdhs` / `diag_block_trade`），统一口径：T 日盘后披露 → **T+1 开盘买**，持有 N 日，超额 vs 中证500。均预注册冻结。
- 注：**机构调研**（首选）数据源无长历史（tushare `stk_surv` 无权限、akshare 接口逐股遍历且 date 被忽略）→ 换龙虎榜机构。

## 验证 / 数据
- **龙虎榜机构净买 → REJECT**：净买/净卖两组前瞻超额**都负**（N10 −1.55/−1.48），差值 long **−0.05**。龙虎榜=公开拥挤链尾（上榜负漂移），机构净买无额外信息（`#2 共线`）。
- **股东户数/筹码集中 → REJECT（耳语级正向）**：户数下降相对上升略好（N20 差值 mean **+0.18%**、median +0.40%），方向对；但**绝对为负、不单调（弱降>强降）、60 日反转 −0.19**。
- **大宗交易折溢价 → REJECT（最强生命体征）**：折溢价梯度**单调**、溢价组**三窗全正**（K1 ✅ +0.40；K2 ✅ 溢价−大折价 N20 **+1.44%**）；但 **K3 机构专用席位 ❌（买−卖 −0.28，方向反）+ median 全负**（均值靠右尾）。
- **大宗溢价动量中性化复验（H-BLK-B）→ PASS / REVIVE（本轮唯一 PASS）**：`scripts/diag_block_momneutral.py`（预注册 `docs/designs/block-momneutral-prereg-2026-09-12.md`）——动量化后 **Δ 三档全正**（down +0.98/mid +0.70/up +0.69）、`Δ_neutral` **四窗全正**（OOS2 +0.29/train +1.57/valid +1.61/long +0.82）；溢价组**偏下跌档**（45.7% vs 折价 39.7%），**不是动量换皮**。仅"溢价大宗"腿复活；机构席位仍死。
- **大宗溢价组合级回放（H-BLK-C）→ REJECT / park（线关闭）**：`scripts/replay_block_premium.py`（预注册 `docs/designs/block-premium-replay-prereg-2026-09-12.md`）——溢价做成日频 EW 多头篮子（持有 10 日、扣 30bp、日均活跃 178.5）：long 年化 **+0.89% vs 中证500 +1.96%**（超额 **−1.33%**，仅 valid +6.62 正）；折价篮子 −6.86%。**K2 ✅（premium>disc 在组合层成立）但 K1 ❌**——价差**不可实现**（价值在空头腿，A 股不可做空）。**交易级 PASS 未过组合门。**
- 共同死法：**右尾依赖（median 负）+ 与动量/拥挤共线（#2）**；幅度薄（≤1.4%/20d），成本后难活。**例外：溢价大宗腿中性化后过线，但组合级仍不可实现。**

## 后续影响 / 留给谁
- **"深层次正向"框架有微弱支持（大折溢价梯度、户数下降）但都不够格做策略**；三条深源收口。
- **溢价大宗腿：交易级 PASS → 组合级 park**。最终结论 = **折溢价是"多空相对"信息，不是长多选股信号**（价值在空头腿，A 股不可做空）。**大宗线关闭**，不再补参数/窗口。
- **唯一残余（写死，暂不追）**：**大折价大宗 = 负向标记**（长期年化 −6.86% vs 基准 +1.96），可作"避开/减配"过滤器候选；须先证相对 S-3/主线闸**有覆盖、不被吸收**（§五 维3/维4），否则同属不可实现空头信息。
- **管线门教训**：H-BLK-C 首跑误用原始大宗价（非 `price/close−1`）→ 折价腿为空（n=0 一眼可见），修正重跑。
- **散户关注度（大V 跟风）**：可采集代理（东财人气榜/雪球关注）**历史仅当前快照或 ~1 年**，无法回填。**已完成**：东财人气榜 D1 早已 SHELVE（`cn_hot_rank`，追高税）；**雪球关注快照 job 已上线（OPT-176，2026-09-12）**——`cn_xq_follow`（migration 0049）+ 工作日 15:40 job 每日落库，向前积累，攒够 12 个月再开诊断。KOL 个人页历史回填脆、有幸存者偏差，暂不做。
- 新增数据资产（只读、gitignored）：`data/{lhb,gdhs,block}/`。
- 无新 OPT/TIP；不改 Live/schema。

## 关联档
- 实验：`docs/backtests/inst/{lhb-inst-netbuy,gdhs-concentration,block-premium}-2026-09-12.md`
- 预注册：`docs/designs/{lhb-inst,gdhs-concentration,block-premium}-prereg-2026-09-12.md`
- 脚本：`scripts/sync_{lhb_inst,gdhs,block_trade}.py` · `scripts/diag_{lhb_inst,gdhs,block_trade}.py`
