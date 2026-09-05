# S-3 入场耗尽否决：核心买 strength，顶不追（2026-09-05 启动）

> **一句话**：卫星侧做减法三连死后，把同一个耗尽信号换到 horizon 对得上的地方——S-3 持有最长 60 天、信号目标 20 天，同属慢尺度。冻结 S-3 入场里混入的 `strong_scoop_exhaustion`（ret60>0.40 & 放量>1.2x，阈值逐字抄形态文档，不扫网格）直接跳过、不顺位补。单假设、诊断先行（OOS2+train，valid 不碰），不过就关。
> **关键词**：S-3 耗尽否决 horizon匹配 预注册 单假设
> **状态**：计划已冻结，待跑。结论（PASS 或 REJECT 都留档）进本档 §4；任何情况下不自动进 Live，不改冻结 S-3 常量。

**基线**：`strategy-params.md §1` 冻结 S-3（score65/hold60/RS0.5/gates full/trailing −8/Strong-ATR/neutral_block/entry_style auto/D2 45/D3 仓位/panic2/熔断−25/mp10）+ 三窗 NAV 基线 OOS2 +47.3/train +34.1/valid +38.7（n=93/51/16 ⚠️）。
**为什么是这条路**：
1. E-veto 死于 horizon 错配（20 天信号否决 3 天脉冲），不是信号死——S-3 的持有尺度（最长 60 天、trail8 multi腿同理）与信号尺度（20 天）同属一档，错配论证反过来支持这里。
2. 未开垦地：48+ 次失败全在卫星侧/权重/退出/松闸，S-3 入场侧的股票级否决（收紧方向）从未测过；“松闸全拒”禁的是放宽，不是收紧，不适用。
3. 先例：环境条件式收紧通过过（neutral_block valid +10.7pt/DD 12.1→2.7；entry_style auto），选择性跳过最差尾巴是走得通的一类。
4. 风险诚实：失败模式 #2（防守收紧截右尾）是最大坟场；S-3 valid 仅 n=16 underpowered，最终必须看 twin 合成窗判定。

---

## 2. 预注册冻结细则（跑之前定死，跑中不改）

- **否决定义（逐字抄）**：信号日 S（= 入场前一交易日，决策信息集内，零新增前视）判定 `strong_scoop_exhaustion` 且 `ret60 > 0.40` 且 `vol_ratio > 1.2`（对应落库 prob 0.894 档；实现与 `factor_signals_service.scan_strong_scoop_exhaustion` 逐行同口径，已在 2025-06-16 七信号上 bit 级对过，见 E-veto 档 §5）。**只测这一个组合**，不开 ret60/vol 网格。
- **执行**：S-3 引擎入场前跳过命中票（strict，不顺位补；槽位自然空闲）。持仓中命中不提前卖（只管进门，不管清仓，避免第二个开关）。回放阶段需引擎加实验旗 `skip_exhausted_entries`（默认关，Live 不传）。
- **范围**：CN S-3（STOCK 腿主力，数据最全）。HK 不动（train 极弱 + valid 小，搅在一起说不清；CN 过了再另开档）。
- **诊断指标**：已实现单笔净 pnl（引擎自带退出：trail/止损/hold），按入场否决标记切 exhausted/clean，报均值/胜率/n/平均持有天数（持有天数仅描述）。
- **窗口**：诊断只看 OOS2+train（valid 不碰）；回放三窗为拒收闸； twin 合成（opp_50 习惯卫星 + 新核心）为最终判定；2021/22/23 长窗描述性；holdout 只读。
- **口径**：window-local 空簿；S-3 回测口径（名义 10%×10）与冻结基线一致；成本引擎内已含。

## 3. 拒收线（任一触发即 REJECT，关闭方向不补变体）

1. 诊断双窗不一致（exhausted 有一窗更好）→ 连回放都不进，直接 REJECT（E-veto 翻版：方向反即死）。
2. 覆盖 <1% 交易笔数（否决几乎不触发 = 无增量，关方向）或 >25%（砍掉四分之一入场 = 换策略不是否决，关方向）。
3. 回放任一窗 S-3 腿 tot 相对冻结基线差 `< −5pt`，或任一窗 sharpe 差 `< −0.3`。
4. twin 合成任一窗输核心（loses_core_tot）或 valid twin−core ≤ +2.7 缓冲的一半（即新核心不能吃掉习惯卫星本就很薄的 valid 缓冲——合成 valid Δ 必须 > +1.3pt）。
5. S-3 valid n=16 下任何“valid 单窗显著好、选参窗平”都判 underpowered，不算 PASS（防小样本幻觉）。

PASS 线：诊断双窗同向为负 → 回放三窗 tot/sharpe 不差于基线 → twin 合成三窗 beats_core 且 valid 缓冲不塌。**任何情况不自动进 Live**；真要上还需 paper C4 20 笔前瞻确认（§3 流程第 5 步）。

## 4. 执行步骤（一次一刀）

1. **诊断电池**：新脚本 `scripts/diag_s3_exhaust.py`（只读）：冻结 S-3 CN 跑 OOS2/train（`run_walk_forward.S3_CONFIG` 原样）， realized trades 按入场否决标记切分，报均值/胜率/n。valid 不碰。
2. **三窗回放**（诊断过才开）：引擎实验旗 + `run_walk_forward --param` 式三窗，对照冻结基线。
3. **twin 合成**：新核心 NAV × 习惯卫星（冻结 C1·14:30）opp_50，判 beats_core + valid 缓冲。
4. **收尾**：本档补结果表 + 判定；`SUMMARY.md §1` 追一行。

复现（待第 1 步落地后填实测）：

```bash
cd services/data-sync-service
PYTHONPATH=src:scripts python3 scripts/diag_s3_exhaust.py
```

## 5. 结果（2026-09-05 诊断电池实测 · REJECT，未进回放）

> **判定：REJECT/零覆盖**。冻结 S-3 的实现交易里根本没有耗尽票可否决（OOS2 0/93、train 0/51），否决变体与基线 bit 一致，跑回放没有意义。按预注册 §3.2 关闭方向。

**脚本**：`services/data-sync-service/scripts/diag_s3_exhaust.py`（只读；`run_walk_forward.S3_CONFIG` 原样；realized trades 按信号日标记切分；valid 未碰）。
**管线自检**：诊断跑出的交易行数与冻结基线逐数一致（OOS2 93 / train 51），S-3 侧无漂移。

拆门表（OOS2 93 笔，逐门倒下）：trend 门 61（信号日 MA20>MA60 且 30 天前站上 MA60 不成立）· bottom_old 15 · depth 越界 9 · 阈值差一线 7（最近一笔 ret=0.67/vr=1.03，量比差 0.17）· recover/novol 0。

读法：

1. **S-3 与耗尽天然不交集**：S-3 入场票 60 日涨幅大多 <10%（中位约 +5%），61/93 在信号日连 MA 多头排列都没形成——S-3 钓的是趋势早期（刚转强/电风扇回调），耗尽顶是趋势末期（+40% 后的派发），两者在鱼塘的不同水层。否决覆盖恒为 0，不是阈值严一点松一点能解决的（最近一笔量比 1.03，放到 1.0 也只多 1 笔，噪音级）。
2. 这是 E-veto 的镜像结论：卫星侧否决死于“方向反”（horizon 错配），核心侧否决死于“无交集”（选股错层）。耗尽信号本身（89%、四桶稳定）不推翻，它只是和双子星的两条腿都不咬合——判别层继续 stale 着（`scoop-exhaustion-oos-check`），不借本结论碰它。
3. 附带正信息：S-3 对晚期派发顶**结构性免疫**（钓早期鱼），核心不需要这类否决——冻结 S-3 常量一个不动，引擎零改动。
4. **Live 不动**；`SUMMARY.md §1` 追一行即关档。

```bash
cd services/data-sync-service
PYTHONPATH=src:scripts python3 scripts/diag_s3_exhaust.py
```
