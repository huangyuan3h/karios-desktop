# 卫星习惯排名 H1：全天振幅 vs 无前视键（2026-09-04）

> **一句话**：现习惯 Live 的全天振幅排名含未来信息（14:30 时 high/low 还没走完）。试了两个 14:30 前可知的排名键，**两个全拒**：gap% 升序三窗 tot/sr/dd 全崩（OOS2 vs base −96.3）；|14:30/今开−1| 升序 valid 好看（+14.4）但选参窗崩（OOS2 −21.5），按纪律拒收。**Live 排名不动**。
> **关键词**：习惯双子星 rank_key gap_asc absrunup_asc 过拟合纪律

**脚本**：`services/data-sync-service/scripts/compare_sat_rank_hhmm.py`
（诊断：`scripts/diag_sat_rank_keys.py`，只看 OOS2+train，不碰 valid）
**原始表**：`data/backtest_reports/sat_rank_hhmm_2026-09-04.json`
**口径**：window-local 空簿、clip4、C1 3%、same_1430、body=3、第 3 日 14:30 卖、核心择强 trail8、`opp_50`。只改 S-gap 桶内排名键。

---

## 0. 为什么做 H1

回测排名 `sorted(gap_stocks, key=amp)` 用的当日全天 `(high-low)/close`，14:30 做决定时不可能知道。5 分钟已入库（5251 只×504 日，1000–1500 七根全齐），所以诚实键是可做的。R-wide 闸同样用全天收盘（另记残差，本刀不动，保证 base 与变体同闸可比）。

## 1. 诊断（OOS2+train 三档远期收益，不进 valid）

205 个开闸日、约 1.4 万个过 C1 的缺口名，3 日远期净收益按日三档：

| key | T1(低) | T2 | T3(高) | spread T1−T3 |
|-----|--------|----|--------|--------------|
| 全天振幅（现行） | −1.19% | −1.82% | −2.86% | **+1.67** |
| gap% | −1.57% | −1.46% | −2.84% | +1.27 |
| \|14:30/今开−1\| | −1.55% | −1.99% | −2.34% | +0.80 |
| 14:30/今开−1（有符号） | −2.41% | −1.86% | −1.63% | −0.78（方向反直觉，弃） |
| 10:00→14:30 drift / \|drift\| | 平坦（+0.27/+0.11，弃） | | | |

按诊断只带两个变体进 walk-forward：`gap_asc`、`absrunup_asc`。

## 2. 三窗 twin（tot / sr / dd）

| 窗口 | 核心 | rank_amp（现 Live） | rank_gap | rank_absrun |
|------|------|--------------------|----------|-------------|
| OOS2 | +17.8/0.72/18.0 | **+94.1/2.94/16.1** | −2.2/0.08/23.7 | +72.6/2.31/18.7 |
| train | +40.7/3.01/8.4 | **+55.3/4.54/5.7** | +29.3/2.07/10.8 | +46.9/3.55/6.7 |
| valid | +139.1/3.37/11.9 | **+141.8/3.58/11.9** | +130.2/3.40/11.9 | +156.2/3.80/11.9 |

相对 base（现习惯）：

| 变体 | OOS2 tot/sr/dd | train | valid | 判定 |
|------|----------------|-------|-------|------|
| rank_gap | −96.3/−2.86/+7.6 | −26.0/−2.47/+5.1 | −11.6/−0.18/0 | **REJECT/total**（卫星腿 OOS2 −30.0/sr−0.74/dd45.0） |
| rank_absrun | −21.5/−0.63/+2.6 | −8.4/−0.99/+1.0 | +14.4/+0.22/0 | **REJECT/total**（OOS2 破 −5pt 线） |

## 3. 判定

- **rank_gap 永不重开**：最小缺口 = 最弱脉冲，卫星腿 OOS2 打到 −30%。诊断里 T1≈T2 的平坦段在组合里变成了深坑——无条件分档 ≠ 组合效应（槽位/strict/择时），再次证明必须走完整回放。
- **rank_absrun 是标准过拟合陷阱**：valid +14.4/sr+0.22 很好看，但 OOS2 −21.5。按纪律（选参窗定、验证窗只验）拒收，不进 Live。如实记录它的 vs 核心 profile（tot 三窗 +54.8/+6.2/+17.1，仅 OOS2 dd +0.7），供以后参考，不采纳。
- **H1 结论（negative result）**：诚实排名键打不过全天振幅。现习惯的 valid +2.7 缓冲里有一部分是排名前视，这是已知残差（Live 实际用盘中快照振幅排名，既非全天也非本刀两键）。不等回测修好：paper 已记每笔 `entryPxSrc`，攒够 20 笔用实盘价差实证排名口径差。
- **连带残差**：R-wide 闸也用全天收盘 breadth，记 H1-followup（动闸是另一个变量，另开一刀，不在本刀顺手改）。

复现：

```bash
cd services/data-sync-service
PYTHONPATH=src:scripts python3 scripts/diag_sat_rank_keys.py
PYTHONPATH=src:scripts python3 scripts/compare_sat_rank_hhmm.py --save-report
```
