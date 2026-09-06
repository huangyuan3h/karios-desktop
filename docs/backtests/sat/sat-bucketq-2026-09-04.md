# 卫星习惯 bucket_q H3：top-1/2 vs top-1/3（2026-09-04）

> **一句话**：桶放宽到 1/2 不赚钱——选参窗 tot/sr 全弱于 1/3（OOS2 −1.4、train −2.3/sr−0.41），valid 完全一样。**Live 保持 top-1/3**，4 槽不动。
> **关键词**：习惯双子星 bucket_q 跟随性约束

**脚本**：`services/data-sync-service/scripts/compare_sat_bucketq.py`
**原始表**：`data/backtest_reports/sat_bucketq_2026-09-04.json`
**口径**：window-local 空簿、clip4、C1 3%、same_1430、body=3、第 3 日 14:30 卖、全天振幅排名、核心择强 trail8、`opp_50`。只改 bucket_q（strict 4 槽不变——跟随性约束，不动槽位）。选参只看 OOS2+train，valid 只验。

---

## 1. 三窗 twin（tot / sr / dd）

| 窗口 | 核心 | bq2（top-1/2） | bq3（Live，top-1/3） |
|------|------|---------------|---------------------|
| OOS2 | +17.8/0.72/18.0 | +92.7/2.84/16.6 | **+94.1/2.94/16.1** |
| train | +40.7/3.01/8.4 | +53.0/4.13/5.7 | **+55.3/4.54/5.7** |
| valid | +139.1/3.37/11.9 | +141.8/3.58/11.9 | **+141.8/3.58/11.9** |

bq2 vs base：tot −1.4/−2.3/+0.0，sr −0.10/−0.41/+0.00，dd +0.5/0/0 → PASS/worse_sharpe+worse_dd；vs 核心仍 beats_core，但选参窗被 bq3 全包。fills 几乎不变（槽位才是约束）。

## 2. 判定

- **bucket_q=3 维持**：放宽桶边际进来的是更差的缺口票（H1 gap 结论一致：弱脉冲不赚钱），strict 4 槽下多不出机会，只多出噪音。valid 两边分毫不差，进一步说明桶不是 valid 的约束。
- R11 在冻结 T 开盘口径下 S-gap 最优 bq3/15 槽；本刀在习惯口径 + 4 槽下复核：bq3 仍优。两边一致，参数可信。
- H4（R-wide 闸重验）是最后一刀，优先级最低。

复现：

```bash
cd services/data-sync-service
PYTHONPATH=src:scripts python3 scripts/compare_sat_bucketq.py --save-report
```
