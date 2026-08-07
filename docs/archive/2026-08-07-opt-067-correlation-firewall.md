# OPT-067：组合相关性防火墙（todo §16 L3-P5 · V7.0-01 转正）

> **完成日期**：2026-08-07
> **目标**：L3-P5「组合风控」——V7.0-01 跨资产相关性热力网从「暂缓」转正：堵住「恒生科技 ETF + 腾讯 + 通信 ETF 三种资产同一条 Beta」的隐形集中度。

## 方案（V7.0-01 设计原文落地）

**混合法：语义因子簇为主 + 经验相关性为辅**，不裸用 20 日纯统计相关性：

| 层 | 实现 |
|----|------|
| **语义因子簇（主）** | `service/correlation.py::cluster_for_symbol`：ETF → 跟踪指数桶（代码前缀映射：513180/159740→港股科技、512480→半导体、516880→通信…）；HK → 科技股清单（腾讯/阿里/小米/美团…）；CN → 东财行业子串规则（半导体/通信/有色/白酒/银行…）→ 9 个簇 + other |
| **经验相关性（辅）** | 20 日收益率 Pearson（daily 表 union 日历对齐）；<15 对齐样本 fail-open 回语义层；r>0.75 对输出 topPairs |
| **硬约束** | 语义簇暴露 **>30%** → 簇内新 BUY/ADD 下发 `CORRELATION_CAP_BLOCK`（只拦新开仓，不强制平仓）；Suggest% 经 roomCorrelation 进 min 链（绑定约束 note='correlation'） |

## 实测（当前真实组合）

- **tech_hk 簇 34.2%（腾讯 6.3% + 恒生科技 ETF 27.9%）> 30% → overLimit，新开仓被拦**——精确复现 V7.0-01 描述场景
- 经验相关：**HK:00700 × ETF:513180 r=0.926**（腾讯与恒生科技 ETF 高度共振，防火墙有效）
- 亿联网络 → tech_comm（通信），紫金矿业 → metal（有色），均未超限

## 交付物

| 件 | 说明 |
|----|------|
| `service/correlation.py`（**新**） | 簇定义 / cluster_for_symbol / cluster_exposure / blocked_clusters / evaluate_correlation_cap / correlation_matrix（日历对齐）/ _pearson |
| API | `GET /api/backtest/correlation-status?include_matrix=`（当前持仓簇暴露 + 超限 + blockedSymbols + topPairs） |
| FE 纯函数 | `execution-action.ts`：`isCorrelationClusterBlocked`（≥30% 拦）+ `suggestFireSizePct` 加 `roomCorrelation` 进 min 链（note='correlation'）+ `evaluateNewEntryGates` 支持 `clusterExposurePct` → `CORRELATION_CAP_BLOCK` + `deriveActionCard` 透传 |
| FE 展示 | **回测页新增「组合相关性防火墙」面板**（簇占比卡片 + 超限红色标记 + 高相关对 + fail-open 提示）；WatchlistTable 拉取 correlation status，每行传 clusterExposurePct |
| FE query | `useCorrelationStatusQuery` + `clusterExposureForSymbol` |

## 验证

- 后端 1388 passed / 2 skipped（唯一失败为既有 flaky）；前端 500 passed；tsc 干净
- 新测试：后端 `test_correlation.py` 9 个（簇映射 / 暴露聚合 / 30% 边界 / Pearson / 日历对齐辅助）；前端 5 个（blocked 判定 / CORRELATION_CAP_BLOCK / min 链 note / 默认不绑定）

## 反模式确认（未做）

- ❌ 未用纯统计相关性作唯一依据（日历错位/崩盘期滞后——语义层为主）
- ❌ 未强制卖出（只拦新开仓；存量持仓保留）
- ❌ 未把 other 簇纳入 cap（无语义信息不误伤）
- ❌ 未做相关性矩阵全量 UI（只展示 topPairs + 簇占比；全矩阵留待需要时）

## L3 里程碑至此全部完成（P1~P5 ✓）
