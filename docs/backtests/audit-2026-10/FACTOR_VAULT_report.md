# 因子冷库 Tab 交付报告（2026-10-03）

## 做了什么
在回测页加了一个新 tab，名字叫「因子冷库」，紧挨着「S-gap 失效趋势」。
后端加了只读接口 `GET /api/backtest/factor-vault`，数据是冻结文件
`services/data-sync-service/data/backtest_reports/factor_vault.json`
（加一个只追加的每日状态文件 `factor_vault_history.json`），
由新脚本 `scripts/generate_factor_vault.py` 从已有审计数据算出来：
S-gap 最近 40 笔用 `scratch/h2f_trades.json` 精确重算，
港湾股票腿最近 40 笔用 `scratch/h2d_blotter_long.json` 精确重算，
其余试点用审计冻结窗聚合（FINAL/H2/G/H2b/H2d/H2j/H2k 里写死的数字，
不跑新 DB 查询）。前端新组件 `FactorVaultPanel.tsx` 画一张统一体检表，
点每一行展开 OOS2/train/valid/holdout 时间轴分位/净收益图（复用 S-gap
tab 的 recharts 画法）。共享层加了 `factorVault` Zod 校验。
每天 19:10（周一到周五）有一个只读调度 `factor_vault_refresh` 重算两个
冻结文件（只写文件，不碰 DB，不改任何 live 参数、订单、券商逻辑），
失败就保留昨天的文件。没碰任何 Crimson 代码，星舰B还是 live 唯一基线。

27 个因子就是审计里所有曾经有效过的（FINAL 第 2 节总表全覆盖）：
星舰B族 5 个，小市值 3 个，事件/超卖/形态/技术 14 个，动量/择时 2 个，
再加还活着的 3 台发动机做参考行（港湾/母港M30/B腿）。

## 因子现状（统一规则算出来的，2026-10-03 生成）
一共 27 个：冷库 15，观察 11，复活 1（只有港湾参考行）。

冷库（15，跌破 50 分位或净小于等于 0，建议仓位都是 0%）：
星舰B默认（分位 2.2%，净 -14.79 点，40 笔）、H2-a25（无真分位，-15.46 点）、
纯B3组合（-14.85 点）、星舰H2 100%（-17.46 点）、容量版星舰50M（19.6%，-16.10 点）、
H11（0.0%，-78.40 %/笔，holdout binding 失败）、H08（-27.60）、H13（-0.80）、
H05（-54.00）、H07（-55.20）、H04r（-10.00）、H10（-30.00）、H19（-80.80）、
G事件POS（-160.40，holdout 0 笔所以用 valid 窗）、G择时MA60（-7.80，38 期）。

观察（11，不足 20 笔或分位 50 到 75，或有净收益但没有真随机分位所以按规则永不复活）：
H06（8 笔，-43.04，笔数不够）、H01（40 笔，+86.40，但无真分位且 valid 曾经显著为负）、
H02（+72.80，同上）、H03（+31.20，同上）、H09（+16.80，同上）、H15（2 期，+7.90）、
H16（2 期，+3.20）、G小市值（2 期，-5.40，笔数不够）、G动量（1 期，+11.20，噪声）、
母港M30参考（2 期，-8.80）、B腿参考（2 期，-0.20）。
H01/H02/H03/H09 这几个最近 40 笔看着是正的，但按 H3 的老规矩（要 holdout 收复加邻域稳定加真安慰剂
95%以上）一条都不满足，所以只给观察，不给复活，更不给仓位。

复活（1，只有参考行）：
港湾参考（分位 100.0%，净 +42.69 点，40 笔）。它是 valid 窗 99.8% 有 edge 的那条腿，
最近 40 笔确实强。但它 history 最后一格 holdout 是 0 笔（没信号），按两步确认规则
（要连续两格复活才给 10%，再两格才给 20%）确认数不够，所以徽章是复活绿灯，
建议仓位还是 0%，旁边写着 live 前置（paper 20 笔加 holdout 收复加过滤版 valid 转正）
没满，实盘仍是 0%。这正是冷库想要的：灯可以亮，钱不动，数据说了算。

统一规则（H2k 复活规则，代码里和界面上都写了同一段话）：
滚动最近 40 笔（月频是 40 期），看两个数：同期随机分位，和扣费后净收益。
冷库是分位小于 50 或净小于等于 0；观察是分位 50 到 75 或笔数不足 20；
复活要分位大于等于 75 并且净大于 0，另外还要连续两格确认才从 0% 加到 10%，
再确认才加到 20%，上限 20%，两步之间至少隔 20 笔；只要分位掉回 50 以下或碰到
熔断（60 天负 10%、单笔负 8%、连续 3 笔同日全亏或单周负 5%、数据链告警、
holdout 扩到负 25%）就一级一级降回 0%。没有跑过真同期随机的试点，分位显示横线，
永远不显示复活。卫星扣 32.28bp 每笔，转移和 B 腿扣 5bp 每边。

## 怎么打开看
1. `cd ~/Projects/karios-desktop && pnpm dev`（后端要根目录 `.env` 里有 `DATABASE_URL`，
   但这个 tab 本身不查 DB，没库也能看表）。
2. 浏览器开 `http://localhost:3000`，点左侧「回测」，再点 tab「因子冷库」。
3. 点任何一行展开它 OOS2/train/valid/holdout 四格的时间轴图（分位紫线加 50/75 线，
   净收益绿线加零线）。
4. 接口直查：`http://127.0.0.1:4330/api/backtest/factor-vault`。
5. 重算冻结文件：`cd services/data-sync-service && PYTHONPATH=src python3 scripts/generate_factor_vault.py`。
6. 静态预览图：`~/Projects/wealth-ideas/karios-audit-2026-10/factor_vault_preview.png`
  （同一 JSON 用 matplotlib 画的英文版表格）。

## 每天怎么更新
- 调度 `factor_vault_refresh`（周一到周五 19:10，亚洲/上海）：跑上面的生成脚本，
  只重写两个 JSON 文件，不写 DB，不调参，不下单，失败就留着昨天的文件。
  已在 `scheduler/__init__.py` 注册，和 rolling_oos 等别的任务走同一个注册表。
- 历史文件 `factor_vault_history.json` 每天追加一行 `{date, states}`，
  表里的「未变档天数」就是从它往回数的。今天是第一天，所以全是 0 天或按规则从 0 起算，
  明天开始就能看到谁变了。
- 手动补跑就用第 5 步那条单命令。

## profit-leads 规则执行
本轮没追加。按规则要「每笔扣费后为正、或事件/组合分位明显优于随机」才记：
港湾参考看着是 100% 分位，但它是已知 PASS 腿（H2d 已经判过，不是新 alpha，按 G/H2d
先例已知高收益不追加）；H01/H02/H03/H09 最近 40 笔虽为正，但 valid 曾经显著为负、
跨窗不一致，且都没有真同期随机分位（按 H2/H3 老规矩三条件一条都不满足）；
G 动量 holdout 只有 1 笔、H15/H16 只有 2 笔，都是噪声；容量版星舰 valid/holdout
双负。没有出现新的跨窗一致加 holdout 不差加真分位 95% 以上的指标，所以延续不追加。

## 测试结果
- 后端新测试 `test_factor_vault.py`：8 通过（含规则边界、无真分位永不复活、两步确认、
  校准、27 行全家族覆盖、历史天数、接口 404/坏文件/正常加历史合并）。
- 后端相关（factor_vault + sgap + backtest 路由 + combo_tiers + catalog）：86 通过。
- 后端调度接线（scheduler jobs extra + cron weekdays）：116 通过，1 跳过
  （修过一次自家 cron 用了数字 1-5，改成 mon-fri 后通过；期望集合里加了 factor_vault_refresh）。
- 共享层：95 通过（含新增 factorVault 2 个），`pnpm -C packages/shared build` 通过。
- 前端相关（vault 面板 5 个 + sgap 面板 4 个 + 回测查询，含新增 vault 查询 2 个）：23 通过。
- 前端全量：942 通过，2 个 watchlist（PortfolioHealthCard、TodayTodoCard）失败、
  1 跳过——干净 HEAD 上也是同样 2 个失败，和本次改动无关。
- 前端 typecheck：通过。前端 build：通过。

## 分支
`feat/sgap-decay-tab`，提交 a19ae8ae。没 push，没 merge。

KARIOS FACTOR VAULT DONE
