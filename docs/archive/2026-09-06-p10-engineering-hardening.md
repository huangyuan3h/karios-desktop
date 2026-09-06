# P0-10 工程坚实（H1–H6） · 归档于 2026-09-06

## 当时的目标（todo 链接）

- `docs/todo.md` P0-10：代码让 agent 一读就懂、一下手就不踩坑；测试盖住核心策略链；
  用户一眼确信线上跑的就是冻结配方，2 分钟知道今天跟什么。
- 基线：覆盖率 86.35%/门 85；核心链 engine 73 / paper_twin_star 67 / twin_star 73·64；
  OPT 未完成 125/126/145/146；CI 缺死链门；`coverage.json` 跟踪中常脏。

## 实际做了什么

| # | 动作 | 结果 |
|---|------|------|
| H1 | 核心链覆盖率补齐 | engine 99.6 / sleeve 98 / state 99 / paper 100 / jobs 100+98 / daily 100 / intraday 98；门 85→88；~380 新用例全 hermetic；余下 6 行证伪死代码（候选 OPT 清理） |
| H2 | 稳定性收尾 | OPT-125 池 2/20+三超时+取连接 1×重试+compose `/healthz` 健康门；OPT-126 三宿主 10min 探针+streak3 告警+横幅字段；OPT-146 港股 54 行重标+结算语义 §1.7；OPT-145 分钟对拍脚本固化；checklist 未完成清零（108 归档） |
| H3 | Agent 读写公约 | 13 文件 17 站点→pool；裸 `pro_api` 只剩 realtime_quote（tk.csv）+ bar_5min（全局 token）有据豁免；AGENTS +1 节（单池/契约/单例+reset/零网络 import） |
| H4 | 漂移护栏 | linkcheck（archive 豁免）+ CI 门，真抓 tv.py 死链 1 处；shared health.ts 契约+4 用例；横幅 quota/熔断行；test_health shape 断言 |
| H5 | 用户信任 | 后端跨层 attestation（TS 字面量=引擎常量）+ 版本常量 + 计划面板"clip4 v3.1"徽；paper_twin_star_recon 进 action brief（差异推高事件）；跟随页 reason/recipe 早已存在，补"凭什么信" |
| H6 | CI 锁门 | CI=lint+typecheck+test+linkcheck+build；coverage.json 去跟踪 |

## 验证 / 数据

- 后端全量 4178 passed + 3 skipped，89.56% ≥ 88；前端 879 passed；shared 78 passed。
- 途中抓到的真回归×2：池化丢隐式 commit（8 DB 测试红）；`SimpleNamespace(pro=pro)` 少 lambda 致 mock 假阳性。
- DB 纪律：54 行 UPDATE 重标外零写污染（`CN:99` 0 行）；requires_postgres 未动。

## 后续影响 / 留给谁

- OPT-127（P2 前端轮询）125 后已解锁，待唤醒。
- engine 6 行死分支（1646/1650/1687/2308/2710-2711）候选清理 OPT。
- 下一个产品面事项：paper C4 攒 20 笔（P0-3）、双子星 Watchlist 跑顺（P0-0/P0-4）。
