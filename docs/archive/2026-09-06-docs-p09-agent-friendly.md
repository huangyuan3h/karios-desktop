# P0-9 文档轨（agent 友好 + 每文件一职责） · 归档于 2026-09-06

## 当时的目标（todo 链接）

- `docs/todo.md` P0-9：文档"找得到但不好读"——两大 checklist 只增不减、同一数字 5 处互抄、改策略路由 4 处各列一遍。
  目标：每文件一职责、都不太长；agent 按任务 3–5 个文件读完开工；普通人 2 分钟 follow。
- 纪律：不动 modules 正文；不动 archive/（只增）；一次一条。

## 实际做了什么

| # | 动作 | 结果 |
|---|------|------|
| D0 | backtests 按策略分文件夹 | `core/`(9) `sat/`(22) `s3/`(11) `factors/`(10) `hedge/`(1) `vendor/`(2)，根留 8 索引；`git mv` 55 个保历史；133 处链接重定向 |
| D1 | 拆 optimization-checklist | 3844→299 行；105 条按天归档（13 新档）；8 条未完成→045/075/127 进冬眠档（标注 open 可唤醒），正文留 5 条 + 冬眠区 + 39 行索引 |
| D2 | 拆 trading-improvement-checklist | 1024→115 行；8 个主题归档；TIP-014 状态正名（`[ ]`→`[x]`主体落地）；补 TIP-015 条目（决策 Agent 闭环 M1–M4） |
| D3 | 数字单一源 | 190.6 定数（报告 product 窗 core=190.6，Δ4.3 自洽；190.7 只剩冻结实验档+注释）；规则"一致+出处"：clip4→state-bucket §3.0，S-3 基线→strategy-params §3，三窗→backtests/README |
| D4 | 改策略路由统一 | AGENTS Strategy 节为主源（+自查步骤+调参查找表）；SUMMARY/READMEs/todo 改指过去 |
| D5 | 普通人一页纸 | todo §0 下 2 分钟版；§0/§1 去旧§号；"124–127"→"124–126（127 冬眠）" |

## 验证 / 数据

- 死链 0（docs 全库 + 仓库根，含 D0 漏检的 AGENTS 7 处补修）、孤儿 0。
- OPT 覆盖率：正文 109 历史编号 vs 索引程序化核对 100%；发现历史重号（057/058/060 双条目、059 档文不对题）与幽灵编号（077/101 从未立项），索引注明不重编号。
- 行数：optimization 3844→299、trading 1024→115。
- 未动：modules 正文（规则：动前 grep 代码核对，另批）、archive 历史快照（只增）。

## 后续影响 / 留给谁

- 工程轨（评估已出，待开）：E1 OPT-124 收盘链 token → E2 OPT-125 连接池 → E3 策略注册表 → E4 Zod↔Python 对齐 → E5 CI → E6 paper/live 双引擎。
- D1 验收曾写 <300：实收 299（模板折叠指 AGENTS 贡献最后 7 行）。
- D3 验收曾写"同一数字只剩 1 处"：执行为"一致+出处"（掏空 SUMMARY/pick-strong 表格会伤入口可读性）。
- TIP-014 状态由 `[ ]` 改 `[x]`：证据链在 D2 小结，用户可 veto。
- designs/ 待分类 20 项（designs/README 已全收录，状态未定）。
