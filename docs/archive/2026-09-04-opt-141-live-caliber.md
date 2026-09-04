# OPT-141 习惯口径成绩单冻结  · 归档于 2026-09-04

## 当时的目标（todo 链接）
- `docs/todo.md` P0-5（业务 6）→ `docs/optimization-checklist.md` OPT-141
- Live 习惯（same_1430/C1/exit1430）要有自己的三窗成绩单，不再借冻结 T 开盘的

## 实际做了什么
- 先核对现状：引擎**已有**全套习惯字段（fill_mode/fill_hhmm/exit_hhmm/
  max_open_to_1430_pct=C1），09-03 的 C1·14:30 行已是三窗全过核心——OPT-141
  的真缺口不是"把字段加进引擎"，而是**成交来源血统**（缺 14:30 bar 时回测
  在干什么，没人量化过）
- 引擎加 `entryPxSrc/exitPxSrc`（与 paper 的计数器同口径）：入场缺 bar 不买
  （保守）；出场缺 bar 回退收盘（**以前沉默**，现记 `exitPxSrc=close`）；
  replay summary 聚合 `fillSrc`；compare 脚本报告每窗带 fillSrc（+`--report-name`）
- 今日代码+数据重放三窗（`sat_live_caliber_2026-09-04.json`，冻结文件不动）
- 单测 2 例（bar 血统 / 收盘回退被记下来）；实验文档
  `docs/backtests/sat-live-caliber-2026-09-04.md` + SUMMARY 行

## 验证 / 数据
- 三窗 vs 核心：OOS2 **+76.3/+2.22/−1.9** · train **+14.5/+1.53/−2.7** ·
  valid **+2.7/+0.21/0** → **PASS+/beats_core**，与 09-03 逐数一致（成绩单未腐）
- fillSrc：入场 480/480 真 bar；出场 452/480 真 bar（OOS2 19、train 5、
  valid 4 笔收盘回退，~5%）。回退偏向**低估** 14:30 边（真 bar 已证 14:30
  卖优于收盘卖），不推翻判定
- 后端全量 **3686 passed / 0 failed**；lint 无新增；测试残留 0
- paper 侧血统计数器就绪但 twin_star 簿 0 行（OPT-138 修好日历后才开攒）——
  C4 20 笔后做第一次回测-paper 血统对账（P0-3 的事）

## 后续影响 / 留给谁
- 习惯成绩单冻结为 sat-live-caliber 表；冻结 T 开盘引擎仍是新参数对照基线
- 残差清单（不变）：全天振幅排名略乐观；出场 ~5% 收盘回退；valid 缓冲薄
- OPT-143 可引用本表的 fillSrc 方法论做 14:30 覆盖率表
