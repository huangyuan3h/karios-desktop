# opt-145-vendor-minute · 归档于 2026-09-06

> 从 `docs/optimization-checklist.md` 原文迁移（只读快照，不回写）。正文 0 条未完成（另有 3 条冬眠：OPT-045/075/127，见正文冬眠区）。

---

### OPT-145：外购分钟/复权数据入库（P1）—— A 形式化回填收尾

**状态**：[x] 2026-09-06（B/D 早 done；本批补 A 交接块"形式化脚本+单测"）

**落地**：
- 新 `scripts/compare_vendor_minute.py`（只读）：`data/2024_5min` vendor CSV
 （`parse_vendor_csv` + `LAST_HOUR_TIMES` 七根）vs baostock 全年重拉（adjustflag=3），
  按 `(ts,day,time)` 对 close；纯函数 `pick_sample/match_points/summarize/verdict` +
  CLI（`--seed/--sample/--year/--limit-codes/--save-report`）。双门同报：
  原字面门（七根 1 分钱 ≥99.5%）+ 修订门（1430/1500 + 滑点 base10bps/stress30bps）。
- `tests/test_compare_vendor_minute.py` 新 4 用例（join/吻合率/分位/抽样确定性/双门）。
- 实跑复现（`--limit-codes 600000.SH`）：总体 95.7%，1500→97.5%，与 ad-hoc
  40 只结论（90.59%/1500~98%，快照时差非错数）同分布——方法固化有效。

**验证**：4 passed（+ adj 2 passed）；脚本真实数据端到端跑通。
