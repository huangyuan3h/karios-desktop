# opt-160-artifact-untrack · 归档于 2026-09-12

> 从 `docs/optimization-checklist.md` 原文迁移（只读快照，不回写）。

---

### OPT-160：运行产物去跟踪分类治理

**状态**：[x] 2026-09-12

**背景**（2026-09-11 工程盘点）：仓库里混入了大量**运行期产物**被 git 跟踪——
`services/.../data` 下 `*_latest.json`(38)/`timeline_cache/`(42)/`twin_star_intraday/`(9)/
`third_asset_cache.json`(1) 共 90，加 `apps/ai-service/coverage/`(12)+`packages/shared/coverage/`(9)+
`coverage_tmp.json`(1)。`git status` 长期脏、diff 噪音、易误提交 run 输出。

**分类原则**：**frozen truth 保留**（`walk_forward_baseline*.json`、`*_frozen_*.json`、
`state_sliced_navs/`、`data/index/*.zip`），**可再生输出/缓存去跟踪**。

| 处理 | 对象 | 再生来源 |
|------|------|----------|
| 去跟踪 | `data/backtest_reports/*_latest.json`（38） | 各 `scripts/*`（`run_walk_forward.py` 等） |
| 去跟踪 | `data/backtest_reports/timeline_cache/`（42） | `api/backtest_routes.py`（磁盘缓存） |
| 去跟踪 | `data/twin_star_intraday/*.json`（9） | `scheduler/twin_star_intraday_job.py` |
| 去跟踪 | `data/third_asset_cache.json` | `scripts/sleeve_nav_sim.py` / `run_walk_forward_dual.py` |
| 去跟踪 | `apps/ai-service/coverage/`、`packages/shared/coverage/`、`coverage_tmp.json` | pytest/vitest coverage |

**实际做了什么**：
1. `git rm --cached` 111 个文件（**本地文件保留**，只停止跟踪）。
2. 根 `.gitignore` 加 `coverage_tmp.json` + 4 条运行产物规则（`*_latest.json`/`timeline_cache/`/
   `twin_star_intraday/`/`third_asset_cache.json`）；`coverage/` 已有规则顺带覆盖两个 coverage 目录。
3. `services/data-sync-service/README.md` 新增 **Runtime artifacts (OPT-160)** 节，列明各产物再生来源 +
   frozen truth 白名单。

**验证 / 数据**：
- `git rm --cached` 后 `git diff --cached --diff-filter=D` = **111**；本地文件 `ls` 仍在。
- `git check-ignore -v`：所有样例命中新规则（`.gitignore:29/31/25/10`）。
- 报告加载相关测试 **57 passed**（`test_backtest_routes_extra` / `test_backtest_routes_contract` /
  `test_notifications` / `test_portfolio_nav_sim` / `test_health`，均用 `tmp_path`，不依赖被去跟踪文件）。

**后续影响 / 留给谁**：
- 提交在 **OPT-162**（工作区 WIP 切分提交）；本项只改索引与 `.gitignore`。
- 注意：`walk_forward_baseline_20260911.json`（OPT-157 qfq 重建后的新冻结基线）**尚未跟踪**，
  属 frozen truth，应随 OPT-162 一并提交。
- `docs/` 4 条死链不属本项，见 OPT-161。
