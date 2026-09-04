# 强股勺型做空 OOS 快照：方法缺失，不下结论（2026-09-04）

> **一句话**：形态目录 8 项中 7 项已 REJECT，唯一 ≥80%（强股勺型做空 83–92%）已落库为
> `factor_signals.strong_scoop_exhaustion`（方向判别层）。本想用 08-24~09-03 做 OOS 快照，
> 但破位入场规则的原始实现（morph 脚本）不在 repo，用 detection-close 代做空入场是稻草人
> （stop 中位仅 +0.4%，day-1 全止损）。**不编规则、不下结论**；另发现生产表只有 2025-06-16
> 一天 77 条 backfill、无每日调度，判别层实际 stale。
> **关键词**：勺型 耗尽顶 OOS 方法缺失

**规则**：只读 dry-run，未写 `factor_signals` 表；非证伪、非证实。

---

## 1. 核查（只读）

- 08-24~09-03 dry-run（与 `factor_signals_service.py:41` 同阈值）：172 signals，约 12–39/天。
- 错误审计（detection-close 做空，stop=前高×1.02，target=勺底×0.99）：122 止损优先 / 2 目标 / 48 未决。
- 入场距 stop 中位 **+0.4%**、距 target **−14.5%**，止损中位 day-1——价格本来就贴着旧高，
  该审计必全止损，与原研究“破位日放量派发时做空”不是同一入场，不可比。

## 2. 生产 gap（附带发现）

- `factor_signals` 表 77 行全是 2025-06-16 backfill；`scheduler/` 无每日扫描 job，
  只有 API 按需触发（`factor_routes.py:38`）。`strategy-params.md §7` 写的“日频扫描全市场，
  信号落库待接”实际没在跑。
- 后续（二选一，需拍板）：(a) 把 `scan_strong_scoop_exhaustion` 接入每日盘后调度，
  让判别层真正产信号（工程项）；(b) §2.8 头肩顶按独立协议验（新检测代码；鉴于 8 形态
  7 拒 1 受限，ROI 低，不建议优先）。

## 3. 判定

- **underpowered/方法缺失**：OOS 快照本次做不出有效读数；原 83–92% 结论维持（不推翻），
  也不视为新增确认。待破位规则实现落库或有 20 日完整前瞻窗口再重验。
