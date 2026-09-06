# opt-124-tushare-pool · 归档于 2026-09-06

> 从 `docs/optimization-checklist.md` 原文迁移（只读快照，不回写）。正文只留 4 条未完成 OPT（另有 3 条冬眠：OPT-045/075/127，见正文冬眠区）。

---

### OPT-124：Tushare 多 token 轮换 + 配额看门狗（P0）

**状态**：[x] 2026-09-06
**优先级**：P0（单 key 是 EOD 链唯一付费 SPOF；周五 200/min 已实证）
**关联**：`designs/stability-audit-2026-09-01.md` §4/§6

**落地**（与计划的 3 处差异）：
- 新 `clients/tushare_pool.py`（不在 `service/` 下，避免 service 层循环引用）：`TusharePool` round-robin + 每 key 60s 滑动窗口（默认 200/min，`hk_daily` 1/min 带 60s 间隔）+ 每 key 冷却 + `index_global` 100/day 计数（耗尽抛 `DailyQuotaExhausted` 供调用方降级 Tencent/ak，不烧重试）。
- 限流信号命中切 key 即时重试；仅当所有 key 都 hot 才 `sleep 35s`（cn_extra_sync 惯用法）后开新一轮，最多 2 轮后原错上抛（调用方自有外层 retry）。
- `config.py` 加 `tushare_tokens`：`TUSHARE_TOKEN="k1,k2"` 逗号分隔优先，缺省回退单 `TU_SHARE_API_KEY`（1-key 池，行为与旧单 key 一致）；`tu_share_api_key` 字段保留（范围外 service 仍用）。
- 5 个 service 改走 `get_pool().pro()`（`ts.pro_api` 直调清零，import 已删）：close_sync / etf_daily（3 处）/ macro_daily（`_tushare_pro`+`try_tushare_pro`，market_sentiment/macro_snapshot_on_demand 等范围外调用方零改动）/ hk_daily / adj_factor；缺 key 文案保持 `"TU_SHARE_API_KEY is not set"`（旧测试/前端匹配不断）。
- `GET /api/health/datasources` 加 `tushare_quota`（key 数/rotation 数/每 key minuteUsed+cooling/daily 用量，后缀只露末 4 位；best-effort 永不炸 endpoint）。
- 8 个旧测试文件（close_sync×2 / adj_factor / etf_daily / hk_daily / macro_daily×2 / multi_asset_signal_freshness）`mod.ts` patch 改为 `mod.get_pool` patch + fake settings 补 `tushare_tokens`。
- 未做（留给 E4/后续）：范围外 15+ 处 `ts.pro_api`（daily/index_basic/realtime_quote 等）仍直调，未收敛——反模式注明。

**验证**：`tests/test_tushare_pool.py` 新 21 用例（轮换/切 key 无 sleep/全 hot 睡 35/两轮上抛/201 次退避/跨 key 容量优先/hk 60s 间隔/100day 耗尽+隔日重置/快照脱敏/配置三态/服务 guard/健康字段）；全量 `3755 passed + 3 skipped`，覆盖率 86.35%（门 85%），`tushare_pool.py` 96% / `config.py` 98%，ruff 全过。
