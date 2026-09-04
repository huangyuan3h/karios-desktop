# OPT-142 日历收敛 + API 校验 + Alembic 卫生  · 归档于 2026-09-04

## 当时的目标（todo 链接）
- `docs/todo.md` P0-5（工程 6-7）→ `docs/optimization-checklist.md` OPT-142

## 实际做了什么
- 中央谓词 `trade_calendar_utils.is_non_trading_day`（日历优先，Mon–Fri 只当
  未播种 fallback；永不抛）。收敛 8 处：market_regime×2、market_sentiment、
  notifications×2（14:20 前置判断）、twin_star_intraday `in_live_tape_window`、
  health freshness 放宽、`scheduler/__init__` catchup 早退。保留且注释的：
  notifications 双 fallback 循环、_is_cn_session_day fail-open、周一算术×3、
  backtest_recon 找周五、日历实现内部——`weekday()` 只剩真值/算术处
- API 请求体（只做了有 JSON 体的 3 个文件；其余 12 个是 Query 触发器，无体可验）：
  industry_flow×2（days/topN 上限）、news sources 建/改（空名空 URL 200+error
  字符串 → 422）、decision sessions/消息（role 收紧 Literal 三值）
- 附带修真 bug：FE 建 decision session 发的 `systemPrompt/modelProfile`
  （camel）前后都被静默丢弃——SessionCreate 加别名接住（snake 照旧兼容）
- Alembic：0020 文件重号只是文件名问题（链本来线性，41 环节验证），
  `git mv → 0035b_cn_extra_data.py`（revision id 不变，已落库无感）+ 头注；
  删 `behavior_audit` pre-0040 过渡降级（dev 已升，机制归 migration）

## 验证 / 数据
- 后端全量 **3690 passed / 0 failed**；新测 4 例（is_non_trading_day 周末/
  假日/开盘日/永不抛）；ruff 无新增；残留 0
- `alembic history` 41 环节线性，`current` 仍 0040，baseline 测试绿

## 后续影响 / 留给谁
- 长假（≥3 天）freshness 放宽只给 48h——国庆级长假仍会部分狼叫，可接受，
  真要修得算连续非交易日 streak（没做）
- news 空体从 200+error 变 422：FE 正常路径不受影响（都带名+URL），只有
  非法输入行为变好
- 各环境跑过 0040 才能删降级——已删，如有未升库会直接报错（故意的，
  migration 才是机制）
