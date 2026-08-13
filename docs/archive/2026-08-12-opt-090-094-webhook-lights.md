# OPT-090~094：webhook 推送 + 一键启动 + 红绿灯回测定案  · 归档于 2026-08-12

## 当时的目标（todo 链接）
- todo §4 API 开放 · §14 #3：决策/告警 webhook 事件订阅（设计稿拍板后实现）
- todo §0 工程：一键启动统一（用户："所有启动都来自 npm run dev"）
- todo §3 收益：红绿灯仓位启发式与回测的一致性验证（用户指令："不符合就替换或删除"）

## 实际做了什么
1. **OPT-090 webhook P1**（设计稿 §7 拍板：两者都要 / E3 1 小时一轮 / 先 API / E5 评估后做）：
   三层表（events 幂等 dedupe_key / subscriptions url+HMAC secret+event_types[] /
   deliveries pending→sent·failed×3→dead，5/15/60 分钟退避）+ `/api/webhook/*` CRUD +
   投递器（HMAC-SHA256 `X-Karios-Signature`、5s 超时、30 条/订阅/分钟限频、每分钟 job）+
   E1 `job_failed` + E3 `intraday_drawdown`（10-14 点整点巡检 open paper 仓 ≤-8%）；
   alembic 0030；cookbook §9（订阅 curl + Python 接收端签名校验示例）
2. **OPT-091 webhook P2 + 稳定性审计**：E2 paper 链断链 / E4 接近止损（brief 组装时）/
   E5 候选新增 diff（17:35 job，只推新增——消失=闸门关闭属正常）/ E6 OOS warning /
   E7 对账缺票；前端 WebhookPage（订阅列表/创建/删除/test）。审计：核心链全绿、
   备份正常、测试零残留（清理 2 行 + baseline 重存）、alembic head=0030
3. **OPT-092 一键启动**：`data-sync-service` dev script 去掉 `--reload`（08-11 misfire
   循环根因）+ 新增 `dev:reload`（4331 备用）；验证 `npm run dev` 一键三服务
   （3000/4310/4330）；清理手动前台 uvicorn
4. **OPT-093 红绿灯回测**：`scripts/backtest_index_lights.py`（逐日 as-of 回放 +
   S-3 同窗模拟，1196 CN + 599 HK 笔按入场日灯分层）。**CN 红灯日显著差**
   （胜率 27% vs 42%、中位 -5.5%）→ 定义正确保留；**HK 无区分度**（红/黄/绿中位
   -5.0/-2.0/-5.1%，红灯均值 +18% 系右上尾暴利单假象）→ **删除 HK positionRangeHint**
5. **OPT-094 CN 红灯日禁开仓定案**：反事实三窗（剔除红灯：OOS2 胜率 48→54%、valid
   61→79% 收益 +10%、无窗变差）+ walk-forward（OOS2 +1.0pt / valid +10.7pt 回撤
   11.8→1.5%）→ 引擎 `light_red_block`（默认关）+ live `S3_LIGHT_RED_BLOCK=True`
   （红灯日候选=0，无推荐）+ 前端 A 股闸门红标「红灯日 · 禁开新仓」（HK 不标）

## 验证 / 数据
- 后端 3349 passed · 前端 746 passed · shared 64 · ruff 干净
- webhook E2E 实证：本地接收器收到事件且 HMAC 签名校验通过（sig_ok=True）
- 红灯实证：2026-06-01（红灯日）候选=0；今天正常
- 服务统一 `npm run dev` 管理，改 Python 需重启（调度器稳定优先）

## 后续影响 / 留给谁
- webhook P2 已全：E1~E7 事件源 + 前端页；等待用户创建实际订阅（cookbook §9）
- HK 红绿灯维持"不区分"口径（严禁按均值反转——过拟合右上尾）
- 红绿灯定案已进 S-3：CN 红灯禁开（回测与 live 同码）；HK 不受影响
