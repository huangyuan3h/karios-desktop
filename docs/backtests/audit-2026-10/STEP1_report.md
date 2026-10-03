# Karios 重整 第1步报告：新组合层（算档库）—— STEP1_report (2026-10-03)

> 范围：只做第1步「只加不删」。不改前端（第2步再做），不碰下单/券商逻辑，不改任何策略参数，不删文件，不 push，不合 main。
> 纪律：永不用 Crimson 代码或数据；未用 read 打开 .png/.jpg；只操作自己的进程/分支。

## 0. 分支与基线

- 新分支：`refactor/portfolio-layer-step1`（从 `fix/data-sync-2026-10 @ e9305ec3` 切出）
- 切分支前：`git stash push -u -m "pre-step1 stash 2026-10-03"`，将 `fix/data-sync-2026-10` 工作区脏改动（含他人未提交改动与 untracked）全部暂存，不带入新分支。新分支起点干净，`git status` 为 clean（除本步新增文件外）。
- 基线 commit：`e9305ec3 fix(data-sync): reliable sync — margin completeness guard, hsgt turnover label, ETF snapshot automation, watchlist 20:30 retry, health check`
- 前置已读：`RESTRUCTURE_plan.md`（第1步定义 PR1）、`H2j_capital_tiers.md`（档位/阈值/熔断）、`H2k_sgap_revival.md`（复活规则上线前沿用旧三条件）、`DATAFIX_report.md`。
- Yuan 2026-10-03 拍板（写死实现）：星舰B live 唯一基线不动；默认 A25；星舰上限 20%；熔断 60天 −15% 降一档（单笔 −8% 暂停、周 −5% 减半等不动）；星舰前置未满 0% 回填；切档 155/145/210/180 万下月首交易日执行。

## 1. 改了哪些文件（只加不删 + 1处加线 wiring）

新增（5个）：

1. `services/data-sync-service/src/data_sync_service/service/combo_tiers_config.py` — 冻结配置（唯一参数源）
   - 默认总资产 120万（`DEFAULT_TOTAL_ASSETS`，PROMPT §3；env `KARIOS_TOTAL_ASSETS` 可覆盖）
   - 三档固定比例：A `40%港湾+20%M30+20%B3+20%容量星舰`（H2j §2 A25 + RST §2.3 + PROMPT）；B `50%+30%+20%现金`（H2j §2 B0，星舰0%）；C `20%+60%+10%B腿/货基+10%容量星舰`（H2j §2 C200）；CASH `100%现金`（RST §2.3 熔断终端）
   - 星舰上限 20%（PROMPT + H2j §4 + RST §2.6）
   - 切档阈值 155/145/210/180万 + 缓冲带 145–155/180–210 + 下月首交易日（H2j §3 + RST §2.3 + PROMPT）
   - 熔断 −15%/−8%/−5%/−25%（H2j §3 + RST §2.3/§3 + FINAL §0.6）
   - 星舰旧三条件 paper20 + holdout收至−10%内 + 过滤valid>0（FINAL §0.6.5/G §4/H2b §2.7 + RST §2.6 + H2k §4）
   - 成本备注 5bp/15bp/32.28bp+sqrt150bp（H2j §2 + RST §2.3）；单票上限 A2.5%/B5%/C2.5%（H2j §2）
   - 星舰配方备注 S-gap>3%+amp前1/3+C1+body=3+50M+4槽（RECIPES 52-64，仅备注不改参）
   - 全部 `Final` + `frozen dataclass`，每项带中文出处注释
2. `services/data-sync-service/src/data_sync_service/service/combo_tiers.py` — 纯函数组合层（无DB/无网络/无时钟/无下单）
   - `resolve_asset_tier(total, current)`：缓冲 + 每次只切一档（A↔B↔C，A不直跳C）
   - `check_fuse(nav_60d)`：`last/max−1` 60天口径 −15% 降档；单日 −8% `pause_new`；单周（近5个交易日）−5% `halve`；空/短序列无熔断（fail-open 规划态）
   - `parse_starship_status`：bool 或三条件 mapping，默认 False（fail-closed 0%）
   - `apply_starship_gate`：未满则星舰 0% 并按原比例回填底仓
   - `plan_tier(total, current, nav, starship)`：资产档 → 熔断降档（A→B→C→CASH）→ 星舰门 → 权重/金额/再平衡单/单票上限/下次规则/成本备注；星舰超 20% 强制回剪
3. `services/data-sync-service/src/data_sync_service/api/portfolio_routes.py` — 只读 API（GET）
4. `services/data-sync-service/scripts/portfolio_tier_plan.py` — 只读 CLI（JSON 输出）
5. `services/data-sync-service/tests/test_combo_tiers.py` — 27 个纯单元测试（无DB/无网络）

修改（1处加线，无删改）：

- `services/data-sync-service/src/data_sync_service/main.py`：+4行（import + `include_router(portfolio_router)`，RESTRUCTURE PR1 标注）。未动任何前端、broker/下单、策略参数文件。

未删除任何文件。未 push，未合 main。

## 2. 测试结果（贴结果）

命令（后端快速单测口径，`--no-cov` 跳过 88% 覆盖率门）：

```bash
cd services/data-sync-service
PYTHONPATH=src .venv/bin/python -m pytest tests/test_combo_tiers.py --no-cov -q
```

结果：

```text
27 passed in 0.11s
```

覆盖契约（PROMPT §4 全覆盖）：阈值边界 155/145/210/180、缓冲带保持、不来回切（含A不直跳C）、熔断降档 A→B→C→CASH、−14.9% 不降/−15.1% 降、单日 −8% pause、单周 −5% halve、空NAV不熔断、星舰就绪保持/未满回填（A 50/25/25、C 22.22/66.67/11.11）、三条件门、B档不受门影响、各档（含门后）权重合计 100%、星舰 ≤20%、金额=权重×总资产、再平衡单覆盖五腿、非法输入抛错。

回归（确认未碰坏现有链路）：

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/test_combo_tiers.py tests/test_allocation_sleeve.py tests/test_api.py --no-cov -q
```

```text
49 passed in 83.27s (0:01:23)
```

Ruff：`ruff check --fix` + `ruff format` 已作用于 4 个新文件；`test_combo_tiers.py` 27 passed 保持。

## 3. API/CLI 用法

### 只读 API：`GET /portfolio/tier-plan`

```bash
# 默认（无总资产来源 → 120万 default，当前档默认 A，星舰默认 0% fail-closed）
curl 'http://127.0.0.1:8000/portfolio/tier-plan'

# 指定资产与当前档
curl 'http://127.0.0.1:8000/portfolio/tier-plan?total_assets=1600000&current_tier=A'

# 60天净值熔断检查（逗号分隔，老→新，T-1 口径）
curl 'http://127.0.0.1:8000/portfolio/tier-plan?total_assets=1200000&current_tier=A&nav_60d=1.0,0.99,0.85'

# 星舰前置：直接就绪 或 三条件
curl '.../portfolio/tier-plan?total_assets=1200000&starship_ready=true'
curl '.../portfolio/tier-plan?total_assets=1200000&paper20_pass=true&holdout_recovered=true&filtered_valid_positive=true'
```

- `total_assets` 省略时来源链：`?total_assets=` → `$KARIOS_TOTAL_ASSETS` → 冻结默认 120万；回包 `total_assets_source` 标明 `query/env/default`。
- `current_tier` 省略默认 `A`（默认进攻档 A25）；`B/C` 按 H2j。
- `nav_60d` 省略则不判熔断（`n=0`）；传了才判 60天/单日/单周三旗。
- `starship_ready` 显式优先；否则三条件 AND；全省略默认 `false`（当前 paper ~4/20 未满，保持 0%）。
- 回包：`current_tier/asset_tier/target_tier/weights/amounts/fuse/starship/single_ticket_cap/rebalance_orders/next_rebalance_rule/cost_notes + disclaimer`（星舰B live 基线声明，只读）。

### CLI：`scripts/portfolio_tier_plan.py`（只读，无DB/无网络）

```bash
cd services/data-sync-service
PYTHONPATH=src .venv/bin/python scripts/portfolio_tier_plan.py --total-assets 1200000 --current-tier A --pretty
PYTHONPATH=src .venv/bin/python scripts/portfolio_tier_plan.py --total-assets 1600000 --current-tier A --pretty
PYTHONPATH=src .venv/bin/python scripts/portfolio_tier_plan.py --total-assets 2200000 --current-tier B --pretty
# 星舰就绪版
PYTHONPATH=src .venv/bin/python scripts/portfolio_tier_plan.py --total-assets 1200000 --starship-ready --pretty
# 三条件版
PYTHONPATH=src .venv/bin/python scripts/portfolio_tier_plan.py --total-assets 1200000 --paper20-pass --holdout-recovered --filtered-valid-positive --pretty
# NAV 熔断版
PYTHONPATH=src .venv/bin/python scripts/portfolio_tier_plan.py --total-assets 1200000 --nav "1.0,0.99,0.84"
# env 默认资产版
KARIOS_TOTAL_ASSETS=1300000 PYTHONPATH=src .venv/bin/python scripts/portfolio_tier_plan.py --current-tier A
```

## 4. 示例输出（120万 / 160万 / 220万，前置默认未满 0% 回填）

> 以下为 `plan_tier(..., starship=False)` 纯函数输出摘要（`weights` 6位小数，`amounts` 元）。

### 120万 current A → asset A → target A（星舰 20%→0% 回填 50/25/25）

```text
weights: harbor 0.5, m30 0.25, b3 0.25, cash 0.0, starship 0.0
amounts: harbor 600000.0, m30 300000.0, b3 300000.0, cash 0.0, starship 0.0
starship: ready False, requested 0.2, effective 0.0, refilled True, cap 0.2
single_ticket_cap: 0.025
next: month-end total-assets judgement, next-month first trading day
```

CLI：`--total-assets 1200000 --current-tier A`
星舰就绪对照（`--starship-ready`）：`harbor 0.4 / m30 0.2 / b3 0.2 / starship 0.2` → `480000 / 240000 / 240000 / 240000`。

### 160万 current A → asset B → target B（星舰本来 0%，不受门影响）

```text
weights: harbor 0.5, m30 0.3, b3 0.0, starship 0.0, cash 0.2
amounts: harbor 800000.0, m30 480000.0, b3 0.0, starship 0.0, cash 320000.0
starship: ready False, requested 0.0, effective 0.0, refilled False
single_ticket_cap: 0.05
```

CLI：`--total-assets 1600000 --current-tier A`（≥155万故 A→B，B3/货基在现金内月调）。

### 220万 current B → asset C → target C（星舰 10%→0% 回填 22.22/66.67/11.11）

```text
weights: harbor 0.222222, m30 0.666667, b3 0.111111, cash 0.0, starship 0.0
amounts: harbor 488888.89, m30 1466666.67, b3 244444.44, cash 0.0, starship 0.0
starship: ready False, requested 0.1, effective 0.0, refilled True
single_ticket_cap: 0.025
```

CLI：`--total-assets 2200000 --current-tier B`（≥210万故 B→C）。
星舰就绪对照：`harbor 0.2 / m30 0.6 / b3 0.1 / starship 0.1` → `440000 / 1320000 / 220000 / 220000`。

熔断示例（`nav=[1.0]*59+[0.849]`，−15.1%）：`120万 A → target B`（`fuse_downgraded True`）；`220万 C → target CASH`（全现金）。

## 5. Commit（未 push，未合 main）

- commit：`59700298 feat(portfolio): RESTRUCTURE PR1 combo-tier layer (frozen config + pure plan + read-only API/CLI + tests)`（分支 `refactor/portfolio-layer-step1`，基线 `e9305ec3`）
- 内容：`main.py (+4)` + 5 个新增文件（见 §1），共 6 files changed, 906 insertions(+)。
- 验证：`git status` 干净（仅本步提交）；`git log --oneline -3` 为 `59700298 / e9305ec3 / 15b95e9d`；未 push，未合 main。

## 6. 下一步建议（第2步及以后，不在本步做）

1. PR2 主页面 Today 卡（只加）：在 Dashboard 叠 `HomeToday`，读本步 `plan_tier` + 现有 `strategy_today/catalog/watchlist-automation/health_check`，不新建调度；`allocation_decide` 只读调用本步纯函数。默认 110–120万显示 A25 + vs星舰B与vs随机双列 + valid/holdout 并列。
2. H2k 到来即替换星舰门：本步 `parse_starship_status` 已隔离三条件，H2k N40/X75（0%→10%→20% 两步、降回 <50%、双确认、20笔间隔）可直接在门后加仓位步进，不动阈值与回填逻辑。
3. 真实总资产来源：当前 `query → env → 120万 default`；如需接券商/组合NAV，以只读方式实现 `resolve_total_assets` 的 env 上游（绝不碰下单链），并在回包保留 `source`。
4. 下月首交易日历：当前 `next_rebalance_rule` 为规则字符串；PR2 可接 `trade_calendar` 只读查询算出具体日期（本步保持纯函数无时钟）。
5. 全量后端 + 前端 CI：合 main 前跑一次全量 `pytest`（重型回测 >5min）与 `vitest + typecheck`（本机未跑，改动为后端只加，前端零改）。

## 7. 合规声明

- 未改前端、未碰 `broker`/下单/券商连接、未改任何策略参数（星舰B配方/执行链原样保留，仅算权重）、未删文件、未 push、未合 main。
- 永不用 Crimson 代码或数据；未用 read 打开 `.png/.jpg`；未杀他人进程（pytest 尾部 pool 线程警告为测试框架自有线程，非 kill 操作）。
- 赚钱线索：本步为组合层脚手架，未跑任何新策略回测，无新增 profit-leads（按 H2j/H2k 规则不追加）。
