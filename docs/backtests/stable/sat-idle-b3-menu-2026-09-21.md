# 星舰停车资产 B3 菜单扩展（H-SAT-IDLE-B3-MENU）· 2026-09-21 · **REJECT**

> **何时看**：想知道「给 B3 风险预算菜单补 OIL/恒生科技/有色 能不能提升星舰停车腿收益」时。
> **何时不看**：Live 操作（Live = 港湾）。
> **一句话**：冻结裁决 **REJECT**——补 OIL 让 B3 **全面变差**（OOS2 Δ **−7.3**、train −3.3、valid −0.5、
> long −1.1）；B3(6+OIL) standalone long +45.6/−5.3/1.60 vs B3(5) **+46.2/−4.7/1.76**。
> **5 资产 B3 菜单已是平台，补商品/港股/行业腿只会稀释。**
> **关键词**：星舰 · 停车资产 · B3 · 资产菜单 · 动态宇宙 · REJECT

> 预注册：[`sat-idle-b3-menu-prereg-2026-09-21.md`](../../designs/sat-idle-b3-menu-prereg-2026-09-21.md)（跑前冻结）
> 脚本 `scripts/eval_sat_idle_b3_menu.py`（只读）· 报告 `data/backtest_reports/sat_idle_b3_menu_2026-09-21.json`
> **Live 不动**；星舰前置不变。

---

## 1. 口径

停车资产 = **B3 风险预算 + 因果动态宇宙**（资产在有 ≥60 个交易日历史后才入池；OIL 2023-11-28 才上市，
故长窗早段菜单重归一化——不是调参）。权重 ∝ 1/σ、月频、5bp/边、全因果。卫星腿冻结（同 A4 口径）。

## 2. 结果（long；Δ vs A4_true = B3 5 资产）

| 臂 | 菜单 | B3 腿 standalone（long） | 星舰组合（long） | Δtot vs A4 |
|---|---|---|---|---|
| A4_true（对照） | {300,500,金,纳,债} | +46.2 / 8.2 / −4.7 / 1.76 | **+654.3 / 52.1 / −6.8 / 3.67** | — |
| **A7_true（主）** | +OIL | +45.6 / 8.1 / −5.3 / 1.60 | +653.2 / 52.0 / −6.8 / 3.62 | **−1.1** |
| A8_true | +OIL+恒生科技 | +42.2 / 7.6 / −8.3 / 1.30 | +643.1 / 51.6 / −7.7 / 3.51 | −11.2 |
| A9_true | +OIL+有色 | +46.0 / 8.2 / −9.7 / 1.34 | +641.6 / 51.5 / −7.3 / 3.47 | −12.7 |

三窗（A7 vs A4）：OOS2 **−7.3** · train −3.3 · valid −0.5。

## 3. 裁决（冻结 K1–K4）

| 判据 | 阈值 | 结果 |
|---|---|---|
| K1 三窗 Δtot vs A4 | 全 ≥ 0 | ❌ `[−7.3, −3.3, −0.5]` |
| K2 long Δtot vs A4 | ≥ +50pt | ❌ −1.1 |
| K3 long ΔMDD vs A4 | ≥ −3pt | ✅ 0.0 |
| K4 long ΔSharpe | > 0 | ❌ −0.05 |
| **裁决** | | **REJECT** |

## 4. 发现

1. **OIL 让 B3 变差**：逆波动率把高波的 OIL 权重压得很小，但它的加入**稀释了金/纳指/债**这些更优腿，
   且自带深回撤 → B3(6) long 收益 −0.6pt、MDD −0.6pt、Sharpe −0.16。
2. **补更多腿更差**：+恒生科技 → B3(8) long +42.2/−8.3/1.30；+有色 → B3(9) +46.0/−9.7/1.34
   （有色腿本身波动大、A 股同族高相关）。
3. **5 资产菜单是平台**（与 stable-core "不扫资产清单" 的直觉一致）：OIL/黄金/纳指/债 的低相关组合
   已经吃满分散红利；再补腿的边际是**稀释**而非增益。死因 = **#3 方向（补腿无更优点）+ #7 菜单挖掘**。

## 5. 边界（诚实项）

- 动态宇宙是为 OIL 晚上市而设（仅影响长窗早段）；3 个审计窗 OIL 全程存在，结论不依赖该设计。
- `merge_recent_db_closes` 对 512400 报 "no common basis"（CSV/DB 无重叠 → tail 原始追加），
  A9（有色）数字含该基期跳变，**仅价目参考**；不影响主判定 A7。
- A8 首次运行漏载恒生科技（已修重跑）；修正后 B3(8) 数据如上。

## 6. 结论 / 下一步

- **"优化 B3 本身（补腿）"方向关闭**：菜单已是平台，补腿 = 稀释。
- 剩余未测路径：**A. 港湾（Live）也用 B3 停车**（同款 argmax 停车拖回撤，见 B11 valid MDD −21.8）·
  **C. 停车资产 = 指数轮动池**（000300/000688，已知生命体征 OOS2 −3.1）·
  **D. B3 × 套筒 连续配比前沿**（A4↔A2，量化"每 pt 回撤买多少收益"）。

## 7. 复现

```bash
cd services/data-sync-service
PYTHONPATH=src:scripts python3 scripts/eval_sat_idle_b3_menu.py --save-report
PYTHONPATH=src:scripts python3 scripts/eval_sat_idle_b3_menu.py --windows long
```

## 8. 关联

- 停车候选真值：[`sat-idle-b3-drawdown-2026-09-21.md`](sat-idle-b3-drawdown-2026-09-21.md)（A4_true PASS）
- B3 结构来源：[`stable-core-2026-09-12.md`](stable-core-2026-09-12.md)（H-STABLE，"不扫资产清单"）
- 基准：[`etf-benchmark-parking-2026-09-13.md`](etf-benchmark-parking-2026-09-13.md)（B13）
