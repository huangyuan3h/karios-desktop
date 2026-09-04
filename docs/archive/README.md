# Archive — 已完成 & 历史材料归档

> **用途**：todo.md 里标记 `[done]` 的任务，**核心结论 / 数据 / 决策记录**迁到这里，长期可查可引。
> 仓库根的架构/代码主清单不动；本目录只放产品级"已完成时点成果"。

---

## 目录约定

```
docs/archive/
├── README.md                 ← 本文件
├── YYYY-MM-{slug}.md        ← 某条 todo done 后的归档（slug 用短横线小写）
└── themes/                  ← 跨条目的专题复盘（可选）
    └── {theme}.md
```

### 归档一条 todo 的最小模板

```text
# {Y标题}  · 归档于 YYYY-MM-DD

## 当时的目标（todo 链接）
- 引用 docs/todo.md 对应节 + 任务 ID

## 实际做了什么
- ...

## 验证 / 数据
- ...

## 后续影响 / 留给谁
- 是否需要补 OPT-xxx / TIP-xxx / V6.x
```

---

## 已沉淀的历史材料

| 日期 | 来源 todo 节 | 归档文件 | 一句话结论 |
|------|--------------|----------|-----------|
| 2026-08-04 | §2 收益 · P2 | [`2026-08-04-tip-011-execution-source.md`](./2026-08-04-tip-011-execution-source.md) | 开火来源归因：TV/ALPHA/MANUAL 贯穿 write-path，`/v1/execution/source-stats` + Copy attribution 表，用户零操作 |
| 2026-08-15 | §8 探索池 D3/D6 + 回测文档整理 | [`2026-08-15-backtest-d3-d6-docs.md`](./2026-08-15-backtest-d3-d6-docs.md) | D3 环境仓位固化（uptrend 1.25×/fan 0.75×，三窗全升长窗+64pt）；D6 profit_trail 复核排除（截断右尾）；新建 docs/backtests/ 四份实验记录（成功+失败全记录） |
| 2026-08-15 | §8 信号池 P1-P26 第一阶段 | [`2026-08-15-signal-pool-p1-p26.md`](./2026-08-15-signal-pool-p1-p26.md) | 10 项实验全拒收（P1-P8 技术形态 + P12 波动率动量 + P16-ST）——S-3 alpha = RS + 环境感知 + 纪律；待验证 14 项，下一候选 P11 行业中性 RS |
| 2026-09-02 | P0-0 刀 7 | [`2026-09-02-twin-star-ops-knife7-watchlist-flow.md`](./2026-09-02-twin-star-ops-knife7-watchlist-flow.md) | Watchlist 日流程落地；sat/S-3 按标的拆账；S-3 缺票不当双子星交易铃 |

---

## 维护规则

1. **完成判定**：todo 上勾 `[done]` 的同时，把"做了什么 + 关键数据"摘到归档文件里，不堆细节。
2. **颗粒度**：单次归档 ≈ 半天到一周可读；超过就拆。
3. **代码细节去哪**：实现层面的 diff 在 git/PR 里查；归档只留**结论级**信息（数字、决策、阈值、坑）。
4. **不允许反向修改**：归档文件是历史快照，不为新现实回写。
