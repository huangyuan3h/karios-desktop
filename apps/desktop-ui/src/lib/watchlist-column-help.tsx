import type { ReactNode } from 'react';

/**
 * Watchlist table column metadata — bilingual headers + hover tooltips.
 *
 * Goal: 老婆反馈 — 英文列名/缩写看不懂，hover 上去立刻知道是什么、怎么算、单位是什么。
 *
 * Each entry describes ONE column header. Use in `<th>` to render label +
 * optional CN subtitle + tooltip body.
 *
 * Field semantics:
 * - id:        stable column key
 * - label:     primary text shown in <th> (English-friendly)
 * - sub:       optional CN/EN subtitle rendered below label in smaller font
 * - short:     ≤60-char native browser tooltip (title= attribute). Shown on header.
 * - detail:    full body of the rich tooltip portal — multi-line, formulas, examples.
 * - unit:      small badge after value (e.g. '%', '¥', 'x')
 *
 * Coverage rule: every header that ever renders in WatchlistTable must have an entry
 * so we never regress to a bare ambiguous label.
 */

export type WatchlistColumnHelp = {
  id: string;
  label: string;
  sub?: string;
  short: string;
  detail: ReactNode;
  unit?: string;
};

// Note: detail ReactNode will be inlined at runtime in the portal. The literal
// text strings below are stable, copy-friendly summaries — if you need to
// reference a formula, keep it ASCII-only so terminal users see the same content.

export const WATCHLIST_COLUMN_HELP: Record<string, WatchlistColumnHelp> = {
  color: {
    id: 'color',
    label: 'Color',
    sub: '标签',
    short: '手动打标的彩色标签（白/红/橙/黄/绿/蓝/紫/灰）。',
    detail: '点 cell 弹出色板。可以用来分组（行业/主题/仓位段）。',
  },
  symbol: {
    id: 'symbol',
    label: 'Symbol',
    sub: '代码',
    short: '股票代码（CN: 6 位 / HK: 5 位 / US: ticker）。',
    detail: '点 cell 跳转到 StockPage。CN 标的会带 CN: 前缀（如 CN:600519）。',
  },
  name: {
    id: 'name',
    label: 'Name',
    sub: '名称',
    short: '股票中文名 / 英文名。',
    detail: '来源：tushare stock_basic。',
  },
  industry: {
    id: 'industry',
    label: 'Industry',
    sub: '行业',
    short: 'tushare 行业分类（hover cell 看完整行业链）。',
    detail:
      '来自 tushare stock_basic industry → industry_classify。若 cell 命中"通达信 / 同花顺 / 申万"等行业映射，hover 上方会展示完整路径。',
  },
  positionPct: {
    id: 'positionPct',
    label: '仓位 %',
    sub: 'Position',
    short: '当前账户里这只票占用的资金百分比（手填）。',
    detail:
      '你在 Watchlist 里手动填的数字。系统用这个算 sleeveExposurePct 和单票仓位上限（默认 20%）。留空 = 不计仓位。',
    unit: '%',
  },
  costPrice: {
    id: 'costPrice',
    label: '成本价',
    sub: 'Cost',
    short: '你的持仓成本价（手填，用来算 P&L%）。',
    detail:
      '手动输入，单位元。回车提交；非法字符会被拒绝（最多两位小数）。用于 P&L% = (现价-成本)/成本 × 100。',
    unit: '¥',
  },
  currentPrice: {
    id: 'currentPrice',
    label: '现价',
    sub: 'Current',
    short: '当前价；realtime/EOD 取决于 asOfDate。',
    detail:
      '盘中使用实时 quote；盘后/隔夜回退到 trend bar 的 close。hover cell 末尾可看 asOfDate。',
    unit: '¥',
  },
  stopLoss: {
    id: 'stopLoss',
    label: '止损',
    sub: 'StopLoss',
    short: '动态止损价 = max(final_support - atr_k*ATR(14), hard_stop)。',
    detail:
      '点 cell 看完整 formula / final_support / buffer / hard_stop / 立刻离场检查 4 条（EMA5<EMA20 / Close<EMA20 / 动能+量能同时衰退）。',
    unit: '¥',
  },
  execAction: {
    id: 'execAction',
    label: '执行',
    sub: 'Action',
    short: '执行动作: BUY / ADD / HOLD / TRIM / EXIT / PURGE / WATCH_SILENT。',
    detail:
      '由 deriveActionCard() 综合 trend + gate + mainline + sleeve + sector + catalyst 给出。\n\n' +
      '- BUY: 评分达标 + 主流 + gate 允许\n' +
      '- ADD: 持仓已建 + 浮盈可加 + 仓位未满\n' +
      '- HOLD: 持仓正常，无任何触发\n' +
      '- TRIM: 触发减持条件（例：T1 锁定 / 行业流出 / 防御板块）\n' +
      '- EXIT: 立刻离场（执行场 = 当前价）\n' +
      '- PURGE: 系统判定这只票应剔除\n' +
      '- WATCH_SILENT: 静默观察（默认无交易动作）\n\n' +
      '悬停看 hover title: action + suggestAddPct + mainlineTag。',
  },
  trigger: {
    id: 'trigger',
    label: '触发价',
    sub: 'Trigger',
    short: '持仓中: Exit_Stop; 未持仓: Entry_Trigger (buyZoneHigh)。',
    detail:
      '持仓内的止损价 = max(hardStop, trailStop)；空仓时的入场狙击价 = buyZoneHigh。\n\n' +
      '何时该动手？\n' +
      '- 持仓: 价格 ≤ trigger 考虑减仓 / 止损\n' +
      '- 空仓: 价格 ≤ trigger 考虑试错买入',
    unit: '¥',
  },
  trail: {
    id: 'trail',
    label: '追踪止损',
    sub: 'ATR Trail',
    short: 'Chandelier 追踪止损 — PnL≥10% 时激活；用 peak - 2*ATR(14)。',
    detail:
      '未激活时显示"未激活"（需要 PnL ≥ 10% 才解锁）。激活后显示"已激活↑" + peak/trailStop（hover cell 看具体数值）。',
  },
  buy: {
    id: 'buy',
    label: '建议买入',
    sub: 'Buy',
    short: '买入建议: buy (推荐试错) / wait (等回调) / avoid (回避)。',
    detail:
      '点 cell 看 buyWhy 与 suggestZone。forced=true 表示被 hard rule 强制覆盖（例如 A_pullback 被 A_breakout 覆盖）。',
  },
  hotTop3: {
    id: 'hotTop3',
    label: '板块 Top3',
    sub: 'HotTop3',
    short: '✓ 表示该股所属行业今日进入行业资金流入 Top3。',
    detail:
      '来源: industryFundFlow.dailyTop。命中后 ✓ 提示，作为"主线 + 资金 + 个股"三重共振的辅助确认。',
  },
  rs: {
    id: 'rs',
    label: 'RS',
    sub: '相对强度',
    short: '相对 CSI300 的 20 日涨跌幅差值（>0 跑赢 / <-10% 为 leader）。',
    detail:
      'RS = stock_20d_pctChg − CSI300_20d_pctChg。\n\n' +
      '颜色规则:\n' +
      '- 红色: rs < 0（跑输大盘）\n' +
      '- 绿色: rs > 0（跑赢大盘）\n' +
      '- 加粗: RS_Leader（rs < −10% 表示在弱势市场抗跌，主线候选）',
    unit: '%',
  },
  vwap: {
    id: 'vwap',
    label: 'VWAP',
    sub: '成交量加权均价',
    short: '当日成交量加权均价；CN 个股盘中有 quote 时实时计算。',
    detail:
      'VWAP = amount / volume。盘后 / 隔夜为空。盘中现价 > VWAP x1.5 会触发 alerts.above_vwap_premium 警告。',
    unit: '¥',
  },
  intradayPct: {
    id: 'intradayPct',
    label: '日内 %',
    sub: 'Intraday',
    short: '今日涨跌幅（quote 实时 / 收盘后取 pct_chg）。',
    detail: '正值红色？no — Karios 约定 + 绿色 / - 红色（与 CN 散户视角相反）。>6% 标记为 surge 并触发阻断。',
    unit: '%',
  },
  volumeRatio: {
    id: 'volumeRatio',
    label: '量比 VR',
    sub: 'Volume Ratio',
    short: '当前成交量 / 过去 5 日同时段均量（>=1.5 强势 / <1 弱势）。单位 x。',
    detail:
      '量比 = avgVol(5) / avgVol(30)。\n\n颜色:\n- 红: <1\n- 绿: >=1.5x\n- 默认: 1~1.5x',
    unit: 'x',
  },
  instFlow: {
    id: 'instFlow',
    label: '主力资金',
    sub: 'Inst Flow',
    short: '主力净流入金额（万元，正=流入 / 负=流出）。',
    detail:
      '来源: tushare moneyflow。\n\n- 风险阈值: 当日主力净流出 >= 阈值的板块内单票会触发 watchlist risk。\n- hover cell 看完整 tooltip（流入/流出/股价阶段）。',
  },
  gap: {
    id: 'gap',
    label: '跳空',
    sub: 'Gap',
    short: '今日开盘价 vs 昨日收盘价：✓ 表示向上跳空高开。',
    detail: '跳空高开 + 弱势/震荡市场 → GAP_UP_WEAK_BLOCK，阻止买入。',
  },
  alerts: {
    id: 'alerts',
    label: '风险预警',
    sub: 'Alerts',
    short: '盘中建仓风险预警列表（severity: warn / block）。',
    detail:
      '常见告警:\n' +
      '- above_vwap_premium (warn): 现价 > VWAP x1.5，远离均价\n' +
      '- intraday_surge (block): 涨幅 > 6%，已发酵\n' +
      '- gap_up_weak (block): 跳空 + 弱势/震荡\n' +
      '- inst_outflow_block (block): 行业主力净流出',
  },
  pnl: {
    id: 'pnl',
    label: '盈亏 %',
    sub: 'P&L',
    short: '持仓盈亏 = (现价 − 成本) / 成本 × 100。',
    detail:
      '颜色:\n- 绿: pnl ≥ 5%\n- 红: pnl ≤ 0%\n- 默认: 0% < pnl < 5%\n\n未填成本价时显示 —。',
    unit: '%',
  },
  score: {
    id: 'score',
    label: 'Score',
    sub: '评分',
    short: '0-100 综合评分（CN daily，无 LLM）。',
    detail:
      '点 cell 看公式：EMA + MACD + NearHigh + RSI + Volume + VR hardcap 加权。' +
      'scoreParts 在 TrendOkResult.scoreParts 中按贡献度绝对值排序展示。',
  },
  trendOk: {
    id: 'trendOk',
    label: 'TrendOK',
    sub: '趋势',
    short: '✅ = 6 条规则全部满足；❌ = 任意不满足；🔄 = recovering；— = 数据不足。',
    detail:
      '6 条硬规则:\n' +
      '1) Close > EMA(20) AND EMA(20) > EMA(60)\n' +
      '2) MACD line > 0\n' +
      '3) MACD histogram > 0\n' +
      '4) Close ≥ 0.90 × High(20)\n' +
      '5) RSI(14) ∈ [50, 90]\n' +
      '6) AvgVol(5) > 0.9 × AvgVol(30)\n\n' +
      'recovering 表示曾跌破 EMA20 但出现拐点（MACD hist 连续扩张 3 天）。',
  },
  action: {
    id: 'action',
    label: '操作',
    sub: 'Action',
    short: '快捷按钮：Reference（丢到 chat） / Remove（移出自选股）。',
    detail:
      'Reference 把当前 symbol 的 trend + close + score 等快照丢给 chat，用于 AI 分析上下文。Remove 会弹出确认。',
  },
};

/**
 * Lookup with runtime fallback so missing entries don't crash rendering.
 */
export function getWatchlistColumnHelp(id: string): WatchlistColumnHelp {
  return (
    WATCHLIST_COLUMN_HELP[id] ?? {
      id,
      label: id,
      short: '',
      detail: id,
    }
  );
}

/**
 * Build a tooltip ReactNode body — joins title + bullet list.
 */
export function buildWatchlistColumnTooltipBody(
  h: WatchlistColumnHelp,
  options: { hint?: string } = {},
): React.ReactNode {
  return (
    <>
      <div className="mb-2 flex items-center justify-between">
        <div className="font-medium">
          {h.label}
          {h.sub ? <span className="ml-2 text-[var(--k-muted)]">{h.sub}</span> : null}
        </div>
        {h.unit ? (
          <div className="font-mono text-[var(--k-muted)]">unit: {h.unit}</div>
        ) : null}
      </div>
      <div className="text-[var(--k-muted)]">{h.short}</div>
      <div className="mt-2 whitespace-pre-line text-[var(--k-text)]">{h.detail}</div>
      {options.hint ? (
        <div className="mt-2 text-[10px] text-[var(--k-muted)]">{options.hint}</div>
      ) : null}
    </>
  );
}
