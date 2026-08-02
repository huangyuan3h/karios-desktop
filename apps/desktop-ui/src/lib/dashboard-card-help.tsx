import type { ReactNode } from 'react';

/**
 * Dashboard card section metadata — bilingual headers + hover tooltips.
 *
 * Used in DashboardPage for:
 *   - Sentiment card "Last 5 days" table headers
 *   - Sentiment card ETF flow table headers
 *   - Index traffic lights rule explanations (collapsed to hover)
 *   - Watchlist risk card table headers
 *   - Screener card table headers
 *   - Sync result table headers
 *   - News card description
 *
 * Goal: 老婆反馈 — 英文缩写看不懂、参数不知道干什么，hover 上去立刻明白。
 */

export type DashboardHelp = {
  id: string;
  label: string;
  sub?: string;
  short: string;
  detail: ReactNode;
  unit?: string;
};

export const DASHBOARD_HELP: Record<string, DashboardHelp> = {
  // --- Sentiment "Last 5 days" table ---
  'sentiment5d.date': {
    id: 'sentiment5d.date',
    label: '日期',
    sub: 'date',
    short: '交易日（YYYY-MM-DD）。',
    detail: '来自 tushare index_daily / trade_cal 缓存。',
  },
  'sentiment5d.ratio': {
    id: 'sentiment5d.ratio',
    label: '涨跌比',
    sub: 'Up/Down ratio',
    short: '当日上涨家数 ÷ 下跌家数（>1 普涨 / <1 普跌）。',
    detail:
      '计算口径：\n' +
      '- 上涨: close > pre_close\n' +
      '- 下跌: close < pre_close\n' +
      '- 平盘: close == pre_close\n\n' +
      '情绪阈值:\n' +
      '- ratio > 3: 普涨（green）\n' +
      '- ratio < 0.5: 普跌（red）',
  },
  'sentiment5d.turnover': {
    id: 'sentiment5d.turnover',
    label: '成交额',
    sub: 'turnover',
    short: '沪深两市全市场当日成交额（万亿 / 亿）。',
    detail:
      '汇总自 index_daily 缓存（000300.SH + 000905.SH + 399006.SZ 等）。' +
      '15:00 后取收盘后值；盘中可看 quote 实时值。',
  },
  'sentiment5d.premiumPct': {
    id: 'sentiment5d.premiumPct',
    label: '昨涨停溢价',
    sub: 'premium%',
    short: '昨日涨停股今日开盘平均溢价率（衡量赚钱效应）。',
    detail:
      '昨涨停股今日开盘价 / 昨收盘价 − 1，正值=有溢价，负值=集体低开。\n' +
      '>= 5% 表示打板资金次日仍有肉，<= 0 表示 "杀溢价"。',
    unit: '%',
  },
  'sentiment5d.failedPct': {
    id: 'sentiment5d.failedPct',
    label: '炸板率',
    sub: 'failed%',
    short: '昨日涨停股今日未能封板的比率（衡量接力情绪）。',
    detail:
      '炸板率 = (今日未能封板的昨日涨停股数) / (昨日涨停股总数)。\n' +
      '>50% 表示打板情绪恶化，<20% 表示强势。',
    unit: '%',
  },
  'sentiment5d.risk': {
    id: 'sentiment5d.risk',
    label: '风险模式',
    sub: 'risk',
    short: 'riskMode = 综合判断的仓位策略。',
    detail:
      '常见值:\n' +
      '- confirmed_uptrend: 主升浪（绿，加仓）\n' +
      '- hot: 局部活跃（绿/黄，正常）\n' +
      '- caution: 警示（黄，谨慎）\n' +
      '- no_new_positions: 不开新仓（红）\n' +
      '- extreme_caution / breadth_panic: 极度谨慎（红）\n' +
      '- capitulation_v_bottom: 恐慌冰点（紫，左侧试错）\n' +
      '- euphoric: 顶部狂热（紫）',
  },

  // --- ETF fund flow table ---
  'etf.name': {
    id: 'etf.name',
    label: 'ETF 名称',
    sub: 'Name',
    short: '基金名称（来自你 Watchlist 里的 ETF 代码）。',
    detail: '代码来自 Watchlist，名称来自 tushare fund_basic。',
  },
  'etf.symbol': {
    id: 'etf.symbol',
    label: '代码',
    sub: 'Symbol',
    short: 'ETF 6 位代码（如 510300 = 沪深300ETF）。',
    detail: '点 code 跳转到 StockPage。',
  },
  'etf.mainFlow': {
    id: 'etf.mainFlow',
    label: '主力净流入',
    sub: 'Main Flow',
    short: '今日主力净流入金额（正=流入 / 负=流出）。',
    detail: '数据源: 东方财富 ETF 实时资金流；盘后取最新 cache。',
  },
  'etf.superLarge': {
    id: 'etf.superLarge',
    label: '超大/大单',
    sub: 'Super/Large',
    short: '超大单净流入 / 大单净流入（格式 x/y）。',
    detail:
      '订单金额阈值:\n' +
      '- 超大单 >= 100 万元\n' +
      '- 大单 20~100 万元\n' +
      '两者同向 = 主力态度一致；背离 = 警惕对倒。',
  },
  'etf.flow3d': {
    id: 'etf.flow3d',
    label: '3日净流入',
    sub: '3D Net Flow',
    short: '近 3 个交易日累计主力净流入。',
    detail: '3 日趋势，比单日更稳定。',
  },
  'etf.realtimeAsOf': {
    id: 'etf.realtimeAsOf',
    label: '实时截至',
    sub: 'Realtime AsOf',
    short: '数据最新时间戳（YYYY-MM-DD HH:MM 或 EOD）。',
    detail: '盘后同步结束后显示 EOD；盘中正常为分钟级 quote 时间。',
  },
  'etf.source': {
    id: 'etf.source',
    label: '数据源',
    sub: 'Source',
    short: 'eastmoney / akshare / tushare 之一。',
    detail: '盘后同步会写 source 字段，便于溯源。',
  },
  'etf.status': {
    id: 'etf.status',
    label: '状态',
    sub: 'Status',
    short: 'Live / Market Closed / Stale / Missing 之一。',
    detail:
      '- Live: 盘中且 quote 实时刷新\n' +
      '- Market Closed: 已收盘，quote 不再变化\n' +
      '- Stale: 数据停滞（盘中超过 N 分钟未更新）\n' +
      '- Missing: 当日数据缺失',
  },
  'etf.signal': {
    id: 'etf.signal',
    label: '信号',
    sub: 'Signal',
    short: '由主力和超大/大单推导的流入强度（流入/强流入/流出/数据滞后）。',
    detail:
      '- 强流入: mainFlow 显著为正且超大单/大单同向\n' +
      '- 流入: 主力为正\n' +
      '- 流出: 主力为负\n' +
      '- Data Lag: 实时数据缺失，等待盘后',
  },

  // --- Index traffic lights rule explanations ---
  'idxRule.title': {
    id: 'idxRule.title',
    label: '信号规则（简版）',
    sub: 'Index traffic lights',
    short: '指数红/黄/绿/深绿的判定规则（hover 看完整版）。',
    detail:
      '🟢 绿 (light_green)\n' +
      'Price > MA20 AND MA5 > MA20 AND MA20 向上\n' +
      '预估全天量 > MA5_Vol × 0.8\n' +
      '仓位建议: 50%-60%\n\n' +
      '❇️ 深绿 (deep_green)\n' +
      'MA5 > MA20 > MA60 AND Price > EMA(10)\n' +
      '全市场成交额连续 > 1.5 万亿\n' +
      'Breadth > 50% OR 单一板块流入 > 50 亿\n' +
      '仓位建议: 80%-100%\n\n' +
      '🟡 黄 (yellow)\n' +
      'Price > MA20 但 MA20 斜率向下\n' +
      'OR 预估全天量 < MA5_Vol × 0.8\n' +
      'OR MA5 < MA20\n' +
      '仓位建议: 30%\n\n' +
      '🔴 红 (red)\n' +
      'Price < MA20 OR MA5 < MA20\n' +
      '仓位建议: 0%-10%',
  },
  'idxRule.posRange': {
    id: 'idxRule.posRange',
    label: '建议仓位 %',
    sub: 'pos range',
    short: '基于指数信号的建议仓位区间（具体数值取决于 gate.mode）。',
    detail:
      'gate.mode 会覆盖这个范围:\n' +
      '- allow: 100% × pos range\n' +
      '- hedge: 50% × pos range\n' +
      '- defend: 30% × pos range\n' +
      '- cash: 0%',
    unit: '%',
  },

  // --- Watchlist risk table ---
  'risk.symbol': {
    id: 'risk.symbol',
    label: '代码',
    sub: 'Symbol',
    short: '股票代码（CN: 前缀）。',
    detail: '来自 Watchlist。',
  },
  'risk.name': {
    id: 'risk.name',
    label: '名称',
    sub: 'Name',
    short: '股票中文名（来自 Watchlist）。',
    detail: '来源: Watchlist 列表。',
  },
  'risk.intradayPct': {
    id: 'risk.intradayPct',
    label: '日内 %',
    sub: 'Intraday',
    short: '今日涨跌幅（>6% 红色 surge 警示）。',
    detail: '>6% 标红 — 已经发酵，建仓风险高。',
    unit: '%',
  },
  'risk.vr': {
    id: 'risk.vr',
    label: '量比',
    sub: 'VR',
    short: 'avgVol(5) / avgVol(30)。>=1.5 强 / <1 弱。',
    detail: '量比显著放大配合放量突破才考虑参与。',
    unit: 'x',
  },
  'risk.gap': {
    id: 'risk.gap',
    label: '跳空',
    sub: 'Gap',
    short: '今日开盘 vs 昨收（✓ 跳空高开）。',
    detail: '跳空高开 + 弱势/震荡市场 = GAP_UP_WEAK_BLOCK。',
  },
  'risk.alerts': {
    id: 'risk.alerts',
    label: '告警',
    sub: 'Alerts',
    short: '本只股票的盘中建仓风险点列表。',
    detail:
      'severity=block 阻断买入；severity=warn 仅提示。\n' +
      '常见代码: above_vwap_premium / intraday_surge / gap_up_weak / inst_outflow_block。',
  },

  // --- Screener card ---
  'screener.name': {
    id: 'screener.name',
    label: '名称',
    sub: 'Name',
    short: 'TradingView Screener 名称（用户在 ScreenerPage 自定义）。',
    detail: '红色行: rowCount=0 或 missing 的 screener（提示需要修复）。',
  },
  'screener.capturedAt': {
    id: 'screener.capturedAt',
    label: '抓取时间',
    sub: 'capturedAt',
    short: '最近一次抓取的 timestamp（YYYY-MM-DD HH:MM）。',
    detail: '盘后自动抓取；用户也可手动 "Sync" 触发。',
  },
  'screener.rows': {
    id: 'screener.rows',
    label: '命中数',
    sub: 'rows',
    short: 'screener 当前行数（0 表示数据缺失）。',
    detail: '0 行 = 数据可能丢失，需要 Sync 重新抓。',
  },
  'screener.filters': {
    id: 'screener.filters',
    label: '筛选器',
    sub: 'filters',
    short: '当前 screener 的 filter 数量。',
    detail: '用于确认 screener 配置复杂度。',
  },

  // --- Sync result table ---
  'sync.step': {
    id: 'sync.step',
    label: '步骤',
    sub: 'Step',
    short: '同步任务 step 名（industryFundFlow / marketSentiment / screeners / news / watchlist 等）。',
    detail: '同一字段也展示在同步过程中（带进度）。',
  },
  'sync.ok': {
    id: 'sync.ok',
    label: '成功',
    sub: 'OK',
    short: '该 step 是否成功（true / false）。',
    detail: '失败时 Message 列会给出原因（网络超时 / 数据缺失 / etc.）。',
  },
  'sync.duration': {
    id: 'sync.duration',
    label: '耗时',
    sub: 'Duration',
    short: '该 step 的耗时（毫秒）。',
    detail: '> 30s 通常意味着上游接口阻塞或网络问题。',
  },
  'sync.message': {
    id: 'sync.message',
    label: '说明',
    sub: 'Message',
    short: '失败原因或附加说明（成功时为空）。',
    detail: '可包含异常堆栈 / 数据源 / 错误码。',
  },

  // --- News card ---
  'news.brief': {
    id: 'news.brief',
    label: '24 小时新闻摘要',
    sub: 'AI-generated',
    short: 'AI 摘要（由本地 ai-service 生成），重点关注 A 股 / 港股 / 美股 / 大宗商品。',
    detail:
      '生成流程:\n' +
      '1. 抓取 24 小时内的新闻（关键词白名单过滤）\n' +
      '2. 喂给本地 ai-service 生成摘要\n' +
      '3. 失败时回退到关键词过滤后的 raw titles\n\n' +
      '可点 "Regenerate" 强制重生成；"Open News" 跳到 News 页看完整新闻。',
  },
  'news.asOf': {
    id: 'news.asOf',
    label: '生成时间',
    sub: 'Generated',
    short: 'AI 摘要生成的 timestamp。',
    detail: '盘后定时同步：每个交易日 17:35 重新生成。',
  },
};

export function getDashboardHelp(id: string): DashboardHelp {
  return (
    DASHBOARD_HELP[id] ?? {
      id,
      label: id,
      short: '',
      detail: id,
    }
  );
}

export function buildDashboardHelpTooltipBody(h: DashboardHelp): ReactNode {
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
    </>
  );
}
