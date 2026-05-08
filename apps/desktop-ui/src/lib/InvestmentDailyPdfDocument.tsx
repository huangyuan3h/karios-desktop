'use client';

import * as React from 'react';
import { Document, Page, Text, View, StyleSheet, Font } from '@react-pdf/renderer';

import type { InvestmentDailyPdfLayout, PdfTableBlock } from '@/lib/investmentDailyPdfTypes';

/** CJK line breaks: react-pdf otherwise treats long CJK runs as one "word" and they overflow fixed widths. */
function hyphenateCjkWord(word: string): string[] {
  if (/[\u3000-\u303f\u3040-\u30ff\u3400-\u4dbf\u4e00-\u9fff\uf900-\ufaff]/.test(word)) {
    return Array.from(word);
  }
  return [word];
}

const FONT_SRC =
  'https://cdn.jsdelivr.net/gh/notofonts/noto-cjk@main/Sans/SubsetOTF/SC/NotoSansSC-Regular.otf';

let fontRegistered = false;
function ensureChineseFont(): void {
  if (fontRegistered) return;
  Font.register({ family: 'NotoSansSC', src: FONT_SRC });
  fontRegistered = true;
}

/** Fixed header is out of flow; reserve enough top padding so body never draws under it (incl. line-height slack). */
const PAGE_TOP_RESERVED_FOR_HEADER = 84;

const styles = StyleSheet.create({
  page: {
    fontFamily: 'NotoSansSC',
    fontSize: 9.5,
    lineHeight: 1.45,
    color: '#111',
    paddingTop: PAGE_TOP_RESERVED_FOR_HEADER,
    paddingBottom: 48,
    paddingHorizontal: 40,
    flexDirection: 'column',
    alignItems: 'stretch',
  },
  body: {
    width: '100%',
    maxWidth: '100%',
    flexDirection: 'column',
    alignSelf: 'stretch',
    paddingTop: 2,
  },
  headerBand: {
    position: 'absolute',
    top: 20,
    left: 40,
    right: 40,
    paddingBottom: 8,
    borderBottomWidth: 1.5,
    borderBottomColor: '#1a1a1a',
  },
  headerTitle: { fontSize: 9.5, fontWeight: 700, color: '#222', lineHeight: 1.35 },
  headerSub: { fontSize: 8.5, color: '#555', marginTop: 3, lineHeight: 1.35 },
  footerText: {
    position: 'absolute',
    bottom: 22,
    left: 40,
    right: 40,
    fontSize: 8,
    color: '#666',
    textAlign: 'center',
  },
  docTitle: {
    fontSize: 18,
    fontWeight: 700,
    marginTop: 2,
    marginBottom: 6,
    width: '100%',
    maxWidth: '100%',
    lineHeight: 1.25,
  },
  metaLine: { fontSize: 10, color: '#333', marginBottom: 16, width: '100%', maxWidth: '100%', lineHeight: 1.35 },
  h2: {
    fontSize: 12,
    fontWeight: 700,
    marginTop: 14,
    marginBottom: 6,
    color: '#0b1f3a',
    borderLeftWidth: 3,
    borderLeftColor: '#1a5fb4',
    paddingLeft: 8,
    width: '100%',
    maxWidth: '100%',
  },
  h3: {
    fontSize: 10.5,
    fontWeight: 700,
    marginTop: 8,
    marginBottom: 4,
    width: '100%',
    maxWidth: '100%',
  },
  prose: {
    fontSize: 9.5,
    marginBottom: 10,
    textAlign: 'left',
    width: '100%',
    maxWidth: '100%',
  },
  envBox: {
    width: '100%',
    maxWidth: '100%',
    alignSelf: 'stretch',
    padding: 10,
    marginBottom: 10,
    borderWidth: 1,
    borderColor: '#ccd6e4',
    backgroundColor: '#f8fafc',
  },
  envText: {
    width: '100%',
    maxWidth: '100%',
    fontSize: 9.5,
    lineHeight: 1.45,
  },
  tableOuter: { marginBottom: 10, width: '100%', maxWidth: '100%' },
  tableWrap: { width: '100%', maxWidth: '100%', borderWidth: 1, borderColor: '#bbb' },
  rowHeader: {
    width: '100%',
    maxWidth: '100%',
    flexDirection: 'row',
    backgroundColor: '#eef1f5',
    borderBottomWidth: 1,
    borderBottomColor: '#bbb',
  },
  rowBody: {
    width: '100%',
    maxWidth: '100%',
    flexDirection: 'row',
    borderBottomWidth: 1,
    borderBottomColor: '#ddd',
  },
  cell: {
    flexGrow: 1,
    flexShrink: 1,
    flexBasis: 0,
    minWidth: 0,
    maxWidth: '100%',
    paddingVertical: 4,
    paddingHorizontal: 2,
  },
  cellHeader: { fontWeight: 700, fontSize: 7.4, width: '100%', maxWidth: '100%' },
  cellText: { fontSize: 7.2, width: '100%', maxWidth: '100%', lineHeight: 1.35 },
  bullet: {
    fontSize: 9,
    marginBottom: 3,
    paddingLeft: 4,
    width: '100%',
    maxWidth: '100%',
  },
  muted: { fontSize: 9, color: '#666', marginBottom: 8, width: '100%', maxWidth: '100%' },
});

function PdfTableView({ block }: { block: PdfTableBlock }) {
  return (
    <View style={styles.tableOuter}>
      {block.title ? (
        <Text style={styles.h3} minPresenceAhead={56} hyphenationCallback={hyphenateCjkWord}>
          {block.title}
        </Text>
      ) : null}
      <View style={styles.tableWrap}>
        <View style={styles.rowHeader}>
          {block.headers.map((h, i) => (
            <View key={`h-${i}`} style={styles.cell}>
              <Text style={styles.cellHeader} wrap hyphenationCallback={hyphenateCjkWord}>
                {h}
              </Text>
            </View>
          ))}
        </View>
        {block.rows.map((row, ri) => (
          <View key={`r-${ri}`} style={styles.rowBody}>
            {row.map((cell, ci) => (
              <View key={`c-${ri}-${ci}`} style={styles.cell}>
                <Text style={styles.cellText} wrap hyphenationCallback={hyphenateCjkWord}>
                  {cell}
                </Text>
              </View>
            ))}
          </View>
        ))}
      </View>
    </View>
  );
}

export function InvestmentDailyPdfDocument({ layout }: { layout: InvestmentDailyPdfLayout }) {
  ensureChineseFont();
  const { report, subtitleTimeZh, asOfDate, envZh } = layout;
  const hasIndustrySection =
    layout.industryTopByDate != null ||
    layout.industryInflow != null ||
    layout.industryOutflow != null;

  return (
    <Document title="投资要点日报" creator="Karios" producer="@react-pdf/renderer" language="zh-CN">
      <Page size="A4" style={styles.page} wrap>
        <View style={styles.headerBand} fixed>
          <Text style={styles.headerTitle}>Karios · 投资要点日报</Text>
          <Text style={styles.headerSub}>内部研究资料 · 请勿外传</Text>
        </View>

        <Text
          style={styles.footerText}
          fixed
          render={({ pageNumber, totalPages }) =>
            `第 ${pageNumber} / ${totalPages} 页 · ${subtitleTimeZh} · Karios Dashboard`
          }
        />

        <View style={styles.body}>
        <Text style={styles.docTitle} hyphenationCallback={hyphenateCjkWord}>
          投资要点日报
        </Text>
        <Text style={styles.metaLine} wrap hyphenationCallback={hyphenateCjkWord}>
          数据日期：{asOfDate || '—'} · 导出：{subtitleTimeZh}
        </Text>

        <Text style={styles.h2} minPresenceAhead={80} hyphenationCallback={hyphenateCjkWord}>
          一、市场环境与宏观外盘
        </Text>
        {envZh ? (
          <View style={styles.envBox}>
            <Text style={styles.envText} wrap hyphenationCallback={hyphenateCjkWord}>
              {envZh}
            </Text>
          </View>
        ) : (
          <Text style={styles.prose} hyphenationCallback={hyphenateCjkWord}>
            （暂无市场环境摘要原文）
          </Text>
        )}
        <Text style={styles.h3} hyphenationCallback={hyphenateCjkWord}>
          要点提炼
        </Text>
        <Text style={styles.prose} wrap hyphenationCallback={hyphenateCjkWord}>
          {report.marketEnvironmentHighlights}
        </Text>
        {layout.macroTable ? <PdfTableView block={layout.macroTable} /> : null}

        <Text style={styles.h2} minPresenceAhead={80} hyphenationCallback={hyphenateCjkWord}>
          二、红绿灯 · 仓位与情绪
        </Text>
        {layout.sentimentIndexTable ? <PdfTableView block={layout.sentimentIndexTable} /> : null}
        {layout.sentimentDailyTable ? <PdfTableView block={layout.sentimentDailyTable} /> : null}
        <Text style={styles.h3} hyphenationCallback={hyphenateCjkWord}>
          规则与口径
        </Text>
        <Text style={styles.prose} wrap hyphenationCallback={hyphenateCjkWord}>
          {layout.sentimentStaticNotes}
        </Text>
        {layout.sentimentRuleLines.map((line, i) => (
          <Text key={`sr-${i}`} style={styles.bullet} wrap hyphenationCallback={hyphenateCjkWord}>
            • {line}
          </Text>
        ))}
        <Text style={styles.h3} hyphenationCallback={hyphenateCjkWord}>
          投研解读
        </Text>
        <Text style={styles.prose} wrap hyphenationCallback={hyphenateCjkWord}>
          {report.trafficLightPositionAndSentiment}
        </Text>

        <Text style={styles.h2} minPresenceAhead={80} hyphenationCallback={hyphenateCjkWord}>
          三、行业资金（原始表）
        </Text>
        {hasIndustrySection ? (
          <>
            {layout.industryInflow ? <PdfTableView block={layout.industryInflow} /> : null}
            {layout.industryOutflow ? <PdfTableView block={layout.industryOutflow} /> : null}
            {layout.industryTopByDate ? <PdfTableView block={layout.industryTopByDate} /> : null}
          </>
        ) : (
          <Text style={styles.muted} hyphenationCallback={hyphenateCjkWord}>
            暂无行业日期矩阵 / 五日流向表数据。
          </Text>
        )}

        <Text style={styles.h2} minPresenceAhead={80} hyphenationCallback={hyphenateCjkWord}>
          四、热点行业工作流
        </Text>
        <Text style={styles.h3} hyphenationCallback={hyphenateCjkWord}>
          工作流规则（V4.0）
        </Text>
        {layout.hotStaticRules.map((r, i) => (
          <Text key={`hr-${i}`} style={styles.bullet} wrap hyphenationCallback={hyphenateCjkWord}>
            • {r}
          </Text>
        ))}
        <PdfTableView block={layout.hotPicksTable} />
        <Text style={styles.h3} hyphenationCallback={hyphenateCjkWord}>
          书面分析
        </Text>
        <Text style={styles.prose} wrap hyphenationCallback={hyphenateCjkWord}>
          {report.hotIndustriesFormalAnalysis}
        </Text>

        <Text style={styles.h2} minPresenceAhead={80} hyphenationCallback={hyphenateCjkWord}>
          五、主线与资金流向（综合评述）
        </Text>
        <Text style={styles.prose} wrap hyphenationCallback={hyphenateCjkWord}>
          {report.capitalFlowAndMainline}
        </Text>

        <Text style={styles.h2} minPresenceAhead={80} hyphenationCallback={hyphenateCjkWord}>
          六、本期重点标的（3）
        </Text>
        {report.topStocks.map((s, i) => (
          <View key={`st-${i}`} style={{ marginBottom: 8, width: '100%', maxWidth: '100%' }}>
            <Text
              style={{ fontSize: 10, fontWeight: 700, width: '100%', maxWidth: '100%' }}
              wrap
              hyphenationCallback={hyphenateCjkWord}
            >
              {i + 1}. {s.symbol}（{s.name}）
            </Text>
            <Text style={styles.prose} wrap hyphenationCallback={hyphenateCjkWord}>
              {s.rationale}
            </Text>
          </View>
        ))}

        <Text style={styles.h2} minPresenceAhead={80} hyphenationCallback={hyphenateCjkWord}>
          七、重要资讯摘要（5）
        </Text>
        {report.topNews.map((n, i) => (
          <View key={`nw-${i}`} style={{ marginBottom: 8, width: '100%', maxWidth: '100%' }}>
            <Text
              style={{ fontSize: 9.5, fontWeight: 700, width: '100%', maxWidth: '100%' }}
              wrap
              hyphenationCallback={hyphenateCjkWord}
            >
              {i + 1}. {n.title}
            </Text>
            <Text style={styles.prose} wrap hyphenationCallback={hyphenateCjkWord}>
              {n.summary}
            </Text>
          </View>
        ))}
        </View>
      </Page>
    </Document>
  );
}
