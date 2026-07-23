/* eslint-disable @typescript-eslint/no-explicit-any */
'use client';

import * as React from 'react';
import { useQueryClient } from '@tanstack/react-query';

import { apiPostJson } from '@/lib/api/client';
import { AI_BASE_URL } from '@/lib/endpoints';
import { isShanghaiSyncWindow } from '@/lib/market-hours';
import {
  dashboardLiteQueryKey,
  mergeDashboardSummaryParts,
  saveDashboardSummaryCache,
  useDashboardSummaryQuery,
  type DashboardSummary,
} from '@/lib/queries/dashboard';
import {
  fetchDashboardNews,
  dashboardNewsQueryKey,
  useDashboardNewsQuery,
} from '@/lib/queries/news';
import {
  dashboardSentimentQueryKey,
  useDashboardSentimentQuery,
} from '@/lib/queries/sentiment';
import { stripModelThinking } from '@/lib/strip-model-thinking';

const NEWS_BRIEF_CACHE_KEY = 'karios.dashboard.newsBrief.v1';
const NEWS_BRIEF_MIN_REFRESH_MS = 4 * 60 * 60 * 1000;

type NewsBriefCache = {
  summary?: string;
  updatedAt?: string;
  fallback?: string;
  fallbackUpdatedAt?: string;
};

function buildNewsFallback(items: any[]): string | null {
  const rows = (Array.isArray(items) ? items : [])
    .slice(0, 8)
    .map((it: any, idx: number) => {
      const title = String(it?.title ?? '').trim();
      if (!title) return null;
      const source = String(it?.sourceId ?? '').trim();
      const publishedAt = String(it?.publishedAt ?? '').trim();
      const meta = [source, publishedAt].filter(Boolean).join(' | ');
      return `${idx + 1}. ${title}${meta ? ` (${meta})` : ''}`;
    })
    .filter(Boolean) as string[];
  if (!rows.length) return null;
  return ['Latest headlines:', ...rows].join('\n');
}

function mergeDashboardSummary(
  lite: DashboardSummary | undefined,
  sentiment: DashboardSummary | undefined,
  news: DashboardSummary | undefined,
): DashboardSummary | null {
  return mergeDashboardSummaryParts(lite, sentiment, news);
}

export function useDashboardSummary() {
  const queryClient = useQueryClient();
  const liteQuery = useDashboardSummaryQuery();
  const sentimentQuery = useDashboardSentimentQuery();
  const newsQuery = useDashboardNewsQuery();

  const summary = React.useMemo(
    () => mergeDashboardSummary(liteQuery.data, sentimentQuery.data, newsQuery.data),
    [liteQuery.data, sentimentQuery.data, newsQuery.data],
  );

  const summaryLoading =
    (liteQuery.isFetching && !liteQuery.data) ||
    (sentimentQuery.isFetching && !sentimentQuery.data) ||
    (newsQuery.isFetching && !newsQuery.data);

  const [error, setError] = React.useState<string | null>(null);
  const [sentimentBusy, setSentimentBusy] = React.useState(false);
  const [newsSummary, setNewsSummary] = React.useState<string | null>(null);
  const [newsSummaryUpdatedAt, setNewsSummaryUpdatedAt] = React.useState<string | null>(null);
  const [newsFallback, setNewsFallback] = React.useState<string | null>(null);
  const [newsSummaryBusy, setNewsSummaryBusy] = React.useState(false);

  function saveNewsBriefCache(patch: NewsBriefCache) {
    try {
      const raw = window.localStorage.getItem(NEWS_BRIEF_CACHE_KEY);
      const prev = raw ? (JSON.parse(raw) as NewsBriefCache) : {};
      window.localStorage.setItem(NEWS_BRIEF_CACHE_KEY, JSON.stringify({ ...prev, ...patch }));
    } catch {
      // ignore
    }
  }

  function shouldRefreshNewsBrief(lastUpdatedAt: string | null): boolean {
    if (!lastUpdatedAt) return true;
    const t = new Date(lastUpdatedAt).getTime();
    if (!Number.isFinite(t)) return true;
    return Date.now() - t >= NEWS_BRIEF_MIN_REFRESH_MS;
  }

  function applySummaryToCache(next: DashboardSummary) {
    queryClient.setQueryData(dashboardLiteQueryKey(), (prev: DashboardSummary | undefined) => ({
      ...(prev ?? {}),
      asOfDate: next.asOfDate,
      industryFundFlow: next.industryFundFlow,
      screeners: next.screeners,
      marketEnvironmentZh: next.marketEnvironmentZh,
    }));
    queryClient.setQueryData(dashboardSentimentQueryKey(), (prev: DashboardSummary | undefined) => ({
      ...(prev ?? {}),
      asOfDate: next.asOfDate,
      marketSentiment: next.marketSentiment,
      macroSnapshot: next.macroSnapshot,
      marketEnvironmentZh: next.marketEnvironmentZh,
    }));
    queryClient.setQueryData(dashboardNewsQueryKey(), (prev: DashboardSummary | undefined) => ({
      ...(prev ?? {}),
      news: next.news,
    }));
    saveDashboardSummaryCache(next);
  }

  React.useEffect(() => {
    if (!newsQuery.data) return;
    const fallback = buildNewsFallback((newsQuery.data as any)?.news?.items ?? []);
    if (fallback) {
      const fallbackUpdatedAt = new Date().toISOString();
      setNewsFallback(fallback);
      saveNewsBriefCache({ fallback, fallbackUpdatedAt });
    }
  }, [newsQuery.data]);

  React.useEffect(() => {
    const err = liteQuery.error ?? sentimentQuery.error ?? newsQuery.error;
    if (!err) return;
    setError(err instanceof Error ? err.message : String(err));
  }, [liteQuery.error, sentimentQuery.error, newsQuery.error]);

  React.useEffect(() => {
    try {
      const raw = window.localStorage.getItem(NEWS_BRIEF_CACHE_KEY);
      if (!raw) return;
      const obj = JSON.parse(raw) as NewsBriefCache;
      const cachedSummary =
        typeof obj?.summary === 'string' ? stripModelThinking(obj.summary) : '';
      const updatedAt = typeof obj?.updatedAt === 'string' ? obj.updatedAt.trim() : '';
      const fallback = typeof obj?.fallback === 'string' ? obj.fallback.trim() : '';
      if (cachedSummary) setNewsSummary(cachedSummary);
      if (updatedAt) setNewsSummaryUpdatedAt(updatedAt);
      if (fallback) setNewsFallback(fallback);
    } catch {
      // ignore
    }
  }, []);

  async function refetchSummary() {
    await Promise.all([
      liteQuery.refetch(),
      sentimentQuery.refetch(),
      newsQuery.refetch(),
    ]);
  }

  async function onSyncSentiment() {
    setSentimentBusy(true);
    setError(null);
    try {
      const force = isShanghaiSyncWindow();
      const q = force ? 'true' : 'false';
      await Promise.all([
        apiPostJson('/market/cn/sentiment/sync', { force }),
        apiPostJson(`/sync/etf-fund-flow-watchlist?force=${q}`, {}),
        apiPostJson(`/sync/top-inst-watchlist?force=${q}`, {}),
        apiPostJson(`/sync/option-iv-daily?force=${q}`, {}),
      ]);
      await queryClient.invalidateQueries({ queryKey: dashboardSentimentQueryKey() });
      await queryClient.invalidateQueries({ queryKey: dashboardLiteQueryKey() });
      await queryClient.invalidateQueries({ queryKey: ['macro', 'snapshot'] });
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setSentimentBusy(false);
    }
  }

  async function regenerateNewsSummary() {
    setNewsSummaryBusy(true);
    setError(null);
    try {
      const s = await fetchDashboardNews();
      queryClient.setQueryData(dashboardNewsQueryKey(), s);
      const newsData = (s as any)?.news;
      if (newsData && Array.isArray(newsData.items) && newsData.items.length > 0) {
        const aiRes = await fetch(`${AI_BASE_URL}/news/summary`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ items: newsData.items, hours: 24 }),
        });
        if (aiRes.ok) {
          const aiData = await aiRes.json();
          const summaryText =
            typeof aiData?.summary === 'string' ? stripModelThinking(aiData.summary) : '';
          if (summaryText) {
            const updatedAt = new Date().toISOString();
            setNewsSummary(summaryText);
            setNewsSummaryUpdatedAt(updatedAt);
            saveNewsBriefCache({ summary: summaryText, updatedAt });
          }
        } else {
          const errText = await aiRes.text();
          setError(`AI error: ${errText}`);
        }
      } else {
        setError('No news items available');
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setNewsSummaryBusy(false);
    }
  }

  return {
    summary,
    summaryLoading,
    refetchSummary,
    error,
    setError,
    applySummaryToCache,
    newsSummary,
    newsSummaryUpdatedAt,
    newsFallback,
    newsSummaryBusy,
    sentimentBusy,
    onSyncSentiment,
    regenerateNewsSummary,
    shouldRefreshNewsBrief,
    saveNewsBriefCache,
    setNewsSummary,
    setNewsSummaryUpdatedAt,
    setNewsSummaryBusy,
  };
}
