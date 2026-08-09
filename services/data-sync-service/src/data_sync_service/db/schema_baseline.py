"""Aggregated baseline DDL for Alembic migration 0001.

Source of truth remains per-module CREATE_SQL in db/*.py; this module collects them
in dependency-safe order for `alembic upgrade head` on empty databases.
"""

from __future__ import annotations

from data_sync_service.db.alpha_radar import (
    CREATE_DOCUMENTS_SQL,
    CREATE_META_SQL,
    CREATE_TRENDS_SQL,
    TREND_COLUMN_MIGRATIONS,
    TRENDS_TABLE,
)
from data_sync_service.db.alpha_radar import (
    CREATE_SOURCES_SQL as ALPHA_RADAR_SOURCES_SQL,
)
from data_sync_service.db.broker import CREATE_SQL as BROKER_CREATE_SQL
from data_sync_service.db.daily import CREATE_SQL as DAILY_CREATE_SQL
from data_sync_service.db.decision import CREATE_SQL as DECISION_CREATE_SQL
from data_sync_service.db.etf_fund_flow import CREATE_SQL as ETF_FUND_FLOW_CREATE_SQL
from data_sync_service.db.index_basic import CREATE_SQL as INDEX_BASIC_CREATE_SQL
from data_sync_service.db.index_daily import CREATE_SQL as INDEX_DAILY_CREATE_SQL
from data_sync_service.db.industry_fund_flow import CREATE_SQL as INDUSTRY_FUND_FLOW_CREATE_SQL
from data_sync_service.db.industry_mainline_metrics import CREATE_SQL as MAINLINE_METRICS_CREATE_SQL
from data_sync_service.db.industry_mainline_scores import CREATE_SQL as MAINLINE_SCORES_CREATE_SQL
from data_sync_service.db.journal import CREATE_SQL as JOURNAL_CREATE_SQL
from data_sync_service.db.macro_daily import CREATE_SQL as MACRO_DAILY_CREATE_SQL
from data_sync_service.db.market_detail import CREATE_SQL as MARKET_DETAIL_CREATE_SQL
from data_sync_service.db.market_sentiment import CREATE_SQL as MARKET_SENTIMENT_CREATE_SQL
from data_sync_service.db.news import CREATE_ITEMS_SQL
from data_sync_service.db.news import CREATE_SOURCES_SQL as NEWS_SOURCES_SQL
from data_sync_service.db.research import CREATE_TABLE_SQL as RESEARCH_CREATE_SQL
from data_sync_service.db.stock_basic import CREATE_SQL as STOCK_BASIC_CREATE_SQL
from data_sync_service.db.stock_eastmoney_industry import CREATE_SQL as EM_INDUSTRY_CREATE_SQL
from data_sync_service.db.stoploss import CREATE_INDEX_SQL as STOPLOSS_INDEX_SQL
from data_sync_service.db.stoploss import CREATE_SQL as STOPLOSS_CREATE_SQL
from data_sync_service.db.sync_job_record import CREATE_SQL as SYNC_JOB_RECORD_CREATE_SQL
from data_sync_service.db.system_prompts import CREATE_SQL as SYSTEM_PROMPTS_CREATE_SQL
from data_sync_service.db.top_inst import CREATE_SQL as TOP_INST_CREATE_SQL
from data_sync_service.db.trade_calendar import CREATE_SQL as TRADE_CALENDAR_CREATE_SQL
from data_sync_service.db.trade_review import CREATE_SQL as TRADE_REVIEW_CREATE_SQL
from data_sync_service.db.tv import CREATE_SQL as TV_CREATE_SQL
from data_sync_service.db.tv_chrome_settings import CREATE_SQL as TV_CHROME_SETTINGS_CREATE_SQL
from data_sync_service.db.user_trades import CREATE_SQL as USER_TRADES_CREATE_SQL
from data_sync_service.db.watchlist_automation import CREATE_SQL as WATCHLIST_AUTOMATION_CREATE_SQL

BASELINE_REVISION = "0001_baseline"


def _strip_line_comments(sql: str) -> str:
    """Remove `-- ...` line comments so semicolons inside them do not split statements."""
    lines: list[str] = []
    for line in sql.splitlines():
        if "--" in line:
            line = line[: line.index("--")]
        lines.append(line)
    return "\n".join(lines)


def _split_sql(sql: str) -> list[str]:
    statements: list[str] = []
    for part in _strip_line_comments(sql).split(";"):
        stmt = part.strip()
        if stmt:
            statements.append(f"{stmt};")
    return statements


def _alpha_radar_trend_column_patches() -> list[str]:
    """Runtime patches from TREND_COLUMN_MIGRATIONS not present in CREATE_TRENDS_SQL."""
    create_columns = {
        "macro_theme",
        "catalyst_grade",
        "catalyst",
        "global_target",
        "urgency_level",
        "keywords_for_mapping",
        "cn_symbols",
        "mapping_confidence",
        "risk_status",
        "trend_json",
        "created_at",
    }
    patches: list[str] = []
    for column_name, column_type in TREND_COLUMN_MIGRATIONS:
        if column_name in create_columns:
            continue
        patches.append(
            f"ALTER TABLE {TRENDS_TABLE} ADD COLUMN IF NOT EXISTS {column_name} {column_type};"
        )
    return patches


def baseline_ddl_statements() -> list[str]:
    """Return ordered DDL statements for the initial schema snapshot."""
    ordered_sql = [
        DAILY_CREATE_SQL,
        STOCK_BASIC_CREATE_SQL,
        INDEX_DAILY_CREATE_SQL,
        INDEX_BASIC_CREATE_SQL,
        MACRO_DAILY_CREATE_SQL,
        TRADE_CALENDAR_CREATE_SQL,
        SYNC_JOB_RECORD_CREATE_SQL,
        EM_INDUSTRY_CREATE_SQL,
        INDUSTRY_FUND_FLOW_CREATE_SQL,
        ETF_FUND_FLOW_CREATE_SQL,
        TOP_INST_CREATE_SQL,
        MARKET_SENTIMENT_CREATE_SQL,
        MAINLINE_METRICS_CREATE_SQL,
        MAINLINE_SCORES_CREATE_SQL,
        MARKET_DETAIL_CREATE_SQL,
        STOPLOSS_CREATE_SQL,
        STOPLOSS_INDEX_SQL,
        JOURNAL_CREATE_SQL,
        TRADE_REVIEW_CREATE_SQL,
        TV_CHROME_SETTINGS_CREATE_SQL,
        TV_CREATE_SQL,
        NEWS_SOURCES_SQL,
        CREATE_ITEMS_SQL,
        ALPHA_RADAR_SOURCES_SQL,
        CREATE_DOCUMENTS_SQL,
        CREATE_TRENDS_SQL,
        CREATE_META_SQL,
        BROKER_CREATE_SQL,
        WATCHLIST_AUTOMATION_CREATE_SQL,
        SYSTEM_PROMPTS_CREATE_SQL,
        RESEARCH_CREATE_SQL,
        DECISION_CREATE_SQL,
        USER_TRADES_CREATE_SQL,
    ]
    statements: list[str] = []
    for sql in ordered_sql:
        statements.extend(_split_sql(sql))
    statements.extend(_alpha_radar_trend_column_patches())
    return statements
