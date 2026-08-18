"""user_trades_alpha as-of snapshot tests (§19.3).

Integration tests write alpha-radar rows under a `CN:99` test-symbol prefix and
MUST clean them up (AGENTS.md DB hygiene discipline): the autouse fixture
deletes the test source row — its documents and trends cascade away.
"""

from __future__ import annotations

import pytest

from data_sync_service.db import alpha_radar as ar
from data_sync_service.db import get_connection
from data_sync_service.service.user_trades_alpha import alpha_snapshot_for

TEST_SOURCE_ID = "src-test-alpha-snap"
TEST_SYMBOL = "CN:99alpha1"

pytestmark = pytest.mark.requires_postgres


@pytest.fixture(autouse=True)
def _cleanup_test_rows():
    yield
    ar.ensure_tables()
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM alpha_radar_sources WHERE id = %s", (TEST_SOURCE_ID,))


def _insert_trend(
    *,
    trend_id: str,
    doc_id: str,
    published_at: str,
    fetched_at: str,
    symbol: str = TEST_SYMBOL,
    grade: str = "A",
    confidence: float = 0.8,
    risk_status: str = "active",
) -> None:
    ar.ensure_tables()
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO alpha_radar_sources (id, name, url, category, enabled, created_at)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING
            """,
            (TEST_SOURCE_ID, "test", f"https://t/{doc_id}", "test", True, "2026-01-01"),
        )
        cur.execute(
            """
            INSERT INTO alpha_radar_documents
                (id, source_id, title, url, category, published_at, fetched_at, processing_status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (doc_id, TEST_SOURCE_ID, f"doc {doc_id}", f"https://d/{doc_id}", "test",
             published_at, fetched_at, "processed"),
        )
        cur.execute(
            """
            INSERT INTO alpha_radar_trends (
                id, document_id, trend_name, catalyst_grade, catalyst, global_target,
                urgency_level, keywords_for_mapping, cn_symbols, mapping_confidence,
                risk_status, trend_json, created_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (trend_id, doc_id, f"trend {trend_id}", grade, "cat", "target", "A",
             "kw", f'[{{"symbol": "{symbol}"}}]', confidence, risk_status,
             '{"hkSymbols": []}', fetched_at),
        )


def test_snapshot_counts_only_events_visible_as_of() -> None:
    # Event 5 days before trade_date -> counted.
    _insert_trend(trend_id="t-snap-in", doc_id="d-snap-in",
                  published_at="2026-08-08T02:00:00+08:00",
                  fetched_at="2026-08-08T02:00:00+08:00")
    # Event on trade_date itself -> counted (as-of day boundary inclusive).
    _insert_trend(trend_id="t-snap-same", doc_id="d-snap-same",
                  published_at="2026-08-13T09:00:00+08:00",
                  fetched_at="2026-08-13T09:00:00+08:00",
                  grade="S", confidence=0.95)
    # Event AFTER trade_date -> must NOT count (no lookahead).
    _insert_trend(trend_id="t-snap-future", doc_id="d-snap-future",
                  published_at="2026-08-15T09:00:00+08:00",
                  fetched_at="2026-08-15T09:00:00+08:00")
    # Event older than the 14-day window -> must NOT count.
    _insert_trend(trend_id="t-snap-old", doc_id="d-snap-old",
                  published_at="2026-07-25T09:00:00+08:00",
                  fetched_at="2026-07-25T09:00:00+08:00")
    # Another symbol -> must NOT count.
    _insert_trend(trend_id="t-snap-other", doc_id="d-snap-other",
                  published_at="2026-08-10T09:00:00+08:00",
                  fetched_at="2026-08-10T09:00:00+08:00",
                  symbol="CN:99alpha9")

    snap = alpha_snapshot_for(TEST_SYMBOL, "2026-08-13")

    assert snap is not None
    assert snap["asOf"] == "2026-08-13"
    assert snap["nEvents"] == 2
    assert snap["hasSA"] is True
    assert snap["maxConfidence"] == 0.95
    grades = {e["grade"] for e in snap["events"]}
    assert grades == {"A", "S"}
    assert any(e["daysAgo"] == 5 for e in snap["events"])
    assert snap["riskStatuses"] == ["active"]


def test_snapshot_none_when_no_matching_events() -> None:
    _insert_trend(trend_id="t-snap-nomatch", doc_id="d-snap-nomatch",
                  published_at="2026-08-10T09:00:00+08:00",
                  fetched_at="2026-08-10T09:00:00+08:00")
    assert alpha_snapshot_for("CN:99nomatch", "2026-08-13") is None
