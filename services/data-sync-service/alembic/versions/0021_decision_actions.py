"""Add decision_actions table (建议 → 执行 → 效果追踪).

Extracted action recommendations from decision-agent briefs, matched
against execution journal changes, with price outcomes.
Keep in sync with db/decision.py CREATE_SQL.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0021_decision_actions"
down_revision: str | Sequence[str] | None = "0020_decision_sessions"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS decision_actions (
            id                 BIGSERIAL PRIMARY KEY,
            session_id         BIGINT,
            message_id         BIGINT,
            symbol             TEXT NOT NULL,
            action             TEXT NOT NULL,
            rationale          TEXT,
            confidence         DOUBLE PRECISION,
            status             TEXT NOT NULL DEFAULT 'proposed',
            source             TEXT NOT NULL DEFAULT 'decision_agent',
            snapshot_date      DATE,
            matched_change_id  TEXT,
            outcome            JSONB,
            created_at         TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        CREATE INDEX IF NOT EXISTS ix_decision_actions_created
            ON decision_actions (created_at DESC);
        CREATE INDEX IF NOT EXISTS ix_decision_actions_symbol
            ON decision_actions (symbol);
        CREATE INDEX IF NOT EXISTS ix_decision_actions_message
            ON decision_actions (message_id);
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS decision_actions;")
