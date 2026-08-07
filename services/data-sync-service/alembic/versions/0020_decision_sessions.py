"""Add decision agent loop tables (TIP-015).

Decision-agent sessions/messages persisted server-side, plus daily
decision snapshots for the 10-day archive layer and outcome feedback.
Keep in sync with db/decision.py CREATE_SQL.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0020_decision_sessions"
down_revision: str | Sequence[str] | None = "0019_research_reports"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS decision_sessions (
            id             BIGSERIAL PRIMARY KEY,
            title          TEXT,
            model_profile  TEXT,
            system_prompt  TEXT,
            created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
            last_active_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        CREATE TABLE IF NOT EXISTS decision_messages (
            id               BIGSERIAL PRIMARY KEY,
            session_id       BIGINT NOT NULL REFERENCES decision_sessions(id) ON DELETE CASCADE,
            role             TEXT NOT NULL,
            content          TEXT NOT NULL DEFAULT '',
            context_snapshot JSONB,
            created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        CREATE INDEX IF NOT EXISTS ix_decision_messages_session
            ON decision_messages (session_id, created_at);
        CREATE TABLE IF NOT EXISTS decision_snapshots (
            id               BIGSERIAL PRIMARY KEY,
            snapshot_date    DATE NOT NULL UNIQUE,
            active_layer_ref JSONB,
            agent_exchanges  JSONB,
            outcome          JSONB,
            status           TEXT NOT NULL DEFAULT 'open',
            created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS decision_snapshots;")
    op.execute("DROP TABLE IF EXISTS decision_messages;")
    op.execute("DROP TABLE IF EXISTS decision_sessions;")
