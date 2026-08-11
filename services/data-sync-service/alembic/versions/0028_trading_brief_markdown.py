"""0028 trading-session briefs markdown (2026-08-11).

morning_briefs gains a rendered-markdown column for the three trading-
session briefs (trading-open 10:00 / trading-midday 12:00 / trading-action
14:30). The structured sections live in `items` (JSONB); `markdown` carries
the compact ~30s-read rendering for the front-end card (react-markdown)
and copy-paste to the broker/decision agent. Null for legacy news briefs.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0028_trading_brief_markdown"
down_revision: str | Sequence[str] | None = "0027_recon_return_diff"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "morning_briefs",
        sa.Column("markdown", sa.Text(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("morning_briefs", "markdown")
