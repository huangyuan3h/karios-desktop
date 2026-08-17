"""0033 webhook subscription provider (2026-08-14).

webhook_subscriptions gains a `provider` column: 'generic' (raw event JSON
POST, HMAC-signed) or 'bark' (formatted title/body for the Bark push app —
https://api.day.app/<device-key>, see cookbook §9). Delivery re-formats the
event body for bark providers before signing.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0033_webhook_provider"
down_revision: str | Sequence[str] | None = "0032_user_trades_alpha_snapshot"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        "ALTER TABLE webhook_subscriptions ADD COLUMN IF NOT EXISTS "
        "provider TEXT NOT NULL DEFAULT 'generic';"
    )


def downgrade() -> None:
    op.execute("ALTER TABLE webhook_subscriptions DROP COLUMN IF EXISTS provider;")
