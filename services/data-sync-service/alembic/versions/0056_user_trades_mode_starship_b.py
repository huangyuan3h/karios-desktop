"""0056 user_trades.strategy_mode: allow 'starship_b'.

Starship B became a first-class strategy mode (OPT-234 / 2026-09-24; the
watchlist default = 'starship_b'), and the shared ``UserTradeStrategyModeSchema``
already allows it — but the DB CHECK (and ``db/user_trades.py`` STRATEGY_MODES)
still rejected it, so ``GET /satellite-paper/user?strategyMode=starship_b``
returned 400.

Revision id kept <= 32 chars (alembic_version.version_num is varchar(32)).
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0056_user_trades_mode_starship_b"
down_revision: str | Sequence[str] | None = "0055_amp_1430"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_MODES = (
    "'harbor', 'homeport', 'starport', 'starship', 'starship_robust', "
    "'starship_b', 'twin_star', 'legacy_unknown'"
)
_OLD = (
    "'harbor', 'homeport', 'starport', 'starship', 'starship_robust', "
    "'twin_star', 'legacy_unknown'"
)


def upgrade() -> None:
    op.execute(
        "ALTER TABLE user_trades DROP CONSTRAINT IF EXISTS user_trades_strategy_mode_check;"
        "ALTER TABLE user_trades ADD CONSTRAINT user_trades_strategy_mode_check "
        f"CHECK (strategy_mode IN ({_MODES}));"
    )


def downgrade() -> None:
    op.execute(
        "ALTER TABLE user_trades DROP CONSTRAINT IF EXISTS user_trades_strategy_mode_check;"
        "ALTER TABLE user_trades ADD CONSTRAINT user_trades_strategy_mode_check "
        f"CHECK (strategy_mode IN ({_OLD}));"
    )
