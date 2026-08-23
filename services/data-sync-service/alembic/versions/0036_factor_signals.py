"""0036 factor_signals (2026-08-23).

New table factor_signals: daily morphology / microstructure signals (independent of S-3).
First signal: strong_scoop_exhaustion — exhaustion top in strong stocks (scoop pullback
in a strong stock that breaks down on distribution volume).

Validated out-of-sample 82-92% hit, +10-15% R (short side) across 2021-2026 chunks
(see docs/designs/pattern-factor-validation.md §2.4). Used by the new Factors page.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0036_factor_signals"
down_revision: str | Sequence[str] | None = "0020_cn_extra_data"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS factor_signals (
            trade_date   DATE NOT NULL,
            symbol       TEXT NOT NULL,
            factor_name  TEXT NOT NULL,
            direction    TEXT NOT NULL,
            entry_price  DOUBLE PRECISION NOT NULL,
            target_price DOUBLE PRECISION NOT NULL,
            stop_price   DOUBLE PRECISION NOT NULL,
            probability  DOUBLE PRECISION NOT NULL,
            hold_days    INTEGER NOT NULL DEFAULT 20,
            status       TEXT NOT NULL DEFAULT 'pending',
            ret60        DOUBLE PRECISION,
            vol_ratio    DOUBLE PRECISION,
            industry     TEXT,
            board        TEXT,
            symbol_name  TEXT,
            updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (trade_date, symbol, factor_name)
        );
        CREATE INDEX IF NOT EXISTS idx_factor_signals_date ON factor_signals(trade_date DESC);
        CREATE INDEX IF NOT EXISTS idx_factor_signals_factor ON factor_signals(factor_name, trade_date DESC);
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS factor_signals;")
