"""Index outbox events by aggregate to preserve dispatch ordering.

Revision ID: 0003_outbox_order
Revises: 0002_outbox
"""

from typing import Sequence, Union

from alembic import op

revision: str = "0003_outbox_order"
down_revision: Union[str, None] = "0002_outbox"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_index(
        "ix_outbox_events_aggregate_pending",
        "outbox_events",
        ["topic", "event_key", "published_at", "created_at", "id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_outbox_events_aggregate_pending",
        table_name="outbox_events",
    )
