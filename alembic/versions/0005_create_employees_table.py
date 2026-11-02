"""Create employees table with 1-to-1 relationship to users.

Revision ID: 0005_create_employees
Revises: 0004_inventory_reservations
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "0005_create_employees"
down_revision: Union[str, None] = "0004_inventory_reservations"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "employees",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("age", sa.Integer(), nullable=False),
        sa.Column("location", sa.String(length=255), nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("user_id", name="uq_employees_user_id"),
    )
    op.create_index("ix_employees_name", "employees", ["name"])
    op.create_index("ix_employees_location", "employees", ["location"])
    op.create_index("ix_employees_created_at_id", "employees", ["created_at", "id"])


def downgrade() -> None:
    op.drop_index("ix_employees_created_at_id", table_name="employees")
    op.drop_index("ix_employees_location", table_name="employees")
    op.drop_index("ix_employees_name", table_name="employees")
    op.drop_table("employees")
