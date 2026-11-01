"""Add global inventory reservation state and expiry records.

Revision ID: 0004_inventory_reservations
Revises: 0003_outbox_order
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "0004_inventory_reservations"
down_revision: Union[str, None] = "0003_outbox_order"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "products",
        sa.Column(
            "reserved_quantity",
            sa.Integer(),
            server_default="0",
            nullable=False,
        ),
    )
    op.create_check_constraint(
        "reserved_quantity_lte_stock",
        "products",
        "reserved_quantity >= 0 AND reserved_quantity <= stock",
    )
    op.create_table(
        "inventory_reservations",
        sa.Column("id", sa.Uuid(as_uuid=True), nullable=False),
        sa.Column("event_id", sa.String(length=255), nullable=False),
        sa.Column("reservation_id", sa.String(length=255), nullable=False),
        sa.Column("order_id", sa.String(length=255), nullable=False),
        sa.Column("product_id", sa.Uuid(as_uuid=True), nullable=False),
        sa.Column("quantity", sa.Integer(), nullable=False),
        sa.Column("location_id", sa.String(length=255), nullable=True),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint("quantity > 0", name="quantity_positive"),
        sa.CheckConstraint(
            "status IN ('reserved', 'rejected', 'expired')",
            name="valid_status",
        ),
        sa.ForeignKeyConstraint(
            ["product_id"],
            ["products.id"],
            name="fk_inventory_reservations_product_id_products",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_inventory_reservations"),
        sa.UniqueConstraint(
            "event_id",
            name="uq_inventory_reservations_event_id",
        ),
        sa.UniqueConstraint(
            "reservation_id",
            name="uq_inventory_reservations_reservation_id",
        ),
    )
    op.create_index(
        "ix_inventory_reservations_expiration",
        "inventory_reservations",
        ["status", "expires_at"],
        unique=False,
    )
    op.create_index(
        "ix_inventory_reservations_created_at",
        "inventory_reservations",
        ["created_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_inventory_reservations_expiration",
        table_name="inventory_reservations",
    )
    op.drop_index(
        "ix_inventory_reservations_created_at",
        table_name="inventory_reservations",
    )
    op.drop_table("inventory_reservations")
    op.drop_constraint(
        "ck_products_reserved_quantity_lte_stock",
        "products",
        type_="check",
    )
    op.drop_column("products", "reserved_quantity")
