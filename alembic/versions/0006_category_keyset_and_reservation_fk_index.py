"""Add missing keyset/foreign-key indexes.

Adds the (created_at, id) composite keyset-pagination index to `categories`
(CategoryService.list_keyset() already seeks on it, but the index was never
declared, forcing a full sort on every category list page) and adds the
missing index on `inventory_reservations.product_id` (a foreign key without
an index forces a full-table scan whenever a product is looked up or deleted).

Revision ID: 0006_category_keyset_reservation_fk_index
Revises: 0005_create_employees
"""

from typing import Sequence, Union

from alembic import op

revision: str = "0006_category_keyset_reservation_fk_index"
down_revision: Union[str, None] = "0005_create_employees"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_index(
        "ix_categories_created_at_id", "categories", ["created_at", "id"]
    )
    op.create_index(
        "ix_inventory_reservations_product_id", "inventory_reservations", ["product_id"]
    )


def downgrade() -> None:
    op.drop_index("ix_inventory_reservations_product_id", table_name="inventory_reservations")
    op.drop_index("ix_categories_created_at_id", table_name="categories")
