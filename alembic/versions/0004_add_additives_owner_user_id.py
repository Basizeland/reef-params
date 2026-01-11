"""Add owner_user_id to additives.

Revision ID: 0004_add_additives_owner_user_id
Revises: 0003_add_test_kits_owner_user_id
Create Date: 2025-01-01 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


# revision identifiers, used by Alembic.
revision = "0004_add_additives_owner_user_id"
down_revision = "0003_add_test_kits_owner_user_id"
branch_labels = None
depends_on = None


def column_exists(table_name: str, column_name: str) -> bool:
    """Check if a column exists in a table."""
    bind = op.get_bind()
    inspector = inspect(bind)
    try:
        columns = [col['name'] for col in inspector.get_columns(table_name)]
        return column_name in columns
    except Exception:
        return False


def upgrade() -> None:
    if not column_exists("additives", "owner_user_id"):
        with op.batch_alter_table("additives") as batch_op:
            batch_op.add_column(sa.Column("owner_user_id", sa.Integer(), nullable=True))


def downgrade() -> None:
    if column_exists("additives", "owner_user_id"):
        with op.batch_alter_table("additives") as batch_op:
            batch_op.drop_column("owner_user_id")
