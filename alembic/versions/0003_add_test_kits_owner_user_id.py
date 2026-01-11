"""Add owner_user_id to test_kits.

Revision ID: 0003_add_test_kits_owner_user_id
Revises: 0002_add_icp_uploads
Create Date: 2025-01-01 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


# revision identifiers, used by Alembic.
revision = "0003_add_test_kits_owner_user_id"
down_revision = "0002_add_icp_uploads"
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
    bind = op.get_bind()
    inspector = inspect(bind)

    # Check if test_kits table exists
    if "test_kits" not in inspector.get_table_names():
        print("MIGRATION 0003: test_kits table does not exist, skipping")
        return

    # Check if column already exists
    columns = [col['name'] for col in inspector.get_columns("test_kits")]
    print(f"MIGRATION 0003: test_kits columns: {columns}")

    if "owner_user_id" in columns:
        print("MIGRATION 0003: owner_user_id column already exists, skipping")
        return

    print("MIGRATION 0003: Adding owner_user_id column to test_kits")
    op.add_column("test_kits", sa.Column("owner_user_id", sa.Integer(), nullable=True))
    print("MIGRATION 0003: Successfully added owner_user_id column")


def downgrade() -> None:
    if column_exists("test_kits", "owner_user_id"):
        with op.batch_alter_table("test_kits") as batch_op:
            batch_op.drop_column("owner_user_id")
