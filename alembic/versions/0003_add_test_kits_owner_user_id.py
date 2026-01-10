"""Add owner_user_id to test_kits.

Revision ID: 0003_add_test_kits_owner_user_id
Revises: 0002_add_icp_uploads
Create Date: 2025-01-01 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "0003_add_test_kits_owner_user_id"
down_revision = "0002_add_icp_uploads"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("test_kits") as batch_op:
        batch_op.add_column(sa.Column("owner_user_id", sa.Integer(), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table("test_kits") as batch_op:
        batch_op.drop_column("owner_user_id")
