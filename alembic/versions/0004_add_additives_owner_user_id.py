"""Add owner_user_id to additives.

Revision ID: 0004_add_additives_owner_user_id
Revises: 0003_add_test_kits_owner_user_id
Create Date: 2025-01-01 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "0004_add_additives_owner_user_id"
down_revision = "0003_add_test_kits_owner_user_id"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("additives") as batch_op:
        batch_op.add_column(sa.Column("owner_user_id", sa.Integer(), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table("additives") as batch_op:
        batch_op.drop_column("owner_user_id")
