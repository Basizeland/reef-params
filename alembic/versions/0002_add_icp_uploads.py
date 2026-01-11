"""add icp_uploads table

Revision ID: 0002_add_icp_uploads
Revises: 0001_initial
Create Date: 2026-01-08 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


# revision identifiers, used by Alembic.
revision = "0002_add_icp_uploads"
down_revision = "0001_initial"
branch_labels = None
depends_on = None


def table_exists(table_name: str) -> bool:
    """Check if a table exists."""
    bind = op.get_bind()
    inspector = inspect(bind)
    return table_name in inspector.get_table_names()


def upgrade() -> None:
    if not table_exists("icp_uploads"):
        op.create_table(
            "icp_uploads",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("user_id", sa.Integer(), nullable=True),
            sa.Column("tank_id", sa.Integer(), nullable=True),
            sa.Column("filename", sa.Text(), nullable=True),
            sa.Column("content_type", sa.Text(), nullable=True),
            sa.Column("r2_key", sa.Text(), nullable=True),
            sa.Column("created_at", sa.Text(), nullable=True),
        )


def downgrade() -> None:
    if table_exists("icp_uploads"):
        op.drop_table("icp_uploads")
