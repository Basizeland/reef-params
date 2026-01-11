"""Add all tank_profiles columns for dosing and volume tracking.

Revision ID: 0006_add_tank_profiles_columns
Revises: 0005_add_performance_indexes
Create Date: 2026-01-11

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


# revision identifiers, used by Alembic.
revision = '0006_add_tank_profiles_columns'
down_revision = '0005_add_performance_indexes'
branch_labels = None
depends_on = None


def column_exists(table_name: str, column_name: str) -> bool:
    """Check if a column exists in a table."""
    bind = op.get_bind()
    inspector = inspect(bind)
    columns = [col['name'] for col in inspector.get_columns(table_name)]
    return column_name in columns


def upgrade():
    """Add all tank_profiles columns that were previously added at runtime."""
    # Define all columns to add
    columns_to_add = [
        ('volume_l', sa.Float()),
        ('net_percent', sa.Float()),
        ('dosing_mode', sa.Text()),
        ('all_in_one_solution', sa.Text()),
        ('all_in_one_daily_ml', sa.Float()),
        ('all_in_one_container_ml', sa.Float()),
        ('all_in_one_remaining_ml', sa.Float()),
        ('alk_solution', sa.Text()),
        ('alk_daily_ml', sa.Float()),
        ('alk_container_ml', sa.Float()),
        ('alk_remaining_ml', sa.Float()),
        ('ca_solution', sa.Text()),
        ('ca_daily_ml', sa.Float()),
        ('ca_container_ml', sa.Float()),
        ('ca_remaining_ml', sa.Float()),
        ('mg_solution', sa.Text()),
        ('mg_daily_ml', sa.Float()),
        ('mg_container_ml', sa.Float()),
        ('mg_remaining_ml', sa.Float()),
        ('nitrate_solution', sa.Text()),
        ('nitrate_daily_ml', sa.Float()),
        ('nitrate_container_ml', sa.Float()),
        ('nitrate_remaining_ml', sa.Float()),
        ('phosphate_solution', sa.Text()),
        ('phosphate_daily_ml', sa.Float()),
        ('phosphate_container_ml', sa.Float()),
        ('phosphate_remaining_ml', sa.Float()),
        ('trace_solution', sa.Text()),
        ('trace_daily_ml', sa.Float()),
        ('trace_container_ml', sa.Float()),
        ('trace_remaining_ml', sa.Float()),
        ('nopox_daily_ml', sa.Float()),
        ('nopox_container_ml', sa.Float()),
        ('nopox_remaining_ml', sa.Float()),
        ('calcium_reactor_daily_ml', sa.Float()),
        ('calcium_reactor_effluent_dkh', sa.Float()),
        ('kalk_solution', sa.Text()),
        ('kalk_daily_ml', sa.Float()),
        ('kalk_container_ml', sa.Float()),
        ('kalk_remaining_ml', sa.Float()),
        ('use_all_in_one', sa.Integer()),
        ('use_alk', sa.Integer()),
        ('use_ca', sa.Integer()),
        ('use_mg', sa.Integer()),
        ('use_nitrate', sa.Integer()),
        ('use_phosphate', sa.Integer()),
        ('use_trace', sa.Integer()),
        ('use_nopox', sa.Integer()),
        ('use_calcium_reactor', sa.Integer()),
        ('use_kalkwasser', sa.Integer()),
        ('dosing_container_updated_at', sa.Text()),
        ('dosing_low_days', sa.Float()),
    ]

    # Add columns only if they don't already exist
    with op.batch_alter_table("tank_profiles") as batch_op:
        for col_name, col_type in columns_to_add:
            if not column_exists("tank_profiles", col_name):
                batch_op.add_column(sa.Column(col_name, col_type, nullable=True))


def downgrade():
    """Remove all tank_profiles dosing columns."""
    columns_to_drop = [
        'volume_l', 'net_percent', 'dosing_mode',
        'all_in_one_solution', 'all_in_one_daily_ml', 'all_in_one_container_ml', 'all_in_one_remaining_ml',
        'alk_solution', 'alk_daily_ml', 'alk_container_ml', 'alk_remaining_ml',
        'ca_solution', 'ca_daily_ml', 'ca_container_ml', 'ca_remaining_ml',
        'mg_solution', 'mg_daily_ml', 'mg_container_ml', 'mg_remaining_ml',
        'nitrate_solution', 'nitrate_daily_ml', 'nitrate_container_ml', 'nitrate_remaining_ml',
        'phosphate_solution', 'phosphate_daily_ml', 'phosphate_container_ml', 'phosphate_remaining_ml',
        'trace_solution', 'trace_daily_ml', 'trace_container_ml', 'trace_remaining_ml',
        'nopox_daily_ml', 'nopox_container_ml', 'nopox_remaining_ml',
        'calcium_reactor_daily_ml', 'calcium_reactor_effluent_dkh',
        'kalk_solution', 'kalk_daily_ml', 'kalk_container_ml', 'kalk_remaining_ml',
        'use_all_in_one', 'use_alk', 'use_ca', 'use_mg', 'use_nitrate', 'use_phosphate',
        'use_trace', 'use_nopox', 'use_calcium_reactor', 'use_kalkwasser',
        'dosing_container_updated_at', 'dosing_low_days',
    ]

    with op.batch_alter_table("tank_profiles") as batch_op:
        for col_name in columns_to_drop:
            if column_exists("tank_profiles", col_name):
                batch_op.drop_column(col_name)
