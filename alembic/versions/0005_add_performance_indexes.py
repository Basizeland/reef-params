"""add performance indexes on foreign keys

Revision ID: 0005_add_performance_indexes
Revises: 0004_add_additives_owner_user_id
Create Date: 2026-01-11

"""
from alembic import op
from sqlalchemy import inspect

# revision identifiers, used by Alembic.
revision = '0005_add_performance_indexes'
down_revision = '0004_add_additives_owner_user_id'
branch_labels = None
depends_on = None


def table_exists(table_name: str) -> bool:
    """Check if a table exists."""
    bind = op.get_bind()
    inspector = inspect(bind)
    return table_name in inspector.get_table_names()


def index_exists(index_name: str) -> bool:
    """Check if an index exists."""
    bind = op.get_bind()
    inspector = inspect(bind)
    # Check all tables for this index
    for table_name in inspector.get_table_names():
        indexes = inspector.get_indexes(table_name)
        if any(idx['name'] == index_name for idx in indexes):
            return True
    return False


def column_exists(table_name: str, column_name: str) -> bool:
    """Check if a column exists in a table."""
    if not table_exists(table_name):
        return False
    bind = op.get_bind()
    inspector = inspect(bind)
    columns = [col['name'] for col in inspector.get_columns(table_name)]
    return column_name in columns


def create_index_if_not_exists(index_name: str, table_name: str, columns: list) -> None:
    """Create an index only if the table exists, all columns exist, and index doesn't exist."""
    if not table_exists(table_name):
        return

    # Check if all columns exist
    for column in columns:
        if not column_exists(table_name, column):
            return

    # Check if index already exists
    if not index_exists(index_name):
        op.create_index(index_name, table_name, columns)


def upgrade():
    # Add indexes on foreign keys for better query performance
    # Only create indexes for tables that exist

    # Sessions table - queried on every request
    create_index_if_not_exists('idx_sessions_user_id', 'sessions', ['user_id'])
    create_index_if_not_exists('idx_sessions_session_token', 'sessions', ['session_token'])

    # Samples table - queried frequently for tank details
    create_index_if_not_exists('idx_samples_tank_id', 'samples', ['tank_id'])
    create_index_if_not_exists('idx_samples_taken_at', 'samples', ['taken_at'])

    # Sample values - queried for every sample
    create_index_if_not_exists('idx_sample_values_sample_id', 'sample_values', ['sample_id'])
    create_index_if_not_exists('idx_sample_values_parameter_id', 'sample_values', ['parameter_id'])

    # Targets - queried frequently for tank displays
    create_index_if_not_exists('idx_targets_tank_id', 'targets', ['tank_id'])

    # Dosing notifications
    create_index_if_not_exists('idx_dosing_notifications_tank_id', 'dosing_notifications', ['tank_id'])

    # Tank profiles
    create_index_if_not_exists('idx_tank_profiles_tank_id', 'tank_profiles', ['tank_id'])

    # User tanks (junction table)
    create_index_if_not_exists('idx_user_tanks_user_id', 'user_tanks', ['user_id'])
    create_index_if_not_exists('idx_user_tanks_tank_id', 'user_tanks', ['tank_id'])

    # Dosing log
    create_index_if_not_exists('idx_dosing_log_tank_id', 'dosing_log', ['tank_id'])
    create_index_if_not_exists('idx_dosing_log_created_at', 'dosing_log', ['created_at'])

    # Dosing entries
    create_index_if_not_exists('idx_dosing_entries_tank_id', 'dosing_entries', ['tank_id'])

    # Tank journal
    create_index_if_not_exists('idx_tank_journal_tank_id', 'tank_journal', ['tank_id'])
    create_index_if_not_exists('idx_tank_journal_created_at', 'tank_journal', ['created_at'])

    # Parameter definitions
    create_index_if_not_exists('idx_parameter_defs_user_id', 'parameter_defs', ['user_id'])

    # Audit log
    create_index_if_not_exists('idx_audit_log_user_id', 'audit_log', ['user_id'])
    create_index_if_not_exists('idx_audit_log_created_at', 'audit_log', ['created_at'])

    # Push subscriptions
    create_index_if_not_exists('idx_push_subscriptions_user_id', 'push_subscriptions', ['user_id'])

    # API tokens
    create_index_if_not_exists('idx_api_tokens_user_id', 'api_tokens', ['user_id'])
    create_index_if_not_exists('idx_api_tokens_token_hash', 'api_tokens', ['token_hash'])

    # ICP uploads
    create_index_if_not_exists('idx_icp_uploads_user_id', 'icp_uploads', ['user_id'])
    create_index_if_not_exists('idx_icp_uploads_tank_id', 'icp_uploads', ['tank_id'])
    create_index_if_not_exists('idx_icp_uploads_created_at', 'icp_uploads', ['created_at'])


def downgrade():
    # Remove indexes in reverse order (only if they exist)
    def drop_index_if_exists(index_name: str) -> None:
        """Drop an index only if it exists."""
        if index_exists(index_name):
            op.drop_index(index_name)

    drop_index_if_exists('idx_icp_uploads_created_at')
    drop_index_if_exists('idx_icp_uploads_tank_id')
    drop_index_if_exists('idx_icp_uploads_user_id')
    drop_index_if_exists('idx_api_tokens_token_hash')
    drop_index_if_exists('idx_api_tokens_user_id')
    drop_index_if_exists('idx_push_subscriptions_user_id')
    drop_index_if_exists('idx_audit_log_created_at')
    drop_index_if_exists('idx_audit_log_user_id')
    drop_index_if_exists('idx_parameter_defs_user_id')
    drop_index_if_exists('idx_tank_journal_created_at')
    drop_index_if_exists('idx_tank_journal_tank_id')
    drop_index_if_exists('idx_dosing_entries_tank_id')
    drop_index_if_exists('idx_dosing_log_created_at')
    drop_index_if_exists('idx_dosing_log_tank_id')
    drop_index_if_exists('idx_user_tanks_tank_id')
    drop_index_if_exists('idx_user_tanks_user_id')
    drop_index_if_exists('idx_tank_profiles_tank_id')
    drop_index_if_exists('idx_dosing_notifications_tank_id')
    drop_index_if_exists('idx_targets_tank_id')
    drop_index_if_exists('idx_sample_values_parameter_id')
    drop_index_if_exists('idx_sample_values_sample_id')
    drop_index_if_exists('idx_samples_taken_at')
    drop_index_if_exists('idx_samples_tank_id')
    drop_index_if_exists('idx_sessions_session_token')
    drop_index_if_exists('idx_sessions_user_id')
