"""add performance indexes on foreign keys

Revision ID: 0005
Revises: 0004
Create Date: 2026-01-11

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = '0005'
down_revision = '0004'
branch_labels = None
depends_on = None


def upgrade():
    # Add indexes on foreign keys for better query performance
    # These are heavily queried columns that currently lack indexes

    # Sessions table - queried on every request
    op.create_index('idx_sessions_user_id', 'sessions', ['user_id'])
    op.create_index('idx_sessions_session_token', 'sessions', ['session_token'])

    # Samples table - queried frequently for tank details
    op.create_index('idx_samples_tank_id', 'samples', ['tank_id'])
    op.create_index('idx_samples_taken_at', 'samples', ['taken_at'])

    # Sample values - queried for every sample
    op.create_index('idx_sample_values_sample_id', 'sample_values', ['sample_id'])
    op.create_index('idx_sample_values_parameter_id', 'sample_values', ['parameter_id'])

    # Targets - queried frequently for tank displays
    op.create_index('idx_targets_tank_id', 'targets', ['tank_id'])

    # Dosing notifications
    op.create_index('idx_dosing_notifications_tank_id', 'dosing_notifications', ['tank_id'])

    # Tank profiles
    op.create_index('idx_tank_profiles_tank_id', 'tank_profiles', ['tank_id'])

    # User tanks (junction table)
    op.create_index('idx_user_tanks_user_id', 'user_tanks', ['user_id'])
    op.create_index('idx_user_tanks_tank_id', 'user_tanks', ['tank_id'])

    # Dosing log
    op.create_index('idx_dosing_log_tank_id', 'dosing_log', ['tank_id'])
    op.create_index('idx_dosing_log_created_at', 'dosing_log', ['created_at'])

    # Dosing entries
    op.create_index('idx_dosing_entries_tank_id', 'dosing_entries', ['tank_id'])

    # Tank journal
    op.create_index('idx_tank_journal_tank_id', 'tank_journal', ['tank_id'])
    op.create_index('idx_tank_journal_created_at', 'tank_journal', ['created_at'])

    # Parameter definitions
    op.create_index('idx_parameter_defs_user_id', 'parameter_defs', ['user_id'])

    # Audit log
    op.create_index('idx_audit_log_user_id', 'audit_log', ['user_id'])
    op.create_index('idx_audit_log_created_at', 'audit_log', ['created_at'])

    # Push subscriptions
    op.create_index('idx_push_subscriptions_user_id', 'push_subscriptions', ['user_id'])

    # API tokens
    op.create_index('idx_api_tokens_user_id', 'api_tokens', ['user_id'])
    op.create_index('idx_api_tokens_token_hash', 'api_tokens', ['token_hash'])

    # ICP uploads
    op.create_index('idx_icp_uploads_user_id', 'icp_uploads', ['user_id'])
    op.create_index('idx_icp_uploads_tank_id', 'icp_uploads', ['tank_id'])
    op.create_index('idx_icp_uploads_created_at', 'icp_uploads', ['created_at'])


def downgrade():
    # Remove indexes in reverse order
    op.drop_index('idx_icp_uploads_created_at')
    op.drop_index('idx_icp_uploads_tank_id')
    op.drop_index('idx_icp_uploads_user_id')
    op.drop_index('idx_api_tokens_token_hash')
    op.drop_index('idx_api_tokens_user_id')
    op.drop_index('idx_push_subscriptions_user_id')
    op.drop_index('idx_audit_log_created_at')
    op.drop_index('idx_audit_log_user_id')
    op.drop_index('idx_parameter_defs_user_id')
    op.drop_index('idx_tank_journal_created_at')
    op.drop_index('idx_tank_journal_tank_id')
    op.drop_index('idx_dosing_entries_tank_id')
    op.drop_index('idx_dosing_log_created_at')
    op.drop_index('idx_dosing_log_tank_id')
    op.drop_index('idx_user_tanks_tank_id')
    op.drop_index('idx_user_tanks_user_id')
    op.drop_index('idx_tank_profiles_tank_id')
    op.drop_index('idx_dosing_notifications_tank_id')
    op.drop_index('idx_targets_tank_id')
    op.drop_index('idx_sample_values_parameter_id')
    op.drop_index('idx_sample_values_sample_id')
    op.drop_index('idx_samples_taken_at')
    op.drop_index('idx_samples_tank_id')
    op.drop_index('idx_sessions_session_token')
    op.drop_index('idx_sessions_user_id')
