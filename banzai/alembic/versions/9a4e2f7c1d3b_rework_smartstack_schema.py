"""Rework smartstack schema.

Revision ID: 9a4e2f7c1d3b
Revises: 5b5b96094c33
Create Date: 2026-07-06 21:45:00.000000

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = '9a4e2f7c1d3b'
down_revision: Union[str, Sequence[str], None] = '5b5b96094c33'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    # These tables are also created directly from the SQLAlchemy models by create_db(site_deploy=True),
    # so this migration must tolerate either starting state.
    if inspector.has_table('subframes'):
        op.drop_table('subframes')
    if not inspector.has_table('stacks'):
        op.create_table(
            'stacks',
            sa.Column('moluid', sa.String(length=100), nullable=False),
            sa.Column('camera', sa.String(length=50), nullable=True),
            sa.Column('frmtotal', sa.Integer(), nullable=True),
            sa.Column('status', sa.String(length=20), nullable=True),
            sa.Column('created_at', sa.DateTime(), nullable=True),
            sa.Column('last_stackframe_at', sa.DateTime(), nullable=True),
            sa.Column('completed_at', sa.DateTime(), nullable=True),
            sa.Column('finalize_attempts', sa.Integer(), nullable=True),
            sa.Column('next_attempt_at', sa.DateTime(), nullable=True),
            sa.Column('last_preview_count', sa.Integer(), nullable=True),
            sa.PrimaryKeyConstraint('moluid'),
        )
        op.create_index(op.f('ix_stacks_camera'), 'stacks', ['camera'], unique=False)
    if not inspector.has_table('stackframes'):
        op.create_table(
            'stackframes',
            sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
            sa.Column('moluid', sa.String(length=100), nullable=False),
            sa.Column('stack_num', sa.Integer(), nullable=False),
            sa.Column('filepath', sa.String(length=255), nullable=False),
            sa.Column('dateobs', sa.DateTime(), nullable=True),
            sa.Column('is_last', sa.Boolean(), nullable=True),
            sa.Column('instrument_enqueue_timestamp', sa.BigInteger(), nullable=True),
            sa.Column('created_at', sa.DateTime(), nullable=True),
            sa.ForeignKeyConstraint(['moluid'], ['stacks.moluid'], ondelete='CASCADE'),
            sa.PrimaryKeyConstraint('id'),
            sa.UniqueConstraint('moluid', 'stack_num', name='uq_stackframe_moluid_num'),
        )
        op.create_index(op.f('ix_stackframes_moluid'), 'stackframes', ['moluid'], unique=False)


def downgrade() -> None:
    """Downgrade schema."""
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    if inspector.has_table('stackframes'):
        op.drop_index(op.f('ix_stackframes_moluid'), table_name='stackframes')
        op.drop_table('stackframes')
    if inspector.has_table('stacks'):
        op.drop_index(op.f('ix_stacks_camera'), table_name='stacks')
        op.drop_table('stacks')
    if not inspector.has_table('subframes'):
        op.create_table(
            'subframes',
            sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
            sa.Column('moluid', sa.String(length=100), nullable=False),
            sa.Column('stack_num', sa.Integer(), nullable=False),
            sa.Column('frmtotal', sa.Integer(), nullable=False),
            sa.Column('camera', sa.String(length=50), nullable=False),
            sa.Column('filepath', sa.String(length=255), nullable=False),
            sa.Column('is_last', sa.Boolean(), nullable=True),
            sa.Column('status', sa.String(length=20), nullable=False),
            sa.Column('dateobs', sa.DateTime(), nullable=True),
            sa.Column('created_at', sa.DateTime(), nullable=True),
            sa.Column('completed_at', sa.DateTime(), nullable=True),
            sa.PrimaryKeyConstraint('id'),
            sa.UniqueConstraint('moluid', 'stack_num', name='uq_subframe_moluid_num'),
        )
        op.create_index(op.f('ix_subframes_moluid'), 'subframes', ['moluid'], unique=False)
        op.create_index(op.f('ix_subframes_camera'), 'subframes', ['camera'], unique=False)
