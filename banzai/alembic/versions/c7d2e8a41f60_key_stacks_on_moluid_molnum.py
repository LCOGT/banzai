"""Key smartstacks on MOLUID and MOLNUM.

Revision ID: c7d2e8a41f60
Revises: 9a4e2f7c1d3b
Create Date: 2026-10-30 12:00:00.000000

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = 'c7d2e8a41f60'
down_revision: Union[str, Sequence[str], None] = '9a4e2f7c1d3b'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

# Stack workers should be stopped while this runs. The tables are rebuilt rather than altered so the
# migration does not depend on the constraint names of tables that create_db may have created.
UNKNOWN_MOLNUM = 0


def _read_rows(bind, table_name):
    table = sa.Table(table_name, sa.MetaData(), autoload_with=bind)
    return [dict(row) for row in bind.execute(sa.select(table)).mappings()]


def _stack_columns(key_columns):
    return [
        *key_columns,
        sa.Column('camera', sa.String(length=50), nullable=True),
        sa.Column('frmtotal', sa.Integer(), nullable=True),
        sa.Column('status', sa.String(length=20), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.Column('last_stackframe_at', sa.DateTime(), nullable=True),
        sa.Column('completed_at', sa.DateTime(), nullable=True),
        sa.Column('finalize_attempts', sa.Integer(), nullable=True),
        sa.Column('next_attempt_at', sa.DateTime(), nullable=True),
        sa.Column('last_preview_count', sa.Integer(), nullable=True),
    ]


def _stackframe_columns(key_columns):
    return [
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        *key_columns,
        sa.Column('stack_num', sa.Integer(), nullable=False),
        sa.Column('filepath', sa.String(length=255), nullable=False),
        sa.Column('dateobs', sa.DateTime(), nullable=True),
        sa.Column('is_last', sa.Boolean(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=True),
    ]


def _rebuild_tables(stacks, stackframes, key, unique_name):
    """Recreate both tables keyed on ``key``, restoring the given rows with fresh stackframe ids."""
    op.drop_table('stackframes')
    op.drop_table('stacks')
    key_columns = [sa.Column('moluid', sa.String(length=100), nullable=False)]
    if 'molnum' in key:
        key_columns.append(sa.Column('molnum', sa.Integer(), autoincrement=False, nullable=False))
    stacks_table = op.create_table(
        'stacks',
        *_stack_columns(key_columns),
        sa.PrimaryKeyConstraint(*key),
    )
    op.create_index(op.f('ix_stacks_camera'), 'stacks', ['camera'], unique=False)
    frame_key_columns = [sa.Column(column.name, column.type, nullable=False) for column in key_columns]
    stackframes_table = op.create_table(
        'stackframes',
        *_stackframe_columns(frame_key_columns),
        sa.ForeignKeyConstraint(key, [f'stacks.{column}' for column in key], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint(*key, 'stack_num', name=unique_name),
    )
    op.create_index(op.f('ix_stackframes_moluid'), 'stackframes', ['moluid'], unique=False)
    if stacks:
        op.bulk_insert(stacks_table, stacks)
    if stackframes:
        op.bulk_insert(stackframes_table, stackframes)


def upgrade() -> None:
    """Upgrade schema."""
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    # Non-site databases may not have these tables, and create_db can already have built them from the current models.
    if not inspector.has_table('stacks') or 'molnum' in {column['name'] for column in inspector.get_columns('stacks')}:
        return

    stacks = [{**row, 'molnum': UNKNOWN_MOLNUM} for row in _read_rows(bind, 'stacks')]
    stackframes = [{**row, 'molnum': UNKNOWN_MOLNUM} for row in _read_rows(bind, 'stackframes')]
    for row in stackframes:
        del row['id']
    _rebuild_tables(stacks, stackframes, ['moluid', 'molnum'], 'uq_stackframe_moluid_molnum_num')


def downgrade() -> None:
    """Downgrade schema, keeping the lowest MOLNUM of each MOLUID because the old key allows only one."""
    bind = op.get_bind()
    kept_molnums = {}
    for row in _read_rows(bind, 'stacks'):
        kept_molnums[row['moluid']] = min(row['molnum'], kept_molnums.get(row['moluid'], row['molnum']))

    stacks = []
    for row in _read_rows(bind, 'stacks'):
        if row['molnum'] == kept_molnums[row['moluid']]:
            del row['molnum']
            stacks.append(row)
    stackframes = []
    for row in _read_rows(bind, 'stackframes'):
        if row['molnum'] == kept_molnums.get(row['moluid']):
            del row['molnum']
            del row['id']
            stackframes.append(row)
    _rebuild_tables(stacks, stackframes, ['moluid'], 'uq_stackframe_moluid_num')
