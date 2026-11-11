"""Rename the smartstack key column from moluid to stack_id.

Revision ID: c3d81e5a7f42
Revises: 9a4e2f7c1d3b
Create Date: 2026-11-10 00:00:00.000000

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = 'c3d81e5a7f42'
down_revision: Union[str, Sequence[str], None] = '9a4e2f7c1d3b'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _rename(old: str, new: str) -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    # create_db(site_deploy=True) builds these tables from the models, so they may already use the new name.
    for table in ('stacks', 'stackframes'):
        if not inspector.has_table(table):
            continue
        if old not in {column['name'] for column in inspector.get_columns(table)}:
            continue
        if table == 'stackframes':
            index_names = {index['name'] for index in inspector.get_indexes(table)}
            if f'ix_stackframes_{old}' in index_names:
                op.drop_index(f'ix_stackframes_{old}', table_name=table)
        # RENAME COLUMN also rewrites the foreign key and unique constraint definitions.
        op.execute(f'ALTER TABLE {table} RENAME COLUMN {old} TO {new}')
        if table == 'stackframes':
            op.create_index(f'ix_stackframes_{new}', table, [new], unique=False)
            # SQLite cannot rename a constraint; there the old name stays, which is harmless.
            if bind.dialect.name == 'postgresql':
                op.execute(f'ALTER TABLE stackframes RENAME CONSTRAINT uq_stackframe_{old}_num '
                           f'TO uq_stackframe_{new}_num')
                op.execute(f'ALTER TABLE stackframes RENAME CONSTRAINT stackframes_{old}_fkey '
                           f'TO stackframes_{new}_fkey')


def upgrade() -> None:
    """Upgrade schema."""
    _rename('moluid', 'stack_id')


def downgrade() -> None:
    """Downgrade schema."""
    _rename('stack_id', 'moluid')
