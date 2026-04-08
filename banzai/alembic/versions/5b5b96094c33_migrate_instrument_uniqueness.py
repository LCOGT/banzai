"""Migrate instrument uniqueness.

WARNING: This migration is data lossy. The upgrade drops the blockid, proposal,
public_date, telescope, schedulable, and enclosure columns permanently. The
downgrade will restore these columns but cannot recover the original data.

Revision ID: 5b5b96094c33
Revises:
Create Date: 2026-04-08 14:47:57.894924

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '5b5b96094c33'
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.drop_column('calimages', 'blockid')
    op.drop_column('calimages', 'proposal')
    op.drop_column('calimages', 'public_date')
    op.drop_index(op.f('ix_instruments_enclosure'), table_name='instruments')
    op.drop_index(op.f('ix_instruments_telescope'), table_name='instruments')
    op.drop_constraint('instrument_constraint', 'instruments', type_='unique')
    op.create_unique_constraint(
        'instrument_constraint', 'instruments', ['site', 'camera', 'name']
    )
    op.drop_column('instruments', 'telescope')
    op.drop_column('instruments', 'schedulable')
    op.drop_column('instruments', 'enclosure')
    op.alter_column(
        'sites', 'latitude',
        existing_type=sa.REAL(),
        type_=sa.Float(),
        existing_nullable=True
    )
    op.alter_column(
        'sites', 'longitude',
        existing_type=sa.REAL(),
        type_=sa.Float(),
        existing_nullable=True
    )
    op.alter_column(
        'sites', 'elevation',
        existing_type=sa.REAL(),
        type_=sa.Float(),
        existing_nullable=True
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.alter_column(
        'sites', 'elevation',
        existing_type=sa.Float(),
        type_=sa.REAL(),
        existing_nullable=True
    )
    op.alter_column(
        'sites', 'longitude',
        existing_type=sa.Float(),
        type_=sa.REAL(),
        existing_nullable=True
    )
    op.alter_column(
        'sites', 'latitude',
        existing_type=sa.Float(),
        type_=sa.REAL(),
        existing_nullable=True
    )
    op.add_column('instruments', sa.Column(
        'enclosure', sa.VARCHAR(length=20), autoincrement=False, nullable=True))
    op.add_column('instruments', sa.Column(
        'schedulable', sa.BOOLEAN(), autoincrement=False, nullable=True))
    op.add_column('instruments', sa.Column(
        'telescope', sa.VARCHAR(length=20), autoincrement=False, nullable=True))
    op.drop_constraint('instrument_constraint', 'instruments', type_='unique')
    op.create_unique_constraint(
        'instrument_constraint', 'instruments',
        ['site', 'camera', 'enclosure', 'telescope'],
        postgresql_nulls_not_distinct=False
    )
    op.create_index(op.f('ix_instruments_telescope'), 'instruments', ['telescope'], unique=False)
    op.create_index(op.f('ix_instruments_enclosure'), 'instruments', ['enclosure'], unique=False)
    op.add_column('calimages', sa.Column(
        'public_date', postgresql.TIMESTAMP(), autoincrement=False, nullable=True))
    op.add_column('calimages', sa.Column(
        'proposal', sa.VARCHAR(length=50), autoincrement=False, nullable=True))
    op.add_column('calimages', sa.Column(
        'blockid', sa.INTEGER(), autoincrement=False, nullable=True))
