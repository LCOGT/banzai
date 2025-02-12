"""Add calibrations block info, proposal id, and public date to CalibrationImage

Revision ID: 8e35c490a971
Revises: 
Create Date: 2025-01-31 12:19:03.407606

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


# revision identifiers, used by Alembic.
revision: str = '8e35c490a971'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind.engine)
    columns = [col['name'] for col in inspector.get_columns('calimages')]

    if 'blockid' not in columns:
        op.add_column('calimages', sa.Column('blockid', sa.Integer(), nullable=True))
    if 'proposal' not in columns:
        op.add_column('calimages', sa.Column('proposal', sa.String(50), nullable=True))
    if 'public_date' not in columns:
        op.add_column('calimages', sa.Column('public_date', sa.DateTime(), nullable=True))


def downgrade() -> None:
    op.drop_column('calimages', 'blockid')
    op.drop_column('calimages', 'proposal')
    op.drop_column('calimages', 'public_date')
