"""Add calibrations block info, proposal id, and public date to CalibrationImage

Revision ID: 8e35c490a971
Revises: 
Create Date: 2025-01-31 12:19:03.407606

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect
from sqlalchemy.sql import text
import requests
import os
import datetime


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
    if os.getenv("AUTH_TOKEN") is not None:
        auth_header = {'Authorization': f'Token {os.environ["AUTH_TOKEN"]}'}
    else:
        auth_header = None

    connection = op.get_bind()
    rows = connection.execute(text("SELECT id, frameid, filename FROM calimages"))
    for row in rows:
        params = {'basename_exact': row.filename.replace('.fz', '').replace('.fits', '')}
        request_results = requests.get('https://archive-api.lco.global/frames/', params=params, headers=auth_header)
        request_results = request_results.json()['results']
        if len(request_results) == 0:
            continue

        blockid = request_results[0]['BLKUID']
        proposal = request_results[0]['proposal_id']
        date_formats = ['%Y-%m-%dT%H:%M:%S.%fZ', '%Y-%m-%dT%H:%M:%SZ']
        for date_format in date_formats:
            try:
                public_date = datetime.datetime.strptime(request_results[0]['public_date'], date_format)
                break
            except ValueError:
                continue
        values = {'id': row.id, 'public_date': public_date, 'proposal': proposal, 'blockid': blockid}
        query_str = 'blockid = :blockid, proposal = :proposal, public_date = :public_date'
        if row.frameid is None:
            query_str += ', frameid = :frameid'
            values['frameid'] = request_results[0]['id']
        connection.execute(
            text(f"UPDATE calimages SET {query_str} WHERE id = :id"),
            values
        )


def downgrade() -> None:
    op.drop_column('calimages', 'blockid')
    op.drop_column('calimages', 'proposal')
    op.drop_column('calimages', 'public_date')
