"""Migration of past data for blockid, public-date, and proposalid

Revision ID: 51b98e26cc50
Revises: 8e35c490a971
Create Date: 2025-02-10 11:08:49.733571

"""
from typing import Sequence, Union

from alembic import op
from sqlalchemy.sql import text
import os
import requests
import datetime

# revision identifiers, used by Alembic.
revision: str = '51b98e26cc50'
down_revision: Union[str, None] = '8e35c490a971'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = '8e35c490a971'


def parse_date(date_to_parse):
    date_formats = ['%Y-%m-%dT%H:%M:%S.%fZ', '%Y-%m-%dT%H:%M:%SZ', '%Y-%m-%d %H:%M:%S.%f']
    for date_format in date_formats:
        try:
            parsed_date = datetime.datetime.strptime(date_to_parse, date_format)
            break
        except ValueError:
            continue
    return parsed_date


def upgrade() -> None:
    if os.getenv("AUTH_TOKEN") is not None:
        auth_header = {'Authorization': f'Token {os.environ["AUTH_TOKEN"]}'}
    else:
        auth_header = None

    connection = op.get_bind()

    query_str = "SELECT id, frameid, filename,type, dateobs FROM calimages"
    # Get all of the frames from all of the cameras for all obstypes in this batch
    rows = connection.execute(text(query_str)).fetchall()
    for row in rows:
        basename = row.filename.replace('.fits', '').replace('.fz', '')
        if row.frameid is not None:
            request_results = requests.get(f'https://archive-api.lco.global/frames/{row.frameid}', headers=auth_header)
            request_results = request_results.json()
        else:
            params = {'basename_exact': basename, 'start': parse_date(row.dateobs) - datetime.timedelta(days=1),
                      'end': parse_date(row.dateobs) + datetime.timedelta(days=1)}
            request_results = requests.get('https://archive-api.lco.global/frames/', params=params, headers=auth_header)
            if len(request_results.json()['results']) == 0:
                continue
            request_results = request_results.json()['results'][0]

        values = {'id': row.id,
                  'public_date': parse_date(request_results['public_date']),
                  'proposal': request_results['proposal_id'],
                  'blockid': request_results['BLKUID']}
        query_str = 'blockid = :blockid, proposal = :proposal, public_date = :public_date'
        if row.frameid is None:
            query_str += ', frameid = :frameid'
            values['frameid'] = request_results['id']
        connection.execute(text(f"UPDATE calimages SET {query_str} WHERE id = :id"), values)


def downgrade() -> None:
    pass
