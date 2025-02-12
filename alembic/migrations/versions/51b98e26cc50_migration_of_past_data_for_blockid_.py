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


def parse_public_date(date_to_parse):
    date_formats = ['%Y-%m-%dT%H:%M:%S.%fZ', '%Y-%m-%dT%H:%M:%SZ']
    for date_format in date_formats:
        try:
            public_date = datetime.datetime.strptime(date_to_parse, date_format)
            break
        except ValueError:
            continue
    return public_date


def upgrade() -> None:
    if os.getenv("AUTH_TOKEN") is not None:
        auth_header = {'Authorization': f'Token {os.environ["AUTH_TOKEN"]}'}
    else:
        auth_header = None

    connection = op.get_bind()
    offset = 0
    batch_size = 1000
    query_str = "SELECT id, frameid, filename, type, dateobs, camera FROM calimages INNER JOIN instruments"
    query_str += "ON calimages.instrument_id = instruments.id ORDER BY dateobs LIMIT :limit OFFSET :offset"
    more_data = True
    while more_data:
        # Get all of the frames from all of the cameras for all obstypes in this batch
        rows = connection.execute(text(query_str, {"limit": batch_size, "offset": offset})).fetchall()

        for row in rows:
            instruments = list(set(row.camera for row in rows))
            obstypes = list(set(row.type for row in rows))
        start = min([row.dateobs for row in rows])
        end = max([row.dateobs for row in rows])
        params = {'instruments': instruments, 'obstypes': obstypes, 'start': start, 'end': end}
        request_results = requests.get('https://archive-api.lco.global/frames/aggregate/', params=params, headers=auth_header)
        request_results = request_results.json()['results']
        if len(request_results) == 0:
            more_data = False
            break
        request_results[0]['public_date']
        hashed_results = {result['basename']: {'blockid': result['BLKUID'],
                                               'proposal': result['proposal_id'],
                                               'public_date': parse_public_date(result['public_date'])}
                          for result in request_results}
        for row in rows:
            basename = row.filename.replace('.fits', '').replace('.fz', '')
            values = {'id': row.id, 'public_date': hashed_results[basename]['public_date'],
                      'proposal': hashed_results[basename]['proposal'],
                      'blockid': hashed_results[basename]['blockid']}
            query_str = 'blockid = :blockid, proposal = :proposal, public_date = :public_date'
            if row.frameid is None:
                query_str += ', frameid = :frameid'
                values['frameid'] = request_results[0]['id']
            connection.execute(text(f"UPDATE calimages SET {query_str} WHERE id = :id"), values)

        offset += batch_size


def downgrade() -> None:
    pass
