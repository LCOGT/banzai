import os

ARCHIVE_API_ROOT = 'https://archive-api.lco.global/'

ARCHIVE_API_FRAME_SEARCH = 'https://archive-api.lco.global/frames/?q=a&RLEVEL={rlevel}' \
'&PROPID=&INSTRUME={instrument}&OBJECT=&SITEID={site}&TELID=&FILTER=&' \
'OBSTYPE=&EXPTIME=&BLKUID=&REQNUM=&basename={dayobs}' \
'&start={start}%2000:00:00&end={end}%2023:59&public=true&limit=100000'

ARCHIVE_API_TOKEN = os.getenv('ARCHIVE_API_TOKEN', None)
