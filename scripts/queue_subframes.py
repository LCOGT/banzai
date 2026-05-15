#!/usr/bin/env python3
"""
Send raw FITS subframes to banzai_stack_queue for site-deployment stacking.

Used with docker-compose-site.yml. The banzai-subframe-listener consumes from
this queue, reduces each subframe, and the stacking supervisor produces a
stacked frame once all subframes in a MOLUID group arrive.

Each FITS file's SCI header must contain:
    MOLUID   - observation group identifier
    MOLFRNUM - frame number within the group (1-indexed)
    FRMTOTAL - total frames in the group
    STACK    - 'T'

`last_frame` is set automatically when MOLFRNUM == FRMTOTAL.
"""

import argparse
import glob
import json
import os
import time

from astropy.io import fits

from banzai.utils.messaging import publish_raw_string_to_queue


def main():
    parser = argparse.ArgumentParser(
        description='Send raw FITS subframes to banzai_stack_queue'
    )
    parser.add_argument('directory', help='Directory containing FITS subframes')
    parser.add_argument('--site', default=None,
                        help='Only queue files whose SITEID header matches (e.g. tfn)')
    parser.add_argument('--broker-url', default='amqp://localhost:5672',
                        help='RabbitMQ broker URL (default: amqp://localhost:5672)')
    parser.add_argument('--queue-name', default='banzai_stack_queue',
                        help='Queue name (default: banzai_stack_queue)')
    args = parser.parse_args()

    fits_files = []
    for pattern in ['*.fits', '*.fits.fz']:
        fits_files.extend(glob.glob(os.path.join(args.directory, pattern)))
    fits_files = sorted(fits_files)

    queued = 0
    for filepath in fits_files:
        abs_path = os.path.abspath(filepath)
        with fits.open(filepath) as hdul:
            header = hdul['SCI'].header
            siteid = str(header.get('SITEID', '')).strip()
            molfrnum = int(header.get('MOLFRNUM', 0))
            frmtotal = int(header.get('FRMTOTAL', 0))

        if args.site and siteid.lower() != args.site.lower():
            print(f'  skip    {os.path.basename(filepath)} (SITEID={siteid!r})')
            continue

        last_frame = molfrnum == frmtotal and frmtotal > 0
        body = json.dumps({
            'fits_file': abs_path,
            'last_frame': last_frame,
            'instrument_enqueue_timestamp': int(time.time() * 1000),
        })

        publish_raw_string_to_queue(args.queue_name, body, args.broker_url)
        queued += 1
        print(f'  queued  {os.path.basename(filepath)} '
              f'({molfrnum}/{frmtotal}, last={last_frame})')

    print(f'Queued {queued} of {len(fits_files)} files')


if __name__ == '__main__':
    main()
