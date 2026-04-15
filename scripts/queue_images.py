#!/usr/bin/env python3
"""
Simple script to queue FITS images for processing via RabbitMQ.

This script is used with the docker-compose local deployment to send
local FITS files to the banzai processing pipeline.
"""

import argparse
import os
import glob
from astropy.io import fits
from banzai.utils.file_utils import post_to_archive_queue


def main():
    parser = argparse.ArgumentParser(
        description='Queue FITS images for processing via RabbitMQ'
    )
    parser.add_argument('directory', help='Path to directory containing FITS files')
    parser.add_argument('--broker-url', default='amqp://localhost:5672',
                        help='RabbitMQ broker URL (default: amqp://localhost:5672)')
    parser.add_argument('--exchange', default='fits_files',
                        help='RabbitMQ exchange name (default: fits_files)')
    args = parser.parse_args()

    # Find FITS files
    fits_files = []
    for pattern in ['*.fits', '*.fits.fz']:
        fits_files.extend(glob.glob(os.path.join(args.directory, pattern)))

    print(f'Files to process: {len(fits_files)}')

    # Queue each file — use absolute host paths (containers mount host dirs at the same path)
    for filepath in sorted(fits_files):
        abs_path = os.path.abspath(filepath)
        with fits.open(filepath) as hdul:
            header = hdul['SCI'].header
            siteid = header.get('SITEID', '').strip()
            instrume = header.get('INSTRUME', '').strip()

            if siteid and instrume:
                post_to_archive_queue(
                    filename=os.path.basename(filepath),
                    broker_url=args.broker_url,
                    exchange_name=args.exchange,
                    path=abs_path,
                    SITEID=siteid,
                    INSTRUME=instrume
                )


if __name__ == '__main__':
    main()
