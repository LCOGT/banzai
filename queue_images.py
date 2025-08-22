#!/usr/bin/env python3
"""
Simple script to queue FITS images for processing via RabbitMQ.
Usage: python queue_images.py /path/to/fits/directory
"""

import os
import glob
import sys
from astropy.io import fits
from kombu import Connection, Exchange


def post_to_archive_queue(filename, path, broker_url, exchange_name, **kwargs):
    """Post file to RabbitMQ queue."""
    exchange = Exchange(exchange_name, type='fanout')
    with Connection(broker_url) as conn:
        producer = conn.Producer(exchange=exchange)
        body = {'filename': filename, 'path': path}
        body.update(kwargs)
        producer.publish(body)
        producer.release()


def main():
    if len(sys.argv) != 2:
        print("Usage: python queue_images.py /path/to/fits/directory")
        sys.exit(1)

    directory = sys.argv[1]
    broker_url = 'amqp://localhost:5672'
    exchange_name = 'fits_files'

    # Get container path from environment variable
    container_base_path = '/data/raw'

    # Find FITS files
    fits_files = []
    for pattern in ['*.fits', '*.fits.fz']:
        fits_files.extend(glob.glob(os.path.join(directory, pattern)))

    # fits_files = [fits_files[1]]

    print(f'File to process: {len(fits_files)}')

    # Queue each file
    for filepath in sorted(fits_files):
        with fits.open(filepath) as hdul:
            header = hdul[1].header
            siteid = header.get('SITEID', '').strip()
            instrume = header.get('INSTRUME', '').strip()

            if siteid and instrume:
                # Use container path instead of host path
                container_path = f'{container_base_path}/{os.path.basename(filepath)}'

                post_to_archive_queue(
                    filename=os.path.basename(filepath),
                    path=container_path,
                    broker_url=broker_url,
                    exchange_name=exchange_name,
                    SITEID=siteid,
                    INSTRUME=instrume
                )


if __name__ == '__main__':
    main()
