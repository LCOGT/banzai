"""
ingest.py - Module containing routines to ingest meta from raw images into the database.

Authors
    Curtis McCully

July 2015
"""
from __future__ import absolute_import, print_function
__author__ = 'cmccully'

import glob

import os
from astropy.io import fits
from astropy import time
from astropy.coordinates import SkyCoord
from astropy import units

from . import dbs
from . import logs


def ingest_raw_data(raw_image_list, processed_directory):
    """

    :param raw_image_list:
    :param processed_directory:
    :return:
    """
    logger = logs.get_logger('Ingest')
    db_session = dbs.get_session()
    for raw_image_file in raw_image_list:
        image_filename = os.path.basename(raw_image_file)
        logger.info('Ingesting {image_name}'.format(image_name=image_filename))

        # Check and see if the filename is already in the database
        image_query = db_session.query(dbs.Image)
        image_query = image_query.filter(dbs.Image.rawfilename == image_filename).all()

        if len(image_query) == 0:
            # Create a new row
            image = dbs.Image(rawfilename=image_filename)
        else:
            # Otherwise update the existing data
            # In principle we could just skip this, but this should be fast
            image = image_query[0]

        image.rawpath = os.path.dirname(raw_image_file)

        # Get the fits header of the raw frame
        image_header = fits.getheader(raw_image_file)

        # Save the image_header keywords into a record
        image.dayobs = image_header['DAY-OBS']
        image.exptime = float(image_header['EXPTIME'])
        image.filter_name = image_header['FILTER']
        image.mjd = float(image_header['MJD-OBS'])
        image.telescope = image_header['TELESCOP']
        image.siteid = image_header['SITEID']
        image.airmass = float(image_header['AIRMASS'])
        image.object_name = image_header['OBJECT']
        image.tracknum = image_header['TRACKNUM']
        coordinate = SkyCoord(image_header['RA'], image_header['DEC'],
                              unit=(units.hourangle, units.deg))
        image.ra = coordinate.ra.deg
        image.dec = coordinate.dec.deg
        image.obstype = image_header['OBSTYPE']
        image.reqnum = image_header['REQNUM']
        image.propid = image_header['PROPID']
        image.userid = image_header['USERID']
        image.ccdsum = image_header['CCDSUM']
        image.instrument = image_header['INSTRUME']

        # Split date-obs into the date and time
        date_obs = time.Time(image_header['DATE-OBS'])
        image.dateobs = date_obs.datetime.date()
        image.ut = date_obs.datetime.time()

        image.filename = image_filename[:-7] + '90.fits'

        image.filepath = os.path.join(processed_directory, image.siteid, image.telescope,
                                      image.dayobs.replace('-', ''))

        db_session.add(image)

    # Write out to the database
    db_session.commit()
    db_session.close()


def run_ingest(raw_data_directory, site, instrument, epoch_list, processed_directory):
    """

    :param raw_data_directory:
    :param site:
    :param instrument:
    :param epoch_list:
    :param processed_directory:
    :return:
    """
    logger = logs.get_logger('Ingest')
    for epoch in epoch_list:
        logmsg = 'Ingesting data for {site}/{instrument} on {epoch}'
        logger.info(logmsg.format(site=site, instrument=instrument, epoch=epoch))
        search_path = os.path.join(raw_data_directory, site, instrument, epoch)
        if os.path.exists(os.path.join(search_path, 'preproc')):
            search_path = os.path.join(search_path, 'preproc')
        else:
            search_path = os.path.join(search_path, 'raw')
        image_list = glob.glob(search_path + '/*.fits')
        ingest_raw_data(image_list, processed_directory)
