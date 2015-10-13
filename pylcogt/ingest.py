"""
ingest.py - Module containing routines to ingest meta from raw images into the database.

Authors
    Curtis McCully

July 2015
"""
from __future__ import absolute_import, print_function, division

import glob

import os
from astropy.io import fits
from astropy import time
from astropy.coordinates import SkyCoord
from astropy import units
import shutil

from . import dbs
from . import logs
from . stages import Stage

__author__ = 'cmccully'


class Ingest(Stage):

    def __init__(self, raw_path, processed_path, initial_query):

        log_message = 'Ingesting data for {site}/{instrument} on {epoch}'
        super(Ingest, self).__init__(self.ingest_raw_data, processed_path=processed_path,
                                     initial_query=initial_query, logger_name='Ingest',
                                     log_message=log_message)
        self.raw_path = raw_path

    def ingest_raw_data(self, raw_image_list):
        logger = logs.get_logger('Ingest')

        for raw_image_file in raw_image_list:
            image_filename = os.path.basename(raw_image_file)
            logger.debug('Ingesting {image_name}'.format(image_name=image_filename))

            # Check and see if the filename is already in the database
            image_query = self.db_session.query(dbs.Image)
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

            # Get the telescope
            telescope_query = dbs.Telescope.instrument == image_header["INSTRUME"]

            telescope_query &= (dbs.Telescope.site == image_header['SITEID'])

            telescope_query = self.db_session.query(dbs.Telescope).filter(telescope_query)
            telescope = telescope_query.one()
            image.telescope_id = telescope.id

            # Save the image_header keywords into a record
            image.dayobs = image_header['DAY-OBS'].strip()
            image.exptime = float(image_header['EXPTIME'])
            image.filter_name = image_header['FILTER'].strip()
            image.mjd = float(image_header['MJD-OBS'])
            image.airmass = float(image_header['AIRMASS'])
            image.object_name = image_header['OBJECT'].strip()
            image.tracknum = image_header['TRACKNUM'].strip()
            image.obstype = image_header['OBSTYPE'].strip()
            image.reqnum = image_header['REQNUM'].strip()
            image.propid = image_header['PROPID'].strip()
            image.userid = image_header['USERID'].strip()
            image.ccdsum = image_header['CCDSUM'].strip()
            image.gain = float(image_header['GAIN'])
            image.readnoise = float(image_header['RDNOISE'])
            image.naxis1 = image_header['NAXIS1']
            image.naxis2 = image_header['NAXIS2']

            coordinate = SkyCoord(image_header['RA'], image_header['DEC'],
                                  unit=(units.hourangle, units.deg))
            image.ra = coordinate.ra.deg
            image.dec = coordinate.dec.deg

            # Save the dateobs as a datetime object
            image.dateobs = time.Time(image_header['DATE-OBS']).datetime

            image.filename = image_filename[:-7] + '90.fits'

            image.filepath = os.path.join(self.processed_path, telescope.site,
                                          telescope.instrument, image.dayobs.replace('-', ''))

            self.db_session.add(image)

            # Copy the file into place
            shutil.copy(os.path.join(image.rawpath, image.rawfilename),
                        os.path.join(image.filepath, image.filename))

        # Write out to the database
        self.db_session.commit()

    def select_input_images(self, telescope, epoch):
        search_path = os.path.join(self.raw_path, telescope.site,
                                   telescope.instrument, epoch)

        if os.path.exists(os.path.join(search_path, 'preproc')):
            search_path = os.path.join(search_path, 'preproc')
        else:
            search_path = os.path.join(search_path, 'raw')

        # return the list of file and a dummy image configuration
        return [glob.glob(search_path + '/*.fits')], [dbs.Image()]
