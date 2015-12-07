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
from astropy import time, units
from astropy.coordinates import SkyCoord
import shutil

import itertools

#from opentsdb_python_metrics.metric_wrappers import metric_timer

from . import dbs
from . import logs
from . stages import Stage

from functools import partial

__author__ = 'cmccully'


def header_to_database(image, image_field, header, header_keyword, float_type=False):
    if float_type:
        try:
            setattr(image, image_field, float(header[header_keyword]))
        except ValueError:
            setattr(image, image_field,  None)
    else:
        setattr(image, image_field, header[header_keyword].strip())


# This can't be a class method if we want to use multiprocessing... Stupid python multiprocessing...
def ingest_single_image(logger_name, processed_path, image_suffix_number, raw_image_file):

    db_session = dbs.get_session()
    logger = logs.get_logger(logger_name)
    image_filename = os.path.basename(raw_image_file)

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

    # Get the telescope
    telescope_query = dbs.Telescope.instrument == image_header["INSTRUME"]

    telescope_query &= (dbs.Telescope.site == image_header['SITEID'])

    telescope_query = db_session.query(dbs.Telescope).filter(telescope_query)
    telescope = telescope_query.one()

    image.telescope_id = telescope.id

    image.naxis1 = image_header['NAXIS1']
    image.naxis2 = image_header['NAXIS2']

    # Save the image_header keywords into a record
    header_to_database(image, 'dayobs', image_header, 'DAY-OBS')
    header_to_database(image, 'filter_name', image_header, 'FILTER')
    header_to_database(image, 'object', image_header, 'OBJECT')
    header_to_database(image, 'obstype', image_header, 'OBSTYPE')
    header_to_database(image, 'tracknum', image_header, 'TRACKNUM')
    header_to_database(image, 'reqnum', image_header, 'REQNUM')
    header_to_database(image, 'propid', image_header, 'PROPID')
    header_to_database(image, 'userid', image_header, 'USERID')
    header_to_database(image, 'ccdsum', image_header, 'CCDSUM')

    header_to_database(image, 'exptime', image_header, 'EXPTIME', float_type=True)
    header_to_database(image, 'mjd', image_header, 'MJD-OBS', float_type=True)
    header_to_database(image, 'airmass', image_header, 'AIRMASS', float_type=True)
    header_to_database(image, 'gain', image_header, 'GAIN', float_type=True)
    header_to_database(image, 'readnoise', image_header, 'RDNOISE', float_type=True)

    header_to_database(image, 'pixel_scale', image_header, 'PIXSCALE', float_type=True)
    try:
        coordinate = SkyCoord(image_header['RA'], image_header['DEC'],
                              unit=(units.hourangle, units.deg))
        image.ra = coordinate.ra.deg
        image.dec = coordinate.dec.deg
    except ValueError:
        image.ra = None
        image.dec = None

    # Save the dateobs as a datetime object
    image.dateobs = time.Time(image_header['DATE-OBS']).datetime

    # Strip off the 00.fits
    image.filename = image_filename[:-7]

    image.filepath = os.path.join(processed_path, telescope.site,
                                  telescope.instrument, image.dayobs.replace('-', ''))

    image.ingest_done = True

    # Because we are ingesting, group_by can just be none.
    tags = logs.image_config_to_tags(image, telescope, image.dayobs, None)
    tags['tags']['filename'] = image.filename
    tags['tags']['obstype'] = image.obstype
    logger.debug('Ingesting {image_name}'.format(image_name=image_filename), extra=tags)

    db_session.add(image)

    # Copy the file into place
    shutil.copy(os.path.join(image.rawpath, image.rawfilename),
                os.path.join(image.filepath, image.filename + image_suffix_number + '.fits'))

    # Write out to the database
    db_session.commit()
    db_session.close()


class Ingest(Stage):

    def __init__(self, pipeline_context, initial_query):
        log_message = 'Ingesting data'
        super(Ingest, self).__init__(pipeline_context,
                                     initial_query=initial_query, log_message=log_message,
                                     image_suffix_number='03', previous_stage_done=None)

#    @metric_timer('ingest')
    def do_stage(self, raw_image_list):
        for image in raw_image_list:
            ingest_single_image('Ingest', self.pipeline_context.processed_path, self.image_suffix_number, image)

        return

    def select_input_images(self, telescope, epoch):
        search_path = os.path.join(self.pipeline_context.raw_path, telescope.site,
                                   telescope.instrument, epoch)

        if os.path.exists(os.path.join(search_path, 'preproc')):
            search_path = os.path.join(search_path, 'preproc')
        else:
            search_path = os.path.join(search_path, 'raw')

        # return the list of file and a dummy image configuration
        return [glob.glob(search_path + '/*.fits')], [dbs.Image()]
