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

logger = logs.get_logger('Ingest')

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
def ingest_single_image(raw_path, raw_image):

    db_session = dbs.get_session()

    image_filename = os.path.basename(raw_image.filename())

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

    image.rawpath = raw_path

    # Get the fits header of the raw frame
    image_header = raw_image[0].header

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

    image.ingest_done = True

    # Because we are ingesting, group_by can just be none.
    tags = logs.image_config_to_tags(image, telescope, image.dayobs, None)
    tags['tags']['filename'] = image.filename
    tags['tags']['obstype'] = image.obstype
    logger.debug('Ingesting {image_name}'.format(image_name=image_filename), extra=tags)

    db_session.add(image)

    # Write out to the database
    db_session.commit()
    db_session.close()
    return raw_image


class Ingest(Stage):

    def __init__(self, pipeline_context):
        super(Ingest, self).__init__(pipeline_context)

#    @metric_timer('ingest')
    def do_stage(self, images):
        return [ingest_single_image(self.pipeline_context.rawpath, image) for image in images]


