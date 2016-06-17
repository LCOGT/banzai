""" dbs.py: Database utility functions for banzai

    This is built around the SQLAlchemy ORM

Author
    Curtis McCully (cmccully@lcogt.net)

October 2015
"""
from __future__ import absolute_import, division, print_function, unicode_literals

import os.path

from sqlalchemy import create_engine, pool
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, Integer, String, Date, ForeignKey, Boolean, CHAR
from sqlalchemy.ext.declarative import declarative_base

from glob import glob
from astropy.io import fits
import requests
from banzai.utils import file_utils


# Define how to get to the database
# Note that we need to encode the database password outside of the code base
_DEFAULT_DB = 'mysql://cmccully:password@localhost/test'

Base = declarative_base()


def get_session(db_address=_DEFAULT_DB):
    """
    Get a connection to the database.

    Returns
    -------
    session: SQLAlchemy Database Session
    """
    # Build a new engine for each session. This makes things thread safe.
    engine = create_engine(db_address, poolclass=pool.NullPool)
    Base.metadata.bind = engine

    # We don't use autoflush typically. I have run into issues where SQLAlchemy would try to flush
    # incomplete records causing a crash. None of the queries here are large, so it should be ok.
    db_session = sessionmaker(bind=engine, autoflush=False)
    session = db_session()

    return session


class CalibrationImage(Base):
    """
    Master Calibration Image Database Record

    This defines the calimages table. We use this to keep track of the master calibration frames.
    Typically these are bias, darks, and flat field frames. These are indexed by dayobs to make it
    easy to find the closest calibration frame.
    """
    __tablename__ = 'calimages'
    id = Column(Integer, primary_key=True, autoincrement=True)
    type = Column(String(30), index=True)
    filename = Column(String(50), unique=True)
    filepath = Column(String(100))
    dayobs = Column(Date, index=True)
    ccdsum = Column(String(20))
    filter_name = Column(String(6))
    telescope_id = Column(Integer, ForeignKey("telescopes.id"), index=True)


class Telescope(Base):
    """
    Telescope Database Record

    This defines the telescopes table.
    """
    __tablename__ = 'telescopes'
    id = Column(Integer, primary_key=True, autoincrement=True)
    site = Column(String(10), ForeignKey('sites.id'), index=True)
    instrument = Column(String(20), index=True)
    camera_type = Column(String(20))
    schedulable = Column(Boolean, default=False)


class Site(Base):
    """
    Site Database Record

    This defines the sites table structure
    """
    __tablename__ = 'sites'
    id = Column(String(3), primary_key=True)
    timezone = Column(Integer)


class BadPixelMask(Base):
    """
    Bad Pixel Mask Database Record
    """
    __tablename__ = 'bpms'
    id = Column(Integer, primary_key=True, autoincrement=True)
    telescope_id = Column(Integer, ForeignKey("telescopes.id"), index=True)
    filename = Column(String(50))
    filepath = Column(String(100))
    ccdsum = Column(String(20))


class PreviewImage(Base):
    __tablename__ = 'previewimages'
    id = Column(Integer, primary_key=True, autoincrement=True)
    filename = Column(String(50), index=True)
    checksum = Column(CHAR(32), index=True)


def create_db(bpm_directory, db_address=_DEFAULT_DB,
              configdb_address='http://configdb.lco.gtn/sites/'):
    """
    Create the database structure.

    This only needs to be run once on initialization of the database.
    """
    # Create an engine for the database
    engine = create_engine(db_address)

    # Create all tables in the engine
    # This only needs to be run once on initialization.
    Base.metadata.create_all(engine)

    populate_telescope_tables(db_address=db_address, configdb_address=configdb_address)
    populate_bpm_table(bpm_directory, db_address=db_address)


def parse_configdb(configdb_address='http://configdb.lco.gtn/sites/'):
    """
    Parse the contents of the configdb.

    Parameters
    ----------
    configdb_address : str
                      URL of the configdb, must be inside LCOGT VPN

    Returns
    -------
    sites : list of dicts
            each site dictionary contains a timezone.
    cameras : list of dicts
              each camera dictionary contains a site, instrument code, and camera type.
    """
    results = requests.get(configdb_address).json()['results']
    cameras = []
    sites = []
    for site in results:
        sites.append({'code': site['code'], 'timezone': site['timezone']})
        for enc in site['enclosure_set']:
            for tel in enc['telescope_set']:
                for ins in tel['instrument_set']:
                    sci_cam = ins.get('science_camera')
                    if sci_cam is not None:
                        cameras.append({'site': site['code'],
                                        'instrument': sci_cam['code'],
                                        'camera_type': sci_cam['camera_type']['code'],
                                        'schedulable': ins['schedulable']})
    return sites, cameras


def populate_telescope_tables(db_address=_DEFAULT_DB,
                              configdb_address='http://configdb.lco.gtn/sites/'):
    """
    Populate the telescope table

    Parameters
    ----------
    db_address : str
                 sqlalchemy address to the database of the form
                 mysql://username:password@localhost/test

    configdb_address : str
                       URL of the configdb

    Notes
    -----
    This only works inside the LCOGT VPN. This should be run at least when a new camera is
    added to the network
    """

    sites, cameras = parse_configdb(configdb_address=configdb_address)

    db_session = get_session(db_address=db_address)

    for site in sites:
        site_query = Site.id == site['code']
        matching_sites = db_session.query(Site).filter(site_query).all()

        if len(matching_sites) == 0:
            db_session.add(Site(id=site['code'], timezone=site['timezone']))

    db_session.commit()

    # Set all cameras in the table to be schedulable and turn them back on below as needed.
    all_cameras = db_session.query(Telescope).all()
    for camera in all_cameras:
        camera.schedulable = False
    db_session.commit()

    for camera in cameras:

        # Check and see if the site and instrument combinatation already exists in the table
        camera_query = Telescope.site == camera['site']
        camera_query &= Telescope.instrument == camera['instrument']
        matching_cameras = db_session.query(Telescope).filter(camera_query).all()

        if len(matching_cameras) == 0:
            db_session.add(Telescope(site=camera['site'], instrument=camera['instrument'],
                                     camera_type=camera['camera_type'],
                                     schedulable=camera['schedulable']))
        else:
            matching_cameras[0].schedulable = camera['schedulable']

    db_session.commit()
    db_session.close()


def populate_bpm_table(directory, db_address=_DEFAULT_DB):
    db_session = get_session(db_address=db_address)
    bpm_filenames = glob(os.path.join(directory, 'bpm*.fits'))
    for bpm_filename in bpm_filenames:
        site = fits.getval(bpm_filename, 'SITEID').lower()
        instrument = fits.getval(bpm_filename, 'INSTRUME').lower()
        ccdsum = fits.getval(bpm_filename, 'CCDSUM')

        telescope_query = Telescope.site == site
        telescope_query &= Telescope.instrument == instrument
        telescope = db_session.query(Telescope).filter(telescope_query).first()

        if telescope is not None:

            db_session.add(BadPixelMask(telescope_id=telescope.id, filepath=os.path.abspath(directory),
                                        filename=os.path.basename(bpm_filename), ccdsum=ccdsum))

    db_session.commit()
    db_session.close()


def get_telescope_id(site, instrument, db_address=_DEFAULT_DB):
    # TODO:  This dies if the telescope is not in the telescopes table. Maybe ping the configdb?
    db_session = get_session(db_address=db_address)
    criteria = (Telescope.site == site) & (Telescope.instrument == instrument)
    telescope = db_session.query(Telescope).filter(criteria).first()
    db_session.close()
    return telescope.id


def get_telescope(telescope_id, db_address=_DEFAULT_DB):
    db_session = get_session(db_address=db_address)
    telescope = db_session.query(Telescope).filter(Telescope.id == telescope_id).first()
    db_session.close()
    return telescope


def get_bpm(telescope_id, ccdsum, db_address=_DEFAULT_DB):
    db_session = get_session(db_address=db_address)
    bpm = db_session.query(BadPixelMask).filter(BadPixelMask.telescope_id == telescope_id,
                                                BadPixelMask.ccdsum == ccdsum).first()
    db_session.close()
    if bpm is not None:
        bpm_path = os.path.join(bpm.filepath, bpm.filename)
    else:
        bpm_path = None
    return bpm_path


def save_calibration_info(cal_type, output_file, image_config, db_address=_DEFAULT_DB):
    # Store the information into the calibration table
    # Check and see if the bias file is already in the database
    db_session = get_session(db_address=db_address)
    image_query = db_session.query(CalibrationImage)
    output_filename = os.path.basename(output_file)
    image_query = image_query.filter(CalibrationImage.filename == output_filename)
    image_query = image_query.all()

    if len(image_query) == 0:
        # Create a new row
        calibration_image = CalibrationImage()
    else:
        # Otherwise update the existing data
        # In principle we could just skip this, but this should be fast
        calibration_image = image_query[0]

    calibration_image.dayobs = image_config.epoch
    calibration_image.ccdsum = image_config.ccdsum
    calibration_image.filter_name = image_config.filter
    calibration_image.telescope_id = image_config.telescope_id
    calibration_image.type = cal_type.upper()
    calibration_image.filename = output_filename
    calibration_image.filepath = os.path.dirname(output_file)

    db_session.add(calibration_image)
    db_session.commit()
    db_session.close()


def preview_file_already_processed(path, db_address=_DEFAULT_DB):
    db_session = get_session(db_address=db_address)
    query = db_session.query(PreviewImage)
    criteria = PreviewImage.filename == os.path.basename(path)
    criteria &= PreviewImage.checksum == file_utils.get_md5(path)
    query = query.filter(criteria)
    already_processed = len(query.all()) > 0
    db_session.close()
    return already_processed


def set_preview_file_as_processed(path, db_address=_DEFAULT_DB):
    filename = os.path.basename(path)
    checksum = file_utils.get_md5(path)

    db_session = get_session(db_address=db_address)
    query = db_session.query(PreviewImage)
    criteria = PreviewImage.filename == filename
    criteria &= PreviewImage.checksum == checksum
    query = query.filter(criteria)
    if len(query.all()) == 0:
        preview_image = PreviewImage(filename=filename, checksum=checksum)
        db_session.add(preview_image)
        db_session.commit()
    db_session.close()


def get_timezone(site, db_address=_DEFAULT_DB):
    db_session = get_session(db_address=db_address)
    site_list = db_session.query(Site).filter(Site.id == site).all()
    if len(site_list) > 0:
        timezone = site_list[0].timezone
    else:
        timezone = None
    db_session.close()
    return timezone


def get_schedulable_telescopes(site, db_address=_DEFAULT_DB):
    db_session = get_session(db_address=db_address)
    query = (Telescope.site == site) & Telescope.schedulable
    telescopes = db_session.query(Telescope).filter(query).all()
    db_session.close()
    return telescopes
