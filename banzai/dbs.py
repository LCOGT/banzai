""" dbs.py: Database utility functions for banzai

    This is built around the SQLAlchemy ORM

Author
    Curtis McCully (cmccully@lcogt.net)

October 2015
"""
import os.path
import logging
from glob import glob
import datetime

import numpy as np
import requests
from astropy.io import fits
from sqlalchemy import create_engine, pool, desc
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, Integer, String, Date, ForeignKey, Boolean, CHAR
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql.expression import true

from banzai.utils import date_utils
from banzai.utils import fits_utils

# Define how to get to the database
# Note that we need to encode the database password outside of the code base
_DEFAULT_DB = 'mysql://cmccully:password@localhost/test'

Base = declarative_base()

logger = logging.getLogger(__name__)

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
    db_session = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
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
    filter_name = Column(String(32))
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
    creation_date = Column(Date)


class PreviewImage(Base):
    __tablename__ = 'previewimages'
    id = Column(Integer, primary_key=True, autoincrement=True)
    filename = Column(String(50), index=True)
    checksum = Column(CHAR(32), index=True, default='0'*32)
    success = Column(Boolean, default=False)
    tries = Column(Integer, default=0)


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
                                        'schedulable': ins['state'] == 'SCHEDULABLE'})
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
    added to the network.
    """

    sites, cameras = parse_configdb(configdb_address=configdb_address)

    db_session = get_session(db_address=db_address)

    for site in sites:
        add_or_update_record(db_session, Site, {'id': site['code']},
                             {'id': site['code'], 'timezone': site['timezone']})
    db_session.commit()

    # Set all cameras in the table to be schedulable and turn them back on below as needed.
    all_cameras = db_session.query(Telescope).all()
    for camera in all_cameras:
        camera.schedulable = False
    db_session.commit()

    for camera in cameras:
        add_or_update_record(db_session, Telescope,
                             {'site': camera['site'], 'instrument': camera['instrument']},
                             {'site': camera['site'], 'instrument': camera['instrument'],
                              'camera_type': camera['camera_type'],
                              'schedulable': camera['schedulable']})

    db_session.commit()
    db_session.close()


def add_or_update_record(db_session, table_model, equivalence_criteria, record_attributes):
    """
    Add a record to the database if it does not exist or update the record if it does exist.

    Parameters
    ----------
    db_session : SQLAlchemy database session
                 session must be active

    table_model : SQLAlchemy Base
                  The class representation of the table of interest

    equivalence_criteria : dict
                           record attributes that need to match for the records to be considered
                           the same

    record_attributes : dict
                        record attributes that will be set/updated

    Returns
    -------
    record : SQLAlchemy Base
             The object representation of the added/updated record

    Notes
    -----
    The added/updated record is added to the database but not committed. You need to call
    db_session.commit() to write the changes to the database.
    """
    query = true()
    for key in equivalence_criteria.keys():
        query &= getattr(table_model, key) == equivalence_criteria[key]
    record = db_session.query(table_model).filter(query).first()
    if record is None:
        record = table_model(**record_attributes)
        db_session.add(record)

    for attribute in record_attributes:
        setattr(record, attribute, record_attributes[attribute])
    return record


def populate_bpm_table(directory, db_address=_DEFAULT_DB):
    db_session = get_session(db_address=db_address)
    bpm_filenames = glob(os.path.join(directory, 'bpm*.fits*'))
    for bpm_filename in bpm_filenames:
        if bpm_filename[-3:] == '.fz':
            extension_number = 1
        else:
            extension_number = 0

        header = fits.getheader(bpm_filename, extension_number)
        ccdsum = fits.getval(bpm_filename, 'CCDSUM', extension_number)
        creation_date = date_utils.epoch_string_to_date(fits.getval(bpm_filename, 'DAY-OBS',
                                                                    extension_number))

        telescope = get_telescope(header, db_address=db_address)

        if telescope is None:
            logger.error('Telescope is missing from database', extra={'tags': {'site': header['SITEID'],
                                                                               'instrument': header['INSTRUME']}})
            continue

        bpm_attributes = {'telescope_id': telescope.id, 'filepath': os.path.abspath(directory),
                          'filename': os.path.basename(bpm_filename), 'ccdsum': ccdsum,
                          'creation_date': creation_date}

        add_or_update_record(db_session, BadPixelMask, bpm_attributes, bpm_attributes)

    db_session.commit()
    db_session.close()


class TelescopeMissingException(Exception):
    pass


def _query_for_telescope(db_address, site, instrument):
    # Short circuit
    if site is None or instrument is None:
        return None
    db_session = get_session(db_address=db_address)
    criteria = (Telescope.site == site) & (Telescope.instrument == instrument)
    telescope = db_session.query(Telescope).filter(criteria).first()
    db_session.close()
    return telescope


def get_telescope(header, db_address=_DEFAULT_DB):
    site = header.get('SITEID')
    instrument = header.get('INSTRUME')
    telescope_for_instrument = _query_for_telescope(db_address, site, instrument)
    telescope = telescope_for_instrument if telescope_for_instrument is not None \
        else _query_for_telescope(db_address, site, header.get('TELESCOP'))
    if telescope is None:
        err_msg = '{site}/{instrument} is not in the database.'.format(site=site, instrument=instrument)
        logger.error(err_msg, extra={'tags': {'site': site, 'instrument': instrument}})
    return telescope


def get_bpm(telescope_id, ccdsum, db_address=_DEFAULT_DB):
    db_session = get_session(db_address=db_address)
    bpm_query = db_session.query(BadPixelMask).filter(BadPixelMask.telescope_id == telescope_id,
                                                      BadPixelMask.ccdsum == ccdsum)
    bpm = bpm_query.order_by(desc(BadPixelMask.creation_date)).first()
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
    output_filename = os.path.basename(output_file)

    add_or_update_record(db_session, CalibrationImage, {'filename': output_filename},
                         {'dayobs': date_utils.epoch_string_to_date(image_config.epoch),
                          'ccdsum': image_config.ccdsum, 'filter_name': image_config.filter,
                          'telescope_id': image_config.telescope.id, 'type': cal_type.upper(),
                          'filename': output_filename, 'filepath': os.path.dirname(output_file)})

    db_session.commit()
    db_session.close()


def get_telescope_for_file(path, db_address=_DEFAULT_DB):
    data, header, bpm, extension_headers = fits_utils.open_image(path)
    return get_telescope(header, db_address=db_address)


def get_preview_image(path, db_address=_DEFAULT_DB):
    filename = os.path.basename(path)
    db_session = get_session(db_address=db_address)
    preview_image = add_or_update_record(db_session, PreviewImage, {'filename': filename},
                                         {'filename': filename})
    db_session.commit()
    db_session.close()
    return preview_image


def commit_preview_image(preview_image, db_address=_DEFAULT_DB):
    db_session = get_session(db_address=db_address)
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


def get_master_calibration_image(image, calibration_type, group_by_keywords,
                                 db_address=_DEFAULT_DB):
    calibration_criteria = CalibrationImage.type == calibration_type.upper()
    calibration_criteria &= CalibrationImage.telescope_id == image.telescope.id

    for criterion in group_by_keywords:
        if criterion == 'filter':
            calibration_criteria &= CalibrationImage.filter_name == getattr(image, criterion)
        else:
            calibration_criteria &= getattr(CalibrationImage, criterion) == getattr(image, criterion)

    # Only grab the last year. In principle we could go farther back, but this limits the number
    # of files we get back. And if we are using calibrations that are more than a year old
    # it is probably a bad idea anyway.
    epoch_datetime = date_utils.epoch_string_to_date(image.epoch)

    calibration_criteria &= CalibrationImage.dayobs < (epoch_datetime + datetime.timedelta(days=365))
    calibration_criteria &= CalibrationImage.dayobs > (epoch_datetime - datetime.timedelta(days=365))

    db_session = get_session(db_address=db_address)

    calibration_images = db_session.query(CalibrationImage).filter(calibration_criteria).all()

    # Find the closest date
    if len(calibration_images) == 0:
        calibration_file = None
    else:
        date_deltas = np.abs(np.array([i.dayobs - epoch_datetime for i in calibration_images]))
        closest_calibration_image = calibration_images[np.argmin(date_deltas)]
        calibration_file = os.path.join(closest_calibration_image.filepath,
                                        closest_calibration_image.filename)

    db_session.close()
    return calibration_file
