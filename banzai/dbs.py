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
from sqlalchemy import create_engine, pool, desc, type_coerce, cast
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Boolean, CHAR, JSON, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql.expression import true

from banzai.utils import date_utils

# Define how to get to the database
# Note that we need to encode the database password outside of the code base
_DEFAULT_DB = 'mysql://cmccully:password@localhost/test'

_CONFIGDB_ADDRESS = 'http://configdb.lco.gtn/sites/'

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
    type = Column(String(50), index=True)
    filename = Column(String(100), unique=True)
    filepath = Column(String(150))
    dateobs = Column(DateTime, index=True)
    datecreated = Column(DateTime, index=True)
    instrument_id = Column(Integer, ForeignKey("instruments.id"), index=True)
    is_master = Column(Boolean)
    is_bad = Column(Boolean)
    attributes = Column(JSON)


class Instrument(Base):
    """
    Instrument Database Record

    This defines the instruments table.
    """
    __tablename__ = 'instruments'
    id = Column(Integer, primary_key=True, autoincrement=True)
    site = Column(String(15), ForeignKey('sites.id'), index=True, nullable=False)
    enclosure = Column(String(20), index=True, nullable=False)
    telescope = Column(String(20), index=True, nullable=False)
    camera = Column(String(50), index=True, nullable=False)
    type = Column(String(100))
    schedulable = Column(Boolean, default=False)
    __table_args__ = (UniqueConstraint('site', 'enclosure', 'telescope', 'camera', name='instrument_constraint'),)


class Site(Base):
    """
    Site Database Record

    This defines the sites table structure
    """
    __tablename__ = 'sites'
    id = Column(String(15), primary_key=True)
    timezone = Column(Integer)


class ProcessedImage(Base):
    __tablename__ = 'processedimages'
    id = Column(Integer, primary_key=True, autoincrement=True)
    filename = Column(String(100), index=True)
    checksum = Column(CHAR(32), index=True, default='0'*32)
    success = Column(Boolean, default=False)
    tries = Column(Integer, default=0)


def create_db(bpm_directory, db_address=_DEFAULT_DB,
              configdb_address=_CONFIGDB_ADDRESS):
    """
    Create the database structure.

    This only needs to be run once on initialization of the database.
    """
    # Create an engine for the database
    engine = create_engine(db_address)

    # Create all tables in the engine
    # This only needs to be run once on initialization.
    Base.metadata.create_all(engine)

    populate_instrument_tables(db_address=db_address, configdb_address=configdb_address)
    populate_calibration_table_with_bpms(bpm_directory, db_address=db_address)


def parse_configdb(configdb_address=_CONFIGDB_ADDRESS):
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
    instruments : list of dicts
              each camera dictionary contains a site, instrument code, and camera type.
    """
    results = requests.get(configdb_address).json()['results']
    instruments = []
    sites = []
    for site in results:
        sites.append({'code': site['code'], 'timezone': site['timezone']})
        for enc in site['enclosure_set']:
            for tel in enc['telescope_set']:
                for ins in tel['instrument_set']:
                    sci_cam = ins.get('science_camera')
                    if sci_cam is not None:
                        instruments.append({'site': site['code'],
                                            'enclosure': enc['code'],
                                            'telescope': tel['code'],
                                            'camera': sci_cam['code'],
                                            'type': sci_cam['camera_type']['code'],
                                            'schedulable': ins['state'] == 'SCHEDULABLE'})
    return sites, instruments


def populate_instrument_tables(db_address=_DEFAULT_DB,
                               configdb_address=_CONFIGDB_ADDRESS):
    """
    Populate the instrument table

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

    sites, instruments = parse_configdb(configdb_address=configdb_address)

    db_session = get_session(db_address=db_address)

    for site in sites:
        add_or_update_record(db_session, Site, {'id': site['code']},
                             {'id': site['code'], 'timezone': site['timezone']})
    db_session.commit()

    # Set all instruments in the table to be schedulable and turn them back on below as needed.
    all_instruments = db_session.query(Instrument).all()
    for instrument in all_instruments:
        instrument.schedulable = False
    db_session.commit()

    for instrument in instruments:
        add_instrument(instrument, db_address=db_address, db_session=db_session)
    db_session.close()


def add_instrument(instrument, db_address=_DEFAULT_DB, db_session=None):
    if db_session is None:
        existing_session = False
        db_session = get_session(db_address)
    else:
        existing_session = True
    equivalence_criteria = {'site': instrument['site'],
                            'enclosure': instrument['enclosure'],
                            'telescope': instrument['telescope'],
                            'camera': instrument['camera']}
    record_attributes = {'site': instrument['site'],
                         'enclosure': instrument['enclosure'],
                         'telescope': instrument['telescope'],
                         'camera': instrument['camera'],
                         'type': instrument['type'],
                         'schedulable': instrument['schedulable']}

    add_or_update_record(db_session, Instrument, equivalence_criteria, record_attributes)
    db_session.commit()
    if not existing_session:
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


def populate_calibration_table_with_bpms(directory, db_address=_DEFAULT_DB):
    db_session = get_session(db_address=db_address)
    bpm_filenames = glob(os.path.join(directory, 'bpm*.fits*'))
    for bpm_filename in bpm_filenames:
        if bpm_filename[-3:] == '.fz':
            extension_number = 1
        else:
            extension_number = 0

        header = fits.getheader(bpm_filename, extension_number)
        ccdsum = fits.getval(bpm_filename, 'CCDSUM', extension_number)
        dateobs = date_utils.parse_date_obs(fits.getval(bpm_filename, 'DATE-OBS', extension_number))

        instrument = get_instrument(header, db_address=db_address)

        if instrument is None:
            logger.error('Instrument is missing from database', extra_tags={'site': header['SITEID'],
                                                                            'camera': header['INSTRUME']})
            continue

        bpm_attributes = {'type': 'BPM',
                          'filename': os.path.basename(bpm_filename),
                          'filepath': os.path.abspath(directory),
                          'dateobs': dateobs,
                          'datecreated': dateobs,
                          'instrument_id': instrument.id,
                          'is_master': True,
                          'is_bad': False,
                          'attributes': {'ccdsum': ccdsum}}

        add_or_update_record(db_session, CalibrationImage, {'filename': bpm_attributes['filename']}, bpm_attributes)

    db_session.commit()
    db_session.close()


class SiteMissingException(Exception):
    pass


def query_for_instrument(db_address, site, camera, enclosure, telescope, must_be_schedulable=False):
    # Short circuit
    if None in [site, camera, telescope, enclosure]:
        return None
    db_session = get_session(db_address=db_address)
    criteria = (Instrument.site == site) & (Instrument.camera == camera)
    criteria &= (Instrument.enclosure == enclosure) & (Instrument.telescope == telescope)
    if must_be_schedulable:
        criteria &= Instrument.schedulable.is_(True)
    instrument = db_session.query(Instrument).filter(criteria).order_by(Instrument.id.desc()).first()
    db_session.close()
    return instrument


def get_instrument(header, db_address=_DEFAULT_DB, configdb_address=_CONFIGDB_ADDRESS):
    site = header.get('SITEID')
    enclosure = header.get('ENCID')
    telescope = header.get('TELID')
    camera = header.get('INSTRUME')
    instrument = query_for_instrument(db_address, site, camera, enclosure=enclosure, telescope=telescope)
    # if instrument is missing, try to check the configdb
    if instrument is None:
        populate_instrument_tables(db_address=db_address, configdb_address=configdb_address)
        instrument = query_for_instrument(db_address, site, camera, enclosure=enclosure, telescope=telescope)
    if instrument is None:
        camera = header.get('TELESCOP')
        instrument = query_for_instrument(db_address, site, camera, enclosure=enclosure, telescope=telescope)
    if instrument is None:
        # Change the camera in the logs back to the default keyword INSTRUME
        camera = header.get('INSTRUME')
        msg = 'Instrument is not in the database, Please add it before reducing this data.'
        logger.error(msg, extra_tags={'site': site, 'enclosure': enclosure,
                                      'telescope': telescope, 'camera': camera})
        raise ValueError('Instrument is missing from the database.')
    return instrument


def get_bpm_filename(instrument_id, ccdsum, db_address=_DEFAULT_DB):
    db_session = get_session(db_address=db_address)
    criteria = (CalibrationImage.type == 'BPM', CalibrationImage.instrument_id == instrument_id,
                cast(CalibrationImage.attributes['ccdsum'], String) == type_coerce(ccdsum, JSON))
    bpm_query = db_session.query(CalibrationImage).filter(*criteria)
    bpm = bpm_query.order_by(desc(CalibrationImage.dateobs)).first()
    db_session.close()

    if bpm is not None:
        bpm_path = os.path.join(bpm.filepath, bpm.filename)
    else:
        bpm_path = None
    return bpm_path


def save_calibration_info(output_file, image, db_address=_DEFAULT_DB):
    # Store the information into the calibration table
    # Check and see if the bias file is already in the database
    db_session = get_session(db_address=db_address)
    output_filename = os.path.basename(output_file)
    record_attributes = {'type': image.obstype.upper(),
                         'filename': output_filename,
                         'filepath': os.path.dirname(output_file),
                         'dateobs': image.dateobs,
                         'datecreated': image.datecreated,
                         'instrument_id': image.instrument.id,
                         'is_master': image.is_master,
                         'is_bad': image.is_bad,
                         'attributes': {}}
    for attribute in image.attributes:
        record_attributes['attributes'][attribute] = getattr(image, attribute)

    add_or_update_record(db_session, CalibrationImage, {'filename': output_filename}, record_attributes)

    db_session.commit()
    db_session.close()


def get_processed_image(path, db_address=_DEFAULT_DB):
    filename = os.path.basename(path)
    db_session = get_session(db_address=db_address)
    processed_image = add_or_update_record(db_session, ProcessedImage, {'filename': filename},
                                         {'filename': filename})
    db_session.commit()
    db_session.close()
    return processed_image


def commit_processed_image(processed_image, db_address=_DEFAULT_DB):
    db_session = get_session(db_address=db_address)
    db_session.add(processed_image)
    db_session.commit()
    db_session.close()


def get_timezone(site, db_address=_DEFAULT_DB):
    db_session = get_session(db_address=db_address)
    site_list = db_session.query(Site).filter(Site.id == site).all()
    db_session.close()
    if len(site_list) == 0:
        raise SiteMissingException
    return site_list[0].timezone


def get_instruments_at_site(site, db_address=_DEFAULT_DB, ignore_schedulability=False):
    db_session = get_session(db_address=db_address)
    query = (Instrument.site == site)
    if not ignore_schedulability:
        query &= Instrument.schedulable
    instruments = db_session.query(Instrument).filter(query).all()
    db_session.close()
    return instruments


def get_master_calibration_image(image, calibration_type, master_selection_criteria,
                                 use_only_older_calibrations=False, db_address=_DEFAULT_DB):
    calibration_criteria = CalibrationImage.type == calibration_type.upper()
    calibration_criteria &= CalibrationImage.instrument_id == image.instrument.id
    calibration_criteria &= CalibrationImage.is_master.is_(True)

    for criterion in master_selection_criteria:
        # We have to cast to strings according to the sqlalchemy docs for version 1.3:
        # https://docs.sqlalchemy.org/en/latest/core/type_basics.html?highlight=json#sqlalchemy.types.JSON
        calibration_criteria &= cast(CalibrationImage.attributes[criterion], String) ==\
                                type_coerce(getattr(image, criterion), JSON)

    # During real-time reduction, we want to avoid using different master calibrations for the same block,
    # therefore we make sure the the calibration frame used was created before the block start time
    if use_only_older_calibrations and image.block_start is not None:
        calibration_criteria &= CalibrationImage.datecreated < image.block_start

    db_session = get_session(db_address=db_address)
    calibration_images = db_session.query(CalibrationImage).filter(calibration_criteria).all()

    # Exit if no calibration file found
    if len(calibration_images) == 0:
        return None

    # Find the closest date
    date_deltas = np.abs(np.array([i.dateobs - image.dateobs for i in calibration_images]))
    closest_calibration_image = calibration_images[np.argmin(date_deltas)]
    calibration_file = os.path.join(closest_calibration_image.filepath, closest_calibration_image.filename)

    if abs(min(date_deltas)) > datetime.timedelta(days=30):
        msg = "The closest calibration file in the database was created more than 30 days before or after " \
              "the image being reduced."
        logger.warning(msg, image=image, extra_tags={'master_calibration': os.path.basename(calibration_file)})

    return calibration_file


def get_individual_calibration_images(instrument, calibration_type, min_date, max_date,
                                      use_masters=False, db_address=_DEFAULT_DB):

    calibration_criteria = CalibrationImage.instrument_id == instrument.id
    calibration_criteria &= CalibrationImage.type == calibration_type.upper()
    calibration_criteria &= CalibrationImage.is_master.is_(use_masters)
    calibration_criteria &= CalibrationImage.dateobs > min_date
    calibration_criteria &= CalibrationImage.dateobs < max_date

    db_session = get_session(db_address=db_address)
    images = db_session.query(CalibrationImage).filter(calibration_criteria).all()
    db_session.close()

    image_paths = [os.path.join(image.filepath, image.filename) for image in images]

    return image_paths


def mark_frame(filename, mark_as, db_address=_DEFAULT_DB):
    set_is_bad_to = True if mark_as == "bad" else False
    logger.debug("Setting the is_bad parameter for {filename} to {set_is_bad_to}".format(
        filename=filename, set_is_bad_to=set_is_bad_to))
    db_session = get_session(db_address=db_address)
    # First check to make sure the image is in the database
    image = db_session.query(CalibrationImage).filter(CalibrationImage.filename == filename).first()
    if image is None:
        logger.error("Frame {filename} not found in database, exiting".format(filename=filename))
        return
    if image.is_bad is set_is_bad_to:
        logger.error("The is_bad parameter for {filename} is already set to {is_bad}, exiting".format(
            filename=filename, is_bad=image.is_bad))
        return
    equivalence_criteria = {'filename': filename}
    record_attributes = {'filename': filename,
                         'is_bad': set_is_bad_to}
    add_or_update_record(db_session, CalibrationImage, equivalence_criteria, record_attributes)
    db_session.commit()
    db_session.close()
