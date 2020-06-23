""" dbs.py: Database utility functions for banzai

    This is built around the SQLAlchemy ORM

Author
    Curtis McCully (cmccully@lcogt.net)

October 2015
"""
import os.path
import logging
import datetime
from dateutil.parser import parse

import numpy as np
import requests
from sqlalchemy import create_engine, pool, type_coerce, cast
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Boolean, CHAR, JSON, UniqueConstraint, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql.expression import true
from contextlib import contextmanager

Base = declarative_base()

logger = logging.getLogger('banzai')


@contextmanager
def get_session(db_address):
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
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


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
    frameid = Column(Integer, nullable=True)
    dateobs = Column(DateTime, index=True)
    datecreated = Column(DateTime, index=True)
    instrument_id = Column(Integer, ForeignKey("instruments.id"), index=True)
    is_master = Column(Boolean)
    is_bad = Column(Boolean)
    good_until = Column(DateTime, default=datetime.datetime(3000, 1, 1))
    good_after = Column(DateTime, default=datetime.datetime(1000, 1, 1))
    attributes = Column(JSON)


class Instrument(Base):
    """
    Instrument Database Record

    This defines the instruments table.
    """
    __tablename__ = 'instruments'
    id = Column(Integer, primary_key=True, autoincrement=True)
    site = Column(String(15), ForeignKey('sites.id'), index=True, nullable=False)
    camera = Column(String(50), index=True, nullable=False)
    type = Column(String(100))
    name = Column(String(100), index=True, nullable=False)
    __table_args__ = (UniqueConstraint('site', 'camera', 'name', name='instrument_constraint'),)


class Site(Base):
    """
    Site Database Record

    This defines the sites table structure
    """
    __tablename__ = 'sites'
    id = Column(String(15), primary_key=True)
    timezone = Column(Integer)
    latitude = Column(Float)
    longitude = Column(Float)
    elevation = Column(Float)


class ProcessedImage(Base):
    __tablename__ = 'processedimages'
    id = Column(Integer, primary_key=True, autoincrement=True)
    filename = Column(String(100), index=True)
    frameid = Column(Integer, nullable=True)
    checksum = Column(CHAR(32), index=True, default='0'*32)
    success = Column(Boolean, default=False)
    tries = Column(Integer, default=0)


def parse_configdb(configdb_address):
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
    # this will be removed when configdb is updated
    CAMERAS_FOR_INSTRUMENTS = {'nres01': 'fa09', 'nres02': 'fa17', 'nres03': 'fa13', 'nres04': 'fa18',
                               'floyds01': 'en06', 'floyds02': 'en12'}
    # end of hotfix
    for site in results:
        sites.append({'code': site['code'], 'timezone': site['timezone'],
                      'longitude': site['lon'], 'latitude': site['lat'], 'elevation': site['elevation']})
        for enc in site['enclosure_set']:
            for tel in enc['telescope_set']:
                for ins in tel['instrument_set']:
                    sci_cam = ins.get('science_camera')
                    if sci_cam is not None:
                        instrument = {'site': site['code'],
                                      'camera': sci_cam['code'],
                                      'name': ins.get('code'),
                                      'type': sci_cam['camera_type']['code']}
                        # hotfix for configdb
                        if not instrument['name']:
                            instrument['name'] = instrument['camera']
                        if instrument['name'] in CAMERAS_FOR_INSTRUMENTS:
                            instrument['camera'] = CAMERAS_FOR_INSTRUMENTS[instrument['name']]
                        instruments.append(instrument)
    return sites, instruments


def add_instrument(instrument, db_address):
    with get_session(db_address=db_address) as db_session:
        equivalence_criteria = {'site': instrument['site'],
                                'camera': instrument['camera'],
                                'name': instrument['name']}
        record_attributes = {'site': instrument['site'],
                             'camera': instrument['camera'],
                             'name': instrument['name'],
                             'type': instrument['type']}

        instrument_record = add_or_update_record(db_session, Instrument, equivalence_criteria, record_attributes)
        db_session.commit()
    return instrument_record


def add_site(site, db_address):
    with get_session(db_address=db_address) as db_session:
        equivalence_criteria = {'id': site['id']}
        record_attributes = {'id': site['id'],
                             'timezone': site['timezone'],
                             'longitude': site['longitude'],
                             'latitude': site['latitude'],
                             'elevation': site['elevation']}

        instrument_record = add_or_update_record(db_session, Instrument, equivalence_criteria, record_attributes)
        db_session.commit()
    return instrument_record


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


class SiteMissingException(Exception):
    pass


def query_for_instrument(db_address, site, camera, name=None):
    # Short circuit
    if None in [site, camera]:
        return None
    with get_session(db_address=db_address) as db_session:
        criteria = (Instrument.site == site) & (Instrument.camera == camera)
        if name is not None:
            criteria &= Instrument.name == name
        instrument = db_session.query(Instrument).filter(criteria).order_by(Instrument.id.desc()).first()
    return instrument


def save_calibration_info(output_file, image, db_address):
    # Store the information into the calibration table
    # Check and see if the bias file is already in the database
    with get_session(db_address=db_address) as db_session:
        output_filename = os.path.basename(output_file)
        record_attributes = {'type': image.obstype.upper(),
                             'filename': output_filename,
                             'filepath': os.path.dirname(output_file),
                             'dateobs': image.dateobs,
                             'datecreated': image.datecreated,
                             'instrument_id': image.instrument.id,
                             'is_master': image.is_master,
                             'is_bad': image.is_bad,
                             'frameid': image.frame_id,
                             'attributes': {}}
        for attribute in image.grouping_criteria:
            record_attributes['attributes'][attribute] = str(getattr(image, attribute))

        add_or_update_record(db_session, CalibrationImage, {'filename': output_filename}, record_attributes)

        db_session.commit()


def get_processed_image(path, db_address):
    # TODO: add support for AWS path styles
    filename = os.path.basename(path)
    with get_session(db_address=db_address) as db_session:
        processed_image = add_or_update_record(db_session, ProcessedImage, {'filename': filename},
                                               {'filename': filename})
        db_session.commit()
    return processed_image


def commit_processed_image(processed_image, db_address):
    with get_session(db_address=db_address) as db_session:
        db_session.add(processed_image)
        db_session.commit()


def save_processed_image(path, md5, db_address):
    filename = os.path.basename(path)
    output_record = get_processed_image(filename, db_address)
    output_record.success = True
    output_record.checksum = md5
    commit_processed_image(output_record, db_address)


def get_timezone(site, db_address):
    with get_session(db_address=db_address) as db_session:
        site_list = db_session.query(Site).filter(Site.id == site).all()
    if len(site_list) == 0:
        raise SiteMissingException
    return site_list[0].timezone


def get_instruments_at_site(site, db_address):
    with get_session(db_address=db_address) as db_session:
        query = (Instrument.site == site)
        instruments = db_session.query(Instrument).filter(query).all()
    return instruments


def get_instrument_by_id(id, db_address):
    with get_session(db_address=db_address) as db_session:
        instrument = db_session.query(Instrument).filter(Instrument.id==id).first()
    return instrument


def cal_record_to_file_info(record):
    if record is None:
        return None
    # if the filepath in the DB is not set, make sure we set the path to None.
    if record.filepath is None:
        path = None
    else:
        path = os.path.join(record.filepath, record.filename)
    file_info = {'frameid': record.frameid,
                 'path': path,
                 'filename': record.filename}
    return file_info


def get_master_cal_record(image, calibration_type, master_selection_criteria, db_address,
                          use_only_older_calibrations=False):
    calibration_criteria = CalibrationImage.type == calibration_type.upper()
    calibration_criteria &= CalibrationImage.instrument_id == image.instrument.id
    calibration_criteria &= CalibrationImage.is_master.is_(True)
    calibration_criteria &= CalibrationImage.is_bad.is_(False)

    for criterion in master_selection_criteria:
        # We have to cast to strings according to the sqlalchemy docs for version 1.3:
        # https://docs.sqlalchemy.org/en/latest/core/type_basics.html?highlight=json#sqlalchemy.types.JSON
        calibration_criteria &= cast(CalibrationImage.attributes[criterion], String) ==\
                                type_coerce(str(getattr(image, criterion)), JSON)

    # During real-time reduction, we want to avoid using different master calibrations for the same block,
    # therefore we make sure the the calibration frame used was created before the block start time
    if use_only_older_calibrations and getattr(image, 'block_start') is not None:
        calibration_criteria &= CalibrationImage.datecreated < image.block_start

    calibration_criteria &= CalibrationImage.good_after <= image.dateobs
    calibration_criteria &= CalibrationImage.good_until >= image.dateobs

    with get_session(db_address=db_address) as db_session:
        calibration_images = db_session.query(CalibrationImage).filter(calibration_criteria).all()

    # Exit if no calibration file found
    if len(calibration_images) == 0:
        return None

    # Find the closest date
    date_deltas = np.abs(np.array([i.dateobs - image.dateobs for i in calibration_images]))
    closest_calibration_image = calibration_images[np.argmin(date_deltas)]

    return closest_calibration_image


def get_master_cal(image, calibration_type, master_selection_criteria, db_address,
                   use_only_older_calibrations=False):
    return cal_record_to_file_info(get_master_cal_record(image, calibration_type, master_selection_criteria, db_address,
                                                         use_only_older_calibrations=use_only_older_calibrations))


def get_individual_cal_records(instrument, calibration_type, min_date: str, max_date: str, db_address: str,
                               include_bad_frames: bool = False):
    calibration_criteria = CalibrationImage.instrument_id == instrument.id
    calibration_criteria &= CalibrationImage.type == calibration_type.upper()
    calibration_criteria &= CalibrationImage.dateobs >= parse(min_date).replace(tzinfo=None)
    calibration_criteria &= CalibrationImage.dateobs <= parse(max_date).replace(tzinfo=None)
    calibration_criteria &= CalibrationImage.is_master == False

    calibration_criteria &= CalibrationImage.is_master == False

    if not include_bad_frames:
        calibration_criteria &= CalibrationImage.is_bad == False

    with get_session(db_address=db_address) as db_session:
        image_records = db_session.query(CalibrationImage).filter(calibration_criteria).all()

    return image_records


def get_individual_cal_frames(instrument, calibration_type, min_date: str, max_date: str, db_address: str,
                              include_bad_frames: bool = False):
    image_records = get_individual_cal_records(instrument, calibration_type, min_date, max_date, db_address,
                                               include_bad_frames=include_bad_frames)
    return [cal_record_to_file_info(record) for record in image_records]


def mark_frame(filename, mark_as, db_address):
    set_is_bad_to = True if mark_as == "bad" else False
    logger.debug("Setting the is_bad parameter for {filename} to {set_is_bad_to}".format(
        filename=filename, set_is_bad_to=set_is_bad_to))
    with get_session(db_address=db_address) as db_session:
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


def create_db(db_address):
    # Create an engine for the database
    engine = create_engine(db_address)

    # Create all tables in the engine
    # This only needs to be run once on initialization.
    Base.metadata.create_all(engine)


def populate_instrument_tables(db_address, configdb_address):
    """
    Populate the instrument table from the configdb

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
    with get_session(db_address=db_address) as db_session:
        for site in sites:
            add_or_update_record(db_session, Site, {'id': site['code']},
                                 {'id': site['code'], 'timezone': site['timezone']})

    for instrument in instruments:
        add_instrument(instrument, db_address)
