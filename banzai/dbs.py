""" dbs.py: Database utility functions for banzai

    This is built around the SQLAlchemy ORM

Author
    Curtis McCully (cmccully@lcogt.net)

October 2015
"""
import os.path
import datetime
from dateutil.parser import parse
import requests
from sqlalchemy import create_engine, pool, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Boolean, CHAR, JSON, UniqueConstraint, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql.expression import true
from contextlib import contextmanager
from banzai.logs import get_logger

Base = declarative_base()

logger = get_logger()


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
    nx = Column(Integer)
    ny = Column(Integer)
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
                      'longitude': site['long'], 'latitude': site['lat'], 'elevation': site['elevation']})
        for enc in site['enclosure_set']:
            for tel in enc['telescope_set']:
                for ins in tel['instrument_set']:
                    for sci_cam in ins['science_cameras']:
                        if sci_cam is not None:
                            camera_size = sci_cam['camera_type']['size']
                            if camera_size == 'N/A':
                                nx = 25
                                ny = 25
                            else:
                                nx, ny = camera_size.split('x')
                            # Convert from arcminutes to arcseconds and then to pixels
                            nx = int(float(nx) * 60 / float(sci_cam['camera_type']['pscale']))
                            ny = int(float(ny) * 60 / float(sci_cam['camera_type']['pscale']))
                            instrument = {'site': site['code'],
                                          'camera': sci_cam['code'],
                                          'name': ins.get('code'),
                                          'type': ins['instrument_type']['code'],
                                          'nx': nx, 'ny': ny}
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
                             'type': instrument['type'],
                             'nx': instrument['nx'],
                             'ny': instrument['ny']}

        instrument_record = add_or_update_record(db_session, Instrument, equivalence_criteria, record_attributes)
        db_session.commit()
    return instrument_record


def add_site(site, db_address):
    with get_session(db_address=db_address) as db_session:
        equivalence_criteria = {'id': site['code']}
        record_attributes = {'id': site['code'],
                             'timezone': site['timezone'],
                             'longitude': site['longitude'],
                             'latitude': site['latitude'],
                             'elevation': site['elevation']}

        site_record = add_or_update_record(db_session, Site, equivalence_criteria, record_attributes)
        db_session.commit()
    return site_record


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


def save_calibration_info(calibration_image: CalibrationImage, db_address):
    record_attributes = vars(calibration_image)
    # There is not a clean way to back a dict object from a calibration image object without this instance state
    # parameter. Gross.
    record_attributes.pop('_sa_instance_state')
    with get_session(db_address=db_address) as db_session:
        add_or_update_record(db_session, CalibrationImage, {'filename': record_attributes['filename']},
                             record_attributes)
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
    output_record.checksum = md5
    commit_processed_image(output_record, db_address)


def get_timezone(site, db_address):
    site = get_site(site, db_address)
    return site.timezone


def get_instruments_at_site(site, db_address):
    with get_session(db_address=db_address) as db_session:
        query = (Instrument.site == site)
        instruments = db_session.query(Instrument).filter(query).all()
    return instruments


def get_instrument_by_id(id, db_address):
    with get_session(db_address=db_address) as db_session:
        instrument = db_session.query(Instrument).filter(Instrument.id==id).first()
    return instrument


def get_site(site_id, db_address):
    with get_session(db_address=db_address) as db_session:
        site_list = db_session.query(Site).filter(Site.id == site_id).all()
    if len(site_list) == 0:
        raise SiteMissingException
    return site_list[0]


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
                 'filename': record.filename,
                 'dateobs': record.dateobs}
    return file_info


def build_master_calibration_criteria(image, calibration_type, master_selection_criteria,
                                      use_only_older_calibrations):
    calibration_criteria = CalibrationImage.type == calibration_type.upper()
    calibration_criteria &= CalibrationImage.instrument_id == image.instrument.id
    calibration_criteria &= CalibrationImage.is_master.is_(True)
    calibration_criteria &= CalibrationImage.is_bad.is_(False)

    for criterion in master_selection_criteria:
        # We have to cast to strings according to the sqlalchemy docs for version 1.3:
        # https://docs.sqlalchemy.org/en/latest/core/type_basics.html?highlight=json#sqlalchemy.types.JSON
        calibration_criteria &= CalibrationImage.attributes[criterion].as_string() ==\
                                str(getattr(image, criterion))

    # During real-time reduction, we want to avoid using different master calibrations for the same block,
    # therefore we make sure the the calibration frame used was created before the block start time
    if use_only_older_calibrations and getattr(image, 'block_start') is not None:
        calibration_criteria &= CalibrationImage.datecreated < image.block_start

    calibration_criteria &= CalibrationImage.good_after <= image.dateobs
    calibration_criteria &= CalibrationImage.good_until >= image.dateobs
    return calibration_criteria


def query_calibrations(image, calibration_criteria, db_address):
    with get_session(db_address=db_address) as db_session:
        if 'postgres' in db_session.bind.dialect.name:
            order_func = func.abs(func.extract("epoch", CalibrationImage.dateobs) -
                                  func.extract("epoch", image.dateobs))
        elif 'sqlite' in db_session.bind.dialect.name:
            order_func = func.abs(func.julianday(CalibrationImage.dateobs) - func.julianday(image.dateobs))
        else:
            raise NotImplementedError("Only postgres and sqlite are supported")
        image_filter = db_session.query(CalibrationImage).filter(calibration_criteria)
        calibration_image = image_filter.order_by(order_func).first()
    return calibration_image


def get_master_cal_record(image, calibration_type, master_selection_criteria, db_address,
                          use_only_older_calibrations=False):
    calibration_criteria = build_master_calibration_criteria(image, calibration_type, master_selection_criteria,
                                                             use_only_older_calibrations)
    calibration_image = query_calibrations(image, calibration_criteria, db_address)
    return calibration_image


def get_individual_cal_records(instrument, calibration_type, min_date: str, max_date: str, db_address: str,
                               include_bad_frames: bool = False):
    calibration_criteria = CalibrationImage.instrument_id == instrument.id
    calibration_criteria &= CalibrationImage.type == calibration_type.upper()
    calibration_criteria &= CalibrationImage.dateobs >= parse(min_date).replace(tzinfo=None)
    calibration_criteria &= CalibrationImage.dateobs <= parse(max_date).replace(tzinfo=None)
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
    for site in sites:
        add_site(site, db_address)
    for instrument in instruments:
        add_instrument(instrument, db_address)

def create_local_db(local_db_address, aws_db_address, site_id):
    """
    Create a local SQLite database and populate it with sites and instruments
    copied over from the AWS calibration database for a specific site.

    Parameters
    ----------
    local_db_address : str
        SQLAlchemy address for the local SQLite database
    aws_db_address : str
        SQLAlchemy address for the AWS PostgreSQL database
    site_code : str
        Site code to replicate (e.g., 'ogg', 'lsc')
    """

    # Check if local database file already exists (for SQLite)
    if 'sqlite' in local_db_address and '://' in local_db_address:
        db_path = local_db_address.split(':///')[-1]
        if os.path.exists(db_path):
            logger.error(f"Database file {db_path} already exists. Please remove it first.")
            return

    # Create the local database structure
    logger.info(f"Creating local database at {local_db_address}")
    create_db(local_db_address)

    # Get site from AWS database
    logger.info(f"Fetching site {site_id} from AWS database")
    site_record = get_site(site_id, aws_db_address)

    # Get instruments for this site from AWS database
    #
    # The following block could have been replaced with a simpler single line:
    # `instrument_records = get_instruments_at_site(site_id, aws_db_address)`
    # but the postgres db in AWS appears to have two instruments (ID 3211 & 235)
    # that violate the unique site/camera/name constraint defined in the
    # Instruments db class. This leads to an error if we try to copy over both
    # of those instruments.
    #
    # As a workaround, instead of copying all instruments, we copy over the
    # subset with a unique site/camera/name attribute group, selecting
    # the latest version in the case of duplicates.
    logger.info(f"Fetching instruments for site {site_id} from AWS database")
    with get_session(db_address=aws_db_address) as db_session:
        # Get the latest instrument for each (site, camera, name) combination
        subquery = db_session.query(
            func.max(Instrument.id).label('max_id')
        ).filter(
            Instrument.site == site_id
        ).group_by(
            Instrument.site, Instrument.camera, Instrument.name
        ).subquery()

        instrument_records = db_session.query(Instrument).join(
            subquery, Instrument.id == subquery.c.max_id
        ).all()

    # Replicate site to local database
    logger.info(f"Replicating site {site_id} to local database")
    replicate_site(site_record, local_db_address)

    # Replicate instruments to local database
    logger.info(f"Replicating {len(instrument_records)} instruments to local database")
    for instrument in instrument_records:
        replicate_instrument(instrument, local_db_address)

    logger.info(f"Successfully created local database with {len(instrument_records)} instruments from site {site_id}")


def replicate_site(site_record, db_address):
    """
    Replicate a site record to the target database, preserving the original ID.

    Parameters
    ----------
    site_record : Site
        Source site record from SQLAlchemy query
    db_address : str
        Target database address
    """
    site_dict = {
        'code': site_record.id,  # Site ID is the site code
        'timezone': site_record.timezone,
        'longitude': site_record.longitude,
        'latitude': site_record.latitude,
        'elevation': site_record.elevation
    }
    add_site(site_dict, db_address)


def replicate_instrument(instrument_record, db_address):
    """
    Replicate an instrument record to the target database, preserving the original ID.
    We don't use the add_instrument method because that method doesn't allow specifying the ID.

    Parameters
    ----------
    instrument_record : Instrument
        Source instrument record from SQLAlchemy query
    db_address : str
        Target database address
    """
    with get_session(db_address=db_address) as db_session:
        # Create new instrument with original ID
        new_instrument = Instrument(
            id=instrument_record.id,  # Preserve original ID
            site=instrument_record.site,
            camera=instrument_record.camera,
            type=instrument_record.type,
            name=instrument_record.name,
            nx=instrument_record.nx,
            ny=instrument_record.ny
        )
        db_session.add(new_instrument)
        db_session.commit()