""" dbs.py: Database utility functions for PyLCOGT

    This is built around the SQLAlchemy ORM

Author
    Curtis McCully (cmccully@lcogt.net)

October 2015
"""
from __future__ import absolute_import, print_function, division

import os.path
import contextlib
import itertools

import sqlalchemy
from sqlalchemy import create_engine, pool
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, Integer, String, Float, Date, ForeignKey, DateTime, Boolean
from sqlalchemy.ext.declarative import declarative_base

# Define how to get to the database
# Note that we need to encode the database password outside of the code base
_DEFAULT_DB = 'mysql+mysqlconnector://cmccully:password@localhost/test'

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


class Image(Base):
    """
    Image Database Record

    This defines the images table. Most of these keywords are parsed from the headers.
    telescope_id is a foreign key to the telescopes table.
    """
    __tablename__ = 'images'

    # Define the table structure
    id = Column(Integer, primary_key=True, autoincrement=True)
    filename = Column(String(36), index=True, unique=True)
    telescope_id = Column(Integer, ForeignKey("telescopes.id"), index=True)
    filepath = Column(String(100))
    rawfilename = Column(String(36))
    rawpath = Column(String(100))
    object_name = Column(String(50))
    mjd = Column(Float, index=True)
    dateobs = Column(DateTime)
    dayobs = Column(Date, index=True)
    exptime = Column(Float)
    filter_name = Column(String(2))
    obstype = Column(String(20))
    airmass = Column(Float)
    ra = Column(Float)
    dec = Column(Float)
    userid = Column(String(20))
    propid = Column(String(20))
    tracknum = Column(String(20))
    reqnum = Column(String(20))
    ccdsum = Column(String(10))
    gain = Column(Float)
    readnoise = Column(Float)
    naxis1 = Column(Integer)
    naxis2 = Column(Integer)
    pixel_scale = Column(Float)
    focus = Column(Integer)
    # Reduction Status
    ingest_done = Column(Boolean, default=False)
    bias_done = Column(Boolean, default=False)
    trim_done = Column(Boolean, default=False)
    dark_done = Column(Boolean, default=False)
    flat_done = Column(Boolean, default=False)
    wcs_done = Column(Boolean, default=False)
    cat_done = Column(Boolean, default=False)

    def get_full_filename(self, suffix, extension='.fits'):
        image_file = os.path.join(self.filepath, self.filename)
        image_file += suffix + extension

        return image_file


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
    filename = Column(String(40), unique=True)
    filepath = Column(String(100))
    dayobs = Column(Date, index=True)
    ccdsum = Column(String(20))
    filter_name = Column(String(2))
    telescope_id = Column(Integer, ForeignKey("telescopes.id"), index=True)


class Telescope(Base):
    """
    Telescope Database Record

    This defines the telescopes table.
    """
    __tablename__ = 'telescopes'
    id = Column(Integer, primary_key=True, autoincrement=True)
    site = Column(String(10), index=True)
    telescope_id = Column(String(20), index=True)
    instrument = Column(String(20), index=True)
    camera_type = Column(String(20))


def create_db(db_address=_DEFAULT_DB):
    """
    Create the database structure.

    This only needs to be run once on initialization of the database.
    """
    # Create an engine for the database
    engine = create_engine(db_address)

    # Create all tables in the engine
    # This only needs to be run once on initialization.
    Base.metadata.create_all(engine)

    populate_telescope_table(db_address)


def populate_telescope_table(db_address=_DEFAULT_DB):
    """
    Populate the telescope table

    This only needs to be done once on initialization.
    Any new instruments will need to be added to this table manually.

    We really should replace this with a call to the configdb.
    """

    db_session = get_session(db_address)
    db_session.add(Telescope(site='coj', telescope_id='2m0-02', instrument='fs03',
                             camera_type='spectral'))
    db_session.add(Telescope(site='coj', telescope_id='1m0-11', instrument='kb79',
                             camera_type='sbig'))
    db_session.add(Telescope(site='coj', telescope_id='1m0-03', instrument='kb71',
                             camera_type='sbig'))
    db_session.add(Telescope(site='elp', telescope_id='1m0-08', instrument='kb74',
                             camera_type='sbig'))
    db_session.add(Telescope(site='lsc', telescope_id='1m0-05', instrument='kb78',
                             camera_type='sbig'))
    db_session.add(Telescope(site='lsc', telescope_id='1m0-09', instrument='fl03',
                             camera_type='sinistro'))
    db_session.add(Telescope(site='lsc', telescope_id='1m0-04', instrument='fl04',
                             camera_type='sinistro'))
    db_session.add(Telescope(site='cpt', telescope_id='1m0-10', instrument='kb70',
                             camera_type='sbig'))
    db_session.add(Telescope(site='cpt', telescope_id='1m0-13', instrument='kb76',
                             camera_type='sbig'))
    db_session.add(Telescope(site='cpt', telescope_id='1m0-12', instrument='kb75',
                             camera_type='sbig'))
    db_session.add(Telescope(site='ogg', telescope_id='2m0-01', instrument='fs02',
                             camera_type='spectral'))
    db_session.add(Telescope(site='ogg', telescope_id='2m0-01', instrument='em01',
                             camera_type='merope'))
    db_session.add(Telescope(site='elp', telescope_id='1m0-08', instrument='fl05',
                             camera_type='sinistro'))
    db_session.commit()
    db_session.close()


def save_images(images):
    """
    Save images to the database.

    :param images: List of images to save.
    :return: Not needed.
    """

    with contextlib.closing(get_session()) as session:
        for image in images:
            session.add(image)
        session.commit()

    return


def get_telescope_list(args):
    db_session = get_session()
    telescope_query = sqlalchemy.sql.expression.true()

    if args.site != '':
        telescope_query &= Telescope.site == args.site

    if args.instrument != '':
        telescope_query &= Telescope.instrument == args.instrument

    if args.telescope != '':
        telescope_query &= Telescope.telescope_id == args.telescope

    if args.camera_type != '':
        telescope_query &= Telescope.camera_type == args.camera_type

    telescope_list = db_session.query(Telescope).filter(telescope_query).all()

    db_session.close()
    return telescope_list


def generate_initial_query(args):
      # Get the telescopes for which we want to reduce data.
    db_session = get_session()

    image_query = sqlalchemy.sql.expression.true()

    if args.filter != '':
        image_query &= Image.filter_name == args.filter

    if args.binning != '':
        ccdsum = args.binning.replace('x', ' ')
        image_query &= Image.ccdsum == ccdsum

    db_session.close()
    return image_query

def select_input_images(telescope, epoch, initial_query, previous_stage_done, group_by_list):
    # Select only the images we want to work on
    query = initial_query & (Image.telescope_id == telescope.id)
    query &= (Image.dayobs == epoch)

    # Only select images that have had the previous stage completed
    query &= previous_stage_done

    db_session = get_session()

    if group_by_list:
        config_list = []
        # Get the distinct values of ccdsum and filters
        for group_by in group_by_list:
            config_query = db_session.query(group_by)

            distinct_configs = config_query.filter(query).distinct().all()
            config_list.append([x[0] for x in distinct_configs])
        config_queries = []

        for config in itertools.product(*config_list):
            config_query = query

            for i in range(len(group_by_list)):
                # Select images with the correct binning/filter
                config_query &= (group_by_list[i] == config[i])
            config_queries.append(config_query)

    else:
        config_queries = [query]

    input_image_list = []
    config_list = []
    for image_config in config_queries:

        image_list = db_session.query(Image).filter(image_config).all()

        # Convert from image objects to file names
        input_image_list.append(image_list)

        if len(image_list) == 0:
            config_list.append([])
        else:
            config_list.append(image_list[0])
    db_session.close()
    return input_image_list, config_list

def get_telescope_id(site, instrument):
    # TODO:  This dies if the telescope is not in the telescopes table. Maybe ping the configdb?
    db_session = get_session()
    criteria = (Telescope.site == site) & (Telescope.instrument == instrument)
    telescope = db_session.query(Telescope).filter(criteria).first()
    return telescope.id


def save_calibration_info(cal_type, output_file, image_config):
    # Store the information into the calibration table
    # Check and see if the bias file is already in the database
    db_session = get_session()
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
