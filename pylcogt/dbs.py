""" dbs.py: Database utility functions for PyLCOGT

    This is built around the SQLAlchemy ORM

Author
    Curtis McCully (cmccully@lcogt.net)

October 2015
"""
from __future__ import absolute_import, print_function, division

from sqlalchemy import create_engine, pool
from sqlalchemy.orm import sessionmaker

from sqlalchemy import Column, Integer, String, Float, Date, ForeignKey, DateTime, Boolean
from sqlalchemy.ext.declarative import declarative_base

# Define how to get to the database
# Note that we need to encode the database password outside of the code base
_DEFAULT_DB = 'mysql+mysqldb://hibernate:hibernate@localhost/test'

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
    db_session.commit()
    db_session.close()
