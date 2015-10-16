from __future__ import absolute_import, print_function, division
__author__ = 'cmccully'

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from sqlalchemy import Column, Integer, String, Float, Date, ForeignKey, DateTime
from sqlalchemy.ext.declarative import declarative_base

# Define how to get to the database
db_address = 'mysql+mysqlconnector://cmccully:password@cmccully-linux/test'

Base = declarative_base()

def get_session():
    engine = create_engine(db_address)
    # Bind the engine to the metadata of the Base class so that the
    # declaratives can be accessed through a DBSession instance
    Base.metadata.bind = engine

    DBSession = sessionmaker(bind=engine, autoflush=False)
    session = DBSession()

    return session


class Image(Base):
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
    dayobs  = Column(Date, index=True)
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


class Calibration_Image(Base):
    __tablename__ = 'calimages'
    id = Column(Integer, primary_key=True, autoincrement=True)
    type = Column(String(30), index=True)
    filename = Column(String(30), unique=True)
    filepath = Column(String(100))
    dayobs = Column(Date, index=True)
    ccdsum = Column(String(20))
    filter_name = Column(String(2))
    telescope_id = Column(Integer, ForeignKey("telescopes.id"), index=True)


class Reduction_Status(Base):
    __tablename__ = 'reductionstatus'
    id = Column(Integer, primary_key=True, autoincrement=True)


class Telescope(Base):
    __tablename__ = 'telescopes'
    id = Column(Integer, primary_key=True, autoincrement=True)
    site = Column(String(10), index=True)
    telescope_id = Column(String(20), index=True)
    instrument = Column(String(20), index=True)
    camera_type = Column(String(20))


def create_db():
    # Create an engine for the database
    engine = create_engine(db_address)

    # Create all tables in the engine
    # This only needs to be run once on initialization.
    Base.metadata.create_all(engine)


def populate_telescope_table():
    # Populate the telescope table
    # This only needs to be done once on initialization.
    # Any new instruments will need to be added to this table manually (or via chronjob)
    db_session = get_session()
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
