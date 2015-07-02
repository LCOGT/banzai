from __future__ import absolute_import, print_function
__author__ = 'cmccully'

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from sqlalchemy import Column, Integer, String, Float, Time, Date
from sqlalchemy.ext.declarative import declarative_base

# Define how to get to the database
db_address = 'mysql://cmccully:password@localhost/test'

Base = declarative_base()

def get_session():
    engine = create_engine(db_address)
    # Bind the engine to the metadata of the Base class so that the
    # declaratives can be accessed through a DBSession instance
    Base.metadata.bind = engine

    DBSession = sessionmaker(bind=engine)
    session = DBSession()

    return session

class Image(Base):
    __tablename__ = 'images'

    # Define the table structure
    id = Column(Integer, primary_key=True, autoincrement=True)
    filename = Column(String(36), index=True, unique=True)
    filepath = Column(String(100))
    rawfilename = Column(String(36))
    rawpath = Column(String(100))
    object_name = Column(String(50))
    mjd = Column(Float)
    dateobs = Column(Date)
    dayobs  = Column(Date, index=True)
    exptime = Column(Float)
    filter_name = Column(String(2))
    grism = Column(String(20))
    telescope = Column(String(20))
    instrument = Column(String(20))
    obstype = Column(String(20))
    airmass = Column(Float)
    ut = Column(Time)
    slit = Column(String(20))
    lamp = Column(String(20))
    ra = Column(Float)
    dec = Column(Float)
    userid = Column(String(20))
    propid = Column(String(20))
    groupid = Column(String(20))
    tracknum = Column(String(20))
    reqnum = Column(String(20))
    ccdsum = Column(String(10))
    siteid = Column(String(10))


class Calibration_Image(Base):
    __tablename__ = 'calimages'
    id = Column(Integer, primary_key=True, autoincrement=True)


class Reduction_Status(Base):
    __tablename__ = 'reductionstatus'
    id = Column(Integer, primary_key=True, autoincrement=True)


def create_db():
    # Create an engine for the database
    engine = create_engine(db_address)

    # Create all tables in the engine
    # This only needs to be run once on initialization.
    Base.metadata.create_all(engine)
