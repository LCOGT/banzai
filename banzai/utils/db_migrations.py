import argparse

from sqlalchemy import Column, Integer, String, Date, ForeignKey, Boolean, CHAR
from sqlalchemy.ext.declarative import declarative_base

from banzai import dbs


Base = declarative_base()


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


def base_to_dict(base):
    return [{key: value for key, value in row.__dict__.items() if not key.startswith('_')} for row in base]


def add_rows(base, row_list, db_session):
    [db_session.add(base(**row)) for row in row_list]


def change_key_name(row_list, old_key, new_key):
    for row in row_list:
        row[new_key] = row.pop(old_key)


def migrate_db():

    parser = argparse.ArgumentParser()
    parser.add_argument('old_db_address',
                        help='Old database for migration: Should be in SQLAlchemy form')
    parser.add_argument('new_db_address',
                        help='New databse: Should be in SQLAlchemy form')
    args = parser.parse_args()

    # Create new db
    dbs.create_db(args.new_db_address)

    old_db_session = dbs.get_session(db_address=args.old_db_address)
    new_db_session = dbs.get_session(db_address=args.new_db_address)

    # First copy sites table
    sites = base_to_dict(old_db_session.query(Site).all())
    add_rows(dbs.Site, sites, new_db_session)

    # Copy the PreviewImage table to ProcssedImage (attributes are all the same)
    preview_images = base_to_dict(old_db_session.query(PreviewImage).all())
    add_rows(dbs.ProcessedImage, preview_images, new_db_session)

    # Move Telescope to Instrument with a couple of variable renames
    telescopes = base_to_dict(old_db_session.query(Telescope).all())
    change_key_name(telescopes, 'instrument', 'camera')
    change_key_name(telescopes, 'camera_type', 'type')
    add_rows(dbs.Instrument, telescopes, new_db_session)

    # Convert old CalibrationImage to new type
    calibrations = base_to_dict(old_db_session.query(CalibrationImage).all())
    change_key_name(calibrations, 'dayobs', 'dateobs')
    change_key_name(calibrations, 'telescope_id', 'instrument_id')
    for row in calibrations:
        row['is_master'] = True
        row['attributes'] = {'filter': row.pop('filter_name'), 'ccdsum': row.pop('ccdsum')}
        del(row['id'])
    add_rows(dbs.CalibrationImage, calibrations, new_db_session)

    # Add BPMs to CalibrationImage
    bpms = base_to_dict(old_db_session.query(BadPixelMask).all())
    change_key_name(bpms, 'creation_date', 'dateobs')
    change_key_name(bpms, 'telescope_id', 'instrument_id')
    for row in bpms:
        row['type'] = 'BPM'
        row['is_master'] = True
        row['attributes'] = {'ccdsum': row.pop('ccdsum')}
        del(row['id'])
    add_rows(dbs.CalibrationImage, bpms, new_db_session)

    new_db_session.commit()
    new_db_session.close()
    old_db_session.close()
