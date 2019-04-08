import argparse
import logging

from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, String, Date, ForeignKey, Boolean, CHAR
from sqlalchemy.ext.declarative import declarative_base

from banzai import dbs, logs, main

logger = logging.getLogger(__name__)

Base = declarative_base()


# The five base classes below are taken from Banzai version < 0.16.0
class CalibrationImage(Base):
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
    __tablename__ = 'telescopes'
    id = Column(Integer, primary_key=True, autoincrement=True)
    site = Column(String(10), ForeignKey('sites.id'), index=True)
    instrument = Column(String(20), index=True)
    camera_type = Column(String(20))
    schedulable = Column(Boolean, default=False)


class Site(Base):
    __tablename__ = 'sites'
    id = Column(String(3), primary_key=True)
    timezone = Column(Integer)


class BadPixelMask(Base):
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


def create_new_db(db_address):
    engine = create_engine(db_address)
    dbs.Base.metadata.create_all(engine)


def base_to_dict(base):
    return [{key: value for key, value in row.__dict__.items() if not key.startswith('_')} for row in base]


def change_key_name(row_list, old_key, new_key):
    for row in row_list:
        row[new_key] = row.pop(old_key)


def add_rows(db_session, base, row_list, max_chunk_size=100000):
    for i in range(0, len(row_list), max_chunk_size):
        logger.debug("Inserting rows {a} to {b}".format(a=i+1, b=min(i+max_chunk_size, len(row_list))))
        db_session.bulk_insert_mappings(base, row_list[i:i + max_chunk_size])
        db_session.commit()


def migrate_db():

    parser = argparse.ArgumentParser()
    parser.add_argument('old_db_address',
                        help='Old database address to be migrated: Should be in SQLAlchemy form')
    parser.add_argument('new_db_address',
                        help='New database address: Should be in SQLAlchemy form')
    parser.add_argument("--log-level", default='debug', choices=['debug', 'info', 'warning',
                                                                 'critical', 'fatal', 'error'])
    args = parser.parse_args()

    logs.set_log_level(args.log_level)
    logger.info("Creating new DB {new_db_address} from old DB {old_db_address}".format(
        new_db_address=args.new_db_address, old_db_address=args.old_db_address))
    create_new_db(args.new_db_address)

    with dbs.get_session(db_address=args.old_db_address) as old_db_session, dbs.get_session(db_address=args.new_db_address) as new_db_session:

        # First copy sites table
        logger.info("Querying and organizing the old Site table")
        sites = base_to_dict(old_db_session.query(Site).all())
        logger.info("Adding {n} rows from the old Site table to the new Site table".format(n=len(sites)))
        add_rows(new_db_session, dbs.Site, sites)

        # Move Telescope to Instrument with a couple of variable renames
        logger.info("Querying and organizing the old Telescope table")
        telescopes = base_to_dict(old_db_session.query(Telescope).all())
        change_key_name(telescopes, 'instrument', 'camera')
        change_key_name(telescopes, 'camera_type', 'type')
        logger.info("Adding {n} rows from the old Telescope table to the new Instrument table".format(
            n=len(telescopes)))
        add_rows(new_db_session, dbs.Instrument, telescopes)

        # Move old BPMs to CalibrationImage
        logger.info("Querying and organizing the old BadPixelMask table")
        bpms = base_to_dict(old_db_session.query(BadPixelMask).all())
        for row in bpms:
            row['type'] = 'BPM'
            row['is_master'] = True
            row['attributes'] = {'ccdsum': row.pop('ccdsum')}
            del(row['id'])
        change_key_name(bpms, 'creation_date', 'dateobs')
        change_key_name(bpms, 'telescope_id', 'instrument_id')
        # BPMs have some duplicates, remove them
        already_seen = []
        bpms_pruned = []
        for row in bpms:
            if row['filename'] not in already_seen:
                bpms_pruned.append(row)
                already_seen.append(row['filename'])
        logger.info("Adding {n} rows from the old BadPixelMask table to the new CalibrationImage table".format(
            n=len(bpms_pruned)))
        add_rows(new_db_session, dbs.CalibrationImage, bpms_pruned)

        # Convert old CalibrationImage to new type
        logger.info("Querying and organizing the old CalibrationsImage table")
        calibrations = base_to_dict(old_db_session.query(CalibrationImage).all())
        for row in calibrations:
            row['is_master'] = True
            row['attributes'] = {'filter': row.pop('filter_name'), 'ccdsum': row.pop('ccdsum')}
            del(row['id'])
        change_key_name(calibrations, 'dayobs', 'dateobs')
        change_key_name(calibrations, 'telescope_id', 'instrument_id')
        logger.info("Adding {n} rows from the old CalibrationImage table to the new CalibrationImage table".format(
            n=len(calibrations)))
        add_rows(new_db_session, dbs.CalibrationImage, calibrations)

        # Copy the PreviewImage table to ProcssedImage (attributes are all the same)
        logger.info("Querying and organizing the old PreviewImage table")
        preview_images = base_to_dict(old_db_session.query(PreviewImage).all())
        logger.info("Adding {n} rows from the old PreviewImage table to the new ProcessedImage table".format(
            n=len(preview_images)))
        add_rows(new_db_session, dbs.ProcessedImage, preview_images)

        logger.info("Finished")
