"""
Populate the publication database with test data for site E2E tests.
"""

import datetime

from sqlalchemy import text

from banzai import dbs


# Hard-coded test data from AWS DB

SITE_DATA = {
    'id': 'lsc',
    'timezone': -4,
    'latitude': -30.167383,
    'longitude': -70.80479,
    'elevation': 2198
}

INSTRUMENT_DATA = {
    'id': 150140,
    'site': 'lsc',
    'camera': 'sq34',
    'type': '0m4-SciCam-QHY600',
    'name': 'sq34',
    'nx': 9564,
    'ny': 6376
}

# Phase 1: Top 2 calibrations per type (7 total)
INITIAL_CALIBRATIONS = [
    # BIAS - top 2
    {
        'id': 5979446,
        'type': 'BIAS',
        'filename': 'lsc0m476-sq34-20260121-bias-central30x30-bin1x1.fits.fz',
        'frameid': 90974905,
        'dateobs': datetime.datetime(2026, 1, 21, 16, 24, 34),
        'attributes': {'configuration_mode': 'central30x30', 'binning': '[1, 1]'},
    },
    {
        'id': 5975897,
        'type': 'BIAS',
        'filename': 'lsc0m476-sq34-20260120-bias-central30x30-bin1x1.fits.fz',
        'frameid': 90939126,
        'dateobs': datetime.datetime(2026, 1, 20, 16, 24, 19),
        'attributes': {'configuration_mode': 'central30x30', 'binning': '[1, 1]'},
    },
    # DARK - top 2
    {
        'id': 5979427,
        'type': 'DARK',
        'filename': 'lsc0m476-sq34-20260121-dark-central30x30-bin1x1.fits.fz',
        'frameid': 90974779,
        'dateobs': datetime.datetime(2026, 1, 21, 16, 35, 18),
        'attributes': {'configuration_mode': 'central30x30', 'binning': '[1, 1]', 'ccd_temperature': '5'},
    },
    {
        'id': 5975887,
        'type': 'DARK',
        'filename': 'lsc0m476-sq34-20260120-dark-central30x30-bin1x1.fits.fz',
        'frameid': 90938981,
        'dateobs': datetime.datetime(2026, 1, 20, 16, 35, 3),
        'attributes': {'configuration_mode': 'central30x30', 'binning': '[1, 1]', 'ccd_temperature': '5'},
    },
    # SKYFLAT - top 2
    {
        'id': 5979769,
        'type': 'SKYFLAT',
        'filename': 'lsc0m476-sq34-20260121-skyflat-central30x30-bin1x1-V.fits.fz',
        'frameid': 90976978,
        'dateobs': datetime.datetime(2026, 1, 22, 0, 2, 57),
        'attributes': {'configuration_mode': 'central30x30', 'binning': '[1, 1]', 'filter': 'V'},
    },
    {
        'id': 5968870,
        'type': 'SKYFLAT',
        'filename': 'lsc0m476-sq34-20260118-skyflat-central30x30-bin1x1-V.fits.fz',
        'frameid': 90873065,
        'dateobs': datetime.datetime(2026, 1, 18, 16, 48, 59),
        'attributes': {'configuration_mode': 'central30x30', 'binning': '[1, 1]', 'filter': 'V'},
    },
    # BPM - only 1
    {
        'id': 3829062,
        'type': 'BPM',
        'filename': 'lsc0m409-sq34-20240314-bpm-central30x30.fits.fz',
        'frameid': 69359142,
        'dateobs': datetime.datetime(2024, 3, 14, 21, 22, 45),
        'attributes': {'configuration_mode': 'central30x30', 'binning': '[1, 1]'},
    },
]

# Phase 2: Additional older calibrations (6 more)
ADDITIONAL_CALIBRATIONS = [
    # BIAS - older versions
    {
        'id': 5972173,
        'type': 'BIAS',
        'filename': 'lsc0m476-sq34-20260119-bias-central30x30-bin1x1.fits.fz',
        'frameid': 90904603,
        'dateobs': datetime.datetime(2026, 1, 19, 16, 24, 2),
        'attributes': {'configuration_mode': 'central30x30', 'binning': '[1, 1]'},
    },
    {
        'id': 5968578,
        'type': 'BIAS',
        'filename': 'lsc0m476-sq34-20260118-bias-central30x30-bin1x1.fits.fz',
        'frameid': 90872481,
        'dateobs': datetime.datetime(2026, 1, 18, 16, 23, 44),
        'attributes': {'configuration_mode': 'central30x30', 'binning': '[1, 1]'},
    },
    # DARK - older versions
    {
        'id': 5972154,
        'type': 'DARK',
        'filename': 'lsc0m476-sq34-20260119-dark-central30x30-bin1x1.fits.fz',
        'frameid': 90904508,
        'dateobs': datetime.datetime(2026, 1, 19, 16, 34, 46),
        'attributes': {'configuration_mode': 'central30x30', 'binning': '[1, 1]', 'ccd_temperature': '5'},
    },
    {
        'id': 5968565,
        'type': 'DARK',
        'filename': 'lsc0m476-sq34-20260118-dark-central30x30-bin1x1.fits.fz',
        'frameid': 90872463,
        'dateobs': datetime.datetime(2026, 1, 18, 16, 34, 26),
        'attributes': {'configuration_mode': 'central30x30', 'binning': '[1, 1]', 'ccd_temperature': '5'},
    },
    # SKYFLAT - older versions
    {
        'id': 5965298,
        'type': 'SKYFLAT',
        'filename': 'lsc0m476-sq34-20260117-skyflat-central30x30-bin1x1-V.fits.fz',
        'frameid': 90845177,
        'dateobs': datetime.datetime(2026, 1, 17, 16, 48, 41),
        'attributes': {'configuration_mode': 'central30x30', 'binning': '[1, 1]', 'filter': 'V'},
    },
    {
        'id': 5950906,
        'type': 'SKYFLAT',
        'filename': 'lsc0m476-sq34-20260113-skyflat-central30x30-bin1x1-V.fits.fz',
        'frameid': 90715810,
        'dateobs': datetime.datetime(2026, 1, 13, 16, 47, 11),
        'attributes': {'configuration_mode': 'central30x30', 'binning': '[1, 1]', 'filter': 'V'},
    },
]


def insert_calibrations(db_address: str, calibrations: list, skip_existing: bool = False) -> None:
    """Insert calibration records into the database."""
    with dbs.get_session(db_address) as session:
        print(f"Inserting {len(calibrations)} calibration images")
        for cal_data in calibrations:
            if skip_existing:
                existing = session.query(dbs.CalibrationImage).filter(
                    dbs.CalibrationImage.id == cal_data['id']
                ).first()
                if existing:
                    print(f"  - {cal_data['type']}: {cal_data['filename']} (already exists, skipping)")
                    continue

            cal = dbs.CalibrationImage(
                id=cal_data['id'],
                type=cal_data['type'],
                filename=cal_data['filename'],
                filepath=None,
                frameid=cal_data['frameid'],
                dateobs=cal_data['dateobs'],
                instrument_id=INSTRUMENT_DATA['id'],
                is_master=True,
                is_bad=False,
                attributes=cal_data['attributes'],
            )
            session.add(cal)
            print(f"  - {cal_data['type']}: {cal_data['filename']}")

        session.commit()

    print("Calibration insertion complete")


def insert_initial_data(db_address: str) -> None:
    """Insert site, instrument, and top 2 calibrations per type."""
    print(f"Creating database schema at {db_address}")
    dbs.create_db(db_address)

    print("Creating publication banzai_calibrations")
    with dbs.get_session(db_address) as session:
        session.execute(text(
            "CREATE PUBLICATION banzai_calibrations FOR TABLE sites, instruments, calimages"
        ))
        session.commit()

    with dbs.get_session(db_address) as session:
        print(f"Inserting site: {SITE_DATA['id']}")
        site = dbs.Site(**SITE_DATA)
        session.add(site)
        session.flush()

        print(f"Inserting instrument: {INSTRUMENT_DATA['name']} (id={INSTRUMENT_DATA['id']})")
        instrument = dbs.Instrument(**INSTRUMENT_DATA)
        session.add(instrument)
        session.flush()
        session.commit()

    insert_calibrations(db_address, INITIAL_CALIBRATIONS)
    print("Initial data insertion complete")


def insert_additional_calibrations(db_address: str) -> None:
    """Insert older calibrations for cache update tests."""
    insert_calibrations(db_address, ADDITIONAL_CALIBRATIONS, skip_existing=True)
    print("Additional calibrations insertion complete")
