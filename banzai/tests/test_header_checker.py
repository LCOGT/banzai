from banzai.tests.utils import FakeImage
from banzai.qc import header_checker


import mock
import numpy as np

def test_no_input_images():
    tester = header_checker.HeaderSanity(None)
    images = tester.do_stage([])
    assert len(images) == 0


def test_group_by_keywords():
    tester = header_checker.HeaderSanity(None)
    assert tester.group_by_keywords is None

def test_no_exptime_in_frames():
    tester = header_checker.HeaderSanity(None)
    tester.logger.error = mock.MagicMock()
    nx = 101
    ny = 103

    images = [FakeImage(nx=nx, ny=ny) for x in range(6)]

    for image in images:

        tester.check_header_keyword_present(['EXPTIME'], image)
        assert tester.logger.error.called

    assert len(images) == 6

def test_bad_RA_format():
    tester = header_checker.HeaderSanity(None)
    tester.logger.error = mock.MagicMock()
    nx = 101
    ny = 103

    images = [FakeImage(nx=nx, ny=ny) for x in range(6)]
    for image in images:
        image.header['RA'] = 20

        tester.check_header_format('RA', image)
        assert tester.logger.error.called

    assert len(images) == 6


def test_NA_in_RA():
    tester = header_checker.HeaderSanity(None)
    tester.logger.error = mock.MagicMock()
    nx = 101
    ny = 103

    images = [FakeImage(nx=nx, ny=ny) for x in range(6)]
    for image in images:
        image.header['RA'] = 'N/A'

        tester.check_header_na(['RA'], image)
        assert tester.logger.error.called

    assert len(images) == 6


def test_RA_outside_range():
    tester = header_checker.HeaderSanity(None)
    tester.logger.error = mock.MagicMock()
    nx = 101
    ny = 103

    images = [FakeImage(nx=nx, ny=ny) for x in range(8)]
    for image in images:
        image.header['CRVAL1'] = 7892.28

        tester.check_ra_range(image)
        assert tester.logger.error.called

    assert len(images) == 8


def test_DEC_outside_range():
    tester = header_checker.HeaderSanity(None)
    tester.logger.error = mock.MagicMock()
    nx = 101
    ny = 103

    images = [FakeImage(nx=nx, ny=ny) for x in range(5)]
    for image in images:
        image.header['CRVAL2'] = -189.52

        tester.check_dec_range(image)
        assert tester.logger.error.called

    assert len(images) == 5


def test_negative_EXPTIME_value():
    tester = header_checker.HeaderSanity(None)
    tester.logger.error = mock.MagicMock()
    nx = 101
    ny = 103

    images = [FakeImage(nx=nx, ny=ny) for x in range(5)]
    for image in images:
        image.header['EXPTIME'] = -np.random.uniform(0, 1000)

        tester.check_exptime_value(image)
        assert tester.logger.error.called

    assert len(images) == 5


def test_nulle_EXPTIME_value_on_science_frames():
    tester = header_checker.HeaderSanity(None)
    tester.logger.error = mock.MagicMock()
    nx = 101
    ny = 103

    images = [FakeImage(nx=nx, ny=ny) for x in range(5)]
    for image in images:
        image.header['EXPTIME'] = 0.0
        image.header['OBSTYPE'] = 'EXPOSE'

        tester.check_exptime_value(image)
        assert tester.logger.error.called

    assert len(images) == 5
