import logging

import mock

from banzai.tests.utils import FakeImage
from banzai.qc import header_checker

logger = logging.getLogger('banzai.qc.header_checker')


class FakeHeaderImage(FakeImage):
    def __init__(self, header=None):
        super(FakeHeaderImage, self).__init__()
        if header is None:
            self.header = {}
        else:
            self.header = header


def test_no_input_images():
    tester = header_checker.HeaderSanity(None)
    images = tester.do_stage([])
    assert len(images) == 0


def test_all_keywords_missing():
    logger.error = mock.MagicMock()
    tester = header_checker.HeaderSanity(None)
    bad_keywords = tester.check_keywords_missing_or_na(FakeHeaderImage())
    assert set(bad_keywords) == set(tester.expected_header_keywords)


def test_all_keywords_na():
    tester = header_checker.HeaderSanity(None)
    image = FakeHeaderImage({keyword: "N/A" for keyword in tester.expected_header_keywords})
    bad_keywords = tester.check_keywords_missing_or_na(image)
    assert set(bad_keywords) == set(tester.expected_header_keywords)


def test_all_keywords_okay():
    tester = header_checker.HeaderSanity(None)
    image = FakeHeaderImage({keyword: "test" for keyword in tester.expected_header_keywords})
    bad_keywords = tester.check_keywords_missing_or_na(image)
    assert set(bad_keywords) == set([])


def test_one_keyword_missing_and_one_na():
    tester = header_checker.HeaderSanity(None)
    image = FakeHeaderImage({keyword: "test" for keyword in tester.expected_header_keywords[1:]})
    image.header[tester.expected_header_keywords[1]] = 'N/A'
    bad_keywords = tester.check_keywords_missing_or_na(image)
    assert set(bad_keywords) == set(tester.expected_header_keywords[0:2])


def test_ra_outside_range():
    logger.error = mock.MagicMock()
    tester = header_checker.HeaderSanity(None)
    tester.check_ra_range(FakeHeaderImage({'CRVAL1': 360.1}))
    assert logger.error.called


def test_ra_within_range():
    logger.error = mock.MagicMock()
    tester = header_checker.HeaderSanity(None)
    tester.check_ra_range(FakeHeaderImage({'CRVAL1': 359.9}))
    tester.check_ra_range(FakeHeaderImage({'CRVAL1': 0.1}))
    assert not logger.error.called


def test_dec_outside_range():
    logger.error = mock.MagicMock()
    tester = header_checker.HeaderSanity(None)
    tester.check_dec_range(FakeHeaderImage({'CRVAL2': -90.1}))
    assert logger.error.called


def test_dec_within_range():
    logger.error = mock.MagicMock()
    tester = header_checker.HeaderSanity(None)
    tester.check_dec_range(FakeHeaderImage({'CRVAL2': -89.9}))
    tester.check_dec_range(FakeHeaderImage({'CRVAL2': 89.9}))
    tester.check_dec_range(FakeHeaderImage({'CRVAL2': 0.0}))
    assert not logger.error.called


def test_null_exptime_value():
    logger.error = mock.MagicMock()
    tester = header_checker.HeaderSanity(None)
    tester.check_exptime_value(FakeHeaderImage({'EXPTIME': 0.0, 'OBSTYPE': 'test'}))
    assert logger.error.called


def test_negative_exptime_value():
    logger.error = mock.MagicMock()
    tester = header_checker.HeaderSanity(None)
    tester.check_exptime_value(FakeHeaderImage({'EXPTIME': -0.1, 'OBSTYPE': 'test'}))
    assert logger.error.called


def test_null_or_negative_exptime_value_for_bias():
    logger.error = mock.MagicMock()
    tester = header_checker.HeaderSanity(None)
    tester.check_exptime_value(FakeHeaderImage({'EXPTIME': 0.0, 'OBSTYPE': 'BIAS'}))
    tester.check_exptime_value(FakeHeaderImage({'EXPTIME': -0.1, 'OBSTYPE': 'BIAS'}))
    assert not logger.error.called


def test_postive_exptime_value():
    logger.error = mock.MagicMock()
    tester = header_checker.HeaderSanity(None)
    tester.check_exptime_value(FakeHeaderImage({'EXPTIME': 0.1, 'OBSTYPE': 'test'}))
    assert not logger.error.called
