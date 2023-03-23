import mock
import pytest

from banzai.tests.utils import FakeLCOObservationFrame, FakeContext
from banzai.qc.header_checker import HeaderChecker
from banzai.logs import get_logger

logger = get_logger()

pytestmark = pytest.mark.header_checker


class FakeHeaderImage(FakeLCOObservationFrame):
    def __init__(self, meta=None):
        super(FakeHeaderImage, self).__init__()
        if meta is None:
            self.primary_hdu.meta = {}
        else:
            self.primary_hdu.meta = meta


def test_null_input_image():
    tester = HeaderChecker(FakeContext())
    image = tester.run(None)
    assert image is None


def test_all_keywords_missing():
    tester = HeaderChecker(FakeContext())
    bad_keywords = tester.check_keywords_missing_or_na(FakeHeaderImage())
    assert set(bad_keywords) == set(tester.expected_header_keywords)


def test_all_keywords_na():
    tester = HeaderChecker(FakeContext())
    image = FakeHeaderImage({keyword: "N/A" for keyword in tester.expected_header_keywords})
    bad_keywords = tester.check_keywords_missing_or_na(image)
    assert set(bad_keywords) == set(tester.expected_header_keywords)


def test_all_keywords_okay():
    tester = HeaderChecker(FakeContext())
    image = FakeHeaderImage({keyword: "test" for keyword in tester.expected_header_keywords})
    bad_keywords = tester.check_keywords_missing_or_na(image)
    assert set(bad_keywords) == set([])


def test_one_keyword_missing_and_one_na():
    tester = HeaderChecker(FakeContext())
    image = FakeHeaderImage({keyword: "test" for keyword in tester.expected_header_keywords[1:]})
    image.meta[tester.expected_header_keywords[1]] = 'N/A'
    bad_keywords = tester.check_keywords_missing_or_na(image)
    assert set(bad_keywords) == set(tester.expected_header_keywords[0:2])


def test_ra_outside_range():
    logger.error = mock.MagicMock()
    tester = HeaderChecker(FakeContext())
    tester.check_ra_range(FakeHeaderImage({'CRVAL1': 360.1}))
    assert logger.error.called


def test_ra_within_range():
    logger.error = mock.MagicMock()
    tester = HeaderChecker(FakeContext())
    tester.check_ra_range(FakeHeaderImage({'CRVAL1': 359.9}))
    tester.check_ra_range(FakeHeaderImage({'CRVAL1': 0.1}))
    assert not logger.error.called


def test_dec_outside_range():
    logger.error = mock.MagicMock()
    tester = HeaderChecker(FakeContext())
    tester.check_dec_range(FakeHeaderImage({'CRVAL2': -90.1}))
    assert logger.error.called


def test_dec_within_range():
    logger.error = mock.MagicMock()
    tester = HeaderChecker(FakeContext())
    tester.check_dec_range(FakeHeaderImage({'CRVAL2': -89.9}))
    tester.check_dec_range(FakeHeaderImage({'CRVAL2': 89.9}))
    tester.check_dec_range(FakeHeaderImage({'CRVAL2': 0.0}))
    assert not logger.error.called


def test_null_exptime_value():
    logger.error = mock.MagicMock()
    tester = HeaderChecker(FakeContext())
    tester.check_exptime_value(FakeHeaderImage({'EXPTIME': 0.0, 'OBSTYPE': 'test'}))
    assert logger.error.called


def test_negative_exptime_value():
    logger.error = mock.MagicMock()
    tester = HeaderChecker(FakeContext())
    tester.check_exptime_value(FakeHeaderImage({'EXPTIME': -0.1, 'OBSTYPE': 'test'}))
    assert logger.error.called


def test_null_or_negative_exptime_value_for_bias():
    logger.error = mock.MagicMock()
    tester = HeaderChecker(FakeContext())
    tester.check_exptime_value(FakeHeaderImage({'EXPTIME': 0.0, 'OBSTYPE': 'BIAS'}))
    tester.check_exptime_value(FakeHeaderImage({'EXPTIME': -0.1, 'OBSTYPE': 'BIAS'}))
    assert not logger.error.called


def test_postive_exptime_value():
    logger.error = mock.MagicMock()
    tester = HeaderChecker(FakeContext())
    tester.check_exptime_value(FakeHeaderImage({'EXPTIME': 0.1, 'OBSTYPE': 'test'}))
    assert not logger.error.called
