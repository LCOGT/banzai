import pytest

from banzai.tests.utils import FakeLCOObservationFrame, FakeContext, FakeInstrument, FakeCCDData
from banzai.utils import image_utils

pytestmark = pytest.mark.image_utils


def test_image_can_be_processed_known_obstype():
    image = FakeLCOObservationFrame([FakeCCDData(meta={'OBSTYPE': 'BIAS'})])

    assert image_utils.image_can_be_processed(image, FakeContext())


def test_image_cannot_be_processed_unknown_obstype():
    image = FakeLCOObservationFrame([FakeCCDData(meta={'OBSTYPE': 'FOO'})])

    assert not image_utils.image_can_be_processed(image, FakeContext())


def test_sinistro_image_can_be_processed():
    image = FakeLCOObservationFrame([FakeCCDData(meta={'OBSTYPE': 'BIAS'})])
    image.instrument = FakeInstrument(type='1m0-SciCam-Sinistro')

    assert image_utils.image_can_be_processed(image, FakeContext())


def test_nres_image_cannot_be_processed():
    image = FakeLCOObservationFrame([FakeCCDData(meta={'OBSTYPE': 'BIAS'})])
    image.instrument = FakeInstrument(type='1m0-NRES-SciCam')

    assert not image_utils.image_can_be_processed(image, FakeContext())


def test_floyds_image_cannot_be_processed():
    image = FakeLCOObservationFrame([FakeCCDData(meta={'OBSTYPE': 'BIAS'})])
    image.instrument = FakeInstrument(type='2m0-FLOYDS-SciCam')

    assert not image_utils.image_can_be_processed(image, FakeContext())
