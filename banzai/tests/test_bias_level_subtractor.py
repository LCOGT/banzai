import pytest
import numpy as np

from banzai.bias import BiasMasterLevelSubtractor
from banzai.tests.utils import FakeImage


@pytest.fixture(scope='module')
def set_random_seed():
    np.random.seed(9723492)


class FakeBiasImage(FakeImage):
    def __init__(self, bias_level=0.0):
        super(FakeBiasImage, self).__init__(image_multiplier=bias_level)
        self.header = {'BIASLVL': bias_level}


def test_no_input_images():
    subtractor = BiasMasterLevelSubtractor(None)
    images = subtractor.do_stage([])
    assert len(images) == 0


def test_header_has_biaslevel():
    subtractor = BiasMasterLevelSubtractor(None)
    images = subtractor.do_stage([FakeImage() for x in range(6)])
    for image in images:
        assert 'BIASLVL' in image.header


def test_header_biaslevel_is_1():
    subtractor = BiasMasterLevelSubtractor(None)
    images = subtractor.do_stage([FakeImage(image_multiplier=1.0) for x in range(6)])
    for image in images:
        assert image.header.get('BIASLVL') == 1


def test_header_mbiaslevel_is_2():
    subtractor = BiasMasterLevelSubtractor(None)
    images = subtractor.do_stage([FakeImage(image_multiplier=2.0) for x in range(6)])
    for image in images:
        assert image.header.get('BIASLVL') == 2.0


def test_bias_master_level_subtraction_is_reasonable():
    input_bias = 2000.0
    read_noise = 15.0

    subtractor = BiasMasterLevelSubtractor(None)
    images = [FakeImage() for x in range(6)]
    for image in images:
        image.data = np.random.normal(input_bias, read_noise, size=(image.ny, image.nx))

    images = subtractor.do_stage(images)

    for image in images:
        np.testing.assert_allclose(np.zeros(image.data.shape), image.data, atol=8 * read_noise)
        np.testing.assert_allclose(image.header.get('BIASLVL'), input_bias, atol=1.0)
