import pytest
import numpy as np

from banzai.bias import BiasMasterLevelSubtractor
from banzai.tests.utils import FakeImage


@pytest.fixture(scope='module')
def set_random_seed():
    np.random.seed(9723492)


def test_null_input_image():
    subtractor = BiasMasterLevelSubtractor(None)
    image = subtractor.run(None)
    assert image is None


def test_header_has_biaslevel():
    subtractor = BiasMasterLevelSubtractor(None)
    image = subtractor.do_stage(FakeImage())
    assert 'BIASLVL' in image.header


def test_header_biaslevel_is_1():
    subtractor = BiasMasterLevelSubtractor(None)
    image = subtractor.do_stage(FakeImage(image_multiplier=1.0))
    assert image.header.get('BIASLVL') == 1


def test_header_mbiaslevel_is_2():
    subtractor = BiasMasterLevelSubtractor(None)
    image = subtractor.do_stage(FakeImage(image_multiplier=2.0))
    assert image.header.get('BIASLVL') == 2.0


def test_bias_master_level_subtraction_is_reasonable(set_random_seed):
    input_bias = 2000.0
    read_noise = 15.0

    subtractor = BiasMasterLevelSubtractor(None)
    image = FakeImage()
    image.data = np.random.normal(input_bias, read_noise, size=(image.ny, image.nx))
    image = subtractor.do_stage(image)

    np.testing.assert_allclose(np.zeros(image.data.shape), image.data, atol=8 * read_noise)
    np.testing.assert_allclose(image.header.get('BIASLVL'), input_bias, atol=1.0)
