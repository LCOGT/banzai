from banzai.tests.utils import FakeImage, FakeInstrument
from banzai.munge import set_crosstalk_header_keywords, sinistro_mode_is_supported
import numpy as np


def test_no_coefficients_no_defaults():
    assert not sinistro_mode_is_supported(FakeImage(data=np.zeros((4, 103, 101)), header={'INSTRUME': 'blah27'}))


def test_no_coefficients_with_defaults():
    assert not sinistro_mode_is_supported(FakeImage(data=np.zeros((4, 103, 101)), header={'INSTRUME': 'fa06'}))


def test_when_has_partial_coefficients():
    header = {'CRSTLK{i}{j}'.format(i=i+1, j=j+1): 0 for i in range(2) for j in range(2)}
    header['INSTRUME'] = 'blah27'
    assert not sinistro_mode_is_supported(FakeImage(data=np.zeros((4, 103, 101)), header=header))


def test_when_has_coefficients():
    header = {'CRSTLK{i}{j}'.format(i=i + 1, j=j + 1): 0 for i in range(4) for j in range(4)}
    header['INSTRUME'] = 'blah27'
    assert sinistro_mode_is_supported(FakeImage(data=np.zeros((4, 103, 101)), header=header))


def test_defaults_do_not_override_header():
    header = {'CRSTLK{i}{j}'.format(i=i + 1, j=j + 1): 0 for i in range(4) for j in range(4)}
    header['INSTRUME'] = 'fa06'
    image = FakeImage(data=np.zeros((4, 103, 101)), header=header, camera='fa06')
    image.instrument = FakeInstrument(type='sinistro', camera='fa06')
    set_crosstalk_header_keywords(image)
    for j in range(4):
        for i in range(4):
            assert image.header['CRSTLK{i}{j}'.format(i=i + 1, j=j + 1)] == 0.0
