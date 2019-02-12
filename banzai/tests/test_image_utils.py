import pytest

from banzai.utils import image_utils
from banzai.tests.utils import FakeImage


def throws_inhomogeneous_set_exception(image1, image2,  keyword, additional_group_by_attributes=None):
    with pytest.raises(image_utils.InhomogeneousSetException) as exception_info:
        image_utils.check_image_homogeneity([image1, image2], additional_group_by_attributes)
    assert 'Images have different {0}s'.format(keyword) == str(exception_info.value)


def test_raises_exception_if_nx_are_different():
    throws_inhomogeneous_set_exception(FakeImage(nx=101), FakeImage(nx=102), 'nx')


def test_raises_exception_if_ny_are_different():
    throws_inhomogeneous_set_exception(FakeImage(ny=103), FakeImage(ny=104), 'ny')


def test_raises_exception_if_sites_are_different():
    throws_inhomogeneous_set_exception(FakeImage(site='elp'), FakeImage(site='ogg'), 'site')


def test_raises_exception_if_cameras_are_different():
    throws_inhomogeneous_set_exception(FakeImage(camera='kb76'), FakeImage(camera='kb77'), 'camera')


def test_raises_exception_if_ccdsums_are_different():
    throws_inhomogeneous_set_exception(FakeImage(ccdsum='1 1'), FakeImage(ccdsum='2 2'), 'ccdsum', ['ccdsum'])


def test_raises_exception_if_filters_are_different():
    throws_inhomogeneous_set_exception(FakeImage(filter='w'), FakeImage(filter='V'), 'filter', ['filter'])
