from banzai.images import Image
from banzai.tests.utils import FakeContext


def test_null_filename():
    test_image = Image(FakeContext, filename=None)
    assert test_image.data is None
