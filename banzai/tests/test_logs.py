import pytest

from banzai.tests.utils import FakeLCOObservationFrame
from banzai.logs import _image_to_tags

pytestmark = pytest.mark.logs


def test_image_to_tags():
    image = FakeLCOObservationFrame()
    tags = _image_to_tags(image)
    for key, item in tags.items():
        if key not in ['request_num', 'instrument', 'site']:
            assert getattr(image, key) == item
