from banzai.tests.utils import FakeImage
from banzai.logs import _image_to_tags


def test_image_to_tags():
    image = FakeImage()
    tags = _image_to_tags(image)
    for key, item in tags.items():
        if key not in ['request_num', 'instrument']:
            assert getattr(image, key) == item
