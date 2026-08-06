import json

from kombu import Connection, Queue

from banzai.utils import messaging


def publish_and_fetch(fits_path, small_thumbnail, large_thumbnail, **kwargs):
    messaging.post_to_shipper_queue('memory://', 'ship_files', 'ship', fits_path, small_thumbnail,
                                    large_thumbnail, 1783200000123, **kwargs)
    with Connection('memory://') as connection:
        queue = Queue('ship')(connection.channel())
        message = queue.get(no_ack=True)
        queue.purge()
    return message


def test_post_to_shipper_queue_body_final():
    message = publish_and_fetch('/data/final.fits', '/data/final-small_thumbnail.jpg',
                                '/data/final-large_thumbnail.jpg')
    # Receiving the message at all proves the durable queue was bound before
    # publish: fanout exchanges drop messages with no bound queue.
    assert message is not None
    # The shipper json.loads the raw body itself; a kombu-serialized dict
    # (application/json) crashes it as a non-retryable failure.
    assert message.content_type == 'text/plain'
    assert json.loads(message.body) == {
        'fits': '/data/final.fits',
        'small_thumbnail': '/data/final-small_thumbnail.jpg',
        'large_thumbnail': '/data/final-large_thumbnail.jpg',
        'instrument_enqueue_timestamp': 1783200000123,
    }


def test_post_to_shipper_queue_body_preview():
    thumbnail_metadata = {
        'frame_basename': 'preview-e45',
        'DATE-OBS': '2026-07-04T12:00:00.000',
        'DAY-OBS': '20260704',
        'INSTRUME': 'fa16',
        'SITEID': 'cpt',
        'TELID': '1m0a',
        'NCOMBINE': 2,
    }
    message = publish_and_fetch(None, '/data/preview-small_thumbnail.jpg',
                                '/data/preview-large_thumbnail.jpg',
                                thumbnail_metadata=thumbnail_metadata)
    assert message is not None
    assert message.content_type == 'text/plain'
    body = json.loads(message.body)
    assert body['fits'] is None
    assert body['thumbnail_metadata'] == thumbnail_metadata
