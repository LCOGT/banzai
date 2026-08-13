import json

from kombu import Connection, Producer, Queue

from banzai.utils import messaging


def publish_and_fetch(fits_path, small_thumbnail, large_thumbnail, **kwargs):
    messaging.post_to_shipper_queue('memory://', 'ship_files', 'ship', fits_path, small_thumbnail,
                                    large_thumbnail, **kwargs)
    with Connection('memory://') as connection:
        queue = Queue('ship')(connection.channel())
        message = queue.get(no_ack=True)
        queue.purge()
    return message


def test_post_to_shipper_queue_confirms_on_declared_channel(monkeypatch):
    # The memory transport preserves the real kombu topology and publish path,
    # but does not model broker acknowledgements, so record the safety options.
    declared_queues = []
    producer_calls = []
    publish_calls = []
    real_queue_declare = Queue.declare
    real_connection_producer = Connection.Producer
    real_producer_publish = Producer.publish

    def record_queue_declare(self, *args, **kwargs):
        declared_queues.append(self)
        return real_queue_declare(self, *args, **kwargs)

    def record_connection_producer(self, channel=None, *args, **kwargs):
        producer_calls.append((self, channel, kwargs.copy()))
        return real_connection_producer(self, channel, *args, **kwargs)

    def record_producer_publish(self, body, *args, **kwargs):
        publish_calls.append(kwargs.copy())
        return real_producer_publish(self, body, *args, **kwargs)

    monkeypatch.setattr(Queue, 'declare', record_queue_declare)
    monkeypatch.setattr(Connection, 'Producer', record_connection_producer)
    monkeypatch.setattr(Producer, 'publish', record_producer_publish)

    message = publish_and_fetch('/data/final.fits', '/data/final-small_thumbnail.jpg',
                                '/data/final-large_thumbnail.jpg')

    connection, producer_channel, producer_kwargs = producer_calls[0]
    declared_queue = declared_queues[0]
    assert message is not None
    assert connection.connect_timeout == 5
    assert connection.transport_options['confirm_publish'] is True
    assert declared_queue.durable is True
    assert declared_queue.exchange.type == 'fanout'
    assert declared_queue.exchange.durable is True
    assert producer_channel is declared_queue.channel
    assert producer_kwargs['exchange'].name == declared_queue.exchange.name
    assert producer_kwargs['auto_declare'] is False
    assert publish_calls[0]['confirm_timeout'] == 5


def test_post_to_shipper_queue_body_final(monkeypatch):
    monkeypatch.setattr(messaging.time, 'time', lambda: 1783200000.123)
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
