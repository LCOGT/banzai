import importlib

import banzai.settings as settings
from banzai.utils import messaging


class RecordingChannel:
    def __init__(self, calls):
        self.calls = calls

    def exchange_declare(self, exchange, type, durable, **kwargs):
        self.calls.append(('exchange_declare', exchange, type, durable))

    def queue_declare(self, queue, durable, **kwargs):
        self.calls.append(('queue_declare', queue, durable))

    def queue_bind(self, queue, exchange, routing_key, **kwargs):
        self.calls.append(('queue_bind', queue, exchange, routing_key))

    def prepare_queue_arguments(self, arguments, **kwargs):
        return arguments


class RecordingProducer:
    def __init__(self, calls, exchange=None, **kwargs):
        self.calls = calls
        self.calls.append(('producer', exchange))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False

    def publish(self, body, **kwargs):
        self.calls.append(('publish', body, kwargs))

    def release(self):
        self.calls.append(('release',))


class RecordingConnection:
    instances = []

    def __init__(self, broker_url):
        self.broker_url = broker_url
        self.calls = []
        self.channel_instance = RecordingChannel(self.calls)
        self.__class__.instances.append(self)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False

    def channel(self):
        return self.channel_instance

    def Producer(self, exchange=None, **kwargs):
        return RecordingProducer(self.calls, exchange=exchange, **kwargs)


def patch_connection(monkeypatch):
    RecordingConnection.instances = []
    monkeypatch.setattr(messaging, 'Connection', RecordingConnection)


def test_post_to_shipper_queue_declares_durable_fanout(monkeypatch):
    patch_connection(monkeypatch)

    messaging.post_to_shipper_queue(
        'amqp://broker',
        'ship_files',
        'ship',
        '/data/final.fits',
        '/data/final-small_thumbnail.jpg',
        '/data/final-large_thumbnail.jpg',
        1783200000123,
    )

    connection = RecordingConnection.instances[0]
    exchange_declare = connection.calls[0]
    queue_declare_index = next(i for i, call in enumerate(connection.calls) if call[0] == 'queue_declare')
    queue_bind_index = next(i for i, call in enumerate(connection.calls) if call[0] == 'queue_bind')
    publish_index = next(i for i, call in enumerate(connection.calls) if call[0] == 'publish')

    assert exchange_declare == ('exchange_declare', 'ship_files', 'fanout', True)
    assert ('queue_declare', 'ship', True) in connection.calls
    assert ('queue_bind', 'ship', 'ship_files', '') in connection.calls
    assert queue_declare_index < publish_index
    assert queue_bind_index < publish_index


def test_post_to_shipper_queue_body_final(monkeypatch):
    patch_connection(monkeypatch)

    messaging.post_to_shipper_queue(
        'amqp://broker',
        'ship_files',
        'ship',
        '/data/final.fits',
        '/data/final-small_thumbnail.jpg',
        '/data/final-large_thumbnail.jpg',
        1783200000123,
    )

    publish_call = next(call for call in RecordingConnection.instances[0].calls if call[0] == 'publish')
    assert publish_call[1] == {
        'fits': '/data/final.fits',
        'small_thumbnail': '/data/final-small_thumbnail.jpg',
        'large_thumbnail': '/data/final-large_thumbnail.jpg',
        'instrument_enqueue_timestamp': 1783200000123,
    }


def test_post_to_shipper_queue_body_preview(monkeypatch):
    patch_connection(monkeypatch)

    messaging.post_to_shipper_queue(
        'amqp://broker',
        'ship_files',
        'ship',
        None,
        '/data/preview-small_thumbnail.jpg',
        '/data/preview-large_thumbnail.jpg',
        1783200000123,
    )

    publish_call = next(call for call in RecordingConnection.instances[0].calls if call[0] == 'publish')
    assert publish_call[1]['fits'] is None
    assert set(publish_call[1]) == {
        'fits',
        'small_thumbnail',
        'large_thumbnail',
        'instrument_enqueue_timestamp',
    }


def test_settings_shipper_defaults(monkeypatch):
    try:
        monkeypatch.delenv('SHIPPER_EXCHANGE', raising=False)
        monkeypatch.delenv('SHIPPER_QUEUE_NAME', raising=False)
        monkeypatch.delenv('SMARTSTACK_PREVIEWS', raising=False)
        importlib.reload(settings)

        assert settings.SHIPPER_EXCHANGE == 'ship_files'
        assert settings.SHIPPER_QUEUE_NAME == 'ship'
        assert settings.SMARTSTACK_PREVIEWS is True

        monkeypatch.setenv('SMARTSTACK_PREVIEWS', 'false')
        importlib.reload(settings)
        assert settings.SMARTSTACK_PREVIEWS is False
    finally:
        monkeypatch.undo()
        importlib.reload(settings)
