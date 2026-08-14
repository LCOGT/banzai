"""RabbitMQ publishing helpers.

Three patterns are used in banzai:

- ``post_to_archive_queue`` publishes a kombu-serialized dict to the
  ``fits_files`` fanout exchange. Consumed by ``PipelineListener`` in
  ``banzai/main.py`` as the archive-ingestion path.
- ``post_to_shipper_queue`` publishes a plain-text JSON string to the fanout
  exchange consumed by the site shipper. It declares the durable queue
  binding before publishing so RabbitMQ does not drop fanout messages with
  no bound queue.
- ``publish_raw_string_to_queue`` publishes a plain-text JSON string to a
  named queue via the default exchange. Mirrors how the site software
  publishes stackframe-ready notifications: bodies arrive as bytes/str and
  the consumer must ``json.loads`` them rather than relying on kombu to
  deserialize a dict.

The site convention is plain-text JSON everywhere the site software is the
peer: consumers ``json.loads`` the raw body themselves, so publishing a
kombu-serialized dict (``application/json``) hands them an already-decoded
dict and crashes them.
"""
import json
import time

from kombu import Connection, Exchange, Queue


SHIPPER_CONNECT_TIMEOUT = 5  # seconds
SHIPPER_CONFIRM_TIMEOUT = 5  # seconds


def post_to_archive_queue(filename, broker_url, exchange_name='fits_files', **kwargs):
    """Post file to RabbitMQ listener queue for processing.

    kwargs should include either 'frameid' (int) or 'path' (str), plus any
    additional metadata like SITEID, INSTRUME.
    """
    if 'frameid' not in kwargs and 'path' not in kwargs:
        raise ValueError("post_to_archive_queue requires either 'frameid' or 'path' in kwargs")
    exchange = Exchange(exchange_name, type='fanout')
    with Connection(broker_url) as conn:
        producer = conn.Producer(exchange=exchange)
        body = {'filename': filename}
        if 'frameid' in kwargs:
            body['frameid'] = f'{kwargs.pop("frameid"):d}'
        if 'path' in kwargs:
            body['path'] = kwargs.pop('path')
        body.update(kwargs)
        producer.publish(body)
        producer.release()


def post_to_shipper_queue(broker_url, exchange_name, queue_name, fits_path, small_thumbnail, large_thumbnail,
                          thumbnail_metadata=None):
    """Publish smartstack product paths for the site shipper to upload.

    Fanout exchanges drop messages when no queue is bound, so the durable
    queue binding is declared before publishing each message.

    The body is sent as a plain-text JSON string: the shipper json.loads the
    raw body itself and rejects kombu-serialized dicts as non-transient
    failures (no retry), silently dropping the product. Preview callers may
    submit thumbnail_metadata when no FITS path is available. Publisher
    confirms make broker rejection or acknowledgement timeout visible to the
    caller so the stack finalizer can retry. Each publish gets a fresh epoch-ms
    timestamp marking when this product is submitted to the shipper.
    """
    exchange = Exchange(exchange_name, type='fanout', durable=True)
    queue = Queue(queue_name, exchange=exchange, durable=True)
    body = {
        'fits': fits_path,
        'small_thumbnail': small_thumbnail,
        'large_thumbnail': large_thumbnail,
    }
    if thumbnail_metadata is not None:
        body['thumbnail_metadata'] = thumbnail_metadata

    with Connection(broker_url, connect_timeout=SHIPPER_CONNECT_TIMEOUT,
                    transport_options={'confirm_publish': True}) as conn:
        channel = conn.channel()
        bound_queue = queue(channel)
        bound_queue.declare()
        with conn.Producer(channel=channel, exchange=bound_queue.exchange, auto_declare=False) as producer:
            body['instrument_enqueue_timestamp'] = int(time.time() * 1000)
            producer.publish(json.dumps(body), content_type='text/plain', content_encoding='utf-8',
                             confirm_timeout=SHIPPER_CONFIRM_TIMEOUT)


def publish_raw_string_to_queue(queue_name, body, broker_url='amqp://localhost:5672'):
    """Publish a raw string to a named RabbitMQ queue.

    Mirrors how the site software publishes stackframe-ready notifications:
    the body is sent as a plain-text JSON string to a named queue via the
    default exchange, with content_type='text/plain' (no kombu serialization).
    Consumers receive a bytes/str body and must json.loads it themselves
    rather than relying on kombu to deserialize a dict.
    """
    with Connection(broker_url) as conn:
        queue = Queue(queue_name, channel=conn.channel())
        queue.declare()
        with conn.Producer() as producer:
            producer.publish(body, routing_key=queue_name,
                             content_type='text/plain', content_encoding='utf-8')
