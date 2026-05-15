"""RabbitMQ publishing helpers.

Two patterns are used in banzai:

- ``post_to_archive_queue`` publishes a kombu-serialized dict to the
  ``fits_files`` fanout exchange. Consumed by ``PipelineListener`` in
  ``banzai/main.py`` as the archive-ingestion path.
- ``publish_raw_string_to_queue`` publishes a plain-text JSON string to a
  named queue via the default exchange. Mirrors how the site software
  publishes subframe-ready notifications: bodies arrive as bytes/str and
  the consumer must ``json.loads`` them rather than relying on kombu to
  deserialize a dict.
"""
from kombu import Connection, Exchange, Queue


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


def publish_raw_string_to_queue(queue_name, body, broker_url='amqp://localhost:5672'):
    """Publish a raw string to a named RabbitMQ queue.

    Mirrors how the site software publishes subframe-ready notifications:
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
