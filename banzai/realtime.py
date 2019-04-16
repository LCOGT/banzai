import logging

from kombu.mixins import ConsumerMixin

from banzai.celery import process_image

logger = logging.getLogger(__name__)


class RealtimeModeListener(ConsumerMixin):
    def __init__(self, runtime_context):
        self.runtime_context = runtime_context
        self.broker_url = runtime_context.broker_url

    def on_connection_error(self, exc, interval):
        logger.error("{0}. Retrying connection in {1} seconds...".format(exc, interval))
        self.connection = self.connection.clone()
        self.connection.ensure_connection(max_retries=10)

    def get_consumers(self, Consumer, channel):
        consumer = Consumer(queues=[self.queue], callbacks=[self.on_message])
        # Only fetch one thing off the queue at a time
        consumer.qos(prefetch_count=1)
        return [consumer]

    def on_message(self, body, message):
        path = body.get('path')
        process_image.apply_async(args=(path, self.runtime_context._asdict()))
        message.ack()  # acknowledge to the sender we got this message (it can be popped)
