from __future__ import absolute_import, division, print_function, unicode_literals

import logging
from kombu.mixins import ConsumerMixin

from banzai import tasks

logger = logging.getLogger('banzai')


class PreviewModeListener(ConsumerMixin):
    def __init__(self, broker_url, pipeline_context):
        self.broker_url = broker_url
        self.pipeline_context = pipeline_context

    def get_consumers(self, Consumer, channel):
        return [Consumer(queues=[self.queue], callbacks=[self.on_message])]

    def on_message(self, body, message):
        path = body.get('path')
        if 'e00.fits' in path or 's00.fits' in path:
            tasks.reduce_preview_image.delay(path, self.pipeline_context.copy())
        message.ack()


class EndOfNightListener(ConsumerMixin):
    def __init__(self, broker_url, pipeline_context):
        self.broker_url = broker_url
        self.pipeline_context = pipeline_context

    def get_consumers(self, Consumer, channel):
        return [Consumer(queues=[self.queue], callbacks=[self.on_message])]

    def on_message(self, body, message):
        site = body.get('site')
        instrument = body.get('instrument')
        dayobs = body.get('dayobs')

        tasks.reduce_end_of_night.delay(site, self.pipeline_context.copy(), instrument=instrument,
                                        dayobs=dayobs)
        message.ack()
