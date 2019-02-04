import logging
import abc

from banzai import logs

logger = logging.getLogger(__name__)


class Stage(abc.ABC):

    def __init__(self, pipeline_context):
        self.pipeline_context = pipeline_context

    @property
    def stage_name(self):
        return '.'.join([__name__, self.__class__.__name__])

    def run(self, images):
        if len(images) > 0:
            logger.info('Running {0}'.format(self.stage_name), image=images[0])
        processed_images = []
        try:
            processed_images = self.do_stage(images)
        except Exception:
            logger.error(logs.format_exception())
        return processed_images

    @abc.abstractmethod
    def do_stage(self, images):
        return images
