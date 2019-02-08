import logging
import abc
import itertools

from banzai import logs

logger = logging.getLogger(__name__)


class Stage(abc.ABC):

    def __init__(self, pipeline_context):
        self.pipeline_context = pipeline_context

    @property
    def stage_name(self):
        return '.'.join([__name__, self.__class__.__name__])

    def run(self, image):
        if image is None:
            return image
        logger.info('Running {0}'.format(self.stage_name), image=image)
        try:
            image = self.do_stage(image)
        except Exception:
            logger.error(logs.format_exception())
        return image

    @abc.abstractmethod
    def do_stage(self, image):
        return image


class MultiFrameStage(abc.ABC):

    def __init__(self, pipeline_context):
        self.pipeline_context = pipeline_context

    @property
    def stage_name(self):
        return '.'.join([__name__, self.__class__.__name__])

    @property
    @abc.abstractmethod
    def group_by_attributes(self):
        return []

    def get_grouping(self, image):
        grouping_criteria = [image.site, image.camera, image.epoch]
        if self.group_by_attributes:
            grouping_criteria += [getattr(image, keyword) for keyword in self.group_by_attributes()]
        return grouping_criteria

    def run(self, images):
        images.sort(key=self.get_grouping)
        processed_images = []
        for _, image_set in itertools.groupby(images, self.get_grouping):
            try:
                image_set = list(image_set)
                logger.info('Running {0}'.format(self.stage_name), image=image_set[0])
                processed_images += self.do_stage(image_set)
            except Exception:
                logger.error(logs.format_exception())
        return processed_images

    @abc.abstractmethod
    def do_stage(self, images):
        return images
