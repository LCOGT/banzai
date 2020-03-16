import logging
import abc
import itertools

from banzai import logs
from banzai.frames import ObservationFrame

logger = logging.getLogger('banzai')


class Stage(abc.ABC):

    def __init__(self, runtime_context):
        self.runtime_context = runtime_context

    @property
    def stage_name(self):
        return '.'.join([__name__, self.__class__.__name__])

    @property
    def group_by_attributes(self):
        return []

    def get_grouping(self, image):
        grouping_criteria = [image.instrument.site, image.instrument.id]
        if self.group_by_attributes:
            grouping_criteria += [getattr(image, keyword) for keyword in self.group_by_attributes]
        return grouping_criteria

    def run(self, images):
        if not images:
            return images
        images.sort(key=self.get_grouping)
        processed_images = []
        for _, image_set in itertools.groupby(images, self.get_grouping):
            try:
                image_set = list(image_set)
                logger.info('Running {0}'.format(self.stage_name), image=image_set[0])
                if len(image_set) == 1:
                    image_set = image_set[0]
                processed_images.append(self.do_stage(image_set))
            except Exception:
                logger.error(logs.format_exception())
        return processed_images

    @abc.abstractmethod
    def do_stage(self, images) -> ObservationFrame:
        return images
