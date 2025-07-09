import abc
import itertools
from collections.abc import Iterable

from banzai import logs
from banzai.frames import ObservationFrame
from banzai.utils.metrics import add_span_attribute, add_span_event, create_manual_span

logger = logs.get_logger()


class Stage(abc.ABC):

    def __init__(self, runtime_context):
        self.runtime_context = runtime_context

    @property
    def stage_name(self):
        return '.'.join([__name__, self.__class__.__name__])

    @property
    def group_by_attributes(self):
        return []

    @property
    def process_by_group(self):
        return False

    def get_grouping(self, image):
        grouping_criteria = [image.instrument.site, image.instrument.id]
        if self.group_by_attributes:
            grouping_criteria += [getattr(image, keyword) for keyword in self.group_by_attributes]
        return grouping_criteria

    def run(self, images):
        if self.runtime_context:
            traced_context = self.runtime_context.__dict__
        else:
            traced_context = {}
        with create_manual_span(self.stage_name, {"context": traced_context}):
            if not images:
                return images
            if self.group_by_attributes or self.process_by_group:
                images.sort(key=self.get_grouping)
                image_sets = [list(image_set) for _, image_set in itertools.groupby(images, self.get_grouping)]
            else:
                # Treat each image individually
                image_sets = images
            add_span_attribute("image_set_length", len(image_sets))
            processed_images = []
            for image_set in image_sets:
                try:
                    if isinstance(image_set, Iterable):
                        image = image_set[0]
                    else:
                        image = image_set
                    logger.info('Running {0}'.format(self.stage_name), image=image)
                    add_span_event(f"stage started for image {image}")
                    processed_image = self.do_stage(image_set)
                    if processed_image is not None:
                        processed_images.append(processed_image)
                except Exception:
                    logger.error(logs.format_exception())
                    if isinstance(image_set, Iterable):
                        for image in image_set:
                            logger.error('Reduction stopped', image=image)
                    else:
                        logger.error('Reduction stopped', image=image)
                    add_span_event("reduction stopped")

            add_span_attribute("processed_image_set_length", len(processed_images))
            return processed_images

    @abc.abstractmethod
    def do_stage(self, images) -> ObservationFrame:
        return images
