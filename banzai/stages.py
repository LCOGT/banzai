import itertools
import logging
import abc

import elasticsearch

from banzai.utils.qc import format_qc_results

logger = logging.getLogger(__name__)


class Stage(abc.ABC):

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
        grouping_criteria = [image.site, image.instrument, image.epoch]
        if self.group_by_attributes:
            grouping_criteria += [getattr(image, keyword) for keyword in self.group_by_attributes]
        return grouping_criteria

    def run_stage(self, image_set):
        image_set = list(image_set)
        logger.info('Running {0}'.format(self.stage_name), image=image_set[0])
        return self.do_stage(image_set)

    def run(self, images):
        images.sort(key=self.get_grouping)
        processed_images = []
        for _, image_set in itertools.groupby(images, self.get_grouping):
            try:
                processed_images += self.run_stage(image_set)
            except Exception as e:
                logger.error(e)
        return processed_images

    @abc.abstractmethod
    def do_stage(self, images):
        return images

    def save_qc_results(self, qc_results, image, **kwargs):
        """
        Save the Quality Control results to ElasticSearch

        Parameters
        ----------
        qc_results : dict
                     Dictionary of key value pairs to be saved to ElasticSearch
        image : banzai.images.Image
                Image that should be linked

        Notes
        -----
        File name, site, instrument, dayobs and timestamp are always saved in the database.
        """

        es_output = {}
        if getattr(self.pipeline_context, 'post_to_elasticsearch', False):
            filename, results_to_save = format_qc_results(qc_results, image)
            es = elasticsearch.Elasticsearch(self.pipeline_context.elasticsearch_url)
            try:
                es_output = es.update(index=self.pipeline_context.elasticsearch_qc_index,
                                      doc_type=self.pipeline_context.elasticsearch_doc_type,
                                      id=filename, body={'doc': results_to_save, 'doc_as_upsert': True},
                                      retry_on_conflict=5, timestamp=results_to_save['@timestamp'], **kwargs)
            except Exception as e:
                error_message = 'Cannot update elasticsearch index to URL \"{url}\": {exception}'
                logger.error(error_message.format(url=self.pipeline_context.elasticsearch_url, exception=e))
        return es_output
