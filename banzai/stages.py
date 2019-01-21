import logging
import abc
from banzai import logs
import elasticsearch

from banzai.utils.qc import format_qc_results

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
        File name, site, camera, dayobs and timestamp are always saved in the database.
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
            except Exception:
                error_message = 'Cannot update elasticsearch index to URL \"{url}\": {exception}'
                logger.error(error_message.format(url=self.pipeline_context.elasticsearch_url,
                                                  exception=logs.format_exception()))
        return es_output
