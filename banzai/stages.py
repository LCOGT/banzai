import logging
import abc

import elasticsearch

from banzai.utils.qc import format_qc_results

logger = logging.getLogger(__name__)


class StageBase(abc.ABC):

    def __init__(self, pipeline_context):
        self.pipeline_context = pipeline_context

    @property
    def stage_name(self):
        return '.'.join([__name__, self.__class__.__name__])

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


class Stage(StageBase):

    def __init__(self, pipeline_context):
        super(Stage, self).__init__(pipeline_context)

    def run(self, image):
        if image is None:
            return image
        logger.info('Running {0}'.format(self.stage_name), image=image)
        try:
            image = self.do_stage(image)
        except Exception as e:
            logger.error(e)
        return image

    @abc.abstractmethod
    def do_stage(self, image):
        return image
