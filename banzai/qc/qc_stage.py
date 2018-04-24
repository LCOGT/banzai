from banzai.stages import Stage
import elasticsearch


class QCStage(Stage):

    ES_URLS = ['http://es1.lco.gtn:9200',
               'http://es2.lco.gtn:9200',
               'http://es3.lco.gtn:9200',
               'http://es4.lco.gtn:9200']
    ES_INDEX = "banzai_qc"
    ES_DOC_TYPE = "qc"

    def __init__(self, pipeline_context):
        super(QCStage, self).__init__(pipeline_context)

    @property
    def group_by_keywords(self):
        return None

    def do_stage(self, images):
        pass

    def save_qc_results(self, qc_results, image, es_url=ES_URLS, **kwargs):
        """
        Save the Quality Control results to ElasticSearch

        Parameters
        ----------
        qc_results : dict
                     Dictionary of key value pairs to be saved to ElasticSearch
        image : banzai.images.Image
                Image that should be linked
        es_url : str
                 URL of ElasticSearch database

        Notes
        -----
        File name, site, instrument, dayobs and timestamp are always saved in the database.
        """
        es_output = {}
        if getattr(self.pipeline_context, 'post_to_elasticsearch', False):
            filename, results_to_save = self._format_qc_results(qc_results, image)
            es = elasticsearch.Elasticsearch(es_url)
            try:
                es_output = self._push_to_elasticsearch(es, filename, results_to_save, **kwargs)
            except ConnectionError:
                self.logger.error('Cannot connect to elasticsearch DB')
            except Exception as e:
                self.logger.error('Cannot update elasticsearch index: {0}'.format(e))
        return es_output

    @staticmethod
    def _format_qc_results(qc_results, image):
        results_to_save = {'site': image.site,
                           'instrument': image.instrument,
                           'dayobs': image.epoch,
                           'timestamp': image.dateobs}
        for key, value in qc_results.items():
            results_to_save[key] = value
        filename = image.filename.replace('.fits', '').replace('.fz', '')
        return filename, results_to_save

    def _push_to_elasticsearch(self, es, filename, results_to_save, **kwargs):
        return es.update(index=self.ES_INDEX, doc_type=self.ES_DOC_TYPE, id=filename,
                         body={'doc': results_to_save, 'doc_as_upsert': True}, **kwargs)
