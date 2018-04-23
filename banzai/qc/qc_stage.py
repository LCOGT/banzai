from banzai.stages import Stage
import elasticsearch


ES_URLS = ['http://es1.lco.gtn:9200',
           'http://es2.lco.gtn:9200',
           'http://es3.lco.gtn:9200',
           'http://es4.lco.gtn:9200']

ES_URLS = ["flkjf"]
class QCStage(Stage):

    def __init__(self, pipeline_context):
        super(QCStage, self).__init__(pipeline_context)

    @property
    def group_by_keywords(self):
        return None

    def do_stage(self, images):
        pass

    def save_qc_results(self, qc_results, image, es_url=ES_URLS):
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
        File name, site, instrument, and dayobs are always saved in the database.

        """
        post_to_elasticsearch = getattr(self, 'pipeline_context.post_to_elasticsearch', False)
        if post_to_elasticsearch:
            results_to_save = {'site': image.site,
                               'instrument': image.instrument,
                               'dayobs': image.epoch,
                               'timestamp': image.dateobs}
            for key, value in qc_results.items():
                results_to_save[key] = value
            es = elasticsearch.Elasticsearch(es_url)
            filename = image.filename.replace('.fits', '').replace('.fz', '')
            try:
                es.update(index='banzai_qc', doc_type='qc', id=filename,
                          body={'doc': results_to_save, 'doc_as_upsert': True})
            except ConnectionError:
                self.logger.error('Cannot connect to elasticsearch DB')
            except Exception as e:
                self.logger.error('Cannot update elasticsearch index: {0}'.format(e))
