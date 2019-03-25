import logging

import numpy as np
import elasticsearch

from banzai import logs

logger = logging.getLogger(__name__)


def format_qc_results(qc_results, image):
    results_to_save = {'site': image.site,
                       'instrument': image.camera,
                       'dayobs': image.epoch,
                       'request_number': image.request_number,
                       'block_id': image.block_id,
                       'molecule_id': image.molecule_id,
                       'obstype': image.obstype,
                       'filter': image.filter,
                       '@timestamp': image.dateobs}
    for key, value in qc_results.items():
        # Elasticsearch does not like numpy.bool_ types
        if type(value) == np.bool_:
            value = bool(value)
        results_to_save[key] = value
    filename = image.filename.replace('.fits', '').replace('.fz', '')
    return filename, results_to_save


def save_qc_results(runtime_context, qc_results, image, **kwargs):
    """
    Save the Quality Control results to ElasticSearch

    Parameters
    ----------
    runtime_context: object
                      Context instance with runtime values
    qc_results : dict
                 Dictionary of key value pairs to be saved to ElasticSearch
    image : banzai.images.Image
            Image that should be linked

    Notes
    -----
    File name, site, camera, dayobs and timestamp are always saved in the database.
    """

    es_output = {}
    if getattr(runtime_context, 'post_to_elasticsearch', False):
        filename, results_to_save = format_qc_results(qc_results, image)
        es = elasticsearch.Elasticsearch(runtime_context.elasticsearch_url)
        try:
            es_output = es.update(index=runtime_context.elasticsearch_qc_index,
                                  doc_type=runtime_context.elasticsearch_doc_type,
                                  id=filename, body={'doc': results_to_save, 'doc_as_upsert': True},
                                  retry_on_conflict=5, timestamp=results_to_save['@timestamp'], **kwargs)
        except Exception:
            error_message = 'Cannot update elasticsearch index to URL \"{url}\": {exception}'
            logger.error(error_message.format(url=runtime_context.elasticsearch_url,
                                              exception=logs.format_exception()))
    return es_output
