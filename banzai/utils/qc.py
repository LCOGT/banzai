import logging

import numpy as np
from opensearchpy import OpenSearch

from banzai import logs

logger = logging.getLogger('banzai')


def format_qc_results(qc_results, image):
    results_to_save = {'site': image.site,
                       'instrument': image.camera,
                       'dayobs': image.epoch,
                       'request_number': image.request_number,
                       'obstype': image.obstype,
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
    image : banzai.frames.ObservationFrame
            Image that should be linked

    Notes
    -----
    File name, site, camera, dayobs and timestamp are always saved in the database.
    """

    os_output = {}
    if getattr(runtime_context, 'post_to_elasticsearch', False):
        filename, results_to_save = format_qc_results(qc_results, image)
        os = OpenSearch(runtime_context.elasticsearch_url, read_timeout='1m')
        try:
            os_output = os.update(index=runtime_context.elasticsearch_qc_index,
                                  id=filename, body={'doc': results_to_save, 'doc_as_upsert': True},
                                  retry_on_conflict=5, timeout='30s', **kwargs)
        except Exception:
            error_message = 'Cannot update elasticsearch index to URL \"{url}\": {exception}'
            logger.error(error_message.format(url=runtime_context.elasticsearch_url,
                                              exception=logs.format_exception()))
    return os_output
