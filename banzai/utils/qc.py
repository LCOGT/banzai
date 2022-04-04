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
        # OpenSearch does not like numpy.bool_ types
        if type(value) == np.bool_:
            value = bool(value)
        results_to_save[key] = value
    filename = image.filename.replace('.fits', '').replace('.fz', '')
    return filename, results_to_save


def save_qc_results(runtime_context, qc_results, image, **kwargs):
    """
    Save the Quality Control results to OpenSearch

    Parameters
    ----------
    runtime_context: object
                      Context instance with runtime values
    qc_results : dict
                 Dictionary of key value pairs to be saved to OpenSearch
    image : banzai.frames.ObservationFrame
            Image that should be linked

    Notes
    -----
    File name, site, camera, dayobs and timestamp are always saved in the database.
    """

    os_output = {}
    if getattr(runtime_context, 'post_to_opensearch', False):
        filename, results_to_save = format_qc_results(qc_results, image)
        os = OpenSearch(runtime_context.opensearch_url, timeout=60)
        try:
            os_output = os.update(index=runtime_context.opensearch_qc_index,
                                  id=filename, body={'doc': results_to_save, 'doc_as_upsert': True},
                                  retry_on_conflict=5, **kwargs)
        except Exception:
            error_message = 'Cannot update opensearch index to URL \"{url}\": {exception}'
            logger.error(error_message.format(url=runtime_context.opensearch_url,
                                              exception=logs.format_exception()))
    return os_output
