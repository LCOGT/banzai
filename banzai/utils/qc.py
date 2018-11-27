import numpy as np


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
