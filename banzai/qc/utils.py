import elasticsearch

def save_qc_results(qc_results, image, es_url='http://elasticsearch.lco.gtn:9200'):
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
    results_to_save = {'site': image.site, 'instrument': image.instrument, 'dayobs': image.epoch}
    for key, value in qc_results.items():
        results_to_save[key] = value
    es = elasticsearch.Elasticsearch(es_url)
    filename = image.filename.replace('.fits', '').replace('.fz', '')
    es.update(index='banzai_qc', doc_type='qc', id=filename, body={'doc': results_to_save, 'doc_as_upsert': True})
