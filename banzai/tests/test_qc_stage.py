import elasticsearch
from datetime import timedelta

from banzai.tests.utils import FakeImage, FakeContext
from banzai.qc.qc_stage import QCStage
from banzai.qc import SaturationTest, ThousandsTest, PointingTest, PatternNoiseDetector, HeaderSanity


class FakeElasticsearchContext(FakeContext):
    def __init__(self, *args, **kwargs):
        super(FakeContext, self).__init__(*args, **kwargs)
        self.post_to_elasticsearch = True


def test_es_connect():
    es = elasticsearch.Elasticsearch(QCStage.ES_URLS)
    assert es.ping()
    assert es.indices.exists(index=QCStage.ES_INDEX)


def test_qc_stage_class():
    qc_stage = QCStage(FakeElasticsearchContext())
    assert qc_stage.pipeline_context.post_to_elasticsearch


def test_save_results():
    qc_stage = QCStage(FakeElasticsearchContext())
    image = FakeImage()
    output = qc_stage.save_qc_results({}, image, _source=True)
    assert '_shards' in output
    assert output['_shards']['failed'] == 0
    assert 'get' in output


def test_save_results_with_changed_parameters():
    # Push the defualt test image info
    qc_stage = QCStage(FakeElasticsearchContext())
    image = FakeImage()
    qc_stage.save_qc_results({}, image)
    # Change the image parameters and push again
    image.site = "fake_site"
    image.instrument = "fake_instrument"
    image.epoch = str(int(image.epoch) + 1)
    image.dateobs += timedelta(days=1)
    # Post to ES and test results
    output = qc_stage.save_qc_results({}, image, _source=True)
    assert output['result'] == 'updated'
    results = output['get']['_source']
    assert results['site'] == image.site
    assert results['instrument'] == image.instrument
    assert results['dayobs'] == image.epoch
    assert results['timestamp'] == image.dateobs.strftime("%Y-%m-%dT%H:%M:%S")


def test_saturation_es_update():
    # Set initial values
    qc_stage = QCStage(FakeElasticsearchContext())
    image = FakeImage()
    qc_stage.save_qc_results({'Saturated': True, 'saturation_fraction': 0.99}, image)
    # Run saturation test
    tester = SaturationTest(FakeElasticsearchContext())
    image.header['SATURATE'] = 65535
    image.data += 5.0
    tester.do_stage([image])
    # Check info from elasticsearch
    results = elasticsearch.Elasticsearch(QCStage.ES_URLS).get_source(
        index=QCStage.ES_INDEX, doc_type=QCStage.ES_DOC_TYPE, id='test')
    assert not results['Saturated']
    assert results['saturation_fraction'] == 0.0


def test_sinistro_1000s_es_update():
    # Set initial values
    qc_stage = QCStage(FakeElasticsearchContext())
    image = FakeImage()
    qc_stage.save_qc_results({'Error1000s': True, 'fraction_1000s': 0.99}, image)
    # Run sinistro 1000s test
    tester = ThousandsTest(FakeElasticsearchContext())
    tester.do_stage([image])
    # Check info from elasticsearch
    results = elasticsearch.Elasticsearch(QCStage.ES_URLS).get_source(
        index=QCStage.ES_INDEX, doc_type=QCStage.ES_DOC_TYPE, id='test')
    assert not results['Error1000s']
    assert results['fraction_1000s'] == 0.0


def test_pointing_es_update():
    # Set initial values
    qc_stage = QCStage(FakeElasticsearchContext())
    image = FakeImage()
    qc_stage.save_qc_results({'PointingSevere': True,
                              'PointingWarning': True,
                              'pointing_offset': 100.}, image)
    # Run pointing test
    image.header['CRVAL1'] = '1.0'
    image.header['CRVAL2'] = '-1.0'
    image.header['OFST-RA'] = '0:04:00.00'
    image.header['OFST-DEC'] = '-01:00:00.000'
    tester = PointingTest(FakeElasticsearchContext())
    tester.do_stage([image])
    # Check info from elasticsearch
    results = elasticsearch.Elasticsearch(QCStage.ES_URLS).get_source(
        index=QCStage.ES_INDEX, doc_type=QCStage.ES_DOC_TYPE, id='test')
    assert not results['PointingSevere']
    assert not results['PointingWarning']
    assert results['pointing_offset'] < 1E10


def test_pattern_noise_es_update():
    # Set initial values
    qc_stage = QCStage(FakeElasticsearchContext())
    image = FakeImage()
    qc_stage.save_qc_results({'PatternNoise': True}, image)
    # Run pattern noise test
    tester = PatternNoiseDetector(FakeElasticsearchContext())
    tester.do_stage([image])
    # Check info from elasticsearch
    results = elasticsearch.Elasticsearch(QCStage.ES_URLS).get_source(
        index=QCStage.ES_INDEX, doc_type=QCStage.ES_DOC_TYPE, id='test')
    assert not results['PatternNoise']


def test_header_checker_es_update():
    # Set initial values
    qc_stage = QCStage(FakeElasticsearchContext())
    image = FakeImage()
    header_check_booleans = ['HeaderBadDecValue', 'HeaderBadRAValue',
                             'HeaderKeywordsMissing', 'HeaderKeywordsNA']

    qc_stage.save_qc_results({'PatternNoise': True}, image)
    # Run header sanity test
    tester = HeaderSanity(FakeElasticsearchContext())
    for key in tester.header_expected_format.keys():
        image.header[key] = 1.0
    image.header['EXPTIME'] = 0
    tester.do_stage([image])
    # Check info from elasticsearch
    results = elasticsearch.Elasticsearch(QCStage.ES_URLS).get_source(
        index=QCStage.ES_INDEX, doc_type=QCStage.ES_DOC_TYPE, id='test')
    #assert not results['PatternNoise']
