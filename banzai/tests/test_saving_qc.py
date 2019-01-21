import mock
import numpy as np

from banzai.tests.utils import FakeImage, FakeContext, FakeStage
from banzai.utils.qc import format_qc_results


def test_format_qc_results_basic_info():
    image = FakeImage()
    filename, results = format_qc_results({}, image)
    assert results['site'] == image.site
    assert results['instrument'] == image.camera
    assert results['dayobs'] == image.epoch
    assert results['@timestamp'] == image.dateobs
    assert results['obstype'] == image.obstype
    assert filename in image.filename


def test_format_qc_results_new_info():
    filename, results = format_qc_results({"key1": "value1",
                                           "key2": "value2"},
                                            FakeImage())
    assert results["key1"] == "value1"
    assert results["key2"] == "value2"


def test_format_qc_results_numpy_bool():
    filename, results = format_qc_results({"normal_bool": True,
                                           "numpy_bool": np.bool_(True)},
                                           FakeImage())
    assert type(results["normal_bool"]) == bool
    assert type(results["numpy_bool"]) == bool


def test_save_qc_results_no_post_to_elasticsearch_attribute():
    stage = FakeStage(FakeContext())
    assert stage.save_qc_results({}, FakeImage()) == {}


@mock.patch('banzai.stages.elasticsearch.Elasticsearch')
def test_save_qc_results(mock_es):
    context = FakeContext()
    context.post_to_elasticsearch = True
    context.elasticsearch_url = '/'
    stage = FakeStage(context)
    stage.save_qc_results({}, FakeImage())
    assert mock_es.called
