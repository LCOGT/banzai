import mock
import numpy as np

from banzai.utils import qc
from banzai.tests.utils import FakeImage, FakeContext, FakeStage


def test_format_qc_results_basic_info():
    image = FakeImage()
    filename, results = qc.format_qc_results({}, image)
    assert results['site'] == image.site
    assert results['instrument'] == image.camera
    assert results['dayobs'] == image.epoch
    assert results['@timestamp'] == image.dateobs
    assert results['obstype'] == image.obstype
    assert filename in image.filename


def test_format_qc_results_new_info():
    filename, results = qc.format_qc_results({"key1": "value1",
                                              "key2": "value2"},
                                             FakeImage())
    assert results["key1"] == "value1"
    assert results["key2"] == "value2"


def test_format_qc_results_numpy_bool():
    filename, results = qc.format_qc_results({"normal_bool": True,
                                              "numpy_bool": np.bool_(True)},
                                             FakeImage())
    assert type(results["normal_bool"]) == bool
    assert type(results["numpy_bool"]) == bool


def test_save_qc_results_no_post_to_elasticsearch_attribute():
    stage = FakeStage(FakeContext())
    assert qc.save_qc_results(stage.pipeline_context, {}, FakeImage()) == {}


@mock.patch('banzai.utils.qc.elasticsearch.Elasticsearch')
def test_save_qc_results(mock_es):
    context = FakeContext()
    context.post_to_elasticsearch = True
    context.elasticsearch_url = '/'
    stage = FakeStage(context)
    qc.save_qc_results(stage.pipeline_context, {}, FakeImage())
    assert mock_es.called
