import mock
import numpy as np
import pytest

from astropy.io.fits import Header

from banzai.utils import qc
from banzai.tests.utils import FakeLCOObservationFrame, FakeCCDData, FakeContext, FakeStage

pytestmark = pytest.mark.saving_qc

test_header = Header({'DATE-OBS': '2020-01-10T13:00:00.000',
                      'INSTRUME': 'fa16',
                      'DAY-OBS': '20200110',
                      'OBSTYPE': 'EXPOSE',
                      'SITEID': 'CPT'})


def test_format_qc_results_basic_info():
    image = FakeLCOObservationFrame([FakeCCDData(meta=test_header)])
    filename, results = qc.format_qc_results({}, image)
    assert results['site'] == image.site
    assert results['instrument'] == image.camera
    assert results['dayobs'] == image.epoch
    assert results['@timestamp'] == image.dateobs
    assert results['obstype'] == image.obstype
    assert filename in image.filename


def test_format_qc_results_new_info():
    image = FakeLCOObservationFrame([FakeCCDData(meta=test_header)])
    filename, results = qc.format_qc_results({"key1": "value1",
                                              "key2": "value2"},
                                             image)
    assert results["key1"] == "value1"
    assert results["key2"] == "value2"


def test_format_qc_results_numpy_bool():
    image = FakeLCOObservationFrame([FakeCCDData(meta=test_header)])
    filename, results = qc.format_qc_results({"normal_bool": True,
                                              "numpy_bool": np.bool_(True)},
                                             image)
    assert type(results["normal_bool"]) == bool
    assert type(results["numpy_bool"]) == bool


def test_save_qc_results_no_post_to_elasticsearch_attribute():
    stage = FakeStage(FakeContext())
    image = FakeLCOObservationFrame([FakeCCDData(meta=test_header)])
    assert qc.save_qc_results(stage.runtime_context, {}, image) == {}


@mock.patch('banzai.utils.qc.elasticsearch.Elasticsearch')
def test_save_qc_results(mock_es):
    context = FakeContext()
    image = FakeLCOObservationFrame([FakeCCDData(meta=test_header)])
    context.post_to_elasticsearch = True
    context.elasticsearch_url = '/'
    stage = FakeStage(context)
    qc.save_qc_results(stage.runtime_context, {}, image)
    assert mock_es.called
