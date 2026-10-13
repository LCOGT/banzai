import hashlib
import json
import os
from contextlib import contextmanager
from io import BytesIO
from types import SimpleNamespace
from unittest.mock import Mock

import numpy as np
import pytest
from astropy.io import fits

from banzai import data, lco, logs
from banzai.calibrations import CalibrationUser
from banzai.stages import Stage
from banzai.tests.utils import FakeCCDData, FakeContext, FakeInstrument, FakeLCOObservationFrame
from banzai.utils import stage_utils


def timing_records(caplog):
    records = [record for record in caplog.records
               if getattr(record, 'tags', {}).get('event') == 'reduction_timing']
    for record in records:
        assert record.levelname == 'INFO'
        assert isinstance(record.tags['duration_s'], float)
        assert record.tags['duration_s'] >= 0
        assert record.tags['process_id'] == os.getpid()
        assert record.tags['processName']
        json.dumps(record.tags)
    return [record.tags for record in records]


@pytest.fixture
def advance(monkeypatch):
    now = [0.0]
    monkeypatch.setattr(logs.time, 'perf_counter', lambda: now[0])

    def tick(seconds):
        now[0] += seconds

    return tick


def make_frame(filename='member-e00.fits', filter_name='rp'):
    return FakeLCOObservationFrame(
        file_path=filename,
        hdu_list=[FakeCCDData(nx=4, ny=3, name='SCI', meta=fits.Header({
            'OBSTYPE': 'EXPOSE', 'RLEVEL': 0, 'REQNUM': 123, 'CONFMODE': 'central30x30',
            'OBSMODE': 'NORMAL', 'MOLUID': 'test-mol', 'MOLFRNUM': 2, 'FRMTOTAL': 3,
            'FILTER': filter_name,
        }))],
    )


def set_pipeline(monkeypatch, factory, stage_class):
    context = FakeContext(FRAME_FACTORY='factory', ORDERED_STAGES=['stage'],
                          LAST_STAGE={'EXPOSE': None}, EXTRA_STAGES={'EXPOSE': None})
    monkeypatch.setattr(stage_utils.import_utils, 'import_attribute',
                        lambda name: {'factory': lambda: factory, 'stage': stage_class}[name])
    return context


@pytest.mark.parametrize('mode, shape, fpack, archive, cache', [
    ('full_frame', (6, 8), False, False, True),
    ('central30x30', (3, 4), True, True, True),
    ('central30x30', (3, 4), False, True, False),
])
def test_real_open_and_write_preserve_fits_and_separate_durations(
        monkeypatch, tmp_path, caplog, advance, mode, shape, fpack, archive, cache):
    header = fits.Header({
        'OBSTYPE': 'EXPOSE', 'EXTNAME': 'SCI', 'DAY-OBS': '20260915', 'INSTRUME': 'sq39',
        'REQNUM': 123, 'CONFMODE': mode, 'OBSMODE': 'NORMAL', 'MOLUID': 'test-mol',
        'MOLFRNUM': 2, 'FRMTOTAL': 3, 'GAIN': 2.0, 'RDNOISE': 5.0, 'SATURATE': 65535.0,
        'MAXLIN': 60000.0, 'CCDSUM': '1 1', 'L1PUBDAT': '2026-09-15T00:00:00', 'RLEVEL': 0,
    })
    pixels = np.arange(np.prod(shape), dtype=np.uint16).reshape(shape)
    raw_path = tmp_path / 'member-e00.fits'
    fits.PrimaryHDU(pixels, header).writeto(raw_path)

    class LastStage(Stage):
        def do_stage(self, image):
            assert isinstance(image.data, np.memmap)
            advance(7)
            return image

    context = set_pipeline(monkeypatch, lco.LCOFrameFactory(), LastStage)
    context.processed_path = str(tmp_path / 'processed')
    context.fpack = fpack
    context.post_to_archive = archive
    context.no_file_cache = not cache
    context.reduction_level = 9
    real_open = lco.fits_utils.open_fits_file

    def open_fits(*args, **kwargs):
        result = real_open(*args, **kwargs)
        advance(2)
        return result

    def instrument(*args):
        advance(3)
        return FakeInstrument(1, 'cpt', 'sq39', type='0m4')

    monkeypatch.setattr(lco.fits_utils, 'open_fits_file', open_fits)
    monkeypatch.setattr(lco.LCOFrameFactory, 'get_instrument_from_header', staticmethod(instrument))
    original_to_fits = lco.LCOObservationFrame.to_fits

    def to_fits(image, runtime_context):
        advance(11)
        return original_to_fits(image, runtime_context)

    monkeypatch.setattr(lco.LCOObservationFrame, 'to_fits', to_fits)

    class CountingBuffer(BytesIO):
        reads = 0

        def read(self, *args):
            self.reads += 1
            return super().read(*args)

    monkeypatch.setattr(data, 'BytesIO', CountingBuffer)
    original_from_fits = data.DataProduct.from_fits
    products = []

    def from_fits(*args):
        advance(13)
        product = original_from_fits(*args)
        product.file_buffer.reads = 0
        products.append(product)
        return product

    monkeypatch.setattr(data.DataProduct, 'from_fits', from_fits)
    writes = []

    @contextmanager
    def cache_open(path, mode):
        with open(path, mode) as file:
            def write(contents):
                advance(17)
                writes.append(path)
                return file.write(contents)
            yield SimpleNamespace(write=write)

    monkeypatch.setattr(lco, 'open', cache_open, raising=False)
    original_md5 = hashlib.md5

    def md5(contents):
        advance(19)
        return original_md5(contents)

    monkeypatch.setattr(lco.hashlib, 'md5', md5)

    def upload(*args, **kwargs):
        advance(23)
        return {'frameid': 456}

    monkeypatch.setattr(lco.file_utils, 'post_to_ingester', upload)
    saved = []

    def save(filename, checksum, db_address):
        advance(31)
        saved.append((filename, checksum))

    monkeypatch.setattr(lco.dbs, 'save_processed_image', save)
    images = stage_utils.run_pipeline_stages([{'path': str(raw_path)}], context)

    assert len(images) == len(products) == 1
    product = products[0]
    contents = product.file_buffer.getvalue()
    assert saved == [(product.filename, original_md5(contents).hexdigest())]
    assert product.file_buffer.reads == (2 if cache else 1)
    assert len(writes) == (1 if cache else 0)
    assert product.frame_id == (456 if archive else None)
    if cache:
        with open(writes[0], 'rb') as file:
            assert file.read() == contents
    with fits.open(BytesIO(contents)) as hdus:
        np.testing.assert_array_equal(hdus['SCI'].data, pixels)
        np.testing.assert_array_equal(hdus['BPM'].data, np.zeros(shape, dtype=np.uint8))
        np.testing.assert_allclose(hdus['ERR'].data, 2.5)
        assert hdus['SCI'].header['RLEVEL'] == 9
        assert hdus['SCI'].header['MOLUID'] == 'test-mol'

    records = timing_records(caplog)
    operations = {record['operation']: record for record in records}
    expected = {'input_open': 5, 'stage': 7, 'output_metadata': 0, 'output_preparation': 11,
                'fits_serialization': 13, 'checksum': 19, 'processed_image_record': 31}
    if archive:
        expected['archive_upload'] = 23
    if cache:
        expected['file_cache_write'] = 17
    expected['output_write'] = sum(expected.values()) - 12
    expected['reduction_total'] = expected['output_write'] + 12
    assert set(operations) == set(expected)
    for operation, duration in expected.items():
        assert operations[operation]['duration_s'] == duration
    for record in records:
        assert record['input_filename'] == raw_path.name
        assert record['request_num'] == 123
        assert record['camera'] == 'sq39'
        assert record['image_shapes'] == [list(shape)]
        assert record['configuration_mode'] == mode
        assert record['observing_mode'] == 'NORMAL'
        assert record['smartstack_moluid'] == 'test-mol'
        assert record['smartstack_stack_num'] == 2
        assert record['smartstack_frmtotal'] == 3
    for operation in ('output_preparation', 'fits_serialization', 'checksum', 'processed_image_record'):
        assert operations[operation]['output_filename'] == product.filename
    assert operations['reduction_total']['scope'] == 'frame'
    assert operations['reduction_total']['outcome'] == 'outputs_written'
    assert operations['reduction_total']['reduction_level'] == 0
    assert list(operations).index('stage') < list(operations).index('output_metadata')
    assert any(record.message == 'Running banzai.stages.LastStage' for record in caplog.records)


@pytest.mark.parametrize('failure, total_outcome', [
    ('open_error', 'error'), ('open_rejected', 'rejected'),
    ('stage_error', 'stopped'), ('stage_rejected', 'stopped'), ('write_error', 'error'),
])
def test_pipeline_unsuccessful_behavior(monkeypatch, caplog, advance, failure, total_outcome):
    image = make_frame()
    error = RuntimeError('original failure')

    def open_frame(*args):
        advance(2)
        if failure == 'open_error':
            raise error
        return None if failure == 'open_rejected' else image

    class TestStage(Stage):
        def do_stage(self, image):
            advance(3)
            if failure == 'stage_error':
                raise error
            return None if failure == 'stage_rejected' else image

    image.write = Mock(side_effect=error)
    context = set_pipeline(monkeypatch, SimpleNamespace(open=open_frame), TestStage)
    if failure in ('open_error', 'write_error'):
        with pytest.raises(RuntimeError) as raised:
            stage_utils.run_pipeline_stages([{'path': image.filename}], context)
        assert raised.value is error
    else:
        assert stage_utils.run_pipeline_stages([{'path': image.filename}], context) is None
    assert image.write.call_count == (1 if failure == 'write_error' else 0)
    records = timing_records(caplog)
    failed_operation = {'open': 'input_open', 'stage': 'stage', 'write': 'output_write'}[failure.split('_')[0]]
    failed = next(record for record in records if record['operation'] == failed_operation)
    assert failed['outcome'] == ('rejected' if failure.endswith('rejected') else 'error')
    assert records[-1]['operation'] == 'reduction_total'
    assert records[-1]['outcome'] == total_outcome
    assert records[-1]['input_filename'] == image.filename
    if failure == 'stage_error':
        assert any(record.message == 'Reduction stopped' for record in caplog.records)


@pytest.mark.parametrize('group_outcome', ['success', 'rejected', 'error'])
def test_batch_filtering_and_grouping_are_preserved(monkeypatch, caplog, advance, group_outcome):
    images = [make_frame('r.fits', 'rp'), make_frame('g1.fits', 'gp'), make_frame('g2.fits', 'gp')]
    for image in images:
        image.write = Mock()
    opened = iter([*images, None])
    groups = []

    class GroupStage(Stage):
        group_by_attributes = ['filter']

        def do_stage(self, group):
            advance(2)
            groups.append(group)
            if group[0].filter == 'gp':
                if group_outcome == 'error':
                    raise RuntimeError('group failed')
                if group_outcome == 'rejected':
                    return None
            return group[0]

    context = set_pipeline(monkeypatch, SimpleNamespace(open=lambda *args: next(opened)), GroupStage)
    context.CALIBRATION_STACKER_STAGES = {'EXPOSE': ['stage']}
    result = stage_utils.run_pipeline_stages(
        [{'path': image.filename} for image in images] + [{'path': 'missing.fits'}], context,
        calibration_maker=True)
    assert groups == [[images[1], images[2]], [images[0]]]
    assert result == ([images[1], images[0]] if group_outcome == 'success' else [images[0]])
    assert sum(image.write.call_count for image in images) == len(result)
    records = timing_records(caplog)
    grouped = [record for record in records if record['operation'] == 'stage']
    assert [record['scope'] for record in grouped] == ['group', 'group']
    assert [record['input_count'] for record in grouped] == [2, 1]
    assert grouped[0]['input_filenames'] == ['g1.fits', 'g2.fits']
    assert grouped[0]['outcome'] == group_outcome
    total = records[-1]
    assert total['scope'] == 'batch'
    assert total['input_count'] == 4
    assert total['opened_count'] == 3
    assert total['output_count'] == len(result)
    assert total['outcome'] == 'outputs_written'
    assert 'input_filename' not in total


@pytest.mark.parametrize('failure', [None, 'selection', 'missing', 'override_missing',
                                      'open', 'open_rejected', 'apply', 'apply_rejected'])
def test_calibration_suboperations_and_failures(monkeypatch, caplog, advance, failure):
    image, master = make_frame(), make_frame('master.fits')
    error = RuntimeError('calibration failure')

    class TestCalibration(CalibrationUser):
        calibration_type = 'BIAS'

        def get_calibration_file_info(self, image):
            advance(2)
            if failure == 'selection':
                raise error
            if failure in ('missing', 'override_missing'):
                return None
            # Preserve the current frameid=None handling; that fix belongs to another branch.
            return {'filename': master.filename, 'frameid': None}

        def apply_master_calibration(self, image, master_image):
            assert master_image is master and master.is_master
            advance(5)
            if failure == 'apply':
                raise error
            return None if failure == 'apply_rejected' else image

    def open_master(*args):
        advance(3)
        if failure == 'open':
            raise error
        return None if failure == 'open_rejected' else master

    monkeypatch.setattr(lco.LCOFrameFactory, 'open', open_master)
    update_frameid = Mock()
    monkeypatch.setattr(lco.dbs, 'update_calibration_frameid', update_frameid)
    calibration = TestCalibration(FakeContext(override_missing=failure == 'override_missing'))
    if failure in ('selection', 'open', 'apply'):
        with pytest.raises(RuntimeError) as raised:
            calibration.do_stage(image)
        assert raised.value is error
    elif failure == 'open_rejected':
        with pytest.raises(AttributeError):
            calibration.do_stage(image)
    else:
        expected = None if failure in ('missing', 'apply_rejected') else image
        assert calibration.do_stage(image) is expected
    update_frameid.assert_not_called()
    records = timing_records(caplog)
    assert records[0]['operation'] == 'calibration_selection'
    assert records[0]['duration_s'] == 2
    if len(records) > 1:
        assert records[1]['operation'] == 'calibration_open'
        assert records[1]['duration_s'] == 3
    if len(records) > 2:
        assert records[2]['operation'] == 'calibration_apply'
        assert records[2]['duration_s'] == 5
    for record in records:
        assert record['input_filename'] == image.filename
        if record['operation'] != 'calibration_selection' or failure is None:
            assert record['calibration_filename'] == master.filename
    expected_outcome = ('missing' if failure in ('missing', 'override_missing') else
                        'rejected' if failure in ('open_rejected', 'apply_rejected') else
                        'error' if failure else 'success')
    assert records[-1]['outcome'] == expected_outcome


@pytest.mark.parametrize('failure', ['output_preparation', 'fits_serialization', 'archive_upload',
                                      'archive_response', 'file_cache_write', 'checksum', 'processed_image_record'])
def test_output_failures_propagate_without_success_records(monkeypatch, caplog, tmp_path, failure):
    image = make_frame()
    image.save_processing_metadata = Mock()
    context = FakeContext(fpack=False, processed_path=str(tmp_path),
                          post_to_archive=failure.startswith('archive'), no_file_cache=False)
    error = OSError('output failure')
    fail = Mock(side_effect=error)
    monkeypatch.setattr(lco.dbs, 'save_processed_image', Mock())
    if failure == 'output_preparation':
        image.to_fits = fail
    elif failure == 'fits_serialization':
        monkeypatch.setattr(data.DataProduct, 'from_fits', fail)
    elif failure.startswith('archive'):
        monkeypatch.setattr(lco.file_utils, 'post_to_ingester',
                            Mock(return_value={}) if failure == 'archive_response' else fail)
    elif failure == 'file_cache_write':
        monkeypatch.setattr(lco, 'open', fail, raising=False)
    elif failure == 'checksum':
        monkeypatch.setattr(lco.hashlib, 'md5', fail)
    else:
        monkeypatch.setattr(lco.dbs, 'save_processed_image', fail)
    with pytest.raises(RuntimeError if failure == 'archive_response' else OSError) as raised:
        image.write(context)
    if failure == 'archive_response':
        assert str(raised.value) == 'Archive ingester response did not contain a frameid, cannot continue'
    else:
        assert raised.value is error
    records = timing_records(caplog)
    assert records[-1]['operation'] == ('archive_upload' if failure == 'archive_response' else failure)
    assert records[-1]['outcome'] == 'error'
    assert all(record['outcome'] == 'success' for record in records[:-1])


def test_timing_metadata_cannot_stop_an_operation(caplog, advance):
    class BadMetadata:
        @property
        def filename(self):
            raise ValueError('bad metadata')

    with logs.time_operation('test', image=BadMetadata()):
        advance(0.25)
    record, = timing_records(caplog)
    assert record['timing_metadata_error'] is True
    assert record['outcome'] == 'success'
    assert record['duration_s'] == 0.25
