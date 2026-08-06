import os
from types import SimpleNamespace
from unittest.mock import MagicMock

import numpy as np
import pytest
from astropy.io import fits
from astropy.io.fits import Header

import banzai.smartstack_products as products
from banzai.context import Context
from banzai.data import CCDData, HeaderOnly
from banzai.lco import LCOObservationFrame
from banzai.tests.utils import FakeCCDData, FakeContext, FakeInstrument, FakeLCOObservationFrame
from banzai.utils import date_utils
from banzai.utils.file_utils import make_jpg_filenames


# Large enough for the (32, 32) background mesh that apply_smartstack_metadata measures L1 stats on.
FRAME_SHAPE = (64, 64)


class FakeFrameFactory:
    images_by_path = {}

    def open(self, file_info, runtime_context):
        return self.images_by_path.get(file_info['path'])


def stackframe(stack_num, filepath=None, instrument_enqueue_timestamp=None):
    if filepath is None:
        filepath = f'/tmp/cpt1m010-fa16-20240706-{stack_num:04d}-e09.fits'
    if instrument_enqueue_timestamp is None:
        instrument_enqueue_timestamp = stack_num * 1000
    return SimpleNamespace(
        filepath=filepath,
        stack_num=stack_num,
        instrument_enqueue_timestamp=instrument_enqueue_timestamp,
    )


def make_frame(filename, value=1.0, exptime=10.0, date_obs='2024-07-06T00:00:00.000', molfrnum=1,
               frmtotal=3, frame_class=FakeLCOObservationFrame, saturate=65535.0, maxlin=60000.0,
               rdnoise=8.0):
    common_header = {
        'OBSTYPE': 'EXPOSE',
        'DATE-OBS': date_obs,
        'EXPTIME': exptime,
        'PROPID': 'standard',
        'SITEID': 'cpt',
        'INSTRUME': 'fa16',
        'TELID': '1m0a',
        'MOLFRNUM': molfrnum,
        'FRMTOTAL': frmtotal,
        'UTSTOP': date_obs.split('T')[1],
        'SATURATE': saturate,
        'MAXLIN': maxlin,
        'RDNOISE': rdnoise,
    }
    primary_header = Header({
        **common_header,
        'DAY-OBS': '20240706',
        'DATE': date_obs,
        'FILTER': 'rp',
        'OBJECT': 'test target',
    })
    science_header = Header({
        **common_header,
        'EXTNAME': 'SCI',
        'CCDSUM': '1 1',
        'DETSEC': f'[1:{FRAME_SHAPE[1]},1:{FRAME_SHAPE[0]}]',
        'DATASEC': f'[1:{FRAME_SHAPE[1]},1:{FRAME_SHAPE[0]}]',
    })
    science = FakeCCDData(
        data=value * np.ones(FRAME_SHAPE, dtype=np.float32),
        mask=np.zeros(FRAME_SHAPE, dtype=np.uint8),
        uncertainty=np.ones(FRAME_SHAPE, dtype=np.float32),
        meta=science_header,
        name='SCI',
        memmap=False,
    )
    frame = frame_class(
        hdu_list=[HeaderOnly(meta=primary_header, name=''), science],
        file_path=filename,
        hdu_order=['SCI', 'BPM', 'ERR'],
    )
    frame.instrument = FakeInstrument(site='cpt', camera='fa16')
    return frame


def fake_sum_combine(products):
    def combine_images(images, output_image, nsigma, method):
        assert nsigma == products.STACK_NSIGMA_REJECT
        assert method == 'sum'
        output_image.data[:] = sum(image.data for image in images)
        return output_image

    return combine_images


def test_build_stacked_frame_structure(monkeypatch):
    input_images = [
        make_frame('/tmp/cpt1m010-fa16-20240706-0031-e09.fits', value=2.0),
        make_frame('/tmp/cpt1m010-fa16-20240706-0032-e09.fits', value=3.0),
    ]
    stackframes = [stackframe(31, '/tmp/31.fits'), stackframe(32, '/tmp/32.fits')]
    monkeypatch.setattr(products, 'open_stackframe_images', lambda rows, context: input_images)
    monkeypatch.setattr(products, 'combine_images', fake_sum_combine(products))

    output_frame = products.build_stacked_frame(stackframes, FakeContext(), 'mol-1')

    assert isinstance(output_frame.primary_hdu, HeaderOnly)
    assert len(output_frame.ccd_hdus) == 1
    assert np.all(output_frame.ccd_hdus[0].data == 5.0)
    assert np.all(output_frame.ccd_hdus[0].mask == 0)
    assert np.all(output_frame.ccd_hdus[0].uncertainty == 0.0)
    assert output_frame.hdu_order == ['', 'SCI', 'BPM', 'ERR']
    assert output_frame.instrument is input_images[0].instrument
    assert np.all(input_images[0].ccd_hdus[0].data == 2.0)


def test_metadata():
    output_frame = make_frame('/tmp/cpt1m010-fa16-20240706-0031-e45.fits', value=7.0)
    input_images = [
        make_frame('/tmp/cpt1m010-fa16-20240706-0033-e09.fits', exptime=20.0,
                   date_obs='2024-07-06T00:01:00.000'),
        make_frame('/tmp/cpt1m010-fa16-20240706-0031-e09.fits', exptime=10.0,
                   date_obs='2024-07-06T00:00:00.000'),
    ]
    stackframes = [stackframe(33), stackframe(31)]

    products.apply_smartstack_metadata(output_frame, input_images, stackframes, 'mol-1')

    for header in (output_frame.primary_hdu.meta, output_frame.ccd_hdus[0].meta):
        assert header['SATURATE'] == pytest.approx(2.0 * 65535.0)
        assert header['MAXLIN'] == pytest.approx(2.0 * 60000.0)
        assert header['RDNOISE'] == pytest.approx(8.0 * np.sqrt(2.0))
        assert header['UTSTOP'] == '00:01:00.000'
        assert header['L1MEAN'] == pytest.approx(7.0)
        assert header['L1MEDIAN'] == pytest.approx(7.0)
        assert header['L1SIGMA'] == pytest.approx(0.0)
        assert header['EXPTIME'] == 30.0
        assert header.comments['EXPTIME'] == '[s] Total exposure time'
        assert header['DATE-OBS'] == date_utils.date_obs_to_string(input_images[1].dateobs)
        assert header['NCOMBINE'] == 2
        assert header['MOLUID'] == 'mol-1'
        assert 'DATE' in header
        assert 'MOLFRNUM' not in header
        assert header['IMCOM001'] == 'cpt1m010-fa16-20240706-0031-e09.fits'
        assert header['IMCOM002'] == 'cpt1m010-fa16-20240706-0033-e09.fits'
        assert 'Images combined to create smartstack image:' in header['HISTORY']


@pytest.mark.parametrize('file_extension', ['.fits', '.fits.fz'])
def test_build_thumbnail_metadata(file_extension):
    output_frame = make_frame(f'/tmp/cpt1m010-fa16-20240706-0031-e45{file_extension}', exptime=30.0)
    for header in (output_frame.primary_hdu.meta, output_frame['SCI'].meta):
        header['DAY-OBS'] = '20240706'
        header['MOLUID'] = 'mol-1'
        header['NCOMBINE'] = 2
        header['RLEVEL'] = 9
        header['MOLFRNUM'] = 1
        header['UTSTOP'] = '00:00:10.000'
    output_frame['SCI'].meta['SITEID'] = 'ogg'

    metadata = products.build_thumbnail_metadata(output_frame)

    assert metadata == {
        'frame_basename': 'cpt1m010-fa16-20240706-0031-e45',
        'RLEVEL': 45,
        'DATE-OBS': '2024-07-06T00:00:00.000',
        'DAY-OBS': '20240706',
        'INSTRUME': 'fa16',
        'SITEID': 'cpt',
        'TELID': '1m0a',
        'MOLUID': 'mol-1',
        'NCOMBINE': 2,
        'FRMTOTAL': 3,
        'PROPID': 'standard',
        'OBSTYPE': 'EXPOSE',
        'EXPTIME': 30.0,
        'OBJECT': 'test target',
        'FILTER': 'rp',
    }


def test_build_thumbnail_metadata_rejects_missing_required_primary_value():
    output_frame = make_frame('/tmp/cpt1m010-fa16-20240706-0031-e45.fits')
    for header in (output_frame.primary_hdu.meta, output_frame['SCI'].meta):
        header['MOLUID'] = 'mol-1'
        header['NCOMBINE'] = 1
    output_frame.primary_hdu.meta['TELID'] = '  '

    with pytest.raises(ValueError, match='TELID'):
        products.build_thumbnail_metadata(output_frame)


@pytest.mark.parametrize('file_extension', ['.fits', '.fits.fz'])
def test_output_names_stay_fixed_as_stack_grows(monkeypatch, file_extension):
    images_by_stack_num = {
        1: make_frame(f'/tmp/cpt1m010-fa16-20240706-0031-e09{file_extension}', value=1.0),
        2: make_frame(f'/tmp/cpt1m010-fa16-20240706-0033-e09{file_extension}', value=1.0),
    }
    monkeypatch.setattr(
        products,
        'open_stackframe_images',
        lambda rows, context: [images_by_stack_num[row.stack_num] for row in rows],
    )
    monkeypatch.setattr(products, 'combine_images', fake_sum_combine(products))

    preview_frame = products.build_stacked_frame([stackframe(1)], FakeContext(), 'mol-1')
    final_frame = products.build_stacked_frame([stackframe(2), stackframe(1)], FakeContext(), 'mol-1')

    expected_filename = f'cpt1m010-fa16-20240706-0031-e45{file_extension}'
    assert preview_frame.filename == final_frame.filename == expected_filename


def test_build_stacked_frame_rejects_empty():
    with pytest.raises(ValueError):
        products.build_stacked_frame([], FakeContext(), 'mol-1')


@pytest.mark.parametrize('filename', [
    '/tmp/cpt1m010-fa16-20240706-0031-e08.fits',
    '/tmp/cpt1m010-fa16-20240706-0031-e09.txt',
])
def test_build_stacked_frame_rejects_invalid_filename(monkeypatch, filename):
    input_image = make_frame(filename)
    monkeypatch.setattr(products, 'open_stackframe_images', lambda rows, context: [input_image])

    with pytest.raises(ValueError, match='Could not parse smartstack input filename'):
        products.build_stacked_frame([stackframe(1)], FakeContext(), 'mol-1')


@pytest.mark.parametrize('written_suffix', ['', '.fz'], ids=['fits', 'fpack'])
def test_run_final_returns_paths_and_writes(monkeypatch, tmp_path, written_suffix):
    output_frame = make_frame('/tmp/cpt1m010-fa16-20240706-0031-e45.fits', value=1.0)
    context = FakeContext(processed_path=str(tmp_path))
    output_dir = output_frame.get_output_directory(context)
    written_filename = output_frame.filename + written_suffix
    output_frame.write = MagicMock(
        return_value=[SimpleNamespace(filepath=str(output_dir), filename=written_filename)]
    )
    small_name, large_name = make_jpg_filenames(output_frame.filename)
    monkeypatch.setattr(products, 'build_stacked_frame', lambda rows, runtime_context, moluid: output_frame)
    monkeypatch.setattr(products, 'stretch_for_display', lambda data: np.zeros(FRAME_SHAPE, dtype=np.uint8))
    monkeypatch.setattr(products, 'save_jpg', lambda display, path, max_size: open(path, 'wb').close())

    fits_path, small_path, large_path = products.run_final([stackframe(31), stackframe(32)], context, 'mol-1')

    assert fits_path == os.path.join(output_dir, written_filename)
    assert small_path == os.path.join(output_dir, small_name)
    assert large_path == os.path.join(output_dir, large_name)
    assert os.path.exists(small_path)
    assert os.path.exists(large_path)
    output_frame.write.assert_called_once()
    (write_context,) = output_frame.write.call_args.args
    assert write_context.reduction_level == products.SMARTSTACK_REDUCTION_LEVEL
    assert write_context.processed_path == context.processed_path


def test_run_preview_publishes_null_fits(monkeypatch, tmp_path):
    output_frame = make_frame('/tmp/cpt1m010-fa16-20240706-0031-e45.fits', value=1.0)
    for header in (output_frame.primary_hdu.meta, output_frame['SCI'].meta):
        header['MOLUID'] = 'mol-1'
        header['NCOMBINE'] = 3
    output_frame.write = MagicMock(return_value=[])
    publish = MagicMock()
    context = FakeContext(
        processed_path=str(tmp_path),
        broker_url='amqp://broker',
        SHIPPER_EXCHANGE='ship_files',
        SHIPPER_QUEUE_NAME='ship',
    )
    rows = [
        stackframe(31, instrument_enqueue_timestamp=111),
        stackframe(33, instrument_enqueue_timestamp=333),
        stackframe(32, instrument_enqueue_timestamp=222),
    ]
    monkeypatch.setattr(products, 'build_stacked_frame', lambda stackframes, runtime_context, moluid: output_frame)
    monkeypatch.setattr(products, 'render_jpgs', lambda frame, runtime_context: ('/tmp/small.jpg', '/tmp/large.jpg'))
    monkeypatch.setattr(products, 'post_to_shipper_queue', publish)

    products.run_preview(rows, context, 'mol-1')

    output_frame.write.assert_not_called()
    publish.assert_called_once_with(
        context.broker_url,
        context.SHIPPER_EXCHANGE,
        context.SHIPPER_QUEUE_NAME,
        fits_path=None,
        small_thumbnail='/tmp/small.jpg',
        large_thumbnail='/tmp/large.jpg',
        instrument_enqueue_timestamp=333,
        thumbnail_metadata=products.build_thumbnail_metadata(output_frame),
    )


def test_open_stackframe_images_raises_on_unopenable():
    context = FakeContext(FRAME_FACTORY=f'{__name__}.FakeFrameFactory')
    FakeFrameFactory.images_by_path = {'/tmp/good.fits': make_frame('/tmp/good.fits')}

    with pytest.raises(RuntimeError, match='/tmp/missing.fits'):
        products.open_stackframe_images([stackframe(1, '/tmp/good.fits'), stackframe(2, '/tmp/missing.fits')], context)


def test_write_and_reopen_smoke(monkeypatch, tmp_path):
    input_images = [
        make_frame('/tmp/cpt1m010-fa16-20240706-0031-e09.fits', value=2.0, frame_class=LCOObservationFrame),
        make_frame('/tmp/cpt1m010-fa16-20240706-0032-e09.fits', value=3.0, frame_class=LCOObservationFrame),
    ]
    stackframes = [stackframe(31, '/tmp/31.fits'), stackframe(32, '/tmp/32.fits')]
    # Deliberately wrong reduction_level: run_final must stamp RLEVEL 45 regardless of the caller's context.
    context = Context(vars(FakeContext(
        processed_path=str(tmp_path),
        reduction_level=91,
        fpack=False,
        post_to_archive=False,
        no_file_cache=False,
    )))
    monkeypatch.setattr(products, 'open_stackframe_images', lambda rows, runtime_context: input_images)
    monkeypatch.setattr(products, 'stretch_for_display', lambda data: np.zeros(FRAME_SHAPE, dtype=np.uint8))
    monkeypatch.setattr(products, 'save_jpg', lambda display, path, max_size: open(path, 'wb').close())
    monkeypatch.setattr('banzai.lco.dbs.save_processed_image', lambda filename, md5, db_address: None)

    fits_path, _, _ = products.run_final(stackframes, context, 'mol-1')

    with fits.open(fits_path) as hdul:
        assert 'SCI' in hdul
        assert 'BPM' in hdul
        assert 'ERR' in hdul
        assert hdul[0].header['RLEVEL'] == 45
        for header in (hdul[0].header, hdul['SCI'].header):
            assert header['NCOMBINE'] == 2
            assert header['MOLUID'] == 'mol-1'
            assert 'MOLFRNUM' not in header
            assert header['IMCOM001'] == 'cpt1m010-fa16-20240706-0031-e09.fits'
            assert header['IMCOM002'] == 'cpt1m010-fa16-20240706-0032-e09.fits'
            assert 'IMCOM003' not in header
            assert header['SATURATE'] == pytest.approx(2.0 * 65535.0)
            assert header['MAXLIN'] == pytest.approx(2.0 * 60000.0)
            assert header['RDNOISE'] == pytest.approx(8.0 * np.sqrt(2.0))
            assert header['L1MEAN'] == pytest.approx(5.0)
            assert header['L1MEDIAN'] == pytest.approx(5.0)
            assert header['L1SIGMA'] == pytest.approx(0.0)
        assert np.all(hdul['SCI'].data == 5.0)
