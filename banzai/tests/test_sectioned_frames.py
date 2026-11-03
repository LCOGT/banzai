"""Stack inputs opened in sections: one descriptor per frame, no temp memmaps, bit-identical results.

Every test here opens real FITS files written to tmp_path through the real LCOFrameFactory, so the
sectioned path is exercised end to end rather than through fakes. The instrument lookup is the only thing
patched, because it needs a database.
"""
import gc
import os
import resource
from types import SimpleNamespace
from unittest import mock

import numpy as np
import pytest
from astropy.io import fits

from banzai import data as banzai_data
from banzai.bias import BiasMaker
from banzai.data import CCDData, combine_images, create_combination_output_hdu
from banzai.lco import LCOFrameFactory
from banzai.smartstack_products import build_stacked_frame
import banzai.smartstack_products as products
from banzai.tests.utils import FakeContext, FakeInstrument
from banzai.utils.fits_utils import pack
from banzai.utils.image_utils import Section

SectionedCCDData = getattr(banzai_data, 'SectionedCCDData', None)
SectionedFrameError = getattr(banzai_data, 'SectionedFrameError', None)

SHAPE = (24, 32)
LAYOUTS = ['header-primary', 'sci-primary']


def science_header(obstype='EXPOSE', nsubread=1, ismaster=False):
    header = fits.Header()
    header['OBSTYPE'] = obstype
    header['EXTNAME'] = 'SCI'
    header['EXTVER'] = 1
    header['GAIN'] = 1.0
    header['RDNOISE'] = 3.0
    header['SATURATE'] = 65535.0
    header['MAXLIN'] = 60000.0
    header['DATE-OBS'] = '2026-10-22T22:12:17.924'
    header['DAY-OBS'] = '20261022'
    header['EXPTIME'] = 20.0
    header['NSUBREAD'] = nsubread
    header['BIASLVL'] = 100.0
    header['SITEID'] = 'cpt'
    header['INSTRUME'] = 'sq45'
    header['TELESCOP'] = '0m4b'
    header['CCDSUM'] = '1 1'
    header['CONFMODE'] = 'full_frame'
    header['DETSEC'] = '[1:32,1:24]'
    header['DATASEC'] = '[1:32,1:24]'
    header['TRIMSEC'] = '[1:32,1:24]'
    if ismaster:
        header['ISMASTER'] = True
    return header


def make_arrays(seed, dtype=np.float32):
    rng = np.random.default_rng(seed)
    data = rng.normal(1000.0, 10.0, SHAPE)
    if np.issubdtype(dtype, np.integer):
        data = np.clip(np.round(data), 0, 65535)
    data = data.astype(dtype)
    bpm = np.zeros(SHAPE, dtype=np.uint8)
    bpm[rng.integers(0, SHAPE[0], 5), rng.integers(0, SHAPE[1], 5)] = 1
    err = rng.uniform(2.0, 4.0, SHAPE).astype(np.float32)
    return data, bpm, err


def write_frame(path, data, bpm=None, err=None, header=None, layout='header-primary', compressed=False):
    """Write a frame the way BANZAI products look: either a header-only primary with a SCI extension
    (n09 layout) or the science array in the primary HDU (reduced calibration layout)."""
    header = header if header is not None else science_header()
    if layout == 'header-primary':
        primary_header = fits.Header()
        for keyword in ('OBSTYPE', 'DATE-OBS', 'DAY-OBS', 'EXPTIME', 'SITEID', 'INSTRUME', 'TELESCOP'):
            primary_header[keyword] = header[keyword]
        hdus = [fits.PrimaryHDU(header=primary_header), fits.ImageHDU(data=data, header=header, name='SCI')]
    elif layout == 'sci-primary':
        hdus = [fits.PrimaryHDU(data=data, header=header)]
    else:
        raise ValueError(layout)
    for name, array in (('BPM', bpm), ('ERR', err)):
        if array is not None:
            extension_header = fits.Header({'EXTVER': 1})
            hdus.append(fits.ImageHDU(data=array, header=extension_header, name=name))
    hdu_list = fits.HDUList(hdus)
    if compressed:
        hdu_list = pack(hdu_list, lossless_extensions=['BPM'])
    hdu_list.writeto(path, overwrite=True)
    return path


def write_frames(tmp_path, n, layout='header-primary', compressed=False, with_bpm=True, with_err=True,
                 dtype=np.float32, obstype='EXPOSE', nsubread=1, ismaster=False, prefix='cpt0m410-sq45-20261022'):
    extension = '.fits.fz' if compressed else '.fits'
    paths = []
    for i in range(n):
        data, bpm, err = make_arrays(seed=i, dtype=dtype)
        header = science_header(obstype=obstype, nsubread=nsubread, ismaster=ismaster)
        path = tmp_path / f'{prefix}-{i + 1:04d}-n09{extension}'
        write_frame(path, data, bpm if with_bpm else None, err if with_err else None, header=header,
                    layout=layout, compressed=compressed)
        paths.append(path)
    return paths


@pytest.fixture
def context():
    return FakeContext()


@pytest.fixture
def factory():
    instrument = FakeInstrument(0, 'cpt', 'sq45', 'aqwb', '0m4b', '0m4-SciCam-QHY600')
    with mock.patch.object(LCOFrameFactory, 'get_instrument_from_header', return_value=instrument):
        yield LCOFrameFactory()


def open_eager(factory, path, context):
    return factory.open({'path': str(path)}, context)


def open_sectioned(factory, path, context):
    return factory.open({'path': str(path)}, context, sectioned=True)


def science_hdu(frame):
    hdu = frame['SCI']
    return hdu if hdu is not None else frame.primary_hdu


def open_fds():
    return len(os.listdir('/dev/fd'))


def full_section():
    return Section(x_start=1, x_stop=SHAPE[1], y_start=1, y_stop=SHAPE[0])


def combine(frames, method):
    inputs = [science_hdu(frame) for frame in frames]
    output = create_combination_output_hdu(inputs[0], inputs[0].meta, memmap=False)
    combine_images(inputs, output, nsigma=3.0, method=method)
    return output


def native(dtype):
    """The eager path keeps uncompressed FITS arrays big-endian; sectioned sections are native. Compare on kind and size."""
    return np.dtype(dtype).newbyteorder('=')


def assert_same_arrays(actual, expected):
    np.testing.assert_array_equal(actual.data, expected.data)
    np.testing.assert_array_equal(actual.mask, expected.mask)
    np.testing.assert_array_equal(actual.uncertainty, expected.uncertainty)
    assert native(actual.data.dtype) == native(expected.data.dtype)
    assert native(actual.uncertainty.dtype) == native(expected.uncertainty.dtype)


def require_sectioned_class():
    assert SectionedCCDData is not None, 'banzai.data.SectionedCCDData is not implemented'
    assert SectionedFrameError is not None, 'banzai.data.SectionedFrameError is not implemented'


# --------------------------------------------------------------------------------------------------- opening


@pytest.mark.parametrize('layout', LAYOUTS)
@pytest.mark.parametrize('compressed', [False, True])
def test_sectioned_open_matches_eager_header_properties(tmp_path, factory, context, layout, compressed):
    require_sectioned_class()
    path = write_frames(tmp_path, 1, layout=layout, compressed=compressed, obstype='BIAS', nsubread=2)[0]
    eager = open_eager(factory, path, context)
    sectioned = open_sectioned(factory, path, context)

    assert isinstance(science_hdu(sectioned), SectionedCCDData)
    assert type(sectioned) is type(eager)
    for attribute in ('obstype', 'epoch', 'dateobs', 'exptime', 'bias_level', 'filename'):
        assert getattr(sectioned, attribute) == getattr(eager, attribute), attribute
    eager_sci, sectioned_sci = science_hdu(eager), science_hdu(sectioned)
    assert sectioned_sci.n_sub_exposures == eager_sci.n_sub_exposures == 2
    assert sectioned_sci.shape == eager_sci.shape
    assert native(sectioned_sci.dtype) == native(eager_sci.dtype)
    assert sectioned_sci.name == eager_sci.name
    for keyword in ('GAIN', 'RDNOISE', 'SATURATE', 'MAXLIN', 'DETSEC', 'DATASEC', 'NSUBREAD'):
        assert sectioned_sci.meta[keyword] == eager_sci.meta[keyword], keyword
    assert sectioned_sci.gain == eager_sci.gain
    assert sectioned_sci.read_noise == eager_sci.read_noise
    assert sectioned_sci.detector_section.to_region_keyword() == eager_sci.detector_section.to_region_keyword()
    assert sectioned_sci.data_section.to_region_keyword() == eager_sci.data_section.to_region_keyword()
    assert sectioned_sci.memmap is False
    assert [hdu.name for hdu in sectioned._hdus] == [hdu.name for hdu in eager._hdus]


# --------------------------------------------------------------------------------------------------- combining


COMBINE_CASES = [
    # layout, compressed, with_bpm, with_err, dtype, ismaster, method
    ('header-primary', False, True, True, np.float32, False, 'sum'),
    ('header-primary', True, True, True, np.float32, False, 'sum'),
    ('sci-primary', False, True, True, np.float32, False, 'mean'),
    ('sci-primary', True, True, True, np.float32, False, 'mean'),
    ('header-primary', False, False, False, np.float32, False, 'sum'),
    ('sci-primary', False, True, False, np.float32, True, 'mean'),
    ('header-primary', False, True, True, np.uint16, False, 'sum'),
    ('sci-primary', True, True, True, np.uint16, False, 'mean'),
]


@pytest.mark.parametrize('layout,compressed,with_bpm,with_err,dtype,ismaster,method', COMBINE_CASES)
def test_combine_sectioned_inputs_equals_eager(tmp_path, factory, context, layout, compressed, with_bpm, with_err,
                                          dtype, ismaster, method):
    require_sectioned_class()
    paths = write_frames(tmp_path, 5, layout=layout, compressed=compressed, with_bpm=with_bpm, with_err=with_err,
                         dtype=dtype, ismaster=ismaster)
    eager_frames = [open_eager(factory, path, context) for path in paths]
    sectioned_frames = [open_sectioned(factory, path, context) for path in paths]

    expected = combine(eager_frames, method)
    actual = combine(sectioned_frames, method)

    assert_same_arrays(actual, expected)


def test_sectioned_trim_is_native_byte_order_and_writable(tmp_path, factory, context):
    require_sectioned_class()
    path = write_frames(tmp_path, 1)[0]
    section = science_hdu(open_sectioned(factory, path, context)).trim(trim_section=full_section(), memmap=False)
    for array in (section.data, section.mask, section.uncertainty):
        assert array.dtype.isnative
        assert array.flags.writeable
    assert section.memmap is False


# --------------------------------------------------------------------------------------------------- descriptors


def test_sectioned_frames_hold_one_descriptor_each_and_release_on_close(tmp_path, factory, context):
    require_sectioned_class()
    paths = write_frames(tmp_path, 20)
    gc.collect()
    baseline = open_fds()

    frames = [open_sectioned(factory, path, context) for path in paths]
    assert open_fds() - baseline == len(paths)

    for frame in frames:
        science_hdu(frame).close()
    del frames
    gc.collect()
    assert open_fds() == baseline


def test_sectioned_stack_combines_under_a_low_descriptor_limit(tmp_path, factory, context):
    """A build must stay within one descriptor per frame all the way through the combine, not just at open."""
    require_sectioned_class()
    paths = write_frames(tmp_path, 20)
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    gc.collect()
    limit = open_fds() + len(paths) + 4  # one per frame plus a little slack for the test's own file handling
    try:
        resource.setrlimit(resource.RLIMIT_NOFILE, (limit, hard))
        sectioned_frames = [open_sectioned(factory, path, context) for path in paths]
        assert len(sectioned_frames) == len(paths)
        output = combine(sectioned_frames, 'sum')
        assert np.isfinite(output.data).all()
        for frame in sectioned_frames:
            science_hdu(frame).close()
    finally:
        resource.setrlimit(resource.RLIMIT_NOFILE, (soft, hard))


# --------------------------------------------------------------------------------------------------- scaling


@pytest.mark.parametrize('layout', LAYOUTS)
def test_sectioned_scalar_division_matches_eager(tmp_path, factory, context, layout):
    require_sectioned_class()
    path = write_frames(tmp_path, 1, layout=layout, obstype='BIAS', nsubread=2)[0]
    eager = open_eager(factory, path, context)
    sectioned = open_sectioned(factory, path, context)

    # Scale the science HDU directly; the frame-level dispatch is covered by the BiasMaker test below.
    for frame in (eager, sectioned):
        hdu = science_hdu(frame)
        hdu /= hdu.n_sub_exposures

    section = full_section()
    assert_same_arrays(science_hdu(sectioned).trim(trim_section=section, memmap=False),
                       science_hdu(eager).trim(trim_section=section, memmap=False))
    for keyword in ('SATURATE', 'GAIN', 'MAXLIN'):
        assert science_hdu(sectioned).meta[keyword] == science_hdu(eager).meta[keyword], keyword


def test_bias_maker_over_sectioned_frames_equals_eager(tmp_path, factory, context):
    require_sectioned_class()
    paths = write_frames(tmp_path, 5, layout='sci-primary', obstype='BIAS', nsubread=2)
    eager_master = BiasMaker(context).make_master_calibration_frame([open_eager(factory, p, context) for p in paths])
    sectioned_master = BiasMaker(context).make_master_calibration_frame([open_sectioned(factory, p, context) for p in paths])

    assert_same_arrays(sectioned_master.primary_hdu, eager_master.primary_hdu)
    assert sectioned_master.bias_level == eager_master.bias_level
    assert sectioned_master.filename == eager_master.filename
    assert sectioned_master.primary_hdu.meta['DATE-OBS'] == eager_master.primary_hdu.meta['DATE-OBS']


# --------------------------------------------------------------------------------------------------- read-only


def test_sectioned_frame_rejects_array_access_and_mutation(tmp_path, factory, context):
    require_sectioned_class()
    # Science array in the primary HDU, so frame-level arithmetic dispatches to the sectioned HDU itself.
    path = write_frames(tmp_path, 1, layout='sci-primary')[0]
    sectioned = open_sectioned(factory, path, context)
    sci = science_hdu(sectioned)
    ones = np.ones(SHAPE, dtype=np.float32)
    zeros = np.zeros(SHAPE, dtype=np.uint8)

    with pytest.raises(SectionedFrameError):
        sci.data
    with pytest.raises(SectionedFrameError):
        sci.mask
    with pytest.raises(SectionedFrameError):
        sci.uncertainty
    with pytest.raises(SectionedFrameError):
        sci.data = ones
    with pytest.raises(SectionedFrameError):
        sci.uncertainty = ones
    with pytest.raises(SectionedFrameError):
        sci.add_mask(zeros)
    with pytest.raises(SectionedFrameError):
        sci -= ones
    with pytest.raises(SectionedFrameError):
        sectioned -= open_sectioned(factory, path, context)


# --------------------------------------------------------------------------------------------------- smartstack


def stackframes_for(paths):
    return [SimpleNamespace(filepath=str(path), stack_num=i + 1) for i, path in enumerate(paths)]


def test_build_stacked_frame_closes_sectioned_inputs(tmp_path, factory, context):
    require_sectioned_class()
    paths = write_frames(tmp_path, 4, obstype='SUB_EXP')
    gc.collect()
    baseline = open_fds()

    output = build_stacked_frame(stackframes_for(paths), context, 'mol-1')

    assert output['SCI'].meta['NCOMBINE'] == 4
    assert type(output['SCI']) is CCDData
    assert output['SCI'].memmap is False
    # The output lives in memory, so with the inputs closed nothing holds a descriptor.
    assert open_fds() == baseline


def test_build_stacked_frame_closes_sectioned_inputs_on_failure(tmp_path, factory, context, monkeypatch):
    require_sectioned_class()
    paths = write_frames(tmp_path, 4, obstype='SUB_EXP')

    def exploding_combine(*args, **kwargs):
        raise RuntimeError('combine failed')

    monkeypatch.setattr(products, 'combine_images', exploding_combine)
    gc.collect()
    baseline = open_fds()

    with pytest.raises(RuntimeError, match='combine failed'):
        build_stacked_frame(stackframes_for(paths), context, 'mol-1')

    gc.collect()
    assert open_fds() == baseline

