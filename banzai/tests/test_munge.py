import pytest
from banzai.tests.utils import FakeCCDData, FakeLCOObservationFrame, FakeInstrument
from banzai.lco import MissingCrosstalkCoefficients, LCOFrameFactory, MissingSaturate
from banzai.data import HeaderOnly

pytestmark = pytest.mark.munge


def test_no_coefficients_no_defaults():
    with pytest.raises(MissingCrosstalkCoefficients):
        LCOFrameFactory._init_crosstalk(FakeLCOObservationFrame(hdu_list=[FakeCCDData(meta={'INSTRUME': 'blah27'})
                                                                          for hdu in range(4)]))


def test_no_coefficients_with_defaults():
    LCOFrameFactory._init_crosstalk(FakeLCOObservationFrame(hdu_list=[FakeCCDData(meta={'INSTRUME': 'fa06'})
                                                                      for hdu in range(4)]))


def test_when_has_partial_coefficients():
    header = {'INSTRUME': 'blah27'}
    # Only fill up the first two extensions with CRSTLK keywords: CRSTLK11,12,21,22
    for i in range(2):
        for j in range(2):
            header['CRSTLK{i}{j}'.format(i=i+1, j=j+1)] = 1.0
    hdu_list = [HeaderOnly(meta=header)] + [FakeCCDData() for i in range(4)]
    with pytest.raises(MissingCrosstalkCoefficients):
        LCOFrameFactory._init_crosstalk(FakeLCOObservationFrame(hdu_list=hdu_list))


def test_when_has_coefficients():
    header = {'INSTRUME': 'blah27', 'SATURATE': 65536.0}
    # Only fill up the first two extensions with CRSTLK keywords: CRSTLK11,12,21,22
    for i in range(4):
        for j in range(4):
            header['CRSTLK{i}{j}'.format(i=i+1, j=j+1)] = 1.0
    hdu_list = [HeaderOnly(meta=header)] + [FakeCCDData() for i in range(4)]
    LCOFrameFactory._init_crosstalk(FakeLCOObservationFrame(hdu_list=hdu_list))


def test_defaults_do_not_override_header():
    header = {'INSTRUME': 'fa06'}
    for i in range(4):
        for j in range(4):
            header['CRSTLK{i}{j}'.format(i=i+1, j=j+1)] = 1.0

    hdu_list = [HeaderOnly(meta=header)] + [FakeCCDData() for i in range(4)]
    fake_image = FakeLCOObservationFrame(hdu_list=hdu_list)
    LCOFrameFactory._init_crosstalk(fake_image)

    for i in range(4):
        for j in range(4):
            assert fake_image.primary_hdu.meta['CRSTLK{i}{j}'.format(i=i + 1, j=j + 1)] == 1.0


def test_image_no_saturate_header_or_default():
    with pytest.raises(MissingSaturate):
        LCOFrameFactory._init_saturate(FakeLCOObservationFrame(hdu_list=[FakeCCDData(meta={'INSTRUME': 'blah27'})]))


def test_saturate_no_default_but_header():
    header = {'INSTRUME': 'blah27', 'SATURATE': 63535.0}
    LCOFrameFactory._init_saturate(FakeLCOObservationFrame(hdu_list=[FakeCCDData(meta=header)]))


def test_saturate_default_but_not_header():
    header = {'INSTRUME': 'fa06'}
    frame = FakeLCOObservationFrame(hdu_list=[FakeCCDData(meta=header)])
    frame.instrument = FakeInstrument(type='1m0-scicam-sinistro')
    LCOFrameFactory._init_saturate(frame)


def test_default_saturate_does_not_override_header():
    header = {'INSTRUME': 'fa06', 'SATURATE': 63535.0}
    frame = FakeLCOObservationFrame(hdu_list=[FakeCCDData(meta=header)])
    frame.instrument = FakeInstrument(type='1m0-scicam-sinistro')
    LCOFrameFactory._init_saturate(frame)
    assert frame.meta['SATURATE'] == 63535.0
