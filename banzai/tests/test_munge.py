# import pytest
# from astropy.io.fits import Header
#
# from banzai.tests.utils import FakeInstrument, FakeCCDData, FakeLCOObservationFrame
# from banzai.munge import set_crosstalk_header_keywords, sinistro_mode_is_supported, image_has_valid_saturate_value, \
#     update_saturate, munge
#
# pytestmark = pytest.mark.munge
#
#
# def test_no_coefficients_no_defaults():
#     assert not sinistro_mode_is_supported(FakeLCOObservationFrame(hdu_list=[FakeCCDData(meta={'INSTRUME': 'blah27'})
#                                                                             for hdu in range(4)]))
#
#
# def test_no_coefficients_with_defaults():
#     assert sinistro_mode_is_supported(FakeLCOObservationFrame(hdu_list=[FakeCCDData(meta={'INSTRUME': 'fa06'})
#                                                                         for hdu in range(4)]))
#
#
# def test_when_has_partial_coefficients():
#     extension_headers = []
#     #Only fill up the first two extensions with CRSTLK keywords: CRSTLK11,12,21,22
#     for i in range(2):
#         extension_headers.append({'CRSTLK{i}{j}'.format(i=i+1, j=j + 1): 0 for j in range(2)})
#         extension_headers[i]['INSTRUME'] = 'blah27'
#
#     # Add in a couple of empty headers for the last two extensions
#     extension_headers.extend([{'INSTRUME': 'blah27'} for i in range(1)])
#     hdu_list = [FakeCCDData(meta=header) for header in extension_headers]
#
#     assert not sinistro_mode_is_supported(FakeLCOObservationFrame(hdu_list=[FakeCCDData(meta=header)
#                                                                             for header in extension_headers]))
#
#
# def test_when_has_coefficients():
#     extension_headers = []
#     for i in range(4):
#         extension_headers.append({'CRSTLK{i}{j}'.format(i=i+1, j=j + 1): 0 for j in range(4)})
#         extension_headers[i]['INSTRUME'] = 'blah27'
#
#     assert sinistro_mode_is_supported(FakeLCOObservationFrame(hdu_list=[FakeCCDData(meta=header)
#                                                                         for header in extension_headers]))
#
#
# def test_defaults_do_not_override_header():
#     extension_headers = []
#     for i in range(4):
#         extension_headers.append({'CRSTLK{i}{j}'.format(i=i+1, j=j + 1): 0 for j in range(4)})
#         extension_headers[i]['INSTRUME'] = 'fa06'
#     image = FakeLCOObservationFrame(hdu_list=[FakeCCDData(meta=header)
#                                     for header in extension_headers])
#     image.instrument = FakeInstrument(type='sinistro', camera='fa06')
#     set_crosstalk_header_keywords(image)
#     for i in range(4):
#         for j in range(4):
#             assert image.ccd_hdus[i].meta['CRSTLK{i}{j}'.format(i=i + 1, j=j + 1)] == 0.0
#
#
# def test_image_invalid_saturate_value():
#     image = FakeLCOObservationFrame(hdu_list=[FakeCCDData(meta={'SATURATE': 0.0})])
#
#     assert not image_has_valid_saturate_value(image)
#
#
# def test_update_saturate():
#     image = FakeLCOObservationFrame(hdu_list=[FakeCCDData(meta=Header({'SATURATE': 0.0}))])
#
#     update_saturate(image, 35000.0)
#
#     assert image.meta['SATURATE'] == 35000.0
#
# #TODO: Update this
# def test_munge_sinistro():
#     extension_headers = []
#     for i in range(4):
#         extension_headers.append({'CRSTLK{i}{j}'.format(i=i+1, j=j + 1): 0 for j in range(4)})
#         extension_headers[i]['INSTRUME'] = 'fa06'
#     image = FakeLCOObservationFrame(hdu_list=[FakeCCDData(meta=header)
#                                     for header in extension_headers])
#     image.primary_hdu.meta['SATURATE'] = 47500.0
#     image.instrument = FakeInstrument(type='sinistro', camera='fa06')
#
#     munge(image)
