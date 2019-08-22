import logging

import numpy as np
from astropy.io import fits

logger = logging.getLogger('banzai')


class SinistroModeNotSupported(Exception):
    pass


def munge(image):
    if 'sinistro' in image.instrument.type.lower():
        if sinistro_mode_is_supported(image):
            munge_sinistro(image)
        else:
            raise SinistroModeNotSupported('Sinistro mode not supported {f}'.format(f=image.filename))
    # 1m SBIGS
    elif '1m0' in image.instrument.type:
        # Saturation level from ORAC Pipeline
        update_saturate(image, 46000.0)
    elif '0m4' in image.instrument.type or '0m8' in image.instrument.type:
        # Measured by Daniel Harbeck
        update_saturate(image, 64000.0)
    elif 'fs02' == image.instrument.camera:
        # These values were given by Joe Tufts on 2016-06-07
        binning = image.header.get('CCDSUM', '1 1')
        n_binned_pixels = int(binning[0]) * int(binning[2])
        update_saturate(image, 125000.0 * n_binned_pixels / float(image.header['GAIN']))

    if not image_has_valid_saturate_value(image):
        raise ValueError('Saturate value not valid {f}'.format(f=image.filename))


def update_saturate(image, saturation_level):
    if image.header.get('SATURATE', 0.0) == 0.0:
        image.header['SATURATE'] = (saturation_level, '[ADU] Saturation level used')
    if image.header.get('MAXLIN', 0.0) == 0.0:
        image.header['MAXLIN'] = (saturation_level, '[ADU] Non-linearity level')


def crosstalk_coefficients_in_header(image):
    """
    Check if there are crosstalk coeffients in the header of an image

    Parameters
    ----------
    image: banzai.images.Image
           Sinistro image to check

    Returns
    -------
    in_header: bool
               True if all the crosstalk coefficients are in the header
    """
    n_amps = image.get_n_amps()

    crosstalk_values = [image.header.get('CRSTLK{0}{1}'.format(i+1, j+1))
                        for i in range(n_amps) for j in range(n_amps) if i!=j]
    return None not in crosstalk_values


def sinistro_mode_is_supported(image):
    """
    Check to make sure the Sinistro image was taken in a supported mode.

    Parameters
    ----------
    image: banzai.images.Image
           Sinistro image to check

    Returns
    -------
    supported: bool
               True if reduction is supported
    """
    supported = True

    if image.camera not in crosstalk_coefficients.keys() and not crosstalk_coefficients_in_header(image):
        supported = False
        logger.error('Crosstalk Coefficients missing!', image=image)

    return supported


# We need to properly set the datasec and detsec keywords in case we didn't read out the
# middle row (the "Missing Row Problem").
sinistro_datasecs = {'missing': ['[1:2048,1:2048]', '[1:2048,1:2048]',
                                 '[1:2048,2:2048]', '[1:2048,2:2048]'],
                     'full': ['[1:2048,1:2048]', '[1:2048,1:2048]',
                              '[1:2048,2:2049]', '[1:2048,2:2049]']}
sinistro_detsecs = {'missing': ['[1:2048,1:2048]', '[4096:2049,1:2048]',
                                '[4096:2049,4096:2050]', '[1:2048,4096:2050]'],
                    'full': ['[1:2048,1:2048]', '[4096:2049,1:2048]',
                             '[4096:2049,4096:2049]', '[1:2048,4096:2049]']}


def munge_sinistro(image):
    if not image.extension_headers:
        image.extension_headers = [fits.Header() for i in range(4)]

    if not hasattr(image.gain, "__len__"):
        # Gain is a single value
        gain = image.gain
        image.gain = [gain for i in range(4)]

    if image.header['SATURATE'] == 0:
        image.header['SATURATE'] = 47500.0

    set_crosstalk_header_keywords(image)

    if image.data.shape[1] > 2048:
        datasecs = sinistro_datasecs['full']
        detsecs = sinistro_detsecs['full']
    else:
        datasecs = sinistro_datasecs['missing']
        detsecs = sinistro_detsecs['missing']

    for i in range(4):
        for keyword, value, comment in [('BIASSEC', '[2055:2080,1:2048]', '[binned pixel] Overscan Region'),
                                        ('DATASEC', datasecs[i], '[binned pixel] Data section'),
                                        ('DETSEC', detsecs[i], '[unbinned pixel] Detector section')]:
            # Don't override values if they exist already
            if image.extension_headers[i].get(keyword) is None:
                image.extension_headers[i][keyword] = value


def image_has_valid_saturate_value(image):
    """
    Check if the image has a valid saturate value.

    Parameters
    ----------
    image: banzai.images.Image

    Returns
    -------
    valid: bool
           True if the image has a non-zero saturate value. False otherwise.

    Notes
    -----
    The saturate keyword being zero causes a lot of headaches so we should just dump
    the image if the saturate value is zero after we have fixed the typical incorrect values.
    """
    valid = True

    if float(image.header['SATURATE']) == 0.0:
        logger.error('SATURATE keyword cannot be zero', image=image)
        valid = False

    return valid


def set_crosstalk_header_keywords(image):
    n_amps = image.get_n_amps()
    coefficients = crosstalk_coefficients[image.camera]

    for i in range(n_amps):
        for j in range(n_amps):
            if i != j:
                crosstalk_comment = '[Crosstalk coefficient] Signal from Q{i} onto Q{j}'.format(i=i+1, j=j+1)
                keyword = 'CRSTLK{0}{1}'.format(i + 1, j + 1)
                # Don't override existing header keywords.
                if image.header.get(keyword) is None:
                    image.header[keyword] = coefficients[i, j], crosstalk_comment


"""These matrices should have the following structure:
coeffs = [[Q11, Q12, Q13, Q14],
          [Q21, Q22, Q23, Q24],
          [Q31, Q32, Q33, Q34],
          [Q41, Q42, Q43, Q44]]

The corrected data, D, from quadrant i is
D1 = D1 - Q21 D2 - Q31 D3 - Q41 D4
D2 = D2 - Q12 D1 - Q32 D3 - Q42 D4
D3 = D3 - Q13 D1 - Q23 D2 - Q43 D4
D4 = D4 - Q14 D1 - Q24 D2 - Q34 D3
"""

crosstalk_coefficients = {'fl01': np.array([[0.00000, 0.00074, 0.00081, 0.00115],
                                            [0.00070, 0.00000, 0.00118, 0.00085],
                                            [0.00076, 0.00115, 0.00000, 0.00088],
                                            [0.00107, 0.00075, 0.00080, 0.00000]]),
                          'fl02': np.array([[0.00000, 0.00084, 0.00088, 0.00125],
                                            [0.00083, 0.00000, 0.00124, 0.00096],
                                            [0.00086, 0.00121, 0.00000, 0.00098],
                                            [0.00116, 0.00085, 0.00092, 0.00000]]),
                          'fl03': np.array([[0.00000, 0.00076, 0.00079, 0.00115],
                                            [0.00073, 0.00000, 0.00117, 0.00084],
                                            [0.00074, 0.00113, 0.00000, 0.00084],
                                            [0.00105, 0.00075, 0.00080, 0.00000]]),
                          'fl04': np.array([[0.00000, 0.00088, 0.00096, 0.00131],
                                            [0.00087, 0.00000, 0.00132, 0.00099],
                                            [0.00087, 0.00127, 0.00000, 0.00103],
                                            [0.00123, 0.00089, 0.00094, 0.00000]]),
                          'fl05': np.array([[0.00000, 0.00084, 0.00090, 0.00126],
                                            [0.00089, 0.00000, 0.00133, 0.00095],
                                            [0.00097, 0.00155, 0.00000, 0.00108],
                                            [0.00134, 0.00096, 0.00095, 0.00000]]),
                          'fl06': np.array([[0.00000, 0.00076, 0.00068, 0.00129],
                                            [0.00082, 0.00000, 0.00141, 0.00090],
                                            [0.00095, 0.00124, 0.00000, 0.00107],
                                            [0.00110, 0.00076, 0.00106, 0.00000]]),
                          'fl07': np.array([[0.00000, 0.00075, 0.00077, 0.00113],
                                            [0.00071, 0.00000, 0.00113, 0.00082],
                                            [0.00070, 0.00108, 0.00000, 0.00086],
                                            [0.00095, 0.00067, 0.00077, 0.00000]]),
                          'fl08': np.array([[0.00000, 0.00057, 0.00078, 0.00130],
                                            [0.00112, 0.00000, 0.00163, 0.00123],
                                            [0.00104, 0.00113, 0.00000, 0.00113],
                                            [0.00108, 0.00048, 0.00065, 0.00000]]),
                          'fl10': np.array([[0.00000, 0.00000, 0.00000, 0.00000],
                                            [0.00000, 0.00000, 0.00000, 0.00000],
                                            [0.00000, 0.00000, 0.00000, 0.00000],
                                            [0.00000, 0.00000, 0.00000, 0.00000]]),
                          'fl11': np.array([[0.00000, 0.00075, 0.00078, 0.00113],
                                            [0.00065, 0.00000, 0.00114, 0.00096],
                                            [0.00070, 0.00101, 0.00000, 0.00086],
                                            [0.00098, 0.00073, 0.00082, 0.00000]]),
                          'fl12': np.array([[0.00000, 0.00083, 0.00089, 0.00127],
                                            [0.00079, 0.00000, 0.00117, 0.00091],
                                            [0.00081, 0.00113, 0.00000, 0.00094],
                                            [0.00105, 0.00081, 0.00087, 0.00000]]),
                          'fl14': np.array([[0.00000, 0.00084, 0.00086, 0.00121],
                                            [0.00094, 0.00000, 0.00134, 0.00103],
                                            [0.00094, 0.00129, 0.00000, 0.00105],
                                            [0.00097, 0.00092, 0.00099, 0.00000]]),
                          'fl15': np.array([[0.00000, 0.00071, 0.00083, 0.00110],
                                            [0.00069, 0.00000, 0.00107, 0.00081],
                                            [0.00071, 0.00098, 0.00000, 0.00083],
                                            [0.00091, 0.00071, 0.00078, 0.00000]]),
                          'fl16': np.array([[0.00000, 0.00080, 0.00084, 0.00125],
                                            [0.00071, 0.00000, 0.00122, 0.00088],
                                            [0.00071, 0.00121, 0.00000, 0.00090],
                                            [0.00116, 0.00084, 0.00089, 0.00000]]),

                          # Archon-controlled imagers as of fall 2018

                          'fa01': np.array([[0.00000, 0.00074, 0.00081, 0.00115],
                                            [0.00070, 0.00000, 0.00118, 0.00085],
                                            [0.00076, 0.00115, 0.00000, 0.00088],
                                            [0.00107, 0.00075, 0.00080, 0.00000]]),
                          'fa02': np.array([[0.00000, 0.00084, 0.00088, 0.00125],
                                            [0.00083, 0.00000, 0.00124, 0.00096],
                                            [0.00086, 0.00121, 0.00000, 0.00098],
                                            [0.00116, 0.00085, 0.00092, 0.00000]]),
                          'fa03': np.array([[0.00000, 0.00076, 0.00079, 0.00115],
                                            [0.00073, 0.00000, 0.00117, 0.00084],
                                            [0.00074, 0.00113, 0.00000, 0.00084],
                                            [0.00105, 0.00075, 0.00080, 0.00000]]),
                          'fa04': np.array([[0.00000, 0.00088, 0.00096, 0.00131],
                                            [0.00087, 0.00000, 0.00132, 0.00099],
                                            [0.00087, 0.00127, 0.00000, 0.00103],
                                            [0.00123, 0.00089, 0.00094, 0.00000]]),
                          'fa05': np.array([[0.00000, 0.00084, 0.00090, 0.00126],
                                            [0.00089, 0.00000, 0.00133, 0.00095],
                                            [0.00097, 0.00155, 0.00000, 0.00108],
                                            [0.00134, 0.00096, 0.00095, 0.00000]]),
                          'fa06': np.array([[0.00000, 0.00076, 0.00068, 0.00129],
                                            [0.00082, 0.00000, 0.00141, 0.00090],
                                            [0.00095, 0.00124, 0.00000, 0.00107],
                                            [0.00110, 0.00076, 0.00106, 0.00000]]),
                          'fa07': np.array([[0.00000, 0.00075, 0.00077, 0.00113],
                                            [0.00071, 0.00000, 0.00113, 0.00082],
                                            [0.00070, 0.00108, 0.00000, 0.00086],
                                            [0.00095, 0.00067, 0.00077, 0.00000]]),
                          'fa08': np.array([[0.00000, 0.00057, 0.00078, 0.00130],
                                            [0.00112, 0.00000, 0.00163, 0.00123],
                                            [0.00104, 0.00113, 0.00000, 0.00113],
                                            [0.00108, 0.00048, 0.00065, 0.00000]]),
                          'fa10': np.array([[0.00000, 0.00000, 0.00000, 0.00000],
                                            [0.00000, 0.00000, 0.00000, 0.00000],
                                            [0.00000, 0.00000, 0.00000, 0.00000],
                                            [0.00000, 0.00000, 0.00000, 0.00000]]),
                          'fa11': np.array([[0.00000, 0.00075, 0.00078, 0.00113],
                                            [0.00065, 0.00000, 0.00114, 0.00096],
                                            [0.00070, 0.00101, 0.00000, 0.00086],
                                            [0.00098, 0.00073, 0.00082, 0.00000]]),
                          'fa12': np.array([[0.00000, 0.00083, 0.00089, 0.00127],
                                            [0.00079, 0.00000, 0.00117, 0.00091],
                                            [0.00081, 0.00113, 0.00000, 0.00094],
                                            [0.00105, 0.00081, 0.00087, 0.00000]]),
                          'fa14': np.array([[0.00000, 0.00084, 0.00086, 0.00121],
                                            [0.00094, 0.00000, 0.00134, 0.00103],
                                            [0.00094, 0.00129, 0.00000, 0.00105],
                                            [0.00097, 0.00092, 0.00099, 0.00000]]),
                          'fa15': np.array([[0.00000, 0.00071, 0.00083, 0.00110],
                                            [0.00069, 0.00000, 0.00107, 0.00081],
                                            [0.00071, 0.00098, 0.00000, 0.00083],
                                            [0.00091, 0.00071, 0.00078, 0.00000]]),
                          'fa16': np.array([[0.00000, 0.00080, 0.00084, 0.00125],
                                            [0.00071, 0.00000, 0.00122, 0.00088],
                                            [0.00071, 0.00121, 0.00000, 0.00090],
                                            [0.00116, 0.00084, 0.00089, 0.00000]]),
                          }
