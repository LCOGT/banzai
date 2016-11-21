from __future__ import absolute_import, division, print_function, unicode_literals
from banzai.stages import Stage
import numpy as np
import os
from banzai import dbs, logs


class DataMunger(Stage):
    def __init__(self, pipeline_context):
        super(DataMunger, self).__init__(pipeline_context)

    @property
    def group_by_keywords(self):
        return None

    def do_stage(self, images):
        images_to_remove = []
        for image in images:
            telescope = dbs.get_telescope(image.telescope_id,
                                          db_address=self.pipeline_context.db_address)
            # TODO: Currently we only support 1x1 Sinistro frames and only support 1x1 frames.
            # TODO: 2x2 frames cannot use hard coded values because we read out more pixels.
            if 'sinistro' in telescope.camera_type.lower():
                if image.header['CCDSUM'] == '1 1':
                    keywords_to_update = [('BIASSEC1', ('[2055:2080,1:2048]',
                                                        '[binned pixel] Section of overscan data for Q1')),
                                          ('BIASSEC2', ('[2055:2080,1:2048]',
                                                        '[binned pixel] Section of overscan data for Q2')),
                                          ('BIASSEC3', ('[2055:2080,1:2048]',
                                                        '[binned pixel] Section of overscan data for Q3')),
                                          ('BIASSEC4', ('[2055:2080,1:2048]',
                                                        '[binned pixel] Section of overscan data for Q4')),
                                          ('DATASEC1', ('[1:2048,1:2048]',
                                                        '[binned pixel] Data section for Q1')),
                                          ('DATASEC2', ('[1:2048,1:2048]',
                                                        '[binned pixel] Data section for Q2'))]

                    if image.data.shape[1] > 2048:
                        keywords_to_update.append(('DATASEC3', ('[1:2048,2:2049]',
                                                   '[binned pixel] Data section for Q3')))
                        keywords_to_update.append(('DATASEC4', ('[1:2048,2:2049]',
                                                   '[binned pixel] Data section for Q4')))
                    else:
                        keywords_to_update.append(('DATASEC3', ('[1:2048,1:2048]',
                                                                '[binned pixel] Data section for Q3')))
                        keywords_to_update.append(('DATASEC4', ('[1:2048,1:2048]',
                                                                '[binned pixel] Data section for Q4')))
                    keywords_to_update.append(('DETSEC1', ('[1:2048,1:2048]',
                                                           '[binned pixel] Detector section for Q1')))
                    keywords_to_update.append(('DETSEC2', ('[4096:2049,1:2048]',
                                                           '[binned pixel] Detector section for Q2')))
                    keywords_to_update.append(('DETSEC3', ('[4096:2049,4096:2049]',
                                                           '[binned pixel] Detector section for Q3')))
                    keywords_to_update.append(('DETSEC4', ('[1:2048,4096:2049]',
                                                           '[binned pixel] Detector section for Q4')))
                elif image.header['CCDSUM'] == '2 2':
                    keywords_to_update = [('BIASSEC1', ('[1025:1040,1:1024]',
                                                        '[binned pixel] Section of overscan data for Q1')),
                                          ('BIASSEC2', ('[1025:1040,1:1024]',
                                                        '[binned pixel] Section of overscan data for Q2')),
                                          ('BIASSEC3', ('[1025:1040,1:1024]',
                                                        '[binned pixel] Section of overscan data for Q3')),
                                          ('BIASSEC4', ('[1025:1040,1:1024]',
                                                        '[binned pixel] Section of overscan data for Q4')),
                                          ('DATASEC1', ('[1:1024,1:1024]',
                                                        '[binned pixel] Data section for Q1')),
                                          ('DATASEC2', ('[1:1024,1:1024]',
                                                        '[binned pixel] Data section forQ2'))]

                    keywords_to_update.append(('DATASEC3', ('[1:1024,1:1024]',
                                                                '[binned pixel] Data section for Q3')))
                    keywords_to_update.append(('DATASEC4', ('[1:1024,1:1024]',
                                                                '[binned pixel] Data section for Q4')))
                    keywords_to_update.append(('DETSEC1', ('[1:1024,1:1024]',
                                                           '[binned pixel] Detector section for Q1')))
                    keywords_to_update.append(('DETSEC2', ('[2048:1025,1:1024]',
                                                           '[binned pixel] Detector section for Q2')))
                    keywords_to_update.append(('DETSEC3', ('[2048:1025,2048:1025]',
                                                           '[binned pixel] Detector section for Q3')))
                    keywords_to_update.append(('DETSEC4', ('[1:1024,2048:1025]',
                                                           '[binned pixel] Detector section for Q4')))

                try:
                    set_crosstalk_header_keywords(image)
                except KeyError as e:
                    images_to_remove.append(image)
                    logging_tags = logs.image_config_to_tags(image, self.group_by_keywords)
                    logs.add_tag(logging_tags, 'filename', image.filename)
                    self.logger.error('Crosstalk Coefficients missing!', extra=logging_tags)

                for keyword, value in keywords_to_update:
                    _add_header_keyword(keyword, value, image)

                if image.header['CCDSUM'] == '2 2':
                    image.header['TRIMSEC'] = ('[1:2048,1:2048]', '[binned pixel] Section of useful data')
                if image.header['SATURATE'] == 0:
                    image.header['SATURATE'] = 47500.0

            # 1m SBIGS
            elif '1m0' in telescope.camera_type:
                image.header['SATURATE'] = (46000.0, '[ADU] Saturation level used')
            elif '0m4' in telescope.camera_type or '0m8' in telescope.camera_type:
                image.header['SATURATE'] = (56000.0, '[ADU] Saturation level used')
            elif 'fs02' == telescope.instrument:
                # These values were given by Joe Tufts on 2016-06-07
                # These should really be measured empirically.
                if image.header['CCDSUM'] == '2 2':
                    image.header['SATURATE'] = (500000.0 / float(image.header['GAIN']),
                                                '[ADU] Saturation level used')
                elif image.header['CCDSUM'] == '1 1':
                    image.header['SATURATE'] = (125000.0 / float(image.header['GAIN']),
                                                '[ADU] Saturation level used')
            # Throw an exception if the saturate value is set to zero
            try:
                if float(image.header['SATURATE']) == 0.0:
                    images_to_remove.append(image)
                    raise ValueError('SATURATE keyword cannot be zero: {filename}'.format(filename=os.path.basename(image.filename)))
            except ValueError:
                continue

        for image in images_to_remove:
            images.remove(image)

        return images


def _add_header_keyword(keyword, value, image):
    if image.header.get(keyword) is None:
        image.header[keyword] = value


def set_crosstalk_header_keywords(image):
    n_amps = image.data.shape[0]
    coefficients = crosstalk_coefficients[image.instrument]

    for i in range(n_amps):
        for j in range(n_amps):
            if i != j:
                crosstalk_comment = '[Crosstalk coefficient] Signal from Q{i} onto Q{j}'.format(i=i+1, j=j+1)
                image.header['CRSTLK{0}{1}'.format(i + 1, j + 1)] = (coefficients[i, j], crosstalk_comment)

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
                          }
