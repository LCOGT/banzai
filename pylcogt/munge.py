from pylcogt.stages import Stage
import numpy as np


class DataMunger(Stage):
    def __init__(self, pipeline_context):
        super(DataMunger, self).__init__(pipeline_context)

    def group_by_keywords(self):
        return None

    def do_stage(self, images):
        for image in images:
            # TODO: Currently we only munge Sinistro frames and only support 1x1 frames.
            # TODO: 2x2 frames cannot use hard coded values because we read out more pixels.
            if image.instrument[:2] == 'fl':
                keywords_to_update = {'BIASSEC1': '[1:2048,2055:2080]',
                                      'BIASSEC2': '[1:2048,2055:2080]',
                                      'BIASSEC3': '[1:2048,2055:2080]',
                                      'BIASSEC4': '[1:2048,2055:2080]',
                                      'DATASEC1': '[1:2048,1:2048]',
                                      'DATASEC2': '[1:2048,1:2048]',
                                      'DETSEC1': '[1:2048,1:2048]',
                                      'DETSEC2': '[4096:2049,1:2048]',
                                      'DETSEC3': '[4096:2049,4096:2049]',
                                      'DETSEC4': '[1:2048,4096:2049]'}

                if image.data[2].shape[0] > 2048:
                    keywords_to_update['DATASEC3'] = '[1:2048,2:2049]'
                    keywords_to_update['DATASEC4'] = '[1:2048,2:2049]'

                else:
                    keywords_to_update['DETSEC3'] = '[1:2048,2:2049]'
                    keywords_to_update['DETSEC4'] = '[1:2048,2:2049]'

                set_crosstalk_header_keywords(image)

                for keyword in keywords_to_update:
                    _add_header_keyword(keyword, keywords_to_update[keyword], image)


def _add_header_keyword(keyword, value, image):
    if image.header.get(keyword) is None:
        image.header[keyword] = value


def set_crosstalk_header_keywords(image):
    n_amps = image.data.shape[0]
    coefficients = crosstalk_coefficients.get(image.instrument, default=np.zeros((n_amps, n_amps)))

    for j in range(n_amps):
        for i in range(n_amps):
            if i != j:
                image.header['CRSTLK{0}{1}'.format(i + 1, j + 1)] = coefficients[j, i]

crosstalk_coefficients = {'fl01': np.array([[0.0, 0.00074, 0.00081, 0.00115],
                                            [0.0007, 0.0, 0.00118, 0.00085],
                                            [0.00076, 0.00115, 0.0, 0.00088],
                                            [0.00107, 0.00075, 0.0008, 0.0]]),
                          'fl02': np.array([[0.0, 0.00084, 0.00088, 0.00125],
                                            [0.00083, 0.0, 0.00124, 0.00096],
                                            [0.00086, 0.00121, 0.0, 0.00098],
                                            [0.00116, 0.00085, 0.00092, 0.0]]),
                          'fl03': np.array([[0.0, 0.00076, 0.00079, 0.00115],
                                            [0.00073, 0.0, 0.00117, 0.00084],
                                            [0.00074, 0.00113, 0.0, 0.00084],
                                            [0.00105, 0.00075, 0.0008, 0.0]]),
                          'fl04': np.array([[0.0, 0.00088, 0.00096, 0.00131],
                                            [0.00087, 0.0, 0.00132, 0.00099],
                                            [0.00087, 0.00127, 0.0, 0.00103],
                                            [0.00123, 0.00089, 0.00094, 0.0]]),
                          'fl05': np.array([[0.0, 0.00084, 0.00090, 0.00126],
                                            [0.00089, 0.0, 0.00133, 0.00095],
                                            [0.00097, 0.00155, 0.0, 0.00108],
                                            [0.00134, 0.00096, 0.00095, 0.0]]),
                          'fl06': np.array([[0.0, 0.00078, 0.00085, 0.00118],
                                            [0.00075, 0.0, 0.00112, 0.00087],
                                            [0.00074, 0.00110, 0.0, 0.00089],
                                            [0.00098, 0.00075, 0.00081, 0.0]]),
                          'fl07': np.array([[0.0, 0.00075, 0.00077, 0.00113],
                                            [0.00071, 0.0, 0.00113, 0.00082],
                                            [0.0007, 0.00108, 0.0, 0.00086],
                                            [0.00095, 0.00067, 0.00077, 0.0]]),
                          'fl08': np.array([[0.0, 0.00057, 0.00078, 0.0013],
                                            [0.00112, 0.0, 0.00163, 0.00123],
                                            [0.00104, 0.00113, 0.0, 0.00113],
                                            [0.00108, 0.00048, 0.00065, 0.0]])
                          }
