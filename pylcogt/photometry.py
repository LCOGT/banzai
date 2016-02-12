from __future__ import absolute_import, print_function, division
import os
import numpy as np

from astropy.io import fits

from .stages import Stage

import sep

__author__ = 'cmccully'


class SourceDetector(Stage):
    def __init__(self, pipeline_context):
        super(SourceDetector, self).__init__(pipeline_context)

    @property
    def group_by_keywords(self):
        return None

    def do_stage(self, images):
        for i, image in enumerate(images):
            self.logger.info('Extracting sources from {filename}'.format(filename=image.filename))
            data = image.data.copy()
            error = (data + image.readnoise ** 2.0) ** 0.5
            mask = np.zeros(data.shape)

            try:
                bkg = sep.Background(data, bw=32, bh=32, fw=3, fh=3)
            except ValueError:
                data = data.byteswap(True).newbyteorder()
                bkg = sep.Background(data, bw=32, bh=32, fw=3, fh=3)
            bkg.subfrom(data)
            threshold = 3.0 * bkg.globalrms

            sources = sep.extract(data, threshold, err=error, mask=mask)

            x, y, a, b, theta = sources[['x', 'y', 'a', 'b', 'theta]']]

            theta[theta > (np.pi / 2.0)] -= np.pi
            theta[theta < (-np.pi / 2.0)] += np.pi
            kronrad, krflag = sep.kron_radius(data, x, y, a, b, theta, 6.0)
            flux, fluxerr, flag = sep.sum_ellipse(data, x, y, a, b, np.pi / 2.0, 2.5*kronrad,
                                                  subpix=1, err=error)

            sources['flux'] = flux
            sources['fluxerr'] = fluxerr

            image.catalog = fits.from_columns([fits.Column(name=i.upper(), format='E', array=sources[i])
                                               for i in ['x', 'y', 'flux', 'fluxerr', 'a', 'b', 'theta']])
