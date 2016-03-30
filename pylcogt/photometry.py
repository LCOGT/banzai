from __future__ import absolute_import, print_function, division
import os
import numpy as np

from astropy.table import Table
from pylcogt.utils import stats

from pylcogt.stages import Stage
from pylcogt import logs

import sep

__author__ = 'cmccully'


class SourceDetector(Stage):
    # Note that threshold is number of sigma, not an absolute number because we provide the error
    # array to SEP.
    threshold = 3.0

    def __init__(self, pipeline_context):
        super(SourceDetector, self).__init__(pipeline_context)

    @property
    def group_by_keywords(self):
        return None

    def do_stage(self, images):
        for i, image in enumerate(images):
            data = image.data.copy()
            error = (np.abs(data) + image.readnoise ** 2.0) ** 0.5
            mask = image.bpm > 0

            try:
                bkg = sep.Background(data, bw=32, bh=32, fw=3, fh=3)
            except ValueError:
                data = data.byteswap(True).newbyteorder()
                bkg = sep.Background(data, bw=32, bh=32, fw=3, fh=3)
            bkg.subfrom(data)

            sources = sep.extract(data, self.threshold, err=error, mask=mask)

            sources = Table(sources)
            x = sources['x']
            y = sources['y']
            a = sources['a']
            b = sources['b']
            theta = sources['theta']

            theta[theta > (np.pi / 2.0)] -= np.pi
            theta[theta < (-np.pi / 2.0)] += np.pi
            kronrad, krflag = sep.kron_radius(data, x, y, a, b, theta, 6.0)
            flux, fluxerr, flag = sep.sum_ellipse(data, x, y, a, b, np.pi / 2.0, 2.5*kronrad,
                                                  subpix=1, err=error)

            sources['flux'] = flux
            sources['fluxerr'] = fluxerr
            sources['flag'] |= flag

            # Get the FWHM
            hwhm, flag = sep.flux_radius(data, sources['x'], sources['y'], 6.*sources['a'], 0.5,
                                         normflux=sources['flux'], subpix=5)

            # Cut bright pixels. Often cosmic rays
            sources = sources[hwhm > 0.5]
            hwhm = hwhm[hwhm > 0.5]
            hwhm_mean = stats.sigma_clipped_mean(hwhm, 3.0)

            hwhm_std = stats.robust_standard_deviation(hwhm)

            good_stars = hwhm > (hwhm_mean - 3.0 * hwhm_std)
            sources = sources[good_stars]

            # Update the catalog to match fits convention instead of python array convention
            sources['x'] += 1.0
            sources['y'] += 1.0

            image.catalog = sources['x', 'y', 'a', 'b', 'theta', 'flux', 'fluxerr']
            image.catalog.sort('flux')
            image.catalog.reverse()

            logging_tags = logs.image_config_to_tags(image, self.group_by_keywords)
            logs.add_tag(logging_tags, 'filename', os.path.basename(image.filename))

            # Save some background statistics in the header
            mean_background = stats.sigma_clipped_mean(bkg.back(), 5.0)
            image.header['L1MEAN'] = (mean_background,
                                      '[counts] Sigma clipped mean of frame background')
            logs.add_tag(logging_tags, 'L1MEAN', mean_background)

            median_background = np.median(bkg.back())
            image.header['L1MEDIAN'] = (median_background,
                                        '[counts] Median of frame background')
            logs.add_tag(logging_tags, 'L1MEDIAN', median_background)

            std_background = stats.robust_standard_deviation(bkg.back())
            image.header['L1SIGMA'] = (std_background,
                                       '[counts] Robust std dev of frame background')
            logs.add_tag(logging_tags, 'L1SIGMA', std_background)

            # Save some image statistics to the header
            fwhm = 2.0 * hwhm_mean * image.pixel_scale
            image.header['L1FWHM'] = (fwhm, '[arcsec] Frame FWHM in arcsec')
            logs.add_tag(logging_tags, 'L1FWHM', fwhm)

            mean_ellipticity = stats.sigma_clipped_mean(1.0 - (sources['b'] / sources['a']), 3.0)
            image.header['L1ELLIP'] = (mean_ellipticity, 'Mean image ellipticity (1-B/A)')
            logs.add_tag(logging_tags, 'L1ELLIP', mean_ellipticity)

            mean_position_angle = stats.sigma_clipped_mean(sources['theta'], 3.0)
            image.header['L1ELLIPA'] = (mean_position_angle, '[deg] PA of mean image ellipticity')
            logs.add_tag(logging_tags, 'L1ELLIPA', mean_position_angle)

            self.logger('Extracted sources', extra=logging_tags)
        return images
