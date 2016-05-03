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
    threshold = 10.0
    min_area = 9

    def __init__(self, pipeline_context):
        super(SourceDetector, self).__init__(pipeline_context)

    @property
    def group_by_keywords(self):
        return None

    def do_stage(self, images):
        for i, image in enumerate(images):
            try:
                # Set the number of source pixels to be 5% of the total. This keeps us safe from
                # satellites and airplanes.
                sep.set_extract_pixstack(int(image.nx * image.ny * 0.05))

                data = image.data.copy()
                error = (np.abs(data) + image.readnoise ** 2.0) ** 0.5
                mask = image.bpm > 0

                # Fits can be backwards byte order, so fix that if need be and subtract
                # the background
                try:
                    bkg = sep.Background(data, mask=mask, bw=32, bh=32, fw=3, fh=3)
                except ValueError:
                    data = data.byteswap(True).newbyteorder()
                    bkg = sep.Background(data, mask=mask, bw=32, bh=32, fw=3, fh=3)
                bkg.subfrom(data)

                # Do an initial source detection
                # TODO: Add back in masking after we are sure SEP works
                sources = sep.extract(data, self.threshold, minarea=self.min_area,
                                      err=error, deblend_cont=0.005)

                # Convert the detections into a table
                sources = Table(sources)

                # Calculate the ellipticity
                sources['ellipticity'] = 1.0 - (sources['b'] / sources['a'])

                # Fix any value of theta that are invalid due to floating point rounding
                # -pi / 2 < theta < pi / 2
                sources['theta'][sources['theta'] > (np.pi / 2.0)] -= np.pi
                sources['theta'][sources['theta'] < (-np.pi / 2.0)] += np.pi

                # Calculate the kron radius
                kronrad, krflag = sep.kron_radius(data, sources['x'], sources['y'],
                                                  sources['a'], sources['b'],
                                                  sources['theta'], 6.0)
                sources['flag'] |= krflag
                sources['kronrad'] = kronrad

                # Calcuate the equivilent of flux_auto
                flux, fluxerr, flag = sep.sum_ellipse(data, sources['x'], sources['y'],
                                                      sources['a'], sources['b'],
                                                      np.pi / 2.0, 2.5 * kronrad,
                                                      subpix=1, err=error)
                sources['flux'] = flux
                sources['fluxerr'] = fluxerr
                sources['flag'] |= flag

                # Calculate the FWHMs of the stars:
                fwhm = 2.0 * (np.log(2) * (sources['a'] ** 2.0 + sources['b'] ** 2.0)) ** 0.5
                sources['fwhm'] = fwhm

                # Cut individual bright pixels. Often cosmic rays
                sources = sources[fwhm > 1.0]

                # Measure the flux profile
                flux_radii, flag = sep.flux_radius(data, sources['x'], sources['y'],
                                                   6.0 * sources['a'], [0.25, 0.5, 0.75],
                                                   normflux=sources['flux'], subpix=5)
                sources['flag'] |= flag
                sources['fluxrad25'] = flux_radii[:, 0]
                sources['fluxrad50'] = flux_radii[:, 1]
                sources['fluxrad75'] = flux_radii[:, 2]

                # Calculate the windowed positions
                sig = 2.0 / 2.35 * sources['fluxrad50']
                xwin, ywin, flag = sep.winpos(data, sources['x'], sources['y'], sig)
                sources['flag'] |= flag
                sources['xwin'] = xwin
                sources['ywin'] = ywin

                # Calculate the average background at each source
                bkgflux, fluxerr, flag = sep.sum_ellipse(bkg.back(), sources['x'], sources['y'],
                                                         sources['a'], sources['b'], np.pi / 2.0,
                                                         2.5 * kronrad, subpix=1)
                #masksum, fluxerr, flag = sep.sum_ellipse(mask, sources['x'], sources['y'],
                #                                         sources['a'], sources['b'], np.pi / 2.0,
                #                                         2.5 * kronrad, subpix=1)

                background_area = (2.5 * kronrad) ** 2.0 * sources['a'] * sources['b'] * np.pi
                sources['background'] = bkgflux / background_area  #- masksum)

                # Update the catalog to match fits convention instead of python array convention
                sources['x'] += 1.0
                sources['y'] += 1.0

                sources['xpeak'] += 1.0
                sources['ypeak'] += 1.0

                sources['xwin'] += 1.0
                sources['ywin'] += 1.0

                sources['theta'] = np.degrees(sources['theta'])

                image.catalog = sources['x', 'y', 'xwin', 'ywin', 'xpeak', 'ypeak',
                                        'flux', 'fluxerr', 'background', 'fwhm',
                                        'a', 'b', 'theta', 'kronrad', 'ellipticity',
                                        'fluxrad25', 'fluxrad50', 'fluxrad75',
                                        'x2', 'y2', 'xy', 'flag']

                # Add the units and description to the catalogs
                image.catalog['x'].unit = 'pixel'
                image.catalog['x'].description = 'X coordinate of the object'
                image.catalog['y'].unit = 'pixel'
                image.catalog['y'].description = 'Y coordinate of the object'
                image.catalog['xwin'].unit = 'pixel'
                image.catalog['xwin'].description = 'Windowed X coordinate of the object'
                image.catalog['ywin'].unit = 'pixel'
                image.catalog['ywin'].description = 'Windowed Y coordinate of the object'
                image.catalog['xpeak'].unit = 'pixel'
                image.catalog['xpeak'].description = 'X coordinate of the peak'
                image.catalog['ypeak'].unit = 'pixel'
                image.catalog['ypeak'].description = 'Windowed Y coordinate of the peak'
                image.catalog['flux'].unit = 'counts'
                image.catalog['flux'].description = 'Flux within a Kron-like elliptical aperture'
                image.catalog['fluxerr'].unit = 'counts'
                image.catalog['fluxerr'].description = 'Erronr on the flux within a Kron-like elliptical aperture'
                image.catalog['background'].unit = 'counts'
                image.catalog['background'].description = 'Average background value in the aperture'
                image.catalog['fwhm'].unit = 'pixel'
                image.catalog['fwhm'].description = 'FWHM of the object'
                image.catalog['a'].unit = 'pixel'
                image.catalog['a'].description = 'Semi-major axis of the object'
                image.catalog['b'].unit = 'pixel'
                image.catalog['b'].description = 'Semi-minor axis of the object'
                image.catalog['theta'].unit = 'degrees'
                image.catalog['theta'].description = 'Position angle of the object'
                image.catalog['kronrad'].unit = 'pixel'
                image.catalog['kronrad'].description = 'Kron radius used for extraction'
                image.catalog['ellipticity'].description = 'Ellipticity'
                image.catalog['fluxrad25'].unit = 'pixel'
                image.catalog['fluxrad25'].description = 'Radius containing 25% of the flux'
                image.catalog['fluxrad50'].unit = 'pixel'
                image.catalog['fluxrad50'].description = 'Radius containing 50% of the flux'
                image.catalog['fluxrad75'].unit = 'pixel'
                image.catalog['fluxrad75'].description = 'Radius containing 75% of the flux'
                image.catalog['x2'].unit = 'pixel^2'
                image.catalog['x2'].description = 'Variance on X coordinate of the object'
                image.catalog['y2'].unit = 'pixel^2'
                image.catalog['y2'].description = 'Variance on Y coordinate of the object'
                image.catalog['xy'].unit = 'pixel^2'
                image.catalog['xy'].description = 'XY covariance of the object'
                image.catalog['flag'].description = 'Bit mask combination of extraction and photometry flags'

                image.catalog.sort('flux')
                image.catalog.reverse()

                logging_tags = logs.image_config_to_tags(image, self.group_by_keywords)
                logs.add_tag(logging_tags, 'filename', os.path.basename(image.filename))

                # Save some background statistics in the header
                mean_background = stats.sigma_clipped_mean(bkg.back(), 5.0)
                image.header['L1MEAN'] = (mean_background,
                                          '[counts] Sigma clipped mean of frame background')
                logs.add_tag(logging_tags, 'L1MEAN', float(mean_background))

                median_background = np.median(bkg.back())
                image.header['L1MEDIAN'] = (median_background,
                                            '[counts] Median of frame background')
                logs.add_tag(logging_tags, 'L1MEDIAN', float(median_background))

                std_background = stats.robust_standard_deviation(bkg.back())
                image.header['L1SIGMA'] = (std_background,
                                           '[counts] Robust std dev of frame background')
                logs.add_tag(logging_tags, 'L1SIGMA', float(std_background))

                # Save some image statistics to the header
                good_objects = image.catalog['flag'] == 0

                seeing = np.median(image.catalog['fwhm'][good_objects]) * image.pixel_scale
                image.header['L1FWHM'] = (seeing, '[arcsec] Frame FWHM in arcsec')
                logs.add_tag(logging_tags, 'L1FWHM', float(fwhm))

                mean_ellipticity = stats.sigma_clipped_mean(sources['ellipticity'][good_objects],
                                                            3.0)
                image.header['L1ELLIP'] = (mean_ellipticity, 'Mean image ellipticity (1-B/A)')
                logs.add_tag(logging_tags, 'L1ELLIP', float(mean_ellipticity))

                mean_position_angle = stats.sigma_clipped_mean(sources['theta'][good_objects], 3.0)
                image.header['L1ELLIPA'] = (mean_position_angle,
                                            '[deg] PA of mean image ellipticity')
                logs.add_tag(logging_tags, 'L1ELLIPA', float(mean_position_angle))

                self.logger.info('Extracted sources', extra=logging_tags)

            except Exception as e:
                logging_tags = logs.image_config_to_tags(image, self.group_by_keywords)
                logs.add_tag(logging_tags, 'filename', os.path.basename(image.filename))
                self.logger.error(e, extra=logging_tags)
        return images
