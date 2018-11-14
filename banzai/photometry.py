import logging

import numpy as np
from astropy.table import Table
import sep

from banzai.utils import stats, array_utils
from banzai.stages import Stage
from banzai.images import DataTable
from banzai import logs

logger = logging.getLogger(__name__)


class SourceDetector(Stage):
    # Note that threshold is number of sigma, not an absolute number because we provide the error
    # array to SEP.
    threshold = 10.0
    min_area = 9

    def __init__(self, pipeline_context):
        super(SourceDetector, self).__init__(pipeline_context)

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

                # We remove anything with a detection flag >= 8
                # This includes memory overflows and objects that are too close the edge
                sources = sources[sources['flag'] < 8]

                sources = array_utils.prune_nans_from_table(sources)

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

                # Do circular aperture photometry for diameters of 1" to 6"
                for diameter in [1, 2, 3, 4, 5, 6]:
                    flux, fluxerr, flag = sep.sum_circle(data, sources['x'], sources['y'],
                                                         diameter / 2.0 / image.pixel_scale, gain=1.0, err=error)
                    sources['fluxaper{0}'.format(diameter)] = flux
                    sources['fluxerr{0}'.format(diameter)] = fluxerr
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
                                                         2.5 * sources['kronrad'], subpix=1)
                # masksum, fluxerr, flag = sep.sum_ellipse(mask, sources['x'], sources['y'],
                #                                         sources['a'], sources['b'], np.pi / 2.0,
                #                                         2.5 * kronrad, subpix=1)

                background_area = (2.5 * sources['kronrad']) ** 2.0 * sources['a'] * sources['b'] * np.pi # - masksum
                sources['background'] = bkgflux
                sources['background'][background_area > 0] /= background_area[background_area > 0]
                # Update the catalog to match fits convention instead of python array convention
                sources['x'] += 1.0
                sources['y'] += 1.0

                sources['xpeak'] += 1
                sources['ypeak'] += 1

                sources['xwin'] += 1.0
                sources['ywin'] += 1.0

                sources['theta'] = np.degrees(sources['theta'])

                catalog = sources['x', 'y', 'xwin', 'ywin', 'xpeak', 'ypeak',
                                        'flux', 'fluxerr', 'peak', 'fluxaper1', 'fluxerr1',
                                        'fluxaper2', 'fluxerr2', 'fluxaper3', 'fluxerr3',
                                        'fluxaper4', 'fluxerr4', 'fluxaper5', 'fluxerr5',
                                        'fluxaper6', 'fluxerr6', 'background', 'fwhm',
                                        'a', 'b', 'theta', 'kronrad', 'ellipticity',
                                        'fluxrad25', 'fluxrad50', 'fluxrad75',
                                        'x2', 'y2', 'xy', 'flag']

                # Add the units and description to the catalogs
                catalog['x'].unit = 'pixel'
                catalog['x'].description = 'X coordinate of the object'
                catalog['y'].unit = 'pixel'
                catalog['y'].description = 'Y coordinate of the object'
                catalog['xwin'].unit = 'pixel'
                catalog['xwin'].description = 'Windowed X coordinate of the object'
                catalog['ywin'].unit = 'pixel'
                catalog['ywin'].description = 'Windowed Y coordinate of the object'
                catalog['xpeak'].unit = 'pixel'
                catalog['xpeak'].description = 'X coordinate of the peak'
                catalog['ypeak'].unit = 'pixel'
                catalog['ypeak'].description = 'Windowed Y coordinate of the peak'
                catalog['flux'].unit = 'count'
                catalog['flux'].description = 'Flux within a Kron-like elliptical aperture'
                catalog['fluxerr'].unit = 'count'
                catalog['fluxerr'].description = 'Error on the flux within Kron aperture'
                catalog['peak'].unit = 'count'
                catalog['peak'].description = 'Peak flux (flux at xpeak, ypeak)'
                for diameter in [1, 2, 3, 4, 5, 6]:
                    catalog['fluxaper{0}'.format(diameter)].unit = 'count'
                    catalog['fluxaper{0}'.format(diameter)].description = 'Flux from fixed circular aperture: {0}" diameter'.format(diameter)
                    catalog['fluxerr{0}'.format(diameter)].unit = 'count'
                    catalog['fluxerr{0}'.format(diameter)].description = 'Error on Flux from circular aperture: {0}"'.format(diameter)

                catalog['background'].unit = 'count'
                catalog['background'].description = 'Average background value in the aperture'
                catalog['fwhm'].unit = 'pixel'
                catalog['fwhm'].description = 'FWHM of the object'
                catalog['a'].unit = 'pixel'
                catalog['a'].description = 'Semi-major axis of the object'
                catalog['b'].unit = 'pixel'
                catalog['b'].description = 'Semi-minor axis of the object'
                catalog['theta'].unit = 'degree'
                catalog['theta'].description = 'Position angle of the object'
                catalog['kronrad'].unit = 'pixel'
                catalog['kronrad'].description = 'Kron radius used for extraction'
                catalog['ellipticity'].description = 'Ellipticity'
                catalog['fluxrad25'].unit = 'pixel'
                catalog['fluxrad25'].description = 'Radius containing 25% of the flux'
                catalog['fluxrad50'].unit = 'pixel'
                catalog['fluxrad50'].description = 'Radius containing 50% of the flux'
                catalog['fluxrad75'].unit = 'pixel'
                catalog['fluxrad75'].description = 'Radius containing 75% of the flux'
                catalog['x2'].unit = 'pixel^2'
                catalog['x2'].description = 'Variance on X coordinate of the object'
                catalog['y2'].unit = 'pixel^2'
                catalog['y2'].description = 'Variance on Y coordinate of the object'
                catalog['xy'].unit = 'pixel^2'
                catalog['xy'].description = 'XY covariance of the object'
                catalog['flag'].description = 'Bit mask of extraction/photometry flags'

                catalog.sort('flux')
                catalog.reverse()

                # Save some background statistics in the header
                mean_background = stats.sigma_clipped_mean(bkg.back(), 5.0)
                image.header['L1MEAN'] = (mean_background,
                                          '[counts] Sigma clipped mean of frame background')

                median_background = np.median(bkg.back())
                image.header['L1MEDIAN'] = (median_background,
                                            '[counts] Median of frame background')

                std_background = stats.robust_standard_deviation(bkg.back())
                image.header['L1SIGMA'] = (std_background,
                                           '[counts] Robust std dev of frame background')

                # Save some image statistics to the header
                good_objects = catalog['flag'] == 0

                seeing = np.median(catalog['fwhm'][good_objects]) * image.pixel_scale
                image.header['L1FWHM'] = (seeing, '[arcsec] Frame FWHM in arcsec')

                mean_ellipticity = stats.sigma_clipped_mean(sources['ellipticity'][good_objects],
                                                            3.0)
                image.header['L1ELLIP'] = (mean_ellipticity, 'Mean image ellipticity (1-B/A)')

                mean_position_angle = stats.sigma_clipped_mean(sources['theta'][good_objects], 3.0)
                image.header['L1ELLIPA'] = (mean_position_angle,
                                            '[deg] PA of mean image ellipticity')

                logging_tags = {key: float(image.header[key]) for key in ['L1MEAN', 'L1MEDIAN', 'L1SIGMA',
                                                                          'L1FWHM', 'L1ELLIP', 'L1ELLIPA']}

                logger.info('Extracted sources', image=image, extra_tags=logging_tags)
                # adding catalog (a data table) to the appropriate images attribute.
                image.data_tables['catalog'] = DataTable(data_table=catalog, name='CAT')
            except Exception:
                logger.error(logs.format_exception(), image=image)
        return images
