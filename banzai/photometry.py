from urllib.parse import urljoin

import numpy as np
from astropy.table import Table
from requests import HTTPError

from banzai.utils import stats, array_utils
from banzai.utils.photometry_utils import get_reference_sources, match_catalogs, to_magnitude, fit_photometry
from banzai.stages import Stage
from banzai.data import DataTable
from banzai import logs

from photutils.background import Background2D
from skimage import measure
from photutils.segmentation import make_2dgaussian_kernel, detect_sources, deblend_sources, SourceCatalog
from astropy.convolution import convolve
from astropy.convolution.kernels import CustomKernel


logger = logs.get_logger()


def radius_of_contour(contour, source):
    x = contour[:, 1]
    y = contour[:, 0]
    x_center = (source.bbox_xmax - source.bbox_xmin + 1) / 2.0 - 0.5
    y_center = (source.bbox_ymax - source.bbox_ymin + 1) / 2.0 - 0.5

    return np.percentile(np.sqrt((x - x_center)**2.0 + (y - y_center) ** 2.0), 90)


def flag_sources(sources, source_labels, segmentation_map, mask, flag, mask_value):
    affected_sources = np.unique(segmentation_map.data[mask == mask_value])
    sources['flag'][np.in1d(source_labels, affected_sources)] |= flag


def flag_deblended(sources, catalog, segmentation_map, deblended_seg_map, flag_value=2):
    # By default deblending appends labels instead of reassigning them so we can just use the
    # extras in the deblended map
    deblended_sources = np.unique(deblended_seg_map.data[deblended_seg_map > np.max(segmentation_map)])
    # Get the sources that were originally blended
    original_blends = np.unique(segmentation_map.data[deblended_seg_map > np.max(segmentation_map)])
    deblended_sources = np.hstack([deblended_sources, original_blends])
    sources['flag'][np.in1d(catalog.labels, deblended_sources)] |= flag_value


def flag_edge_sources(image, sources, flag_value=8):
    ny, nx = image.shape
    # Check 4 points on the kron aperture, one on each side of the major and minor axis
    minor_xmin = sources['x'] - sources['b'] * sources['kronrad'] * np.sin(np.deg2rad(sources['theta']))
    minor_xmax = sources['x'] + sources['b'] * sources['kronrad'] * np.sin(np.deg2rad(sources['theta']))
    minor_ymin = sources['y'] - sources['b'] * sources['kronrad'] * np.cos(np.deg2rad(sources['theta']))
    minor_ymax = sources['y'] + sources['b'] * sources['kronrad'] * np.cos(np.deg2rad(sources['theta']))
    major_ymin = sources['y'] - sources['a'] * sources['kronrad'] * np.sin(np.deg2rad(sources['theta']))
    major_ymax = sources['y'] + sources['a'] * sources['kronrad'] * np.sin(np.deg2rad(sources['theta']))
    major_xmin = sources['x'] - sources['a'] * sources['kronrad'] * np.cos(np.deg2rad(sources['theta']))
    major_xmax = sources['x'] + sources['a'] * sources['kronrad'] * np.cos(np.deg2rad(sources['theta']))

    # Note we are already 1 indexed here
    sources_off = np.logical_or(minor_xmin < 1, major_xmin < 1)
    sources_off = np.logical_or(sources_off, minor_ymin < 1)
    sources_off = np.logical_or(sources_off, major_ymin < 1)
    sources_off = np.logical_or(sources_off, minor_xmax > nx)
    sources_off = np.logical_or(sources_off, major_xmax > nx)
    sources_off = np.logical_or(sources_off, minor_ymax > ny)
    sources_off = np.logical_or(sources_off, major_ymax > ny)
    sources[sources_off]['flag'] |= flag_value


class SourceDetector(Stage):
    threshold = 2.5
    min_area = 9

    def __init__(self, runtime_context):
        super(SourceDetector, self).__init__(runtime_context)

    def do_stage(self, image):
        try:
            data = image.data.copy()
            error = image.uncertainty

            # From what I can piece together, the background estimator makes a low resolution mesh set by box size
            # (32, 32) here and then applies a filter to the low resolution image. The filter size is 3x3 here.
            # The defaults we use here are a mesh creator is from source extractor which is a mode estimator.
            # The default filter that works on the mesh image is a median filter.
            bkg = Background2D(data, (32, 32), filter_size=(3, 3))
            data -= bkg.background

            # Convolve the image with a 2D Guassian, but with the normalization SEP uses as
            # that is correct.
            # The default kernel used by Source Extractor is [[1,2,1], [2,4,2], [1,2,1]]
            # The kernel corresponds to fwhm = 1.9 which we adopt here
            kernel = make_2dgaussian_kernel(1.9, size=3)
            convolved_data = convolve(data / (error * error), kernel)

            # We include the correct match filter normalization here that is not included in
            # vanilla source extractor
            kernel_squared = CustomKernel(kernel.array * kernel.array)
            normalization = np.sqrt(convolve(1 / (error * error), kernel_squared))
            convolved_data /= normalization
            logger.info('Running image segmentation', image=image)
            # Do an initial source detection
            segmentation_map = detect_sources(convolved_data, self.threshold, npixels=self.min_area)

            logger.info('Deblending sources', image=image)
            # Note that nlevels here is DEBLEND_NTHRESH in source extractor which is 32 by default
            deblended_seg_map = deblend_sources(convolved_data, segmentation_map,
                                                npixels=self.min_area, nlevels=32,
                                                contrast=0.005, progress_bar=False,
                                                nproc=1, mode='sinh')
            logger.info('Finished deblending. Estimat', image=image)
            # Convert the segmentation map to a source catalog
            catalog = SourceCatalog(data, deblended_seg_map, convolved_data=convolved_data, error=error,
                                    background=bkg.background)

            sources = Table({'x': catalog.xcentroid + 1.0, 'y': catalog.ycentroid + 1.0,
                             'xwin': catalog.xcentroid_win + 1.0, 'ywin': catalog.ycentroid_win + 1.0,
                             'xpeak': catalog.maxval_xindex + 1, 'ypeak': catalog.maxval_yindex + 1,
                             'peak': catalog.max_value,
                             'a': catalog.semimajor_sigma.value, 'b': catalog.semiminor_sigma.value,
                             'theta': catalog.orientation.to('deg').value, 'ellipticity': catalog.ellipticity.value,
                             'kronrad': catalog.kron_radius.value,
                             'flux': catalog.kron_flux, 'fluxerr': catalog.kron_fluxerr,
                             'x2': catalog.covar_sigx2.value, 'y2': catalog.covar_sigy2.value,
                             'xy': catalog.covar_sigxy.value,
                             'background': catalog.background_mean})

            for r in range(1, 7):
                radius_arcsec = r / image.pixel_scale
                sources[f'fluxaper{r}'], sources[f'fluxerr{r}'] = catalog.circular_photometry(radius_arcsec)

            for r in [0.25, 0.5, 0.75]:
                sources['fluxrad' + f'{r:.2f}'.lstrip("0.")] = catalog.fluxfrac_radius(r)

            sources['flag'] = 0

            # Flag = 1 for sources with bad pixels
            flag_sources(sources, catalog.labels, deblended_seg_map, image.mask, flag=1, mask_value=1)
            # Flag = 2 for sources that are deblended
            flag_deblended(sources, catalog, segmentation_map, deblended_seg_map, flag_value=2)
            # Flag = 4 for sources that have saturated pixels
            flag_sources(sources, catalog.labels, deblended_seg_map, image.mask, flag=4, mask_value=2)
            # Flag = 8 if kron aperture falls off the image
            flag_edge_sources(image, sources, flag_pixel=8)
            # Flag = 16 if source has cosmic ray pixels
            flag_sources(sources, catalog.labels, deblended_seg_map, image.mask, flag=16, mask_value=8)

            sources = array_utils.prune_nans_from_table(sources)

            # Cut individual bright pixels. Often cosmic rays
            sources = sources[sources['fluxrad50'] > 0.5]

            # Calculate the FWHMs of the stars:
            sources['fwhm'] = np.nan
            sources['fwtm'] = np.nan
            # Here we estimate contours
            for source, row in zip(sources, catalog):
                if source['flag'] == 0:
                    for ratio, keyword in zip([0.5, 0.1], ['fwhm', 'fwtm']):
                        contours = measure.find_contours(data[row.bbox_ymin: row.bbox_ymax + 1,
                                                         row.bbox_xmin: row.bbox_xmax + 1],
                                                         ratio * source['peak'])
                        if contours:
                            # If there are multiple contours like a donut might have take the outer
                            contour_radii = [radius_of_contour(contour, row) for contour in contours]
                            source[keyword] = 2.0 * np.nanmax(contour_radii)

            # Add the units and description to the catalogs
            sources['x'].unit = 'pixel'
            sources['x'].description = 'X coordinate of the object'
            sources['y'].unit = 'pixel'
            sources['y'].description = 'Y coordinate of the object'
            sources['xwin'].unit = 'pixel'
            sources['xwin'].description = 'Windowed X coordinate of the object'
            sources['ywin'].unit = 'pixel'
            sources['ywin'].description = 'Windowed Y coordinate of the object'
            sources['xpeak'].unit = 'pixel'
            sources['xpeak'].description = 'X coordinate of the peak'
            sources['ypeak'].unit = 'pixel'
            sources['ypeak'].description = 'Windowed Y coordinate of the peak'
            sources['flux'].unit = 'count'
            sources['flux'].description = 'Flux within a Kron-like elliptical aperture'
            sources['fluxerr'].unit = 'count'
            sources['fluxerr'].description = 'Error on the flux within Kron aperture'
            sources['peak'].unit = 'count'
            sources['peak'].description = 'Peak flux (flux at xpeak, ypeak)'
            for diameter in [1, 2, 3, 4, 5, 6]:
                sources['fluxaper{0}'.format(diameter)].unit = 'count'
                sources['fluxaper{0}'.format(diameter)].description = 'Flux from fixed circular aperture: {0}" diameter'.format(diameter)
                sources['fluxerr{0}'.format(diameter)].unit = 'count'
                sources['fluxerr{0}'.format(diameter)].description = 'Error on Flux from circular aperture: {0}"'.format(diameter)

            sources['background'].unit = 'count'
            sources['background'].description = 'Average background value in the aperture'
            sources['fwhm'].unit = 'pixel'
            sources['fwhm'].description = 'FWHM of the object'
            sources['fwtm'].unit = 'pixel'
            sources['fwtm'].description = 'Full-Width Tenth Maximum'
            sources['a'].unit = 'pixel'
            sources['a'].description = 'Semi-major axis of the object'
            sources['b'].unit = 'pixel'
            sources['b'].description = 'Semi-minor axis of the object'
            sources['theta'].unit = 'degree'
            sources['theta'].description = 'Position angle of the object'
            sources['kronrad'].unit = 'pixel'
            sources['kronrad'].description = 'Kron radius used for extraction'
            sources['ellipticity'].description = 'Ellipticity'
            sources['fluxrad25'].unit = 'pixel'
            sources['fluxrad25'].description = 'Radius containing 25% of the flux'
            sources['fluxrad50'].unit = 'pixel'
            sources['fluxrad50'].description = 'Radius containing 50% of the flux'
            sources['fluxrad75'].unit = 'pixel'
            sources['fluxrad75'].description = 'Radius containing 75% of the flux'
            sources['x2'].unit = 'pixel^2'
            sources['x2'].description = 'Variance on X coordinate of the object'
            sources['y2'].unit = 'pixel^2'
            sources['y2'].description = 'Variance on Y coordinate of the object'
            sources['xy'].unit = 'pixel^2'
            sources['xy'].description = 'XY covariance of the object'
            sources['flag'].description = 'Bit mask of extraction/photometry flags'

            sources.sort('flux')
            sources.reverse()

            # Save some background statistics in the header
            mean_background = stats.sigma_clipped_mean(bkg.background, 5.0)
            image.meta['L1MEAN'] = (mean_background,
                                    '[counts] Sigma clipped mean of frame background')

            median_background = np.median(bkg.background)
            image.meta['L1MEDIAN'] = (median_background,
                                      '[counts] Median of frame background')

            std_background = stats.robust_standard_deviation(bkg.background)
            image.meta['L1SIGMA'] = (std_background,
                                     '[counts] Robust std dev of frame background')

            # Save some image statistics to the header
            good_objects = sources['flag'] == 0
            for quantity in ['fwhm', 'ellipticity', 'theta']:
                good_objects = np.logical_and(good_objects, np.logical_not(np.isnan(sources[quantity])))
            if good_objects.sum() == 0:
                image.meta['L1FWHM'] = ('NaN', '[arcsec] Frame FWHM in arcsec')
                image.meta['L1FWTM'] = ('NaN', 'Ratio of FWHM to Full-Width Tenth Max')

                image.meta['L1ELLIP'] = ('NaN', 'Mean image ellipticity (1-B/A)')
                image.meta['L1ELLIPA'] = ('NaN', '[deg] PA of mean image ellipticity')
            else:
                seeing = np.nanmedian(sources['fwhm'][good_objects]) * image.pixel_scale
                image.meta['L1FWHM'] = (seeing, '[arcsec] Frame FWHM in arcsec')
                image.meta['L1FWTM'] = (np.nanmedian(sources['fwtm'][good_objects] / sources['fwhm'][good_objects]),
                                        'Ratio of FWHM to Full-Width Tenth Max')

                mean_ellipticity = stats.sigma_clipped_mean(sources['ellipticity'][good_objects], 3.0)
                image.meta['L1ELLIP'] = (mean_ellipticity, 'Mean image ellipticity (1-B/A)')

                mean_position_angle = stats.sigma_clipped_mean(sources['theta'][good_objects], 3.0)
                image.meta['L1ELLIPA'] = (mean_position_angle, '[deg] PA of mean image ellipticity')

            logging_tags = {key: float(image.meta[key]) for key in ['L1MEAN', 'L1MEDIAN', 'L1SIGMA',
                                                                    'L1FWHM', 'L1ELLIP', 'L1ELLIPA']}

            logger.info('Extracted sources', image=image, extra_tags=logging_tags)
            # adding catalog (a data table) to the appropriate images attribute.
            image.add_or_update(DataTable(sources, name='CAT'))
        except Exception:
            logger.error(logs.format_exception(), image=image)
        return image


class PhotometricCalibrator(Stage):
    color_to_fit = {'gp': 'g-r', 'rp': 'r-i', 'ip': 'r-i', 'zs': 'i-z'}

    def __init__(self, runtime_context):
        super(PhotometricCalibrator, self).__init__(runtime_context)

    def do_stage(self, image):
        if image.filter not in ['gp', 'rp', 'ip', 'zs']:
            return image

        if image['CAT'] is None:
            logger.warning("Not photometrically calibrating image because no catalog exists", image=image)
            return image

        if image.meta.get('WCSERR', 1) > 0:
            logger.warning("Not photometrically calibrating image because WCS solution failed", image=image)
            return image

        try:
            # Get the sources in the frame
            reference_catalog = get_reference_sources(image.meta,
                                                      urljoin(self.runtime_context.REFERENCE_CATALOG_URL, '/image'),
                                                      nx=image.shape[1], ny=image.shape[0])
        except HTTPError as e:
            logger.error(f'Error retrieving photometric reference catalog: {e}', image=image)
            return image

        # Match the catalog to the detected sources
        good_sources = np.logical_and(image['CAT'].data['flag'] == 0, image['CAT'].data['flux'] > 0.0)
        matched_catalog = match_catalogs(image['CAT'].data[good_sources], reference_catalog)

        if len(matched_catalog) == 0:
            logger.error('No matching sources found. Skipping zeropoint determination', image=image)
            return image
        # catalog_mag = instrumental_mag + zeropoint + color_coefficient * color
        # Fit the zeropoint and color_coefficient rejecting outliers
        # Note the zero index here in the filter name is because we only store teh first letter of the filter name
        try:
            zeropoint, zeropoint_error, color_coefficient, color_coefficient_error = \
                fit_photometry(matched_catalog, image.filter[0], self.color_to_fit[image.filter], image.exptime)
        except:
            logger.error(f"Error fitting photometry: {logs.format_exception()}", image=image)
            return image

        # Save the zeropoint, color coefficient and errors to header
        image.meta['L1ZP'] = zeropoint, "Instrumental zeropoint [mag]"
        image.meta['L1ZPERR'] = zeropoint_error, "Error on Instrumental zeropoint [mag]"
        image.meta['L1COLORU'] = self.color_to_fit[image.filter], "Color used for calibration"
        image.meta['L1COLOR'] = color_coefficient, "Color coefficient [mag]"
        image.meta['L1COLERR'] = color_coefficient_error, "Error on color coefficient [mag]"
        # Calculate the mag of each of the items in the catalog (without the color term) saving them
        image['CAT'].data['mag'], image['CAT'].data['magerr'] = to_magnitude(image['CAT'].data['flux'], image['CAT'].data['fluxerr'],
                                                                             zeropoint, image.exptime)
        return image
