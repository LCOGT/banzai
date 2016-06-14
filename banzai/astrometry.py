from __future__ import absolute_import, print_function, division
from banzai.stages import Stage
from banzai import logs
from banzai.utils import image_utils
import os, subprocess, shlex
from astropy.io import fits
import tempfile
from astropy.wcs import WCS

__author__ = 'cmccully'


class WCSSolver(Stage):
    cmd = 'solve-field --crpix-center --no-verify --no-fits2fits --no-tweak ' \
          ' --radius 2.0 --ra {ra} --dec {dec} --guess-scale ' \
          '--scale-units arcsecperpix --scale-low {scale_low} --scale-high {scale_high} ' \
          '--no-plots -N none --use-sextractor --no-remove-lines ' \
          '--code-tolerance 0.003 --pixel-error 20 -d 1-200 ' \
          '--solved none --match none --rdls none --wcs {wcs_name} --corr none --overwrite ' \
          '-X X -Y Y -s FLUX --width {nx} --height {ny} {catalog_name}'

    def __init__(self, pipeline_context):
        super(WCSSolver, self).__init__(pipeline_context)

    @property
    def group_by_keywords(self):
        return None

    def do_stage(self, images):

        for i, image in enumerate(images):
            logging_tags = logs.image_config_to_tags(image, self.group_by_keywords)

            # Save the catalog to a temporary file
            filename = os.path.basename(image.filename)
            logs.add_tag(logging_tags, 'filename', filename)

            with tempfile.TemporaryDirectory() as tmpdirname:
                catalog_name = os.path.join(tmpdirname, filename.replace('.fits', '.cat.fits'))
                try:
                    image.write_catalog(catalog_name, nsources=40)
                except image_utils.MissingCatalogException:
                    image.header['WCSERR'] = (4, 'Error status of WCS fit. 0 for no error')
                    self.logger.error('No source catalog. Not attempting WCS solution',
                                      extra=logging_tags)
                    continue

                # Run astrometry.net
                wcs_name = os.path.join(tmpdirname, filename.replace('.fits', '.wcs.fits'))
                command = self.cmd.format(ra=image.ra, dec=image.dec, scale_low=0.9*image.pixel_scale,
                                          scale_high=1.1*image.pixel_scale, wcs_name=wcs_name,
                                          catalog_name=catalog_name, nx=image.nx, ny=image.ny)

                console_output = subprocess.check_output(shlex.split(command))
                self.logger.debug(console_output, extra=logging_tags)

                if os.path.exists(wcs_name):
                    # Copy the WCS keywords into original image
                    new_header = fits.getheader(wcs_name)

                    header_keywords_to_update = ['CTYPE1', 'CTYPE2', 'CRPIX1', 'CRPIX2', 'CRVAL1',
                                                 'CRVAL2', 'CD1_1', 'CD1_2', 'CD2_1', 'CD2_2']
                    for keyword in header_keywords_to_update:
                        image.header[keyword] = new_header[keyword]

                    image.header['WCSERR'] = (0, 'Error status of WCS fit. 0 for no error')

                    # Clean up wcs file
                    os.remove(wcs_name)

                    # Add the RA and Dec values to the catalog
                    add_ra_dec_to_catalog(image)
                else:
                    image.header['WCSERR'] = (4, 'Error status of WCS fit. 0 for no error')

            logs.add_tag(logging_tags, 'WCSERR', image.header['WCSERR'])
            self.logger.info('Attempted WCS Solve', extra=logging_tags)
        return images


def add_ra_dec_to_catalog(image):
    image_wcs = WCS(image.header)
    ras, decs = image_wcs.all_pix2world(image.catalog['x'], image.catalog['y'], 1)
    image.catalog['ra'] = ras
    image.catalog['dec'] = decs
    image.catalog['ra'].unit = 'degrees'
    image.catalog['dec'].unit = 'degrees'
    image.catalog['ra'].description = 'Right Ascension'
    image.catalog['dec'].description = 'Declination'
