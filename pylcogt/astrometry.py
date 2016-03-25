from __future__ import absolute_import, print_function, division
from pylcogt.stages import Stage
from . import dbs, logs
import os, subprocess, shlex
from astropy.io import fits

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

            catalog_name = filename.replace('.fits', '.cat.fits')
            image.write_catalog(catalog_name, nsources=40)
            # Run astrometry.net
            wcs_name = filename.replace('.fits', '.wcs.fits')
            command = self.cmd.format(ra=image.ra, dec=image.dec, scale_low=0.9*image.pixel_scale,
                                      scale_high=1.1*image.pixel_scale, wcs_name=wcs_name,
                                      catalog_name=catalog_name, nx=image.nx, ny=image.ny)

            console_output = subprocess.check_output(shlex.split(command))
            self.logger.debug(console_output, extra=logging_tags)

            # Cleanup temp files created by astrometry.net
            basename = catalog_name[:-5]  # Split off the .fits from the image filename
            # Remove the extra temporary files
            if os.path.exists(basename + '.axy'):
                os.remove(basename + '.axy')
            if os.path.exists(basename + '-indx.xyls'):
                os.remove(basename + '-indx.xyls')

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
            else:
                image.header['WCSERR'] = (4, 'Error status of WCS fit. 0 for no error')

            # Clean up the catalog file
            os.remove(catalog_name)

            logs.add_tag(logging_tags, 'WCSERR', image.header['WCSERR'])
            self.logger.info('Attempted WCS Solve', extra=logging_tags)
        return images
