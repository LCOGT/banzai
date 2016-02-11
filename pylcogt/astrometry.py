from __future__ import absolute_import, print_function, division
from .stages import Stage
from . import dbs, logs
import os, subprocess, shlex
from astropy.io import fits

__author__ = 'cmccully'

class Astrometry(Stage):
    cmd = 'solve-field --crpix-center --no-verify --no-fits2fits --no-tweak ' \
          ' --radius 2.0 --ra {ra} --dec {dec} --guess-scale ' \
          '--scale-units arcsecperpix --scale-low {scale_low} --scale-high {scale_high} ' \
          '--no-plots -N {output_name}   --use-sextractor ' \
          '--code-tolerance 0.003 --pixel-error 20 -d 1-200 ' \
          '--solved none --match none --rdls none --wcs none --corr none --overwrite {image_name}'

    def __init__(self, pipeline_context):
        super(Astrometry, self).__init__(pipeline_context)

    @property
    def group_by_keywords(self):
        return None

    def do_stage(self, images):

        for i, image in enumerate(images):
            self.logger.info('Solving WCS for {filename}'.format(filename=image.filename))

            # Run astrometry.net
            output_name = image_file.replace('.fits', '.wcs.fits')
            command = self.cmd.format(ra=image.ra, dec=image.dec, scale_low=0.9*image.pixel_scale,
                                      scale_high=1.1*image.pixel_scale, output_name=output_name,
                                      image_name=image_file)

            subprocess.check_output(shlex.split(command))

            # Cleanup temp files created by astrometry.net
            basename = image_file[:-5] # Split off the .fits from the image filename
            # Remove the extra temporary files
            if os.path.exists(basename + '.axy'):
                os.remove(basename + '.axy')
            if os.path.exists(basename + '-indx.xyls'):
                os.remove(basename + '-indx.xyls')

            if os.path.exists(output_name):
                # Copy the WCS keywords into original image
                new_header = fits.getheader(output_name)

                header_keywords_to_update = ['CTYPE1', 'CTYPE2', 'CRPIX1', 'CRPIX2', 'CRVAL1', 'CRVAL2',
                                             'CD1_1', 'CD1_2', 'CD2_1', 'CD2_2']
                for keyword in header_keywords_to_update:
                    image_hdu[0].header[keyword] = new_header[keyword]

                image.header['WCSERR'] = 0

            else:
                image.header['WCSERR'] = 4

        return images