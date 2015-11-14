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
                '--no-plots -N {output_name}  --downsample 4 --use-sextractor ' \
                '--solved none --match none --rdls none --wcs none --corr none --overwrite {image_name}'
    def __init__(self, raw_path, processed_path, initial_query, cpu_pool):

        astrometry_query = initial_query & (dbs.Image.obstype=='EXPOSE')

        super(Astrometry, self).__init__(self.solve_wcs, processed_path=processed_path,
                                   initial_query=astrometry_query, logger_name='Astrometry', cal_type='wcs',
                                         previous_stage_done=dbs.Image.flat_done, previous_suffix_number='25',
                                         image_suffix_number='90', cpu_pool=cpu_pool)
        self.log_message = 'Solving for the WCS of images from {instrument} at {site} on {epoch}.'
        self.group_by = None

    def get_output_images(self, telescope, epoch):
        image_sets, image_configs = self.select_input_images(telescope, epoch)
        return [image for image_set in image_sets for image in image_set]

    def solve_wcs(self, image_files, output_files, clobber=True):
        logger = logs.get_logger('Astrometry')
        db_session = dbs.get_session()
        for i, image in enumerate(image_files):
            logger.info('Solving WCS for {filename}'.format(filename=image.filename))
            # Run astrometry.net

            image_file = os.path.join(image.filepath, image.filename)
            image_file += self.previous_image_suffix + '.fits'
            output_name = image_file.replace('.fits','.wcs.fits')
            command = self.cmd.format(ra=image.ra, dec=image.dec, scale_low=0.9*image.pixel_scale,
                                      scale_high=1.1*image.pixel_scale, output_name=output_name,
                                      image_name=image_file)

            subprocess.check_output(shlex.split(command))

            basename = image_file[:-5] # Split off the .fits from the image filename
            # Remove the extra temporary files
            if os.path.exists(basename + '.axy'):
                os.remove(basename + '.axy')
            if os.path.exists(basename + '-indx.xyls'):
                os.remove(basename + '-indx.xyls')


            image_hdu = fits.open(image_file)

            if os.path.exists(output_name):
                # Copy the WCS keywords into original image
                new_header = fits.getheader(output_name)

                header_keywords_to_update = ['CTYPE1', 'CTYPE2', 'CRPIX1', 'CRPIX2', 'CRVAL1', 'CRVAL2',
                                             'CD1_1', 'CD1_2', 'CD2_1', 'CD2_2']
                for keyword in header_keywords_to_update:
                    image_hdu[0].header[keyword] = new_header[keyword]

                image_hdu[0].header['WCSERR'] = 0

            else:
                image_hdu[0].header['WCSERR'] = 4

            output_filename = os.path.join(output_files[i].filepath, output_files[i].filename)
            output_filename += self.image_suffix_number + '.fits'
            image_hdu.writeto(output_filename, clobber=clobber)

            image.wcs_done = True
            db_session.add(image)
            db_session.commit()
        db_session.close()