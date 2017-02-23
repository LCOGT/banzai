from __future__ import absolute_import, division, print_function, unicode_literals
import os
from glob import glob

from astropy.io import fits
import numpy as np

from banzai import dbs
from banzai import logs
from banzai.utils import file_utils
from banzai.utils import fits_utils

logger = logs.get_logger(__name__)


def select_images(image_list, image_types):
    images = []
    for filename in image_list:
        try:
            obstype = None
            hdu_list = fits.open(filename)
            for hdu in hdu_list:
                if 'OBSTYPE' in hdu.header.keys():
                    obstype = hdu.header['OBSTYPE']

            if obstype in image_types:
                images.append(filename)
            else:
                logger.error('Unable to get OBSTYPE', extra={'tags': {'filename': filename}})
        except Exception as e:
            logger.error('Exception getting OBSTYPE: {e}'.format(e=e),
                         extra={'tags': {'filename': filename}})
    return images


def make_image_list(pipeline_context):

    search_path = os.path.join(pipeline_context.raw_path)

    if pipeline_context.filename is None:
        # return the list of file and a dummy image configuration
        fits_files = glob(search_path + '/*.fits')
        fz_files = glob(search_path + '/*.fits.fz')

        fz_files_to_remove = []
        for i, f in enumerate(fz_files):
            if f[:-3] in fits_files:
                fz_files_to_remove.append(f)

        for f in fz_files_to_remove:
            fz_files.remove(f)
        image_list = fits_files + fz_files

    else:
        image_list = glob(os.path.join(pipeline_context.raw_path, pipeline_context.filename))
    return image_list


def check_image_homogeneity(images):
    for attribute in ['nx', 'ny', 'ccdsum', 'epoch', 'site', 'instrument']:
        if len(set([getattr(image, attribute) for image in images])) > 1:
            raise InhomogeneousSetException('Images have different {0}s'.format(attribute))
    return images[0]


class InhomogeneousSetException(Exception):
    pass


class MissingCatalogException(Exception):
    pass


def save_images(pipeline_context, images, master_calibration=False):
    output_files = []
    for image in images:
        output_directory = file_utils.make_output_directory(pipeline_context, image)
        if not master_calibration:
            image.filename = image.filename.replace('00.fits',
                                                    '{:02d}.fits'.format(int(pipeline_context.rlevel)))

        image_filename = os.path.basename(image.filename)
        filepath = os.path.join(output_directory, image_filename)
        output_files.append(filepath)
        image.writeto(filepath, pipeline_context.fpack)
        if pipeline_context.fpack:
            image_filename += '.fz'
            filepath += '.fz'
        if master_calibration:
            dbs.save_calibration_info(image.obstype, filepath, image,
                                      db_address=pipeline_context.db_address)

        if pipeline_context.post_to_archive:
            logger.info('Posting {filename} to the archive'.format(filename=image_filename))
            try:
                file_utils.post_to_archive_queue(filepath)
            except Exception as e:
                logger.error("Could not post {0} to ingester.".format(filepath))
                logger.error(e)
                continue
    return output_files


def get_bpm(image, pipeline_context):
    bpm_filename = dbs.get_bpm(image.telescope_id, image.ccdsum,
                               db_address=pipeline_context.db_address)
    if bpm_filename is None:
        bpm = None
        image.header['L1IDMASK'] = ('', 'Id. of mask file used')
    else:
        bpm_hdu = fits.open(bpm_filename)
        bpm_sci_extensions = fits_utils.get_sci_extensions(bpm_hdu)
        if len(bpm_sci_extensions) > 1:
            extension_shape = bpm_sci_extensions[0].data.shape
            bpm_shape = (len(bpm_sci_extensions), extension_shape[0], extension_shape[1])
            bpm = np.zeros(bpm_shape, dtype=np.uint8)
            for i, sci_extension in enumerate(bpm_sci_extensions):
                bpm[i, :, :] = sci_extension.data[:, :]
        else:
            bpm = np.array(bpm_hdu[0].data, dtype=np.uint8)
        if bpm.shape != image.data.shape:
            tags = logs.image_config_to_tags(image, None)
            logs.add_tag(tags, 'filename', image.filename)
            logger.error('BPM shape mismatch', extra=tags)
            err_msg = 'BPM shape mismatch for {filename} ' \
                      '{site}/{instrument}'.format(filename=image.filename, site=image.site,
                                                   instrument=image.instrument)
            raise ValueError(err_msg)
        image.header['L1IDMASK'] = (os.path.basename(bpm_filename), 'Id. of mask file used')

    return bpm
