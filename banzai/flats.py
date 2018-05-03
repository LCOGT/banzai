from __future__ import absolute_import, division, print_function, unicode_literals

import numpy as np
import os.path

from banzai.utils import stats, fits_utils
from banzai.stages import CalibrationMaker, ApplyCalibration, Stage
from banzai.images import Image
from banzai import logs

__author__ = 'cmccully'


class FlatNormalizer(Stage):
    def __init__(self, pipeline_context):
        super(FlatNormalizer, self).__init__(pipeline_context)

    @property
    def group_by_keywords(self):
        return None

    def do_stage(self, images):
        for image in images:
            quarter_nx = image.nx // 4
            quarter_ny = image.ny // 4
            # Get the sigma clipped mean of the central 25% of the image
            flat_normalization = stats.sigma_clipped_mean(image.data[quarter_ny: -quarter_ny,
                                                                     quarter_nx: -quarter_nx], 3.5)
            image.data /= flat_normalization
            image.header['FLATLVL'] = flat_normalization
            logging_tags = logs.image_config_to_tags(image, self.group_by_keywords)
            logs.add_tag(logging_tags, 'filename', os.path.basename(image.filename))
            logs.add_tag(logging_tags, 'flat_normalization', flat_normalization)
            self.logger.info('Calculate flat normalization', extra=logging_tags)

        return images


class FlatMaker(CalibrationMaker):
    def __init__(self, pipeline_context):
        super(FlatMaker, self).__init__(pipeline_context)

    @property
    def calibration_type(self):
        return 'skyflat'

    @property
    def group_by_keywords(self):
        return ['ccdsum', 'filter']

    @property
    def min_images(self):
        return 5

    def make_master_calibration_frame(self, images, image_config, logging_tags):
        flat_data = np.zeros((images[0].ny, images[0].nx, len(images)), dtype=np.float32)
        flat_mask = np.zeros((images[0].ny, images[0].nx, len(images)), dtype=np.uint8)

        master_flat_filename = self.get_calibration_filename(images[0])
        logs.add_tag(logging_tags, 'master_flat', os.path.basename(master_flat_filename))
        for i, image in enumerate(images):

            flat_data[:, :, i] = image.data[:, :]
            flat_mask[:, :, i] = image.bpm[:, :]

        master_flat = stats.sigma_clipped_mean(flat_data, 3.0, axis=2, mask=flat_mask, fill_value=1.0, inplace=True)

        master_bpm = np.array(master_flat == 1.0, dtype=np.uint8)

        master_bpm = np.logical_and(master_bpm, master_flat < 0.2)

        master_flat[master_flat < 0.2] = 1.0

        master_flat_header = fits_utils.create_master_calibration_header(images)

        master_flat_image = Image(self.pipeline_context, data=master_flat,
                                  header=master_flat_header)
        master_flat_image.filename = master_flat_filename
        master_flat_image.bpm = master_bpm

        logs.pop_tag(logging_tags, 'master_flat')
        logs.add_tag(logging_tags, 'filename', os.path.basename(master_flat_image.filename))
        self.logger.info('Created master flat', extra=logging_tags)

        return [master_flat_image]


class FlatDivider(ApplyCalibration):
    def __init__(self, pipeline_context):

        super(FlatDivider, self).__init__(pipeline_context)

    @property
    def group_by_keywords(self):
        return ['ccdsum', 'filter']

    @property
    def calibration_type(self):
        return 'skyflat'

    def apply_master_calibration(self, images, master_calibration_image, logging_tags):

        master_flat_filename = master_calibration_image.filename
        master_flat_data = master_calibration_image.data
        logs.add_tag(logging_tags, 'master_flat',
                     os.path.basename(master_calibration_image.filename))
        for image in images:
            logs.add_tag(logging_tags, 'filename', os.path.basename(image.filename))
            self.logger.info('Flattening image', extra=logging_tags)
            image.data /= master_flat_data
            image.bpm |= master_calibration_image.bpm
            master_flat_filename = os.path.basename(master_flat_filename)
            image.header['L1IDFLAT'] = (master_flat_filename, 'ID of flat frame used')
            image.header['L1STATFL'] = (1, 'Status flag for flat field correction')

        return images


class FlatComparer(ApplyCalibration):
    # In a 16 megapixel image, this should flag 0 or 1 pixels statistically, much much less than 5% of the image
    SIGNAL_TO_NOISE_THRESHOLD = 6.0
    ACCEPTABLE_PIXEL_FRACTION = 0.05

    def __init__(self, pipeline_context):
        super(FlatComparer, self).__init__(pipeline_context)

    @property
    def group_by_keywords(self):
        return ['ccdsum', 'filter']

    @property
    def calibration_type(self):
        return 'flat'

    def on_missing_master_calibration(self, logging_tags):
        self.logger.warning('No master Dark frame exists. Assuming these images are ok.', logging_tags)

    def apply_master_calibration(self, images, master_calibration_image, logging_tags):
        # Short circuit
        if master_calibration_image.data is None:
            return images

        images_to_reject = []

        for image in images:
            # We assume the images have already been normalized before this stage is run.
            bad_pixel_fraction = np.abs(image.data - master_calibration_image.data)
            # Estimate the noise of the image
            flat_normalization = float(image.header['FLATLVL'])
            noise = (image.readnoise ** 2.0 + image.data * flat_normalization) ** 0.5
            noise /= flat_normalization
            bad_pixel_fraction /= noise
            bad_pixel_fraction = bad_pixel_fraction >= self.SIGNAL_TO_NOISE_THRESHOLD
            bad_pixel_fraction = bad_pixel_fraction.sum() / float(bad_pixel_fraction.size)

            qc_results = {'FLAT_MASTER_DIFF_FRAC': bad_pixel_fraction,
                          'FLAT_SN_THRESHOLD': self.SIGNAL_TO_NOISE_THRESHOLD,
                          'FLAT_ACCEPTABLE_PIXEL_FRACTION': self.ACCEPTABLE_PIXEL_FRACTION}
            for qc_check, qc_result in qc_results.items():
                logs.add_tag(logging_tags, qc_check, qc_result)

            if bad_pixel_fraction > self.ACCEPTABLE_PIXEL_FRACTION:
                # Reject the image and log an error
                images_to_reject.append(image)
                qc_results['REJECTED'] = True
                logs.add_tag(logging_tags, 'REJECTED', True)
                self.logger.error('Rejecting flat image because it deviates too much from the previous master',
                                  extra=logging_tags)
            else:
                qc_results['REJECTED'] = False

            self.save_qc_results(qc_results, image)

        for image_to_reject in images_to_reject:
            images.remove(image_to_reject)
        return images
