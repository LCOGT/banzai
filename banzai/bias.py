from __future__ import absolute_import, division, print_function, unicode_literals

import os.path

import numpy as np

from banzai.images import Image
from banzai import logs
from banzai.stages import CalibrationMaker, ApplyCalibration, Stage
from banzai.utils import stats, fits_utils
from banzai.qc.utils import save_qc_results


__author__ = 'cmccully'


class BiasMaker(CalibrationMaker):

    def __init__(self, pipeline_context):
        super(BiasMaker, self).__init__(pipeline_context)

    @property
    def group_by_keywords(self):
        return ['ccdsum']

    @property
    def calibration_type(self):
        return 'BIAS'

    @property
    def min_images(self):
        return 5

    def make_master_calibration_frame(self, images, image_config, logging_tags):

        bias_data = np.zeros((image_config.ny, image_config.nx, len(images)), dtype=np.float32)
        bias_mask = np.zeros((image_config.ny, image_config.nx, len(images)), dtype=np.uint8)
        bias_level_array = np.zeros(len(images), dtype=np.float32)

        master_bias_filename = self.get_calibration_filename(image_config)
        logs.add_tag(logging_tags, 'master_bias', os.path.basename(master_bias_filename))
        for i, image in enumerate(images):
            bias_level_array[i] = stats.sigma_clipped_mean(image.data, 3.5, mask=image.bpm)

            logs.add_tag(logging_tags, 'filename', os.path.basename(image.filename))
            logs.add_tag(logging_tags, 'BIASLVL', float(bias_level_array[i]))
            self.logger.debug('Calculating bias level', extra=logging_tags)
            # Subtract the bias level for each image
            bias_data[:, :, i] = image.data[:, :] - bias_level_array[i]
            bias_mask[:, :, i] = image.bpm[:, :]

        mean_bias_level = stats.sigma_clipped_mean(bias_level_array, 3.0)

        master_bias = stats.sigma_clipped_mean(bias_data, 3.0, axis=2, mask=bias_mask, inplace=True)

        del bias_data
        del bias_mask

        master_bpm = np.array(master_bias == 0.0, dtype=np.uint8)

        header = fits_utils.create_master_calibration_header(images)

        header['BIASLVL'] = (mean_bias_level, 'Mean bias level of master bias')
        master_bias_image = Image(self.pipeline_context, data=master_bias, header=header)
        master_bias_image.filename = master_bias_filename
        master_bias_image.bpm = master_bpm

        logs.pop_tag(logging_tags, 'master_bias')
        logs.add_tag(logging_tags, 'filename', os.path.basename(master_bias_image.filename))
        logs.add_tag(logging_tags, 'BIASLVL', mean_bias_level)
        self.logger.debug('Average bias level in ADU', extra=logging_tags)

        return [master_bias_image]


class BiasSubtractor(ApplyCalibration):
    def __init__(self, pipeline_context):
        super(BiasSubtractor, self).__init__(pipeline_context)

    @property
    def group_by_keywords(self):
        return ['ccdsum']

    @property
    def calibration_type(self):
        return 'bias'

    def apply_master_calibration(self, images, master_calibration_image, logging_tags):

        master_bias_data = master_calibration_image.data
        master_bias_level = float(master_calibration_image.header['BIASLVL'])

        logs.add_tag(logging_tags, 'bias', master_bias_level)
        logs.add_tag(logging_tags, 'master_bias',
                     os.path.basename(master_calibration_image.filename))

        for image in images:
            logs.add_tag(logging_tags, 'filename', os.path.basename(image.filename))

            image.subtract(master_bias_level)
            image.subtract(master_bias_data)

            image.bpm |= master_calibration_image.bpm
            image.header['BIASLVL'] = (master_bias_level, 'Mean bias level of master bias')

            master_bias_filename = os.path.basename(master_calibration_image.filename)
            image.header['L1IDBIAS'] = (master_bias_filename, 'ID of bias frame used')
            image.header['L1STATBI'] = (1, "Status flag for bias frame correction")
            logs.add_tag(logging_tags, 'BIASLVL', image.header['BIASLVL'])
            logs.add_tag(logging_tags, 'L1IDBIAS', image.header['L1IDBIAS'])

            self.logger.info('Subtracting bias', extra=logging_tags)
        return images


class OverscanSubtractor(Stage):
    def __init__(self, pipeline_context):
        super(OverscanSubtractor, self).__init__(pipeline_context)

    @property
    def group_by_keywords(self):
        return None

    def do_stage(self, images):

        for image in images:
            logging_tags = logs.image_config_to_tags(image, self.group_by_keywords)
            logs.add_tag(logging_tags, 'filename', os.path.basename(image.filename))

            # Subtract the overscan if it exists
            if len(image.data.shape) > 2:
                for i in range(image.data.shape[0]):
                    overscan_level = _subtract_overscan_3d(image, i)
                    logs.add_tag(logging_tags, 'OVERSCN{0}'.format(i + 1), overscan_level)
                    self.logger.info('Subtracting overscan', extra=logging_tags)
            else:
                overscan_level = _subtract_overscan_2d(image)
                logs.add_tag(logging_tags, 'OVERSCAN', overscan_level)
                self.logger.info('Subtracting overscan', extra=logging_tags)

        return images


class BiasComparer(ApplyCalibration):
    # In a 16 megapixel image, this should flag 0 or 1 pixels statistically, much much less than 5% of the image
    SIGNAL_TO_NOISE_THRESHOLD = 6.0
    ACCEPTABLE_PIXEL_FRACTION = 0.05

    def __init__(self, pipeline_context):
        super(BiasComparer, self).__init__(pipeline_context)

    @property
    def group_by_keywords(self):
        return ['ccdsum']

    def on_missing_master_calibration(self, logging_tags):
        self.logger.warning('No master Bias frame exists. Assuming these images are ok.', logging_tags)

    def apply_master_calibration(self, images, master_calibration_image, logging_tags):
        # Short circuit
        if master_calibration_image.data is None:
            return images

        images_to_reject = []

        for image in images:
            # Estimate the noise in the image
            noise = np.ones(image.data.shape) * image.readnoise

            # If the the fraction of pixels that deviate from the master by a s/n threshold exceeds an acceptable fraction
            bad_pixel_fraction = np.abs(image.data - master_calibration_image.data)
            bad_pixel_fraction /= noise
            bad_pixel_fraction = bad_pixel_fraction >= self.SIGNAL_TO_NOISE_THRESHOLD
            bad_pixel_fraction = bad_pixel_fraction.sum() / float(bad_pixel_fraction.size)

            qc_results = {'BIAS_CAL_DIFF_FRAC': bad_pixel_fraction, 'SN_THRESHOLD': self.SIGNAL_TO_NOISE_THRESHOLD,
                          'ACCEPTABLE_PIXEL_FRACTION': self.ACCEPTABLE_PIXEL_FRACTION}
            for qc_check, qc_result in qc_results.items():
                logs.add_tag(logging_tags, qc_check, qc_result)

            if bad_pixel_fraction > self.ACCEPTABLE_PIXEL_FRACTION:
                # Reject the image and log an error
                images_to_reject.append(image)
                qc_results['REJECTED'] = True
                logs.add_tag(logging_tags, 'REJECTED', True)
                self.logger.error('Rejecting bias image because it deviates too much from the previous master',
                                  extra=logging_tags)
            else:
                qc_results['REJECTED'] = False

                self.save_qc_results(qc_results, image)

        for image_to_reject in images_to_reject:
            images.remove(image_to_reject)
        return images


def _subtract_overscan_3d(image, i):
    overscan_region = fits_utils.parse_region_keyword(image.extension_headers[i].get('BIASSEC'))
    if overscan_region is not None:
        overscan_level = stats.sigma_clipped_mean(image.data[i][overscan_region], 3)
        image.header['L1STATOV'] = (1, 'Status flag for overscan correction')
    else:
        overscan_level = 0.0
        image.header['L1STATOV'] = (0, 'Status flag for overscan correction')

    overscan_comment = 'Overscan value that was subtracted from Q{0}'.format(i + 1)
    image.header['OVERSCN{0}'.format(i + 1)] = (overscan_level, overscan_comment)

    image.data[i] -= overscan_level
    return overscan_level


def _subtract_overscan_2d(image):
    overscan_region = fits_utils.parse_region_keyword(image.header.get('BIASSEC'))
    if overscan_region is not None:
        overscan_level = stats.sigma_clipped_mean(image.data[overscan_region], 3)
        image.header['L1STATOV'] = (1, 'Status flag for overscan correction')
    else:
        overscan_level = 0.0
        image.header['L1STATOV'] = (0, 'Status flag for overscan correction')

    image.header['OVERSCAN'] = (overscan_level, 'Overscan value that was subtracted')
    image.data -= overscan_level
    return overscan_level
