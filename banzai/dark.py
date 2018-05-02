from __future__ import absolute_import, division, print_function, unicode_literals

import numpy as np
import os.path

from banzai.utils import stats, fits_utils
from banzai import logs
from banzai.images import Image
from banzai.stages import CalibrationMaker, ApplyCalibration

__author__ = 'cmccully'


class DarkMaker(CalibrationMaker):
    def __init__(self, pipeline_context):
        super(DarkMaker, self).__init__(pipeline_context)

    @property
    def group_by_keywords(self):
        return ['ccdsum']

    @property
    def calibration_type(self):
        return 'DARK'

    @property
    def min_images(self):
        return 5

    def make_master_calibration_frame(self, images, image_config, logging_tags):
        dark_data = np.zeros((images[0].ny, images[0].nx, len(images)), dtype=np.float32)
        dark_mask = np.zeros((images[0].ny, images[0].nx, len(images)), dtype=np.uint8)

        master_dark_filename = self.get_calibration_filename(images[0])

        logs.add_tag(logging_tags, 'master_dark', os.path.basename(master_dark_filename))
        for i, image in enumerate(images):
            logs.add_tag(logging_tags, 'filename', os.path.basename(image.filename))
            self.logger.debug('Combining dark', extra=logging_tags)

            dark_data[:, :, i] = image.data[:, :]
            dark_data[:, :, i] /= image.exptime
            dark_mask[:, :, i] = image.bpm[:, :]

        master_dark = stats.sigma_clipped_mean(dark_data, 3.0, axis=2, mask=dark_mask, inplace=True)

        # Memory cleanup
        del dark_data
        del dark_mask

        master_bpm = np.array(master_dark == 0.0, dtype=np.uint8)
        master_dark[master_bpm] = 0.0

        # Save the master dark image with all of the combined images in the header
        master_dark_header = fits_utils.create_master_calibration_header(images)
        master_dark_image = Image(self.pipeline_context, data=master_dark,
                                  header=master_dark_header)
        master_dark_image.filename = master_dark_filename
        master_dark_image.bpm = master_bpm

        logs.pop_tag(logging_tags, 'master_dark')
        logs.add_tag(logging_tags, 'filename', os.path.basename(master_dark_image.filename))
        self.logger.info('Created master dark', extra=logging_tags)
        return [master_dark_image]


class DarkSubtractor(ApplyCalibration):
    def __init__(self, pipeline_context):
        super(DarkSubtractor, self).__init__(pipeline_context)

    @property
    def calibration_type(self):
        return 'dark'

    @property
    def group_by_keywords(self):
        return ['ccdsum']

    def apply_master_calibration(self, images, master_calibration_image, logging_tags):
        master_dark_data = master_calibration_image.data
        master_dark_filename = os.path.basename(master_calibration_image.filename)
        logs.add_tag(logging_tags, 'master_dark',
                     os.path.basename(master_calibration_image.filename))

        for image in images:
            logs.add_tag(logging_tags, 'filename', os.path.basename(image.filename))
            self.logger.info('Subtracting dark', extra=logging_tags)
            image.data -= master_dark_data * image.exptime
            image.bpm |= master_calibration_image.bpm
            image.header['L1IDDARK'] = (master_dark_filename, 'ID of dark frame used')
            image.header['L1STATDA'] = (1, 'Status flag for dark frame correction')
        return images


class DarkComparer(ApplyCalibration):
    # In a 16 megapixel image, this should flag 0 or 1 pixels statistically, much much less than 5% of the image
    SIGNAL_TO_NOISE_THRESHOLD = 6.0
    ACCEPTABLE_PIXEL_FRACTION = 0.05

    def __init__(self, pipeline_context):
        super(DarkComparer, self).__init__(pipeline_context)

    @property
    def group_by_keywords(self):
        return ['ccdsum']

    @property
    def calibration_type(self):
        return 'dark'

    def on_missing_master_calibration(self, logging_tags):
        self.logger.warning('No master Dark frame exists. Assuming these images are ok.', logging_tags)

    def apply_master_calibration(self, images, master_calibration_image, logging_tags):
        # Short circuit
        if master_calibration_image.data is None:
            return images

        images_to_reject = []

        for image in images:
            # Scale the dark frame by the exposure time first!
            bad_pixel_fraction = np.abs(image.data / image.exptime - master_calibration_image.data)
            # Estimate the noise of the image
            noise = (image.readnoise ** 2.0 + np.abs(image.data)) ** 0.5
            noise /= image.exptime
            bad_pixel_fraction /= noise
            bad_pixel_fraction = bad_pixel_fraction >= self.SIGNAL_TO_NOISE_THRESHOLD
            bad_pixel_fraction = bad_pixel_fraction.sum() / float(bad_pixel_fraction.size)

            qc_results = {'DARK_MASTER_DIFF_FRAC': bad_pixel_fraction,
                          'DARK_SN_THRESHOLD': self.SIGNAL_TO_NOISE_THRESHOLD,
                          'DARK_ACCEPTABLE_PIXEL_FRACTION': self.ACCEPTABLE_PIXEL_FRACTION}
            for qc_check, qc_result in qc_results.items():
                logs.add_tag(logging_tags, qc_check, qc_result)

            if bad_pixel_fraction > self.ACCEPTABLE_PIXEL_FRACTION:
                # Reject the image and log an error
                images_to_reject.append(image)
                qc_results['REJECTED'] = True
                logs.add_tag(logging_tags, 'REJECTED', True)
                self.logger.error('Rejecting dark image because it deviates too much from the previous master',
                                  extra=logging_tags)
            else:
                qc_results['REJECTED'] = False

            self.save_qc_results(qc_results, image)

        for image_to_reject in images_to_reject:
            images.remove(image_to_reject)
        return images
